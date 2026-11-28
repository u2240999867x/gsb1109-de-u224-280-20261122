"""End-to-end tests for Catalog.repack_assets and the render repack module."""

from __future__ import annotations

import tempfile
from fractions import Fraction
from pathlib import Path
from typing import Any

import pytest

import pystac
from pystac.asset import Asset
from pystac.catalog import Catalog
from pystac.collection import Collection
from pystac.extensions.repack import RepackError, parse_expression
from pystac.item import Item

RENDER_SCHEMA = "https://stac-extensions.github.io/render/v2.0.0/schema.json"

# Sample pixel used by the scenario: A=20, B=10, C=60.
SAMPLE = {"A": 20, "B": 10, "C": 60}


def _item(item_id: str, collection_id: str) -> Item:
    item = Item(
        id=item_id,
        geometry=None,
        bbox=None,
        datetime=pystac.utils.now_in_utc(),
        properties={},
    )
    item.collection_id = collection_id
    for key in ("A", "B", "C"):
        item.add_asset(key, Asset(href=f"{item_id}-{key}.tif", title=key))
    return item


def _build_catalog() -> tuple[Catalog, Collection, Item, Item, Item]:
    catalog = Catalog(id="water-catalog", description="standing water imagery")
    collection = Collection(
        id="water-collection",
        description="shared render recipes",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent([[None, None]]),
        ),
        stac_extensions=[RENDER_SCHEMA],
        extra_fields={
            "renders": {
                "contrast": {
                    "title": "Contrast",
                    "assets": ["A", "B", "C"],
                    "bidx": [1, 1, 1],
                    "expression": "(b1-b3)/(b1+b3)",
                    "rescale": [[-1, 1]],
                    "nodata": -9999,
                }
            },
        },
    )
    catalog.add_child(collection)

    upstream = _item("upstream-station", collection.id)
    harbor = _item("harbor", collection.id)
    riverbend = _item("riverbend", collection.id)

    # The harbor ships its own item-level card in addition to the shared one.
    harbor.stac_extensions = [RENDER_SCHEMA]
    harbor.properties["renders"] = {
        "difference": {
            "title": "Harbor difference",
            "assets": ["A", "B"],
            "bidx": [1, 1],
            "expression": "b1-b2",
            "colormap_name": "viridis",
            "resampling": "bilinear",
        }
    }

    collection.add_items([upstream, harbor, riverbend])
    return catalog, collection, upstream, harbor, riverbend


def _pack_asset() -> dict[str, Any]:
    return {
        "href": "pack.tif",
        "type": "image/tiff; application=geotiff; profile=cloud-optimized",
        "roles": ["data"],
    }


def _term(asset: str, band: int, coefficient: Any = 1) -> dict[str, Any]:
    return {"asset": asset, "band": band, "coefficient": coefficient}


def evaluate(card: dict[str, Any], values: dict[tuple[str, int], Fraction]):
    """Evaluate a render card exactly the way the web page reads it.

    Returns None to represent the no-data position shown when a ratio's
    denominator evaluates to zero.
    """
    parsed = parse_expression(card["expression"])
    positional = [
        Fraction(values[(asset_key, band_index)])
        for asset_key, band_index in zip(card["assets"], card["bidx"])
    ]

    def value_of(affine: Any) -> Fraction:
        total = affine.constant
        for ref, coeff in affine.coeffs.items():
            total += coeff * positional[ref.index - 1]
        return total

    numerator = value_of(parsed.numerator)
    if parsed.denominator is None:
        return numerator
    denominator = value_of(parsed.denominator)
    if denominator == 0:
        return None
    return numerator / denominator


def _original_values() -> dict[tuple[str, int], Fraction]:
    return {
        ("A", 1): Fraction(SAMPLE["A"]),
        ("B", 1): Fraction(SAMPLE["B"]),
        ("C", 1): Fraction(SAMPLE["C"]),
    }


def _pack_values_batch1() -> dict[tuple[str, int], Fraction]:
    a, b, c = (Fraction(SAMPLE[key]) for key in ("A", "B", "C"))
    return {
        ("pack", 1): a + b,  # U = A + B
        ("pack", 2): b + c,  # V = B + C
        ("pack", 3): a + c,  # T = A + C
    }


def _first_manifest() -> dict[str, Any]:
    return {
        "harbor": {
            "pack": {
                "asset": _pack_asset(),
                "bands": [
                    {"terms": [_term("A", 1), _term("B", 1)]},
                    {"terms": [_term("B", 1), _term("C", 1)]},
                    {"terms": [_term("A", 1), _term("C", 1)]},
                ],
            }
        },
        "riverbend": {
            "pack": {
                "asset": _pack_asset(),
                "bands": [
                    {"terms": [_term("A", 1), _term("B", 1)]},
                    {"terms": [_term("B", 1), _term("C", 1)]},
                    {"terms": [_term("A", 1), _term("C", 1)]},
                ],
            }
        },
    }


def test_first_batch_preserves_cards_and_saves() -> None:
    catalog, collection, upstream, harbor, riverbend = _build_catalog()

    original_contrast = collection.extra_fields["renders"]["contrast"]
    original_values = _original_values()
    expected_contrast = evaluate(original_contrast, original_values)
    original_difference = harbor.properties["renders"]["difference"]
    expected_difference = evaluate(original_difference, original_values)
    assert expected_contrast == Fraction(-1, 2)
    assert expected_difference == Fraction(10)

    repacked = catalog.repack_assets(_first_manifest())

    # The source catalog is untouched and can be retried.
    assert set(harbor.assets) == {"A", "B", "C"}
    assert "renders" not in riverbend.properties

    repacked_items = {item.id: item for item in repacked.get_items(recursive=True)}
    new_harbor = repacked_items["harbor"]
    new_riverbend = repacked_items["riverbend"]
    new_upstream = repacked_items["upstream-station"]

    # Old per-band files are replaced by the combined pack.
    assert set(new_harbor.assets) == {"pack"}
    assert set(new_riverbend.assets) == {"pack"}
    assert set(new_upstream.assets) == {"A", "B", "C"}

    pack_values = _pack_values_batch1()
    harbor_contrast = new_harbor.properties["renders"]["contrast"]
    harbor_difference = new_harbor.properties["renders"]["difference"]
    riverbend_contrast = new_riverbend.properties["renders"]["contrast"]

    assert evaluate(harbor_contrast, pack_values) == expected_contrast
    assert evaluate(harbor_difference, pack_values) == expected_difference
    assert evaluate(riverbend_contrast, pack_values) == expected_contrast

    # Item-level cards keep their title and display settings.
    assert harbor_difference["title"] == "Harbor difference"
    assert harbor_difference["colormap_name"] == "viridis"
    assert harbor_difference["resampling"] == "bilinear"
    assert harbor_contrast["title"] == "Contrast"
    assert harbor_contrast["rescale"] == [[-1, 1]]
    assert harbor_contrast["nodata"] == -9999

    # Unaffected items keep using the collection's original recipe.
    assert "renders" not in new_upstream.properties
    assert collection.extra_fields["renders"]["contrast"] == original_contrast

    with tempfile.TemporaryDirectory() as tmp_dir:
        repacked.normalize_hrefs(str(Path(tmp_dir) / "cat"))
        repacked.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
        reloaded = Catalog.from_file(str(Path(tmp_dir) / "cat" / "catalog.json"))
        reloaded_items = {item.id: item for item in reloaded.get_items(recursive=True)}
        reloaded_collection = reloaded_items["harbor"].get_collection()
        assert reloaded_collection is not None

        reloaded_upstream = reloaded_items["upstream-station"]
        assert "renders" not in reloaded_upstream.properties
        assert (
            reloaded_collection.extra_fields["renders"]["contrast"] == original_contrast
        )

        reloaded_harbor = reloaded_items["harbor"]
        reloaded_riverbend = reloaded_items["riverbend"]
        assert set(reloaded_harbor.assets) == {"pack"}
        assert set(reloaded_riverbend.assets) == {"pack"}
        assert evaluate(
            reloaded_harbor.properties["renders"]["contrast"], pack_values
        ) == Fraction(-1, 2)
        assert evaluate(
            reloaded_harbor.properties["renders"]["difference"], pack_values
        ) == Fraction(10)
        assert evaluate(
            reloaded_riverbend.properties["renders"]["contrast"], pack_values
        ) == Fraction(-1, 2)


def _second_manifest(harbor_fixed: bool) -> dict[str, Any]:
    # U, V, T refer to the first-batch pack bands; the asset key stays "pack".
    # Both items deliver D = U - V (band 1) and T (band 2). The riverbend keeps
    # the constant 0*U + 7; the harbor is fixed by delivering V instead.
    riverbend_bands = [
        {"terms": [_term("pack", 1, 1), _term("pack", 2, -1)]},
        {"terms": [_term("pack", 3, 1)]},
        {"terms": [], "constant": 7},
    ]
    harbor_bands = [
        {"terms": [_term("pack", 1, 1), _term("pack", 2, -1)]},
        {"terms": [_term("pack", 3, 1)]},
        {"terms": [_term("pack", 2, 1)]}
        if harbor_fixed
        else {"terms": [], "constant": 7},
    ]
    return {
        "harbor": {"pack": {"asset": _pack_asset(), "bands": harbor_bands}},
        "riverbend": {"pack": {"asset": _pack_asset(), "bands": riverbend_bands}},
    }


def test_second_batch_rejects_harbor_difference_as_a_whole() -> None:
    catalog, _collection, _upstream, _harbor, _riverbend = _build_catalog()
    first = catalog.repack_assets(_first_manifest())

    with tempfile.TemporaryDirectory() as tmp_dir:
        first.normalize_hrefs(str(Path(tmp_dir) / "cat"))
        first.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
        reloaded = Catalog.from_file(str(Path(tmp_dir) / "cat" / "catalog.json"))

        before_items = {item.id: item for item in reloaded.get_items(recursive=True)}
        before_harbor_assets = dict(before_items["harbor"].assets)
        before_riverbend_renders = dict(
            before_items["riverbend"].properties.get("renders", {})
        )

        with pytest.raises(RepackError, match="harbor") as exc_info:
            reloaded.repack_assets(_second_manifest(harbor_fixed=False))
        assert "difference" in str(exc_info.value)

        # Whole-batch rejection: nothing on the reloaded catalog was mutated,
        # so the same input directory can be retried once the manifest is fixed.
        after_items = {item.id: item for item in reloaded.get_items(recursive=True)}
        assert after_items["harbor"].assets == before_harbor_assets
        assert (
            dict(after_items["riverbend"].properties.get("renders", {}))
            == before_riverbend_renders
        )
        assert set(after_items["harbor"].assets) == {"pack"}


def test_fixed_second_batch_succeeds_and_ignores_constant() -> None:
    catalog, _collection, _upstream, _harbor, _riverbend = _build_catalog()
    first = catalog.repack_assets(_first_manifest())

    with tempfile.TemporaryDirectory() as tmp_dir:
        first.normalize_hrefs(str(Path(tmp_dir) / "cat1"))
        first.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
        reloaded = Catalog.from_file(str(Path(tmp_dir) / "cat1" / "catalog.json"))
        fixed = reloaded.repack_assets(_second_manifest(harbor_fixed=True))

        fixed.normalize_hrefs(str(Path(tmp_dir) / "cat2"))
        fixed.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
        reloaded_again = Catalog.from_file(str(Path(tmp_dir) / "cat2" / "catalog.json"))

        items = {item.id: item for item in reloaded_again.get_items(recursive=True)}
        harbor = items["harbor"]
        riverbend = items["riverbend"]
        upstream = items["upstream-station"]

        a, b, c = (Fraction(SAMPLE[key]) for key in ("A", "B", "C"))
        u, v, t = a + b, b + c, a + c
        d = u - v
        second_pack = {
            ("pack", 1): d,
            ("pack", 2): t,
            ("pack", 3): v,
        }

        harbor_contrast = harbor.properties["renders"]["contrast"]
        harbor_difference = harbor.properties["renders"]["difference"]
        riverbend_contrast = riverbend.properties["renders"]["contrast"]

        assert evaluate(harbor_contrast, second_pack) == Fraction(-1, 2)
        assert evaluate(harbor_difference, second_pack) == Fraction(10)
        assert evaluate(riverbend_contrast, second_pack) == Fraction(-1, 2)

        # The riverbend contrast only needs D and T; the third delivered band
        # (constant or otherwise) is absent from the card inputs.
        assert set(riverbend_contrast["assets"]) == {"pack"}
        assert sorted(riverbend_contrast["bidx"]) == [1, 2]

        # No data whenever the denominator T is zero.
        zero_denominator = {
            ("pack", 1): Fraction(5),
            ("pack", 2): Fraction(0),
            ("pack", 3): Fraction(9),
        }
        assert evaluate(riverbend_contrast, zero_denominator) is None

        # The upstream station still follows the untouched collection recipe.
        assert "renders" not in upstream.properties
        collection = upstream.get_collection()
        assert collection is not None
        assert collection.extra_fields["renders"]["contrast"]["assets"] == [
            "A",
            "B",
            "C",
        ]


def test_manifest_missing_d_rejects_riverbend_contrast() -> None:
    catalog, _collection, _upstream, _harbor, _riverbend = _build_catalog()
    first = catalog.repack_assets(_first_manifest())
    items = {item.id: item for item in first.get_items(recursive=True)}

    # Only T and a constant remain: D is missing, so A and C cannot be split.
    manifest = {
        "riverbend": {
            "pack": {
                "asset": _pack_asset(),
                "bands": [
                    {"terms": [_term("pack", 3, 1)]},
                    {"terms": [], "constant": 7},
                ],
            }
        }
    }
    with pytest.raises(RepackError, match="riverbend") as exc_info:
        first.repack_assets(manifest)
    assert "contrast" in str(exc_info.value)

    # The original catalog remains usable for a corrected retry.
    assert set(items["riverbend"].assets) == {"pack"}


def test_accepts_json_string_manifest() -> None:
    import json

    catalog, _collection, _upstream, _harbor, _riverbend = _build_catalog()
    repacked = catalog.repack_assets(json.dumps(_first_manifest()))
    items = {item.id: item for item in repacked.get_items(recursive=True)}
    assert set(items["harbor"].assets) == {"pack"}


def test_unknown_item_and_bad_manifest_are_rejected() -> None:
    catalog, _collection, _upstream, _harbor, _riverbend = _build_catalog()
    with pytest.raises(RepackError, match="unknown items"):
        catalog.repack_assets(
            {"ghost": {"pack": {"asset": _pack_asset(), "bands": [{"terms": []}]}}}
        )
    with pytest.raises(ValueError, match="non-empty"):
        catalog.repack_assets({"harbor": {"pack": {"asset": _pack_asset()}}})


def test_card_preserved_even_when_individual_band_is_not() -> None:
    catalog = Catalog(id="dep-catalog", description="dependent bands")
    collection = Collection(
        id="dep-collection",
        description="recipes",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent([[None, None]]),
        ),
        stac_extensions=[RENDER_SCHEMA],
        extra_fields={
            "renders": {
                # The sum X + Y is exactly the single delivered band, so the
                # card survives even though X and Y cannot be split apart.
                "sum": {
                    "title": "Sum",
                    "assets": ["X", "Y"],
                    "expression": "b1+b2",
                },
                # A pure-constant card stays constant and still names an input.
                "fixed": {
                    "title": "Fixed",
                    "assets": ["X"],
                    "expression": "b1-b1+3",
                },
            }
        },
    )
    catalog.add_child(collection)
    item = Item(
        id="dep-item",
        geometry=None,
        bbox=None,
        datetime=pystac.utils.now_in_utc(),
        properties={},
    )
    item.collection_id = collection.id
    item.add_asset("X", Asset(href="X.tif"))
    item.add_asset("Y", Asset(href="Y.tif"))
    collection.add_item(item)

    manifest = {
        "dep-item": {
            "pack": {
                "asset": _pack_asset(),
                "bands": [{"terms": [_term("X", 1), _term("Y", 1)]}],
            }
        }
    }

    repacked = catalog.repack_assets(manifest)
    new_item = next(repacked.get_items(recursive=True))
    renders = new_item.properties["renders"]

    values = {("pack", 1): Fraction(7)}
    assert evaluate(renders["sum"], values) == Fraction(7)
    assert evaluate(renders["fixed"], values) == Fraction(3)
    assert renders["fixed"]["assets"] == ["pack"]
    assert renders["fixed"]["bidx"] == [1]
    assert renders["sum"]["title"] == "Sum"

    # Requiring X on its own is impossible from a single X + Y band. The
    # original catalog is still intact and can be retried after the fix.
    collection.extra_fields["renders"]["xonly"] = {
        "title": "X only",
        "assets": ["X"],
        "expression": "b1",
    }
    with pytest.raises(RepackError, match="xonly"):
        catalog.repack_assets(manifest)


def test_undelivered_band_card_rejects_and_leaves_catalog_intact() -> None:
    catalog, _collection, _upstream, harbor, _riverbend = _build_catalog()
    original_assets = set(harbor.assets)

    # A new collection card needs B, but the pack only derives A and C.
    collection = harbor.get_collection()
    assert collection is not None
    collection.extra_fields["renders"]["bonly"] = {
        "title": "B only",
        "assets": ["B"],
        "expression": "b1",
    }
    manifest = {
        "harbor": {
            "pack": {
                "asset": _pack_asset(),
                "bands": [
                    {"terms": [_term("A", 1)]},
                    {"terms": [_term("C", 1)]},
                ],
            }
        }
    }

    with pytest.raises(RepackError, match="harbor.*bonly"):
        catalog.repack_assets(manifest)

    # Nothing on the source catalog changed: no pack asset and no rewritten
    # collection cards were copied onto the item.
    assert set(harbor.assets) == original_assets
    assert set(harbor.properties.get("renders", {})) == {"difference"}


def test_rational_coefficients_are_preserved_exactly() -> None:
    catalog = Catalog(id="ratio-catalog", description="rational coefficients")
    collection = Collection(
        id="ratio-collection",
        description="recipes",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent([[None, None]]),
        ),
        stac_extensions=[RENDER_SCHEMA],
        extra_fields={
            "renders": {
                "half": {
                    "title": "Half",
                    "assets": ["A"],
                    "expression": "b1/2",
                }
            }
        },
    )
    catalog.add_child(collection)
    item = Item(
        id="ratio-item",
        geometry=None,
        bbox=None,
        datetime=pystac.utils.now_in_utc(),
        properties={},
    )
    item.collection_id = collection.id
    item.add_asset("A", Asset(href="A.tif"))
    collection.add_item(item)

    manifest = {
        "ratio-item": {
            "pack": {
                "asset": _pack_asset(),
                "bands": [{"terms": [_term("A", 1, "1/2")], "constant": 3}],
            }
        }
    }
    repacked = catalog.repack_assets(manifest)
    new_item = next(repacked.get_items(recursive=True))
    card = new_item.properties["renders"]["half"]
    # pack/2 = A/4... instead the card must recover A exactly: A = 2*(pack - 3),
    # so A/2 = pack - 3.
    assert evaluate(card, {("pack", 1): Fraction(1, 2) + 3}) == Fraction(1, 2)
