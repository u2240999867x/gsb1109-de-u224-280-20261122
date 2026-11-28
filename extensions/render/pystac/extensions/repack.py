"""Band-pack replacement for catalog assets and render cards.

This module powers :meth:`pystac.Catalog.repack_assets`. A supplier manifest
describes, per affected item, the new multi-band assets that replace the
original single-band assets. Every new band is an affine combination of the
original bands::

    new_band = sum(coeff * (asset_key, band_index)) + constant

The render cards (the :stac-ext:`Render Extension <render>` ``renders``
property) used by the web page are then rewritten so that each card keeps the
exact same pixel values and no-data positions using only the new bands. A card
that cannot be preserved makes the whole batch fail; the original catalog is
never modified and can simply be retried.
"""

from __future__ import annotations

import ast
import json
from copy import deepcopy
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Any

import pystac
from pystac.asset import Asset
from pystac.errors import STACError
from pystac.item import Item

#: Key of the ``renders`` property defined by the Render extension.
RENDERS_PROP = "renders"

#: Schema URI of the Render extension, added to repacked items.
RENDER_SCHEMA_URI = "https://stac-extensions.github.io/render/v2.0.0/schema.json"

#: Schema URI prefix used to detect the Render extension.
_RENDER_SCHEMA_PREFIX = "https://stac-extensions.github.io/render/"


class RepackError(STACError):
    """Raised when a supplier manifest cannot preserve one or more render
    cards.

    The catalog passed to :meth:`~pystac.Catalog.repack_assets` is left
    untouched, so the same catalog can be retried once the manifest is fixed.
    """


# ---------------------------------------------------------------------------
# Manifest data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BandRef:
    """A one-based band of an asset, e.g. ``BandRef("image", 2)``."""

    asset: str
    index: int

    def __post_init__(self) -> None:
        if self.index < 1:
            raise ValueError(
                f"band index must be one-based and positive, got {self.index}"
            )


@dataclass(frozen=True)
class Affine:
    """An affine combination of bands plus a constant.

    ``value = sum(coeff[ref] * ref) + constant`` where every coefficient and
    the constant are exact rationals.
    """

    coeffs: dict[BandRef, Fraction] = field(default_factory=dict)
    constant: Fraction = Fraction(0)

    def is_constant(self) -> bool:
        """True when the combination does not depend on any band."""
        return not self.coeffs


@dataclass(frozen=True)
class NewAssetSpec:
    """A replacement asset and the affine definition of each of its bands."""

    asset: Asset
    bands: list[Affine]

    def __post_init__(self) -> None:
        if not self.bands:
            raise ValueError("a replacement asset must define at least one band")


@dataclass(frozen=True)
class ItemRepackSpec:
    """Replacement assets for a single item, keyed by the new asset key."""

    assets: dict[str, NewAssetSpec]


# ---------------------------------------------------------------------------
# Manifest parsing
# ---------------------------------------------------------------------------


def _coerce_rational(value: Any) -> Fraction:
    if isinstance(value, bool):
        raise ValueError(f"boolean is not a valid rational constant: {value!r}")
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, float):
        return Fraction(value).limit_denominator()
    if isinstance(value, str):
        return Fraction(value)
    raise ValueError(f"unsupported rational constant: {value!r}")


def _parse_affine(data: Any) -> Affine:
    if not isinstance(data, dict):
        raise ValueError(
            "each band definition must be an object with 'constant' and/or "
            f"'terms', got {data!r}"
        )
    coeffs: dict[BandRef, Fraction] = {}
    terms = data.get("terms", [])
    if not isinstance(terms, list):
        raise ValueError(f"'terms' must be a list, got {terms!r}")
    for term in terms:
        if not isinstance(term, dict):
            raise ValueError(f"each term must be an object, got {term!r}")
        try:
            asset_key = term["asset"]
            band_index = term["band"]
        except KeyError as e:
            raise ValueError(
                f"term {term!r} is missing required key {e.args[0]!r}"
            ) from None
        if not isinstance(asset_key, str):
            raise ValueError(f"term asset key must be a string, got {asset_key!r}")
        if isinstance(band_index, bool) or not isinstance(band_index, int):
            raise ValueError(f"term band must be an integer, got {band_index!r}")
        ref = BandRef(asset_key, band_index)
        coeff = _coerce_rational(term.get("coefficient", 1))
        coeffs[ref] = coeffs.get(ref, Fraction(0)) + coeff
    constant = _coerce_rational(data.get("constant", 0))
    return Affine(
        coeffs={ref: coeff for ref, coeff in coeffs.items() if coeff != 0},
        constant=constant,
    )


def _parse_new_asset(data: Any) -> NewAssetSpec:
    if not isinstance(data, dict):
        raise ValueError(f"replacement asset must be an object, got {data!r}")
    if "asset" not in data:
        raise ValueError(f"replacement asset {data!r} is missing 'asset'")
    asset = Asset.from_dict(deepcopy(data["asset"]))
    bands_data = data.get("bands")
    if not isinstance(bands_data, list) or not bands_data:
        raise ValueError(
            f"replacement asset {data!r} must define a non-empty 'bands' list"
        )
    bands = [_parse_affine(band_data) for band_data in bands_data]
    return NewAssetSpec(asset=asset, bands=bands)


def parse_manifest(manifest: dict[str, Any]) -> dict[str, ItemRepackSpec]:
    """Parse a supplier manifest into validated :class:`ItemRepackSpec` objects.

    The manifest maps item ids to item entries. Each item entry maps an asset
    key to a replacement asset with a ``bands`` list. Each band is an object
    with ``constant`` and/or ``terms``. Each term references an original
    ``asset`` key, a one-based ``band`` number and an optional rational
    ``coefficient`` (defaults to 1).
    """
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be a mapping of item id to item entry")
    result: dict[str, ItemRepackSpec] = {}
    for item_id, item_data in manifest.items():
        if not isinstance(item_id, str):
            raise ValueError(f"manifest item ids must be strings, got {item_id!r}")
        if not isinstance(item_data, dict):
            raise ValueError(f"manifest entry for item {item_id!r} must be an object")
        assets: dict[str, NewAssetSpec] = {}
        for asset_key, asset_data in item_data.items():
            if not isinstance(asset_key, str):
                raise ValueError(f"asset keys must be strings, got {asset_key!r}")
            assets[asset_key] = _parse_new_asset(asset_data)
        if not assets:
            raise ValueError(f"manifest entry for item {item_id!r} is empty")
        result[item_id] = ItemRepackSpec(assets=assets)
    if not result:
        raise ValueError("manifest must describe at least one item")
    return result


# ---------------------------------------------------------------------------
# Rational expression parsing (render card expressions)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Expr:
    """Parsed render expression: an affine, optionally over a denominator."""

    numerator: Affine
    denominator: Affine | None


_EXPR_ALLOWED = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.USub,
    ast.UAdd,
    ast.Name,
    ast.Load,
    ast.Constant,
)


def _affine_from_node(node: ast.AST) -> Affine:
    if isinstance(node, ast.Name):
        if not node.id.startswith("b") or not node.id[1:].isdigit():
            raise ValueError(f"invalid band reference {node.id!r}; use b1, b2, ...")
        index = int(node.id[1:])
        if index < 1:
            raise ValueError(f"band references must be one-based, got {node.id!r}")
        return Affine(coeffs={BandRef("__input__", index): Fraction(1)})
    if isinstance(node, ast.Constant):
        if isinstance(node.value, bool) or not isinstance(
            node.value, (int, float, str)
        ):
            raise ValueError(f"unsupported constant {node.value!r}")
        return Affine(constant=_coerce_rational(node.value))
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return _scale_affine(_affine_from_node(node.operand), Fraction(-1))
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.UAdd):
        return _affine_from_node(node.operand)
    if isinstance(node, ast.BinOp):
        left = _affine_from_node(node.left)
        if isinstance(node.op, ast.Add):
            return _add_affine(left, _affine_from_node(node.right))
        if isinstance(node.op, ast.Sub):
            return _add_affine(
                left, _scale_affine(_affine_from_node(node.right), Fraction(-1))
            )
        if isinstance(node.op, ast.Mult):
            return _multiply_affine(left, _affine_from_node(node.right))
        if isinstance(node.op, ast.Div):
            right = _affine_from_node(node.right)
            if not right.is_constant():
                raise ValueError(
                    "only division by a rational constant is allowed inside a "
                    "linear combination"
                )
            return _scale_affine(left, Fraction(1) / right.constant)
    raise ValueError(f"unsupported expression syntax at {ast.dump(node)}")


def parse_expression(expression: str) -> _Expr:
    """Parse a render ``expression`` into affine form.

    Accepted expressions use rational constants, parentheses and the four
    arithmetic operators. Band inputs are named ``b1``, ``b2`` ... and refer
    to the positional inputs of the card (``assets``/``bidx``). The expression
    is either a single affine combination plus a constant, or the division of
    two such combinations.
    """
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"invalid expression {expression!r}: {e.msg}") from None
    for node in ast.walk(tree):
        if not isinstance(node, _EXPR_ALLOWED):
            raise ValueError(
                f"expression {expression!r} uses unsupported syntax "
                f"{type(node).__name__}"
            )
    body = tree.body
    if isinstance(body, ast.BinOp) and isinstance(body.op, ast.Div):
        numerator = _affine_from_node(body.left)
        denominator = _affine_from_node(body.right)
        if denominator.is_constant():
            return _Expr(
                _scale_affine(numerator, Fraction(1) / denominator.constant),
                None,
            )
        return _Expr(numerator, denominator)
    return _Expr(_affine_from_node(body), None)


def _add_affine(left: Affine, right: Affine) -> Affine:
    coeffs = dict(left.coeffs)
    for ref, coeff in right.coeffs.items():
        coeffs[ref] = coeffs.get(ref, Fraction(0)) + coeff
    return Affine(
        coeffs={ref: coeff for ref, coeff in coeffs.items() if coeff != 0},
        constant=left.constant + right.constant,
    )


def _scale_affine(affine: Affine, factor: Fraction) -> Affine:
    if factor == 0:
        return Affine()
    return Affine(
        coeffs={ref: coeff * factor for ref, coeff in affine.coeffs.items()},
        constant=affine.constant * factor,
    )


def _multiply_affine(left: Affine, right: Affine) -> Affine:
    if not left.is_constant() and not right.is_constant():
        raise ValueError(
            "multiplying two band-dependent terms is not a linear combination"
        )
    if left.is_constant():
        return _scale_affine(right, left.constant)
    return _scale_affine(left, right.constant)


# ---------------------------------------------------------------------------
# Exact linear algebra over the original bands
# ---------------------------------------------------------------------------


def _new_band_table(spec: ItemRepackSpec) -> dict[BandRef, Affine]:
    """Map every delivered band (asset key + one-based index) to its affine
    definition in terms of the original bands."""
    table: dict[BandRef, Affine] = {}
    for asset_key, asset_spec in spec.assets.items():
        for band_index, affine in enumerate(asset_spec.bands, start=1):
            table[BandRef(asset_key, band_index)] = affine
    return table


def _target_vector(
    affine: Affine, inputs: list[BandRef], variable_refs: list[BandRef]
) -> list[Fraction]:
    """Coefficient vector of a card affine over the ordered original bands.

    Card placeholders (``b1``, ``b2`` ...) are first mapped onto the actual
    asset bands the card reads through ``assets``/``bidx``.
    """
    expanded: dict[BandRef, Fraction] = {}
    for placeholder, coeff in affine.coeffs.items():
        if placeholder.index > len(inputs):
            raise RepackError(
                f"render card references b{placeholder.index} but only "
                f"{len(inputs)} asset inputs are listed"
            )
        original_ref = inputs[placeholder.index - 1]
        expanded[original_ref] = expanded.get(original_ref, Fraction(0)) + coeff
    return [expanded.get(ref, Fraction(0)) for ref in variable_refs]


def _solve_in_row_space(
    target: list[Fraction],
    new_bands: dict[BandRef, Affine],
    variable_refs: list[BandRef],
    item_id: str,
    card_name: str,
) -> Affine:
    """Express a target linear form over the original bands as a combination
    of delivered bands.

    Each delivered band is a row ``n_i = A[i] . x + c_i``. We solve
    ``w . A = target`` exactly via Gaussian elimination on the transposed
    system ``A^T w = target``. If ``target`` is not in the row space of ``A``
    the card cannot be preserved and the batch must be rejected. An individual
    original band need not be recoverable on its own: only the card's overall
    linear form has to lie in the row space.
    """
    delivered = sorted(new_bands, key=lambda ref: (ref.asset, ref.index))
    matrix: list[list[Fraction]] = []
    for var_index, var in enumerate(variable_refs):
        row = [
            new_bands[band_ref].coeffs.get(var, Fraction(0)) for band_ref in delivered
        ]
        row.append(target[var_index])
        matrix.append(row)

    n_rows = len(matrix)
    n_cols = len(delivered)
    # Reduced row echelon form: normalize each pivot row, then clear the pivot
    # column in every other row.
    pivot_row = 0
    for col in range(n_cols):
        candidate = next(
            (r for r in range(pivot_row, n_rows) if matrix[r][col] != 0), None
        )
        if candidate is None:
            continue
        matrix[pivot_row], matrix[candidate] = matrix[candidate], matrix[pivot_row]
        pivot = matrix[pivot_row][col]
        matrix[pivot_row] = [value / pivot for value in matrix[pivot_row]]
        for r in range(n_rows):
            if r == pivot_row or matrix[r][col] == 0:
                continue
            factor = matrix[r][col]
            matrix[r] = [
                value - matrix[pivot_row][c] * factor
                for c, value in enumerate(matrix[r])
            ]
        pivot_row += 1

    for r in range(pivot_row, n_rows):
        if matrix[r][n_cols] != 0:
            raise RepackError(
                f"item {item_id!r}: render card {card_name!r} cannot be "
                "preserved because its band combination is not a linear "
                "combination of the delivered bands"
            )

    weights = [Fraction(0)] * n_cols
    for r in range(pivot_row):
        pivot_col = next(c for c in range(n_cols) if matrix[r][c] != 0)
        weights[pivot_col] = matrix[r][n_cols]

    constant = affine_constant_correction(weights, delivered, new_bands)
    coeffs = {
        band_ref: weight for band_ref, weight in zip(delivered, weights) if weight != 0
    }
    return Affine(coeffs=coeffs, constant=constant)


def affine_constant_correction(
    weights: list[Fraction],
    delivered: list[BandRef],
    new_bands: dict[BandRef, Affine],
) -> Fraction:
    # sum_i w_i * n_i reproduces the target linear form plus
    # sum_i w_i * c_i; subtract that constant to keep the card's offset.
    offset = sum(
        (
            weights[i] * new_bands[band_ref].constant
            for i, band_ref in enumerate(delivered)
        ),
        Fraction(0),
    )
    return -offset


def _rewrite_affine(
    affine: Affine,
    inputs: list[BandRef],
    new_bands: dict[BandRef, Affine],
    item_id: str,
    card_name: str,
) -> Affine:
    """Rewrite an affine over the card's positional inputs in terms of the
    delivered bands, preserving the exact rational value and constant."""
    # Include every band the card reads, even if no delivered band references
    # it; such a column is all zeros in A and forces a rejection.
    delivered_variables = {
        ref
        for delivered_affine in new_bands.values()
        for ref in delivered_affine.coeffs
    }
    variable_refs = sorted(
        delivered_variables | set(inputs),
        key=lambda ref: (ref.asset, ref.index),
    )
    target = _target_vector(affine, inputs, variable_refs)
    solved = _solve_in_row_space(target, new_bands, variable_refs, item_id, card_name)
    return Affine(coeffs=solved.coeffs, constant=solved.constant + affine.constant)


# ---------------------------------------------------------------------------
# Render card rewriting
# ---------------------------------------------------------------------------


def _has_render_extension(obj: pystac.Item | pystac.Collection) -> bool:
    return obj.stac_extensions is not None and any(
        uri.startswith(_RENDER_SCHEMA_PREFIX) for uri in obj.stac_extensions
    )


def _renders_of(obj: pystac.Item | pystac.Collection) -> dict[str, Any]:
    if isinstance(obj, pystac.Item):
        renders = obj.properties.get(RENDERS_PROP)
    else:
        renders = obj.extra_fields.get(RENDERS_PROP)
    return renders or {}


def _card_inputs(card: dict[str, Any], item_id: str) -> list[BandRef]:
    assets = card.get("assets")
    if not isinstance(assets, list) or not assets:
        raise RepackError(
            f"item {item_id!r}: render card must list at least one asset in 'assets'"
        )
    bidx = card.get("bidx")
    if bidx is None:
        indices = [1] * len(assets)
    elif isinstance(bidx, list):
        if len(bidx) != len(assets):
            raise RepackError(
                f"item {item_id!r}: render card 'bidx' must have one entry "
                "per asset when given"
            )
        indices = bidx
    else:
        raise RepackError(f"item {item_id!r}: render card 'bidx' must be a list")
    refs: list[BandRef] = []
    for asset_key, band_index in zip(assets, indices):
        if not isinstance(asset_key, str):
            raise RepackError(
                f"item {item_id!r}: render card asset keys must be strings"
            )
        if isinstance(band_index, bool) or not isinstance(band_index, int):
            raise RepackError(
                f"item {item_id!r}: render card bidx entries must be integers"
            )
        refs.append(BandRef(asset_key, band_index))
    return refs


def _format_rational(value: Fraction) -> str:
    if value.denominator == 1:
        return str(value.numerator)
    return f"{value.numerator}/{value.denominator}"


def _format_affine(affine: Affine) -> str:
    parts: list[str] = []
    for ref, coeff in affine.coeffs.items():
        token = f"b{ref.index}"
        if coeff == 1:
            parts.append(f"+{token}")
        elif coeff == -1:
            parts.append(f"-{token}")
        elif coeff > 0:
            parts.append(f"+{_format_rational(coeff)}*{token}")
        else:
            parts.append(f"-{_format_rational(-coeff)}*{token}")
    if affine.constant != 0 or not parts:
        parts.append(f"+{_format_rational(affine.constant)}")
    expression = "".join(parts)
    if expression.startswith("+"):
        expression = expression[1:]
    return expression


def _as_positional(affine: Affine, order: list[BandRef]) -> Affine:
    return Affine(
        coeffs={
            BandRef("__input__", order.index(ref) + 1): coeff
            for ref, coeff in affine.coeffs.items()
        },
        constant=affine.constant,
    )


def _anchor(order: list[BandRef], new_bands: dict[BandRef, Affine]) -> list[BandRef]:
    """Ensure a rewritten card still names at least one input band, even when
    its formula collapses to a pure constant."""
    if order:
        return order
    return [min(new_bands, key=lambda ref: (ref.asset, ref.index))]


def _rewrite_card(
    name: str,
    card: dict[str, Any],
    inputs: list[BandRef],
    new_bands: dict[BandRef, Affine],
    item_id: str,
) -> dict[str, Any]:
    expression = card.get("expression")
    if not isinstance(expression, str):
        raise RepackError(
            f"item {item_id!r}: render card {name!r} has no band arithmetic "
            "'expression' and cannot be repacked"
        )
    try:
        parsed = parse_expression(expression)
    except ValueError as e:
        raise RepackError(
            f"item {item_id!r}: render card {name!r} has an unsupported expression: {e}"
        ) from e
    numerator = _rewrite_affine(parsed.numerator, inputs, new_bands, item_id, name)
    denominator = (
        _rewrite_affine(parsed.denominator, inputs, new_bands, item_id, name)
        if parsed.denominator is not None
        else None
    )

    # A card that varied with the inputs cannot be served by constant bands.
    def _reject_if_flattened(original: Affine, rewritten: Affine) -> None:
        if original.coeffs and not rewritten.coeffs:
            raise RepackError(
                f"item {item_id!r}: render card {name!r} varies with the input "
                "bands but the delivered bands only provide a constant; a "
                "constant replacement band cannot stand in for an "
                "input-dependent card"
            )

    _reject_if_flattened(parsed.numerator, numerator)
    if parsed.denominator is not None and denominator is not None:
        _reject_if_flattened(parsed.denominator, denominator)

    new_card = dict(card)
    if denominator is None:
        order = _anchor(
            sorted(numerator.coeffs, key=lambda ref: (ref.asset, ref.index)),
            new_bands,
        )
        new_card["assets"] = [ref.asset for ref in order]
        new_card["bidx"] = [ref.index for ref in order]
        new_card["expression"] = _format_affine(_as_positional(numerator, order))
        return new_card

    num_order = sorted(numerator.coeffs, key=lambda ref: (ref.asset, ref.index))
    extra = [
        ref
        for ref in sorted(denominator.coeffs, key=lambda ref: (ref.asset, ref.index))
        if ref not in numerator.coeffs
    ]
    order = _anchor(num_order + extra, new_bands)
    new_card["assets"] = [ref.asset for ref in order]
    new_card["bidx"] = [ref.index for ref in order]
    num_expr = _format_affine(_as_positional(numerator, order))
    den_expr = _format_affine(_as_positional(denominator, order))
    new_card["expression"] = f"({num_expr})/({den_expr})"
    return new_card


# ---------------------------------------------------------------------------
# Per-item and whole-catalog repacking
# ---------------------------------------------------------------------------


def repack_item(item: Item, spec: ItemRepackSpec) -> None:
    """Apply one manifest entry to an item in place.

    Replaces the affected assets, then rewrites the render cards the web page
    uses for the item: item-level cards win by name, otherwise the owning
    collection's card is copied onto the item in rewritten form. Raises
    :class:`RepackError` (without mutating the item) if a card cannot be
    preserved.
    """
    missing = sorted(
        {
            ref.asset
            for asset_spec in spec.assets.values()
            for affine in asset_spec.bands
            for ref in affine.coeffs
            if ref.asset not in item.assets
        }
    )
    if missing:
        raise RepackError(
            f"item {item.id!r}: manifest references source assets absent from "
            f"the item: {missing}"
        )

    new_bands = _new_band_table(spec)

    collection = item.get_collection()
    collection_renders = (
        _renders_of(collection)
        if collection is not None and _has_render_extension(collection)
        else {}
    )
    item_renders = _renders_of(item) if _has_render_extension(item) else {}

    rewritten: dict[str, dict[str, Any]] = {}
    for name in sorted(set(item_renders) | set(collection_renders)):
        source = item_renders.get(name) or collection_renders[name]
        inputs = _card_inputs(source, item.id)
        rewritten[name] = _rewrite_card(name, source, inputs, new_bands, item.id)

    # All cards are preservable. Commit the asset replacement: the combined
    # packs supersede the original per-band assets they were derived from. Any
    # unrelated asset (e.g. metadata) is left in place, and a replacement that
    # reuses an existing key simply overwrites it.
    replaced_keys = set(spec.assets)
    source_keys = {
        ref.asset
        for asset_spec in spec.assets.values()
        for affine in asset_spec.bands
        for ref in affine.coeffs
    }
    for source_key in source_keys - replaced_keys:
        item.assets.pop(source_key, None)
    for asset_key, asset_spec in spec.assets.items():
        replacement = asset_spec.asset.clone()
        replacement.set_owner(item)
        item.assets[asset_key] = replacement

    final_renders = dict(item_renders)
    final_renders.update(rewritten)
    if final_renders:
        item.stac_extensions = list(item.stac_extensions or [])
        if not _has_render_extension(item):
            item.stac_extensions.append(RENDER_SCHEMA_URI)
        item.properties[RENDERS_PROP] = final_renders


def repack_catalog(
    catalog: pystac.Catalog,
    manifest: dict[str, Any] | str,
) -> pystac.Catalog:
    """Return a full copy of ``catalog`` with the supplier manifest applied.

    The manifest may be a parsed mapping or a JSON string. Every affected item
    is validated on a throwaway copy first, so a :class:`RepackError` leaves
    the original catalog usable for an immediate retry.
    """
    if isinstance(manifest, str):
        loaded: Any = json.loads(manifest)
    else:
        loaded = manifest
    specs = parse_manifest(loaded)

    items_by_id = {item.id: item for item in catalog.get_items(recursive=True)}
    unknown = sorted(set(specs) - set(items_by_id))
    if unknown:
        raise RepackError(f"manifest references unknown items: {unknown}")

    draft = catalog.full_copy()
    draft_items = {item.id: item for item in draft.get_items(recursive=True)}
    for item_id, spec in specs.items():
        repack_item(draft_items[item_id], spec)
    return draft
