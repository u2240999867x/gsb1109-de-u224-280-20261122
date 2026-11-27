"""Tests for template registration and versioned asset updates."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from pystac import (
    Asset,
    AssetTemplateRegistry,
    Collection,
    Extent,
    Item,
    ItemAssetDefinition,
    MediaType,
    SpatialExtent,
    TemporalExtent,
    enable_asset_tracking,
    ingest_asset,
    unregistered_assets,
)

DATA_KEY = "data"
PROVIDER_KEY = "ingest:provider"

V1 = {
    "type": MediaType.GEOTIFF,
    "roles": ["data"],
    PROVIDER_KEY: {"region": "north", "dataset": "v1", "notes": "auto"},
}

V2 = {
    "type": "application/x-parquet",
    "roles": ["analysis"],
    PROVIDER_KEY: "central",
}

V3 = {
    "type": "application/zip",
    "roles": ["archive"],
    PROVIDER_KEY: {"region": "south", "dataset": "v3", "notes": "auto-v3"},
}


def make_collection() -> Collection:
    collection = Collection(
        id="imagery",
        description="template-driven imagery collection",
        extent=Extent(
            SpatialExtent([[-180.0, -90.0, 180.0, 90.0]]),
            TemporalExtent([[datetime(2020, 1, 1), None]]),
        ),
    )
    collection.item_assets = {DATA_KEY: ItemAssetDefinition(dict(V1))}
    return collection


def make_item(item_id: str) -> Item:
    return Item(
        id=item_id,
        geometry=None,
        bbox=None,
        datetime=datetime(2024, 1, 1),
        properties={},
    )


def save_and_reload(item: Item, tmp_path: Path, name: str) -> Item:
    path = tmp_path / name
    item.save_object(dest_href=str(path))
    reloaded = Item.from_file(str(path))
    enable_asset_tracking(reloaded)
    return reloaded


def test_three_version_review_workflow(tmp_path: Path) -> None:
    collection = make_collection()
    original = make_item("original")

    # Ingest generates the asset from the v1 item_assets template and registers it.
    original_asset = ingest_asset(original, DATA_KEY, collection, "v1", "v1/data.tif")
    assert original_asset.roles == ["data"]
    assert original_asset.media_type == MediaType.GEOTIFF
    assert original_asset.extra_fields[PROVIDER_KEY] == V1[PROVIDER_KEY]

    registry = AssetTemplateRegistry(original)
    assert registry.version(DATA_KEY) == "v1"

    # The review copy is an independent clone with its own registry copy.
    review = original.clone()
    review.id = "review"
    review_asset = review.assets[DATA_KEY]
    assert review_asset is not original_asset
    assert AssetTemplateRegistry(review).is_registered(DATA_KEY)

    # Reviewer edits use ordinary attribute assignment, dict mutation and
    # deletion. The roles write-back repeats the template value on purpose.
    review_asset.roles = ["data"]
    provider = review_asset.extra_fields[PROVIDER_KEY]
    provider["region"] = "harbor"
    del provider["notes"]

    # QC checkpoints the decisions before saving so same-value write-backs and
    # deletions survive the save / re-read round trip.
    review_registry = AssetTemplateRegistry(review)
    decisions = review_registry.record_decisions(DATA_KEY)
    assert decisions["roles"] == ["data"]
    assert decisions[PROVIDER_KEY] == {
        "region": "harbor",
        "notes": "__missing__",
    }

    review = save_and_reload(review, tmp_path, "review-v1.json")
    review_asset = review.assets[DATA_KEY]
    assert review_asset.extra_fields[PROVIDER_KEY] == {
        "region": "harbor",
        "dataset": "v1",
    }
    assert "notes" not in review_asset.extra_fields[PROVIDER_KEY]

    # Publisher releases v2: scalar provider, new roles, new media type.
    collection.item_assets[DATA_KEY] = ItemAssetDefinition(dict(V2))
    v2_template = collection.item_assets[DATA_KEY].to_dict()

    AssetTemplateRegistry(original).update(DATA_KEY, "v2", v2_template)
    AssetTemplateRegistry(review).update(DATA_KEY, "v2", v2_template)

    # Review copy keeps local decisions, including the deleted sub-key.
    assert review_asset.extra_fields[PROVIDER_KEY] == {"region": "harbor"}
    assert review_asset.roles == ["data"]
    assert review_asset.media_type == "application/x-parquet"

    # The registry still knows what the template itself says.
    review_registry = AssetTemplateRegistry(review)
    assert review_registry.snapshot(DATA_KEY)[PROVIDER_KEY] == "central"
    assert review_registry.version(DATA_KEY) == "v2"

    # The untouched original follows the template completely.
    assert original.assets[DATA_KEY].extra_fields[PROVIDER_KEY] == "central"
    assert original.assets[DATA_KEY].roles == ["analysis"]
    assert original.assets[DATA_KEY].media_type == "application/x-parquet"

    # A freshly ingested asset under v2 looks exactly like the template.
    fresh_item = make_item("fresh")
    fresh_asset = ingest_asset(fresh_item, DATA_KEY, collection, "v2", "v2/data.pq")
    assert fresh_asset.extra_fields[PROVIDER_KEY] == "central"
    assert fresh_asset.roles == ["analysis"]
    assert fresh_asset.media_type == "application/x-parquet"

    # Another save / re-read, then a list operation on the current roles.
    review = save_and_reload(review, tmp_path, "review-v2.json")
    review_asset = review.assets[DATA_KEY]
    review_asset.roles.append("reviewed")
    assert review_asset.roles == ["data", "reviewed"]

    # Publisher releases v3: provider is a dict again, roles and media change.
    collection.item_assets[DATA_KEY] = ItemAssetDefinition(dict(V3))
    v3_template = collection.item_assets[DATA_KEY].to_dict()

    AssetTemplateRegistry(review).update(DATA_KEY, "v3", v3_template)
    AssetTemplateRegistry(original).update(DATA_KEY, "v3", v3_template)

    # Local region wins, deleted notes stay missing, untouched dataset follows
    # the new template, and the appended role is preserved.
    assert review_asset.extra_fields[PROVIDER_KEY] == {
        "region": "harbor",
        "dataset": "v3",
    }
    assert "notes" not in review_asset.extra_fields[PROVIDER_KEY]
    assert review_asset.roles == ["data", "reviewed"]
    assert review_asset.media_type == "application/zip"
    assert AssetTemplateRegistry(review).version(DATA_KEY) == "v3"

    # Original keeps tracking the template.
    assert original.assets[DATA_KEY].extra_fields[PROVIDER_KEY] == V3[PROVIDER_KEY]
    assert original.assets[DATA_KEY].roles == ["archive"]
    assert original.assets[DATA_KEY].media_type == "application/zip"


def test_same_value_write_back_is_a_local_decision() -> None:
    collection = make_collection()
    item = make_item("item")
    asset = ingest_asset(item, DATA_KEY, collection, "v1", "data.tif")

    asset.roles = ["data"]
    AssetTemplateRegistry(item).record_decisions(DATA_KEY)

    collection.item_assets[DATA_KEY] = ItemAssetDefinition(dict(V2))
    AssetTemplateRegistry(item).update(
        DATA_KEY, "v2", collection.item_assets[DATA_KEY].to_dict()
    )

    assert item.assets[DATA_KEY].roles == ["data"]


def test_explicitly_deleted_field_stays_missing() -> None:
    collection = make_collection()
    item = make_item("item")
    asset = ingest_asset(item, DATA_KEY, collection, "v1", "data.tif")

    del asset.extra_fields[PROVIDER_KEY]["notes"]
    AssetTemplateRegistry(item).record_decisions(DATA_KEY)

    # v2 drops the provider dict entirely, then v3 reintroduces the notes key.
    for version, template in (("v2", V2), ("v3", V3)):
        AssetTemplateRegistry(item).update(
            DATA_KEY, version, ItemAssetDefinition(dict(template)).to_dict()
        )

    provider = item.assets[DATA_KEY].extra_fields[PROVIDER_KEY]
    assert provider == {"region": "south", "dataset": "v3"}
    assert "notes" not in provider


def test_unregistered_assets_are_left_alone_and_reported() -> None:
    collection = make_collection()
    item = make_item("item")
    ingest_asset(item, DATA_KEY, collection, "v1", "registered.tif")

    legacy = Asset(
        href="legacy.tif",
        media_type="application/octet-stream",
        roles=["legacy"],
        extra_fields={"legacy:field": "keep-me"},
    )
    item.add_asset("legacy", legacy)

    collection.item_assets[DATA_KEY] = ItemAssetDefinition(dict(V2))
    updated = AssetTemplateRegistry(item).update_all(
        {
            "data": collection.item_assets["data"].to_dict(),
            "legacy": collection.item_assets["data"].to_dict(),
        },
        "v2",
    )
    assert updated == [DATA_KEY]

    assert item.assets["legacy"].media_type == "application/octet-stream"
    assert item.assets["legacy"].roles == ["legacy"]
    assert item.assets["legacy"].extra_fields == {"legacy:field": "keep-me"}
    assert unregistered_assets([item]) == [("item", "legacy")]

    other = make_item("other")
    other.add_asset("solo", Asset(href="solo.tif"))
    assert unregistered_assets([item, other]) == [
        ("item", "legacy"),
        ("other", "solo"),
    ]


def test_template_asset_and_clone_edits_are_independent() -> None:
    collection = make_collection()
    item = make_item("item")
    asset = ingest_asset(item, DATA_KEY, collection, "v1", "data.tif")

    # Mutating the template definition after generation does not touch the asset.
    collection.item_assets[DATA_KEY].properties["roles"] = ["analysis"]
    assert asset.roles == ["data"]

    # Edits on a clone never leak back into the original.
    clone = item.clone()
    clone.assets[DATA_KEY].extra_fields[PROVIDER_KEY]["region"] = "harbor"
    assert item.assets[DATA_KEY].extra_fields[PROVIDER_KEY]["region"] == "north"
    assert clone.assets[DATA_KEY].extra_fields[PROVIDER_KEY]["region"] == "harbor"


def test_registry_survives_save_and_reload(tmp_path: Path) -> None:
    collection = make_collection()
    item = make_item("item")
    asset = ingest_asset(item, DATA_KEY, collection, "v1", "data.tif")
    asset.roles.append("reviewed")
    AssetTemplateRegistry(item).record_decisions(DATA_KEY)

    reloaded = save_and_reload(item, tmp_path, "item.json")
    registry = AssetTemplateRegistry(reloaded)
    assert registry.version(DATA_KEY) == "v1"
    assert registry.decisions(DATA_KEY)["roles"] == ["data", "reviewed"]
    assert isinstance(reloaded.assets[DATA_KEY].roles, list)

    collection.item_assets[DATA_KEY] = ItemAssetDefinition(dict(V2))
    registry.update(DATA_KEY, "v2", collection.item_assets[DATA_KEY].to_dict())
    assert reloaded.assets[DATA_KEY].roles == ["data", "reviewed"]


def test_update_unknown_asset_raises() -> None:
    item = make_item("item")
    with pytest.raises(KeyError):
        AssetTemplateRegistry(item).update("missing", "v2", dict(V2))
