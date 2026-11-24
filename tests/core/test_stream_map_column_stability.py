"""End-to-end coverage for published column-name stability across deployments.

Scenario (the "orders" stream split into a detail and a public report):

- V1 ships ``wholesale_hub.zip`` flattened to ``wh__zip`` (separator ``__``,
  max flattened key length 16) and aliases the primary key
  ``warehouse_order_number`` as ``won``.
- V2 stops ``wholesale_hub`` and adds ``warehouse_history.zip`` plus a real
  top-level ``wh__zip`` attribute. Both new values are delivered in the detail
  report under their own (non-``wh__zip``) columns, while the public report may
  read ``warehouse_history.zip`` as ``wh__zip``.
- V3 is a brand-new Tap process consuming the V2 publications. It restores a
  nullable ``wholesale_hub`` whose ``zip`` reclaims the original ``wh__zip``
  column, while the two V2 columns keep their names and values.

Downstream consumers must keep reading every published column by its original
name, ``won`` stays a required, non-null primary key sourced from
``warehouse_order_number``, and an explicit null is distinguished from an
omitted object.
"""

from __future__ import annotations

import logging

import pytest

from singer_sdk.mapper import PluginMapper

# An isolated, silent logger: avoid process-global logging.disable(), which
# would interfere with other test modules' logging assertions.
_mapper_logger = logging.getLogger("test_stream_map_column_stability")
_mapper_logger.setLevel(logging.CRITICAL)
_mapper_logger.propagate = False
_mapper_logger.addHandler(logging.NullHandler())

SEPARATOR = "__"
MAX_KEY_LENGTH = 16

FLATTENING_CONFIG = {
    "flattening_enabled": True,
    "flattening_max_depth": 1,
    "flattening_separator": SEPARATOR,
    "flattening_max_key_length": MAX_KEY_LENGTH,
}

WON_SCHEMA = {"type": "string"}
NULLABLE_STRING = {"type": ["string", "null"]}


def _hub_object_schema() -> dict:
    return {
        "type": ["object", "null"],
        "properties": {"zip": NULLABLE_STRING},
    }


def _mapper(stream_maps: dict, raw_schema: dict) -> PluginMapper:
    mapper = PluginMapper(
        plugin_config={**FLATTENING_CONFIG, "stream_maps": stream_maps},
        logger=_mapper_logger,
    )
    mapper.register_raw_stream_schema(
        "orders",
        raw_schema,
        ["warehouse_order_number"],
    )
    return mapper


def _projection(mapper: PluginMapper, alias: str):
    stream_map = next(
        stream_map
        for stream_map in mapper.stream_maps["orders"]
        if stream_map.stream_alias == alias
    )
    return stream_map


def _assert_won_is_primary_key(stream_map) -> None:
    assert stream_map.transformed_key_properties == ["won"]
    schema = stream_map.transformed_schema
    assert schema["required"] == ["won"]
    assert schema["properties"]["won"] == WON_SCHEMA


# ---------------------------------------------------------------------------
# Version 1
# ---------------------------------------------------------------------------


V1_SCHEMA = {
    "type": "object",
    "required": ["warehouse_order_number"],
    "properties": {
        "warehouse_order_number": WON_SCHEMA,
        "wholesale_hub": _hub_object_schema(),
    },
}

V1_STREAM_MAPS = {
    "orders": None,
    "orders_detail": {
        "__source__": "orders",
        "won": "warehouse_order_number",
        "warehouse_order_number": None,
        "__key_properties__": ["won"],
    },
    "orders_public": {
        "__source__": "orders",
        "won": "warehouse_order_number",
        "warehouse_order_number": None,
        "wholesale_hub": None,
        "__key_properties__": ["won"],
    },
}


def test_v1_detail_publishes_wh_zip_and_public_excludes_it():
    mapper = _mapper(V1_STREAM_MAPS, V1_SCHEMA)

    detail = _projection(mapper, "orders_detail")
    _assert_won_is_primary_key(detail)
    assert set(detail.transformed_schema["properties"]) == {"won", "wh__zip"}
    assert detail.transformed_schema["properties"]["wh__zip"] == NULLABLE_STRING

    record = detail.transform(
        {
            "warehouse_order_number": "W1",
            "wholesale_hub": {"zip": "10001"},
        },
    )
    assert record == {"won": "W1", "wh__zip": "10001"}

    public = _projection(mapper, "orders_public")
    _assert_won_is_primary_key(public)
    assert set(public.transformed_schema["properties"]) == {"won"}
    assert public.transform(
        {
            "warehouse_order_number": "W1",
            "wholesale_hub": {"zip": "10001"},
        },
    ) == {"won": "W1"}


def test_v1_null_hub_is_null_column_but_omitted_hub_has_no_column():
    mapper = _mapper(V1_STREAM_MAPS, V1_SCHEMA)
    detail = _projection(mapper, "orders_detail")

    explicit_null = detail.transform(
        {"warehouse_order_number": "W2", "wholesale_hub": None},
    )
    assert explicit_null == {"won": "W2", "wh__zip": None}
    assert "wh__zip" in explicit_null

    omitted = detail.transform({"warehouse_order_number": "W3"})
    assert omitted == {"won": "W3"}
    assert "wh__zip" not in omitted


# ---------------------------------------------------------------------------
# Version 2
# ---------------------------------------------------------------------------


V2_SCHEMA = {
    "type": "object",
    "required": ["warehouse_order_number"],
    "properties": {
        "warehouse_order_number": WON_SCHEMA,
        "warehouse_history": _hub_object_schema(),
        # A real top-level attribute whose name collides with the flattened
        # warehouse_history.zip column under the 16-character limit.
        "wh__zip": NULLABLE_STRING,
    },
}

V2_STREAM_MAPS = {
    "orders": None,
    "orders_detail": {
        "__source__": "orders",
        "__else__": None,
        "won": "warehouse_order_number",
        # The literal wh__zip column name stays reserved for the V3 return of
        # wholesale_hub.zip, so the two V2 values get distinct columns.
        "hist_zip": "warehouse_history['zip']",
        "top_wh_zip": "wh__zip",
        "__key_properties__": ["won"],
    },
    "orders_public": {
        "__source__": "orders",
        "__else__": None,
        "won": "warehouse_order_number",
        "wh__zip": "warehouse_history['zip']",
        "__key_properties__": ["won"],
    },
}


def test_v2_collision_does_not_crash_and_detail_keeps_distinct_columns():
    # Registering the raw stream (which includes the ignored source stream)
    # used to raise "Duplicate column name produced in schema: wh__zip".
    mapper = _mapper(V2_STREAM_MAPS, V2_SCHEMA)

    detail = _projection(mapper, "orders_detail")
    _assert_won_is_primary_key(detail)
    # wh__zip is intentionally not declared by the V2 detail report.
    assert "wh__zip" not in detail.transformed_schema["properties"]
    assert set(detail.transformed_schema["properties"]) == {
        "won",
        "hist_zip",
        "top_wh_zip",
    }

    record = detail.transform(
        {
            "warehouse_order_number": "W1",
            "warehouse_history": {"zip": "20002"},
            "wh__zip": "30003",
        },
    )
    assert record == {"won": "W1", "hist_zip": "20002", "top_wh_zip": "30003"}


def test_v2_detail_explicit_null_vs_omitted_paths():
    mapper = _mapper(V2_STREAM_MAPS, V2_SCHEMA)
    detail = _projection(mapper, "orders_detail")

    explicit_null = detail.transform(
        {
            "warehouse_order_number": "W2",
            "warehouse_history": None,
            "wh__zip": None,
        },
    )
    assert explicit_null == {
        "won": "W2",
        "hist_zip": None,
        "top_wh_zip": None,
    }

    omitted = detail.transform({"warehouse_order_number": "W3"})
    assert omitted == {"won": "W3"}
    assert "hist_zip" not in omitted
    assert "top_wh_zip" not in omitted


def test_v2_public_reads_warehouse_history_as_wh_zip():
    mapper = _mapper(V2_STREAM_MAPS, V2_SCHEMA)
    public = _projection(mapper, "orders_public")
    _assert_won_is_primary_key(public)
    assert set(public.transformed_schema["properties"]) == {"won", "wh__zip"}

    assert public.transform(
        {
            "warehouse_order_number": "W1",
            "warehouse_history": {"zip": "20002"},
            "wh__zip": "30003",
        },
    ) == {"won": "W1", "wh__zip": "20002"}


# ---------------------------------------------------------------------------
# Version 3 - new Tap process, reordered schema, restored nullable hub
# ---------------------------------------------------------------------------


# Property order is deliberately different from V2 to prove order is irrelevant.
V3_SCHEMA = {
    "type": "object",
    "required": ["warehouse_order_number"],
    "properties": {
        "wh__zip": NULLABLE_STRING,
        "wholesale_hub": _hub_object_schema(),
        "warehouse_history": _hub_object_schema(),
        "warehouse_order_number": WON_SCHEMA,
    },
}

V3_STREAM_MAPS = {
    "orders": None,
    "orders_detail": {
        "__source__": "orders",
        "__else__": None,
        "won": "warehouse_order_number",
        # V2 columns keep their exact names and values.
        "hist_zip": "warehouse_history['zip']",
        "top_wh_zip": "wh__zip",
        # The restored nullable hub reclaims the original V1 column name.
        "wh__zip": "wholesale_hub['zip']",
        "__key_properties__": ["won"],
    },
    "orders_public": {
        "__source__": "orders",
        "__else__": None,
        "won": "warehouse_order_number",
        # Public wh__zip keeps reading warehouse_history.zip.
        "wh__zip": "warehouse_history['zip']",
        "__key_properties__": ["won"],
    },
}


def test_v3_detail_three_zips_read_back_independently():
    mapper = _mapper(V3_STREAM_MAPS, V3_SCHEMA)
    detail = _projection(mapper, "orders_detail")
    _assert_won_is_primary_key(detail)
    assert set(detail.transformed_schema["properties"]) == {
        "won",
        "wh__zip",
        "hist_zip",
        "top_wh_zip",
    }

    record = detail.transform(
        {
            "warehouse_order_number": "W1",
            "wholesale_hub": {"zip": "10001"},
            "warehouse_history": {"zip": "20002"},
            "wh__zip": "30003",
        },
    )
    assert record == {
        "won": "W1",
        "wh__zip": "10001",
        "hist_zip": "20002",
        "top_wh_zip": "30003",
    }


def test_v3_restored_hub_null_vs_omitted():
    mapper = _mapper(V3_STREAM_MAPS, V3_SCHEMA)
    detail = _projection(mapper, "orders_detail")

    explicit_null = detail.transform(
        {
            "warehouse_order_number": "W2",
            "wholesale_hub": None,
            "warehouse_history": {"zip": "20002"},
            "wh__zip": "30003",
        },
    )
    assert explicit_null["wh__zip"] is None
    assert "wh__zip" in explicit_null

    omitted = detail.transform(
        {
            "warehouse_order_number": "W3",
            "warehouse_history": {"zip": "20002"},
            "wh__zip": "30003",
        },
    )
    assert "wh__zip" not in omitted
    # V2 columns are unaffected either way.
    assert omitted["hist_zip"] == "20002"
    assert omitted["top_wh_zip"] == "30003"


def test_v3_public_keeps_reading_warehouse_history():
    mapper = _mapper(V3_STREAM_MAPS, V3_SCHEMA)
    public = _projection(mapper, "orders_public")
    _assert_won_is_primary_key(public)

    assert public.transform(
        {
            "warehouse_order_number": "W1",
            "wholesale_hub": {"zip": "10001"},
            "warehouse_history": {"zip": "20002"},
            "wh__zip": "30003",
        },
    ) == {"won": "W1", "wh__zip": "20002"}


@pytest.mark.parametrize(
    "version_maps,version_schema",
    [
        pytest.param(V1_STREAM_MAPS, V1_SCHEMA, id="v1"),
        pytest.param(V2_STREAM_MAPS, V2_SCHEMA, id="v2"),
        pytest.param(V3_STREAM_MAPS, V3_SCHEMA, id="v3"),
    ],
)
def test_won_always_sourced_from_warehouse_order_number(
    version_maps: dict,
    version_schema: dict,
):
    mapper = _mapper(version_maps, version_schema)
    for alias in ("orders_detail", "orders_public"):
        projection = _projection(mapper, alias)
        record = projection.transform(
            {"warehouse_order_number": "PK-42", **_all_optional_null(version_schema)},
        )
        assert record["won"] == "PK-42"
        _assert_won_is_primary_key(projection)


def _all_optional_null(version_schema: dict) -> dict:
    return {
        key: None
        for key, prop in version_schema["properties"].items()
        if key != "warehouse_order_number" and "null" in prop.get("type", [])
    }


def test_v3_process_consumes_v2_published_flat_schema_unchanged():
    """A brand-new process reads the V2 publications by their original names.

    The V3 Tap process receives the already-flattened V2 detail SCHEMA and
    RECORD messages. Registering them in a new mapper must not rename any
    published column, so downstream keeps reading old and new data the same way.
    """
    v2_published_detail_schema = {
        "type": "object",
        "required": ["won"],
        "properties": {
            "won": WON_SCHEMA,
            "hist_zip": NULLABLE_STRING,
            "top_wh_zip": NULLABLE_STRING,
        },
    }
    # A new process with flattening still enabled; the input is already flat.
    mapper = PluginMapper(
        plugin_config={
            **FLATTENING_CONFIG,
            "stream_maps": {
                "orders_detail": {
                    "won": "won",
                    "hist_zip": "hist_zip",
                    "top_wh_zip": "top_wh_zip",
                    "__key_properties__": ["won"],
                },
            },
        },
        logger=_mapper_logger,
    )
    mapper.register_raw_stream_schema(
        "orders_detail",
        v2_published_detail_schema,
        ["won"],
    )
    stream_map = mapper.stream_maps["orders_detail"][0]

    assert set(stream_map.transformed_schema["properties"]) == {
        "won",
        "hist_zip",
        "top_wh_zip",
    }
    _assert_won_is_primary_key(stream_map)

    assert stream_map.transform(
        {"won": "W1", "hist_zip": "20002", "top_wh_zip": "30003"},
    ) == {"won": "W1", "hist_zip": "20002", "top_wh_zip": "30003"}

