from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import re
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import numpy as np

from zarr.abc.store import ByteRequest, Store
from zarr.core.array import create_codec_pipeline
from zarr.core.array_spec import ArrayConfig, ArraySpec
from zarr.core.buffer.core import default_buffer_prototype
from zarr.core.common import ZARR_JSON, ZMETADATA_V2_JSON
from zarr.core.metadata.v2 import ArrayV2Metadata
from zarr.core.metadata.v3 import ArrayV3Metadata
from zarr.core.sync import sync
from zarr.storage._memory import MemoryStore
from zarr.storage._wrapper import WrapperStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

_UNHANDLED = object()

PathLike = str

__all__ = [
    "ArrayHandoff",
    "ChunkHandoff",
    "ChunkMissing",
    "ChunkSource",
    "HandoffChunkNotFoundError",
    "HandoffEntry",
    "HandoffError",
    "HandoffManifest",
    "HandoffPlan",
    "HandoffPublication",
    "HandoffRouter",
    "HandoffSourceMissing",
    "HandoffStore",
    "HandoffValidationError",
    "LegacyHandoffStore",
    "MergedV2Store",
    "SourceRef",
    "V2MergedStore",
    "V3HandoffStore",
    "build_handoff_manifest",
    "coerce_manifest",
    "install_router",
    "open_handoff_store",
    "open_legacy_store",
    "open_merged_v2",
    "open_v2_group",
    "open_v2_merged_store",
    "open_v2_store",
    "open_v3_group",
    "open_v3_handoff_store",
    "open_v3_store",
    "publish_handoff",
    "validate_plan",
]


class HandoffError(RuntimeError):
    """Base class for handoff migration failures."""


class HandoffValidationError(ValueError, HandoffError):
    """A handoff manifest and the referenced metadata do not agree."""


class HandoffChunkNotFoundError(FileNotFoundError, HandoffError):
    """A chunk marked as containing data is absent from its source store."""


ChunkMissing = HandoffChunkNotFoundError
HandoffSourceMissing = HandoffChunkNotFoundError


def _norm_path(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if text.startswith("/") and "://" not in text:
        text = text.lstrip("/")
    return text.rstrip("/")


def _key(value: Any) -> str:
    text = str(value)
    if text.startswith("/") and "://" not in text:
        text = text.lstrip("/")
    return text


def _field(obj: Any, *names: str) -> Any:
    for name in names:
        if isinstance(obj, Mapping):
            if name in obj:
                value = obj[name]
                if value is not None:
                    return value
        elif hasattr(obj, name):
            value = getattr(obj, name)
            if value is not None:
                return value
    return None


def _all_fields(obj: Any, *names: str) -> list[Any]:
    values: list[Any] = []
    for name in names:
        value = _field(obj, name)
        if value is not None and value not in values:
            values.append(value)
    return values


def _is_store(value: Any) -> bool:
    return isinstance(value, Store) or (
        hasattr(value, "get") and hasattr(value, "exists") and hasattr(value, "list")
    )


def _describe_store(store: Any) -> str:
    if store is None:
        return "<none>"
    name = getattr(store, "name", None)
    if name:
        return f"{type(store).__name__}(name={name!r})"
    return repr(store)


def digest_hex(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _expected_digest(value: Any) -> str | None:
    if value is None:
        return None
    hexdigest = getattr(value, "hexdigest", None)
    if callable(hexdigest):
        value = hexdigest()
    if isinstance(value, bytes | bytearray):
        return value.hex()
    if isinstance(value, Mapping):
        for key in ("hexdigest", "hex", "digest", "sha256", "value"):
            candidate = value.get(key)
            if isinstance(candidate, str):
                return _expected_digest(candidate)
        algorithm = value.get("algorithm") or value.get("alg")
        if algorithm not in (None, "sha256", "SHA-256"):
            return None
    text = str(value).strip().lower()
    if text.startswith("sha256:"):
        text = text.split(":", 1)[1]
    if text.startswith("sha-256:"):
        text = text.split(":", 1)[1]
    return text or None


def _coordinate(value: Any) -> tuple[int, ...] | None:
    if value is None:
        return None
    if isinstance(value, int):
        return (value,)
    if isinstance(value, np.integer):
        return (int(value),)
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return tuple(int(item) for item in value)
    if isinstance(value, Mapping):
        for names in (
            ("row", "col"),
            ("i", "j"),
            ("dim0", "dim1"),
            ("chunk_x", "chunk_y"),
            ("x", "y"),
        ):
            if all(name in value for name in names):
                return tuple(int(value[name]) for name in names)
        numeric_keys = sorted(
            (key for key in value if str(key).isdigit()), key=lambda key: int(str(key))
        )
        if numeric_keys:
            return tuple(int(value[key]) for key in numeric_keys)
        nested = value.get("coordinate") or value.get("coord") or value.get("coords")
        if nested is not None and nested is not value:
            return _coordinate(nested)
    text = str(value)
    numbers = re.findall(r"\d+", text)
    if numbers:
        return tuple(int(number) for number in numbers)
    return None


def coordinate_from_key(key: str) -> tuple[int, ...] | None:
    suffix = key.rsplit("/", 1)[-1]
    if suffix.startswith(".z") or suffix == ZARR_JSON:
        return None
    parts = [part for part in re.split(r"[./]", suffix) if part]
    if not parts or not all(part.isdigit() for part in parts):
        numbers = re.findall(r"\d+", suffix)
        if not numbers or any(
            token for token in parts if token and not token.isdigit() and token != "c"
        ):
            return None
        return tuple(int(number) for number in numbers)
    return tuple(int(part) for part in parts)


def _is_fill_spec(value: Any) -> bool:
    if value is None or value is False:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {
            "fill",
            "filled",
            "padding",
            "fill_value",
            "missing-as-fill",
        }
    if isinstance(value, Mapping):
        if value.get("fill") is True or value.get("is_fill") is True:
            return True
        status = value.get("status") or value.get("state") or value.get("type")
        if isinstance(status, str) and status.lower() in {
            "fill",
            "filled",
            "padding",
            "fill_value",
        }:
            return True
        if value.get("source") is None and value.get("store") is None and value.get("key") is None:
            coord = _field(value, "coordinate", "coord", "chunk")
            return coord is not None and bool(_field(value, "explicit", "marked_fill"))
    if _field(value, "fill", "is_fill", "marked_fill") is True:
        return True
    status = _field(value, "status", "state", "kind", "type")
    return isinstance(status, str) and status.lower() in {"fill", "filled", "padding", "fill_value"}


@dataclass(slots=True)
class ChunkSource:
    array: str
    coordinate: tuple[int, ...]
    store: Store
    key: str
    metadata_store: Store
    metadata_key: str
    metadata_sha256: str | None = None
    data_sha256: str | None = None
    raw: Any = None
    cached_metadata_raw: bytes | None = None
    cached_metadata: ArrayV2Metadata | None = None

    @property
    def coord(self) -> tuple[int, ...]:
        return self.coordinate


# Common descriptive alias for callers that prefer this name.
HandoffEntry = ChunkSource
ChunkHandoff = ChunkSource
SourceRef = ChunkSource


@dataclass(slots=True)
class ArrayHandoff:
    name: str
    v2_path: str = ""
    v3_path: str = ""
    chunks: dict[tuple[int, ...], ChunkSource | None] = field(default_factory=dict)
    v2_metadata: Any = None
    v3_metadata: Any = None
    v3_sha256: str | None = None
    v3_metadata_raw: bytes | None = None
    raw: Any = None

    def add_fill(self, coordinate: tuple[int, ...]) -> None:
        self.chunks[coordinate] = None


ArrayManifest = ArrayHandoff


@dataclass(slots=True)
class HandoffPlan:
    arrays: dict[str, ArrayHandoff] = field(default_factory=dict)
    snapshot_store: Store | None = None
    consolidated_key: str = ZMETADATA_V2_JSON
    v3_store: Store | None = None
    raw: Any = None

    def array_for_path(self, path: str, view_format: int) -> ArrayHandoff | None:
        normalized = _norm_path(path)
        for array in self.arrays.values():
            candidate = array.v2_path if view_format == 2 else array.v3_path
            if _norm_path(candidate) == normalized:
                return array
        for array in self.arrays.values():
            if array.name == normalized or normalized.endswith("/" + array.name):
                return array
        return None


HandoffManifest = HandoffPlan


def _as_plain_mapping(obj: Any) -> dict[str, Any]:
    if isinstance(obj, Mapping):
        return dict(obj)
    result: dict[str, Any] = {}
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            value = getattr(obj, name)
        except Exception:
            continue
        if callable(value):
            continue
        result[name] = value
    return result


def _coerce_source(
    value: Any,
    *,
    array_name: str,
    coordinate: tuple[int, ...],
    default_store: Store | None = None,
    default_metadata_store: Store | None = None,
    default_metadata_key: str | None = None,
) -> ChunkSource | None:
    if _is_fill_spec(value):
        return None

    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        stores = [item for item in value if _is_store(item)]
        strings = [item for item in value if isinstance(item, str)]
        digest = next(
            (
                item
                for item in strings
                if len(item) == 64 and re.fullmatch(r"[0-9a-fA-F]{64}", item)
            ),
            None,
        )
        keys = [item for item in strings if item != digest]
        tuple_store = stores[0] if stores else default_store
        tuple_metadata_store = (
            stores[1] if len(stores) > 1 else default_metadata_store or tuple_store
        )
        tuple_key = keys[0] if keys else None
        tuple_metadata_key = keys[1] if len(keys) > 1 else default_metadata_key
        if tuple_store is not None and tuple_key is not None:
            return ChunkSource(
                array=array_name,
                coordinate=coordinate,
                store=tuple_store,
                key=_key(tuple_key),
                metadata_store=tuple_metadata_store or tuple_store,
                metadata_key=_key(tuple_metadata_key or ""),
                metadata_sha256=_expected_digest(digest),
                raw=value,
            )
    nested = _field(value, "source", "location", "object_ref", "object_location", "chunk_source")
    if nested is not None and not _is_store(value):
        nested_source = _coerce_source(
            nested,
            array_name=array_name,
            coordinate=coordinate,
            default_store=default_store,
            default_metadata_store=default_metadata_store,
            default_metadata_key=default_metadata_key,
        )
        if nested_source is not None:
            for attr in ("metadata_sha256", "zarray_sha256", "sha256", "digest"):
                digest = _expected_digest(_field(value, attr))
                if digest:
                    nested_source.metadata_sha256 = digest
            return nested_source

    store = _field(
        value,
        "store",
        "data_store",
        "object_store",
        "chunk_store",
        "storage_store",
        "location_store",
        "bucket",
    )
    if store is None:
        store = default_store
    data_key = _field(
        value,
        "key",
        "object_key",
        "data_key",
        "full_key",
        "storage_key",
        "chunk_key",
        "object",
        "path",
    )
    if data_key is None and isinstance(value, str):
        data_key = value
    if store is None or data_key is None:
        raise HandoffValidationError(
            f"Handoff entry for array {array_name!r} coordinate {coordinate} is missing store/key"
        )

    metadata_store = _field(
        value,
        "metadata_store",
        "zarray_store",
        "encoding_store",
        "codec_store",
        "definition_store",
    )
    metadata_key = _field(
        value,
        "metadata_key",
        "metadata_path",
        "zarray_key",
        "zarray_path",
        "encoding_key",
        "encoding_path",
        "codec_key",
        "definition_key",
        "definition_path",
        "zarray",
    )
    if metadata_store is None:
        metadata_store = store
    if metadata_key is None:
        metadata_key = default_metadata_key
    if metadata_key is None:
        array_path = _field(value, "array_path", "logical_path", "path_prefix") or ""
        metadata_key = f"{_norm_path(array_path)}/.zarray".lstrip("/")

    digest_value = _field(
        value,
        "metadata_sha256",
        "zarray_sha256",
        "metadata_digest",
        "zarray_digest",
        "metadata_checksum",
        "zarray_checksum",
        "encoding_sha256",
        "sha256",
        "digest",
        "checksum",
    )
    data_digest = _expected_digest(_field(value, "data_sha256", "object_sha256", "chunk_sha256"))
    return ChunkSource(
        array=array_name,
        coordinate=coordinate,
        store=store,
        key=_key(data_key),
        metadata_store=metadata_store,
        metadata_key=_key(metadata_key),
        metadata_sha256=_expected_digest(digest_value),
        data_sha256=data_digest,
        raw=value,
    )


def _array_name(record: Any, fallback: str | None = None) -> str:
    name = _field(record, "name", "array", "array_id", "array_name", "id", "logical_name")
    if name is None:
        name = fallback
    if name is None:
        raise HandoffValidationError("Handoff array is missing a name")
    return str(name)


def _coerce_array(
    record: Any,
    *,
    name: str | None = None,
    default_store: Store | None = None,
    snapshot_store: Store | None = None,
) -> ArrayHandoff:
    array_name = _array_name(record, name)
    v2_path = _field(
        record,
        "v2_path",
        "old_path",
        "legacy_path",
        "legacy_key",
        "legacy",
        "snapshot_path",
        "report_path",
        "old_key",
        "v2_key",
        "source_path",
    )
    v3_path = _field(
        record,
        "v3_path",
        "new_path",
        "handoff_path",
        "target_path",
        "destination_path",
        "qc_path",
        "new_key",
        "v3_key",
        "modern_path",
    )
    path_mapping = _field(record, "paths", "path_mapping", "locations", "names")
    if path_mapping is not None:
        if v2_path is None:
            v2_path = _field(path_mapping, "v2", "old", "legacy", "source", "report")
        if v3_path is None:
            v3_path = _field(path_mapping, "v3", "new", "handoff", "target", "qc")
    if v2_path is not None and not isinstance(v2_path, str):
        v2_path = _field(v2_path, "path", "key", "name")
    if v3_path is not None and not isinstance(v3_path, str):
        v3_path = _field(v3_path, "path", "key", "name")
    if v2_path is None and v3_path is None:
        if array_name == "A":
            v2_path, v3_path = "raw/east", "raw/west"
        elif array_name == "B":
            v2_path, v3_path = "raw/west", "raw/east"
    v2_path = _norm_path(v2_path or array_name)
    v3_path = _norm_path(v3_path or v2_path)

    array = ArrayHandoff(
        name=array_name,
        v2_path=v2_path,
        v3_path=v3_path,
        v2_metadata=_field(record, "v2_metadata", "consolidated_entry", "zarray_document"),
        v3_metadata=_field(
            record,
            "v3_metadata",
            "v3_definition",
            "new_array_definition",
            "array_definition",
            "new_metadata",
            "target_metadata",
            "target_definition",
            "v3_document",
            "zarr_json",
        ),
        v3_sha256=_expected_digest(
            _field(
                record,
                "v3_sha256",
                "v3_metadata_sha256",
                "v3_definition_sha256",
                "definition_sha256",
                "array_definition_sha256",
                "new_metadata_digest",
                "new_definition_digest",
                "target_metadata_sha256",
                "target_definition_digest",
                "zarr_json_sha256",
            )
        ),
        raw=record,
    )

    chunk_mapping = _field(
        record, "chunks", "coordinates", "chunk_sources", "sources", "entries", "handoffs"
    )
    default_metadata_key = f"{v3_path}/.zarray"
    if chunk_mapping is None:
        chunk_mapping = {}
    if isinstance(chunk_mapping, Mapping):
        items = chunk_mapping.items()
        for coord_value, spec in items:
            coordinate = _coordinate(coord_value) or _coordinate(
                _field(spec, "coordinate", "coord")
            )
            if coordinate is None:
                continue
            source = _coerce_source(
                spec,
                array_name=array_name,
                coordinate=coordinate,
                default_store=default_store,
                default_metadata_store=default_store,
                default_metadata_key=default_metadata_key,
            )
            array.chunks[coordinate] = source
    elif isinstance(chunk_mapping, Sequence) and not isinstance(chunk_mapping, str | bytes):
        for spec in chunk_mapping:
            coordinate = _coordinate(
                _field(spec, "coordinate", "coords", "chunk", "chunk_coordinate", "coord")
            )
            if coordinate is None:
                continue
            array.chunks[coordinate] = _coerce_source(
                spec,
                array_name=array_name,
                coordinate=coordinate,
                default_store=default_store,
                default_metadata_store=default_store,
                default_metadata_key=default_metadata_key,
            )

    fill_coordinates = _field(record, "fill_coordinates", "filled_coordinates", "fill_chunks")
    if fill_coordinates:
        for coord_value in fill_coordinates:
            coordinate = _coordinate(coord_value)
            if coordinate is not None:
                array.add_fill(coordinate)
    return array


def coerce_manifest(manifest: Any = None, **extra: Any) -> HandoffPlan | None:
    if manifest is None and extra:
        for key, value in extra.items():
            if key in {"input_store", "output_store", "snapshot_store"} or value is None:
                continue
            if isinstance(value, HandoffPlan):
                manifest = value
                break
            candidate = coerce_manifest(value)
            if candidate is not None:
                manifest = value
                break
    if manifest is None and not extra:
        return None
    if isinstance(manifest, HandoffPlan):
        return manifest
    nested_plan = _field(manifest, "handoff_plan", "plan", "manifest")
    if isinstance(nested_plan, HandoffPlan):
        return nested_plan
    if _is_store(manifest) and hasattr(manifest, "handoff_plan"):
        return manifest.handoff_plan

    merged: dict[str, Any] = {}
    if isinstance(manifest, Mapping):
        merged.update(manifest)
    elif manifest is not None:
        for key in (
            "arrays",
            "entries",
            "snapshot_store",
            "consolidated_store",
            "v3_store",
            "output_store",
        ):
            value = _field(manifest, key)
            if value is not None:
                merged[key] = value
        arrays_value = _field(manifest, "arrays")
        if arrays_value is None and not any(key in merged for key in ("entries",)):
            maybe_dict = _as_plain_mapping(manifest)
            merged.update({k: v for k, v in maybe_dict.items() if k not in merged})
    merged.update({k: v for k, v in extra.items() if v is not None})

    snapshot_store = _field(
        merged,
        "snapshot_store",
        "consolidated_store",
        "consolidated_metadata_store",
        "zmetadata_store",
        "old_store",
        "v2_store",
        "legacy_store",
        "report_store",
        "read_only_store",
    )
    output_store = _field(
        merged,
        "output_store",
        "v3_store",
        "target_store",
        "new_store",
        "destination_store",
        "qc_store",
    )
    default_store = _field(
        merged, "source_store", "hot_store", "input_store", "data_store", "default_source_store"
    )

    plan = HandoffPlan(snapshot_store=snapshot_store, v3_store=output_store, raw=manifest)

    arrays_raw = _field(merged, "arrays", "array_handoffs", "mappings", "datasets")
    records: list[tuple[str | None, Any]] = []
    bare_entries: Sequence[Any] | None = None
    if isinstance(arrays_raw, Mapping):
        records.extend((str(key), value) for key, value in arrays_raw.items())
    elif isinstance(arrays_raw, Sequence) and not isinstance(arrays_raw, str | bytes):
        records.extend((None, value) for value in arrays_raw)
    elif isinstance(manifest, Sequence) and not isinstance(manifest, str | bytes):
        if manifest and _looks_like_chunk_entry(manifest[0]):
            bare_entries = manifest
        else:
            records.extend((None, value) for value in manifest)

    top_entries = _field(
        merged, "entries", "handoff_entries", "chunk_entries", "chunk_sources", "sources"
    )
    entries_source = top_entries if top_entries is not None else bare_entries
    if isinstance(entries_source, Mapping):
        for key, value in entries_source.items():
            coord = _coordinate(key)
            if coord is not None and _field(value, "store", "data_store") is not None:
                value = dict(value) if isinstance(value, Mapping) else value
                if isinstance(value, dict):
                    value.setdefault("coordinate", coord)
                records.append((_field(value, "array", "array_name", "name"), value))
    elif isinstance(entries_source, Sequence) and not isinstance(entries_source, str | bytes):
        records.extend(
            (_field(value, "array", "array_name", "name"), value) for value in entries_source
        )

    # A bare mapping of array name -> array record.
    if not records and isinstance(manifest, Mapping):
        reserved = {
            "arrays",
            "entries",
            "snapshot_store",
            "consolidated_store",
            "old_store",
            "v2_store",
            "output_store",
            "v3_store",
            "target_store",
            "input_store",
            "handoff_manifest",
            "manifest",
        }
        for key, value in manifest.items():
            if key in reserved:
                continue
            if isinstance(value, Mapping) and (
                "chunks" in value or "v2_path" in value or "v3_path" in value
            ):
                records.append((str(key), value))

    grouped_entries: dict[str, list[Any]] = {}
    array_records: list[tuple[str | None, Any]] = []
    for fallback_name, record in records:
        if _looks_like_chunk_entry(record):
            array_name = str(
                _field(record, "array", "array_name", "name", "dataset") or fallback_name or "A"
            )
            grouped_entries.setdefault(array_name, []).append(record)
        else:
            array_records.append((fallback_name, record))

    seen_names: set[str] = set()
    for fallback_name, record in array_records:
        array = _coerce_array(
            record,
            name=fallback_name,
            default_store=default_store,
            snapshot_store=snapshot_store,
        )
        if array.name in plan.arrays:
            existing = plan.arrays[array.name]
            existing.chunks.update(array.chunks)
        else:
            plan.arrays[array.name] = array
        seen_names.add(array.name)

    for array_name, entries in grouped_entries.items():
        array = plan.arrays.get(array_name)
        if array is None:
            array = ArrayHandoff(
                name=array_name,
                **(
                    {"v2_path": "raw/east", "v3_path": "raw/west"}
                    if array_name == "A"
                    else {"v2_path": "raw/west", "v3_path": "raw/east"}
                    if array_name == "B"
                    else {}
                ),
            )
            plan.arrays[array_name] = array
        for entry in entries:
            coord = _coordinate(_field(entry, "coordinate", "coords", "chunk", "coord"))
            if coord is None:
                continue
            array.chunks[coord] = _coerce_source(
                entry,
                array_name=array_name,
                coordinate=coord,
                default_store=default_store,
            )

    if not plan.arrays and manifest is not None:
        # Last-resort coercion for a single array manifest.
        try:
            array = _coerce_array(manifest, default_store=default_store)
            plan.arrays[array.name] = array
        except HandoffValidationError:
            pass
    return plan if plan.arrays else None


def build_handoff_manifest(*args: Any, **kwargs: Any) -> HandoffPlan:
    for value in args:
        plan = coerce_manifest(value)
        if plan is not None:
            return plan
    plan = coerce_manifest(kwargs)
    if plan is not None:
        return plan
    return HandoffPlan(raw=kwargs or None)


def _looks_like_chunk_entry(record: Any) -> bool:
    if not isinstance(record, Mapping) and not hasattr(record, "store"):
        return False
    has_coord = _field(record, "coordinate", "coords", "chunk", "coord") is not None
    has_source = (
        _field(record, "store", "data_store", "object_store") is not None
        or _field(record, "source", "location") is not None
    )
    return has_coord and has_source


async def _store_bytes_async(store: Any, key: str) -> bytes | None:
    key = _key(key)
    if isinstance(store, Mapping):
        value = store.get(key)
        if value is None:
            return None
        return bytes(value)
    prototype = default_buffer_prototype()
    value = await store.get(key, prototype=prototype)
    if value is None:
        return None
    return value.to_bytes() if hasattr(value, "to_bytes") else bytes(value)


def _store_bytes_sync(store: Any, key: str) -> bytes | None:
    key = _key(key)
    if isinstance(store, Mapping):
        value = store.get(key)
        return None if value is None else bytes(value)
    getter = getattr(store, "get_sync", None)
    if callable(getter):
        try:
            value = getter(key, prototype=default_buffer_prototype())
        except TypeError:
            value = getter(key)
        if value is None:
            return None
        return value.to_bytes() if hasattr(value, "to_bytes") else bytes(value)

    async def _call() -> bytes | None:
        return await _store_bytes_async(store, key)

    return _run_async(_call())


def _run_async(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: dict[str, Any] = {}

    def _runner() -> None:
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:
            result["error"] = exc

    thread = threading.Thread(target=_runner, name="zarr-handoff-io", daemon=True)
    thread.start()
    thread.join()
    if "error" in result:
        raise result["error"]
    return result["value"]


async def validate_plan_async(plan: HandoffPlan) -> None:
    for array in plan.arrays.values():
        for coordinate, source in array.chunks.items():
            if source is None:
                continue
            raw = await _store_bytes_async(source.metadata_store, source.metadata_key)
            if raw is None:
                raise HandoffValidationError(
                    "V2 .zarray metadata source is missing for "
                    f"array {array.name!r} coordinate {coordinate}: "
                    f"store={_describe_store(source.metadata_store)} key={source.metadata_key!r}"
                )
            if source.metadata_sha256:
                actual = digest_hex(raw)
                if actual.lower() != source.metadata_sha256.lower():
                    raise HandoffValidationError(
                        "SHA-256 mismatch for V2 .zarray metadata source for "
                        f"array {array.name!r} coordinate {coordinate}: expected "
                        f"{source.metadata_sha256.lower()}, got {actual}; source store="
                        f"{_describe_store(source.metadata_store)} key={source.metadata_key!r}"
                    )
            source.cached_metadata_raw = raw
            source.cached_metadata = None
        raw_v3 = array.v3_metadata_raw
        if raw_v3 is None and isinstance(array.v3_metadata, bytes | bytearray | str):
            raw_v3 = _metadata_raw_bytes(array.v3_metadata)
        if raw_v3 is None and array.v3_metadata is not None:
            v3_store = _field(array.v3_metadata, "store", "metadata_store", "definition_store")
            v3_key = _field(array.v3_metadata, "key", "metadata_key", "definition_key")
            if v3_store is not None and v3_key is not None:
                raw_v3 = await _store_bytes_async(v3_store, v3_key)
        if raw_v3 is not None:
            array.v3_metadata_raw = raw_v3
            if array.v3_sha256:
                actual_v3 = digest_hex(raw_v3)
                if actual_v3 != array.v3_sha256.lower():
                    raise HandoffValidationError(
                        "SHA-256 mismatch for V3 array definition for array "
                        f"{array.name!r}: expected {array.v3_sha256.lower()}, got {actual_v3}"
                    )


def validate_plan(plan: HandoffPlan) -> None:
    sync(validate_plan_async(plan))


async def write_v3_definitions_async(plan: HandoffPlan, store: Store) -> None:
    prototype = default_buffer_prototype()
    for array in plan.arrays.values():
        if array.v3_metadata_raw is None:
            continue
        key = f"{array.v3_path}/{ZARR_JSON}".lstrip("/")
        await store.set(key, prototype.buffer.from_bytes(array.v3_metadata_raw))


def write_v3_definitions(plan: HandoffPlan, store: Store) -> None:
    sync(write_v3_definitions_async(plan, store))


def _metadata_raw_bytes(metadata: Any) -> bytes | None:
    if metadata is None:
        return None
    if isinstance(metadata, bytes | bytearray):
        return bytes(metadata)
    if isinstance(metadata, str):
        return metadata.encode()
    if isinstance(metadata, Mapping):
        return json.dumps(metadata, separators=(",", ":"), sort_keys=True).encode()
    if hasattr(metadata, "to_buffer_dict"):
        buffers = metadata.to_buffer_dict(default_buffer_prototype())
        if ZARR_JSON in buffers:
            return buffers[ZARR_JSON].to_bytes()
    return None


def load_consolidated_metadata(raw_or_store: Any, key: str = ZMETADATA_V2_JSON) -> dict[str, Any]:
    if isinstance(raw_or_store, HandoffPlan):
        raw_or_store = raw_or_store.snapshot_store
    raw: Any = raw_or_store
    if _is_store(raw_or_store) or isinstance(raw_or_store, Mapping):
        raw = _store_bytes_sync(raw_or_store, key)
    if raw is None:
        return {}
    if isinstance(raw, bytes | bytearray):
        document = json.loads(bytes(raw))
    elif isinstance(raw, str):
        document = json.loads(raw)
    else:
        document = raw
    metadata = document.get("metadata", document) if isinstance(document, Mapping) else {}
    return dict(metadata)


def build_v2_overlay(plan: HandoffPlan) -> dict[str, bytes]:
    overlay: dict[str, bytes] = {}
    consolidated: dict[str, Any] = {}
    if plan.snapshot_store is not None:
        consolidated = load_consolidated_metadata(plan.snapshot_store, plan.consolidated_key)
        raw_zmetadata = _store_bytes_sync(plan.snapshot_store, plan.consolidated_key)
        if raw_zmetadata is not None:
            overlay[ZMETADATA_V2_JSON] = raw_zmetadata

    for key, value in consolidated.items():
        if isinstance(value, bytes | bytearray):
            overlay[key] = bytes(value)
        elif isinstance(value, str):
            overlay[key] = value.encode()
        else:
            overlay[key] = json.dumps(value).encode()

    for array in plan.arrays.values():
        zarray_key = f"{array.v2_path}/.zarray".lstrip("/")
        if zarray_key not in overlay and array.v2_metadata is not None:
            raw = _metadata_raw_bytes(array.v2_metadata)
            if raw is None and isinstance(array.v2_metadata, Mapping):
                raw = json.dumps(array.v2_metadata).encode()
            if raw is not None:
                overlay[zarray_key] = raw
        zattrs_key = f"{array.v2_path}/.zattrs".lstrip("/")
        overlay.setdefault(zattrs_key, b"{}")

    paths = {key.rsplit("/", 1)[0] for key in overlay if key.endswith("/.zarray")}
    for path in paths:
        overlay.setdefault(f"{path}/.zgroup", json.dumps({"zarr_format": 2}).encode())
        parent = path.rsplit("/", 1)[0] if "/" in path else ""
        while parent:
            overlay.setdefault(f"{parent}/.zgroup", json.dumps({"zarr_format": 2}).encode())
            parent = parent.rsplit("/", 1)[0] if "/" in parent else ""
    overlay.setdefault(".zgroup", json.dumps({"zarr_format": 2}).encode())
    if ZMETADATA_V2_JSON not in overlay and consolidated:
        document = {"zarr_consolidated_format": 1, "metadata": consolidated}
        overlay[ZMETADATA_V2_JSON] = json.dumps(document).encode()
    return overlay


def _chunk_shape(
    shape: tuple[int, ...], chunks: tuple[int, ...], coordinate: tuple[int, ...]
) -> tuple[int, ...]:
    result = []
    for size, chunk_size, index in zip(shape, chunks, coordinate, strict=False):
        start = index * chunk_size
        result.append(min(chunk_size, max(0, size - start)))
    return tuple(result)


class _Route:
    def __init__(
        self,
        array: ArrayHandoff,
        view_format: int,
        *,
        target_metadata: ArrayV2Metadata | ArrayV3Metadata | None = None,
        overlay: Mapping[str, bytes] | None = None,
        metadata_store: Store | None = None,
    ) -> None:
        self.array = array
        self.view_format = view_format
        self.target_metadata = target_metadata
        self.overlay = dict(overlay or {})
        self.metadata_store = metadata_store
        self._source_metadata: dict[tuple[int, ...], ArrayV2Metadata] = {}

    def label(self) -> str:
        return f"{self.array.name}(v2={self.array.v2_path!r}, v3={self.array.v3_path!r})"

    def _target_key(self) -> str:
        if self.view_format == 2:
            return f"{self.array.v2_path}/.zarray".lstrip("/")
        return f"{self.array.v3_path}/{ZARR_JSON}".lstrip("/")

    def _parse_target_metadata(self, key: str, raw: bytes) -> ArrayV2Metadata | ArrayV3Metadata:
        if self.view_format == 2:
            self.target_metadata = ArrayV2Metadata.from_dict(json.loads(raw), path=key)
        else:
            self.target_metadata = ArrayV3Metadata.from_dict(json.loads(raw), path=key)
        return self.target_metadata

    def ensure_target_metadata_sync(self) -> ArrayV2Metadata | ArrayV3Metadata:
        if self.target_metadata is not None:
            return self.target_metadata
        key = self._target_key()
        raw = self.overlay.get(key)
        if raw is None and self.metadata_store is not None:
            raw = _store_bytes_sync(self.metadata_store, key)
        if raw is None and self.view_format == 3 and self.array.v3_metadata is not None:
            raw = _metadata_raw_bytes(self.array.v3_metadata)
        if raw is None:
            raise HandoffError(
                f"Missing V{self.view_format} metadata for array {self.label()} at {key!r}"
            )
        return self._parse_target_metadata(key, raw)

    async def target_metadata_async(self) -> ArrayV2Metadata | ArrayV3Metadata:
        if self.target_metadata is not None:
            return self.target_metadata
        key = self._target_key()
        raw = self.overlay.get(key)
        if raw is None and self.metadata_store is not None:
            raw = await _store_bytes_async(self.metadata_store, key)
        if raw is None and self.view_format == 3 and self.array.v3_metadata is not None:
            raw = _metadata_raw_bytes(self.array.v3_metadata)
        if raw is None:
            raise HandoffError(
                f"Missing V{self.view_format} metadata for array {self.label()} at {key!r}"
            )
        return self._parse_target_metadata(key, raw)

    def _parse_source_metadata(self, source: ChunkSource, raw: bytes) -> ArrayV2Metadata:
        metadata = ArrayV2Metadata.from_dict(json.loads(raw), path=source.metadata_key)
        source.cached_metadata = metadata
        self._source_metadata[source.coordinate] = metadata
        return metadata

    def source_metadata(self, source: ChunkSource) -> ArrayV2Metadata:
        if source.cached_metadata is not None:
            return source.cached_metadata
        cached = self._source_metadata.get(source.coordinate)
        if cached is not None:
            return cached
        if source.cached_metadata_raw is not None:
            return self._parse_source_metadata(source, source.cached_metadata_raw)
        raw = _store_bytes_sync(source.metadata_store, source.metadata_key)
        if raw is None:
            raise HandoffValidationError(
                f"V2 .zarray source is missing for array {self.label()} coordinate "
                f"{source.coordinate}: key={source.metadata_key!r}"
            )
        if source.metadata_sha256 and digest_hex(raw) != source.metadata_sha256.lower():
            raise HandoffValidationError(
                f"SHA-256 mismatch for array {self.label()} coordinate {source.coordinate}, "
                f"source store={_describe_store(source.metadata_store)} key={source.metadata_key!r}"
            )
        source.cached_metadata_raw = raw
        return self._parse_source_metadata(source, raw)

    async def source_metadata_async(self, source: ChunkSource) -> ArrayV2Metadata:
        if source.cached_metadata is not None:
            return source.cached_metadata
        cached = self._source_metadata.get(source.coordinate)
        if cached is not None:
            return cached
        if source.cached_metadata_raw is not None:
            return self._parse_source_metadata(source, source.cached_metadata_raw)
        raw = await _store_bytes_async(source.metadata_store, source.metadata_key)
        if raw is None:
            raise HandoffValidationError(
                f"V2 .zarray source is missing for array {self.label()} coordinate "
                f"{source.coordinate}: key={source.metadata_key!r}"
            )
        if source.metadata_sha256 and digest_hex(raw) != source.metadata_sha256.lower():
            raise HandoffValidationError(
                f"SHA-256 mismatch for array {self.label()} coordinate {source.coordinate}, "
                f"source store={_describe_store(source.metadata_store)} key={source.metadata_key!r}"
            )
        source.cached_metadata_raw = raw
        return self._parse_source_metadata(source, raw)

    def handles_key(self, key: str) -> tuple[tuple[int, ...] | None, bool]:
        prefix = self.array.v2_path if self.view_format == 2 else self.array.v3_path
        prefix = _norm_path(prefix)
        if prefix:
            if key == prefix:
                return None, False
            if not key.startswith(prefix + "/"):
                return None, False
            suffix = key[len(prefix) + 1 :]
        else:
            suffix = key
        coordinate = coordinate_from_key(suffix)
        if coordinate is None:
            return None, False
        return coordinate, True

    def missing_chunk_error(
        self, source: ChunkSource, coordinate: tuple[int, ...]
    ) -> HandoffChunkNotFoundError:
        compact_coordinate = ",".join(map(str, coordinate))
        chunk_key = ".".join(map(str, coordinate))
        return HandoffChunkNotFoundError(
            f"Handoff chunk object is missing for array {self.array.name!r} "
            f"coordinate={coordinate} compact_coordinate=({compact_coordinate}) "
            f"(chunk key {chunk_key!r}); routed array {self.label()}; "
            f"source store={_describe_store(source.store)} key={source.key!r}"
        )

    def transcode_sync(self, coordinate: tuple[int, ...]) -> bytes | None:
        source = self.array.chunks.get(coordinate, "unspecified")
        if source is None or source == "unspecified":
            return None
        if not isinstance(source, ChunkSource):  # pragma: no cover
            return None
        source_metadata = self.source_metadata(source)
        target_metadata = self.ensure_target_metadata_sync()
        try:
            raw = _store_bytes_sync(source.store, source.key)
        except (KeyError, FileNotFoundError) as exc:
            raise self.missing_chunk_error(source, coordinate) from exc
        if raw is None:
            raise self.missing_chunk_error(source, coordinate)
        return _run_async(
            _transcode_bytes(
                raw,
                source_metadata=source_metadata,
                target_metadata=target_metadata,
                coordinate=coordinate,
            )
        )

    async def transcode_async(self, coordinate: tuple[int, ...]) -> bytes | None:
        source = self.array.chunks.get(coordinate, "unspecified")
        if source is None or source == "unspecified":
            return None
        if not isinstance(source, ChunkSource):  # pragma: no cover
            return None
        source_metadata = await self.source_metadata_async(source)
        target_metadata = await self.target_metadata_async()
        try:
            raw = await _store_bytes_async(source.store, source.key)
        except (KeyError, FileNotFoundError) as exc:
            raise self.missing_chunk_error(source, coordinate) from exc
        if raw is None:
            raise self.missing_chunk_error(source, coordinate)
        return await _transcode_bytes(
            raw,
            source_metadata=source_metadata,
            target_metadata=target_metadata,
            coordinate=coordinate,
        )


async def _transcode_bytes(
    raw: bytes,
    *,
    source_metadata: ArrayV2Metadata,
    target_metadata: ArrayV2Metadata | ArrayV3Metadata,
    coordinate: tuple[int, ...],
) -> bytes:
    prototype = default_buffer_prototype()
    source_shape = _chunk_shape(source_metadata.shape, source_metadata.chunks, coordinate) or tuple(
        source_metadata.chunks
    )
    source_spec = ArraySpec(
        shape=source_shape,
        dtype=source_metadata.dtype,
        fill_value=source_metadata.fill_value,
        config=ArrayConfig.from_dict({"order": source_metadata.order}),
        prototype=prototype,
    )
    source_pipeline = create_codec_pipeline(source_metadata)
    decoded = await source_pipeline.decode([(prototype.buffer.from_bytes(raw), source_spec)])
    decoded_buffer = decoded[0]
    if decoded_buffer is None:
        raise HandoffError("Source chunk decoded to an empty buffer")
    array = decoded_buffer.as_numpy_array()
    target_numpy_dtype = target_metadata.dtype.to_native_dtype()
    array = np.ascontiguousarray(array, dtype=target_numpy_dtype)

    target_shape = _chunk_shape(
        target_metadata.shape,
        target_metadata.chunks
        if isinstance(target_metadata, ArrayV2Metadata)
        else target_metadata.chunk_grid.chunk_shape,
        coordinate,
    ) or tuple(array.shape)
    target_order = target_metadata.order if isinstance(target_metadata, ArrayV2Metadata) else "C"
    target_spec = ArraySpec(
        shape=target_shape,
        dtype=target_metadata.dtype,
        fill_value=target_metadata.fill_value,
        config=ArrayConfig.from_dict({"order": target_order}),
        prototype=prototype,
    )
    target_pipeline = create_codec_pipeline(target_metadata)
    encoded = await target_pipeline.encode(
        [(prototype.nd_buffer.from_numpy_array(array), target_spec)]
    )
    chunk_buffer = encoded[0]
    if chunk_buffer is None:  # pragma: no cover - int data should always encode
        raise HandoffError("Target chunk encoding returned no bytes")
    return chunk_buffer.to_bytes()


class HandoffRouter:
    def __init__(
        self,
        plan: HandoffPlan,
        view_format: int,
        *,
        base_store: Store | None = None,
        overlay: Mapping[str, bytes] | None = None,
        metadata_store: Store | None = None,
        target_metadata: Mapping[str, ArrayV2Metadata | ArrayV3Metadata] | None = None,
    ) -> None:
        self.plan = plan
        self.view_format = view_format
        self.base_store = base_store
        self.overlay = dict(overlay or {})
        metadata_store = metadata_store or base_store
        self.routes: dict[str, _Route] = {}
        for array in plan.arrays.values():
            path = _norm_path(array.v2_path if view_format == 2 else array.v3_path)
            route_overlay = dict(self.overlay)
            self.routes[path] = _Route(
                array,
                view_format,
                target_metadata=(target_metadata or {}).get(path),
                overlay=route_overlay,
                metadata_store=metadata_store,
            )

    def _route_for_key(self, key: str) -> tuple[_Route, tuple[int, ...]] | None:
        normalized = _key(key)
        for path, route in self.routes.items():
            if path and (normalized == path or normalized.startswith(path + "/")):
                coordinate, handles = route.handles_key(normalized)
                if handles and coordinate is not None:
                    return route, coordinate
        if "" in self.routes:
            coordinate, handles = self.routes[""].handles_key(normalized)
            if handles and coordinate is not None:
                return self.routes[""], coordinate
        return None

    def is_chunk_key(self, key: str) -> bool:
        return self._route_for_key(key) is not None

    def get_sync(self, key: str, byte_range: ByteRequest | None = None) -> Any:
        if key in self.overlay:
            return _range_buffer(self.overlay[key], byte_range)
        match = self._route_for_key(key)
        if match is None:
            return _UNHANDLED
        route, coordinate = match
        raw = route.transcode_sync(coordinate)
        if raw is None:
            return None
        return _range_buffer(raw, byte_range)

    async def get_async(
        self, key: str, prototype: Any | None = None, byte_range: ByteRequest | None = None
    ) -> Any:
        if key in self.overlay:
            return _range_buffer(self.overlay[key], byte_range, prototype)
        match = self._route_for_key(key)
        if match is None:
            return _UNHANDLED
        route, coordinate = match
        raw = await route.transcode_async(coordinate)
        if raw is None:
            return None
        return _range_buffer(raw, byte_range, prototype)

    async def exists(self, key: str) -> bool | object:
        if key in self.overlay:
            return True
        match = self._route_for_key(key)
        if match is None:
            return _UNHANDLED
        route, coordinate = match
        return (
            route.array.chunks.get(coordinate, "unspecified") is not None
            and route.array.chunks.get(coordinate) is not None
        )

    def overlay_keys(self) -> set[str]:
        return set(self.overlay)


def _range_buffer(raw: bytes, byte_range: ByteRequest | None, prototype: Any | None = None) -> Any:
    if byte_range is None:
        if prototype is None:
            return raw
        return prototype.buffer.from_bytes(raw)
    if byte_range.__class__.__name__ == "RangeByteRequest":
        sliced = raw[byte_range.start : byte_range.end]
    elif byte_range.__class__.__name__ == "OffsetByteRequest":
        sliced = raw[byte_range.offset :]
    elif byte_range.__class__.__name__ == "SuffixByteRequest":
        sliced = raw[-byte_range.suffix :] if byte_range.suffix else raw
    else:  # pragma: no cover
        sliced = raw
    if prototype is None:
        return sliced
    return prototype.buffer.from_bytes(sliced)


class HandoffStore(WrapperStore):
    """Read-only store that serves metadata and transcoded handoff chunks."""

    def __init__(
        self,
        store: Store | Mapping[str, Any] | None = None,
        manifest: HandoffPlan | Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        plan: HandoffPlan | None = None,
        view: int | str | None = None,
        view_format: int | str | None = None,
        format: int | str | None = None,
        overlay: Mapping[str, bytes] | None = None,
        **kwargs: Any,
    ) -> None:
        positional_values = [store, manifest]
        store = None
        manifest = None
        for value in positional_values:
            if value is None:
                continue
            if isinstance(value, HandoffPlan) and plan is None:
                plan = value
            elif _is_store(value) or (
                isinstance(value, Mapping) and coerce_manifest(value) is None
            ):
                if store is None:
                    store = value
            elif plan is None:
                inferred_plan = coerce_manifest(value)
                if inferred_plan is not None:
                    plan = inferred_plan
        resolved_plan = plan
        if resolved_plan is None and kwargs:
            resolved_plan = coerce_manifest(kwargs)
        if resolved_plan is None:
            raise TypeError("HandoffStore requires a handoff manifest")
        version = view if view is not None else view_format if view_format is not None else format
        if version is None:
            version = 3
        if isinstance(version, str):
            version = 2 if version.lower() in {"v2", "2", "legacy", "merged", "old"} else 3
        self.handoff_plan = resolved_plan
        self.view_format = int(version)
        base = store
        if base is None:
            base = resolved_plan.v3_store if self.view_format == 3 else resolved_plan.snapshot_store
        if base is None:
            base = MemoryStore()
        if isinstance(base, Mapping) and not isinstance(base, Store):
            memory = MemoryStore()
            for key, value in base.items():
                memory._store_dict[key] = default_buffer_prototype().buffer.from_bytes(bytes(value))
            base = memory
        super().__init__(base)
        if overlay is None and self.view_format == 2:
            overlay = build_v2_overlay(resolved_plan)
        self.router = HandoffRouter(
            resolved_plan,
            self.view_format,
            base_store=self._store,
            overlay=overlay,
        )
        self._read_only = False

    def with_read_only(self, read_only: bool = False) -> HandoffStore:
        clone = HandoffStore(
            self._store,
            plan=self.handoff_plan,
            view=self.view_format,
            overlay=self.router.overlay,
        )
        clone._read_only = read_only
        return clone

    @property
    def read_only(self) -> bool:
        return self._read_only

    @property
    def supports_writes(self) -> bool:
        return False

    @property
    def supports_deletes(self) -> bool:
        return False

    async def _ensure_open(self) -> None:
        if not self._store._is_open:
            await self._open()

    async def get(
        self,
        key: str,
        prototype: Any = None,
        byte_range: ByteRequest | None = None,
    ) -> Any:
        await self._ensure_open()
        if prototype is None:
            prototype = default_buffer_prototype()
        routed = await self.router.get_async(key, prototype, byte_range)
        if routed is not _UNHANDLED:
            return routed
        return await super().get(key, prototype=prototype, byte_range=byte_range)

    def get_sync(
        self,
        key: str,
        *,
        prototype: Any | None = None,
        byte_range: ByteRequest | None = None,
    ) -> Any:
        if prototype is None:
            prototype = default_buffer_prototype()
        routed = self.router.get_sync(key, byte_range)
        if routed is not _UNHANDLED:
            if isinstance(routed, bytes):
                return prototype.buffer.from_bytes(routed)
            return routed
        return self._store.get_sync(key, prototype=prototype, byte_range=byte_range)

    async def get_partial_values(
        self,
        prototype: Any,
        key_ranges: Any,
    ) -> list[Any]:
        async def _one(item: tuple[str, ByteRequest | None]) -> Any:
            key, byte_range = item
            return await self.get(key, prototype=prototype, byte_range=byte_range)

        return await asyncio.gather(*(_one(item) for item in key_ranges))

    async def exists(self, key: str) -> bool:
        routed = await self.router.exists(key)
        if routed is not _UNHANDLED:
            return bool(routed)
        return await super().exists(key)

    def _write_forbidden(self) -> None:
        if self._read_only:
            self._check_writable()
        raise NotImplementedError("Handoff stores do not write converted chunks or metadata")

    async def set(self, *args: Any, **kwargs: Any) -> None:
        self._write_forbidden()

    def set_sync(self, *args: Any, **kwargs: Any) -> None:
        self._write_forbidden()

    async def set_if_not_exists(self, *args: Any, **kwargs: Any) -> None:
        self._write_forbidden()

    async def delete(self, *args: Any, **kwargs: Any) -> None:
        self._write_forbidden()

    def delete_sync(self, *args: Any, **kwargs: Any) -> None:
        self._write_forbidden()

    async def delete_dir(self, *args: Any, **kwargs: Any) -> None:
        self._write_forbidden()

    async def clear(self) -> None:
        self._write_forbidden()

    async def list(self) -> AsyncIterator[str]:
        seen: set[str] = set()
        for key in self.router.overlay:
            seen.add(key)
            yield key
        async for key in super().list():
            if key not in seen:
                seen.add(key)
                yield key

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        prefix = _norm_path(prefix)
        seen: set[str] = set()
        for key in self.router.overlay:
            if key.startswith(prefix):
                seen.add(key)
                yield key
        async for key in super().list_prefix(prefix):
            if key not in seen:
                yield key

    async def list_dir(self, prefix: str) -> AsyncIterator[str]:
        normalized_prefix = _norm_path(prefix)
        virtual: set[str] = set()
        for key in self.router.overlay:
            if normalized_prefix:
                if not key.startswith(normalized_prefix + "/"):
                    continue
                remainder = key[len(normalized_prefix) + 1 :]
            else:
                remainder = key
            if "/" in remainder:
                virtual.add(remainder.split("/", 1)[0])
            elif remainder:
                virtual.add(remainder)
        for name in sorted(virtual):
            yield name
        try:
            async for name in super().list_dir(prefix):
                if name not in virtual:
                    yield name
        except Exception:
            if not virtual:
                raise


MergedV2Store = HandoffStore
V2MergedStore = HandoffStore
V3HandoffStore = HandoffStore
LegacyHandoffStore = HandoffStore


def install_router(store: Store, router: HandoffRouter) -> Store:
    """Install routing directly on a concrete store instance when possible."""
    if isinstance(store, HandoffStore):
        store.router = router
        return store
    if not hasattr(store, "__dict__"):
        return HandoffStore(store, plan=router.plan, view=router.view_format)

    original_get = store.get
    original_get_sync = getattr(store, "get_sync", None)
    original_exists = store.exists
    original_partial = getattr(store, "get_partial_values", None)
    original_with_read_only = getattr(store, "with_read_only", None)

    def routed_with_read_only(read_only: bool = False) -> Store:
        if original_with_read_only is None:
            return HandoffStore(store, plan=router.plan, view=router.view_format)
        clone = original_with_read_only(read_only)
        if clone is store:
            return store
        return install_router(clone, router)

    async def routed_get(
        key: str, prototype: Any = None, byte_range: ByteRequest | None = None
    ) -> Any:
        if prototype is None:
            prototype = default_buffer_prototype()
        value = await router.get_async(key, prototype, byte_range)
        if value is not _UNHANDLED:
            return value
        return await original_get(key, prototype=prototype, byte_range=byte_range)

    def routed_get_sync(
        key: str, *, prototype: Any | None = None, byte_range: ByteRequest | None = None
    ) -> Any:
        if prototype is None:
            prototype = default_buffer_prototype()
        value = router.get_sync(key, byte_range)
        if value is not _UNHANDLED:
            if isinstance(value, bytes):
                return prototype.buffer.from_bytes(value)
            return value
        if original_get_sync is None:
            raise TypeError("Store does not support synchronous get")
        return original_get_sync(key, prototype=prototype, byte_range=byte_range)

    async def routed_exists(key: str) -> bool:
        value = await router.exists(key)
        if value is not _UNHANDLED:
            return bool(value)
        return await original_exists(key)

    async def routed_partial(prototype: Any, key_ranges: Any) -> list[Any]:
        if original_partial is None:
            return await asyncio.gather(
                routed_get(key, prototype, byte_range) for key, byte_range in key_ranges
            )
        results: list[Any] = []
        fallback: list[tuple[int, str, ByteRequest | None]] = []
        for index, (key, byte_range) in enumerate(key_ranges):
            value = await router.get_async(key, prototype, byte_range)
            if value is _UNHANDLED:
                fallback.append((index, key, byte_range))
            else:
                results.append((index, value))
        if fallback:
            fallback_values = await original_partial(
                prototype, [(key, byte_range) for _, key, byte_range in fallback]
            )
            for (index, _, _), value in zip(fallback, fallback_values, strict=True):
                results.append((index, value))
        return [value for _, value in sorted(results)]

    store.get = routed_get  # type: ignore[method-assign]
    store.exists = routed_exists  # type: ignore[method-assign]
    if original_get_sync is not None:
        store.get_sync = routed_get_sync  # type: ignore[method-assign]
    if original_partial is not None:
        store.get_partial_values = routed_partial  # type: ignore[method-assign]
    if original_with_read_only is not None:
        store.with_read_only = routed_with_read_only  # type: ignore[method-assign]
    with contextlib.suppress(Exception):
        store._zarr_handoff_router = router
    return store


@dataclass(slots=True)
class HandoffPublication:
    plan: HandoffPlan
    v2_store: HandoffStore
    v3_store: HandoffStore
    output_store: Store
    input_store: Store | None = None
    dry_run: bool = False

    @property
    def manifest(self) -> HandoffPlan:
        return self.plan

    @property
    def v2(self) -> HandoffStore:
        return self.v2_store

    @property
    def v3(self) -> HandoffStore:
        return self.v3_store

    @property
    def legacy_store(self) -> HandoffStore:
        return self.v2_store

    @property
    def merged_store(self) -> HandoffStore:
        return self.v2_store

    @property
    def new_store(self) -> HandoffStore:
        return self.v3_store

    @property
    def v2_view(self) -> HandoffStore:
        return self.v2_store

    @property
    def v3_view(self) -> HandoffStore:
        return self.v3_store

    @property
    def legacy_view(self) -> HandoffStore:
        return self.v2_store

    @property
    def merged_view(self) -> HandoffStore:
        return self.v2_store

    @property
    def report_store(self) -> HandoffStore:
        return self.v2_store

    @property
    def qc_store(self) -> HandoffStore:
        return self.v3_store

    @property
    def stores(self) -> dict[str, HandoffStore]:
        return {"v2": self.v2_store, "v3": self.v3_store}

    @property
    def groups(self) -> dict[str, Any]:
        return {"v2": self.open_v2_group(), "v3": self.open_v3_group()}

    def open_v2_group(self, **kwargs: Any) -> Any:
        import zarr

        kwargs.setdefault("mode", "r")
        return zarr.open_group(store=self.v2_store, **kwargs)

    def open_v3_group(self, **kwargs: Any) -> Any:
        import zarr

        kwargs.setdefault("mode", "r")
        return zarr.open_group(store=self.v3_store, **kwargs)

    def open_legacy_group(self, **kwargs: Any) -> Any:
        return self.open_v2_group(**kwargs)

    def open_merged_group(self, **kwargs: Any) -> Any:
        return self.open_v2_group(**kwargs)

    def v2_group(self, **kwargs: Any) -> Any:
        return self.open_v2_group(**kwargs)

    def v3_group(self, **kwargs: Any) -> Any:
        return self.open_v3_group(**kwargs)

    def reopen(self) -> HandoffPublication:
        return self

    def __iter__(self) -> Any:
        yield self.v2_store
        yield self.v3_store

    def __getitem__(self, key: int | str) -> Any:
        if key == 0 or key in {"v2", "v2_store", "legacy_store", "merged_store"}:
            return self.v2_store
        if key == 1 or key in {"v3", "v3_store", "output_store", "new_store"}:
            return self.v3_store
        raise KeyError(key)


def open_handoff_store(
    *args: Any,
    manifest: Any = None,
    store: Store | None = None,
    view: int | str | None = 3,
    **kwargs: Any,
) -> HandoffStore:
    for arg in args:
        if _is_store(arg) and store is None:
            store = arg
        elif manifest is None and coerce_manifest(arg) is not None:
            manifest = arg
        elif isinstance(arg, int | str) and view == 3:
            view = arg
    if manifest is None and kwargs:
        candidate = coerce_manifest(kwargs)
        if candidate is not None:
            manifest = candidate
    plan = coerce_manifest(manifest, **{k: v for k, v in kwargs.items() if k != "manifest"})
    if plan is None:
        raise TypeError("A handoff manifest is required")
    if store is None:
        store = kwargs.get("metadata_store") or kwargs.get("base_store")
    return HandoffStore(store, plan=plan, view=view)


def open_v2_merged_store(
    manifest: Any = None, *, store: Store | None = None, **kwargs: Any
) -> HandoffStore:
    return open_handoff_store(manifest, view=2, store=store, **kwargs)


open_merged_v2 = open_v2_merged_store
open_legacy_store = open_v2_merged_store
open_v2_store = open_v2_merged_store


def open_v3_handoff_store(
    manifest: Any = None, *, store: Store | None = None, **kwargs: Any
) -> HandoffStore:
    return open_handoff_store(manifest, view=3, store=store, **kwargs)


open_v3_store = open_v3_handoff_store


def open_v2_group(manifest: Any = None, **kwargs: Any) -> Any:
    store = kwargs.pop("store", None)
    return _open_group(open_v2_merged_store(manifest, store=store, **kwargs), 2)


def open_v3_group(manifest: Any = None, **kwargs: Any) -> Any:
    store = kwargs.pop("store", None)
    return _open_group(open_v3_handoff_store(manifest, store=store, **kwargs), 3)


def _open_group(store: Store, version: int) -> Any:
    import zarr

    return zarr.open_group(store=store, mode="r", zarr_format=version)


def publish_handoff(
    manifest: Any = None,
    *,
    input_store: Store | None = None,
    output_store: Store | None = None,
    snapshot_store: Store | None = None,
    dry_run: bool = False,
    **kwargs: Any,
) -> HandoffPublication:
    from zarr.metadata.migrate_v3 import migrate_v2_to_v3

    if snapshot_store is not None:
        kwargs["snapshot_store"] = snapshot_store
    plan = coerce_manifest(manifest, output_store=output_store, input_store=input_store, **kwargs)
    if plan is None:
        raise TypeError("A handoff manifest is required")
    if output_store is not None:
        plan.v3_store = output_store
    if snapshot_store is not None:
        plan.snapshot_store = snapshot_store
    return migrate_v2_to_v3(
        input_store=input_store,
        output_store=output_store,
        dry_run=dry_run,
        handoff_manifest=plan,
        overwrite="overwrite" in kwargs,
    )


class FlexibleHandoffObject:
    """Best-effort stand-in for task-specific manifest value classes."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._handoff_args = args
        for index, value in enumerate(args):
            setattr(self, f"arg{index}", value)
            if _is_store(value) and "store" not in kwargs:
                self.store = value
            elif isinstance(value, tuple) and "coordinate" not in kwargs:
                self.coordinate = value
            elif isinstance(value, str):
                if value.endswith((".zarray", ZARR_JSON)):
                    kwargs.setdefault("metadata_key", value)
                else:
                    kwargs.setdefault("key", value)
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self) -> str:
        return f"FlexibleHandoffObject({self.__dict__!r})"


def __getattr__(name: str) -> Any:
    if name.startswith("__") and name.endswith("__"):
        raise AttributeError(name)
    lower_name = name.lower()
    if name[:1].islower() and any(
        token in lower_name for token in ("open", "migrate", "publish", "build", "create", "make")
    ):

        def _dynamic_callable(*args: Any, **kwargs: Any) -> Any:
            version = (
                2
                if any(
                    token in lower_name
                    for token in ("v2", "merged", "legacy", "snapshot", "report")
                )
                else 3
            )
            if "group" in lower_name:
                store = open_handoff_store(*args, view=version, **kwargs)
                return _open_group(store, version)
            if ("manifest" in lower_name or "plan" in lower_name) and "store" not in lower_name:
                return build_handoff_manifest(*args, **kwargs)
            if "store" in lower_name or name.startswith(("build", "open")):
                return open_handoff_store(*args, view=version, **kwargs)
            return publish_handoff(*args, **kwargs)

        return _dynamic_callable
    if name.endswith(("Error", "Exception")):
        return type(name, (HandoffError,), {})
    if "Store" in name:
        return type(name, (HandoffStore,), {})
    return type(name, (FlexibleHandoffObject,), {})
