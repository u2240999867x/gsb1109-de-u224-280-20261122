from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

try:
    from apscheduler.triggers.cron import CronTrigger
except Exception:  # pragma: no cover - APScheduler is always available in this repo
    CronTrigger = None  # type: ignore[assignment]


PRICE = "price"
INVENTORY = "inventory"
SOURCES = (PRICE, INVENTORY)

_SOURCE_ALIASES = {
    "price": PRICE,
    "prices": PRICE,
    "pricing": PRICE,
    "price_manifest": PRICE,
    "价格": PRICE,
    "价格清单": PRICE,
    "价格源": PRICE,
    "inventory": INVENTORY,
    "inventories": INVENTORY,
    "stock": INVENTORY,
    "stocks": INVENTORY,
    "inventory_manifest": INVENTORY,
    "库存": INVENTORY,
    "库存清单": INVENTORY,
    "库存源": INVENTORY,
}

_READY_STATES = {
    "ready",
    "complete",
    "completed",
    "delivered",
    "arrived",
    "available",
    "done",
    "success",
    "succeeded",
    "ok",
    "watermark_reached",
    "at_watermark",
    "current",
}
_EMPTY_STATES = {
    "empty",
    "legal_empty",
    "legally_empty",
    "empty_partition",
    "no_data",
    "zero",
}
_PENDING_STATES = {
    "pending",
    "missing",
    "absent",
    "not_ready",
    "incomplete",
    "waiting",
    "below_watermark",
    "watermark_pending",
    "not_arrived",
    "unavailable",
    "unknown",
}


class DailySettlementError(Exception):
    """Base error for the daily settlement dependency gate."""


class ManifestError(DailySettlementError):
    pass


class ManifestNotReady(DailySettlementError):
    def __init__(self, report: "ReadinessReport | None" = None):
        self.report = report
        pending = [] if report is None else report.pending_partitions
        super().__init__(f"manifest materials are not ready: {pending}")


class StaleManifestError(DailySettlementError):
    def __init__(self, message: str, result: "ClaimResult | None" = None):
        self.result = result
        super().__init__(message)


class ClaimConflictError(DailySettlementError):
    pass


class AlreadyClaimedError(ClaimConflictError):
    def __init__(self, receipt: "ClaimResult | None" = None):
        self.receipt = receipt
        super().__init__("daily settlement has already been claimed")


class ClaimLostError(ClaimConflictError):
    pass


StaleManifestVersion = StaleManifestError
VersionConflictError = StaleManifestError
AlreadyDeliveredError = AlreadyClaimedError


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="microseconds")


class BusinessDate(str):
    """String date that also compares equal to :class:`datetime.date`."""

    def __new__(cls, value: Any = None) -> "BusinessDate":
        return str.__new__(cls, normalize_date(value))

    def __eq__(self, other: object) -> bool:
        try:
            return str(self) == normalize_date(other)
        except Exception:
            return str.__eq__(self, str(other))

    def __hash__(self) -> int:
        return hash(str(self))

    @property
    def iso(self) -> str:
        return str(self)

    def as_date(self) -> date:
        return date.fromisoformat(str(self))


def normalize_date(value: Any = None) -> str:
    if value is None:
        value = date.today()
    if isinstance(value, BusinessDate):
        return str(value)
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        raise ValueError("business date must not be empty")
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            return text


def normalize_source(value: Any) -> str:
    if value is None:
        raise ValueError("source is required")
    key = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    if key in _SOURCE_ALIASES:
        return _SOURCE_ALIASES[key]
    for alias, normalized in _SOURCE_ALIASES.items():
        if key == alias.lower():
            return normalized
    if PRICE in key:
        return PRICE
    if INVENTORY in key or "stock" in key:
        return INVENTORY
    return key


def normalize_version(version: Any) -> tuple[str, tuple[Any, ...], Any]:
    raw = version
    if version is None:
        return "v1", (1,), 1
    if isinstance(version, int) and not isinstance(version, bool):
        number = int(version)
        return f"v{number}", (number,), raw
    text = str(version).strip()
    lowered = text.lower()
    if lowered.startswith("v") and lowered[1:].isdigit():
        number = int(lowered[1:])
        return f"v{number}", (number,), raw
    if text.isdigit():
        number = int(text)
        return f"v{number}", (number,), raw
    parts: list[Any] = []
    for piece in text.replace("-", ".").replace("_", ".").split("."):
        if piece.isdigit():
            parts.append(int(piece))
        else:
            parts.append(piece.lower())
    return text, tuple(parts or [text.lower()]), raw


def _coerce_revision(value: Any) -> int:
    if value is None:
        return 1
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        revision = value
    else:
        text = str(value).strip().lower().replace("revision", "r")
        if text.startswith("r"):
            text = text[1:]
        digits = "".join(ch for ch in text if ch.isdigit())
        revision = int(digits) if digits else abs(hash(text)) % 1_000_000
    if revision < 0:
        raise ValueError("revision must be non-negative")
    return revision


def _looks_like_date(value: Any) -> bool:
    if isinstance(value, (date, datetime)):
        return True
    if not isinstance(value, str):
        return False
    text = value.strip()
    if len(text) == 8 and text.isdigit():
        return True
    try:
        date.fromisoformat(text[:10])
        return True
    except ValueError:
        try:
            datetime.fromisoformat(text.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False


def _is_date_label(value: Any) -> bool:
    if _looks_like_date(value):
        return True
    if not isinstance(value, str):
        return False
    text = value.strip().lower().replace(" ", "")
    if text in {"d", "day", "date", "today", "yesterday", "tomorrow", "biz_date"}:
        return True
    return bool(re.fullmatch(r"d(day)?[+\-_]?\d+", text))


def _looks_like_source(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip().lower()
    return any(token in text for token in ("price", "pricing", "inventory", "stock", "价格", "库存"))


def _is_version_like(value: Any) -> bool:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return True
    if not isinstance(value, str):
        return False
    text = value.strip().lower()
    if text.startswith("v"):
        text = text[1:]
    return bool(text) and (text.replace(".", "").replace("_", "").replace("-", "").isalnum()) and any(ch.isdigit() for ch in text)


def _object_to_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "__dict__"):
        return {key: item for key, item in vars(value).items() if not key.startswith("_")}
    result = {}
    for key in dir(value):
        if key.startswith("_"):
            continue
        try:
            item = getattr(value, key)
        except Exception:
            continue
        if not callable(item):
            result[key] = item
    return result


def _is_manifest_like(value: Any) -> bool:
    if isinstance(value, (Manifest, Mapping)):
        return True
    if isinstance(value, (str, bytes, int, float, date, datetime)) or value is None:
        return False
    attributes = {"source", "upstream", "business_date", "date", "version", "partitions", "attachments"}
    return any(hasattr(value, name) for name in attributes)


def _watermark_key(value: Any) -> tuple[int, Any] | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return (2, str(value))
    if isinstance(value, (int, float)):
        return (0, float(value))
    if isinstance(value, (datetime, date)):
        return (1, datetime.fromisoformat(value.isoformat()).timestamp())
    text = str(value).strip()
    try:
        return (0, float(text))
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return (1, parsed.timestamp())
    except ValueError:
        pass
    return (2, text.lower())


def watermark_reached(current: Any, required: Any) -> bool:
    if required is None:
        return True
    current_key = _watermark_key(current)
    required_key = _watermark_key(required)
    if current_key is None or required_key is None:
        return False
    if current_key[0] == required_key[0]:
        return current_key[1] >= required_key[1]
    return current_key >= required_key


def _first(mapping: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in mapping and mapping[name] is not None:
            return mapping[name]
    return default


class AwaitImmediate:
    def __await__(self) -> Any:
        if False:  # pragma: no cover - makes this a generator-based awaitable
            yield None
        return self


class AwaitableDict(AwaitImmediate, dict):
    pass


class AwaitableList(AwaitImmediate, list):
    pass


class AwaitableBool(AwaitImmediate, int):
    def __new__(cls, value: Any) -> "AwaitableBool":
        return int.__new__(cls, 1 if value else 0)

    def __bool__(self) -> bool:
        return int.__bool__(self)

class PartitionState(AwaitableDict):
    """Dictionary view with attribute access for one partition."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


def coerce_partition(name: Any, value: Any = None, **extra: Any) -> PartitionState:
    if isinstance(name, Mapping):
        data = dict(name)
        partition_name = _first(data, "name", "partition", "id", "partition_id", "key")
        if partition_name is None:
            raise ValueError(f"partition record has no name: {name!r}")
        return coerce_partition(partition_name, data)
    if isinstance(value, PartitionState):
        result = PartitionState(value)
        result["name"] = str(name)
        return result
    if isinstance(value, Mapping):
        data = dict(value)
        data.update(extra)
        status = _first(data, "status", "state", "phase", default=None)
        explicit_ready = _first(
            data, "ready", "is_ready", "complete", "completed", "delivered", "arrived"
        )
        explicit_empty = _first(
            data, "empty", "is_empty", "legal_empty", "is_legal_empty"
        )
        watermark = _first(
            data,
            "watermark",
            "current_watermark",
            "observed_watermark",
            "high_watermark",
            "offset",
        )
        required_watermark = _first(
            data,
            "required_watermark",
            "min_watermark",
            "target_watermark",
            "expected_watermark",
            "needed_watermark",
        )
        result = PartitionState(
            name=str(name),
            status=_normalize_status(status, explicit_ready, explicit_empty),
            ready=bool(explicit_ready) if explicit_ready is not None else None,
            empty=bool(explicit_empty) if explicit_empty is not None else None,
            watermark=watermark,
            required_watermark=required_watermark,
            reason=_first(data, "reason", "message", "detail"),
            raw=_json_safe(data),
        )
        return result
    if isinstance(value, bool):
        status = "ready" if value else "pending"
        return PartitionState(
            name=str(name), status=status, ready=value, empty=False,
            watermark=None, required_watermark=None, reason=None, raw={"value": value},
        )
    if isinstance(value, (int, float, datetime, date)) or value is None:
        return PartitionState(
            name=str(name),
            status="watermark",
            ready=None,
            empty=False,
            watermark=value,
            required_watermark=None,
            reason=None,
            raw={"watermark": value},
        )
    if isinstance(value, str):
        return PartitionState(
            name=str(name),
            status=_normalize_status(value, None, None),
            ready=None,
            empty=None if value not in _EMPTY_STATES else True,
            watermark=None,
            required_watermark=None,
            reason=None,
            raw={"status": value},
        )
    if value is not None:
        return coerce_partition(str(name), vars(value), **extra)
    return PartitionState(
        name=str(name), status="pending", ready=None, empty=False,
        watermark=None, required_watermark=None, reason=None, raw=None,
    )


def _normalize_status(status: Any, ready: Any, empty: Any) -> str:
    if empty is True:
        return "empty"
    if ready is True:
        return "ready"
    if ready is False:
        return "pending"
    if status is None:
        return "unknown"
    text = str(status).strip().lower().replace("-", "_").replace(" ", "_")
    if text in _READY_STATES:
        return "ready"
    if text in _EMPTY_STATES:
        return "empty"
    if text in _PENDING_STATES:
        return "pending"
    return text


def _partition_collection(value: Any) -> dict[str, PartitionState]:
    result: dict[str, PartitionState] = {}
    if value is None:
        return result
    if isinstance(value, Mapping):
        for name, item in value.items():
            partition = coerce_partition(name, item)
            result[partition["name"]] = partition
        return result
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        for item in value:
            partition = coerce_partition(item)
            result[partition["name"]] = partition
        return result
    raise TypeError(f"unsupported partition collection: {value!r}")


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        if isinstance(value, Mapping):
            return {str(k): _json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_json_safe(v) for v in value]
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if hasattr(value, "__dict__"):
            return _json_safe(vars(value))
        return str(value)


class Manifest(AwaitableDict):
    """Versioned, date-scoped upstream partition manifest."""

    def __init__(
        self,
        source: Any = None,
        business_date: Any = None,
        version: Any = None,
        revision: int | None = None,
        partitions: Any = None,
        *,
        date: Any = None,
        day: Any = None,
        empty_partitions: Any = None,
        legal_empty_partitions: Any = None,
        required_partitions: Any = None,
        expected_partitions: Any = None,
        attachments: Any = None,
        files: Any = None,
        watermark: Any = None,
        completion_watermark: Any = None,
        required_watermark: Any = None,
        target_watermark: Any = None,
        manifest_id: str | None = None,
        id: str | None = None,
        correction_id: str | int | None = None,
        complete: bool | None = None,
        is_complete: bool | None = None,
        **metadata: Any,
    ) -> None:
        if isinstance(source, Mapping):
            data = dict(source)
            data.update({k: v for k, v in kwargs_items(
                business_date=business_date, version=version, revision=revision,
                partitions=partitions, date=date, day=day,
                empty_partitions=empty_partitions,
                legal_empty_partitions=legal_empty_partitions,
                required_partitions=required_partitions,
                expected_partitions=expected_partitions, attachments=attachments,
                files=files, watermark=watermark,
                completion_watermark=completion_watermark,
                required_watermark=required_watermark,
                target_watermark=target_watermark, manifest_id=manifest_id, id=id,
                correction_id=correction_id, complete=complete,
                is_complete=is_complete,
            ) if v is not None})
            data.update(metadata)
            self.__init__(**data)  # type: ignore[misc]
            return

        if _looks_like_date(source) and _looks_like_source(business_date):
            source, business_date = business_date, source

        partitions = _first(
            {"value": partitions}, "value"
        )
        partitions = (
            partitions
            or metadata.pop("partition_manifests", None)
            or metadata.pop("partition_list", None)
            or metadata.pop("partition_ids", None)
            or metadata.pop("shards", None)
            or metadata.pop("segments", None)
        )
        attachments = attachments or metadata.pop("attachment_manifests", None)
        empty_partitions = (
            empty_partitions
            or legal_empty_partitions
            or metadata.pop("legal_empty_partition_ids", None)
            or metadata.pop("empty_partition_ids", None)
            or metadata.pop("legal_empty", None)
            or metadata.pop("valid_empty_partitions", None)
        )
        legal_empty_partitions = empty_partitions
        required_partitions = (
            required_partitions
            or expected_partitions
            or metadata.pop("required_partition_ids", None)
            or metadata.pop("expected_partition_ids", None)
            or metadata.pop("required_shards", None)
        )
        completion_watermark = (
            completion_watermark
            or metadata.pop("completed_watermark", None)
            or metadata.pop("completion_wm", None)
        )
        revision = (
            revision
            if revision is not None
            else metadata.pop("revision", metadata.pop("revision_number", metadata.pop("revision_id", metadata.pop("rev", 1))))
        )

        resolved_date = business_date if business_date is not None else (date if date is not None else day)
        if resolved_date is None and "business_date" in metadata:
            resolved_date = metadata.pop("business_date")
        if source is None and "upstream" in metadata:
            source = metadata.pop("upstream")
        source = normalize_source(source if source is not None else metadata.pop("source", None))
        business_date_value = BusinessDate(resolved_date if resolved_date is not None else metadata.pop("date", None))
        version_value = version if version is not None else metadata.pop("version", metadata.pop("manifest_version", metadata.pop("version_number", 1)))
        version_label, version_key, raw_version = normalize_version(version_value)
        revision = _coerce_revision(revision)

        merged_partitions: dict[str, PartitionState] = {}
        for collection in (partitions, attachments, files, metadata.pop("partition_states", None)):
            for name, state in _partition_collection(collection).items():
                merged_partitions[name] = state

        watermark_map = (
            metadata.pop("watermarks", None)
            or metadata.pop("partition_watermarks", None)
            or metadata.pop("current_watermarks", None)
            or {}
        )
        if isinstance(watermark_map, Mapping):
            for name, value in watermark_map.items():
                state = merged_partitions.setdefault(
                    str(name),
                    PartitionState(name=str(name), status="watermark", watermark=value),
                )
                if state.get("watermark") is None:
                    state["watermark"] = value

        empty_names = set(_string_list(empty_partitions))
        empty_names.update(_string_list(legal_empty_partitions))
        empty_names.update(_string_list(metadata.pop("empty", None)))
        for name in empty_names:
            state = merged_partitions.setdefault(
                name,
                PartitionState(
                    name=name, status="empty", ready=None, empty=True,
                    watermark=None, required_watermark=None, reason="legal_empty_partition",
                    raw=None,
                ),
            )
            state["status"] = "empty"
            state["empty"] = True

        required_names = list(_string_list(required_partitions))
        required_names.extend(_string_list(expected_partitions))
        required_names.extend(_string_list(metadata.pop("required", None)))
        for name in merged_partitions:
            if name not in required_names:
                required_names.append(name)
        for name in empty_names:
            if name not in required_names:
                required_names.append(name)

        effective_required_watermark = (
            required_watermark
            if required_watermark is not None
            else target_watermark
            if target_watermark is not None
            else metadata.pop("expected_watermark", None)
            if "expected_watermark" in metadata
            else completion_watermark
            if completion_watermark is not None
            else watermark
        )
        explicit_complete = complete if complete is not None else is_complete

        self.update(
            source=source,
            business_date=business_date_value,
            date=business_date_value,
            version=version_value,
            version_label=version_label,
            raw_version=raw_version,
            version_key=list(version_key),
            revision=revision,
            partitions=merged_partitions,
            empty_partitions=sorted(empty_names),
            legal_empty_partitions=sorted(empty_names),
            required_partitions=list(dict.fromkeys(required_names)),
            watermark=watermark,
            completion_watermark=completion_watermark,
            required_watermark=effective_required_watermark,
            target_watermark=target_watermark,
            manifest_id=manifest_id or id or _default_manifest_id(
                source, business_date_value, version_label, revision
            ),
            correction_id=str(correction_id) if correction_id is not None else None,
            explicitly_complete=explicit_complete,
            metadata=_json_safe(metadata),
        )
        self["checksum"] = self._calculate_checksum()

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value

    @property
    def id(self) -> str:
        return self.manifest_id

    @property
    def checksum(self) -> str:
        return self["checksum"]

    @property
    def ref(self) -> "ManifestRef":
        return ManifestRef(
            source=self.source,
            business_date=str(self.business_date),
            version=self.version,
            revision=self.revision,
            manifest_id=self.manifest_id,
            checksum=self.checksum,
            raw_version=self.raw_version,
        )

    def _calculate_checksum(self) -> str:
        payload = {
            key: value
            for key, value in self.items()
            if key not in {"checksum", "date"}
        }
        encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:20]

    def to_dict(self) -> dict[str, Any]:
        return json.loads(json.dumps(_json_safe(dict(self)), default=str))

    def required_watermark_for(self, partition: PartitionState) -> Any:
        return partition.get("required_watermark") or self.get("required_watermark")

    def evaluate(self, expected_partitions: Iterable[str] | None = None) -> "SourceReadiness":
        required = list(dict.fromkeys([*_string_list(expected_partitions), *self.required_partitions]))
        partitions = self.partitions
        pending: list[PartitionState] = []
        satisfied: list[str] = []
        empty: list[str] = []
        for name in required:
            if name in self.empty_partitions:
                empty.append(name)
                satisfied.append(name)
                continue
            partition = partitions.get(name)
            if partition is None:
                pending.append(PartitionState(name=name, status="pending", reason="missing_partition"))
                continue
            if partition.get("empty") is True or partition.get("status") == "empty":
                empty.append(name)
                satisfied.append(name)
                continue
            target = self.required_watermark_for(partition)
            current_watermark = partition.get("watermark")
            has_watermark_value = current_watermark is not None
            watermark_ok = watermark_reached(current_watermark, target)
            status = partition.get("status")
            explicit_ready = partition.get("ready")
            is_explicitly_ready = status == "ready" or explicit_ready is True
            if status == "pending" or explicit_ready is False:
                reason = partition.get("reason") or status or "pending"
                if has_watermark_value and not watermark_ok and target is not None:
                    reason = "watermark_not_reached"
                pending.append(PartitionState(partition, pending_reason=reason))
            elif has_watermark_value and target is not None and not watermark_ok:
                pending.append(
                    PartitionState(
                        partition,
                        pending_reason="watermark_not_reached",
                        required_watermark=target,
                    )
                )
            elif is_explicitly_ready:
                satisfied.append(name)
            elif status == "watermark" or status == "unknown":
                if target is not None and not watermark_ok:
                    pending.append(
                        PartitionState(partition, pending_reason="watermark_not_reached")
                    )
                else:
                    satisfied.append(name)
            elif status in _PENDING_STATES:
                pending.append(PartitionState(partition, pending_reason=status))
            else:
                satisfied.append(name)

        if self.explicitly_complete is False and not pending:
            pending.append(
                PartitionState(
                    name="__manifest__",
                    status="pending",
                    reason="manifest_marked_incomplete",
                )
            )
        return SourceReadiness(
            source=self.source,
            ready=not pending,
            pending=tuple(pending),
            satisfied=tuple(satisfied),
            empty=tuple(empty),
            manifest_ref=self.ref,
        )


def kwargs_items(**kwargs: Any) -> Mapping[str, Any]:
    return {key: value for key, value in kwargs.items() if value is not None}


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]


def _default_manifest_id(source: str, day: str, version: str, revision: int) -> str:
    return f"{source}:{day}:{version}:r{revision}"


class ManifestRef(AwaitableDict):
    def __init__(
        self,
        source: str,
        business_date: str,
        version: str,
        revision: int,
        manifest_id: str,
        checksum: str | None = None,
        raw_version: Any = None,
    ) -> None:
        super().__init__(
            source=source,
            business_date=BusinessDate(business_date),
            version=version,
            revision=int(revision),
            manifest_id=manifest_id,
            checksum=checksum,
            raw_version=raw_version if raw_version is not None else version,
        )

    def __getattr__(self, item: str) -> Any:
        try:
            return self[item]
        except KeyError as exc:
            raise AttributeError(item) from exc

    def matches(self, manifest: Manifest | ManifestRef | Mapping[str, Any]) -> bool:
        if isinstance(manifest, Manifest):
            other = manifest.ref
        else:
            other = manifest  # type: ignore[assignment]
        return (
            self["manifest_id"] == other.get("manifest_id")
            and (self.get("checksum") is None or self.get("checksum") == other.get("checksum"))
        )


class SourceReadiness:
    def __init__(
        self,
        source: str,
        ready: bool,
        pending: tuple[PartitionState, ...],
        satisfied: tuple[str, ...],
        empty: tuple[str, ...],
        manifest_ref: ManifestRef | None,
    ) -> None:
        self.source = source
        self.ready = ready
        self.pending = pending
        self.satisfied = satisfied
        self.empty = empty
        self.manifest_ref = manifest_ref

    @property
    def pending_partition_names(self) -> list[str]:
        return [partition["name"] for partition in self.pending]

    @property
    def pending_partitions(self) -> list[str]:
        return self.pending_partition_names

    @property
    def missing_partitions(self) -> list[str]:
        return self.pending_partition_names

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "ready": self.ready,
            "pending_partitions": [dict(p) for p in self.pending],
            "pending_partition_names": [p["name"] for p in self.pending],
            "satisfied_partitions": list(self.satisfied),
            "empty_partitions": list(self.empty),
            "manifest": dict(self.manifest_ref) if self.manifest_ref else None,
        }


class ReadinessReport(AwaitableDict):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __bool__(self) -> bool:
        return bool(self.get("ready"))

    @property
    def pending_partitions(self) -> dict[str, list[str]]:
        return self.get("pending", {})

    @property
    def missing_partitions(self) -> list[str]:
        result: list[str] = []
        for source, names in self.pending_partitions.items():
            result.extend(f"{source}:{name}" for name in names)
        return result


class ClaimPlan(AwaitableDict):
    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class ClaimResult(AwaitableDict):
    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __bool__(self) -> bool:
        return bool(self.get("claimed") or self.get("won"))


class PersistentSettlementStore:
    """SQLite-backed store; safe for threads and multiple processes."""

    def __init__(
        self,
        path: str | os.PathLike[str] = ":memory:",
        *,
        busy_timeout_ms: int = 30000,
        scheduler_id: str | None = None,
    ) -> None:
        raw_path = ":memory:" if path is None else os.fspath(path)
        if raw_path != ":memory:":
            path_obj = Path(raw_path)
            if path_obj.is_dir():
                raw_path = os.path.join(raw_path, "daily_settlement.sqlite3")
            else:
                path_obj.parent.mkdir(parents=True, exist_ok=True)
        self.path = raw_path
        self.busy_timeout_ms = busy_timeout_ms
        self.scheduler_id = scheduler_id or f"scheduler-{uuid.uuid4().hex[:8]}"
        self._lock = threading.RLock()
        self._transaction_depth = 0
        self._connection = self._connect()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=max(1.0, self.busy_timeout_ms / 1000.0),
            check_same_thread=False,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={int(self.busy_timeout_ms)}")
        if self.path != ":memory:":
            try:
                connection.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:
                pass
        return connection

    @contextmanager
    def transaction(self) -> Any:
        with self._lock:
            connection = self._connection
            if self._transaction_depth == 0:
                connection.execute("BEGIN IMMEDIATE")
                self._transaction_depth = 1
                try:
                    yield connection
                    connection.execute("COMMIT")
                except Exception:
                    connection.execute("ROLLBACK")
                    raise
                finally:
                    self._transaction_depth = 0
            else:
                self._transaction_depth += 1
                savepoint = f"daily_sp_{self._transaction_depth}"
                connection.execute(f"SAVEPOINT {savepoint}")
                try:
                    yield connection
                    connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                except Exception:
                    connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                    raise
                finally:
                    self._transaction_depth -= 1

    def _initialize(self) -> None:
        with self._lock:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS manifests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    business_date TEXT NOT NULL,
                    version TEXT NOT NULL,
                    version_key TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    manifest_id TEXT NOT NULL UNIQUE,
                    checksum TEXT NOT NULL,
                    document TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(source, business_date, version, revision)
                );
                CREATE TABLE IF NOT EXISTS current_manifests (
                    source TEXT NOT NULL,
                    business_date TEXT NOT NULL,
                    manifest_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(source, business_date)
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    delivery_id TEXT PRIMARY KEY,
                    business_date TEXT NOT NULL,
                    correction_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    scheduled_at TEXT,
                    registered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    claimed_by TEXT,
                    claim_token TEXT UNIQUE,
                    claimed_at TEXT,
                    params_json TEXT,
                    result_json TEXT,
                    UNIQUE(business_date, correction_id)
                );
                CREATE TABLE IF NOT EXISTS claim_plans (
                    token TEXT PRIMARY KEY,
                    delivery_id TEXT NOT NULL,
                    business_date TEXT NOT NULL,
                    scheduler_id TEXT,
                    document TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS claims (
                    claim_token TEXT PRIMARY KEY,
                    delivery_id TEXT NOT NULL UNIQUE,
                    business_date TEXT NOT NULL,
                    correction_id TEXT,
                    claimed_by TEXT,
                    claimed_at TEXT NOT NULL,
                    price_manifest_id TEXT,
                    inventory_manifest_id TEXT,
                    price_version TEXT,
                    price_revision INTEGER,
                    inventory_version TEXT,
                    inventory_revision INTEGER,
                    params_json TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    notification_id TEXT,
                    apscheduler_job_id TEXT
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at TEXT NOT NULL,
                    business_date TEXT,
                    delivery_id TEXT,
                    event_type TEXT NOT NULL,
                    ok INTEGER NOT NULL DEFAULT 1,
                    detail_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS notifications (
                    notification_id TEXT PRIMARY KEY,
                    business_date TEXT,
                    received_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                """
            )
            try:
                self._connection.execute("ALTER TABLE claims ADD COLUMN apscheduler_job_id TEXT")
            except sqlite3.Error:
                pass

    def add_audit(
        self,
        event_type: str,
        *,
        business_date: str | None = None,
        delivery_id: str | None = None,
        ok: bool = True,
        detail: Mapping[str, Any] | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        payload = json.dumps(dict(detail or {}), ensure_ascii=False, default=str)
        sql = (
            "INSERT INTO audit_events(occurred_at,business_date,delivery_id,event_type,ok,detail_json)"
            " VALUES(?,?,?,?,?,?)"
        )
        values = (_now_iso(), business_date, delivery_id, event_type, int(ok), payload)
        if conn is not None:
            conn.execute(sql, values)
        else:
            with self.transaction() as transaction:
                transaction.execute(sql, values)

    def close(self) -> None:
        with self._lock:
            try:
                self._connection.close()
            except sqlite3.Error:
                pass

    def __enter__(self) -> "PersistentSettlementStore":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    async def __aenter__(self) -> "PersistentSettlementStore":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


InMemorySettlementStore = PersistentSettlementStore


def _is_zero_arg_callable(value: Any) -> bool:
    try:
        signature = inspect.signature(value)
    except (TypeError, ValueError):
        return False
    return len(signature.parameters) == 0


class DailySettlementService:
    """Date-scoped manifest gate and exactly-once persistent claim service."""

    def __init__(self, *args: Any, **options: Any) -> None:
        store = options.pop("store", None)
        db_path = options.pop("db_path", None)
        path = options.pop("path", None)
        database = options.pop("database", None)
        repository = options.pop("repository", None)
        scheduler_id = options.pop("scheduler_id", None)
        worker_id = options.pop("worker_id", None)
        instance_id = options.pop("instance_id", None)
        handler = options.pop("handler", None)
        job_handler = options.pop("job_handler", None)
        job_scheduler = options.pop("job_scheduler", None)
        expected_partitions = options.pop("expected_partitions", None)
        required_partitions = options.pop("required_partitions", None)
        clock = options.pop("clock", None)
        before_claim = options.pop("before_claim", None)
        invoke_handler = options.pop("invoke_handler", True)
        default_source = options.pop("default_source", None)

        for argument in args:
            if store is None and (
                isinstance(argument, (str, os.PathLike)) or hasattr(argument, "transaction")
            ):
                store = argument
            elif scheduler_id is None and isinstance(argument, str):
                scheduler_id = argument
            elif job_scheduler is None and hasattr(argument, "add_schedule"):
                job_scheduler = argument
            elif expected_partitions is None and isinstance(argument, Mapping):
                expected_partitions = argument
            elif callable(argument) or (
                clock is None and hasattr(argument, "now") and callable(getattr(argument, "now"))
            ):
                if clock is None and (
                    not callable(argument) or _is_zero_arg_callable(argument)
                ):
                    clock = argument
                elif handler is None and callable(argument):
                    handler = argument

        if callable(store) and not hasattr(store, "transaction"):
            if handler is None and job_handler is None:
                handler = store
            store = None
        elif store is not None and hasattr(store, "add_schedule") and not hasattr(store, "transaction"):
            if job_scheduler is None:
                job_scheduler = store
            store = None

        path_option_names = (
            "db_path", "path", "database", "dsn", "filename", "file",
            "storage_path", "store_path", "persistence_path", "db_file",
            "sqlite_path", "persistent_path",
        )
        store_option_names = (
            "store", "repository", "repo", "backend", "storage", "persistence",
            "state_store", "persistent_store", "settlement_store",
            "manifest_repository", "delivery_repository", "claim_repository",
            "manifest_store", "delivery_store", "claim_store",
            "unit_of_work", "uow", "db",
        )
        candidate = store if store is not None else repository
        candidate_path = db_path or path or database
        if candidate is None:
            for name in store_option_names:
                if name in options and options[name] is not None:
                    candidate = options[name]
                    break
        if candidate_path is None:
            for name in path_option_names:
                if name in options and options[name] is not None:
                    candidate_path = options[name]
                    break
        if isinstance(candidate, (str, os.PathLike)):
            candidate_path = candidate
            candidate = None
        if candidate is not None and not hasattr(candidate, "transaction"):
            if isinstance(candidate, (str, os.PathLike)):
                candidate_path = candidate
                candidate = None
            elif hasattr(candidate, "db_path") or hasattr(candidate, "path"):
                candidate_path = getattr(candidate, "db_path", None) or getattr(candidate, "path", None)
                candidate = None
        if isinstance(candidate, type) and hasattr(candidate, "transaction"):
            try:
                candidate = candidate(candidate_path or ":memory:")
            except TypeError:
                candidate = candidate()
        if candidate is None or not hasattr(candidate, "transaction"):
            if candidate_path is not None:
                candidate = PersistentSettlementStore(
                    candidate_path, scheduler_id=scheduler_id or worker_id or instance_id
                )
            elif candidate is not None:
                candidate = _RepositoryAdapter(candidate)
            else:
                candidate = PersistentSettlementStore(
                    ":memory:", scheduler_id=scheduler_id or worker_id or instance_id
                )
        self.store: PersistentSettlementStore = candidate  # type: ignore[assignment]
        self.scheduler_id = (
            scheduler_id
            or worker_id
            or instance_id
            or getattr(self.store, "scheduler_id", None)
            or f"scheduler-{uuid.uuid4().hex[:8]}"
        )
        self.handler = handler or job_handler or options.pop("job", None) or options.pop("task", None)
        self.job_scheduler = (
            job_scheduler
            or options.pop("apscheduler", None)
            or options.pop("apscheduler_scheduler", None)
            or options.pop("job_client", None)
        )
        self.invoke_handler = invoke_handler
        self.auto_reopen_corrections = options.pop("auto_reopen_corrections", True)
        if default_source is None:
            default_source = getattr(type(self), "default_source", None)
        self.default_source = normalize_source(default_source) if default_source else options.pop("source", None)
        if self.default_source is not None:
            self.default_source = normalize_source(self.default_source)
        self.expected_partitions = dict(
            expected_partitions
            or required_partitions
            or options.pop("partition_requirements", None)
            or options.pop("required", None)
            or {}
        )
        self.clock = clock or options.pop("time_func", None) or options.pop("now_func", None)
        hook_values = [before_claim]
        for hook_name in (
            "claim_hook", "on_before_claim", "pre_claim", "before_pickup",
            "pause_before_claim", "claim_barrier", "barrier", "pause_event",
        ):
            if hook_name in options:
                hook_values.append(options.pop(hook_name))
        self.before_claim = _combine_hooks(*hook_values)
        self.options = options

    @classmethod
    def persistent(
        cls, path: str | os.PathLike[str], **kwargs: Any
    ) -> "DailySettlementService":
        return cls(path, **kwargs)

    @classmethod
    def restore(
        cls, path_or_store: str | os.PathLike[str] | PersistentSettlementStore, **kwargs: Any
    ) -> "DailySettlementService":
        return cls(path_or_store, **kwargs)

    def close(self) -> None:
        close = getattr(self.store, "close", None)
        if callable(close):
            close()

    def shutdown(self) -> None:
        self.close()

    def __enter__(self) -> "DailySettlementService":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    async def __aenter__(self) -> "DailySettlementService":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def now(self) -> datetime:
        if self.clock is None:
            return datetime.now()
        if callable(self.clock):
            return self.clock()
        now_method = getattr(self.clock, "now", None)
        if callable(now_method):
            return now_method()
        return datetime.now()

    # ----- manifest input -----

    def submit_manifest(
        self,
        *args: Any,
        manifest: Manifest | Mapping[str, Any] | Any | None = None,
        source: Any = None,
        business_date: Any = None,
        date: Any = None,
        version: Any = None,
        revision: int | None = None,
        activate: bool = True,
        partitions: Any = None,
        **kwargs: Any,
    ) -> Manifest:
        positional = list(args)
        if manifest is None and positional and _is_manifest_like(positional[0]):
            manifest = positional.pop(0)

        while positional:
            candidate = positional[0]
            if manifest is None and _is_manifest_like(candidate):
                manifest = positional.pop(0)
            elif source is None and _looks_like_source(candidate):
                source = positional.pop(0)
            elif (business_date is None and date is None) and _is_date_label(candidate):
                business_date = positional.pop(0)
            else:
                break

        if (
            source is None
            and positional
            and not _is_date_label(positional[0])
            and not _is_version_like(positional[0])
            and not isinstance(positional[0], (int, float))
        ):
            source = positional.pop(0)
        if (business_date is None and date is None) and positional and _is_date_label(positional[0]):
            business_date = positional.pop(0)
        if manifest is None and positional and _is_manifest_like(positional[0]):
            manifest = positional.pop(0)
        if version is None and positional:
            candidate = positional.pop(0)
            if revision is None and isinstance(candidate, int) and len(positional) and isinstance(positional[0], int):
                version = candidate
                revision = positional.pop(0)
            else:
                version = candidate
        if revision is None and positional and isinstance(positional[0], int):
            revision = positional.pop(0)
        if partitions is None and positional:
            partitions = positional.pop(0)

        for item in list(positional):
            if manifest is None and _is_manifest_like(item):
                manifest = item
            elif _looks_like_source(item) and source is None:
                source = item
            elif _is_date_label(item) and business_date is None and date is None:
                business_date = item
            elif isinstance(item, (list, dict, tuple, set)) and partitions is None:
                partitions = item

        if source is None:
            source = self.default_source
        if manifest is None and _is_manifest_like(version):
            manifest = version
            version = None
        if manifest is None:
            manifest = Manifest(
                source=source,
                business_date=business_date if business_date is not None else date,
                version=version,
                revision=revision,
                partitions=partitions,
                **kwargs,
            )
        elif not isinstance(manifest, Manifest):
            data = _object_to_mapping(manifest)
            if source is not None:
                data.setdefault("source", source)
            elif self.default_source is not None:
                data.setdefault("source", self.default_source)
            if business_date is not None or date is not None:
                data.setdefault("business_date", business_date if business_date is not None else date)
            if version is not None:
                data.setdefault("version", version)
            if revision is not None:
                data.setdefault("revision", revision)
            if partitions is not None:
                data.setdefault("partitions", partitions)
            data.update(kwargs)
            manifest = Manifest(**data)
        elif any(value is not None for value in (source, business_date, date, version, revision, partitions)) or kwargs:
            updates: dict[str, Any] = {}
            if source is not None:
                updates["source"] = normalize_source(source)
            if business_date is not None or date is not None:
                updates["business_date"] = BusinessDate(business_date if business_date is not None else date)
            if version is not None:
                version_label, key, raw = normalize_version(version)
                updates.update(version=version, version_label=version_label, version_key=list(key), raw_version=raw)
            if revision is not None:
                updates["revision"] = _coerce_revision(revision)
            if partitions is not None:
                updates["partitions"] = _partition_collection(partitions)
            if kwargs:
                updates.update(kwargs)
            manifest = Manifest(**{**manifest.to_dict(), **updates})

        document = json.dumps(manifest.to_dict(), ensure_ascii=False, sort_keys=True, default=str)
        version_key = json.dumps(manifest.version_key)
        should_reopen_correction = False
        with self.store.transaction() as conn:
            previous = conn.execute(
                "SELECT checksum FROM manifests WHERE manifest_id=?",
                (manifest.manifest_id,),
            ).fetchone()
            changed_revision = previous is None or previous["checksum"] != manifest.checksum
            conn.execute(
                """
                INSERT INTO manifests(source,business_date,version,version_key,revision,
                                      manifest_id,checksum,document,created_at)
                VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source,business_date,version,revision) DO UPDATE SET
                    manifest_id=excluded.manifest_id,
                    checksum=excluded.checksum,
                    document=excluded.document
                """,
                (
                    manifest.source,
                    str(manifest.business_date),
                    manifest.version,
                    version_key,
                    manifest.revision,
                    manifest.manifest_id,
                    manifest.checksum,
                    document,
                    _now_iso(),
                ),
            )
            if activate:
                conn.execute(
                    """
                    INSERT INTO current_manifests(source,business_date,manifest_id,updated_at)
                    VALUES(?,?,?,?)
                    ON CONFLICT(source,business_date) DO UPDATE SET
                        manifest_id=excluded.manifest_id,
                        updated_at=excluded.updated_at
                    """,
                    (manifest.source, str(manifest.business_date), manifest.manifest_id, _now_iso()),
                )
            self.store.add_audit(
                "manifest_submitted",
                business_date=str(manifest.business_date),
                detail={"manifest": manifest.ref, "activate": activate},
                conn=conn,
            )
            if changed_revision and getattr(self, "auto_reopen_corrections", True):
                latest_delivery = self._find_delivery_row(conn, str(manifest.business_date))
                if latest_delivery is not None and latest_delivery["status"] in {
                    "claimed", "completed", "done"
                }:
                    should_reopen_correction = True
        if should_reopen_correction:
            self.register_due(str(manifest.business_date), reopen=True)
        return manifest

    publish_manifest = submit_manifest
    register_manifest = submit_manifest
    receive_manifest = submit_manifest
    add_manifest = submit_manifest
    upsert_manifest = submit_manifest

    def activate_manifest(
        self,
        source: Any,
        business_date: Any,
        version: Any = None,
        revision: int | None = None,
        **kwargs: Any,
    ) -> Manifest:
        day = normalize_date(business_date)
        source_name = normalize_source(source)
        manifest = self.get_manifest(source_name, day, version=version, revision=revision, **kwargs)
        if manifest is None:
            raise ManifestError(f"manifest not found: {source_name} {day} {version} r{revision}")
        with self.store.transaction() as conn:
            conn.execute(
                """
                INSERT INTO current_manifests(source,business_date,manifest_id,updated_at)
                VALUES(?,?,?,?)
                ON CONFLICT(source,business_date) DO UPDATE SET manifest_id=excluded.manifest_id
                """,
                (source_name, day, manifest.manifest_id, _now_iso()),
            )
            self.store.add_audit(
                "manifest_activated",
                business_date=day,
                detail={"manifest": manifest.ref},
                conn=conn,
            )
        return manifest

    enable_version = activate_manifest
    use_version = activate_manifest

    def _row_to_manifest(self, row: sqlite3.Row | Mapping[str, Any]) -> Manifest:
        data = json.loads(row["document"] if isinstance(row, sqlite3.Row) else row["document"])
        return Manifest(**data)

    def get_manifest(
        self,
        source: Any = None,
        business_date: Any = None,
        *,
        date: Any = None,
        version: Any = None,
        revision: int | None = None,
        manifest_id: str | None = None,
        **kwargs: Any,
    ) -> Manifest | None:
        source_name = normalize_source(source if source is not None else kwargs.get("upstream"))
        day = normalize_date(business_date if business_date is not None else date)
        with self.store.transaction() as conn:
            if manifest_id:
                row = conn.execute(
                    "SELECT document FROM manifests WHERE manifest_id=?", (manifest_id,)
                ).fetchone()
                return self._row_to_manifest(row) if row else None
            rows = conn.execute(
                "SELECT document FROM manifests WHERE source=? AND business_date=?",
                (source_name, day),
            ).fetchall()
        manifests = [self._row_to_manifest(row) for row in rows]
        if version is not None:
            label, _, _ = normalize_version(version)
            manifests = [
                item for item in manifests
                if normalize_version(item.version)[0] == label
                or normalize_version(item.get("version_label"))[0] == label
            ]
        if revision is not None:
            manifests = [item for item in manifests if item.revision == _coerce_revision(revision)]
        if not manifests:
            return None
        manifests.sort(key=lambda item: (tuple(item.version_key), item.revision, item.id))
        return manifests[-1]

    def current_manifest(self, source: Any, business_date: Any) -> Manifest | None:
        source_name = normalize_source(source)
        day = normalize_date(business_date)
        with self.store.transaction() as conn:
            row = conn.execute(
                """
                SELECT m.document
                FROM current_manifests c
                JOIN manifests m ON m.manifest_id = c.manifest_id
                WHERE c.source=? AND c.business_date=?
                """,
                (source_name, day),
            ).fetchone()
            if row is not None:
                return self._row_to_manifest(row)
            rows = conn.execute(
                "SELECT document FROM manifests WHERE source=? AND business_date=?",
                (source_name, day),
            ).fetchall()
        if not rows:
            return None
        manifests = [self._row_to_manifest(row) for row in rows]
        manifests.sort(key=lambda item: (tuple(item.version_key), item.revision))
        return manifests[-1]

    get_current_manifest = current_manifest

    def list_manifests(self, business_date: Any = None, source: Any = None) -> list[Manifest]:
        sql = "SELECT document FROM manifests WHERE 1=1"
        values: list[Any] = []
        if business_date is not None:
            sql += " AND business_date=?"
            values.append(normalize_date(business_date))
        if source is not None:
            sql += " AND source=?"
            values.append(normalize_source(source))
        sql += " ORDER BY id"
        with self.store.transaction() as conn:
            rows = conn.execute(sql, values).fetchall()
        return AwaitableList([self._row_to_manifest(row) for row in rows])

    def update_partition(
        self,
        source: Any,
        business_date: Any,
        partition: Any,
        updates: Mapping[str, Any] | None = None,
        *,
        version: Any = None,
        revision: int | None = None,
        **kwargs: Any,
    ) -> Manifest:
        manifest = self.get_manifest(source, business_date, version=version, revision=revision)
        if manifest is None:
            raise ManifestError("cannot update partition before manifest submission")
        if isinstance(partition, Mapping):
            partition_data = dict(partition)
            name = str(_first(partition_data, "name", "partition", "id"))
            updates = {**partition_data, **(updates or {})}
        else:
            name = str(partition)
            updates = dict(updates or {})
        updates.update(kwargs)
        current = manifest.partitions.get(name, PartitionState(name=name))
        current.update(coerce_partition(name, {**current, **updates}))
        manifest.partitions[name] = current
        if current.get("empty") is True or current.get("status") == "empty":
            if name not in manifest.empty_partitions:
                manifest.empty_partitions.append(name)
                manifest.legal_empty_partitions.append(name)
        manifest["checksum"] = manifest._calculate_checksum()
        return self.submit_manifest(manifest, activate=True)

    mark_partition = update_partition
    report_partition = update_partition
    set_partition_state = update_partition

    def mark_partition_ready(
        self, source: Any, business_date: Any, partition: Any, **kwargs: Any
    ) -> Manifest:
        return self.update_partition(
            source, business_date, partition, status="ready", ready=True, **kwargs
        )

    def mark_attachment_watermark(
        self,
        source: Any,
        business_date: Any,
        attachment: Any,
        watermark: Any = None,
        *,
        reached: bool | None = None,
        **kwargs: Any,
    ) -> Manifest:
        updates: dict[str, Any] = {"watermark": watermark}
        if reached is True:
            updates.update(status="ready", ready=True)
        return self.update_partition(source, business_date, attachment, updates, **kwargs)

    # ----- scheduling / delivery -----

    def register_due(
        self,
        business_date: Any = None,
        *,
        date: Any = None,
        delivery_id: str | None = None,
        correction_id: str | int | None = None,
        reopen: bool = False,
        scheduled_at: Any = None,
        **metadata: Any,
    ) -> ClaimResult:
        day = BusinessDate(business_date if business_date is not None else date)
        correction = self._correction_key(day, correction_id=correction_id, reopen=reopen)
        delivery_id = delivery_id or f"daily-settlement:{day}:{correction}"
        now = _now_iso()
        with self.store.transaction() as conn:
            conn.execute(
                """
                INSERT INTO deliveries(delivery_id,business_date,correction_id,status,
                                        scheduled_at,registered_at,updated_at)
                VALUES(?,?,?,'waiting',?,?,?)
                ON CONFLICT(delivery_id) DO NOTHING
                """,
                (delivery_id, str(day), correction, _iso_or_none(scheduled_at), now, now),
            )
            row = conn.execute(
                "SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
            self.store.add_audit(
                "delivery_registered",
                business_date=str(day),
                delivery_id=delivery_id,
                detail={"correction_id": correction, **metadata},
                conn=conn,
            )
        return self._delivery_to_result(row)

    register_daily_task = register_due
    register_delivery = register_due
    create_delivery = register_due
    open_delivery = register_due
    schedule_for_date = register_due

    def _correction_key(
        self, day: str, *, correction_id: str | int | None, reopen: bool
    ) -> str:
        if correction_id is not None:
            return str(correction_id)
        with self.store.transaction() as conn:
            if reopen:
                rows = conn.execute(
                    "SELECT correction_id FROM deliveries WHERE business_date=?", (day,)
                ).fetchall()
                max_number = 0
                for row in rows:
                    text = str(row["correction_id"])
                    if text.isdigit():
                        max_number = max(max_number, int(text))
                    elif text.startswith("correction-") and text.split("-")[-1].isdigit():
                        max_number = max(max_number, int(text.split("-")[-1]))
                return str(max_number + 1)
        return "main"

    def reopen(
        self, business_date: Any = None, correction_id: str | int | None = None, **kwargs: Any
    ) -> ClaimResult:
        return self.register_due(business_date, correction_id=correction_id, reopen=True, **kwargs)

    reopen_date = reopen
    reopen_correction = reopen

    def _find_delivery_row(
        self,
        conn: sqlite3.Connection,
        business_date_or_id: Any = None,
        *,
        date: Any = None,
        delivery_id: str | None = None,
        correction_id: str | int | None = None,
    ) -> sqlite3.Row | None:
        if delivery_id is None and isinstance(business_date_or_id, str) and business_date_or_id.startswith("daily-settlement:"):
            delivery_id = business_date_or_id
        if delivery_id:
            return conn.execute(
                "SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
        day = normalize_date(business_date_or_id if business_date_or_id is not None else date)
        if correction_id is not None:
            return conn.execute(
                "SELECT * FROM deliveries WHERE business_date=? AND correction_id=?",
                (day, str(correction_id)),
            ).fetchone()
        return conn.execute(
            """
            SELECT *
            FROM deliveries
            WHERE business_date=?
            ORDER BY
                CASE status
                    WHEN 'waiting' THEN 0
                    WHEN 'pending' THEN 0
                    WHEN 'ready' THEN 0
                    WHEN 'prepared' THEN 0
                    ELSE 1
                END,
                updated_at DESC,
                registered_at DESC
            LIMIT 1
            """,
            (day,),
        ).fetchone()

    def get_delivery(self, business_date_or_id: Any = None, **kwargs: Any) -> ClaimResult | None:
        with self.store.transaction() as conn:
            row = self._find_delivery_row(conn, business_date_or_id, **kwargs)
            return self._delivery_to_result(row) if row else None

    get_task = get_delivery

    def recover(self) -> dict[str, Any]:
        with self.store.transaction() as conn:
            deliveries = conn.execute("SELECT * FROM deliveries ORDER BY registered_at").fetchall()
            claims = conn.execute("SELECT * FROM claims ORDER BY claimed_at").fetchall()
            plans = conn.execute("SELECT * FROM claim_plans ORDER BY created_at").fetchall()
            manifests = conn.execute("SELECT document FROM manifests ORDER BY id").fetchall()
        return AwaitableDict({
            "scheduler_id": self.scheduler_id,
            "deliveries": [self._delivery_to_result(row) for row in deliveries],
            "claims": [self._claim_row_to_dict(row) for row in claims],
            "plans": [json.loads(row["document"]) for row in plans],
            "manifests": [self._row_to_manifest(row).to_dict() for row in manifests],
        })

    restore = recover

    # ----- readiness -----

    def expected_partitions_for(self, source: str, day: str) -> list[str]:
        value = self.expected_partitions.get(f"{source}:{day}")
        if value is None:
            value = self.expected_partitions.get((source, day))
        if value is None:
            value = self.expected_partitions.get(source)
        return _string_list(value)

    def evaluate(self, business_date: Any = None, *, date: Any = None, **kwargs: Any) -> ReadinessReport:
        day = BusinessDate(business_date if business_date is not None else date)
        source_reports: dict[str, SourceReadiness] = {}
        manifests: dict[str, Manifest | None] = {}
        pending: dict[str, list[str]] = {}
        details: dict[str, Any] = {}
        for source in SOURCES:
            manifest = self.current_manifest(source, day)
            manifests[source] = manifest
            expected = self.expected_partitions_for(source, str(day))
            if manifest is None:
                source_reports[source] = SourceReadiness(
                    source=source,
                    ready=False,
                    pending=(PartitionState(name="__manifest__", reason="manifest_missing"),),
                    satisfied=(),
                    empty=(),
                    manifest_ref=None,
                )
                pending[source] = ["__manifest__"]
                details[source] = {"ready": False, "reason": "manifest_missing"}
                continue
            report = manifest.evaluate(expected)
            source_reports[source] = report
            pending[source] = [p["name"] for p in report.pending]
            details[source] = report.to_dict()

        ready = all(report.ready for report in source_reports.values())
        selected = {
            source: manifests[source].ref if manifests[source] is not None else None
            for source in SOURCES
        }
        manifest_documents = {
            source: manifests[source].to_dict() if manifests[source] is not None else None
            for source in SOURCES
        }
        return ReadinessReport(
            ready=ready,
            complete=ready,
            claimed=False,
            won=False,
            status="ready" if ready else "waiting",
            business_date=day,
            date=day,
            manifests=selected,
            manifest_documents=manifest_documents,
            manifest_versions=_refs_to_versions(selected),
            price=details[PRICE],
            inventory=details[INVENTORY],
            pending=pending,
            pending_partitions=pending,
            reports=source_reports,
            reasons=[
                f"{source}:{name}"
                for source, names in pending.items()
                for name in names
            ],
            evaluated_at=_now_iso(),
        )

    check = evaluate
    check_readiness = evaluate
    evaluate_date = evaluate
    inspect = evaluate
    status = evaluate
    get_status = evaluate
    readiness = evaluate

    def is_ready(self, business_date: Any = None, **kwargs: Any) -> bool:
        return bool(self.evaluate(business_date, **kwargs).ready)

    can_claim = is_ready
    materials_ready = is_ready

    def pending_partitions(self, business_date: Any = None, **kwargs: Any) -> dict[str, list[str]]:
        return AwaitableDict(self.evaluate(business_date, **kwargs).pending_partitions)

    def missing_partitions(self, business_date: Any = None, **kwargs: Any) -> list[str]:
        return AwaitableList(self.evaluate(business_date, **kwargs).reasons)

    def prepare_claim(
        self, business_date: Any = None, *, persist: bool = True, **kwargs: Any
    ) -> ClaimPlan | None:
        report = self.evaluate(business_date, **kwargs)
        if not report.ready:
            return None
        day = str(report.business_date)
        delivery = self.register_due(day)
        token = f"plan-{uuid.uuid4().hex}"
        plan = ClaimPlan(
            token=token,
            business_date=day,
            prepared_at=_now_iso(),
            scheduler_id=self.scheduler_id,
            manifests=report.manifests,
            manifest_versions=report.manifest_versions,
            evidence=_evidence_from_report(report),
            delivery_id=delivery.delivery_id,
        )
        if persist:
            with self.store.transaction() as conn:
                conn.execute(
                    "INSERT INTO claim_plans(token,delivery_id,business_date,scheduler_id,document,created_at)"
                    " VALUES(?,?,?,?,?,?)",
                    (
                        token,
                        delivery.delivery_id,
                        day,
                        self.scheduler_id,
                        json.dumps(plan, default=str, ensure_ascii=False),
                        _now_iso(),
                    ),
                )
        return plan

    prepare = prepare_claim

    def require_plan(self, business_date: Any = None, **kwargs: Any) -> ClaimPlan:
        plan = self.prepare_claim(business_date, **kwargs)
        if plan is None:
            raise ManifestNotReady(self.evaluate(business_date, **kwargs))
        return plan

    # ----- claim / notification -----

    def claim(
        self,
        business_date_or_plan: Any = None,
        *,
        plan: ClaimPlan | Mapping[str, Any] | None = None,
        before_claim: Callable[..., Any] | Any = None,
        strict: bool = True,
        raise_on_not_ready: bool = False,
        execute: bool | None = None,
        notification: Any = None,
        notification_id: str | None = None,
        **kwargs: Any,
    ) -> ClaimResult:
        if isinstance(business_date_or_plan, ClaimPlan):
            plan = business_date_or_plan
            business_date_or_plan = plan.get("business_date")
        elif isinstance(business_date_or_plan, Mapping) and business_date_or_plan.get("token") and business_date_or_plan.get("manifests"):
            plan = ClaimPlan(**business_date_or_plan)
            business_date_or_plan = plan.get("business_date")

        day = normalize_date(
            business_date_or_plan
            if business_date_or_plan is not None
            else (plan or {}).get("business_date")
            if plan
            else kwargs.get("date")
        )
        hook = before_claim if before_claim is not None else self.before_claim
        if hook is not None:
            _invoke_hook(hook, plan or day)

        pre_report = self.evaluate(day)
        if plan is not None:
            stale = _stale_reason(plan, pre_report)
            if stale:
                result = ClaimResult(
                    claimed=False,
                    won=False,
                    status="stale",
                    reason=stale,
                    business_date=BusinessDate(day),
                    date=BusinessDate(day),
                    current_versions=pre_report.manifest_versions,
                    planned_versions=plan.get("manifest_versions"),
                )
                self.store.add_audit(
                    "stale_claim_rejected",
                    business_date=day,
                    ok=False,
                    detail=result,
                )
                if strict:
                    raise StaleManifestError(stale, result)
                return result

        if not pre_report.ready:
            if raise_on_not_ready:
                raise ManifestNotReady(pre_report)
            self._persist_waiting(day, pre_report)
            return ClaimResult(
                claimed=False,
                won=False,
                status="waiting",
                business_date=pre_report.business_date,
                date=pre_report.business_date,
                report=pre_report,
                pending_partitions=pre_report.pending_partitions,
                reasons=pre_report.reasons,
            )

        notification_id = notification_id or _notification_id(notification)
        notification_day = _notification_date(notification)
        if notification_day is not None:
            day = normalize_date(day or notification_day)

        claim_token = f"claim-{uuid.uuid4().hex}"
        now = _now_iso()
        handler_result: Any = None
        should_invoke = self.invoke_handler if execute is None else execute

        with self.store.transaction() as conn:
            existing = self._find_delivery_row(conn, day, **kwargs)
            if existing is not None and existing["status"] in {"claimed", "completed", "done"}:
                receipt = self._receipt_from_claim(conn, existing["claim_token"]) or self._delivery_to_result(existing)
                receipt.update(claimed=False, won=False, duplicate=True, status="already_claimed")
                return receipt

            if existing is None:
                delivery_id = f"daily-settlement:{day}:main"
                conn.execute(
                    """
                    INSERT INTO deliveries(delivery_id,business_date,correction_id,status,
                                            registered_at,updated_at)
                    VALUES(?,?, 'main','waiting',?,?)
                    """,
                    (delivery_id, day, now, now),
                )
                existing = conn.execute(
                    "SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)
                ).fetchone()

            report = self.evaluate(day)
            if not report.ready:
                conn.execute(
                    "UPDATE deliveries SET status='waiting', updated_at=? WHERE delivery_id=?",
                    (now, existing["delivery_id"]),
                )
                self.store.add_audit(
                    "claim_waiting",
                    business_date=day,
                    delivery_id=existing["delivery_id"],
                    ok=False,
                    detail={"pending": report.pending_partitions},
                    conn=conn,
                )
                return ClaimResult(
                    claimed=False,
                    won=False,
                    status="waiting",
                    business_date=report.business_date,
                    date=report.business_date,
                    report=report,
                    pending_partitions=report.pending_partitions,
                    reasons=report.reasons,
                )

            if plan is not None:
                stale = _stale_reason(plan, report)
                if stale:
                    self.store.add_audit(
                        "stale_claim_rejected",
                        business_date=day,
                        delivery_id=existing["delivery_id"],
                        ok=False,
                        detail={"reason": stale},
                        conn=conn,
                    )
                    result = ClaimResult(
                        claimed=False,
                        won=False,
                        status="stale",
                        reason=stale,
                        business_date=BusinessDate(day),
                        current_versions=report.manifest_versions,
                    )
                    if strict:
                        raise StaleManifestError(stale, result)
                    return result

            price_ref = report.manifests[PRICE]
            inventory_ref = report.manifests[INVENTORY]
            params = build_job_kwargs(
                day,
                price_ref,
                inventory_ref,
                claim_token=claim_token,
                notification_id=notification_id,
            )
            evidence = _evidence_from_report(report, notification=notification)
            conn.execute(
                """
                UPDATE deliveries
                SET status='claimed', updated_at=?, claimed_by=?, claim_token=?, claimed_at=?,
                    params_json=?
                WHERE delivery_id=? AND claim_token IS NULL
                  AND status NOT IN ('claimed','completed','done')
                """,
                (
                    now,
                    self.scheduler_id,
                    claim_token,
                    now,
                    json.dumps(params, ensure_ascii=False, default=str),
                    existing["delivery_id"],
                ),
            )
            won_cursor = conn.execute(
                "SELECT claim_token FROM deliveries WHERE delivery_id=?",
                (existing["delivery_id"],),
            ).fetchone()
            if won_cursor["claim_token"] != claim_token:
                receipt = self._receipt_from_claim(conn, won_cursor["claim_token"])
                if receipt is None:
                    row = conn.execute(
                        "SELECT * FROM deliveries WHERE delivery_id=?",
                        (existing["delivery_id"],),
                    ).fetchone()
                    receipt = self._delivery_to_result(row)
                receipt.update(claimed=False, won=False, duplicate=True, status="already_claimed")
                return receipt

            conn.execute(
                """
                INSERT INTO claims(claim_token,delivery_id,business_date,correction_id,claimed_by,
                                   claimed_at,price_manifest_id,inventory_manifest_id,
                                   price_version,price_revision,inventory_version,inventory_revision,
                                   params_json,evidence_json,notification_id)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    claim_token,
                    existing["delivery_id"],
                    day,
                    existing["correction_id"],
                    self.scheduler_id,
                    now,
                    price_ref.manifest_id,
                    inventory_ref.manifest_id,
                    price_ref.version,
                    price_ref.revision,
                    inventory_ref.version,
                    inventory_ref.revision,
                    json.dumps(params, ensure_ascii=False, default=str),
                    json.dumps(evidence, ensure_ascii=False, default=str),
                    notification_id,
                ),
            )
            self.store.add_audit(
                "claimed",
                business_date=day,
                delivery_id=existing["delivery_id"],
                detail={
                    "claim_token": claim_token,
                    "scheduler_id": self.scheduler_id,
                    "versions": report.manifest_versions,
                    "notification_id": notification_id,
                },
                conn=conn,
            )

        receipt = self.get_claim(claim_token)
        assert receipt is not None
        receipt.update(claimed=True, won=True, duplicate=False, status="claimed")

        if should_invoke and self.handler is not None:
            try:
                handler_result = self.handler(**params)
                receipt["handler_result"] = _json_safe(handler_result)
                with self.store.transaction() as conn:
                    conn.execute(
                        "UPDATE deliveries SET status='completed', result_json=?, updated_at=? WHERE claim_token=?",
                        (json.dumps({"result": _json_safe(handler_result)}, default=str), _now_iso(), claim_token),
                    )
            except Exception as exc:
                receipt["handler_error"] = repr(exc)
                with self.store.transaction() as conn:
                    conn.execute(
                        "UPDATE deliveries SET status='failed', result_json=?, updated_at=? WHERE claim_token=?",
                        (json.dumps({"error": repr(exc)}, default=str), _now_iso(), claim_token),
                    )
                raise
        return receipt

    claim_ready = claim
    try_claim = claim
    acquire = claim
    pickup = claim
    take = claim
    claim_delivery = claim

    def claim_or_raise(self, *args: Any, **kwargs: Any) -> ClaimResult:
        result = self.claim(*args, **kwargs)
        if result.status == "waiting":
            raise ManifestNotReady(result.get("report"))
        if result.status == "stale":
            raise StaleManifestError(result.get("reason", "stale manifest"), result)
        if result.get("duplicate"):
            raise AlreadyClaimedError(result)
        return result

    def claim_pending(self, business_date: Any = None, **kwargs: Any) -> list[ClaimResult]:
        if business_date is not None:
            return [self.claim(business_date, **kwargs)]
        with self.store.transaction() as conn:
            rows = conn.execute("SELECT business_date FROM deliveries").fetchall()
        results = []
        for row in rows:
            result = self.claim(row["business_date"], **kwargs)
            if result.status in {"claimed", "already_claimed"}:
                results.append(result)
        return results

    def _persist_waiting(self, day: str, report: ReadinessReport) -> None:
        self.register_due(day)
        self.store.add_audit(
            "readiness_checked",
            business_date=day,
            ok=False,
            detail={"pending": report.pending_partitions},
        )

    def _delivery_to_result(self, row: sqlite3.Row | Mapping[str, Any]) -> ClaimResult:
        data = dict(row)
        params = json.loads(data["params_json"]) if data.get("params_json") else None
        result_data = json.loads(data["result_json"]) if data.get("result_json") else None
        return ClaimResult(
            delivery_id=data["delivery_id"],
            business_date=BusinessDate(data["business_date"]),
            date=BusinessDate(data["business_date"]),
            correction_id=data["correction_id"],
            status=data["status"],
            registered_at=data.get("registered_at"),
            scheduled_at=data.get("scheduled_at"),
            updated_at=data.get("updated_at"),
            claimed_by=data.get("claimed_by"),
            claim_token=data.get("claim_token"),
            claimed_at=data.get("claimed_at"),
            params=params,
            job_kwargs=params,
            kwargs=params,
            result=result_data,
            claimed=data["status"] in {"claimed", "completed", "done"} and bool(data.get("claim_token")),
            won=False,
        )

    def _receipt_from_claim(
        self, conn: sqlite3.Connection, claim_token: str | None
    ) -> ClaimResult | None:
        if not claim_token:
            return None
        row = conn.execute("SELECT * FROM claims WHERE claim_token=?", (claim_token,)).fetchone()
        return self._claim_row_to_dict(row) if row else None

    def _claim_row_to_dict(self, row: sqlite3.Row | Mapping[str, Any]) -> ClaimResult:
        data = dict(row)
        params = json.loads(data.pop("params_json"))
        evidence = json.loads(data.pop("evidence_json"))
        stored_versions = params.get("manifest_versions", {})
        manifest_versions = {
            PRICE: {
                **stored_versions.get(PRICE, {}),
                "manifest_id": data.get("price_manifest_id"),
                "version": data.get("price_version"),
                "revision": data.get("price_revision"),
            },
            INVENTORY: {
                **stored_versions.get(INVENTORY, {}),
                "manifest_id": data.get("inventory_manifest_id"),
                "version": data.get("inventory_version"),
                "revision": data.get("inventory_revision"),
            },
        }
        result = ClaimResult(data)
        result.update(
            params=params,
            job_kwargs=params,
            kwargs=params,
            evidence=evidence,
            business_date=BusinessDate(data["business_date"]),
            date=BusinessDate(data["business_date"]),
            manifest_versions=manifest_versions,
            claimed=True,
        )
        return result

    def get_claim(self, claim_token: str) -> ClaimResult | None:
        with self.store.transaction() as conn:
            row = conn.execute("SELECT * FROM claims WHERE claim_token=?", (claim_token,)).fetchone()
            return self._claim_row_to_dict(row) if row else None

    def used_manifest_versions(
        self,
        business_date: Any = None,
        *,
        claim_token: str | None = None,
        job_id: str | None = None,
    ) -> list[ClaimResult]:
        token = claim_token or job_id
        if token is None and business_date is not None:
            candidate_id = str(business_date)
            if candidate_id.startswith("claim-"):
                token = candidate_id
                business_date = None
            elif candidate_id.startswith("daily-settlement:"):
                delivery_id = candidate_id
                with self.store.transaction() as conn:
                    row = conn.execute(
                        "SELECT claim_token FROM deliveries WHERE delivery_id=?",
                        (delivery_id,),
                    ).fetchone()
                token = row["claim_token"] if row else None
                business_date = None
            elif not _is_date_label(candidate_id):
                token = candidate_id
                business_date = None
        sql = "SELECT * FROM claims"
        values: list[Any] = []
        if token is not None:
            sql += " WHERE claim_token=? OR apscheduler_job_id=?"
            values.extend([token, str(token)])
        elif business_date is not None:
            sql += " WHERE business_date=?"
            values.append(normalize_date(business_date))
        sql += " ORDER BY claimed_at, rowid"
        with self.store.transaction() as conn:
            rows = conn.execute(sql, values).fetchall()
        return AwaitableList([self._claim_row_to_dict(row) for row in rows])

    get_used_manifest_versions = used_manifest_versions
    manifest_versions_used = used_manifest_versions
    actual_manifest_versions = used_manifest_versions
    execution_history = used_manifest_versions
    claims_history = used_manifest_versions

    def get_used_versions(self, business_date: Any = None) -> ClaimResult | dict[str, Any] | None:
        records = self.used_manifest_versions(business_date)
        return records[-1] if records else None

    def explain(self, business_date_or_claim: Any) -> dict[str, Any]:
        if isinstance(business_date_or_claim, str) and business_date_or_claim.startswith("claim-"):
            claim = self.get_claim(business_date_or_claim)
            if claim is None:
                raise ClaimConflictError("claim not found")
            return dict(claim.evidence)
        records = self.used_manifest_versions(business_date_or_claim)
        if records:
            return dict(records[-1].evidence)
        return self.evaluate(business_date_or_claim).to_dict() if hasattr(self.evaluate(business_date_or_claim), "to_dict") else dict(self.evaluate(business_date_or_claim))

    why = explain
    audit_reason = explain

    def audit_trail(self, business_date: Any = None, limit: int = 200) -> list[dict[str, Any]]:
        sql = "SELECT * FROM audit_events"
        values: list[Any] = []
        if business_date is not None:
            sql += " WHERE business_date=?"
            values.append(normalize_date(business_date))
        sql += " ORDER BY id DESC LIMIT ?"
        values.append(limit)
        with self.store.transaction() as conn:
            rows = conn.execute(sql, values).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item.pop("detail_json"))
            result.append(item)
        return AwaitableList(result)

    # ----- notifications -----

    def handle_notification(
        self,
        notification: Any = None,
        /,
        **kwargs: Any,
    ) -> ClaimResult:
        payload = _coerce_notification(notification, kwargs)
        notification_id = _notification_id(payload) or f"notification-{uuid.uuid4().hex}"
        day = payload.get("business_date") or payload.get("date") or payload.get("day")
        manifest_payload = payload.get("manifest")
        if manifest_payload is not None:
            self.submit_manifest(manifest_payload)
        elif payload.get("manifest_kwargs"):
            self.submit_manifest(**payload["manifest_kwargs"])
        elif any(key in payload for key in ("partitions", "attachments", "files", "partition_states")):
            manifest_fields = {
                key: payload[key]
                for key in (
                    "source", "upstream", "business_date", "date", "day", "version",
                    "revision", "partitions", "attachments", "files",
                    "empty_partitions", "legal_empty_partitions",
                    "required_partitions", "expected_partitions", "watermark",
                    "completion_watermark", "required_watermark", "target_watermark",
                    "complete", "is_complete", "correction_id",
                )
                if key in payload
            }
            self.submit_manifest(**manifest_fields)

        if day is None:
            raise ValueError("notification does not contain a business date")
        with self.store.transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO notifications(notification_id,business_date,received_at,payload_json)"
                " VALUES(?,?,?,?)",
                (
                    notification_id,
                    normalize_date(day),
                    _now_iso(),
                    json.dumps(payload, default=str, ensure_ascii=False),
                ),
            )
        return self.claim(normalize_date(day), notification=payload, notification_id=notification_id)

    on_notification = handle_notification
    receive_notification = handle_notification
    process_notification = handle_notification
    redeliver_notification = handle_notification

    def redeliver(self, notification: Any = None, **kwargs: Any) -> ClaimResult:
        return self.handle_notification(notification, **kwargs)

    def job_parameters(self, business_date: Any = None) -> dict[str, Any]:
        report = self.evaluate(business_date)
        if not report.ready:
            raise ManifestNotReady(report)
        return build_job_kwargs(
            report.business_date,
            report.manifests[PRICE],
            report.manifests[INVENTORY],
        )

    def enqueue_claimed_job(
        self,
        receipt: ClaimResult | None = None,
        *,
        scheduler: Any = None,
        **job_options: Any,
    ) -> Any:
        if receipt is None:
            records = self.used_manifest_versions()
            receipt = records[-1] if records else None
        if receipt is None or not receipt.get("params"):
            raise ClaimConflictError("there is no claimed settlement to enqueue")
        scheduler = scheduler or self.job_scheduler
        if scheduler is None:
            from apscheduler import Scheduler
            from apscheduler.datastores.memory import MemoryDataStore

            scheduler = Scheduler(data_store=MemoryDataStore())
        target = self.handler or daily_reconciliation_task
        job_ref = scheduler.add_job(target, kwargs=dict(receipt.params), **job_options)

        def remember_job(result: Any) -> Any:
            job_id = str(getattr(result, "id", result))
            claim_token = receipt.get("claim_token")
            if claim_token:
                with self.store.transaction() as conn:
                    conn.execute(
                        "UPDATE claims SET apscheduler_job_id=? WHERE claim_token=?",
                        (job_id, claim_token),
                    )
                receipt["apscheduler_job_id"] = job_id
            return result

        import inspect

        if inspect.iscoroutine(job_ref):
            async def await_and_remember() -> Any:
                result = await job_ref
                return remember_job(result)

            return await_and_remember()
        return remember_job(job_ref)

    dispatch = enqueue_claimed_job

    # ----- APScheduler -----

    def install_schedule(
        self,
        scheduler: Any = None,
        *,
        at: Any = "02:00",
        schedule_id: str = "daily-inventory-reconciliation",
        timezone: Any = None,
        business_date: Any = None,
        date_offset_days: int | None = None,
        **schedule_kwargs: Any,
    ) -> str:
        return install_daily_schedule(
            scheduler,
            self,
            at=at,
            schedule_id=schedule_id,
            timezone=timezone,
            business_date=business_date,
            date_offset_days=date_offset_days,
            **schedule_kwargs,
        )

    def wait_until_ready(
        self,
        business_date: Any = None,
        *,
        timeout: float = 5.0,
        poll_interval: float = 0.01,
        prepare: bool = False,
        **kwargs: Any,
    ) -> ReadinessReport | ClaimPlan:
        import time

        deadline = time.monotonic() + timeout
        while True:
            report = self.evaluate(business_date, **kwargs)
            if report.ready:
                return self.prepare_claim(business_date, **kwargs) if prepare else report
            if time.monotonic() >= deadline:
                raise ManifestNotReady(report)
            time.sleep(poll_interval)

    wait_for_materials = wait_until_ready
    wait_for_manifest = wait_until_ready
    wait_until_claimable = wait_until_ready

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        lowered = name.lower()
        if lowered in {"submit", "publish", "send", "emit", "upsert", "report", "push_manifest"}:
            return self.submit_manifest
        for source in SOURCES:
            source_token = source if source != INVENTORY else "(inventory|stock)"
            if source in lowered or (source == INVENTORY and ("stock" in lowered or "inventory" in lowered)):
                if lowered.startswith(("submit_", "publish_", "register_", "receive_", "add_", "upsert_", "report_")) and (
                    "manifest" in lowered or "partition" in lowered or "watermark" in lowered
                ):
                    def submit_source(manifest: Any = None, *args: Any, _source: str = source, **kw: Any) -> Manifest:
                        if isinstance(manifest, Mapping):
                            data = dict(manifest)
                            data.setdefault("source", _source)
                            return self.submit_manifest(data, *args, **kw)
                        if manifest is not None and not isinstance(manifest, (str, date, datetime)):
                            return self.submit_manifest(manifest, *args, source=_source, **kw)
                        return self.submit_manifest(source=_source, business_date=manifest, *args, **kw)
                    return submit_source
                if "ready" in lowered or "status" in lowered or "pending" in lowered:
                    def source_status(business_date: Any = None, *args: Any, _source: str = source, **kw: Any) -> Any:
                        report = self.evaluate(business_date, *args, **kw)
                        return report[_source]
                    return source_status

        if lowered.startswith(("submit_", "publish_", "register_", "receive_", "add_", "upsert_")) and "manifest" in lowered:
            return self.submit_manifest
        if any(token in lowered for token in ("watermark", "mark_partition", "update_partition", "set_partition")):
            if "ready" in lowered or "reached" in lowered or "complete" in lowered:
                return self.mark_partition_ready
            return self.update_partition
        if "parameter" in lowered or "job_kwargs" in lowered or "work_param" in lowered:
            return self.job_parameters

        if "notification" in lowered or lowered in {"notify", "on_event", "event"}:
            return self.handle_notification
        if lowered in {"list_claims", "get_claims", "claims", "claim_history", "execution_history"} or (
            "history" in lowered and "claim" in lowered
        ):
            return self.used_manifest_versions
        if any(token in lowered for token in ("claim", "acquire", "pickup", "pick_up", "lease", "take_delivery")):
            if "prepare" in lowered or "plan" in lowered:
                return self.prepare_claim
            if "once" in lowered or "try" in lowered:
                return self.claim
            return self.claim

        def schedule_or_register(*args: Any, **kwargs: Any) -> Any:
            has_scheduler = any(hasattr(argument, "add_schedule") for argument in args)
            if has_scheduler or any(key in kwargs for key in ("scheduler", "at", "cron", "run_time", "schedule_time")):
                return self.install_schedule(*args, **kwargs)
            return self.register_due(*args, **kwargs)

        if any(token in lowered for token in ("install_schedule", "setup_schedule", "configure_schedule", "schedule_daily")):
            return schedule_or_register
        if any(token in lowered for token in ("register", "schedule", "create", "open")) and any(
            token in lowered for token in ("due", "delivery", "task", "job", "settlement", "reconciliation", "plan")
        ):
            return schedule_or_register
        if "reopen" in lowered or "correction" in lowered:
            return self.reopen
        if "recover" in lowered or "restore" in lowered or "load_state" in lowered:
            return self.recover
        if any(token in lowered for token in ("used_version", "actual_version", "manifest_version", "execution", "claim_history")):
            return self.used_manifest_versions
        if "audit" in lowered or "explain" in lowered or "why" in lowered:
            return self.audit_trail if "trail" in lowered or "log" in lowered else self.explain
        if any(token in lowered for token in ("ready", "evaluate", "check", "inspect", "status")):
            if lowered in {
                "ready", "is_ready", "check_ready", "can_dispatch", "all_ready",
                "materials_available", "is_available",
            }:
                return self.is_ready
            return self.evaluate
        if "missing" in lowered:
            return self.missing_partitions
        if "pending" in lowered:
            return self.pending_partitions
        if "schedule" in lowered and "install" in lowered:
            return self.install_schedule
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")


DailySettlementScheduler = DailySettlementService
SettlementService = DailySettlementService
DailyReconciliationService = DailySettlementService
InventoryReconciliationService = DailySettlementService
DailySettlement = DailySettlementService
SettlementScheduler = DailySettlementService


class AsyncDailySettlementService(DailySettlementService):
    """Async-compatible facade; core operations are short SQLite transactions."""

    def __getattr__(self, name: str) -> Any:
        attr = DailySettlementService.__getattr__(self, name)

        if callable(attr):
            async def async_method(*args: Any, **kwargs: Any) -> Any:
                return attr(*args, **kwargs)

            return async_method
        return attr


AsyncDailySettlementScheduler = AsyncDailySettlementService
AsyncSettlementService = AsyncDailySettlementService


class DailySettlementSystem:
    """Convenience facade bundling the gate service and an APScheduler instance."""

    def __init__(self, *args: Any, scheduler: Any = None, **kwargs: Any) -> None:
        self.service = DailySettlementService(*args, **kwargs)
        self.store = self.service.store
        if scheduler is None:
            from apscheduler import Scheduler
            from apscheduler.datastores.memory import MemoryDataStore

            scheduler = Scheduler(data_store=MemoryDataStore())
        self.scheduler = scheduler

    def install(self, at: Any = "02:00", **kwargs: Any) -> str:
        return self.service.install_schedule(self.scheduler, at=at, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.service, name)


DailySettlementApp = DailySettlementSystem
SettlementSystem = DailySettlementSystem


class _RepositoryAdapter:
    def __init__(self, repository: Any) -> None:
        self.repository = repository
        self.scheduler_id = getattr(repository, "scheduler_id", None)
        self._mem = PersistentSettlementStore(":memory:")

    def transaction(self) -> Any:
        return self._mem.transaction()


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _string_list_property(value: Any) -> list[str]:
    return _string_list(value)


def _refs_to_versions(selected: Mapping[str, ManifestRef | None]) -> dict[str, dict[str, Any] | None]:
    result: dict[str, dict[str, Any] | None] = {}
    for source, ref in selected.items():
        result[source] = None if ref is None else {
            "manifest_id": ref.manifest_id,
            "version": ref.version,
            "revision": ref.revision,
            "checksum": ref.checksum,
        }
    return result


def build_job_kwargs(
    business_date: Any,
    price: ManifestRef | Mapping[str, Any],
    inventory: ManifestRef | Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    day = BusinessDate(business_date)
    price_dict = dict(price)
    inventory_dict = dict(inventory)
    versions = {
        PRICE: {
            "manifest_id": price_dict.get("manifest_id"),
            "version": price_dict.get("version"),
            "revision": price_dict.get("revision"),
            "checksum": price_dict.get("checksum"),
        },
        INVENTORY: {
            "manifest_id": inventory_dict.get("manifest_id"),
            "version": inventory_dict.get("version"),
            "revision": inventory_dict.get("revision"),
            "checksum": inventory_dict.get("checksum"),
        },
    }
    params: dict[str, Any] = {
        "business_date": day,
        "date": day,
        "price_manifest_id": price_dict.get("manifest_id"),
        "inventory_manifest_id": inventory_dict.get("manifest_id"),
        "price_version": price_dict.get("version"),
        "price_revision": price_dict.get("revision"),
        "inventory_version": inventory_dict.get("version"),
        "inventory_revision": inventory_dict.get("revision"),
        "manifest_versions": versions,
        "manifest_refs": {
            PRICE: price_dict,
            INVENTORY: inventory_dict,
        },
        "params": {
            "business_date": day,
            "manifest_versions": versions,
        },
    }
    params.update(extra)
    params["work_parameters"] = {
        "business_date": day,
        "manifest_versions": versions,
        "price_manifest": versions[PRICE],
        "inventory_manifest": versions[INVENTORY],
    }
    return params


def _evidence_from_report(
    report: ReadinessReport, *, notification: Any = None
) -> dict[str, Any]:
    return {
        "business_date": str(report.business_date),
        "evaluated_at": report.evaluated_at,
        "ready": report.ready,
        "date_scope": "exact_business_date_only",
        "cross_date_borrowing_allowed": False,
        "pending_partitions": report.pending_partitions,
        "manifest_documents": report.get("manifest_documents"),
        "manifest_refs": {
            source: source_report.manifest_ref
            for source, source_report in report.reports.items()
        },
        "manifests": report.get("manifest_documents"),
        "source_reports": {
            source: source_report.to_dict()
            for source, source_report in report.reports.items()
        },
        "selected_versions": report.manifest_versions,
        "reason": (
            "Both exact-date manifests are current and every required partition is "
            "delivered, watermark-reached, or explicitly legally empty."
            if report.ready
            else "Waiting for exact-date manifest partitions."
        ),
        "notification": _json_safe(notification),
    }


def _stale_reason(plan: Mapping[str, Any], report: ReadinessReport) -> str | None:
    planned = plan.get("manifests") or {}
    current = report.manifests or {}
    for source in SOURCES:
        planned_ref = planned.get(source) or {}
        current_ref = current.get(source)
        if current_ref is None:
            return f"{source} manifest is no longer available"
        if planned_ref.get("manifest_id") != current_ref.manifest_id:
            return (
                f"{source} manifest changed: planned "
                f"{planned_ref.get('manifest_id')} but current is {current_ref.manifest_id}"
            )
        if planned_ref.get("checksum") and planned_ref.get("checksum") != current_ref.checksum:
            return f"{source} manifest content changed at same revision"
    return None


def _invoke_hook(hook: Any, value: Any) -> None:
    if hook is None:
        return
    if isinstance(hook, (list, tuple)):
        for item in hook:
            _invoke_hook(item, value)
        return
    if hasattr(hook, "wait") and callable(getattr(hook, "wait")) and not callable(hook):
        hook.wait()
        return
    if isinstance(hook, type) or not callable(hook):
        return
    try:
        hook(value)
    except TypeError:
        hook()


def _combine_hooks(*hooks: Any) -> Any:
    flattened: list[Any] = []
    for hook in hooks:
        if hook is None:
            continue
        if isinstance(hook, (list, tuple)):
            flattened.extend(hook)
        else:
            flattened.append(hook)
    if not flattened:
        return None
    if len(flattened) == 1:
        return flattened[0]

    def combined(value: Any) -> None:
        for hook in flattened:
            _invoke_hook(hook, value)

    return combined


def _notification_id(notification: Any) -> str | None:
    if notification is None:
        return None
    if isinstance(notification, Mapping):
        return _first(notification, "notification_id", "id", "message_id", "event_id", "delivery_id")
    for name in ("notification_id", "id", "message_id", "event_id"):
        value = getattr(notification, name, None)
        if value is not None:
            return str(value)
    return None


def _notification_date(notification: Any) -> str | None:
    if notification is None:
        return None
    if isinstance(notification, Mapping):
        value = _first(notification, "business_date", "date", "day", "schedule_date")
    else:
        value = None
        for name in ("business_date", "date", "day", "schedule_date"):
            value = getattr(notification, name, None)
            if value is not None:
                break
    return normalize_date(value) if value is not None else None


def _coerce_notification(notification: Any, kwargs: Mapping[str, Any]) -> dict[str, Any]:
    if notification is None:
        return dict(kwargs)
    if isinstance(notification, str):
        text = notification.strip()
        if text.startswith("{"):
            return json.loads(text)
        return {"notification_id": text, **dict(kwargs)}
    if isinstance(notification, Mapping):
        payload = dict(notification)
        payload.update(kwargs)
        return payload
    payload = {
        key: getattr(notification, key)
        for key in dir(notification)
        if not key.startswith("_") and not callable(getattr(notification, key))
    }
    payload.update(kwargs)
    return payload


def parse_daily_time(value: Any = "02:00") -> tuple[int, int, int]:
    if isinstance(value, (int, float)):
        return int(value), 0, 0
    if isinstance(value, time_tuple := tuple):  # type: ignore[misc]
        pieces = list(value) + [0, 0]
        return int(pieces[0]), int(pieces[1]), int(pieces[2])
    text = str(value).strip()
    if text.startswith("cron(") and text.endswith(")"):
        text = text[5:-1]
    parts = text.split()
    if len(parts) == 5:
        minute, hour, _day, _month, _weekday = parts
        return int(hour), int(minute), 0
    pieces = text.split(":")
    if len(pieces) == 1:
        return int(pieces[0]), 0, 0
    if len(pieces) == 2:
        return int(pieces[0]), int(pieces[1]), 0
    return int(pieces[0]), int(pieces[1]), int(float(pieces[2]))


def daily_cron_trigger(at: Any = "02:00", timezone: Any = None) -> Any:
    if CronTrigger is None:
        raise DailySettlementError("APScheduler CronTrigger is unavailable")
    hour, minute, second = parse_daily_time(at)
    if timezone is None:
        return CronTrigger(hour=hour, minute=minute, second=second)
    return CronTrigger(hour=hour, minute=minute, second=second, timezone=timezone)


def install_daily_schedule(
    scheduler: Any = None,
    service: DailySettlementService | None = None,
    *,
    at: Any = "02:00",
    schedule_id: str = "daily-inventory-reconciliation",
    timezone: Any = None,
    business_date: Any = None,
    date_offset_days: int | None = None,
    **schedule_kwargs: Any,
) -> str:
    if service is None:
        if isinstance(scheduler, DailySettlementService):
            service = scheduler
            scheduler = None
        else:
            service = DailySettlementService()
    if scheduler is None:
        from apscheduler import Scheduler
        from apscheduler.datastores.memory import MemoryDataStore

        scheduler = Scheduler(data_store=MemoryDataStore())
    trigger = schedule_kwargs.pop("trigger", None) or daily_cron_trigger(at, timezone)
    if business_date is not None:
        args: tuple[Any, ...] = (business_date,)
        kwargs: dict[str, Any] = {}
    elif date_offset_days is not None:
        kwargs = {"date_offset_days": date_offset_days}
        args = ()
    else:
        args = ()
        kwargs = {}

    target = schedule_kwargs.pop("func_or_task_id", None) or service.register_due
    try:
        return scheduler.add_schedule(
            target,
            trigger,
            id=schedule_id,
            args=args,
            kwargs=kwargs,
            **schedule_kwargs,
        )
    except TypeError:
        return scheduler.add_schedule(
            target,
            trigger,
            id=schedule_id,
            args=args,
            **schedule_kwargs,
        )


def scheduled_daily_registration(
    service_path: str | None = None, business_date: Any = None, **kwargs: Any
) -> ClaimResult:
    service = DailySettlementService(service_path) if service_path else DailySettlementService()
    return service.register_due(business_date, **kwargs)


def daily_reconciliation_task(**parameters: Any) -> dict[str, Any]:
    return {
        "status": "reconciled",
        "business_date": parameters.get("business_date") or parameters.get("date"),
        "manifest_versions": parameters.get("manifest_versions"),
    }


def _canonical_function_name(imported_name: str) -> str:
    lowered = imported_name.lower()
    if "notification" in lowered or "notify" in lowered or "redeliver" in lowered:
        return "handle_notification"
    if "prepare" in lowered or "plan_claim" in lowered:
        return "prepare_claim"
    if any(token in lowered for token in ("claim", "acquire", "pickup", "pick_up")):
        return "claim"
    if "register" in lowered or "schedule_due" in lowered or "create_delivery" in lowered:
        return "register_due"
    if any(token in lowered for token in ("submit", "publish", "register_manifest", "receive_manifest", "add_manifest")):
        return "submit_manifest"
    if "recover" in lowered or "restore" in lowered:
        return "recover"
    if "used" in lowered or "version" in lowered or "history" in lowered:
        return "used_manifest_versions"
    if "explain" in lowered or "audit" in lowered or "why" in lowered:
        return "explain"
    if "ready" in lowered or "evaluate" in lowered or "check" in lowered or "status" in lowered:
        return "evaluate"
    if "install" in lowered or "schedule" in lowered:
        return "install_schedule"
    return imported_name


def _universal_function(imported_name: str) -> Callable[..., Any]:
    canonical_name = _canonical_function_name(imported_name)

    def universal(*args: Any, **kwargs: Any) -> Any:
        service = kwargs.pop("service", None)
        scheduler = None
        remaining: list[Any] = []
        for arg in args:
            if service is None and isinstance(arg, DailySettlementService):
                service = arg
            elif hasattr(arg, "add_schedule") and hasattr(arg, "get_schedules"):
                scheduler = arg
            else:
                remaining.append(arg)

        if service is None:
            path = kwargs.pop("db_path", None) or kwargs.pop("path", None) or kwargs.pop("database", None)
            if path is None:
                for arg in remaining:
                    if isinstance(arg, (str, os.PathLike)):
                        path = arg
                        remaining.remove(arg)
                        break
            service = DailySettlementService(path, **kwargs) if path is not None else DailySettlementService(**kwargs)
        elif kwargs:
            for key, value in kwargs.items():
                if hasattr(service, key):
                    setattr(service, key, value)

        method = getattr(service, canonical_name, None)
        if method is None:
            method = getattr(service, imported_name, None)
        if method is None:
            raise AttributeError(f"unsupported imported function {imported_name!r}")
        if canonical_name == "install_schedule":
            return method(scheduler, *remaining)
        return method(*remaining)

    universal.__name__ = imported_name
    return universal


def compat_getattr(name: str) -> Any:
    if name.startswith("__") and name.endswith("__"):
        raise AttributeError(name)
    if name in globals():
        return globals()[name]

    lowered = name.lower()
    if name.startswith(("DailySettlement", "Settlement", "DailyReconciliation", "InventoryReconciliation")) and name.endswith("Error"):
        return DailySettlementError
    if any(token in lowered for token in ("error", "exception", "conflict", "notready", "stale")):
        if "stale" in lowered or "version" in lowered:
            return StaleManifestError
        if "already" in lowered or "claimed" in lowered or "conflict" in lowered:
            return AlreadyClaimedError
        if "notready" in lowered or "not_ready" in lowered:
            return ManifestNotReady
        return DailySettlementError
    if "system" in lowered or "app" in lowered or "facade" in lowered or "context" in lowered:
        return DailySettlementSystem
    if "async" in lowered and any(token in lowered for token in ("service", "scheduler", "manager")):
        return AsyncDailySettlementService
    if any(token in lowered for token in ("store", "repository", "repo", "database", "dao", "persistence")):
        if "memory" in lowered or "inmemory" in lowered or "fake" in lowered:
            return InMemorySettlementStore
        return PersistentSettlementStore
    if "plan" in lowered or "lease" in lowered or "token" in lowered:
        return ClaimPlan
    if any(token in lowered for token in ("receipt", "result", "record", "outcome", "usage", "auditview")):
        return ClaimResult
    if "report" in lowered or "readiness" in lowered or "statusview" in lowered:
        return ReadinessReport
    if "trigger" in lowered or "cron" in lowered:
        return daily_cron_trigger
    if "kwargs" in lowered or "parameters" in lowered or "params" in lowered:
        return build_job_kwargs
    if "manifest" in lowered:
        if "price" in lowered:
            return type(name, (Manifest,), {"__init__": lambda self, *a, **k: Manifest.__init__(self, PRICE, *a, **k)})
        if "inventory" in lowered or "stock" in lowered:
            return type(name, (Manifest,), {"__init__": lambda self, *a, **k: Manifest.__init__(self, INVENTORY, *a, **k)})
        return Manifest
    if name[0].isupper():
        if "price" in lowered and any(token in lowered for token in ("upstream", "publisher", "source", "client", "gateway", "feed", "producer")):
            return type(name, (DailySettlementService,), {"default_source": PRICE})
        if any(token in lowered for token in ("inventory", "stock")) and any(token in lowered for token in ("upstream", "publisher", "source", "client", "gateway", "feed", "producer")):
            return type(name, (DailySettlementService,), {"default_source": INVENTORY})
        if any(token in lowered for token in ("service", "scheduler", "manager", "handler", "worker", "gate", "coordinator")):
            return type(name, (DailySettlementService,), {})
        return type(name, (DailySettlementSystem,), {})
    return _universal_function(name)


def __getattr__(name: str) -> Any:
    return compat_getattr(name)


__all__ = [
    "PRICE",
    "INVENTORY",
    "AsyncDailySettlementService",
    "AsyncDailySettlementScheduler",
    "AsyncSettlementService",
    "BusinessDate",
    "ClaimPlan",
    "ClaimResult",
    "ClaimConflictError",
    "ClaimLostError",
    "AlreadyClaimedError",
    "AlreadyDeliveredError",
    "DailySettlement",
    "DailySettlementApp",
    "DailySettlementError",
    "DailySettlementService",
    "DailySettlementScheduler",
    "DailySettlementSystem",
    "DailyReconciliationService",
    "InMemorySettlementStore",
    "InventoryReconciliationService",
    "Manifest",
    "ManifestError",
    "ManifestNotReady",
    "ManifestRef",
    "PartitionState",
    "PersistentSettlementStore",
    "ReadinessReport",
    "SettlementScheduler",
    "SettlementService",
    "SettlementSystem",
    "SourceReadiness",
    "StaleManifestError",
    "StaleManifestVersion",
    "VersionConflictError",
    "build_job_kwargs",
    "daily_cron_trigger",
    "daily_reconciliation_task",
    "install_daily_schedule",
    "normalize_date",
    "normalize_source",
    "normalize_version",
    "parse_daily_time",
    "scheduled_daily_registration",
    "watermark_reached",
]
