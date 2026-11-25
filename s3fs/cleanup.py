"""Coordinated, replay-safe removal of S3 object versions.

A replay ledger records, for each replay batch, which concrete S3 object
versions may be read.  Cleanup is different from ordinary ``rm``: removing
one physical version can make several batches unreplayable, while a new
upload to the same key must never be mistaken for the version a batch uses.

:class:`VersionCleanup` keeps that decision state per filing process.  It is
deliberately conservative:

* Every target is qualified by bucket, key and version id.  A bare key is
  never accepted, so the current version or a newly written replacement can
  never be removed by accident.
* A target is reserved before the S3 delete is attempted.  Other work orders
  for the same version observe ``pending`` and do not delete it again.
* A successful delete is only reported as final while the ledger still
  references that version.  Otherwise the physical object is gone, but the
  batch's replay dependency remains explicit in the receipt.
* An ambiguous outcome (timeout, connection loss, mixed batch response)
  leaves the target ``pending``.  A status lookup re-checks it; unrelated
  targets in the same work order are unaffected.
"""

from __future__ import annotations

import asyncio
import collections
import json
import os
import threading
import time
from dataclasses import dataclass, field


TARGET_STATES = ("available", "pending", "deleted", "rejected")
_STATE_AVAILABLE = "available"
_STATE_PENDING = "pending"
_STATE_DELETED = "deleted"
_STATE_REJECTED = "rejected"


def _qualify(bucket, key, version_id):
    if not bucket:
        raise ValueError("cleanup targets require a bucket")
    if not key:
        raise ValueError("cleanup targets require an object key")
    if not version_id:
        raise ValueError("cleanup targets require an explicit VersionId")
    return (str(bucket), str(key), str(version_id))


def _target_text(target):
    return f"{target[0]}/{target[1]}?versionId={target[2]}"


def _target_dict(target):
    return {
        "bucket": target[0],
        "key": target[1],
        "version_id": target[2],
        "versionId": target[2],
    }


def _from_path(path):
    text = str(path)
    for prefix in ("s3://", "s3a://"):
        if text.startswith(prefix):
            text = text[len(prefix) :]
            break
    bucket, sep, rest = text.partition("/")
    if not sep:
        raise ValueError(f"cleanup target is missing a key: {path!r}")
    path_part, mark, query = rest.partition("?")
    version_id = None
    if mark:
        for item in query.split("&"):
            name, equals, value = item.partition("=")
            if equals and name == "versionId":
                version_id = value
                break
    if not version_id:
        raise ValueError(
            f"cleanup target must include an explicit VersionId "
            f"(?versionId=...); refusing to remove the current version of "
            f"{bucket}/{path_part}"
        )
    return _qualify(bucket, path_part, version_id)


def _version_value(mapping):
    return (
        mapping.get("versionId")
        or mapping.get("version_id")
        or mapping.get("version")
        or mapping.get("VersionId")
    )


def _target_list(value):
    """Normalise a collection of target descriptions.

    Accepted forms include version-qualified paths, ``(bucket, key,
    versionId)`` tuples, target dictionaries, and mappings of path/object to
    version id.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, tuple):
        if len(value) == 3 and all(isinstance(item, str) for item in value):
            return [value]
        return [item for item in value]
    if isinstance(value, dict):
        if _is_target_dict(value):
            return [value]
        expanded = []
        for path, versions in value.items():
            if isinstance(versions, (list, tuple, set)):
                expanded.extend(
                    {"path": path, "version_id": version} for version in versions
                )
            else:
                expanded.append({"path": path, "version_id": versions})
        return expanded
    if isinstance(value, (list, set)):
        expanded = []
        for item in value:
            expanded.extend(_target_list(item))
        return expanded
    return [value]


def _is_target_dict(value):
    if not isinstance(value, dict):
        return False
    if _version_value(value):
        return True
    keys = set(value)
    return bool(keys & {"path", "uri", "s3", "object"}) or (
        "bucket" in keys and "key" in keys
    )


def _split_bucket_key(path):
    text = str(path)
    for prefix in ("s3://", "s3a://"):
        if text.startswith(prefix):
            text = text[len(prefix) :]
            break
    bucket, sep, key = text.partition("/")
    if not sep:
        raise ValueError(f"cleanup target is missing a key: {path!r}")
    return bucket, key.partition("?")[0]


def _canonical(path):
    """Return ``(bucket, key, version_id)`` from a version-qualified target."""
    if isinstance(path, dict):
        if "path" in path or "uri" in path or "s3" in path or "object" in path:
            source = path.get(
                "path", path.get("uri", path.get("s3", path.get("object")))
            )
            version_id = _version_value(path)
            if version_id:
                bucket, key = _split_bucket_key(source)
                return _qualify(bucket, key, version_id)
            return _canonical(source)
        version_id = _version_value(path)
        key = path.get("key")
        bucket = path.get("bucket")
        if bucket and "/" in str(bucket) and not key:
            bucket, key = _split_bucket_key(bucket)
        if not bucket and key and "/" in str(key):
            bucket, key = _split_bucket_key(key)
        return _qualify(bucket, key, version_id)
    if isinstance(path, (tuple, list)):
        if len(path) != 3:
            raise ValueError("target tuples must be (bucket, key, versionId)")
        return _qualify(*path)
    return _from_path(path)


def parse_target(path):
    """Parse a version-qualified S3 target into its three components."""
    return _canonical(path)


def _read_json(source):
    if isinstance(source, (str, os.PathLike)):
        if os.path.isfile(source):
            with open(source, encoding="utf8") as fh:
                text = fh.read()
        elif source.lstrip().startswith(("{", "[")):
            text = source
        else:
            return source
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            rows = [json.loads(line) for line in text.splitlines() if line.strip()]
            return rows
    return source


@dataclass
class _BatchState:
    name: str
    copies: tuple
    line: object = None


@dataclass
class _Attempt:
    line: object
    order: object
    reason: str
    started: float = field(default_factory=time.time)
    readable: bool | None = None
    error: str | None = None

    def as_dict(self):
        out = {
            "line": self.line,
            "work_order": self.order,
            "reason": self.reason,
            "started": self.started,
        }
        if self.readable is not None:
            out["readable"] = self.readable
        if self.error:
            out["error"] = self.error
        return out


@dataclass
class _TargetState:
    state: str = _STATE_AVAILABLE
    readable: bool | None = None
    reason: str | None = None
    error: str | None = None
    attempts: int = 0
    attempt: _Attempt | None = None


def _line_key(line):
    try:
        hash(line)
    except TypeError:
        return json.dumps(line, sort_keys=True, default=str)
    return line


def _consume_result(task):
    """Retrieve a background task's result so it is never reported as leaked."""
    try:
        task.result()
    except BaseException:
        pass


class CleanupReport(list):
    """A receipt list with convenient grouped views.

    It behaves like the list of per-target receipts, while supporting
    ``report["receipts"]``, ``report["lines"]`` and ``report["summary"]`` for
    callers that prefer an explicitly grouped result.
    """

    def __init__(self, receipts, orders=()):
        super().__init__(receipts)
        self.orders = list(orders)

    @property
    def receipts(self):
        return list(self)

    @property
    def lines(self):
        grouped = collections.OrderedDict()
        by_key = {}
        for receipt in self:
            by_key[(_line_key(receipt["line"]), receipt["target"])] = receipt
        for order in self.orders:
            key = _line_key(order["line"])
            ordered = []
            for target in order["targets"]:
                receipt = by_key.get((key, _target_text(target)))
                if receipt is not None:
                    ordered.append(receipt)
            if ordered:
                grouped[key] = ordered
        for receipt in self:
            key = _line_key(receipt["line"])
            if key not in grouped:
                grouped[key] = [item for item in self if _line_key(item["line"]) == key]
        return grouped

    @property
    def summary(self):
        return dict(collections.Counter(item["status"] for item in self))

    @property
    def by_line(self):
        return self.lines

    @property
    def by_target(self):
        return {item["target"]: item for item in self}

    @property
    def removed(self):
        return [item for item in self if item["status"] == _STATE_DELETED]

    @property
    def pending(self):
        return [item for item in self if item["status"] == _STATE_PENDING]

    def for_line(self, line):
        return self.lines.get(_line_key(line), [])

    def for_target(self, target):
        return self.by_target.get(_target_text(_canonical(target)))

    def __getitem__(self, key):
        if isinstance(key, str):
            if key in ("receipts", "results", "items"):
                return self.receipts
            if key in ("lines", "by_line"):
                return self.lines
            if key in ("targets", "by_target", "by_targets"):
                return self.by_target
            if key == "summary":
                return self.summary
            if key in ("removed", "deleted"):
                return self.removed
            if key == "pending":
                return self.pending
            if key in self.lines:
                return self.lines[key]
            by_target = self.by_target
            if key in by_target:
                return by_target[key]
        return super().__getitem__(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except (KeyError, IndexError, TypeError):
            return default

    def keys(self):
        return ("receipts", "lines", "summary")


class CleanupStatus(list):
    """List of status records with lookup by target string or components."""

    def __init__(self, records, coverage=None, orders=()):
        super().__init__(records)
        self.coverage = coverage or {}
        self.orders = list(orders)

    def by_target(self):
        return {record["target"]: record for record in self}

    @property
    def targets(self):
        return self.by_target()

    @property
    def lines(self):
        grouped = collections.OrderedDict()
        for order in self.orders:
            grouped.setdefault(_line_key(order["line"]), [])
        for record in self:
            for line in record.get("lines", []):
                grouped.setdefault(_line_key(line), []).append(record)
        return grouped

    @property
    def summary(self):
        return dict(collections.Counter(record["status"] for record in self))

    @property
    def coverage_ok(self):
        return all(entry["has_copy"] for entry in self.coverage.values())

    def get_for(self, target):
        target = _target_text(_canonical(target))
        return self.by_target().get(target)

    def __getitem__(self, key):
        if isinstance(key, str):
            if key in ("targets", "records", "items"):
                return list(self)
            if key == "coverage":
                return self.coverage
            if key in ("lines", "by_line"):
                return self.lines
            if key == "summary":
                return self.summary
            if key == "coverage_ok":
                return self.coverage_ok
            record = self.get_for(key)
            if record is not None:
                return record
            if key in self.lines:
                return self.lines[key]
        return super().__getitem__(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except (KeyError, IndexError, TypeError):
            return default


class VersionCleanup:
    """Replay-aware cleanup state for one :class:`S3FileSystem`."""

    def __init__(self, fs):
        self.fs = fs
        self._lock = threading.RLock()
        self._plan_lock = None
        self._plan_pid = None
        self._batches: dict[str, _BatchState] = {}
        self._targets: dict[tuple, _TargetState] = {}
        self._ledger_lock = threading.RLock()
        self._last_ledger = None
        self._last_orders = []

    @property
    def plan_lock(self):
        """Serialise decisions that reserve targets or verify alternatives.

        Two concurrent work orders must not each treat the other's target as
        the remaining readable copy and then both delete.  The lock is bound
        to the running event loop because a synchronous filesystem may use
        one loop for its lifetime while tests create fresh loops.
        """
        loop = asyncio.get_running_loop()
        if self._plan_lock is None or self._plan_pid != id(loop):
            self._plan_lock = asyncio.Lock()
            self._plan_pid = id(loop)
        return self._plan_lock

    # ------------------------------------------------------------------
    # ledger and work-order normalisation
    # ------------------------------------------------------------------
    @staticmethod
    def _ledger_entries(ledger):
        ledger = _read_json(ledger)
        if ledger is None:
            raise ValueError("a replay ledger is required")
        if isinstance(ledger, dict):
            if "batches" in ledger:
                batches = ledger["batches"]
            elif "ledger" in ledger:
                batches = ledger["ledger"]
            elif all(
                isinstance(value, (list, tuple, set, dict, str))
                for value in ledger.values()
            ):
                batches = [
                    {"name": key, "copies": value} for key, value in ledger.items()
                ]
            else:
                raise ValueError("unrecognised replay ledger")
        else:
            batches = ledger

        if isinstance(batches, dict):
            batches = [{"name": key, "copies": value} for key, value in batches.items()]

        entries = []
        for index, entry in enumerate(batches):
            if isinstance(entry, dict):
                name = (
                    entry.get("name")
                    or entry.get("batch")
                    or entry.get("batch_id")
                    or entry.get("id")
                    or entry.get("key")
                    or entry.get("批次")
                )
                copies = (
                    entry.get("copies")
                    or entry.get("objects")
                    or entry.get("versions")
                    or entry.get("targets")
                    or entry.get("files")
                    or entry.get("副本")
                    or []
                )
                line = entry.get("line", entry.get("source_line", entry.get("行")))
            elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                name, copies = entry
                line = None
            else:
                name = getattr(entry, "name", None) or getattr(entry, "batch", None)
                copies = (
                    getattr(entry, "copies", None)
                    or getattr(entry, "objects", None)
                    or getattr(entry, "versions", None)
                )
                line = getattr(entry, "line", getattr(entry, "source_line", None))
            if not name:
                raise ValueError(f"batch entry has no name: {entry!r}")
            copies = _target_list(copies)
            entries.append(
                _BatchState(str(name), tuple(_canonical(copy) for copy in copies), line)
            )
        if not entries:
            raise ValueError("the replay ledger contains no batches")
        return entries

    @staticmethod
    def _normalise_work_orders(work_orders):
        work_orders = _read_json(work_orders)
        if work_orders is None:
            raise ValueError("at least one work order is required")
        if isinstance(work_orders, dict):
            if "work_orders" in work_orders:
                work_orders = work_orders["work_orders"]
            elif "targets" in work_orders or "versions" in work_orders:
                work_orders = [work_orders]
            elif all(
                isinstance(value, (list, tuple, set, dict, str))
                for value in work_orders.values()
            ):
                work_orders = [
                    {"line": line, "targets": targets}
                    for line, targets in work_orders.items()
                ]
            else:
                raise ValueError("unrecognised work order collection")
        elif isinstance(work_orders, (str, tuple)):
            work_orders = [work_orders]

        orders = []
        for index, order in enumerate(work_orders):
            if isinstance(order, dict):
                line = order.get(
                    "line", order.get("source_line", order.get("行", index))
                )
                order_id = order.get(
                    "work_order",
                    order.get(
                        "workOrder",
                        order.get("id", order.get("ticket", order.get("工单"))),
                    ),
                )
                targets = (
                    order.get("targets")
                    or order.get("versions")
                    or order.get("objects")
                    or order.get("remove")
                    or order.get("paths")
                    or order.get("target")
                    or order.get("版本")
                    or order.get("移除")
                    or []
                )
            else:
                line = index
                order_id = None
                targets = order
            orders.append(
                {
                    "line": line,
                    "order": order_id,
                    "targets": [_canonical(target) for target in _target_list(targets)],
                }
            )
        if not orders:
            raise ValueError("at least one work order is required")
        return orders

    def _install_ledger(self, entries, merge=False):
        with self._ledger_lock:
            if not merge:
                self._batches.clear()
            for entry in entries:
                self._batches[entry.name] = entry

    # ------------------------------------------------------------------
    # availability and replay coverage
    # ------------------------------------------------------------------
    def _protecting(self, target):
        return {name for name, entry in self._batches.items() if target in entry.copies}

    def _state(self, target):
        return self._targets.get(target)

    def _touch(self, target):
        state = self._targets.get(target)
        if state is None:
            state = _TargetState()
            self._targets[target] = state
        return state

    def _is_available(self, target):
        state = self._state(target)
        if state is None or state.state == _STATE_AVAILABLE:
            return True
        if state.state == _STATE_DELETED:
            return False
        if state.state == _STATE_PENDING:
            return False
        if state.state == _STATE_REJECTED:
            return state.readable is True
        return False

    def _blocking_batches(self, target):
        blockers = []
        for batch in self._batches.values():
            if target not in batch.copies:
                continue
            if not any(
                self._is_available(copy) for copy in batch.copies if copy != target
            ):
                blockers.append(batch.name)
        return blockers

    def _blocking_waits(self, target):
        waits = []
        for batch in self._batches.values():
            if target not in batch.copies:
                continue
            missing = [
                copy
                for copy in batch.copies
                if copy != target and not self._is_available(copy)
            ]
            if missing:
                for copy in missing:
                    state = self._state(copy)
                    waits.append(
                        {
                            "batch": batch.name,
                            "target": _target_text(copy),
                            "status": state.state if state else _STATE_AVAILABLE,
                            "reason": _state_reason(state),
                        }
                    )
        return waits

    def _guards(self, target):
        """Batches for which this exact version is now the only readable copy."""
        if not self._is_available(target):
            return []
        guards = []
        for batch in self._batches.values():
            if target not in batch.copies:
                continue
            if not any(
                other != target and self._is_available(other) for other in batch.copies
            ):
                guards.append(batch.name)
        return guards

    def _coverage(self):
        answer = {}
        for name, batch in self._batches.items():
            available = [
                _target_text(copy) for copy in batch.copies if self._is_available(copy)
            ]
            answer[name] = {
                "line": batch.line,
                "registered": [_target_text(copy) for copy in batch.copies],
                "available": available,
                "has_copy": bool(available),
                "missing": [
                    _target_text(copy)
                    for copy in batch.copies
                    if not self._is_available(copy)
                ],
            }
        return answer

    # ------------------------------------------------------------------
    # receipts
    # ------------------------------------------------------------------
    def _readable(self, target):
        """Best-known direct readability of the exact version.

        ``None`` means it has not been checked.  ``available`` is different:
        a version may be readable yet unavailable for replay because an
        unconfirmed delete may still remove it.
        """
        state = self._state(target)
        if state is None:
            return None
        if state.readable is not None:
            return state.readable
        if state.state == _STATE_AVAILABLE:
            return True
        if state.state == _STATE_DELETED:
            return False
        return None

    def _receipt(
        self,
        line,
        order,
        target,
        status,
        action,
        reason=None,
        error=None,
        readable=None,
    ):
        state = self._state(target)
        if readable is None:
            readable = self._readable(target)
        removed = status == _STATE_DELETED
        entry = {
            "line": line,
            "work_order": order,
            "target": _target_text(target),
            **_target_dict(target),
            "status": status,
            "state": status,
            "action": action,
            "removed": removed,
            "deleted": removed,
            "readable": readable,
            "available": self._is_available(target),
            "protected_batches": sorted(self._protecting(target)),
            "protects": sorted(self._protecting(target)),
            "protecting": sorted(self._protecting(target)),
            "guards": sorted(self._guards(target)),
            "requires_confirmation": status == _STATE_PENDING,
        }
        if reason:
            entry["reason"] = reason
        if error:
            entry["error"] = error
        waits = self._blocking_waits(target)
        if waits:
            entry["waiting_for"] = waits
        if state and state.attempt and state.state == _STATE_PENDING:
            entry["pending_attempt"] = state.attempt.as_dict()
        if state and state.state == _STATE_REJECTED:
            entry["retryable"] = True
        return entry

    # ------------------------------------------------------------------
    # deletions
    # ------------------------------------------------------------------
    async def _delete_one(self, target):
        bucket, key, version_id = target
        response = await self.fs._call_s3(
            "delete_object", Bucket=bucket, Key=key, VersionId=version_id
        )
        error = _delete_errors(response, target)
        if error is not None:
            exception = PermissionError(
                error.get("Message") or error.get("Code") or "delete rejected"
            )
            exception.response = {"Error": error}
            raise exception
        return response

    async def _check_one(self, target):
        bucket, key, version_id = target
        try:
            await self.fs._call_s3(
                "head_object", Bucket=bucket, Key=key, VersionId=version_id
            )
        except Exception as error:
            if _is_missing(error):
                return False
            raise
        return True

    async def _verify_alternatives(self, target):
        """Check that every affected batch has another readable copy.

        Only exact-version reads count.  A different version at the same key
        (for example a supplier's fresh upload) is not a replay copy of the
        registered version.
        """
        alternatives = {}
        for batch in self._batches.values():
            if target not in batch.copies:
                continue
            candidates = [
                copy
                for copy in batch.copies
                if copy != target and self._is_available(copy)
            ]
            alternatives[batch.name] = candidates

        to_check = []
        for candidates in alternatives.values():
            for candidate in candidates:
                if candidate not in to_check:
                    to_check.append(candidate)

        verified = {}
        if to_check:
            results = await asyncio.gather(
                *(self._check_one(candidate) for candidate in to_check),
                return_exceptions=True,
            )
            for candidate, exists in zip(to_check, results):
                if isinstance(exists, BaseException):
                    verified[candidate] = None
                else:
                    verified[candidate] = exists
                if verified[candidate] is False:
                    with self._lock:
                        state = self._targets.setdefault(candidate, _TargetState())
                        if state.state == _STATE_AVAILABLE:
                            state.state = _STATE_DELETED
                            state.readable = False

        unverified = []
        for batch, candidates in alternatives.items():
            if not any(verified.get(candidate) is True for candidate in candidates):
                unverified.append(batch)
        return unverified

    def _invalidate(self, target):
        invalidate = getattr(self.fs, "invalidate_cache", None)
        if invalidate is not None:
            bucket, key, _ = target
            invalidate(f"{bucket}/{key}")

    def _reserve(self, target, attempt):
        state = self._targets.setdefault(target, _TargetState())
        if state.state == _STATE_DELETED:
            return "already-deleted", None
        if state.state == _STATE_PENDING:
            return "waiting", state
        blockers = self._blocking_batches(target)
        if blockers:
            return "blocked", blockers
        if state.state == _STATE_REJECTED:
            if state.readable is not True:
                return "blocked", [
                    {
                        "batch": name,
                        "target": _target_text(target),
                        "status": _STATE_REJECTED,
                        "reason": "the rejected version has not been verified readable",
                    }
                    for name in sorted(self._protecting(target))
                ]
        state.state = _STATE_PENDING
        state.readable = None
        state.reason = "a delete for this exact version is in flight"
        state.error = None
        state.attempt = attempt
        state.attempts += 1
        return "reserved", state

    async def _handle_target(self, line, order, target):
        attempt = _Attempt(
            line,
            order,
            "a delete for this exact version is in flight",
        )
        # Reserve and verify under the plan lock.  Receipt rendering happens
        # afterwards so a slow or disconnected delete does not block other
        # work orders indefinitely.
        async with self.plan_lock:
            with self._lock:
                outcome, detail = self._reserve(target, attempt)
            if outcome == "reserved":
                try:
                    unverified = await self._verify_alternatives(target)
                except Exception as error:
                    with self._lock:
                        state = self._targets[target]
                        state.state = _STATE_AVAILABLE
                        state.reason = None
                        state.error = f"{type(error).__name__}: {error}"
                        state.attempt = None
                    return self._receipt(
                        line,
                        order,
                        target,
                        "blocked",
                        "not-attempted",
                        reason="could not verify the other registered copies for replay",
                        error=state.error,
                        readable=self._is_available(target),
                    ) | {"blocked_batches": sorted(self._protecting(target))}
                if unverified:
                    with self._lock:
                        state = self._targets[target]
                        state.state = _STATE_AVAILABLE
                        state.reason = None
                        state.error = None
                        state.attempt = None
                    return self._receipt(
                        line,
                        order,
                        target,
                        "blocked",
                        "not-attempted",
                        reason="removing this version would leave a replay batch "
                        "without a registered readable copy",
                        readable=self._is_available(target),
                    ) | {
                        "blocked_batches": sorted(unverified),
                        "waiting_for": self._blocking_waits(target),
                    }

        if outcome == "already-deleted":
            return self._receipt(line, order, target, _STATE_DELETED, "already-deleted")
        if outcome == "waiting":
            state = detail
            pending = state.attempt.as_dict() if state.attempt else None
            return self._receipt(
                line,
                order,
                target,
                _STATE_PENDING,
                "waiting",
                reason="a delete for this exact version is already in flight; "
                "not issuing another delete",
                readable=self._readable(target),
            ) | {"pending_attempt": pending}
        if outcome == "rejected":
            state = detail
            return self._receipt(
                line,
                order,
                target,
                _STATE_REJECTED,
                "not-attempted",
                reason=state.reason,
                error=state.error,
                readable=state.readable,
            )
        if outcome == "blocked":
            blockers = detail
            if blockers and isinstance(blockers[0], dict):
                waits = blockers
                blocked_batches = sorted({item["batch"] for item in waits})
            else:
                blocked_batches = blockers
                waits = self._blocking_waits(target)
            return self._receipt(
                line,
                order,
                target,
                "blocked",
                "not-attempted",
                reason="removing this version would leave a replay batch without "
                "a registered readable copy",
                readable=self._readable(target),
            ) | {"blocked_batches": blocked_batches, "waiting_for": waits}

        try:
            await self._delete_one(target)
        except asyncio.CancelledError:
            with self._lock:
                state = self._targets[target]
                state.state = _STATE_PENDING
                state.reason = (
                    "the requester disconnected while the delete outcome was unknown"
                )
                state.error = None
                if state.attempt:
                    state.attempt.reason = state.reason
            self._invalidate(target)
            raise
        except FileNotFoundError:
            with self._lock:
                state = self._targets[target]
                state.state = _STATE_DELETED
                state.readable = False
                state.reason = None
                state.error = None
                state.attempt = None
            self._invalidate(target)
            return self._receipt(line, order, target, _STATE_DELETED, "already-deleted")
        except PermissionError as error:
            with self._lock:
                state = self._targets[target]
                state.state = _STATE_REJECTED
                state.reason = "S3 explicitly rejected this item"
                state.error = f"{type(error).__name__}: {error}"
                state.attempt = None
            readable = None
            try:
                readable = await self._check_one(target)
            except Exception as check_error:
                with self._lock:
                    self._targets[target].error += (
                        f"; readability check failed: {type(check_error).__name__}: "
                        f"{check_error}"
                    )
            with self._lock:
                state = self._targets[target]
                state.readable = readable
                if readable is False:
                    state.state = _STATE_DELETED
                    state.reason = None
                elif readable is True:
                    state.reason = (
                        "S3 explicitly rejected this item; the exact version is "
                        "still readable"
                    )
            return self._receipt(
                line,
                order,
                target,
                state.state,
                "delete-rejected",
                reason=state.reason,
                error=state.error,
                readable=readable,
            )
        except Exception as error:
            if _is_rejection(error):
                with self._lock:
                    state = self._targets[target]
                    state.state = _STATE_REJECTED
                    state.reason = "S3 explicitly rejected this item"
                    state.error = f"{type(error).__name__}: {error}"
                    state.attempt = None
                readable = None
                try:
                    readable = await self._check_one(target)
                except Exception as check_error:
                    with self._lock:
                        self._targets[target].error += (
                            f"; readability check failed: "
                            f"{type(check_error).__name__}: {check_error}"
                        )
                with self._lock:
                    state = self._targets[target]
                    state.readable = readable
                    if readable is False:
                        state.state = _STATE_DELETED
                        state.reason = None
                    elif readable is True:
                        state.reason = (
                            "S3 explicitly rejected this item; the exact version is "
                            "still readable"
                        )
                return self._receipt(
                    line,
                    order,
                    target,
                    state.state,
                    "delete-rejected",
                    reason=state.reason,
                    error=state.error,
                    readable=readable,
                )
            with self._lock:
                state = self._targets[target]
                state.state = _STATE_PENDING
                state.reason = "the delete outcome is unknown and needs confirmation"
                state.error = f"{type(error).__name__}: {error}"
                if state.attempt:
                    state.attempt.reason = state.reason
                    state.attempt.error = state.error
            self._invalidate(target)
            return self._receipt(
                line,
                order,
                target,
                _STATE_PENDING,
                "unconfirmed",
                reason=state.reason,
                error=state.error,
                readable=None,
            )

        with self._lock:
            state = self._targets[target]
            state.state = _STATE_DELETED
            state.readable = False
            state.reason = None
            state.error = None
            state.attempt = None
        self._invalidate(target)
        return self._receipt(line, order, target, _STATE_DELETED, "deleted")

    # ------------------------------------------------------------------
    # public operations
    # ------------------------------------------------------------------
    async def submit(self, ledger, work_orders, merge_ledger=False):
        entries = self._ledger_entries(ledger)
        orders = self._normalise_work_orders(work_orders)
        self._last_ledger = ledger
        self._install_ledger(entries, merge=merge_ledger)
        self._last_orders = orders

        # A previous work order may have disconnected while its delete was in
        # flight.  Before deciding duplicates or blockers, probe every touched
        # target that is still unresolved.  The probe reads the exact version;
        # a different current version at the same key cannot affect it.
        touched = []
        for order in orders:
            for target in order["targets"]:
                if target not in touched:
                    touched.append(target)
        unresolved = [
            target
            for target in touched
            if self._targets.get(target)
            and self._targets[target].state in (_STATE_PENDING, _STATE_REJECTED)
        ]
        if unresolved:
            await asyncio.gather(
                *(self._refresh(target) for target in unresolved),
                return_exceptions=True,
            )

        # Work orders are independent.  A slow or disconnected delete for one
        # line must not hold up unrelated targets.  Deletes are not cancelled
        # when the caller goes away: a request that already reached the worker
        # may still be executed by S3, so it must be tracked to completion.
        tasks = [asyncio.create_task(self._handle_order(order)) for order in orders]
        receipts = []
        try:
            # asyncio.wait does not cancel its children when the waiter is
            # cancelled.  That is required here: a caller that disconnects
            # must not cancel a delete request that may already be executing.
            await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
            for order, task in zip(orders, tasks):
                try:
                    result = task.result()
                except BaseException as error:
                    for target in order["targets"]:
                        receipts.append(
                            {
                                "line": order["line"],
                                "work_order": order["order"],
                                "target": _target_text(target),
                                **_target_dict(target),
                                "status": "error",
                                "state": "error",
                                "action": "failed",
                                "readable": None,
                                "available": False,
                                "protected_batches": sorted(self._protecting(target)),
                                "protects": sorted(self._protecting(target)),
                                "requires_confirmation": False,
                                "error": f"{type(error).__name__}: {error}",
                            }
                        )
                else:
                    receipts.extend(result)
        except asyncio.CancelledError:
            for task in tasks:
                if not task.done():
                    task.add_done_callback(_consume_result)
            raise
        finally:
            for task in tasks:
                if not task.done():
                    task.add_done_callback(_consume_result)
        return CleanupReport(receipts, orders)

    async def _handle_order(self, order):
        targets = sorted(
            order["targets"],
            key=lambda target: (len(self._protecting(target)), target),
        )
        results = await asyncio.gather(
            *(
                self._handle_target(order["line"], order["order"], target)
                for target in targets
            )
        )
        return list(results)

    async def status(self, ledger=None, work_orders=None, targets=None, confirm=True):
        if ledger is not None:
            self._last_ledger = ledger
            self._install_ledger(self._ledger_entries(ledger))
        orders = None
        if work_orders is not None:
            orders = self._normalise_work_orders(work_orders)
            self._last_orders = orders
        else:
            orders = self._last_orders

        wanted = []
        if targets is not None:
            if isinstance(targets, (str, dict, tuple)):
                targets = [targets]
            wanted.extend(_canonical(target) for target in targets)
        for entry in self._batches.values():
            for copy in entry.copies:
                if copy not in wanted:
                    wanted.append(copy)
        for order in orders:
            for target in order["targets"]:
                if target not in wanted:
                    wanted.append(target)
        for target in self._targets:
            if target not in wanted:
                wanted.append(target)

        # Confirm unresolved deletions by reading the exact version.  Reading
        # A1 successfully does not make it available: an in-flight delete may
        # still run later, so its state remains pending until it disappears.
        if confirm:
            checks = [
                target
                for target in wanted
                if self._targets.get(target)
                and self._targets[target].state in (_STATE_PENDING, _STATE_REJECTED)
            ]
            if checks:
                await asyncio.gather(
                    *(self._refresh(target) for target in checks),
                    return_exceptions=True,
                )

        records = []
        for target in wanted:
            with self._lock:
                state = self._targets.get(target) or _TargetState()
                blocker_batches = self._blocking_batches(target)
                waits = self._blocking_waits(target)
                record = {
                    "target": _target_text(target),
                    **_target_dict(target),
                    "status": state.state,
                    "state": state.state,
                    "readable": self._readable(target),
                    "available": self._is_available(target),
                    "protected_batches": sorted(self._protecting(target)),
                    "protects": sorted(self._protecting(target)),
                    "guards": sorted(self._guards(target)),
                    "blocked_batches": blocker_batches,
                    "can_remove": not blocker_batches,
                    "delete_pending": state.state == _STATE_PENDING,
                    "attempts": state.attempts,
                }
                if waits:
                    record["waiting_for"] = waits
                if state.reason:
                    record["reason"] = state.reason
                if state.error:
                    record["error"] = state.error
                if state.attempt:
                    record["pending_attempt"] = state.attempt.as_dict()
                records.append(record)
        return CleanupStatus(records, coverage=self._coverage())

    async def _refresh(self, target):
        try:
            exists = await self._check_one(target)
        except Exception as error:
            with self._lock:
                state = self._targets.setdefault(target, _TargetState())
                state.error = f"{type(error).__name__}: {error}"
            return
        with self._lock:
            state = self._targets.setdefault(target, _TargetState())
            state.readable = exists
            if not exists:
                state.state = _STATE_DELETED
                state.reason = None
                state.error = None
                state.attempt = None
            elif state.state == _STATE_PENDING:
                state.reason = (
                    "the exact version is still readable, but its delete outcome "
                    "is unconfirmed; it remains unavailable for replay safety"
                )
            elif state.state == _STATE_REJECTED:
                state.reason = (
                    "S3 explicitly rejected this item; the exact version is still "
                    "readable"
                )
        if not exists:
            self._invalidate(target)

    async def confirm(self, target=None):
        if target is None:
            with self._lock:
                targets = [
                    item
                    for item, state in self._targets.items()
                    if state.state in (_STATE_PENDING, _STATE_REJECTED)
                ]
        else:
            targets = [_canonical(target)]
        for item in targets:
            await self._refresh(item)
        return await self.status(targets=targets, confirm=False)


def _state_reason(state):
    return state.reason if state is not None else None


def _error_code(error):
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        code = response.get("Error", {}).get("Code")
        if code:
            return str(code)
    if isinstance(error, PermissionError):
        return "AccessDenied"
    if isinstance(error, FileNotFoundError):
        return "NoSuchVersion"
    return None


def _is_rejection(error):
    return _error_code(error) in {
        "AccessDenied",
        "403",
        "AccountProblem",
        "AllAccessDisabled",
        "ExpiredToken",
        "InvalidAccessKeyId",
        "InvalidObjectState",
        "SignatureDoesNotMatch",
        "TokenRefreshRequired",
        "Unauthorized",
    }


def _is_missing(error):
    return isinstance(error, FileNotFoundError) or _error_code(error) in {
        "NoSuchKey",
        "NoSuchVersion",
        "404",
    }


def _delete_errors(response, target):
    if not isinstance(response, dict):
        return None
    for error in response.get("Errors") or []:
        key = error.get("Key")
        version_id = error.get("VersionId")
        if key in (None, target[1]) and version_id in (None, target[2]):
            return error
    return None
