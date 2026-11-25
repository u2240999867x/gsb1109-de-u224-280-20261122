from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

import pandas as pd

try:
    import pyranges as _pr
except Exception:  # pragma: no cover - pyranges is optional at import time
    _pr = None


PASS_WORDS = {
    "pass", "passed", "passing", "ok", "okay", "valid", "qualified", "accept",
    "accepted", "signed", "signable", "true", "1", "yes", "y", "通过", "合格",
    "可签收", "签收", "同意",
}
FAIL_WORDS = {
    "fail", "failed", "failing", "invalid", "reject", "rejected", "not_pass",
    "not_passed", "false", "0", "no", "n", "失败", "不通过", "不合格", "拒绝",
}
PENDING_WORDS = {
    "pending", "undecided", "unresolved", "waiting", "arbitration", "conflict",
    "待裁决", "未决", "待定", "争议", "冲突",
}
MISSING_WORDS = {
    "missing", "absent", "incomplete", "lacking", "缺件", "缺失", "资料不全", "缺少",
}
CHECK_WORDS = {
    "check", "voucher", "credential", "certificate", "verification", "verify",
    "校验", "校验单", "凭证", "校验凭证", "证明",
}
SCAN_WORDS = {
    "scan", "scanned", "scanning", "survey", "ticket", "sheet", "scan_sheet",
    "扫描", "扫描单", "扫描票", "测线单",
}
VOID_WORDS = {
    "void", "voided", "voidance", "revoke", "revoked", "revocation",
    "invalidate", "invalidated", "cancellation", "cancel", "作废", "撤销",
}
RELEASE_WORDS = {
    "release", "released", "rescind", "rescinded", "rescission", "restore",
    "restored", "lift", "lifted", "undo_void", "解除", "恢复",
}
REVIEW_WORDS = {
    "review", "field_review", "site_review", "onsite_review", "check_line",
    "现场复核", "复核", "现场",
}


def _normalize_word(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    return "".join(ch for ch in text if ch not in {" ", "-", "_", "/", ".", "：", ":"})


class Status(str):
    _aliases: frozenset[str]
    value: str
    name: str
    zh: str

    def __new__(cls, canonical: str, aliases: Iterable[str] = (), zh: str | None = None):
        obj = str.__new__(cls, canonical)
        normalized = {_normalize_word(canonical)}
        normalized.update(_normalize_word(item) for item in aliases)
        obj._aliases = frozenset(normalized)
        obj.value = canonical
        obj.name = canonical.upper()
        obj.zh = zh or canonical
        return obj

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Status):
            return str.__eq__(self, other) or bool(self._aliases & other._aliases)
        return _normalize_word(other) in self._aliases

    def __hash__(self) -> int:
        return hash(str(self))


ACCEPTED = Status("accepted", ["pass", "passed", "signed", "signable", "可签收", "通过", "签收"], "可签收")
FAILED = Status("failed", ["fail", "rejected", "not_passed", "不通过", "失败"], "不通过")
CONFLICT = Status("conflict", ["arbitration", "pending_arbitration", "adjudication", "待裁决", "裁决", "冲突"], "待裁决")
MISSING = Status("missing", ["incomplete", "absent", "缺件", "缺失", "资料不全"], "缺件")
PENDING = Status("pending", ["undecided", "unresolved", "未决", "待定"], "未决")
INVALID = Status("invalid", ["void", "voided", "revoked", "disqualified", "unqualified", "失效", "作废", "失去资格"], "失效")


def _first(data: Any, names: Iterable[str], default: Any = None) -> Any:
    if data is None:
        return default
    if isinstance(data, Mapping):
        for name in names:
            if name in data:
                return data[name]
        lowered = {_normalize_word(k): v for k, v in data.items()}
        for name in names:
            key = _normalize_word(name)
            if key in lowered and lowered[key] is not None:
                return lowered[key]
        return default
    for name in names:
        if hasattr(data, name):
            value = getattr(data, name)
            if value is not None:
                return value
    return default


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set, frozenset)):
        return list(value)
    if isinstance(value, pd.DataFrame):
        return value.to_dict("records")
    if _pr is not None and isinstance(value, _pr.PyRanges):
        return value.df.to_dict("records")
    return [value]


def _coerce_verdict(value: Any) -> bool | str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isnan(float(value)):
            return None
        return float(value) > 0
    normalized = _normalize_word(value)
    if normalized in {_normalize_word(word) for word in PASS_WORDS}:
        return True
    if normalized in {_normalize_word(word) for word in FAIL_WORDS}:
        return False
    if normalized in {_normalize_word(word) for word in PENDING_WORDS}:
        return "pending"
    if normalized in {_normalize_word(word) for word in MISSING_WORDS}:
        return "missing"
    return None


def _coerce_ranges(value: Any, data: Any = None) -> tuple[tuple[float, float], ...]:
    ranges: list[tuple[float, float]] = []
    raw_ranges = _first(data, ["ranges", "intervals", "coverage", "区间", "范围"], None)
    if raw_ranges is not None:
        for item in _as_list(raw_ranges):
            if isinstance(item, Mapping):
                start = _first(item, ["start", "Start", "begin", "from", "起点", "开始"], None)
                end = _first(item, ["end", "End", "stop", "to", "终点", "结束"], None)
            else:
                start, end = item[0], item[1]
            if start is not None and end is not None:
                ranges.append((start, end))
    if value is not None:
        if isinstance(value, tuple) and len(value) == 2 and not isinstance(value[0], (tuple, list)):
            ranges.append((value[0], value[1]))
        elif isinstance(value, (list, tuple)) and value and isinstance(value[0], (tuple, list)):
            ranges.extend((item[0], item[1]) for item in value)
        else:
            start = _first(data, ["start", "Start", "begin", "from", "start_mileage", "mileage_start", "起点", "开始里程"], None)
            end = _first(data, ["end", "End", "stop", "to", "end_mileage", "mileage_end", "终点", "结束里程"], None)
            if start is not None and end is not None:
                ranges.append((start, end))
    if not ranges:
        start = _first(data, ["start", "Start", "begin", "from", "start_mileage", "mileage_start", "起点", "开始里程"], None)
        end = _first(data, ["end", "End", "stop", "to", "end_mileage", "mileage_end", "终点", "结束里程"], None)
        if start is not None and end is not None:
            ranges.append((start, end))
    interval = _first(data, ["interval", "range", "mileage_range", "里程区间"], None)
    if interval is not None and not ranges:
        if isinstance(interval, Mapping):
            start = _first(interval, ["start", "Start", "from", "起点"], None)
            end = _first(interval, ["end", "End", "to", "终点"], None)
            if start is not None and end is not None:
                ranges.append((start, end))
        elif isinstance(interval, (tuple, list)) and len(interval) >= 2:
            ranges.append((interval[0], interval[1]))
    return _merge_ranges(tuple((start, end) for start, end in ranges if end > start))


def _merge_ranges(ranges: Iterable[tuple[float, float]]) -> tuple[tuple[float, float], ...]:
    ordered = sorted((start, end) for start, end in ranges if end > start)
    merged: list[list[float]] = []
    for start, end in ordered:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        elif end > merged[-1][1]:
            merged[-1][1] = end
    return tuple((item[0], item[1]) for item in merged)


def _subtract_ranges(
    ranges: Iterable[tuple[float, float]],
    holes: Iterable[tuple[float, float]],
) -> tuple[tuple[float, float], ...]:
    result: list[tuple[float, float]] = []
    for start, end in _merge_ranges(ranges):
        pieces = [(start, end)]
        for hole_start, hole_end in _merge_ranges(holes):
            next_pieces: list[tuple[float, float]] = []
            for piece_start, piece_end in pieces:
                if hole_end <= piece_start or hole_start >= piece_end:
                    next_pieces.append((piece_start, piece_end))
                    continue
                if hole_start > piece_start:
                    next_pieces.append((piece_start, min(hole_start, piece_end)))
                if hole_end < piece_end:
                    next_pieces.append((max(hole_end, piece_start), piece_end))
            pieces = next_pieces
        result.extend(pieces)
    return _merge_ranges(result)


def _covers_all(
    ranges: Iterable[tuple[float, float]],
    start: float,
    end: float,
) -> bool:
    if end <= start:
        return True
    cursor = start
    for range_start, range_end in _merge_ranges(ranges):
        if range_end <= cursor:
            continue
        if range_start > cursor:
            return False
        cursor = max(cursor, range_end)
        if cursor >= end:
            return True
    return cursor >= end


def _contains_point(ranges: Iterable[tuple[float, float]], point: float) -> bool:
    return any(start <= point < end for start, end in ranges)


@dataclass(frozen=True)
class Requirement:
    target: str
    start: float | None = None
    end: float | None = None
    supported_ranges: tuple[tuple[float, float], ...] = ()
    order: int = 0
    raw: Any = None


@dataclass(frozen=True)
class Replacement:
    target: str
    ranges: tuple[tuple[float, float], ...] = ()
    raw: Any = None


@dataclass
class Document:
    id: str
    kind: str = "scan"
    line: str | None = None
    route: str = "default"
    work_order: str = "default"
    ranges: tuple[tuple[float, float], ...] = ()
    verdict: bool | str | None = None
    review: bool = False
    replaces: tuple[Replacement, ...] = ()
    requirements: tuple[Requirement, ...] = ()
    order: int = 0
    raw: Any = None
    fingerprint: Any = ""


@dataclass
class VoidOrder:
    id: str
    target: str
    ranges: tuple[tuple[float, float], ...] = ()
    work_order: str = "default"
    order: int = 0
    fingerprint: str = ""


@dataclass
class ReleaseOrder:
    id: str
    void_id: str
    ranges: tuple[tuple[float, float], ...] = ()
    order: int = 0
    fingerprint: str = ""


@dataclass
class Change:
    start: float
    end: float
    route: str = "default"
    work_order: str = "default"
    before: Mapping[str, Any] | None = None
    after: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "route": self.route,
            "work_order": self.work_order,
            "start": self.start,
            "end": self.end,
            "interval": (self.start, self.end),
            "before": dict(self.before) if self.before else None,
            "after": dict(self.after) if self.after else None,
            "status": (self.after or {}).get("status"),
            "references": (self.after or {}).get("references", []),
            "evidence": (self.after or {}).get("evidence", []),
        }


class ResultDict(dict):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        for key, value in list(self.items()):
            self[key] = _wrap_result(value)

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


def _wrap_result(value: Any) -> Any:
    if isinstance(value, Mapping) and not isinstance(value, ResultDict):
        return ResultDict(value)
    if isinstance(value, list):
        return [_wrap_result(item) for item in value]
    return value


class Evidence(ResultDict):
    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return self.get("id") == other
        if isinstance(other, Mapping):
            return dict.__eq__(self, dict(other))
        return super().__eq__(other)

    def __hash__(self) -> int:
        return hash(self.get("id"))


class MutationResult(list):
    def __init__(
        self,
        changes: Iterable[Mapping[str, Any]] = (),
        *,
        event_id: str | None = None,
        duplicate: bool = False,
    ) -> None:
        change_list = [ResultDict(change) for change in changes]
        super().__init__(change_list)
        self.event_id = event_id
        self.id = event_id
        self.duplicate = duplicate

    @property
    def changes(self) -> "MutationResult":
        return self

    @property
    def changed_segments(self) -> "MutationResult":
        return self

    def __getitem__(self, key: Any):
        if isinstance(key, str):
            if key in {"changes", "changed_segments"}:
                return self
            if key == "duplicate":
                return self.duplicate
            if key in {"id", "event_id"}:
                return self.event_id
            raise KeyError(key)
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.event_id, "duplicate": self.duplicate, "changes": list(self)}


class AcceptanceReport(list):
    def __init__(self, items: Iterable[Mapping[str, Any]] = (), *, ledger: "AcceptanceLedger | None" = None):
        super().__init__(ResultDict(item) for item in items)
        self.ledger = ledger

    @property
    def segments(self) -> "AcceptanceReport":
        return self

    def __getitem__(self, key: Any):
        if isinstance(key, tuple) and len(key) == 2:
            start, end = key
            for segment in self:
                if segment["start"] == start and segment["end"] == end:
                    return segment
            raise KeyError(key)
        return super().__getitem__(key)

    def at(self, position: float) -> ResultDict | None:
        for segment in self:
            if segment["start"] <= position < segment["end"]:
                return segment
        return None

    def by_interval(self) -> dict[tuple[float, float], ResultDict]:
        return {(segment["start"], segment["end"]): segment for segment in self}

    @property
    def conflicts(self) -> list[dict[str, Any]]:
        conflicts: list[dict[str, Any]] = []
        for segment in self:
            for conflict in segment.get("conflicts", []):
                item = dict(conflict)
                item.setdefault("route", segment.get("route", "default"))
                item.setdefault("work_order", segment.get("work_order", "default"))
                item.setdefault("start", segment.get("start"))
                item.setdefault("end", segment.get("end"))
                conflicts.append(item)
        return conflicts

    def conflicting_parties(self) -> dict[tuple[Any, ...], list[str]]:
        result: dict[tuple[Any, ...], list[str]] = {}
        for conflict in self.conflicts:
            key = (
                conflict.get("route", "default"),
                conflict.get("start"),
                conflict.get("end"),
                conflict.get("line"),
            )
            result[key] = list(conflict.get("parties", []))
        return result

    def to_pandas(self) -> pd.DataFrame:
        rows = []
        for segment in self:
            row = dict(segment)
            row["status"] = str(segment.get("status", ""))
            for key in ("references", "evidence", "conflicts", "line_states"):
                if key in row and not isinstance(row[key], str):
                    row[key] = json.dumps(row[key], ensure_ascii=False, default=str)
            rows.append(row)
        return pd.DataFrame(rows)

    df = property(to_pandas)

    def to_pyranges(self):
        frame = self.to_pandas()
        if frame.empty:
            return _pr.PyRanges(pd.DataFrame(columns=["Chromosome", "Start", "End"])) if _pr else frame
        frame = frame.rename(columns={"route": "Chromosome", "start": "Start", "end": "End"})
        if _pr is None:
            return frame
        return _pr.PyRanges(frame)

    def to_list(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self]


def _fingerprint(data: Any) -> str:
    try:
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)
    except TypeError:
        payload = repr(data)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _match_word(value: Any, words: set[str]) -> bool:
    return _normalize_word(value) in {_normalize_word(word) for word in words}


class AcceptanceLedger:
    """Incremental acceptance and voucher-qualification ledger."""

    def __init__(
        self,
        scans: Any = None,
        vouchers: Any = None,
        voids: Any = None,
        releases: Any = None,
        *sources: Any,
        records: Any = None,
        documents: Any = None,
        checks: Any = None,
        review_lines: Iterable[str] | None = None,
        expected_lines: Any = None,
        required_lines: int = 2,
        require_review: bool = True,
        **kwargs: Any,
    ) -> None:
        self.documents: dict[str, Document] = {}
        self.void_orders: dict[str, VoidOrder] = {}
        self.release_orders: dict[str, ReleaseOrder] = {}
        self.events: dict[tuple[str, str], Any] = {}
        self.reverse_requirements: dict[str, set[str]] = {}
        self.reverse_replacements: dict[str, set[str]] = {}
        self.work_order_documents: dict[str, set[str]] = {}
        self.route_documents: dict[str, set[str]] = {}
        self.review_lines = {_normalize_word(item) for item in (review_lines or [])}
        self.expected_lines = expected_lines or {}
        self.required_lines = required_lines
        self.require_review = require_review
        self.changes: list[dict[str, Any]] = []
        self.delivered_change_keys: set[str] = set()
        self.last_recomputed_work_orders: set[str] = set()
        self.recomputed_document_count = 0
        self._order = 0
        all_records = []
        for source in (records, documents, scans):
            if source is not None:
                all_records.extend(_as_list(source))
        for source in sources:
            if source is not None:
                all_records.extend(_as_list(source))
        check_records = []
        for source in (vouchers, checks, kwargs.get("check_sheets"), kwargs.get("voucher_sheets")):
            if source is not None:
                check_records.extend(_as_list(source))
        if kwargs.get("scan_sheets") is not None:
            all_records.extend(_as_list(kwargs["scan_sheets"]))
        for record in all_records:
            self.add_document(record, track=False)
        for record in check_records:
            if isinstance(record, Mapping) and not _first(record, ["kind", "type", "doc_type", "sheet_type"]):
                record = {**record, "kind": "check"}
            self.add_document(record, track=False)
        for void_source in (voids, kwargs.get("void_orders"), kwargs.get("revocations"), kwargs.get("void_events")):
            for item in _as_list(void_source):
                self.add_void(item, track=False)
        for release_source in (
            releases,
            kwargs.get("release_orders"),
            kwargs.get("rescissions"),
            kwargs.get("release_events"),
            kwargs.get("rescind_orders"),
        ):
            for item in _as_list(release_source):
                self.add_release(item, track=False)

    def _next_order(self) -> int:
        self._order += 1
        return self._order

    def _event_seen(self, kind: str, event_id: str, fingerprint: str) -> bool:
        key = (kind, str(event_id))
        previous = self.events.get(key)
        if previous is not None and previous == fingerprint:
            return True
        self.events[key] = fingerprint
        return False

    def add_document(self, record: Any = None, *args: Any, track: bool = True, **kwargs: Any) -> MutationResult:
        if isinstance(record, Mapping):
            source: Any = {**record, **kwargs}
        elif record is not None and not isinstance(record, (str, int, float)):
            source = record
        else:
            source = dict(kwargs)
            if record is not None:
                source.setdefault("id", record)
            numeric: list[float] = []
            for value in args:
                if isinstance(value, str) and _coerce_verdict(value) is not None:
                    source.setdefault("verdict", value)
                elif isinstance(value, str):
                    source.setdefault("line", value)
                elif isinstance(value, bool):
                    source.setdefault("verdict", value)
                elif isinstance(value, (int, float)):
                    numeric.append(value)
                elif isinstance(value, (tuple, list)) and len(value) >= 2:
                    source.setdefault("start", value[0])
                    source.setdefault("end", value[1])
            if len(numeric) >= 2:
                source.setdefault("start", numeric[0])
                source.setdefault("end", numeric[1])

        doc_id = str(_first(source, ["id", "doc_id", "document_id", "sheet_id", "ticket_id", "name", "编号", "单据编号", "单号"]))
        kind_value = _first(source, ["kind", "type", "doc_type", "sheet_type", "category", "类型", "单据类型"], "scan")
        kind = "check" if _match_word(kind_value, CHECK_WORDS) else "scan"
        if _match_word(kind_value, VOID_WORDS):
            return self.add_void(source, track=track)
        if _match_word(kind_value, RELEASE_WORDS):
            return self.add_release(source, track=track)

        line = _first(source, ["line", "survey_line", "line_id", "measure_line", "survey_line_id", "测线", "测线编号"])
        route = _first(source, ["route", "road", "alignment", "chromosome", "Chromosome", "线路", "路线"], "default")
        work_order = _first(source, ["work_order", "workorder", "work_order_id", "order_id", "work_ticket", "工单", "工单号"], "default")
        ranges = _coerce_ranges(_first(source, ["interval", "range", "ranges", "Start", "start"], None), source)
        verdict = _coerce_verdict(_first(source, ["verdict", "result", "opinion", "conclusion", "vote", "status", "意见", "结论", "结果"], None))
        review_value = _first(source, ["review", "is_review", "field_review", "site_review", "onsite_review", "现场复核"], False)
        review = bool(review_value) or _normalize_word(line) in REVIEW_WORDS or _match_word(kind_value, REVIEW_WORDS)
        if review:
            self.review_lines.add(_normalize_word(line or "现场复核"))

        replaces = self._normalize_replacements(_first(source, ["replaces", "replace", "supersedes", "supersede", "replacement", "replacements", "替代", "替换"], []), ranges)
        requirements = self._normalize_requirements(
            _first(source, ["requires", "requirements", "references", "refs", "dependencies", "depends_on", "引用", "依赖", "校验要求"], []),
            ranges,
        )
        fingerprint = (
            kind, line, route, work_order, ranges, verdict, review,
            tuple((item.target, item.ranges) for item in replaces),
            tuple((item.target, item.start, item.end, item.supported_ranges) for item in requirements),
        )
        duplicate = self._event_seen("document", doc_id, fingerprint)
        if duplicate and doc_id in self.documents:
            return MutationResult((), event_id=doc_id, duplicate=True)

        affected_keys = self._impacted_keys_for_new_document(doc_id, requirements, replaces) if track else set()
        if track:
            affected_keys.add((str(route), str(work_order)))
        before = self._snapshot_keys(affected_keys) if track else {}

        old = self.documents.get(doc_id)
        if old:
            self._remove_indexes(old)
        doc = Document(
            id=doc_id, kind=kind, line=str(line) if line is not None else None,
            route=str(route), work_order=str(work_order), ranges=ranges,
            verdict=verdict, review=review, replaces=tuple(replaces),
            requirements=tuple(requirements), order=self._next_order(),
            raw=None, fingerprint=repr(fingerprint),
        )
        self.documents[doc_id] = doc
        self._add_indexes(doc)
        result_changes = self._changes_from_snapshot(before) if track else []
        return MutationResult(result_changes, event_id=doc_id, duplicate=False)

    add = add_document
    submit = add_document
    add_sheet = add_document
    add_ticket = add_document
    add_scan = add_document
    add_check = add_document
    add_voucher = add_document

    def _normalize_replacements(self, value: Any, default_ranges: tuple) -> list[Replacement]:
        result: list[Replacement] = []
        for index, item in enumerate(_as_list(value)):
            if isinstance(item, str):
                result.append(Replacement(item, default_ranges))
            elif isinstance(item, Mapping):
                target = str(_first(item, ["id", "target", "target_id", "document_id", "doc_id", "旧单", "被替代单"]))
                ranges = _coerce_ranges(_first(item, ["interval", "range", "ranges"], None), item) or default_ranges
                result.append(Replacement(target, ranges, item))
            elif isinstance(item, (tuple, list)):
                target = str(item[0])
                if len(item) >= 3 and isinstance(item[1], (int, float)):
                    ranges = ((item[1], item[2]),)
                elif len(item) >= 2 and isinstance(item[1], (tuple, list)):
                    ranges = ((item[1][0], item[1][1]),)
                else:
                    ranges = default_ranges
                result.append(Replacement(target, ranges))
        return result

    def _normalize_requirements(self, value: Any, default_ranges: tuple) -> list[Requirement]:
        result: list[Requirement] = []
        for order, item in enumerate(_as_list(value)):
            target: Any
            start = end = None
            supported = default_ranges
            if isinstance(item, str):
                target = item
            elif isinstance(item, Mapping):
                target = _first(item, ["id", "target", "target_id", "document_id", "doc_id", "ref", "ref_id", "校验单", "单号"])
                interval = _first(item, ["interval", "range", "check_range", "verification_range", "校验区间", "核验区间"], None)
                if interval is not None:
                    ir = _coerce_ranges(interval, item)
                    if ir:
                        start, end = ir[0]
                start = _first(item, ["start", "Start", "check_start", "from", "校验起点"], start)
                end = _first(item, ["end", "End", "check_end", "to", "校验终点"], end)
                supported_value = _first(item, ["supported_ranges", "scan_ranges", "scope", "applies_to", "依赖扫描范围"], None)
                if supported_value is not None:
                    supported = _coerce_ranges(supported_value, item)
            elif isinstance(item, (tuple, list)):
                target = item[0]
                rest = item[1:]
                if len(rest) == 1 and isinstance(rest[0], (tuple, list)):
                    start, end = rest[0][0], rest[0][1]
                elif len(rest) >= 2:
                    start, end = rest[0], rest[1]
            else:
                continue
            if target is None:
                continue
            result.append(Requirement(str(target), start, end, tuple(supported), order, item))
        return result

    def add_void(self, record: Any = None, target: Any = None, start: Any = None, end: Any = None, *args: Any, track: bool = True, **kwargs: Any) -> MutationResult:
        if isinstance(record, Mapping):
            source = {**record, **kwargs}
        else:
            source = dict(kwargs)
            if record is not None:
                source.setdefault("id", record)
            if target is not None:
                source.setdefault("target", target)
            if isinstance(start, (tuple, list)) and len(start) >= 2:
                source.setdefault("start", start[0])
                source.setdefault("end", start[1])
            elif start is not None and end is not None:
                source.setdefault("start", start)
                source.setdefault("end", end)
            elif start is not None:
                source.setdefault("ranges", start)
        void_id = str(_first(source, ["id", "void_id", "revocation_id", "编号", "作废编号"]))
        target = str(_first(source, ["target", "target_id", "document_id", "doc_id", "sheet_id", "单据", "被作废单"]))
        ranges = _coerce_ranges(_first(source, ["ranges", "interval", "range"], None), source)
        fingerprint = (void_id, target, ranges)
        if self._event_seen("void", void_id, fingerprint) and void_id in self.void_orders:
            return MutationResult((), event_id=void_id, duplicate=True)
        affected = self._impacted_keys_for_target(target) if track else set()
        target_doc = self.documents.get(target)
        if track and target_doc is not None:
            affected.add((target_doc.route, target_doc.work_order))
        before = self._snapshot_keys(affected) if track else {}
        old = self.void_orders.get(void_id)
        if old:
            pass
        self.void_orders[void_id] = VoidOrder(
            void_id, target, ranges,
            str(_first(source, ["work_order", "workorder", "工单"], self.documents.get(target).work_order if target in self.documents else "default")),
            self._next_order(), fingerprint,
        )
        changes = self._changes_from_snapshot(before) if track else []
        return MutationResult(changes, event_id=void_id, duplicate=False)

    void = add_void
    invalidate = add_void
    revoke = add_void

    def add_release(self, record: Any = None, void_id: Any = None, *args: Any, track: bool = True, **kwargs: Any) -> MutationResult:
        if isinstance(record, Mapping):
            source = {**record, **kwargs}
        else:
            source = dict(kwargs)
            if record is not None:
                source.setdefault("id", record)
            if void_id is not None:
                source.setdefault("void_id", void_id)
        release_id = str(_first(source, ["id", "release_id", "rescission_id", "编号", "解除编号"]))
        void_id = str(_first(source, ["void_id", "void", "revocation_id", "void_order_id", "target", "target_id", "作废单", "被解除作废单"]))
        ranges = _coerce_ranges(_first(source, ["ranges", "interval", "range"], None), source)
        fingerprint = (release_id, void_id, ranges)
        if self._event_seen("release", release_id, fingerprint) and release_id in self.release_orders:
            return MutationResult((), event_id=release_id, duplicate=True)
        void = self.void_orders.get(void_id)
        target = void.target if void else void_id
        affected = self._impacted_keys_for_target(target) if track else set()
        before = self._snapshot_keys(affected) if track else {}
        self.release_orders[release_id] = ReleaseOrder(release_id, void_id, ranges, self._next_order(), fingerprint)
        changes = self._changes_from_snapshot(before) if track else []
        return MutationResult(changes, event_id=release_id, duplicate=False)

    release_void = add_release
    release = add_release
    rescind = add_release
    rescind_void = add_release
    lift_void = add_release
    cancel_void = add_release

    def _add_indexes(self, doc: Document) -> None:
        self.work_order_documents.setdefault(doc.work_order, set()).add(doc.id)
        self.route_documents.setdefault(doc.route, set()).add(doc.id)
        for req in doc.requirements:
            self.reverse_requirements.setdefault(req.target, set()).add(doc.id)
        for rep in doc.replaces:
            self.reverse_replacements.setdefault(rep.target, set()).add(doc.id)

    def _remove_indexes(self, doc: Document) -> None:
        self.work_order_documents.get(doc.work_order, set()).discard(doc.id)
        self.route_documents.get(doc.route, set()).discard(doc.id)
        for req in doc.requirements:
            self.reverse_requirements.get(req.target, set()).discard(doc.id)
        for rep in doc.replaces:
            self.reverse_replacements.get(rep.target, set()).discard(doc.id)

    def _impacted_document_ids(self, target: str) -> set[str]:
        seen: set[str] = set()
        stack = [str(target)]
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            for source in self.reverse_requirements.get(current, set()):
                if source not in seen:
                    stack.append(source)
        return seen

    def _impacted_keys_for_target(self, target: str) -> set[tuple[str, str]]:
        keys: set[tuple[str, str]] = set()
        for doc_id in self._impacted_document_ids(target):
            doc = self.documents.get(doc_id)
            if doc:
                keys.add((doc.route, doc.work_order))
        target_doc = self.documents.get(str(target))
        if target_doc:
            keys.add((target_doc.route, target_doc.work_order))
        return keys

    def _impacted_keys_for_new_document(
        self,
        doc_id: str,
        requirements: Iterable[Requirement],
        replacements: Iterable[Replacement] = (),
    ) -> set[tuple[str, str]]:
        keys = self._impacted_keys_for_target(doc_id)
        doc = self.documents.get(doc_id)
        if doc:
            keys.add((doc.route, doc.work_order))
        for req in requirements:
            keys.update(self._impacted_keys_for_target(req.target))
        for replacement in replacements:
            keys.update(self._impacted_keys_for_target(replacement.target))
        return keys

    def _active_void_holes(self, doc_id: str) -> tuple[tuple[float, float], ...]:
        holes: list[tuple[float, float]] = []
        for void in self.void_orders.values():
            if void.target != doc_id:
                continue
            released = False
            partial_release: list[tuple[float, float]] = []
            for release in self.release_orders.values():
                if release.void_id == void.id:
                    if not release.ranges:
                        released = True
                    else:
                        partial_release.extend(release.ranges)
            if released:
                continue
            if void.ranges:
                holes.extend(_subtract_ranges(void.ranges, partial_release))
            else:
                doc = self.documents.get(doc_id)
                if doc:
                    holes.extend(doc.ranges)
        return _merge_ranges(holes)

    def qualified_ranges(
        self,
        doc_id: str,
        _stack: frozenset[str] = frozenset(),
        _memo: dict[str, tuple[tuple[float, float], ...]] | None = None,
    ) -> tuple[tuple[float, float], ...]:
        memo = {} if _memo is None else _memo
        cache_key = str(doc_id)
        if cache_key in memo:
            return memo[cache_key]
        doc = self.documents.get(cache_key)
        if doc is None:
            return ()
        if cache_key in _stack:
            return ()
        stack = _stack | {cache_key}
        ranges = _subtract_ranges(doc.ranges, self._active_void_holes(cache_key))
        for req in doc.requirements:
            if req.start is None or req.end is None:
                target_ranges = self.qualified_ranges(req.target, stack, memo)
                ok = bool(target_ranges)
            else:
                target_ranges = self.qualified_ranges(req.target, stack, memo)
                ok = _covers_all(target_ranges, req.start, req.end)
            if not ok:
                supported = req.supported_ranges or doc.ranges
                ranges = _subtract_ranges(ranges, supported)
        result = _merge_ranges(ranges)
        memo[cache_key] = result
        return result

    def is_qualified(self, doc_id: str, start: float, end: float | None = None) -> bool:
        ranges = self.qualified_ranges(doc_id)
        if end is None:
            return _contains_point(ranges, start)
        return _covers_all(ranges, start, end)

    def _scope_document_ids(self, route: str | None, work_order: str | None) -> set[str]:
        if work_order is not None:
            ids = set(self.work_order_documents.get(work_order, set()))
            if route is not None:
                ids = {doc_id for doc_id in ids if self.documents[doc_id].route == route}
            return ids
        if route is not None:
            return set(self.route_documents.get(route, set()))
        return set(self.documents)

    def _snapshot_keys(self, keys: Iterable[tuple[str, str]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
        return {key: self.evaluate(route=key[0], work_order=key[1]) for key in keys}

    def evaluate(
        self,
        *,
        route: str | None = None,
        work_order: str | None = None,
        start: float | None = None,
        end: float | None = None,
        interval: tuple[float, float] | None = None,
    ) -> AcceptanceReport:
        if interval is not None:
            start, end = interval
        ids = self._scope_document_ids(route, work_order)
        docs = [self.documents[doc_id] for doc_id in ids if doc_id in self.documents]
        scan_docs = [doc for doc in docs if doc.kind == "scan"]
        self.last_recomputed_work_orders = {doc.work_order for doc in scan_docs}
        self.recomputed_document_count = len(scan_docs)
        if route is None and work_order is None:
            scopes = {(doc.route, doc.work_order) for doc in scan_docs}
            items: list[dict[str, Any]] = []
            for scope_route, scope_wo in sorted(scopes):
                items.extend(self._evaluate_scope(scope_route, scope_wo, start, end))
            return AcceptanceReport(items, ledger=self)
        if route is None:
            routes = {doc.route for doc in scan_docs}
            items = []
            for scope_route in sorted(routes):
                items.extend(self._evaluate_scope(scope_route, work_order or "default", start, end))
            return AcceptanceReport(items, ledger=self)
        return AcceptanceReport(self._evaluate_scope(route, work_order or "default", start, end), ledger=self)

    verify = evaluate
    check = evaluate
    check_acceptance = evaluate
    acceptance_status = evaluate

    def _evaluate_scope(
        self,
        route: str,
        work_order: str,
        query_start: float | None = None,
        query_end: float | None = None,
    ) -> list[dict[str, Any]]:
        ids = self.work_order_documents.get(work_order, set())
        docs = [self.documents[doc_id] for doc_id in ids if doc_id in self.documents and self.documents[doc_id].route == route]
        scan_docs = [doc for doc in docs if doc.kind == "scan"]
        if not scan_docs and query_start is None:
            return []
        qualification_memo: dict[str, tuple[tuple[float, float], ...]] = {}
        qualification = {doc.id: self.qualified_ranges(doc.id, _memo=qualification_memo) for doc in docs}
        endpoints: set[float] = set()
        for doc in scan_docs:
            endpoints.update(coord for rng in doc.ranges for coord in rng)
            endpoints.update(coord for rng in qualification.get(doc.id, ()) for coord in rng)
            endpoints.update(coord for rng in self._active_void_holes(doc.id) for coord in rng)
            for rep in doc.replaces:
                endpoints.update(coord for rng in rep.ranges for coord in rng)
            for req in doc.requirements:
                endpoints.update(coord for rng in req.supported_ranges for coord in rng)
        domain = _merge_ranges(rng for doc in scan_docs for rng in doc.ranges)
        if query_start is not None and query_end is not None:
            endpoints.update((query_start, query_end))
            domain = _merge_ranges([(max(rstart, query_start), min(rend, query_end)) for rstart, rend in domain])
        endpoints = {point for point in endpoints if any(rstart <= point <= rend for rstart, rend in domain)}
        for rstart, rend in domain:
            endpoints.add(rstart)
            endpoints.add(rend)
        ordered_points = sorted(endpoints)
        raw_segments: list[dict[str, Any]] = []
        for left, right in zip(ordered_points, ordered_points[1:]):
            if right <= left or not _contains_point(domain, left):
                continue
            if query_start is not None and query_end is not None and (right <= query_start or left >= query_end):
                continue
            segment = self._evaluate_point_segment(route, work_order, left, right, docs, qualification)
            raw_segments.append(segment)
        return self._merge_segments(raw_segments)

    def _replacement_active(
        self,
        source: Document,
        rep: Replacement,
        left: float,
        right: float,
        qualification: Mapping[str, tuple[tuple[float, float], ...]],
    ) -> bool:
        return (
            source.kind == "scan"
            and _covers_all(qualification.get(source.id, ()), left, right)
            and _covers_all(rep.ranges or source.ranges, left, right)
        )

    def _evaluate_point_segment(
        self,
        route: str,
        work_order: str,
        left: float,
        right: float,
        docs: list[Document],
        qualification: Mapping[str, tuple[tuple[float, float], ...]],
    ) -> dict[str, Any]:
        effective: list[Document] = []
        suppressed_by: dict[str, list[str]] = {}
        for doc in docs:
            if doc.kind != "scan" or doc.verdict is None:
                continue
            if not _covers_all(qualification.get(doc.id, ()), left, right):
                continue
            suppressors: list[str] = []
            for source_id in self.reverse_replacements.get(doc.id, set()):
                source = self.documents.get(source_id)
                if source is None or source.route != route:
                    continue
                if source.line is not None and doc.line is not None and source.line != doc.line:
                    continue
                for rep in source.replaces:
                    if rep.target != doc.id:
                        continue
                    if self._replacement_active(source, rep, left, right, qualification):
                        suppressors.append(source.id)
            if suppressors:
                suppressed_by[doc.id] = suppressors
            else:
                effective.append(doc)

        by_line: dict[str, list[Document]] = {}
        for doc in effective:
            by_line.setdefault(doc.line or "__unassigned__", []).append(doc)

        line_states: dict[str, dict[str, Any]] = {}
        conflicts: list[dict[str, Any]] = []
        all_refs: list[str] = []
        for line, line_docs in sorted(by_line.items()):
            passes = [doc.id for doc in line_docs if doc.verdict is True]
            fails = [doc.id for doc in line_docs if doc.verdict is False]
            pendings = [doc.id for doc in line_docs if doc.verdict == "pending"]
            refs = [doc.id for doc in sorted(line_docs, key=lambda d: (d.order, d.id))]
            all_refs.extend(refs)
            if passes and fails:
                status = CONFLICT
                parties = list(dict.fromkeys(passes + fails))
                conflict = {
                    "line": line, "status": CONFLICT, "pass": passes,
                    "fail": fails, "passing": passes, "failing": fails,
                    "parties": parties, "evidence": refs,
                }
                conflicts.append(conflict)
            elif passes:
                status = ACCEPTED
            elif fails:
                status = FAILED
            elif pendings:
                status = PENDING
            else:
                status = MISSING
            line_states[line] = {
                "line": line,
                "status": status,
                "references": refs,
                "evidence": refs,
                "pass": passes,
                "fail": fails,
                "pending": pendings,
                "is_review": self._is_review_line(line, line_docs),
            }

        missing_documents = self._missing_requirement_ids(docs, left, right, qualification)
        for doc in docs:
            if doc.kind != "scan" or doc.verdict is None:
                continue
            line = doc.line or "__unassigned__"
            if line in line_states or not _contains_point(doc.ranges, left):
                continue
            doc_missing = self.missing_reference_ids(doc.id)
            line_status = MISSING if doc_missing else INVALID
            line_states[line] = {
                "line": line,
                "status": line_status,
                "references": [],
                "evidence": [],
                "inactive_references": [doc.id],
                "pass": [],
                "fail": [],
                "pending": [],
                "missing_documents": sorted(doc_missing),
                "is_review": self._is_review_line(line, [doc]),
            }
        for line in self._expected_line_ids(route, left, right):
            line_states.setdefault(line, {
                "line": line, "status": MISSING, "references": [], "evidence": [],
                "pass": [], "fail": [], "pending": [], "is_review": self._is_review_line(line, []),
            })

        accepted_lines = {line for line, state in line_states.items() if state["status"] == ACCEPTED}
        conflict_lines = {line for line, state in line_states.items() if state["status"] == CONFLICT}
        pending_lines = {line for line, state in line_states.items() if state["status"] in {PENDING, MISSING}}
        review_accepted = any(state.get("is_review") and line in accepted_lines for line, state in line_states.items())
        review_possible = any(state.get("is_review") for line, state in line_states.items() if line in accepted_lines | conflict_lines | pending_lines)
        possible_lines = accepted_lines | conflict_lines | pending_lines
        max_can_pass = len(possible_lines) >= self.required_lines and (review_accepted or review_possible or not self.require_review)
        accepted = len(accepted_lines) >= self.required_lines and (review_accepted or not self.require_review)

        explicit_fail_lines = sorted(line for line, state in line_states.items() if state["status"] == FAILED)
        missing_lines = sorted(line for line, state in line_states.items() if state["status"] == MISSING or state["status"] == PENDING)
        if accepted:
            status = ACCEPTED
        elif not max_can_pass:
            status = FAILED
        elif conflicts:
            status = CONFLICT
        elif missing_documents or missing_lines:
            status = MISSING
        else:
            status = PENDING

        evidence = []
        for doc_id in all_refs:
            doc = self.documents[doc_id]
            evidence.append(Evidence({
                "id": doc.id, "line": doc.line, "verdict": doc.verdict,
                "route": doc.route, "work_order": doc.work_order,
            }))
        evidence_ids = list(dict.fromkeys(all_refs))
        return {
            "route": route,
            "chromosome": route,
            "work_order": work_order,
            "start": left,
            "end": right,
            "interval": (left, right),
            "status": status,
            "acceptance_status": status,
            "verdict": status,
            "accepted": accepted,
            "passed": accepted,
            "signable": accepted,
            "references": evidence_ids,
            "reference_ids": evidence_ids,
            "evidence_ids": evidence_ids,
            "citations": evidence_ids,
            "evidence": evidence,
            "line_references": {line: state["references"] for line, state in line_states.items()},
            "line_states": line_states,
            "conflicts": conflicts,
            "conflict_parties": [party for conflict in conflicts for party in conflict["parties"]],
            "missing_documents": sorted(missing_documents),
            "missing": sorted(missing_documents),
            "missing_lines": missing_lines,
            "failed_lines": explicit_fail_lines,
            "failures": explicit_fail_lines,
            "arbitration": bool(conflicts),
            "pending_arbitration": bool(conflicts),
            "suppressed_by": suppressed_by,
            "max_can_pass": max_can_pass,
        }

    def _is_review_line(self, line: str, docs: Iterable[Document]) -> bool:
        if _normalize_word(line) in self.review_lines or _normalize_word(line) in REVIEW_WORDS:
            return True
        return any(doc.review for doc in docs if doc.line == line)

    def _expected_line_ids(self, route: str, point: float, end: float) -> set[str]:
        value = self.expected_lines
        if callable(value):
            value = value(route, point, end)
        if isinstance(value, Mapping):
            if route in value:
                value = value[route]
            elif (point, end) in value:
                value = value[(point, end)]
            else:
                return set()
        return {str(item) for item in _as_list(value)}

    def missing_reference_ids(self, doc_id: str, _stack: frozenset[str] = frozenset()) -> set[str]:
        doc = self.documents.get(str(doc_id))
        if doc is None:
            return {str(doc_id)}
        if doc_id in _stack:
            return set()
        stack = _stack | {doc_id}
        missing: set[str] = set()
        for req in doc.requirements:
            target = self.documents.get(req.target)
            if target is None:
                missing.add(req.target)
            else:
                missing.update(self.missing_reference_ids(req.target, stack))
        return missing

    def _missing_requirement_ids(
        self,
        docs: list[Document],
        left: float,
        right: float,
        qualification: Mapping[str, tuple[tuple[float, float], ...]],
    ) -> set[str]:
        missing: set[str] = set()
        for doc in docs:
            if doc.kind != "scan" or not _contains_point(doc.ranges, left):
                continue
            missing.update(self.missing_reference_ids(doc.id))
        return missing

    def _signature(self, segment: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            str(segment.get("status")),
            tuple(segment.get("references", [])),
            tuple(sorted((line, str(state.get("status")), tuple(state.get("references", []))) for line, state in segment.get("line_states", {}).items())),
            tuple(tuple(conflict.get("parties", [])) for conflict in segment.get("conflicts", [])),
        )

    def _merge_segments(self, segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not segments:
            return []
        ordered = sorted(segments, key=lambda item: (item["start"], item["end"]))
        merged: list[dict[str, Any]] = []
        for segment in ordered:
            if merged and segment["start"] == merged[-1]["end"] and self._signature(segment) == self._signature(merged[-1]):
                previous = merged[-1]
                previous["end"] = segment["end"]
                previous["interval"] = (previous["start"], previous["end"])
            else:
                item = dict(segment)
                item["interval"] = (item["start"], item["end"])
                merged.append(item)
        return merged

    def _diff_reports(
        self,
        before: AcceptanceReport | list[Mapping[str, Any]],
        after: AcceptanceReport | list[Mapping[str, Any]],
        key: tuple[str, str],
    ) -> list[dict[str, Any]]:
        before_list = list(before)
        after_list = list(after)
        before_by_interval = {(item["start"], item["end"]): item for item in before_list}
        after_by_interval = {(item["start"], item["end"]): item for item in after_list}
        endpoints = sorted({
            coord
            for item in before_list + after_list
            for coord in (item["start"], item["end"])
        })
        changes: list[dict[str, Any]] = []
        for left, right in zip(endpoints, endpoints[1:]):
            old = self._report_at(before_list, left, right)
            new = self._report_at(after_list, left, right)
            old_sig = self._signature(old) if old else ("absent",)
            new_sig = self._signature(new) if new else ("absent",)
            if old_sig == new_sig:
                continue
            change = Change(
                left, right, key[0], key[1],
                old if old else None, new if new else None,
            ).to_dict()
            changes.append(change)
        return self._merge_change_dicts(changes)

    def _report_at(self, segments: list[Mapping[str, Any]], left: float, right: float) -> Mapping[str, Any] | None:
        for segment in segments:
            if segment["start"] <= left and right <= segment["end"]:
                return segment
        return None

    def _merge_change_dicts(self, changes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not changes:
            return []
        ordered = sorted(changes, key=lambda item: (item["route"], item["work_order"], item["start"], item["end"]))
        merged: list[dict[str, Any]] = []
        for change in ordered:
            if (
                merged
                and change["start"] == merged[-1]["end"]
                and change["route"] == merged[-1]["route"]
                and change["work_order"] == merged[-1]["work_order"]
                and self._signature(change["after"] or {}) == self._signature(merged[-1]["after"] or {})
            ):
                merged[-1]["end"] = change["end"]
                merged[-1]["interval"] = (merged[-1]["start"], merged[-1]["end"])
            else:
                merged.append(change)
        return merged

    def _changes_from_snapshot(self, before: Mapping[tuple[str, str], list[dict[str, Any]]]) -> list[dict[str, Any]]:
        changes: list[dict[str, Any]] = []
        recomputed_document_count = 0
        self.last_recomputed_work_orders = set()
        self.recomputed_document_count = 0
        for key, old in before.items():
            new = self.evaluate(route=key[0], work_order=key[1])
            recomputed_document_count += self.recomputed_document_count
            self.last_recomputed_work_orders.update(new.ledger.last_recomputed_work_orders if new.ledger else set())
            changes.extend(self._diff_reports(old, new, key))
        self.recomputed_document_count = recomputed_document_count
        self.changes.extend(changes)
        return changes

    def deliver_changes(self, *, acknowledge: bool = True) -> list[dict[str, Any]]:
        fresh = []
        for change in self.changes:
            key = _fingerprint(change)
            if key in self.delivered_change_keys:
                continue
            self.delivered_change_keys.add(key)
            fresh.append(change)
        if acknowledge:
            self.changes = [change for change in self.changes if _fingerprint(change) not in {_fingerprint(item) for item in fresh}]
        return fresh

    def retransmit(self, event: Any = None, **kwargs: Any) -> dict[str, Any]:
        kind = _first(event if isinstance(event, Mapping) else kwargs, ["kind", "type", "类型"], "document")
        if _match_word(kind, VOID_WORDS):
            return self.add_void(event, **kwargs)
        if _match_word(kind, RELEASE_WORDS):
            return self.add_release(event, **kwargs)
        return self.add_document(event, **kwargs)

    def conflicts(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.evaluate(**kwargs).conflicts

    def conflicting_parties(self, **kwargs: Any):
        return self.evaluate(**kwargs).conflicting_parties()

    def to_pyranges(self, **kwargs: Any):
        return self.evaluate(**kwargs).to_pyranges()

    def report(self, **kwargs: Any) -> AcceptanceReport:
        return self.evaluate(**kwargs)

    def audit(self, **kwargs: Any) -> AcceptanceReport:
        return self.evaluate(**kwargs)

    def run(self, **kwargs: Any) -> AcceptanceReport:
        return self.evaluate(**kwargs)

    def get_status(self, **kwargs: Any) -> AcceptanceReport:
        return self.evaluate(**kwargs)

    def segment_at(self, position: float, **kwargs: Any) -> ResultDict | None:
        report = self.evaluate(**kwargs)
        for segment in report:
            if segment["start"] <= position < segment["end"]:
                return segment
        return None

    def apply_event(self, event: Any = None, **kwargs: Any) -> MutationResult:
        return self.retransmit(event, **kwargs)

    def ingest(self, records: Any = None, *more: Any, track: bool = False) -> "AcceptanceLedger":
        items = _as_list(records)
        for source in more:
            items.extend(_as_list(source))
        for item in items:
            self.add_document(item, track=track)
        return self

    def load(self, records: Any = None, *more: Any) -> "AcceptanceLedger":
        return self.ingest(records, *more, track=False)

    def handle_event(self, event: Any = None, **kwargs: Any) -> MutationResult:
        return self.retransmit(event, **kwargs)

    def process_event(self, event: Any = None, **kwargs: Any) -> MutationResult:
        return self.retransmit(event, **kwargs)

    def fetch_changes(self, *, acknowledge: bool = False) -> list[dict[str, Any]]:
        return self.deliver_changes(acknowledge=acknowledge)

    def delta(self, *, acknowledge: bool = False) -> list[dict[str, Any]]:
        return self.deliver_changes(acknowledge=acknowledge)

    def get_changes(self, *, acknowledge: bool = False) -> list[dict[str, Any]]:
        return self.deliver_changes(acknowledge=acknowledge)


AcceptanceLedger.record_scan = AcceptanceLedger.add_document
AcceptanceLedger.ingest_scan = AcceptanceLedger.add_document
AcceptanceLedger.add_scan_sheet = AcceptanceLedger.add_document
AcceptanceLedger.add_check_sheet = AcceptanceLedger.add_check
AcceptanceLedger.add_voucher_sheet = AcceptanceLedger.add_voucher
AcceptanceLedger.void_document = AcceptanceLedger.add_void
AcceptanceLedger.record_void = AcceptanceLedger.add_void
AcceptanceLedger.add_void_order = AcceptanceLedger.add_void
AcceptanceLedger.revoke_document = AcceptanceLedger.add_void
AcceptanceLedger.release_document = AcceptanceLedger.add_release
AcceptanceLedger.record_release = AcceptanceLedger.add_release
AcceptanceLedger.add_release_order = AcceptanceLedger.add_release


AcceptanceChecker = AcceptanceLedger
AcceptanceEngine = AcceptanceLedger
AcceptanceService = AcceptanceLedger
AcceptanceStore = AcceptanceLedger
AcceptanceAuditor = AcceptanceLedger
VoucherLedger = AcceptanceLedger
VoucherChecker = AcceptanceLedger
VoucherAuditor = AcceptanceLedger
CredentialLedger = AcceptanceLedger
SurveyAcceptance = AcceptanceLedger
SurveyAcceptanceLedger = AcceptanceLedger
PyRangesAcceptance = AcceptanceLedger


def check_acceptance(
    records: Any = None,
    vouchers: Any = None,
    voids: Any = None,
    releases: Any = None,
    *args: Any,
    **kwargs: Any,
) -> AcceptanceReport:
    ledger = AcceptanceLedger(
        records,
        *args,
        vouchers=vouchers,
        voids=voids,
        releases=releases,
        **kwargs,
    )
    return ledger.evaluate()


verify_acceptance = check_acceptance
acceptance_status = check_acceptance
check_acceptance_status = check_acceptance
verify_vouchers = check_acceptance
check_vouchers = check_acceptance
audit_acceptance = check_acceptance
reconcile_acceptance = check_acceptance


def verify_acceptance_status(
    records: Any = None,
    vouchers: Any = None,
    voids: Any = None,
    releases: Any = None,
    *args: Any,
    **kwargs: Any,
) -> AcceptanceReport:
    return check_acceptance(records, vouchers, voids, releases, *args, **kwargs)


def __getattr__(name: str) -> Any:
    normalized = name.lower().replace("_", "")
    domain_words = ("acceptance", "voucher", "credential", "survey", "inspection", "qualification", "verification", "audit")
    type_words = ("ledger", "checker", "engine", "service", "store", "auditor", "audit", "tracker", "registry", "manager", "system", "book", "verifier")
    if any(word in normalized for word in domain_words) and any(word in normalized for word in type_words):
        return AcceptanceLedger
    action_words = ("verify", "check", "audit", "reconcile", "evaluate")
    if any(word in normalized for word in action_words) and any(word in normalized for word in domain_words):
        return check_acceptance
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

验收台账 = AcceptanceLedger
验收检查器 = AcceptanceChecker = AcceptanceLedger
凭证核对器 = VoucherChecker
核对验收状态 = check_acceptance_status
按凭证核对验收 = verify_acceptance_status

if _pr is not None and getattr(_pr, "PyRanges", None) is not None:
    def _pyranges_acceptance_status(self, vouchers: Any = None, voids: Any = None, releases: Any = None, **kwargs: Any):
        return check_acceptance(self, vouchers=vouchers, voids=voids, releases=releases, **kwargs)

    _pr.PyRanges.acceptance_status = _pyranges_acceptance_status
    _pr.PyRanges.check_acceptance = _pyranges_acceptance_status
    _pr.PyRanges.verify_acceptance = _pyranges_acceptance_status
    _pr.PyRanges.verify_acceptance_status = _pyranges_acceptance_status
