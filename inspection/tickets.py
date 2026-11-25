"""Bitemporal, incremental clustering for inspection alarms.

The public surface is intentionally small but accepts both domain objects and
plain dictionaries.  A ledger entry carries two independent coordinates:

* ``business_version`` -- the first business version where its state applies;
* ``sequence`` -- the immutable ledger/posting order.

Queries use ``(business_version, cutoff)`` and therefore reproduce a daily
report even after late corrections have been imported.
"""

from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field, replace
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple


DEFAULT_LINE = ""
DEFAULT_DIRECTION = ""

__all__ = [
    "Alarm",
    "Alert",
    "Update",
    "AlarmUpdate",
    "Correction",
    "Ticket",
    "WorkOrder",
    "DailyReport",
    "DailyReportRecord",
    "DailyReportBook",
    "VersionedTicketStore",
    "TicketStore",
    "InspectionTicketSystem",
    "LocalTicketIndex",
    "IncrementalTicketClusterer",
    "cluster_alarms",
    "cluster_alarms_sweep",
    "cluster_alarms_with_pyranges",
    "alarms_to_pyranges",
    "diff_tickets",
    "build_scenario_store",
]


class ConflictError(ValueError):
    """Raised when an idempotency key is reused with different content."""


def _first(mapping: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in mapping and mapping[name] is not None:
            return mapping[name]
    return default


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "__dict__"):
        return {k: v for k, v in vars(value).items() if not k.startswith("_")}
    raise TypeError(f"expected a mapping or domain object, got {type(value)!r}")


def _number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{field_name} is required")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric, got {value!r}") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


def _integer(value: Any, field_name: str) -> int:
    number = _number(value, field_name)
    if not number.is_integer():
        raise ValueError(f"{field_name} must be an integer, got {value!r}")
    return int(number)


def _time_token(value: Any) -> Tuple[str, Any]:
    if value is None:
        return ("none", None)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return ("number", float(value))
    return ("value", str(value))


def _id_token(value: Any) -> Tuple[str, Any]:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return ("number", float(value))
    return ("value", str(value))


@dataclass(frozen=True)
class Alarm:
    alarm_id: Any
    start: float
    end: float
    first_reported_at: Any
    line: str = DEFAULT_LINE
    direction: str = DEFAULT_DIRECTION

    def __post_init__(self) -> None:
        if self.alarm_id is None:
            raise ValueError("alarm_id is required")
        start = _number(self.start, "start")
        end = _number(self.end, "end")
        if end <= start:
            raise ValueError("alarm interval must have positive length")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(
            self, "line", str(self.line if self.line is not None else DEFAULT_LINE)
        )
        object.__setattr__(
            self,
            "direction",
            str(self.direction if self.direction is not None else DEFAULT_DIRECTION),
        )

    @property
    def id(self) -> Any:
        return self.alarm_id

    @property
    def partition(self) -> Tuple[str, str]:
        return self.line, self.direction

    def overlaps(self, other: "Alarm") -> bool:
        return (
            self.line == other.line
            and self.direction == other.direction
            and self.start < other.end
            and other.start < self.end
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alarm_id": self.alarm_id,
            "id": self.alarm_id,
            "line": self.line,
            "direction": self.direction,
            "start": self.start,
            "end": self.end,
            "first_reported_at": self.first_reported_at,
        }


Alert = Alarm


@dataclass(frozen=True)
class Update:
    alarm_id: Any
    business_version: int
    sequence: int
    operation: str = "upsert"
    start: Optional[float] = None
    end: Optional[float] = None
    first_reported_at: Any = None
    line: str = DEFAULT_LINE
    direction: str = DEFAULT_DIRECTION
    update_id: Optional[str] = None

    @classmethod
    def from_obj(
        cls, obj: Any, *, default_sequence: Optional[int] = None
    ) -> "Update":
        data = _as_mapping(obj)
        alarm_id = _first(
            data, "alarm_id", "AlarmID", "id", "alert_id", "warning_id"
        )
        if alarm_id is None:
            raise ValueError("alarm_id is required")

        business_version = _first(
            data,
            "business_version",
            "biz_version",
            "version",
            "effective_from_version",
            "valid_from_version",
            "from_version",
            "业务版本",
        )
        if business_version is None:
            raise ValueError("business_version is required")

        sequence = _first(
            data,
            "sequence",
            "ledger_sequence",
            "ledger_seq",
            "entry_sequence",
            "record_sequence",
            "posted_sequence",
            "seq",
            "入账顺序号",
        )
        if sequence is None:
            sequence = default_sequence
        if sequence is None:
            raise ValueError("sequence is required")

        operation = str(
            _first(data, "operation", "op", "action", "type", default="upsert")
        ).lower()
        if operation in {
            "delete",
            "deleted",
            "remove",
            "removed",
            "cancel",
            "cancelled",
        }:
            operation = "revoke"
        elif operation in {
            "add",
            "added",
            "create",
            "created",
            "correct",
            "corrected",
            "correction",
            "update",
            "updated",
            "upserted",
            "加入",
            "更正",
        }:
            operation = "upsert"
        elif operation in {"revoke", "revoked", "cancel", "cancelled", "撤销"}:
            operation = "revoke"

        if _first(
            data,
            "revoked",
            "deleted",
            "is_revoked",
            "is_deleted",
            default=False,
        ):
            operation = "revoke"
        active = _first(data, "active", "is_active", default=None)
        if active is False:
            operation = "revoke"
        status = str(_first(data, "status", "state", default="")).lower()
        if status in {"revoked", "deleted", "cancelled", "canceled", "inactive"}:
            operation = "revoke"

        start = _first(
            data, "start", "Start", "start_mileage", "from_mileage", "mileage_start"
        )
        end = _first(
            data, "end", "End", "end_mileage", "to_mileage", "mileage_end"
        )
        reported_at = _first(
            data,
            "first_reported_at",
            "first_report_time",
            "first_seen_at",
            "reported_at",
            "report_time",
            "FirstReportTime",
        )
        line = _first(
            data, "line", "line_id", "route", "Line", "Chromosome", "线路"
        )
        direction = _first(
            data, "direction", "dir", "Direction", "Strand", "strand", "方向"
        )
        update_id = _first(
            data, "update_id", "entry_id", "idempotency_key", "source_update_id"
        )

        return cls(
            alarm_id=alarm_id,
            business_version=_integer(business_version, "business_version"),
            sequence=_integer(sequence, "sequence"),
            operation=operation,
            start=None if start is None else _number(start, "start"),
            end=None if end is None else _number(end, "end"),
            first_reported_at=reported_at,
            line=DEFAULT_LINE if line is None else str(line),
            direction=DEFAULT_DIRECTION if direction is None else str(direction),
            update_id=None if update_id is None else str(update_id),
        )

    @property
    def is_revocation(self) -> bool:
        return self.operation == "revoke"

    def fingerprint(self) -> Tuple[Any, ...]:
        return (
            self.alarm_id,
            self.business_version,
            self.sequence,
            self.operation,
            self.start,
            self.end,
            _time_token(self.first_reported_at),
            self.line,
            self.direction,
        )


AlarmUpdate = Update
Correction = Update


def ticket_leader_id(alarms: Sequence[Alarm]) -> Any:
    if not alarms:
        raise ValueError("cannot choose a ticket leader for an empty cluster")
    return min(
        alarms,
        key=lambda alarm: (
            _time_token(alarm.first_reported_at),
            _id_token(alarm.alarm_id),
        ),
    ).alarm_id


@dataclass(frozen=True)
class Ticket:
    ticket_id: Any
    alarms: Tuple[Alarm, ...]
    line: str = DEFAULT_LINE
    direction: str = DEFAULT_DIRECTION

    @classmethod
    def from_alarms(cls, alarms: Sequence[Alarm]) -> "Ticket":
        if not alarms:
            raise ValueError("a ticket must contain at least one alarm")
        ordered = tuple(sorted(alarms, key=lambda alarm: _id_token(alarm.alarm_id)))
        first = ordered[0]
        return cls(
            ticket_id=ticket_leader_id(ordered),
            alarms=ordered,
            line=first.line,
            direction=first.direction,
        )

    @property
    def id(self) -> Any:
        return self.ticket_id

    @property
    def work_order_id(self) -> Any:
        return self.ticket_id

    @property
    def alarm_ids(self) -> Tuple[Any, ...]:
        return tuple(alarm.alarm_id for alarm in self.alarms)

    @property
    def member_ids(self) -> Tuple[Any, ...]:
        return self.alarm_ids

    @property
    def members(self) -> Set[Any]:
        return set(self.alarm_ids)

    def __contains__(self, alarm_id: object) -> bool:
        return alarm_id in self.members or str(alarm_id) in {
            str(value) for value in self.alarm_ids
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticket_id": self.ticket_id,
            "id": self.ticket_id,
            "work_order_id": self.ticket_id,
            "line": self.line,
            "direction": self.direction,
            "alarm_ids": list(self.alarm_ids),
            "member_ids": list(self.alarm_ids),
            "members": list(self.alarm_ids),
            "leader_id": self.ticket_id,
        }


WorkOrder = Ticket


def cluster_alarms_sweep(alarms: Iterable[Alarm]) -> List[Ticket]:
    """Cluster strict half-open overlaps and transitive overlap chains."""

    by_partition: Dict[Tuple[str, str], List[Alarm]] = {}
    for alarm in alarms:
        by_partition.setdefault(alarm.partition, []).append(alarm)

    tickets: List[Ticket] = []
    for partition_alarms in by_partition.values():
        ordered = sorted(
            partition_alarms,
            key=lambda alarm: (alarm.start, alarm.end, str(alarm.alarm_id)),
        )
        cluster: List[Alarm] = []
        reach = -math.inf
        for alarm in ordered:
            if cluster and alarm.start >= reach:
                tickets.append(Ticket.from_alarms(cluster))
                cluster = []
                reach = -math.inf
            cluster.append(alarm)
            reach = max(reach, alarm.end)
        if cluster:
            tickets.append(Ticket.from_alarms(cluster))

    return sorted(
        tickets,
        key=lambda ticket: (
            ticket.line,
            ticket.direction,
            _time_token(ticket.ticket_id),
            str(ticket.ticket_id),
        ),
    )


def cluster_alarms(alarms: Iterable[Alarm]) -> List[Ticket]:
    return cluster_alarms_sweep(alarms)


def alarms_to_pyranges(alarms: Iterable[Alarm]) -> "pr.PyRanges":
    import pyranges as pr

    rows = list(alarms)
    return pr.from_dict(
        {
            "Chromosome": [
                f"{alarm.line or DEFAULT_LINE}\x1f{alarm.direction or DEFAULT_DIRECTION}"
                for alarm in rows
            ],
            "Start": [alarm.start for alarm in rows],
            "End": [alarm.end for alarm in rows],
            "AlarmID": [str(alarm.alarm_id) for alarm in rows],
            "OriginalAlarmID": [alarm.alarm_id for alarm in rows],
        }
    )


def pyranges_overlap_edges(alarms: Iterable[Alarm]) -> Set[Tuple[str, str]]:
    rows = list(alarms)
    if not rows:
        return set()
    joined = alarms_to_pyranges(rows).join(
        alarms_to_pyranges(rows), apply_strand_suffix=False
    ).df
    edges: Set[Tuple[str, str]] = set()
    if joined.empty:
        return edges
    for left_id, right_id in zip(joined["AlarmID"], joined["AlarmID_b"]):
        if left_id == right_id:
            continue
        edges.add(tuple(sorted((str(left_id), str(right_id)))))  # type: ignore[arg-type]
    return edges


def cluster_alarms_with_pyranges(alarms: Iterable[Alarm]) -> List[Ticket]:
    rows = list(alarms)
    if not rows:
        return []
    by_string_id = {str(alarm.alarm_id): alarm for alarm in rows}
    parent = {alarm_id: alarm_id for alarm_id in by_string_id}

    def find(alarm_id: str) -> str:
        root = alarm_id
        while parent[root] != root:
            root = parent[root]
        while parent[alarm_id] != alarm_id:
            parent[alarm_id], alarm_id = root, parent[alarm_id]
        return root

    for left, right in pyranges_overlap_edges(rows):
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    groups: Dict[str, List[Alarm]] = {}
    for alarm_id, alarm in by_string_id.items():
        groups.setdefault(find(alarm_id), []).append(alarm)
    return [Ticket.from_alarms(group) for group in groups.values()]


@dataclass
class IncrementalUpdateResult:
    old_tickets: List[Ticket]
    new_tickets: List[Ticket]
    affected_ticket_ids: Set[Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "old_tickets": [ticket.to_dict() for ticket in self.old_tickets],
            "new_tickets": [ticket.to_dict() for ticket in self.new_tickets],
            "affected_ticket_ids": list(self.affected_ticket_ids),
        }


class LocalTicketIndex:
    """Incremental index for one currently visible alarm state.

    Only old affected components and components spatially touched by the new
    interval are reclustered.  A line with a million unrelated alarms is not
    scanned.
    """

    def __init__(self, alarms: Optional[Iterable[Alarm]] = None) -> None:
        self.alarms: Dict[Any, Alarm] = {}
        self.tickets: Dict[Any, Ticket] = {}
        self.alarm_ticket: Dict[Any, Any] = {}
        self._hull_keys: Dict[Tuple[str, str], List[Tuple[float, Any]]] = {}
        self._hull_hi: Dict[Any, float] = {}
        if alarms is not None:
            self.rebuild(alarms)

    def rebuild(self, alarms: Iterable[Alarm]) -> List[Ticket]:
        rows = list(alarms)
        self.alarms = {alarm.alarm_id: alarm for alarm in rows}
        self.tickets.clear()
        self.alarm_ticket.clear()
        self._hull_keys.clear()
        self._hull_hi.clear()
        tickets = cluster_alarms_sweep(rows)
        for ticket in tickets:
            self._install_ticket(ticket, sort_keys=False)
        for keys in self._hull_keys.values():
            keys.sort(key=lambda item: (item[0], str(item[1])))
        return tickets

    def _install_ticket(self, ticket: Ticket, *, sort_keys: bool = True) -> None:
        self.tickets[ticket.ticket_id] = ticket
        for alarm_id in ticket.alarm_ids:
            self.alarm_ticket[alarm_id] = ticket.ticket_id
        lo = min(alarm.start for alarm in ticket.alarms)
        hi = max(alarm.end for alarm in ticket.alarms)
        self._hull_hi[ticket.ticket_id] = hi
        keys = self._hull_keys.setdefault((ticket.line, ticket.direction), [])
        key = (lo, ticket.ticket_id)
        if sort_keys:
            position = bisect_left(keys, (lo,))
            keys.insert(position, key)
        else:
            keys.append(key)

    def _remove_ticket(self, ticket_id: Any) -> Optional[Ticket]:
        ticket = self.tickets.pop(ticket_id, None)
        if ticket is None:
            return None
        for alarm_id in ticket.alarm_ids:
            self.alarm_ticket.pop(alarm_id, None)
        self._hull_hi.pop(ticket_id, None)
        keys = self._hull_keys.get((ticket.line, ticket.direction))
        if keys is not None:
            index = bisect_left(keys, (min(a.start for a in ticket.alarms),))
            while index < len(keys) and keys[index][1] != ticket_id:
                index += 1
            if index < len(keys) and keys[index][1] == ticket_id:
                keys.pop(index)
        return ticket

    def _nearby_ticket_ids(self, alarm: Alarm) -> Set[Any]:
        keys = self._hull_keys.get(alarm.partition, [])
        if not keys:
            return set()
        position = bisect_left(keys, (alarm.start,))
        candidate_positions = set()
        if position > 0:
            candidate_positions.add(position - 1)
        for index in range(position, len(keys)):
            lo, ticket_id = keys[index]
            if lo >= alarm.end:
                break
            candidate_positions.add(index)

        result: Set[Any] = set()
        for index in candidate_positions:
            if index >= len(keys):
                continue
            lo, ticket_id = keys[index]
            hi = self._hull_hi[ticket_id]
            if lo < alarm.end and alarm.start < hi:
                result.add(ticket_id)
        return result

    def upsert(self, alarm: Alarm) -> IncrementalUpdateResult:
        old_alarm = self.alarms.get(alarm.alarm_id)
        affected_ids: Set[Any] = set(self._nearby_ticket_ids(alarm))
        if old_alarm is not None:
            old_ticket_id = self.alarm_ticket.get(old_alarm.alarm_id)
            if old_ticket_id is not None:
                affected_ids.add(old_ticket_id)

        old_tickets = [
            ticket for ticket_id in affected_ids if (ticket := self.tickets.get(ticket_id))
        ]
        candidate_alarms: Dict[Any, Alarm] = {}
        for ticket in old_tickets:
            for member in ticket.alarms:
                if member.alarm_id != alarm.alarm_id:
                    candidate_alarms[member.alarm_id] = member

        for ticket_id in affected_ids:
            self._remove_ticket(ticket_id)

        self.alarms[alarm.alarm_id] = alarm
        candidate_alarms[alarm.alarm_id] = alarm
        new_tickets = cluster_alarms_sweep(candidate_alarms.values())
        for ticket in new_tickets:
            self._install_ticket(ticket)

        return IncrementalUpdateResult(
            old_tickets=sorted(old_tickets, key=lambda t: str(t.ticket_id)),
            new_tickets=new_tickets,
            affected_ticket_ids=affected_ids,
        )

    def remove(self, alarm_id: Any) -> IncrementalUpdateResult:
        old_alarm = self.alarms.pop(alarm_id, None)
        if old_alarm is None:
            return IncrementalUpdateResult([], [], set())
        old_ticket_id = self.alarm_ticket.get(alarm_id)
        affected_ids = {old_ticket_id} if old_ticket_id is not None else set()
        old_ticket = self.tickets.get(old_ticket_id) if old_ticket_id is not None else None
        old_tickets = [old_ticket] if old_ticket is not None else []

        candidate_alarms: Dict[Any, Alarm] = {}
        if old_ticket is not None:
            for member in old_ticket.alarms:
                if member.alarm_id != alarm_id:
                    candidate_alarms[member.alarm_id] = member
            self._remove_ticket(old_ticket.ticket_id)

        new_tickets = cluster_alarms_sweep(candidate_alarms.values())
        for ticket in new_tickets:
            self._install_ticket(ticket)
        return IncrementalUpdateResult(
            old_tickets=old_tickets,
            new_tickets=new_tickets,
            affected_ticket_ids=affected_ids,
        )

    def apply(self, update: Update) -> IncrementalUpdateResult:
        if update.is_revocation:
            return self.remove(update.alarm_id)
        existing = self.alarms.get(update.alarm_id)
        if existing is None and (
            update.start is None
            or update.end is None
            or update.first_reported_at is None
        ):
            raise ValueError(f"first active entry for {update.alarm_id!r} is incomplete")
        alarm = Alarm(
            alarm_id=update.alarm_id,
            start=update.start if update.start is not None else existing.start,  # type: ignore[union-attr]
            end=update.end if update.end is not None else existing.end,  # type: ignore[union-attr]
            first_reported_at=(
                update.first_reported_at
                if update.first_reported_at is not None
                else existing.first_reported_at  # type: ignore[union-attr]
            ),
            line=update.line or existing.line,  # type: ignore[union-attr]
            direction=update.direction or existing.direction,  # type: ignore[union-attr]
        )
        return self.upsert(alarm)

    def ticket_for_alarm(self, alarm_id: Any) -> Ticket:
        ticket_id = self.alarm_ticket[alarm_id]
        return self.tickets[ticket_id]

    def get_ticket(self, ticket_id: Any) -> Ticket:
        return self.tickets[ticket_id]

    def all_tickets(self) -> List[Ticket]:
        return cluster_alarms_sweep(self.alarms.values())


IncrementalTicketClusterer = LocalTicketIndex


@dataclass
class TicketChange:
    business_version: Optional[int] = None
    old_ticket_id: Any = None
    ticket_id: Any = None
    new_ticket_id: Any = None
    change_type: str = "updated"
    old_members: Tuple[Any, ...] = ()
    members: Tuple[Any, ...] = ()
    new_members: Tuple[Any, ...] = ()
    added: Tuple[Any, ...] = ()
    removed: Tuple[Any, ...] = ()
    predecessor_ids: Tuple[Any, ...] = ()
    successor_ids: Tuple[Any, ...] = ()

    def __post_init__(self) -> None:
        if self.new_ticket_id is None and self.ticket_id is not None:
            self.new_ticket_id = self.ticket_id
        if self.ticket_id is None and self.new_ticket_id is not None:
            self.ticket_id = self.new_ticket_id
        if not self.new_members and self.members:
            self.new_members = tuple(self.members)
        if not self.members and self.new_members:
            self.members = tuple(self.new_members)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "business_version": self.business_version,
            "old_ticket_id": self.old_ticket_id,
            "ticket_id": self.ticket_id,
            "new_ticket_id": self.new_ticket_id,
            "change_type": self.change_type,
            "old_members": list(self.old_members),
            "members": list(self.members),
            "new_members": list(self.new_members),
            "added": list(self.added),
            "removed": list(self.removed),
            "predecessor_ids": list(self.predecessor_ids),
            "successor_ids": list(self.successor_ids),
        }


@dataclass
class VersionChange:
    business_version: int
    old_cutoff: int
    new_cutoff: int
    ticket_changes: List[TicketChange] = field(default_factory=list)
    lineage: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def changes(self) -> List[TicketChange]:
        return self.ticket_changes

    @property
    def changed_business_version(self) -> int:
        return self.business_version

    def to_dict(self) -> Dict[str, Any]:
        return {
            "business_version": self.business_version,
            "old_cutoff": self.old_cutoff,
            "new_cutoff": self.new_cutoff,
            "ticket_changes": [change.to_dict() for change in self.ticket_changes],
            "changes": [change.to_dict() for change in self.ticket_changes],
            "lineage": self.lineage,
        }


@dataclass(frozen=True)
class DailyReport:
    business_version: int
    cutoff: int
    ticket_id: Any = None
    report_date: Optional[str] = None
    members: Tuple[Any, ...] = ()
    ticket_ids: Tuple[Any, ...] = ()

    @property
    def ledger_cutoff(self) -> int:
        return self.cutoff

    @property
    def entry_cutoff(self) -> int:
        return self.cutoff

    def key(self) -> Tuple[int, int, Any]:
        return self.business_version, self.cutoff, self.ticket_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_date": self.report_date,
            "business_version": self.business_version,
            "version": self.business_version,
            "cutoff": self.cutoff,
            "ledger_cutoff": self.cutoff,
            "entry_cutoff": self.cutoff,
            "ticket_id": self.ticket_id,
            "members": list(self.members),
            "ticket_ids": list(self.ticket_ids),
        }


DailyReportRecord = DailyReport


@dataclass
class DailyReportBook:
    business_version: int
    cutoff: int
    report_date: Optional[str]
    reports: List[DailyReport]

    @property
    def ticket_ids(self) -> List[Any]:
        return [report.ticket_id for report in self.reports]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_date": self.report_date,
            "business_version": self.business_version,
            "cutoff": self.cutoff,
            "ledger_cutoff": self.cutoff,
            "ticket_ids": self.ticket_ids,
            "reports": [report.to_dict() for report in self.reports],
        }


def diff_ticket_sets(
    old_tickets: Sequence[Ticket],
    new_tickets: Sequence[Ticket],
    *,
    business_version: Optional[int] = None,
) -> Tuple[List[TicketChange], List[Dict[str, Any]]]:
    old_by_id = {ticket.ticket_id: ticket for ticket in old_tickets}
    new_by_id = {ticket.ticket_id: ticket for ticket in new_tickets}
    old_member_ticket = {
        alarm_id: ticket.ticket_id
        for ticket in old_tickets
        for alarm_id in ticket.alarm_ids
    }
    new_member_ticket = {
        alarm_id: ticket.ticket_id
        for ticket in new_tickets
        for alarm_id in ticket.alarm_ids
    }

    changes: List[TicketChange] = []
    lineage: List[Dict[str, Any]] = []
    handled_new: Set[Any] = set()

    for old_id in sorted(old_by_id, key=lambda value: str(value)):
        old_ticket = old_by_id[old_id]
        successor_ids = sorted(
            {
                new_member_ticket[alarm_id]
                for alarm_id in old_ticket.alarm_ids
                if alarm_id in new_member_ticket
            },
            key=lambda value: str(value),
        )
        if not successor_ids:
            changes.append(
                TicketChange(
                    business_version=business_version,
                    old_ticket_id=old_id,
                    ticket_id=None,
                    change_type="deleted",
                    old_members=old_ticket.alarm_ids,
                    members=(),
                    predecessor_ids=(old_id,),
                    successor_ids=(),
                )
            )
            lineage.append(
                {
                    "change_type": "delete",
                    "old_ticket_id": old_id,
                    "successor_ids": [],
                }
            )
            continue

        lineage_type = (
            "unchanged"
            if len(successor_ids) == 1 and successor_ids[0] == old_id
            else "split"
            if len(successor_ids) > 1
            else "renamed"
        )
        lineage.append(
            {
                "change_type": lineage_type,
                "old_ticket_id": old_id,
                "successor_ids": successor_ids,
            }
        )

        for new_id in successor_ids:
            handled_new.add(new_id)
            new_ticket = new_by_id[new_id]
            old_set = set(old_ticket.alarm_ids)
            new_set = set(new_ticket.alarm_ids)
            retained = tuple(sorted(old_set & new_set, key=lambda value: str(value)))
            removed = tuple(sorted(old_set - new_set, key=lambda value: str(value)))
            added = tuple(sorted(new_set - old_set, key=lambda value: str(value)))

            if len(successor_ids) > 1 and new_id != old_id:
                change_type = "split_created"
                export_added = new_ticket.alarm_ids
                export_removed: Tuple[Any, ...] = ()
            elif len(successor_ids) > 1:
                change_type = "split_retained"
                export_added = added
                export_removed = removed
            elif new_id != old_id:
                change_type = "renamed"
                export_added = added
                export_removed = removed
            elif added or removed:
                change_type = "updated"
                export_added = added
                export_removed = removed
            else:
                change_type = "unchanged"
                export_added = ()
                export_removed = ()

            if change_type == "unchanged":
                continue

            changes.append(
                TicketChange(
                    business_version=business_version,
                    old_ticket_id=old_id,
                    ticket_id=new_id,
                    change_type=change_type,
                    old_members=old_ticket.alarm_ids,
                    members=new_ticket.alarm_ids,
                    added=export_added,
                    removed=export_removed,
                    predecessor_ids=(old_id,),
                    successor_ids=tuple(successor_ids),
                )
            )

    for new_id in sorted(new_by_id, key=lambda value: str(value)):
        if new_id in handled_new:
            continue
        new_ticket = new_by_id[new_id]
        predecessor_ids = sorted(
            {
                old_member_ticket[alarm_id]
                for alarm_id in new_ticket.alarm_ids
                if alarm_id in old_member_ticket
            },
            key=lambda value: str(value),
        )
        changes.append(
            TicketChange(
                business_version=business_version,
                old_ticket_id=predecessor_ids[0] if predecessor_ids else None,
                ticket_id=new_id,
                change_type="merged" if predecessor_ids else "created",
                members=new_ticket.alarm_ids,
                added=new_ticket.alarm_ids,
                predecessor_ids=tuple(predecessor_ids),
                successor_ids=(new_id,),
            )
        )

    return changes, lineage


def diff_tickets(
    old_tickets: Sequence[Ticket],
    new_tickets: Sequence[Ticket],
    *,
    focus_alarm_ids: Optional[Set[Any]] = None,
    business_version: Optional[int] = None,
) -> List[TicketChange]:
    if focus_alarm_ids is None:
        focused_old = list(old_tickets)
        focused_new = list(new_tickets)
    else:
        old_ids = {
            ticket.ticket_id
            for ticket in old_tickets
            if any(alarm_id in focus_alarm_ids for alarm_id in ticket.alarm_ids)
        }
        new_ids = {
            ticket.ticket_id
            for ticket in new_tickets
            if any(alarm_id in focus_alarm_ids for alarm_id in ticket.alarm_ids)
        }
        focused_old = [ticket for ticket in old_tickets if ticket.ticket_id in old_ids]
        focused_new = [ticket for ticket in new_tickets if ticket.ticket_id in new_ids]

    changes, _ = diff_ticket_sets(
        focused_old, focused_new, business_version=business_version
    )
    return changes


@dataclass
class _MaterializedView:
    business_version: int
    cutoff: int
    index: LocalTicketIndex


class VersionedTicketStore:
    def __init__(self, *, use_pyranges: bool = False) -> None:
        self.use_pyranges = use_pyranges
        self._updates: List[Update] = []
        self._by_alarm: Dict[Any, List[Update]] = {}
        self._idempotency: Dict[str, Tuple[Any, ...]] = {}
        self._fingerprints: Set[Tuple[Any, ...]] = set()
        self._reports: Dict[Tuple[int, int, Any], DailyReport] = {}
        self._books: Dict[Tuple[Optional[str], int, int], DailyReportBook] = {}
        self._views: Dict[int, _MaterializedView] = {}
        self._ticket_snapshots: Dict[Tuple[int, int], List[Ticket]] = {}
        self._ticket_lookup: Dict[Tuple[int, int], Dict[Any, Ticket]] = {}
        self._ticket_string_lookup: Dict[Tuple[int, int], Dict[str, Ticket]] = {}
        self._alarm_lookup: Dict[Tuple[int, int], Dict[Any, Ticket]] = {}
        self._alarm_string_lookup: Dict[Tuple[int, int], Dict[str, Ticket]] = {}

    @property
    def updates(self) -> Tuple[Update, ...]:
        return tuple(sorted(self._updates, key=lambda update: update.sequence))

    @property
    def max_sequence(self) -> int:
        return max((update.sequence for update in self._updates), default=0)

    @property
    def latest_sequence(self) -> int:
        return self.max_sequence

    @property
    def current_cutoff(self) -> int:
        return self.max_sequence

    @property
    def business_versions(self) -> Tuple[int, ...]:
        return tuple(sorted({update.business_version for update in self._updates}))

    def ingest(
        self,
        updates: Any,
        *,
        sequence: Optional[int] = None,
        source: Optional[str] = None,
    ) -> "VersionedTicketStore":
        del source
        if isinstance(updates, (str, bytes, Mapping)) or not isinstance(updates, Iterable):
            items = [updates]
        else:
            items = list(updates)

        auto_sequence = self.max_sequence
        if sequence is not None:
            auto_sequence = max(auto_sequence, sequence)
        for item in items:
            default_sequence = sequence
            if default_sequence is None and not _has_sequence(item):
                auto_sequence += 1
                default_sequence = auto_sequence
            update = Update.from_obj(item, default_sequence=default_sequence)
            self._ingest_one(update)
        return self

    def import_updates(self, updates: Any, **kwargs: Any) -> "VersionedTicketStore":
        return self.ingest(updates, **kwargs)

    def load(self, updates: Any, **kwargs: Any) -> "VersionedTicketStore":
        return self.ingest(updates, **kwargs)

    def add_update(self, update: Any, **kwargs: Any) -> "VersionedTicketStore":
        return self.ingest(update, **kwargs)

    def _ingest_one(self, update: Update) -> None:
        if update.sequence <= 0:
            raise ValueError("sequence must be positive")
        fingerprint = update.fingerprint()
        if update.update_id is not None:
            if update.update_id in self._idempotency:
                if self._idempotency[update.update_id] != fingerprint:
                    raise ConflictError(
                        f"update_id {update.update_id!r} was already imported with different data"
                    )
                return
            self._idempotency[update.update_id] = fingerprint
        if fingerprint in self._fingerprints:
            return
        self._fingerprints.add(fingerprint)
        self._updates.append(update)
        self._by_alarm.setdefault(update.alarm_id, []).append(update)
        self._invalidate_snapshots(update)

    def _invalidate_snapshots(self, update: Update) -> None:
        stale_snapshots = [
            key
            for key in self._ticket_snapshots
            if update.business_version <= key[0] and update.sequence <= key[1]
        ]
        for key in stale_snapshots:
            self._ticket_snapshots.pop(key, None)
            self._ticket_lookup.pop(key, None)
            self._ticket_string_lookup.pop(key, None)
            self._alarm_lookup.pop(key, None)
            self._alarm_string_lookup.pop(key, None)

        stale_views = [
            business_version
            for business_version, view in self._views.items()
            if update.business_version <= business_version
            and update.sequence <= view.cutoff
        ]
        for business_version in stale_views:
            self._views.pop(business_version, None)

    def _resolve_alarm(
        self, alarm_id: Any, business_version: int, cutoff: int
    ) -> Optional[Alarm]:
        visible = [
            update
            for update in self._by_alarm.get(alarm_id, [])
            if update.business_version <= business_version
            and update.sequence <= cutoff
        ]
        if not visible:
            return None
        visible.sort(key=lambda update: (update.business_version, update.sequence))

        active = False
        data: Dict[str, Any] = {}
        for update in visible:
            if update.is_revocation:
                active = False
                data = {}
                continue
            active = True
            if update.start is not None:
                data["start"] = update.start
            if update.end is not None:
                data["end"] = update.end
            if update.first_reported_at is not None:
                data["first_reported_at"] = update.first_reported_at
            if update.line:
                data["line"] = update.line
            if update.direction:
                data["direction"] = update.direction

        if not active:
            return None
        if data.get("start") is None or data.get("end") is None:
            return None
        if data.get("first_reported_at") is None:
            raise ValueError(
                f"first_reported_at is required for active alarm {alarm_id!r}"
            )
        return Alarm(
            alarm_id=alarm_id,
            start=data["start"],
            end=data["end"],
            first_reported_at=data["first_reported_at"],
            line=data.get("line", DEFAULT_LINE),
            direction=data.get("direction", DEFAULT_DIRECTION),
        )

    def state_at(
        self, business_version: int, cutoff: Optional[int] = None
    ) -> Dict[Any, Alarm]:
        cutoff = self.max_sequence if cutoff is None else cutoff
        result: Dict[Any, Alarm] = {}
        for alarm_id in self._by_alarm:
            alarm = self._resolve_alarm(alarm_id, business_version, cutoff)
            if alarm is not None:
                result[alarm_id] = alarm
        return result

    def alarms_at(
        self, business_version: int, cutoff: Optional[int] = None
    ) -> List[Alarm]:
        return list(self.state_at(business_version, cutoff).values())

    def _store_ticket_snapshot(
        self, business_version: int, cutoff: int, tickets: Sequence[Ticket]
    ) -> None:
        cache_key = (business_version, cutoff)
        ticket_list = list(tickets)
        self._ticket_snapshots[cache_key] = ticket_list
        self._ticket_lookup[cache_key] = {
            ticket.ticket_id: ticket for ticket in ticket_list
        }
        self._ticket_string_lookup[cache_key] = {
            str(ticket.ticket_id): ticket for ticket in ticket_list
        }
        self._alarm_lookup[cache_key] = {
            alarm_id: ticket
            for ticket in ticket_list
            for alarm_id in ticket.alarm_ids
        }
        self._alarm_string_lookup[cache_key] = {
            str(alarm_id): ticket
            for ticket in ticket_list
            for alarm_id in ticket.alarm_ids
        }

    def tickets_at(
        self,
        business_version: int,
        cutoff: Optional[int] = None,
        *,
        line: Optional[str] = None,
        direction: Optional[str] = None,
    ) -> List[Ticket]:
        cutoff = self.max_sequence if cutoff is None else cutoff
        cache_key = (business_version, cutoff)
        if cache_key not in self._ticket_snapshots:
            alarms = self.alarms_at(business_version, cutoff)
            if self.use_pyranges:
                tickets = cluster_alarms_with_pyranges(alarms)
            else:
                tickets = cluster_alarms_sweep(alarms)
            self._store_ticket_snapshot(business_version, cutoff, tickets)
        tickets = self._ticket_snapshots[cache_key]
        if line is not None:
            tickets = [ticket for ticket in tickets if ticket.line == str(line)]
        if direction is not None:
            tickets = [
                ticket for ticket in tickets if ticket.direction == str(direction)
            ]
        return list(tickets)

    def cluster(
        self, business_version: int, cutoff: Optional[int] = None, **filters: Any
    ) -> List[Ticket]:
        return self.tickets_at(business_version, cutoff, **filters)

    def _materialized_view(
        self, business_version: int, cutoff: int
    ) -> _MaterializedView:
        view = self._views.get(business_version)
        if view is None or view.cutoff > cutoff:
            alarms = self.alarms_at(business_version, cutoff)
            index = LocalTicketIndex(alarms)
            view = _MaterializedView(business_version, cutoff, index)
            self._views[business_version] = view
            return view

        if view.cutoff < cutoff:
            self._advance_view(view, cutoff)
        return view

    def _advance_view(self, view: _MaterializedView, cutoff: int) -> None:
        old_cutoff = view.cutoff
        sequences = sorted(
            {
                update.sequence
                for update in self._updates
                if old_cutoff < update.sequence <= cutoff
            }
        )
        for sequence in sequences:
            alarm_ids = {
                update.alarm_id
                for update in self._updates
                if update.sequence == sequence
            }
            for alarm_id in alarm_ids:
                old_alarm = self._resolve_alarm(
                    alarm_id, view.business_version, old_cutoff
                )
                new_alarm = self._resolve_alarm(
                    alarm_id, view.business_version, cutoff
                )
                if old_alarm == new_alarm:
                    continue
                if new_alarm is None:
                    view.index.remove(alarm_id)
                else:
                    view.index.upsert(new_alarm)
        view.cutoff = cutoff
        self._store_ticket_snapshot(
            view.business_version, cutoff, list(view.index.tickets.values())
        )

    def get_ticket(
        self,
        business_version: Optional[int] = None,
        cutoff: Optional[int] = None,
        ticket_id: Any = None,
        *,
        version: Optional[int] = None,
        ledger_cutoff: Optional[int] = None,
        entry_cutoff: Optional[int] = None,
        as_of: Optional[int] = None,
        alarm_id: Any = None,
    ) -> Ticket:
        business_version = (
            business_version if business_version is not None else version
        )
        cutoff = cutoff if cutoff is not None else ledger_cutoff
        cutoff = cutoff if cutoff is not None else entry_cutoff
        cutoff = cutoff if cutoff is not None else as_of
        if business_version is None or cutoff is None:
            raise ValueError("business_version and cutoff are required")
        business_version = _integer(business_version, "business_version")
        cutoff = _integer(cutoff, "cutoff")

        cache_key = (business_version, cutoff)
        self.tickets_at(business_version, cutoff)

        if ticket_id is not None:
            ticket = self._ticket_lookup[cache_key].get(ticket_id)
            if ticket is None:
                ticket = self._ticket_string_lookup[cache_key].get(str(ticket_id))
            if ticket is not None:
                return ticket
            raise KeyError(
                f"ticket {ticket_id!r} not found at "
                f"business_version={business_version}, cutoff={cutoff}"
            )

        if alarm_id is not None:
            ticket = self._alarm_lookup[cache_key].get(alarm_id)
            if ticket is None:
                ticket = self._alarm_string_lookup[cache_key].get(str(alarm_id))
            if ticket is not None:
                return ticket
            raise KeyError(f"alarm {alarm_id!r} is not on an active ticket")

        raise ValueError("ticket_id or alarm_id is required")

    def find_ticket(
        self, alarm_id: Any, business_version: int, cutoff: Optional[int] = None
    ) -> Ticket:
        return self.get_ticket(
            business_version, cutoff, alarm_id=alarm_id
        )

    def publish_daily_report(
        self,
        business_version: int,
        cutoff: Optional[int] = None,
        *,
        report_date: Optional[str] = None,
    ) -> DailyReportBook:
        cutoff = self.max_sequence if cutoff is None else cutoff
        if self.use_pyranges:
            tickets = self.tickets_at(business_version, cutoff)
        else:
            view = self._materialized_view(business_version, cutoff)
            tickets = sorted(
                view.index.tickets.values(),
                key=lambda ticket: (
                    ticket.line,
                    ticket.direction,
                    _time_token(ticket.ticket_id),
                    str(ticket.ticket_id),
                ),
            )
        self._store_ticket_snapshot(business_version, cutoff, tickets)
        ticket_ids = tuple(ticket.ticket_id for ticket in tickets)
        reports: List[DailyReport] = []
        for ticket in tickets:
            report = DailyReport(
                report_date=report_date,
                business_version=business_version,
                cutoff=cutoff,
                ticket_id=ticket.ticket_id,
                members=ticket.alarm_ids,
                ticket_ids=ticket_ids,
            )
            reports.append(report)
            self._reports[report.key()] = report
        book = DailyReportBook(
            report_date=report_date,
            business_version=business_version,
            cutoff=cutoff,
            reports=reports,
        )
        self._books[(report_date, business_version, cutoff)] = book
        return book

    def save_daily_report(
        self,
        business_version: Optional[int] = None,
        cutoff: Optional[int] = None,
        ticket_id: Any = None,
        *,
        report_date: Optional[str] = None,
        ticket_ids: Optional[Iterable[Any]] = None,
    ) -> Any:
        if isinstance(business_version, DailyReport):
            report = business_version
            self._reports[report.key()] = report
            return report
        if isinstance(business_version, Mapping):
            data = business_version
            report = DailyReport(
                business_version=_integer(
                    _first(data, "business_version", "version"),
                    "business_version",
                ),
                cutoff=_integer(
                    _first(data, "cutoff", "ledger_cutoff", "entry_cutoff"),
                    "cutoff",
                ),
                ticket_id=_first(data, "ticket_id", "id"),
                report_date=_first(data, "report_date", "date"),
                members=tuple(_first(data, "members", default=()) or ()),
                ticket_ids=tuple(_first(data, "ticket_ids", default=()) or ()),
            )
            self._reports[report.key()] = report
            return report

        if ticket_id is not None:
            cutoff = self.max_sequence if cutoff is None else cutoff
            ticket = self.get_ticket(business_version, cutoff, ticket_id)
            report = DailyReport(
                report_date=report_date,
                business_version=business_version,  # type: ignore[arg-type]
                cutoff=cutoff,  # type: ignore[arg-type]
                ticket_id=ticket.ticket_id,
                members=ticket.alarm_ids,
            )
            self._reports[report.key()] = report
            return report

        return self.publish_daily_report(
            business_version,  # type: ignore[arg-type]
            cutoff,
            report_date=report_date,
        ).reports

    def add_daily_report(self, report: DailyReport) -> None:
        self._reports[report.key()] = report

    def get_daily_report(
        self,
        business_version: Any = None,
        cutoff: Optional[int] = None,
        ticket_id: Any = None,
    ) -> DailyReport:
        if isinstance(business_version, DailyReport):
            report = business_version
            return self.get_daily_report(
                report.business_version, report.cutoff, report.ticket_id
            )
        if isinstance(business_version, Mapping):
            data = business_version
            return self.get_daily_report(
                _first(data, "business_version", "version"),
                _first(data, "cutoff", "ledger_cutoff", "entry_cutoff"),
                _first(data, "ticket_id", "id"),
            )
        if isinstance(business_version, tuple):
            version_value, cutoff_value, ticket_value = business_version
            return self.get_daily_report(version_value, cutoff_value, ticket_value)

        business_version = _integer(business_version, "business_version")
        cutoff = _integer(cutoff, "cutoff")
        key = (business_version, cutoff, ticket_id)
        archived = self._reports.get(key)
        try:
            ticket = self.get_ticket(business_version, cutoff, ticket_id)
            members = ticket.alarm_ids
        except KeyError:
            if archived is None:
                raise
            return archived
        if archived is not None:
            return replace(archived, members=members)
        return DailyReport(
            business_version=business_version,
            cutoff=cutoff,
            ticket_id=ticket_id,
            members=members,
        )

    def query_report(self, *args: Any, **kwargs: Any) -> DailyReport:
        return self.get_daily_report(*args, **kwargs)

    def daily_report(self, *args: Any, **kwargs: Any) -> DailyReport:
        return self.get_daily_report(*args, **kwargs)

    def reconcile(
        self,
        old_cutoff: Optional[int] = None,
        new_cutoff: Optional[int] = None,
        *,
        since: Optional[int] = None,
        business_versions: Optional[Iterable[int]] = None,
        incremental: bool = True,
    ) -> List[VersionChange]:
        if old_cutoff is None:
            old_cutoff = since
        if old_cutoff is None:
            raise ValueError("old_cutoff or since is required")
        new_cutoff = self.max_sequence if new_cutoff is None else new_cutoff
        changed_alarms = {
            update.alarm_id
            for update in self._updates
            if old_cutoff < update.sequence <= new_cutoff
        }
        candidate_versions = self._candidate_versions(
            changed_alarms, business_versions, old_cutoff, new_cutoff
        )

        result: List[VersionChange] = []
        for business_version in candidate_versions:
            if incremental:
                old_tickets, new_tickets = self._incremental_before_after(
                    business_version, old_cutoff, new_cutoff, changed_alarms
                )
            else:
                old_tickets = self.tickets_at(business_version, old_cutoff)
                new_tickets = self.tickets_at(business_version, new_cutoff)
            changes, lineage = diff_ticket_sets(
                old_tickets, new_tickets, business_version=business_version
            )
            if changes:
                result.append(
                    VersionChange(
                        business_version=business_version,
                        old_cutoff=old_cutoff,
                        new_cutoff=new_cutoff,
                        ticket_changes=changes,
                        lineage=lineage,
                    )
                )
        return result

    def _incremental_before_after(
        self,
        business_version: int,
        old_cutoff: int,
        new_cutoff: int,
        changed_alarms: Set[Any],
    ) -> Tuple[List[Ticket], List[Ticket]]:
        view = self._materialized_view(business_version, old_cutoff)
        index = view.index
        old_ticket_ids = {
            index.alarm_ticket[alarm_id]
            for alarm_id in changed_alarms
            if alarm_id in index.alarm_ticket
        }
        for alarm_id in changed_alarms:
            new_alarm = self._resolve_alarm(
                alarm_id, business_version, new_cutoff
            )
            if new_alarm is not None:
                old_ticket_ids.update(index._nearby_ticket_ids(new_alarm))
        old_tickets = [
            index.tickets[ticket_id]
            for ticket_id in old_ticket_ids
            if ticket_id in index.tickets
        ]
        old_member_ids = {
            alarm_id
            for ticket in old_tickets
            for alarm_id in ticket.alarm_ids
        }

        self._advance_view(view, new_cutoff)
        new_ticket_ids = {
            index.alarm_ticket[alarm_id]
            for alarm_id in old_member_ids | changed_alarms
            if alarm_id in index.alarm_ticket
        }
        new_tickets = [
            index.tickets[ticket_id]
            for ticket_id in new_ticket_ids
            if ticket_id in index.tickets
        ]
        return old_tickets, new_tickets

    def changes_since(
        self, cutoff: int, new_cutoff: Optional[int] = None, **kwargs: Any
    ) -> List[VersionChange]:
        return self.reconcile(cutoff, new_cutoff, **kwargs)

    def _candidate_versions(
        self,
        changed_alarms: Set[Any],
        explicit_versions: Optional[Iterable[int]],
        old_cutoff: int,
        new_cutoff: int,
    ) -> List[int]:
        if explicit_versions is not None:
            return sorted({int(version) for version in explicit_versions})

        delta_updates = [
            update
            for update in self._updates
            if old_cutoff < update.sequence <= new_cutoff
            and update.alarm_id in changed_alarms
        ]
        versions: Set[int] = set()
        known_versions = self.business_versions
        for update in delta_updates:
            alarm_versions = sorted(
                {
                    item.business_version
                    for item in self._by_alarm.get(update.alarm_id, [])
                    if item.sequence <= new_cutoff
                }
            )
            later_versions = [
                version
                for version in alarm_versions
                if version > update.business_version
            ]
            if later_versions:
                stop = min(later_versions)
                versions.update(
                    version
                    for version in known_versions
                    if update.business_version <= version < stop
                )
            else:
                versions.update(
                    version
                    for version in known_versions
                    if version >= update.business_version
                )
        return sorted(versions)


TicketStore = VersionedTicketStore
InspectionTicketSystem = VersionedTicketStore


def _has_sequence(item: Any) -> bool:
    data = _as_mapping(item)
    return _first(
        data,
        "sequence",
        "ledger_sequence",
        "ledger_seq",
        "entry_sequence",
        "record_sequence",
        "posted_sequence",
        "seq",
        "入账顺序号",
    ) is not None


def build_scenario_store(*, corrected: bool = False) -> VersionedTicketStore:
    store = VersionedTicketStore()
    rows = [
        {
            "update_id": "v1-a",
            "sequence": 1,
            "business_version": 1,
            "alarm_id": "A",
            "start": 0,
            "end": 4,
            "first_reported_at": 9,
        },
        {
            "update_id": "v1-b",
            "sequence": 1,
            "business_version": 1,
            "alarm_id": "B",
            "start": 8,
            "end": 12,
            "first_reported_at": 3,
        },
        {
            "update_id": "v1-c",
            "sequence": 1,
            "business_version": 1,
            "alarm_id": "C",
            "start": 10,
            "end": 14,
            "first_reported_at": 6,
        },
        {
            "update_id": "v2-x",
            "sequence": 2,
            "business_version": 2,
            "alarm_id": "X",
            "start": 3,
            "end": 9,
            "first_reported_at": 4,
        },
        {
            "update_id": "v3-x-revoke",
            "sequence": 3,
            "business_version": 3,
            "alarm_id": "X",
            "operation": "revoke",
        },
        {
            "update_id": "v4-b-revoke",
            "sequence": 4,
            "business_version": 4,
            "alarm_id": "B",
            "operation": "revoke",
        },
    ]
    store.ingest(rows)
    if corrected:
        store.ingest(
            {
                "update_id": "v2-x-correction",
                "sequence": 5,
                "business_version": 2,
                "alarm_id": "X",
                "operation": "correct",
                "start": 3,
                "end": 8,
            }
        )
    return store
