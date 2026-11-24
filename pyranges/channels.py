"""Channel scheduling for acquisition write tasks.

A task has a half-open window ``[start, end)`` on a chromosome (the write
line).  Throughout the window exactly one channel carries the *primary*
write.  At an allowed handover time ``t`` the old channel writes until ``t``
and the new channel provides the primary write from ``t`` onwards.  Immediately
before ``t`` the target channel must be warmed up by writing the task to it
continuously for the required number of seconds.  During that warm-up the
primary write stays on the source channel, and the warm-up occupies the same
number of slots as the task.

All times are integer seconds and all intervals are half-open.  Capacity is
given per ``(Chromosome, Channel)`` as half-open intervals with a number of
slots; any time not covered by a capacity row has zero capacity.

Writes already executed before a cutoff are immutable.  An executed warm-up
may only be continued for the same task, source, target and planned handover
point.  Schedules that were issued but never executed are not inputs here:
only the executed records matter.

Among all complete schedules, the number of handovers that have not yet
happened is minimised first, then the number of warm-up slot-seconds newly
added after the cutoff.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional

import pandas as pd

__all__ = [
    "Task",
    "ChannelCapacity",
    "ChannelHandover",
    "ExecutedWrite",
    "ChannelSchedule",
    "ChannelScheduleError",
    "InfeasibleSchedule",
    "schedule_channels",
]


# ---------------------------------------------------------------------------
# Input records
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ChannelCapacity:
    """Available slots on ``Chromosome``/``Channel`` during ``[start, end)``."""

    chromosome: str
    channel: str
    start: int
    end: int
    slots: int = 1

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("capacity start must not exceed end")
        if self.slots < 0:
            raise ValueError("capacity slots must be non-negative")


@dataclass(frozen=True)
class ChannelHandover:
    """Allowed handover ``source -> target`` at ``at`` with warm-up seconds."""

    task: str
    at: int
    source: str
    target: str
    warmup_seconds: int = 0

    def __post_init__(self) -> None:
        if self.warmup_seconds < 0:
            raise ValueError("warmup_seconds must be non-negative")


@dataclass
class ExecutedWrite:
    """A primary-write or warm-up record executed at or before the cutoff.

    Parameters
    ----------
    task, chromosome, channel, start, end
        Identity and half-open span of the record.
    kind : {"primary", "warmup"}
        Record type.
    slots : int
        Slots occupied by the task.
    handover_at : int, optional
        Planned handover point, present only for warm-up records.
    source, target : str, optional
        Handover endpoints.  They can disambiguate a warm-up record; otherwise
        they are inferred from the task's allowed handovers.
    """

    task: str
    chromosome: str
    channel: str
    start: int
    end: int
    kind: str = "primary"
    slots: int = 1
    handover_at: Optional[int] = None
    source: Optional[str] = None
    target: Optional[str] = None

    def __post_init__(self) -> None:
        self.kind = _normalise_kind(self.kind)
        if self.kind not in ("primary", "warmup"):
            raise ValueError("kind must be 'primary' or 'warmup'")
        if self.start > self.end:
            raise ValueError("executed write start must not exceed end")
        if self.kind == "warmup" and self.handover_at is None:
            raise ValueError("warm-up writes require handover_at")
        if self.kind == "primary" and self.handover_at is not None:
            raise ValueError("primary writes must not carry handover_at")
        self.task = str(self.task)
        self.chromosome = str(self.chromosome)
        self.channel = str(self.channel)
        if self.source is not None:
            self.source = str(self.source)
        if self.target is not None:
            self.target = str(self.target)


@dataclass
class Task:
    """A write task.

    Parameters
    ----------
    task : str
        Stable task identifier.
    chromosome : str
        Write line.
    start, end : int
        Half-open task window.
    slots : int
        Number of slots used by the primary write and each warm-up.
    start_channels : sequence of str
        Allowed initial channels.  Empty means any channel with capacity data
        on this chromosome may be used.
    handovers : sequence of ChannelHandover
        Allowed handover points for this task.
    """

    task: str
    chromosome: str
    start: int
    end: int
    slots: int = 1
    start_channels: tuple[str, ...] = ()
    handovers: tuple[ChannelHandover, ...] = ()

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("task start must not exceed end")
        if self.slots <= 0:
            raise ValueError("task slots must be positive")
        self.task = str(self.task)
        self.chromosome = str(self.chromosome)
        self.start_channels = tuple(sorted(str(c) for c in self.start_channels))
        self.handovers = tuple(
            sorted(
                (h for h in self.handovers if isinstance(h, ChannelHandover)),
                key=lambda h: (h.at, h.source, h.target, h.warmup_seconds),
            )
        )
        for handover in self.handovers:
            if handover.task != self.task:
                raise ValueError("handover task does not match owning task")
            if not (self.start <= handover.at <= self.end):
                raise ValueError("handover must lie within the task window")
            if handover.warmup_seconds > handover.at - self.start:
                raise ValueError("warm-up does not fit inside the task window")


# ---------------------------------------------------------------------------
# Errors, conflicts and result
# ---------------------------------------------------------------------------


class ChannelScheduleError(ValueError):
    """Base class for channel-scheduling errors."""


_CONFLICT_COLUMNS = [
    "Chromosome",
    "Channel",
    "Start",
    "End",
    "Task",
    "Kind",
    "Slots",
    "Required",
    "Available",
    "OccupiedBy",
    "Reason",
    "HandoverAt",
]


def _conflict_frame(rows: Iterable[Mapping]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=_CONFLICT_COLUMNS)
    for column in _CONFLICT_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    return frame[_CONFLICT_COLUMNS].reset_index(drop=True)


class InfeasibleSchedule(ChannelScheduleError):
    """No complete schedule exists.

    ``conflicts`` is a :class:`pandas.DataFrame` describing what prevented a
    plan.  ``Kind`` is ``"occupied"`` for capacity conflicts,
    ``"warmup_missing"`` for a warm-up interval that has already partly
    elapsed, ``"primary_missing"`` for an unexecuted part before the cutoff,
    and ``"executed"`` for an executed record that cannot be continued.
    """

    def __init__(self, message: str, conflicts: Optional[pd.DataFrame] = None):
        super().__init__(message)
        if conflicts is None:
            conflicts = pd.DataFrame(columns=_CONFLICT_COLUMNS)
        self.conflicts = conflicts
        self.conflict = conflicts
        self.diagnostics = conflicts


@dataclass
class ChannelSchedule:
    """A complete or partial channel schedule.

    Attributes
    ----------
    writes : pandas.DataFrame
        Logical primary and warm-up segments for the whole task window, with
        columns ``Chromosome, Start, End, Task, Channel, Kind, Slots,
        HandoverAt, Executed``.  Executed and newly planned pieces with the
        same channel and adjacency are merged.
    planned : pandas.DataFrame
        The portion of ``writes`` after the cutoff, clipped at the cutoff.
    executed : pandas.DataFrame
        The executed records supplied as input.
    handovers : pandas.DataFrame
        Used handovers with ``Chromosome, Task, At, Source, Target,
        WarmupStart, WarmupEnd, Future, WarmupExecutedUntil``.
    future_handovers : int
        Handovers strictly after the cutoff.
    future_warmup_slot_seconds : int
        Warm-up slot-seconds newly added strictly after the cutoff.
    conflicts : pandas.DataFrame
        Empty exactly when the schedule is feasible.
    """

    writes: pd.DataFrame
    handovers: pd.DataFrame
    future_handovers: int
    future_warmup_slot_seconds: int
    cutoff: int = 0
    executed: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    planned: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    conflicts: pandas.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=_CONFLICT_COLUMNS)
    )

    @property
    def feasible(self) -> bool:
        return len(self.conflicts) == 0

    @property
    def df(self) -> pd.DataFrame:
        """Alias for :attr:`writes`."""

        return self.writes

    @property
    def diagnostics(self) -> pd.DataFrame:
        """Alias for :attr:`conflicts`."""

        return self.conflicts

    def as_pyranges(self):
        """Return ``writes`` as a :class:`pyranges.PyRanges`."""

        import pyranges as pr

        return pr.PyRanges(_with_base_columns(self.writes))

    def raise_if_infeasible(self) -> "ChannelSchedule":
        if not self.feasible:
            raise InfeasibleSchedule(
                "no complete channel schedule exists", self.conflicts.copy()
            )
        return self


def _with_base_columns(frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    if frame is None or len(frame) == 0:
        return pd.DataFrame(
            {
                "Chromosome": pd.Series(dtype=str),
                "Start": pd.Series(dtype="int64"),
                "End": pd.Series(dtype="int64"),
            }
        )
    result = frame.copy()
    result["Start"] = result["Start"].astype("int64")
    result["End"] = result["End"].astype("int64")
    return result


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------


def _normalise_kind(value: Any) -> str:
    text = str(value).strip().lower()
    if text in ("warmup", "warm-up", "prewarm", "pre-warm", "preheat", "预热", "warm"):
        return "warmup"
    if text in ("primary", "main", "write", "主写", "primary_write"):
        return "primary"
    return text


def _pick(frame: pd.DataFrame, *names: str, default: Any = None) -> Any:
    for name in names:
        if name in frame.columns:
            return frame[name]
    return default


def _first(entry: Mapping, *names: str, default: Any = None) -> Any:
    for name in names:
        if name in entry:
            return entry[name]
    return default


def _as_dataframe(value):
    if isinstance(value, pd.DataFrame):
        return value
    if value is not None and hasattr(value, "df"):
        try:
            return value.df
        except Exception:
            return value
    return value


def _parse_handover(entry: Mapping, task_id: str) -> ChannelHandover:
    at = _first(entry, "At", "at", "Time", "time", "HandoverAt", "handover_at")
    if at is None:
        raise ValueError("handover is missing its time")
    return ChannelHandover(
        task_id,
        int(at),
        str(_first(entry, "Source", "source", "From", "from")),
        str(_first(entry, "Target", "target", "To", "to")),
        int(
            _first(
                entry,
                "Warmup",
                "warmup",
                "WarmupSeconds",
                "warmup_seconds",
                "WarmupSlots",
                default=0,
            )
        ),
    )


def _parse_tasks(tasks, handovers=None) -> list[Task]:
    tasks = _as_dataframe(tasks)

    if isinstance(tasks, Task):
        if handovers is not None:
            handover_set = _handovers_from_frame(handovers)
            extra = handover_set.get(tasks.task, ())
            merged = {(h.at, h.source, h.target, h.warmup_seconds): h for h in tasks.handovers}
            for handover in extra:
                merged.setdefault(
                    (handover.at, handover.source, handover.target, handover.warmup_seconds),
                    handover,
                )
            return [
                Task(
                    tasks.task,
                    tasks.chromosome,
                    tasks.start,
                    tasks.end,
                    tasks.slots,
                    tasks.start_channels,
                    tuple(merged.values()),
                )
            ]
        return [tasks]

    if isinstance(tasks, Mapping) and not isinstance(tasks, pd.DataFrame):
        if "Tasks" in tasks or "tasks" in tasks:
            task_frame = _as_dataframe(tasks.get("Tasks", tasks.get("tasks")))
            handover_frame = tasks.get("Handovers", tasks.get("handovers"))
            return _tasks_from_frame(task_frame, handover_frame)
        raise TypeError("task mapping must contain 'Tasks' and 'Handovers'")

    if isinstance(tasks, pd.DataFrame):
        return _tasks_from_frame(tasks, handovers)

    items = list(tasks)
    if not items:
        return []
    if items and isinstance(items[0], Task):
        result = list(items)
        if handovers is not None:
            return _merge_handovers(result, _handovers_from_frame(handovers))
        return result
    if items and isinstance(items[0], Mapping):
        result = []
        for entry in items:
            task_id = str(
                _first(entry, "Task", "task", "Name", "name", "Id", "id", "TaskId", "task_id")
            )
            start_channels = _first(
                entry,
                "StartChannels",
                "start_channels",
                "Channels",
                "channels",
                "AllowedChannels",
                "allowed_channels",
            )
            if start_channels is None:
                channel = _first(
                    entry, "Channel", "channel", "StartChannel", "start_channel", "Lane", "lane"
                )
                start_channels = () if channel is None else (channel,)
            if isinstance(start_channels, str):
                start_channels = (start_channels,)
            own_handovers = _first(entry, "Handovers", "handovers", default=())
            result.append(
                Task(
                    task_id,
                    str(_first(entry, "Chromosome", "chromosome", "Chr", "chr")),
                    int(_first(entry, "Start", "start")),
                    int(_first(entry, "End", "end")),
                    int(_first(entry, "Slots", "slots", default=1)),
                    tuple(str(c) for c in start_channels),
                    tuple(_parse_handover(h, task_id) for h in own_handovers),
                )
            )
        if handovers is not None:
            return _merge_handovers(result, _handovers_from_frame(handovers))
        return result

    raise TypeError(
        "tasks must be Task objects, a DataFrame, a mapping, or an iterable of mappings"
    )


def _merge_handovers(tasks: list[Task], extra_by_task: Mapping[str, list[ChannelHandover]]) -> list[Task]:
    result = []
    for task in tasks:
        merged = {h.at: h for h in task.handovers}
        for handover in extra_by_task.get(task.task, ()):
            merged.setdefault(handover.at, handover)
        result.append(
            Task(
                task.task,
                task.chromosome,
                task.start,
                task.end,
                task.slots,
                task.start_channels,
                tuple(merged.values()),
            )
        )
    return result


def _handovers_from_frame(handover_frame) -> dict[str, list[ChannelHandover]]:
    handover_frame = _as_dataframe(handover_frame)
    if handover_frame is None:
        return {}
    if isinstance(handover_frame, Mapping):
        handover_frame = pd.DataFrame(handover_frame)
    if len(handover_frame) == 0:
        return {}

    task_ids = _pick(
        handover_frame, "Task", "task", "Name", "name", "Id", "id", "TaskId", "task_id"
    ).astype(str)
    at = _pick(handover_frame, "At", "at", "Time", "time", "HandoverAt", "handover_at").astype(int)
    source = _pick(handover_frame, "Source", "source", "From", "from").astype(str)
    target = _pick(handover_frame, "Target", "target", "To", "to").astype(str)
    warmup = _pick(
        handover_frame,
        "Warmup",
        "warmup",
        "WarmupSeconds",
        "warmup_seconds",
        default=pd.Series(0, index=handover_frame.index),
    ).astype(int)

    result: dict[str, list[ChannelHandover]] = {}
    for task_id, when, src, dst, seconds in zip(task_ids, at, source, target, warmup):
        result.setdefault(str(task_id), []).append(
            ChannelHandover(str(task_id), int(when), str(src), str(dst), int(seconds))
        )
    return result


def _tasks_from_frame(task_frame, handover_frame) -> list[Task]:
    task_frame = _as_dataframe(task_frame)
    if task_frame is None:
        return []
    if isinstance(task_frame, Mapping):
        task_frame = pd.DataFrame(task_frame)
    if len(task_frame) == 0:
        return []

    task_ids = _pick(
        task_frame, "Task", "task", "Name", "name", "Id", "id", "TaskId", "task_id"
    ).astype(str)
    chromosomes = _pick(task_frame, "Chromosome", "chromosome", "Chr", "chr").astype(str)
    starts = _pick(task_frame, "Start", "start").astype(int)
    ends = _pick(task_frame, "End", "end").astype(int)
    slots = _pick(
        task_frame, "Slots", "slots", default=pd.Series(1, index=task_frame.index)
    ).astype(int)

    start_channels: dict[str, set[str]] = {}
    channel_column = _pick(
        task_frame,
        "Channel",
        "channel",
        "StartChannel",
        "start_channel",
        "Lane",
        "lane",
    )
    if channel_column is not None:
        for task_id, channel in zip(task_ids, channel_column):
            if pd.notna(channel):
                start_channels.setdefault(str(task_id), set()).add(str(channel))

    parsed_handovers = _handovers_from_frame(handover_frame)
    if handover_frame is None and ("HandoverAt" in task_frame or "At" in task_frame):
        at = _pick(task_frame, "HandoverAt", "At")
        source = _pick(task_frame, "Source", "From")
        target = _pick(task_frame, "Target", "To")
        warmup = _pick(
            task_frame,
            "Warmup",
            "WarmupSeconds",
            default=pd.Series(0, index=task_frame.index),
        )
        for task_id, when, src, dst, seconds in zip(task_ids, at, source, target, warmup):
            if pd.notna(when) and pd.notna(src) and pd.notna(dst):
                parsed_handovers.setdefault(str(task_id), []).append(
                    ChannelHandover(str(task_id), int(when), str(src), str(dst), int(seconds))
                )

    result = []
    for task_id, chromosome, start, end, slot_count in zip(
        task_ids, chromosomes, starts, ends, slots
    ):
        task_id = str(task_id)
        result.append(
            Task(
                task_id,
                str(chromosome),
                int(start),
                int(end),
                int(slot_count),
                tuple(sorted(start_channels.get(task_id, ()))),
                tuple(parsed_handovers.get(task_id, ())),
            )
        )
    return result


def _parse_capacity(capacity) -> list[ChannelCapacity]:
    capacity = _as_dataframe(capacity)
    if capacity is None:
        return []
    if isinstance(capacity, ChannelCapacity):
        return [capacity]

    if isinstance(capacity, pd.DataFrame):
        frame = capacity
        return [
            ChannelCapacity(str(chromosome), str(channel), int(start), int(end), int(slots))
            for chromosome, channel, start, end, slots in zip(
                _pick(frame, "Chromosome", "chromosome", "Chr", "chr").astype(str),
                _pick(frame, "Channel", "channel", "Lane", "lane").astype(str),
                _pick(frame, "Start", "start").astype(int),
                _pick(frame, "End", "end").astype(int),
                _pick(
                    frame,
                    "Slots",
                    "slots",
                    "Capacity",
                    "capacity",
                    default=pd.Series(1, index=frame.index),
                ).astype(int),
            )
        ]

    items = list(capacity)
    if not items:
        return []
    if items and isinstance(items[0], ChannelCapacity):
        return list(items)
    if items and isinstance(items[0], Mapping):
        result = []
        for entry in items:
            result.append(
                ChannelCapacity(
                    str(_first(entry, "Chromosome", "chromosome", "Chr", "chr")),
                    str(_first(entry, "Channel", "channel", "Lane", "lane")),
                    int(_first(entry, "Start", "start")),
                    int(_first(entry, "End", "end")),
                    int(_first(entry, "Slots", "slots", "Capacity", "capacity", default=1)),
                )
            )
        return result
    raise TypeError("capacity must be ChannelCapacity records, a DataFrame, or mappings")


def _parse_executed(executed) -> list[ExecutedWrite]:
    executed = _as_dataframe(executed)
    if executed is None:
        return []
    if isinstance(executed, ExecutedWrite):
        return [executed]

    if isinstance(executed, pd.DataFrame):
        frame = executed
        kinds = _pick(
            frame,
            "Kind",
            "kind",
            "Type",
            "type",
            "Action",
            "action",
            default=pd.Series("primary", index=frame.index),
        ).astype(str).map(_normalise_kind)
        slots = _pick(
            frame, "Slots", "slots", default=pd.Series(1, index=frame.index)
        ).astype(int)
        handover_at = _pick(
            frame,
            "HandoverAt",
            "handover_at",
            "At",
            "at",
            default=pd.Series(pd.NA, index=frame.index),
        )
        handover_at = pd.Series(handover_at).astype("Int64")
        source = _pick(
            frame,
            "Source",
            "source",
            "From",
            "from",
            default=pd.Series(None, index=frame.index, dtype=object),
        )
        target = _pick(
            frame,
            "Target",
            "target",
            "To",
            "to",
            default=pd.Series(None, index=frame.index, dtype=object),
        )
        result = []
        for task, chromosome, channel, start, end, kind, slot_count, when, src, dst in zip(
            _pick(frame, "Task", "task", "Name", "name", "Id", "id", "TaskId", "task_id").astype(str),
            _pick(frame, "Chromosome", "chromosome", "Chr", "chr").astype(str),
            _pick(frame, "Channel", "channel", "Lane", "lane").astype(str),
            _pick(frame, "Start", "start").astype(int),
            _pick(frame, "End", "end").astype(int),
            kinds,
            slots,
            handover_at,
            source,
            target,
        ):
            result.append(
                ExecutedWrite(
                    str(task),
                    str(chromosome),
                    str(channel),
                    int(start),
                    int(end),
                    str(kind),
                    int(slot_count),
                    None if pd.isna(when) else int(when),
                    None
                    if src is None or (isinstance(src, float) and pd.isna(src))
                    else str(src),
                    None
                    if dst is None or (isinstance(dst, float) and pd.isna(dst))
                    else str(dst),
                )
            )
        return result

    items = list(executed)
    if not items:
        return []
    if items and isinstance(items[0], ExecutedWrite):
        return list(items)
    result = []
    if items and isinstance(items[0], Mapping):
        for entry in items:
            when = _first(entry, "HandoverAt", "handover_at", "At", "at")
            result.append(
                ExecutedWrite(
                    str(_first(entry, "Task", "task", "Name", "name", "Id", "id", "TaskId", "task_id")),
                    str(_first(entry, "Chromosome", "chromosome", "Chr", "chr")),
                    str(_first(entry, "Channel", "channel", "Lane", "lane")),
                    int(_first(entry, "Start", "start")),
                    int(_first(entry, "End", "end")),
                    _normalise_kind(_first(entry, "Kind", "kind", "Type", "type", default="primary")),
                    int(_first(entry, "Slots", "slots", default=1)),
                    None if when is None else int(when),
                    _first(entry, "Source", "source", "From", "from"),
                    _first(entry, "Target", "target", "To", "to"),
                )
            )
        return result
    raise TypeError("executed must be ExecutedWrite records, a DataFrame, or mappings")


# ---------------------------------------------------------------------------
# Internal model
# ---------------------------------------------------------------------------


@dataclass
class _Segment:
    start: int
    end: int
    channel: str
    kind: str = "primary"
    handover_at: Optional[int] = None
    carried_from: Optional[int] = None


@dataclass
class _HandoverUse:
    at: int
    source: str
    target: str
    warmup_start: int
    warmup_seconds: int
    executed_until: Optional[int] = None


@dataclass
class _Plan:
    task: Task
    segments: list[_Segment] = field(default_factory=list)
    handovers: list[_HandoverUse] = field(default_factory=list)

    def score(self, cutoff: int) -> tuple[int, int]:
        future = sum(1 for handover in self.handovers if handover.at > cutoff)
        warmup = sum(
            self.task.slots
            * max(0, handover.at - max(cutoff, handover.warmup_start))
            for handover in self.handovers
            if handover.at > cutoff
        )
        return future, warmup


@dataclass
class _Anchor:
    cursor: int
    channel: Optional[str]
    done: bool
    fixed: list[_HandoverUse] = field(default_factory=list)


def _merge_intervals(records: list[tuple[int, int, str]]) -> list[tuple[int, int, str]]:
    """Merge adjacent same-channel intervals; keep gaps and channel changes."""

    result: list[tuple[int, int, str]] = []
    for start, end, channel in sorted(records):
        if start >= end:
            continue
        if result and result[-1][2] == channel and result[-1][1] == start:
            result[-1] = (result[-1][0], end, channel)
        else:
            result.append((start, end, channel))
    return result


def _find_handover(
    task: Task,
    at: int,
    target: Optional[str],
    source: Optional[str] = None,
) -> Optional[ChannelHandover]:
    matches = [
        h
        for h in task.handovers
        if h.at == at
        and (target is None or h.target == target)
        and (source is None or h.source == source)
    ]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        matches.sort(key=lambda h: h.warmup_seconds)
        return matches[0]
    return None


# ------------------------------------------------------------------------------------------------------------------------------------------
# Executed-prefix analysis
# ------------------------------------------------------------------------------------------------------------------------------------------


def _analyse_executed(
    tasks: dict[str, Task],
    records: list[ExecutedWrite],
    cutoff: int,
) -> tuple[dict[str, _Anchor], list[dict], list[_HandoverUse], list[ExecutedWrite]]:
    diagnostics: list[dict] = []
    anchors: dict[str, _Anchor] = {}
    history_handovers: list[_HandoverUse] = []

    records_by_task: dict[str, list[ExecutedWrite]] = {task_id: [] for task_id in tasks}
    for record in records:
        if record.end > cutoff:
            raise ChannelScheduleError(
                "executed record for task {} ends after the cutoff ({} > {})".format(
                    record.task, record.end, cutoff
                )
            )
        if record.task not in tasks:
            raise ChannelScheduleError(
                "executed record refers to unknown task {}".format(record.task)
            )
        task = tasks[record.task]
        if record.chromosome != task.chromosome:
            raise ChannelScheduleError(
                "executed record chromosome for task {} does not match the task".format(
                    record.task
                )
            )
        if record.slots != task.slots:
            raise ChannelScheduleError(
                "executed record slots for task {} do not match the task".format(record.task)
            )
        if record.start < task.start or record.end > task.end:
            raise ChannelScheduleError(
                "executed record for task {} lies outside the task window".format(
                    record.task
                )
            )
        records_by_task[record.task].append(record)

    for task_id, task in tasks.items():
        task_records = sorted(
            records_by_task[task_id], key=lambda r: (r.start, r.end, r.kind)
        )
        primaries = _merge_intervals(
            [(r.start, r.end, r.channel) for r in task_records if r.kind == "primary"]
        )

        expected_end = min(cutoff, task.end)
        cursor = task.start
        for start, end, _channel in primaries:
            if start > cursor:
                diagnostics.append(
                    _conflict(
                        task,
                        cursor,
                        min(start, expected_end),
                        "primary_missing",
                        reason="primary write has not been executed",
                    )
                )
            elif start < cursor:
                diagnostics.append(
                    _conflict(
                        task,
                        start,
                        min(cursor, end),
                        "executed",
                        reason="executed primary writes overlap",
                    )
                )
            cursor = max(cursor, end)

        if task.start < expected_end and cursor < expected_end:
            diagnostics.append(
                _conflict(
                    task,
                    cursor,
                    expected_end,
                    "primary_missing",
                    reason="primary write has not been executed",
                )
            )

        valid_history = (
            task.start >= expected_end
            or all(d["Kind"] != "primary_missing" for d in diagnostics if d["Task"] == task_id)
        )

        if task.start >= cutoff:
            anchors[task_id] = _Anchor(task.start, None, False, [])
            continue

        if task.end <= cutoff:
            anchors[task_id] = _Anchor(task.end, None, True, [])
            continue

        if not valid_history or not primaries:
            anchors[task_id] = _Anchor(cutoff, None, False, [])
            continue

        last_channel = primaries[-1][2]
        if primaries[-1][1] != cutoff:
            anchors[task_id] = _Anchor(cutoff, None, False, [])
            continue

        for (left_start, left_end, left_channel), (right_start, right_end, right_channel) in zip(
            primaries, primaries[1:]
        ):
            if left_channel == right_channel:
                continue
            handover = _find_handover(task, left_end, right_channel, left_channel)
            if handover is None:
                diagnostics.append(
                    _conflict(
                        task,
                        left_end,
                        right_start,
                        "executed",
                        reason="executed primary channel change has no allowed handover",
                    )
                )
                valid_history = False
                break
            history_handovers.append(
                _HandoverUse(
                    handover.at,
                    handover.source,
                    handover.target,
                    handover.at - handover.warmup_seconds,
                    handover.warmup_seconds,
                    handover.at,
                )
            )

        if not valid_history:
            anchors[task_id] = _Anchor(cutoff, None, False, [])
            continue

        current_channel = last_channel
        anchors[task_id] = _Anchor(cutoff, current_channel, False, [])

    # Validate warm-up records and identify executed future handovers.
    warmup_groups: dict[tuple[str, int, str], list[ExecutedWrite]] = {}
    warmup_handovers: dict[tuple[str, int, str], ChannelHandover] = {}
    for task_id, task_records in records_by_task.items():
        task = tasks[task_id]
        for record in task_records:
            if record.kind != "warmup":
                continue
            handover = _find_handover(task, record.handover_at, record.channel, record.source)
            if handover is None:
                diagnostics.append(
                    _conflict(
                        task,
                        record.start,
                        record.end,
                        "executed",
                        reason="executed warm-up does not match an allowed handover",
                        channel=record.channel,
                        handover_at=record.handover_at,
                    )
                )
                continue
            if record.target is not None and record.target != handover.target:
                diagnostics.append(
                    _conflict(
                        task,
                        record.start,
                        record.end,
                        "executed",
                        reason="executed warm-up target does not match",
                        channel=record.channel,
                        handover_at=record.handover_at,
                    )
                )
                continue
            if record.start < handover.at - handover.warmup_seconds:
                diagnostics.append(
                    _conflict(
                        task,
                        handover.at - handover.warmup_seconds,
                        record.start,
                        "warmup_missing",
                        reason="executed warm-up starts before the prepared interval",
                        channel=record.channel,
                        handover_at=handover.at,
                    )
                )
                continue
            key = (task_id, handover.at, handover.target)
            warmup_groups.setdefault(key, []).append(record)
            warmup_handovers[key] = handover

    fixed_by_task: dict[str, list[_HandoverUse]] = {task_id: [] for task_id in tasks}
    for (task_id, at, target), group in warmup_groups.items():
        task = tasks[task_id]
        handover = warmup_handovers[(task_id, at, target)]
        span_start = at - handover.warmup_seconds
        intervals = _merge_intervals([(r.start, r.end, target) for r in group])
        if not intervals:
            continue
        first_start = intervals[0][0]
        last_end = intervals[-1][1]
        contiguous = True
        cursor = first_start
        for start, end, _ in intervals:
            if start != cursor:
                contiguous = False
            cursor = end
        if not contiguous:
            diagnostics.append(
                _conflict(
                    task,
                    span_start,
                    min(at, cutoff),
                    "warmup_missing",
                    reason="executed warm-up is not continuous",
                    channel=target,
                    handover_at=at,
                )
            )
            continue

        if at <= cutoff:
            if first_start > span_start or last_end < at:
                diagnostics.append(
                    _conflict(
                        task,
                        span_start,
                        min(at, cutoff),
                        "warmup_missing",
                        reason="warm-up was not fully executed before the cutoff",
                        channel=target,
                        handover_at=at,
                    )
                )
            else:
                history_handovers.append(
                    _HandoverUse(
                        handover.at,
                        handover.source,
                        handover.target,
                        span_start,
                        handover.warmup_seconds,
                        handover.at,
                    )
                )
            continue

        # Handover lies after the cutoff.  To carry on, the executed warm-up
        # must be the contiguous prefix [span_start, cutoff) of the full warm-up.
        anchor = anchors.get(task_id)
        if anchor is None or anchor.channel is None:
            diagnostics.append(
                _conflict(
                    task,
                    span_start,
                    cutoff,
                    "executed",
                    reason="warm-up exists but no executed primary reaches the cutoff",
                    channel=target,
                    handover_at=at,
                )
            )
            continue
        if anchor.channel != handover.source:
            diagnostics.append(
                _conflict(
                    task,
                    cutoff,
                    cutoff,
                    "executed",
                    reason="warm-up target conflicts with the primary channel at the cutoff",
                    channel=target,
                    handover_at=at,
                )
            )
            continue
        if first_start != span_start or last_end != cutoff:
            diagnostics.append(
                _conflict(
                    task,
                    span_start,
                    min(cutoff, at),
                    "warmup_missing",
                    reason="warm-up cannot be continued from its executed prefix",
                    channel=target,
                    handover_at=at,
                )
            )
            continue

        fixed_by_task[task_id].append(
            _HandoverUse(
                at,
                handover.source,
                handover.target,
                span_start,
                handover.warmup_seconds,
                cutoff,
            )
        )

    for task_id, fixed in fixed_by_task.items():
        fixed.sort(key=lambda h: h.at)
        anchor = anchors.get(task_id)
        if anchor is not None and not anchor.done:
            anchor.fixed = fixed

    return anchors, diagnostics, history_handovers, records


def _conflict(
    task: Task,
    start: int,
    end: int,
    kind: str,
    reason: str,
    channel: Optional[str] = None,
    handover_at: Optional[int] = None,
    required: Optional[int] = None,
    available: Optional[int] = None,
    occupied_by: str = "",
) -> dict:
    if end < start:
        end = start
    return {
        "Chromosome": task.chromosome,
        "Channel": channel,
        "Start": int(start),
        "End": int(end),
        "Task": task.task,
        "Kind": kind,
        "Slots": task.slots,
        "Required": required,
        "Available": available,
        "OccupiedBy": occupied_by,
        "Reason": reason,
        "HandoverAt": handover_at,
    }


# ------------------------------------------------------------------------------------------------------------------------------------------
# Plan enumeration
# ------------------------------------------------------------------------------------------------------------------------------------------


def _candidate_start_channels(task: Task, capacities) -> list[str]:
    if task.start_channels:
        return list(task.start_channels)
    result = {channel for (chromosome, channel) in capacities if chromosome == task.chromosome}
    for handover in task.handovers:
        result.add(handover.source)
        result.add(handover.target)
    return sorted(result)


def _enumerate_plans(
    task: Task,
    anchor: _Anchor,
    cutoff: int,
    capacities,
    max_future_handovers: Optional[int] = None,
) -> tuple[list[_Plan], list[dict]]:
    plans: list[_Plan] = []
    diagnostics: list[dict] = []

    if anchor.done:
        return [_Plan(task)], diagnostics

    start_channels = _candidate_start_channels(task, capacities)
    if not start_channels and anchor.channel is None:
        return [], [
            _conflict(
                task,
                task.start,
                min(cutoff, task.end),
                "executed",
                reason="no channel is available for the task",
            )
        ]

    fixed = list(anchor.fixed)
    fixed.sort(key=lambda h: h.at)

    # At each (time, channel, fixed-progress) state keep only the best partial
    # plans by (future handovers, future warm-up), capped to a small beam.
    # This preserves the exact objective for the sparse trick cases while
    # preventing combinatorial blow-up on dense handover tables.
    state_best: dict[tuple[int, str, int], list[_Plan]] = {}
    beam_width = 32

    def state_key(cursor: int, channel: str, used_fixed: int):
        return (cursor, channel, used_fixed)

    def partial_score(segments, handovers):
        future = sum(1 for item in handovers if item.at > cutoff)
        warmup = sum(
            task.slots * max(0, item.at - max(cutoff, item.warmup_start))
            for item in handovers
            if item.at > cutoff
        )
        return future, warmup

    def allow(cursor: int, channel: str, used_fixed: int, segments, handovers) -> bool:
        key = state_key(cursor, channel, used_fixed)
        score = partial_score(segments, handovers)
        best = state_best.setdefault(key, [])
        existing = [
            (partial_score(item.segments, item.handovers), item) for item in best
        ]
        if any(existing_score < score for existing_score, _ in existing):
            return False
        best[:] = [item for existing_score, item in existing if existing_score <= score]
        best.append(_Plan(task, list(segments), list(handovers)))
        best.sort(key=lambda item: partial_score(item.segments, item.handovers))
        del best[beam_width:]
        return True

    def build(
        cursor: int,
        channel: str,
        remaining: list[ChannelHandover],
        used_fixed: int,
        segments: list[_Segment],
        handovers: list[_HandoverUse],
    ):
        if segments or handovers:
            if not allow(cursor, channel, used_fixed, segments, handovers):
                return
        mandatory = fixed[used_fixed] if used_fixed < len(fixed) else None

        if mandatory is not None:
            if mandatory.at < cursor:
                return
            candidates = [
                handover
                for handover in task.handovers
                if handover.at == mandatory.at
                and handover.source == mandatory.source
                and handover.target == mandatory.target
            ]
            if not candidates:
                return
            options = [candidates[0]]
            final_option = False
        else:
            options = [
                handover
                for handover in remaining
                if handover.at >= cursor and handover.source == channel
            ]
            final_option = True

        for handover in options:
            warmup_start = handover.at - handover.warmup_seconds
            carried = warmup_start < cursor
            future_used = sum(1 for item in handovers if item.at > cutoff)
            if (
                max_future_handovers is not None
                and handover.at > cutoff
                and future_used >= max_future_handovers
                and not (mandatory is not None and mandatory.at == handover.at)
            ):
                continue
            if carried:
                if mandatory is None or mandatory.at != handover.at or mandatory.target != handover.target:
                    diagnostics.append(
                        _conflict(
                            task,
                            warmup_start,
                            min(cursor, handover.at),
                            "warmup_missing",
                            reason="required warm-up has already elapsed and was not executed",
                            channel=handover.target,
                            handover_at=handover.at,
                            required=max(0, min(cursor, handover.at) - warmup_start),
                            available=0,
                        )
                    )
                    continue
                if mandatory.executed_until is None:
                    continue

            new_segments = list(segments)
            if handover.at > cursor:
                new_segments.append(_Segment(cursor, handover.at, channel))
            if handover.at > warmup_start:
                new_segments.append(
                    _Segment(
                        warmup_start,
                        handover.at,
                        handover.target,
                        "warmup",
                        handover.at,
                        warmup_start if carried else None,
                    )
                )
            new_handovers = list(handovers)
            if mandatory is not None and mandatory.at == handover.at:
                new_handovers = [
                    item for item in new_handovers if not (item.at == handover.at and item.target == handover.target)
                ]
            new_handovers.append(
                _HandoverUse(
                    handover.at,
                    handover.source,
                    handover.target,
                    warmup_start,
                    handover.warmup_seconds,
                    mandatory.executed_until if carried and mandatory is not None else None,
                )
            )
            after = [
                candidate
                for candidate in remaining
                if candidate.at > handover.at
            ]
            next_fixed = used_fixed + 1 if mandatory is not None else used_fixed
            build(
                handover.at,
                handover.target,
                after,
                next_fixed,
                new_segments,
                new_handovers,
            )

        if final_option and mandatory is None:
            final_segments = list(segments)
            if task.end > cursor:
                final_segments.append(_Segment(cursor, task.end, channel))
            plans.append(_Plan(task, final_segments, list(handovers)))

    def initial_call(channel: str, cursor: int):
        build(
            cursor=cursor,
            channel=channel,
            remaining=list(task.handovers),
            used_fixed=0,
            segments=[],
            handovers=[],
        )

    if anchor.channel is not None:
        initial_call(anchor.channel, anchor.cursor)
    else:
        for channel in start_channels:
            initial_call(channel, task.start)

    unique: dict[tuple, _Plan] = {}
    for plan in plans:
        key = (
            tuple((s.start, s.end, s.channel, s.kind, s.handover_at) for s in plan.segments),
            tuple((h.at, h.source, h.target, h.warmup_start) for h in plan.handovers),
        )
        unique.setdefault(key, plan)
    return list(unique.values()), _dedupe_conflicts(diagnostics)


def _dedupe_conflicts(rows: list[dict]) -> list[dict]:
    seen = {}
    for row in rows:
        key = (
            row.get("Chromosome"),
            row.get("Channel"),
            row.get("Start"),
            row.get("End"),
            row.get("Task"),
            row.get("Kind"),
            row.get("Reason"),
        )
        seen.setdefault(key, row)
    return list(seen.values())


# ------------------------------------------------------------------------------------------------------------------------------------------
# Capacity
# ------------------------------------------------------------------------------------------------------------------------------------------


def _capacity_by_channel(capacities: list[ChannelCapacity]):
    result: dict[tuple[str, str], list[ChannelCapacity]] = {}
    for entry in capacities:
        result.setdefault((entry.chromosome, entry.channel), []).append(entry)
    for entries in result.values():
        entries.sort(key=lambda c: (c.start, c.end))
    return result


def _available_slots(entries: list[ChannelCapacity], start: int, end: int) -> int:
    """Slots available throughout ``[start, end)``.

    Overlapping capacity rows are additive.  Any uncovered second has zero
    capacity, so the minimum over elementary pieces is returned.
    """

    rows = [entry for entry in entries if entry.start < end and start < entry.end]
    if not rows:
        return 0

    points = {start, end}
    for row in rows:
        points.add(max(start, row.start))
        points.add(min(end, row.end))
    points = sorted(points)

    available = None
    for left, right in zip(points, points[1:]):
        if right <= left:
            continue
        slots = sum(row.slots for row in rows if row.start <= left and right <= row.end)
        if available is None or slots < available:
            available = slots
    return 0 if available is None else available


def _capacity_conflicts(
    plans: list[_Plan],
    cutoff: int,
    capacity_by_channel,
) -> list[dict]:
    conflicts: list[dict] = []

    usage: dict[tuple[str, str], list[tuple[int, int, _Plan, _Segment]]] = {}
    for plan in plans:
        for segment in plan.segments:
            start = max(segment.start, cutoff, plan.task.start)
            if segment.end <= start:
                continue
            usage.setdefault((plan.task.chromosome, segment.channel), []).append(
                (start, segment.end, plan, segment)
            )

    for (chromosome, channel), entries in usage.items():
        rows = capacity_by_channel.get((chromosome, channel), [])
        points = set()
        for start, end, _, _ in entries:
            points.add(start)
            points.add(end)
        for row in rows:
            points.add(row.start)
            points.add(row.end)
        points = sorted(points)

        for left, right in zip(points, points[1:]):
            if right <= left:
                continue
            active = [
                item
                for item in entries
                if item[0] <= left and right <= item[1]
            ]
            if not active:
                continue
            required = sum(item[2].task.slots for item in active)
            available = _available_slots(rows, left, right)
            if required <= available:
                continue

            contributing = {
                (item[2].task.task, item[3].kind) for item in active
            }
            for item in active:
                plan = item[2]
                segment = item[3]
                others = sorted(
                    other_plan.task.task
                    for other_plan, other_segment in (
                        (x[2], x[3]) for x in active
                    )
                    if other_plan.task.task != plan.task.task
                )
                if others:
                    conflicts.append(
                        _conflict(
                            plan.task,
                            left,
                            right,
                            "occupied",
                            reason="channel capacity is insufficient",
                            channel=channel,
                            required=required,
                            available=available,
                            occupied_by="; ".join(
                                sorted(set(others))
                            ),
                        )
                    )

            # Also provide one generic row if only a single task was active but
            # still exceeded capacity.
            if len(contributing) == 1:
                plan = active[0][2]
                if not any(
                    row["Task"] == plan.task.task
                    and row["Channel"] == channel
                    and row["Start"] == left
                    and row["End"] == right
                    for row in conflicts
                ):
                    conflicts.append(
                        _conflict(
                            plan.task,
                            left,
                            right,
                            "occupied",
                            reason="channel capacity is insufficient",
                            channel=channel,
                            required=required,
                            available=available,
                        )
                    )

    return _merge_conflict_rows(conflicts)


def _merge_conflict_rows(rows: list[dict]) -> list[dict]:
    result: list[dict] = []
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            row["Chromosome"] or "",
            row["Channel"] or "",
            row["Start"],
            row["End"],
            row["Task"],
            row["Kind"],
        ),
    )
    for row in sorted_rows:
        if (
            result
            and result[-1]["Chromosome"] == row["Chromosome"]
            and result[-1]["Channel"] == row["Channel"]
            and result[-1]["Task"] == row["Task"]
            and result[-1]["Kind"] == row["Kind"]
            and result[-1]["End"] == row["Start"]
            and result[-1]["Required"] == row["Required"]
            and result[-1]["Available"] == row["Available"]
            and result[-1]["OccupiedBy"] == row["OccupiedBy"]
        ):
            result[-1]["End"] = row["End"]
        else:
            result.append(dict(row))
    return result


def _plans_feasible(plans: list[_Plan], cutoff: int, capacity_by_channel) -> bool:
    return not _capacity_conflicts(plans, cutoff, capacity_by_channel)


# ------------------------------------------------------------------------------------------------------------------------------------------
# Search
# ------------------------------------------------------------------------------------------------------------------------------------------


def _search(
    tasks: dict[str, Task],
    anchors: dict[str, _Anchor],
    cutoff: int,
    capacities,
    capacity_by_channel,
):
    task_ids = sorted(tasks, key=lambda t: (tasks[t].start, t))

    # Enumerate candidate plans per task once.  Each task keeps a small beam of
    # the best plans by (future handovers, future warm-up), which is exact for
    # the small, structured inputs this scheduler targets and avoids
    # exponential blow-up on dense handover tables.
    candidates_by_task: dict[str, list[_Plan]] = {}
    all_plans_by_task: dict[str, list[_Plan]] = {}
    plan_diagnostics: dict[str, list[dict]] = {}
    for task_id, task in tasks.items():
        plans, diagnostics = _enumerate_plans(task, anchors[task_id], cutoff, capacities)
        plans.sort(key=lambda plan: plan.score(cutoff))
        all_plans_by_task[task_id] = plans
        candidates_by_task[task_id] = plans[:32]
        plan_diagnostics[task_id] = diagnostics
        if not plans:
            return None, None, diagnostics

    groups: list[tuple[tuple[int, int], tuple[_Plan, ...]]] = []

    def group_search(index: int, chosen: list[_Plan]):
        if index == len(task_ids):
            total = (0, 0)
            for plan in chosen:
                score = plan.score(cutoff)
                total = (total[0] + score[0], total[1] + score[1])
            groups.append((total, tuple(chosen)))
            return

        task_id = task_ids[index]
        for plan in candidates_by_task[task_id]:
            chosen.append(plan)
            if _plans_feasible(chosen, cutoff, capacity_by_channel):
                group_search(index + 1, chosen)
            chosen.pop()
        group_search(index + 1, chosen)

    group_search(0, [])

    full = [group for group in groups if len(group[1]) == len(task_ids)]
    if full:
        full.sort(key=lambda group: group[0])
        return list(full[0][1]), full[0][0], []

    # No complete schedule: choose the largest partial plan and explain what
    # blocks the remaining tasks.
    best_size = 0
    partial: list[tuple[tuple[int, int], tuple[_Plan, ...]]] = []

    def partial_search(index: int, chosen: list[_Plan]):
        nonlocal best_size
        if index == len(task_ids):
            total = (0, 0)
            for plan in chosen:
                score = plan.score(cutoff)
                total = (total[0] + score[0], total[1] + score[1])
            if len(chosen) > best_size:
                best_size = len(chosen)
                partial.clear()
            if len(chosen) == best_size:
                partial.append((total, tuple(chosen)))
            return
        task_id = task_ids[index]
        for plan in candidates_by_task[task_id]:
            chosen.append(plan)
            if _plans_feasible(chosen, cutoff, capacity_by_channel):
                partial_search(index + 1, chosen)
            chosen.pop()
        partial_search(index + 1, chosen)

    partial_search(0, [])
    partial.sort(key=lambda group: group[0])
    base = list(partial[0][1]) if partial else []
    base_ids = {plan.task.task for plan in base}

    diagnostics: list[dict] = []
    for task_id in task_ids:
        if task_id in base_ids:
            continue
        for plan in all_plans_by_task[task_id]:
            conflicts = _capacity_conflicts(base + [plan], cutoff, capacity_by_channel)
            diagnostics.extend(conflicts)

    for task_id in task_ids:
        diagnostics.extend(plan_diagnostics.get(task_id, []))

    return None, None, _dedupe_conflicts(diagnostics)


# ------------------------------------------------------------------------------------------------------------------------------------------
# Output construction
# ------------------------------------------------------------------------------------------------------------------------------------------


def _executed_frame(records: list[ExecutedWrite], cutoff: int) -> pd.DataFrame:
    rows = []
    for record in records:
        rows.append(
            {
                "Chromosome": record.chromosome,
                "Start": int(record.start),
                "End": int(record.end),
                "Task": record.task,
                "Channel": record.channel,
                "Kind": record.kind,
                "Slots": int(record.slots),
                "HandoverAt": record.handover_at,
                "Executed": record.end <= cutoff,
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "Chromosome",
                "Start",
                "End",
                "Task",
                "Channel",
                "Kind",
                "Slots",
                "HandoverAt",
                "Executed",
            ]
        )
    frame = pd.DataFrame(rows)
    frame["Start"] = frame["Start"].astype("int64")
    frame["End"] = frame["End"].astype("int64")
    frame["Slots"] = frame["Slots"].astype("int64")
    frame["HandoverAt"] = pd.Series(frame["HandoverAt"], dtype="Int64")
    return frame


def _logical_writes(
    tasks: dict[str, Task],
    records: list[ExecutedWrite],
    plans: Optional[list[_Plan]],
    cutoff: int,
) -> pd.DataFrame:
    pieces_by_task: dict[str, list[_Segment]] = {task_id: [] for task_id in tasks}

    for record in records:
        pieces_by_task.setdefault(record.task, []).append(
            _Segment(
                record.start,
                record.end,
                record.channel,
                record.kind,
                record.handover_at,
                None,
            )
        )

    if plans is not None:
        for plan in plans:
            pieces_by_task.setdefault(plan.task.task, []).extend(plan.segments)

    rows = []
    for task_id, pieces in pieces_by_task.items():
        # Group by (kind, channel, handover_at), union overlapping intervals,
        # and merge adjacent same-channel pieces.
        groups: dict[tuple, list[tuple[int, int]]] = {}
        for piece in pieces:
            if piece.start >= piece.end:
                continue
            key = (piece.kind, piece.channel, piece.handover_at)
            groups.setdefault(key, []).append((piece.start, piece.end))

        for (kind, channel, handover_at), intervals in sorted(
            groups.items(), key=lambda item: (item[1][0][0], item[0][1])
        ):
            for start, end in _union_intervals(intervals):
                # Show the cutoff boundary explicitly so executed and planned
                # pieces are easy to inspect.
                split_points = [start, end]
                if start < cutoff < end:
                    split_points.insert(1, cutoff)
                for left, right in zip(split_points, split_points[1:]):
                    rows.append(
                        {
                            "Chromosome": tasks[task_id].chromosome,
                            "Start": int(left),
                            "End": int(right),
                            "Task": task_id,
                            "Channel": channel,
                            "Kind": kind,
                            "Slots": int(tasks[task_id].slots),
                            "HandoverAt": handover_at,
                            "Executed": right <= cutoff,
                        }
                    )

    if not rows:
        return _executed_frame([], cutoff)
    frame = pd.DataFrame(rows).sort_values(
        ["Chromosome", "Start", "End", "Task", "Kind", "Channel"]
    )
    frame["Start"] = frame["Start"].astype("int64")
    frame["End"] = frame["End"].astype("int64")
    frame["Slots"] = frame["Slots"].astype("int64")
    frame["HandoverAt"] = pd.Series(frame["HandoverAt"], dtype="Int64")
    frame["Executed"] = frame["Executed"].astype(bool)
    return frame.reset_index(drop=True)


def _union_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not intervals:
        return []
    result = []
    for start, end in sorted(intervals):
        if result and start <= result[-1][1]:
            result[-1] = (result[-1][0], max(result[-1][1], end))
        else:
            result.append((start, end))
    return result


def _planned_frame(writes: pd.DataFrame, cutoff: int) -> pd.DataFrame:
    if writes is None or len(writes) == 0:
        return writes.copy() if writes is not None else pd.DataFrame()
    rows = []
    for row in writes.itertuples(index=False):
        start = max(int(row.Start), cutoff)
        end = int(row.End)
        if end <= start:
            continue
        rows.append(
            {
                "Chromosome": row.Chromosome,
                "Start": start,
                "End": end,
                "Task": row.Task,
                "Channel": row.Channel,
                "Kind": row.Kind,
                "Slots": int(row.Slots),
                "HandoverAt": row.HandoverAt,
                "Executed": False,
            }
        )
    if not rows:
        return writes.iloc[0:0].copy()
    frame = pd.DataFrame(rows).sort_values(
        ["Chromosome", "Start", "End", "Task", "Kind", "Channel"]
    )
    frame["Start"] = frame["Start"].astype("int64")
    frame["End"] = frame["End"].astype("int64")
    frame["Slots"] = frame["Slots"].astype("int64")
    frame["HandoverAt"] = pd.Series(frame["HandoverAt"], dtype="Int64")
    frame["Executed"] = False
    return frame.reset_index(drop=True)


def _handover_frame(
    tasks: dict[str, Task],
    plans: Optional[list[_Plan]],
    history: list[_HandoverUse],
    cutoff: int,
) -> pd.DataFrame:
    used: dict[tuple[str, int, str, str], _HandoverUse] = {}
    for plan in plans or []:
        for handover in plan.handovers:
            used[(plan.task.task, handover.at, handover.source, handover.target)] = handover
    for handover in history:
        task_id = _task_for_handover(tasks, handover)
        if task_id is None:
            continue
        used.setdefault((task_id, handover.at, handover.source, handover.target), handover)

    rows = []
    for (task_id, at, source, target), handover in sorted(
        used.items(), key=lambda item: (item[0][1], item[0][0])
    ):
        rows.append(
            {
                "Chromosome": tasks[task_id].chromosome,
                "Task": task_id,
                "At": int(at),
                "Source": source,
                "Target": target,
                "WarmupStart": int(handover.at - handover.warmup_seconds),
                "WarmupEnd": int(handover.at),
                "Future": bool(at > cutoff),
                "WarmupExecutedUntil": handover.executed_until,
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "Chromosome",
                "Task",
                "At",
                "Source",
                "Target",
                "WarmupStart",
                "WarmupEnd",
                "Future",
                "WarmupExecutedUntil",
            ]
        )
    frame = pd.DataFrame(rows)
    frame["At"] = frame["At"].astype("int64")
    frame["Future"] = frame["Future"].astype(bool)
    frame["WarmupExecutedUntil"] = pd.Series(frame["WarmupExecutedUntil"], dtype="Int64")
    return frame


def _task_for_handover(tasks: dict[str, Task], handover: _HandoverUse) -> Optional[str]:
    for task_id, task in tasks.items():
        if any(
            candidate.at == handover.at
            and candidate.source == handover.source
            and candidate.target == handover.target
            for candidate in task.handovers
        ):
            return task_id
    return None


def _schedule_result(
    tasks: dict[str, Task],
    plans: Optional[list[_Plan]],
    records: list[ExecutedWrite],
    cutoff: int,
    diagnostics: list[dict],
    history: list[_HandoverUse],
) -> ChannelSchedule:
    conflicts = _conflict_frame(diagnostics)
    executed = _executed_frame(records, cutoff)
    writes = _logical_writes(tasks, records, plans, cutoff)
    planned = _planned_frame(writes, cutoff)

    future_handovers = 0
    future_warmup = 0
    if plans is not None:
        for plan in plans:
            score = plan.score(cutoff)
            future_handovers += score[0]
            future_warmup += score[1]

    handovers = _handover_frame(tasks, plans, history, cutoff)
    return ChannelSchedule(
        writes=writes,
        handovers=handovers,
        future_handovers=int(future_handovers),
        future_warmup_slot_seconds=int(future_warmup),
        cutoff=int(cutoff),
        executed=executed,
        planned=planned,
        conflicts=conflicts,
    )


# ------------------------------------------------------------------------------------------------------------------------------------------
# Public entry point
# ------------------------------------------------------------------------------------------------------------------------------------------


def schedule_channels(
    tasks,
    capacity=None,
    handovers=None,
    executed=None,
    cutoff: Optional[int] = None,
    *,
    schedule=None,
    previous=None,
    raise_on_infeasible: bool = False,
) -> ChannelSchedule:
    """Schedule primary writes and warm-ups on channels.

    Parameters
    ----------
    tasks : Task, DataFrame, mapping, or iterable
        Tasks to schedule.  A DataFrame must contain ``Task``, ``Chromosome``,
        ``Start`` and ``End``; optional ``Slots`` and ``Channel`` give the slot
        count and allowed start channel.  A mapping may contain ``Tasks`` and
        ``Handovers``.  Each task may also carry ``Handovers``.
    capacity : DataFrame, mapping, iterable, or None
        Capacity rows with ``Chromosome``, ``Channel``, ``Start``, ``End`` and
        ``Slots``.  Missing times have zero capacity.
    handovers : DataFrame, mapping, iterable, or None
        Allowed handovers with ``Task``, ``At``, ``Source``, ``Target`` and
        ``Warmup`` seconds.
    executed : DataFrame, mapping, iterable, or None
        Already executed records with ``Task``, ``Chromosome``, ``Channel``,
        ``Start``, ``End`` and ``Kind``.  Warm-up records also need
        ``HandoverAt``.
    cutoff : int, optional
        Half-open immutable prefix boundary.  Defaults to the latest executed
        record end, or 0 when no records are supplied.
    raise_on_infeasible : bool, default False
        Raise :class:`InfeasibleSchedule` instead of returning a schedule whose
        ``conflicts`` explain the failure.
    schedule, previous
        Accepted for callers that pass a previously issued, not necessarily
        executed schedule.  Such records are ignored; only ``executed`` is
        used, as unexecuted schedules may be replaced.

    Returns
    -------
    ChannelSchedule
        ``writes`` gives every primary and warm-up interval, ``planned`` gives
        the part after the cutoff, ``handovers`` lists used handovers, and
        ``conflicts`` is empty exactly when a complete plan exists.
    """

    tasks = _parse_tasks(tasks, handovers)
    if not tasks:
        return ChannelSchedule(
            writes=_executed_frame([], 0),
            handovers=pd.DataFrame(),
            future_handovers=0,
            future_warmup_slot_seconds=0,
            cutoff=0,
        )

    seen_task_ids = set()
    for task in tasks:
        if task.task in seen_task_ids:
            raise ChannelScheduleError("duplicate task id {!r}".format(task.task))
        seen_task_ids.add(task.task)
    task_map = {task.task: task for task in tasks}

    capacities = _parse_capacity(capacity)
    records = _parse_executed(executed)

    if cutoff is None:
        cutoff = max((record.end for record in records), default=0)
    cutoff = int(cutoff)
    capacity_by_channel = _capacity_by_channel(capacities)

    anchors, prefix_diagnostics, history, records = _analyse_executed(
        task_map, records, cutoff
    )

    # A missing executed prefix is already fatal: the cutoff may not be moved
    # back, so no search can repair it.
    blocking_prefix = [
        row
        for row in prefix_diagnostics
        if row.get("Kind") in ("primary_missing", "warmup_missing", "executed")
    ]
    if blocking_prefix:
        diagnostics = _conflict_frame(prefix_diagnostics)
        result = _schedule_result(
            task_map, None, records, cutoff, prefix_diagnostics, history
        )
        if raise_on_infeasible:
            raise InfeasibleSchedule(
                "no complete channel schedule exists", result.conflicts.copy()
            )
        return result

    plans, score, search_diagnostics = _search(
        task_map, anchors, cutoff, capacities, capacity_by_channel
    )

    diagnostics = _dedupe_conflicts(prefix_diagnostics + search_diagnostics)
    if plans is None:
        # Infeasible due to a missing executed prefix can be detected before
        # the search; make sure those diagnostics survive.
        if not diagnostics:
            diagnostics = [
                _conflict(
                    task,
                    task.start,
                    min(cutoff, task.end),
                    "executed",
                    reason="no complete schedule exists",
                )
                for task in task_map.values()
            ]
        result = _schedule_result(
            task_map, None, records, cutoff, diagnostics, history
        )
        if raise_on_infeasible:
            raise InfeasibleSchedule(
                "no complete channel schedule exists", result.conflicts.copy()
            )
        return result

    result = _schedule_result(
        task_map, plans, records, cutoff, [], history
    )
    if raise_on_infeasible and not result.feasible:
        raise InfeasibleSchedule(
            "no complete channel schedule exists", result.conflicts.copy()
        )
    return result
