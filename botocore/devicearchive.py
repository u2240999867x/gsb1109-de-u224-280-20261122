# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""
NOTE: All classes and functions in this module are considered private and are
subject to abrupt breaking changes. Please do not use them directly.

Device event archive collection across discontinuous device clocks.

A device archives events against UTC microsecond windows that are half open
(``[start, end)``). The device clock may be steered, including being moved
backwards, so the same device-time range can be observed twice within a single
boot segment. The collector therefore queries upstream services using device
clock coordinates and reconstructs, per boot segment, which events can fall in
the requested UTC window.

Time values in this module are integer microseconds unless a name explicitly
says otherwise. Two upstream interfaces are supported:

* A millisecond JSON interface that takes millisecond Unix timestamps in a JSON
  body and can echo the exact device timestamp of some events.
* A whole-second interface that takes integer second boundaries in RFC 822
  date headers and buckets device times down to the containing second; a bucket
  label ``s`` stands for every microsecond in ``[s, s + 1s)``.

Both services filter on the post-adjustment device clock with half-open
ranges. Adjustment decisions are only known to happen inside non-overlapping
UTC candidate intervals, so adjudication considers every timeline compatible
with the collected evidence: an event is only included when every compatible
timeline places it in the window and excluded when none do.
"""
from dataclasses import dataclass, field
from email.utils import formatdate, parsedate_to_datetime
import json

MILLISECOND = "millisecond"
WHOLE_SECOND = "whole_second"

INCLUDED = "included"
EXCLUDED = "excluded"
PENDING = "pending"
CONFLICT = "conflict"
OK = "ok"

_MICROS_PER_MILLI = 1000
_MICROS_PER_SECOND = 1_000_000


def _floor_div(value, divisor):
    return value // divisor


def _ceil_div(value, divisor):
    return -((-value) // divisor)


def _merge_intervals(intervals):
    """Merge overlapping or touching half-open intervals, sorted."""
    merged = []
    for start, end in sorted(intervals):
        if start >= end:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


@dataclass
class ClockAdjustment:
    """A discrete clock adjustment within a boot segment.

    The adjustment happens once at an unknown UTC instant inside the
    half-open candidate interval ``[utc_start, utc_end)``. From that instant
    on the device clock uses a new offset changed by the signed ``jump``
    (microseconds applied to the device reading).
    """

    utc_start: int
    utc_end: int
    jump: int


@dataclass
class BootSegment:
    """A single boot: initial offset and ordered clock adjustments."""

    boot_id: object
    initial_offset: int
    adjustments: list = field(default_factory=list)

    def offsets(self):
        offsets = [self.initial_offset]
        for adjustment in self.adjustments:
            offsets.append(offsets[-1] + adjustment.jump)
        return offsets


@dataclass
class ArchiveWindow:
    """Half-open UTC archive window in microseconds."""

    start: int
    end: int

    def __post_init__(self):
        if self.start >= self.end:
            raise ValueError("Archive window must be half open and non-empty")


@dataclass
class MillisecondRequest:
    """A device-time half-open range for the millisecond JSON interface."""

    device_start: int
    device_end: int

    @property
    def start_millis(self):
        return self.device_start // _MICROS_PER_MILLI

    @property
    def end_millis(self):
        return self.device_end // _MICROS_PER_MILLI

    @property
    def body(self):
        return json.dumps(
            {
                "start_time_millis": self.start_millis,
                "end_time_millis": self.end_millis,
            },
            separators=(",", ":"),
        )


@dataclass
class WholeSecondRequest:
    """A device-time half-open range aligned to whole seconds.

    Both boundaries are sent as RFC 822 GMT date request headers.
    """

    device_start: int
    device_end: int

    @property
    def start_seconds(self):
        return self.device_start // _MICROS_PER_SECOND

    @property
    def end_seconds(self):
        return self.device_end // _MICROS_PER_SECOND

    @property
    def headers(self):
        return {
            "X-Device-Start-Date": formatdate(
                self.start_seconds, usegmt=True
            ),
            "X-Device-End-Date": formatdate(self.end_seconds, usegmt=True),
        }

    @classmethod
    def parse_header(cls, value):
        """Return the integer second boundary encoded in an RFC 822 header."""
        return int(parsedate_to_datetime(value).timestamp())


@dataclass
class MillisecondRecord:
    """An event echoed with its exact millisecond device timestamp."""

    sequence: int
    device_time_millis: int


@dataclass
class WholeSecondRecord:
    """An event bucketed into the second starting at ``second_start_millis``.

    The label represents every device microsecond in
    ``[second_start_millis, second_start_millis + 1000)`` milliseconds.
    """

    sequence: int
    second_start_millis: int


@dataclass
class CollectionPlan:
    """Device-time intervals to query, per interface, in microseconds."""

    millisecond: list
    whole_second: list


@dataclass
class EventEvidence:
    """Raw device-time evidence gathered for one event sequence."""

    sequence: int
    millisecond_millis: object = None
    whole_second_millis: object = None


@dataclass
class ArchiveDecision:
    """Adjudication of one boot segment against one UTC window."""

    status: str
    window: ArchiveWindow
    included: list
    excluded: list
    pending: list
    evidence: dict
    plan: CollectionPlan
    feasible_timeline_count: int = 0


class _ConstraintGraph:
    """Difference constraints ``a <= b + weight`` solved with Bellman-Ford.

    A fixed zero node anchors absolute (constant) bounds. Infeasibility shows
    up as a negative cycle and raises ``EvidenceConflictError``.
    """

    def __init__(self):
        self._index = {"Z": 0}
        self.edges = []

    def _node(self, name):
        if name not in self._index:
            self._index[name] = len(self._index)
        return self._index[name]

    def le(self, left, right, weight):
        self.edges.append((self._node(right), self._node(left), weight))

    def feasible(self):
        size = len(self._index)
        distances = [0] * size
        for _ in range(size):
            relaxed = False
            for source, target, weight in self.edges:
                candidate = distances[source] + weight
                if candidate < distances[target]:
                    distances[target] = candidate
                    relaxed = True
            if not relaxed:
                return True
        for source, target, weight in self.edges:
            if distances[source] + weight < distances[target]:
                return False
        return True


def _device_ranges_for_window(segment, window):
    """Project a UTC window onto device time for each offset step.

    Within step ``k`` the device clock reads ``utc + offset_k``, so the UTC
    window maps to a device-time interval per step. Backward adjustments make
    these intervals overlap; they are kept separate until the interface
    granularity is applied.
    """
    offsets = segment.offsets()
    adjustments = segment.adjustments
    ranges = []
    for step, offset in enumerate(offsets):
        utc_low = window.start
        if step > 0:
            utc_low = max(utc_low, adjustments[step - 1].utc_start)
        utc_high = window.end
        if step < len(adjustments):
            utc_high = min(utc_high, adjustments[step].utc_end)
        if utc_low < utc_high:
            ranges.append(
                (utc_low + offset, utc_high + offset)
            )
    return ranges


def _snap_millisecond(intervals):
    snapped = [
        (
            _floor_div(start, _MICROS_PER_MILLI) * _MICROS_PER_MILLI,
            _ceil_div(end, _MICROS_PER_MILLI) * _MICROS_PER_MILLI,
        )
        for start, end in intervals
    ]
    return _merge_intervals(snapped)


def _snap_whole_second(intervals):
    snapped = [
        (
            _floor_div(start, _MICROS_PER_SECOND) * _MICROS_PER_SECOND,
            _ceil_div(end, _MICROS_PER_SECOND) * _MICROS_PER_SECOND,
        )
        for start, end in intervals
    ]
    return _merge_intervals(snapped)


def build_collection_plan(segment, window):
    """Compute the minimal device-time requests for each interface.

    Each interval covers the UTC window at the granularity the interface can
    express. Distinct device-time ranges stay separate so unrelated device
    time between them is never requested.
    """
    ranges = _device_ranges_for_window(segment, window)
    return CollectionPlan(
        millisecond=_snap_millisecond(ranges),
        whole_second=_snap_whole_second(ranges),
    )


def _enumerate_piece_patterns(event_count, adjustment_count):
    """Non-decreasing step index assignments for ordered events.

    Event sequences follow real time and offset steps only move forward, so
    each event's step index is a non-decreasing sequence over ``0..m``.
    """
    patterns = []

    def build(index, minimum, current):
        if index == event_count:
            patterns.append(tuple(current))
            return
        for step in range(minimum, adjustment_count + 1):
            current.append(step)
            build(index + 1, step, current)
            current.pop()

    build(0, 0, [])
    return patterns


def _pattern_is_feasible(
    pattern, sequences, evidence_by_sequence, segment, window, extra
):
    graph = _ConstraintGraph()
    offsets = segment.offsets()
    adjustment_count = len(segment.adjustments)

    for index, adjustment in enumerate(segment.adjustments, start=1):
        step_var = f"T{index}"
        graph.le("Z", step_var, -adjustment.utc_start)
        graph.le(step_var, "Z", adjustment.utc_end - 1)
        if index > 1:
            graph.le(f"T{index - 1}", step_var, -1)

    for position, sequence in enumerate(sequences):
        step = pattern[position]
        offset = offsets[step]
        dev = f"d{sequence}"
        utc = f"u{sequence}"

        graph.le(utc, dev, -offset)
        graph.le(dev, utc, offset)

        if step > 0:
            step_var = f"T{step}"
            graph.le(step_var, dev, -offset)
            graph.le(step_var, utc, 0)
        if step < adjustment_count:
            next_var = f"T{step + 1}"
            graph.le(dev, next_var, offset - 1)
            graph.le(utc, next_var, -1)

        evidence = evidence_by_sequence.get(sequence)
        if evidence is not None:
            for kind, value in evidence:
                if kind == MILLISECOND:
                    graph.le("Z", dev, -value)
                    graph.le(dev, "Z", value)
                else:
                    graph.le("Z", dev, -value)
                    graph.le(dev, "Z", value + _MICROS_PER_SECOND - 1)

        if position > 0:
            previous = f"u{sequences[position - 1]}"
            graph.le(previous, utc, -1)

    for kind, sequence in extra:
        utc = f"u{sequence}"
        if kind == "in":
            graph.le("Z", utc, -window.start)
            graph.le(utc, "Z", window.end - 1)
        elif kind == "out-left":
            graph.le(utc, "Z", window.start - 1)
        else:
            graph.le("Z", utc, -window.end)

    return graph.feasible()


def _collect_evidence(millisecond_records, whole_second_records):
    evidence_by_sequence = {}
    raw_evidence = {}

    def add(sequence, kind, value):
        evidence_by_sequence.setdefault(sequence, []).append((kind, value))
        raw = raw_evidence.setdefault(
            sequence, EventEvidence(sequence=sequence)
        )
        if kind == MILLISECOND:
            raw.millisecond_millis = value // _MICROS_PER_MILLI
        else:
            raw.whole_second_millis = value // _MICROS_PER_MILLI

    for record in millisecond_records:
        add(
            record.sequence,
            MILLISECOND,
            record.device_time_millis * _MICROS_PER_MILLI,
        )
    for record in whole_second_records:
        add(
            record.sequence,
            WHOLE_SECOND,
            record.second_start_millis * _MICROS_PER_MILLI,
        )
    return evidence_by_sequence, raw_evidence


def adjudicate(
    segment,
    window,
    millisecond_records,
    whole_second_records,
    plan=None,
):
    """Decide event membership using all evidence from one boot segment.

    Returns an ``ArchiveDecision``. When the evidence admits no timeline the
    whole segment is reported as a conflict and no event is delivered as an
    adjudicated result.
    """
    if plan is None:
        plan = build_collection_plan(segment, window)

    evidence_by_sequence, raw_evidence = _collect_evidence(
        millisecond_records, whole_second_records
    )
    sequences = sorted(evidence_by_sequence)
    patterns = _enumerate_piece_patterns(
        len(sequences), len(segment.adjustments)
    )

    feasible_patterns = [
        pattern
        for pattern in patterns
        if _pattern_is_feasible(
            pattern,
            sequences,
            evidence_by_sequence,
            segment,
            window,
            (),
        )
    ]

    if not feasible_patterns:
        return ArchiveDecision(
            status=CONFLICT,
            window=window,
            included=[],
            excluded=[],
            pending=[],
            evidence=raw_evidence,
            plan=plan,
            feasible_timeline_count=0,
        )

    included = []
    excluded = []
    pending = []
    for sequence in sequences:
        can_be_in = any(
            _pattern_is_feasible(
                pattern,
                sequences,
                evidence_by_sequence,
                segment,
                window,
                (("in", sequence),),
            )
            for pattern in feasible_patterns
        )
        can_be_left = any(
            _pattern_is_feasible(
                pattern,
                sequences,
                evidence_by_sequence,
                segment,
                window,
                (("out-left", sequence),),
            )
            for pattern in feasible_patterns
        )
        can_be_right = any(
            _pattern_is_feasible(
                pattern,
                sequences,
                evidence_by_sequence,
                segment,
                window,
                (("out-right", sequence),),
            )
            for pattern in feasible_patterns
        )
        can_be_out = can_be_left or can_be_right
        if can_be_in and not can_be_out:
            included.append(sequence)
        elif can_be_out and not can_be_in:
            excluded.append(sequence)
        else:
            pending.append(sequence)

    return ArchiveDecision(
        status=OK,
        window=window,
        included=included,
        excluded=excluded,
        pending=pending,
        evidence=raw_evidence,
        plan=plan,
        feasible_timeline_count=len(feasible_patterns),
    )


class RecordingSource:
    """Test double transport that records every request it receives."""

    def __init__(self, handler):
        self.handler = handler
        self.requests = []

    def send(self, request):
        self.requests.append(request)
        return list(self.handler(request))


class DeviceArchiveCollector:
    """Collects and adjudicates one boot segment against one UTC window."""

    def __init__(self, millisecond_source, whole_second_source=None):
        self.millisecond_source = millisecond_source
        self.whole_second_source = whole_second_source

    def collect(self, segment, window):
        plan = build_collection_plan(segment, window)
        millisecond_records = []
        for start, end in plan.millisecond:
            millisecond_records.extend(
                self.millisecond_source.send(
                    MillisecondRequest(start, end)
                )
            )

        whole_second_records = []
        if self.whole_second_source is not None:
            for start, end in plan.whole_second:
                whole_second_records.extend(
                    self.whole_second_source.send(
                        WholeSecondRequest(start, end)
                    )
                )

        return adjudicate(
            segment,
            window,
            millisecond_records,
            whole_second_records,
            plan=plan,
        )


def verify_coverage(planned_intervals, received_requests):
    """Check that received requests exactly cover the planned intervals.

    Returns a ``(missing, extra)`` tuple of interval lists; both are empty
    when the union of received half-open device-time ranges matches the plan
    without gaps or over-fetching.
    """
    received = _merge_intervals(
        [
            (request.device_start, request.device_end)
            for request in received_requests
        ]
    )
    planned = _merge_intervals(planned_intervals)
    missing = []
    for start, end in planned:
        covered_start = start
        for received_start, received_end in received:
            if received_end <= covered_start:
                continue
            if received_start > covered_start:
                missing.append((covered_start, min(end, received_start)))
            covered_start = max(covered_start, received_end)
            if covered_start >= end:
                break
        if covered_start < end:
            missing.append((covered_start, end))

    extra = []
    for start, end in received:
        for planned_start, planned_end in planned:
            overlap_start = max(start, planned_start)
            overlap_end = min(end, planned_end)
            if overlap_start < overlap_end:
                if start < overlap_start:
                    extra.append((start, overlap_start))
                start = overlap_end
        if start < end:
            extra.append((start, end))
    return _merge_intervals(missing), _merge_intervals(extra)
