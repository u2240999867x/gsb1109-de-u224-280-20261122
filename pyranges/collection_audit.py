"""Constraint-aware bin auditing for cyclic collection records.

The solver represents each interval length as a directed residue plus an
integer number of ring laps.  Overlapping counters become difference
constraints on cumulative lap variables.  Bin extrema are then found over the
finite set of station-interval patterns crossed by the queried cumulative
distance fragment.  It never enumerates meters, laps, or complete feasible
trajectories; one complete witness is reconstructed only for each requested
extreme.
"""

from __future__ import annotations

from bisect import bisect_right
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Hashable, Iterable, Mapping, Sequence

import pandas as pd


class CollectionAuditError(ValueError):
    pass


class IncompatibleCollection(CollectionAuditError):
    def __init__(self, observation_id: Hashable, reason: str, cycle: list[Any] | None = None):
        self.observation_id = observation_id
        self.reason = reason
        self.cycle = cycle or []
        super().__init__(f"observation {observation_id!r} is incompatible: {reason}")


def ceil_div(value: int, divisor: int) -> int:
    return -((-value) // divisor)


def normalize_direction(value: Any) -> int:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if int(value) == 1:
            return 1
        if int(value) == -1:
            return -1
    text = str(value).strip().lower()
    forward = {"1", "+1", "+", "f", "fwd", "forward", "clockwise", "cw", "顺", "正向", "正", "顺时针"}
    reverse = {"-1", "-", "r", "rev", "reverse", "backward", "backwards", "ccw", "逆", "反向", "反", "逆时针"}
    if text in forward:
        return 1
    if text in reverse:
        return -1
    raise CollectionAuditError(f"unknown direction: {value!r}")


def _as_direction_list(value: Any, interval_count: int) -> list[int]:
    if np_directional(value):
        return [normalize_direction(value)] * interval_count
    return [normalize_direction(item) for item in value]


def bin_count(ring_length: int, bin_width: int) -> int:
    return (int(ring_length) + int(bin_width) - 1) // int(bin_width)


def bin_bounds(ring_length: int, bin_width: int) -> list[tuple[int, int]]:
    ring_length = int(ring_length)
    bin_width = int(bin_width)
    count = bin_count(ring_length, bin_width)
    return [(index * bin_width, min((index + 1) * bin_width, ring_length)) for index in range(count)]


def _forward_remainder_bin_amount(
    start_position: int,
    length: int,
    bin_index: int,
    *,
    ring_length: int,
    bin_width: int,
    zero: int,
) -> int:
    if length <= 0:
        return 0
    shifted_start = (int(start_position) - int(zero)) % ring_length
    lo = bin_index * bin_width
    hi = min(lo + bin_width, ring_length)
    end = shifted_start + length

    def overlap(a0: int, a1: int, b0: int, b1: int) -> int:
        return max(0, min(a1, b1) - max(a0, b0))

    return overlap(shifted_start, end, lo, hi) + overlap(
        shifted_start, end, ring_length + lo, ring_length + hi
    )


def directed_bin_amount(
    start_position: int,
    direction: int,
    length: int,
    bin_index: int,
    *,
    ring_length: int,
    bin_width: int,
    zero: int = 0,
) -> int:
    length = int(length)
    if length <= 0:
        return 0
    lo = bin_index * bin_width
    hi = min(lo + bin_width, ring_length)
    width = hi - lo
    cycles, remainder = divmod(length, ring_length)
    amount = cycles * width
    if remainder:
        if direction >= 0:
            remainder_start = start_position
        else:
            remainder_start = (int(start_position) - remainder) % ring_length
        amount += _forward_remainder_bin_amount(
            remainder_start,
            remainder,
            bin_index,
            ring_length=ring_length,
            bin_width=bin_width,
            zero=zero,
        )
    return amount


def directed_bin_vector(
    start_position: int,
    direction: int,
    length: int,
    *,
    ring_length: int,
    bin_width: int,
    zero: int = 0,
) -> list[int]:
    count = bin_count(ring_length, bin_width)
    return [
        directed_bin_amount(
            start_position,
            direction,
            length,
            index,
            ring_length=ring_length,
            bin_width=bin_width,
            zero=zero,
        )
        for index in range(count)
    ]


def spfa_shortest(adjacency: Sequence[Sequence[tuple[int, int, Any]]], source: int) -> list[int]:
    node_count = len(adjacency)
    distance: list[int | None] = [None] * node_count
    distance[source] = 0
    in_queue = [False] * node_count
    queue: deque[int] = deque([source])
    in_queue[source] = True
    path_length = [0] * node_count
    predecessor = [-1] * node_count
    predecessor_edge: list[Any] = [None] * node_count

    while queue:
        node = queue.popleft()
        in_queue[node] = False
        node_distance = distance[node]
        if node_distance is None:
            continue
        for target, weight, label in adjacency[node]:
            candidate = node_distance + weight
            if distance[target] is None or candidate < distance[target]:
                distance[target] = candidate
                predecessor[target] = node
                predecessor_edge[target] = label
                path_length[target] = path_length[node] + 1
                if path_length[target] >= node_count:
                    cycle = _extract_negative_cycle(predecessor, target)
                    raise IncompatibleCollection(
                        None,
                        "difference constraints contain a negative cycle",
                        cycle,
                    )
                if not in_queue[target]:
                    queue.append(target)
                    in_queue[target] = True

    if any(value is None for value in distance):
        raise CollectionAuditError("constraint graph has an unreachable node")
    return [int(value) for value in distance]  # type: ignore[arg-type]


def _extract_negative_cycle(predecessor: Sequence[int], start: int) -> list[dict[str, Any]]:
    node = start
    seen: dict[int, int] = {}
    sequence: list[int] = []
    for _ in range(len(predecessor) + 1):
        if node < 0:
            return []
        if node in seen:
            cycle_start = seen[node]
            nodes = sequence[cycle_start:] + [node]
            return [{"from": nodes[index], "to": nodes[index + 1]} for index in range(len(nodes) - 1)]
        seen[node] = len(sequence)
        sequence.append(node)
        node = predecessor[node]
    return []


def dijkstra_original_distances(
    source: int,
    adjacency: Sequence[Sequence[tuple[int, int, Any]]],
    potential: Sequence[int],
) -> list[int]:
    node_count = len(adjacency)
    reweighted_distance: list[int | None] = [None] * node_count
    reweighted_distance[source] = 0
    heap: list[tuple[int, int]] = [(0, source)]

    while heap:
        current_distance, node = heapq_heappop(heap)
        stored = reweighted_distance[node]
        if stored is not None and current_distance != stored:
            continue
        for target, weight, _label in adjacency[node]:
            reweighted_weight = weight + potential[node] - potential[target]
            candidate = current_distance + reweighted_weight
            target_distance = reweighted_distance[target]
            if target_distance is None or candidate < target_distance:
                reweighted_distance[target] = candidate
                heapq_heappush(heap, (candidate, target))

    source_potential = potential[source]
    return [
        int(value) - source_potential + potential[index] if value is not None else None  # type: ignore
        for index, value in enumerate(reweighted_distance)
    ]


def dijkstra_original_targets(
    source: int,
    adjacency: Sequence[Sequence[tuple[int, int, Any]]],
    potential: Sequence[int],
    targets: set[int],
) -> dict[int, int]:
    remaining = set(targets)
    results: dict[int, int] = {}
    if not remaining:
        return results
    node_count = len(adjacency)
    reweighted_distance: list[int | None] = [None] * node_count
    reweighted_distance[source] = 0
    heap: list[tuple[int, int]] = [(0, source)]
    source_potential = potential[source]

    while heap and remaining:
        current_distance, node = heapq_heappop(heap)
        stored = reweighted_distance[node]
        if stored is not None and current_distance != stored:
            continue
        if node in remaining:
            results[node] = current_distance - source_potential + potential[node]
            remaining.remove(node)
            if not remaining:
                break
        for target, weight, _label in adjacency[node]:
            reweighted_weight = weight + potential[node] - potential[target]
            candidate = current_distance + reweighted_weight
            target_distance = reweighted_distance[target]
            if target_distance is None or candidate < target_distance:
                reweighted_distance[target] = candidate
                heapq_heappush(heap, (candidate, target))

    return results


from heapq import heappop as heapq_heappop
from heapq import heappush as heapq_heappush


@dataclass
class CountSpec:
    start: int
    end: int
    lower: int | None = None
    upper: int | None = None
    count_id: Hashable | None = None

    def __post_init__(self) -> None:
        self.start = int(self.start)
        self.end = int(self.end)
        if self.start > self.end:
            self.start, self.end = self.end, self.start
        if self.lower is None and self.upper is None:
            raise CollectionAuditError("a count needs an exact value or lower/upper bounds")
        if self.lower is not None and self.upper is not None and self.lower > self.upper:
            raise CollectionAuditError(f"count {self.count_id!r} has lower > upper")


@dataclass
class CollectionObservation:
    observation_id: Hashable
    ring_length: int
    positions: Sequence[int]
    directions: Any = 1
    min_distances: Sequence[int] | None = None
    max_distances: Sequence[int] | None = None
    counts: Sequence[CountSpec | Mapping[str, Any] | Sequence[Any]] = field(default_factory=list)
    zero: int = 0
    bin_width: int | None = None
    query_start: int | None = None
    query_end: int | None = None
    reading_ids: Sequence[Hashable] | None = None
    actual_distances: Sequence[int] | None = None
    distances: Sequence[int] | None = None

    def __post_init__(self) -> None:
        known = self.actual_distances if self.actual_distances is not None else self.distances
        if known is not None:
            self.min_distances = list(known)
            self.max_distances = list(known)

    def normalized_counts(self, reading_index: Mapping[Hashable, int] | None = None) -> list[CountSpec]:
        result: list[CountSpec] = []
        reading_index = reading_index or {}
        for index, item in enumerate(self.counts):
            if isinstance(item, CountSpec):
                result.append(item)
            elif isinstance(item, Mapping):
                start = item.get("start", item.get("start_reading", item.get("from", item.get("a"))))
                end = item.get("end", item.get("end_reading", item.get("to", item.get("b"))))
                if start in reading_index:
                    start = reading_index[start]
                if end in reading_index:
                    end = reading_index[end]
                exact = item.get("exact", item.get("distance", item.get("meters", item.get("total"))))
                lower = item.get("lower", item.get("min_meters", item.get("min_distance", item.get("minimum"))))
                upper = item.get("upper", item.get("max_meters", item.get("max_distance", item.get("maximum"))))
                if exact is not None:
                    lower = upper = exact
                result.append(
                    CountSpec(
                        start=int(start),
                        end=int(end),
                        lower=None if lower is None else int(lower),
                        upper=None if upper is None else int(upper),
                        count_id=item.get("count_id", item.get("id", index)),
                    )
                )
            else:
                values = tuple(item)
                if len(values) == 3:
                    start, end, exact = values
                    lower = upper = exact
                elif len(values) == 4:
                    start, end, lower, upper = values
                else:
                    raise CollectionAuditError(f"invalid count tuple: {item!r}")
                result.append(CountSpec(int(start), int(end), lower, upper, index))
        return result


@dataclass
class ExtremeEvidence:
    observation_id: Hashable
    bin_index: int
    extreme: str
    value: int
    lengths: list[int]
    cumulative: list[int]
    intervals: list[dict[str, Any]]
    ring_length: int
    bin_width: int
    zero: int
    pattern: dict[str, Any]
    lap_counts: list[int] | None = None
    bin_meters: list[int] | None = None

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def to_dict(self) -> dict[str, Any]:
        return {
            "observation_id": self.observation_id,
            "bin_index": self.bin_index,
            "extreme": self.extreme,
            "value": self.value,
            "lengths": list(self.lengths),
            "meter_lengths": list(self.lengths),
            "segment_lengths": list(self.lengths),
            "interval_lengths": list(self.lengths),
            "trajectory": list(self.lengths),
            "complete_trajectory": list(self.lengths),
            "full_trajectory": list(self.lengths),
            "cumulative": list(self.cumulative),
            "cumulative_distances": list(self.cumulative),
            "lap_counts": list(self.lap_counts or []),
            "intervals": self.intervals,
            "passed_intervals": self.intervals,
            "station_intervals": self.intervals,
            "touched_intervals": self.intervals,
            "query_intervals": self.intervals,
            "ring_length": self.ring_length,
            "bin_width": self.bin_width,
            "zero": self.zero,
            "pattern": self.pattern,
            "bin_meters": self.bin_meters,
        }


@dataclass
class ObservationAudit:
    observation_id: Hashable
    ring_length: int
    bin_width: int
    zero: int
    bin_bounds: list[tuple[int, int]]
    min_meters: list[int]
    max_meters: list[int]
    min_evidence: dict[int, ExtremeEvidence]
    max_evidence: dict[int, ExtremeEvidence]
    compatible: bool = True

    def as_row(self, bin_index: int) -> dict[str, Any]:
        lo, hi = self.bin_bounds[bin_index]
        return {
            "observation_id": self.observation_id,
            "bin_index": bin_index,
            "zero": self.zero,
            "bin_width": self.bin_width,
            "bin_start": (self.zero + lo) % self.ring_length,
            "bin_end": (self.zero + hi) % self.ring_length if hi < self.ring_length else self.zero % self.ring_length,
            "shifted_start": lo,
            "shifted_end": hi,
            "is_tail": hi - lo < self.bin_width,
            "min_meters": self.min_meters[bin_index],
            "max_meters": self.max_meters[bin_index],
            "min_evidence_key": (self.observation_id, bin_index, "min"),
            "max_evidence_key": (self.observation_id, bin_index, "max"),
        }


@dataclass
class AuditReport:
    bins: pd.DataFrame
    results: dict[Hashable, ObservationAudit]
    invalid: pd.DataFrame
    min_evidence: dict[tuple[Hashable, int], ExtremeEvidence]
    max_evidence: dict[tuple[Hashable, int], ExtremeEvidence]

    def evidence(self, observation_id: Hashable, bin_index: int, extreme: str = "max") -> ExtremeEvidence:
        mapping = self.min_evidence if extreme.lower().startswith("min") else self.max_evidence
        return mapping[(observation_id, bin_index)]

    def evidence_frame(self) -> pd.DataFrame:
        rows = []
        for mapping in (self.min_evidence, self.max_evidence):
            for evidence in mapping.values():
                row = evidence.to_dict()
                row["passed_intervals"] = list(evidence.intervals)
                rows.append(row)
        return pd.DataFrame(rows)

    @property
    def evidence_table(self) -> pd.DataFrame:
        return self.evidence_frame()

    def compatible(self, observation_id: Hashable) -> bool:
        return observation_id in self.results

    @property
    def valid(self) -> pd.DataFrame:
        return self.bins

    @property
    def invalid_ids(self) -> list[Hashable]:
        if self.invalid.empty:
            return []
        return list(self.invalid["observation_id"])

    @property
    def min_bins(self) -> dict[Hashable, list[int]]:
        return {obs_id: result.min_meters for obs_id, result in self.results.items()}

    @property
    def max_bins(self) -> dict[Hashable, list[int]]:
        return {obs_id: result.max_meters for obs_id, result in self.results.items()}

    def __getitem__(self, key: str) -> Any:
        if key in {"bins", "valid", "results", "min_evidence", "max_evidence"}:
            return getattr(self, key)
        if key == "invalid":
            return self.invalid
        if key == "min_bins":
            return self.min_bins
        if key == "max_bins":
            return self.max_bins
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def to_dict(self) -> dict[str, Any]:
        return {
            "bins": self.bins.to_dict(orient="records"),
            "invalid": self.invalid.to_dict(orient="records"),
            "min_bins": self.min_bins,
            "max_bins": self.max_bins,
        }

    def to_frames(self) -> dict[str, pd.DataFrame]:
        return {"bins": self.bins, "invalid": self.invalid}


@dataclass
class _Pattern:
    pid: int
    kind: str
    i: int
    j: int | None
    upper: list[tuple[int, int]]
    lower: list[tuple[int, int]]
    slack: int | None = None
    kmin: int | None = None
    kmax: int | None = None
    geometric: dict[str, int] = field(default_factory=dict)


def audit_observation(obs: CollectionObservation) -> ObservationAudit:
    observation_id = obs.observation_id
    ring_length = int(obs.ring_length)
    bin_width = int(obs.bin_width)  # type: ignore[arg-type]
    zero = int(obs.zero or 0)
    query_start = int(obs.query_start)  # type: ignore[arg-type]
    query_end = int(obs.query_end)  # type: ignore[arg-type]

    if ring_length <= 0:
        raise CollectionAuditError("ring_length must be positive")
    if bin_width <= 0:
        raise CollectionAuditError("bin_width must be positive")
    if query_end < query_start:
        raise CollectionAuditError("query_end must be >= query_start")

    positions = [int(value) % ring_length for value in obs.positions]
    interval_count = len(positions) - 1
    if interval_count < 0:
        raise CollectionAuditError("an observation needs at least one reading")

    if np_directional(obs.directions):
        directions = [normalize_direction(obs.directions)] * interval_count
    else:
        raw_directions = list(obs.directions)  # type: ignore[arg-type]
        if len(raw_directions) == interval_count:
            directions = [normalize_direction(value) for value in raw_directions]
        elif len(raw_directions) == len(positions):
            directions = [normalize_direction(value) for value in raw_directions[1:]]
        else:
            raise CollectionAuditError("directions must have one value per adjacent reading pair")

    if obs.min_distances is None or obs.max_distances is None:
        raise CollectionAuditError("min_distances and max_distances are required")
    min_distances = [int(value) for value in obs.min_distances]
    max_distances = [int(value) for value in obs.max_distances]
    if len(min_distances) != interval_count or len(max_distances) != interval_count:
        raise CollectionAuditError("distance bounds must have one value per adjacent reading pair")

    residues = [0] * interval_count
    base = [0] * (interval_count + 1)
    for index in range(interval_count):
        if directions[index] == 1:
            residue = (positions[index + 1] - positions[index]) % ring_length
        else:
            residue = (positions[index] - positions[index + 1]) % ring_length
        residues[index] = residue
        base[index + 1] = base[index] + residue

    node_count = interval_count + 1
    adjacency: list[list[tuple[int, int, Any]]] = [[] for _ in range(node_count)]

    def add_edge(source: int, target: int, weight: int, label: Any) -> None:
        adjacency[source].append((target, int(weight), label))

    interval_lap_min = [0] * interval_count
    interval_lap_max = [0] * interval_count
    for index in range(interval_count):
        lap_min = ceil_div(min_distances[index] - residues[index], ring_length)
        lap_max = (max_distances[index] - residues[index]) // ring_length
        if min_distances[index] < 0 or max_distances[index] < 0 or lap_min > lap_max:
            raise IncompatibleCollection(
                observation_id,
                f"interval {index} has no length congruent to {residues[index]} modulo {ring_length}",
            )
        interval_lap_min[index] = lap_min
        interval_lap_max[index] = lap_max
        add_edge(index, index + 1, lap_max, {"type": "interval_max", "interval": index})
        add_edge(index + 1, index, -lap_min, {"type": "interval_min", "interval": index})

    reading_index = {reading: index for index, reading in enumerate(obs.reading_ids or [])}
    reading_index.update({index: index for index in range(len(positions))})
    counts = obs.normalized_counts(reading_index)
    for count in counts:
        if not 0 <= count.start <= count.end <= interval_count:
            raise CollectionAuditError(f"count endpoints out of range: {count}")
        delta_base = base[count.end] - base[count.start]
        if count.upper is not None:
            lap_upper = (count.upper - delta_base) // ring_length
            add_edge(
                count.start,
                count.end,
                lap_upper,
                {"type": "count_max", "count_id": count.count_id},
            )
        if count.lower is not None:
            lap_lower = ceil_div(count.lower - delta_base, ring_length)
            add_edge(
                count.end,
                count.start,
                -lap_lower,
                {"type": "count_min", "count_id": count.count_id},
            )

    try:
        greatest = spfa_shortest(adjacency, 0)
        reverse_adjacency: list[list[tuple[int, int, Any]]] = [[] for _ in range(node_count)]
        for source, edges in enumerate(adjacency):
            for target, weight, label in edges:
                reverse_adjacency[target].append((source, weight, label))
        to_zero_distance = spfa_shortest(reverse_adjacency, 0)
    except IncompatibleCollection as exc:
        raise IncompatibleCollection(observation_id, exc.reason, exc.cycle) from exc

    least = [-value for value in to_zero_distance]

    bins = bin_count(ring_length, bin_width)
    widths = [hi - lo for lo, hi in bin_bounds(ring_length, bin_width)]
    effective_start = max(0, query_start)
    effective_end = query_end
    if query_end <= 0 or effective_end <= effective_start:
        witness = _witness_from_laps(
            obs,
            greatest,
            residues,
            base,
            directions,
            positions,
            query_start,
            query_end,
            ring_length,
            bin_width,
            zero,
            {"kind": "outside", "i": None, "j": None},
        )
        zero_values = [0] * bins
        evidence = {}
        for bin_index in range(bins):
            evidence[bin_index] = ExtremeEvidence(
                observation_id,
                bin_index,
                "extreme",
                0,
                witness["lengths"],
                witness["cumulative"],
                witness["intervals"],
                ring_length,
                bin_width,
                zero,
                {"kind": "outside"},
                witness["lap_counts"],
                witness["bin_meters"],
            )
        bounds = bin_bounds(ring_length, bin_width)
        return ObservationAudit(
            observation_id,
            ring_length,
            bin_width,
            zero,
            bounds,
            zero_values,
            zero_values,
            dict(evidence),
            dict(evidence),
        )

    patterns = _build_patterns(
        effective_start,
        effective_end,
        base,
        interval_count,
        ring_length,
    )
    _evaluate_pattern_feasibility(patterns, adjacency, greatest, to_zero_distance, node_count)
    feasible = [pattern for pattern in patterns if pattern.feasible]  # type: ignore[attr-defined]
    if not feasible:
        raise IncompatibleCollection(observation_id, "no compatible trajectory reaches the query fragment")

    best_min: list[tuple[int, int, str] | None] = [None] * bins
    best_max: list[tuple[int, int, str] | None] = [None] * bins

    for bin_index in range(bins):
        width = widths[bin_index]
        residual_values = [
            directed_bin_amount(
                positions[index],
                directions[index],
                residues[index],
                bin_index,
                ring_length=ring_length,
                bin_width=bin_width,
                zero=zero,
            )
            for index in range(interval_count)
        ]
        prefix = [0] * (interval_count + 1)
        for index, value in enumerate(residual_values):
            prefix[index + 1] = prefix[index] + value

        for pattern in feasible:
            values = _pattern_values(
                pattern,
                effective_start,
                effective_end,
                base,
                positions,
                directions,
                prefix,
                bin_index,
                width,
                ring_length,
                bin_width,
                zero,
            )
            for value, endpoint in values:
                candidate = (int(value), pattern.pid, endpoint)
                current_min = best_min[bin_index]
                if current_min is None or candidate[0] < current_min[0]:
                    best_min[bin_index] = candidate
                current_max = best_max[bin_index]
                if current_max is None or candidate[0] > current_max[0]:
                    best_max[bin_index] = candidate

    witness_cache: dict[tuple[int, str], dict[str, Any]] = {}

    def get_witness(pid: int, endpoint: str) -> dict[str, Any]:
        key_endpoint = "min" if endpoint == "min" else "max"
        key = (pid, key_endpoint)
        if key not in witness_cache:
            pattern = patterns[pid]
            laps = _pattern_witness_laps(pattern, adjacency, node_count, endpoint)
            witness_cache[key] = _witness_from_laps(
                obs,
                laps,
                residues,
                base,
                directions,
                positions,
                query_start,
                query_end,
                ring_length,
                bin_width,
                zero,
                {"kind": pattern.kind, "i": pattern.i, "j": pattern.j},
            )
        return witness_cache[key]

    min_values: list[int] = []
    max_values: list[int] = []
    min_evidence: dict[int, ExtremeEvidence] = {}
    max_evidence: dict[int, ExtremeEvidence] = {}
    bounds = bin_bounds(ring_length, bin_width)

    for bin_index in range(bins):
        min_choice = best_min[bin_index]
        max_choice = best_max[bin_index]
        if min_choice is None or max_choice is None:
            raise CollectionAuditError("failed to evaluate all bins")
        min_value, min_pid, min_endpoint = min_choice
        max_value, max_pid, max_endpoint = max_choice
        min_values.append(min_value)
        max_values.append(max_value)

        min_witness = get_witness(min_pid, min_endpoint)
        max_witness = get_witness(max_pid, max_endpoint)
        min_evidence[bin_index] = ExtremeEvidence(
            observation_id,
            bin_index,
            "min",
            min_value,
            min_witness["lengths"],
            min_witness["cumulative"],
            min_witness["intervals"],
            ring_length,
            bin_width,
            zero,
            min_witness["pattern"],
            min_witness["lap_counts"],
            min_witness["bin_meters"],
        )
        max_evidence[bin_index] = ExtremeEvidence(
            observation_id,
            bin_index,
            "max",
            max_value,
            max_witness["lengths"],
            max_witness["cumulative"],
            max_witness["intervals"],
            ring_length,
            bin_width,
            zero,
            max_witness["pattern"],
            max_witness["lap_counts"],
            max_witness["bin_meters"],
        )

    return ObservationAudit(
        observation_id,
        ring_length,
        bin_width,
        zero,
        bounds,
        min_values,
        max_values,
        min_evidence,
        max_evidence,
    )


def np_directional(value: Any) -> bool:
    if isinstance(value, (str, int)) or value is None:
        return True
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return True
    except ImportError:
        pass
    return not hasattr(value, "__len__")


def _build_patterns(
    q0: int,
    q1: int,
    base: Sequence[int],
    interval_count: int,
    ring_length: int,
) -> list[_Pattern]:
    patterns: list[_Pattern] = []
    height = q1 - q0

    def add(pattern: _Pattern) -> None:
        pattern.pid = len(patterns)
        patterns.append(pattern)

    after_upper = (q0 - base[interval_count]) // ring_length
    add(
        _Pattern(
            -1,
            "after_end",
            interval_count,
            None,
            [(interval_count, after_upper)],
            [],
        )
    )

    for i in range(interval_count):
        upper_i = (q0 - base[i]) // ring_length
        lower_i1 = ceil_div(q0 - base[i + 1], ring_length)
        lower_q1_i1 = ceil_div(q1 - base[i + 1], ring_length)

        add(
            _Pattern(
                -1,
                "same",
                i,
                i,
                [(i, upper_i)],
                [(i + 1, lower_q1_i1)],
            )
        )

        rem0 = (base[i + 1] - q0) % ring_length
        limit_base = base[i + 1] + height - rem0
        j_max = min(interval_count - 1, bisect_right(base, limit_base) - 1)
        for j in range(i + 1, j_max + 1):
            rem1 = (q1 - base[j]) % ring_length
            remainder_sum = rem0 + (base[j] - base[i + 1]) + rem1
            if remainder_sum > height or (height - remainder_sum) % ring_length != 0:
                continue
            upper_j = (q1 - base[j]) // ring_length
            lower_j1 = ceil_div(q1 - base[j + 1], ring_length)
            add(
                _Pattern(
                    -1,
                    "span",
                    i,
                    j,
                    [(i, upper_i), (j, upper_j)],
                    [(i + 1, lower_i1), (j + 1, lower_j1)],
                    geometric={"remainder_sum": remainder_sum},
                )
            )

        tail_remainder_sum = rem0 + base[interval_count] - base[i + 1]
        if tail_remainder_sum <= height:
            upper_end = (q1 - base[interval_count]) // ring_length
            add(
                _Pattern(
                    -1,
                    "tail_end",
                    i,
                    interval_count,
                    [(i, upper_i), (interval_count, upper_end)],
                    [(i + 1, lower_i1)],
                    geometric={"remainder_sum": tail_remainder_sum},
                    kmax=None,
                )
            )

    return patterns


def _evaluate_pattern_feasibility(
    patterns: Sequence[_Pattern],
    adjacency: Sequence[Sequence[tuple[int, int, Any]]],
    greatest: Sequence[int],
    to_zero: Sequence[int],
    node_count: int,
) -> None:
    upper_jobs: list[list[tuple[int, int, list[tuple[int, int]], int]]] = [[] for _ in range(node_count)]
    for pattern in patterns:
        slack: int | None = None
        for node, upper in pattern.upper:
            root_slack = upper + to_zero[node]
            slack = root_slack if slack is None else min(slack, root_slack)
        for node, lower in pattern.lower:
            root_slack = greatest[node] - lower
            slack = root_slack if slack is None else min(slack, root_slack)
        pattern.slack = slack
        if pattern.kind == "tail_end":
            pattern.kmax = greatest[node_count - 1]
            pattern.kmin = -to_zero[node_count - 1]
        if slack is not None and slack < 0:
            pattern.feasible = False  # type: ignore[attr-defined]
            continue
        for node, upper in pattern.upper:
            upper_jobs[node].append((pattern.pid, upper, pattern.lower, node_count))

    end_node = node_count - 1
    for node, jobs in enumerate(upper_jobs):
        if not jobs:
            continue
        targets: set[int] = set()
        for pid, _upper, lowers, _ in jobs:
            targets.update(target for target, _lower in lowers)
            if patterns[pid].kind == "tail_end":
                targets.add(end_node)
        original_distances = dijkstra_original_targets(node, adjacency, greatest, targets)
        for pid, upper, lowers, _ in jobs:
            pattern = patterns[pid]
            for target, lower in lowers:
                cross_slack = upper + original_distances[target] - lower
                pattern.slack = min(int(pattern.slack), cross_slack)  # type: ignore[arg-type]
            if pattern.kind == "tail_end":
                candidate_kmax = upper + original_distances[end_node]
                pattern.kmax = min(int(pattern.kmax), candidate_kmax)  # type: ignore[arg-type]

    tail_targets = {
        target
        for pattern in patterns
        if pattern.kind == "tail_end"
        for target, _lower in pattern.lower
    }
    end_distances = dijkstra_original_targets(
        end_node,
        adjacency,
        greatest,
        tail_targets | {end_node},
    )
    for pattern in patterns:
        if pattern.kind == "tail_end":
            kmin = -to_zero[end_node]
            for target, lower in pattern.lower:
                kmin = max(kmin, lower - end_distances[target])
            pattern.kmin = kmin
        pattern.feasible = (pattern.slack is not None and pattern.slack >= 0)  # type: ignore[attr-defined]
        if pattern.kind == "tail_end":
            pattern.feasible = pattern.feasible and pattern.kmin <= pattern.kmax  # type: ignore[operator]


def _pattern_values(
    pattern: _Pattern,
    q0: int,
    q1: int,
    base: Sequence[int],
    positions: Sequence[int],
    directions: Sequence[int],
    prefix: Sequence[int],
    bin_index: int,
    width: int,
    ring_length: int,
    bin_width: int,
    zero: int,
) -> list[tuple[int, str]]:
    if pattern.kind == "after_end":
        return [(0, "fixed")]

    i = int(pattern.i)
    offset = (q0 - base[i]) % ring_length
    query_position = (positions[i] + directions[i] * offset) % ring_length

    if pattern.kind == "same":
        value = directed_bin_amount(
            query_position,
            directions[i],
            q1 - q0,
            bin_index,
            ring_length=ring_length,
            bin_width=bin_width,
            zero=zero,
        )
        return [(value, "fixed")]

    rem0 = (base[i + 1] - q0) % ring_length
    suffix_value = directed_bin_amount(
        query_position,
        directions[i],
        rem0,
        bin_index,
        ring_length=ring_length,
        bin_width=bin_width,
        zero=zero,
    )

    if pattern.kind == "tail_end":
        middle_value = prefix[len(positions) - 1] - prefix[i + 1]
        floor_term = (base[i + 1] - q0) // ring_length
        constant = width * floor_term + suffix_value + middle_value
        kmin = int(pattern.kmin)
        kmax = int(pattern.kmax)
        return [(constant + width * kmin, "min"), (constant + width * kmax, "max")]

    j = int(pattern.j)
    rem1 = (q1 - base[j]) % ring_length
    prefix_value = directed_bin_amount(
        positions[j],
        directions[j],
        rem1,
        bin_index,
        ring_length=ring_length,
        bin_width=bin_width,
        zero=zero,
    )
    middle_value = prefix[j] - prefix[i + 1]
    remainder_sum = int(pattern.geometric["remainder_sum"])
    laps = ((q1 - q0) - remainder_sum) // ring_length
    value = width * laps + suffix_value + middle_value + prefix_value
    return [(value, "fixed")]


def _pattern_witness_laps(
    pattern: _Pattern,
    adjacency: Sequence[Sequence[tuple[int, int, Any]]],
    node_count: int,
    endpoint: str,
) -> list[int]:
    temporary = [list(edges) for edges in adjacency]
    for node, upper in pattern.upper:
        temporary[0].append((node, upper, "query_upper"))
    for node, lower in pattern.lower:
        temporary[node].append((0, -lower, "query_lower"))

    if endpoint == "min":
        reverse: list[list[tuple[int, int, Any]]] = [[] for _ in range(node_count)]
        for source, edges in enumerate(temporary):
            for target, weight, label in edges:
                reverse[target].append((source, weight, label))
        to_zero = spfa_shortest(reverse, 0)
        return [-value for value in to_zero]
    return spfa_shortest(temporary, 0)


def _witness_from_laps(
    obs: CollectionObservation,
    laps: Sequence[int],
    residues: Sequence[int],
    base: Sequence[int],
    directions: Sequence[int],
    positions: Sequence[int],
    query_start: int,
    query_end: int,
    ring_length: int,
    bin_width: int,
    zero: int,
    pattern_info: dict[str, Any],
) -> dict[str, Any]:
    lengths = [
        int(residues[index] + ring_length * (laps[index + 1] - laps[index]))
        for index in range(len(residues))
    ]
    cumulative = [int(base[index] + ring_length * laps[index]) for index in range(len(base))]
    intervals: list[dict[str, Any]] = []
    effective_start = max(0, int(query_start))
    effective_end = int(query_end)
    interval_count = len(lengths)

    if interval_count and effective_end > effective_start:
        first = bisect_right(cumulative, effective_start) - 1
        last = min(interval_count - 1, bisect_left_direct(cumulative, effective_end) - 1)
        first = max(0, first)
        for index in range(first, last + 1):
            overlap_start = max(effective_start, cumulative[index])
            overlap_end = min(effective_end, cumulative[index + 1])
            if overlap_end <= overlap_start:
                continue
            local_start = overlap_start - cumulative[index]
            intervals.append(
                {
                    "interval": index,
                    "interval_index": index,
                    "from_reading": index,
                    "to_reading": index + 1,
                    "start_reading": index,
                    "end_reading": index + 1,
                    "cumulative_start": cumulative[index],
                    "cumulative_end": cumulative[index + 1],
                    "overlap_start": overlap_start,
                    "overlap_end": overlap_end,
                    "query_start": overlap_start,
                    "query_end": overlap_end,
                    "local_start": local_start,
                    "local_end": overlap_end - cumulative[index],
                    "meters": overlap_end - overlap_start,
                    "direction": directions[index],
                    "start_position": positions[index],
                    "end_position": positions[index + 1],
                }
            )

    bin_meters = [0] * bin_count(ring_length, bin_width)
    for item in intervals:
        index = int(item["interval"])
        local_start = int(item["local_start"])
        physical_start = (positions[index] + directions[index] * local_start) % ring_length
        vector = directed_bin_vector(
            physical_start,
            directions[index],
            int(item["meters"]),
            ring_length=ring_length,
            bin_width=bin_width,
            zero=zero,
        )
        for bin_index, value in enumerate(vector):
            bin_meters[bin_index] += value

    return {
        "lengths": lengths,
        "cumulative": cumulative,
        "intervals": intervals,
        "lap_counts": [int(laps[index + 1] - laps[index]) for index in range(len(lengths))],
        "bin_meters": bin_meters,
        "pattern": pattern_info,
    }


from bisect import bisect_left as bisect_left_direct


def audit_collection_records(
    observations: Iterable[CollectionObservation | Mapping[str, Any]] | Mapping[str, Any],
    counts: Any = None,
    ring_length: int | None = None,
    bin_width: int | None = None,
    zero: int = 0,
    query_start: int | None = None,
    query_end: int | None = None,
    query: Sequence[int] | None = None,
) -> AuditReport:
    if isinstance(observations, Mapping) and ("observations" in observations or "records" in observations):
        payload = observations
        observations = payload.get("observations", payload.get("records"))
        counts = counts if counts is not None else payload.get("counts", payload.get("counters"))
        ring_length = ring_length if ring_length is not None else payload.get("ring_length", payload.get("ring"))
        bin_width = bin_width if bin_width is not None else payload.get("bin_width", payload.get("width"))
        zero = payload.get("zero", zero) if zero == 0 else zero
        query_start = query_start if query_start is not None else payload.get("query_start")
        query_end = query_end if query_end is not None else payload.get("query_end")
        query = query if query is not None else payload.get("query")
    elif isinstance(observations, Mapping):
        observations = [observations]

    if query is None and query_start is not None and query_end is None and not isinstance(query_start, (int, str)):
        query = query_start  # type: ignore[assignment]
    if query is not None:
        if isinstance(query, slice):
            query_start, query_end = query.start, query.stop
        else:
            query_start, query_end = int(query[0]), int(query[1])
    if query_start is None or query_end is None:
        raise CollectionAuditError("query_start and query_end, or query=(start,end), are required")
    if bin_width is None:
        raise CollectionAuditError("bin_width is required")

    observation_objects = _coerce_observations(
        observations,
        counts,
        ring_length=ring_length,
        bin_width=bin_width,
        zero=zero,
        query_start=query_start,
        query_end=query_end,
    )

    rows: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    results: dict[Hashable, ObservationAudit] = {}
    min_evidence: dict[tuple[Hashable, int], ExtremeEvidence] = {}
    max_evidence: dict[tuple[Hashable, int], ExtremeEvidence] = {}

    for obs in observation_objects:
        try:
            result = audit_observation(obs)
        except IncompatibleCollection as exc:
            invalid_rows.append(
                {
                    "observation_id": obs.observation_id,
                    "reason": exc.reason,
                    "cycle": exc.cycle,
                    "compatible": False,
                }
            )
            continue
        results[obs.observation_id] = result
        for bin_index in range(len(result.bin_bounds)):
            rows.append(result.as_row(bin_index))
            min_evidence[(obs.observation_id, bin_index)] = result.min_evidence[bin_index]
            max_evidence[(obs.observation_id, bin_index)] = result.max_evidence[bin_index]

    bins_frame = pd.DataFrame(rows)
    if rows:
        bins_frame = bins_frame.sort_values(["observation_id", "bin_index"]).reset_index(drop=True)
    invalid_frame = pd.DataFrame(
        invalid_rows,
        columns=["observation_id", "reason", "cycle", "compatible"],
    )
    return AuditReport(bins_frame, results, invalid_frame, min_evidence, max_evidence)


def _coerce_observations(
    observations: Iterable[CollectionObservation | Mapping[str, Any]],
    counts: Any,
    *,
    ring_length: int | None,
    bin_width: int,
    zero: int,
    query_start: int,
    query_end: int,
) -> list[CollectionObservation]:
    if isinstance(observations, pd.DataFrame):
        if _frame_has_interval_positions(observations):
            return _interval_observations_from_dataframe(
                observations,
                counts if isinstance(counts, pd.DataFrame) else None,
                ring_length=ring_length,
                bin_width=bin_width,
                zero=zero,
                query_start=query_start,
                query_end=query_end,
            )
        return _observations_from_dataframe(
            observations,
            counts if isinstance(counts, pd.DataFrame) else None,
            ring_length=ring_length,
            bin_width=bin_width,
            zero=zero,
            query_start=query_start,
            query_end=query_end,
        )

    observation_items = list(observations)
    if observation_items and all(isinstance(item, Mapping) for item in observation_items):
        if any(_mapping_has_interval_positions(item) for item in observation_items) and not any(
            _mapping_has_reading_sequence(item) for item in observation_items
        ):
            count_groups = _group_counts(counts) if counts is not None else {}
            return _observations_from_interval_mappings(
                observation_items,
                count_groups,
                ring_length=ring_length,
                bin_width=bin_width,
                zero=zero,
                query_start=query_start,
                query_end=query_end,
            )

    result: list[CollectionObservation] = []
    count_groups = _group_counts(counts) if counts is not None else {}
    global_counts = list(counts) if _is_global_count_sequence(counts) else []
    for item in observation_items:
        if isinstance(item, CollectionObservation):
            item.counts = list(item.counts) + list(count_groups.get(item.observation_id, [])) + global_counts
            if item.bin_width is None:
                item.bin_width = bin_width
            if item.query_start is None:
                item.query_start = query_start
            if item.query_end is None:
                item.query_end = query_end
            if not item.zero:
                item.zero = zero
            result.append(item)
            continue
        if not isinstance(item, Mapping):
            raise CollectionAuditError(f"unsupported observation type: {type(item)!r}")
        obs_id = item.get("observation_id", item.get("id", item.get("obs_id")))
        item_ring = item.get("ring_length", item.get("ring", ring_length))
        if item_ring is None:
            raise CollectionAuditError("ring_length is required")
        obs_counts = list(item.get("counts", []))
        obs_counts.extend(count_groups.get(obs_id, []))
        obs_counts.extend(global_counts)
        known_distances = _optional_first_present(
            item,
            ["actual_distances", "distances", "lengths", "actual_meters", "meter_lengths", "known_distances"],
        )
        result.append(
            CollectionObservation(
                observation_id=obs_id,
                ring_length=int(item_ring),
                positions=_first_present(item, ["positions", "readings", "station_positions", "reading_positions"]),
                directions=item.get("directions", item.get("direction", 1)),
                min_distances=_optional_first_present(
                    item,
                    ["min_distances", "minimum_distances", "lower_distances", "min_meters", "lower"],
                )
                or known_distances,
                max_distances=_optional_first_present(
                    item,
                    ["max_distances", "maximum_distances", "upper_distances", "max_meters", "upper"],
                )
                or known_distances,
                counts=obs_counts,
                zero=int(item.get("zero", zero)),
                bin_width=int(item.get("bin_width", bin_width)),
                query_start=int(item.get("query_start", query_start)),
                query_end=int(item.get("query_end", query_end)),
                reading_ids=item.get("reading_ids"),
            )
        )
    return result


def _first_present(item: Mapping[str, Any], keys: Sequence[str]) -> Any:
    value = _optional_first_present(item, keys)
    if value is None:
        raise CollectionAuditError(f"missing one of required fields: {keys}")
    return value


def _optional_first_present(item: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in item and item[key] is not None:
            return item[key]
    return None


def _group_counts(counts: Any) -> dict[Any, list[Any]]:
    if isinstance(counts, pd.DataFrame):
        return _counts_from_dataframe(counts)
    if isinstance(counts, Mapping):
        grouped: dict[Any, list[Any]] = {}
        for key, value in counts.items():
            if isinstance(value, (list, tuple)):
                grouped[key] = list(value)
            else:
                grouped[key] = [value]
        return grouped
    grouped = {}
    for count in counts:
        if isinstance(count, Mapping):
            obs_id = count.get("observation_id", count.get("id", count.get("obs_id")))
            grouped.setdefault(obs_id, []).append(count)
    return grouped


def _is_global_count_sequence(counts: Any) -> bool:
    return isinstance(counts, (list, tuple)) and all(
        isinstance(count, (CountSpec, Sequence)) and not isinstance(count, Mapping) for count in counts
    )


_START_POSITION_KEYS = {
    "start_position",
    "start_pos",
    "from_position",
    "from_pos",
    "start_reading_position",
    "from_reading_position",
    "start_reading_value",
    "from_reading_value",
}
_END_POSITION_KEYS = {
    "end_position",
    "end_pos",
    "to_position",
    "to_pos",
    "end_reading_position",
    "to_reading_position",
    "end_reading_value",
    "to_reading_value",
}
_READING_SEQUENCE_KEYS = {"positions", "readings", "station_positions", "reading_positions"}


def _normalized_key(key: Any) -> str:
    return str(key).strip().lower().replace(" ", "_").replace("-", "_")


def _find_normalized_key(item: Mapping[str, Any], names: set[str]) -> Any:
    for key in item:
        if _normalized_key(key) in names:
            return key
    return None


def _mapping_has_interval_positions(item: Mapping[str, Any]) -> bool:
    return _find_normalized_key(item, _START_POSITION_KEYS) is not None and _find_normalized_key(
        item, _END_POSITION_KEYS
    ) is not None


def _mapping_has_reading_sequence(item: Mapping[str, Any]) -> bool:
    return _find_normalized_key(item, _READING_SEQUENCE_KEYS) is not None


def _observations_from_interval_mappings(
    rows: Sequence[Mapping[str, Any]],
    count_groups: Mapping[Any, list[Any]],
    *,
    ring_length: int | None,
    bin_width: int,
    zero: int,
    query_start: int,
    query_end: int,
) -> list[CollectionObservation]:
    groups: dict[Any, list[Mapping[str, Any]]] = {}
    for row in rows:
        obs_id = row.get("observation_id", row.get("id", row.get("obs_id", "observation")))
        groups.setdefault(obs_id, []).append(row)

    result: list[CollectionObservation] = []
    for obs_id, group in groups.items():
        order_key = _find_normalized_key(group[0], {"interval_index", "order", "sequence", "seq"})
        if order_key is not None:
            group = sorted(group, key=lambda row: row[order_key])
        start_key = _find_normalized_key(group[0], _START_POSITION_KEYS)
        end_key = _find_normalized_key(group[0], _END_POSITION_KEYS)
        positions = [int(group[0][start_key])] + [int(row[end_key]) for row in group]  # type: ignore[index]
        direction_key = _find_normalized_key(group[0], {"direction", "dir"})
        directions = [row[direction_key] if direction_key is not None else 1 for row in group]
        min_key = _find_normalized_key(
            group[0], {"min_meters", "min_distance", "minimum", "lower", "lower_meters"}
        )
        max_key = _find_normalized_key(
            group[0], {"max_meters", "max_distance", "maximum", "upper", "upper_meters"}
        )
        known_key = _find_normalized_key(
            group[0], {"exact_meters", "actual_meters", "distance", "meters", "actual_distance"}
        )
        known_distances = [int(row[known_key]) for row in group] if known_key is not None else None
        ring_key = _find_normalized_key(group[0], {"ring_length", "ring"})
        result.append(
            CollectionObservation(
                observation_id=obs_id,
                ring_length=int(group[0][ring_key] if ring_key is not None else ring_length),  # type: ignore[arg-type]
                positions=positions,
                directions=directions,
                min_distances=[int(row[min_key]) for row in group] if min_key is not None else known_distances,  # type: ignore[index]
                max_distances=[int(row[max_key]) for row in group] if max_key is not None else known_distances,  # type: ignore[index]
                counts=list(count_groups.get(obs_id, [])),
                zero=zero,
                bin_width=bin_width,
                query_start=query_start,
                query_end=query_end,
            )
        )
    return result


def _frame_has_interval_positions(frame: pd.DataFrame) -> bool:
    columns = {_normalized_key(column) for column in frame.columns}
    return bool(columns & _START_POSITION_KEYS) and bool(columns & _END_POSITION_KEYS)


def _interval_observations_from_dataframe(
    frame: pd.DataFrame,
    counts_frame: pd.DataFrame | None,
    *,
    ring_length: int | None,
    bin_width: int,
    zero: int,
    query_start: int,
    query_end: int,
) -> list[CollectionObservation]:
    renamed = frame.copy()
    renamed.columns = [_normalized_key(column) for column in renamed.columns]
    id_column = next((name for name in ["observation_id", "obs_id", "id"] if name in renamed.columns), None)
    group_rows = []
    if id_column is None:
        records = renamed.to_dict(orient="records")
        return _observations_from_interval_mappings(
            records,
            _counts_from_dataframe(counts_frame),
            ring_length=ring_length,
            bin_width=bin_width,
            zero=zero,
            query_start=query_start,
            query_end=query_end,
        )
    for obs_id, group in renamed.groupby(id_column, sort=False):
        records = group.to_dict(orient="records")
        for record in records:
            record.setdefault("observation_id", obs_id)
        group_rows.extend(records)
    return _observations_from_interval_mappings(
        group_rows,
        _counts_from_dataframe(counts_frame),
        ring_length=ring_length,
        bin_width=bin_width,
        zero=zero,
        query_start=query_start,
        query_end=query_end,
    )


def _observations_from_dataframe(
    frame: pd.DataFrame,
    counts_frame: pd.DataFrame | None,
    *,
    ring_length: int | None,
    bin_width: int,
    zero: int,
    query_start: int,
    query_end: int,
) -> list[CollectionObservation]:
    columns = {str(column).lower(): column for column in frame.columns}
    id_col = _column(columns, ["observation_id", "obs_id", "id"])
    order_col = _column(columns, ["order", "sequence", "seq", "reading_index", "passage_order", "timestamp"])
    position_col = _column(columns, ["position", "positions", "reading", "value", "station_position"])
    direction_col = _column(columns, ["direction", "dir"])
    min_col = _column(columns, ["min_distance", "min_distances", "min_meters", "lower_distance", "lower"])
    max_col = _column(columns, ["max_distance", "max_distances", "max_meters", "upper_distance", "upper"])
    known_col = _column(columns, ["actual_distance", "distance", "meters", "length", "actual_meters"])
    ring_col = _column(columns, ["ring_length", "ring"])

    count_groups = _counts_from_dataframe(counts_frame) if counts_frame is not None else {}
    result: list[CollectionObservation] = []
    groups = [(None, frame)] if id_col is None else frame.groupby(id_col, sort=False)
    for obs_id, group in groups:
        if order_col is not None:
            group = group.sort_values(order_col, kind="stable")
        positions = group[position_col].tolist() if position_col is not None else []
        directions: Any = 1
        if direction_col is not None:
            values = group[direction_col].tolist()
            directions = values[0] if len(set(map(str, values))) == 1 else values[1:] if len(values) == len(positions) else values
        known_values = group[known_col].tolist() if known_col is not None else None
        min_distances = group[min_col].tolist() if min_col is not None else known_values
        max_distances = group[max_col].tolist() if max_col is not None else known_values
        item_ring = ring_length
        if ring_col is not None:
            item_ring = int(group[ring_col].iloc[0])
        result.append(
            CollectionObservation(
                observation_id=obs_id,
                ring_length=int(item_ring),  # type: ignore[arg-type]
                positions=positions,
                directions=directions,
                min_distances=min_distances,
                max_distances=max_distances,
                counts=count_groups.get(obs_id, []),
                zero=zero,
                bin_width=bin_width,
                query_start=query_start,
                query_end=query_end,
            )
        )
    return result


def _column(columns: Mapping[str, Any], names: Sequence[str]) -> Any:
    for name in names:
        if name in columns:
            return columns[name]
    return None


def _counts_from_dataframe(frame: pd.DataFrame | None) -> dict[Any, list[dict[str, Any]]]:
    if frame is None:
        return {}
    columns = {str(column).lower(): column for column in frame.columns}
    id_col = _column(columns, ["observation_id", "obs_id", "id"])
    start_col = _column(columns, ["start", "start_reading", "from_reading", "from", "a"])
    end_col = _column(columns, ["end", "end_reading", "to_reading", "to", "b"])
    value_col = _column(columns, ["distance", "meters", "total", "exact", "value"])
    lower_col = _column(columns, ["lower", "min_meters", "min_distance", "minimum"])
    upper_col = _column(columns, ["upper", "max_meters", "max_distance", "maximum"])
    grouped: dict[Any, list[dict[str, Any]]] = {}
    for _, row in frame.iterrows():
        obs_id = row[id_col] if id_col is not None else None
        item: dict[str, Any] = {
            "start_reading": row[start_col],
            "end_reading": row[end_col],
            "count_id": row.get("count_id", row.get("id")),
        }
        if value_col is not None and not pd.isna(row[value_col]):
            item["exact"] = row[value_col]
        if lower_col is not None and not pd.isna(row[lower_col]):
            item["lower"] = row[lower_col]
        if upper_col is not None and not pd.isna(row[upper_col]):
            item["upper"] = row[upper_col]
        grouped.setdefault(obs_id, []).append(item)
    return grouped


def audit_ring_bins(*args: Any, **kwargs: Any) -> AuditReport:
    return audit_collection_records(*args, **kwargs)


def audit_collections(*args: Any, **kwargs: Any) -> AuditReport:
    return audit_collection_records(*args, **kwargs)


def audit_collection(*args: Any, **kwargs: Any) -> AuditReport:
    return audit_collection_records(*args, **kwargs)


def audit_bins(*args: Any, **kwargs: Any) -> AuditReport:
    return audit_collection_records(*args, **kwargs)


def bin_collection_records(*args: Any, **kwargs: Any) -> AuditReport:
    return audit_collection_records(*args, **kwargs)


def verify_collection_records(*args: Any, **kwargs: Any) -> AuditReport:
    return audit_collection_records(*args, **kwargs)


def check_collection_records(*args: Any, **kwargs: Any) -> AuditReport:
    return audit_collection_records(*args, **kwargs)


def reconcile_collection_records(*args: Any, **kwargs: Any) -> AuditReport:
    return audit_collection_records(*args, **kwargs)


def reconcile_collection_bins(*args: Any, **kwargs: Any) -> AuditReport:
    return audit_collection_records(*args, **kwargs)


audit = audit_collection_records
audit_observations = audit_collection_records
audit_ring_collection = audit_collection_records
ring_collection_audit = audit_collection_records
reconcile_collection = audit_collection_records
reconcile_bins = audit_collection_records
reconcile_ring_bins = audit_collection_records
bin_observations = audit_collection_records
collection_bins = audit_collection_records
ring_bins = audit_ring_bins
bin_ring_records = audit_collection_records
bin_audit = audit_collection_records


class CollectionAuditor:
    def __init__(
        self,
        *,
        ring_length: int | None = None,
        bin_width: int | None = None,
        zero: int = 0,
        query_start: int | None = None,
        query_end: int | None = None,
        query: Sequence[int] | None = None,
    ) -> None:
        self.ring_length = ring_length
        self.bin_width = bin_width
        self.zero = zero
        if query is not None:
            query_start, query_end = query[0], query[1]
        self.query_start = query_start
        self.query_end = query_end
        self.observations: list[CollectionObservation] = []
        self.counts: dict[Hashable, list[CountSpec]] = {}

    def add_observation(
        self,
        observation_id: Hashable,
        readings: Sequence[int] | None = None,
        positions: Sequence[int] | None = None,
        directions: Any = 1,
        min_distances: Sequence[int] | None = None,
        max_distances: Sequence[int] | None = None,
        ring_length: int | None = None,
        zero: int | None = None,
        **kwargs: Any,
    ) -> "CollectionAuditor":
        resolved_positions = positions if positions is not None else readings
        if resolved_positions is None:
            raise CollectionAuditError("readings or positions are required")
        obs = CollectionObservation(
            observation_id=observation_id,
            ring_length=int(ring_length if ring_length is not None else self.ring_length),  # type: ignore[arg-type]
            positions=resolved_positions,
            directions=directions,
            min_distances=min_distances,
            max_distances=max_distances,
            counts=list(self.counts.get(observation_id, [])) + list(kwargs.pop("counts", [])),
            zero=int(zero if zero is not None else self.zero),
            bin_width=kwargs.pop("bin_width", self.bin_width),
            query_start=kwargs.pop("query_start", self.query_start),
            query_end=kwargs.pop("query_end", self.query_end),
            **kwargs,
        )
        self.observations.append(obs)
        return self

    def add_reading(self, *args: Any, **kwargs: Any) -> "CollectionAuditor":
        return self.add_observation(*args, **kwargs)

    def add_interval(
        self,
        observation_id: Hashable,
        start_position: int,
        end_position: int,
        direction: Any = 1,
        min_meters: int | None = None,
        max_meters: int | None = None,
        *,
        exact_meters: int | None = None,
    ) -> "CollectionAuditor":
        if exact_meters is not None:
            min_meters = max_meters = exact_meters
        if min_meters is None or max_meters is None:
            raise CollectionAuditError("min_meters and max_meters, or exact_meters, are required")
        existing = next((obs for obs in self.observations if obs.observation_id == observation_id), None)
        if existing is None:
            self.add_observation(
                observation_id,
                positions=[start_position, end_position],
                directions=[direction],
                min_distances=[min_meters],
                max_distances=[max_meters],
            )
            return self
        if existing.positions[-1] != start_position:
            raise CollectionAuditError("interval start does not match the previous reading")
        existing.positions = list(existing.positions) + [end_position]  # type: ignore[arg-type]
        existing.directions = list(_as_direction_list(existing.directions, len(existing.positions) - 2)) + [direction]
        existing.min_distances = list(existing.min_distances or []) + [min_meters]
        existing.max_distances = list(existing.max_distances or []) + [max_meters]
        return self

    def add_count(
        self,
        observation_id: Hashable,
        start: int,
        end: int,
        exact: int | None = None,
        *,
        lower: int | None = None,
        upper: int | None = None,
        count_id: Hashable | None = None,
    ) -> "CollectionAuditor":
        if exact is not None:
            lower = upper = exact
        self.counts.setdefault(observation_id, []).append(
            CountSpec(start, end, lower, upper, count_id)
        )
        for obs in self.observations:
            if obs.observation_id == observation_id:
                obs.counts = list(obs.counts) + [self.counts[observation_id][-1]]
        return self

    def audit(
        self,
        query_start: int | None = None,
        query_end: int | None = None,
        query: Sequence[int] | None = None,
    ) -> AuditReport:
        q0 = query_start if query_start is not None else self.query_start
        q1 = query_end if query_end is not None else self.query_end
        if query is not None:
            q0, q1 = query[0], query[1]
        return audit_collection_records(
            self.observations,
            ring_length=self.ring_length,
            bin_width=self.bin_width,
            zero=self.zero,
            query_start=q0,
            query_end=q1,
        )


RingBinningAuditor = CollectionAuditor

