from __future__ import annotations

import copy
import hashlib
import itertools
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

_MATCHING_LIMIT = 4096
_PATH_LIMIT = 4096


@dataclass
class Issue:
    code: str
    message: str
    severity: str = "error"
    station_id: str | None = None
    observation_id: str | None = None
    source_ref: str | None = None
    line: int | str | None = None
    expected: Any = None
    actual: Any = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Observation:
    id: str
    kind: str
    station_id: str | None = None
    source_ref: str | None = None
    line: int | str | None = None
    input_id: str | None = None
    output_id: str | None = None
    source: str | None = None
    warehouse_row: str | None = None
    resend_group: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    contradictory: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ReconciliationReport:
    batch_id: str
    observations: list[Observation]
    issues: list[Issue]
    stations: list[dict[str, Any]]
    warehouse_results: list[dict[str, Any]]
    global_solution_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "status": self.status,
            "summary": self.summary,
            "observations": [item.to_dict() for item in self.observations],
            "issues": [item.to_dict() for item in self.issues],
            "stations": self.stations,
            "warehouse_results": self.warehouse_results,
            "global_solution_count": self.global_solution_count,
        }

    @property
    def status(self) -> str:
        if any(issue.severity == "error" for issue in self.issues):
            return "completed_with_contradictions"
        return "ok"

    @property
    def summary(self) -> dict[str, int]:
        unique = sum(
            1 for row in self.warehouse_results if row["unique_source"] is True
        )
        ambiguous = sum(1 for row in self.warehouse_results if row["ambiguous"])
        unresolved = len(self.warehouse_results) - unique - ambiguous
        return {
            "observations": len(self.observations),
            "issues": len(self.issues),
            "unique_source_rows": unique,
            "ambiguous_rows": ambiguous,
            "unresolved_rows": unresolved,
        }


def reconcile(
    batch: dict[str, Any],
    corrections: dict[str, Any] | list[dict[str, Any]] | None = None,
) -> ReconciliationReport:
    """Reconcile one offline batch and retain ambiguity instead of guessing."""

    data = copy.deepcopy(batch)
    if corrections:
        existing = data.setdefault("corrections", [])
        if isinstance(corrections, dict):
            existing.append(corrections)
        else:
            existing.extend(corrections)

    observations = _build_observations(data)
    issues: list[Issue] = []
    stations = _build_station_reports(data, observations, issues)

    active_observations = [item for item in observations if not item.contradictory]
    matchings_by_station = [station["matchings"] for station in stations]
    global_solution_count = _product_count(matchings_by_station)

    graph, edge_evidence = _build_graph(
        data, stations, matchings_by_station, active_observations
    )
    anchors = _build_anchors(data, observations)
    warehouse_rows = _normalize_warehouse_rows(data, observations)
    warehouse_results = _trace_warehouse_rows(
        warehouse_rows=warehouse_rows,
        graph=graph,
        edge_evidence=edge_evidence,
        stations=stations,
        observations=active_observations,
        anchors=anchors,
        issues=issues,
    )

    return ReconciliationReport(
        batch_id=str(data.get("batch_id") or data.get("id") or "reconciliation-batch"),
        observations=observations,
        issues=issues,
        stations=stations,
        warehouse_results=warehouse_results,
        global_solution_count=global_solution_count,
    )


def _first_key(raw: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in raw and raw[key] is not None:
            return raw[key]
    return default


def _as_id(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        found = _first_key(value, "id", "ID", "event_id", "name")
        return None if found is None else str(found)
    return str(value)


def _ids(values: Iterable[Any] | None) -> list[str]:
    if not values:
        return []
    return [item for item in (_as_id(value) for value in values) if item]


def _build_observations(data: dict[str, Any]) -> list[Observation]:
    raw_records: list[dict[str, Any]] = []
    for key in ("observations", "logs", "history", "records", "corrections"):
        value = data.get(key)
        if isinstance(value, list):
            raw_records.extend(item for item in value if isinstance(item, dict))

    observations: list[Observation] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(raw_records, start=1):
        observation_id = str(
            _first_key(raw, "id", "observation_id", default=f"OBS-{index:03d}")
        )
        if observation_id in seen_ids:
            observation_id = f"{observation_id}-{index}"
        seen_ids.add(observation_id)

        kind = str(
            _first_key(raw, "kind", "type", "record_type", default="observation")
        )
        observations.append(
            Observation(
                id=observation_id,
                kind=kind,
                station_id=_as_id(
                    _first_key(raw, "station_id", "station", "workstation")
                ),
                source_ref=_as_id(
                    _first_key(raw, "source_ref", "source", "log", "log_file")
                ),
                line=_first_key(raw, "line", "line_number", "row"),
                input_id=_as_id(
                    _first_key(raw, "input_id", "input", "inbound_id", "inbound")
                ),
                output_id=_as_id(
                    _first_key(raw, "output_id", "output", "outbound_id", "outbound")
                ),
                source=_as_id(_first_key(raw, "source_event_id", "original_id")),
                warehouse_row=_as_id(
                    _first_key(raw, "warehouse_row", "warehouse_id", "warehouse")
                ),
                resend_group=_as_id(
                    _first_key(raw, "resend_group", "resend_group_id", "group_id")
                ),
                payload=raw,
            )
        )

    for station in data.get("stations", []):
        if not isinstance(station, dict):
            continue
        station_id = _as_id(_first_key(station, "id", "station_id", "name"))
        for binding_index, binding in enumerate(station.get("bindings", []), start=1):
            if not isinstance(binding, dict):
                continue
            existing_id = _as_id(
                _first_key(binding, "observation_id", "id", "evidence_id")
            )
            if existing_id and any(item.id == existing_id for item in observations):
                continue
            observation_id = existing_id or (
                f"{station_id or 'STATION'}-BINDING-{binding_index:03d}"
            )
            observations.append(
                Observation(
                    id=observation_id,
                    kind="binding",
                    station_id=station_id,
                    source_ref=_as_id(
                        _first_key(binding, "source_ref", "source", "log")
                    ),
                    line=_first_key(binding, "line", "line_number", "row"),
                    input_id=_as_id(
                        _first_key(binding, "input_id", "input", "inbound_id")
                    ),
                    output_id=_as_id(
                        _first_key(binding, "output_id", "output", "outbound_id")
                    ),
                    resend_group=_as_id(
                        _first_key(binding, "resend_group", "group_id")
                    ),
                    payload=binding,
                )
            )

    return observations


def _station_count(station: dict[str, Any], name: str) -> int | None:
    counts = station.get("counts")
    if isinstance(counts, dict) and name in counts:
        value = counts[name]
        return int(value) if value is not None else None
    aliases = {
        "input": ("input_count", "inbound_count", "inputs_count"),
        "output": ("output_count", "outbound_count", "outputs_count"),
    }[name]
    value = _first_key(station, *aliases)
    return int(value) if value is not None else None


def _build_station_reports(
    data: dict[str, Any],
    observations: list[Observation],
    issues: list[Issue],
) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    raw_stations = data.get("stations", [])
    for order, station in enumerate(raw_stations, start=1):
        if not isinstance(station, dict):
            continue
        station_id = str(_first_key(station, "id", "station_id", "name"))
        station_observations = [
            item for item in observations if item.station_id == station_id
        ]
        expected_inputs = _station_count(station, "input")
        expected_outputs = _station_count(station, "output")

        declared_inputs = _ids(station.get("inputs"))
        declared_outputs = _ids(station.get("outputs"))
        output_records = {
            _as_id(item): item
            for item in station.get("outputs", [])
            if isinstance(item, dict) and _as_id(item)
        }
        logical_output = _logical_output_factory(station, output_records)
        candidate_inputs = _ids(
            _first_key(station, "candidate_inputs", "input_candidates")
        )

        observed_inputs = set(declared_inputs)
        observed_outputs: set[str] = set()
        for observation in station_observations:
            if observation.input_id:
                observed_inputs.add(observation.input_id)
            if observation.output_id:
                observed_outputs.add(logical_output(observation.output_id, observation))

        _collect_count_issues(
            station_id=station_id,
            expected_inputs=expected_inputs,
            expected_outputs=expected_outputs,
            observed_inputs=observed_inputs,
            observed_outputs=observed_outputs,
            known_inputs=set(declared_inputs) | set(candidate_inputs),
            known_outputs={logical_output(output_id) for output_id in declared_outputs},
            observations=station_observations,
            issues=issues,
        )

        active_observations = [
            item for item in station_observations if not item.contradictory
        ]
        input_alternatives = _input_alternatives(
            declared_inputs=declared_inputs,
            candidate_inputs=candidate_inputs,
            expected_inputs=expected_inputs,
            observations=active_observations,
        )
        logical_output_ids = list(
            dict.fromkeys(
                [logical_output(output_id) for output_id in declared_outputs]
                + [
                    logical_output(item.output_id, item)
                    for item in active_observations
                    if item.output_id
                ]
            )
        )

        forced_edges = _forced_edges(active_observations, logical_output)
        allowed_edges = _allowed_edges(station, logical_output)
        matchings: list[dict[str, Any]] = []
        seen_matching_keys: set[tuple[tuple[str, str], ...]] = set()
        for input_ids in input_alternatives:
            for pairs in _enumerate_perfect_matchings(
                input_ids=input_ids,
                output_ids=logical_output_ids,
                forced_edges=forced_edges,
                allowed_edges=allowed_edges,
            ):
                key = tuple(sorted(pairs))
                if key in seen_matching_keys:
                    continue
                seen_matching_keys.add(key)
                matchings.append(
                    {
                        "id": _matching_id(station_id, pairs),
                        "index": len(matchings) + 1,
                        "pairs": [
                            {"input": source, "output": target}
                            for source, target in pairs
                        ],
                        "forced": [
                            {"input": source, "output": target}
                            for source, target in sorted(forced_edges)
                        ],
                    }
                )
                if len(matchings) >= _MATCHING_LIMIT:
                    break
            if len(matchings) >= _MATCHING_LIMIT:
                break

        reports.append(
            {
                "id": station_id,
                "order": int(station.get("order", order)),
                "input_count": expected_inputs,
                "output_count": expected_outputs,
                "inputs": declared_inputs or candidate_inputs,
                "logical_outputs": logical_output_ids,
                "candidate_inputs": candidate_inputs,
                "resend_groups": station.get("resend_groups", []),
                "matching_count": len(matchings),
                "matchings": matchings,
                "constraints": _station_constraints(
                    station_id,
                    expected_inputs,
                    expected_outputs,
                    candidate_inputs,
                    station.get("resend_groups", []),
                ),
            }
        )

    reports.sort(key=lambda item: item["order"])
    return reports


def _logical_output_factory(
    station: dict[str, Any], output_records: dict[str, dict[str, Any]]
):
    group_canonical: dict[str, str] = {}
    raw_to_canonical: dict[str, str] = {}
    for group in station.get("resend_groups", []):
        if not isinstance(group, dict):
            continue
        group_id = _as_id(_first_key(group, "id", "resend_group", "group_id"))
        output_ids = _ids(group.get("outputs") or group.get("output_ids"))
        if not group_id or not output_ids:
            continue
        canonical = _as_id(group.get("canonical_output")) or output_ids[0]
        group_canonical[group_id] = canonical
        for output_id in output_ids:
            raw_to_canonical[output_id] = canonical

    def logical_output(output_id: str | None, observation: Observation | None = None):
        if not output_id:
            return output_id
        if output_id in raw_to_canonical:
            return raw_to_canonical[output_id]
        record = output_records.get(output_id)
        if record:
            resend_of = _as_id(record.get("resend_of"))
            if resend_of:
                return resend_of
            group_id = _as_id(record.get("resend_group"))
            if group_id and group_id in group_canonical:
                return group_canonical[group_id]
        if observation and observation.payload.get("resend_of"):
            return str(observation.payload["resend_of"])
        if observation and observation.resend_group in group_canonical:
            return group_canonical[observation.resend_group]
        return output_id

    return logical_output


def _collect_count_issues(
    station_id: str,
    expected_inputs: int | None,
    expected_outputs: int | None,
    observed_inputs: set[str],
    observed_outputs: set[str],
    known_inputs: set[str],
    known_outputs: set[str],
    observations: list[Observation],
    issues: list[Issue],
) -> None:
    for observation in observations:
        claims = observation.payload.get("claims")
        claims = claims if isinstance(claims, dict) else {}
        claimed_input = _first_key(claims, "input_count", "inbound_count")
        claimed_output = _first_key(claims, "output_count", "outbound_count")
        if claimed_input is None:
            claimed_input = _first_key(
                observation.payload, "input_count", "corrected_input_count"
            )
        if claimed_output is None:
            claimed_output = _first_key(
                observation.payload, "output_count", "corrected_output_count"
            )

        if expected_inputs is not None and claimed_input is not None:
            claimed_input = int(claimed_input)
            if claimed_input != expected_inputs:
                observation.contradictory = True
                issues.append(
                    Issue(
                        code="COUNT_CONTRADICTION",
                        message=(
                            f"{station_id} count constraint expects "
                            f"{expected_inputs} inbound events, but observation "
                            f"{observation.id} claims {claimed_input}."
                        ),
                        station_id=station_id,
                        observation_id=observation.id,
                        source_ref=observation.source_ref,
                        line=observation.line,
                        expected=expected_inputs,
                        actual=claimed_input,
                    )
                )
                continue

        if expected_outputs is not None and claimed_output is not None:
            claimed_output = int(claimed_output)
            if claimed_output != expected_outputs:
                observation.contradictory = True
                issues.append(
                    Issue(
                        code="COUNT_CONTRADICTION",
                        message=(
                            f"{station_id} count constraint expects "
                            f"{expected_outputs} outbound events, but observation "
                            f"{observation.id} claims {claimed_output}."
                        ),
                        station_id=station_id,
                        observation_id=observation.id,
                        source_ref=observation.source_ref,
                        line=observation.line,
                        expected=expected_outputs,
                        actual=claimed_output,
                    )
                )
                continue

        if (
            expected_inputs is not None
            and len(observed_inputs) > expected_inputs
            and observation.input_id
            and observation.input_id not in known_inputs
            and observation.kind == "correction"
        ):
            observation.contradictory = True
            issues.append(
                Issue(
                    code="COUNT_CONTRADICTION",
                    message=(
                        f"{station_id} has a complete inbound count of "
                        f"{expected_inputs}, but correction {observation.id} "
                        f"introduces input {observation.input_id}; "
                        f"{len(observed_inputs)} distinct inputs are evidenced."
                    ),
                    station_id=station_id,
                    observation_id=observation.id,
                    source_ref=observation.source_ref,
                    line=observation.line,
                    expected=expected_inputs,
                    actual=len(observed_inputs),
                )
            )

        if (
            expected_outputs is not None
            and len(observed_outputs) > expected_outputs
            and observation.output_id
            and observation.output_id not in known_outputs
            and observation.kind == "correction"
        ):
                observation.contradictory = True
                issues.append(
                    Issue(
                        code="COUNT_CONTRADICTION",
                        message=(
                            f"{station_id} has a complete outbound count of "
                            f"{expected_outputs} logical events, but observation "
                            f"{observation.id} introduces output "
                            f"{observation.output_id}; {len(observed_outputs)} "
                            "distinct logical outputs are evidenced."
                        ),
                        station_id=station_id,
                        observation_id=observation.id,
                        source_ref=observation.source_ref,
                        line=observation.line,
                        expected=expected_outputs,
                        actual=len(observed_outputs),
                    )
                )


def _input_alternatives(
    declared_inputs: list[str],
    candidate_inputs: list[str],
    expected_inputs: int | None,
    observations: list[Observation],
) -> list[list[str]]:
    forced_inputs = [item.input_id for item in observations if item.input_id]
    base = list(dict.fromkeys(declared_inputs + forced_inputs))
    if base:
        return [base]

    pool = list(dict.fromkeys(candidate_inputs + forced_inputs))
    if not pool:
        return [[]]
    count = expected_inputs if expected_inputs is not None else len(pool)
    count = max(count, len(forced_inputs))
    if count > len(pool):
        return []
    return [list(combo) for combo in itertools.combinations(pool, count)]


def _forced_edges(
    observations: list[Observation], logical_output
) -> frozenset[tuple[str, str]]:
    edges: set[tuple[str, str]] = set()
    for observation in observations:
        if observation.input_id and observation.output_id:
            edges.add(
                (
                    observation.input_id,
                    logical_output(observation.output_id, observation),
                )
            )
    return frozenset(edges)


def _allowed_edges(station: dict[str, Any], logical_output) -> set[tuple[str, str]]:
    raw_edges = _first_key(station, "allowed_bindings", "candidate_bindings")
    if not isinstance(raw_edges, list):
        return set()
    edges: set[tuple[str, str]] = set()
    for raw in raw_edges:
        if isinstance(raw, dict):
            source = _as_id(_first_key(raw, "input", "input_id", "source"))
            target = _as_id(_first_key(raw, "output", "output_id", "target"))
        elif isinstance(raw, (list, tuple)) and len(raw) == 2:
            source, target = _as_id(raw[0]), _as_id(raw[1])
        else:
            continue
        if source and target:
            edges.add((source, logical_output(target)))
    return edges


def _enumerate_perfect_matchings(
    input_ids: list[str],
    output_ids: list[str],
    forced_edges: frozenset[tuple[str, str]],
    allowed_edges: set[tuple[str, str]],
) -> list[list[tuple[str, str]]]:
    if len(input_ids) != len(output_ids):
        return []

    forced_by_input: dict[str, str] = {}
    forced_by_output: dict[str, str] = {}
    for source, target in forced_edges:
        if source in forced_by_input and forced_by_input[source] != target:
            return []
        if target in forced_by_output and forced_by_output[target] != source:
            return []
        forced_by_input[source] = target
        forced_by_output[target] = source

    remaining_inputs = [
        source for source in input_ids if source not in forced_by_input
    ]
    remaining_outputs = [
        target for target in output_ids if target not in forced_by_output
    ]
    if len(remaining_inputs) != len(remaining_outputs):
        return []

    adjacency: dict[str, list[str]] = {}
    for source in remaining_inputs:
        candidates = []
        for target in remaining_outputs:
            if allowed_edges and (source, target) not in allowed_edges:
                continue
            candidates.append(target)
        adjacency[source] = candidates

    results: list[list[tuple[str, str]]] = []

    def visit(pairs: list[tuple[str, str]], used_outputs: set[str]) -> None:
        if len(results) >= _MATCHING_LIMIT:
            return
        if len(pairs) == len(input_ids) - len(forced_edges):
            complete = list(forced_edges) + pairs
            results.append(complete)
            return

        choices = [
            source
            for source in remaining_inputs
            if not any(pair[0] == source for pair in pairs)
        ]
        if not choices:
            return
        source = min(
            choices,
            key=lambda item: (
                len([target for target in adjacency[item] if target not in used_outputs]),
                input_ids.index(item),
            ),
        )
        for target in adjacency[source]:
            if target in used_outputs:
                continue
            pairs.append((source, target))
            used_outputs.add(target)
            visit(pairs, used_outputs)
            used_outputs.remove(target)
            pairs.pop()

    visit([], set(forced_by_output))
    return results


def _matching_id(station_id: str, pairs: list[tuple[str, str]]) -> str:
    digest = hashlib.sha1(repr(sorted(pairs)).encode("utf-8")).hexdigest()[:8]
    return f"{station_id}-{digest}"


def _station_constraints(
    station_id: str,
    input_count: int | None,
    output_count: int | None,
    candidate_inputs: list[str],
    resend_groups: Any,
) -> list[dict[str, Any]]:
    constraints = [
        {
            "scope": f"station:{station_id}",
            "type": "complete_input_count",
            "expected": input_count,
        },
        {
            "scope": f"station:{station_id}",
            "type": "complete_output_count",
            "expected": output_count,
        },
        {"scope": f"station:{station_id}", "type": "one_to_one_non_resend"},
    ]
    if candidate_inputs:
        constraints.append(
            {
                "scope": f"station:{station_id}",
                "type": "candidate_input_range",
                "candidate_inputs": candidate_inputs,
            }
        )
    if resend_groups:
        constraints.append(
            {
                "scope": f"station:{station_id}",
                "type": "resend_group_equivalence",
                "groups": resend_groups,
            }
        )
    return constraints


def _product_count(matchings_by_station: list[list[dict[str, Any]]]) -> int:
    count = 1
    for matchings in matchings_by_station:
        count *= max(len(matchings), 1)
    return count


def _build_graph(
    data: dict[str, Any],
    stations: list[dict[str, Any]],
    matchings_by_station: list[list[dict[str, Any]]],
    observations: list[Observation],
) -> tuple[dict[str, list[dict[str, Any]]], dict[tuple[str, str], dict[str, Any]]]:
    graph: dict[str, list[dict[str, Any]]] = {}
    edge_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    def add_edge(source: str, target: str, **metadata: Any) -> None:
        key = (source, target)
        if key in edge_by_key:
            edge = edge_by_key[key]
            matching_id = metadata.get("matching_id")
            if matching_id and matching_id not in edge.setdefault(
                "matching_ids", []
            ):
                edge["matching_ids"].append(matching_id)
            observation_ids = metadata.get("observation_ids") or []
            for observation_id in observation_ids:
                if observation_id not in edge.setdefault("observation_ids", []):
                    edge["observation_ids"].append(observation_id)
            return

        edge = {"from": source, "to": target, **metadata}
        if metadata.get("matching_id"):
            edge["matching_ids"] = [metadata["matching_id"]]
        graph.setdefault(source, []).append(edge)
        edge_by_key[key] = edge

    observation_by_id = {item.id: item for item in observations}
    for station, matchings in zip(stations, matchings_by_station):
        for matching in matchings:
            for pair in matching["pairs"]:
                add_edge(
                    pair["input"],
                    pair["output"],
                    edge_type="station_binding",
                    station_id=station["id"],
                    matching_id=matching["id"],
                    inferred=True,
                )

    for observation in observations:
        if observation.input_id and observation.output_id:
            add_edge(
                observation.input_id,
                observation.output_id,
                edge_type="observed_binding",
                station_id=observation.station_id,
                observation_ids=[observation.id],
                source_ref=observation.source_ref,
                line=observation.line,
                inferred=False,
            )
        if observation.source and observation.input_id:
            add_edge(
                observation.source,
                observation.input_id,
                edge_type="source_link",
                observation_ids=[observation.id],
                source_ref=observation.source_ref,
                line=observation.line,
                inferred=False,
            )
        if observation.output_id and observation.warehouse_row:
            add_edge(
                observation.output_id,
                observation.warehouse_row,
                edge_type="warehouse_binding",
                observation_ids=[observation.id],
                source_ref=observation.source_ref,
                line=observation.line,
                inferred=False,
            )

    for raw in data.get("handoffs", []) + data.get("links", []):
        if not isinstance(raw, dict):
            continue
        source = _as_id(_first_key(raw, "from", "source", "from_output"))
        target = _as_id(_first_key(raw, "to", "target", "to_input"))
        if source and target:
            observation_id = _as_id(
                _first_key(raw, "observation_id", "evidence_id", "id")
            )
            add_edge(
                source,
                target,
                edge_type="handoff",
                observation_ids=[observation_id] if observation_id else [],
                source_ref=_as_id(_first_key(raw, "source_ref", "source_ref")),
                line=raw.get("line"),
                candidate=bool(raw.get("candidate", False)),
                inferred=not bool(observation_id),
            )

    station_inputs = [set(station["inputs"]) for station in stations]
    station_outputs = [set(station["logical_outputs"]) for station in stations]
    for index in range(len(stations) - 1):
        for output_id in station_outputs[index]:
            for input_id in station_inputs[index + 1]:
                if output_id == input_id:
                    add_edge(
                        output_id,
                        input_id,
                        edge_type="identity_handoff",
                        inferred=False,
                    )

    for raw in data.get("source_links", []):
        if not isinstance(raw, dict):
            continue
        source = _as_id(_first_key(raw, "source", "source_id", "original_id"))
        target = _as_id(_first_key(raw, "input", "target", "station_input"))
        if source and target:
            add_edge(source, target, edge_type="source_link", inferred=False)

    for raw in data.get("warehouse_bindings", []):
        if not isinstance(raw, dict):
            continue
        output_id = _as_id(_first_key(raw, "output", "output_id", "station_output"))
        warehouse_id = _as_id(
            _first_key(raw, "warehouse_row", "warehouse_id", "row", "id")
        )
        observation_id = _as_id(
            _first_key(raw, "observation_id", "evidence_id", "id")
        )
        if output_id and warehouse_id:
            add_edge(
                output_id,
                warehouse_id,
                edge_type="warehouse_binding",
                observation_ids=[observation_id] if observation_id else [],
                source_ref=_as_id(_first_key(raw, "source_ref", "source")),
                line=raw.get("line"),
                inferred=not bool(observation_id),
            )

    for row in data.get("warehouse_rows", []):
        if isinstance(row, dict):
            warehouse_id = _as_id(row)
            output_id = _as_id(
                _first_key(row, "output", "output_id", "station_output")
            )
            if warehouse_id and output_id:
                add_edge(output_id, warehouse_id, edge_type="warehouse_binding")

    return graph, edge_by_key


def _build_anchors(
    data: dict[str, Any], observations: list[Observation] | None = None
) -> dict[str, set[str]]:
    anchors: dict[str, set[str]] = {}
    raw_anchors = (
        data.get("endpoint_evidence", [])
        + data.get("endpoint_bindings", [])
        + data.get("anchors", [])
    )
    for raw in raw_anchors:
        if not isinstance(raw, dict):
            continue
        source = _as_id(_first_key(raw, "source", "source_id", "original_id"))
        warehouse_id = _as_id(
            _first_key(raw, "warehouse_row", "warehouse_id", "warehouse")
        )
        if source and warehouse_id:
            anchors.setdefault(warehouse_id, set()).add(source)

    for observation in observations or []:
        if observation.source and observation.warehouse_row:
            anchors.setdefault(observation.warehouse_row, set()).add(
                observation.source
            )
    return anchors


def _normalize_warehouse_rows(
    data: dict[str, Any], observations: list[Observation] | None = None
) -> list[str]:
    rows = _ids(data.get("warehouse_rows"))
    rows.extend(
        _as_id(_first_key(raw, "warehouse_row", "warehouse_id", "row"))
        for raw in data.get("warehouse_bindings", [])
        if isinstance(raw, dict)
    )
    rows.extend(
        observation.warehouse_row
        for observation in observations or []
        if observation.warehouse_row
    )
    return list(dict.fromkeys(row for row in rows if row))


def _trace_warehouse_rows(
    warehouse_rows: list[str],
    graph: dict[str, list[dict[str, Any]]],
    edge_evidence: dict[tuple[str, str], dict[str, Any]],
    stations: list[dict[str, Any]],
    observations: list[Observation],
    anchors: dict[str, set[str]],
    issues: list[Issue],
) -> list[dict[str, Any]]:
    reverse_graph: dict[str, list[dict[str, Any]]] = {}
    for edges in graph.values():
        for edge in edges:
            reverse_graph.setdefault(edge["to"], []).append(edge)

    output_observations: dict[tuple[str, str], list[str]] = {}
    group_canonical_outputs: dict[str, str] = {}
    for station in stations:
        for group in station.get("resend_groups", []) or []:
            if isinstance(group, dict):
                group_id = _as_id(_first_key(group, "id", "resend_group", "group_id"))
                canonical = _as_id(group.get("canonical_output"))
                outputs = _ids(group.get("outputs") or group.get("output_ids"))
                if group_id and canonical:
                    group_canonical_outputs[group_id] = canonical
                elif group_id and outputs:
                    group_canonical_outputs[group_id] = outputs[0]

    for observation in observations:
        if observation.station_id and observation.output_id:
            logical_output_id = str(
                observation.payload.get("resend_of")
                or _first_key(observation.payload, "canonical_output")
                or group_canonical_outputs.get(observation.resend_group or "")
                or observation.output_id
            )
            for output_id in {observation.output_id, logical_output_id}:
                output_observations.setdefault(
                    (observation.station_id, output_id), []
                ).append(observation.id)

    results: list[dict[str, Any]] = []
    station_ids = {station["id"] for station in stations}
    for warehouse_row in warehouse_rows:
        chains: list[dict[str, Any]] = []
        seen_chain_keys: set[tuple[str, ...]] = set()
        for node_path, edge_path in _walk_backward(
            warehouse_row, reverse_graph
        ):
            chain_key = tuple(node_path)
            if chain_key in seen_chain_keys:
                continue
            seen_chain_keys.add(chain_key)
            chain_stations = {
                edge.get("station_id")
                for edge in edge_path
                if edge.get("edge_type") == "station_binding"
            }
            if station_ids and chain_stations != station_ids:
                continue
            source = node_path[0]
            if anchors.get(warehouse_row) and source not in anchors[warehouse_row]:
                continue

            observation_ids: list[str] = []
            resend_observation_ids: list[str] = []
            resend_groups: list[str] = []
            for edge in edge_path:
                for observation_id in edge.get("observation_ids", []) or []:
                    observation_ids.append(observation_id)
                if edge.get("edge_type") == "station_binding":
                    key = (edge["station_id"], edge["to"])
                    for observation_id in output_observations.get(key, []):
                        observation_ids.append(observation_id)
                        observation = next(
                            (item for item in observations if item.id == observation_id),
                            None,
                        )
                        if observation and observation.resend_group:
                            resend_observation_ids.append(observation_id)
                            resend_groups.append(observation.resend_group)

            chain = {
                "source": source,
                "warehouse_row": warehouse_row,
                "nodes": node_path,
                "edges": edge_path,
                "observation_ids": list(dict.fromkeys(observation_ids)),
                "resend_observation_ids": list(
                    dict.fromkeys(resend_observation_ids)
                ),
                "resend_groups": list(dict.fromkeys(resend_groups)),
                "matching_ids": list(
                    dict.fromkeys(
                        matching
                        for edge in edge_path
                        for matching in (
                            edge.get("matching_ids")
                            or ([edge["matching_id"]] if edge.get("matching_id") else [])
                        )
                    )
                ),
            }
            chains.append(chain)
            if len(chains) >= _PATH_LIMIT:
                break

        candidate_sources = list(dict.fromkeys(chain["source"] for chain in chains))
        allowed_anchor_sources = sorted(anchors.get(warehouse_row, set()))
        unique_source = len(candidate_sources) == 1
        unique_chain = len({tuple(chain["nodes"]) for chain in chains}) == 1
        if not chains and allowed_anchor_sources:
            issues.append(
                Issue(
                    code="ENDPOINT_EVIDENCE_CONFLICT",
                    message=(
                        f"No station-valid chain reaches {warehouse_row} while "
                        f"endpoint evidence requires {allowed_anchor_sources}."
                    ),
                    observation_id=None,
                    expected=allowed_anchor_sources,
                )
            )

        alternatives = []
        for source in candidate_sources:
            source_chains = [chain for chain in chains if chain["source"] == source]
            alternatives.append(
                {
                    "source": source,
                    "chain_count": len(source_chains),
                    "matching_ids": list(
                        dict.fromkeys(
                            matching
                            for chain in source_chains
                            for matching in chain["matching_ids"]
                        )
                    ),
                    "still_allowed_constraints": [
                        constraint
                        for station in stations
                        for constraint in station["constraints"]
                    ],
                }
            )

        result = {
            "warehouse_row": warehouse_row,
            "candidate_sources": candidate_sources,
            "unique_source": unique_source,
            "unique_original_event": candidate_sources[0]
            if unique_source
            else None,
            "unique_chain": unique_chain and unique_source,
            "ambiguous": len(candidate_sources) > 1,
            "status": (
                "unique"
                if unique_source and unique_chain
                else "source_unique_chain_ambiguous"
                if unique_source
                else "ambiguous"
                if candidate_sources
                else "unresolved"
            ),
            "selection_policy": "enumerate_all_global_solutions; never order-based",
            "endpoint_anchor_sources": allowed_anchor_sources,
            "alternatives": alternatives,
            "chains": chains,
            "verifiable_constraints": [
                constraint
                for station in stations
                for constraint in station["constraints"]
            ],
        }
        results.append(result)
    return results


def _walk_backward(
    target: str, reverse_graph: dict[str, list[dict[str, Any]]]
) -> Iterable[tuple[list[str], list[dict[str, Any]]]]:
    def visit(
        current: str,
        nodes: tuple[str, ...],
        edges: tuple[dict[str, Any], ...],
        visited: frozenset[str],
    ):
        predecessors = reverse_graph.get(current, [])
        if not predecessors:
            yield list(nodes), list(edges)
            return
        for edge in predecessors:
            source = edge["from"]
            if source in visited:
                continue
            next_visited = visited | {source}
            next_nodes = (source,) + nodes
            next_edges = (edge,) + edges
            yield from visit(
                source, next_nodes, next_edges, next_visited
            )

    yield from visit(target, (target,), (), frozenset({target}))
