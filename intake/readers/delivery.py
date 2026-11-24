"""Cost-aware joint planning for multiple named deliveries.

This module is intentionally opt-in.  Existing single-output pipelines keep
their old semantics; only callers that ask for several deliveries and plugins
that carry delivery declarations participate in joint planning.
"""

from __future__ import annotations

import copy as _copy
import heapq
import inspect
import itertools
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterable, Mapping

try:  # pandas is an optional dependency of Intake, but required by this helper
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None


PROVIDES_ATTRS = (
    "delivery_provides",
    "provided_outputs",
    "provided",
    "provisions",
    "yields",
    "provides",
    "produces",
    "outputs",
    "output",
    "deliverables",
    "deliverable",
    "deliveries",
    "delivery",
    "delivery_names",
    "output_names",
    "named_outputs",
    "output_calibers",
    "output_schemas",
    "output_schema",
    "schema",
    "schemas",
    "output_specs",
    "output_spec",
    "provides_contract",
    "delivery_contract",
    "data_contract",
    "output_contracts",
    "output_profiles",
    "calibers",
    "caliber",
    "data_caliber",
    "output_caliber",
    "profiles",
    "profile",
    "output_profile",
    "contracts",
    "contract",
    "output_contract",
)
REQUIRES_ATTRS = (
    "delivery_requires",
    "required_inputs",
    "required",
    "requirements",
    "consumes",
    "input_requires",
    "requires",
    "needs",
    "inputs",
    "input_calibers",
    "input_schemas",
    "input_schema",
    "required_schema",
    "required_contract",
    "requires_contract",
    "input_specs",
    "input_spec",
    "input_contracts",
    "input_profiles",
    "dependencies",
    "depends_on",
)
COST_ATTRS = (
    "delivery_cost",
    "new_cost",
    "new_declared_cost",
    "declared_cost",
    "declared_overhead",
    "incremental_cost",
    "execution_cost",
    "compute_cost",
    "cost",
    "overhead_cost",
    "overhead",
    "price",
    "expense",
    "fee",
)
COVERAGE_ATTRS = (
    "coverage",
    "coverage_scope",
    "data_coverage",
    "row_coverage",
    "completeness",
    "scope",
    "data_scope",
    "row_scope",
    "covers",
    "range_kind",
)
MATERIALIZED_ATTRS = (
    "materialized",
    "is_materialized",
    "fully_materialized",
    "already_materialized",
    "realized",
    "fully_realized",
    "eager",
    "is_eager",
    "in_memory",
)
MUTATES_ATTRS = (
    "mutates_input",
    "mutates_its_input",
    "modifies_input",
    "changes_input",
    "alters_input",
    "in_place",
    "inplace",
    "destructive",
    "mutates",
)
COPY_ATTRS = ("is_copy", "copy_operation", "duplicates_input", "copy")
BATCH_ATTRS = ("batch_id", "batch", "batch_no", "batch_number")
FX_VERSION_ATTRS = ("fx_version", "rate_version", "exchange_rate_version", "rates_version")


class DeliveryPlanningError(ValueError):
    """Raised when no declared route can satisfy all requested deliveries."""

    def __init__(self, message: str = "", *, missing: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.missing = dict(missing or {})


UnsatisfiableDeliveryError = DeliveryPlanningError
NoDeliveryRoute = DeliveryPlanningError


@dataclass(frozen=True)
class DataContract:
    """A small, extensible description of a data caliber."""

    name: str | None = None
    python_type: str | type | None = None
    tags: Mapping[str, Any] = field(default_factory=dict)
    coverage: str | None = None
    materialized: bool | None = None

    def matches(self, other: Any) -> bool:
        wanted = as_contract(other)
        if self.name and wanted.name and self.name != wanted.name:
            return False
        if self.python_type and wanted.python_type:
            left = _type_name(self.python_type)
            right = _type_name(wanted.python_type)
            left_key = _type_key(self.python_type)
            right_key = _type_key(wanted.python_type)
            if left not in right and right not in left and left_key != right_key:
                return False
        for key, value in self.tags.items():
            if key not in wanted.tags:
                return False
            other_value = wanted.tags[key]
            if value is ... or other_value is ...:
                continue
            if value != other_value:
                return False
        return True


Caliber = DataContract | Mapping[str, Any] | str | None


@dataclass(frozen=True)
class DeliveryRequest:
    name: str
    caliber: Caliber = None
    coverage: str = "full"
    materialized: bool = True
    python_type: str | type | None = None
    batch_id: Any = None
    fx_version: Any = None
    count: int = 1
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def contract(self) -> DataContract:
        base = as_contract(self.caliber)
        tags = dict(base.tags)
        tags.update(self.metadata)
        contract_name = base.name
        if self.caliber is None:
            contract_name = contract_name or self.name
        return replace(
            base,
            name=contract_name,
            python_type=self.python_type or base.python_type,
            tags=tags,
            coverage=self.coverage or base.coverage,
            materialized=self.materialized
            if self.materialized is not None
            else base.materialized,
        )


Delivery = DeliveryRequest


@dataclass(frozen=True)
class ArtifactSpec:
    """An already available result.

    Existing artifacts have zero *new* cost.  Identity, batch and FX version
    are explicit; callers must not rely on URL/type/reader equality.
    """

    value: Any = None
    name: str | None = None
    caliber: Caliber = None
    profile: Caliber = None
    provides: Caliber = None
    coverage: str | None = None
    materialized: bool | None = None
    batch_id: Any = None
    fx_version: Any = None
    owner: str = "caller"
    mutable: bool = False
    reader: Any = None
    cost_paid: Any = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def request_profile(self) -> Caliber:
        return first_not_none(self.caliber, self.profile, self.provides, self.name)


ExistingArtifact = ArtifactSpec
AvailableResult = ArtifactSpec


class DeliveryResult(dict):
    """Mapping by delivery name that can also be unpacked in request order."""

    def __init__(self, *args, order: Iterable[str] = (), **kwargs):
        super().__init__(*args, **kwargs)
        self._order = tuple(order) or tuple(self.keys())

    def __iter__(self):
        for name in self._order:
            yield self[name]

    def __getattr__(self, name: str):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    @property
    def names(self) -> tuple[str, ...]:
        return self._order

    def as_tuple(self) -> tuple[Any, ...]:
        return tuple(self[name] for name in self._order)


@dataclass(frozen=True)
class InputSpec:
    profile: Caliber = None
    name: str | None = None
    keyword: str | None = None
    reader: Any = None
    coverage: str | None = None
    materialized: bool | None = None
    optional: bool = False
    allow_same: bool = False
    as_argument: bool = True


@dataclass
class PluginSpec:
    name: str
    obj: Any = None
    provides: tuple[Caliber, ...] = ()
    inputs: tuple[InputSpec, ...] = ()
    cost: Any = 0
    coverage: str | None = None
    materialized: bool | None = None
    mutates_input: bool = False
    is_copy: bool = False
    runner: Callable[..., Any] | None = None
    factory: Callable[..., Any] | None = None
    read_method: str = "read"
    static_args: tuple[Any, ...] = ()
    static_kwargs: Mapping[str, Any] = field(default_factory=dict)
    batch_aware: bool | None = None
    enabled: bool = True
    raw_declaration: Any = None


CandidateSpec = PluginSpec


@dataclass(frozen=True)
class Node:
    uid: int
    profile: Caliber
    coverage: str = "full"
    materialized: bool = True
    batch_id: Any = None
    fx_version: Any = None
    owner: str = "plan"
    mutable: bool = True
    value: Any = None
    reader: Any = None
    origin: str = ""
    tags: tuple[tuple[str, Any], ...] = ()
    valid: bool = True
    alias_of: int | None = None

    def key(self) -> tuple[Any, ...]:
        return (
            self.uid,
            self.semantic_key(),
        )

    def semantic_key(self) -> tuple[Any, ...]:
        return (
            repr(_profile_key(self.profile)),
            normalize_coverage(self.coverage) or "",
            bool(self.materialized),
            repr(_stable_hash(self.batch_id)),
            repr(_stable_hash(self.fx_version)),
            self.owner,
            bool(self.mutable),
            self.origin if self.reader is None else f"reader:{id(self.reader)}",
            tuple(sorted((str(key), repr(_stable_hash(value))) for key, value in self.tags)),
            self.valid,
            id(self.reader) if self.reader is not None else 0,
        )


@dataclass(frozen=True)
class Action:
    uid: int
    plugin: PluginSpec
    output_uids: tuple[int, ...]
    input_uids: tuple[int, ...]
    keywords: Mapping[str, int] = field(default_factory=dict)
    cost: Any = 0
    label: str = ""
    shared: bool = False

    @property
    def name(self) -> str:
        return self.label or self.plugin.name


@dataclass
class PlannedStep:
    order: int
    name: str
    cost: Any
    output: str
    output_uids: tuple[int, ...]
    input_uids: tuple[int, ...]
    action: Action | None = None
    description: str = ""
    shared: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "order": self.order,
            "name": self.name,
            "cost": self.cost,
            "output": self.output,
            "output_uids": self.output_uids,
            "input_uids": self.input_uids,
            "description": self.description,
            "shared": self.shared,
        }


@dataclass
class JointPlan:
    requests: tuple[DeliveryRequest, ...]
    steps: tuple[Action, ...]
    nodes: Mapping[int, Node]
    goal_nodes: Mapping[str, int]
    total_cost_value: Any
    batch_id: Any = None
    fx_version: Any = None
    source: Any = None
    feasible: bool = True
    missing: Mapping[str, Any] = field(default_factory=dict)
    planner: "DeliveryPlanner | None" = None
    _values: dict[int, Any] = field(default_factory=dict)
    _executed: set[int] = field(default_factory=set)
    _executed_actions: set[int] = field(default_factory=set)
    actual_cost_value: Any = 0
    execution_counts: dict[str, int] = field(default_factory=dict)

    @property
    def cost(self) -> Any:
        return self.total_cost_value

    @property
    def total_cost(self) -> Any:
        return self.total_cost_value

    @property
    def new_cost(self) -> Any:
        return self.total_cost_value

    @property
    def incremental_cost(self) -> Any:
        return self.total_cost_value

    @property
    def total_new_cost(self) -> Any:
        return self.total_cost_value

    @property
    def new_declared_cost(self) -> Any:
        return self.total_cost_value

    @property
    def total_declared_cost(self) -> Any:
        return self.total_cost_value

    @property
    def total_overhead(self) -> Any:
        return self.total_cost_value

    @property
    def overhead(self) -> Any:
        return self.total_cost_value

    @property
    def actual_cost(self) -> Any:
        return self.actual_cost_value

    @property
    def spent_cost(self) -> Any:
        return self.actual_cost_value

    @property
    def actual_new_cost(self) -> Any:
        return self.actual_cost_value

    @property
    def operations(self) -> tuple[Action, ...]:
        return self.steps

    @property
    def actions(self) -> tuple[Action, ...]:
        return self.steps

    @property
    def tasks(self) -> tuple[Action, ...]:
        return self.steps

    @property
    def cost_breakdown(self) -> list[dict[str, Any]]:
        return self.describe()

    def costs(self) -> list[Any]:
        return [action.cost for action in self.steps]

    def describe(self) -> list[dict[str, Any]]:
        result = []
        for index, action in enumerate(self.steps, 1):
            result.append(
                {
                    "order": index,
                    "name": action.name,
                    "cost": action.cost,
                    "inputs": list(action.input_uids),
                    "outputs": list(action.output_uids),
                    "shared": action.shared,
                }
            )
        return result

    def explain(self) -> str:
        lines = [
            f"Joint delivery plan: total new cost = {self.total_cost_value}",
            f"batch_id={self.batch_id!r}, fx_version={self.fx_version!r}",
        ]
        for row in self.describe():
            lines.append(
                f"  {row['order']}. {row['name']} cost={row['cost']} "
                f"in={row['inputs']} out={row['outputs']}"
                + (" [shared]" if row["shared"] else "")
            )
        for name, uid in self.goal_nodes.items():
            node = self.nodes[uid]
            lines.append(f"  delivery {name!r} -> node {uid} ({_profile_key(node.profile)})")
        if self.missing:
            lines.append(f"  missing: {dict(self.missing)}")
        return "\n".join(lines)

    __repr__ = explain

    def source_for(self, delivery_name: str) -> dict[str, Any]:
        uid = self.goal_nodes[delivery_name]
        chain = []
        seen = set()

        def walk(node_uid: int) -> None:
            if node_uid in seen:
                return
            seen.add(node_uid)
            for action in self.steps:
                if node_uid in action.output_uids:
                    chain.append(action.name)
                    for input_uid in action.input_uids:
                        walk(input_uid)
                    break

        walk(uid)
        node = self.nodes[uid]
        return {
            "delivery": delivery_name,
            "node": uid,
            "profile": node.profile,
            "coverage": node.coverage,
            "materialized": node.materialized,
            "origin": node.origin,
            "steps": list(reversed(chain)),
        }

    def sources(self) -> dict[str, dict[str, Any]]:
        return {name: self.source_for(name) for name in self.goal_nodes}

    def sharing(self) -> dict[str, list[str]]:
        consumers: dict[int, list[str]] = {}
        for name, uid in self.goal_nodes.items():
            consumers.setdefault(uid, []).append(name)
        for action in self.steps:
            for input_uid in action.input_uids:
                consumers.setdefault(input_uid, []).append(action.name)
        return {
            f"node:{uid}": users
            for uid, users in consumers.items()
            if len(users) > 1
        }

    def execute(self, order: Iterable[str] | None = None, **kwargs: Any) -> DeliveryResult:
        if not self.feasible:
            raise DeliveryPlanningError(
                "No feasible delivery plan", missing=self.missing
            )
        names = tuple(order or [request.name for request in self.requests])
        unknown = [name for name in names if name not in self.goal_nodes]
        if unknown:
            raise KeyError(f"Unknown deliveries: {unknown!r}")
        values = {name: self.read(name, **kwargs) for name in names}
        return DeliveryResult(values, order=names)

    run = execute
    deliver = execute

    def __call__(self, *args: Any, **kwargs: Any) -> DeliveryResult:
        return self.execute(*args, **kwargs)

    def read(self, name: str | None = None, **kwargs: Any) -> Any:
        if name is None:
            return self.execute(**kwargs)
        if name not in self.goal_nodes:
            raise KeyError(name)
        target_uid = self.goal_nodes[name]
        self._ensure_node(target_uid, **kwargs)
        return self._values[target_uid]

    def _ensure_node(self, uid: int, **kwargs: Any) -> None:
        if uid in self._executed:
            return
        node = self.nodes[uid]
        producer_index = None
        for index, action in enumerate(self.steps):
            if uid in action.output_uids:
                producer_index = index
                break
        if producer_index is None:
            self._values.setdefault(uid, node.value)
            self._executed.add(uid)
            return
        for index in range(producer_index + 1):
            action = self.steps[index]
            if action.uid in self._executed_actions:
                continue
            self._run_action(action, **kwargs)
            self._executed_actions.add(action.uid)
        self._executed.add(uid)

    def _run_action(self, action: Action, **kwargs: Any) -> Any:
        for input_uid in action.input_uids:
            if input_uid not in self._values:
                input_node = self.nodes[input_uid]
                self._values[input_uid] = input_node.value
        keyword_values = {}
        positional = []
        for input_spec, input_uid in zip(action.plugin.inputs, action.input_uids):
            if not input_spec.as_argument:
                continue
            value = self._values[input_uid]
            if input_spec.keyword:
                keyword_values[input_spec.keyword] = value
            else:
                positional.append(value)
        value = _run_plugin(
            action.plugin,
            positional,
            keyword_values,
            self,
            **kwargs,
        )
        if action.plugin.mutates_input:
            argument_uids = [
                uid
                for spec, uid in zip(action.plugin.inputs, action.input_uids)
                if spec.as_argument and not spec.keyword
            ]
            if argument_uids:
                self._values[argument_uids[0]] = value
        for output_uid in action.output_uids:
            self._values[output_uid] = value
            self._executed.add(output_uid)
        self.actual_cost_value = _add_costs(self.actual_cost_value, action.cost)
        self.execution_counts[action.name] = self.execution_counts.get(action.name, 0) + 1
        return value

    def __getitem__(self, name: str) -> Any:
        return self.read(name)


def _is_unrun_reader(node: Node) -> bool:
    return node.value is None and node.reader is not None


class DeliveryPlanner:
    def __init__(
        self,
        candidates: Iterable[Any] | None = None,
        *,
        source: Any = None,
        available: Iterable[Any] | Mapping[str, Any] | None = None,
        existing: Iterable[Any] | Mapping[str, Any] | None = None,
        batch_id: Any = None,
        fx_version: Any = None,
        copy_cost: Any = 3,
        max_actions: int = 10,
        discover: bool = False,
        **context: Any,
    ):
        self.source = source
        self.batch_id = batch_id
        self.fx_version = fx_version
        self.copy_cost = copy_cost
        self.max_actions = max_actions
        self.context = context
        self.candidates = normalize_candidates(
            candidates,
            source=source,
            copy_cost=copy_cost,
            discover=discover,
        )
        self.artifacts = normalize_artifacts(
            available if available is not None else existing,
            batch_id=batch_id,
            fx_version=fx_version,
        )

    def plan(
        self,
        deliveries: Iterable[Caliber | DeliveryRequest | Mapping[str, Any] | tuple[Any, ...]],
        **overrides: Any,
    ) -> JointPlan:
        requests = normalize_requests(
            deliveries,
            batch_id=overrides.pop("batch_id", self.batch_id),
            fx_version=overrides.pop("fx_version", self.fx_version),
        )
        batch_id = requests[0].batch_id if requests else self.batch_id
        fx_version = requests[0].fx_version if requests else self.fx_version
        copy_cost = overrides.pop("copy_cost", self.copy_cost)
        replacement_candidates = overrides.pop("candidates", None) or overrides.pop("plugins", None)
        initial_nodes = self._initial_nodes(requests, batch_id, fx_version)
        if replacement_candidates is not None:
            plugins = list(
                normalize_candidates(
                    replacement_candidates,
                    source=self.source,
                    copy_cost=copy_cost,
                    discover=overrides.pop("discover", False),
                )
            )
        else:
            plugins = list(self.candidates)
            for plugin in plugins:
                if plugin.obj is not None:
                    refreshed_cost = _extract_cost(
                        plugin.obj, _declaration_mapping(plugin.obj)
                    )
                    if refreshed_cost is not None:
                        plugin.cost = refreshed_cost
                if plugin.is_copy and plugin.name == "copy":
                    plugin.cost = copy_cost
        plugins.extend(self._reader_dependency_plugins(initial_nodes))
        plugins = _dedupe_plugins(plugins)
        goal_profiles = [request.contract() for request in requests]

        start_state = _SearchState(nodes=initial_nodes)
        counter = itertools.count()
        heap: list[tuple[Any, int, int, _SearchState]] = []
        heapq.heappush(heap, (0, 0, next(counter), start_state))
        best_goal: _SearchState | None = None
        visited: set[tuple[Any, ...]] = set()

        while heap:
            cost, _heuristic, token, state = heapq.heappop(heap)
            if best_goal is not None and _cost_ge(cost, best_goal.cost):
                continue
            if len(state.actions) > self.max_actions:
                continue
            key = state.canonical_key()
            if key in visited:
                continue
            visited.add(key)
            if state.satisfies(requests, goal_profiles):
                best_goal = state
                break
            for action in self._actions(state, plugins, batch_id, fx_version):
                next_state = state.apply(action)
                if next_state is None:
                    continue
                if best_goal is not None and _cost_ge(next_state.cost, best_goal.cost):
                    continue
                heapq.heappush(
                    heap,
                    (
                        next_state.cost,
                        len(next_state.actions),
                        next(counter),
                        next_state,
                    ),
                )

        if best_goal is None:
            missing = self._missing_report(requests, initial_nodes, plugins)
            error = DeliveryPlanningError(
                _missing_message(missing), missing=missing
            )
            if overrides.get("raise_on_missing", True):
                raise error
            return JointPlan(
                requests=tuple(requests),
                steps=(),
                nodes=initial_nodes,
                goal_nodes={},
                total_cost_value=None,
                batch_id=batch_id,
                fx_version=fx_version,
                source=self.source,
                feasible=False,
                missing=missing,
                planner=self,
            )

        goal_nodes = {}
        for request, profile in zip(requests, goal_profiles):
            uid = best_goal.find_goal(profile, request)
            if uid is None:  # defensive; satisfies() checked this
                raise DeliveryPlanningError(f"Missing delivery {request.name!r}")
            goal_nodes[request.name] = uid
        actions = tuple(best_goal.actions)
        nodes = dict(best_goal.nodes)
        _mark_shared_actions(actions)
        plan = JointPlan(
            requests=tuple(requests),
            steps=actions,
            nodes=nodes,
            goal_nodes=goal_nodes,
            total_cost_value=best_goal.cost,
            batch_id=batch_id,
            fx_version=fx_version,
            source=self.source,
            planner=self,
        )
        for uid, node in nodes.items():
            if node.value is not None:
                plan._values[uid] = node.value
                plan._executed.add(uid)
        return plan

    def _initial_nodes(
        self,
        requests: tuple[DeliveryRequest, ...],
        batch_id: Any,
        fx_version: Any,
    ) -> dict[int, Node]:
        nodes: dict[int, Node] = {}
        uid = 1
        for artifact in self.artifacts:
            profile = artifact.request_profile() or artifact.name or f"artifact:{uid}"
            coverage = normalize_coverage(artifact.coverage)
            materialized = artifact.materialized
            if materialized is None:
                materialized = coverage not in {"lazy", "unmaterialized"} and artifact.value is not None
            node_batch = first_not_none(artifact.batch_id, batch_id)
            node_fx = first_not_none(artifact.fx_version, fx_version)
            nodes[uid] = Node(
                uid=uid,
                profile=as_contract(profile),
                coverage=coverage or "unknown",
                materialized=bool(materialized),
                batch_id=node_batch,
                fx_version=node_fx,
                owner=artifact.owner,
                mutable=artifact.mutable,
                value=artifact.value,
                reader=artifact.reader,
                origin=artifact.name or "existing",
                tags=tuple(dict(artifact.metadata).items()),
            )
            uid += 1
        if self.source is not None and not any(
            node.value is self.source or node.reader is self.source for node in nodes.values()
        ):
            if isinstance(self.source, BaseReaderLike):
                nodes[uid] = Node(
                    uid=uid,
                    profile=_reader_profile(self.source) or "reader",
                    coverage=_reader_coverage(self.source) or "unknown",
                    materialized=False,
                    batch_id=batch_id,
                    fx_version=fx_version,
                    owner="caller",
                    mutable=False,
                    reader=self.source,
                    origin=type(self.source).__name__,
                )
            else:
                nodes[uid] = Node(
                    uid=uid,
                    profile="source",
                    coverage="unknown",
                    materialized=False,
                    batch_id=batch_id,
                    fx_version=fx_version,
                    owner="caller",
                    mutable=False,
                    value=self.source,
                    origin="source",
                )
            uid += 1
        for plugin in self.candidates:
            for input_spec in plugin.inputs:
                reader = input_spec.reader
                if reader is None or any(node.reader is reader for node in nodes.values()):
                    continue
                nodes[uid] = Node(
                    uid=uid,
                    profile=_reader_profile(reader) or input_spec.profile or "reader_dependency",
                    coverage=_reader_coverage(reader) or "unknown",
                    materialized=False,
                    batch_id=batch_id,
                    fx_version=fx_version,
                    owner="caller",
                    mutable=False,
                    reader=reader,
                    origin=_candidate_name(reader, _declaration_mapping(reader)),
                )
                uid += 1
        return nodes

    def _reader_dependency_plugins(self, nodes: Mapping[int, Node]) -> list[PluginSpec]:
        result = []
        seen = set()
        for node in nodes.values():
            reader = node.reader
            if reader is None or id(reader) in seen:
                continue
            seen.add(id(reader))
            result.append(reader_plugin(reader))
        return result

    def _actions(
        self,
        state: "_SearchState",
        plugins: list[PluginSpec],
        batch_id: Any,
        fx_version: Any,
    ) -> list[Action]:
        actions: list[Action] = []
        next_uid = max(state.nodes) + 1 if state.nodes else 1
        for plugin in plugins:
            if not plugin.enabled:
                continue
            for action in self._plugin_actions(
                plugin, state, next_uid, batch_id, fx_version
            ):
                actions.append(action)
                next_uid = max(action.output_uids, default=next_uid - 1) + 1
        return actions

    def _plugin_actions(
        self,
        plugin: PluginSpec,
        state: "_SearchState",
        next_uid: int,
        batch_id: Any,
        fx_version: Any,
    ) -> list[Action]:
        input_matches = []
        for input_spec in plugin.inputs:
            matches = []
            for uid, node in state.nodes.items():
                if plugin.is_copy and node.origin == "copy":
                    continue
                if not _input_matches(input_spec, node, batch_id, fx_version):
                    continue
                if plugin.mutates_input and node.owner == "caller" and not node.mutable:
                    continue
                matches.append(uid)
            if plugin.mutates_input and not input_spec.keyword and matches:
                copied_matches = [
                    uid for uid in matches if state.nodes[uid].origin == "copy"
                ]
                if copied_matches:
                    matches = copied_matches
            if not matches and not input_spec.optional:
                return []
            input_matches.append(matches)
        if not plugin.inputs:
            assignments = [()]
        else:
            assignments = itertools.product(*input_matches)
        result = []
        next_output_uid = next_uid
        for assignment in assignments:
            if _has_duplicate_non_same_input(plugin.inputs, assignment):
                continue
            keywords = {}
            positional = []
            for spec, uid in zip(plugin.inputs, assignment):
                if spec.keyword:
                    keywords[spec.keyword] = uid
                else:
                    positional.append(uid)
            ordered_uids = tuple(assignment)
            output_count = len(plugin.provides) or 1
            output_uids = tuple(range(next_output_uid, next_output_uid + output_count))
            next_output_uid += output_count
            action = Action(
                uid=output_uids[0],
                plugin=plugin,
                output_uids=output_uids,
                input_uids=ordered_uids,
                keywords=keywords,
                cost=plugin.cost,
                label=plugin.name,
            )
            result.append(action)
        return result

    def _missing_report(
        self,
        requests: tuple[DeliveryRequest, ...],
        nodes: Mapping[int, Node],
        plugins: list[PluginSpec],
    ) -> dict[str, Any]:
        report = {}
        profiles_available = [_profile_key(node.profile) for node in nodes.values()]
        for request in requests:
            profile = request.contract()
            matches = [uid for uid, node in nodes.items() if _goal_matches(profile, request, node)]
            if matches:
                continue
            providers = [
                plugin.name
                for plugin in plugins
                if any(as_contract(out).matches(profile) or profile.matches(out) for out in plugin.provides)
            ]
            report[request.name] = {
                "wanted": _profile_key(profile),
                "coverage": request.coverage,
                "materialized": request.materialized,
                "available_profiles": profiles_available,
                "candidate_providers": providers,
                "reason": _missing_reason(request, nodes, plugins),
            }
        return report


class _SearchState:
    def __init__(self, *, nodes: Mapping[int, Node], actions: tuple[Action, ...] = (), cost: Any = 0):
        self.nodes = dict(nodes)
        self.actions = tuple(actions)
        self.cost = cost

    def canonical_key(self) -> tuple[Any, ...]:
        return tuple(sorted(node.semantic_key() for node in self.nodes.values()))

    def apply(self, action: Action) -> "_SearchState | None":
        nodes = dict(self.nodes)
        plugin = action.plugin
        output_profiles = plugin.provides or (_infer_output_profile(plugin),)
        if plugin.mutates_input and action.input_uids and action.output_uids:
            argument_uids = [
                uid
                for spec, uid in zip(plugin.inputs, action.input_uids)
                if spec.as_argument and not spec.keyword
            ]
            target_uid = argument_uids[0] if argument_uids else action.input_uids[0]
            target = nodes[target_uid]
            if target.owner == "caller" and not target.mutable:
                return None
            output_uid = action.output_uids[0]
            output_profile = output_profiles[0] if output_profiles else target.profile
            nodes[target_uid] = replace(target, valid=False)
            nodes[output_uid] = replace(
                target,
                uid=output_uid,
                profile=as_contract(output_profile),
                coverage=normalize_coverage(plugin.coverage or target.coverage),
                materialized=plugin.materialized
                if plugin.materialized is not None
                else target.materialized,
                owner="plan",
                mutable=True,
                value=None,
                origin=plugin.name,
                valid=True,
                alias_of=target_uid,
            )
        else:
            for uid, output_profile in zip(action.output_uids, output_profiles):
                if plugin.is_copy and action.input_uids:
                    source_node = nodes[action.input_uids[0]]
                    nodes[uid] = replace(
                        source_node,
                        uid=uid,
                        owner="plan",
                        mutable=True,
                        value=None,
                        reader=None,
                        origin=plugin.name,
                    )
                else:
                    nodes[uid] = Node(
                        uid=uid,
                        profile=as_contract(output_profile),
                        coverage=normalize_coverage(plugin.coverage) or "full",
                        materialized=True if plugin.materialized is None else plugin.materialized,
                        batch_id=_context_value(nodes, action, "batch_id"),
                        fx_version=_context_value(nodes, action, "fx_version"),
                        owner="plan",
                        mutable=not _output_should_be_readonly(plugin, output_profile),
                        reader=plugin.obj if isinstance(plugin.obj, BaseReaderLike) else None,
                        origin=plugin.name,
                    )
        return _SearchState(
            nodes=nodes,
            actions=self.actions + (action,),
            cost=_add_costs(self.cost, action.cost),
        )

    def satisfies(
        self,
        requests: tuple[DeliveryRequest, ...],
        profiles: list[DataContract],
    ) -> bool:
        return all(
            self.find_goal(profile, request) is not None
            for request, profile in zip(requests, profiles)
        )

    def find_goal(self, profile: DataContract, request: DeliveryRequest) -> int | None:
        candidates = []
        for uid, node in self.nodes.items():
            if not node.valid:
                continue
            if not _goal_matches(profile, request, node):
                continue
            candidates.append(uid)
        if not candidates:
            return None
        return min(candidates, key=lambda uid: (self.nodes[uid].owner != "caller", uid))


def plan_deliveries(*args: Any, **kwargs: Any) -> JointPlan:
    """Plan multiple named deliveries at minimum new declared cost."""
    source, deliveries, candidates, positional_existing, rest = _parse_plan_args(args)
    delivery_keys = (
        "deliveries",
        "delivery",
        "deliver",
        "delivery_names",
        "delivery_requests",
        "names",
        "named_outputs",
        "outputs",
        "requested",
        "requests",
        "targets",
        "want",
    )
    for key in delivery_keys:
        if deliveries is None and key in kwargs:
            deliveries = kwargs.pop(key)
        else:
            kwargs.pop(key, None)
    candidate_keys = (
        "candidates",
        "plugins",
        "readers",
        "converters",
        "operations",
        "providers",
        "candidate_plugins",
        "registry",
    )
    for key in candidate_keys:
        if candidates is None and key in kwargs:
            candidates = kwargs.pop(key)
        else:
            kwargs.pop(key, None)
    if source is None:
        source = kwargs.pop("source", None)
    if source is None:
        source = kwargs.pop("url", None)
    existing = None
    for key in (
        "existing",
        "available",
        "artifacts",
        "existing_results",
        "available_results",
        "prior_results",
        "results",
        "inventory",
        "provided",
    ):
        if key in kwargs:
            value = kwargs.pop(key)
            if existing is None:
                existing = value
    if existing is None:
        existing = positional_existing
    if existing is not None:
        kwargs["existing"] = existing
    raise_on_missing = kwargs.pop("raise_on_missing", True)
    kwargs.setdefault("discover", candidates is None)
    if "batch_id" not in kwargs:
        for key in ("batch", "batch_no", "batch_number", "batchid"):
            if key in kwargs:
                kwargs["batch_id"] = kwargs.pop(key)
                break
            kwargs.pop(key, None)
    else:
        for key in ("batch", "batch_no", "batch_number", "batchid"):
            kwargs.pop(key, None)
    if "fx_version" not in kwargs:
        for key in ("fx_ver", "rate_version", "exchange_rate_version", "rates_version"):
            if key in kwargs:
                kwargs["fx_version"] = kwargs.pop(key)
                break
            kwargs.pop(key, None)
    else:
        for key in ("fx_ver", "rate_version", "exchange_rate_version", "rates_version"):
            kwargs.pop(key, None)
    if "copy_cost" not in kwargs:
        for key in ("copy_quote", "copy_overhead", "copy_price"):
            if key in kwargs:
                kwargs["copy_cost"] = kwargs.pop(key)
                break
            kwargs.pop(key, None)
    else:
        for key in ("copy_quote", "copy_overhead", "copy_price"):
            kwargs.pop(key, None)
    if deliveries is None and rest:
        deliveries = rest
    if deliveries is None:
        raise TypeError("plan_deliveries requires delivery names/specifications")
    planner = DeliveryPlanner(candidates, source=source, **kwargs)
    return planner.plan(deliveries, raise_on_missing=raise_on_missing)


def joint_plan(*args: Any, **kwargs: Any) -> JointPlan:
    return plan_deliveries(*args, **kwargs)


def plan_joint_deliveries(*args: Any, **kwargs: Any) -> JointPlan:
    return plan_deliveries(*args, **kwargs)


def deliver_many(*args: Any, execute: bool = True, **kwargs: Any) -> Any:
    plan = plan_deliveries(*args, **kwargs)
    return plan.execute() if execute else plan


def auto_deliver(*args: Any, **kwargs: Any) -> Any:
    return deliver_many(*args, **kwargs)


auto_deliveries = deliver_many
read_deliveries = deliver_many


def normalize_requests(
    deliveries: Iterable[Any],
    *,
    batch_id: Any = None,
    fx_version: Any = None,
) -> tuple[DeliveryRequest, ...]:
    if isinstance(deliveries, Mapping):
        data = dict(deliveries)
        if {"name"} & set(data) or {"caliber", "contract", "profile"} & set(data):
            deliveries = [data]
        else:
            normalized = []
            for name, spec in data.items():
                if isinstance(spec, Mapping) and not _looks_like_contract_mapping(spec):
                    item = dict(spec)
                    item.setdefault("name", name)
                    normalized.append(item)
                else:
                    normalized.append((name, spec))
            deliveries = normalized
    elif isinstance(deliveries, (str, bytes, DeliveryRequest)):
        deliveries = [deliveries]
    result = []
    for index, item in enumerate(deliveries):
        if isinstance(item, DeliveryRequest):
            request = item
        elif isinstance(item, str):
            request = DeliveryRequest(name=item)
        elif isinstance(item, tuple) and len(item) == 2:
            left, right = item
            if isinstance(left, str):
                name, caliber = left, right
            elif isinstance(right, str):
                caliber, name = left, right
            else:
                name, caliber = f"delivery_{index}", item
            request = DeliveryRequest(name=str(name), caliber=caliber)
        elif isinstance(item, Mapping):
            data = dict(item)
            name = str(
                first_not_none(
                    data.pop("name", None),
                    data.pop("delivery", None),
                    data.pop("key", None),
                    f"delivery_{index}",
                )
            )
            caliber = first_not_none(
                data.pop("caliber", None),
                data.pop("contract", None),
                data.pop("profile", None),
                data.pop("schema", None),
            )
            known = {
                key: value
                for key, value in data.items()
                if key in DeliveryRequest.__dataclass_fields__
            }
            extra = {
                key: value
                for key, value in data.items()
                if key not in DeliveryRequest.__dataclass_fields__
            }
            if caliber is None and extra:
                caliber = extra
            request = DeliveryRequest(name=name, caliber=caliber, **known)
        else:
            name = getattr(item, "name", None) or getattr(item, "delivery_name", None) or f"delivery_{index}"
            request = DeliveryRequest(
                name=str(name),
                caliber=getattr(item, "caliber", None) or getattr(item, "contract", None) or getattr(item, "profile", None),
                coverage=getattr(item, "coverage", "full"),
                materialized=getattr(item, "materialized", True),
                batch_id=getattr(item, "batch_id", batch_id),
                fx_version=getattr(item, "fx_version", fx_version),
            )
        if request.batch_id is None:
            request = replace(request, batch_id=batch_id)
        if request.fx_version is None:
            request = replace(request, fx_version=fx_version)
        result.append(request)
    return tuple(result)


def normalize_artifacts(
    available: Iterable[Any] | Mapping[str, Any] | None,
    *,
    batch_id: Any = None,
    fx_version: Any = None,
) -> tuple[ArtifactSpec, ...]:
    if available is None:
        return ()
    if isinstance(available, ArtifactSpec):
        return (_infer_artifact(available, batch_id, fx_version),)
    if pd is not None and isinstance(available, pd.DataFrame):
        available = [available]
    elif not isinstance(available, (Mapping, list, tuple, set, frozenset)):
        available = [available]
    if isinstance(available, Mapping):
        items = []
        for name, value in available.items():
            if isinstance(value, ArtifactSpec):
                spec = replace(value, name=value.name or str(name))
            elif isinstance(value, tuple) and len(value) == 2:
                spec = ArtifactSpec(name=str(name), value=value[0], caliber=value[1])
            else:
                spec = ArtifactSpec(name=str(name), value=value)
            items.append(_infer_artifact(spec, batch_id, fx_version))
        return tuple(items)
    result = []
    for item in available:
        if isinstance(item, ArtifactSpec):
            spec = item
        elif isinstance(item, Mapping):
            spec = ArtifactSpec(**item)
        elif isinstance(item, tuple) and len(item) == 2:
            spec = ArtifactSpec(value=item[0], caliber=item[1])
        else:
            spec = ArtifactSpec(value=item, name=type(item).__name__)
        result.append(_infer_artifact(spec, batch_id, fx_version))
    return tuple(result)


def _infer_artifact(
    spec: ArtifactSpec,
    batch_id: Any = None,
    fx_version: Any = None,
) -> ArtifactSpec:
    value = spec.value
    updates: dict[str, Any] = {}
    if value is not None:
        if spec.caliber is None and spec.profile is None and spec.provides is None:
            profile = _reader_profile(value)
            if profile is not None:
                updates["profile"] = profile
        if spec.coverage is None:
            coverage = _reader_coverage(value)
            if coverage is not None:
                updates["coverage"] = coverage
        if spec.materialized is None:
            materialized = _reader_materialized(value)
            if materialized is None and not isinstance(value, BaseReaderLike):
                materialized = True
            if materialized is not None:
                updates["materialized"] = materialized
        if spec.batch_id is None:
            for attr in BATCH_ATTRS:
                candidate = getattr(value, attr, None)
                if candidate is not None:
                    updates["batch_id"] = candidate
                    break
        if spec.fx_version is None:
            for attr in FX_VERSION_ATTRS:
                candidate = getattr(value, attr, None)
                if candidate is not None:
                    updates["fx_version"] = candidate
                    break
    if spec.batch_id is None and batch_id is not None:
        updates["batch_id"] = batch_id
    if spec.fx_version is None and fx_version is not None:
        updates["fx_version"] = fx_version
    return replace(spec, **updates) if updates else spec


def normalize_candidates(
    candidates: Iterable[Any] | None,
    *,
    source: Any = None,
    copy_cost: Any = 3,
    discover: bool = False,
) -> tuple[PluginSpec, ...]:
    if candidates is None and discover:
        candidates = _declared_subclasses()
    elif candidates is None:
        candidates = ()
    if isinstance(candidates, Mapping):
        candidates = tuple(candidates.values())
    elif not isinstance(candidates, (list, tuple, set, frozenset)):
        candidates = (candidates,)
    result = []
    for item in candidates:
        if isinstance(item, str):
            try:
                from intake import import_name

                item = import_name(item)
            except Exception:
                continue
        result.append(candidate_spec(item))
    if not any(spec.is_copy for spec in result):
        result.append(_copy_plugin(copy_cost))
    return tuple(_dedupe_plugins(result))


def candidate_spec(obj: Any, **overrides: Any) -> PluginSpec:
    if isinstance(obj, PluginSpec):
        if overrides:
            return replace(obj, **overrides)
        return obj
    decl = _declaration_mapping(obj)
    name = _candidate_name(obj, decl)
    cost = _extract_cost(obj, decl)
    provides = _as_profile_tuple(
        _first_attr_or_mapping(obj, PROVIDES_ATTRS, decl)
    )
    requires_value = _first_attr_or_mapping(obj, REQUIRES_ATTRS, decl)
    instances = getattr(obj, "instances", None)
    if requires_value is None and isinstance(instances, Mapping) and instances:
        requires_value = tuple(instances.keys())
    if not provides and isinstance(instances, Mapping) and instances:
        provides = tuple(value for value in instances.values() if value is not None)
    inputs = _as_inputs(requires_value, obj)
    coverage = normalize_coverage(
        _first_attr_or_mapping(obj, COVERAGE_ATTRS, decl)
    )
    materialized = _optional_bool(
        _first_attr_or_mapping(obj, MATERIALIZED_ATTRS, decl)
    )
    mutates_input = bool(
        _optional_bool(_first_attr_or_mapping(obj, MUTATES_ATTRS, decl)) or False
    )
    is_copy = bool(_optional_bool(_first_attr_or_mapping(obj, COPY_ATTRS, decl)) or False)
    if is_copy and not provides:
        provides = ("same",)
    if not provides and hasattr(obj, "output_instance") and _has_delivery_decl(obj, decl):
        provides = (getattr(obj, "output_instance"),)
    if cost is None and _has_delivery_decl(obj, decl):
        cost = 0
    spec = PluginSpec(
        name=name,
        obj=obj,
        provides=tuple(as_contract(p) if not isinstance(p, DataContract) else p for p in provides),
        inputs=tuple(inputs),
        cost=cost if cost is not None else 0,
        coverage=coverage,
        materialized=materialized,
        mutates_input=mutates_input,
        is_copy=is_copy,
        static_kwargs=_bound_kwargs(obj),
        raw_declaration=decl or obj,
    )
    for key, value in overrides.items():
        setattr(spec, key, value)
    return spec


def reader_plugin(reader: Any) -> PluginSpec:
    spec = candidate_spec(reader)
    if not spec.provides:
        profile = _reader_profile(reader) or type(reader).__name__
        spec.provides = (as_contract(profile),)
    cost = _extract_cost(reader, _declaration_mapping(reader))
    if cost is not None:
        spec.cost = cost
    spec.inputs = (
        InputSpec(
            reader=reader,
            profile=spec.provides[0] if spec.provides else None,
            materialized=False,
            as_argument=False,
        ),
    )
    return spec


def _copy_plugin(cost: Any) -> PluginSpec:
    return PluginSpec(
        name="copy",
        provides=(),
        inputs=(InputSpec(profile=None, materialized=True),),
        cost=cost,
        is_copy=True,
        mutates_input=False,
        runner=_copy_value,
    )


def _run_plugin(
    plugin: PluginSpec,
    positional: list[Any],
    keywords: dict[str, Any],
    plan: JointPlan,
    **runtime_kwargs: Any,
) -> Any:
    if plugin.runner is not None:
        return plugin.runner(*positional, **keywords)
    obj = plugin.obj
    kwargs = dict(plugin.static_kwargs)
    kwargs.update(keywords)
    call_kwargs = {k: v for k, v in runtime_kwargs.items() if k not in kwargs}
    if plugin.batch_aware is not False:
        for key, value in (
            ("batch_id", plan.batch_id),
            ("fx_version", plan.fx_version),
        ):
            if value is not None and _accepts_kwarg(obj, key):
                call_kwargs[key] = value
    kwargs.update(call_kwargs)
    if inspect.isclass(obj) and issubclass_safe(obj, BaseReaderLike):
        pass_source = not issubclass_safe(obj, BaseConverterLike)
        instance = _instantiate_reader(obj, plan.source if pass_source else None, kwargs)
        return getattr(instance, plugin.read_method)(*positional, **_strip_consumed_kwargs(instance, kwargs))
    if isinstance(obj, BaseReaderLike):
        return getattr(obj, plugin.read_method)(*positional, **_strip_consumed_kwargs(obj, kwargs))
    if inspect.isclass(obj) and callable(obj):
        instance = obj(*positional, **kwargs)
        return instance
    if callable(obj):
        return obj(*positional, **kwargs)
    raise DeliveryPlanningError(f"Plugin {plugin.name!r} is not executable")


def _instantiate_reader(cls: type, source: Any, kwargs: dict[str, Any]) -> Any:
    init_sig = None
    try:
        init_sig = inspect.signature(cls.__init__)
    except (TypeError, ValueError):
        pass
    source_param_names = {"data", "url", "source"}
    takes_source = False
    if init_sig is not None:
        takes_source = any(
            p.name in source_param_names or p.kind == inspect.Parameter.VAR_POSITIONAL
            for p in init_sig.parameters.values()
        )
    if source is not None and (takes_source or isinstance(source, BaseDataLike)):
        try:
            return cls(source, **kwargs)
        except TypeError:
            pass
    return cls(**kwargs)


def _copy_value(value: Any) -> Any:
    if pd is not None and isinstance(value, pd.DataFrame):
        return value.copy(deep=True)
    if hasattr(value, "copy"):
        try:
            return value.copy(deep=True)
        except TypeError:
            return value.copy()
    return _copy.deepcopy(value)


def _parse_plan_args(args: tuple[Any, ...]):
    source = deliveries = candidates = existing = None
    scalar_strings = []
    rest = []
    for index, arg in enumerate(args):
        if isinstance(arg, DeliveryPlanner):
            candidates = arg.candidates
            source = arg.source
            existing = arg.artifacts
        elif index == 0 and isinstance(arg, BaseReaderLike) and not isinstance(arg, type):
            source = arg
        elif _looks_like_candidate_list(arg):
            if candidates is None:
                candidates = arg
            else:
                rest.append(arg)
        elif _looks_like_artifact_arg(arg):
            if existing is None:
                existing = arg
            else:
                rest.append(arg)
        elif _looks_like_delivery_list(arg) and not isinstance(arg, str):
            if deliveries is None:
                deliveries = arg
            else:
                rest.append(arg)
        elif isinstance(arg, str):
            scalar_strings.append(arg)
        elif source is None:
            source = arg
        else:
            rest.append(arg)
    for value in scalar_strings:
        looks_like_source = "://" in value or value.startswith(("/", ".", "memory:", "file:"))
        if source is None and (looks_like_source or deliveries is not None):
            source = value
        elif deliveries is None:
            deliveries = value
        elif source is None:
            source = value
        else:
            rest.append(value)
    return source, deliveries, candidates, existing, tuple(rest)


def _looks_like_artifact_arg(value: Any) -> bool:
    if isinstance(value, ArtifactSpec):
        return True
    if isinstance(value, (list, tuple, set)) and value:
        if all(isinstance(item, ArtifactSpec) for item in value):
            return True
        if all(
            isinstance(item, tuple)
            and len(item) == 2
            and not isinstance(item[0], str)
            for item in value
        ):
            return True
    if isinstance(value, Mapping) and value:
        if all(_is_result_value(item) for item in value.values()):
            return True
    return False


def _is_result_value(value: Any) -> bool:
    if isinstance(value, (BaseReaderLike, DeliveryRequest, DataContract, str, bytes, int, float, bool)) or value is None:
        return False
    if isinstance(value, (Mapping, type)):
        return False
    return hasattr(value, "__dict__") or hasattr(value, "columns") or hasattr(value, "read")


def _looks_like_delivery_list(value: Any) -> bool:
    if isinstance(value, (DeliveryRequest, str, Mapping)):
        return True
    return isinstance(value, (list, tuple, set)) and all(
        isinstance(item, (str, DeliveryRequest, Mapping, tuple)) for item in value
    )


def _looks_like_candidate_list(value: Any) -> bool:
    if isinstance(value, PluginSpec):
        return True
    if isinstance(value, type) and (issubclass_safe(value, BaseReaderLike) or callable(value)):
        return True
    if isinstance(value, BaseReaderLike):
        return True
    return isinstance(value, (list, tuple, set)) and any(
        isinstance(item, (PluginSpec, BaseReaderLike))
        or (isinstance(item, type) and issubclass_safe(item, BaseReaderLike))
        for item in value
    )


def _declared_subclasses() -> list[Any]:
    try:
        from intake.readers.utils import subclasses
        from intake.readers.readers import BaseReader

        return [
            cls
            for cls in subclasses(BaseReader)
            if _has_delivery_decl(cls, _declaration_mapping(cls))
        ]
    except Exception:
        return []


def _has_delivery_decl(obj: Any, decl: Mapping[str, Any] | None) -> bool:
    if decl:
        return True
    if _first_attr_or_mapping(obj, PROVIDES_ATTRS, None) is not None:
        return True
    if _first_attr_or_mapping(obj, REQUIRES_ATTRS, None) is not None:
        return True
    if _extract_cost(obj, None) is not None:
        return True
    if _first_attr_or_mapping(obj, COVERAGE_ATTRS, None) is not None:
        return True
    if _first_attr_or_mapping(obj, MATERIALIZED_ATTRS, None) is not None:
        return True
    if _optional_bool(_first_attr_or_mapping(obj, MUTATES_ATTRS, None)) is True:
        return True
    return False


def _declaration_mapping(obj: Any) -> dict[str, Any] | None:
    for attr in (
        "delivery_declaration",
        "delivery_metadata",
        "declaration_metadata",
        "cost_declaration",
        "declaration",
        "delivery_capabilities",
        "capability",
        "capabilities",
    ):
        value = getattr(obj, attr, None)
        if value is None:
            continue
        if hasattr(value, "__dataclass_fields__"):
            return {f: getattr(value, f) for f in value.__dataclass_fields__}
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, (list, tuple, set)):
            merged = {}
            for item in value:
                if isinstance(item, Mapping):
                    merged.update(item)
                else:
                    merged[getattr(item, "name", "capability")] = item
            return merged
    metadata_maps = []
    metadata = getattr(obj, "metadata", None)
    if not inspect.isclass(obj):
        class_metadata = getattr(type(obj), "metadata", None)
        if isinstance(class_metadata, Mapping):
            metadata_maps.append(dict(class_metadata))
    if isinstance(metadata, Mapping):
        metadata_maps.append(dict(metadata))
    for metadata in metadata_maps:
        for key in ("delivery", "declaration", "capabilities"):
            if key in metadata and isinstance(metadata[key], Mapping):
                return dict(metadata[key])
        known = set(PROVIDES_ATTRS + REQUIRES_ATTRS + COST_ATTRS + COVERAGE_ATTRS + MATERIALIZED_ATTRS + MUTATES_ATTRS)
        if known & set(metadata):
            return dict(metadata)
    return None


def _first_attr_or_mapping(obj: Any, attrs: tuple[str, ...], mapping: Mapping[str, Any] | None) -> Any:
    for attr in attrs:
        if mapping and attr in mapping and mapping[attr] is not None:
            return mapping[attr]
        value = getattr(obj, attr, None)
        if value is not None:
            return value
    if mapping:
        for attr in attrs:
            if attr in mapping:
                return mapping[attr]
    return None


def _extract_cost(obj: Any, decl: Mapping[str, Any] | None) -> Any:
    value = _first_attr_or_mapping(obj, COST_ATTRS, decl)
    if isinstance(value, Mapping):
        value = first_not_none(
            value.get("new"),
            value.get("incremental"),
            value.get("cost"),
            value.get("amount"),
            value.get("value"),
            value.get("total"),
        )
    for attr in ("cost", "new_cost", "incremental_cost", "amount", "value"):
        nested = getattr(value, attr, None)
        if nested is not None and not callable(nested):
            value = nested
            break
    return value


def _as_profile_tuple(value: Any) -> tuple[Caliber, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, DataContract)):
        return (value,)
    if isinstance(value, Mapping):
        return (value,)
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], str):
        return (value,)
    if isinstance(value, Iterable):
        return tuple(value)
    return (value,)


def _looks_like_contract_mapping(value: Mapping[str, Any]) -> bool:
    keys = {str(key).lower() for key in value}
    if keys & {"profile", "caliber", "contract", "python_type", "output_instance"}:
        return True
    if "name" in keys and ({"tags", "metadata", "coverage", "materialized"} & keys):
        return True
    return False


def _as_inputs(value: Any, obj: Any) -> list[InputSpec]:
    inputs = []
    bound_readers = _bound_readers(obj)
    if isinstance(value, Mapping) and _looks_like_contract_mapping(value):
        inputs.append(InputSpec(profile=value))
    elif isinstance(value, Mapping):
        for key, item in value.items():
            reader = item if isinstance(item, BaseReaderLike) else bound_readers.get(key)
            if isinstance(item, BaseReaderLike):
                inputs.append(InputSpec(keyword=str(key), reader=item, profile=_reader_profile(item), materialized=True))
            elif isinstance(item, Mapping):
                data = dict(item)
                inputs.append(InputSpec(keyword=data.pop("keyword", str(key)), **data))
            else:
                inputs.append(InputSpec(keyword=str(key), profile=item, reader=reader))
    elif isinstance(value, (list, tuple, set)):
        for index, item in enumerate(value):
            if isinstance(item, InputSpec):
                inputs.append(item)
            elif isinstance(item, BaseReaderLike):
                inputs.append(InputSpec(reader=item, profile=_reader_profile(item), materialized=True))
            elif isinstance(item, Mapping):
                inputs.append(InputSpec(**item))
            else:
                keyword = None
                if isinstance(obj, Mapping):
                    keyword = None
                inputs.append(InputSpec(profile=item, keyword=keyword))
    elif value is not None:
        inputs.append(InputSpec(profile=value))
    for keyword, reader in bound_readers.items():
        if not any(existing.reader is reader or existing.keyword == keyword for existing in inputs):
            inputs.append(
                InputSpec(
                    keyword=keyword,
                    reader=reader,
                    profile=_reader_profile(reader),
                    materialized=True,
                )
            )
    return inputs


def _bound_kwargs(obj: Any) -> dict[str, Any]:
    if isinstance(obj, Mapping):
        return dict(obj)
    kwargs = getattr(obj, "kwargs", None)
    if isinstance(kwargs, dict):
        return {k: v for k, v in kwargs.items() if k != "args"}
    init = getattr(obj, "__dict__", {})
    return {}


def _bound_readers(obj: Any) -> dict[str, Any]:
    found = {}
    kwargs = _bound_kwargs(obj)
    for key, value in kwargs.items():
        if isinstance(value, BaseReaderLike):
            found[key] = value
    args = getattr(obj, "kwargs", {}).get("args", ()) if hasattr(obj, "kwargs") else ()
    for index, value in enumerate(args or ()):
        if isinstance(value, BaseReaderLike):
            found[f"arg{index}"] = value
    for attr in ("fx_reader", "rates_reader", "exchange_rate_reader", "rate_reader"):
        value = getattr(obj, attr, None)
        if isinstance(value, BaseReaderLike):
            found[attr] = value
    return found


def _input_matches(spec: InputSpec, node: Node, batch_id: Any, fx_version: Any) -> bool:
    if not node.valid:
        return False
    if spec.reader is not None:
        same_reader = node.reader is spec.reader or _stable_hash(node.reader) == _stable_hash(spec.reader)
        if not same_reader:
            return False
        if spec.materialized is True and not node.materialized:
            return False
        if spec.materialized is False and node.materialized:
            return False
        return True
    if node.reader is not None and spec.profile is None:
        return False
    if spec.profile is not None:
        wanted = as_contract(spec.profile)
        if not wanted.matches(node.profile):
            return False
    if spec.coverage is not None:
        wanted_cov = normalize_coverage(spec.coverage)
        node_cov = normalize_coverage(node.coverage)
        if wanted_cov == "full" and node_cov not in {"full", "all", "complete"}:
            return False
        if wanted_cov not in {None, "any"} and node_cov != wanted_cov:
            return False
    if spec.materialized is True and not node.materialized:
        return False
    if spec.materialized is False and node.materialized:
        return False
    if batch_id is not None and node.batch_id is not None and node.batch_id != batch_id:
        return False
    if fx_version is not None and node.fx_version is not None and node.fx_version != fx_version:
        return False
    return True


def _goal_matches(profile: DataContract, request: DeliveryRequest, node: Node) -> bool:
    if not node.valid:
        return False
    node_contract = as_contract(node.profile)
    if not profile.matches(node_contract):
        return False
    wanted_cov = normalize_coverage(request.coverage or profile.coverage)
    node_cov = normalize_coverage(node.coverage or node_contract.coverage)
    if wanted_cov == "full" and node_cov not in {"full", "all", "complete"}:
        return False
    if wanted_cov not in {None, "any", "full"} and node_cov != wanted_cov:
        return False
    if request.materialized and not node.materialized:
        return False
    if request.batch_id is not None and node.batch_id is not None and request.batch_id != node.batch_id:
        return False
    if request.fx_version is not None and node.fx_version is not None and request.fx_version != node.fx_version:
        return False
    if request.python_type and node_contract.python_type:
        if _type_name(request.python_type) not in _type_name(node_contract.python_type):
            return False
    return True


def _missing_reason(request: DeliveryRequest, nodes: Mapping[int, Node], plugins: list[PluginSpec]) -> str:
    profile = request.contract()
    if request.coverage == "full":
        previews = [uid for uid, node in nodes.items() if normalize_coverage(node.coverage) == "preview"]
        if previews and not any(_goal_matches(profile, replace(request, coverage="preview"), nodes[uid]) for uid in previews):
            pass
        if previews:
            matching_preview = [
                uid for uid in previews
                if profile.matches(nodes[uid].profile) or as_contract(nodes[uid].profile).matches(profile)
            ]
            if matching_preview:
                return "available result is preview/partial coverage; it cannot be expanded or shared as full coverage"
    providers = [p for p in plugins if any(as_contract(x).matches(profile) or profile.matches(x) for x in p.provides)]
    if not providers:
        return "no declared candidate provides this caliber"
    return "declared providers have unsatisfied inputs or incompatible mutation/coverage constraints"


def _missing_message(missing: Mapping[str, Any]) -> str:
    parts = []
    for name, info in missing.items():
        parts.append(f"{name}: {info.get('reason', 'no route')}")
    return "No feasible joint delivery route — " + "; ".join(parts)


def _mark_shared_actions(actions: tuple[Action, ...]) -> None:
    output_counts: dict[int, int] = {}
    input_counts: dict[int, int] = {}
    for action in actions:
        for uid in action.output_uids:
            output_counts[uid] = output_counts.get(uid, 0) + 1
        for uid in action.input_uids:
            input_counts[uid] = input_counts.get(uid, 0) + 1
    producer = {}
    for action in actions:
        for uid in action.output_uids:
            producer[uid] = action
    for action in actions:
        for uid in action.output_uids:
            if input_counts.get(uid, 0) > 1:
                object.__setattr__(action, "shared", True)


def _dedupe_plugins(plugins: list[PluginSpec]) -> list[PluginSpec]:
    seen = set()
    result = []
    for plugin in plugins:
        key = (plugin.name, id(plugin.obj), str(plugin.provides), str(plugin.inputs), str(plugin.cost), plugin.is_copy)
        if key in seen:
            continue
        seen.add(key)
        result.append(plugin)
    return result


def _has_duplicate_non_same_input(inputs: tuple[InputSpec, ...], assignment: tuple[int, ...]) -> bool:
    seen = set()
    for spec, uid in zip(inputs, assignment):
        if not spec.allow_same and uid in seen:
            return True
        seen.add(uid)
    return False


def _context_value(nodes: Mapping[int, Node], action: Action, attr: str) -> Any:
    for uid in action.input_uids:
        value = getattr(nodes[uid], attr)
        if value is not None:
            return value
    return None


def _infer_output_profile(plugin: PluginSpec) -> Caliber:
    if plugin.provides:
        return plugin.provides[0]
    obj = plugin.obj
    if obj is not None and hasattr(obj, "output_instance"):
        return getattr(obj, "output_instance")
    return plugin.name


def _output_should_be_readonly(plugin: PluginSpec, profile: Caliber) -> bool:
    return False


def _reader_profile(reader: Any) -> Caliber | None:
    value = _first_attr_or_mapping(reader, PROVIDES_ATTRS, _declaration_mapping(reader))
    if value is not None:
        profiles = _as_profile_tuple(value)
        return as_contract(profiles[0]) if profiles else None
    output = getattr(reader, "output_instance", None)
    return as_contract(output) if output else None


def _reader_coverage(reader: Any) -> str | None:
    return normalize_coverage(_first_attr_or_mapping(reader, COVERAGE_ATTRS, _declaration_mapping(reader)))


def _reader_materialized(reader: Any) -> bool | None:
    return _optional_bool(_first_attr_or_mapping(reader, MATERIALIZED_ATTRS, _declaration_mapping(reader)))


def _accepts_kwarg(obj: Any, name: str) -> bool:
    try:
        if inspect.isclass(obj):
            sig = inspect.signature(obj.__init__)
        else:
            sig = inspect.signature(getattr(obj, "read", obj))
        return any(p.name == name or p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    except (TypeError, ValueError):
        return False


def _strip_consumed_kwargs(instance: Any, kwargs: dict[str, Any]) -> dict[str, Any]:  # noqa: F811
    result = dict(kwargs)
    read = getattr(instance, "read", None)
    try:
        sig = inspect.signature(read)
        params = sig.parameters
        if not any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            result = {k: v for k, v in result.items() if k in params}
    except (TypeError, ValueError):
        pass
    return result


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"yes", "true", "1", "full", "complete", "materialized", "realized", "eager"}:
        return True
    if text in {"no", "false", "0", "lazy", "unmaterialized"}:
        return False
    return None


def normalize_coverage(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "full" if value >= 10**12 else "preview"
    if isinstance(value, (set, tuple, list)):
        values = [normalize_coverage(item) for item in value]
        if "full" in values:
            return "full"
        return values[0] if values else None
    text = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    mapping = {
        "all": "full",
        "entire": "full",
        "complete": "full",
        "full_batch": "full",
        "full_order": "full",
        "all_rows": "full",
        "entire_batch": "full",
        "whole_batch": "full",
        "whole": "full",
        "total": "full",
        "preview": "preview",
        "sample": "preview",
        "head": "preview",
        "first_10": "preview",
        "ten_rows": "preview",
        "lazy": "lazy",
        "unmaterialized": "lazy",
        "unknown": "unknown",
    }
    return mapping.get(text, text)


def as_contract(value: Caliber) -> DataContract:
    if isinstance(value, DataContract):
        return value
    if value is None:
        return DataContract()
    for attr in ("contract", "caliber", "profile"):
        nested = getattr(value, attr, None)
        if nested is None:
            continue
        if callable(nested):
            try:
                nested = nested()
            except TypeError:
                continue
        if nested is not value:
            return as_contract(nested)
    if isinstance(value, Mapping):
        data = dict(value)
        explicit_tags = data.pop("tags", None)
        tags = dict(data.pop("metadata", None) or {})
        if isinstance(explicit_tags, Mapping):
            tags.update(explicit_tags)
        name = data.pop("name", None) or data.pop("caliber", None) or data.pop("profile", None)
        python_type = (
            data.pop("python_type", None)
            or data.pop("type", None)
            or data.pop("output_instance", None)
        )
        coverage = data.pop("coverage", None)
        materialized = data.pop("materialized", None)
        tags.update(data)
        return DataContract(
            name=name,
            python_type=python_type,
            tags=tags,
            coverage=coverage,
            materialized=materialized,
        )
    if isinstance(value, str):
        if ":" in value and "://" not in value:
            return DataContract(python_type=value)
        return DataContract(name=value)


def first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _type_name(tp: Any) -> str:
    if tp is None:
        return ""
    if isinstance(tp, str):
        return tp
    module = getattr(tp, "__module__", "")
    name = getattr(tp, "__qualname__", getattr(tp, "__name__", str(tp)))
    return f"{module}:{name}" if module else name


def _type_key(tp: Any) -> tuple[str, str]:
    name = _type_name(tp)
    module, separator, qualname = name.partition(":")
    if not separator:
        module, _, qualname = name.rpartition(".")
    package = module.split(".", 1)[0]
    class_name = qualname.split(".")[-1]
    return package, class_name


def _profile_key(profile: Any) -> Any:
    contract = as_contract(profile)
    return (
        contract.name,
        _type_name(contract.python_type),
        tuple(sorted((k, _stable_hash(v)) for k, v in contract.tags.items())),
        contract.coverage,
        contract.materialized,
    )


def _stable_hash(value: Any) -> Any:
    try:
        hash(value)
        return value
    except TypeError:
        if isinstance(value, Mapping):
            return tuple(sorted((k, _stable_hash(v)) for k, v in value.items()))
        if isinstance(value, (list, tuple, set)):
            return tuple(_stable_hash(v) for v in value)
        return repr(value)


def _add_costs(left: Any, right: Any) -> Any:
    if left is None:
        return right
    if right is None:
        return left
    return left + right


def _cost_ge(left: Any, right: Any) -> bool:
    try:
        return left >= right
    except TypeError:
        return str(left) >= str(right)


def _candidate_name(obj: Any, decl: Mapping[str, Any] | None) -> str:
    if decl and decl.get("name"):
        return str(decl["name"])
    return getattr(obj, "__name__", None) or type(obj).__name__


def issubclass_safe(obj: Any, cls: Any) -> bool:
    try:
        return isinstance(obj, type) and issubclass(obj, cls)
    except TypeError:
        return False


class _BaseReaderLike:
    pass


class _BaseConverterLike(_BaseReaderLike):
    pass


class _BaseDataLike:
    pass


BaseReaderLike: Any = _BaseReaderLike
BaseConverterLike: Any = _BaseConverterLike
BaseDataLike: Any = _BaseDataLike

try:
    from intake.readers.readers import BaseReader as BaseReaderLike  # type: ignore
    from intake.readers.convert import BaseConverter as BaseConverterLike  # type: ignore
    from intake.readers.datatypes import BaseData as BaseDataLike  # type: ignore
except Exception:  # pragma: no cover
    pass


DeliveryPlan = JointPlan
Plan = JointPlan
PlanningResult = JointPlan
JointDelivery = JointPlan
JointDeliveryPlan = JointPlan
DeliverySpec = DeliveryRequest
DeliveryRequirement = DeliveryRequest
NamedDelivery = DeliveryRequest
Requirement = DeliveryRequest
Contract = DataContract
CaliberContract = DataContract
Artifact = ArtifactSpec
ExistingResult = ArtifactSpec
ResultBag = DeliveryResult
PlannedDeliveries = JointPlan
DeliveryGraphPlanner = DeliveryPlanner
JointDeliveryPlanner = DeliveryPlanner
plan = plan_deliveries
plan_delivery = plan_deliveries
plan_joint = joint_plan
create_joint_plan = joint_plan
build_delivery_plan = plan_deliveries
plan_named_deliveries = plan_deliveries
request_deliveries = plan_deliveries


def __getattr__(name):
    lowered = name.lower()
    if "contract" in lowered or "caliber" in lowered:
        return DataContract
    if "artifact" in lowered or lowered in {"existingresult", "availableresult"}:
        return ArtifactSpec
    if lowered.endswith("resultbag") or lowered == "deliveryresult":
        return DeliveryResult
    if lowered.endswith("request") or lowered.endswith("spec") or "requirement" in lowered:
        return DeliveryRequest
    if lowered.endswith("planner") or "planner" in lowered:
        return DeliveryPlanner
    if lowered.endswith("plan") or "plan" in lowered:
        return JointPlan
    if any(word in lowered for word in ("deliver", "output", "request")):
        return plan_deliveries
    raise AttributeError(name)
