# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""Batch collection of CloudWatch ``GetMetricData`` queries.

Several collection lines can share one CloudWatch client and fetch the same
time window on a schedule. CloudWatch caps how many
:class:`~botocore.client.CloudWatch.Client.get_metric_data` query items a
single request may carry. This module turns a whole manifest of metrics and
metric-math formulas into the smallest possible number of requests before
anything is sent.

The manifest is a list of items. Every item is addressed by a group name and a
name that is unique inside that group. A metric item carries a ``metric_stat``
and a formula item carries an ``expression``. Formulas may only reference
metric names, previously declared formula names, numbers and the four basic
arithmetic operators (``+``, ``-``, ``*``, ``/``). A reference to another
group must name the source explicitly with ``group:name``; a bare name only
resolves inside the current group. Display labels are allowed to repeat.

A minimal manifest looks like::

    manifest = [
        {
            "group": "shared",
            "name": "x",
            "metric_stat": {
                "Metric": {
                    "Namespace": "ns",
                    "MetricName": "x",
                    "Dimensions": [],
                },
                "Period": 60,
                "Stat": "Average",
                "Unit": "None",
            },
            "account_id": "111122223333",
        },
        {
            "group": "shared",
            "name": "r",
            "expression": "x * 2",
            "label": "rate",
            "deliver": True,
        },
        {
            "group": "lineA",
            "name": "A",
            "expression": "shared:r + shared:x",
            "label": "rate",
            "deliver": True,
        },
    ]

``metric_stat`` is the native CloudWatch ``MetricStat`` structure (the same
shape accepted by
:class:`~botocore.client.CloudWatch.Client.get_metric_data`), so planned
queries can be sent unchanged. Call
:meth:`CloudWatchMetricBatcher.build_plan` to obtain a :class:`BatchPlan`
describing every request, every query item and the two counts (request count
and total query items actually sent, including support items repeated across
requests). The plan can be inspected or persisted before
:meth:`CloudWatchMetricBatcher.execute_plan` sends it.
"""

import copy
import re
from dataclasses import dataclass, field
from typing import Any

_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)?")
_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?|\.\d+")
_OPERATORS = {"+", "-", "*", "/", "(", ")"}
# Exact set-partition enumeration is Bell-number exponential. Manifests up to
# this many delivered items are solved optimally by enumeration; larger ones
# use a deterministic best-fit heuristic instead.
_EXACT_PARTITION_LIMIT = 10


class CloudWatchBatchingError(ValueError):
    """Raised when a manifest cannot be turned into a valid batch plan.

    The :attr:`issue` carries a locator (group and/or name, plus the offending
    token where relevant) so the problem can be found in the manifest without
    sending a request.
    """

    def __init__(self, issue):
        self.issue = issue
        super().__init__(str(issue))


@dataclass(frozen=True)
class ItemRef:
    """The group/name address of one manifest item."""

    group: str
    name: str

    def __str__(self):
        return f"{self.group}:{self.name}"


@dataclass(frozen=True)
class PlanningIssue:
    """A locatable reason why a manifest could not be planned."""

    kind: str
    message: str
    group: str | None = None
    name: str | None = None
    token: str | None = None

    def __str__(self):
        location = []
        if self.group is not None:
            location.append(f"group={self.group!r}")
        if self.name is not None:
            location.append(f"name={self.name!r}")
        prefix = f"[{', '.join(location)}] " if location else ""
        token = f" (token={self.token!r})" if self.token is not None else ""
        return f"{self.kind}: {prefix}{self.message}{token}"


@dataclass
class PlannedQuery:
    """A single ``MetricDataQueries`` entry inside a planned request.

    Exactly one of ``metric_stat`` or ``expression`` is set. ``ref`` points
    back at the manifest item this query realizes, which makes support items
    auditable. ``return_data`` mirrors the wire ``ReturnData`` flag.
    """

    id: str
    return_data: bool
    ref: ItemRef | None = None
    label: str | None = None
    metric_stat: dict[str, Any] | None = None
    expression: str | None = None
    account_id: str | None = None

    @property
    def kind(self):
        return "formula" if self.expression is not None else "metric"

    def to_client_query(self):
        """Return the plain dict expected by the CloudWatch client."""
        query: dict[str, Any] = {"Id": self.id, "ReturnData": self.return_data}
        if self.label is not None:
            query["Label"] = self.label
        if self.account_id is not None:
            query["AccountId"] = self.account_id
        if self.expression is not None:
            query["Expression"] = self.expression
        else:
            query["MetricStat"] = copy.deepcopy(self.metric_stat)
        return query


@dataclass
class PlannedRequest:
    """One ``GetMetricData`` request and the query items it will carry."""

    index: int
    queries: list[PlannedQuery] = field(default_factory=list)
    delivered_refs: list[ItemRef] = field(default_factory=list)

    @property
    def size(self):
        return len(self.queries)

    @property
    def query_ids(self):
        return [query.id for query in self.queries]

    def to_client_kwargs(self, start_time, end_time, **extra_kwargs):
        """Build the client keyword arguments for this request."""
        kwargs = dict(extra_kwargs)
        kwargs["MetricDataQueries"] = [
            query.to_client_query() for query in self.queries
        ]
        kwargs["StartTime"] = start_time
        kwargs["EndTime"] = end_time
        return kwargs


@dataclass
class BatchPlan:
    """The complete, sendable plan for one time window.

    ``request_count`` is the number of requests and ``total_query_count`` is
    the number of query items actually transmitted across all of them,
    counting support items that have to be repeated in more than one request.
    """

    requests: list[PlannedRequest]
    start_time: Any
    end_time: Any
    extra_kwargs: dict[str, Any]
    max_queries_per_request: int
    # Where each delivery item is returned from.
    delivery: dict[ItemRef, tuple[int, str]]
    # The local query id assigned to every manifest formula/metric item.
    query_ids: dict[ItemRef, list[str]]

    @property
    def request_count(self):
        return len(self.requests)

    @property
    def total_query_count(self):
        return sum(request.size for request in self.requests)

    def to_client_kwargs(self):
        """Yield the client keyword arguments for every request, in order."""
        return [
            request.to_client_kwargs(
                self.start_time, self.end_time, **self.extra_kwargs
            )
            for request in self.requests
        ]

    def request_delivering(self, ref):
        """Return the zero-based index of the request that returns ``ref``."""
        return self.delivery[ref][0]

    def query_id_delivering(self, ref):
        """Return the query id that carries the returned data for ``ref``."""
        return self.delivery[ref][1]


@dataclass
class DeliveredResult:
    """A delivery item matched back to its line and originating request."""

    ref: ItemRef
    label: str | None
    request_index: int
    query_id: str
    result: dict[str, Any]

    @property
    def timestamps(self):
        return self.result.get("Timestamps", [])

    @property
    def values(self):
        return self.result.get("Values", [])

    @property
    def status_code(self):
        return self.result.get("StatusCode")


@dataclass
class BatchExecution:
    """The outcome of sending a :class:`BatchPlan`."""

    plan: BatchPlan
    results: dict[ItemRef, DeliveredResult]
    raw_responses: list[dict[str, Any]]

    def result_for(self, group, name):
        """Return the :class:`DeliveredResult` for one manifest item."""
        return self.results[ItemRef(group, name)]

    def line_results(self, lines):
        """Group delivered results by collection line.

        ``lines`` maps each group name to a list of delivered item names.
        Results are returned in that order, so repeated labels stay separate.
        """
        return {
            group: [self.results[ItemRef(group, name)] for name in names]
            for group, names in lines.items()
        }


@dataclass
class _MetricNode:
    ref: ItemRef
    label: str | None
    metric_stat: dict[str, Any]
    account_id: str | None
    deliver: bool
    signature: Any


@dataclass
class _FormulaNode:
    ref: ItemRef
    label: str | None
    expression: str
    deliver: bool
    refs: list[ItemRef] = field(default_factory=list)


class CloudWatchMetricBatcher:
    """Plan and execute minimal batches of ``GetMetricData`` requests.

    :param client: A CloudWatch client. It is only needed for
        :meth:`execute_plan`; planning works without one.
    :param max_queries_per_request: The gateway cap on query items per
        request. Defaults to the service limit of 500.
    """

    def __init__(self, client=None, max_queries_per_request=500):
        self._client = client
        self._max_queries = max_queries_per_request

    def build_plan(
        self,
        manifest,
        start_time=None,
        end_time=None,
        max_queries_per_request=None,
        **extra_kwargs,
    ):
        """Build the minimal :class:`BatchPlan` for ``manifest``.

        No request is sent. The returned plan exposes the full request layout
        and both counts so the scheduler can arrange the round ahead of time.
        """
        max_queries = (
            max_queries_per_request
            if max_queries_per_request is not None
            else self._max_queries
        )
        if max_queries is None or max_queries < 1:
            raise CloudWatchBatchingError(
                PlanningIssue(
                    "InvalidLimit",
                    "max_queries_per_request must be a positive integer",
                )
            )
        metrics, formulas, order = self._parse_manifest(manifest)
        self._validate_graph(metrics, formulas)
        # Every downstream decision is keyed on the canonical (group, name)
        # order so the produced plan is independent of manifest ordering.
        order = sorted(order, key=lambda ref: (ref.group, ref.name))
        closures, aliases = self._build_closures(metrics, formulas, order)
        delivery_refs = [
            ref for ref in order if self._node_deliver(metrics, formulas, ref)
        ]
        oversized = self._find_oversized(closures, delivery_refs, max_queries)
        if oversized:
            ref, size = oversized
            raise CloudWatchBatchingError(
                PlanningIssue(
                    "QueryLimitExceeded",
                    (
                        f"item needs {size} query items on its own but the "
                        f"limit is {max_queries}"
                    ),
                    group=ref.group,
                    name=ref.name,
                )
            )
        groups = self._plan_groups(delivery_refs, closures, max_queries, order)
        return self._materialize(
            groups,
            metrics,
            formulas,
            closures,
            aliases,
            order,
            start_time,
            end_time,
            extra_kwargs,
            max_queries,
        )

    def execute_plan(self, plan, client=None):
        """Send every request in ``plan`` and reconcile the responses.

        Response packets may arrive in any order: each result is matched by
        its query id and routed back to the manifest item that requested it.
        Only items whose query was sent with ``ReturnData=True`` appear in the
        returned results.
        """
        client = client if client is not None else self._client
        if client is None:
            raise ValueError(
                "A CloudWatch client is required to execute a plan"
            )
        raw_responses = []
        result_by_id: dict[str, dict[str, Any]] = {}
        for request in plan.requests:
            response = client.get_metric_data(
                **request.to_client_kwargs(
                    plan.start_time, plan.end_time, **plan.extra_kwargs
                )
            )
            raw_responses.append(response)
            for result in response.get("MetricDataResults", []):
                result_by_id[result["Id"]] = result

        delivered: dict[ItemRef, DeliveredResult] = {}
        for ref, (request_index, query_id) in plan.delivery.items():
            if query_id not in result_by_id:
                raise CloudWatchBatchingError(
                    PlanningIssue(
                        "MissingResult",
                        (
                            f"no result returned for delivered item "
                            f"(query id={query_id!r})"
                        ),
                        group=ref.group,
                        name=ref.name,
                    )
                )
            delivered[ref] = DeliveredResult(
                ref=ref,
                label=self._label_for(plan, ref),
                request_index=request_index,
                query_id=query_id,
                result=result_by_id[query_id],
            )
        return BatchExecution(
            plan=plan, results=delivered, raw_responses=raw_responses
        )

    def collect(
        self,
        manifest,
        start_time,
        end_time,
        client=None,
        max_queries_per_request=None,
        **extra_kwargs,
    ):
        """Convenience helper: build a plan and immediately execute it."""
        plan = self.build_plan(
            manifest,
            start_time=start_time,
            end_time=end_time,
            max_queries_per_request=max_queries_per_request,
            **extra_kwargs,
        )
        return self.execute_plan(plan, client=client)

    @staticmethod
    def _label_for(plan, ref):
        for request in plan.requests:
            for query in request.queries:
                if query.ref == ref and query.id == plan.delivery[ref][1]:
                    return query.label
        return None

    # -- manifest parsing -------------------------------------------------

    def _parse_manifest(self, manifest):
        metrics: dict[ItemRef, _MetricNode] = {}
        formulas: dict[ItemRef, _FormulaNode] = {}
        order: list[ItemRef] = []
        seen_groups: dict[str, dict[str, ItemRef]] = {}
        for position, item in enumerate(manifest):
            issue = self._validate_item_shape(item, position)
            if issue is not None:
                raise CloudWatchBatchingError(issue)
            group = item["group"]
            name = item["name"]
            ref = ItemRef(group, name)
            if group not in seen_groups:
                seen_groups[group] = {}
            if name in seen_groups[group]:
                raise CloudWatchBatchingError(
                    PlanningIssue(
                        "DuplicateItem",
                        "another item already uses this name in the group",
                        group=group,
                        name=name,
                    )
                )
            seen_groups[group][name] = ref
            order.append(ref)
            label = item.get("label")
            deliver = bool(item.get("deliver", False))
            if "metric_stat" in item:
                metric_stat = copy.deepcopy(item["metric_stat"])
                signature = self._metric_signature(
                    metric_stat, item.get("account_id")
                )
                metrics[ref] = _MetricNode(
                    ref=ref,
                    label=label,
                    metric_stat=metric_stat,
                    account_id=item.get("account_id"),
                    deliver=deliver,
                    signature=signature,
                )
            else:
                formulas[ref] = _FormulaNode(
                    ref=ref,
                    label=label,
                    expression=item["expression"],
                    deliver=deliver,
                )
        self._resolve_references(manifest, seen_groups, formulas)
        return metrics, formulas, order

    @staticmethod
    def _validate_item_shape(item, position):
        if not isinstance(item, dict):
            return PlanningIssue(
                "InvalidManifest",
                f"manifest entry at index {position} is not an object",
            )
        group = item.get("group")
        name = item.get("name")
        if not isinstance(group, str) or not group:
            return PlanningIssue(
                "InvalidManifest",
                f"entry at index {position} needs a non-empty 'group'",
                group=group if isinstance(group, str) else None,
                name=name if isinstance(name, str) else None,
            )
        if not isinstance(name, str) or not name:
            return PlanningIssue(
                "InvalidManifest",
                f"entry at index {position} needs a non-empty 'name'",
                group=group,
                name=name if isinstance(name, str) else None,
            )
        has_metric = "metric_stat" in item
        has_expression = "expression" in item
        if has_metric == has_expression:
            return PlanningIssue(
                "InvalidManifest",
                "entry must set exactly one of 'metric_stat' or 'expression'",
                group=group,
                name=name,
            )
        if has_metric and not isinstance(item["metric_stat"], dict):
            return PlanningIssue(
                "InvalidManifest",
                "'metric_stat' must be an object",
                group=group,
                name=name,
            )
        if has_expression and not isinstance(item["expression"], str):
            return PlanningIssue(
                "InvalidManifest",
                "'expression' must be a string",
                group=group,
                name=name,
            )
        return None

    def _resolve_references(self, manifest, seen_groups, formulas):
        # Map every manifest item (metric or formula) by its address.
        known: dict[ItemRef, ItemRef] = {
            ref: ref
            for group in seen_groups.values()
            for ref in group.values()
        }
        for item in manifest:
            if "expression" not in item:
                continue
            ref = ItemRef(item["group"], item["name"])
            node = formulas[ref]
            node.refs = self._parse_expression(
                node.expression, ref, item["group"], known, seen_groups
            )

    def _parse_expression(self, expression, ref, group, known, seen_groups):
        refs: list[ItemRef] = []
        position = 0
        stripped = expression.strip()
        if not stripped:
            raise CloudWatchBatchingError(
                PlanningIssue(
                    "InvalidExpression",
                    "expression is empty",
                    group=group,
                    name=ref.name,
                )
            )
        while position < len(expression):
            char = expression[position]
            if char.isspace():
                position += 1
                continue
            number = _NUMBER_RE.match(expression, position)
            if number:
                position = number.end()
                continue
            token = _TOKEN_RE.match(expression, position)
            if token:
                text = token.group(0)
                target = self._resolve_token(
                    text, ref, group, known, seen_groups
                )
                refs.append(target)
                position = token.end()
                continue
            if char in _OPERATORS:
                position += 1
                continue
            raise CloudWatchBatchingError(
                PlanningIssue(
                    "InvalidExpression",
                    "expression contains an unsupported character or token",
                    group=group,
                    name=ref.name,
                    token=char,
                )
            )
        if not refs:
            raise CloudWatchBatchingError(
                PlanningIssue(
                    "InvalidExpression",
                    "expression does not reference any metric or formula",
                    group=group,
                    name=ref.name,
                )
            )
        return refs

    def _resolve_token(self, text, ref, group, known, seen_groups):
        if ":" in text:
            source_group, source_name = text.split(":", 1)
            target = ItemRef(source_group, source_name)
            if target not in known:
                raise CloudWatchBatchingError(
                    PlanningIssue(
                        "UnknownReference",
                        "cross-group reference does not point to a declared item",
                        group=group,
                        name=ref.name,
                        token=text,
                    )
                )
            return target
        # A bare name is resolved against the current group only. Cross-group
        # references must name their source explicitly; using a bare name that
        # is only declared elsewhere is reported, and a bare name matching
        # several groups is ambiguous.
        local = ItemRef(group, text)
        if local in known:
            return local
        foreign = [
            other_group
            for other_group in seen_groups
            if other_group != group and ItemRef(other_group, text) in known
        ]
        if len(foreign) > 1:
            raise CloudWatchBatchingError(
                PlanningIssue(
                    "AmbiguousReference",
                    (
                        "name is declared in more than one group; qualify it "
                        f"with 'group:name' (candidates: {sorted(foreign)})"
                    ),
                    group=group,
                    name=ref.name,
                    token=text,
                )
            )
        if foreign:
            raise CloudWatchBatchingError(
                PlanningIssue(
                    "UnknownReference",
                    (
                        "name is not declared in this group; qualify the "
                        "cross-group reference as 'group:name'"
                    ),
                    group=group,
                    name=ref.name,
                    token=text,
                )
            )
        raise CloudWatchBatchingError(
            PlanningIssue(
                "UnknownReference",
                "reference does not match any declared item",
                group=group,
                name=ref.name,
                token=text,
            )
        )

    # -- graph validation -------------------------------------------------

    def _validate_graph(self, metrics, formulas):
        # A formula may only depend on declared metrics or formulas, and may
        # not reference itself (directly or through a cycle).
        visiting: list[ItemRef] = []
        visited: set = set()

        def visit(node_ref):
            if node_ref in visiting:
                cycle = visiting[visiting.index(node_ref) :] + [node_ref]
                formatted = " -> ".join(str(item) for item in cycle)
                raise CloudWatchBatchingError(
                    PlanningIssue(
                        "CyclicReference",
                        f"dependency cycle detected: {formatted}",
                        group=node_ref.group,
                        name=node_ref.name,
                    )
                )
            if node_ref in visited:
                return
            if node_ref in metrics:
                visited.add(node_ref)
                return
            visiting.append(node_ref)
            for dependency in formulas[node_ref].refs:
                if dependency not in metrics and dependency not in formulas:
                    raise CloudWatchBatchingError(
                        PlanningIssue(
                            "UnknownReference",
                            "formula references an item that is not declared",
                            group=node_ref.group,
                            name=node_ref.name,
                            token=str(dependency),
                        )
                    )
                visit(dependency)
            visiting.pop()
            visited.add(node_ref)

        for ref in formulas:
            visit(ref)

    @staticmethod
    def _metric_signature(metric_stat, account_id):
        # ``metric_stat`` uses the native CloudWatch ``MetricStat`` shape, so
        # the same dict can be sent to the client unchanged.
        metric = metric_stat.get("Metric", {})
        dimensions = metric.get("Dimensions") or []
        dimension_key = tuple(
            sorted(
                (dimension.get("Name"), dimension.get("Value"))
                for dimension in dimensions
            )
        )
        return (
            account_id,
            metric.get("Namespace"),
            metric.get("MetricName"),
            dimension_key,
            metric_stat.get("Stat"),
            metric_stat.get("Unit"),
            metric_stat.get("Period"),
        )

    # -- closure computation and sharing ----------------------------------

    def _build_closures(self, metrics, formulas, order):
        signatures: dict[Any, ItemRef] = {}
        aliases: dict[ItemRef, ItemRef] = {}
        for ref in order:
            node = metrics.get(ref)
            if node is None:
                continue
            if node.signature in signatures:
                aliases[ref] = signatures[node.signature]
            else:
                signatures[node.signature] = ref

        closures: dict[ItemRef, frozenset[ItemRef]] = {}

        def closure_of(ref):
            if ref in closures:
                return closures[ref]
            canonical = aliases.get(ref, ref)
            if canonical != ref:
                resolved = closure_of(canonical)
                closures[ref] = resolved
                return resolved
            if ref in metrics:
                closures[ref] = frozenset({ref})
                return closures[ref]
            collected = {ref}
            for dependency in formulas[ref].refs:
                collected.update(closure_of(dependency))
            closures[ref] = frozenset(collected)
            return closures[ref]

        for ref in order:
            closure_of(ref)
        return closures, aliases

    @staticmethod
    def _find_oversized(closures, delivery_refs, max_queries):
        # A bin containing one delivered item holds exactly its closure.
        for ref in delivery_refs:
            size = len(closures[ref])
            if size > max_queries:
                return ref, size
        return None

    # -- bin packing ------------------------------------------------------

    def _plan_groups(self, delivery_refs, closures, max_queries, order):
        rank = {ref: index for index, ref in enumerate(order)}
        items = sorted(delivery_refs, key=lambda ref: rank[ref])

        def union_size(group):
            combined: set = set()
            for ref in group:
                combined.update(closures[ref])
            return len(combined)

        def feasible(partition):
            return all(union_size(group) <= max_queries for group in partition)

        def score(partition):
            return (
                len(partition),
                sum(union_size(group) for group in partition),
            )

        if len(items) <= _EXACT_PARTITION_LIMIT:
            partition = self._plan_exact(items, feasible, score)
        else:
            partition = self._plan_best_fit(
                items, closures, union_size, max_queries
            )
        # Keep request order and within-request order manifest-deterministic.
        partition = [
            sorted(group, key=lambda ref: rank[ref]) for group in partition
        ]
        partition.sort(key=lambda group: rank[group[0]])
        return partition

    @staticmethod
    def _plan_exact(items, feasible, score):
        best = None
        for partition in _iter_partitions(items):
            if not feasible(partition):
                continue
            candidate = score(partition)
            if best is None or candidate < best[0]:
                best = (candidate, partition)
        if best is None:
            # Every individual closure already fits, so a singleton partition
            # is always feasible; this is a defensive guard only.
            raise CloudWatchBatchingError(
                PlanningIssue(
                    "QueryLimitExceeded",
                    "no feasible request grouping exists for the manifest",
                )
            )
        return best[1]

    @staticmethod
    def _plan_best_fit(items, closures, union_size, max_queries):
        # Greedy first-fit-decreasing on closure size: place each delivered
        # item into the feasible bin that grows the least, opening a new bin
        # only when none can take it. Deterministic and order-independent.
        ordered = sorted(
            items,
            key=lambda ref: (-len(closures[ref]), ref.group, ref.name),
        )
        bins: list[list[Any]] = []

        def fits(bin_group, ref):
            return union_size(bin_group + [ref]) <= max_queries

        for ref in ordered:
            target = None
            best_growth = None
            for index, bin_group in enumerate(bins):
                if not fits(bin_group, ref):
                    continue
                growth = union_size(bin_group + [ref]) - union_size(bin_group)
                key = (growth, index)
                if best_growth is None or key < best_growth:
                    best_growth = key
                    target = index
            if target is None:
                bins.append([ref])
            else:
                bins[target].append(ref)
        return bins

    # -- plan materialization ---------------------------------------------

    def _materialize(
        self,
        groups,
        metrics,
        formulas,
        closures,
        aliases,
        order,
        start_time,
        end_time,
        extra_kwargs,
        max_queries,
    ):
        rank = {ref: index for index, ref in enumerate(order)}
        planned_requests: list[PlannedRequest] = []
        delivery: dict[ItemRef, tuple[int, str]] = {}
        query_ids: dict[ItemRef, list[str]] = {}
        # A delivered support item is returned from the first request that
        # contains it; later copies are marked ReturnData=False.
        returned: set = set()

        for request_index, group in enumerate(groups):
            included: set = set()
            for ref in group:
                included.update(closures[ref])
            ordered = sorted(included, key=lambda item: rank[item])
            local_ids = {
                ref: f"q{request_index}_{local_index}"
                for local_index, ref in enumerate(ordered)
            }
            request = PlannedRequest(index=request_index)
            for ref in ordered:
                query_id = local_ids[ref]
                query_ids.setdefault(ref, []).append(query_id)
                is_delivery = ref in group or (
                    self._node_deliver(metrics, formulas, ref)
                    and ref not in returned
                )
                return_data = is_delivery and ref not in returned
                query = self._build_query(
                    ref,
                    query_id,
                    return_data,
                    metrics,
                    formulas,
                    local_ids,
                    aliases,
                )
                request.queries.append(query)
                if return_data:
                    delivery[ref] = (request_index, query_id)
                    returned.add(ref)
                    request.delivered_refs.append(ref)
            planned_requests.append(request)

        # Every requested delivery item must be returned exactly once.
        for ref in order:
            if (
                self._node_deliver(metrics, formulas, ref)
                and ref not in delivery
            ):
                raise CloudWatchBatchingError(
                    PlanningIssue(
                        "InternalPlanningError",
                        "delivered item was not assigned to any request",
                        group=ref.group,
                        name=ref.name,
                    )
                )

        return BatchPlan(
            requests=planned_requests,
            start_time=start_time,
            end_time=end_time,
            extra_kwargs=dict(extra_kwargs),
            max_queries_per_request=max_queries,
            delivery=delivery,
            query_ids=query_ids,
        )

    @staticmethod
    def _node_deliver(metrics, formulas, ref):
        node = metrics.get(ref)
        if node is not None:
            return node.deliver
        return formulas[ref].deliver

    def _build_query(
        self,
        ref,
        query_id,
        return_data,
        metrics,
        formulas,
        local_ids,
        aliases,
    ):
        if ref in metrics:
            node = metrics[ref]
            return PlannedQuery(
                id=query_id,
                return_data=return_data,
                ref=ref,
                label=node.label,
                metric_stat=node.metric_stat,
                account_id=node.account_id,
            )
        node = formulas[ref]
        local_id = {
            dependency: local_ids[aliases.get(dependency, dependency)]
            for dependency in node.refs
        }
        expression = _rewrite_expression(node.expression, local_id, ref)
        return PlannedQuery(
            id=query_id,
            return_data=return_data,
            ref=ref,
            label=node.label,
            expression=expression,
        )


def _rewrite_expression(expression, local_id, ref):
    """Replace manifest references in an expression with local query ids."""

    def replace(match):
        text = match.group(0)
        if ":" in text:
            target = ItemRef(*text.split(":", 1))
        else:
            target = ItemRef(ref.group, text)
        if target not in local_id:
            raise CloudWatchBatchingError(
                PlanningIssue(
                    "InternalPlanningError",
                    "dependency is missing from the request",
                    group=ref.group,
                    name=ref.name,
                    token=text,
                )
            )
        return local_id[target]

    return _TOKEN_RE.sub(replace, expression)


def _iter_partitions(sequence):
    """Yield every set partition of ``sequence`` as lists of lists."""
    sequence = list(sequence)
    if not sequence:
        yield []
        return
    head = sequence[0]
    for partition in _iter_partitions(sequence[1:]):
        # Add the head to one of the existing blocks.
        for index in range(len(partition)):
            candidate = [block[:] for block in partition]
            candidate[index].append(head)
            yield candidate
        # Or start a new block with it.
        yield partition + [[head]]
