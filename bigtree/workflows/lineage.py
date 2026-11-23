"""Data lineage version reconciliation for close-of-book reporting.

This workflow models the lineage DAG of the two trading-daily reports that are
derived from the same dimension table. Every persisted node version records

* when it finished landing (``completed_at``), and
* which versions it actually read from every direct upstream (``inputs``).

At close of book, :class:`LineageReconciler` produces a single, checkable
version manifest: one node version per lineage node, shared by both reports,
with every transitive dependency consistent. Reports are visited in their
agreed priority order and each report tries its candidates from newest to
oldest, backtracking to older candidates whenever the newest pick cannot be
aligned with the other report.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from bigtree.node.dagnode import DAGNode
from bigtree.utils import exceptions

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "LineageConflictError",
    "NodeVersion",
    "LineageReconciler",
    "reconcile_lineage",
]


class LineageConflictError(exceptions.TreeError):
    """Raised when no shared version can satisfy all reports for a node.

    Attributes:
        node_name: lineage node whose versions could not be aligned
        report_versions: mapping of report name to the version it demanded
    """

    def __init__(
        self,
        node_name: str,
        report_versions: Mapping[str, Any],
        message: str | None = None,
    ) -> None:
        self.node_name = node_name
        self.report_versions = dict(report_versions)
        if message is None:
            demands = "; ".join(
                f"{report} requires {node_name} {version}"
                for report, version in self.report_versions.items()
            )
            message = f"Cannot align versions for {node_name}: {demands}"
        super().__init__(message)


@dataclass
class NodeVersion:
    """A single landed version of a lineage node.

    Args:
        node_name: lineage node the version belongs to
        version: version identifier, newer identifier ranks higher
        completed_at: finish timestamp of the landed version, used as the
            tie-breaker when two versions share an identifier
        inputs: direct upstream versions actually read, mapping each upstream
            node name to the version that was read. A missing entry means the
            record did not capture that direct input number and the version
            cannot be verified at close of book
        reports: reports whose lineage can reach this version
        attributes: extra metadata to carry onto the manifest node
    """

    node_name: str
    version: Any
    completed_at: Any
    inputs: Mapping[str, Any] = field(default_factory=dict)
    reports: Sequence[str] = field(default_factory=tuple)
    attributes: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.inputs = {
            upstream: version
            for upstream, version in dict(self.inputs).items()
            if version is not None
        }
        self.reports = tuple(self.reports)
        self.attributes = dict(self.attributes)

    def reads(self, upstream: str) -> Any | None:
        """Return the recorded input version for ``upstream``, if captured."""
        return self.inputs.get(upstream)


class LineageReconciler:
    """Reconcile landed node versions into one close-of-book manifest.

    Examples:
        >>> from bigtree.workflows.lineage import LineageReconciler, NodeVersion
        >>> dim_v1 = NodeVersion("dim", "v1", 1)
        >>> dim_v2 = NodeVersion("dim", "v2", 2)
        >>> fx_new = NodeVersion(
        ...     "fx_report", "r2", 4, {"dim": "v2"}, reports=["fx_report"]
        ... )
        >>> fx_old = NodeVersion(
        ...     "fx_report", "r1", 3, {"dim": "v1"}, reports=["fx_report"]
        ... )
        >>> pnl_new = NodeVersion(
        ...     "pnl_report", "p2", 4, {"dim": "v1"}, reports=["pnl_report"]
        ... )
        >>> reconciler = LineageReconciler(
        ...     [dim_v1, dim_v2, fx_new, fx_old, pnl_new],
        ...     report_priority=["fx_report", "pnl_report"],
        ... )
        >>> manifest = reconciler.reconcile()
        >>> [(row["node"], row["version"]) for row in reconciler.manifest_rows(manifest)]
        [('dim', 'v1'), ('fx_report', 'r1'), ('pnl_report', 'p2')]

    Args:
        versions: every landed node version available at close of book
        report_priority: report output nodes in the agreed selection order;
            each entry must be the name of a lineage node
        edges: optional direct parent-child edges as ``(upstream, node)``
            pairs. When omitted, edges are inferred from recorded inputs
    """

    def __init__(
        self,
        versions: Iterable[NodeVersion],
        report_priority: Sequence[str],
        edges: Sequence[tuple[str, str]] | None = None,
    ) -> None:
        versions = list(versions)
        if not versions:
            raise ValueError("No node versions provided for reconciliation")
        if not report_priority:
            raise ValueError("`report_priority` must contain at least one report")

        self.report_priority = tuple(report_priority)
        self._candidates: dict[str, list[NodeVersion]] = {}
        for candidate in versions:
            self._candidates.setdefault(candidate.node_name, []).append(candidate)

        for node_name, candidates in self._candidates.items():
            identifiers = [candidate.version for candidate in candidates]
            if len(set(identifiers)) != len(identifiers):
                raise ValueError(f"Duplicate version identifier on node {node_name}")
            candidates.sort(
                key=lambda candidate: (candidate.version, candidate.completed_at),
                reverse=True,
            )

        missing_reports = [
            report for report in self.report_priority if report not in self._candidates
        ]
        if missing_reports:
            raise KeyError(
                f"Report output node(s) not found among versions: {missing_reports}"
            )

        self.edges = self._infer_edges(edges)
        self._upstreams: dict[str, set[str]] = {
            node_name: set() for node_name in self._candidates
        }
        for upstream, node_name in self.edges:
            self._upstreams[node_name].add(upstream)
        self._node_levels = self._topological_levels()

        self._pinned: dict[str, NodeVersion] = {}
        self._pinned_by: dict[str, str] = {}
        self._conflict_demands: dict[str, dict[str, Any]] = {}
        self._recorded_skips: set[tuple[str, Any]] = set()
        self.skipped: list[dict[str, Any]] = []
        self._manifest: DAGNode | None = None

    def _infer_edges(
        self, edges: Sequence[tuple[str, str]] | None
    ) -> set[tuple[str, str]]:
        inferred: set[tuple[str, str]] = set()
        if edges is not None:
            inferred.update(edges)
        for candidates in self._candidates.values():
            for candidate in candidates:
                for upstream in candidate.inputs:
                    if upstream not in self._candidates:
                        raise KeyError(
                            f"Node {candidate.node_name} version {candidate.version} "
                            f"reads unknown upstream node {upstream}"
                        )
                    inferred.add((upstream, candidate.node_name))
        for upstream, node_name in inferred:
            if node_name not in self._candidates or upstream not in self._candidates:
                raise KeyError(
                    f"Edge ({upstream}, {node_name}) references unknown node"
                )
        return inferred

    def _topological_levels(self) -> dict[str, int]:
        levels: dict[str, int] = {}
        remaining = {
            node_name: set(upstreams)
            for node_name, upstreams in self._upstreams.items()
        }
        while remaining:
            progressed = False
            for node_name in list(remaining):
                if all(upstream in levels for upstream in remaining[node_name]):
                    levels[node_name] = 1 + max(
                        (levels[upstream] for upstream in remaining[node_name]),
                        default=-1,
                    )
                    del remaining[node_name]
                    progressed = True
            if not progressed:
                raise exceptions.LoopError(
                    "Lineage graph contains a cycle, cannot reconcile versions"
                )
        return levels

    def _ordered_nodes(self) -> list[str]:
        return sorted(
            self._candidates,
            key=lambda node_name: (self._node_levels[node_name], node_name),
        )

    def _record_skip(self, candidate: NodeVersion, reason: str) -> None:
        key = (candidate.node_name, candidate.version)
        if key not in self._recorded_skips:
            self._recorded_skips.add(key)
            self.skipped.append(
                {
                    "node": candidate.node_name,
                    "version": candidate.version,
                    "reason": reason,
                }
            )

    def _verifiable(self, candidate: NodeVersion) -> bool:
        """A candidate is checkable only if every direct input is recorded."""
        missing = self._upstreams[candidate.node_name] - set(candidate.inputs)
        if missing:
            self._record_skip(
                candidate,
                f"missing direct input number for {sorted(missing)}",
            )
            return False
        unavailable = [
            upstream
            for upstream, input_version in candidate.inputs.items()
            if not any(
                other.version == input_version for other in self._candidates[upstream]
            )
        ]
        if unavailable:
            self._record_skip(
                candidate,
                f"reads unavailable input version for {sorted(unavailable)}",
            )
            return False
        return True

    def _report_candidates(self, report: str) -> list[NodeVersion]:
        return [
            candidate
            for candidate in self._candidates[report]
            if report in candidate.reports and self._verifiable(candidate)
        ]

    def reconcile(self) -> DAGNode:
        """Select one version per node and build the shared manifest DAG.

        Returns:
            Root :class:`DAGNode` of the lineage manifest; every node carries
            the selected ``version``, ``completed_at`` and the reports it
            serves, while edges mirror the upstream-to-downstream lineage

        Raises:
            LineageConflictError: when two reports demand different versions of
                the same node and no older candidate combination can align them
        """
        self.skipped = []
        self._recorded_skips = set()
        self._conflict_demands = {}
        if not self._backtrack(0):
            raise self._build_conflict()

        manifest = self._build_manifest()
        self._manifest = manifest
        return manifest

    def _backtrack(self, report_index: int) -> bool:
        if report_index == len(self.report_priority):
            return True
        report = self.report_priority[report_index]
        candidates = self._report_candidates(report)
        if not candidates:
            return False

        for candidate in candidates:
            snapshot = (
                dict(self._pinned),
                dict(self._pinned_by),
            )
            try:
                self._apply_candidate(candidate, report)
            except LineageConflictError:
                self._restore(snapshot)
                continue
            if self._backtrack(report_index + 1):
                return True
            self._restore(snapshot)
        return False

    def _restore(
        self,
        snapshot: tuple[dict[str, NodeVersion], dict[str, str]],
    ) -> None:
        self._pinned, self._pinned_by = snapshot

    def _apply_candidate(self, candidate: NodeVersion, report: str) -> None:
        """Pin a report candidate together with its whole input chain."""
        for upstream, input_version in candidate.inputs.items():
            self._pin_version(upstream, input_version, report)
        self._pin_node(candidate, report)

    def _pin_version(self, node_name: str, version: Any, report: str) -> None:
        existing = self._pinned.get(node_name)
        self._conflict_demands.setdefault(node_name, {}).setdefault(report, version)
        if existing is not None:
            if existing.version != version:
                raise LineageConflictError(
                    node_name,
                    {
                        self._pinned_by[node_name]: existing.version,
                        report: version,
                    },
                )
            return

        candidate = next(
            (
                candidate
                for candidate in self._candidates[node_name]
                if candidate.version == version
            ),
            None,
        )
        if candidate is None or not self._verifiable(candidate):
            raise LineageConflictError(
                node_name,
                {report: version},
                f"Cannot align versions for {node_name}: {report} requires "
                f"unavailable or unverifiable version {version}",
            )
        for upstream, input_version in candidate.inputs.items():
            self._pin_version(upstream, input_version, report)
        self._pin_node(candidate, report)

    def _pin_node(self, candidate: NodeVersion, report: str) -> None:
        node_name = candidate.node_name
        existing = self._pinned.get(node_name)
        if existing is not None and existing.version != candidate.version:
            raise LineageConflictError(
                node_name,
                {
                    self._pinned_by[node_name]: existing.version,
                    report: candidate.version,
                },
            )
        self._pinned[node_name] = candidate
        self._pinned_by[node_name] = report

    def _build_conflict(self) -> LineageConflictError:
        for node_name in self._ordered_nodes():
            demands = self._conflict_demands.get(node_name, {})
            distinct = set(demands.values())
            if len(distinct) > 1:
                return LineageConflictError(node_name, demands)
        first_report = self.report_priority[0]
        node_name = first_report
        return LineageConflictError(
            node_name,
            {first_report: "no verifiable candidate available"},
            f"Cannot align versions for {node_name}: no verifiable candidate "
            "available, check skipped records for missing direct input numbers",
        )

    def _build_manifest(self) -> DAGNode:
        nodes: dict[str, DAGNode] = {}
        for node_name in self._ordered_nodes():
            if node_name not in self._pinned:
                continue
            candidate = self._pinned[node_name]
            nodes[node_name] = DAGNode(
                node_name,
                version=candidate.version,
                completed_at=candidate.completed_at,
                reports=candidate.reports,
                inputs=dict(candidate.inputs),
                **candidate.attributes,
            )
        for upstream, node_name in sorted(self.edges):
            if upstream in nodes and node_name in nodes:
                nodes[node_name].parents = [nodes[upstream]]
        roots = [node for node in nodes.values() if node.is_root]
        if len(roots) == 1:
            return roots[0]
        return DAGNode(
            "lineage_manifest",
            children=roots,
            reports=self.report_priority,
        )

    def iter_manifest(self, manifest: DAGNode | None = None) -> list[DAGNode]:
        """Return manifest nodes in lineage (upstream-first) order."""
        manifest = manifest or self._manifest
        if manifest is None:
            raise ValueError("Call `reconcile()` before iterating the manifest")
        ordered = sorted(
            self._all_nodes(manifest),
            key=lambda node: (
                self._node_levels.get(node.node_name, -1),
                node.node_name,
            ),
        )
        return ordered

    @staticmethod
    def _all_nodes(root: DAGNode) -> set[DAGNode]:
        seen = {root}
        stack = [root]
        while stack:
            node = stack.pop()
            for child in node.children:
                if child not in seen:
                    seen.add(child)
                    stack.append(child)
        return seen

    def manifest_rows(self, manifest: DAGNode | None = None) -> list[dict[str, Any]]:
        """Return the checkable version list as plain rows.

        Each row covers one lineage node with its selected version, finish
        timestamp, the reports relying on it, and every direct input version
        that the selected record actually read.
        """
        rows = []
        for node in self.iter_manifest(manifest):
            if node.node_name == "lineage_manifest":
                continue
            rows.append(
                {
                    "node": node.node_name,
                    "version": node.get_attr("version"),
                    "completed_at": node.get_attr("completed_at"),
                    "reports": list(node.get_attr("reports")),
                    "inputs": dict(node.get_attr("inputs")),
                }
            )
        return rows


def reconcile_lineage(
    versions: Iterable[NodeVersion],
    report_priority: Sequence[str],
    edges: Sequence[tuple[str, str]] | None = None,
) -> tuple[DAGNode, list[dict[str, Any]]]:
    """Convenience wrapper returning the manifest DAG and its version rows.

    Args:
        versions: every landed node version available at close of book
        report_priority: report output nodes in the agreed selection order
        edges: optional explicit ``(upstream, node)`` lineage edges

    Returns:
        Tuple of the manifest root node and the checkable version rows
    """
    reconciler = LineageReconciler(versions, report_priority, edges)
    manifest = reconciler.reconcile()
    return manifest, reconciler.manifest_rows(manifest)
