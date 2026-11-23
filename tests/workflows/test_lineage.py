import unittest

import pytest

from bigtree import dag_iterator
from bigtree.workflows.lineage import (
    LineageConflictError,
    LineageReconciler,
    NodeVersion,
    reconcile_lineage,
)

FX_REPORT = "fx_report"
PNL_REPORT = "pnl_report"
DIM = "dim"


def _close_book_versions():
    """Two reports derived from the same dimension table.

    * FX newest reads dimension v2, its older candidate reads v1
    * PnL newest reads dimension v1
    * FX has an extra older candidate missing the direct input number
    """
    return [
        NodeVersion(DIM, "v2", "t2"),
        NodeVersion(DIM, "v1", "t1"),
        NodeVersion("rate", "q2", "t5", {"source": "s2"}),
        NodeVersion("rate", "q1", "t4", {"source": "s1"}),
        NodeVersion("source", "s2", "t3"),
        NodeVersion("source", "s1", "t2"),
        NodeVersion(
            FX_REPORT,
            "r3",
            "t9",
            {DIM: "v2", "rate": "q2"},
            reports=[FX_REPORT],
        ),
        NodeVersion(
            FX_REPORT,
            "r2",
            "t8",
            {DIM: "v1", "rate": "q1"},
            reports=[FX_REPORT],
        ),
        NodeVersion(FX_REPORT, "r1", "t7", {}, reports=[FX_REPORT]),
        NodeVersion(
            PNL_REPORT,
            "p2",
            "t9",
            {DIM: "v1"},
            reports=[PNL_REPORT],
        ),
        NodeVersion(
            PNL_REPORT,
            "p1",
            "t6",
            {DIM: "v1"},
            reports=[PNL_REPORT],
        ),
    ]


class TestNodeVersion(unittest.TestCase):
    def test_missing_input_normalized_to_none(self):
        candidate = NodeVersion("a", "v1", 1, {"b": None})
        assert candidate.reads("b") is None
        assert candidate.inputs == {}

    def test_reads_recorded_input(self):
        candidate = NodeVersion("a", "v1", 1, {"b": "v3"})
        assert candidate.reads("b") == "v3"


class TestLineageReconciler(unittest.TestCase):
    def setUp(self):
        self.reconciler = LineageReconciler(
            _close_book_versions(),
            report_priority=[FX_REPORT, PNL_REPORT],
        )

    def test_falls_back_to_align_shared_dimension(self):
        manifest = self.reconciler.reconcile()
        selected = {
            row["node"]: row["version"]
            for row in self.reconciler.manifest_rows(manifest)
        }
        assert selected[DIM] == "v1"
        assert selected[FX_REPORT] == "r2"
        assert selected[PNL_REPORT] == "p2"

    def test_transitive_dependencies_are_consistent(self):
        manifest = self.reconciler.reconcile()
        rows = self.reconciler.manifest_rows(manifest)
        selected = {row["node"]: row["version"] for row in rows}
        for row in rows:
            for upstream, input_version in row["inputs"].items():
                assert selected[upstream] == input_version
        assert selected["rate"] == "q1"
        assert selected["source"] == "s1"

    def test_manifest_dag_shares_one_node_per_lineage_node(self):
        manifest = self.reconciler.reconcile()
        edges = {
            (parent.node_name, child.node_name)
            for parent, child in dag_iterator(manifest)
        }
        assert (DIM, FX_REPORT) in edges
        assert (DIM, PNL_REPORT) in edges
        assert ("rate", FX_REPORT) in edges
        assert ("source", "rate") in edges
        node_names = {
            node.node_name for node in self.reconciler.iter_manifest(manifest)
        }
        node_names.discard("lineage_manifest")
        assert node_names == {"source", "rate", DIM, FX_REPORT, PNL_REPORT}

    def test_candidate_missing_direct_input_is_skipped(self):
        self.reconciler.reconcile()
        assert self.reconciler.skipped == [
            {
                "node": FX_REPORT,
                "version": "r1",
                "reason": "missing direct input number for ['dim', 'rate']",
            }
        ]

    def test_newest_kept_when_reports_align(self):
        versions = [
            NodeVersion(DIM, "v2", "t2"),
            NodeVersion(DIM, "v1", "t1"),
            NodeVersion(FX_REPORT, "r3", "t9", {DIM: "v2"}, reports=[FX_REPORT]),
            NodeVersion(PNL_REPORT, "p2", "t9", {DIM: "v2"}, reports=[PNL_REPORT]),
        ]
        reconciler = LineageReconciler(versions, [FX_REPORT, PNL_REPORT])
        manifest = reconciler.reconcile()
        selected = {
            row["node"]: row["version"] for row in reconciler.manifest_rows(manifest)
        }
        assert selected == {
            DIM: "v2",
            FX_REPORT: "r3",
            PNL_REPORT: "p2",
        }

    def test_unresolvable_conflict_names_reports_and_versions(self):
        versions = [
            NodeVersion(DIM, "v2", "t2"),
            NodeVersion(DIM, "v1", "t1"),
            NodeVersion(FX_REPORT, "r3", "t9", {DIM: "v2"}, reports=[FX_REPORT]),
            NodeVersion(PNL_REPORT, "p2", "t9", {DIM: "v1"}, reports=[PNL_REPORT]),
        ]
        reconciler = LineageReconciler(versions, [FX_REPORT, PNL_REPORT])
        with pytest.raises(LineageConflictError) as exc_info:
            reconciler.reconcile()
        error = exc_info.value
        assert error.node_name == DIM
        assert error.report_versions == {
            FX_REPORT: "v2",
            PNL_REPORT: "v1",
        }
        message = str(error)
        assert FX_REPORT in message and PNL_REPORT in message
        assert "v2" in message and "v1" in message

    def test_unavailable_input_version_marks_candidate_unverifiable(self):
        versions = [
            NodeVersion(DIM, "v1", "t1"),
            NodeVersion(FX_REPORT, "r1", "t2", {DIM: "v9"}, reports=[FX_REPORT]),
            NodeVersion(PNL_REPORT, "p1", "t2", {DIM: "v1"}, reports=[PNL_REPORT]),
        ]
        reconciler = LineageReconciler(versions, [FX_REPORT, PNL_REPORT])
        with pytest.raises(LineageConflictError):
            reconciler.reconcile()
        assert reconciler.skipped[0]["node"] == FX_REPORT
        assert reconciler.skipped[0]["reason"].startswith(
            "reads unavailable input version"
        )

    def test_manifest_rows_carry_landing_metadata(self):
        manifest = self.reconciler.reconcile()
        rows = self.reconciler.manifest_rows(manifest)
        fx_row = next(row for row in rows if row["node"] == FX_REPORT)
        assert fx_row["completed_at"] == "t8"
        assert fx_row["reports"] == [FX_REPORT]
        assert fx_row["inputs"] == {DIM: "v1", "rate": "q1"}

    def test_reconcile_lineage_wrapper(self):
        manifest, rows = reconcile_lineage(
            _close_book_versions(), [FX_REPORT, PNL_REPORT]
        )
        assert manifest.is_root
        assert {row["node"] for row in rows} == {
            "source",
            "rate",
            DIM,
            FX_REPORT,
            PNL_REPORT,
        }

    def test_invalid_construction(self):
        with pytest.raises(ValueError, match="No node versions"):
            LineageReconciler([], [FX_REPORT])
        with pytest.raises(ValueError, match="report_priority"):
            LineageReconciler([NodeVersion(DIM, "v1", 1)], [])
        with pytest.raises(ValueError, match="Duplicate version"):
            LineageReconciler(
                [NodeVersion(DIM, "v1", 1), NodeVersion(DIM, "v1", 2)],
                [DIM],
            )
        with pytest.raises(KeyError, match="Report output node"):
            LineageReconciler([NodeVersion(DIM, "v1", 1)], [FX_REPORT])

    def test_cycle_rejected(self):
        versions = [
            NodeVersion("a", "1", 1, {"b": "1"}, reports=["a"]),
            NodeVersion("b", "1", 1, {"a": "1"}, reports=["b"]),
        ]
        with pytest.raises(Exception, match="cycle"):
            LineageReconciler(versions, ["a", "b"]).reconcile()
