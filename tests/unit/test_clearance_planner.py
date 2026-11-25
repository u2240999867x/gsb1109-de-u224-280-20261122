"""Exactness cross-checks for the clearance planner.

Small random instances are solved independently by exhaustive subset
enumeration, which is feasible only at tiny scale, and compared against the
dynamic-program implementation on optimum size, feasibility and the set of
mandatory approvals.
"""

import itertools

import numpy as np
import pandas as pd
import pytest

from pyranges.clearance import ClearancePlanner, plan_clearance


def _example_planner():
    closures = pd.DataFrame(
        [
            ("L", 0, 40, "bLA", "A"),
            ("L", 40, 100, "bLBC1", "B"),
            ("L", 40, 100, "bLBC2", "C"),
            ("R", 0, 40, "bRBC1", "B"),
            ("R", 0, 40, "bRBC2", "C"),
            ("R", 60, 100, "bRBD1", "B"),
            ("R", 60, 100, "bRBD2", "D"),
        ],
        columns=["CircuitId", "Start", "End", "BlockId", "ApprovalId"],
    )
    scopes = pd.DataFrame(
        [("A", 0, 100), ("B", 0, 100), ("C", 0, 100), ("D", 0, 100)],
        columns=["ApprovalId", "Start", "End"],
    )
    return ClearancePlanner(closures, scopes)


def test_first_segment_needs_only_a():
    result = _example_planner().plan(0, 40)
    assert result.feasible
    assert result.new_approval_count == 1
    assert result.new_approvals == ("A",)
    assert result.mandatory == ("A",)
    assert [(s.start, s.end, s.circuit) for s in result.runs] == [(0, 40, "L")]


def test_full_order_before_and_after_a_is_granted():
    planner = _example_planner()
    initial = planner.plan(0, 100)
    assert initial.new_approval_count == 2
    assert set(initial.new_approvals) == {"B", "C"}
    assert set(initial.mandatory) == {"B", "C"}

    after_a = planner.plan(0, 100, approved=("A",))
    assert after_a.new_approval_count == 2
    assert set(after_a.new_approvals) == {"B", "C"}
    assert after_a.mandatory == ("B",)
    assert all("A" not in seg.approvals for seg in after_a.segments)


def test_result_is_independent_of_input_row_order():
    planner = _example_planner()
    rng = np.random.default_rng(11)
    closures = planner.closures.sample(frac=1.0, random_state=rng).reset_index(
        drop=True
    )
    scopes = planner.scopes.sample(frac=1.0, random_state=rng).reset_index(drop=True)
    shuffled = ClearancePlanner(closures, scopes).plan(0, 100, approved=("A",))
    baseline = planner.plan(0, 100, approved=("A",))
    assert shuffled.new_approvals == baseline.new_approvals
    assert shuffled.mandatory == baseline.mandatory
    assert [(s.start, s.end, s.circuit, s.approvals) for s in shuffled.runs] == [
        (s.start, s.end, s.circuit, s.approvals) for s in baseline.runs
    ]


def test_infeasible_mileage_reports_each_circuits_remaining_block():
    closures = pd.DataFrame(
        [
            ("L", 0, 100, "pL", "P"),
            ("R", 20, 80, "pR", "Q"),
        ],
        columns=["CircuitId", "Start", "End", "BlockId", "ApprovalId"],
    )
    scopes = pd.DataFrame([("Q", 20, 60)], columns=["ApprovalId", "Start", "End"])
    planner = ClearancePlanner(closures, scopes)
    result = planner.plan(0, 100)
    assert not result.feasible
    assert len(result.infeasible_regions) == 1
    region = result.infeasible_regions[0]
    assert (region.start, region.end) == (60, 80)
    assert region.blocked == {"L": (("pL", "P"),), "R": (("pR", "Q"),)}
    # Granting every pending number cannot rescue the permanently dead mileage.
    assert not planner.plan(0, 100, approved=("P", "Q")).feasible


def test_disjoint_scopes_with_gap_and_desk_quota():
    closures = pd.DataFrame(
        [("L", 0, 100, "g", "W"), ("R", 40, 60, "u", "U")],
        columns=["CircuitId", "Start", "End", "BlockId", "ApprovalId"],
    )
    scopes = pd.DataFrame(
        [("W", 0, 40), ("W", 60, 100), ("V", 90, 95)],
        columns=["ApprovalId", "Start", "End"],
    )
    planner = ClearancePlanner(closures, scopes)
    # W keeps one desk slot from 0 to 100, gap included; V overlaps at [90,95).
    assert planner.quota_peak() == 2
    result = planner.plan(0, 100)
    assert not result.feasible
    assert (result.infeasible_regions[0].start, result.infeasible_regions[0].end) == (
        40,
        60,
    )


def test_one_off_wrapper_and_validation():
    planner = _example_planner()
    result = plan_clearance(planner.closures, planner.scopes, 0, 40)
    assert result.new_approvals == ("A",)

    with pytest.raises(ValueError):
        planner.plan(10, 10)
    with pytest.raises(ValueError):
        ClearancePlanner(planner.closures.drop(columns=["BlockId"]), planner.scopes)


def test_window_without_closures_is_immediately_open():
    closures = pd.DataFrame(
        [("L", 100, 200, "b", "A")],
        columns=["CircuitId", "Start", "End", "BlockId", "ApprovalId"],
    )
    scopes = pd.DataFrame([("A", 100, 200)], columns=["ApprovalId", "Start", "End"])
    planner = ClearancePlanner(closures, scopes)
    result = planner.plan(0, 50)
    assert result.feasible
    assert result.new_approval_count == 0
    assert result.mandatory == ()
    assert [(seg.start, seg.end, seg.circuit) for seg in result.runs] == [(0, 50, "L")]


def test_single_circuit_makes_its_only_approval_mandatory():
    closures = pd.DataFrame(
        [("L", 100, 200, "b", "A")],
        columns=["CircuitId", "Start", "End", "BlockId", "ApprovalId"],
    )
    scopes = pd.DataFrame([("A", 100, 200)], columns=["ApprovalId", "Start", "End"])
    result = ClearancePlanner(closures, scopes).plan(100, 200)
    assert result.feasible
    assert result.new_approvals == ("A",)
    assert result.mandatory == ("A",)


def test_approval_without_any_scope_is_permanently_blocking():
    closures = pd.DataFrame(
        [("L", 0, 10, "b", "Z"), ("R", 0, 10, "b2", "Z")],
        columns=["CircuitId", "Start", "End", "BlockId", "ApprovalId"],
    )
    scopes = pd.DataFrame([("OTHER", 0, 10)], columns=["ApprovalId", "Start", "End"])
    planner = ClearancePlanner(closures, scopes)
    assert planner.quota_peak() == 1
    result = planner.plan(0, 10)
    assert not result.feasible
    region = result.infeasible_regions[0]
    assert (region.start, region.end) == (0, 10)
    assert set(region.blocked) == {"L", "R"}
    assert region.blocked == {
        "L": (("b", "Z"),),
        "R": (("b2", "Z"),),
    }


def _brute_force(closures, scopes, start, end, approved):
    pending = sorted(set(scopes["ApprovalId"]) - set(approved))
    closure_records = closures[(closures["End"] > start) & (closures["Start"] < end)]
    scope_records = scopes[(scopes["End"] > start) & (scopes["Start"] < end)]
    circuits = sorted(set(closure_records["CircuitId"]))

    def releasable(granted):
        effective = set(granted) | set(approved)
        boundaries = sorted(
            set([start, end])
            | set(closure_records["Start"])
            | set(closure_records["End"])
            | set(scope_records["Start"])
            | set(scope_records["End"])
        )
        boundaries = [b for b in boundaries if start <= b <= end]
        for lo, hi in zip(boundaries[:-1], boundaries[1:]):
            open_circuit = False
            for circuit in circuits:
                active = closure_records[
                    (closure_records["CircuitId"] == circuit)
                    & (closure_records["Start"] < hi)
                    & (closure_records["End"] > lo)
                ]
                blocked = False
                for row in active.itertuples(index=False):
                    if row.ApprovalId not in effective:
                        blocked = True
                        break
                    covers = scope_records[
                        (scope_records["ApprovalId"] == row.ApprovalId)
                        & (scope_records["Start"] < hi)
                        & (scope_records["End"] > lo)
                    ]
                    if covers.empty:
                        blocked = True
                        break
                if not blocked:
                    open_circuit = True
                    break
            if not open_circuit:
                return False
        return True

    optimum = None
    for size in range(len(pending) + 1):
        for combo in itertools.combinations(pending, size):
            if releasable(combo):
                optimum = set(combo)
                break
        if optimum is not None:
            break
    if optimum is None:
        return None, set()
    mandatory = set(optimum)
    for combo in itertools.combinations(pending, len(optimum)):
        candidate = set(combo)
        if releasable(candidate):
            mandatory &= candidate
    return optimum, mandatory


def test_matches_brute_force_on_random_instances():
    rng = np.random.default_rng(20261126)
    approvals = [f"A{i}" for i in range(7)]
    for trial in range(120):
        n_circuits = 2 + (1 if trial % 4 == 0 else 0)
        scope_rows = []
        for approval in approvals:
            for _ in range(rng.integers(1, 3)):
                lo = int(rng.integers(0, 18))
                hi = lo + int(rng.integers(1, 5))
                scope_rows.append((approval, lo, min(hi, 20)))
        scopes = pd.DataFrame(scope_rows, columns=["ApprovalId", "Start", "End"])

        closure_rows = []
        block_seq = 0
        for _ in range(rng.integers(4, 10)):
            circuit = f"C{int(rng.integers(0, n_circuits))}"
            lo = int(rng.integers(0, 19))
            hi = lo + int(rng.integers(1, 4))
            approval = approvals[int(rng.integers(0, len(approvals)))]
            closure_rows.append((circuit, lo, min(hi, 20), f"B{block_seq}", approval))
            block_seq += 1
        closures = pd.DataFrame(
            closure_rows, columns=["CircuitId", "Start", "End", "BlockId", "ApprovalId"]
        )
        closures = closures[closures["End"] > closures["Start"]].reset_index(drop=True)
        if closures.empty:
            continue

        approved = tuple(a for a in approvals[:2] if rng.random() < 0.5)
        planner = ClearancePlanner(closures, scopes)
        result = planner.plan(0, 20, approved=approved)
        optimum, mandatory = _brute_force(closures, scopes, 0, 20, approved)

        if optimum is None:
            assert not result.feasible
            assert result.infeasible_regions
            continue
        assert result.feasible
        assert result.new_approval_count == len(optimum)
        assert set(result.mandatory) == mandatory

        chosen = set(result.new_approvals)
        # The canonical plan must be a minimum, valid release in its own right.
        assert len(chosen) == len(optimum)
        assert _brute_force_releases(closures, scopes, 0, 20, approved, chosen)
        for segment in result.segments:
            for approval in segment.approvals:
                assert approval in chosen
        covered = set()
        for segment in result.runs:
            covered.update(segment.approvals)
        assert covered <= chosen


def _brute_force_releases(closures, scopes, start, end, approved, granted):
    closure_records = closures[(closures["End"] > start) & (closures["Start"] < end)]
    scope_records = scopes[(scopes["End"] > start) & (scopes["Start"] < end)]
    circuits = sorted(set(closure_records["CircuitId"]))
    effective = set(granted) | set(approved)
    boundaries = sorted(
        {start, end}
        | set(closure_records["Start"])
        | set(closure_records["End"])
        | set(scope_records["Start"])
        | set(scope_records["End"])
    )
    boundaries = [b for b in boundaries if start <= b <= end]
    for lo, hi in zip(boundaries[:-1], boundaries[1:]):
        if not any(
            _circuit_open(circuit, closure_records, scope_records, lo, hi, effective)
            for circuit in circuits
        ):
            return False
    return True


def _circuit_open(circuit, closure_records, scope_records, lo, hi, effective):
    active = closure_records[
        (closure_records["CircuitId"] == circuit)
        & (closure_records["Start"] < hi)
        & (closure_records["End"] > lo)
    ]
    for row in active.itertuples(index=False):
        if row.ApprovalId not in effective:
            return False
        if scope_records[
            (scope_records["ApprovalId"] == row.ApprovalId)
            & (scope_records["Start"] < hi)
            & (scope_records["End"] > lo)
        ].empty:
            return False
    return True
