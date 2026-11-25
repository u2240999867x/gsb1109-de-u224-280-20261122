"""Scale benchmark: ~20k records, 1000+ approvals, desk quota <= 10."""

import resource
import time
import tracemalloc

import numpy as np
import pandas as pd

from pyranges.clearance import ClearancePlanner

LENGTH = 1_000_000
N_APPROVALS = 1200
N_SCOPES = 10_000
N_CLOSURES = 10_000
QUOTA = 10


def _allocate_spans(rng):
    occupancy = np.zeros(LENGTH, dtype=np.int16)
    spans: dict[str, tuple[int, int]] = {}
    for idx in range(N_APPROVALS):
        approval = f"A{idx:04d}"
        span_len = int(rng.integers(500, 5_000))
        for _ in range(500):
            first_start = int(rng.integers(0, LENGTH - span_len))
            last_end = first_start + span_len
            if int(occupancy[first_start:last_end].max()) < QUOTA:
                occupancy[first_start:last_end] += 1
                spans[approval] = (first_start, last_end)
                break
        else:
            raise RuntimeError("could not place approval span within the quota")
    return spans


def generate(seed):
    rng = np.random.default_rng(seed)
    approval_ids = [f"A{i:04d}" for i in range(N_APPROVALS)]
    spans = _allocate_spans(rng)

    scope_rows = []
    for approval in approval_ids:
        first_start, last_end = spans[approval]
        cuts = sorted(
            {first_start, last_end}
            | {
                int(rng.integers(first_start, last_end))
                for _ in range(rng.integers(0, 4))
            }
        )
        for lo, hi in zip(cuts[:-1], cuts[1:]):
            if hi > lo:
                scope_rows.append((approval, lo, hi))
    while len(scope_rows) < N_SCOPES:
        approval = approval_ids[int(rng.integers(0, N_APPROVALS))]
        first_start, last_end = spans[approval]
        lo = int(rng.integers(first_start, max(first_start + 1, last_end - 10)))
        hi = min(last_end, lo + int(rng.integers(10, 300)))
        if hi > lo:
            scope_rows.append((approval, lo, hi))
    scopes = pd.DataFrame(scope_rows, columns=["ApprovalId", "Start", "End"])

    circuits = ("L", "R")
    closure_rows = []
    for i in range(N_CLOSURES):
        lo = int(rng.integers(0, LENGTH - 100))
        hi = min(LENGTH, lo + int(rng.integers(20, 400)))
        approval = approval_ids[int(rng.integers(0, N_APPROVALS))]
        closure_rows.append((circuits[i % 2], lo, hi, f"BLK{i:05d}", approval))
    closures = pd.DataFrame(
        closure_rows, columns=["CircuitId", "Start", "End", "BlockId", "ApprovalId"]
    )
    return closures, scopes, approval_ids


def generate_feasible(seed, spans):
    rng = np.random.default_rng(seed + 1)
    approval_ids = list(spans)
    span_starts = np.array([spans[a][0] for a in approval_ids])
    span_ends = np.array([spans[a][1] for a in approval_ids])
    circuits = ("L", "R")
    closure_rows = []
    extra_scopes = []
    for i in range(N_CLOSURES):
        for _attempt in range(200):
            lo = int(rng.integers(0, LENGTH - 200))
            hi = min(LENGTH, lo + int(rng.integers(20, 200)))
            candidates = np.where((span_starts <= lo) & (span_ends >= hi))[0]
            if len(candidates):
                approval = approval_ids[
                    int(candidates[rng.integers(0, len(candidates))])
                ]
                closure_rows.append((circuits[i % 2], lo, hi, f"BLK{i:05d}", approval))
                extra_scopes.append((approval, lo, hi))
                break
        else:
            raise RuntimeError("could not place a coverable closure")
    closures = pd.DataFrame(
        closure_rows, columns=["CircuitId", "Start", "End", "BlockId", "ApprovalId"]
    )
    added_scopes = pd.DataFrame(extra_scopes, columns=["ApprovalId", "Start", "End"])
    return closures, added_scopes


def main():
    closures, scopes, approval_ids = generate(20261126)
    print(
        f"records: {len(closures)} closures + {len(scopes)} scopes "
        f"= {len(closures) + len(scopes)}; approvals: {len(approval_ids)}"
    )

    tracemalloc.start()
    t0 = time.perf_counter()
    planner = ClearancePlanner(closures, scopes)
    build_s = time.perf_counter() - t0
    peak = tracemalloc.get_traced_memory()[1]
    print(f"build: {build_s:.2f}s, tracemalloc peak {peak / 2**20:.1f} MiB")
    print(f"quota peak: {planner.quota_peak()} (<= {QUOTA})")
    assert planner.quota_peak() <= QUOTA

    approved = tuple(approval_ids[::2])

    timings = []
    for label, window, granted in (
        ("full window, none approved", (0, LENGTH), ()),
        ("full window, half approved", (0, LENGTH), approved),
        ("narrow window, none approved", (250_000, 260_000), ()),
        ("repeat full window", (0, LENGTH), approved),
    ):
        t0 = time.perf_counter()
        result = planner.plan(*window, approved=granted)
        elapsed = time.perf_counter() - t0
        timings.append(elapsed)
        state = (
            f"new={result.new_approval_count} mandatory={len(result.mandatory)} "
            f"segments={len(result.segments)} dead={len(result.infeasible_regions)}"
            if result.feasible
            else f"INFEASIBLE dead regions={len(result.infeasible_regions)}"
        )
        print(f"query {label!r}: {elapsed:.2f}s -> {state}")

    # Feasible counterpart exercising the full DP/Pareto/mandatory machinery.
    spans = {
        approval: (
            int(
                planner._approval_spans().set_index("ApprovalId").loc[approval, "Start"]
            ),
            int(planner._approval_spans().set_index("ApprovalId").loc[approval, "End"]),
        )
        for approval in approval_ids
    }
    feasible_closures, feasible_scopes = generate_feasible(20261126, spans)
    print(
        f"feasible order: {len(feasible_closures)} closures + "
        f"{len(feasible_scopes)} scopes = {len(feasible_closures) + len(feasible_scopes)} records"
    )
    feasible_planner = ClearancePlanner(feasible_closures, feasible_scopes)
    assert feasible_planner.quota_peak() <= QUOTA
    for label, granted in (
        ("feasible full, none approved", ()),
        ("feasible full, half approved", approved),
    ):
        t0 = time.perf_counter()
        result = feasible_planner.plan(0, LENGTH, approved=granted)
        elapsed = time.perf_counter() - t0
        timings.append(elapsed)
        assert result.feasible
        print(
            f"query {label!r}: {elapsed:.2f}s -> new={result.new_approval_count} "
            f"mandatory={len(result.mandatory)} segments={len(result.segments)} "
            f"runs={len(result.runs)}"
        )

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(f"tracemalloc peak {peak / 2**20:.1f} MiB; max RSS {rss / 2**20:.1f} MiB")
    assert max(timings) < 60, f"query budget exceeded: {max(timings):.1f}s"
    assert rss < 1024 * 2**20, f"RSS budget exceeded: {rss / 2**20:.1f} MiB"
    print("BUDGETS OK")


if __name__ == "__main__":
    main()
