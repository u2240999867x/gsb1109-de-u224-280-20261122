"""Tests for monitor_lag: stable clock-offset search between two monitors.

The small line is verified millisecond by millisecond against an exact
brute-force expansion; the same pattern is then placed at huge coordinates
with a lag search range spanning days.  A constant-only line is checked
separately.
"""

import random

import numpy as np
import pandas as pd
import pytest
import pyranges as pr

from monitor_lag import (
    REASON_CONSTANT_A,
    REASON_CONSTANT_B,
    REASON_CONSTANT_BOTH,
    SignalLagAnalyzer,
    find_best_lags,
    _compare_r,
)

# ---------------------------------------------------------------------------
# exact brute-force reference (per-millisecond expansion, small cases only)
# ---------------------------------------------------------------------------


def expand(segments):
    dense = {}
    for start, end, value in segments:
        for x in range(start, end):
            dense[x] = value
    return dense


def _stats_dict(n, sa, sb, saa, sbb, sab):
    return dict(
        n=n, sa=sa, sb=sb, saa=saa, sbb=sbb, sab=sab,
        C=n * sab - sa * sb, VA=n * saa - sa * sa, VB=n * sbb - sb * sb,
    )


def brute_per_lag(segments_a, segments_b, lag_min, lag_max):
    """Exact sufficient statistics for every lag, expanding per millisecond."""
    dense_a = expand(segments_a)
    dense_b = expand(segments_b)
    per_lag = {}
    for lag in range(lag_min, lag_max + 1):
        pairs = [(v, dense_b[x + lag]) for x, v in dense_a.items() if x + lag in dense_b]
        n = len(pairs)
        sa = sum(a for a, _ in pairs)
        sb = sum(b for _, b in pairs)
        saa = sum(a * a for a, _ in pairs)
        sbb = sum(b * b for _, b in pairs)
        sab = sum(a * b for a, b in pairs)
        per_lag[lag] = _stats_dict(n, sa, sb, saa, sbb, sab)
    return per_lag


def brute_per_lag_pairwise(segments_a, segments_b, lag_min, lag_max):
    """Exact per-lag statistics from pairwise segment overlaps (vectorized).

    Independent of the module's event machinery; used where per-millisecond
    expansion would be too slow.
    """
    lags = np.arange(lag_min, lag_max + 1, dtype=np.int64)
    totals = {key: np.zeros(len(lags), dtype=np.int64) for key in
              ("n", "sa", "sb", "saa", "sbb", "sab")}
    for a_start, a_end, va in segments_a:
        for b_start, b_end, vb in segments_b:
            lo = np.maximum(a_start, b_start - lags)
            hi = np.minimum(a_end, b_end - lags)
            w = np.clip(hi - lo, 0, None)
            totals["n"] += w
            totals["sa"] += w * va
            totals["sb"] += w * vb
            totals["saa"] += w * va * va
            totals["sbb"] += w * vb * vb
            totals["sab"] += w * va * vb
    per_lag = {}
    for i, lag in enumerate(lags):
        per_lag[int(lag)] = _stats_dict(*(int(totals[key][i]) for key in
                                          ("n", "sa", "sb", "saa", "sbb", "sab")))
    return per_lag


def brute_best(per_lag, min_coverage):
    best = None
    lags = []
    for lag, st in per_lag.items():
        if st["n"] < min_coverage or st["VA"] == 0 or st["VB"] == 0:
            continue
        triple = (st["C"], st["VA"], st["VB"])
        if best is None or _compare_r(triple, best) > 0:
            best = triple
            lags = [lag]
        elif _compare_r(triple, best) == 0:
            lags.append(lag)
    return best, lags


def brute_undefined(per_lag, min_coverage):
    """lag -> reason for lags with enough coverage but undefined correlation."""
    out = {}
    for lag, st in per_lag.items():
        if st["n"] < min_coverage:
            continue
        if st["VA"] == 0 and st["VB"] == 0:
            out[lag] = REASON_CONSTANT_BOTH
        elif st["VA"] == 0:
            out[lag] = REASON_CONSTANT_A
        elif st["VB"] == 0:
            out[lag] = REASON_CONSTANT_B
    return out


def undefined_as_dict(undefined_ranges):
    out = {}
    for start, end, reason in undefined_ranges:
        for lag in range(start, end):
            out[lag] = reason
    return out


def assert_matches_brute(analyzer, result, per_lag, lag_min, lag_max, min_coverage):
    # millisecond-by-millisecond check of every candidate's statistics
    for lag in range(lag_min, lag_max + 1):
        stats = analyzer.sufficient_stats(lag)
        ref = per_lag[lag]
        for key in ("n", "sa", "sb", "saa", "sbb", "sab"):
            assert stats[key] == ref[key], (lag, key, stats[key], ref[key])
    # best lags and tied positions
    best_triple, best_lags = brute_best(per_lag, min_coverage)
    assert result.best_lags == best_lags
    if best_triple is None:
        assert result.best_correlation is None
    else:
        c, va, vb = best_triple
        expected = (float(c * c / (va * vb))) ** 0.5
        if c < 0:
            expected = -expected
        assert result.best_correlation == pytest.approx(expected, rel=1e-12, abs=1e-12)
        ref = per_lag[result.best_lags[0]]
        assert result.best_coverage == ref["n"]
        assert result.best_mean_a == pytest.approx(ref["sa"] / ref["n"])
        assert result.best_mean_b == pytest.approx(ref["sb"] / ref["n"])
    # coverage counts and constant-signal undefined ranges
    assert result.n_lags_with_coverage == sum(
        1 for st in per_lag.values() if st["n"] >= min_coverage
    )
    assert undefined_as_dict(result.undefined) == brute_undefined(per_lag, min_coverage)


# ---------------------------------------------------------------------------
# the small line: shifted pulse pairs, true zeros vs missing, short overlap
# ---------------------------------------------------------------------------

# Monitor A: two copies of a pulse pair, true zeros in between, plus a short
# pulse pair.  Monitor B: the same pulses 1000 ms later, but the stretch
# matching A's true zeros was never transmitted (missing), and the short
# pulse pair sits at a place that only aligns at lag 1300 with 4 ms overlap.
A_SEGS = [
    (100, 110, 5),
    (120, 125, 9),
    (140, 150, 0),   # true zeros
    (200, 202, 7),
    (202, 204, 8),
    (300, 310, 5),
    (320, 325, 9),
]
B_SEGS = [
    (1100, 1110, 5),
    (1120, 1125, 9),
    # [1140, 1150) missing: matches A's true zeros at the true lag
    (1300, 1310, 5),
    (1320, 1325, 9),
    (1500, 1502, 7),  # short pair: aligns with A's short pair only at lag 1300
    (1502, 1504, 8),
]
LAG_MIN, LAG_MAX, MIN_COV = -200, 2200, 10


@pytest.fixture(scope="module")
def small_result():
    analyzer = SignalLagAnalyzer(A_SEGS, B_SEGS)
    result = analyzer.find_best(LAG_MIN, LAG_MAX, MIN_COV)
    per_lag = brute_per_lag(A_SEGS, B_SEGS, LAG_MIN, LAG_MAX)
    assert_matches_brute(analyzer, result, per_lag, LAG_MIN, LAG_MAX, MIN_COV)
    return analyzer, result, per_lag


def test_small_line_best_lags(small_result):
    _, result, _ = small_result
    # perfect correlation at the true lag and at the shifted copies: all tied
    # integer positions are kept
    assert result.best_correlation == 1.0
    assert 1000 in result.best_lags
    assert len(result.best_lags) >= 2  # several distinct legal lags tie


def test_small_line_short_overlap_cannot_win(small_result):
    _, result, per_lag = small_result
    # lag 1300 correlates perfectly but only covers 4 ms < min_coverage
    assert per_lag[1300]["n"] == 4
    assert 1300 not in result.best_lags


def test_small_line_missing_is_not_zero(small_result):
    analyzer, _, _ = small_result
    stats = analyzer.stats_at(1000)
    # A's true zeros pair with B's untransmitted stretch: excluded, so the
    # common coverage is 30 ms, not the 40 ms a zero-fill would fabricate
    assert stats.coverage == 30
    assert stats.mean_a == pytest.approx(190 / 30)
    assert stats.mean_b == pytest.approx(190 / 30)
    assert stats.correlation == 1.0


def test_small_line_accepts_pyranges_and_dataframe():
    def to_pr(segs):
        return pr.PyRanges(pd.DataFrame(
            {"Chromosome": "line1",
             "Start": [s for s, _, _ in segs],
             "End": [e for _, e, _ in segs],
             "Value": [v for _, _, v in segs]}))

    expected = find_best_lags(A_SEGS, B_SEGS, LAG_MIN, LAG_MAX, MIN_COV)
    for left, right in (
        (to_pr(A_SEGS), to_pr(B_SEGS)),
        (to_pr(A_SEGS).df, to_pr(B_SEGS).df),
        (to_pr(A_SEGS), B_SEGS),
    ):
        got = find_best_lags(left, right, LAG_MIN, LAG_MAX, MIN_COV)
        assert got.best_lags == expected.best_lags
        assert got.best_correlation == expected.best_correlation
        assert got.undefined == expected.undefined


def test_restricted_lag_window():
    result = find_best_lags(A_SEGS, B_SEGS, 990, 1010, MIN_COV)
    analyzer = SignalLagAnalyzer(A_SEGS, B_SEGS)
    per_lag = brute_per_lag(A_SEGS, B_SEGS, 990, 1010)
    _, best_lags = brute_best(per_lag, MIN_COV)
    assert result.best_lags == best_lags
    assert all(990 <= lag <= 1010 for lag in result.best_lags)
    assert analyzer  # analyzer reusable for other windows


# ---------------------------------------------------------------------------
# the same pattern at huge coordinates with a day-spanning lag search
# ---------------------------------------------------------------------------

OFFSET = 10 ** 12
DAY_MS = 86_400_000


def shifted(segs, offset):
    return [(s + offset, e + offset, v) for s, e, v in segs]


def test_huge_coordinates_and_day_spanning_search():
    big_a = shifted(A_SEGS, OFFSET)
    big_b = shifted(B_SEGS, OFFSET)
    analyzer = SignalLagAnalyzer(big_a, big_b)
    result = analyzer.find_best(1000 - 3 * DAY_MS, 1000 + 3 * DAY_MS, MIN_COV)
    small = find_best_lags(A_SEGS, B_SEGS, LAG_MIN, LAG_MAX, MIN_COV)
    # identical pattern -> identical best lags, ties and undefined ranges
    assert result.best_lags == small.best_lags
    assert result.best_correlation == 1.0
    assert result.undefined == small.undefined
    assert result.n_lags_with_coverage == small.n_lags_with_coverage
    stats = analyzer.stats_at(1000)
    assert stats.coverage == 30
    assert stats.mean_a == pytest.approx(190 / 30)


def test_half_open_boundaries_at_huge_coordinates():
    off = OFFSET
    analyzer = SignalLagAnalyzer([(off, off + 10, 1)], [(off, off + 10, 1)])
    assert analyzer.stats_at(0).coverage == 10
    assert analyzer.stats_at(9).coverage == 1
    assert analyzer.stats_at(10).coverage == 0   # [s, e) does not touch [e, ...)
    assert analyzer.stats_at(-9).coverage == 1
    assert analyzer.stats_at(-10).coverage == 0


def test_long_constant_segments_without_expansion():
    half = 500_000_000
    segs_a = [(0, half, 1), (half, 2 * half, 2)]
    segs_b = [(0, half, 1), (half, 2 * half, 2)]
    result = find_best_lags(segs_a, segs_b, -2 * half, 2 * half, 1)
    assert result.best_lags == [0]
    assert result.best_correlation == 1.0
    assert result.best_coverage == 2 * half


def test_plateau_all_tied_lags_kept():
    # identical triangle overlaps of (1,1) and (2,2) pairs: r == 1 for a
    # whole stretch of lags, and every one of them must be reported
    segs_a = [(0, 100, 1), (1000, 1100, 2)]
    segs_b = [(50, 150, 1), (1050, 1150, 2)]
    result = find_best_lags(segs_a, segs_b, -300, 300, 1)
    assert result.best_correlation == 1.0
    assert result.best_lags == list(range(-49, 150))


# ---------------------------------------------------------------------------
# constant-only lines: correlation undefined, listed separately with reason
# ---------------------------------------------------------------------------


def test_both_signals_constant():
    result = find_best_lags([(0, 1000, 3)], [(500, 2000, 7)], -1000, 2500, 1)
    assert result.best_lags == []
    assert result.best_correlation is None
    assert result.best_coverage is None
    # common coverage exists for integer lags -499..1999, all undefined
    assert result.undefined == [(-499, 2000, REASON_CONSTANT_BOTH)]
    assert result.n_lags_with_coverage == 2499


def test_one_signal_constant():
    segs_a = [(0, 1000, 3)]
    segs_b = [(500, 700, 1), (700, 2000, 2)]
    result = find_best_lags(segs_a, segs_b, -1000, 2500, 1)
    assert result.best_lags == []
    assert result.best_correlation is None
    # where only one B segment overlaps, both sides are constant there
    assert result.undefined == [
        (-499, -299, REASON_CONSTANT_BOTH),
        (-299, 700, REASON_CONSTANT_A),
        (700, 2000, REASON_CONSTANT_BOTH),
    ]


def test_constant_stretch_only_on_common_coverage():
    # A varies globally but is constant wherever it meets B
    segs_a = [(0, 100, 4), (100, 200, 4), (300, 400, 9)]
    segs_b = [(0, 200, 7)]
    analyzer = SignalLagAnalyzer(segs_a, segs_b)
    stats = analyzer.stats_at(0)
    assert stats.coverage == 200
    assert stats.correlation is None
    assert stats.reason == REASON_CONSTANT_BOTH


# ---------------------------------------------------------------------------
# minimum common coverage
# ---------------------------------------------------------------------------


def test_min_coverage_boundary():
    segs = [(0, 10, 1), (100, 110, 2)]
    included = find_best_lags(segs, segs, -200, 200, 20)
    assert included.best_lags == [0]
    assert included.best_coverage == 20
    excluded = find_best_lags(segs, segs, -200, 200, 21)
    assert excluded.best_lags == []
    assert excluded.best_correlation is None
    assert excluded.n_lags_with_coverage == 0


def test_negative_correlation_is_worse_than_zero():
    segs_a = [(0, 10, 1), (10, 20, 3)]
    segs_b = [(0, 10, 3), (10, 20, 1)]  # anti-correlated at lag 0
    result = find_best_lags(segs_a, segs_b, 0, 0, 1)
    assert result.best_lags == [0]
    assert result.best_correlation == -1.0


# ---------------------------------------------------------------------------
# input validation
# ---------------------------------------------------------------------------


def test_overlapping_segments_rejected():
    with pytest.raises(ValueError):
        SignalLagAnalyzer([(0, 10, 1), (5, 12, 2)], [(0, 5, 1)])


def test_empty_segment_rejected():
    with pytest.raises(ValueError):
        SignalLagAnalyzer([(10, 10, 1)], [(0, 5, 1)])


def test_float_values_supported():
    segs_a = [(0, 10, 1.5), (20, 30, 2.25)]
    segs_b = [(0, 10, 1.5), (20, 30, 2.25)]
    result = find_best_lags(segs_a, segs_b, -50, 50, 5)
    assert result.best_correlation == 1.0
    assert 0 in result.best_lags


# ---------------------------------------------------------------------------
# randomized cross-check against the brute-force reference
# ---------------------------------------------------------------------------


def random_segments(rng, n_segments, max_coord):
    segs = []
    cursor = 0
    for _ in range(n_segments):
        cursor += rng.randint(0, 6)
        length = rng.randint(1, 8)
        segs.append((cursor, cursor + length, rng.randint(0, 4)))
        cursor += length
        if cursor > max_coord:
            break
    return segs


@pytest.mark.parametrize("seed", range(40))
def test_random_against_brute_force(seed):
    rng = random.Random(seed)
    segs_a = random_segments(rng, rng.randint(1, 6), 70)
    segs_b = random_segments(rng, rng.randint(1, 6), 70)
    if not segs_a or not segs_b:
        pytest.skip("degenerate draw")
    min_cov = rng.choice([1, 3, 7])
    analyzer = SignalLagAnalyzer(segs_a, segs_b)
    result = analyzer.find_best(-90, 90, min_cov)
    per_lag = brute_per_lag(segs_a, segs_b, -90, 90)
    assert_matches_brute(analyzer, result, per_lag, -90, 90, min_cov)


@pytest.mark.parametrize("seed", range(10))
def test_random_long_pieces_against_brute_force(seed):
    """Larger coordinates force long linear pieces (the non-dense search path)."""
    rng = random.Random(10_000 + seed)
    segs_a = random_segments(rng, rng.randint(2, 4), 1500)
    segs_b = random_segments(rng, rng.randint(2, 4), 1500)
    # stretch the segments so pieces between knots are long
    factor = rng.choice([21, 51])
    segs_a = [(s * factor, e * factor, v) for s, e, v in segs_a]
    segs_b = [(s * factor, e * factor, v) for s, e, v in segs_b]
    if not segs_a or not segs_b:
        pytest.skip("degenerate draw")
    lag_max = 80 * factor
    min_cov = rng.choice([1, 20, 100])
    analyzer = SignalLagAnalyzer(segs_a, segs_b)
    result = analyzer.find_best(-lag_max, lag_max, min_cov)
    per_lag = brute_per_lag_pairwise(segs_a, segs_b, -lag_max, lag_max)
    assert_matches_brute(analyzer, result, per_lag, -lag_max, lag_max, min_cov)
