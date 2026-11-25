"""Find the stable clock offset between two piecewise-constant monitor signals.

Two unsynchronised monitors report contiguous segments; inside a segment the
signal value is constant, and coordinates where a monitor transmitted nothing
are *missing* (not zero).  Segments are half-open intervals ``[Start, End)``
on a millisecond coordinate axis and may sit at arbitrarily large coordinates.

For every integer candidate lag ``d`` this module computes the Pearson
correlation between signal A and signal B over exactly those positions ``x``
where *both* ``A[x]`` and ``B[x + d]`` are valid (positive lag means B shows
the same readings later than A, i.e. B's clock trails A's by ``d`` ms).
Missing intervals never become zeros and the per-lag means/variances come
only from the truly common coverage of that lag.  Lags whose common coverage
is shorter than ``min_coverage`` cannot win.  Lags where the correlation is
undefined because a signal is constant over the common coverage are reported
separately, with the reason.  All integer lags tied for the best correlation
are returned.

The algorithm never expands the signals to per-millisecond arrays and never
rescans the segments for every candidate lag.  For each pair of segments the
common coverage (and each sufficient statistic) is a tent-shaped function of
the lag, so every statistic is a piecewise-linear function of ``d`` with
``O(#A * #B)`` breakpoints.  Those breakpoints are applied as events, and
each linear piece is then searched exactly (integer arithmetic, Sturm root
isolation for the interior extrema of the rational correlation function), so
the cost is independent of the coordinate magnitude and of the width of the
lag search range.

Typical usage::

    import pyranges as pr
    from monitor_lag import find_best_lags

    result = find_best_lags(gr_a, gr_b, lag_min=-86_400_000,
                            lag_max=86_400_000, min_coverage=1000)
    result.best_lags          # all integer lags tied for the best correlation
    result.best_correlation   # None if every usable lag was undefined
    result.undefined          # [(start, end, reason), ...] half-open lag ranges
"""

from __future__ import annotations

import operator
from bisect import bisect_right
from dataclasses import dataclass
from fractions import Fraction
from math import ceil, floor, sqrt

import pandas as pd
import pyranges as pr

__all__ = [
    "LagStats",
    "LagSearchResult",
    "SignalLagAnalyzer",
    "find_best_lags",
    "REASON_NO_COVERAGE",
    "REASON_CONSTANT_A",
    "REASON_CONSTANT_B",
    "REASON_CONSTANT_BOTH",
]

REASON_NO_COVERAGE = "no common coverage"
REASON_CONSTANT_A = "signal A is constant over the common coverage (zero variance)"
REASON_CONSTANT_B = "signal B is constant over the common coverage (zero variance)"
REASON_CONSTANT_BOTH = "both signals are constant over the common coverage (zero variance)"

_STAT_KEYS = ("n", "sa", "sb", "saa", "sbb", "sab")
_DENSE_THRESHOLD = 64


# ---------------------------------------------------------------------------
# exact scalar helpers
# ---------------------------------------------------------------------------


def _as_coord(value):
    """Return ``value`` as a plain int, rejecting non-integral coordinates."""
    try:
        return operator.index(value)
    except TypeError:
        pass
    as_float = float(value)
    if as_float.is_integer():
        return int(as_float)
    raise ValueError(f"non-integer coordinate: {value!r}")


def _as_number(value):
    """Return an exact number: int when integral, Fraction otherwise."""
    try:
        return operator.index(value)
    except TypeError:
        pass
    if isinstance(value, Fraction):
        return value
    as_float = float(value)
    if as_float.is_integer():
        return int(as_float)
    return Fraction(as_float)


# ---------------------------------------------------------------------------
# exact polynomials (coefficient lists, lowest degree first, int/Fraction)
# ---------------------------------------------------------------------------


def _pstrip(poly):
    while len(poly) > 1 and poly[-1] == 0:
        poly.pop()
    return poly


def _piszero(poly):
    return all(coeff == 0 for coeff in poly)


def _padd(p, q):
    size = max(len(p), len(q))
    return _pstrip(
        [(p[i] if i < len(p) else 0) + (q[i] if i < len(q) else 0) for i in range(size)]
    )


def _psub(p, q):
    return _padd(p, [-coeff for coeff in q])


def _pscale(p, factor):
    return _pstrip([coeff * factor for coeff in p])


def _pmul(p, q):
    if _piszero(p) or _piszero(q):
        return [0]
    result = [0] * (len(p) + len(q) - 1)
    for i, a in enumerate(p):
        if a == 0:
            continue
        for j, b in enumerate(q):
            if b != 0:
                result[i + j] += a * b
    return _pstrip(result)


def _pder(p):
    if len(p) <= 1:
        return [0]
    return _pstrip([i * p[i] for i in range(1, len(p))])


def _peval(p, x):
    result = 0
    for coeff in reversed(p):
        result = result * x + coeff
    return result


def _prem(a, b):
    """Remainder of polynomial ``a`` divided by polynomial ``b`` (exact)."""
    a = list(a)
    deg_b = len(b) - 1
    lead_b = b[-1]
    while not _piszero(a) and len(a) - 1 >= deg_b:
        coeff = Fraction(a[-1]) / Fraction(lead_b)
        shift = len(a) - 1 - deg_b
        for i in range(deg_b + 1):
            a[shift + i] = a[shift + i] - coeff * b[i]
        _pstrip(a)
    return a


def _sturm_sequence(p):
    seq = [_pstrip(list(p))]
    deriv = _pder(p)
    if _piszero(deriv):
        return seq
    seq.append(deriv)
    while True:
        rem = _prem(seq[-2], seq[-1])
        rem = _pstrip([-coeff for coeff in rem])
        if _piszero(rem):
            return seq
        seq.append(rem)


def _sign_changes(seq, x):
    signs = []
    for poly in seq:
        value = _peval(poly, x)
        if value > 0:
            signs.append(1)
        elif value < 0:
            signs.append(-1)
    return sum(1 for i in range(1, len(signs)) if signs[i] != signs[i - 1])


def _sturm_bisect(seq, lo, hi, out):
    """Collect unit intervals (m, m+1] containing distinct roots in (lo, hi]."""
    if _sign_changes(seq, lo) - _sign_changes(seq, hi) <= 0:
        return
    if hi - lo <= 1:
        out.append((lo, hi))
        return
    mid = (lo + hi) // 2
    _sturm_bisect(seq, lo, mid, out)
    _sturm_bisect(seq, mid, hi, out)


def _integer_roots(poly, lo, hi):
    """Exact integer roots of ``poly`` in [lo, hi] (poly must not be identically 0)."""
    seq = _sturm_sequence(poly)
    intervals = []
    _sturm_bisect(seq, lo - 1, hi, intervals)
    roots = set()
    for m, m1 in intervals:
        for candidate in (m - 1, m, m1, m1 + 1):
            if lo <= candidate <= hi and _peval(poly, candidate) == 0:
                roots.add(candidate)
    return roots


def _candidates_near_roots(poly, lo, hi):
    """Integer points next to every real root of ``poly`` in the open interval (lo, hi)."""
    seq = _sturm_sequence(poly)
    intervals = []
    _sturm_bisect(seq, lo, hi, intervals)
    candidates = set()
    for m, m1 in intervals:
        for candidate in (m - 1, m, m1, m1 + 1):
            if lo < candidate < hi:
                candidates.add(candidate)
    return candidates


# ---------------------------------------------------------------------------
# correlation triples: r = C / sqrt(VA * VB), compared exactly
# ---------------------------------------------------------------------------


def _compare_r(t1, t2):
    """Compare r1 and r2 given as (C, VA, VB) triples with VA, VB > 0."""
    c1, va1, vb1 = t1
    c2, va2, vb2 = t2
    s1 = (c1 > 0) - (c1 < 0)
    s2 = (c2 > 0) - (c2 < 0)
    if s1 != s2:
        return 1 if s1 > s2 else -1
    if s1 == 0:
        return 0
    lhs = c1 * c1 * va2 * vb2
    rhs = c2 * c2 * va1 * vb1
    if lhs == rhs:
        return 0
    if s1 > 0:
        return 1 if lhs > rhs else -1
    return 1 if lhs < rhs else -1


def _triple_to_float(triple):
    c, va, vb = triple
    ratio = (c * c) / (va * vb)
    value = sqrt(float(ratio))
    return value if c >= 0 else -value


# ---------------------------------------------------------------------------
# input normalisation
# ---------------------------------------------------------------------------


def _to_segments(signal, value_column):
    """Normalise a signal to a sorted list of (start, end, value) segments.

    Accepts a PyRanges object, a pandas DataFrame, or an iterable of
    (start, end, value) triples.  Segments must be non-overlapping
    half-open intervals; book-ended segments with equal values are merged.
    """
    if isinstance(signal, pr.PyRanges):
        frame = signal.df
    elif isinstance(signal, pd.DataFrame):
        frame = signal
    else:
        frame = pd.DataFrame(list(signal), columns=["Start", "End", value_column])
    missing = {"Start", "End", value_column} - set(frame.columns)
    if missing:
        raise ValueError(f"signal is missing columns: {sorted(missing)}")
    rows = sorted(
        zip(frame["Start"], frame["End"], frame[value_column]),
        key=lambda row: (row[0], row[1]),
    )
    segments = []
    for start, end, value in rows:
        start = _as_coord(start)
        end = _as_coord(end)
        value = _as_number(value)
        if end <= start:
            raise ValueError(f"segment with End <= Start: [{start}, {end})")
        if segments and start < segments[-1][1]:
            raise ValueError(f"overlapping segments near coordinate {start}")
        if segments and start == segments[-1][1] and value == segments[-1][2]:
            segments[-1] = (segments[-1][0], end, value)
        else:
            segments.append((start, end, value))
    return segments


# ---------------------------------------------------------------------------
# piecewise-linear sufficient statistics as a function of the lag
# ---------------------------------------------------------------------------


class _StatCurve:
    """A piecewise-linear function of the lag, stored as tent events.

    Events are second-difference impulses: the value at integer lag ``d`` is
    ``sum(weight * max(0, d - pos))`` over all events, which is exact for
    integer positions.
    """

    def __init__(self, events):
        events.sort()
        self.pos = []
        self.cum_w = []
        self.cum_wp = []
        w_sum = 0
        wp_sum = 0
        i = 0
        while i < len(events):
            pos = events[i][0]
            weight = 0
            while i < len(events) and events[i][0] == pos:
                weight += events[i][1]
                i += 1
            w_sum += weight
            wp_sum += weight * pos
            self.pos.append(pos)
            self.cum_w.append(w_sum)
            self.cum_wp.append(wp_sum)

    def value(self, d):
        idx = bisect_right(self.pos, d) - 1
        if idx < 0:
            return 0
        return self.cum_w[idx] * d - self.cum_wp[idx]

    def linear_coeffs(self, d):
        """(slope, intercept) valid on the linear piece containing ``d``."""
        idx = bisect_right(self.pos, d) - 1
        if idx < 0:
            return 0, 0
        return self.cum_w[idx], -self.cum_wp[idx]


# ---------------------------------------------------------------------------
# result containers
# ---------------------------------------------------------------------------


@dataclass
class LagStats:
    """Exact statistics of one candidate lag, over its true common coverage."""

    lag: int
    coverage: int
    mean_a: float | None
    mean_b: float | None
    correlation: float | None
    reason: str | None  # why the correlation is undefined (None when defined)


@dataclass
class LagSearchResult:
    """Outcome of a lag search.

    ``undefined`` holds half-open lag ranges ``(start, end, reason)`` where
    the common coverage met ``min_coverage`` but the correlation is undefined
    because a signal was constant there.  Lags with too little common
    coverage are simply excluded from winning (counted in
    ``n_lags_with_coverage``'s complement).
    """

    lag_min: int
    lag_max: int
    min_coverage: int
    best_lags: list
    best_correlation: float | None
    best_coverage: int | None
    best_mean_a: float | None
    best_mean_b: float | None
    n_lags_with_coverage: int
    undefined: list


# ---------------------------------------------------------------------------
# the analyzer
# ---------------------------------------------------------------------------


class SignalLagAnalyzer:
    """Precomputed lag structure of two piecewise-constant signals.

    At lag ``d`` the comparison pairs every millisecond ``x`` where A is
    valid with ``B[x + d]`` where that is valid; missing intervals never
    contribute (and never become zeros).
    """

    def __init__(self, signal_a, signal_b, value_column="Value"):
        self.segments_a = _to_segments(signal_a, value_column)
        self.segments_b = _to_segments(signal_b, value_column)
        events = {key: [] for key in _STAT_KEYS}
        for a_start, a_end, va in self.segments_a:
            for b_start, b_end, vb in self.segments_b:
                # Common coverage of this pair at lag d is a tent on
                # (b_start - a_end, b_end - a_start); breakpoints below.
                p1 = b_start - a_end
                p4 = b_end - a_start
                q1 = b_start - a_start
                q2 = b_end - a_end
                p2, p3 = (q1, q2) if q1 <= q2 else (q2, q1)
                weights = (1, va, vb, va * va, vb * vb, va * vb)
                for key, weight in zip(_STAT_KEYS, weights):
                    ev = events[key]
                    ev.append((p1, weight))
                    ev.append((p2, -weight))
                    ev.append((p3, -weight))
                    ev.append((p4, weight))
        self._curves = {key: _StatCurve(ev) for key, ev in events.items()}
        knots = set()
        for curve in self._curves.values():
            knots.update(curve.pos)
        self._knots = sorted(knots)

    # -- exact per-lag statistics ------------------------------------------

    def sufficient_stats(self, lag):
        """Exact (n, sum a, sum b, sum a^2, sum b^2, sum ab) over the common
        coverage of ``lag``; all values are ints or Fractions."""
        d = _as_coord(lag)
        return {key: self._curves[key].value(d) for key in _STAT_KEYS}

    def _triple_at(self, d):
        stats = self.sufficient_stats(d)
        n = stats["n"]
        sa, sb = stats["sa"], stats["sb"]
        saa, sbb, sab = stats["saa"], stats["sbb"], stats["sab"]
        return (n * sab - sa * sb, n * saa - sa * sa, n * sbb - sb * sb)

    def stats_at(self, lag):
        """Coverage, means and Pearson correlation of one candidate lag."""
        d = _as_coord(lag)
        stats = self.sufficient_stats(d)
        n = stats["n"]
        if n == 0:
            return LagStats(d, 0, None, None, None, REASON_NO_COVERAGE)
        mean_a = float(stats["sa"]) / float(n)
        mean_b = float(stats["sb"]) / float(n)
        c, va, vb = self._triple_at(d)
        if va == 0 or vb == 0:
            if va == 0 and vb == 0:
                reason = REASON_CONSTANT_BOTH
            elif va == 0:
                reason = REASON_CONSTANT_A
            else:
                reason = REASON_CONSTANT_B
            return LagStats(d, n, mean_a, mean_b, None, reason)
        return LagStats(d, n, mean_a, mean_b, _triple_to_float((c, va, vb)), None)

    # -- the search ---------------------------------------------------------

    def find_best(self, lag_min, lag_max, min_coverage=1):
        """Search every integer lag in [lag_min, lag_max].

        Returns a LagSearchResult with the best correlation, every integer
        lag tied for it, and the constant-signal undefined ranges.
        """
        lag_min = _as_coord(lag_min)
        lag_max = _as_coord(lag_max)
        min_coverage = _as_coord(min_coverage)
        if lag_max < lag_min:
            raise ValueError("lag_max must be >= lag_min")
        if min_coverage < 1:
            raise ValueError("min_coverage must be >= 1")

        knots = self._knots
        best_triple = None
        best_lags = []
        undefined = []
        n_with_coverage = 0

        def consider(triple, lags):
            nonlocal best_triple, best_lags
            if best_triple is None or _compare_r(triple, best_triple) > 0:
                best_triple = triple
                best_lags = list(lags)
            elif _compare_r(triple, best_triple) == 0:
                best_lags.extend(lags)

        for i in range(len(knots) - 1):
            d0 = max(knots[i], lag_min)
            d1 = min(knots[i + 1] - 1, lag_max)
            if d0 > d1:
                continue
            linear = {
                key: self._curves[key].linear_coeffs(knots[i]) for key in _STAT_KEYS
            }
            wn, intercept_n = linear["n"]
            # restrict the piece to lags with enough common coverage
            lo, hi = d0, d1
            if wn > 0:
                lo = max(lo, ceil(Fraction(min_coverage - intercept_n, wn)))
            elif wn < 0:
                hi = min(hi, floor(Fraction(min_coverage - intercept_n, wn)))
            elif intercept_n < min_coverage:
                continue
            if lo > hi:
                continue
            c0, c1 = lo, hi
            n_with_coverage += c1 - c0 + 1

            # sufficient statistics as exact polynomials in t = d - c0
            def base(key):
                slope, intercept = linear[key]
                return slope * c0 + intercept

            n_p = [base("n"), linear["n"][0]]
            sa_p = [base("sa"), linear["sa"][0]]
            sb_p = [base("sb"), linear["sb"][0]]
            saa_p = [base("saa"), linear["saa"][0]]
            sbb_p = [base("sbb"), linear["sbb"][0]]
            sab_p = [base("sab"), linear["sab"][0]]
            c_poly = _psub(_pmul(n_p, sab_p), _pmul(sa_p, sb_p))
            va_poly = _psub(_pmul(n_p, saa_p), _pmul(sa_p, sa_p))
            vb_poly = _psub(_pmul(n_p, sbb_p), _pmul(sb_p, sb_p))

            va_const = _piszero(va_poly)
            vb_const = _piszero(vb_poly)
            if va_const or vb_const:
                if va_const and vb_const:
                    undefined.append((c0, c1 + 1, REASON_CONSTANT_BOTH))
                    continue
                const_reason = REASON_CONSTANT_A if va_const else REASON_CONSTANT_B
                other_poly = vb_poly if va_const else va_poly
                # isolated points where the other signal is constant too
                both_points = sorted(
                    c0 + t for t in _integer_roots(other_poly, 0, c1 - c0)
                )
                start = c0
                for d in both_points:
                    if start <= d - 1:
                        undefined.append((start, d, const_reason))
                    undefined.append((d, d + 1, REASON_CONSTANT_BOTH))
                    start = d + 1
                if start <= c1:
                    undefined.append((start, c1 + 1, const_reason))
                continue

            width = c1 - c0
            undef_points = {}
            for t in _integer_roots(va_poly, 0, width):
                undef_points.setdefault(c0 + t, set()).add("a")
            for t in _integer_roots(vb_poly, 0, width):
                undef_points.setdefault(c0 + t, set()).add("b")
            for d in sorted(undef_points):
                sides = undef_points[d]
                if sides == {"a", "b"}:
                    reason = REASON_CONSTANT_BOTH
                elif "a" in sides:
                    reason = REASON_CONSTANT_A
                else:
                    reason = REASON_CONSTANT_B
                undefined.append((d, d + 1, reason))

            sub_intervals = []
            u = c0
            for d in sorted(undef_points):
                if u <= d - 1:
                    sub_intervals.append((u, d - 1))
                u = d + 1
            if u <= c1:
                sub_intervals.append((u, c1))

            deriv_poly = None
            for u, v in sub_intervals:
                if v - u + 1 <= _DENSE_THRESHOLD:
                    for d in range(u, v + 1):
                        consider(self._triple_at(d), [d])
                    continue
                if deriv_poly is None:
                    dc = _pder(c_poly)
                    dva = _pder(va_poly)
                    dvb = _pder(vb_poly)
                    # r' = 0  <=>  2 C' VA VB - C (VA' VB + VA VB') = 0
                    deriv_poly = _psub(
                        _pmul(_pscale(_pmul(dc, va_poly), 2), vb_poly),
                        _pmul(
                            c_poly,
                            _padd(_pmul(dva, vb_poly), _pmul(va_poly, dvb)),
                        ),
                    )
                if _piszero(deriv_poly):
                    # r is constant on this piece: every lag ties.
                    consider(self._triple_at(u), range(u, v + 1))
                    continue
                candidates = {u, v}
                for t in _candidates_near_roots(deriv_poly, u - c0, v - c0):
                    candidates.add(c0 + t)
                for d in sorted(candidates):
                    consider(self._triple_at(d), [d])

        if best_triple is None:
            correlation = coverage = mean_a = mean_b = None
        else:
            correlation = _triple_to_float(best_triple)
            stats = self.sufficient_stats(best_lags[0])
            coverage = stats["n"]
            mean_a = float(stats["sa"]) / float(coverage)
            mean_b = float(stats["sb"]) / float(coverage)

        return LagSearchResult(
            lag_min=lag_min,
            lag_max=lag_max,
            min_coverage=min_coverage,
            best_lags=sorted(best_lags),
            best_correlation=correlation,
            best_coverage=coverage,
            best_mean_a=mean_a,
            best_mean_b=mean_b,
            n_lags_with_coverage=n_with_coverage,
            undefined=_merge_ranges(undefined),
        )


def _merge_ranges(ranges):
    """Merge adjacent half-open (start, end, reason) ranges with equal reason."""
    merged = []
    for start, end, reason in sorted(ranges):
        if merged and merged[-1][2] == reason and start <= merged[-1][1]:
            prev_start, prev_end, prev_reason = merged[-1]
            merged[-1] = (prev_start, max(prev_end, end), prev_reason)
        else:
            merged.append((start, end, reason))
    return merged


def find_best_lags(signal_a, signal_b, lag_min, lag_max, min_coverage=1,
                   value_column="Value"):
    """Convenience wrapper: build a SignalLagAnalyzer and search the lag range."""
    analyzer = SignalLagAnalyzer(signal_a, signal_b, value_column=value_column)
    return analyzer.find_best(lag_min, lag_max, min_coverage)
