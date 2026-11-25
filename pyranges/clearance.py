"""Maintenance-window release planning with PyRanges geometry.

The planner answers one operational question: given the closure orders of two
(or more) rail circuits and the mileage scopes in which each approval can lift
them, which *new* approvals must be chased so that every mileage of a planned
half-open window has at least one passable circuit?

Geometry (which fragments of a closure are still in force after applying the
already granted approvals, and which fragments no approval scope can ever
reach) is computed with PyRanges joins plus keyed interval subtraction.  The
minimum set of additional approvals, the deterministic tie-broken release plan
and the approvals mandatory across every minimum plan are then derived with a
bit-mask sweep-line dynamic program. The desk quota (at most ten approval
spans overlapping any mileage) bounds each layer's active universe to ten
approvals, so a layer never holds more than ``2 ** 10`` masks; Pareto pruning
keeps the observed front tiny on the production scale.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import pyranges as pr

_CHROM = "chr1"
_CLOSURE_COLS = ("CircuitId", "Start", "End", "BlockId", "ApprovalId")
_SCOPE_COLS = ("ApprovalId", "Start", "End")
BlockPair = tuple[tuple[str, str], ...]


def _to_frame(data: Any, required: Sequence[str]) -> pd.DataFrame:
    if isinstance(data, pr.PyRanges):
        frame = data.df.drop(columns=["Chromosome", "Strand"], errors="ignore").copy()
    elif isinstance(data, pd.DataFrame):
        frame = data.copy()
    elif isinstance(data, Mapping):
        frame = pd.DataFrame(data)
    elif isinstance(data, Iterable):
        frame = pd.DataFrame(list(data))
    else:
        raise TypeError(f"cannot interpret {type(data)!r} as a record collection")
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    frame = frame[list(required)].copy()
    if frame.isnull().any().any():
        raise ValueError("null values are not allowed in closure/scope records")
    for col in ("Start", "End"):
        frame[col] = frame[col].astype(np.int64)
    if (frame["End"] <= frame["Start"]).any():
        raise ValueError("all mileage intervals must be half-open with End > Start")
    for col in ("CircuitId", "BlockId", "ApprovalId"):
        if col in frame.columns:
            frame[col] = frame[col].astype(str)
    return frame.sort_values(list(required)).reset_index(drop=True)


def _sorted_pairs(items: Iterable[tuple[str, str]]) -> BlockPair:
    return tuple(sorted(set(items)))


@dataclass(frozen=True)
class PlanSegment:
    """One elementary mileage slice of the answer."""

    start: int
    end: int
    circuit: str | None
    approvals: tuple[str, ...]
    blocked: Mapping[str, BlockPair] = field(default_factory=dict)
    permanent: Mapping[str, BlockPair] = field(default_factory=dict)


@dataclass(frozen=True)
class InfeasibleRegion:
    start: int
    end: int
    blocked: Mapping[str, BlockPair]


@dataclass(frozen=True)
class ClearanceResult:
    feasible: bool
    plan_start: int
    plan_end: int
    approved: tuple[str, ...]
    new_approval_count: int
    new_approvals: tuple[str, ...]
    mandatory: tuple[str, ...]
    segments: tuple[PlanSegment, ...]
    runs: tuple[PlanSegment, ...]
    infeasible_regions: tuple[InfeasibleRegion, ...]

    def describe(self) -> str:
        lines = [
            f"plan [{self.plan_start},{self.plan_end})",
            f"feasible: {self.feasible}",
            f"approved on entry: {list(self.approved)}",
        ]
        if self.feasible:
            lines.append(
                f"new approvals needed: {self.new_approval_count} {list(self.new_approvals)}"
            )
            lines.append(f"mandatory in every minimum plan: {list(self.mandatory)}")
            lines.append("release by mileage:")
            for seg in self.runs:
                lines.append(
                    f"  [{seg.start},{seg.end}) via {seg.circuit} using {list(seg.approvals)}"
                )
        if self.infeasible_regions:
            lines.append(
                "mileage that cannot be released even if every pending approval is granted:"
            )
            for region in self.infeasible_regions:
                lines.append(f"  [{region.start},{region.end})")
                for circuit in sorted(region.blocked):
                    detail = ", ".join(
                        f"{block} (needs {approval})"
                        for block, approval in region.blocked[circuit]
                    )
                    lines.append(f"    {circuit}: {detail}")
        return "\n".join(lines)


class ClearancePlanner:
    """Pre-process closure orders and approval scopes for repeated queries.

    Parameters
    ----------
    closures:
        Records with columns ``CircuitId, Start, End, BlockId, ApprovalId``.
        A block stays closed until every closure fragment touching it is
        lifted; each closure fragment names the single approval that can lift
        it.
    scopes:
        Records with columns ``ApprovalId, Start, End``. An approval takes
        effect on every declared scope at once and may be declared in several
        disjoint spans.
    """

    def __init__(self, closures: Any, scopes: Any) -> None:
        self.closures = _to_frame(closures, _CLOSURE_COLS)
        self.scopes = _to_frame(scopes, _SCOPE_COLS)
        if self.closures.empty:
            raise ValueError("at least one closure record is required")
        self.circuits: tuple[str, ...] = tuple(
            sorted(self.closures["CircuitId"].unique())
        )
        self._closure_approval_ids = frozenset(self.closures["ApprovalId"])
        all_approvals = tuple(
            sorted(set(self.scopes["ApprovalId"]) | self._closure_approval_ids)
        )
        self._global_bit = {
            approval: 1 << idx for idx, approval in enumerate(all_approvals)
        }
        self._global_name = {
            bit: approval for approval, bit in self._global_bit.items()
        }
        self._span_cache: pd.DataFrame | None = None

    def quota_peak(self) -> int:
        """Maximum approval-desk slots occupied at any mileage.

        An approval occupies one slot from its first effective mileage to its
        last effective mileage, including gaps between its disjoint scopes,
        per the desk's counting rule.
        """
        spans = self._approval_spans()
        if spans.empty:
            return 0
        endpoints = np.concatenate(
            [
                np.column_stack(
                    [spans["Start"].to_numpy(), np.ones(len(spans), dtype=np.int8)]
                ),
                np.column_stack(
                    [spans["End"].to_numpy(), -np.ones(len(spans), dtype=np.int8)]
                ),
            ]
        )
        order = np.lexsort((endpoints[:, 1], endpoints[:, 0]))
        endpoints = endpoints[order]
        active = peak = 0
        for delta in endpoints[:, 1]:
            active += int(delta)
            peak = max(peak, active)
        return peak

    def _approval_spans(self) -> pd.DataFrame:
        if self._span_cache is None:
            self._span_cache = (
                self.scopes.groupby("ApprovalId", sort=True)
                .agg(Start=("Start", "min"), End=("End", "max"))
                .reset_index()
            )
        return self._span_cache

    def plan(
        self, start: int, end: int, approved: Iterable[str] = ()
    ) -> ClearanceResult:
        if end <= start:
            raise ValueError("plan window must be half-open with end > start")
        approved_ids = tuple(sorted({str(a) for a in approved}))
        approved_set = frozenset(approved_ids)

        residual = self._residual_closures(start, end, approved_set)
        tagged = self._tag_permanence(residual, start, end)
        segments = self._build_segments(start, end, tagged)
        infeasible = [
            seg
            for seg in segments
            if all(circuit in seg.permanent for circuit in self.circuits)
        ]

        if infeasible:
            return ClearanceResult(
                feasible=False,
                plan_start=start,
                plan_end=end,
                approved=approved_ids,
                new_approval_count=0,
                new_approvals=(),
                mandatory=(),
                segments=tuple(segments),
                runs=tuple(self._merge_runs(segments)),
                infeasible_regions=tuple(self._merge_infeasible(infeasible)),
            )

        final_states, history, final_name_of_bit = self._dp(segments)
        last = history[-1]

        def active_names(mask: int) -> frozenset[str]:
            return frozenset(
                name for bit, name in final_name_of_bit.items() if mask & bit
            )

        best_size = min(payload[0] for payload in final_states.values())
        optimal_keys = [
            key for key, payload in final_states.items() if payload[0] == best_size
        ]
        canonical_key = min(
            optimal_keys,
            key=lambda item: (tuple(sorted(active_names(item[0]))), item[1]),
        )
        mandatory_set: frozenset[str] | None = None
        for key in optimal_keys:
            family = self._global_to_names(final_states[key][1])
            mandatory_set = family if mandatory_set is None else mandatory_set & family
        mandatory = tuple(sorted(mandatory_set or ()))

        target_index = int(
            np.flatnonzero(
                (last["masks"] == canonical_key[0])
                & (last["locked"] == canonical_key[1])
            )[0]
        )
        choices = self._backtrack(history, target_index)
        canonical_approvals = tuple(
            sorted({approval for _, req in choices for approval in req})
        )
        chosen_segments = tuple(
            PlanSegment(
                seg.start,
                seg.end,
                choice_circuit,
                choice_approvals,
                seg.blocked,
                seg.permanent,
            )
            for seg, (choice_circuit, choice_approvals) in zip(segments, choices)
        )
        return ClearanceResult(
            feasible=True,
            plan_start=start,
            plan_end=end,
            approved=approved_ids,
            new_approval_count=best_size,
            new_approvals=canonical_approvals,
            mandatory=mandatory,
            segments=chosen_segments,
            runs=tuple(self._merge_runs(chosen_segments)),
            infeasible_regions=(),
        )

    def _clip(self, frame: pd.DataFrame, start: int, end: int) -> pd.DataFrame:
        clipped = frame.loc[(frame["End"] > start) & (frame["Start"] < end)].copy()
        clipped["Start"] = clipped["Start"].clip(lower=start)
        clipped["End"] = clipped["End"].clip(upper=end)
        return clipped.reset_index(drop=True)

    def _residual_closures(
        self, start: int, end: int, approved: frozenset
    ) -> pd.DataFrame:
        clipped = self._clip(self.closures, start, end)
        if clipped.empty or not approved:
            return self._finalize_residual(clipped)

        effective_approved = approved & self._closure_approval_ids
        if not effective_approved:
            return self._finalize_residual(clipped)

        scoped = self._clip(
            self.scopes.loc[self.scopes["ApprovalId"].isin(effective_approved)],
            start,
            end,
        )
        if scoped.empty:
            return self._finalize_residual(clipped)

        covered = self._keyed_overlaps(clipped, "ApprovalId", scoped, "ApprovalId")
        if covered.empty:
            return self._finalize_residual(clipped)
        residual = self._keyed_subtract(clipped, covered, "ApprovalId")
        return self._finalize_residual(residual)

    def _keyed_overlaps(
        self,
        source: pd.DataFrame,
        source_key: str,
        cover: pd.DataFrame,
        cover_key: str,
    ) -> pd.DataFrame:
        """Return merged overlap intervals of matching keys, via PyRanges join."""
        indexed = source.reset_index(drop=True).copy()
        indexed["_sid"] = np.arange(len(indexed), dtype=np.int64)
        source_pr = pr.PyRanges(
            pd.DataFrame(
                {
                    "Chromosome": _CHROM,
                    "Start": indexed["Start"],
                    "End": indexed["End"],
                    "_key": indexed[source_key].astype(str),
                    "_sid": indexed["_sid"],
                }
            )
        )
        cover_pr = pr.PyRanges(
            pd.DataFrame(
                {
                    "Chromosome": _CHROM,
                    "Start": cover["Start"],
                    "End": cover["End"],
                    "_key": cover[cover_key].astype(str),
                }
            )
        )
        joined_pr = source_pr.join(cover_pr, suffix="_c")
        if joined_pr.empty:
            return pd.DataFrame(columns=["_sid", "Start", "End"])
        joined = joined_pr.df
        joined = joined[joined["_key"] == joined["_key_c"]]
        if joined.empty:
            return pd.DataFrame(columns=["_sid", "Start", "End"])
        overlaps = pd.DataFrame(
            {
                "Chromosome": _CHROM,
                "Start": np.maximum(
                    joined["Start"].to_numpy(), joined["Start_c"].to_numpy()
                ),
                "End": np.minimum(joined["End"].to_numpy(), joined["End_c"].to_numpy()),
                "_sid": joined["_sid"].to_numpy(),
            }
        )
        merged = pr.PyRanges(overlaps).merge(by="_sid").df.drop(columns=["Chromosome"])
        return merged.reset_index(drop=True)

    @staticmethod
    def _keyed_subtract(
        source: pd.DataFrame, covered: pd.DataFrame, key_col: str
    ) -> pd.DataFrame:
        """Subtract each row's own-key coverage; rows keep every other column."""
        indexed = source.reset_index(drop=True).copy()
        indexed["_sid"] = np.arange(len(indexed), dtype=np.int64)
        covers: dict[int, np.ndarray] = {}
        if not covered.empty:
            for sid, group in covered.groupby("_sid", sort=True):
                covers[int(sid)] = group[["Start", "End"]].to_numpy()

        rows: list[dict] = []
        for record in indexed.to_dict("records"):
            intervals = covers.get(int(record["_sid"]))
            if intervals is None or len(intervals) == 0:
                rows.append(dict(record))
                continue
            cursor = int(record["Start"])
            for cover_start, cover_end in intervals:
                cover_start, cover_end = int(cover_start), int(cover_end)
                if cover_end <= cursor:
                    continue
                if cover_start >= record["End"]:
                    break
                if cover_start > cursor:
                    piece = dict(record)
                    piece["Start"], piece["End"] = cursor, cover_start
                    rows.append(piece)
                cursor = max(cursor, cover_end)
            if cursor < record["End"]:
                piece = dict(record)
                piece["Start"], piece["End"] = cursor, int(record["End"])
                rows.append(piece)
        result = pd.DataFrame(rows)
        standard = [column for column in source.columns if column != "_sid"]
        if result.empty:
            return pd.DataFrame(columns=standard)
        return result[standard]

    @staticmethod
    def _finalize_residual(frame: pd.DataFrame) -> pd.DataFrame:
        frame = frame.drop(columns=["_sid"], errors="ignore")
        return frame.sort_values(
            ["Start", "End", "CircuitId", "BlockId", "ApprovalId"]
        ).reset_index(drop=True)

    def _tag_permanence(
        self, residual: pd.DataFrame, start: int, end: int
    ) -> pd.DataFrame:
        """Split every residual fragment into liftable and permanent pieces.

        A piece is permanent when no scope of the very approval named on the
        closure reaches it; granting every pending approval cannot remove it.
        Scopes of one approval are merged first, so disjoint scopes with gaps
        leave the gap permanently blocked. Overlap geometry uses PyRanges.
        """
        if residual.empty:
            tagged = residual.copy()
            tagged["Permanent"] = pd.Series(dtype=bool)
            return tagged

        scoped = self._clip(
            self.scopes.loc[
                self.scopes["ApprovalId"].isin(set(residual["ApprovalId"]))
            ],
            start,
            end,
        )
        base_columns = ["CircuitId", "Start", "End", "BlockId", "ApprovalId"]
        if scoped.empty:
            tagged = residual[base_columns].copy()
            tagged["Permanent"] = True
            return tagged.sort_values(
                ["Start", "End", "CircuitId", "BlockId", "ApprovalId"]
            ).reset_index(drop=True)

        indexed = residual.reset_index(drop=True).copy()
        indexed["_sid"] = np.arange(len(indexed), dtype=np.int64)
        covered = self._keyed_overlaps(indexed, "ApprovalId", scoped, "ApprovalId")
        permanent = self._keyed_subtract(indexed, covered, "ApprovalId")
        frames: list[pd.DataFrame] = []
        if not permanent.empty:
            permanent = permanent[base_columns].copy()
            permanent["Permanent"] = True
            frames.append(permanent)

        if not covered.empty:
            liftable = covered.merge(
                indexed[["_sid", "CircuitId", "BlockId", "ApprovalId"]],
                on="_sid",
                how="left",
            )
            liftable = liftable[base_columns].copy()
            liftable["Permanent"] = False
            frames.append(liftable)

        tagged = pd.concat(frames, ignore_index=True)
        tagged["Start"] = tagged["Start"].astype(np.int64)
        tagged["End"] = tagged["End"].astype(np.int64)
        return tagged.sort_values(
            ["Start", "End", "CircuitId", "BlockId", "ApprovalId", "Permanent"]
        ).reset_index(drop=True)

    def _build_segments(
        self,
        start: int,
        end: int,
        tagged: pd.DataFrame,
    ) -> list[PlanSegment]:
        if tagged.empty:
            return [PlanSegment(start, end, self.circuits[0], (), {}, {})]

        boundaries = np.unique(
            np.concatenate(
                [
                    np.array([start, end], dtype=np.int64),
                    tagged["Start"].to_numpy(),
                    tagged["End"].to_numpy(),
                ]
            )
        )
        boundaries = boundaries[(boundaries >= start) & (boundaries <= end)]

        starts = tagged["Start"].to_numpy()
        ends = tagged["End"].to_numpy()
        circuits = tagged["CircuitId"].to_numpy()
        blocks = tagged["BlockId"].to_numpy()
        approvals = tagged["ApprovalId"].to_numpy()
        permanent_flag = tagged["Permanent"].to_numpy()
        order = np.argsort(starts, kind="stable")
        starts, ends = starts[order], ends[order]
        circuits, blocks = circuits[order], blocks[order]
        approvals, permanent_flag = approvals[order], permanent_flag[order]

        segments: list[PlanSegment] = []
        pointer = 0
        active: list[tuple[int, int]] = []
        for lo, hi in zip(boundaries[:-1], boundaries[1:]):
            while pointer < len(tagged) and starts[pointer] <= lo:
                heapq.heappush(active, (int(ends[pointer]), int(pointer)))
                pointer += 1
            while active and active[0][0] <= lo:
                heapq.heappop(active)

            liftable: dict[str, set[tuple[str, str]]] = {
                c: set() for c in self.circuits
            }
            permanent: dict[str, set[tuple[str, str]]] = {
                c: set() for c in self.circuits
            }
            for _, idx in active:
                pair = (str(blocks[idx]), str(approvals[idx]))
                if bool(permanent_flag[idx]):
                    permanent[str(circuits[idx])].add(pair)
                else:
                    liftable[str(circuits[idx])].add(pair)

            blocked = {
                circuit: _sorted_pairs(liftable[circuit] | permanent[circuit])
                for circuit in self.circuits
                if liftable[circuit] or permanent[circuit]
            }
            permanent_map = {
                circuit: _sorted_pairs(permanent[circuit])
                for circuit in self.circuits
                if permanent[circuit]
            }
            open_circuits = [
                circuit
                for circuit in self.circuits
                if not liftable[circuit] and not permanent[circuit]
            ]
            if open_circuits:
                segments.append(
                    PlanSegment(
                        int(lo), int(hi), open_circuits[0], (), blocked, permanent_map
                    )
                )
            else:
                segments.append(
                    PlanSegment(int(lo), int(hi), None, (), blocked, permanent_map)
                )
        return segments

    def _dp(self, segments: Sequence[PlanSegment]):
        """Bit-mask sweep-line DP bounded by the desk quota.

        At any mileage, only approvals whose effective span covers it can lift
        a closure fragment, so one segment's liftable approval universe has at
        most ``quota_peak`` members (<= 10 by contract) and its state space is
        at most ``2 ** quota_peak`` local masks. A state pairs the local mask
        with a locked count of selections whose span has expired, and carries a
        global big-integer intersection over the full selection sets of every
        minimum-cost path reaching it; that intersection yields the mandatory
        approvals. Pareto pruning removes subset-mask / no-larger-locked states.
        Adjacent segments with identical option structure share a macro layer,
        and backtracking re-expands them into elementary slices.
        """
        macro: list[dict] = []
        for segment in segments:
            options = tuple(
                sorted(
                    (
                        (
                            circuit,
                            tuple(
                                sorted(a for _, a in segment.blocked.get(circuit, ()))
                            ),
                        )
                        for circuit in self.circuits
                        if circuit not in segment.permanent
                    ),
                    key=lambda item: (item[0], item[1]),
                )
            )
            universe = tuple(sorted({a for _, req in options for a in req}))
            signature = (options, universe)
            if macro and macro[-1]["signature"] == signature:
                macro[-1]["slices"].append((segment.start, segment.end))
            else:
                macro.append(
                    {
                        "signature": signature,
                        "slices": [(segment.start, segment.end)],
                    }
                )

        history: list[dict] = []
        universal = -1  # sentinel: intersection of no paths yet; -1 & x == x
        # Forward states map (local mask, locked) -> (cost, global intersection).
        # The intersection is over the FULL selection sets of every minimum-cost
        # path reaching the key. Only compact numeric predecessor arrays are
        # persisted in history.
        states: dict[tuple[int, int], tuple[int, int]] = {(0, 0): (0, universal)}
        prev_name_of_bit: dict[int, str] = {}

        for entry in macro:
            options, universe = entry["signature"]
            name_of_bit = {1 << idx: name for idx, name in enumerate(universe)}
            bit_of_name = {name: bit for bit, name in name_of_bit.items()}
            option_global = tuple(
                self._names_to_global(req) for _circuit, req in options
            )

            # Expiry changes only the local mask representation; selection sets
            # are unchanged, so the carried intersection passes through verbatim.
            rolled: dict[tuple[int, int], tuple[int, int]] = {}
            roll_source: dict[tuple[int, int], int] = {}
            prev_keys = list(states)
            for prev_index, old_key in enumerate(prev_keys):
                cost, inter = states[old_key]
                old_mask, locked = old_key
                selected = [
                    name for bit, name in prev_name_of_bit.items() if old_mask & bit
                ]
                continuing = tuple(
                    sorted(name for name in selected if name in bit_of_name)
                )
                expired_count = len(selected) - len(continuing)
                base_mask = self._names_to_mask(continuing, bit_of_name)
                key = (base_mask, locked + expired_count)
                if key not in rolled:
                    rolled[key] = (cost, inter)
                    roll_source[key] = prev_index
                else:
                    rolled[key] = (rolled[key][0], rolled[key][1] & inter)

            # Choose one passable circuit inside this macro layer.
            option_masks = tuple(
                (circuit, req, self._names_to_mask(req, bit_of_name))
                for circuit, req in options
            )
            nxt: dict[tuple[int, int], int] = {}
            nxt_cost: list[int] = []
            nxt_inter: list[int] = []
            pred_prev: list[int] = []
            pred_option: list[int] = []
            for rolled_key, (cost, inter) in rolled.items():
                rolled_index = roll_source[rolled_key]
                mask, locked = rolled_key
                for option_index, (circuit, req, req_mask) in enumerate(option_masks):
                    new_mask = mask | req_mask
                    key = (new_mask, locked)
                    new_cost = locked + self._popcount(new_mask)
                    option_bits = option_global[option_index]
                    edge_inter = (
                        option_bits if inter == universal else inter | option_bits
                    )
                    if key not in nxt:
                        nxt[key] = len(nxt_cost)
                        nxt_cost.append(new_cost)
                        nxt_inter.append(edge_inter)
                        pred_prev.append(rolled_index)
                        pred_option.append(option_index)
                        continue
                    target = nxt[key]
                    if new_cost < nxt_cost[target]:
                        nxt_cost[target] = new_cost
                        nxt_inter[target] = edge_inter
                        pred_prev[target] = rolled_index
                        pred_option[target] = option_index
                    elif new_cost == nxt_cost[target]:
                        nxt_inter[target] &= edge_inter

            nxt_keys = self._pareto_prune(nxt, nxt_cost)
            kept_positions = [nxt[key] for key in nxt_keys]
            masks = np.fromiter(
                (key[0] for key in nxt_keys), dtype=np.int64, count=len(nxt_keys)
            )
            locked_arr = np.fromiter(
                (key[1] for key in nxt_keys), dtype=np.int32, count=len(nxt_keys)
            )
            history.append(
                {
                    "slices": entry["slices"],
                    "options": options,
                    "masks": masks,
                    "locked": locked_arr,
                    "pred_prev": np.asarray(
                        [pred_prev[pos] for pos in kept_positions], dtype=np.int32
                    ),
                    "pred_option": np.asarray(
                        [pred_option[pos] for pos in kept_positions], dtype=np.int16
                    ),
                }
            )
            states = {
                key: (nxt_cost[nxt[key]], nxt_inter[nxt[key]]) for key in nxt_keys
            }
            prev_name_of_bit = name_of_bit

        return states, history, prev_name_of_bit

    @staticmethod
    def _pareto_prune(key_to_index, costs):
        """Drop states dominated by a subset-mask, no-larger-locked state.

        A state (mask_a, locked_a) dominates (mask_b, locked_b) when mask_a is a
        subset of mask_b and locked_a <= locked_b: every completion available to
        the dominated state is reachable no more expensively from the dominator,
        so a minimum-cardinality optimum never needs the dominated state.
        """
        keys = list(key_to_index)
        keys.sort(key=lambda key: (costs[key_to_index[key]], key[1], key[0]))
        kept: list[tuple[int, int]] = []
        for key in keys:
            mask, locked = key
            if any(
                (kept_mask & mask) == kept_mask and kept_locked <= locked
                for kept_mask, kept_locked in kept
            ):
                continue
            kept.append(key)
        return kept

    def _names_to_global(self, names) -> int:
        mask = 0
        for name in names:
            mask |= self._global_bit[name]
        return mask

    @staticmethod
    def _names_to_mask(names, bit_of_name):
        mask = 0
        for name in names:
            mask |= bit_of_name[name]
        return mask

    @staticmethod
    def _popcount(mask: int) -> int:
        return mask.bit_count()

    def _global_to_names(self, bitset: int) -> frozenset[str]:
        if bitset == -1 or bitset == 0:
            return frozenset() if bitset == 0 else frozenset(self._global_bit)
        names = set()
        while bitset:
            lowest = bitset & -bitset
            names.add(self._global_name[lowest])
            bitset ^= lowest
        return frozenset(names)

    def _backtrack(self, history, target_index):
        macro_choices: list[tuple[str, tuple[str, ...]]] = []
        current = int(target_index)
        for layer in range(len(history) - 1, -1, -1):
            record = history[layer]
            option_index = int(record["pred_option"][current])
            circuit, req = record["options"][option_index]
            macro_choices.append((circuit, req))
            current = int(record["pred_prev"][current])
        macro_choices.reverse()

        choices: list[tuple[str, tuple[str, ...]]] = []
        for (circuit, req), record in zip(macro_choices, history):
            for _ in record["slices"]:
                choices.append((circuit, req))
        return choices

    def _merge_runs(self, segments: Sequence[PlanSegment]) -> list[PlanSegment]:
        merged: list[PlanSegment] = []
        for segment in segments:
            if (
                merged
                and merged[-1].end == segment.start
                and merged[-1].circuit == segment.circuit
                and merged[-1].approvals == segment.approvals
            ):
                previous = merged[-1]
                merged[-1] = PlanSegment(
                    previous.start,
                    segment.end,
                    previous.circuit,
                    previous.approvals,
                    previous.blocked,
                    previous.permanent,
                )
            else:
                merged.append(segment)
        return merged

    def _merge_infeasible(
        self, infeasible: Sequence[PlanSegment]
    ) -> list[InfeasibleRegion]:
        merged: list[InfeasibleRegion] = []
        for segment in infeasible:
            current = {
                circuit: pairs for circuit, pairs in sorted(segment.permanent.items())
            }
            if (
                merged
                and merged[-1].end == segment.start
                and merged[-1].blocked == current
            ):
                previous = merged[-1]
                merged[-1] = InfeasibleRegion(previous.start, segment.end, current)
            else:
                merged.append(InfeasibleRegion(segment.start, segment.end, current))
        return merged


def plan_clearance(
    closures: Any,
    scopes: Any,
    start: int,
    end: int,
    approved: Iterable[str] = (),
) -> ClearanceResult:
    """Convenience wrapper for one-off queries."""
    return ClearancePlanner(closures, scopes).plan(start, end, approved)
