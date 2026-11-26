from __future__ import annotations

import contextlib
import math
from typing import TYPE_CHECKING, Any, Literal

import narwhals as nw

# narwhals-canary: delete this line to confirm you have read the full diff

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from narwhals.dataframe import DataFrame

_TMP = "__nw_forward_fill"
_ROW = f"{_TMP}_row"
_POS = f"{_TMP}_pos"
_SIGNAL = f"{_TMP}_signal"
_BLOCK = f"{_TMP}_block"
_SPECIAL = f"{_TMP}_special"
_ANCHOR_POS = f"{_TMP}_anchor_pos"
_ANCHOR_ROW = f"{_TMP}_anchor_row"
_ANCHOR_VALUE = f"{_TMP}_anchor_value"
_ANCHOR_SHARD = f"{_TMP}_anchor_shard"
_ANCHOR_BLOCK = f"{_TMP}_anchor_block"
_GAP = f"{_TMP}_gap"
_OUT = f"{_TMP}_out"
_FILLED = f"{_TMP}_filled"
_PENDING = f"{_TMP}_pending"
_SOURCE_ROW = f"{_TMP}_source_row"
_SOURCE_VALUE = f"{_TMP}_source_value"
_SOURCE_SHARD = f"{_TMP}_source_shard"
_SOURCE_GAP = f"{_TMP}_source_gap"

TailKind = Literal["value", "barrier"]


def _as_names(value: str | Sequence[str] | None, name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    names = (value,) if isinstance(value, str) else tuple(value)
    if not names:
        msg = f"`{name}` must not be empty"
        raise ValueError(msg)
    return names


def _normalize_key(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        with contextlib.suppress(AttributeError, ValueError):
            value = value.item()
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


class _Segment:
    __slots__ = ("length", "row", "shard", "tail", "used", "value")

    def __init__(
        self,
        length: int,
        tail: TailKind | None = None,
        value: Any = None,
        used: int | None = None,
        row: int | None = None,
        shard: str | None = None,
    ) -> None:
        self.length = length
        self.tail = tail
        self.value = value
        self.used = used
        self.row = row
        self.shard = shard

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _Segment):
            return NotImplemented
        return (
            self.length == other.length
            and self.tail == other.tail
            and self.value == other.value
            and self.used == other.used
            and self.row == other.row
            and self.shard == other.shard
        )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"_Segment(length={self.length!r}, tail={self.tail!r}, "
            f"value={self.value!r}, used={self.used!r}, row={self.row!r}, "
            f"shard={self.shard!r})"
        )

    def merge(self, following: _Segment) -> _Segment:
        if following.tail is not None:
            tail = following
        elif self.tail is not None:
            tail = _Segment(
                self.length,
                self.tail,
                self.value,
                None if self.tail == "barrier" else (self.used or 0) + following.length,
                self.row,
                self.shard,
            )
        else:
            tail = self
        return _Segment(
            self.length + following.length,
            tail.tail,
            tail.value,
            tail.used,
            tail.row,
            tail.shard,
        )


class ForwardFillState:
    """Mergeable summary of a batch's forward-fill output.

    A state records, per partition, whether a real signal occurs and the last
    one - or the last blocking shutdown - plus how many rows after it have
    already consumed its fill budget. It holds no rows, so it can be stored
    beside a Parquet shard and merged later without rereading that shard.

    Attributes:
        partitions: The partition columns the state is keyed by.
        orders: The order columns, if any.
        shard: Optional label of the batch.
        limit: Fill budget per real signal.
    """

    __slots__ = ("_segments", "limit", "orders", "partitions", "shard")

    def __init__(
        self,
        segments: Mapping[tuple[Any, ...], _Segment],
        *,
        partitions: tuple[str, ...] = (),
        orders: tuple[str, ...] = (),
        shard: str | None = None,
        limit: int | None = 2,
    ) -> None:
        self._segments = dict(segments)
        self.partitions = partitions
        self.orders = orders
        self.shard = shard
        self.limit = limit

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ForwardFillState):
            return NotImplemented
        return (
            self._segments == other._segments
            and self.partitions == other.partitions
            and self.orders == other.orders
            and self.limit == other.limit
        )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"ForwardFillState(_segments={self._segments!r}, "
            f"partitions={self.partitions!r}, orders={self.orders!r}, "
            f"shard={self.shard!r}, limit={self.limit!r})"
        )

    def keys(self) -> tuple[tuple[Any, ...], ...]:
        """Partition keys present in this state."""
        return tuple(self._segments)

    def merge(self, following: ForwardFillState) -> ForwardFillState:
        """Compose this state with the state of the segment immediately after it.

        Merging is associative and does not depend on the order in which the
        batches finished their jobs: only the argument order must follow sample
        order. Shards computed out of order can therefore be stitched without
        rereading their rows.

        Arguments:
            following: State of the segment which comes directly after this one.

        Raises:
            ValueError: If the states were built with different options.
        """
        if self.partitions != following.partitions:
            msg = "Cannot merge `ForwardFillState`s with different `partition_by`"
            raise ValueError(msg)
        if self.orders != following.orders:
            msg = "Cannot merge `ForwardFillState`s with different `order_by`"
            raise ValueError(msg)
        if self.limit != following.limit:
            msg = "Cannot merge `ForwardFillState`s with different `limit`"
            raise ValueError(msg)
        segments = dict(self._segments)
        for key, segment in following._segments.items():
            previous = segments.get(key)
            segments[key] = segment if previous is None else previous.merge(segment)
        return ForwardFillState(
            segments,
            partitions=self.partitions,
            orders=self.orders,
            shard=following.shard or self.shard,
            limit=self.limit,
        )

    def __or__(self, following: ForwardFillState) -> ForwardFillState:
        return self.merge(following)


class ForwardFillResult:
    """The result of :func:`narwhals.forward_fill`.

    Attributes:
        frame: The batch with the target column forward-filled.
        provenance: Row-aligned evidence for ``frame``. ``source_value`` and
            ``source_gap`` are set only when the output copied a real signal;
            the real signal itself has ``source_gap == 0``. Output which stayed
            null has null source fields; ``pending`` says whether an earlier
            batch could still resolve it.
        state: Mergeable summary of this batch.
    """

    __slots__ = (
        "_limit",
        "_orders",
        "_partitions",
        "_shard",
        "_track",
        "_value",
        "_value_dtype",
        "_visible",
        "frame",
        "provenance",
        "state",
    )

    def __init__(
        self,
        *,
        frame: DataFrame[Any],
        provenance: DataFrame[Any],
        state: ForwardFillState,
        track: DataFrame[Any],
        visible: tuple[str, ...],
        value: str,
        value_dtype: Any,
        partitions: tuple[str, ...],
        orders: tuple[str, ...],
        limit: int | None,
        shard: str | None,
    ) -> None:
        self.frame = frame
        self.provenance = provenance
        self.state = state
        self._track = track
        self._visible = visible
        self._value = value
        self._value_dtype = value_dtype
        self._partitions = partitions
        self._orders = orders
        self._limit = limit
        self._shard = shard

    def resolve(self, state: ForwardFillState) -> ForwardFillResult:
        """Resolve this batch's leading unresolved rows from an earlier prefix.

        Only rows before this batch's first local signal can change. A local
        shutdown blocks the carry, rows already filled locally keep their own
        anchor and budget, and rows past the incoming signal's budget stay
        null. Because the stable suffix is reused as-is, updating the prefix
        state only rewrites the affected leading rows.

        Arguments:
            state: Summary of **all** batches which precede this one. Passing a
                full prefix makes a partition with no signal in it final rather
                than awaiting an even earlier batch.
        """
        if state.partitions != self._partitions:
            msg = "State was built with different `partition_by`"
            raise ValueError(msg)
        if state.orders != self._orders:
            msg = "State was built with different `order_by`"
            raise ValueError(msg)
        if state.limit != self._limit:
            msg = "State was built with a different `limit`"
            raise ValueError(msg)
        track = self._track
        if len(track) == 0:  # pragma: no cover - empty batches return early
            return self
        groups = (
            [((), track)]
            if not self._partitions
            else list(track.group_by(*self._partitions))
        )
        patched: list[DataFrame[Any]] = []
        for key, group in groups:
            if self._partitions:
                key_parts: tuple[Any, ...] = key if isinstance(key, tuple) else (key,)
                normalized = tuple(_normalize_key(item) for item in key_parts)
            else:
                normalized = ()
            segment = state._segments.get(normalized)
            if segment is None:
                patched.append(group)
            elif segment.tail == "value" and segment.value is not None:
                patched.append(
                    _apply_carry(group, segment, self._limit, self._value_dtype)
                )
            else:
                patched.append(group.with_columns(nw.lit(False).alias(_PENDING)))
        merged = (
            patched[0]
            if len(patched) == 1
            else nw.concat(patched, how="vertical").sort(_ROW)
        )
        return ForwardFillResult(
            frame=_frame_from(merged, self._value, self._visible),
            provenance=_provenance(merged),
            state=state.merge(self.state),
            track=merged,
            visible=self._visible,
            value=self._value,
            value_dtype=self._value_dtype,
            partitions=self._partitions,
            orders=self._orders,
            limit=self._limit,
            shard=self._shard,
        )


def _carry_dtype(value: Any, dtype: Any) -> Any:
    if dtype not in {nw.Unknown, nw.String}:
        return dtype
    if isinstance(value, bool):
        return nw.Boolean
    if isinstance(value, int):
        return nw.Int64
    if isinstance(value, float):
        return nw.Float64
    if isinstance(value, str):
        return nw.String
    return dtype


def _apply_carry(
    group: DataFrame[Any], segment: _Segment, limit: int | None, dtype: Any
) -> DataFrame[Any]:
    resolved_dtype = _carry_dtype(segment.value, dtype)
    existing = nw.col(_OUT)
    if resolved_dtype != dtype and dtype in {nw.Unknown, nw.String}:
        existing = existing.cast(resolved_dtype)
    carry_value = nw.lit(segment.value, dtype=resolved_dtype)
    distance = nw.lit((segment.used or 0) + 1, dtype=nw.Int64) + nw.col(_POS)
    within = nw.lit(True) if limit is None else distance <= nw.lit(limit, dtype=nw.Int64)
    patch = nw.col(_PENDING) & within
    source_shard = (
        nw.lit(segment.shard, dtype=nw.String)
        if segment.shard is not None
        else nw.lit(None, dtype=nw.String)
    )
    return group.with_columns(
        nw.when(patch).then(carry_value).otherwise(existing).alias(_OUT),
        (nw.col(_FILLED) | patch).alias(_FILLED),
        nw.lit(False).alias(_PENDING),
        nw.when(patch)
        .then(carry_value)
        .otherwise(nw.col(_SOURCE_VALUE).cast(resolved_dtype))
        .alias(_SOURCE_VALUE),
        nw.when(patch)
        .then(nw.lit(segment.row, dtype=nw.Int64))
        .otherwise(nw.col(_SOURCE_ROW))
        .alias(_SOURCE_ROW),
        nw.when(patch)
        .then(source_shard)
        .otherwise(nw.col(_SOURCE_SHARD))
        .alias(_SOURCE_SHARD),
        nw.when(patch).then(distance).otherwise(nw.col(_SOURCE_GAP)).alias(_SOURCE_GAP),
    )


def forward_fill(
    df: DataFrame[Any],
    column: str,
    *,
    partition_by: str | Sequence[str] | None = None,
    order_by: str | Sequence[str] | None = None,
    limit: int | None = 2,
    block_on: str | Sequence[str] | None = None,
    shard: str | None = None,
    state: ForwardFillState | None = None,
) -> ForwardFillResult:
    r"""Forward-fill one column, with resumable state and source evidence.

    Unlike :meth:`narwhals.Expr.fill_null`, this is a *batch* operation. Each
    Parquet shard can be filled on its own, and the returned
    :class:`narwhals.ForwardFillState` composes with the state of neighbouring
    shards. A ``limit`` of two is therefore spent across the whole sample
    order, not restarted per shard, and shards whose jobs finish out of order
    can be stitched without rereading their rows.

    Arguments:
        df: The dataframe to fill.
        column: The column to fill. Nulls are replaced by the most recent real
            signal at or before them.
        partition_by: Column(s) identifying independently-filled devices.
            Rows with a null key are grouped together.
        order_by: Column(s) defining sample order. This is an order-dependent
            operation: rows must be passed in sample order.
        limit: How many consecutive null rows after a real signal may be
            filled. ``None`` means unlimited. A later real signal starts a new
            budget; it does not refresh the budget of an older one.
        block_on: Optional boolean column marking an explicit shutdown. A
            blocking row is never filled, and it prevents a signal before it
            from being carried past it. A later real signal re-opens filling.
        shard: Optional label stored on the returned state and copied into
            provenance as ``source_shard``.
        state: Summary of **all** preceding batches. When given, this batch's
            leading rows are resolved from it, and the returned ``state`` is
            the merged prefix.

    Returns:
        A :class:`narwhals.ForwardFillResult` with ``frame`` (the filled
        dataframe), ``provenance`` (one row per input row, naming the real
        signal each fill copied) and ``state`` (a mergeable summary).

    Examples:
        >>> import pyarrow as pa
        >>> import narwhals as nw
        >>> df = nw.from_native(
        ...     pa.table({"v": [11, None, None, None, 0], "shutdown": [False] * 5})
        ... )
        >>> result = nw.forward_fill(df, "v", limit=2, shard="A")
        >>> result.frame["v"].to_list()
        [11, 11, 11, None, 0]

        Shards can be processed independently, then resolved in sample order:

        >>> a = nw.from_native(pa.table({"d": ["x"], "v": [11], "seq": [0]}))
        >>> b = nw.from_native(
        ...     pa.table({"d": ["x"] * 3, "v": [None, None, 0], "seq": [1, 2, 3]})
        ... )
        >>> left = nw.forward_fill(a, "v", partition_by="d", order_by="seq", shard="a")
        >>> right = nw.forward_fill(b, "v", partition_by="d", order_by="seq", shard="b")
        >>> right.resolve(left.state).frame["v"].to_list()
        [11, 11, 0]

        Provenance points at the real reading, not at an intermediate fill:

        >>> right.resolve(left.state).provenance.select(
        ...     "source_shard", "source_gap"
        ... ).rows()
        [('a', 1), ('a', 2), ('b', 0)]
    """
    batches = _fill_batch(
        df,
        column,
        partitions=_as_names(partition_by, "partition_by"),
        orders=_as_names(order_by, "order_by"),
        blockers=_as_names(block_on, "block_on"),
        limit=limit,
        shard=shard,
    )
    return batches if state is None else batches.resolve(state)


def _validate(
    df: DataFrame[Any],
    column: str,
    partitions: tuple[str, ...],
    orders: tuple[str, ...],
    blockers: tuple[str, ...],
    limit: int | None,
) -> None:
    from narwhals.exceptions import ColumnNotFoundError

    if not isinstance(column, str):
        msg = f"`column` must be a string, got {type(column).__name__}"
        raise TypeError(msg)
    if isinstance(limit, bool) or (limit is not None and limit < 0):
        msg = f"`limit` must be a non-negative integer or None, got {limit!r}"
        raise ValueError(msg)
    columns = df.columns
    present = set(columns)
    for name in (column, *partitions, *orders, *blockers):
        if name not in present:
            raise ColumnNotFoundError.from_missing_and_available_column_names(
                [name], columns
            )


def _fill_batch(
    df: DataFrame[Any],
    column: str,
    *,
    partitions: tuple[str, ...],
    orders: tuple[str, ...],
    blockers: tuple[str, ...],
    limit: int | None,
    shard: str | None,
) -> ForwardFillResult:
    _validate(df, column, partitions, orders, blockers, limit)
    visible = tuple(df.columns)
    if len(df) == 0:
        return _empty_result(df, column, visible, partitions, orders, limit, shard)
    work = df.with_row_index(_ROW)
    if orders:
        work = work.sort(*orders, _ROW)
    if partitions:
        groups = []
        for _, group in work.group_by(*partitions):
            groups.append(
                _fill_group(
                    group,
                    column,
                    orders=orders,
                    blockers=blockers,
                    limit=limit,
                    shard=shard,
                )
            )
        track = (
            groups[0].sort(_ROW)
            if len(groups) == 1
            else nw.concat(groups, how="vertical").sort(_ROW)
        )
    else:
        track = _fill_group(
            work, column, orders=orders, blockers=blockers, limit=limit, shard=shard
        ).sort(_ROW)
    summary = _summarize(track, partitions, orders, limit, shard)
    return ForwardFillResult(
        frame=_frame_from(track, column, visible),
        provenance=_provenance(track),
        state=summary,
        track=track,
        visible=visible,
        value=column,
        value_dtype=df.schema[column],
        partitions=partitions,
        orders=orders,
        limit=limit,
        shard=shard,
    )


def _empty_result(
    df: DataFrame[Any],
    column: str,
    visible: tuple[str, ...],
    partitions: tuple[str, ...],
    orders: tuple[str, ...],
    limit: int | None,
    shard: str | None,
) -> ForwardFillResult:
    source_dtype: Any = df.schema[column]
    if source_dtype == nw.Unknown:
        source_dtype = nw.Float64
    track = df.select(
        *(nw.col(name) for name in visible),
        nw.col(column).alias(_OUT),
        nw.lit(0, dtype=nw.Int64).alias(_POS),
        nw.lit(False, dtype=nw.Boolean).alias(_SIGNAL),
        nw.lit(False, dtype=nw.Boolean).alias(_BLOCK),
        nw.lit(False, dtype=nw.Boolean).alias(_SPECIAL),
        nw.lit(False, dtype=nw.Boolean).alias(_FILLED),
        nw.lit(False, dtype=nw.Boolean).alias(_PENDING),
        nw.lit(None, dtype=nw.Float64).alias(_SOURCE_ROW),
        nw.lit(None, dtype=source_dtype).alias(_SOURCE_VALUE),
        nw.lit(None, dtype=nw.String).alias(_SOURCE_SHARD),
        nw.lit(None, dtype=nw.Float64).alias(_SOURCE_GAP),
    )
    return ForwardFillResult(
        frame=df,
        provenance=nw.from_dict(
            {
                "observed": [],
                "source_kind": [],
                "value": [],
                "source_row": [],
                "source_shard": [],
                "source_value": [],
                "source_gap": [],
                "pending": [],
            },
            backend=df.implementation,
        ),
        state=ForwardFillState(
            {(): _Segment(0)},
            partitions=partitions,
            orders=orders,
            shard=shard,
            limit=limit,
        ),
        track=track,
        visible=visible,
        value=column,
        value_dtype=df.schema[column],
        partitions=partitions,
        orders=orders,
        limit=limit,
        shard=shard,
    )


def _fill_group(  # noqa: PLR0914
    group: DataFrame[Any],
    column: str,
    *,
    orders: tuple[str, ...],
    blockers: tuple[str, ...],
    limit: int | None,
    shard: str | None,
) -> DataFrame[Any]:
    group = group.sort(*orders, _ROW) if orders else group.sort(_ROW)
    group = group.with_row_index(_POS)
    column_dtype = group.schema[column]
    target_dtype = column_dtype if column_dtype != nw.Unknown else nw.Int64
    signal = nw.col(column).is_null().__invert__().cast(nw.Boolean)
    barrier = (
        nw.col(blockers[0]).fill_null(False).cast(nw.Boolean)
        if blockers
        else nw.lit(False, dtype=nw.Boolean)
    )
    special = signal | barrier
    group = group.with_columns(
        signal.alias(_SIGNAL), barrier.alias(_BLOCK), special.alias(_SPECIAL)
    )
    barrier_pos = (
        nw.when(barrier)
        .then(nw.col(_POS))
        .otherwise(nw.lit(-1, dtype=nw.Int64))
        .cum_max()
    )
    group = group.with_columns(barrier_pos.alias(_ANCHOR_BLOCK))
    has_signal = bool(group.select(signal.any()).item())
    if has_signal:
        signal_pos = (
            nw.when(signal)
            .then(nw.col(_POS))
            .otherwise(nw.lit(-1, dtype=nw.Int64))
            .cum_max()
        )
        anchor_pos = (
            nw.when(signal_pos > barrier_pos)
            .then(signal_pos)
            .otherwise(nw.lit(-1, dtype=nw.Int64))
        )
        group = group.with_columns(
            signal_pos.alias("__nw_forward_fill_signal_pos"),
            barrier_pos.alias("__nw_forward_fill_barrier_pos"),
            anchor_pos.alias(_ANCHOR_POS),
        )
        anchor_shard = (
            nw.lit(shard, dtype=nw.String)
            if shard is not None
            else nw.lit(None, dtype=nw.String)
        )
        anchors = group.filter(nw.col(_SIGNAL)).select(
            nw.col(_POS).alias(_ANCHOR_POS),
            nw.col(column).cast(target_dtype).alias(_ANCHOR_VALUE),
            nw.col(_ROW).cast(nw.Int64).alias(_ANCHOR_ROW),
            anchor_shard.alias(_ANCHOR_SHARD),
        )
        group = group.join(anchors, on=_ANCHOR_POS, how="left")
        gap = nw.col(_POS) - nw.col(_ANCHOR_POS)
        within = nw.lit(True) if limit is None else gap <= limit
        fillable = (
            nw.col(column).is_null()
            & nw.col(_SIGNAL).__invert__()
            & nw.col(_BLOCK).__invert__()
            & (nw.col(_ANCHOR_POS) >= 0)
            & nw.col(_ANCHOR_VALUE).is_null().__invert__()
            & within
        )
        observed = nw.col(column).is_null().__invert__()
        source = observed | fillable
        focused = (
            nw.when(observed)
            .then(nw.col(column))
            .when(fillable)
            .then(nw.col(_ANCHOR_VALUE))
            .otherwise(None)
            .cast(target_dtype)
        )
        pending = (
            nw.col(_ANCHOR_VALUE).is_null()
            & (nw.col(_BLOCK).__invert__())
            & (nw.col(_ANCHOR_BLOCK) < 0)
        )
        return group.with_columns(
            focused.alias(_OUT),
            fillable.alias(_FILLED),
            pending.alias(_PENDING),
            focused.alias(_SOURCE_VALUE),
            nw.when(source).then(nw.col(_ANCHOR_ROW)).otherwise(None).alias(_SOURCE_ROW),
            nw.when(source)
            .then(nw.col(_ANCHOR_SHARD))
            .otherwise(None)
            .alias(_SOURCE_SHARD),
            nw.when(source).then(gap).otherwise(None).alias(_SOURCE_GAP),
        )
    return group.with_columns(
        nw.col(_POS).alias("__nw_forward_fill_signal_pos"),
        nw.col(_POS).alias("__nw_forward_fill_barrier_pos"),
        nw.lit(-1, dtype=nw.Int64).alias(_ANCHOR_POS),
        nw.col(column).cast(target_dtype).alias(_ANCHOR_VALUE),
        nw.col(_ROW).cast(nw.Int64).alias(_ANCHOR_ROW),
        nw.lit(None, dtype=nw.String).alias(_ANCHOR_SHARD),
        nw.col(column).cast(target_dtype).alias(_OUT),
        nw.lit(False, dtype=nw.Boolean).alias(_FILLED),
        (
            nw.col(column).is_null()
            & nw.col(_BLOCK).__invert__()
            & (nw.col(_ANCHOR_BLOCK) < 0)
        ).alias(_PENDING),
        nw.col(column).cast(target_dtype).alias(_SOURCE_VALUE),
        nw.when(nw.col(_SIGNAL)).then(nw.col(_ROW)).otherwise(None).alias(_SOURCE_ROW),
        nw.lit(None, dtype=nw.String).alias(_SOURCE_SHARD),
        nw.when(nw.col(_SIGNAL))
        .then(nw.lit(0, dtype=nw.Int64))
        .otherwise(None)
        .alias(_SOURCE_GAP),
    )


def _summarize(
    work: DataFrame[Any],
    partitions: tuple[str, ...],
    orders: tuple[str, ...],
    limit: int | None,
    shard: str | None,
) -> ForwardFillState:
    segments: dict[tuple[Any, ...], _Segment] = {}
    if partitions:
        for key, group in work.group_by(*partitions):
            normalized = tuple(
                _normalize_key(item)
                for item in (key if isinstance(key, tuple) else (key,))
            )
            segments[normalized] = _segment(group)
    else:
        segments[()] = _segment(work)
    return ForwardFillState(
        segments, partitions=partitions, orders=orders, shard=shard, limit=limit
    )


def _segment(group: DataFrame[Any]) -> _Segment:
    total = len(group)
    specials = group.filter(nw.col(_SPECIAL)).sort(_POS)
    if len(specials) == 0:
        return _Segment(total)
    tail = specials.tail(1)
    tail_pos = int(tail.select(nw.col(_POS).max()).item())
    if bool(tail[_BLOCK].item()):
        return _Segment(total, "barrier")
    value = tail[_OUT].item()
    row = tail[_SOURCE_ROW].item()
    shard = tail[_SOURCE_SHARD].item()
    return _Segment(
        total,
        "value",
        value,
        max(total - tail_pos - 1, 0),
        None if row is None else int(row),
        shard,
    )


def _frame_from(
    track: DataFrame[Any], value: str, visible: tuple[str, ...]
) -> DataFrame[Any]:
    return track.select(
        *(nw.col(_OUT).alias(name) if name == value else nw.col(name) for name in visible)
    )


def _provenance(work: DataFrame[Any]) -> DataFrame[Any]:
    return work.select(
        observed=nw.col(_OUT).is_null().__invert__(),
        source_kind=nw.when(nw.col(_SIGNAL))
        .then(nw.lit("signal"))
        .when(nw.col(_FILLED))
        .then(nw.lit("fill"))
        .otherwise(nw.lit("none")),
        value=nw.col(_OUT),
        source_row=nw.col(_SOURCE_ROW),
        source_shard=nw.col(_SOURCE_SHARD),
        source_value=nw.col(_SOURCE_VALUE),
        source_gap=nw.col(_SOURCE_GAP),
        pending=nw.col(_PENDING),
    )
