"""Data validation parse definition."""

import dataclasses
from collections.abc import (
    Callable,
    Hashable,
    Iterator,
    Sequence,
)
from typing import Any, Optional, Protocol, Union, runtime_checkable

from pandera.api.base.parsers import BaseParser, ParserResult
from pandera.errors import SchemaInitError


def _as_column_tuple(
    columns: Union[str, Sequence[str], None],
    argument: str,
    parser_name: str | None,
) -> tuple[str, ...] | None:
    """Normalize a ``source``/``target`` argument to a tuple of column names."""
    if columns is None:
        return None
    if isinstance(columns, str):
        columns = (columns,)
    columns = tuple(columns)
    if not columns:
        raise SchemaInitError(
            f"parser '{parser_name}' declares an empty {argument}. Pass "
            f"column name(s), or omit {argument} entirely."
        )
    non_strings = [col for col in columns if not isinstance(col, str)]
    if non_strings:
        raise SchemaInitError(
            f"parser '{parser_name}' {argument} must be column names, got "
            f"{non_strings!r}."
        )
    duplicates = sorted({col for col in columns if columns.count(col) > 1})
    if duplicates:
        raise SchemaInitError(
            f"parser '{parser_name}' {argument} has duplicate columns: "
            f"{duplicates}."
        )
    return columns


@dataclasses.dataclass(frozen=True)
class ParseContext:
    """Everything a :class:`ColumnParser` knows about the column it fills.

    Passed to ``bind`` at schema-build time, which is what lets a parser read
    its own column's declared type rather than having to be told what it is
    producing.
    """

    target: str
    """Name of the column being produced."""

    dtype: Any
    """The column's declared data type, or ``None`` if it has none."""

    description: str | None
    """The column's description."""

    nullable: bool
    """Whether the column accepts nulls."""

    checks: tuple[Any, ...]
    """The column's checks."""

    source: tuple[str, ...] | None
    """Columns this parser reads, resolved against the schema-wide default."""

    schema: Any
    """The schema being built, for cross-column resolution."""

    on_error: str = "raise"
    """What to do when the parser fails: ``raise``, ``null`` or ``drop``."""


@runtime_checkable
class ColumnParser(Protocol):
    """An object that knows how to produce a column.

    ``ParsedColumn(parser=...)`` accepts a plain callable or an object
    implementing this protocol. The protocol exists so a parser can inspect
    the column it is filling (via ``bind``) and so independent parsers that
    would otherwise each do their own work can be merged (via ``batch_key``).
    """

    def bind(self, ctx: ParseContext) -> Callable:
        """Return the function that produces this column.

        Called once at schema-build time. Raising
        :class:`~pandera.errors.SchemaInitError` here surfaces the problem
        before any data is touched.
        """
        ...  # pragma: no cover

    def batch_key(self, ctx: ParseContext) -> Hashable:
        """Group key for merging with sibling parsers, or ``None`` to opt out.

        Parsers returning equal non-``None`` keys are handed to ``batch`` and
        compiled into a single :class:`Parser` covering all their targets.
        """
        ...  # pragma: no cover


def _parser_name(column_parser: Any, targets: Sequence[str]) -> str:
    return f"{type(column_parser).__name__.lower()}[{','.join(targets)}]"


def _single_parser(column_parser: ColumnParser, ctx: ParseContext) -> "Parser":
    return Parser(
        column_parser.bind(ctx),
        source=list(ctx.source) if ctx.source else None,
        target=ctx.target,
        frame_input=getattr(column_parser, "frame_input", False),
        name=_parser_name(column_parser, [ctx.target]),
    )


def _batched_parser(
    items: Sequence[tuple[ColumnParser, ParseContext]],
) -> "Parser":
    """Compile a group of batchable column parsers into one ``Parser``."""
    first = items[0][0]
    batch = getattr(type(first), "batch", None)
    if batch is None:
        raise SchemaInitError(
            f"{type(first).__name__} returns a batch_key but does not "
            "implement a `batch` classmethod, so its columns cannot be "
            "merged. Return None from batch_key to opt out of batching."
        )
    sources: list[str] = []
    for _, ctx in items:
        for column in ctx.source or ():
            if column not in sources:
                sources.append(column)
    targets = [ctx.target for _, ctx in items]
    return Parser(
        batch(items),
        source=sources or None,
        target=targets,
        frame_input=True,
        name=_parser_name(first, targets),
    )


def compile_column_parsers(schema: Any) -> list["Parser"]:
    """Collect a schema's parsers, including those declared on its columns.

    Columns that declare their own derivation contribute a :class:`Parser`
    each, except where siblings share a ``batch_key`` and can be served by one
    call. Siblings are only merged when every source the merged call needs is
    already available when it starts: a column whose source is produced by
    another derived column may have to wait for a later call of the same
    service, so one ``batch_key`` can compile into several ordered batches.
    The result is ordered by dependency.
    """
    parsers: list[Parser] = list(schema.parsers)
    batched: list[tuple[ColumnParser, ParseContext]] = []

    for name, column in getattr(schema, "columns", {}).items():
        build = getattr(column, "build_parse_context", None)
        if build is None:
            continue
        ctx = build(name, schema)
        column_parser = column.parser

        if not hasattr(column_parser, "bind"):
            # a plain callable
            parsers.append(column.compile_parser(ctx))
            continue

        key = column_parser.batch_key(ctx)
        if key is None:
            parsers.append(_single_parser(column_parser, ctx))
        else:
            batched.append((column_parser, ctx))

    parsers.extend(_schedule_batched_parsers(batched))
    return order_parsers(parsers)


def _schedule_batched_parsers(
    items: Sequence[tuple[ColumnParser, ParseContext]],
) -> list["Parser"]:
    """Split same-key column parsers into the fewest feasible batches.

    A batch is one call of the service named by the column parser's
    ``batch_key``. One call may fill any number of columns, but every source
    column those columns read must already exist when the call starts, so a
    batch that reads another batch's output has to run after it. The minimum
    number of calls is the fewest monochromatic steps in which the column
    dependency DAG can be eliminated, which is solved exactly here.

    The exact search is exponential only on pathological contracts (many
    services over a wide dependency graph). A state budget bounds it; past
    that budget a deterministic feasible greedy takes over, so schema
    compilation always terminates.
    """
    if not items:
        return []

    n = len(items)
    predecessors = _predecessor_masks(items)
    order = _topological_order(n, predecessors, items)
    keys = [
        column_parser.batch_key(ctx) for column_parser, ctx in items
    ]
    ancestors = _ancestor_closure(n, predecessors)

    try:
        steps = _exact_steps(
            n, keys, predecessors, ancestors, order
        )
    except (_BatchScheduleBudgetExceeded, RecursionError):
        # A very deep or wide dependency graph defeats the exact search; the
        # greedy plan is always feasible even if it uses an extra call.
        steps = _greedy_steps(n, keys, predecessors, order)

    return [
        _batched_parser([items[index] for index in _bits(take)])
        for _, take in steps
    ]


class _BatchScheduleBudgetExceeded(RuntimeError):
    """Internal signal that the exact batch search hit its state budget."""


# Memoized states the exact scheduler may visit before falling back to the
# linear greedy plan. Contracts of a handful of services -- the intended case
# -- visit only a few states even with hundreds of chained columns.
_BATCH_SCHEDULE_STATE_BUDGET = 20_000


def _exact_steps(
    n: int,
    keys: Sequence[Hashable],
    predecessors: Sequence[int],
    ancestors: Sequence[int],
    order: Sequence[int],
) -> list[tuple[Hashable, int]]:
    """Fewest feasible monochromatic elimination steps, via memoized DP.

    A step deletes every currently ready column of one chosen service (a
    column is ready when all its predecessors are already deleted). Taking
    every ready column of the service is without loss of optimality: moving a
    ready same-service column into the current call never increases the call
    count, so the only real choice is which service runs next.
    """
    full = (1 << n) - 1
    key_counts: dict[Hashable, int] = {}
    for key in keys:
        key_counts[key] = key_counts.get(key, 0) + 1
    singleton = {key for key, count in key_counts.items() if count == 1}

    memo: dict[int, tuple[int, list[tuple[Hashable, int]]]] = {}
    states = 0

    def solve(mask: int) -> tuple[int, list[tuple[Hashable, int]]]:
        nonlocal states

        # A service used by one column can never share a call, so that call
        # is forced; firing it the moment it is ready only unlocks downstream
        # columns and never blocks a merge, so it never enters the search.
        prefix: list[tuple[Hashable, int]] = []
        changed = True
        while changed:
            changed = False
            for index in order:
                bit = 1 << index
                if (
                    mask & bit
                    and keys[index] in singleton
                    and not (predecessors[index] & mask)
                ):
                    prefix.append((keys[index], bit))
                    mask ^= bit
                    changed = True

        if mask == 0:
            return len(prefix), prefix
        if mask in memo:
            sub_calls, sub_steps = memo[mask]
            return len(prefix) + sub_calls, prefix + sub_steps

        states += 1
        if states > _BATCH_SCHEDULE_STATE_BUDGET:
            raise _BatchScheduleBudgetExceeded

        ready: dict[Hashable, int] = {}
        for index in order:
            bit = 1 << index
            if mask & bit and not (predecessors[index] & mask):
                ready[keys[index]] = ready.get(keys[index], 0) | bit

        best_calls = n + 1
        best_steps: list[tuple[Hashable, int]] | None = None
        for key, take in ready.items():
            remaining = mask & ~take
            # admissible lower bound: prune branches that cannot improve
            bound = _chain_lower_bound(
                remaining, keys, ancestors, order
            )
            if 1 + bound >= best_calls:
                continue
            sub_calls, sub_steps = solve(remaining)
            candidate = 1 + sub_calls
            if candidate < best_calls:
                best_calls = candidate
                best_steps = [(key, take), *sub_steps]

        assert best_steps is not None
        memo[mask] = (best_calls, best_steps)
        return len(prefix) + best_calls, prefix + best_steps

    _, steps = solve(full)
    return steps


def _greedy_steps(
    n: int,
    keys: Sequence[Hashable],
    predecessors: Sequence[int],
    order: Sequence[int],
) -> list[tuple[Hashable, int]]:
    """Feasible fallback using the widest ready group per call."""
    mask = (1 << n) - 1
    steps: list[tuple[Hashable, int]] = []
    while mask:
        ready: dict[Hashable, int] = {}
        first_seen: dict[Hashable, int] = {}
        for position, index in enumerate(order):
            bit = 1 << index
            if mask & bit and not (predecessors[index] & mask):
                key = keys[index]
                ready[key] = ready.get(key, 0) | bit
                first_seen.setdefault(key, position)
        # widest ready group wins; ties break by earliest appearance in the
        # deterministic topological order.
        key = min(
            ready,
            key=lambda candidate: (
                -ready[candidate].bit_count(),
                first_seen[candidate],
            ),
        )
        take = ready[key]
        steps.append((key, take))
        mask &= ~take
    return steps


def _predecessor_masks(
    items: Sequence[tuple[ColumnParser, ParseContext]],
) -> list[int]:
    """Bitmask of directly preceding batched columns for each column."""
    producer = {ctx.target: index for index, (_, ctx) in enumerate(items)}
    predecessors = [0] * len(items)
    for index, (_, ctx) in enumerate(items):
        for source in ctx.source or ():
            producer_index = producer.get(source)
            if producer_index is not None and producer_index != index:
                predecessors[index] |= 1 << producer_index
    return predecessors


def _ancestor_closure(n: int, predecessors: Sequence[int]) -> list[int]:
    """Transitive ancestor bitmask for every column."""
    ancestors = list(predecessors)
    for middle in range(n):
        bit = 1 << middle
        for index in range(n):
            if ancestors[index] & bit:
                ancestors[index] |= ancestors[middle]
    return ancestors


def _chain_lower_bound(
    mask: int,
    keys: Sequence[Hashable],
    ancestors: Sequence[int],
    order: Sequence[int],
) -> int:
    """Sum, per service, of its longest remaining same-service chain.

    Every node of such a chain needs its own call of the service, so this is a
    lower bound on the calls still required and is safe to prune against.
    """
    chain: dict[int, int] = {}
    best: dict[Hashable, int] = {}
    for index in order:
        bit = 1 << index
        if not (mask & bit):
            continue
        key = keys[index]
        longest = 0
        upstream = ancestors[index] & mask
        while upstream:
            low = upstream & -upstream
            ancestor = low.bit_length() - 1
            if keys[ancestor] == key:
                longest = max(longest, chain.get(ancestor, 0))
            upstream ^= low
        chain[index] = longest + 1
        best[key] = max(best.get(key, 0), chain[index])
    return sum(best.values())


def _bits(mask: int) -> Iterator[int]:
    """Yield the indices set in a bitmask, lowest first."""
    while mask:
        low = mask & -mask
        yield low.bit_length() - 1
        mask ^= low


def _topological_order(
    n: int,
    predecessors: Sequence[int],
    items: Sequence[tuple[ColumnParser, ParseContext]],
) -> list[int]:
    """Order batched column indices so producers precede their dependents.

    Ties keep schema order, so independent columns stay deterministic. A
    dependency cycle here is the same schema error ``order_parsers`` reports
    once all parsers are compiled.
    """
    remaining = (1 << n) - 1
    emitted = 0
    ordered: list[int] = []
    while remaining:
        ready = [
            index
            for index in range(n)
            if (remaining & (1 << index))
            and not (predecessors[index] & ~emitted)
        ]
        if not ready:
            cycle = sorted(
                ctx.target
                for index, (_, ctx) in enumerate(items)
                if remaining & (1 << index)
            )
            raise SchemaInitError(
                "parser dependencies form a cycle involving column(s) "
                f"{cycle}. A derived column cannot depend, directly or "
                "indirectly, on itself."
            )
        for index in ready:
            ordered.append(index)
            emitted |= 1 << index
            remaining ^= 1 << index
    return ordered


def order_parsers(parsers: Sequence["Parser"]) -> list["Parser"]:
    """Order parsers so each runs after the parsers producing its sources.

    Parsers that declare neither a source nor a target keep their list
    position and run first, preserving the behavior of schemas written before
    derivation existed. Declared parsers are then emitted in dependency order,
    falling back to list order between independent ones so the result is
    deterministic.

    :raises SchemaInitError: if the declared dependencies contain a cycle.
    """
    undeclared = [p for p in parsers if not p.derives_columns]
    declared = [p for p in parsers if p.derives_columns]
    if len(declared) < 2:
        return [*undeclared, *declared]

    # column -> indices of the parsers that produce it
    producers: dict[str, list[int]] = {}
    for index, parser in enumerate(declared):
        for column in parser.target or ():
            producers.setdefault(column, []).append(index)

    dependencies: dict[int, set[int]] = {
        index: set() for index in range(len(declared))
    }
    for index, parser in enumerate(declared):
        for column in parser.source or ():
            for producer in producers.get(column, []):
                if producer != index:
                    dependencies[index].add(producer)

    ordered: list[int] = []
    emitted: set[int] = set()
    remaining = list(range(len(declared)))
    while remaining:
        ready = [i for i in remaining if dependencies[i] <= emitted]
        if not ready:
            cycle = sorted(
                column
                for i in remaining
                for column in (declared[i].target or ())
            )
            raise SchemaInitError(
                "parser dependencies form a cycle involving column(s) "
                f"{cycle}. A derived column cannot depend, directly or "
                "indirectly, on itself."
            )
        # ``ready`` preserves list order, so independent parsers stay stable.
        for index in ready:
            ordered.append(index)
            emitted.add(index)
            remaining.remove(index)

    return [*undeclared, *(declared[i] for i in ordered)]


class Parser(BaseParser):
    """Parse a data object for certain properties."""

    def __init__(
        self,
        parser_fn: Callable,
        element_wise: bool = False,
        ignore_na: bool = False,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        source: Union[str, Sequence[str], None] = None,
        target: Union[str, Sequence[str], None] = None,
        frame_input: bool = False,
        **parser_kwargs,
    ) -> None:
        """Apply a parser function to a data object.

        :param parse_fn: A function to parser pandas data structure. For Column
            or SeriesSchema parsers, if element_wise is True, this function
            should have the signature: ``Callable[[pd.Series],
            Union[pd.Series, bool]]``, where the output series is a boolean
            vector.

            If element_wise is False, this function should have the signature:
            ``Callable[[Any], bool]``, where ``Any`` is an element in the
            column.

            For DataFrameSchema parsers, if element_wise=True, fn
            should have the signature: ``Callable[[pd.DataFrame],
            Union[pd.DataFrame, pd.Series, bool]]``, where the output dataframe
            or series contains booleans.

            If element_wise is True, fn is applied to each row in
            the dataframe with the signature ``Callable[[pd.Series], bool]``
            where the series input is a row in the dataframe.
        :param element_wise: Whether or not to apply validator in an
            element-wise fashion. If bool, assumes that all parsers should be
            applied to the column element-wise. If list, should be the same
            number of elements as parsers.
        :param name: optional name for the parser.
        :param title: A human-readable label for the parser.
        :param description: An arbitrary textual description of the parser.
        :param source: column(s) this parser reads. Declaring them lets pandera
            check they are present *before* calling ``parser_fn``, raising
            :class:`~pandera.errors.ParserSourceError` naming the parser and
            the missing column instead of a bare ``KeyError``.
        :param target: column(s) this parser produces. Declaring them lets
            pandera order parsers by their dependencies rather than by list
            position, and verify the function produced what it promised
            (:class:`~pandera.errors.ParserTargetError`).
        :param frame_input: always call ``parser_fn`` with a ``DataFrame``,
            even when a single source and target are declared. Used by
            batched column parsers, which fill several columns from one call
            and so cannot take the ``Series -> Series`` shortcut.
        :param parse_kwargs: key-word arguments to pass into ``parse_fn``

        When ``source`` and ``target`` are both a single column, ``parser_fn``
        is called with that source column as a ``Series`` and is expected to
        return a ``Series``. Otherwise it is called with the dataframe (or the
        selected source columns) and is expected to return a dataframe
        containing the targets.

        See :ref:`here<parsers>` for more usage details.

        """
        super().__init__(name=name)
        self._parser_fn = parser_fn
        self._parser_kwargs = parser_kwargs
        self.element_wise = element_wise
        self.ignore_na = ignore_na
        self.name = name or getattr(
            self._parser_fn, "__name__", self._parser_fn.__class__.__name__
        )
        self.title = title
        self.description = description
        self.source = _as_column_tuple(source, "source", self.name)
        self.target = _as_column_tuple(target, "target", self.name)
        self.frame_input = frame_input

    @property
    def derives_columns(self) -> bool:
        """Whether this parser declares what it reads or produces."""
        return self.source is not None or self.target is not None

    def __call__(
        self, parse_obj: Any, column: str | None = None
    ) -> ParserResult:
        """Validate pandas DataFrame or Series.

        :param parse_obj: pandas DataFrame of Series to validate.
        :param column: for dataframe parsers, apply the parser function to this
            column.
        :returns: ParseResult tuple containing:

            ``parser_output``: boolean scalar, ``Series`` or ``DataFrame``
            indicating which elements passed the parser.

            ``parsed_object``: the parseed object itself. Depending on the
            options provided to the ``Parse``, this will be a pandas Series,
            DataFrame, or if the ``groupby`` option is specified, a
            ``Dict[str, Series]`` or ``Dict[str, DataFrame]`` where the keys
            are distinct groups.

        """
        backend = self.get_backend(parse_obj)(self)
        return backend(parse_obj, column)
