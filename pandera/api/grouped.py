"""Round-based grouped checks with iterative row elimination.

This module implements the backend-agnostic core of *grouped admission
checks*.  A :class:`GroupedCheck` declares a grouping key and a callback that
is invoked once per group with the group's records in original row order.  A
:class:`~pandera.api.dataframe.container.DataFrameSchema` may carry several
such checks; running them with ``validate_grouped`` removes failing rows
round after round until no group fails anymore.

The callback always receives :class:`GroupRecords`, which exposes rows as
plain Python dictionaries plus column accessors (:class:`ValueList`).  No
backend-specific frame leaks into the callback, so the same callable can be
used for a pandas ``DataFrame`` and a pyarrow ``Table``.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Hashable


class GroupedCheckError(Exception):
    """Base class for grouped-check runtime errors."""


class GroupedCheckLengthError(GroupedCheckError):
    """Raised when a callback returns the wrong number of per-row results.

    The exception carries the offending ``rule``, ``group_key`` and ``round``
    so the caller can pinpoint the failure.  No partial, unfinished admission
    result is produced when this is raised.
    """

    def __init__(
        self,
        rule: str,
        group_key: Any,
        round: int,
        expected: int,
        got: int,
    ) -> None:
        self.rule = rule
        self.group_key = group_key
        self.round = round
        self.expected = expected
        self.got = got
        super().__init__(
            f"grouped check {rule!r} returned {got} result(s) for group "
            f"{group_key!r} with {expected} row(s) in round {round}"
        )


class _NullKey:
    """Sentinel marking a genuinely null group key."""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<null-group-key>"


_NULL_KEY = _NullKey()


@dataclass(frozen=True)
class _Unhashable:
    """Hashable stand-in for values that cannot be hashed."""

    kind: str
    repr: str

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return self.repr


class ValueList(list):
    """A list of column values with light, pandas-like conveniences.

    Instances behave like ordinary lists (iteration, ``len``, ``set``), but
    also support element-wise comparisons and a handful of reductions so a
    single callback can be written against pandas and pyarrow inputs.
    """

    def _wrap(self, values: Iterable[Any]) -> "ValueList":
        return ValueList(values)

    def _elementwise(
        self, other: Any, op: Callable[[Any, Any], Any]
    ) -> "ValueList":
        if isinstance(other, (list, tuple, ValueList)):
            return self._wrap(op(a, b) for a, b in zip(self, other))
        return self._wrap(op(a, other) for a in self)

    def __eq__(self, other: Any) -> Any:  # type: ignore[override]
        if isinstance(other, (list, tuple, ValueList)):
            return list(self) == list(other)
        return self._elementwise(other, lambda a, b: a == b)

    def __ne__(self, other: Any) -> Any:  # type: ignore[override]
        if isinstance(other, (list, tuple, ValueList)):
            return list(self) != list(other)
        return self._elementwise(other, lambda a, b: a != b)

    def __lt__(self, other: Any) -> "ValueList":
        return self._elementwise(other, lambda a, b: a < b)

    def __le__(self, other: Any) -> "ValueList":
        return self._elementwise(other, lambda a, b: a <= b)

    def __gt__(self, other: Any) -> "ValueList":
        return self._elementwise(other, lambda a, b: a > b)

    def __ge__(self, other: Any) -> "ValueList":
        return self._elementwise(other, lambda a, b: a >= b)

    def __and__(self, other: Any) -> "ValueList":
        return self._elementwise(other, lambda a, b: bool(a) and bool(b))

    def __or__(self, other: Any) -> "ValueList":
        return self._elementwise(other, lambda a, b: bool(a) or bool(b))

    def __invert__(self) -> "ValueList":
        return self._wrap(not bool(a) for a in self)

    def sum(self) -> Any:
        """Sum of the values (``0`` for an empty list)."""
        return sum(self)

    def mean(self) -> float:
        """Arithmetic mean."""
        return sum(self) / len(self)

    def min(self) -> Any:
        """Smallest value."""
        return min(self)

    def max(self) -> Any:
        """Largest value."""
        return max(self)

    def all(self) -> bool:
        """True if every value is truthy."""
        return all(self)

    def any(self) -> bool:
        """True if any value is truthy."""
        return any(self)

    def unique(self) -> "ValueList":
        """Distinct values in first-seen order."""
        seen: set[Any] = set()
        out = []
        for value in self:
            try:
                if value in seen:
                    continue
                seen.add(value)
            except TypeError:
                if value in out:
                    continue
            out.append(value)
        return ValueList(out)

    def nunique(self) -> int:
        """Number of distinct values."""
        return len(self.unique())

    def isna(self) -> "ValueList":
        """Boolean vector marking null values."""
        return self._wrap(_is_null(value) for value in self)

    def tolist(self) -> list:
        """Return a plain ``list``."""
        return list(self)

    def to_list(self) -> list:
        """Alias for :meth:`tolist`."""
        return list(self)

    def to_numpy(self) -> Any:
        """Return a numpy array if numpy is installed."""
        import numpy as np

        return np.asarray(list(self))


class GroupRecords(Sequence):
    """The rows of one group, in original row order.

    Iterating yields a ``dict`` per row.  Indexing with a column name yields a
    :class:`ValueList`.  ``to_pandas`` / ``to_pyarrow`` convert the group for
    callbacks that prefer a native frame.
    """

    __slots__ = ("_records",)

    def __init__(self, records: list[dict[str, Any]]) -> None:
        self._records = records

    def __len__(self) -> int:
        return len(self._records)

    def __iter__(self) -> Any:
        return iter(self._records)

    def __getitem__(self, item: Any) -> Any:
        if isinstance(item, str):
            return ValueList(row[item] for row in self._records)
        return self._records[item]

    def __repr__(self) -> str:
        return f"GroupRecords({len(self._records)} rows)"

    @property
    def columns(self) -> list[str]:
        """Column names of the group."""
        if not self._records:
            return []
        return list(self._records[0].keys())

    def column(self, name: str) -> ValueList:
        """Return a column as a :class:`ValueList`."""
        return self[name]

    def to_pandas(self) -> Any:
        """Convert the group to a pandas ``DataFrame``."""
        import pandas as pd

        return pd.DataFrame(list(self._records))

    def to_pyarrow(self) -> Any:
        """Convert the group to a pyarrow ``Table``."""
        import pyarrow as pa

        return pa.Table.from_pylist(list(self._records))


def _is_null(value: Any) -> bool:
    """Return True if ``value`` represents a null group-key component."""
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    module = type(value).__module__ or ""
    if module.startswith("pandas") or module.startswith("numpy"):
        try:
            import pandas as pd

            result = pd.isna(value)
            if isinstance(result, bool) or type(result).__name__ == "bool_":
                return bool(result)
        except (TypeError, ValueError):
            pass
    return False


def _to_native(value: Any) -> Any:
    """Normalize a scalar to a plain Python value where possible."""
    if value is None:
        return None
    if _is_null(value):
        return None
    module = type(value).__module__ or ""
    if module.startswith("numpy") and getattr(value, "ndim", 1) == 0:
        try:
            value = value.item()
        except ValueError:
            pass
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _hashable(value: Any) -> Hashable:
    """Return a hashable stand-in for ``value``."""
    try:
        hash(value)
    except TypeError:
        return _Unhashable(type(value).__name__, repr(value))
    return value


def _group_key(
    raw_values: list[Any], single: bool
) -> tuple[Hashable, Any]:
    """Build the cache key and the reported key for one group."""
    normalized = [
        (True, None) if _is_null(value) else (False, _to_native(value))
        for value in raw_values
    ]
    if single:
        is_null, value = normalized[0]
        internal = _NULL_KEY if is_null else _hashable(value)
        report = None if is_null else value
        return internal, report
    internal = tuple(
        _NULL_KEY if is_null else _hashable(value)
        for is_null, value in normalized
    )
    report = tuple(None if is_null else value for is_null, value in normalized)
    return _hashable(internal), report


def _iter_groups(
    records: list[dict[str, Any]],
    positions: list[int],
    groupby: str | list[str],
) -> list[tuple[Hashable, Any, list[int]]]:
    """Collect groups over ``positions`` in original row order."""
    single = isinstance(groupby, str)
    columns = [groupby] if single else list(groupby)
    if not columns:
        raise GroupedCheckError(
            "grouped check requires at least one group column"
        )
    if len(columns) == 1:
        single = True

    groups: dict[Hashable, list[int]] = {}
    reports: dict[Hashable, Any] = {}
    for position in positions:
        record = records[position]
        raw = []
        for column in columns:
            if column not in record:
                raise GroupedCheckError(
                    f"group column {column!r} not found in input"
                )
            raw.append(record[column])
        internal, report = _group_key(raw, single)
        if internal not in groups:
            groups[internal] = []
            reports[internal] = report
        groups[internal].append(position)
    return [
        (internal, reports[internal], groups[internal])
        for internal in groups
    ]


def _group_bool(output: Any) -> bool | None:
    """Return ``output`` as a bool if it is a scalar group verdict."""
    if isinstance(output, bool):
        return output
    module = type(output).__module__ or ""
    if module.startswith("numpy") and getattr(output, "ndim", None) == 0:
        return bool(output)
    if hasattr(output, "as_py") and not isinstance(output, (list, tuple)):
        value = output.as_py()
        if isinstance(value, bool):
            return value
    return None


def _is_row_vector(output: Any) -> bool:
    """True for an iterable of per-row verdicts."""
    if isinstance(output, (list, tuple, ValueList)):
        return True
    if isinstance(output, (str, bytes, dict)):
        return False
    if hasattr(output, "as_py") and not hasattr(output, "__iter__"):
        return False
    module = type(output).__module__ or ""
    if module.startswith("numpy") and getattr(output, "ndim", None) == 0:
        return False
    return hasattr(output, "__iter__")


def _row_vector(output: Any) -> list[bool]:
    """Convert a per-row verdict sequence to plain bools."""
    module = type(output).__module__ or ""
    if module.startswith("numpy") and hasattr(output, "tolist"):
        output = output.tolist()
    values = list(output)
    return [bool(value) for value in values]


def _interpret(
    output: Any,
    size: int,
    rule: str,
    group_key: Any,
    round_number: int,
) -> tuple[str, Any]:
    """Interpret a callback return as a group or per-row verdict."""
    group_verdict = _group_bool(output)
    if group_verdict is not None:
        return "group", group_verdict
    if _is_row_vector(output):
        values = _row_vector(output)
        if len(values) != size:
            raise GroupedCheckLengthError(
                rule, group_key, round_number, size, len(values)
            )
        return "rows", values
    raise GroupedCheckError(
        f"grouped check {rule!r} returned an unsupported result for group "
        f"{group_key!r} in round {round_number}: {type(output)!r}"
    )


def _default_name(fn: Callable, groupby: str | list[str]) -> str:
    """Derive a stable rule name that does not depend on declaration order."""
    name = getattr(fn, "__name__", None)
    if name and name not in ("<lambda>", "grouped_check"):
        return name
    columns = groupby if isinstance(groupby, str) else ",".join(groupby)
    try:
        signature = repr(
            (getattr(fn, "__code__", None).co_code, fn.__code__.co_consts)
        )
    except AttributeError:
        signature = repr(fn)
    digest = hashlib.sha1(signature.encode()).hexdigest()[:8]
    return f"grouped_check[{columns}]:{digest}"


class GroupedCheck:
    """An optional grouped check usable on a ``DataFrameSchema``.

    :param check_fn: ``Callable[[GroupRecords], Union[bool, Sequence[bool]]]``.
        It receives the records of a single group in original row order and
        returns either one boolean for the whole group or one boolean per row.
    :param groupby: a column name or a list of column names used to form
        groups.  Genuinely null keys and the text ``"None"`` are distinct
        groups.
    :param name: a stable identifier used in the rejection list.  Defaults to
        the function name, or a name derived from the grouping columns.
    :param error: an optional human-readable description of the rule.
    :param n_failure_cases: a diagnostic sampling cap.  It never influences
        elimination or the rejection list.
    """

    def __init__(
        self,
        check_fn: Callable[[GroupRecords], Any],
        groupby: str | list[str],
        name: str | None = None,
        error: str | None = None,
        n_failure_cases: int | None = None,
    ) -> None:
        if not callable(check_fn):
            raise TypeError("grouped check function must be callable")
        if isinstance(groupby, str):
            groupby = [groupby]
        elif not isinstance(groupby, Sequence) or isinstance(
            groupby, (bytes, bytearray)
        ):
            raise TypeError(
                "groupby must be a column name or a list of names"
            )
        if not groupby:
            raise ValueError("groupby must contain at least one column")
        self.check_fn = check_fn
        self.groupby: list[str] = list(groupby)
        self.name = name or _default_name(check_fn, self.groupby)
        self.error = error
        self.n_failure_cases = n_failure_cases

    def __repr__(self) -> str:
        return (
            f"GroupedCheck(name={self.name!r}, groupby={self.groupby!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GroupedCheck):
            return NotImplemented
        return (
            self.check_fn == other.check_fn
            and self.groupby == other.groupby
            and self.name == other.name
            and self.error == other.error
            and self.n_failure_cases == other.n_failure_cases
        )

    __hash__ = None  # type: ignore[assignment]


@dataclass(frozen=True)
class RejectionReason:
    """One ``(rule, group key)`` trigger for a rejected row."""

    rule: str
    group_key: Any


@dataclass(frozen=True)
class RowRejection:
    """A single rejected row."""

    position: int
    round: int
    reasons: tuple[RejectionReason, ...] = field(default_factory=tuple)

    @property
    def rules(self) -> tuple[str, ...]:
        """All rule names that triggered this row's rejection."""
        return tuple(sorted({reason.rule for reason in self.reasons}))

    @property
    def group_keys(self) -> tuple[Any, ...]:
        """All group keys that triggered this row's rejection."""
        return tuple(reason.group_key for reason in self.reasons)


@dataclass
class GroupedValidationResult:
    """The outcome of :meth:`DataFrameSchema.validate_grouped`.

    ``table`` holds only the accepted rows in original input order.  The
    ``rejections`` list names each rejected row by its original position,
    the round it was removed in, and every ``(rule, group key)`` that
    triggered it in that round.
    """

    table: Any
    rejections: list[RowRejection]
    rounds: int = 0
    _failure_cap: int | None = field(default=None, repr=False)

    @property
    def accepted(self) -> Any:
        """Alias for :attr:`table`."""
        return self.table

    @property
    def accepted_table(self) -> Any:
        """Alias for :attr:`table`."""
        return self.table

    @property
    def rejection_list(self) -> list[RowRejection]:
        """Alias for :attr:`rejections`."""
        return self.rejections

    @property
    def failure_cases(self) -> list[RowRejection]:
        """A capped diagnostic sample of :attr:`rejections`.

        The cap comes from the rules' ``n_failure_cases`` settings and only
        limits this sampling view.  It never changes :attr:`table`,
        :attr:`rejections`, the removal rounds, or the reported reasons.
        """
        if self._failure_cap is None:
            return list(self.rejections)
        return list(self.rejections[: self._failure_cap])

    @property
    def rejected_positions(self) -> list[int]:
        """Original positions of the rejected rows."""
        return [rejection.position for rejection in self.rejections]

    def as_records(self) -> list[dict[str, Any]]:
        """Return the rejection list as plain dictionaries.

        Each dictionary has ``position``, ``round``, ``rules`` and
        ``group_keys`` keys, suitable for conversion into a dataframe or
        logging.
        """
        return [
            {
                "position": rejection.position,
                "round": rejection.round,
                "rules": rejection.rules,
                "group_keys": rejection.group_keys,
            }
            for rejection in self.rejections
        ]

    def __iter__(self) -> Any:
        return iter((self.table, self.rejections))


def _reason_sort_key(reason: RejectionReason) -> tuple[str, str, str]:
    return (
        reason.rule,
        type(reason.group_key).__name__,
        repr(reason.group_key),
    )


def _sorted_reasons(
    reasons: list[tuple[str, Any]]
) -> tuple[RejectionReason, ...]:
    unique: dict[tuple[str, str, str], RejectionReason] = {}
    for rule, group_key in reasons:
        reason = RejectionReason(rule=rule, group_key=group_key)
        unique[_reason_sort_key(reason)] = reason
    return tuple(
        unique[key] for key in sorted(unique, key=lambda k: k)
    )


def run_grouped_validation(
    records: list[dict[str, Any]],
    rules: list[GroupedCheck],
) -> tuple[list[int], list[RowRejection], int]:
    """Run round-based elimination over ``records``.

    :param records: row dictionaries in original input order.
    :param rules: grouped checks, each with its own grouping columns.
    :returns: accepted positions (ascending), rejections, and rounds run.
    """
    active: set[int] = set(range(len(records)))
    cache: dict[
        tuple[int, Hashable], tuple[tuple[int, ...], tuple[str, Any]]
    ] = {}
    rejected: dict[int, RowRejection] = {}
    round_number = 0

    while True:
        round_number += 1
        snapshot = sorted(active)
        failed: dict[int, list[tuple[str, Any]]] = {}

        for rule_index, rule in enumerate(rules):
            for internal, group_key, positions in _iter_groups(
                records, snapshot, rule.groupby
            ):
                cache_key = (rule_index, internal)
                cached = cache.get(cache_key)
                membership = tuple(positions)
                if cached is not None and cached[0] == membership:
                    # Same members in the same original order: the previously
                    # computed verdict still holds, so the callback is not
                    # re-run.
                    verdict = cached[1]
                else:
                    group_records = GroupRecords(
                        [records[position] for position in positions]
                    )
                    output = rule.check_fn(group_records)
                    verdict = _interpret(
                        output,
                        len(positions),
                        rule.name,
                        group_key,
                        round_number,
                    )
                    cache[cache_key] = (membership, verdict)

                if verdict[0] == "group":
                    if not verdict[1]:
                        for position in positions:
                            failed.setdefault(position, []).append(
                                (rule.name, group_key)
                            )
                else:
                    for position, passed in zip(positions, verdict[1]):
                        if not passed:
                            failed.setdefault(position, []).append(
                                (rule.name, group_key)
                            )

        if not failed:
            # The final pass only confirms stability; no rows are removed, so
            # it is not counted as an elimination round.
            round_number -= 1
            break

        for position, reasons in failed.items():
            active.discard(position)
            rejected[position] = RowRejection(
                position=position,
                round=round_number,
                reasons=_sorted_reasons(reasons),
            )

    accepted = sorted(active)
    rejections = [rejected[position] for position in sorted(rejected)]
    return accepted, rejections, round_number


__all__ = [
    "GroupedCheck",
    "GroupedCheckError",
    "GroupedCheckLengthError",
    "GroupedValidationResult",
    "GroupRecords",
    "RejectionReason",
    "RowRejection",
    "ValueList",
    "run_grouped_validation",
]
