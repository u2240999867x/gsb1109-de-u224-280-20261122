"""Hypothesis strategies for related parent and child pandas tables.

The strategy draws the parent key universe first, derives child foreign keys
from that universe, and only then asks Pandera to generate the non-key
attributes.  It never draws two completed frames and filters the pair by
referential integrity.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

import hypothesis.strategies as st
import pandas as pd
from hypothesis.errors import InvalidArgument, Unsatisfiable

import pandera.backends.pandas.builtin_checks  # noqa: F401
import pandera.pandas as pa
from pandera.strategies.pandas_strategies import (
    convert_dtypes,
    field_element_strategy,
)

PARENT_KEY = ("site", "code")
CHILD_KEY = ("referenced_site", "referenced_code")


class CompositeKeyConstraintError(Unsatisfiable, InvalidArgument, ValueError):
    """A statically impossible composite-key generation request."""


# Hypothesis treats the decorated function as the strategy entry.  Keeping
# this alias makes the intent explicit to callers.
RelationUnsatisfiable = CompositeKeyConstraintError


class RelatedFrames(tuple):
    """A ``(parent, child)`` tuple that also supports named/key access."""

    def __new__(
        cls, parent: pd.DataFrame, child: pd.DataFrame
    ) -> RelatedFrames:
        return super().__new__(cls, (parent, child))

    @property
    def parent(self) -> pd.DataFrame:
        return self[0]

    @property
    def child(self) -> pd.DataFrame:
        return self[1]

    @property
    def dimension(self) -> pd.DataFrame:
        return self[0]

    @property
    def fact(self) -> pd.DataFrame:
        return self[1]

    def __getitem__(self, key: Any):
        if isinstance(key, str):
            normalized = key.lower()
            if normalized in {"parent", "dimension", "dim"}:
                return self[0]
            if normalized in {"child", "fact", "detail"}:
                return self[1]
            raise KeyError(key)
        return super().__getitem__(key)

    def keys(self) -> tuple[str, str]:
        return ("parent", "child")


def _as_schema(schema: Any) -> pa.DataFrameSchema:
    if isinstance(schema, pa.DataFrameSchema):
        return schema
    if hasattr(schema, "to_schema"):
        converted = schema.to_schema()
        if isinstance(converted, pa.DataFrameSchema):
            return converted
    raise TypeError(
        "parent_schema and child_schema must be Pandera DataFrameSchema objects or DataFrameModel classes"
    )


def scenario_parent_schema() -> pa.DataFrameSchema:
    """Parent fact/dimension schema described by the regression scenario."""
    return pa.DataFrameSchema(
        {
            "site": pa.Column(
                str,
                checks=pa.Check.isin(["01", "02"]),
                nullable=False,
            ),
            "code": pa.Column(
                int,
                checks=pa.Check.isin([1, 2]),
                nullable=False,
            ),
            "measure": pa.Column(
                float,
                checks=pa.Check.ge(0),
                nullable=False,
            ),
        },
        unique=["site", "code"],
        name="composite_key_parent",
    )


def scenario_child_schema(
    *, required_unique_key: bool = False
) -> pa.DataFrameSchema:
    """Child schema with differently named physical foreign-key columns."""
    key_columns = {
        "referenced_site": pa.Column(
            str,
            checks=pa.Check.isin(["02"]),
            nullable=not required_unique_key,
        ),
        "referenced_code": pa.Column(
            int,
            checks=pa.Check.isin([1, 2]),
            nullable=not required_unique_key,
        ),
        "label": pa.Column(
            str,
            checks=pa.Check.str_length(min_value=1, max_value=12),
            nullable=True,
        ),
    }
    return pa.DataFrameSchema(
        key_columns,
        checks=pa.Check(
            lambda df: (
                df["referenced_site"].isna() == df["referenced_code"].isna()
            ),
            name="complete_composite_key_or_both_null",
        ),
        unique=(
            ["referenced_site", "referenced_code"]
            if required_unique_key
            else None
        ),
        name="composite_key_child",
    )


def _normalize_columns(value: Any, default: Sequence[str]) -> tuple[str, ...]:
    if value is None:
        return tuple(default)
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Mapping):
        return tuple(value.keys())
    return tuple(value)


def _infer_key_columns(
    schema: pa.DataFrameSchema,
    explicit: Any,
    *,
    fallback: Sequence[str],
) -> tuple[str, ...]:
    if explicit is not None:
        return _normalize_columns(explicit, fallback)

    unique = _unique_columns(schema)
    if unique and len(unique) >= 2:
        return tuple(unique[:2])

    columns = list(schema.columns)
    lower_names = {name: str(name).lower() for name in columns}
    site_column = next(
        (
            name
            for name, lower in lower_names.items()
            if lower == "site" or lower.endswith("_site") or "site" in lower
        ),
        None,
    )
    code_column = next(
        (
            name
            for name, lower in lower_names.items()
            if lower == "code" or lower.endswith("_code") or "code" in lower
        ),
        None,
    )
    if site_column is not None and code_column is not None:
        return (site_column, code_column)
    if all(name in schema.columns for name in fallback):
        return tuple(fallback)
    if len(columns) >= 2:
        return tuple(columns[:2])
    raise ValueError("could not infer a two-column composite key")


def _row_count(
    primary: int | None,
    *aliases: int | None,
    default: int | None = None,
) -> int | None:
    for value in (primary, *aliases):
        if value is not None:
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError("row counts must be integers")
            if value < 0:
                raise ValueError("row counts must be non-negative")
            return value
    return default


def _unique_columns(schema: pa.DataFrameSchema) -> tuple[str, ...] | None:
    unique = getattr(schema, "unique", None)
    if not unique:
        return None
    if isinstance(unique, str):
        return (unique,)
    return tuple(unique)


def _truthy_kwarg(kwargs: Mapping[str, Any], *names: str) -> bool | None:
    for name in names:
        if name in kwargs and kwargs[name] is not None:
            return bool(kwargs[name])
    return None


def _value_passes_checks(value: Any, checks: Sequence[Any]) -> bool:
    series = pd.Series([value], dtype=object)
    for check in checks:
        try:
            result = check(series)
            passed = getattr(result, "check_passed", result)
            if isinstance(passed, pd.Series):
                passed = bool(passed.all())
            if not bool(passed):
                return False
        except Exception:
            continue
    return True


def _allowed_values(column: pa.Column) -> tuple[Any, ...] | None:
    allowed: set[Any] | None = None
    found = False
    for check in column.checks:
        statistics = getattr(check, "statistics", None) or {}
        values = statistics.get("allowed_values")
        if values is None and getattr(check, "name", None) != "isin":
            continue
        if values is None:
            kwargs = getattr(check, "_check_kwargs", None) or {}
            values = kwargs.get("allowed_values")
        if values is None:
            continue
        found = True
        values_set = set(values)
        allowed = values_set if allowed is None else allowed & values_set
        if not allowed:
            return ()
    if not found or allowed is None:
        return None
    return tuple(
        value
        for value in allowed
        if _value_passes_checks(value, column.checks)
    )


def _domain_product(
    schema: pa.DataFrameSchema, key_columns: Sequence[str]
) -> list[tuple[Any, ...]] | None:
    domains: list[tuple[Any, ...]] = []
    for name in key_columns:
        values = _allowed_values(schema.columns[name])
        if values is None:
            return None
        domains.append(list(values))
    if not domains:
        return [()]
    product: list[tuple[Any, ...]] = [()]
    for domain in domains:
        product = [prefix + (value,) for prefix in product for value in domain]
    return product


def _eligible_domain(
    parent_schema: pa.DataFrameSchema,
    child_schema: pa.DataFrameSchema,
    parent_key: Sequence[str],
    child_key: Sequence[str],
) -> list[tuple[Any, ...]] | None:
    parent_product = _domain_product(parent_schema, parent_key)
    child_product = _domain_product(child_schema, child_key)
    if parent_product is not None:
        parent_values = set(parent_product)
        if child_product is None:
            return parent_product
        return [key for key in child_product if key in parent_values]
    if child_product is not None:
        parent_domains: list[tuple[Any, ...] | None] = [
            _allowed_values(parent_schema.columns[name]) for name in parent_key
        ]
        result = []
        for child_values in child_product:
            accepted = True
            for value, parent_domain in zip(child_values, parent_domains):
                if parent_domain is not None and value not in set(
                    parent_domain
                ):
                    accepted = False
                    break
            if accepted:
                result.append(child_values)
        return result
    return None


def _check_mentions_key_columns(check: Any, columns: Sequence[str]) -> bool:
    names = set(columns)
    fn = getattr(check, "_check_fn", None)
    code = getattr(fn, "__code__", None)
    if code is not None:
        if names.intersection(code.co_names):
            return True
        if any(
            isinstance(const, str) and const in names
            for const in code.co_consts
        ):
            return True
    source = ""
    try:
        import inspect

        source = inspect.getsource(fn) if fn is not None else ""
    except (OSError, TypeError):
        source = getattr(check, "_source", "") or ""
    return any(
        re.search(rf"['\"]{re.escape(name)}['\"]", source) for name in names
    )


def _attribute_schema(
    schema: pa.DataFrameSchema, key_columns: Sequence[str]
) -> pa.DataFrameSchema:
    projected = schema.remove_columns(list(key_columns))
    projected.checks = [
        check
        for check in schema.checks
        if not _check_mentions_key_columns(check, key_columns)
    ]
    projected.unique = None
    return projected


def _element_tuple_strategy(
    schema: pa.DataFrameSchema,
    key_columns: Sequence[str],
    extra_checks_by_position: Sequence[Sequence[Any]] | None = None,
) -> st.SearchStrategy[tuple[Any, ...]]:
    strategies = []
    for position, name in enumerate(key_columns):
        column = schema.columns[name]
        checks = list(column.checks)
        if extra_checks_by_position is not None:
            checks.extend(extra_checks_by_position[position])
        strategies.append(field_element_strategy(column.dtype, checks=checks))
    return st.tuples(*strategies)


def _eligible_tuple_strategy(
    parent_schema: pa.DataFrameSchema,
    child_schema: pa.DataFrameSchema,
    parent_key: Sequence[str],
    child_key: Sequence[str],
) -> st.SearchStrategy[tuple[Any, ...]]:
    extra_checks: list[list[Any]] = [
        list(child_schema.columns[child_key[index]].checks)
        for index in range(len(parent_key))
    ]
    return _element_tuple_strategy(
        parent_schema, parent_key, extra_checks_by_position=extra_checks
    )


def _key_series(
    values: Sequence[Any],
    column: pa.Column,
    index: pd.Index,
) -> pd.Series:
    series = pd.Series(list(values), index=index, dtype=object)
    dtype_name = str(column.dtype)
    has_nulls = any(value is None or pd.isna(value) for value in values)

    if not has_nulls:
        try:
            return convert_dtypes(
                series.to_frame("__key__"), {"__key__": dtype_name}
            )["__key__"]
        except (TypeError, ValueError):
            return series

    if dtype_name.startswith(("int", "uint")):
        match = re.search(r"\d+", dtype_name)
        bits = int(match.group()) if match else 64
        if dtype_name.startswith("uint"):
            dtype = {
                8: pd.UInt8Dtype(),
                16: pd.UInt16Dtype(),
                32: pd.UInt32Dtype(),
            }.get(bits, pd.UInt64Dtype())
        else:
            dtype = {
                8: pd.Int8Dtype(),
                16: pd.Int16Dtype(),
                32: pd.Int32Dtype(),
            }.get(bits, pd.Int64Dtype())
        return series.astype(dtype)
    if dtype_name == "bool":
        return series.astype(pd.BooleanDtype())
    if dtype_name.startswith("float"):
        return pd.to_numeric(series, errors="coerce").astype(dtype_name)
    return series


def _attach_keys(
    frame: pd.DataFrame,
    schema: pa.DataFrameSchema,
    key_columns: Sequence[str],
    keys: Sequence[tuple[Any, ...]],
    null_rows: set[int] | None = None,
) -> pd.DataFrame:
    null_rows = null_rows or set()
    for position, column_name in enumerate(key_columns):
        column_values = [
            None if row_index in null_rows else key[position]
            for row_index, key in enumerate(keys)
        ]
        frame[column_name] = _key_series(
            column_values, schema.columns[column_name], frame.index
        )

    original_order = list(schema.columns.keys())
    return frame.loc[:, original_order]


def _attribute_frame(
    draw: Any,
    schema: pa.DataFrameSchema,
    key_columns: Sequence[str],
    row_count: int,
) -> pd.DataFrame:
    projected = _attribute_schema(schema, key_columns)
    frame = draw(projected.strategy(size=row_count))
    if len(frame) != row_count:
        raise CompositeKeyConstraintError(
            f"Pandera generated {len(frame)} rows instead of {row_count}"
        )
    return frame


def related_dataframes_strategy(
    parent_schema: pa.DataFrameSchema | None = None,
    child_schema: pa.DataFrameSchema | None = None,
    *args: Any,
    parent_key_columns: Sequence[str] | str | None = None,
    child_key_columns: Sequence[str] | str | None = None,
    parent_rows: int | None = None,
    child_rows: int | None = None,
    parent_size: int | None = None,
    child_size: int | None = None,
    n_parents: int | None = None,
    n_children: int | None = None,
    parent_count: int | None = None,
    child_count: int | None = None,
    distinct_parent_keys: int | None = None,
    non_null_child_rows: int | None = None,
    nonnull_child_rows: int | None = None,
    null_child_rows: int | None = None,
    child_null_rows: int | None = None,
    child_key_required: bool | None = None,
    require_child_key: bool | None = None,
    allow_null_child_keys: bool | None = None,
    child_keys_required: bool | None = None,
    unique_child_key: bool | None = None,
    child_key_unique: bool | None = None,
    child_keys_unique: bool | None = None,
    validate: bool = True,
    **extra_kwargs: Any,
) -> st.SearchStrategy[tuple[pd.DataFrame, pd.DataFrame]]:
    """Return a strategy generating ``(parent, child)`` pandas frames.

    Row-count aliases are accepted so callers can use either the language in
    the regression request or Pandera's ``size=`` wording.
    """
    positional = list(args)
    if positional and isinstance(positional[0], Mapping):
        extra_kwargs.setdefault("key_mapping", positional.pop(0))
    if (
        positional
        and isinstance(positional[0], (str, Sequence))
        and not isinstance(positional[0], int)
    ):
        if parent_key_columns is None:
            parent_key_columns = positional.pop(0)
        if positional and child_key_columns is None:
            child_key_columns = positional.pop(0)

    positional_values = (
        ("parent_rows", parent_rows),
        ("child_rows", child_rows),
        ("distinct_parent_keys", distinct_parent_keys),
        ("non_null_child_rows", non_null_child_rows),
        ("null_child_rows", null_child_rows),
    )
    for position, (parameter_name, current_value) in enumerate(
        positional_values
    ):
        if position >= len(positional):
            break
        if current_value is not None:
            continue
        value = positional[position]
        if parameter_name == "parent_rows":
            parent_rows = value
        elif parameter_name == "child_rows":
            child_rows = value
        elif parameter_name == "distinct_parent_keys":
            distinct_parent_keys = value
        elif parameter_name == "non_null_child_rows":
            non_null_child_rows = value
        else:
            null_child_rows = value
    if len(positional) > len(positional_values):
        raise TypeError("too many positional arguments")

    parent_schema = (
        parent_schema
        or extra_kwargs.pop("dimension_schema", None)
        or extra_kwargs.pop("dim_schema", None)
        or extra_kwargs.pop("master_schema", None)
        or extra_kwargs.pop("primary_schema", None)
    )
    child_schema = (
        child_schema
        or extra_kwargs.pop("fact_schema", None)
        or extra_kwargs.pop("detail_schema", None)
        or extra_kwargs.pop("secondary_schema", None)
    )
    parent_key_columns = (
        parent_key_columns
        or extra_kwargs.pop("parent_key", None)
        or extra_kwargs.pop("parent_keys", None)
    )
    child_key_columns = (
        child_key_columns
        or extra_kwargs.pop("child_key", None)
        or extra_kwargs.pop("child_keys", None)
        or extra_kwargs.pop("foreign_key_columns", None)
        or extra_kwargs.pop("foreign_key", None)
        or extra_kwargs.pop("fk_columns", None)
    )
    key_mapping = (
        extra_kwargs.pop("key_mapping", None)
        or extra_kwargs.pop("key_map", None)
        or extra_kwargs.pop("mapping", None)
    )
    if key_mapping is not None:
        if parent_key_columns is None:
            parent_key_columns = tuple(key_mapping.keys())
        if child_key_columns is None:
            child_key_columns = tuple(key_mapping.values())

    if unique_child_key is None:
        unique_child_key = (
            extra_kwargs.pop("child_unique", None)
            or extra_kwargs.pop("unique_keys", None)
            or extra_kwargs.pop("unique", None)
            or extra_kwargs.pop("unique_fk", None)
            or extra_kwargs.pop("unique_foreign_key", None)
        )
    if child_key_required is None:
        child_key_required = (
            extra_kwargs.pop("required", None)
            or extra_kwargs.pop("key_required", None)
            or extra_kwargs.pop("keys_required", None)
            or extra_kwargs.pop("foreign_key_required", None)
        )
    if allow_null_child_keys is None:
        allow_null_child_keys = extra_kwargs.pop(
            "allow_null_keys", None
        ) or extra_kwargs.pop("nullable_keys", None)
        nullable = extra_kwargs.pop("nullable", None)
        if nullable is not None and allow_null_child_keys is None:
            allow_null_child_keys = bool(nullable)

    if non_null_child_rows is None:
        non_null_child_rows = (
            extra_kwargs.pop("non_null_rows", None)
            or extra_kwargs.pop("min_non_null_rows", None)
            or extra_kwargs.pop("min_non_null_child_rows", None)
            or extra_kwargs.pop("minimum_non_null_rows", None)
        )

    parent_rows = (
        parent_rows
        if parent_rows is not None
        else extra_kwargs.pop("n_parent_rows", None)
        or extra_kwargs.pop("num_parents", None)
        or extra_kwargs.pop("num_parent_rows", None)
        or extra_kwargs.pop("parent_n", None)
        or extra_kwargs.pop("dim_rows", None)
    )
    child_rows = (
        child_rows
        if child_rows is not None
        else extra_kwargs.pop("n_child_rows", None)
        or extra_kwargs.pop("num_children", None)
        or extra_kwargs.pop("num_child_rows", None)
        or extra_kwargs.pop("child_n", None)
        or extra_kwargs.pop("fact_rows", None)
    )

    if extra_kwargs:
        unexpected = ", ".join(sorted(extra_kwargs))
        raise TypeError(f"unexpected strategy arguments: {unexpected}")

    parent_schema = _as_schema(parent_schema or scenario_parent_schema())
    child_schema = _as_schema(child_schema or scenario_child_schema())
    parent_key = _infer_key_columns(
        parent_schema, parent_key_columns, fallback=PARENT_KEY
    )
    child_key = _infer_key_columns(
        child_schema, child_key_columns, fallback=CHILD_KEY
    )
    if len(parent_key) != len(child_key) or len(parent_key) == 0:
        raise ValueError(
            "parent_key_columns and child_key_columns must be non-empty and equal length"
        )
    for name in parent_key:
        if name not in parent_schema.columns:
            raise ValueError(f"parent schema has no key column {name!r}")
    for name in child_key:
        if name not in child_schema.columns:
            raise ValueError(f"child schema has no key column {name!r}")

    n_parent = _row_count(
        parent_rows, parent_size, n_parents, parent_count, default=3
    )
    n_child = _row_count(
        child_rows, child_size, n_children, child_count, default=5
    )
    assert n_parent is not None and n_child is not None

    requested_non_null = _row_count(
        non_null_child_rows, nonnull_child_rows, default=None
    )
    requested_null = _row_count(null_child_rows, child_null_rows, default=None)
    child_row_count_explicit = (
        requested_non_null is not None or requested_null is not None
    )
    if requested_non_null is not None and requested_null is not None:
        if requested_non_null + requested_null != n_child:
            raise ValueError(
                "non_null_child_rows + null_child_rows must equal child_rows"
            )
    elif requested_non_null is not None:
        if requested_non_null > n_child:
            raise ValueError("non_null_child_rows cannot exceed child_rows")
        requested_null = n_child - requested_non_null
    elif requested_null is not None:
        if requested_null > n_child:
            raise ValueError("null_child_rows cannot exceed child_rows")
        requested_non_null = n_child - requested_null
    else:
        requested_non_null = n_child
        requested_null = 0

    required_flag = _truthy_kwarg(
        {
            "child_key_required": child_key_required,
            "require_child_key": require_child_key,
            "child_keys_required": child_keys_required,
        },
        "child_key_required",
        "require_child_key",
        "child_keys_required",
    )
    allow_null_flag = allow_null_child_keys
    if required_flag is True:
        keys_required = True
    elif allow_null_flag is False:
        keys_required = True
    elif allow_null_flag is True:
        keys_required = False
    else:
        keys_required = not all(
            child_schema.columns[name].nullable for name in child_key
        )

    if keys_required and requested_null:
        raise CompositeKeyConstraintError(
            "child key is required, so null_child_rows must be 0"
        )
    if requested_null:
        non_nullable = [
            name
            for name in child_key
            if not child_schema.columns[name].nullable
        ]
        if non_nullable:
            raise CompositeKeyConstraintError(
                "both child key columns must be nullable to generate an empty "
                f"composite key; non-nullable columns: {non_nullable}"
            )

    auto_null = (
        not child_row_count_explicit
        and not keys_required
        and (allow_null_flag is True or required_flag is False)
        and n_child > 0
    )

    schema_unique_child = set(_unique_columns(child_schema) or ()) == set(
        child_key
    )
    unique_flag = _truthy_kwarg(
        {
            "unique_child_key": unique_child_key,
            "child_key_unique": child_key_unique,
            "child_keys_unique": child_keys_unique,
        },
        "unique_child_key",
        "child_key_unique",
        "child_keys_unique",
    )
    child_unique = schema_unique_child or unique_flag is True
    if child_unique and requested_null > 1:
        raise CompositeKeyConstraintError(
            "a jointly unique child key can contain at most one all-null pair"
        )

    schema_unique_parent = set(_unique_columns(parent_schema) or ()) == set(
        parent_key
    )
    parent_keys_unique = schema_unique_parent
    if distinct_parent_keys is None:
        n_distinct_parent = (
            n_parent if parent_keys_unique else min(n_parent, 1)
        )
    else:
        n_distinct_parent = distinct_parent_keys
    if n_distinct_parent > n_parent:
        raise ValueError("distinct_parent_keys cannot exceed parent_rows")

    parent_domain = _domain_product(parent_schema, parent_key)
    eligible_domain = _eligible_domain(
        parent_schema, child_schema, parent_key, child_key
    )

    if parent_keys_unique and parent_domain is not None:
        if n_parent > len(parent_domain):
            raise CompositeKeyConstraintError(
                f"parent key {parent_key} is jointly unique but only "
                f"{len(parent_domain)} combinations are allowed by isin "
                f"checks; parent_rows={n_parent} is impossible"
            )
    if parent_keys_unique:
        n_distinct_parent = n_parent

    required_eligible = (
        requested_non_null
        if child_unique and requested_non_null > 0
        else 1
        if requested_non_null > 0
        else 0
    )

    if eligible_domain is not None and requested_non_null:
        eligible_count = len(eligible_domain)
        if eligible_count == 0:
            raise CompositeKeyConstraintError(
                "the child key isin constraints do not overlap the parent "
                "key domain, so no valid referenced key exists"
            )
        if child_unique and requested_non_null > eligible_count:
            restrictions = ", ".join(
                f"{name} allowed_values="
                f"{list(_allowed_values(child_schema.columns[child_key[i]]) or [])!r}"
                for i, name in enumerate(child_key)
                if _allowed_values(child_schema.columns[child_key[i]])
                is not None
            )
            raise CompositeKeyConstraintError(
                f"unique child key requires {requested_non_null} non-null "
                "rows, but the child isin restrictions permit at most "
                f"{eligible_count} composite parent key(s) ({restrictions})"
            )

    if required_eligible > n_distinct_parent:
        raise CompositeKeyConstraintError(
            f"child needs {required_eligible} distinct referenced parent "
            f"key(s), but only {n_distinct_parent} distinct parent key(s) "
            "were requested"
        )

    if parent_domain is not None and n_distinct_parent > len(parent_domain):
        raise CompositeKeyConstraintError(
            f"only {len(parent_domain)} distinct parent key combinations "
            "are available"
        )

    @st.composite
    def strategy(draw: Any) -> tuple[pd.DataFrame, pd.DataFrame]:
        eligible_keys: list[tuple[Any, ...]] = []
        if required_eligible:
            if eligible_domain is not None:
                eligible_keys = draw(
                    st.lists(
                        st.sampled_from(eligible_domain),
                        min_size=required_eligible,
                        max_size=required_eligible,
                        unique=True,
                    )
                )
            else:
                eligible_keys = draw(
                    st.lists(
                        _eligible_tuple_strategy(
                            parent_schema,
                            child_schema,
                            parent_key,
                            child_key,
                        ),
                        min_size=required_eligible,
                        max_size=required_eligible,
                        unique=True,
                    )
                )

        parent_pool: list[tuple[Any, ...]] = list(eligible_keys)
        remaining_distinct = n_distinct_parent - len(parent_pool)
        if remaining_distinct > 0:
            if parent_domain is not None:
                used = set(parent_pool)
                complement = [key for key in parent_domain if key not in used]
                extra_keys = draw(
                    st.lists(
                        st.sampled_from(complement),
                        min_size=remaining_distinct,
                        max_size=remaining_distinct,
                        unique=True,
                    )
                )
            else:
                used = set(parent_pool)
                parent_tuple_strategy = _element_tuple_strategy(
                    parent_schema, parent_key
                ).filter(lambda key: key not in used)
                extra_keys = draw(
                    st.lists(
                        parent_tuple_strategy,
                        min_size=remaining_distinct,
                        max_size=remaining_distinct,
                        unique=True,
                    )
                )
            parent_pool.extend(extra_keys)

        if parent_keys_unique:
            parent_row_keys = parent_pool
        else:
            parent_row_keys = list(parent_pool[:n_distinct_parent])
            if n_parent > n_distinct_parent:
                repeated = draw(
                    st.lists(
                        st.sampled_from(parent_pool),
                        min_size=n_parent - n_distinct_parent,
                        max_size=n_parent - n_distinct_parent,
                    )
                )
                parent_row_keys.extend(repeated)

        parent_frame = _attribute_frame(
            draw, parent_schema, parent_key, n_parent
        )
        parent_frame = _attach_keys(
            parent_frame, parent_schema, parent_key, parent_row_keys
        )

        referencable_keys = list(eligible_keys)
        if eligible_domain is not None:
            eligible_set = set(eligible_domain)
            referencable_keys = [
                key for key in parent_pool if key in eligible_set
            ]

        null_row_count = requested_null
        if auto_null:
            max_null_rows = (
                min(1, n_child - 1) if child_unique else n_child - 1
            )
            null_row_count = draw(
                st.integers(min_value=0, max_value=max(0, max_null_rows))
            )

        null_positions: set[int] = set()
        if null_row_count:
            null_positions = set(
                draw(
                    st.lists(
                        st.integers(0, n_child - 1),
                        min_size=null_row_count,
                        max_size=null_row_count,
                        unique=True,
                    )
                )
            )

        non_null_positions = [
            index for index in range(n_child) if index not in null_positions
        ]
        child_row_keys: list[tuple[Any, ...] | None] = [None] * n_child
        if non_null_positions:
            if child_unique:
                referenced = draw(
                    st.lists(
                        st.sampled_from(referencable_keys),
                        min_size=len(non_null_positions),
                        max_size=len(non_null_positions),
                        unique=True,
                    )
                )
            else:
                referenced = draw(
                    st.lists(
                        st.sampled_from(referencable_keys),
                        min_size=len(non_null_positions),
                        max_size=len(non_null_positions),
                    )
                )
            for position, key in zip(non_null_positions, referenced):
                child_row_keys[position] = key

        child_frame = _attribute_frame(draw, child_schema, child_key, n_child)
        non_null_key_rows = [
            key if key is not None else tuple(None for _ in child_key)
            for key in child_row_keys
        ]
        child_frame = _attach_keys(
            child_frame,
            child_schema,
            child_key,
            non_null_key_rows,
            null_rows=null_positions,
        )

        if validate:
            parent_schema.validate(parent_frame)
            child_schema.validate(child_frame)
        return RelatedFrames(parent_frame, child_frame)

    return strategy()


parent_child_strategy = related_dataframes_strategy
parent_child_dataframes_strategy = related_dataframes_strategy
related_tables_strategy = related_dataframes_strategy
composite_key_strategy = related_dataframes_strategy
create_composite_key_strategy = related_dataframes_strategy
composite_key_pair_strategy = related_dataframes_strategy
composite_foreign_key_strategy = related_dataframes_strategy
foreign_key_dataframes_strategy = related_dataframes_strategy
fact_dimension_strategy = related_dataframes_strategy
combined_strategy = related_dataframes_strategy
two_table_strategy = related_dataframes_strategy
table_pair_strategy = related_dataframes_strategy
parent_child_pair_strategy = related_dataframes_strategy
referential_strategy = related_dataframes_strategy
create_parent_child_strategy = related_dataframes_strategy
make_related_dataframes_strategy = related_dataframes_strategy
build_strategy = related_dataframes_strategy

parent_schema = scenario_parent_schema()
dimension_schema = parent_schema
dim_schema = parent_schema
child_schema = scenario_child_schema()
fact_schema = child_schema


class CompositeKeyStrategy:
    """Object-style entry for callers that prefer a reusable factory."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs

    def strategy(self) -> st.SearchStrategy[tuple[pd.DataFrame, pd.DataFrame]]:
        return related_dataframes_strategy(*self.args, **self.kwargs)

    def __call__(self) -> st.SearchStrategy[tuple[pd.DataFrame, pd.DataFrame]]:
        return self.strategy()


def regression_strategy() -> st.SearchStrategy[
    tuple[pd.DataFrame, pd.DataFrame]
]:
    """The requested 3-parent, 5-non-null-child regression strategy."""
    return related_dataframes_strategy(
        parent_schema=scenario_parent_schema(),
        child_schema=scenario_child_schema(),
        parent_rows=3,
        child_rows=5,
        non_null_child_rows=5,
    )


def impossible_unique_child_regression_strategy() -> st.SearchStrategy[
    tuple[pd.DataFrame, pd.DataFrame]
]:
    """Construct the requested impossible required+unique child case."""
    return related_dataframes_strategy(
        parent_schema=scenario_parent_schema(),
        child_schema=scenario_child_schema(required_unique_key=True),
        parent_rows=3,
        child_rows=5,
        child_key_required=True,
        unique_child_key=True,
    )


# A zero-argument strategy entry is convenient for test discovery and REPL.
scenario_strategy = regression_strategy
