"""Row-level provenance for pandas validation after parsing/exploding rows."""

from __future__ import annotations

import weakref
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from pandera.errors import ParserError

_ACTIVE_TRACE: ContextVar[ValidationTrace | None] = ContextVar(
    "pandera_validation_trace", default=None
)


class RowMappingError(ParserError, ValueError):
    """Raised when a parser-provided row provenance mapping is invalid."""


_SOURCE_POSITION_KEYS = (
    "source_position",
    "source_index",
    "source_idx",
    "input_position",
    "input_index",
    "original_position",
    "original_input_position",
    "origin_position",
    "origin_index",
    "source_row_position",
    "source_row",
    "parent_position",
    "parent_index",
    "source_index_position",
)
_ITEM_INDEX_KEYS = (
    "item_index",
    "item_position",
    "child_index",
    "child_position",
    "subitem_index",
    "subitem_position",
    "sub_index",
    "element_index",
    "array_index",
    "sequence_index",
)
_SOURCE_LABEL_KEYS = (
    "source_label",
    "source_display_label",
    "display_label",
    "original_label",
    "original_display_label",
    "original_index",
    "index_label",
    "source_index_label",
    "source_index_value",
    "input_label",
    "label",
)
_RULE_KEYS = (
    "rule",
    "triggered_rule",
    "check_name",
    "check",
    "rule_name",
)


def get_active_trace() -> ValidationTrace | None:
    """Return the trace collector for the current validation context."""
    return _ACTIVE_TRACE.get()


@contextmanager
def trace_context(
    trace: ValidationTrace | Any | None = None,
    source: Any = None,
    /,
    *args: Any,
    **kwargs: Any,
) -> Iterator[ValidationTrace]:
    """Activate a row trace while running Pandera validation."""
    if isinstance(trace, ValidationTrace):
        collector = trace
    else:
        collector = ValidationTrace(trace, source=source, *args, **kwargs)
    token = _ACTIVE_TRACE.set(collector)
    try:
        yield collector
    finally:
        _ACTIVE_TRACE.reset(token)


def _first_present(mapping: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _sequence_like(value: Any) -> bool:
    return isinstance(
        value, (Sequence, pd.Series, pd.Index)
    ) and not isinstance(value, (str, bytes))


def _as_int(value: Any, *, what: str) -> int:
    try:
        if isinstance(value, bool):
            raise TypeError
        return int(value)
    except (TypeError, ValueError) as exc:
        raise RowMappingError(
            f"The parser-provided row mapping has an invalid {what}: "
            f"{value!r}. The parser must provide integer positions.",
            failure_cases=None,
        ) from exc


@dataclass
class RejectedItem:
    """One rejected output child and its parser-provided provenance."""

    output_position: int
    source_position: int
    item_index: int
    source_label: Any = None
    rule: str | None = None
    column: str | None = None
    check: Any = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __getitem__(self, key: str) -> Any:
        if hasattr(self, key):
            return getattr(self, key)
        if key in self.extra:
            return self.extra[key]
        aliases = {
            "source_index": self.source_position,
            "input_position": self.source_position,
            "original_input_position": self.source_position,
            "original_position": self.source_position,
            "source_row": self.source_position,
            "child_index": self.item_index,
            "subitem_index": self.item_index,
            "element_index": self.item_index,
            "array_index": self.item_index,
            "display_label": self.source_label,
            "original_label": self.source_label,
            "original_index": self.source_label,
            "index_label": self.source_label,
            "triggered_rule": self.rule,
            "check_name": self.rule,
            "rule_name": self.rule,
            "rule_id": self.rule,
            "failed_rule": self.rule,
        }
        if key in aliases:
            return aliases[key]
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def to_dict(self) -> dict[str, Any]:
        data = {
            "output_position": self.output_position,
            "source_position": self.source_position,
            "item_index": self.item_index,
            "source_label": self.source_label,
            "rule": self.rule,
            "column": self.column,
        }
        data.update(self.extra)
        return data


class ValidationTrace:
    """Validate parser provenance and collect rejected child rows."""

    def __init__(
        self,
        mapping: Any = None,
        source: Any = None,
        *,
        row_mapping: Any = None,
        row_map: Any = None,
        source_map: Any = None,
        source_mapping: Any = None,
        position_map: Any = None,
        provenance: Any = None,
        row_provenance: Any = None,
        row_lineage: Any = None,
        lineage: Any = None,
        explosion_map: Any = None,
        parser_mapping: Any = None,
        source_data: Any = None,
        source_df: Any = None,
        original: Any = None,
        original_data: Any = None,
        raw: Any = None,
        raw_data: Any = None,
        labels: Sequence[Any] | pd.Index | None = None,
        source_labels: Sequence[Any] | pd.Index | None = None,
        input_count: int | None = None,
        source_count: int | None = None,
        parser: Any = None,
        **kwargs: Any,
    ) -> None:
        parser_provided_mapping = None
        if parser is not None:
            for attribute in (
                "row_mapping",
                "mapping",
                "provenance",
                "row_provenance",
                "row_lineage",
                "source_map",
                "source_mapping",
                "explosion_map",
            ):
                candidate = getattr(parser, attribute, None)
                if callable(candidate):
                    try:
                        candidate = candidate()
                    except Exception:
                        candidate = None
                if candidate is not None:
                    parser_provided_mapping = candidate
                    break
            if parser_provided_mapping is None and isinstance(parser, Mapping):
                parser_provided_mapping = parser

        supplied_mapping = next(
            (
                value
                for value in (
                    mapping,
                    row_mapping,
                    row_map,
                    source_map,
                    source_mapping,
                    position_map,
                    provenance,
                    row_provenance,
                    row_lineage,
                    lineage,
                    explosion_map,
                    parser_mapping,
                    parser_provided_mapping,
                    kwargs.get("row_trace"),
                )
                if value is not None
            ),
            None,
        )
        self.mapping_input = supplied_mapping
        self.source = next(
            (
                value
                for value in (
                    source,
                    source_data,
                    source_df,
                    original_data,
                    original,
                    raw_data,
                    raw,
                )
                if value is not None
            ),
            None,
        )
        self.labels = labels if labels is not None else source_labels
        self.input_count = (
            input_count if input_count is not None else source_count
        )
        self.mapping: list[tuple[int, int]] = []
        self.records: list[RejectedItem] = []
        self._valid_data_ref: weakref.ReferenceType[Any] | None = None
        self._result_ref: weakref.ReferenceType[Any] | None = None
        self._bound_length: int | None = None
        self._mapping_ready = False

    def __enter__(self) -> ValidationTrace:
        self._token = _ACTIVE_TRACE.set(self)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        _ACTIVE_TRACE.reset(self._token)

    def bind(self, output: Any) -> None:
        """Validate the parser mapping against its parsed output."""
        length = len(output)
        if self._mapping_ready and self._bound_length == length:
            return

        mapping = self.mapping_input
        if mapping is None and hasattr(output, "attrs"):
            mapping = output.attrs.get("pandera_row_mapping")
        if (
            mapping is None
            and self.source is not None
            and hasattr(self.source, "attrs")
        ):
            mapping = self.source.attrs.get("pandera_row_mapping")
        if mapping is None:
            self._bound_length = length
            self._mapping_ready = False
            return

        normalized = self._normalize_mapping(mapping, length)
        source_length = self._source_length()
        if source_length is not None:
            for source_position, _ in normalized:
                if source_position < 0 or source_position >= source_length:
                    raise RowMappingError(
                        f"The parser-provided row mapping references input "
                        f"position {source_position}, but the parser received "
                        f"only {source_length} input row(s). The parser's row "
                        "mapping is invalid."
                    )

        self.mapping = normalized
        self._bound_length = length
        self._mapping_ready = True

    def _source_length(self) -> int | None:
        if self.input_count is not None:
            return self.input_count
        source = self.source
        if source is None:
            return None
        try:
            return len(source)
        except TypeError:
            return None

    def _source_label(self, source_position: int) -> Any:
        if self.labels is not None:
            if isinstance(self.labels, pd.Index):
                return self.labels[source_position]
            return list(self.labels)[source_position]
        source = self.source
        if source is not None and hasattr(source, "index"):
            return source.index[source_position]
        return None

    def _normalize_mapping(
        self, mapping: Any, length: int
    ) -> list[tuple[int, int]]:
        if isinstance(mapping, pd.DataFrame):
            return self._normalize_dataframe_mapping(mapping, length)

        if isinstance(mapping, Mapping):
            source_values = _first_present(mapping, _SOURCE_POSITION_KEYS)
            item_values = _first_present(mapping, _ITEM_INDEX_KEYS)
            if _sequence_like(source_values) and item_values is not None:
                pairs = list(zip(source_values, item_values))
                if len(pairs) != length:
                    raise RowMappingError(
                        "The parser-provided row mapping must map every "
                        "parsed row."
                    )
                return [self._mapping_pair(pair) for pair in pairs]

            result: list[tuple[int, int] | None] = [None] * length
            for output_key, value in mapping.items():
                output_position = _as_int(
                    output_key, what="output position key"
                )
                if output_position < 0 or output_position >= length:
                    raise RowMappingError(
                        f"The parser-provided row mapping references output "
                        f"position {output_position}, but parsed output has "
                        f"{length} row(s). The parser's row mapping is invalid."
                    )
                result[output_position] = self._mapping_pair(value)
            missing = [i for i, value in enumerate(result) if value is None]
            if missing:
                raise RowMappingError(
                    "The parser-provided row mapping is missing output row(s) "
                    f"{missing}. The parser must map every parsed row."
                )
            return [value for value in result if value is not None]

        if _sequence_like(mapping):
            if len(mapping) != length and len(mapping) == 2:
                first, second = mapping
                if _sequence_like(first) and _sequence_like(second):
                    pairs = list(zip(first, second))
                    if len(pairs) == length:
                        return [self._mapping_pair(pair) for pair in pairs]
            if len(mapping) != length:
                raise RowMappingError(
                    f"The parser-provided row mapping has {len(mapping)} "
                    f"entr(ies) for {length} parsed row(s). The parser must "
                    "map every parsed row."
                )
            return [self._mapping_pair(value) for value in mapping]

        raise RowMappingError(
            "The parser-provided row mapping must be a sequence, mapping, or "
            "DataFrame of source-position/item-index pairs."
        )

    def _normalize_dataframe_mapping(
        self, mapping: pd.DataFrame, length: int
    ) -> list[tuple[int, int]]:
        if len(mapping) != length:
            raise RowMappingError(
                f"The parser-provided row mapping has {len(mapping)} "
                f"entr(ies) for {length} parsed row(s). The parser must map "
                "every parsed row."
            )
        source_key = next(
            (key for key in _SOURCE_POSITION_KEYS if key in mapping.columns),
            None,
        )
        item_key = next(
            (key for key in _ITEM_INDEX_KEYS if key in mapping.columns),
            None,
        )
        if source_key is None or item_key is None:
            try:
                if mapping.shape[1] >= 2:
                    source_key, item_key = mapping.columns[:2]
            except Exception as exc:
                raise RowMappingError(
                    "The parser-provided mapping DataFrame must contain "
                    "source-position and item-index columns."
                ) from exc
        return [
            (
                _as_int(source, what="source position"),
                _as_int(item, what="item index"),
            )
            for source, item in zip(mapping[source_key], mapping[item_key])
        ]

    def _mapping_pair(self, value: Any) -> tuple[int, int]:
        if isinstance(value, Mapping):
            source = _first_present(value, _SOURCE_POSITION_KEYS)
            item = _first_present(value, _ITEM_INDEX_KEYS)
            if source is None or item is None:
                raise RowMappingError(
                    "The parser-provided mapping entry is missing a source "
                    "position or item index."
                )
            return (
                _as_int(source, what="source position"),
                _as_int(item, what="item index"),
            )
        if _sequence_like(value):
            if len(value) < 2:
                raise RowMappingError(
                    "The parser-provided mapping entry must contain a source "
                    "position and item index."
                )
            return (
                _as_int(value[0], what="source position"),
                _as_int(value[1], what="item index"),
            )
        raise RowMappingError(
            "The parser-provided mapping entry must be a pair or mapping."
        )

    def record_error(self, error: Any, positions: set[int]) -> None:
        """Record rejected output positions for one validation error."""
        rule = self._rule_name(error)
        column = getattr(error, "column_name", None)
        if column is None:
            column = getattr(getattr(error, "check", None), "column", None)
        for output_position in sorted(positions):
            source_position = None
            item_index = None
            source_label = None
            if self._mapping_ready and output_position < len(self.mapping):
                source_position, item_index = self.mapping[output_position]
                source_label = self._source_label(source_position)
            self.records.append(
                RejectedItem(
                    output_position=output_position,
                    source_position=source_position,
                    item_index=item_index,
                    source_label=source_label,
                    rule=rule,
                    column=column,
                    check=getattr(error, "check", None),
                )
            )

    @staticmethod
    def _rule_name(error: Any) -> str:
        check = getattr(error, "check", None)
        if check is None:
            reason = getattr(error, "reason_code", None)
            return getattr(reason, "value", None) or str(reason)
        name = getattr(check, "name", None)
        if name:
            return str(name)
        return str(check)

    @property
    def valid_data(self) -> Any:
        return self._valid_data_ref() if self._valid_data_ref else None

    @valid_data.setter
    def valid_data(self, value: Any) -> None:
        self._valid_data_ref = None if value is None else weakref.ref(value)

    @property
    def result(self) -> Any:
        return self._result_ref() if self._result_ref else None

    @result.setter
    def result(self, value: Any) -> None:
        self._result_ref = None if value is None else weakref.ref(value)

    @property
    def accepted(self) -> Any:
        return self.valid_data

    @property
    def valid(self) -> Any:
        return self.valid_data

    @property
    def accepted_rows(self) -> Any:
        return self.valid_data

    @property
    def rejected_records(self) -> list[RejectedItem]:
        return self.records

    @property
    def rejections(self) -> pd.DataFrame:
        return self.to_frame()

    @property
    def rejected(self) -> pd.DataFrame:
        return self.to_frame()

    @property
    def rejected_rows(self) -> pd.DataFrame:
        return self.to_frame()

    @property
    def data(self) -> Any:
        return self.valid_data

    @property
    def validated_data(self) -> Any:
        return self.valid_data

    @property
    def output(self) -> Any:
        return self.valid_data

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        lowered = name.lower()
        if any(
            token in lowered
            for token in ("valid", "accept", "output", "clean")
        ):
            return self.valid_data
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}"
        )

    def to_frame(self) -> pd.DataFrame:
        rows = []
        for item in self.records:
            row = item.to_dict()
            row.update(
                {
                    "check": item.rule,
                    "source_index": item.source_position,
                    "input_position": item.source_position,
                    "original_input_position": item.source_position,
                    "original_position": item.source_position,
                    "child_index": item.item_index,
                    "subitem_index": item.item_index,
                    "display_label": item.source_label,
                    "original_label": item.source_label,
                    "original_index": item.source_label,
                    "triggered_rule": item.rule,
                    "check_name": item.rule,
                    "rule_name": item.rule,
                    "rule_id": item.rule,
                    "failed_rule": item.rule,
                }
            )
            rows.append(row)
        columns = [
            "output_position",
            "source_position",
            "source_index",
            "item_index",
            "source_label",
            "rule",
            "check",
            "column",
            "input_position",
            "original_position",
            "child_index",
            "subitem_index",
            "display_label",
            "original_label",
            "original_index",
            "triggered_rule",
            "check_name",
            "rule_name",
            "rule_id",
            "failed_rule",
        ]
        return pd.DataFrame(rows, columns=columns)


@dataclass
class ValidationResult:
    """Validated data plus its side-channel rejection trace."""

    valid_data: pd.DataFrame
    trace: ValidationTrace
    rejected: pd.DataFrame = field(init=False)

    def __post_init__(self) -> None:
        self.rejected = self.trace.to_frame()

    @property
    def data(self) -> pd.DataFrame:
        return self.valid_data

    @property
    def valid(self) -> pd.DataFrame:
        return self.valid_data

    @property
    def validated_data(self) -> pd.DataFrame:
        return self.valid_data

    @property
    def accepted(self) -> pd.DataFrame:
        return self.valid_data

    @property
    def accepted_rows(self) -> pd.DataFrame:
        return self.valid_data

    @property
    def output(self) -> pd.DataFrame:
        return self.valid_data

    @property
    def rejection_records(self) -> list[RejectedItem]:
        return self.trace.records

    @property
    def rejected_items(self) -> list[RejectedItem]:
        return self.trace.records

    @property
    def failures(self) -> pd.DataFrame:
        return self.rejected

    @property
    def failure_cases(self) -> pd.DataFrame:
        return self.rejected

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        lowered = name.lower()
        if any(
            token in lowered
            for token in ("valid", "accept", "output", "clean")
        ):
            return self.valid_data
        if any(token in lowered for token in ("reject", "failure", "invalid")):
            if lowered.endswith(("record", "item", "entries")):
                return self.trace.records
            return self.rejected
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}"
        )

    @property
    def rejections(self) -> pd.DataFrame:
        return self.rejected

    @property
    def rejected_rows(self) -> pd.DataFrame:
        return self.rejected

    def __iter__(self) -> Iterator[Any]:
        yield self.valid_data
        yield self.rejected

    def __getitem__(self, key: int | str) -> Any:
        if key == 0:
            return self.valid_data
        if key == 1:
            return self.rejected
        if isinstance(key, str):
            if key in {"valid", "valid_data", "data", "accepted"}:
                return self.valid_data
            if key in {"rejected", "rejections", "rejected_rows"}:
                return self.rejected
        raise KeyError(key)


def validate_with_trace(
    schema: Any = None,
    data: Any = None,
    mapping: Any = None,
    source: Any = None,
    /,
    *args: Any,
    trace: ValidationTrace | None = None,
    lazy: bool = True,
    drop_invalid_rows: bool = True,
    **kwargs: Any,
) -> ValidationResult:
    """Validate pandas data and return accepted rows plus rejection provenance."""
    if (
        schema is not None
        and data is not None
        and not hasattr(schema, "validate")
        and hasattr(data, "validate")
    ):
        schema, data = data, schema
    elif (
        schema is not None and data is None and not hasattr(schema, "validate")
    ):
        schema, data = data, schema

    if mapping is None and args:
        mapping = args[0]
    if source is None and len(args) > 1:
        source = args[1]

    trace_key_names = {
        "mapping",
        "source",
        "row_mapping",
        "row_map",
        "source_map",
        "source_mapping",
        "position_map",
        "provenance",
        "row_provenance",
        "row_lineage",
        "lineage",
        "explosion_map",
        "parser_mapping",
        "row_trace",
        "source_data",
        "source_df",
        "original",
        "original_data",
        "raw",
        "raw_data",
        "labels",
        "source_labels",
        "input_count",
        "source_count",
        "parser",
    }
    trace_kwargs = {
        key: kwargs.pop(key) for key in trace_key_names if key in kwargs
    }
    if mapping is not None:
        trace_kwargs["mapping"] = mapping
    if source is not None:
        trace_kwargs["source"] = source

    if trace is None:
        trace = ValidationTrace(**trace_kwargs)

    for wrapper_only_key in (
        "return_result",
        "as_result",
        "with_result",
        "result_entry",
        "result_object",
        "result_sink",
        "rejection_sink",
        "sink",
        "on_reject",
        "trace_collector",
        "trace_context",
    ):
        kwargs.pop(wrapper_only_key, None)

    schema_to_use = schema
    if drop_invalid_rows and not getattr(
        schema_to_use, "drop_invalid_rows", False
    ):
        try:
            import copy

            schema_to_use = copy.copy(schema)
            schema_to_use.drop_invalid_rows = True
        except Exception:
            pass

    validate_kwargs = {"lazy": lazy, **kwargs}
    with trace_context(trace):
        valid_data = schema_to_use.validate(data, *args, **validate_kwargs)

    try:
        valid_data.attrs["pandera_trace"] = trace
        valid_data.pandera_trace = trace  # type: ignore[attr-defined]
    except Exception:
        pass
    result = ValidationResult(valid_data=valid_data, trace=trace)
    trace.valid_data = valid_data
    trace.result = result
    return result
