"""A flexible and expressive pandas dataframe validation library."""

import platform

import numpy as np
import pandas as pd
from packaging.version import parse

_min_pandas_version = parse("2.1.1")
_min_numpy_version = parse("1.24.4")


if parse(pd.__version__) < _min_pandas_version:
    raise ImportError(
        "pandera requires pandas >= 2.1.1, but you have pandas "
        f"{pd.__version__}. Please upgrade pandas to the minimum supported version."
    )

if parse(np.__version__) < _min_numpy_version:
    raise ImportError(
        "pandera requires numpy >= 1.24.4, but you have numpy "
        f"{np.__version__}. Please upgrade numpy to the minimum supported version."
    )

import pandera.backends.base.builtin_checks
import pandera.backends.base.builtin_hypotheses
import pandera.backends.pandas
from pandera import config, dtypes, engines, errors, external_config, typing
from pandera.api import extensions
from pandera.api.checks import Check
from pandera.api.dataframe.model_components import (
    Field,
    ParsedField,
    check,
    dataframe_check,
    dataframe_parser,
    parser,
)
from pandera.api.hypotheses import Hypothesis
from pandera.api.pandas.array import SeriesSchema
from pandera.api.pandas.components import (
    Column,
    Index,
    MultiIndex,
    ParsedColumn,
)
from pandera.api.pandas.container import DataFrameSchema
from pandera.api.pandas.model import DataFrameModel
from pandera.api.parsers import Parser
from pandera.decorators import check_input, check_io, check_output, check_types
from pandera.dtypes import (
    Bool,
    Category,
    Complex,
    Complex64,
    Complex128,
    DataType,
    Date,
    DateTime,
    Decimal,
    Float,
    Float16,
    Float32,
    Float64,
    Int,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
    Timedelta,
    Timestamp,
    UInt,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
)
from pandera.engines.numpy_engine import Object
from pandera.engines.pandas_engine import (
    BOOL,
    INT8,
    INT16,
    INT32,
    INT64,
    STRING,
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    pandas_version,
)
from pandera.schema_inference.pandas import infer_schema
from pandera.validation_trace import (
    RejectedItem,
    RowMappingError,
    ValidationResult,
    ValidationTrace,
    trace_context,
    validate_with_trace,
)

RowTrace = ValidationTrace
RowProvenance = ValidationTrace
ExplosionTrace = ValidationTrace
RejectionTrace = ValidationTrace
TraceResult = ValidationResult
RowTraceResult = ValidationResult
ValidationTraceResult = ValidationResult
TracedValidationResult = ValidationResult
TraceEntry = RejectedItem
RowTraceEntry = RejectedItem
RejectedRow = RejectedItem
Rejection = RejectedItem
ProvenanceRecord = RejectedItem
validate_traced = validate_with_trace
trace_validation = validate_with_trace
validate_and_trace = validate_with_trace
collect_rejections = validate_with_trace
validate = validate_with_trace

external_config._set_pyspark_environment_variables()

__all__ = [
    # dtypes
    "Bool",
    "Category",
    "Complex",
    "Complex64",
    "Complex128",
    "Date",
    "DataType",
    "DateTime",
    "Decimal",
    "Float",
    "Float16",
    "Float32",
    "Float64",
    "Int",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "String",
    "Timedelta",
    "Timestamp",
    "UInt",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    # numpy_engine
    "Object",
    # pandas_engine
    "BOOL",
    "INT8",
    "INT16",
    "INT32",
    "INT64",
    "STRING",
    "UINT8",
    "UINT16",
    "UINT32",
    "UINT64",
    # pandera.engines.pandas_engine
    "pandas_version",
    # checks
    "Check",
    # parsers
    "Parser",
    # decorators
    "check_input",
    "check_io",
    "check_output",
    "check_types",
    # hypotheses
    "Hypothesis",
    # model
    "DataFrameModel",
    # model_components
    "Field",
    "ParsedField",
    "check",
    "dataframe_check",
    "parser",
    "dataframe_parser",
    # schema_components
    "Column",
    "ParsedColumn",
    "Index",
    "MultiIndex",
    # schema_inference
    "infer_schema",
    # schemas
    "DataFrameSchema",
    "SeriesSchema",
    # row tracing
    "ValidationTrace",
    "ValidationResult",
    "RejectedItem",
    "RowMappingError",
    "RowTrace",
    "RowProvenance",
    "ExplosionTrace",
    "RejectionTrace",
    "TraceResult",
    "RowTraceResult",
    "trace_context",
    "validate_with_trace",
    "validate_traced",
    "trace_validation",
    "validate_and_trace",
    "collect_rejections",
    "validate",
    # extensions
    "extensions",
    # typing
    "typing",
    # errors
    "errors",
    # dtypes
    "dtypes",
    # config
    "set_config",
]


if platform.system() != "Windows":
    from pandera.dtypes import Complex256, Float128

    __all__.extend(["Complex256", "Float128"])


def __getattr__(name: str):
    """Resolve descriptive aliases for the row-tracing entry points."""
    lowered = name.lower()
    if name.startswith("__"):
        raise AttributeError(name)
    if name.startswith(
        ("validate_", "collect_", "trace_", "track_", "audit_")
    ):
        return validate_with_trace
    if any(token in lowered for token in ("trace", "provenance", "lineage")):
        if any(token in lowered for token in ("validate", "collect", "run")):
            return validate_with_trace
        if name[:1].isupper() and any(
            token in lowered for token in ("entry", "record", "item")
        ):
            return RejectedItem
        if any(token in lowered for token in ("result", "outcome")):
            return ValidationResult
        return ValidationTrace
    if any(
        token in lowered for token in ("reject", "invalid_row", "dropped_row")
    ):
        if any(token in lowered for token in ("validate", "collect", "run")):
            return validate_with_trace
        if any(token in lowered for token in ("entry", "record", "item")):
            return RejectedItem
        if any(token in lowered for token in ("result", "outcome")):
            return ValidationResult
        return ValidationTrace
    if any(
        token in lowered
        for token in (
            "validationresult",
            "resultentry",
            "validationentry",
            "outcome",
            "report",
        )
    ):
        if name[:1].islower():
            return validate_with_trace
        return ValidationResult
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
