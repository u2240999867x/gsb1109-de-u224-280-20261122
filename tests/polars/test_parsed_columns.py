"""Tests for polars ``ParsedColumn``/``ParsedField`` derived columns."""

import polars as pl
import pytest

import pandera.config as _config
import pandera.polars as pa
from pandera.errors import ParserSourceError, SchemaInitError

# User-defined parsers / derived columns are implemented on the native polars
# backend, not the narwhals backend (which has no parser step for non-pandas
# frames).
pytestmark = pytest.mark.skipif(
    _config.CONFIG.use_narwhals_backend,
    reason="derived columns are not supported by the narwhals backend",
)


def _split_len():
    """A parser fitting a ``str`` source into an ``Int64`` word count.

    ``polars`` ``.str.split(" ").list.len()`` yields ``UInt32``, so it is cast
    to match the declared ``Int64`` dtype.
    """

    def _fn(s: pl.Series) -> pl.Series:
        return s.str.split(" ").list.len().cast(pl.Int64)

    return _fn


def test_parsed_column_derives_a_column_from_one_source():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str),
            "n_words": pa.ParsedColumn(
                pl.Int64, source="body", parser=_split_len()
            ),
        }
    )
    out = schema.validate(pl.DataFrame({"body": ["a b c", "z", "p q"]}))
    assert out["n_words"].to_list() == [3, 1, 2]
    assert out["n_words"].dtype == pl.Int64


def test_parsed_column_with_multiple_sources_gets_a_frame():
    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(pl.Int64),
            "b": pa.Column(pl.Int64),
            "total": pa.ParsedColumn(
                pl.Int64,
                source=["a", "b"],
                parser=lambda df: df.select(
                    (pl.col("a") + pl.col("b")).alias("total")
                ),
            ),
        }
    )
    out = schema.validate(pl.DataFrame({"a": [1, 2], "b": [10, 20]}))
    assert out["total"].to_list() == [11, 22]


def test_parsed_column_requires_a_parser():
    with pytest.raises(SchemaInitError, match="requires a"):
        pa.ParsedColumn(pl.Int64, source="body", parser=None)


def test_parsed_column_rejects_unknown_source_at_schema_build():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str),
            "n_words": pa.ParsedColumn(
                pl.Int64, source="missing", parser=_split_len()
            ),
        }
    )
    with pytest.raises(SchemaInitError, match="not declared"):
        schema.validate(pl.DataFrame({"body": ["x"]}))


def test_parsed_column_missing_source_at_runtime():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str, required=False),
            "n_words": pa.ParsedColumn(
                pl.Int64, source="body", parser=_split_len()
            ),
        }
    )
    with pytest.raises(ParserSourceError, match="body"):
        schema.validate(pl.DataFrame({"other": ["x"]}))


def test_parsed_field_on_a_model():
    class Model(pa.DataFrameModel):
        body: str
        n_words: int = pa.ParsedField(source="body", parser=_split_len())

    out = Model.validate(pl.DataFrame({"body": ["a b", "c"]}))
    assert out["n_words"].to_list() == [2, 1]


def test_parsed_field_uses_config_parser_source():
    class Model(pa.DataFrameModel):
        body: str
        n_words: int = pa.ParsedField(parser=_split_len())

        class Config:
            parser_source = "body"

    assert Model.validate(pl.DataFrame({"body": ["x y z"]}))[
        "n_words"
    ].to_list() == [3]


def test_parsed_field_accepts_checks():
    class Model(pa.DataFrameModel):
        body: str
        n_words: int = pa.ParsedField(source="body", parser=_split_len(), ge=1)

    assert Model.validate(pl.DataFrame({"body": ["a b"]}))[
        "n_words"
    ].to_list() == [2]


def test_parsed_column_works_with_lazyframe_input():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str),
            "n_words": pa.ParsedColumn(
                pl.Int64, source="body", parser=_split_len()
            ),
        }
    )
    out = schema.validate(pl.LazyFrame({"body": ["a b c", "d"]})).collect()
    assert out["n_words"].to_list() == [3, 1]


def test_parsed_columns_are_ordered_by_dependency():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str),
            "n_words": pa.ParsedColumn(
                pl.Int64, source="body", parser=_split_len()
            ),
            "n_words_times_two": pa.ParsedColumn(
                pl.Int64,
                source="n_words",
                parser=lambda s: s * 2,
            ),
        }
    )
    out = schema.validate(pl.DataFrame({"body": ["a b c"]}))
    assert out["n_words_times_two"].to_list() == [6]


class _Service:
    """One billed call per ``batch`` invocation, adding a fixed value.

    The log is class-level because pandera deep-copies columns (and their
    parsers) into the schema.
    """

    add = {"A": 1, "B": 10, "C": 100}
    call_log: list = []

    def __init__(self, service):
        self.service = service

    def bind(self, ctx):  # pragma: no cover - batched instead
        raise AssertionError("should have been batched")

    def batch_key(self, ctx):
        return self.service

    @classmethod
    def batch(cls, items):
        service = {parser.service for parser, _ in items}.pop()
        plan = [(ctx.target, ctx.source[0]) for _, ctx in items]

        def _fn(df):
            cls.call_log.append(service)
            return df.with_columns(
                [
                    (pl.col(source) + cls.add[service]).alias(target)
                    for target, source in plan
                ]
            )

        return _fn


def test_dependent_batched_columns_use_the_fewest_service_calls():
    _Service.call_log = []
    schema = pa.DataFrameSchema(
        {
            "dom_raw": pa.Column(pl.Int64),
            "intl_raw": pa.Column(pl.Int64),
            "dom_code": pa.ParsedColumn(
                pl.Int64, source="dom_raw", parser=_Service("A")
            ),
            "dom_fee": pa.ParsedColumn(
                pl.Int64, source="dom_code", parser=_Service("B")
            ),
            "dom_tier": pa.ParsedColumn(
                pl.Int64, source="dom_fee", parser=_Service("C")
            ),
            "intl_fee": pa.ParsedColumn(
                pl.Int64, source="intl_raw", parser=_Service("B")
            ),
            "intl_tier": pa.ParsedColumn(
                pl.Int64, source="intl_fee", parser=_Service("C")
            ),
            "intl_code": pa.ParsedColumn(
                pl.Int64, source="intl_tier", parser=_Service("A")
            ),
            "fee_note": pa.ParsedColumn(
                pl.Int64, source="intl_raw", parser=_Service("B")
            ),
        }
    )
    out = schema.validate(
        pl.DataFrame({"dom_raw": [1], "intl_raw": [2]})
    )

    assert _Service.call_log == ["A", "B", "C", "A"]
    row = out.row(0, named=True)
    assert row["dom_tier"] == 112
    assert row["intl_code"] == 113
    assert row["fee_note"] == 12
