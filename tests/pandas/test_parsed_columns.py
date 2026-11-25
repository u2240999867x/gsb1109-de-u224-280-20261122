"""Tests for declarative derived columns: ParsedColumn, ParsedField and the
ColumnParser protocol."""

import pandas as pd
import pytest

import pandera.pandas as pa
from pandera.api.parsers import ColumnParser, ParseContext, Parser
from pandera.errors import (
    ParserSourceError,
    SchemaError,
    SchemaErrors,
    SchemaInitError,
)

# --------------------------------------------------------------------------
# object API
# --------------------------------------------------------------------------


def test_parsed_column_derives_from_source():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str),
            "n_words": pa.ParsedColumn(
                int,
                source="body",
                parser=lambda s: s.str.split().str.len(),
            ),
        }
    )
    out = schema.validate(pd.DataFrame({"body": ["a b c", "d e"]}))
    assert out["n_words"].tolist() == [3, 2]


def test_parsed_column_accepts_everything_column_accepts():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str),
            "n_words": pa.ParsedColumn(
                int,
                source="body",
                parser=lambda s: s.str.split().str.len().astype(str),
                checks=pa.Check.ge(1),
                coerce=True,
                description="token count",
            ),
        }
    )
    out = schema.validate(pd.DataFrame({"body": ["a b c"]}))
    assert out["n_words"].tolist() == [3]
    assert schema.columns["n_words"].description == "token count"


def test_parsed_column_checks_are_enforced():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str),
            "n": pa.ParsedColumn(
                int,
                source="body",
                parser=lambda s: s.str.len(),
                checks=pa.Check.ge(10),
            ),
        }
    )
    with pytest.raises((SchemaError, SchemaErrors)):
        schema.validate(pd.DataFrame({"body": ["abc"]}))


def test_parsed_column_requires_a_parser():
    with pytest.raises(SchemaInitError, match="requires a `parser`"):
        pa.ParsedColumn(int, source="body", parser=None)


def test_source_must_be_declared_in_the_schema():
    schema = pa.DataFrameSchema(
        {"x": pa.ParsedColumn(int, source="nope", parser=lambda s: s)}
    )
    with pytest.raises(SchemaInitError, match="not declared in the schema"):
        schema.validate(pd.DataFrame({"x": [1]}))


def test_multi_source_parsed_column():
    schema = pa.DataFrameSchema(
        {
            "first": pa.Column(str),
            "last": pa.Column(str),
            "full": pa.ParsedColumn(
                str,
                source=["first", "last"],
                parser=lambda df: df["first"] + " " + df["last"],
            ),
        }
    )
    out = schema.validate(
        pd.DataFrame({"first": ["ada"], "last": ["lovelace"]})
    )
    assert out["full"].tolist() == ["ada lovelace"]


def test_parsed_columns_chain():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str),
            # declared before the column it depends on
            "doubled": pa.ParsedColumn(
                int, source="n", parser=lambda s: s * 2
            ),
            "n": pa.ParsedColumn(
                int, source="body", parser=lambda s: s.str.len()
            ),
        }
    )
    out = schema.validate(pd.DataFrame({"body": ["abc"]}))
    assert out["n"].tolist() == [3]
    assert out["doubled"].tolist() == [6]


# --------------------------------------------------------------------------
# schema-wide default source
# --------------------------------------------------------------------------


def test_parser_source_default():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str),
            "n": pa.ParsedColumn(int, parser=lambda s: s.str.len()),
        },
        parser_source="body",
    )
    assert schema.validate(pd.DataFrame({"body": ["abcd"]}))["n"].tolist() == [
        4
    ]


def test_field_source_overrides_schema_default():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str),
            "other": pa.Column(str),
            "n": pa.ParsedColumn(
                int, source="other", parser=lambda s: s.str.len()
            ),
        },
        parser_source="body",
    )
    out = schema.validate(pd.DataFrame({"body": ["abcd"], "other": ["xy"]}))
    assert out["n"].tolist() == [2]


# --------------------------------------------------------------------------
# model API
# --------------------------------------------------------------------------


def test_parsed_field_with_config_source():
    class Tickets(pa.DataFrameModel):
        body: str
        n_words: int = pa.ParsedField(
            parser=lambda s: s.str.split().str.len(), ge=1
        )

        class Config:
            parser_source = "body"

    out = Tickets.validate(pd.DataFrame({"body": ["a b c", "d e"]}))
    assert out["n_words"].tolist() == [3, 2]


def test_parsed_field_with_explicit_source():
    class Tickets(pa.DataFrameModel):
        body: str
        n: int = pa.ParsedField(source="body", parser=lambda s: s.str.len())

    assert Tickets.validate(pd.DataFrame({"body": ["abcd"]}))[
        "n"
    ].tolist() == [4]


def test_parsed_field_keeps_field_arguments():
    class Model(pa.DataFrameModel):
        body: str
        n: int = pa.ParsedField(
            source="body",
            parser=lambda s: s.str.len(),
            ge=100,
            description="length",
        )

    schema = Model.to_schema()
    assert schema.columns["n"].description == "length"
    with pytest.raises((SchemaError, SchemaErrors)):
        Model.validate(pd.DataFrame({"body": ["abc"]}))


def test_parsed_field_builds_a_parsed_column():
    class Model(pa.DataFrameModel):
        body: str
        n: int = pa.ParsedField(source="body", parser=lambda s: s.str.len())

    assert isinstance(Model.to_schema().columns["n"], pa.ParsedColumn)


# --------------------------------------------------------------------------
# @parser(..., source=...)
# --------------------------------------------------------------------------


def test_parser_decorator_with_source_derives_a_column():
    class Tickets(pa.DataFrameModel):
        body: str
        n_words: int

        @pa.parser("n_words", source="body")
        def count_words(cls, s):
            """Number of whitespace-separated tokens."""
            return s.str.split().str.len()

    out = Tickets.validate(pd.DataFrame({"body": ["a b c", "d e"]}))
    assert out["n_words"].tolist() == [3, 2]


def test_parser_decorator_without_source_is_unchanged():
    class Model(pa.DataFrameModel):
        a: int

        @pa.parser("a")
        def clip(cls, s):
            return s.clip(lower=0)

    assert Model.validate(pd.DataFrame({"a": [-1, 2]}))["a"].tolist() == [0, 2]


def test_parser_decorator_rejects_unknown_target():
    with pytest.raises(SchemaInitError, match="non-existing field"):

        class Model(pa.DataFrameModel):
            a: int

            @pa.parser("nope", source="a")
            def derive(cls, s):
                return s

        Model.to_schema()


# --------------------------------------------------------------------------
# ColumnParser protocol
# --------------------------------------------------------------------------


class _Doubler:
    """A minimal ColumnParser that reads the column's declared dtype."""

    def __init__(self, batch_with=None):
        self.batch_with = batch_with
        self.bound_contexts = []

    def bind(self, ctx: ParseContext):
        self.bound_contexts.append(ctx)

        def _fn(series):
            return series * 2

        return _fn

    def batch_key(self, ctx: ParseContext):
        return self.batch_with


class _Batched:
    """A ColumnParser that fills all its columns in one pass.

    Call counting lives on the class because pandera deep-copies columns (and
    therefore their parsers) into the schema, so instance state set here is
    not the state the schema sees.
    """

    calls = 0

    def __init__(self, key="shared"):
        self.key = key

    def bind(self, ctx: ParseContext):  # pragma: no cover - batched instead
        raise AssertionError("should have been batched")

    def batch_key(self, ctx: ParseContext):
        return self.key

    @classmethod
    def batch(cls, items):
        targets = [ctx.target for _, ctx in items]

        def _fn(df):
            _Batched.calls += 1
            source = df.columns[0]
            return df.assign(
                **{
                    target: df[source] * (i + 1)
                    for i, target in enumerate(targets)
                }
            )

        return _fn


def test_column_parser_protocol_is_satisfied():
    assert isinstance(_Doubler(), ColumnParser)
    assert isinstance(_Batched(), ColumnParser)
    assert not isinstance(lambda s: s, ColumnParser)


def test_column_parser_bind_receives_the_columns_type():
    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "b": pa.ParsedColumn(
                float,
                source="a",
                parser=_Doubler(),
                description="doubled",
                nullable=True,
                coerce=True,
            ),
        }
    )
    schema.validate(pd.DataFrame({"a": [1, 2]}))

    # pandera deep-copies columns into the schema, so inspect the schema's
    # copy rather than the object handed to ParsedColumn.
    (ctx,) = schema.columns["b"].parser.bound_contexts
    assert ctx.target == "b"
    assert str(ctx.dtype) == "float64"
    assert ctx.description == "doubled"
    assert ctx.nullable is True
    assert ctx.source == ("a",)
    assert ctx.schema is schema


def test_bind_errors_surface_at_schema_build_time():
    class _Refuses:
        def bind(self, ctx):
            raise SchemaInitError(f"cannot fill column '{ctx.target}'")

        def batch_key(self, ctx):
            return None

    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "b": pa.ParsedColumn(int, source="a", parser=_Refuses()),
        }
    )
    with pytest.raises(SchemaInitError, match="cannot fill column 'b'"):
        schema.validate(pd.DataFrame({"a": [1]}))


def test_parsers_sharing_a_batch_key_are_merged():
    _Batched.calls = 0
    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "x": pa.ParsedColumn(int, source="a", parser=_Batched()),
            "y": pa.ParsedColumn(int, source="a", parser=_Batched()),
            "z": pa.ParsedColumn(int, source="a", parser=_Batched()),
        }
    )
    out = schema.validate(pd.DataFrame({"a": [1]}))
    # three columns filled by a single invocation
    assert _Batched.calls == 1
    assert out["x"].tolist() == [1]
    assert out["y"].tolist() == [2]
    assert out["z"].tolist() == [3]


def test_distinct_batch_keys_are_not_merged():
    _Batched.calls = 0
    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "x": pa.ParsedColumn(int, source="a", parser=_Batched("one")),
            "y": pa.ParsedColumn(int, source="a", parser=_Batched("two")),
        }
    )
    schema.validate(pd.DataFrame({"a": [1]}))
    assert _Batched.calls == 2


def test_none_batch_key_opts_out_of_batching():
    schema = pa.DataFrameSchema(
        {
            "src": pa.Column(int),
            "x": pa.ParsedColumn(int, source="src", parser=_Doubler()),
            "y": pa.ParsedColumn(int, source="src", parser=_Doubler()),
        }
    )
    schema.validate(pd.DataFrame({"src": [1]}))
    # each was bound separately rather than merged
    assert len(schema.columns["x"].parser.bound_contexts) == 1
    assert len(schema.columns["y"].parser.bound_contexts) == 1


def test_batch_key_without_batch_method_is_an_error():
    class _NoBatch:
        def bind(self, ctx):
            return lambda s: s

        def batch_key(self, ctx):
            return "shared"

    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "x": pa.ParsedColumn(int, source="a", parser=_NoBatch()),
            "y": pa.ParsedColumn(int, source="a", parser=_NoBatch()),
        }
    )
    with pytest.raises(SchemaInitError, match="does not implement a `batch`"):
        schema.validate(pd.DataFrame({"a": [1]}))


class _Service:
    """A paid parsing service.

    One ``batch`` invocation is one billed call: it adds a service-specific
    constant to each source column and can fill any number of targets whose
    sources are present when the call starts. The log is class-level because
    pandera deep-copies columns (and parsers) into the schema.
    """

    add = {"A": 1, "B": 10, "C": 100}
    call_log: list = []

    def __init__(self, service):
        self.service = service

    def bind(self, ctx: ParseContext):  # pragma: no cover - batched instead
        raise AssertionError("should have been batched")

    def batch_key(self, ctx: ParseContext):
        return self.service

    @classmethod
    def batch(cls, items):
        service = {parser.service for parser, _ in items}.pop()
        plan = [(ctx.target, ctx.source[0]) for _, ctx in items]

        def _fn(df):
            cls.call_log.append(
                (service, tuple(target for target, _ in plan))
            )
            out = df
            for target, source in plan:
                out = out.assign(**{target: df[source] + cls.add[service]})
            return out

        return _fn


class _Total:
    """A table-level derivation that opts out of batching."""

    def bind(self, ctx: ParseContext):
        def _fn(df):
            return df.assign(
                total=df["dom_tier"] + df["intl_code"]
            )

        return _fn

    def batch_key(self, ctx: ParseContext):
        return None


def _settlement_columns():
    return [
        ("dom_raw", pa.Column(int)),
        ("intl_raw", pa.Column(int)),
        (
            "dom_code",
            pa.ParsedColumn(
                int, source="dom_raw", parser=_Service("A")
            ),
        ),
        (
            "dom_fee",
            pa.ParsedColumn(
                int, source="dom_code", parser=_Service("B")
            ),
        ),
        (
            "dom_tier",
            pa.ParsedColumn(
                int, source="dom_fee", parser=_Service("C")
            ),
        ),
        (
            "intl_fee",
            pa.ParsedColumn(
                int, source="intl_raw", parser=_Service("B")
            ),
        ),
        (
            "intl_tier",
            pa.ParsedColumn(
                int, source="intl_fee", parser=_Service("C")
            ),
        ),
        (
            "intl_code",
            pa.ParsedColumn(
                int, source="intl_tier", parser=_Service("A")
            ),
        ),
        (
            "fee_note",
            pa.ParsedColumn(
                int, source="intl_raw", parser=_Service("B")
            ),
        ),
        (
            "total",
            pa.ParsedColumn(
                int,
                source=["dom_tier", "intl_code"],
                parser=_Total(),
            ),
        ),
    ]


def test_dependent_batched_columns_use_the_fewest_service_calls():
    _Service.call_log = []
    schema = pa.DataFrameSchema(dict(_settlement_columns()))
    out = schema.validate(
        pd.DataFrame({"dom_raw": [1], "intl_raw": [2]})
    )

    # two chains weave through the same three services, but each service is
    # only revisited when a dependency forces it: A, B, C, then A again.
    assert [call[0] for call in _Service.call_log] == [
        "A",
        "B",
        "C",
        "A",
    ]

    # columns ready at different times still share the one B call: the
    # intl-only fields are ready immediately, dom_fee only after the first A.
    b_calls = [
        targets
        for service, targets in _Service.call_log
        if service == "B"
    ]
    assert len(b_calls) == 1
    assert set(b_calls[0]) == {"dom_fee", "intl_fee", "fee_note"}

    row = out.iloc[0]
    assert row["dom_tier"] == 112
    assert row["intl_code"] == 113
    assert row["fee_note"] == 12
    assert row["total"] == 225


def test_batching_plan_is_independent_of_declaration_order():
    expected = ["A", "B", "C", "A"]
    for column_order in (
        _settlement_columns(),
        list(reversed(_settlement_columns())),
    ):
        _Service.call_log = []
        schema = pa.DataFrameSchema(dict(column_order))
        out = schema.validate(
            pd.DataFrame({"dom_raw": [1], "intl_raw": [2]})
        )
        assert [call[0] for call in _Service.call_log] == expected
        assert out.iloc[0]["total"] == 225


def test_a_cycle_between_batched_parsers_is_still_an_error():
    schema = pa.DataFrameSchema(
        {
            "x": pa.ParsedColumn(
                int, source="y", parser=_Service("A")
            ),
            "y": pa.ParsedColumn(
                int, source="x", parser=_Service("B")
            ),
        }
    )
    with pytest.raises(SchemaInitError, match="cycle"):
        schema.validate(pd.DataFrame({"x": [1], "y": [2]}))


class _SoloStep:
    """A None batch_key parser whose output feeds a later batched parser."""

    def bind(self, ctx: ParseContext):
        return lambda series: series + 1

    def batch_key(self, ctx: ParseContext):
        return None


def test_none_batch_key_output_can_feed_a_later_batch():
    _Service.call_log = []
    schema = pa.DataFrameSchema(
        {
            "raw": pa.Column(int),
            "seeded": pa.ParsedColumn(
                int, source="raw", parser=_SoloStep()
            ),
            "derived": pa.ParsedColumn(
                int, source="seeded", parser=_Service("A")
            ),
        }
    )
    out = schema.validate(pd.DataFrame({"raw": [1]}))
    # the solo parser runs on its own; its column is still a valid source for
    # the single service call that follows.
    assert out["seeded"].tolist() == [2]
    assert out["derived"].tolist() == [3]
    assert [call[0] for call in _Service.call_log] == ["A"]


def test_scheduler_defers_ready_columns_to_reach_the_minimum():
    # c_early is ready immediately, but firing service C at once would force a
    # second C call later for c_late. The minimum plan waits and serves both in
    # one C call, so the whole contract takes four calls rather than five.
    _Service.call_log = []
    schema = pa.DataFrameSchema(
        {
            "r0": pa.Column(int),
            "r1": pa.Column(int),
            "b0": pa.ParsedColumn(
                int, source="r0", parser=_Service("B")
            ),
            "c_early": pa.ParsedColumn(
                int, source="r1", parser=_Service("C")
            ),
            "a_mid": pa.ParsedColumn(
                int, source="b0", parser=_Service("A")
            ),
            "c_late": pa.ParsedColumn(
                int, source="b0", parser=_Service("C")
            ),
            "b_from_c": pa.ParsedColumn(
                int, source="c_early", parser=_Service("B")
            ),
            "b_from_a": pa.ParsedColumn(
                int, source="a_mid", parser=_Service("B")
            ),
        }
    )
    out = schema.validate(pd.DataFrame({"r0": [10], "r1": [20]}))

    assert len(_Service.call_log) == 4
    c_calls = [
        targets
        for service, targets in _Service.call_log
        if service == "C"
    ]
    assert len(c_calls) == 1
    assert set(c_calls[0]) == {"c_early", "c_late"}
    b_calls = [
        targets
        for service, targets in _Service.call_log
        if service == "B"
    ]
    assert set(b_calls[-1]) == {"b_from_c", "b_from_a"}

    row = out.iloc[0]
    assert row["b0"] == 20
    assert row["c_early"] == 120
    assert row["a_mid"] == 21
    assert row["c_late"] == 120
    assert row["b_from_c"] == 130
    assert row["b_from_a"] == 31



def test_parsed_column_missing_source_at_runtime():
    """The source is declared in the schema but absent from the data."""
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str, required=False),
            "n": pa.ParsedColumn(
                int, source="body", parser=lambda s: s.str.len()
            ),
        }
    )
    with pytest.raises(ParserSourceError, match="body"):
        schema.validate(pd.DataFrame({"other": [1]}))


def test_schema_level_parsers_still_run_alongside_parsed_columns():
    schema = pa.DataFrameSchema(
        {
            "body": pa.Column(str),
            "n": pa.ParsedColumn(
                int, source="body", parser=lambda s: s.str.len()
            ),
        },
        parsers=[Parser(lambda df: df.assign(body=df["body"].str.strip()))],
    )
    out = schema.validate(pd.DataFrame({"body": ["  abc  "]}))
    assert out["body"].tolist() == ["abc"]
    assert out["n"].tolist() == [3]
