import dataclasses
import datetime
import enum
import sys
import uuid
from decimal import Decimal

import pytest

import msgspec

try:
    import yaml  # noqa
except ImportError:
    pytestmark = pytest.mark.skip(reason="PyYAML is not installed")

from .utils import py315_or_later_only

if sys.version_info >= (3, 15):
    # This is needed for `ruff` to recognize `frozendict` name
    # and to not raise `F821`:
    from builtins import frozendict

UTC = datetime.timezone.utc


class ExStruct(msgspec.Struct):
    x: int
    y: str


@dataclasses.dataclass
class ExDataclass:
    x: int
    y: str


class ExEnum(enum.Enum):
    one = "one"
    two = "two"


class ExIntEnum(enum.IntEnum):
    one = 1
    two = 2


def test_module_dir():
    assert set(dir(msgspec.yaml)) == {"encode", "decode"}


def test_pyyaml_not_installed_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "yaml", None)

    with pytest.raises(ImportError, match="PyYAML"):
        msgspec.yaml.encode(1)

    with pytest.raises(ImportError, match="PyYAML"):
        msgspec.yaml.decode("1", type=int)


@pytest.mark.parametrize(
    "val",
    [
        None,
        True,
        False,
        1,
        1.5,
        "fizz",
        datetime.datetime(2022, 1, 2, 3, 4, 5, 6),
        datetime.datetime(2022, 1, 2, 3, 4, 5, 6, UTC),
        datetime.date(2022, 1, 2),
        [1, 2],
        {"one": 2},
        {1: "two"},
    ],
)
def test_roundtrip_any(val):
    msg = msgspec.yaml.encode(val)
    res = msgspec.yaml.decode(msg)
    assert res == val


@pytest.mark.parametrize(
    "val, type",
    [
        (None, None),
        (True, bool),
        (False, bool),
        (1, int),
        (1.5, float),
        ("fizz", str),
        (b"fizz", bytes),
        (b"fizz", bytearray),
        (datetime.datetime(2022, 1, 2, 3, 4, 5, 6), datetime.datetime),
        (datetime.datetime(2022, 1, 2, 3, 4, 5, 6, UTC), datetime.datetime),
        (datetime.date(2022, 1, 2), datetime.date),
        (datetime.time(12, 34), datetime.time),
        (uuid.uuid4(), uuid.UUID),
        (ExEnum.one, ExEnum),
        (ExIntEnum.one, ExIntEnum),
        ([1, 2], list[int]),
        ((1, 2), tuple[int, ...]),
        ({1, 2}, set[int]),
        (frozenset({1, 2}), frozenset[int]),
        (("one", 2), tuple[str, int]),
        ({"one": 2}, dict[str, int]),
        ({1: "two"}, dict[int, str]),
        (ExStruct(1, "two"), ExStruct),
        (ExDataclass(1, "two"), ExDataclass),
    ],
)
def test_roundtrip_typed(val, type):
    msg = msgspec.yaml.encode(val)
    res = msgspec.yaml.decode(msg, type=type)
    assert res == val


@py315_or_later_only
def test_roundtrip_frozendict():
    val = frozendict({"x": 1})
    msg = msgspec.yaml.encode(val)
    res = msgspec.yaml.decode(msg, type=frozendict[str, int])
    assert res == val
    assert type(res) is frozendict


def test_encode_error():
    class Oops:
        pass

    with pytest.raises(TypeError, match="Encoding objects of type Oops is unsupported"):
        msgspec.yaml.encode(Oops())


def test_encode_enc_hook():
    msg = msgspec.yaml.encode(Decimal(1.5), enc_hook=str)
    assert msgspec.yaml.decode(msg) == "1.5"


@pytest.mark.parametrize("order", [None, "deterministic"])
def test_encode_order(order):
    msg = {"y": 1, "x": 2, "z": 3}
    res = msgspec.yaml.encode(msg, order=order)
    sol = yaml.safe_dump(msg, sort_keys=bool(order)).encode("utf-8")
    assert res == sol


def test_decode_str_or_bytes_like():
    assert msgspec.yaml.decode("[1, 2]") == [1, 2]
    assert msgspec.yaml.decode(b"[1, 2]") == [1, 2]
    assert msgspec.yaml.decode(bytearray(b"[1, 2]")) == [1, 2]
    assert msgspec.yaml.decode(memoryview(b"[1, 2]")) == [1, 2]
    with pytest.raises(TypeError):
        msgspec.yaml.decode(1)


@pytest.mark.parametrize(
    "msg",
    [
        b"{{",
        b"!!binary 123",
        b"date: 2025-02-29",
        b"timestamp: 2024-01-01T24:00:00Z",
        b"number: !!int invalid",
        b"flag: !!bool maybe",
        b"number: !!int ''",
        b"timestamp: !!timestamp abc",
    ],
)
def test_decode_parse_error(msg):
    with pytest.raises(msgspec.DecodeError):
        msgspec.yaml.decode(msg)


def test_decode_validation_error():
    with pytest.raises(msgspec.ValidationError, match="Expected `str`"):
        msgspec.yaml.decode(b"[1, 2, 3]", type=list[str])


@pytest.mark.parametrize("strict", [True, False])
def test_decode_strict_or_lax(strict):
    msg = b"a: ['1', '2']"
    typ = dict[str, list[int]]

    if strict:
        with pytest.raises(msgspec.ValidationError, match="Expected `int`"):
            msgspec.yaml.decode(msg, type=typ, strict=strict)
    else:
        res = msgspec.yaml.decode(msg, type=typ, strict=strict)
        assert res == {"a": [1, 2]}


def test_decode_dec_hook():
    def dec_hook(typ, val):
        if typ is Decimal:
            return Decimal(val)
        raise TypeError

    res = msgspec.yaml.decode("'1.5'", type=Decimal, dec_hook=dec_hook)
    assert res == Decimal("1.5")


class ColumnConfig(msgspec.Struct):
    rate: float
    decimals: int
    currency: str = "USD"
    name: str = ""


class SupplierDoc(msgspec.Struct):
    suppliers: dict[str, ColumnConfig]


YAML_TEMPLATES = """\
defaults: &defaults
  rate: 1.5
  decimals: 2
fx: &fx
  rate: 0.92
base: &base
  <<: *defaults
  currency: EUR
suppliers:
  alpha:
    <<: *base
    name: alpha
  beta:
    <<: [*base, *fx]
    decimals: 4
    name: beta
"""


def _bad_rate(doc=YAML_TEMPLATES):
    return doc.replace("rate: 1.5", "rate: bad")


def test_decode_yaml_template_merge():
    res = msgspec.yaml.decode(YAML_TEMPLATES, type=SupplierDoc)
    assert res == SupplierDoc(
        suppliers={
            "alpha": ColumnConfig(1.5, 2, "EUR", "alpha"),
            "beta": ColumnConfig(0.92, 4, "EUR", "beta"),
        }
    )


def test_decode_yaml_template_error_points_at_value_line_and_inheritance():
    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode(_bad_rate(), type=SupplierDoc)

    exc = rec.value
    assert exc.table == "alpha"
    assert exc.path in ("$.suppliers.alpha.rate",)
    assert exc.line == 2
    assert [site.anchor for site in exc.sites] == ["base", "defaults"]
    assert all(site.relation == "merge" for site in exc.sites)
    # The document line of the value, not the record that inherited it
    assert "value at line 2" in str(exc)
    assert "&base" in str(exc) and "&defaults" in str(exc)


def test_decode_yaml_template_override_lets_one_table_decode():
    # `beta` inherits from `&fx`, which overrides the bad rate; `alpha` doesn't.
    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode(_bad_rate(), type=SupplierDoc)
    assert rec.value.table == "alpha"

    # If only the overriding table remains, decoding succeeds.
    doc2 = """\
defaults: &defaults
  rate: bad
  decimals: 2
fx: &fx
  rate: 0.92
base: &base
  <<: *defaults
suppliers:
  beta:
    <<: [*base, *fx]
    decimals: 4
    name: beta
"""
    res = msgspec.yaml.decode(doc2, type=SupplierDoc)
    assert res.suppliers["beta"].rate == 0.92


def test_decode_yaml_template_error_independent_of_declaration_order():
    first = """\
defaults: &defaults
  rate: bad
  decimals: 2
suppliers:
  alpha:
    <<: *defaults
    name: alpha
  beta:
    <<: *defaults
    name: beta
"""
    second = """\
defaults: &defaults
  rate: bad
  decimals: 2
suppliers:
  beta:
    <<: *defaults
    name: beta
  alpha:
    <<: *defaults
    name: alpha
"""
    with pytest.raises(msgspec.ValidationError) as rec1:
        msgspec.yaml.decode(first, type=SupplierDoc)
    with pytest.raises(msgspec.ValidationError) as rec2:
        msgspec.yaml.decode(second, type=SupplierDoc)
    assert rec1.value.table == rec2.value.table == "alpha"
    assert rec1.value.line == rec2.value.line == 2
    # The inheritance site naturally moves with the declaration.
    assert rec1.value.sites[0].line == rec2.value.sites[0].line - 3


def test_decode_yaml_template_recursive_alias_reports_use_site():
    doc = """\
loop: &loop
  <<: *loop
  rate: bad
  decimals: 2
suppliers:
  only:
    <<: *loop
    name: only
"""
    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode(doc, type=SupplierDoc)

    exc = rec.value
    assert exc.table == "only"
    assert exc.line == 3
    recursive = [site for site in exc.sites if site.relation == "recursive"]
    assert len(recursive) == 1
    assert recursive[0].anchor == "loop"
    assert recursive[0].line == 2
    assert "recursive reference to &loop at line 2" in str(exc)


def test_decode_yaml_template_recursive_alias_does_not_hide_other_tables():
    doc = """\
loop: &loop
  <<: *loop
  rate: 1.5
  decimals: 2
suppliers:
  loops:
    <<: *loop
    name: loops
  plain:
    rate: bad
    decimals: 6
    name: plain
"""
    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode(doc, type=SupplierDoc)

    exc = rec.value
    assert exc.table == "plain"
    assert exc.path == "$.suppliers.plain.rate"
    assert exc.line == 10
    assert exc.sites == ()


def test_decode_yaml_template_multihop_chain():
    doc = """\
leaf: &leaf
  rate: bad
  decimals: 1
  currency: USD
mid: &mid
  <<: *leaf
suppliers:
  alpha:
    <<: *mid
    name: alpha
"""
    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode(doc, type=SupplierDoc)

    exc = rec.value
    assert exc.line == 2
    assert [site.anchor for site in exc.sites] == ["mid", "leaf"]
    # Each site points at the `<<` that performed the inheritance.
    assert [site.line for site in exc.sites] == [9, 6]
    assert [site.target_line for site in exc.sites] == [5, 1]


def test_decode_yaml_template_alias_site_and_structured_details():
    doc = """\
template: &t
  rate: bad
  decimals: 2
  currency: USD
suppliers:
  alpha: *t
"""
    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode(doc, type=SupplierDoc)

    exc = rec.value
    assert exc.line == 2
    assert exc.sites[0].relation == "alias"
    assert exc.sites[0].anchor == "t"
    assert exc.sites[0].line == 6
    details = exc.origin.as_dict()
    assert details["path"] == "$.suppliers.alpha.rate"
    assert details["table"] == "alpha"
    assert details["line"] == 2
    assert details["sites"][0]["relation"] == "alias"


def test_decode_yaml_template_explicit_key_wins():
    doc = """\
defaults: &defaults
  rate: bad
  decimals: 2
  currency: USD
suppliers:
  alpha:
    <<: *defaults
    rate: 1.25
    name: alpha
"""
    res = msgspec.yaml.decode(doc, type=SupplierDoc)
    assert res.suppliers["alpha"].rate == 1.25

    doc2 = doc.replace("    rate: 1.25\n", "    rate: bad\n    decimals: 4\n")
    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode(doc2, type=SupplierDoc)
    exc = rec.value
    assert exc.line == 8
    assert exc.sites == ()


def test_decode_yaml_template_unenriched_errors_keep_their_message():
    # Messages whose path is already complete and unanchored are untouched.
    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode("[1, 'x']", type=list[int])
    assert str(rec.value) == "Expected `int`, got `str` - at `$[1]`"

    # Mapping values render as `[...]` in the underlying message; those get the
    # resolved logical path plus the value's position.
    doc = "suppliers:\n  alpha:\n    rate: bad\n    decimals: 2\n"
    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode(doc, type=SupplierDoc)
    assert str(rec.value) == (
        "Expected `float`, got `str` - at `$.suppliers.alpha.rate`; "
        "table `alpha`, value at line 3"
    )
    assert rec.value.path == "$.suppliers.alpha.rate"
    assert rec.value.line == 3
    assert rec.value.sites == ()


def test_decode_yaml_template_quoted_numeric_key():
    doc = """\
suppliers:
  "10":
    rate: bad
    decimals: 2
  "2":
    rate: 1.5
    decimals: 2
"""
    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode(doc, type=SupplierDoc)
    assert rec.value.table == "10"
    assert rec.value.line == 3


def test_decode_yaml_template_large_document():
    lines = [
        "defaults: &defaults",
        "  rate: bad",
        "  decimals: 2",
        "suppliers:",
    ]
    for i in range(2000):
        lines += [f"  s{i:05d}:", "    <<: *defaults"]
    doc = "\n".join(lines) + "\n"

    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode(doc, type=SupplierDoc)
    exc = rec.value
    assert exc.table == "s00000"
    assert exc.line == 2
    assert exc.sites[0].anchor == "defaults"


def test_decode_yaml_template_scalar_anchor_and_non_string_keys():
    doc = """\
defaults: &defaults
  rate: bad
  decimals: 2
suppliers:
  1:
    <<: *defaults
"""

    class IntKeyed(msgspec.Struct):
        suppliers: dict[int, ColumnConfig]

    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode(doc, type=IntKeyed)
    exc = rec.value
    assert exc.path == "$.suppliers[1].rate"
    assert exc.table == "suppliers"
    assert exc.line == 2
    assert exc.anchor is None
    assert [site.anchor for site in exc.sites] == ["defaults"]


def test_decode_yaml_template_root_errors_are_unchanged():
    with pytest.raises(msgspec.ValidationError) as rec:
        msgspec.yaml.decode("bad: 1\n", type=int)
    assert str(rec.value) == "Expected `int`, got `object`"
    assert not hasattr(rec.value, "path")
