from __future__ import annotations

import pytest

import msgspec
from msgspec import Struct


class _Counter:
    def __init__(self):
        self.count = 0

    def __call__(self):
        self.count += 1
        return self.count


@pytest.fixture(params=["json", "msgpack"])
def proto(request):
    return getattr(msgspec, request.param)


@pytest.fixture
def counter():
    return _Counter()


def ticket_type(counter):
    class Ticket(Struct):
        sku: str
        qty: int
        note: str | None = None
        ticket: int = msgspec.field(default_factory=counter)

    return Ticket


def encode_pair(proto):
    first = {"sku": "A-001", "qty": 1, "note": None}
    second = {"sku": "B-002", "qty": "bad"}
    return proto.encode(first), proto.encode(second)


def test_precheck_does_not_evaluate_factories(proto, counter):
    Ticket = ticket_type(counter)
    first, second = encode_pair(proto)

    good = proto.precheck(first, type=Ticket)
    assert good.value.sku == "A-001"
    assert good.value.qty == 1
    assert good.value.note is None
    assert counter.count == 0
    assert good.value.ticket is msgspec.DEFERRED
    assert good.deferred == ["$.ticket"]

    with pytest.raises(msgspec.ValidationError):
        proto.precheck(second, type=Ticket)
    assert counter.count == 0


def test_apply_defaults_uses_original_message(proto, counter):
    Ticket = ticket_type(counter)
    first, _ = encode_pair(proto)
    result = proto.precheck(first, type=Ticket)
    assert counter.count == 0

    # A reviewer edits the prechecked value; the submitted line still governs
    msgspec.structs.force_setattr(result.value, "qty", 99)
    out = result.apply_defaults()
    assert out == Ticket(sku="A-001", qty=1, note=None, ticket=1)
    assert counter.count == 1

    # Each application is based on the original submitted message
    out2 = result.apply_defaults()
    assert out2.qty == 1
    assert out2.ticket == 2
    assert counter.count == 2


def test_decoder_precheck(proto, counter):
    Ticket = ticket_type(counter)
    first, _ = encode_pair(proto)
    dec = proto.Decoder(Ticket)
    result = dec.precheck(first)
    assert result.deferred == ["$.ticket"]
    assert result.message == first
    assert counter.count == 0
    assert result.apply_defaults().ticket == 1


def test_normal_decode_still_calls_factory(proto, counter):
    Ticket = ticket_type(counter)
    first, _ = encode_pair(proto)
    out = proto.decode(first, type=Ticket)
    assert out.ticket == 1
    assert counter.count == 1


def test_precheck_all_deferred_fields(proto):
    class Example(Struct):
        a: int
        b: str = "default"
        c: list = msgspec.field(default_factory=list)

    msg = proto.encode({"a": 1})
    result = proto.precheck(msg, type=Example)
    assert result.deferred == ["$.b", "$.c"]
    assert result.value.b is msgspec.DEFERRED
    assert result.value.c is msgspec.DEFERRED

    out = result.apply_defaults()
    assert out == Example(1)


def test_precheck_required_field_still_errors(proto):
    class Example(Struct):
        a: int
        b: int

    msg = proto.encode({"a": 1})
    with pytest.raises(msgspec.ValidationError, match="missing required field `b`"):
        proto.precheck(msg, type=Example)


def test_precheck_nested_deferred_paths(proto):
    msg = proto.encode({"name": "a", "inner": {"x": 1}})
    result = proto.precheck(msg, type=Outer)
    assert result.deferred == ["$.inner.y"]
    assert result.value.inner.y is msgspec.DEFERRED

    out = result.apply_defaults()
    assert out == Outer("a", Inner(1, 0))


def test_precheck_array_like(proto):
    msg = proto.encode([1])
    result = proto.precheck(msg, type=Point)
    assert result.deferred == ["$.y"]
    out = result.apply_defaults()
    assert out == Point(1, 0)


def test_precheck_stray_validation(proto):
    class Example(Struct):
        a: int

    msg = proto.encode({"a": "not an int"})
    with pytest.raises(msgspec.ValidationError, match="Expected `int`"):
        proto.precheck(msg, type=Example)


def test_precheck_type_and_message_attributes(proto):
    class Example(Struct):
        a: int
        b: int = 0

    msg = proto.encode({"a": 1})
    result = proto.precheck(msg, type=Example)
    assert result.type is Example
    assert result.message == msg
    assert result.deferred == ["$.b"]
    assert repr(result).startswith("PrecheckResult(")
    assert isinstance(result, msgspec.PrecheckResult)


def test_deferred_singleton():
    assert msgspec.DEFERRED is msgspec.DEFERRED
    assert isinstance(msgspec.DEFERRED, msgspec.DeferredType)
    assert repr(msgspec.DEFERRED) == "DEFERRED"
    with pytest.raises(TypeError):
        msgspec.DeferredType(1)


def test_precheck_bad_input_errors(proto):
    with pytest.raises(msgspec.DecodeError):
        proto.precheck(b"" if proto is msgspec.msgpack else b"", type=int)


def test_precheck_generic_alias(proto):
    msg = proto.encode([{"a": 1}])
    result = proto.precheck(msg, type=list[Example])
    assert result.deferred == ["$[0].b"]


class Inner(Struct):
    x: int
    y: int = 0


class Outer(Struct):
    name: str
    inner: Inner


class Point(Struct, array_like=True):
    x: int
    y: int = 0


class Example(Struct):
    a: int
    b: int = 0


def test_precheck_lines_isolates_failures():
    calls = []

    def factory():
        calls.append(1)
        return len(calls)

    class Ticket(Struct):
        sku: str
        qty: int
        ticket: int = msgspec.field(default_factory=factory)

    buf = (
        b'{"sku": "A-001", "qty": 1}\n'
        b'{"sku": "B-002", "qty": "bad"}\n'
        b'{"sku": "C-003", "qty": 3}\n'
    )
    results = msgspec.json.precheck_lines(buf, type=Ticket)
    assert len(results) == 3

    assert results[0].ok
    assert results[0].deferred == ["$.ticket"]
    assert results[1].ok is False
    assert results[1].value is None
    assert isinstance(results[1].error, msgspec.ValidationError)
    assert "qty" in str(results[1].error)
    assert results[2].ok

    # Prechecking never consumed a ticket number
    assert calls == []

    # Applying defaults only for the accepted rows
    accepted = [r for r in results if r.ok]
    filled = [r.apply_defaults() for r in accepted]
    assert [x.ticket for x in filled] == [1, 2]
    assert calls == [1, 1]

    with pytest.raises(msgspec.ValidationError):
        results[1].apply_defaults()


def test_precheck_lines_empty_and_blank():
    assert msgspec.json.precheck_lines(b"", type=int) == []
    assert len(msgspec.json.precheck_lines(b"\n \n", type=int)) == 0


def test_precheck_dataclass(proto):
    from dataclasses import dataclass, field

    @dataclass
    class Item:
        a: int
        b: int = field(default_factory=lambda: 0)
        c: list = field(default_factory=list)

    msg = proto.encode({"a": 1})
    result = proto.precheck(msg, type=Item)
    assert result.deferred == ["$.b", "$.c"]
    assert result.value.b is msgspec.DEFERRED
    assert result.value.c is msgspec.DEFERRED
    out = result.apply_defaults()
    assert out == Item(1)
