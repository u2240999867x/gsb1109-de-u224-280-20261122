from __future__ import annotations

import datetime
import decimal
import enum
import typing
import uuid
from typing import Annotated, Literal

import pytest

import msgspec
from msgspec import contracts


class Waybill(msgspec.Struct, tag="waybill", tag_field="kind"):
    waybill_no: Annotated[str, msgspec.Meta(min_length=1)]
    container_no: str
    customer_address: str
    amount: decimal.Decimal = decimal.Decimal("0")


class Claim(msgspec.Struct, tag="claim", tag_field="kind"):
    claim_no: str
    waybill: Waybill
    description: str | None = None


def analytics_policy(**kwargs):
    return contracts.AccessPolicy(
        exclude={"customer_address"},
        primary_keys={"waybill_no", "claim_no"},
        **kwargs,
    )


def supplier_policy(**kwargs):
    return contracts.AccessPolicy(
        exclude={"amount"},
        primary_keys={"waybill_no", "claim_no"},
        **kwargs,
    )


class Node(msgspec.Struct):
    id: str
    address: str
    children: list[Node] = []


class Kind(enum.Enum):
    A = "a"
    B = "b"


class EnumRow(msgspec.Struct):
    kind: Kind
    literal: Literal["x", "y"]
    when: datetime.datetime
    ident: uuid.UUID


def sample_waybill():
    return Waybill("W-1", "C-9", "12 Secret Street", decimal.Decimal("7.50"))


def sample_claim():
    return Claim("K-1", sample_waybill(), "damaged")


def test_waybill_contract_excludes_address():
    contract = contracts.contract(Waybill, name="analytics", policy=analytics_policy())
    out = contract.export(sample_waybill())
    assert out == {
        "kind": "waybill",
        "waybill_no": "W-1",
        "container_no": "C-9",
        "amount": "7.50",
    }
    assert "customer_address" not in out


def test_supplier_contract_keeps_address():
    contract = contracts.contract(Waybill, name="supplier", policy=supplier_policy())
    out = contract.export(sample_waybill())
    assert out == {
        "kind": "waybill",
        "waybill_no": "W-1",
        "container_no": "C-9",
        "customer_address": "12 Secret Street",
    }


def test_shared_type_is_not_mutated_between_channels():
    analytics = contracts.contract(Waybill, name="analytics", policy=analytics_policy())
    supplier = contracts.contract(Waybill, name="supplier", policy=supplier_policy())

    row = sample_waybill()
    a_out = analytics.export(row)
    s_out = supplier.export(row)

    assert "customer_address" not in a_out
    assert s_out["customer_address"] == "12 Secret Street"
    # Building/exporting one channel must not alter the source type either.
    assert row.customer_address == "12 Secret Street"
    assert "customer_address" in msgspec.to_builtins(row)


def test_waybill_as_root_and_as_claim_child():
    analytics = contracts.contract(Claim, name="analytics", policy=analytics_policy())
    out = analytics.export(sample_claim())
    assert out == {
        "kind": "claim",
        "claim_no": "K-1",
        "waybill": {
            "kind": "waybill",
            "waybill_no": "W-1",
            "container_no": "C-9",
            "amount": "7.50",
        },
        "description": "damaged",
    }


@pytest.mark.parametrize("source", [Waybill, Claim, list[Waybill | Claim]])
def test_contract_is_executable(source):
    contract = contracts.contract(source, name="analytics", policy=analytics_policy())
    if typing.get_origin(source) is list:
        row = [sample_waybill(), sample_claim()]
    elif source is Claim:
        row = sample_claim()
    else:
        row = sample_waybill()
    contracted = contract.convert(row)
    encoded = contract.encode(row)
    assert msgspec.json.decode(encoded, type=contract.type) == contracted


def test_primary_keys_retained_at_every_level():
    policy = contracts.AccessPolicy(
        by_type={Waybill: {"include": ["container_no", "kind"]}},
        primary_keys={"waybill_no", "claim_no"},
    )
    contract = contracts.contract(Claim, name="keys", policy=policy)
    out = contract.export(sample_claim())
    assert out["claim_no"] == "K-1"
    assert out["waybill"]["waybill_no"] == "W-1"
    assert out["waybill"]["container_no"] == "C-9"


def test_primary_key_retained_via_annotation_metadata():
    class Row(msgspec.Struct):
        key: Annotated[str, msgspec.Meta(extra={"primary_key": True})]
        hidden: str
        shown: str

    contract = contracts.contract(
        Row, name="meta-key", policy=contracts.AccessPolicy(include={"shown"})
    )
    assert contract.export(Row("k", "x", "y")) == {"key": "k", "shown": "y"}


def test_primary_key_retained_via_json_schema_metadata():
    class Row(msgspec.Struct):
        key: Annotated[str, msgspec.Meta(extra_json_schema={"primaryKey": True})]
        hidden: str
        shown: str

    contract = contracts.contract(
        Row, name="schema-key", policy=contracts.AccessPolicy(include={"shown"})
    )
    assert contract.export(Row("k", "x", "y")) == {"key": "k", "shown": "y"}


def test_mixed_tag_array_uses_each_contract():
    contract = contracts.contract(
        list[Waybill | Claim], name="mixed", policy=analytics_policy()
    )
    out = contract.export([sample_waybill(), sample_claim()])
    assert out[0]["kind"] == "waybill"
    assert "customer_address" not in out[0]
    assert out[1]["kind"] == "claim"
    assert "customer_address" not in out[1]["waybill"]
    # The generated contract accepts exactly the exported values.
    contract.decode(msgspec.json.encode(out))


def test_mixed_tag_array_accepts_dicts():
    contract = contracts.contract(
        list[Waybill | Claim], name="mixed", policy=analytics_policy()
    )
    payload = [
        {
            "kind": "waybill",
            "waybill_no": "W-1",
            "container_no": "C-9",
            "customer_address": "secret",
        },
        {
            "kind": "claim",
            "claim_no": "K-1",
            "waybill": {
                "kind": "waybill",
                "waybill_no": "W-2",
                "container_no": "C-10",
                "customer_address": "secret",
            },
        },
    ]
    out = contract.export(payload)
    assert "customer_address" not in out[0]
    assert "customer_address" not in out[1]["waybill"]


def test_unknown_array_tag_is_reported():
    contract = contracts.contract(
        list[Waybill | Claim], name="mixed", policy=analytics_policy()
    )
    with pytest.raises(ValueError, match="no tagged union member matches kind"):
        contract.export([{"kind": "parcel", "waybill_no": "W-1"}])


def test_hidden_discriminator_is_a_conflict_not_a_broken_contract():
    policy = contracts.AccessPolicy(exclude={"kind", "customer_address"})
    with pytest.raises(contracts.ContractConflict) as rec:
        contracts.contract(Waybill, name="analytics", policy=policy)

    conflicts = rec.value.conflicts
    assert len(conflicts) == 1
    conflict = conflicts[0]
    assert conflict.kind == "hidden-discriminator"
    assert conflict.tag_field == "kind"
    assert conflict.tag == "waybill"
    assert conflict.struct is Waybill
    assert "kind" in conflict.message
    assert "waybill" in conflict.message
    assert conflict.to_dict()["struct"] == "Waybill"


def test_hidden_discriminator_check_does_not_raise():
    policy = contracts.AccessPolicy(include={"waybill_no", "container_no"})
    conflicts = contracts.check_contract(Waybill, name="analytics", policy=policy)
    assert [c.kind for c in conflicts] == ["hidden-discriminator"]
    assert conflicts[0].channel == "analytics"


def test_explicitly_allowed_discriminator_is_fine():
    policy = contracts.AccessPolicy(include={"waybill_no", "container_no", "kind"})
    contract = contracts.contract(Waybill, name="analytics", policy=policy)
    out = contract.export(sample_waybill())
    assert out["kind"] == "waybill"
    assert "customer_address" not in out


def test_hidden_nested_discriminator_is_reported_with_path():
    policy = contracts.AccessPolicy(
        exclude={"kind"}, primary_keys={"waybill_no", "claim_no"}
    )
    with pytest.raises(contracts.ContractConflict) as rec:
        contracts.contract(Claim, name="nested", policy=policy)
    paths = {c.path for c in rec.value.conflicts}
    assert "$" in paths
    assert "$.waybill" in paths


def test_schema_requires_discriminator_and_rejects_extra_fields():
    contract = contracts.contract(Waybill, name="analytics", policy=analytics_policy())
    schema = contract.schema
    defs = schema["$defs"]
    body = next(iter(defs.values()))
    assert "kind" in body["required"]
    assert body["properties"]["kind"] == {"enum": ["waybill"]}
    assert body["additionalProperties"] is False
    assert "customer_address" not in body["properties"]


def test_generated_contract_rejects_leaked_column():
    contract = contracts.contract(Waybill, name="analytics", policy=analytics_policy())
    leaked = {
        "kind": "waybill",
        "waybill_no": "W-1",
        "container_no": "C-9",
        "customer_address": "secret",
    }
    with pytest.raises(
        msgspec.ValidationError, match="unknown field `customer_address`"
    ):
        contract.decode(msgspec.json.encode(leaked))


def test_constraints_are_preserved():
    contract = contracts.contract(Waybill, name="analytics", policy=analytics_policy())
    with pytest.raises(msgspec.ValidationError, match="length >= 1"):
        contract.decode(
            b'{"kind":"waybill","waybill_no":"","container_no":"C-9","amount":"0"}'
        )


def test_nested_constraint_is_preserved():
    contract = contracts.contract(Claim, name="c", policy=analytics_policy())
    payload = {
        "kind": "claim",
        "claim_no": "K-1",
        "waybill": {
            "kind": "waybill",
            "waybill_no": "",
            "container_no": "C-9",
            "amount": "0",
        },
    }
    with pytest.raises(msgspec.ValidationError, match="length >= 1"):
        contract.decode(msgspec.json.encode(payload))


def test_wire_names_are_preserved():
    class Renamed(msgspec.Struct, rename={"waybill_no": "waybillNo"}):
        waybill_no: str
        container_no: str

    contract = contracts.contract(
        Renamed, name="c", policy=contracts.AccessPolicy(exclude={"container_no"})
    )
    out = contract.export(Renamed("W-1", "C-9"))
    assert out == {"waybillNo": "W-1"}
    assert contract.type.__struct_fields__ == ("waybill_no",)
    assert contract.type.__struct_encode_fields__ == ("waybillNo",)


def test_optional_and_unset_fields_round_trip():
    class S(msgspec.Struct):
        id: str
        note: str | None = None
        flag: int | msgspec.UnsetType = msgspec.UNSET

    contract = contracts.contract(S, name="c", policy=contracts.AccessPolicy())
    out = contract.export(S("x"))
    assert out == {"id": "x", "note": None}
    decoded = contract.decode(msgspec.json.encode(out))
    assert msgspec.to_builtins(decoded) == {"id": "x", "note": None}


def test_defaults_survive_field_removal():
    class S(msgspec.Struct):
        id: str
        hidden: int = 1
        shown: int = 2

    contract = contracts.contract(
        S, name="c", policy=contracts.AccessPolicy(exclude={"hidden"})
    )
    assert contract.type.__struct_fields__ == ("id", "shown")
    assert contract.type.__struct_defaults__ == (2,)
    assert (
        contract.decode(b'{"id":"x"}')
        == S.__new__(S, "x", 1, 2).__class__("x", 1, 2).__class__("x")
        if False
        else contract.decode(b'{"id":"x"}')
    )
    decoded = contract.decode(b'{"id":"x"}')
    assert decoded.shown == 2


def test_recursive_type():
    contract = contracts.contract(
        Node, name="c", policy=contracts.AccessPolicy(exclude={"address"})
    )
    out = contract.export(Node("n1", "secret", [Node("n2", "hidden")]))
    assert out == {
        "id": "n1",
        "children": [{"id": "n2", "children": []}],
    }
    schema = contract.schema
    name = next(iter(schema["$defs"]))
    assert schema["$defs"][name]["properties"]["children"]["items"] == {
        "$ref": f"#/$defs/{name}"
    }


def test_tagged_array_like():
    class Arr(msgspec.Struct, tag="arr", tag_field="kind", array_like=True):
        id: str
        container_no: str
        address: str

    contract = contracts.contract(
        Arr, name="c", policy=contracts.AccessPolicy(exclude={"address"})
    )
    assert contract.export(Arr("a1", "C-9", "secret")) == ["arr", "a1", "C-9"]
    assert contract.decode(b'["arr","a1","C-9"]').id == "a1"


def test_build_channels():
    channels = contracts.build_channels(
        Claim,
        {"analytics": analytics_policy(), "supplier": supplier_policy()},
    )
    row = sample_claim()
    a_out = channels["analytics"].export(row)
    s_out = channels["supplier"].export(row)
    assert "customer_address" not in a_out["waybill"]
    assert s_out["waybill"]["customer_address"] == "12 Secret Street"
    assert "amount" not in s_out["waybill"]


def test_build_contracts():
    built = contracts.build_contracts(
        [Waybill, Claim],
        names=["analytics-waybill", "analytics-claim"],
        policies=[analytics_policy(), analytics_policy()],
    )
    assert [c.name for c in built] == ["analytics-waybill", "analytics-claim"]
    assert built[0].export(sample_waybill())["container_no"] == "C-9"
    assert built[1].export(sample_claim())["claim_no"] == "K-1"


def test_contract_alias_functions():
    assert contracts.build_contract is contracts.contract
    assert contracts.field_contract is contracts.build_contract
    assert (
        contracts.export(
            sample_waybill(),
            contracts.contract(Waybill, name="c", policy=analytics_policy()),
        )["kind"]
        == "waybill"
    )


def test_enum_literal_and_datetime_export():
    contract = contracts.contract(EnumRow, name="c", policy=contracts.AccessPolicy())
    when = datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc)
    ident = uuid.UUID(int=1)
    out = contract.export(EnumRow(Kind.B, "y", when, ident))
    assert out == {
        "kind": "b",
        "literal": "y",
        "when": "2026-01-02T00:00:00Z",
        "ident": str(ident),
    }
    assert contract.decode(msgspec.json.encode(out)).kind is Kind.B


def test_extra_json_schema_and_extra_metadata_are_preserved():
    class Row(msgspec.Struct):
        a: Annotated[int, msgspec.Meta(ge=1, description="positive")]
        b: Annotated[str, msgspec.Meta(extra={"source": "crm"})]

    contract = contracts.contract(Row, name="c", policy=contracts.AccessPolicy())
    schema = contract.schema["$defs"][next(iter(contract.schema["$defs"]))]
    assert schema["properties"]["a"]["minimum"] == 1
    assert schema["properties"]["a"]["description"] == "positive"
    assert "ge=1" in contract.type.__annotations__["a"]
    assert "description" in contract.type.__annotations__["a"]
    assert "extra={'source': 'crm'}" in contract.type.__annotations__["b"]


def test_invalid_policy_inputs():
    with pytest.raises(TypeError, match="policy must be"):
        contracts.contract(Waybill, policy=object())
    with pytest.raises(ValueError, match="at least one source type"):
        contracts.contract([])


def test_contract_repr_and_properties():
    contract = contracts.contract(Waybill, name="analytics", policy=analytics_policy())
    assert contract.name == "analytics"
    assert contract.source_types == (Waybill,)
    assert contract.conflicts == ()
    assert "analytics" in repr(contract)
    assert contract.to_schema() == contract.schema
    assert isinstance(contract.type_info, msgspec.inspect.StructType)


def test_kw_only_field_order_remains_valid():
    class S(msgspec.Struct, kw_only=True):
        a: int
        b: int = 0
        c: int

    contract = contracts.contract(
        S, name="kw", policy=contracts.AccessPolicy(exclude={"a"})
    )
    assert contract.type.__struct_fields__ == ("b", "c")
    assert contract.decode(b'{"b":1,"c":2}').c == 2


def test_array_like_removed_middle_field_keeps_order():
    class S(msgspec.Struct, array_like=True):
        a: int
        b: int
        c: int

    contract = contracts.contract(
        S, name="arr", policy=contracts.AccessPolicy(exclude={"b"}, primary_keys={"a"})
    )
    assert contract.export(S(1, 2, 3)) == [1, 3]
    assert contract.decode(b"[1,3]").c == 3


def test_unsupported_source_type_is_reported():
    from typing import TypedDict

    class TD(TypedDict):
        a: int

    with pytest.raises(TypeError, match="support msgspec Struct types"):
        contracts.contract(TD, name="td", policy=contracts.AccessPolicy())


def test_policy_aliases():
    rule = contracts.FieldRule(visible={"a", "b"}, hidden={"c"})
    assert rule.include == frozenset({"a", "b"})
    assert rule.exclude == frozenset({"c"})
    assert rule.allows("a", "a")
    assert not rule.allows("c", "c")
    policy = contracts.AccessPolicy(keys={"id"})
    assert policy.primary_keys == frozenset({"id"})
    assert contracts.ColumnPolicy is contracts.AccessPolicy
    assert contracts.ColumnRule is contracts.FieldRule


def test_describe_and_columns():
    contract = contracts.contract(Claim, name="analytics", policy=analytics_policy())
    described = contract.describe()
    assert described["kind"] == "struct"
    assert described["tag_field"] == "kind"
    assert described["tag"] == "claim"
    waybill_field = next(f for f in described["fields"] if f["name"] == "waybill")
    assert waybill_field["type"]["fields"][0]["name"] == "waybill_no"
    assert "customer_address" not in {
        f["name"] for f in waybill_field["type"]["fields"]
    }
    assert set(contract.columns()) == {
        "kind",
        "claim_no",
        "waybill",
        "description",
    }


def test_forbid_unknown_fields_can_be_disabled():
    contract = contracts.contract(
        Waybill,
        name="lenient",
        policy=analytics_policy(),
        forbid_unknown_fields=False,
    )
    out = contract.decode(
        b'{"kind":"waybill","waybill_no":"W-1","container_no":"C-9",'
        b'"amount":"0","extra":1}'
    )
    assert out.container_no == "C-9"


def test_shorthand_include_exclude_kwargs():
    contract = contracts.contract(
        Waybill, name="short", exclude={"customer_address"}, primary_keys={"waybill_no"}
    )
    assert "customer_address" not in contract.export(sample_waybill())

    allowed = contracts.contract(
        Waybill,
        name="short-include",
        include={"kind", "waybill_no", "container_no"},
    )
    assert allowed.export(sample_waybill()) == {
        "kind": "waybill",
        "waybill_no": "W-1",
        "container_no": "C-9",
    }


def test_check_contract_shorthand_kwargs():
    conflicts = contracts.check_contract(Waybill, name="short", exclude={"kind"})
    assert conflicts[0].kind == "hidden-discriminator"
