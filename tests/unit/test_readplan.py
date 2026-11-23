"""Tests for readplan: static read planning over msgspec contracts."""

import pytest

import msgspec

from typing import List, Optional, Union

from readplan import (
    And,
    Exists,
    Not,
    Or,
    Present,
    ReadRule,
    TagEq,
    evaluate,
    plan_reads,
)


class Refund(msgspec.Struct, tag="refund", tag_field="kind"):
    # amount is optional: the legacy currency column is only read when
    # amount was never sent.
    amount: Optional[int] = None
    legacy_currency: Optional[str] = msgspec.field(default=None, name="legacyCurrency")


class Charge(msgspec.Struct, tag="charge", tag_field="kind"):
    # amount is required but may be explicitly null.
    amount: Optional[int]
    charge_ref: str
    legacy_currency: Optional[str] = msgspec.field(default=None, name="legacyCurrency")


class Line(msgspec.Struct, tag=True):
    sku: str


class DiscountLine(msgspec.Struct, tag="discount"):
    pct: int


class Event(msgspec.Struct):
    txn: Union[Refund, Charge]
    lines: List[Union[Line, DiscountLine]]
    note: Optional[str] = None


def test_absent_amount_guard_is_reachable_for_refund():
    plan = plan_reads(
        Event,
        [
            ReadRule(
                "legacy_currency",
                "txn.legacyCurrency",
                And(TagEq("txn", "refund"), Not(Present("txn.amount"))),
            )
        ],
    )
    (read,) = plan.reads
    assert read.reachable
    assert read.reason is None
    txn = read.witness["txn"]
    assert txn["kind"] == "refund"
    assert "amount" not in txn  # amount was never sent
    assert "legacyCurrency" in txn  # encoded name, not the Python attr name
    msgspec.convert(read.witness, type=Event)


def test_absent_amount_guard_is_unreachable_for_charge():
    plan = plan_reads(
        Event,
        [
            ReadRule(
                "legacy_currency",
                "txn.legacyCurrency",
                And(TagEq("txn", "charge"), Not(Present("txn.amount"))),
            )
        ],
    )
    (read,) = plan.reads
    assert not read.reachable
    assert "required" in read.reason
    assert "null" in read.reason  # explains null is still "present"


def test_required_nullable_amount_is_always_present():
    # The contract allows an explicit null amount...
    charge = msgspec.convert({"kind": "charge", "amount": None, "charge_ref": "x"}, type=Charge)
    assert charge.amount is None
    # ...so a guard on presence (not nullness) holds, and the read is planned.
    plan = plan_reads(
        Event,
        [ReadRule("amount", "txn.amount", And(TagEq("txn", "charge"), Present("txn.amount")))],
    )
    (read,) = plan.reads
    assert read.reachable
    assert read.witness["txn"]["kind"] == "charge"
    assert "amount" in read.witness["txn"]


def test_present_distinguishes_absent_from_null():
    assert evaluate(Not(Present("txn.amount")), {"txn": {"kind": "refund"}}, Event)
    assert not evaluate(
        Not(Present("txn.amount")), {"txn": {"kind": "refund", "amount": None}}, Event
    )


def test_exists_over_list_element_tags():
    plan = plan_reads(
        Event,
        [ReadRule("amount", "txn.amount", Exists("lines", TagEq("", "discount")))],
    )
    (read,) = plan.reads
    assert read.reachable
    assert any(
        isinstance(line, dict) and line.get("type") == "discount"
        for line in read.witness["lines"]
    )
    msgspec.convert(read.witness, type=Event)


def test_exists_with_unsatisfiable_element_condition():
    plan = plan_reads(
        Event,
        [ReadRule("amount", "txn.amount", Exists("lines", TagEq("", "gift")))],
    )
    (read,) = plan.reads
    assert not read.reachable
    assert "no element" in read.reason
    assert "'Line'" in read.reason and "'discount'" in read.reason


def test_not_exists_witness_uses_empty_list():
    plan = plan_reads(
        Event,
        [ReadRule("amount", "txn.amount", Not(Exists("lines", TagEq("", "discount"))))],
    )
    (read,) = plan.reads
    assert read.reachable
    assert read.witness["lines"] == []
    msgspec.convert(read.witness, type=Event)


def test_unknown_tag_is_pruned_with_declared_tags():
    plan = plan_reads(Event, [ReadRule("amount", "txn.amount", TagEq("txn", "credit"))])
    (read,) = plan.reads
    assert not read.reachable
    assert "'credit'" in read.reason
    assert "'refund'" in read.reason and "'charge'" in read.reason


def test_python_attribute_name_is_not_a_valid_source_path():
    plan = plan_reads(Event, [ReadRule("cur", "txn.legacy_currency")])
    (read,) = plan.reads
    assert not read.reachable
    assert "legacy_currency" in read.reason
    assert "legacyCurrency" in read.reason  # declared encoded names are listed


def test_conflicting_tag_guards():
    plan = plan_reads(
        Event,
        [ReadRule("amount", "txn.amount", And(TagEq("txn", "refund"), TagEq("txn", "charge")))],
    )
    (read,) = plan.reads
    assert not read.reachable
    # both tags are mentioned: each conjunct excludes the other's variant
    assert "'refund'" in read.reason and "'charge'" in read.reason


def test_guard_conflicts_with_source_path_variant():
    # charge_ref only exists on Charge, but the guard pins txn to Refund.
    plan = plan_reads(
        Event,
        [ReadRule("ref", "txn.charge_ref", TagEq("txn", "refund"))],
    )
    (read,) = plan.reads
    assert not read.reachable
    assert "conflict" in read.reason
    assert "txn.charge_ref" in read.reason


def test_or_guard_keeps_both_variants_live():
    # Both variants are considered from the contract alone, even if an early
    # sampling batch only ever contained one of them.
    plan = plan_reads(
        Event,
        [ReadRule("amount", "txn.amount", Or(TagEq("txn", "refund"), TagEq("txn", "charge")))],
    )
    (read,) = plan.reads
    assert read.reachable


def test_union_root_with_empty_path_tag_guard():
    plan = plan_reads(
        Union[Refund, Charge],
        [ReadRule("amount", "amount", TagEq("", "charge"))],
    )
    (read,) = plan.reads
    assert read.reachable
    assert read.witness["kind"] == "charge"
    assert "amount" in read.witness
    msgspec.convert(read.witness, type=Union[Refund, Charge])


def test_unguarded_read_is_unconditionally_reachable():
    plan = plan_reads(Event, [ReadRule("note", "note")])
    (read,) = plan.reads
    assert read.reachable
    assert "note" in read.witness


def test_plan_aggregates_columns_and_report_uses_encoded_names():
    plan = plan_reads(
        Event,
        [
            ReadRule("amount", "txn.amount"),
            ReadRule(
                "legacy_cur",
                "txn.legacyCurrency",
                And(TagEq("txn", "refund"), Not(Present("txn.amount"))),
            ),
            ReadRule(
                "legacy_cur",
                "txn.legacyCurrency",
                And(TagEq("txn", "charge"), Not(Present("txn.amount"))),
            ),
        ],
    )
    assert plan.sources_for("amount") == ["txn.amount"]
    assert plan.sources_for("legacy_cur") == ["txn.legacyCurrency"]
    assert len(plan.pruned()) == 1
    report = plan.report()
    assert "<- txn.legacyCurrency" in report
    assert "<- txn.legacy_currency" not in report
    # witnesses are keyed by encoded names too
    (witness,) = {tuple(sorted(r.witness["txn"])) for r in plan.reads if r.reachable and r.column == "legacy_cur"}
    assert "legacyCurrency" in witness
    assert "legacy_currency" not in witness


def test_every_reachable_witness_is_legal_and_reaches_the_path():
    rules = [
        ReadRule("amount", "txn.amount", TagEq("txn", "refund")),
        ReadRule("amount", "txn.amount", TagEq("txn", "charge")),
        ReadRule(
            "legacy_currency",
            "txn.legacyCurrency",
            And(TagEq("txn", "refund"), Not(Present("txn.amount"))),
        ),
        ReadRule("amount", "txn.amount", Exists("lines", TagEq("", "discount"))),
        ReadRule("amount", "txn.amount", Not(Exists("lines", TagEq("", "Line")))),
        ReadRule("sku", "lines", None),
    ]
    plan = plan_reads(Event, rules)
    for rule, read in zip(rules, plan.reads):
        assert read.reachable, read.reason
        assert read.note is None
        decoded = msgspec.convert(read.witness, type=Event)
        assert decoded is not None
        if rule.guard is not None:
            assert evaluate(rule.guard, read.witness, Event)
        assert evaluate(Present(rule.source), read.witness, Event)


def test_empty_source_path_rejected():
    with pytest.raises(ValueError):
        plan_reads(Event, [ReadRule("x", "")])
