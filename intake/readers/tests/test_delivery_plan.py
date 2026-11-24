from decimal import Decimal

import pandas as pd
import pytest

from intake.readers import BaseConverter, BaseReader
from intake.readers.delivery import ArtifactSpec, DeliveryPlanner, plan_deliveries

pytestmark = pytest.mark.skipif(pd is None, reason="pandas is required")

RAW = {"name": "raw_orders", "tags": {"amount": "text", "currency": "text"}}
RAW_FLOAT = {"name": "raw_orders", "tags": {"amount": "binary_float"}}
LEDGER = {"name": "ledger", "tags": {"amount": "Decimal", "currency": "CNY"}}
LAZY = "lazy_orders"
FX = "fx_table"
REQUESTS = [("ledger", LEDGER), ("raw", RAW), ("voucher", FX)]

ROWS = [
    ("O1", "USD", "9007199254740993.01"),
    ("O1", "USD", "0.10"),
    ("O2", "JPY", "100"),
    ("O3", "USD", "1.00"),
    ("O4", "JPY", "2"),
    ("O5", "JPY", "5"),
    ("O6", "USD", "6"),
    ("O7", "JPY", "7"),
    ("O8", "USD", "8"),
    ("O9", "JPY", "9"),
    ("O10", "USD", "10"),
    ("O11", "JPY", "11"),
]


class LazyOrders:
    def __init__(self, rows):
        self.rows = rows

    def materialize(self):
        return pd.DataFrame(self.rows, columns=["order_id", "currency", "amount"])


class FXReader(BaseReader):
    cost = 4
    provides = FX
    coverage = "full"
    materialized = True

    def __init__(self):
        super().__init__()
        self.read_count = 0

    def _read(self, **kwargs):
        self.read_count += 1
        return pd.DataFrame(
            [("USD", Decimal("7")), ("JPY", Decimal("0.05"))],
            columns=["currency", "rate"],
        )


def make_plugins():
    fx = FXReader()

    class LazyOrderReader(BaseReader):
        cost = 1
        provides = LAZY
        coverage = "lazy"
        materialized = False
        read_count = 0

        def _read(self, source=None, **kwargs):
            type(self).read_count += 1
            return LazyOrders(ROWS)

    class MaterializeOrders(BaseConverter):
        cost = 7
        provides = RAW
        requires = LAZY
        coverage = "full"
        materialized = True
        run_count = 0
        last_output = None

        def run(self, lazy, **kwargs):
            type(self).run_count += 1
            output = lazy.materialize()
            type(self).last_output = output
            return output

    class FloatOrderReader(BaseReader):
        cost = 3
        provides = RAW_FLOAT
        coverage = "full"
        materialized = True

        def _read(self, **kwargs):
            frame = pd.DataFrame(ROWS, columns=["order_id", "currency", "amount"])
            frame["amount"] = frame["amount"].astype(float)
            return frame

    def to_ledger(raw, rates):
        rate_map = dict(zip(rates.currency, rates.rate))
        ledger = raw.copy(deep=True)
        ledger["amount"] = [
            (Decimal(str(value)) * rate_map[currency]).quantize(Decimal("0.01"))
            for value, currency in zip(raw.amount, raw.currency)
        ]
        ledger["currency"] = "CNY"
        return ledger

    def apply_ledger_in_place(raw, rates):
        rate_map = dict(zip(rates.currency, rates.rate))
        raw["amount"] = [
            (Decimal(str(value)) * rate_map[currency]).quantize(Decimal("0.01"))
            for value, currency in zip(raw.amount, raw.currency)
        ]
        raw["currency"] = "CNY"
        return raw

    class InPlaceLedger(BaseConverter):
        cost = 2
        provides = LEDGER
        requires = RAW
        mutates_input = True
        fx_reader = fx

        def run(self, raw, fx_reader, **kwargs):
            return apply_ledger_in_place(raw, fx_reader)

    class CopyPreservingLedger(BaseConverter):
        cost = 6
        provides = LEDGER
        requires = RAW
        mutates_input = False
        fx_reader = fx

        def run(self, raw, fx_reader, **kwargs):
            return to_ledger(raw, fx_reader)

    class DirectLedgerReader(BaseReader):
        cost = 8
        provides = LEDGER
        coverage = "full"
        materialized = True

        def _read(self, fx_reader, **kwargs):
            raw = pd.DataFrame(ROWS, columns=["order_id", "currency", "amount"])
            return to_ledger(raw, fx_reader)

    direct = DirectLedgerReader(fx_reader=fx)
    candidates = [
        LazyOrderReader,
        MaterializeOrders,
        FloatOrderReader,
        InPlaceLedger,
        CopyPreservingLedger,
        direct,
    ]
    return candidates, fx, LazyOrderReader, MaterializeOrders, CopyPreservingLedger


def make_plan(candidates, **kwargs):
    options = dict(
        source="orders://batch-1",
        batch_id="B1",
        fx_version="fx-v1",
        copy_cost=3,
    )
    options.update(kwargs)
    return plan_deliveries(REQUESTS, candidates, **options)


def test_minimum_joint_plan_cost_and_precision():
    candidates, fx, lazy_cls, materialize_cls, _ = make_plugins()
    lazy_cls.read_count = 0
    materialize_cls.run_count = 0
    plan = make_plan(candidates)

    assert plan.cost == 17
    assert [action.name for action in plan.steps] == [
        "LazyOrderReader",
        "MaterializeOrders",
        "copy",
        "FXReader",
        "InPlaceLedger",
    ]
    assert lazy_cls.read_count == 0
    assert materialize_cls.run_count == 0
    assert fx.read_count == 0

    result = plan.execute()
    ledger = result["ledger"]
    raw = result["raw"]
    voucher = result["voucher"]

    assert [str(value) for value in ledger.amount.iloc[:3]] == [
        "63050394783186951.07",
        "0.70",
        "5.00",
    ]
    assert ledger.currency.unique().tolist() == ["CNY"]
    assert raw.amount.iloc[0] == "9007199254740993.01"
    assert raw.currency.iloc[0] == "USD"
    assert len(raw) == len(ledger) == len(ROWS)
    assert raw.order_id.tolist() == [row[0] for row in ROWS]
    assert fx.read_count == 1
    assert plan.actual_cost == 17
    assert plan.execution_counts["FXReader"] == 1
    assert voucher is plan.read("voucher")
    assert raw is plan.read("raw")
    assert raw is materialize_cls.last_output
    assert raw.amount.iloc[0] == "9007199254740993.01"


def test_copy_quote_selected_route_changes():
    candidates, _, _, _, preserving = make_plugins()
    plan = make_plan(candidates, copy_cost=7)
    assert plan.cost == 18
    assert "CopyPreservingLedger" in [action.name for action in plan.steps]

    without_preserving = [candidate for candidate in candidates if candidate is not preserving]
    plan = make_plan(without_preserving, copy_cost=7)
    assert plan.cost == 20
    names = [action.name for action in plan.steps]
    assert "DirectLedgerReader" in names
    assert "LazyOrderReader" in names


def test_existing_lazy_artifact_omits_sunk_reader_cost():
    candidates, _, _, _, _ = make_plugins()
    lazy = LazyOrders(ROWS)
    artifact = ArtifactSpec(
        value=lazy,
        profile=LAZY,
        coverage="lazy",
        materialized=False,
    )
    plan = make_plan(candidates, existing=[artifact])
    assert plan.cost == 16
    assert "LazyOrderReader" not in [action.name for action in plan.steps]


def test_preview_is_not_full_coverage():
    candidates, _, _, _, _ = make_plugins()
    preview = pd.DataFrame(ROWS[:10], columns=["order_id", "currency", "amount"])
    artifact = ArtifactSpec(
        value=preview,
        profile=RAW,
        coverage="preview",
        materialized=True,
    )
    plan = make_plan(candidates, existing=[artifact])
    assert plan.cost == 17
    result = plan.execute()
    assert len(result["raw"]) == len(ROWS)
    assert len(preview) == 10


def test_caller_owned_materialized_raw_is_not_mutated():
    candidates, fx, _, _, preserving = make_plugins()
    raw = LazyOrders(ROWS).materialize()
    artifact = ArtifactSpec(value=raw, profile=RAW, coverage="full", materialized=True)
    plan = make_plan(candidates, existing=[artifact])
    assert plan.cost == 9
    result = plan.execute()
    assert result["raw"] is raw
    assert raw.amount.iloc[0] == "9007199254740993.01"
    assert raw.currency.iloc[0] == "USD"
    assert result["ledger"].amount.iloc[0] == Decimal("63050394783186951.07")
    assert fx.read_count == 1

    plan = make_plan(candidates, existing=[artifact], copy_cost=7)
    assert plan.cost == 10

    without_preserving = [candidate for candidate in candidates if candidate is not preserving]
    plan = make_plan(without_preserving, existing=[artifact], copy_cost=7)
    assert plan.cost == 12
    assert "DirectLedgerReader" in [action.name for action in plan.steps]


def test_batch_and_fx_version_are_part_of_identity():
    candidates, _, _, _, _ = make_plugins()
    raw = LazyOrders(ROWS).materialize()
    artifact = ArtifactSpec(
        value=raw,
        profile=RAW,
        coverage="full",
        materialized=True,
        batch_id="B2",
        fx_version="other",
    )
    plan = make_plan(candidates, existing=[artifact], batch_id="B1", fx_version="fx-v1")
    assert plan.cost == 17


def test_missing_caliber_reports_delivery_instead_of_reading():
    planner = DeliveryPlanner([], batch_id="B1", fx_version="v1")
    with pytest.raises(Exception) as exc_info:
        planner.plan([("raw", RAW), ("ledger", LEDGER)])
    assert "raw" in str(exc_info.value)
    assert exc_info.value.missing["raw"]["reason"]
