from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext

import pytest

from tests import testmodels
from tortoise import fields
from tortoise.exceptions import ConfigurationError, FieldError, IntegrityError
from tortoise.expressions import F
from tortoise.functions import Avg, Max, Sum


def test_max_digits_empty():
    with pytest.raises(
        TypeError,
        match="missing 2 required positional arguments: 'max_digits' and 'decimal_places'",
    ):
        fields.DecimalField()  # pylint: disable=E1120


def test_decimal_places_empty():
    with pytest.raises(TypeError, match="missing 1 required positional argument: 'decimal_places'"):
        fields.DecimalField(max_digits=1)  # pylint: disable=E1120


def test_max_fields_bad():
    with pytest.raises(ConfigurationError, match="'max_digits' must be >= 1"):
        fields.DecimalField(max_digits=0, decimal_places=2)


def test_decimal_places_bad():
    with pytest.raises(ConfigurationError, match="'decimal_places' must be >= 0"):
        fields.DecimalField(max_digits=2, decimal_places=-1)


@pytest.mark.asyncio
async def test_empty(db):
    with pytest.raises(IntegrityError):
        await testmodels.DecimalFields.create()


@pytest.mark.asyncio
async def test_create(db):
    obj0 = await testmodels.DecimalFields.create(decimal=Decimal("1.23456"), decimal_nodec=18.7)
    obj = await testmodels.DecimalFields.get(id=obj0.id)
    assert obj.decimal == Decimal("1.2346")
    assert obj.decimal_nodec == 19
    assert obj.decimal_null is None
    await obj.save()
    obj2 = await testmodels.DecimalFields.get(id=obj.id)
    assert obj == obj2


@pytest.mark.asyncio
async def test_update(db):
    obj0 = await testmodels.DecimalFields.create(decimal=Decimal("1.23456"), decimal_nodec=18.7)
    await testmodels.DecimalFields.filter(id=obj0.id).update(decimal=Decimal("2.345"))
    obj = await testmodels.DecimalFields.get(id=obj0.id)
    assert obj.decimal == Decimal("2.345")
    assert obj.decimal_nodec == 19
    assert obj.decimal_null is None


@pytest.mark.asyncio
async def test_filter(db):
    obj0 = await testmodels.DecimalFields.create(decimal=Decimal("1.23456"), decimal_nodec=18.7)
    obj = await testmodels.DecimalFields.filter(decimal=Decimal("1.2346")).first()
    assert obj == obj0
    obj = (
        await testmodels.DecimalFields.annotate(d=F("decimal")).filter(d=Decimal("1.2346")).first()
    )
    assert obj == obj0
    objs = await testmodels.DecimalFields.filter(decimal_nodec__gt=2).all()
    assert obj in objs
    objs = await testmodels.DecimalFields.filter(decimal_nodec__lt=100).all()
    assert obj in objs


@pytest.mark.asyncio
async def test_f_expression_update(db):
    obj0 = await testmodels.DecimalFields.create(decimal=Decimal("1.23456"), decimal_nodec=18.7)
    await obj0.filter(id=obj0.id).update(decimal=F("decimal") + Decimal("1"))
    obj1 = await testmodels.DecimalFields.get(id=obj0.id)
    assert obj1.decimal == Decimal("2.2346")
    await obj0.filter(id=obj0.id).update(decimal=Decimal("1") - F("decimal"))
    obj1 = await testmodels.DecimalFields.get(id=obj0.id)
    assert obj1.decimal == Decimal("-1.2346")


@pytest.mark.asyncio
async def test_values(db):
    obj0 = await testmodels.DecimalFields.create(decimal=Decimal("1.23456"), decimal_nodec=18.7)
    values = await testmodels.DecimalFields.get(id=obj0.id).values("decimal", "decimal_nodec")
    assert values["decimal"] == Decimal("1.2346")
    assert values["decimal_nodec"] == 19


@pytest.mark.asyncio
async def test_values_list(db):
    obj0 = await testmodels.DecimalFields.create(decimal=Decimal("1.23456"), decimal_nodec=18.7)
    values = await testmodels.DecimalFields.get(id=obj0.id).values_list("decimal", "decimal_nodec")
    assert list(values) == [Decimal("1.2346"), 19]


@pytest.mark.asyncio
async def test_order_by(db):
    await testmodels.DecimalFields.create(decimal=Decimal("0"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("9.99"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("27.27"), decimal_nodec=1)
    values = (
        await testmodels.DecimalFields.all().order_by("decimal").values_list("decimal", flat=True)
    )
    assert values == [Decimal("0"), Decimal("9.99"), Decimal("27.27")]


@pytest.mark.asyncio
async def test_aggregate_sum(db):
    await testmodels.DecimalFields.create(decimal=Decimal("0"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("9.99"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("27.27"), decimal_nodec=1)
    values = (
        await testmodels.DecimalFields.all()
        .annotate(sum_decimal=Sum("decimal"))
        .values("sum_decimal")
    )
    assert values[0] == {"sum_decimal": Decimal("37.26")}


@pytest.mark.asyncio
async def test_aggregate_sum_with_f_expression(db):
    await testmodels.DecimalFields.create(decimal=Decimal("0"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("9.99"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("27.27"), decimal_nodec=1)
    values = (
        await testmodels.DecimalFields.all()
        .annotate(sum_decimal=Sum(F("decimal")))
        .values("sum_decimal")
    )
    assert values[0] == {"sum_decimal": Decimal("37.26")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(sum_decimal=Sum(F("decimal") + 1))
        .values("sum_decimal")
    )
    assert values[0] == {"sum_decimal": Decimal("40.26")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(sum_decimal=Sum(F("decimal") + F("decimal")))
        .values("sum_decimal")
    )
    assert values[0] == {"sum_decimal": Decimal("74.52")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(sum_decimal=Sum(F("decimal") + F("decimal_nodec")))
        .values("sum_decimal")
    )
    assert values[0] == {"sum_decimal": Decimal("4E+1")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(sum_decimal=Sum(F("decimal") + F("decimal_null")))
        .values("sum_decimal")
    )
    assert values[0] == {"sum_decimal": None}


@pytest.mark.asyncio
async def test_aggregate_sum_no_exist_field_with_f_expression(db):
    with pytest.raises(
        FieldError,
        match="There is no non-virtual field not_exist on Model DecimalFields",
    ):
        await (
            testmodels.DecimalFields.all()
            .annotate(sum_decimal=Sum(F("not_exist")))
            .values("sum_decimal")
        )


@pytest.mark.asyncio
async def test_aggregate_sum_different_field_type_at_right_with_f_expression(db):
    with pytest.raises(
        FieldError, match="Cannot use arithmetic expression between different field type"
    ):
        await (
            testmodels.DecimalFields.all()
            .annotate(sum_decimal=Sum(F("decimal") + F("id")))
            .values("sum_decimal")
        )


@pytest.mark.asyncio
async def test_aggregate_sum_different_field_type_at_left_with_f_expression(db):
    with pytest.raises(
        FieldError, match="Cannot use arithmetic expression between different field type"
    ):
        await (
            testmodels.DecimalFields.all()
            .annotate(sum_decimal=Sum(F("id") + F("decimal")))
            .values("sum_decimal")
        )


@pytest.mark.asyncio
async def test_aggregate_avg(db):
    await testmodels.DecimalFields.create(decimal=Decimal("0"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("9.99"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("27.27"), decimal_nodec=1)
    values = (
        await testmodels.DecimalFields.all()
        .annotate(avg_decimal=Avg("decimal"))
        .values("avg_decimal")
    )
    assert values[0] == {"avg_decimal": Decimal("12.42")}


@pytest.mark.asyncio
async def test_aggregate_avg_with_f_expression(db):
    await testmodels.DecimalFields.create(decimal=Decimal("0"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("9.99"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("27.27"), decimal_nodec=1)
    values = (
        await testmodels.DecimalFields.all()
        .annotate(avg_decimal=Avg(F("decimal")))
        .values("avg_decimal")
    )
    assert values[0] == {"avg_decimal": Decimal("12.42")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(avg_decimal=Avg(F("decimal") + 1))
        .values("avg_decimal")
    )
    assert values[0] == {"avg_decimal": Decimal("13.42")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(avg_decimal=Avg(F("decimal") + F("decimal")))
        .values("avg_decimal")
    )
    assert values[0] == {"avg_decimal": Decimal("24.84")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(avg_decimal=Avg(F("decimal") + F("decimal_nodec")))
        .values("avg_decimal")
    )
    assert values[0] == {"avg_decimal": Decimal("13")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(avg_decimal=Avg(F("decimal") + F("decimal_null")))
        .values("avg_decimal")
    )
    assert values[0] == {"avg_decimal": None}


@pytest.mark.asyncio
async def test_aggregate_max(db):
    await testmodels.DecimalFields.create(decimal=Decimal("0"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("9.99"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("27.27"), decimal_nodec=1)
    values = (
        await testmodels.DecimalFields.all()
        .annotate(max_decimal=Max("decimal"))
        .values("max_decimal")
    )
    assert values[0] == {"max_decimal": Decimal("27.27")}


@pytest.mark.asyncio
async def test_aggregate_max_with_f_expression(db):
    await testmodels.DecimalFields.create(decimal=Decimal("0"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("9.99"), decimal_nodec=1)
    await testmodels.DecimalFields.create(decimal=Decimal("27.27"), decimal_nodec=1)
    values = (
        await testmodels.DecimalFields.all()
        .annotate(max_decimal=Max(F("decimal")))
        .values("max_decimal")
    )
    assert values[0] == {"max_decimal": Decimal("27.27")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(max_decimal=Max(F("decimal") + 1))
        .values("max_decimal")
    )
    assert values[0] == {"max_decimal": Decimal("28.27")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(max_decimal=Max(F("decimal") + F("decimal")))
        .values("max_decimal")
    )
    assert values[0] == {"max_decimal": Decimal("54.54")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(max_decimal=Max(F("decimal") + F("decimal_nodec")))
        .values("max_decimal")
    )
    assert values[0] == {"max_decimal": Decimal("28")}

    values = (
        await testmodels.DecimalFields.all()
        .annotate(max_decimal=Max(F("decimal") + F("decimal_null")))
        .values("max_decimal")
    )
    assert values[0] == {"max_decimal": None}


@pytest.mark.asyncio
async def test_scale_is_preserved(db):
    # Decimal.__eq__ compares numerically, so `== Decimal("27.27")` cannot see
    # the scale. Assert on the string form, which is what reaches a template,
    # an f-string or a JSON serializer.
    obj0 = await testmodels.DecimalFields.create(decimal=Decimal("100.00"), decimal_nodec=10)
    obj = await testmodels.DecimalFields.get(id=obj0.id)
    assert str(obj.decimal) == "100.0000"
    assert str(obj.decimal_nodec) == "10"
    assert obj.decimal.as_tuple().exponent == -4
    assert obj.decimal_nodec.as_tuple().exponent == 0


def test_to_python_value_scale():
    field = fields.DecimalField(max_digits=12, decimal_places=2)
    for value, expected in (
        ("100.00", "100.00"),
        ("100", "100.00"),
        ("10", "10.00"),
        ("1000000", "1000000.00"),
        ("0", "0.00"),
        ("1234.5", "1234.50"),
        ("12.3456", "12.35"),
    ):
        assert str(field.to_python_value(value)) == expected

    field0 = fields.DecimalField(max_digits=12, decimal_places=0)
    assert str(field0.to_python_value("10")) == "10"
    assert str(field0.to_python_value("100")) == "100"


def test_to_python_value_high_precision():
    field = fields.DecimalField(max_digits=40, decimal_places=10)
    assert field.to_python_value("123456789012345678901234567890.5") == Decimal(
        "123456789012345678901234567890.5000000000"
    )
    field = fields.DecimalField(max_digits=30, decimal_places=2)
    assert field.to_python_value("1234567890123456789012345678.50") == Decimal(
        "1234567890123456789012345678.50"
    )


def test_to_python_value_high_precision_boundaries():
    field = fields.DecimalField(max_digits=28, decimal_places=0)
    assert field.to_python_value("1" * 28) == Decimal("1" * 28)

    field = fields.DecimalField(max_digits=30, decimal_places=2)
    with pytest.raises(InvalidOperation):
        field.to_python_value("1" * 31)

    field = fields.DecimalField(max_digits="40", decimal_places=10)
    assert field.to_python_value("1" * 30) == Decimal("1" * 30 + ".0000000000")


def test_to_python_value_keeps_context_rounding():
    field = fields.DecimalField(max_digits=5, decimal_places=2)
    with localcontext() as ctx:
        ctx.rounding = ROUND_HALF_UP
        assert str(field.to_python_value("1.005")) == "1.01"


BIG = Decimal("92233720368547758.07")
BIG_ALT = Decimal("92233720368547758.06")
THRESHOLD = Decimal("92233720368547758.06")


async def _seed_bills(old_batch: int = 1, reverse: bool = False) -> None:
    done, pending, first, second = "done", "pending", "A", "B"
    rows = [
        (old_batch, first, done, BIG),
        (old_batch, first, done, BIG),
        (old_batch + 1, first, done, -BIG_ALT),
        (old_batch, second, done, BIG),
        (old_batch, second, done, BIG_ALT),
        (old_batch + 1, second, done, -BIG_ALT),
        (old_batch + 1, first, pending, Decimal("1.00")),
    ]
    if reverse:
        rows.reverse()
    for batch, warehouse, status, amount in rows:
        await testmodels.BillEntry.create(
            batch=batch, warehouse=warehouse, status=status, amount=amount
        )


def _report():
    # Only settled entries, every positive and offsetting negative contributes.
    return (
        testmodels.BillEntry.filter(status="done").annotate(net=Sum("amount")).group_by("warehouse")
    )


@pytest.mark.asyncio
async def test_exact_decimal_filter_beyond_float_precision(db):
    await _seed_bills()
    # Detail filtering must tell the two adjacent amounts apart.
    rows = await (
        testmodels.BillEntry.filter(status="done", amount__gt=THRESHOLD)
        .order_by("warehouse")
        .values_list("warehouse", "amount")
    )
    assert rows == [
        ("A", BIG),
        ("A", BIG),
        ("B", BIG),
    ]


@pytest.mark.asyncio
async def test_exact_decimal_net_aggregate_filter_order_values(db):
    await _seed_bills()
    values = await _report().order_by("-net").values("warehouse", "net")
    assert values == [
        {"warehouse": "A", "net": Decimal("92233720368547758.08")},
        {"warehouse": "B", "net": Decimal("92233720368547758.07")},
    ]
    assert all(isinstance(row["net"], Decimal) for row in values)
    assert all(row["net"].as_tuple().exponent == -2 for row in values)

    filtered = await _report().filter(net__gt=BIG).values("warehouse", "net")
    assert filtered == [{"warehouse": "A", "net": Decimal("92233720368547758.08")}]


@pytest.mark.asyncio
async def test_exact_decimal_net_after_correction(db):
    await _seed_bills()
    await testmodels.BillEntry.filter(batch=2, warehouse="A", status="done").update(amount=-BIG)
    values = await _report().order_by("-net").values("warehouse", "net")
    assert {row["warehouse"]: row["net"] for row in values} == {"A": BIG, "B": BIG}
    assert await _report().filter(net__gt=BIG).values("warehouse", "net") == []


@pytest.mark.asyncio
async def test_exact_decimal_insertion_order_independent(db):
    await _seed_bills(reverse=False)
    forward = await _report().order_by("-net").values("warehouse", "net")

    await testmodels.BillEntry.all().delete()
    await _seed_bills(reverse=True)
    backward = await _report().order_by("-net").values("warehouse", "net")

    assert forward == backward


@pytest.mark.asyncio
async def test_exact_decimal_aggregate_variants(db):
    from tortoise.expressions import Q

    await _seed_bills()
    filtered = (
        await testmodels.BillEntry.filter(status="done")
        .annotate(total=Sum("amount", _filter=Q(warehouse="A")))
        .values("total")
    )
    assert filtered == [{"total": Decimal("92233720368547758.08")}]

    distinct = (
        await testmodels.BillEntry.filter(warehouse="A", status="done")
        .annotate(total=Sum("amount", distinct=True))
        .values("total")
    )
    # Two identical credits collapse, the negative offset is still applied exactly.
    assert distinct == [{"total": Decimal("0.01")}]

    # Integers still use the native aggregate and stay exact.
    await testmodels.ValidatorModel.create(max_value=2)
    await testmodels.ValidatorModel.create(max_value=3)
    ints = await testmodels.ValidatorModel.annotate(total=Sum("max_value")).values("total")
    assert ints == [{"total": 5}]
