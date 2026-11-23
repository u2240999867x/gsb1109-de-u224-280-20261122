from typing import Annotated

import pytest

import msgspec
from msgspec import Meta
from msgspec.units import (
    CONTRACT,
    DEFAULT_UNIT_KEY,
    OVERRIDE,
    UnitColumn,
    compile_unit_table,
)

from .utils import temp_module


class Measurement(msgspec.Struct):
    value: float
    label: str = "m"


class DeviceEvent(msgspec.Struct):
    calibration: Annotated[Measurement, Meta(extra={"unit": "mV"})]
    reading: Annotated[Measurement, Meta(extra={"unit": "V"})]


FACTORS = {"mV": 0.001, "V": 1.0, "uV": 0.000001}


def _event_row():
    return DeviceEvent(
        calibration=Measurement(1000.0),
        reading=Measurement(2.0),
    )


def test_compile_reads_contract_units():
    table = compile_unit_table(DeviceEvent, factors=FACTORS, target_unit="V")

    assert table.root is DeviceEvent
    assert table.target_unit == "V"
    assert table.overrides == {}
    assert table.columns == (
        UnitColumn(
            path="$.calibration",
            cls=Measurement,
            field="calibration",
            unit="mV",
            contract_unit="mV",
            rule=CONTRACT,
            factor=0.001,
        ),
        UnitColumn(
            path="$.reading",
            cls=Measurement,
            field="reading",
            unit="V",
            contract_unit="V",
            rule=CONTRACT,
            factor=1.0,
        ),
    )
    assert set(table.by_path()) == {"$.calibration", "$.reading"}


def test_columns_without_factors_default_to_identity():
    table = compile_unit_table(DeviceEvent)
    assert [column.factor for column in table.columns] == [1.0, 1.0]


def test_override_applies_only_at_its_occurrence_path():
    table = compile_unit_table(
        DeviceEvent,
        overrides={"calibration": "uV"},
        factors=FACTORS,
    )

    calibration, reading = table.columns
    assert calibration.rule == OVERRIDE
    assert calibration.unit == "uV"
    assert calibration.contract_unit == "mV"
    assert calibration.factor == 0.000001
    # The sibling occurrence keeps the contract scope.
    assert reading.rule == CONTRACT
    assert reading.unit == "V"


def test_override_accepts_mapping_and_root_prefix_variants():
    kwargs = dict(factors=FACTORS)
    t1 = compile_unit_table(
        DeviceEvent, overrides={"$.calibration": {"unit": "uV"}}, **kwargs
    )
    t2 = compile_unit_table(DeviceEvent, overrides={"$.calibration": "uV"}, **kwargs)
    t3 = compile_unit_table(DeviceEvent, overrides={"calibration": "uV"}, **kwargs)

    for table in (t1, t2, t3):
        column = table.by_path()["$.calibration"]
        assert column.rule == OVERRIDE
        assert column.unit == "uV"
        assert table.overrides == {"$.calibration": "uV"}


def test_override_without_factor_falls_back_to_contract_factor():
    table = compile_unit_table(
        DeviceEvent,
        overrides={"calibration": "uV"},
        factors={"mV": 0.001},
    )
    assert table.by_path()["$.calibration"].factor == 0.001


def test_override_may_add_a_unit_where_contract_has_none():
    class Plain(msgspec.Struct):
        measurement: Measurement

    table = compile_unit_table(Plain, overrides={"measurement": "V"}, factors=FACTORS)
    column = table.by_path()["$.measurement"]
    assert column.rule == OVERRIDE
    assert column.unit == "V"
    assert column.contract_unit is None
    # The occurrence has no `Metadata` wrapper, yet the factor still reaches the
    # numeric leaves when converting a row.
    out = table.convert(Plain(Measurement(3.0)))
    assert out.measurement.value == 3.0


def test_override_on_unwrapped_scalar_field():
    class Sample(msgspec.Struct):
        voltage: float

    table = compile_unit_table(
        Sample, overrides={"voltage": "mV"}, factors={"mV": 0.001}
    )
    column = table.by_path()["$.voltage"]
    assert column.rule == OVERRIDE
    assert column.cls is None
    assert table.convert(Sample(2500.0)).voltage == 2.5


def test_invalid_override_values_raise():
    with pytest.raises(ValueError, match="'unit' entry"):
        compile_unit_table(DeviceEvent, overrides={"calibration": {}})
    with pytest.raises(ValueError, match="unit string"):
        compile_unit_table(DeviceEvent, overrides={"calibration": 5})


def test_override_path_without_occurrence_raises():
    with pytest.raises(KeyError, match=r"\$.no_such_field"):
        compile_unit_table(DeviceEvent, overrides={"no_such_field": "V"})


def test_custom_unit_key():
    class Tagged(msgspec.Struct):
        reading: Annotated[Measurement, Meta(extra={"physical_unit": "V"})]

    table = compile_unit_table(Tagged, unit_key="physical_unit", factors={"V": 1.0})
    assert table.by_path()["$.reading"].unit == "V"
    # The default key finds nothing.
    assert compile_unit_table(Tagged).columns == ()


def test_type_without_unit_metadata_compiles_empty():
    class Bare(msgspec.Struct):
        value: float

    table = compile_unit_table(Bare)
    assert table.columns == ()
    assert table.convert(Bare(3.0)) == Bare(3.0)


def test_convert_scales_numeric_leaves_and_preserves_rest():
    table = compile_unit_table(DeviceEvent, factors=FACTORS)
    row = _event_row()
    out = table.convert(row)

    assert isinstance(out, DeviceEvent)
    assert isinstance(out.calibration, Measurement)
    assert out.calibration.value == 1.0
    assert out.reading.value == 2.0
    assert out.calibration.label == "m"
    # The input row is not mutated.
    assert row.calibration.value == 1000.0


def test_convert_works_on_mapping_rows():
    table = compile_unit_table(DeviceEvent, factors=FACTORS)
    row = {
        "calibration": {"value": 1000.0, "label": "m"},
        "reading": {"value": 2.0, "label": "m"},
    }
    assert table.convert(row) == {
        "calibration": {"value": 1.0, "label": "m"},
        "reading": {"value": 2.0, "label": "m"},
    }


def test_two_compilations_are_independent():
    table_a = compile_unit_table(
        DeviceEvent, overrides={"calibration": "uV"}, factors=FACTORS
    )
    table_b = compile_unit_table(
        DeviceEvent, overrides={"reading": "uV"}, factors=FACTORS
    )
    row = _event_row()

    assert table_a.by_path()["$.calibration"].rule == OVERRIDE
    assert table_a.by_path()["$.reading"].rule == CONTRACT
    assert table_b.by_path()["$.calibration"].rule == CONTRACT
    assert table_b.by_path()["$.reading"].rule == OVERRIDE

    out_a = table_a.convert(row)
    out_b = table_b.convert(row)
    assert out_a.calibration.value == 0.001
    assert out_a.reading.value == 2.0
    assert out_b.calibration.value == 1.0
    assert out_b.reading.value == 0.000002


_RECURSIVE_CODE = """
from typing import Annotated
import msgspec
from msgspec import Meta

class Measurement(msgspec.Struct):
    value: float

class ThresholdAlert(msgspec.Struct):
    reading: Annotated[Measurement, Meta(extra={"unit": "V"})]
    nested: "ThresholdAlert | None" = None

class DeviceEvent(msgspec.Struct):
    calibration: Annotated[Measurement, Meta(extra={"unit": "mV"})]
    reading: Annotated[Measurement, Meta(extra={"unit": "V"})]
    alert: ThresholdAlert | None = None
"""


def test_recursive_structure_emits_one_column_per_occurrence():
    with temp_module(_RECURSIVE_CODE) as mod:
        table = compile_unit_table(
            mod.DeviceEvent,
            overrides={"alert.reading": "uV"},
            factors=FACTORS,
        )
        paths = [column.path for column in table.columns]
        # The nested cycle adds no duplicate occurrence columns.
        assert paths == [
            "$.calibration",
            "$.reading",
            "$.alert.reading",
        ]
        alert_column = table.by_path()["$.alert.reading"]
        assert alert_column.rule == OVERRIDE
        assert alert_column.contract_unit == "V"


def test_override_scopes_through_recursion_and_restores_at_parent():
    with temp_module(_RECURSIVE_CODE) as mod:
        table = compile_unit_table(
            mod.DeviceEvent,
            overrides={"alert.reading": "uV"},
            factors=FACTORS,
        )
        row = mod.DeviceEvent(
            calibration=mod.Measurement(1000.0),
            reading=mod.Measurement(2.0),
            alert=mod.ThresholdAlert(
                mod.Measurement(3.0),
                mod.ThresholdAlert(mod.Measurement(4.0), None),
            ),
        )
        out = table.convert(row)

        # Parent occurrences keep their own contract scopes.
        assert out.calibration.value == 1.0
        assert out.reading.value == 2.0
        # The override takes effect on entry and again on recursive re-entry.
        assert out.alert.reading.value == 0.000003
        assert out.alert.nested.reading.value == 0.000004
        assert out.alert.nested.nested is None


def test_recursive_contract_scope_without_override():
    with temp_module(_RECURSIVE_CODE) as mod:
        table = compile_unit_table(mod.DeviceEvent, factors=FACTORS)
        row = mod.DeviceEvent(
            calibration=mod.Measurement(1.0),
            reading=mod.Measurement(2.0),
            alert=mod.ThresholdAlert(
                mod.Measurement(3.0),
                mod.ThresholdAlert(mod.Measurement(4.0)),
            ),
        )
        out = table.convert(row)
        assert out.alert.reading.value == 3.0
        assert out.alert.nested.reading.value == 4.0


_SIBLING_CODE = """
from typing import Annotated
import msgspec
from msgspec import Meta

class Measurement(msgspec.Struct):
    value: float

class Alert(msgspec.Struct):
    reading: Annotated[Measurement, Meta(extra={"unit": "V"})]
    reference: Annotated[Measurement, Meta(extra={"unit": "mV"})]
    child: "Alert | None" = None

class DeviceEvent(msgspec.Struct):
    alert: Alert | None = None
"""


def test_override_does_not_leak_to_sibling_same_type_in_recursion():
    with temp_module(_SIBLING_CODE) as mod:
        table = compile_unit_table(
            mod.DeviceEvent,
            overrides={"alert.reading": "uV"},
            factors=FACTORS,
        )
        reading = table.by_path()["$.alert.reading"]
        reference = table.by_path()["$.alert.reference"]
        assert reading.rule == OVERRIDE
        assert reading.unit == "uV"
        # The sibling occurrence of the same type keeps its contract scope.
        assert reference.rule == CONTRACT
        assert reference.unit == "mV"

        row = mod.DeviceEvent(
            mod.Alert(
                mod.Measurement(3.0),
                mod.Measurement(5000.0),
                mod.Alert(mod.Measurement(4.0), mod.Measurement(6000.0)),
            )
        )
        out = table.convert(row)
        assert out.alert.reading.value == 0.000003
        assert out.alert.reference.value == 5.0
        # Recursive re-entry repeats both scopes.
        assert out.alert.child.reading.value == 0.000004
        assert out.alert.child.reference.value == 6.0


def test_convert_handles_collections_of_unit_carrying_types():
    class Batch(msgspec.Struct):
        readings: list[Annotated[Measurement, Meta(extra={"unit": "mV"})]]

    table = compile_unit_table(Batch, factors=FACTORS)
    column = table.by_path()["$.readings[*]"]
    assert column.rule == CONTRACT
    out = table.convert(Batch([Measurement(1000.0), Measurement(2000.0)]))
    assert [item.value for item in out.readings] == [1.0, 2.0]


def test_default_unit_key_constant():
    assert DEFAULT_UNIT_KEY == "unit"
