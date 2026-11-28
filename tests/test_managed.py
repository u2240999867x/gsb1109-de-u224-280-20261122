# SPDX-License-Identifier: MIT

import attr
import pytest


@pytest.fixture(autouse=True)
def clear_managed_rejection_reasons():
    attr.managed.clear_rejection_reasons()


def box_count(value):
    return int(value[2])


def loose(value):
    if isinstance(value, str):
        if value.startswith("散"):
            return int(value[1:])
        if value.endswith("箱"):
            return box_count(value) * 6
    return value


def strict(value):
    if isinstance(value, str):
        if value.startswith("散"):
            return int(value[1:])
        if value.endswith("箱"):
            return box_count(value) * 8
    return value


def total_at_most(limit):
    def validator(instance, field, value):
        if sum(value) > limit:
            raise ValueError(f"total {sum(value)} exceeds {limit}")

    return validator


def matching_details(instance, field, value):
    if value != attr.managed.current_value(instance.receiving):
        raise ValueError("receiving and review candidates differ")


@attr.s
class ReceivingOrder:
    receiving = attr.ib(**attr.managed.field(converter=loose))
    review = attr.ib(
        validator=matching_details, **attr.managed.field(converter=loose)
    )


@attr.s
class AuditOrder:
    details = attr.ib(
        validator=total_at_most(15), **attr.managed.field(converter=strict)
    )


def test_managed_scan_and_rebind_scenario():
    source = attr.managed.ManagedList([4, 5])
    target = attr.managed.ManagedList([16])
    receiving_order = ReceivingOrder(source, source)
    audit_order = AuditOrder(source)

    assert receiving_order.receiving is source
    assert receiving_order.review is source
    assert audit_order.details is source

    attr.managed.scan(receiving_order, "receiving", "散6", 1)

    assert source == [4, 6]
    assert receiving_order.receiving == [4, 6]
    assert receiving_order.review == [4, 6]
    assert audit_order.details == [4, 6]

    with pytest.raises(attr.managed.ManagedRejectionError):
        audit_order.details = target
    assert audit_order.details is source
    assert source == [4, 6]
    assert target == [16]
    assert (
        attr.managed.rejection_reasons(
            field="details", raw_value=target, index=None
        )[0].error.args[0]
        == "total 16 exceeds 15"
    )
    assert not target._bindings

    attr.managed.clear_rejection_reasons()
    with pytest.raises(attr.managed.ManagedRejectionError):
        attr.managed.scan(audit_order, "details", "散12", 1)
    assert source == [4, 6]
    assert attr.managed.rejection_reasons(
        field="details", raw_value="散12", index=1
    )

    attr.managed.clear_rejection_reasons()
    with pytest.raises(attr.managed.ManagedRejectionError) as exc_info:
        attr.managed.scan(receiving_order, "receiving", "乙货2箱", 1)

    conflict = exc_info.value.reasons[0].error
    assert isinstance(conflict, attr.managed.InconsistentManagedValueError)
    assert conflict.values == {
        "receiving": 12,
        "review": 12,
        "details": 16,
    }
    assert source == [4, 6]
    assert target == [16]

    target[:] = [7]
    audit_order.details = target
    assert audit_order.details is target
    assert target == [7]

    attr.managed.scan(receiving_order, "receiving", "乙货2箱", 1)
    assert source == [4, 12]
    assert receiving_order.receiving == [4, 12]
    assert receiving_order.review == [4, 12]
    assert audit_order.details == [7]

    with pytest.raises(attr.managed.ManagedRejectionError):
        attr.managed.scan(audit_order, "details", "散16", 0)
    assert target == [7]


def test_slice_split_preserves_confirmed_values():
    seen = []

    def recording_converter(value):
        seen.append(value)
        return loose(value)

    @attr.s
    class Order:
        details = attr.ib(**attr.managed.field(converter=recording_converter))

    source = attr.managed.ManagedList([4, 6])
    order = Order(source)

    attr.managed.scan(
        order,
        "details",
        [attr.managed.confirmed(4), "散2", "散4"],
        slice(None),
    )

    assert source == [4, 2, 4]
    assert order.details is source
    assert seen == ["散2", "散4"]


def test_direct_slice_assignment_uses_field_rules():
    source = attr.managed.ManagedList([4, 6])
    order = AuditOrder(source)

    with pytest.raises(attr.managed.ManagedRejectionError):
        source[1] = "散12"

    assert source == [4, 6]
    assert order.details is source
    assert attr.managed.rejection_reasons(
        field="details", raw_value="散12", index=1
    )


def test_direct_slice_assignment_reports_field_interpretations():
    source = attr.managed.ManagedList([4, 6])
    receiving_order = ReceivingOrder(source, source)
    audit_order = AuditOrder(source)

    with pytest.raises(attr.managed.ManagedRejectionError) as exc_info:
        source[1] = "乙货2箱"

    conflict = exc_info.value.reasons[0].error
    assert isinstance(conflict, attr.managed.InconsistentManagedValueError)
    assert conflict.values == {
        "receiving": 12,
        "review": 12,
        "details": 16,
    }
    assert source == [4, 6]
    assert receiving_order.receiving is source
    assert audit_order.details is source


def test_same_named_fields_on_different_classes_keep_separate_bindings():
    @attr.s
    class First:
        details = attr.ib(**attr.managed.field(converter=loose))

    @attr.s
    class Second:
        details = attr.ib(**attr.managed.field(converter=strict))

    source = attr.managed.ManagedList([1, 2])
    first = First(source)
    second = Second(source)

    with pytest.raises(attr.managed.ManagedRejectionError) as exc_info:
        source[0] = "乙货2箱"

    conflict = exc_info.value.reasons[0].error
    assert isinstance(conflict, attr.managed.InconsistentManagedValueError)
    assert conflict.values == {
        "First.details": 12,
        "Second.details": 16,
    }
    assert len(exc_info.value.reasons) == 2
    assert source == [1, 2]
    assert first.details is source
    assert second.details is source


def test_validators_observe_one_candidate_until_commit():
    observed = []

    def record_receiving(instance, field, value):
        observed.append(("receiving", list(value)))

    def record_review(instance, field, value):
        observed.append(("review", list(value)))
        if sum(value) > 10:
            raise ValueError("rejected")

    @attr.s
    class Order:
        receiving = attr.ib(
            validator=record_receiving, **attr.managed.field(converter=loose)
        )
        review = attr.ib(
            validator=record_review, **attr.managed.field(converter=loose)
        )

    source = attr.managed.ManagedList([4, 6])
    order = Order(source, source)
    observed.clear()

    with pytest.raises(attr.managed.ManagedRejectionError):
        attr.managed.scan(order, "receiving", "散7", 1)

    assert observed == [
        ("receiving", [4, 7]),
        ("review", [4, 7]),
    ]
    assert source == [4, 6]


def test_uninvolved_fields_keep_reading_committed_values():
    @attr.s
    class Order:
        managed_field = attr.ib(**attr.managed.field(converter=loose))
        plain_field = attr.ib()

    source = attr.managed.ManagedList([1])
    order = Order(source, "ready")

    attr.managed.scan(order, "managed_field", "散2", 0)

    assert order.managed_field == [2]
    assert order.plain_field == "ready"


def test_managed_list_aliases_reference_same_type():
    assert attr.managed.ManagedSequence is attr.managed.ManagedList
    assert attr.managed.ManagedDetails is attr.managed.ManagedList
