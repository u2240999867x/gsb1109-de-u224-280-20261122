"""Tests for the stateful correction mapper."""

from __future__ import annotations

import decimal

import pytest

from singer_sdk.correction import (
    CorrectionEvent,
    CorrectionMapper,
    CorrectionOp,
    CorrectionPolicy,
    CorrectionTarget,
)
from singer_sdk.exceptions import (
    CorrectionMappingError,
    MappingError,
    SingerSDKError,
    StaleRevisionError,
)
from singer_sdk.singerlib.json import deserialize_json, serialize_json


def make_record(
    txn_id: str,
    revision: int,
    day: str,
    amount: float | str,
    *,
    filtered: bool = False,
) -> dict:
    """Build a payment transaction record."""
    return {
        "txn_id": txn_id,
        "revision": revision,
        "posting_date": day,
        "amount": amount,
        "filtered": filtered,
    }


@pytest.fixture
def policy() -> CorrectionPolicy:
    """A policy keyed by transaction id, with a simple retention flag."""
    return CorrectionPolicy(
        business_id_properties="txn_id",
        revision_property="revision",
        day_property="posting_date",
        amount_property="amount",
        retain=lambda record: not record.get("filtered", False),
    )


@pytest.fixture
def mapper(policy: CorrectionPolicy) -> CorrectionMapper:
    """A correction mapper using the fixture policy."""
    return CorrectionMapper(policy)


def test_issubclass_hierarchy() -> None:
    """Correction errors follow the SDK exception hierarchy."""
    assert issubclass(CorrectionMappingError, MappingError)
    assert issubclass(CorrectionMappingError, SingerSDKError)
    assert issubclass(StaleRevisionError, CorrectionMappingError)


def test_first_revision_emits_detail_upsert_and_daily_add(
    mapper: CorrectionMapper,
) -> None:
    """A first retained version upserts the detail row and adds the amount."""
    events = mapper.map_record(
        "payments",
        make_record("txn-1", 1, "2026-10-01", 90),
    )

    assert len(events) == 2

    detail, daily = events
    assert detail.target is CorrectionTarget.DETAIL
    assert detail.op is CorrectionOp.UPSERT
    assert detail.business_id == "txn-1"
    assert detail.record["amount"] == 90

    assert daily.target is CorrectionTarget.DAILY
    assert daily.day == "2026-10-01"
    assert daily.amount_delta == decimal.Decimal(90)
    assert daily.daily_total == decimal.Decimal(90)
    assert mapper.daily_totals == {
        ("payments", "2026-10-01"): decimal.Decimal(90),
    }


def test_correction_moves_day_and_filters_revision(mapper: CorrectionMapper) -> None:
    """Headline scenario: revoke day 1, and do not re-add on day 2 when filtered."""
    mapper.map_record("payments", make_record("txn-1", 1, "2026-10-01", 90))

    events = mapper.map_record(
        "payments",
        make_record("txn-1", 2, "2026-10-02", 90, filtered=True),
    )

    detail, daily = events

    # The old detail row is removed; nothing new is inserted.
    assert detail.target is CorrectionTarget.DETAIL
    assert detail.op is CorrectionOp.DELETE
    assert detail.record["txn_id"] == "txn-1"
    assert detail.record["posting_date"] == "2026-10-01"
    assert all(event.op is not CorrectionOp.UPSERT for event in events)

    # Day 1 loses the full 90; the filtered day-2 attribution creates no event.
    assert daily.target is CorrectionTarget.DAILY
    assert daily.day == "2026-10-01"
    assert daily.amount_delta == decimal.Decimal(-90)
    assert daily.daily_total == decimal.Decimal(0)
    assert ("payments", "2026-10-02") not in mapper.daily_totals
    assert mapper.applied_revision("payments", "txn-1") == 2


def test_correction_between_two_retained_days(mapper: CorrectionMapper) -> None:
    """A retained day move revokes the old day and adds the new one."""
    mapper.map_record("payments", make_record("txn-1", 1, "2026-10-01", 90))

    events = mapper.map_record(
        "payments",
        make_record("txn-1", 2, "2026-10-02", 90),
    )

    assert [
        event.op for event in events if event.target is CorrectionTarget.DETAIL
    ] == [
        CorrectionOp.DELETE,
        CorrectionOp.UPSERT,
    ]
    daily = [event for event in events if event.target is CorrectionTarget.DAILY]
    assert [(event.day, event.amount_delta) for event in daily] == [
        ("2026-10-01", decimal.Decimal(-90)),
        ("2026-10-02", decimal.Decimal(90)),
    ]
    assert mapper.daily_totals == {
        ("payments", "2026-10-01"): decimal.Decimal(0),
        ("payments", "2026-10-02"): decimal.Decimal(90),
    }


def test_amount_change_on_same_day_emits_single_net_delta(
    mapper: CorrectionMapper,
) -> None:
    """Same-day amount corrections collapse into one signed delta."""
    mapper.map_record("payments", make_record("txn-1", 1, "2026-10-01", 90))

    events = mapper.map_record(
        "payments",
        make_record("txn-1", 2, "2026-10-01", 60),
    )

    daily = [event for event in events if event.target is CorrectionTarget.DAILY]
    assert len(daily) == 1
    assert daily[0].amount_delta == decimal.Decimal(-30)
    assert daily[0].daily_total == decimal.Decimal(60)


def test_multiple_records_share_a_daily_total(mapper: CorrectionMapper) -> None:
    """The daily total accumulates across distinct business identities."""
    mapper.map_record("payments", make_record("txn-1", 1, "2026-10-01", 90))
    mapper.map_record("payments", make_record("txn-2", 1, "2026-10-01", 10))

    assert mapper.daily_totals[("payments", "2026-10-01")] == decimal.Decimal(100)

    events = mapper.map_record(
        "payments",
        make_record("txn-1", 2, "2026-10-01", 40),
    )
    daily = next(event for event in events if event.target is CorrectionTarget.DAILY)
    assert daily.amount_delta == decimal.Decimal(-50)
    assert daily.daily_total == decimal.Decimal(50)


def test_stale_revision_arriving_late_is_ignored(mapper: CorrectionMapper) -> None:
    """Ordering follows revision numbers, not arrival time."""
    # Newer revision arrives first.
    mapper.map_record("payments", make_record("txn-1", 2, "2026-10-02", 90))
    totals_before = mapper.daily_totals

    # Late arrival of the older revision changes nothing.
    events = mapper.map_record(
        "payments",
        make_record("txn-1", 1, "2026-10-01", 90),
    )

    assert events == []
    assert mapper.daily_totals == totals_before
    assert mapper.stale_revision_count == 1
    assert mapper.applied_revision("payments", "txn-1") == 2


def test_replayed_revision_cannot_restore_amount(mapper: CorrectionMapper) -> None:
    """Replaying an old revision after consumption never brings money back."""
    mapper.map_record("payments", make_record("txn-1", 1, "2026-10-01", 90))
    mapper.map_record(
        "payments",
        make_record("txn-1", 2, "2026-10-02", 90, filtered=True),
    )
    totals_after_correction = mapper.daily_totals

    replay_events = mapper.map_record(
        "payments",
        make_record("txn-1", 1, "2026-10-01", 90),
    )

    assert replay_events == []
    assert mapper.daily_totals == totals_after_correction
    assert mapper.daily_totals[("payments", "2026-10-01")] == decimal.Decimal(0)


def test_duplicate_revision_is_idempotent(mapper: CorrectionMapper) -> None:
    """Re-delivering the same revision emits nothing."""
    record = make_record("txn-1", 1, "2026-10-01", 90)
    first = mapper.map_record("payments", record)
    second = mapper.map_record("payments", record)

    assert len(first) == 2
    assert second == []
    assert mapper.stale_revision_count == 1


def test_stale_revision_can_raise(policy: CorrectionPolicy) -> None:
    """The mapper can be configured to fail loudly on stale revisions."""
    mapper = CorrectionMapper(policy, on_stale_revision="raise")
    mapper.map_record("payments", make_record("txn-1", 2, "2026-10-02", 90))

    with pytest.raises(StaleRevisionError) as exc_info:
        mapper.map_record("payments", make_record("txn-1", 1, "2026-10-01", 90))

    assert exc_info.value.stream == "payments"
    assert exc_info.value.business_id == "txn-1"
    assert exc_info.value.incoming_revision == 1
    assert exc_info.value.applied_revision == 2


def test_invalid_stale_revision_setting(policy: CorrectionPolicy) -> None:
    """Unknown stale-revision strategies are rejected at construction."""
    with pytest.raises(CorrectionMappingError, match="on_stale_revision"):
        CorrectionMapper(policy, on_stale_revision="explode")  # type: ignore[arg-type]


def test_filtered_first_revision_emits_nothing(mapper: CorrectionMapper) -> None:
    """A filtered first version establishes the baseline without effects."""
    events = mapper.map_record(
        "payments",
        make_record("txn-1", 1, "2026-10-01", 90, filtered=True),
    )

    assert events == []
    assert mapper.daily_totals == {}

    # The later retained version only adds; there is no prior row to delete.
    events = mapper.map_record(
        "payments",
        make_record("txn-1", 2, "2026-10-01", 90),
    )
    detail_ops = [
        event.op for event in events if event.target is CorrectionTarget.DETAIL
    ]
    assert detail_ops == [CorrectionOp.UPSERT]


def test_retained_after_filtered_readds(mapper: CorrectionMapper) -> None:
    """A retained revision following a filtered one inserts and adds anew."""
    mapper.map_record("payments", make_record("txn-1", 1, "2026-10-01", 90))
    mapper.map_record(
        "payments",
        make_record("txn-1", 2, "2026-10-02", 90, filtered=True),
    )

    events = mapper.map_record(
        "payments",
        make_record("txn-1", 3, "2026-10-03", 30),
    )

    detail, daily = events
    assert detail.op is CorrectionOp.UPSERT
    assert detail.record["posting_date"] == "2026-10-03"
    assert daily.day == "2026-10-03"
    assert daily.amount_delta == decimal.Decimal(30)
    assert daily.daily_total == decimal.Decimal(30)


def test_streams_are_keyed_independently(policy: CorrectionPolicy) -> None:
    """The same business id on two streams does not collide."""
    mapper = CorrectionMapper(policy)
    mapper.map_record("payments", make_record("txn-1", 2, "2026-10-01", 90))

    events = mapper.map_record(
        "refunds",
        make_record("txn-1", 1, "2026-10-01", 90),
    )

    assert len(events) == 2
    assert mapper.applied_revision("payments", "txn-1") == 2
    assert mapper.applied_revision("refunds", "txn-1") == 1


def test_to_record_messages(policy: CorrectionPolicy) -> None:
    """Events convert to Singer record messages for both tables."""
    mapper = CorrectionMapper(
        policy,
        detail_stream_suffix="_details",
        daily_stream_suffix="_daily_totals",
    )
    mapper.map_record("payments", make_record("txn-1", 1, "2026-10-01", 90))
    correction = mapper.map_record(
        "payments",
        make_record("txn-1", 2, "2026-10-02", 90, filtered=True),
    )

    messages = list(mapper.to_record_messages(correction))
    detail_message, daily_message = messages

    assert detail_message.stream == "payments_details"
    assert detail_message.record["_sdc_deleted_at"] is not None
    assert detail_message.record["txn_id"] == "txn-1"

    assert daily_message.stream == "payments_daily_totals"
    assert daily_message.record == {
        "posting_date": "2026-10-01",
        "amount": decimal.Decimal(0),
        "amount_delta": decimal.Decimal(-90),
    }


def test_map_record_message_accepts_singer_dict(mapper: CorrectionMapper) -> None:
    """The mapper accepts a raw Singer RECORD message dictionary."""
    message_dict = {
        "type": "RECORD",
        "stream": "payments",
        "record": make_record("txn-1", 1, "2026-10-01", 90),
    }

    events = mapper.map_record_message(message_dict)

    assert len(events) == 2
    assert all(event.stream == "payments" for event in events)


def test_snapshot_and_restore_roundtrip(policy: CorrectionPolicy) -> None:
    """State survives a snapshot/restore cycle, including decimals."""
    mapper = CorrectionMapper(policy)
    mapper.map_record("payments", make_record("txn-1", 1, "2026-10-01", "90.50"))
    mapper.map_record(
        "payments",
        make_record("txn-1", 2, "2026-10-02", "90.50", filtered=True),
    )

    state = mapper.snapshot()
    # The snapshot must travel through the SDK's JSON message writer.
    restored_state = deserialize_json(serialize_json(state))

    restored = CorrectionMapper(policy)
    restored.restore(restored_state)

    assert restored.applied_revision("payments", "txn-1") == 2
    assert restored.daily_totals == mapper.daily_totals

    # Restored baselines still reject stale revisions.
    replayed = restored.map_record(
        "payments",
        make_record("txn-1", 1, "2026-10-01", "90.50"),
    )
    assert replayed == []

    # And accept the next revision, computing deltas against restored state.
    events = restored.map_record(
        "payments",
        make_record("txn-1", 3, "2026-10-03", "10.00"),
    )
    daily = next(event for event in events if event.target is CorrectionTarget.DAILY)
    assert daily.amount_delta == decimal.Decimal("10.00")


def test_restore_rejects_unknown_format(policy: CorrectionPolicy) -> None:
    """Unknown snapshot formats fail rather than silently resetting state."""
    mapper = CorrectionMapper(policy)
    with pytest.raises(CorrectionMappingError, match="format version"):
        mapper.restore({"format_version": 999, "entries": [], "daily": []})


def test_composite_business_id() -> None:
    """Composite identities are supported and survive snapshots."""
    composite_policy = CorrectionPolicy(
        business_id_properties=("txn_id", "posting_date"),
        revision_property="revision",
        day_property="posting_date",
        amount_property="amount",
    )
    mapper = CorrectionMapper(composite_policy)
    mapper.map_record(
        "payments",
        make_record("txn-1", 1, "2026-10-01", 90),
    )

    assert mapper.applied_revision("payments", ("txn-1", "2026-10-01")) == 1

    restored = CorrectionMapper(composite_policy)
    restored.restore(mapper.snapshot())
    assert restored.applied_revision("payments", ("txn-1", "2026-10-01")) == 1


def test_missing_business_identity_raises(mapper: CorrectionMapper) -> None:
    """Records without an identity cannot be corrected safely."""
    record = make_record("txn-1", 1, "2026-10-01", 90)
    record.pop("txn_id")

    with pytest.raises(CorrectionMappingError, match="business identity"):
        mapper.map_record("payments", record)


def test_non_integer_revision_raises(mapper: CorrectionMapper) -> None:
    """Revisions must be comparable integers."""
    record = make_record("txn-1", "1", "2026-10-01", 90)  # type: ignore[arg-type]

    with pytest.raises(CorrectionMappingError, match="integer revision"):
        mapper.map_record("payments", record)


def test_invalid_amount_raises(mapper: CorrectionMapper) -> None:
    """Non-numeric amounts fail explicitly."""
    record = make_record("txn-1", 1, "2026-10-01", "lots")

    with pytest.raises(CorrectionMappingError, match="invalid amount"):
        mapper.map_record("payments", record)


def test_amountless_record_does_not_touch_daily_table(
    mapper: CorrectionMapper,
) -> None:
    """Records without an amount only drive detail corrections."""
    record = make_record("txn-1", 1, "2026-10-01", 90)
    record.pop("amount")

    events = mapper.map_record("payments", record)

    assert [event.target for event in events] == [CorrectionTarget.DETAIL]
    assert mapper.daily_totals == {}


def test_event_factory_helpers() -> None:
    """The event factories fill in the expected coordinates."""
    detail = CorrectionEvent.detail(
        op=CorrectionOp.UPSERT,
        stream="payments",
        business_id="txn-1",
        revision=1,
        record={"txn_id": "txn-1"},
    )
    daily = CorrectionEvent.daily(
        stream="payments",
        business_id="txn-1",
        revision=1,
        day="2026-10-01",
        amount_delta=decimal.Decimal(90),
        daily_total=decimal.Decimal(90),
    )

    assert detail.target is CorrectionTarget.DETAIL
    assert daily.target is CorrectionTarget.DAILY
    assert daily.op is None
