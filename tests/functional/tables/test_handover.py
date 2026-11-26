"""Point lookups across a table-partition handover."""

import asyncio

import pytest

from faust.exceptions import TablePartitionNotReady
from faust.types import TP

P0 = 0
P1 = 1
OFFSET_BALANCE = 40
OFFSET_DELETED = 41
OFFSET_REOPENED = 42


@pytest.fixture()
def table(app):
    return app.Table("balances", default=int)


def changelog_tp(table, partition):
    return TP(table.changelog_topic.get_topic_name(), partition)


def apply(table, partition, key, value, offset):
    event = table_for_event(table, partition, key, value, offset)
    table.apply_changelog_batch([event])
    return event


def table_for_event(table, partition, key, value, offset):
    from tests.helpers import new_event

    return new_event(
        table.app,
        key,
        value,
        topic=table.changelog_topic.get_topic_name(),
        partition=partition,
        offset=offset,
    )


def assign(table, assigned, newly_assigned, generation_id):
    table.begin_recovery(
        generation_id,
        {changelog_tp(table, partition) for partition in assigned},
        {changelog_tp(table, partition) for partition in newly_assigned},
    )


def complete(table, active, generation_id):
    table.complete_recovery(
        generation_id,
        {changelog_tp(table, partition) for partition in active},
    )


def test_balance_lookup_returns_zero_for_reopened_account(table):
    assign(table, [P0, P1], [P0, P1], 1)
    apply(table, P0, "K", 7, OFFSET_BALANCE)
    apply(table, P1, "J", 12, OFFSET_BALANCE)
    complete(table, [P0, P1], 1)

    lookup = table.lookup("K")
    assert lookup.value == 7
    assert (lookup.partition, lookup.offset) == (P0, OFFSET_BALANCE)

    apply(table, P0, "K", None, OFFSET_DELETED)
    deleted = table.lookup("K")
    assert deleted.value is None
    assert (deleted.partition, deleted.offset) == (P0, OFFSET_DELETED)

    apply(table, P0, "K", 0, OFFSET_REOPENED)
    reopened = table.lookup("K")
    # Zero is a real balance, not a tombstone.
    assert reopened.value == 0
    assert (reopened.partition, reopened.offset) == (P0, OFFSET_REOPENED)


def test_partition_being_taken_over_reports_not_ready(table):
    assign(table, [P0, P1], [P0, P1], 1)
    apply(table, P0, "K", 7, OFFSET_BALANCE)
    apply(table, P1, "J", 12, OFFSET_BALANCE)
    complete(table, [P0, P1], 1)

    # p0 is handed to another worker; p1 stays with this one.
    assign(table, [P1], set(), 2)
    with pytest.raises(TablePartitionNotReady):
        table.lookup("K")

    # The continuing partition does not wait for p0's takeover.
    lookup = table.lookup("J")
    assert lookup.value == 12
    assert (lookup.partition, lookup.offset) == (P1, OFFSET_BALANCE)


def test_lookup_held_across_takeover_resolves_at_new_position(table):
    assign(table, [P0, P1], [P0, P1], 1)
    apply(table, P0, "K", 7, OFFSET_BALANCE)
    complete(table, [P0, P1], 1)

    held = table.lookup("K")
    assert held.value == 7

    # p0 is handed away, then handed back; the held response must not be
    # served from the old position while the partition is mid-takeover.
    assign(table, [P1], set(), 2)
    with pytest.raises(TablePartitionNotReady):
        held.value

    assign(table, [P0, P1], [P0], 3)
    with pytest.raises(TablePartitionNotReady):
        held.value

    apply(table, P0, "K", None, OFFSET_DELETED)
    apply(table, P0, "K", 0, OFFSET_REOPENED)
    complete(table, [P0, P1], 3)

    assert held.value == 0
    assert (held.partition, held.offset) == (P0, OFFSET_REOPENED)


def test_batched_and_split_replay_have_the_same_result(table):
    assign(table, [P0, P1], [P0, P1], 1)
    complete(table, [P0, P1], 1)
    for first, second in [
        (
            table_for_event(table, P0, "K", 0, OFFSET_REOPENED),
            table_for_event(table, P0, "K", None, OFFSET_DELETED),
        ),
        (
            table_for_event(table, P0, "K", None, OFFSET_DELETED),
            table_for_event(table, P0, "K", 0, OFFSET_REOPENED),
        ),
    ]:
        table.data.reset_state()
        table.apply_changelog_batch([first, second])
        result = table.lookup("K")
        assert result.value == 0
        assert (result.partition, result.offset) == (P0, OFFSET_REOPENED)


@pytest.mark.asyncio
async def test_stale_recovery_callback_does_not_prune_new_partition(app):
    table = app.Table("balances", default=int)
    assign(table, [P0], [P0], 1)
    apply(table, P0, "K", 7, OFFSET_BALANCE)
    complete(table, [P0], 1)

    # A recovery callback for generation 1 is still blocked when p0 is
    # handed back in generation 2 and recovered.
    assign(table, [P0], [P0], 2)
    apply(table, P0, "K", None, OFFSET_DELETED)
    apply(table, P0, "K", 0, OFFSET_REOPENED)
    complete(table, [P0], 2)

    await table.on_recovery_completed_for_generation(
        1,
        {changelog_tp(table, P0)},
        set(),
    )
    # The stale completion was ignored: neither the data nor the readiness
    # of the current generation changed.
    assert table.lookup("K").value == 0
    assert table.partition_ready(P0)


@pytest.mark.asyncio
async def test_recover_callback_does_not_block_replay(app):
    release = asyncio.Event()
    started = asyncio.Event()

    async def on_recover():
        started.set()
        await release.wait()

    table = app.Table("balances", default=int, on_recover=on_recover)
    assign(table, [P0], [P0], 1)
    complete(table, [P0], 1)
    table.schedule_recover_callbacks(1)
    await started.wait()

    # The recovery loop is free: a new generation can recover p0 while the
    # old callback is still blocked.
    assign(table, [P0], [P0], 2)
    apply(table, P0, "K", 0, OFFSET_REOPENED)
    complete(table, [P0], 2)
    assert table.lookup("K").value == 0

    release.set()
    await asyncio.sleep(0)


def test_tombstoned_key_is_missing_even_with_default(table):
    assign(table, [P0], [P0], 1)
    complete(table, [P0], 1)
    apply(table, P0, "K", 7, OFFSET_BALANCE)
    assert table["K"] == 7
    apply(table, P0, "K", None, OFFSET_DELETED)
    with pytest.raises(KeyError):
        table["K"]
    apply(table, P0, "K", 0, OFFSET_REOPENED)
    assert table["K"] == 0
