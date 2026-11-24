import pandas as pd
import pytest

from streamz import Stream
from streamz.dashboard import (
    CANCEL,
    DELTA,
    SNAPSHOT,
    UPSERT,
    DashboardView,
    InvalidBatch,
    InventoryDashboard,
    InventoryEngine,
    Message,
)

COLUMNS = ["order_id", "region", "fee", "op"]


def make_batch(rows):
    return pd.DataFrame(rows, columns=COLUMNS)


def entry(message, region):
    return message.entries[region]


def delta_versions(message, region):
    kind, payload = entry(message, region)
    assert kind == DELTA
    return [version for version, _ in payload]


def delta_values(message, region):
    kind, payload = entry(message, region)
    assert kind == DELTA
    return [value for _, value in payload]


def snapshot(message, region):
    kind, payload = entry(message, region)
    assert kind == SNAPSHOT
    return payload


@pytest.fixture
def engine():
    return InventoryEngine(retention=2)


@pytest.fixture
def dashboard():
    return InventoryDashboard(
        InventoryEngine(retention=2, start_version=39))


def seed_v40(dashboard):
    return dashboard.submit(make_batch([
        ("E1", "east", 10, UPSERT),
        ("E2", "east", 10, UPSERT),
        ("W0", "west", None, UPSERT),
        ("N1", "north", 99, UPSERT),
    ]))


# ---------------------------------------------------------------------------
# Engine semantics
# ---------------------------------------------------------------------------

def test_successful_batch_commits_one_contiguous_version(engine):
    version = engine.submit(make_batch([
        ("a", "east", 1, UPSERT),
        ("b", "east", 2, UPSERT),
    ]))
    assert version.version == 1
    assert engine.current_version == 1
    state = engine.region_state("east")
    assert state.count == 2
    assert state.min_fee == 1
    assert state.registered is True


def test_rows_apply_in_order_within_batch(engine):
    engine.submit(make_batch([("a", "east", 5, UPSERT)]))
    version = engine.submit(make_batch([
        ("a", "north", 7, UPSERT),
        ("a", "south", 7, UPSERT),
    ]))
    assert "a" not in engine.region_state("east").orders
    assert "a" not in engine.region_state("north").orders
    assert engine.region_state("south").orders == {"a": 7}
    # The pass-through region lost its only order and is marked removed.
    assert engine.region_state("east").registered is False
    assert version.changes["east"] == (0, None, False)
    assert version.changes["south"] == (1, 7, True)


def test_upsert_then_cancel_within_same_batch(engine):
    version = engine.submit(make_batch([
        ("a", "east", 5, UPSERT),
        ("a", "east", None, CANCEL),
    ]))
    state = engine.region_state("east")
    assert state.count == 0
    assert state.registered is False
    assert version.changes["east"] == (0, None, False)


def test_only_changed_regions_are_published(engine):
    engine.submit(make_batch([("a", "east", 5, UPSERT)]))
    version = engine.submit(make_batch([("a", "east", 5, UPSERT)]))
    assert version.changes == {}
    # A version is still produced and occupies retention.
    assert engine.current_version == 2
    assert [item.version for item in engine._versions] == [1, 2]


def test_min_fee_ignores_empty_fees_but_count_is_kept(engine):
    engine.submit(make_batch([
        ("a", "east", None, UPSERT),
        ("b", "east", None, UPSERT),
    ]))
    state = engine.region_state("east")
    assert state.count == 2
    assert state.min_fee is None
    assert state.registered is True

    version = engine.submit(make_batch([("b", "east", 4, UPSERT)]))
    assert version.changes["east"] == (2, 4, True)


def test_last_cancel_marks_region_removed_but_keeps_count(engine):
    engine.submit(make_batch([("a", "east", None, UPSERT)]))
    version = engine.submit(make_batch([("a", "east", None, CANCEL)]))
    state = engine.region_state("east")
    assert state.registered is False
    assert state.count == 0
    assert version.changes["east"] == (0, None, False)


def test_re_registering_a_removed_region_publishes_again(engine):
    engine.submit(make_batch([("a", "east", 5, UPSERT)]))
    engine.submit(make_batch([("a", "east", None, CANCEL)]))
    version = engine.submit(make_batch([("b", "east", 9, UPSERT)]))
    assert version.changes["east"] == (1, 9, True)
    assert engine.region_state("east").registered is True


def test_invalid_batch_is_rejected_wholesale(engine):
    engine.submit(make_batch([("a", "east", 5, UPSERT)]))
    with pytest.raises(InvalidBatch):
        engine.submit(make_batch([
            ("b", "east", 6, UPSERT),
            ("a", "east", None, CANCEL),
            ("ghost", "east", None, CANCEL),
        ]))
    # No version, no state change.
    assert engine.current_version == 1
    assert engine.region_state("east").orders == {"a": 5}


@pytest.mark.parametrize("rows", [
    [(None, "east", 1, UPSERT)],
    [("a", None, 1, UPSERT)],
    [("a", "east", 1, "delete")],
    [("a", "east", None, CANCEL)],
])
def test_invalid_rows_raise(engine, rows):
    with pytest.raises(InvalidBatch):
        engine.submit(make_batch(rows))
    assert engine.current_version == 0


def test_retention_evicts_old_versions_and_records_watermark(engine):
    for index in range(4):
        engine.submit(make_batch([("o%d" % index, "east", index, UPSERT)]))
    assert engine.current_version == 4
    assert engine.oldest_retained_version == 3
    assert engine.has_evicted_gap("east", 1)
    assert not engine.has_evicted_gap("east", 2)
    assert not engine.has_evicted_gap("west", 0)


def test_unrelated_batches_still_consume_retention_slots(engine):
    engine.submit(make_batch([("a", "east", 1, UPSERT)]))
    engine.submit(make_batch([("b", "west", 2, UPSERT)]))
    engine.submit(make_batch([("c", "west", 3, UPSERT)]))
    # The east-only version 1 was evicted although later batches never touched
    # the dashboard's east region.
    assert engine.oldest_retained_version == 2
    assert engine.has_evicted_gap("east", 0)


def test_version_stream_emits_committed_versions(engine):
    emitted = engine.version_stream.sink_to_list()
    version = engine.submit(make_batch([("a", "east", 1, UPSERT)]))
    assert emitted == [version]


# ---------------------------------------------------------------------------
# Subscription, greeting and acknowledgement
# ---------------------------------------------------------------------------

def test_greeting_cuts_all_regions_to_one_committed_version(dashboard):
    seed_v40(dashboard)
    view = dashboard.connect(
        ["east", "west"],
        held_versions={"east": 40, "west": 40},
        connection_id="p1",
    )
    message = dashboard.greet(view)
    assert message.version == 40
    assert set(message.entries) == {"east", "west"}
    assert delta_versions(message, "east") == []
    assert delta_versions(message, "west") == []
    assert dashboard.acknowledge(view, message) is True


def test_greeting_gives_full_state_without_footing(dashboard):
    seed_v40(dashboard)
    view = dashboard.connect(["east"], connection_id="p1")
    message = dashboard.greet(view)
    payload = snapshot(message, "east")
    assert payload == {
        "registered": True,
        "count": 2,
        "min_fee": 10,
        "orders": {"E1": 10, "E2": 10},
    }


def test_greeting_marks_never_seen_region_as_absent(dashboard):
    seed_v40(dashboard)
    view = dashboard.connect(["atlantis"], connection_id="p1")
    message = dashboard.greet(view)
    payload = snapshot(message, "atlantis")
    assert payload["registered"] is False
    assert payload["count"] == 0
    assert payload["min_fee"] is None
    assert payload["orders"] == {}


def test_greeting_snapshots_region_whose_changes_were_evicted(dashboard):
    seed_v40(dashboard)
    # Two east changes push v40's content out of retention.
    dashboard.submit(make_batch([("E1", "east", 8, UPSERT)]))
    dashboard.submit(make_batch([("E1", "east", 6, UPSERT)]))
    dashboard.submit(make_batch([("E1", "east", 4, UPSERT)]))
    assert dashboard.engine.has_evicted_gap("east", 40)

    view = dashboard.connect(["east"], held_versions={"east": 40},
                             connection_id="p1")
    message = dashboard.greet(view)
    payload = snapshot(message, "east")
    assert payload["registered"] is True
    assert payload["count"] == 2
    assert payload["min_fee"] == 4


def test_greeting_fills_missing_deltas_and_keeps_caught_up_regions(dashboard):
    seed_v40(dashboard)
    dashboard.submit(make_batch([("W1", "west", 12, UPSERT)]))
    view = dashboard.connect(
        ["east", "west"],
        held_versions={"east": 41, "west": 40},
        connection_id="p1",
    )
    message = dashboard.greet(view)
    assert message.version == 41
    assert delta_versions(message, "east") == []
    assert delta_values(message, "west") == [(2, 12, True)]


def test_subscription_scope_drops_regions_on_reconnect(dashboard):
    seed_v40(dashboard)
    old = dashboard.connect(["east", "west"], connection_id="p1")
    dashboard.greet(old)
    dashboard.disconnect(old)
    new = dashboard.connect(["east"], connection_id="p2")
    message = dashboard.greet(new)
    assert set(message.entries) == {"east"}
    assert not old.active


def test_two_messages_at_same_version_are_distinct(dashboard):
    seed_v40(dashboard)
    first = dashboard.connect(["east"], held_versions={"east": 40},
                              connection_id="p1")
    message1 = dashboard.greet(first)
    dashboard.disconnect(first)
    second = dashboard.connect(["east"], held_versions={"east": 40},
                               connection_id="p2")
    message2 = dashboard.greet(second)
    assert message1.version == message2.version == 40
    assert message1.message_id != message2.message_id
    assert dashboard.acknowledge(first, message1) is False
    assert dashboard.acknowledge(second, message2) is True


# ---------------------------------------------------------------------------
# Version delivery, retention replacement and stale receipts
# ---------------------------------------------------------------------------

def test_delivers_one_message_with_all_affected_regions(dashboard):
    seed_v40(dashboard)
    view = dashboard.connect(["east", "north"],
                             held_versions={"east": 40, "north": 40},
                             connection_id="p1")
    dashboard.greet(view)
    dashboard.acknowledge(view, view.pending)

    dashboard.submit(make_batch([
        ("E1", "east", 8, UPSERT),
        ("N1", "north", 98, UPSERT),
    ]))
    message = view.pending
    assert message.version == 41
    assert set(message.entries) == {"east", "north"}
    assert delta_values(message, "east") == [(2, 8, True)]
    assert delta_values(message, "north") == [(1, 98, True)]


def test_batches_keep_coming_without_acknowledgement(dashboard):
    seed_v40(dashboard)
    view = dashboard.connect(["east"], held_versions={"east": 40},
                             connection_id="p1")
    dashboard.greet(view)
    dashboard.acknowledge(view, view.pending)

    dashboard.submit(make_batch([("E1", "east", 8, UPSERT)]))
    first = view.pending
    dashboard.submit(make_batch([("E1", "east", 6, UPSERT)]))
    dashboard.submit(make_batch([("E1", "east", 4, UPSERT)]))
    assert dashboard.engine.current_version == 43
    assert view.pending is first


def test_delivered_message_survives_eviction_without_replacement(dashboard):
    seed_v40(dashboard)
    view = dashboard.connect(["east", "west"],
                             held_versions={"east": 40, "west": 40},
                             connection_id="p1")
    dashboard.greet(view)
    dashboard.acknowledge(view, view.pending)

    dashboard.submit(make_batch([("E1", "east", 8, UPSERT)]))
    delivered = view.pending
    # Two west-only batches evict v41, whose east change was already delivered.
    dashboard.submit(make_batch([("W1", "west", 12, UPSERT)]))
    dashboard.submit(make_batch([("W2", "west", 13, UPSERT)]))
    assert dashboard.engine.oldest_retained_version == 42
    assert view.pending is delivered


def test_replacement_is_emitted_when_needed_change_is_evicted(dashboard):
    seed_v40(dashboard)
    view = dashboard.connect(["east", "west"],
                             held_versions={"east": 40, "west": 40},
                             connection_id="p1")
    dashboard.greet(view)
    dashboard.acknowledge(view, view.pending)

    dashboard.submit(make_batch([("E1", "east", 8, UPSERT)]))
    v41_message = view.pending
    dashboard.submit(make_batch([
        ("E1", "south", 7, UPSERT),
        ("E2", "east", None, CANCEL),
    ]))
    dashboard.submit(make_batch([("W1", "west", 12, UPSERT)]))
    assert view.pending is v41_message
    dashboard.submit(make_batch([("N1", "north", 98, UPSERT)]))

    replacement = view.pending
    assert replacement is not v41_message
    assert replacement.version == 44
    assert snapshot(replacement, "east")["registered"] is False
    assert delta_values(replacement, "west") == [(2, 12, True)]
    # The stale receipt cannot confirm the replacement.
    assert dashboard.acknowledge(view, v41_message) is False
    assert view.pending is replacement
    assert dashboard.acknowledge(view, replacement) is True


def test_acknowledgement_releases_queued_versions(dashboard):
    seed_v40(dashboard)
    view = dashboard.connect(["east"], held_versions={"east": 40},
                             connection_id="p1")
    dashboard.greet(view)
    dashboard.acknowledge(view, view.pending)

    dashboard.submit(make_batch([("E1", "east", 8, UPSERT)]))
    first = view.pending
    dashboard.submit(make_batch([("E2", "east", 3, UPSERT)]))
    assert view.pending is first
    assert dashboard.acknowledge(view, first) is True

    follow_up = view.pending
    assert follow_up.version == 42
    assert delta_versions(follow_up, "east") == [42]
    assert delta_values(follow_up, "east") == [(2, 3, True)]


def test_stale_connection_receipt_never_releases_new_message(dashboard):
    seed_v40(dashboard)
    old = dashboard.connect(["east"], held_versions={"east": 40},
                            connection_id="p1")
    old_message = dashboard.greet(old)
    dashboard.acknowledge(old, old_message)

    dashboard.submit(make_batch([("E1", "east", 8, UPSERT)]))
    dashboard.disconnect(old)
    new = dashboard.connect(["east"], held_versions={"east": 40},
                            connection_id="p2")
    new_message = dashboard.greet(new)
    assert dashboard.acknowledge(old, old.pending) is False
    assert new.pending is new_message
    assert dashboard.acknowledge(new, new_message) is True


# ---------------------------------------------------------------------------
# Full narrative from the specification
# ---------------------------------------------------------------------------

def test_full_dashboard_narrative(dashboard):
    seed_v40(dashboard)
    page1 = dashboard.connect(["east", "west"],
                              held_versions={"east": 40, "west": 40},
                              connection_id="page1")
    dashboard.acknowledge(page1, dashboard.greet(page1))

    dashboard.submit(make_batch([("E1", "east", 8, UPSERT)]))
    v41 = page1.pending
    assert v41.version == 41
    assert delta_values(v41, "east") == [(2, 8, True)]

    dashboard.submit(make_batch([
        ("E1", "north", 7, UPSERT),
        ("E1", "south", 7, UPSERT),
        ("E2", "east", None, CANCEL),
    ]))
    dashboard.submit(make_batch([("W1", "west", 12, UPSERT)]))
    assert dashboard.engine.oldest_retained_version == 42
    assert page1.pending is v41

    dashboard.submit(make_batch([("N1", "north", 98, UPSERT)]))
    replacement = page1.pending
    assert replacement.version == 44
    assert snapshot(replacement, "east")["registered"] is False
    assert delta_values(replacement, "west") == [(2, 12, True)]

    dashboard.disconnect(page1)
    page2 = dashboard.connect(
        ["east", "west", "north"],
        held_versions={"east": 40, "west": 43, "north": 43},
        connection_id="page2",
    )
    greeting = dashboard.greet(page2)
    assert greeting.version == 44
    assert snapshot(greeting, "east")["registered"] is False
    assert delta_values(greeting, "north") == [(1, 98, True)]
    assert delta_versions(greeting, "west") == []

    dashboard.submit(make_batch([("W1", "west", None, CANCEL)]))
    assert page2.pending is greeting
    assert dashboard.acknowledge(page1, v41) is False
    assert dashboard.acknowledge(page2, replacement) is False
    assert page2.pending is greeting

    assert dashboard.acknowledge(page2, greeting) is True
    v45 = page2.pending
    assert v45.version == 45
    assert delta_values(v45, "west") == [(1, None, True)]
    assert delta_versions(v45, "east") == []
    assert delta_versions(v45, "north") == []
    assert dashboard.acknowledge(page2, v45) is True
    assert page2.pending is None


# ---------------------------------------------------------------------------
# Stream integration and coexistence with plain groupby
# ---------------------------------------------------------------------------

def test_attach_feeds_pandas_micro_batches_through_a_stream():
    source = Stream()
    dashboard = InventoryDashboard(retention=2)
    dashboard.attach(source)
    source.emit(make_batch([("a", "east", 1, UPSERT)]))
    assert dashboard.engine.current_version == 1
    assert dashboard.engine.region_state("east").min_fee == 1

    with pytest.raises(InvalidBatch):
        source.emit(make_batch([("a", "east", None, CANCEL),
                                ("b", "east", None, CANCEL)]))
    assert dashboard.engine.current_version == 1


def test_append_only_groupby_still_works_alongside_dashboard():
    from streamz.dataframe import DataFrame

    example = pd.DataFrame(columns=COLUMNS)
    source = Stream()
    sdf = DataFrame(source, example=example)
    grouped = sdf.groupby("region").fee.sum()
    group_output = grouped.stream.sink_to_list()

    dashboard = InventoryDashboard(retention=2)
    dashboard.attach(source)

    source.emit(make_batch([
        ("a", "east", 1, UPSERT),
        ("b", "east", 2, UPSERT),
        ("c", "west", 4, UPSERT),
    ]))

    assert dashboard.engine.region_state("east").count == 2
    assert len(group_output) == 1
    result = group_output[-1]
    assert result.loc["east"] == 3
    assert result.loc["west"] == 4


def test_message_is_hashable_and_view_types():
    engine = InventoryEngine()
    view = DashboardView(engine, ["east"], connection_id="p1")
    assert isinstance(view.regions, set)
    message = view.build_message()
    assert isinstance(message, Message)
    assert message.connection_id == "p1"


def test_invalid_batch_does_not_notify_connected_views(dashboard):
    seed_v40(dashboard)
    view = dashboard.connect(["east"], held_versions={"east": 40},
                             connection_id="p1")
    dashboard.acknowledge(view, dashboard.greet(view))
    delivered_before = len(view.messages)

    with pytest.raises(InvalidBatch):
        dashboard.submit(make_batch([("ghost", "east", None, CANCEL)]))

    assert dashboard.engine.current_version == 40
    assert view.pending is None
    assert len(view.messages) == delivered_before


def test_greeting_before_any_batch_marks_every_region_absent():
    dashboard = InventoryDashboard(retention=2)
    view = dashboard.connect(["east", "west"], connection_id="p1")
    message = dashboard.greet(view)
    assert message.version == 0
    for region in ("east", "west"):
        payload = snapshot(message, region)
        assert payload["registered"] is False
        assert payload["count"] == 0
        assert payload["min_fee"] is None


def test_only_regions_without_history_get_snapshots_on_greeting(dashboard):
    seed_v40(dashboard)
    # East accumulates changes whose v40 footing leaves retention.
    dashboard.submit(make_batch([("E1", "east", 8, UPSERT)]))
    dashboard.submit(make_batch([("E1", "east", 6, UPSERT)]))
    dashboard.submit(make_batch([("W1", "west", 12, UPSERT)]))

    view = dashboard.connect(
        ["east", "west"],
        held_versions={"east": 40, "west": 42},
        connection_id="p1",
    )
    message = dashboard.greet(view)
    assert message.version == 43
    assert snapshot(message, "east")["min_fee"] == 6
    assert delta_values(message, "west") == [(2, 12, True)]
