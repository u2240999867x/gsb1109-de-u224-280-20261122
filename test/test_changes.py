import pytest
from sqlalchemy import text

import dataset
from dataset.util import DatasetError


@pytest.fixture(scope="function")
def db():
    db = dataset.connect("sqlite:///:memory:", track_changes=True)
    yield db
    db.executable.rollback()
    for table in db.tables:
        db[table].drop()
    db.close()


def test_changes_require_tracking():
    plain = dataset.connect("sqlite:///:memory:")
    try:
        plain["widgets"].insert({"name": "a"})
        with pytest.raises(DatasetError):
            plain.changes()
        with pytest.raises(DatasetError):
            plain["widgets"].changes()
    finally:
        plain.close()


def test_insert_update_delete_are_tracked(db):
    tbl = db["stations"]
    row_id = tbl.insert({"name": "alpha", "status": "new"})
    tbl.update({"id": row_id, "status": "active"}, ["id"])
    tbl.delete(id=row_id)

    changes = db.changes()
    assert [c["op"] for c in changes] == ["insert", "update", "delete"]
    inserted, updated, deleted = changes
    assert inserted["table"] == "stations"
    assert inserted["pk"] == {"id": row_id}
    assert inserted["row"]["status"] == "new"
    assert updated["row"]["status"] == "active"
    assert deleted["pk"] == {"id": row_id}
    assert deleted["row"]["status"] == "active"


def test_values_are_read_back_from_database(db):
    # Server-side defaults and triggers must be reflected in the changes.
    db.executable.execute(
        text(
            "CREATE TABLE devices ("
            "id INTEGER PRIMARY KEY, "
            "status TEXT, "
            "created TEXT DEFAULT 'auto-ts')"
        )
    )
    db.executable.execute(
        text(
            "CREATE TRIGGER devices_status AFTER INSERT ON devices "
            "BEGIN UPDATE devices SET status = 'triggered' WHERE id = NEW.id; "
            "END"
        )
    )
    db.commit()
    devices = db.load_table("devices")
    row_id = devices.insert({"status": "original"})

    (change,) = db.changes()
    assert change["op"] == "insert"
    assert change["pk"] == {"id": row_id}
    assert change["row"]["status"] == "triggered"
    assert change["row"]["created"] == "auto-ts"


def test_committed_changes_only_visible_after_commit(db):
    stations = db["stations"]
    events = db["events"]
    event_id = events.insert({"kind": "deploy"})
    start = db.changes()
    assert len(start) == 1  # the committed events insert

    with db:
        station_id = stations.insert({"name": "s1", "status": "new"})
        # Inner group of changes, rolled back (savepoint semantics).
        nested = db.executable.begin_nested()
        stations.update({"id": station_id, "status": "wrong"}, ["id"])
        nested.rollback()
        # The outer transaction fixes the status and removes a temporary
        # record created within the same transaction.
        stations.update({"id": station_id, "status": "active"}, ["id"])
        tmp_id = stations.insert({"name": "tmp", "status": "new"})
        stations.delete(id=tmp_id)
        events.delete(id=event_id)

        # Not committed yet: no new changes are visible.
        assert len(db.changes(since=start.bookmark)) == 0
        assert len(stations.changes(since=start.bookmark)) == 0

    changes = db.changes(since=start.bookmark)
    assert len(changes) == 2

    station_changes = stations.changes(since=start.bookmark)
    assert len(station_changes) == 1
    (station,) = station_changes
    assert station["op"] == "insert"
    assert station["pk"] == {"id": station_id}
    assert station["row"]["status"] == "active"
    assert station["row"]["name"] == "s1"

    event_changes = events.changes(since=start.bookmark)
    assert len(event_changes) == 1
    (event,) = event_changes
    assert event["op"] == "delete"
    assert event["pk"] == {"id": event_id}
    assert event["row"]["kind"] == "deploy"


def test_nested_dataset_transaction_rollback(db):
    stations = db["stations"]
    start = db.changes()
    db.begin()
    station_id = stations.insert({"name": "s1", "status": "new"})
    db.begin()
    stations.update({"id": station_id, "status": "wrong"}, ["id"])
    db.rollback()
    stations.update({"id": station_id, "status": "active"}, ["id"])
    db.commit()

    (change,) = stations.changes(since=start)
    assert change["op"] == "insert"
    assert change["row"]["status"] == "active"


def test_bookmark_can_be_handed_to_next_reader(db):
    tbl = db["stations"]
    first_id = tbl.insert({"name": "one"})
    confirmed = db.changes()
    assert [c["pk"] for c in confirmed] == [{"id": first_id}]

    # The next auditor resumes from the confirmed position.
    second_id = tbl.insert({"name": "two"})
    follow_up = db.changes(since=confirmed.bookmark)
    assert [c["pk"] for c in follow_up] == [{"id": second_id}]

    # A ChangeSet itself can be passed as the position.
    assert len(db.changes(since=follow_up)) == 0
    assert len(db.changes()) == 2


def test_outer_rollback_leaves_no_changes(db):
    tbl = db["stations"]
    start = db.changes()
    db.begin()
    tbl.insert({"name": "ghost"})
    db.rollback()
    assert len(db.changes(since=start)) == 0
    assert len(tbl.changes(since=start)) == 0

    # Tracking still works after the rollback.
    row_id = tbl.insert({"name": "real"})
    (change,) = tbl.changes(since=start)
    assert change["op"] == "insert"
    assert change["pk"] == {"id": row_id}


def test_update_and_delete_collapse_per_commit(db):
    tbl = db["stations"]
    row_id = tbl.insert({"name": "s1", "status": "new"})
    start = db.changes()
    with db:
        tbl.update({"id": row_id, "status": "mid"}, ["id"])
        tbl.update({"id": row_id, "status": "final"}, ["id"])
    confirmed = tbl.changes(since=start)
    (change,) = confirmed
    assert change["op"] == "update"
    assert change["row"]["status"] == "final"

    with db:
        tbl.delete(id=row_id)
        tbl.insert({"id": row_id, "name": "s1", "status": "reborn"})
    (change,) = tbl.changes(since=confirmed.bookmark)
    assert change["op"] == "update"
    assert change["row"]["status"] == "reborn"
