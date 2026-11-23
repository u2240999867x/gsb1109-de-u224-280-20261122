import pytest

pytest.importorskip("sqlalchemy")

from pydantic import BaseModel

from redbird.batch import BatchItem, BatchOutcome, BatchSynchronizer, SQLiteLedger
from redbird.repos import SQLRepo, RESTRepo


class Item(BaseModel):
    id: str
    name: str


class ItemWithValue(BaseModel):
    id: str
    value: str


def test_ledger_from_sql_repo(tmp_path):
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///{}".format(tmp_path / "repo.db"))
    repo = SQLRepo(
        model=Item,
        engine=engine,
        table="items",
        id_field="id",
        if_missing="create",
    )
    ledger = SQLiteLedger.from_repo(repo)
    ledger.create_target_table({"id": str, "name": str})
    ledger.ensure_schema()

    entries = ledger.apply("B1", "O1", [BatchItem("a", {"name": "A"})])
    assert entries[0].local_status == "applied"
    assert ledger.read_current("a") == {"id": "a", "name": "A"}

    # The repository still reads the same row.
    assert repo.filter_by(id="a").all() == [Item(id="a", name="A")]


def test_rest_store_from_repo():
    pytest.importorskip("requests")
    repo = RESTRepo(model=dict, url="http://example.test/items", id_field="id")
    from redbird.batch import RESTBatchStore

    store = RESTBatchStore.from_repo(repo)
    assert store.url == "http://example.test/items"
    assert store.id_field == "id"
    assert store.item_url("abc/123") == "http://example.test/items/abc%2F123"


def test_outcome_lookup(tmp_path):
    class Bridge:
        def __init__(self):
            self.state = {}

        def read_marker(self, resource_id):
            from redbird.batch import BatchResourceNotFound, RemoteMarker
            if resource_id not in self.state:
                raise BatchResourceNotFound(resource_id)
            data = self.state[resource_id]
            return RemoteMarker(
                resource_id=resource_id,
                version=data["version"],
                batch_id=data["batch_id"],
                operation_id=data["operation_id"],
                raw=dict(data),
            )

        def mark(self, resource_id, batch_id, operation_id, expected_version):
            data = self.state[resource_id]
            assert data["version"] == expected_version
            data["batch_id"] = batch_id
            data["operation_id"] = operation_id
            data["version"] += 1
            return self.read_marker(resource_id)

        def clear_marker(self, resource_id, batch_id, operation_id, expected_version):
            data = self.state[resource_id]
            assert data["version"] == expected_version
            data["batch_id"] = None
            data["operation_id"] = None
            data["version"] += 1
            return self.read_marker(resource_id)

    ledger = SQLiteLedger(
        conn_string="sqlite:///{}".format(tmp_path / "outcomes.db"),
        target_table="items",
    )
    ledger.create_target_table({"id": str, "value": str})
    ledger.ensure_schema()
    bridge = Bridge()
    bridge.state["a"] = {"id": "a", "version": 1, "batch_id": None, "operation_id": None}
    sync = BatchSynchronizer(ledger=ledger, source=bridge)

    sync.record("B1", "O1", [BatchItem("a", {"value": "x"})])
    sync.resume("B1", "O1")
    assert sync.outcome("B1", "a", operation_id="O1") == BatchOutcome.RETAINED.value

    sync.withdraw("B1", "O1")
    assert sync.outcome("B1", "a", operation_id="O1") == BatchOutcome.WITHDRAWN.value
    assert sync.outcome("B1", "missing", operation_id="O1") is None
