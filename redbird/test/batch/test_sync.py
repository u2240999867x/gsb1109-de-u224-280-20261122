import json

import pytest

pytest.importorskip("sqlalchemy")

from redbird.batch import (
    BatchItem,
    BatchOutcome,
    BatchSynchronizer,
    BatchResourceNotFound,
    BatchTransportError,
    BatchVersionConflict,
    RemoteMarker,
    RESTBatchStore,
    SQLiteLedger,
)


class FakeSource:
    """Small in-process REST resource store with versions and markers."""

    def __init__(self, resources):
        self.resources = {}
        for resource in resources:
            data = dict(resource)
            data.setdefault("version", 1)
            data.setdefault("batch_id", None)
            data.setdefault("operation_id", None)
            self.resources[data["id"]] = data
        self.requests = []
        self.fail_after_mark = set()
        self.fail_after_clear = set()

    def read(self, resource_id):
        self.requests.append(("get", resource_id))
        data = self.resources.get(resource_id)
        if data is None:
            return None
        return dict(data)

    def mark(self, resource_id, batch_id, operation_id, expected_version):
        self.requests.append(("mark", resource_id))
        data = self.resources.get(resource_id)
        if data is None:
            raise AssertionError("missing resource")
        if expected_version is not None and data["version"] != expected_version:
            raise AssertionError("version mismatch")
        data["batch_id"] = batch_id
        data["operation_id"] = operation_id
        data["version"] += 1
        if resource_id in self.fail_after_mark:
            self.fail_after_mark.remove(resource_id)
            raise BatchTransportError("response lost after mark")
        return dict(data)

    def clear(self, resource_id, batch_id, operation_id, expected_version):
        self.requests.append(("clear", resource_id))
        data = self.resources.get(resource_id)
        if data is None:
            raise AssertionError("missing resource")
        if expected_version is not None and data["version"] != expected_version:
            raise AssertionError("version mismatch")
        data["batch_id"] = None
        data["operation_id"] = None
        data["version"] += 1
        if resource_id in self.fail_after_clear:
            self.fail_after_clear.remove(resource_id)
            raise BatchTransportError("response lost after clear")
        return dict(data)

    def set_owner(self, resource_id, batch_id, operation_id):
        data = self.resources[resource_id]
        data["batch_id"] = batch_id
        data["operation_id"] = operation_id
        data["version"] += 1


class Bridge:
    """Adapt FakeSource to the interface used by BatchSynchronizer."""

    def __init__(self, source):
        self.source = source

    def _marker(self, resource_id, data):
        return RemoteMarker(
            resource_id=resource_id,
            version=data["version"],
            batch_id=data["batch_id"],
            operation_id=data["operation_id"],
            raw=data,
        )

    def read_marker(self, resource_id):
        data = self.source.read(resource_id)
        if data is None:
            raise BatchResourceNotFound(resource_id)
        return self._marker(resource_id, data)

    def mark(self, resource_id, batch_id, operation_id, expected_version):
        data = self.source.mark(resource_id, batch_id, operation_id, expected_version)
        return self._marker(resource_id, data)

    def clear_marker(self, resource_id, batch_id, operation_id, expected_version):
        data = self.source.clear(resource_id, batch_id, operation_id, expected_version)
        return self._marker(resource_id, data)


def make_ledger(tmp_path):
    ledger = SQLiteLedger(
        conn_string="sqlite:///{}".format(tmp_path / "sync.db"),
        target_table="items",
    )
    ledger.create_target_table({"id": str, "name": str, "sync_value": str})
    ledger.ensure_schema()
    return ledger


def make_sync(tmp_path, resources):
    ledger = make_ledger(tmp_path)
    source = FakeSource(resources)
    return BatchSynchronizer(ledger=ledger, source=Bridge(source)), ledger, source


def reopen_sync(tmp_path, source):
    """Simulate a process restart with the same database and remote state."""
    ledger = SQLiteLedger(
        conn_string="sqlite:///{}".format(tmp_path / "sync.db"),
        target_table="items",
    )
    ledger.ensure_schema()
    remote = FakeSource([])
    remote.resources = {key: dict(value) for key, value in source.resources.items()}
    return BatchSynchronizer(ledger=ledger, source=Bridge(remote)), ledger, remote


def test_rest_marker_written_once_and_resumed(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [
        {"id": "a"}, {"id": "b"}, {"id": "c"},
    ])
    sync.record("B1", "O1", [
        BatchItem("a", {"name": "A", "sync_value": "1"}),
        BatchItem("b", {"name": "B", "sync_value": "1"}),
        BatchItem("c", {"name": "C", "sync_value": "1"}),
    ])

    # Simulate a stop while marking c: the request reached the source but the
    # caller never saw the response.
    source.fail_after_mark.add("c")
    report = sync.resume("B1", "O1")
    states = {entry.resource_id: entry.outcome for entry in report.entries}
    assert states == {
        "a": BatchOutcome.RETAINED.value,
        "b": BatchOutcome.RETAINED.value,
        "c": BatchOutcome.PENDING.value,
    }
    assert source.resources["c"]["batch_id"] == "B1"

    report = sync.resume("B1", "O1")
    assert {entry.outcome for entry in report.entries} == {BatchOutcome.RETAINED.value}
    assert [call for call in source.requests if call == ("mark", "a")] == [("mark", "a")]
    assert [call for call in source.requests if call == ("mark", "b")] == [("mark", "b")]
    assert [call for call in source.requests if call == ("mark", "c")] == [("mark", "c")]


def test_shutdown_window_scenario(tmp_path):
    """The exact mixed scenario from the request.

    - row 3's REST marker was written but the response was lost,
    - row 2's local sync value was corrected manually,
    - resource 1's REST marker was taken over by another batch.
    """
    sync, ledger, source = make_sync(tmp_path, [
        {"id": "a"}, {"id": "b"}, {"id": "c"},
    ])
    sync.record("B1", "O1", [
        BatchItem("a", {"name": "A", "sync_value": "1"}),
        BatchItem("b", {"name": "B", "sync_value": "1"}),
        BatchItem("c", {"name": "C", "sync_value": "1"}),
    ])

    source.fail_after_mark.add("c")
    sync.resume("B1", "O1")

    import sqlalchemy
    with ledger.engine.begin() as conn:
        conn.execute(sqlalchemy.text("UPDATE items SET sync_value = 'manual' WHERE id = 'b'"))
    source.set_owner("a", "B2", "O2")

    restarted, restarted_ledger, restarted_source = reopen_sync(tmp_path, source)
    report = restarted.resume("B1")

    states = {entry.resource_id: entry for entry in report.entries}
    assert states["a"].outcome == BatchOutcome.CONFLICT.value
    assert states["a"].remote_batch == "B2"
    assert states["a"].remote_operation == "O2"
    assert states["b"].outcome == BatchOutcome.RETAINED.value
    assert restarted_ledger.read_current("b")["sync_value"] == "manual"
    assert states["c"].outcome == BatchOutcome.RETAINED.value

    # c was already marked before the shutdown: reading it is enough.
    assert ("mark", "c") not in restarted_source.requests
    assert restarted_source.resources["c"]["batch_id"] == "B1"


def test_lost_rest_response_is_not_remarked(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    sync.record("B1", "O1", [BatchItem("a", {"name": "A", "sync_value": "1"})])
    source.fail_after_mark.add("a")

    report = sync.resume("B1", "O1")
    assert report.entries[0].outcome == BatchOutcome.PENDING.value
    assert source.resources["a"]["batch_id"] == "B1"

    report = sync.resume("B1", "O1")
    assert report.entries[0].outcome == BatchOutcome.RETAINED.value
    assert source.resources["a"]["version"] == 2
    assert [call for call in source.requests if call == ("mark", "a")] == [("mark", "a")]


def test_manual_local_value_is_preserved_and_marked(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "b"}])
    sync.record("B1", "O1", [BatchItem("b", {"name": "old", "sync_value": "1"})])
    sync.resume("B1", "O1")

    import sqlalchemy
    with ledger.engine.begin() as conn:
        conn.execute(sqlalchemy.text("UPDATE items SET sync_value = 'manual' WHERE id = 'b'"))

    report = sync.resume("B1", "O1")
    entry = report.entries[0]
    assert entry.outcome == BatchOutcome.RETAINED.value
    assert entry.local_status == "preserved"
    assert ledger.read_current("b")["sync_value"] == "manual"
    assert source.resources["b"]["batch_id"] == "B1"


def test_resource_claimed_by_other_batch_is_conflict(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    sync.record("B1", "O1", [BatchItem("a", {"name": "A", "sync_value": "1"})])
    source.set_owner("a", "B2", "O2")

    report = sync.resume("B1", "O1")
    entry = report.entries[0]
    assert entry.outcome == BatchOutcome.CONFLICT.value
    assert entry.remote_batch == "B2"
    assert entry.remote_operation == "O2"
    assert source.resources["a"]["batch_id"] == "B2"


def test_withdraw_owned_rows_and_keep_conflicts(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}, {"id": "b"}, {"id": "c"}])
    sync.record("B1", "O1", [
        BatchItem("a", {"name": "A", "sync_value": "1"}),
        BatchItem("b", {"name": "B", "sync_value": "1"}),
        BatchItem("c", {"name": "C", "sync_value": "1"}),
    ])
    sync.resume("B1", "O1")

    # b is locally superseded and c is taken over remotely.
    import sqlalchemy
    with ledger.engine.begin() as conn:
        conn.execute(sqlalchemy.text("UPDATE items SET sync_value = 'manual' WHERE id = 'b'"))
    source.set_owner("c", "B2", "O2")

    report = sync.withdraw("B1", "O1")
    assert ledger.read_current("a") is None
    assert ledger.read_current("b")["sync_value"] == "manual"
    assert ledger.read_current("c")["sync_value"] == "1"
    states = {entry.resource_id: entry.outcome for entry in report.entries}
    assert states == {
        "a": BatchOutcome.WITHDRAWN.value,
        "b": BatchOutcome.CONFLICT.value,
        "c": BatchOutcome.CONFLICT.value,
    }
    assert source.resources["a"]["batch_id"] is None
    assert source.resources["b"]["batch_id"] == "B1"
    assert source.resources["c"]["batch_id"] == "B2"


def test_withdraw_interrupted_after_rest_clear_can_resume(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    sync.record("B1", "O1", [BatchItem("a", {"name": "A", "sync_value": "1"})])
    sync.resume("B1", "O1")

    source.fail_after_clear.add("a")
    report = sync.withdraw("B1", "O1")
    assert report.entries[0].outcome == BatchOutcome.PENDING.value
    assert ledger.read_current("a")["sync_value"] == "1"
    assert source.resources["a"]["batch_id"] is None

    report = sync.withdraw("B1", "O1")
    assert report.entries[0].outcome == BatchOutcome.WITHDRAWN.value
    assert ledger.read_current("a") is None


def test_withdraw_interrupted_before_local_revert_can_resume(tmp_path, monkeypatch):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    sync.record("B1", "O1", [BatchItem("a", {"name": "A", "sync_value": "1"})])
    sync.resume("B1", "O1")

    original = ledger.withdraw_local
    def stop(*args, **kwargs):
        raise RuntimeError("process stopped before local revert")
    monkeypatch.setattr(ledger, "withdraw_local", stop)

    with pytest.raises(RuntimeError):
        sync.withdraw("B1", "O1")
    assert ledger.read_current("a")["sync_value"] == "1"
    assert source.resources["a"]["batch_id"] is None

    # The operation now records withdrawal intent.  A plain resume must
    # continue withdrawing instead of re-marking the resource.
    monkeypatch.setattr(ledger, "withdraw_local", original)
    report = sync.resume("B1", "O1")
    assert report.entries[0].outcome == BatchOutcome.WITHDRAWN.value
    assert ledger.read_current("a") is None
    assert source.resources["a"]["batch_id"] is None


def test_resume_does_not_remark_after_interrupted_withdraw(tmp_path, monkeypatch):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    sync.record("B1", "O1", [BatchItem("a", {"name": "A", "sync_value": "1"})])
    sync.resume("B1", "O1")

    original = ledger.withdraw_local
    def stop(*args, **kwargs):
        raise RuntimeError("process stopped before local revert")
    monkeypatch.setattr(ledger, "withdraw_local", stop)

    with pytest.raises(RuntimeError):
        sync.withdraw("B1", "O1")
    assert ledger.entry("B1", "O1", "a").intent == "withdraw"

    monkeypatch.setattr(ledger, "withdraw_local", original)
    remote_before = dict(source.resources["a"])
    requests_before = list(source.requests)
    report = sync.resume("B1", "O1")
    assert report.entries[0].outcome == BatchOutcome.WITHDRAWN.value
    assert ("mark", "a") not in source.requests[len(requests_before):]
    assert source.resources["a"]["version"] == remote_before["version"]


def test_withdraw_keeps_manually_edited_created_row(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    sync.record("B1", "O1", [BatchItem("a", {"name": "A", "sync_value": "1"})])
    sync.resume("B1", "O1")

    import sqlalchemy
    with ledger.engine.begin() as conn:
        conn.execute(sqlalchemy.text("UPDATE items SET name = 'manual' WHERE id = 'a'"))

    report = sync.withdraw("B1", "O1")
    # The batch created the row but a person then edited it.  Deleting the row
    # would discard that edit, so both sides are left untouched.
    assert report.entries[0].outcome == BatchOutcome.CONFLICT.value
    current = ledger.read_current("a")
    assert current["name"] == "manual"
    assert current["sync_value"] == "1"
    assert source.resources["a"]["batch_id"] == "B1"


def test_withdraw_keeps_manual_edit_to_batch_field(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    import sqlalchemy
    with ledger.engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            "INSERT INTO items (id, name, sync_value) VALUES ('a', 'old', 'old')"
        ))
    sync.record("B1", "O1", [BatchItem("a", {"sync_value": "1"})])
    sync.resume("B1", "O1")

    with ledger.engine.begin() as conn:
        conn.execute(sqlalchemy.text("UPDATE items SET sync_value = 'manual' WHERE id = 'a'"))

    report = sync.withdraw("B1", "O1")
    assert report.entries[0].outcome == BatchOutcome.CONFLICT.value
    assert ledger.read_current("a")["sync_value"] == "manual"
    assert source.resources["a"]["batch_id"] == "B1"


def test_withdraw_restores_previous_value_with_unrelated_manual_edit(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    import sqlalchemy
    with ledger.engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            "INSERT INTO items (id, name, sync_value) VALUES ('a', 'old', 'old')"
        ))
    sync.record("B1", "O1", [BatchItem("a", {"sync_value": "1"})])
    sync.resume("B1", "O1")

    with ledger.engine.begin() as conn:
        conn.execute(sqlalchemy.text("UPDATE items SET name = 'manual' WHERE id = 'a'"))

    report = sync.withdraw("B1", "O1")
    assert report.entries[0].outcome == BatchOutcome.WITHDRAWN.value
    current = ledger.read_current("a")
    assert current["sync_value"] == "old"
    assert current["name"] == "manual"
    assert source.resources["a"]["batch_id"] is None


def test_withdraw_keeps_partially_manual_field(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    import sqlalchemy
    with ledger.engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            "INSERT INTO items (id, name, sync_value) VALUES ('a', 'old', 'old')"
        ))
    sync.record("B1", "O1", [BatchItem("a", {"name": "A", "sync_value": "1"})])
    sync.resume("B1", "O1")

    # name was manually restored to the old value, sync_value is still the
    # batch's value.  The restore must clear sync_value and keep name.
    with ledger.engine.begin() as conn:
        conn.execute(sqlalchemy.text("UPDATE items SET name = 'old' WHERE id = 'a'"))

    report = sync.withdraw("B1", "O1")
    assert report.entries[0].outcome == BatchOutcome.WITHDRAWN.value
    current = ledger.read_current("a")
    assert current["name"] == "old"
    assert current["sync_value"] == "old"
    assert source.resources["a"]["batch_id"] is None


def test_interrupted_before_rest_clear_resumes_withdrawal(tmp_path, monkeypatch):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    sync.record("B1", "O1", [BatchItem("a", {"name": "A", "sync_value": "1"})])
    sync.resume("B1", "O1")

    original = sync._clear_remote
    def stop(entry):
        raise RuntimeError("process stopped before REST clear")
    monkeypatch.setattr(sync, "_clear_remote", stop)

    with pytest.raises(RuntimeError):
        sync.withdraw("B1", "O1")
    entry = ledger.entry("B1", "O1", "a")
    assert entry.intent == "withdraw"
    assert entry.remote_status == "marked"
    assert source.resources["a"]["batch_id"] == "B1"
    assert ledger.read_current("a")["sync_value"] == "1"

    monkeypatch.setattr(sync, "_clear_remote", original)
    requests_before = list(source.requests)
    report = sync.resume("B1", "O1")
    assert report.entries[0].outcome == BatchOutcome.WITHDRAWN.value
    assert ledger.read_current("a") is None
    assert source.resources["a"]["batch_id"] is None
    assert ("mark", "a") not in source.requests[len(requests_before):]


def test_rerecord_does_not_overwrite_existing_row(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    sync.record("B1", "O1", [BatchItem("a", {"name": "A", "sync_value": "1"})])

    import sqlalchemy
    with ledger.engine.begin() as conn:
        conn.execute(sqlalchemy.text("UPDATE items SET sync_value = 'manual' WHERE id = 'a'"))

    # A retry of record() must not rewrite a row already registered by the
    # operation.
    sync.record("B1", "O1", [BatchItem("a", {"name": "A", "sync_value": "2"})])
    assert ledger.read_current("a")["sync_value"] == "manual"
    assert len(ledger.entries("B1", "O1")) == 1


def test_withdraw_before_any_mark_reverts_local_rows(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}])
    sync.record("B1", "O1", [BatchItem("a", {"name": "A", "sync_value": "1"})])

    # Simulate a stop before the first resume.
    report = sync.withdraw("B1", "O1")
    assert report.entries[0].outcome == BatchOutcome.WITHDRAWN.value
    assert ledger.read_current("a") is None
    assert ("clear", "a") not in source.requests
    assert source.resources["a"]["batch_id"] is None


def test_summary_reports_all_outcomes(tmp_path):
    sync, ledger, source = make_sync(tmp_path, [{"id": "a"}, {"id": "b"}, {"id": "c"}])
    sync.record("B1", "O1", [
        BatchItem("a", {"name": "A", "sync_value": "1"}),
        BatchItem("b", {"name": "B", "sync_value": "1"}),
        BatchItem("c", {"name": "C", "sync_value": "1"}),
    ])
    sync.resume("B1", "O1")
    source.set_owner("c", "B2", "O2")

    sync.withdraw("B1", "O1", resource_ids=["a"])
    sync.resume("B1", "O1", resource_ids=["c"])
    summary = sync.summary("B1", "O1")
    assert summary["withdrawn"] == 1
    assert summary["conflict"] == 1
    assert summary["retained"] == 1
    entries = sync.status("B1", "O1")
    assert entries["a"].outcome == "withdrawn"
    assert entries["b"].outcome == "retained"
    assert entries["c"].outcome == "conflict"


class _RESTState:
    def __init__(self, resources):
        self.resources = resources
        self.requests = []


def _rest_callbacks(state):
    def get(request):
        key = request.url.rsplit("/", 1)[-1]
        state.requests.append(("get", key))
        return (200, {"Content-Type": "application/json"}, json.dumps(state.resources[key]))

    def patch(request):
        key = request.url.rsplit("/", 1)[-1]
        state.requests.append(("patch", key))
        data = state.resources[key]
        expected = request.headers.get("If-Match")
        if expected is not None and expected != '"{}"'.format(data["version"]):
            return (412, {"Content-Type": "application/json"}, json.dumps({"detail": "version"}))
        payload = json.loads(request.body)
        data.update(payload)
        data["version"] += 1
        return (200, {"Content-Type": "application/json"}, json.dumps(data))

    return get, patch


def test_real_rest_conditional_requests():
    responses = pytest.importorskip("responses")
    state = _RESTState({"a": {"id": "a", "version": 1, "batch_id": None, "operation_id": None}})
    get, patch = _rest_callbacks(state)
    url = "http://example.test/items"
    with responses.RequestsMock() as rsps:
        rsps.add_callback(responses.GET, url + "/a", callback=get)
        rsps.add_callback(responses.PATCH, url + "/a", callback=patch)
        store = RESTBatchStore(url=url)
        marker = store.read_marker("a")
        assert marker.version == 1
        assert marker.present is False
        result = store.mark("a", "B1", "O1", expected_version=marker.version)
        assert result.batch_id == "B1"
        assert result.operation_id == "O1"
        assert result.version == 2
        result = store.clear_marker("a", "B1", "O1", expected_version=result.version)
        assert result.present is False
        assert result.version == 3


def test_real_rest_processed_flag_is_atomic():
    responses = pytest.importorskip("responses")
    state = _RESTState({
        "a": {"id": "a", "version": 1, "batch_id": None, "operation_id": None, "processed": False}
    })
    get, patch = _rest_callbacks(state)
    url = "http://example.test/items"
    with responses.RequestsMock() as rsps:
        rsps.add_callback(responses.GET, url + "/a", callback=get)
        rsps.add_callback(responses.PATCH, url + "/a", callback=patch)
        store = RESTBatchStore(url=url, processed_field="processed")
        marker = store.read_marker("a")
        assert marker.marked is False
        result = store.mark("a", "B1", "O1", expected_version=marker.version)
        assert result.marked is True
        assert result.batch_id == "B1"
        result = store.clear_marker("a", "B1", "O1", expected_version=result.version)
        assert result.marked is False
        assert result.present is False
        assert state.resources["a"]["batch_id"] is None


def test_real_rest_version_conflict_does_not_overwrite():
    responses = pytest.importorskip("responses")
    state = _RESTState({"a": {"id": "a", "version": 1, "batch_id": None, "operation_id": None}})
    get, patch = _rest_callbacks(state)
    url = "http://example.test/items"
    with responses.RequestsMock() as rsps:
        rsps.add_callback(responses.GET, url + "/a", callback=get)
        rsps.add_callback(responses.PATCH, url + "/a", callback=patch)
        store = RESTBatchStore(url=url)
        stale = store.read_marker("a")
        fresh = store.read_marker("a")
        store.mark("a", "B1", "O1", expected_version=fresh.version)
        with pytest.raises(BatchVersionConflict):
            store.mark("a", "B2", "O2", expected_version=stale.version)
        current = store.read_marker("a")
        assert current.batch_id == "B1"
        assert current.operation_id == "O1"
