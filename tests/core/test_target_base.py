from __future__ import annotations

import copy
import datetime as dt
import decimal
import io
import json
import signal
import sys
from contextlib import redirect_stdout
from unittest import mock

import pytest

from singer_sdk.exceptions import (
    MissingKeyPropertiesError,
    RecordsWithoutSchemaException,
)
from singer_sdk.helpers.capabilities import PluginCapabilities, TargetCapabilities
from singer_sdk.singerlib.json import deserialize_json
from tests.conftest import BatchSinkMock, SQLSinkMock, SQLTargetMock, TargetMock

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


def test_get_sink():
    input_schema_1 = {
        "properties": {
            "id": {
                "type": ["string", "null"],
            },
            "col_ts": {
                "format": "date-time",
                "type": ["string", "null"],
            },
        },
    }
    input_schema_2 = copy.deepcopy(input_schema_1)
    key_properties = []
    target = TargetMock(config={"add_record_metadata": True})
    sink = BatchSinkMock(target, "foo", input_schema_1, key_properties)
    target._sinks_active["foo"] = sink
    sink_returned = target.get_sink(
        "foo",
        schema=input_schema_2,
        key_properties=key_properties,
    )
    assert sink_returned == sink


def test_validate_record():
    target = TargetMock()
    sink = BatchSinkMock(
        target=target,
        stream_name="test",
        schema={
            "properties": {
                "id": {"type": ["integer"]},
                "name": {"type": ["string"]},
            },
        },
        key_properties=["id"],
    )

    # Test valid record
    sink._singer_validate_message({"id": 1, "name": "test"})

    # Test invalid record
    with pytest.raises(MissingKeyPropertiesError):
        sink._singer_validate_message({"name": "test"})


def test_target_about_info():
    target = TargetMock()
    about = target._get_about_info()

    assert about.capabilities == [
        PluginCapabilities.ABOUT,
        PluginCapabilities.STREAM_MAPS,
        PluginCapabilities.FLATTENING,
        PluginCapabilities.STRUCTURED_LOGGING,
        TargetCapabilities.VALIDATE_RECORDS,
        PluginCapabilities.BATCH,
    ]

    assert "stream_maps" in about.settings["properties"]
    assert "stream_map_config" in about.settings["properties"]
    assert "flattening_enabled" in about.settings["properties"]
    assert "flattening_max_depth" in about.settings["properties"]
    assert "batch_config" in about.settings["properties"]
    assert "add_record_metadata" in about.settings["properties"]
    assert "batch_size_rows" in about.settings["properties"]


def test_sql_get_sink():
    input_schema_1 = {
        "properties": {
            "id": {
                "type": ["string", "null"],
            },
            "col_ts": {
                "format": "date-time",
                "type": ["string", "null"],
            },
        },
    }
    input_schema_2 = copy.deepcopy(input_schema_1)
    key_properties = []
    target = SQLTargetMock(config={"sqlalchemy_url": "sqlite:///"})
    sink = SQLSinkMock(
        target=target,
        stream_name="foo",
        schema=input_schema_1,
        key_properties=key_properties,
        connector=target.target_connector,
    )
    target._sinks_active["foo"] = sink
    sink_returned = target.get_sink(
        "foo",
        schema=input_schema_2,
        key_properties=key_properties,
    )
    assert sink_returned is sink


def test_add_sqlsink_and_get_sink():
    input_schema_1 = {
        "properties": {
            "id": {
                "type": ["string", "null"],
            },
            "col_ts": {
                "format": "date-time",
                "type": ["string", "null"],
            },
        },
    }
    input_schema_2 = copy.deepcopy(input_schema_1)
    key_properties = []
    target = SQLTargetMock(config={"sqlalchemy_url": "sqlite:///"})
    sink = target.add_sqlsink(
        "foo",
        schema=input_schema_2,
        key_properties=key_properties,
    )

    sink_returned = target.get_sink(
        "foo",
    )

    assert sink_returned is sink

    # Test invalid call
    with pytest.raises(RecordsWithoutSchemaException):
        target.get_sink(
            "bar",
        )


def test_create_sink_override():
    """Target.create_sink() can be overridden to inject extra constructor args."""
    injected = {}

    class CustomSink(BatchSinkMock):
        def __init__(self, *args, extra=None, **kwargs):
            super().__init__(*args, **kwargs)
            injected["extra"] = extra

    class CustomTarget(TargetMock):
        @override
        def create_sink(self, *, stream_name, schema, key_properties=None):
            return CustomSink(
                target=self,
                stream_name=stream_name,
                schema=schema,
                key_properties=key_properties,
                extra="injected_value",
            )

    schema = {"properties": {"id": {"type": ["integer"]}}}
    target = CustomTarget()
    sink = target.add_sink("test_stream", schema, [])
    assert isinstance(sink, CustomSink)
    assert injected["extra"] == "injected_value"


def test_sql_create_sink_override():
    """SQLTarget.create_sink() can be overridden to inject extra constructor args."""
    injected = {}

    class CustomSQLSink(SQLSinkMock):
        def __init__(self, *args, extra=None, **kwargs):
            super().__init__(*args, **kwargs)
            injected["extra"] = extra

    class CustomSQLTarget(SQLTargetMock):
        @override
        def create_sink(self, *, stream_name, schema, key_properties=None):
            return CustomSQLSink(
                target=self,
                stream_name=stream_name,
                schema=schema,
                key_properties=key_properties,
                connector=self.target_connector,
                extra="sql_injected",
            )

    schema = {"properties": {"id": {"type": ["integer"]}}}
    target = CustomSQLTarget(config={"sqlalchemy_url": "sqlite:///"})
    sink = target.add_sqlsink("test_stream", schema, [])
    assert isinstance(sink, CustomSQLSink)
    assert injected["extra"] == "sql_injected"


def test_batch_size_rows_and_max_size():
    input_schema_1 = {
        "properties": {
            "id": {
                "type": ["string", "null"],
            },
            "col_ts": {
                "format": "date-time",
                "type": ["string", "null"],
            },
        },
    }
    key_properties = []
    target_default = TargetMock()
    sink_default = BatchSinkMock(
        target=target_default,
        stream_name="foo",
        schema=input_schema_1,
        key_properties=key_properties,
    )
    target_set = TargetMock(config={"batch_size_rows": 100000})
    sink_set = BatchSinkMock(
        target=target_set,
        stream_name="bar",
        schema=input_schema_1,
        key_properties=key_properties,
    )
    assert sink_default.stream_name == "foo"
    assert sink_default._batch_size_rows is None
    assert sink_default.batch_size_rows is None
    assert sink_default.max_size == 10000
    assert sink_set.stream_name == "bar"
    assert sink_set._batch_size_rows == 100000
    assert sink_set.batch_size_rows == 100000
    assert sink_set.max_size == 100000


def test_duplicate_termination_signal_does_not_redrain():
    """A second signal arriving mid-drain is ignored and sinks drain once."""
    target = TargetMock()
    drain_calls: list[dict] = []

    def fake_drain_all(**kwargs) -> None:
        drain_calls.append(kwargs)
        # simulate the duplicate signal arriving while the drain is in flight,
        # e.g. delivered by the OS process group and forwarded by Meltano
        target._handle_termination(signal.SIGINT, None)

    with (
        mock.patch.object(target, "drain_all", side_effect=fake_drain_all),
        pytest.raises(SystemExit) as exc_info,
    ):
        target._handle_termination(signal.SIGTERM, None)

    assert exc_info.value.code == 0
    assert len(drain_calls) == 1


def test_target_write_state():
    target = TargetMock()
    buf = io.StringIO()
    with redirect_stdout(buf):
        target._write_state_message(
            {
                "integer": 42,
                "decimal": decimal.Decimal("3.14"),
                "datetime": dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc),
            },
        )

    buf.seek(0)
    assert deserialize_json(buf.read()) == {
        "integer": 42,
        "decimal": decimal.Decimal("3.14"),
        "datetime": "2021-01-01T00:00:00+00:00",
    }


def _schema_message(stream_name: str) -> dict:
    return {
        "type": "SCHEMA",
        "stream": stream_name,
        "schema": {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
        },
    }


def _process_messages(target: TargetMock, messages: list[dict]) -> None:
    with redirect_stdout(io.StringIO()):
        target.process_lines(
            io.StringIO("\n".join(json.dumps(message) for message in messages)),
        )


def test_target_version_transition_state_gating():
    """State for a stream mid-version-switch is held back until activation."""
    target = TargetMock()
    _process_messages(
        target,
        [
            _schema_message("versioned"),
            _schema_message("incremental"),
            # Old version data flows for both streams.
            {
                "type": "RECORD",
                "stream": "versioned",
                "record": {"id": 1},
                "version": 100,
            },
            {"type": "RECORD", "stream": "incremental", "record": {"id": 1}},
            {
                "type": "STATE",
                "value": {
                    "bookmarks": {
                        "versioned": {"replication_key_value": 1},
                        "incremental": {"replication_key_value": 1},
                    },
                },
            },
            # A new table version starts while the old version is still open.
            {
                "type": "RECORD",
                "stream": "versioned",
                "record": {"id": 2},
                "version": 200,
            },
            {"type": "RECORD", "stream": "incremental", "record": {"id": 2}},
            {
                "type": "STATE",
                "value": {
                    "bookmarks": {
                        "versioned": {"replication_key_value": 2},
                        "incremental": {"replication_key_value": 2},
                    },
                },
            },
        ],
    )

    with redirect_stdout(io.StringIO()):
        target.drain_all()

    # The new version's STATE is not trusted yet: the versioned stream's
    # bookmark is rolled back to the old version's confirmed progress, while
    # the incremental-only stream's bookmark keeps advancing.
    assert target.state_messages_written[-1] == {
        "bookmarks": {
            "versioned": {"replication_key_value": 1},
            "incremental": {"replication_key_value": 2},
        },
    }

    # The new version is fully delivered and activated: subscribers switch
    # from the old version to the new one.
    _process_messages(
        target,
        [
            {
                "type": "ACTIVATE_VERSION",
                "stream": "versioned",
                "version": 200,
            },
            {
                "type": "STATE",
                "value": {
                    "bookmarks": {
                        "versioned": {"replication_key_value": 3},
                        "incremental": {"replication_key_value": 3},
                    },
                },
            },
        ],
    )

    with redirect_stdout(io.StringIO()):
        target.drain_all()

    assert target.state_messages_written[-1] == {
        "bookmarks": {
            "versioned": {"replication_key_value": 3},
            "incremental": {"replication_key_value": 3},
        },
    }


def test_target_version_transition_corrupted_batch(tmp_path):
    """A failed version switch resumes from the old version's confirmed state."""
    old_batch = tmp_path / "old.jsonl"
    old_batch.write_text('{"id": 1}\n{"id": 2}\n')
    new_batch_1 = tmp_path / "new.1.jsonl"
    new_batch_1.write_text('{"id": 3}\n{"id": 4}\n')
    new_batch_2 = tmp_path / "new.2.jsonl"
    new_batch_2.write_text('{"id": 5}\n{"id": 6')

    target = TargetMock()
    _process_messages(
        target,
        [
            _schema_message("versioned"),
            _schema_message("incremental"),
            # Two old-version batches are read and confirmed.
            {
                "type": "BATCH",
                "stream": "versioned",
                "encoding": {"format": "jsonl"},
                "manifest": [old_batch.as_uri()],
                "version": 100,
            },
            {"type": "RECORD", "stream": "incremental", "record": {"id": 1}},
            {
                "type": "STATE",
                "value": {
                    "bookmarks": {
                        "versioned": {"replication_key_value": 2},
                        "incremental": {"replication_key_value": 1},
                    },
                },
            },
        ],
    )

    with redirect_stdout(io.StringIO()):
        target.drain_all()

    assert target.state_messages_written[-1] == {
        "bookmarks": {
            "versioned": {"replication_key_value": 2},
            "incremental": {"replication_key_value": 1},
        },
    }

    # The first batch of the new version lands along with the new STATE.
    _process_messages(
        target,
        [
            {
                "type": "BATCH",
                "stream": "versioned",
                "encoding": {"format": "jsonl"},
                "manifest": [new_batch_1.as_uri()],
                "version": 200,
            },
            {"type": "RECORD", "stream": "incremental", "record": {"id": 2}},
            {
                "type": "STATE",
                "value": {
                    "bookmarks": {
                        "versioned": {"replication_key_value": 4},
                        "incremental": {"replication_key_value": 2},
                    },
                },
            },
        ],
    )

    with redirect_stdout(io.StringIO()):
        target.drain_all()

    # The new version's STATE is not adopted just because its first batch
    # landed. The incremental-only stream's confirmed progress still advances.
    assert target.state_messages_written[-1] == {
        "bookmarks": {
            "versioned": {"replication_key_value": 2},
            "incremental": {"replication_key_value": 2},
        },
    }

    # The second batch of the new version is corrupted: the run fails and the
    # last emitted state still points at the old version's confirmed progress.
    with pytest.raises(json.JSONDecodeError), redirect_stdout(io.StringIO()):
        _process_messages(
            target,
            [
                {
                    "type": "BATCH",
                    "stream": "versioned",
                    "encoding": {"format": "jsonl"},
                    "manifest": [new_batch_2.as_uri()],
                    "version": 200,
                },
            ],
        )

    assert target.state_messages_written[-1] == {
        "bookmarks": {
            "versioned": {"replication_key_value": 2},
            "incremental": {"replication_key_value": 2},
        },
    }


def test_target_activate_version_legacy_flow():
    """Old-style taps emitting ACTIVATE_VERSION first see no state gating."""
    target = TargetMock()
    _process_messages(
        target,
        [
            _schema_message("versioned"),
            {"type": "ACTIVATE_VERSION", "stream": "versioned", "version": 100},
            {
                "type": "RECORD",
                "stream": "versioned",
                "record": {"id": 1},
                "version": 100,
            },
            {
                "type": "STATE",
                "value": {
                    "bookmarks": {
                        "versioned": {"replication_key_value": 1},
                    },
                },
            },
        ],
    )

    with redirect_stdout(io.StringIO()):
        target.drain_all()

    assert target.state_messages_written[-1] == {
        "bookmarks": {
            "versioned": {"replication_key_value": 1},
        },
    }


def test_target_pending_version_without_confirmed_bookmark():
    """A stream with no confirmed progress is omitted from emitted state."""
    target = TargetMock()
    _process_messages(
        target,
        [
            _schema_message("versioned"),
            _schema_message("incremental"),
            # The version switch starts before any state was confirmed.
            {
                "type": "RECORD",
                "stream": "versioned",
                "record": {"id": 1},
                "version": 100,
            },
            {
                "type": "RECORD",
                "stream": "versioned",
                "record": {"id": 2},
                "version": 200,
            },
            {"type": "RECORD", "stream": "incremental", "record": {"id": 1}},
            {
                "type": "STATE",
                "value": {
                    "bookmarks": {
                        "versioned": {"replication_key_value": 2},
                        "incremental": {"replication_key_value": 1},
                    },
                },
            },
        ],
    )

    with redirect_stdout(io.StringIO()):
        target.drain_all()

    assert target.state_messages_written[-1] == {
        "bookmarks": {
            "incremental": {"replication_key_value": 1},
        },
    }
