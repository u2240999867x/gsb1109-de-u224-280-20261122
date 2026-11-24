from __future__ import annotations

import datetime
import json
import sqlite3
import typing as t

import pytest
import sqlalchemy.exc

from singer_sdk.sql import SQLConnector, SQLSink, SQLTarget


def _adapt_datetime(value: datetime.datetime) -> str:
    return value.isoformat(sep=" ")


sqlite3.register_adapter(
    datetime.datetime,
    _adapt_datetime,
)


class AtomicOrderSink(SQLSink):
    connector_class = SQLConnector


class AtomicOrderTarget(SQLTarget):
    name = "atomic-order-target"
    default_sink_class = AtomicOrderSink


OLD_SCHEMA = {
    "type": "object",
    "properties": {
        "event_id": {"type": "string"},
        "order_id": {"type": "string"},
        "archive_note": {"type": ["null", "string"]},
        "ledger_ref": {"type": ["null", "string"]},
        "_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"},
    },
}

NEW_SCHEMA = {
    "type": "object",
    "properties": {
        **OLD_SCHEMA["properties"],
        "warehouse_code": {"type": ["null", "string"]},
    },
}

E41 = {
    "event_id": "E41",
    "order_id": "O1",
    "archive_note": "2026-10-01T09:00:00Z",
    "ledger_ref": "L41",
    "_sdc_deleted_at": None,
}
E42 = {
    "event_id": "E42",
    "order_id": "O1",
    "archive_note": "人工撤单",
    "ledger_ref": "L42",
    "warehouse_code": "WH-2",
    "_sdc_deleted_at": "2026-10-02T08:00:00Z",
}
E43 = {
    "event_id": "E43",
    "order_id": "O2",
    "archive_note": "冲突事件",
    "ledger_ref": "L42",
    "warehouse_code": "WH-3",
    "_sdc_deleted_at": None,
}
E44 = {
    "event_id": "E44",
    "order_id": "O4",
    "archive_note": "正常事件",
    "ledger_ref": "L44",
    "warehouse_code": "WH-4",
    "_sdc_deleted_at": None,
}


def _config(db_path: str) -> dict[str, t.Any]:
    return {
        "sqlalchemy_url": f"sqlite:///{db_path}",
        "add_record_metadata": True,
        "batch_size_rows": 1000,
        "atomic_event_writes": True,
        "stream_maps": {
            "orders": {
                "__alias__": "orders_audit",
                "__else__": None,
                "event_id": "event_id",
                "order_id": "order_id",
                "_sdc_deleted_at": "archive_note",
            },
            "orders_cdc": {
                "__source__": "orders",
                "archive_note": None,
                "_sdc_deleted_at": None,
            },
        },
    }


def _schema_message(schema: dict[str, t.Any]) -> dict[str, t.Any]:
    return {
        "stream": "orders",
        "schema": schema,
        "key_properties": ["event_id"],
    }


def _write_batch(path: t.Any, records: list[dict[str, t.Any]]) -> str:
    path.write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records),
        encoding="utf-8",
    )
    return path.as_uri()


def _rows(db_path: str, sql: str) -> list[tuple[t.Any, ...]]:
    with sqlite3.connect(db_path) as connection:
        return connection.execute(sql).fetchall()


def test_atomic_event_projections_roll_back_failed_event_and_replay(
    tmp_path: t.Any,
) -> None:
    db_path = tmp_path / "orders.db"
    batch_path = tmp_path / "events.jsonl"
    config = _config(str(db_path))

    target = AtomicOrderTarget(config=config, validate_config=False)
    target._process_schema_message(_schema_message(OLD_SCHEMA))  # noqa: SLF001

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE UNIQUE INDEX ux_orders_cdc_ledger_ref ON orders_cdc(ledger_ref)",
        )

    target._process_record_message(  # noqa: SLF001
        {"type": "RECORD", "stream": "orders", "record": E41},
    )
    target._process_schema_message(_schema_message(NEW_SCHEMA))  # noqa: SLF001

    manifest = _write_batch(batch_path, [E42, E43, E44])
    batch_message = {
        "type": "BATCH",
        "stream": "orders",
        "encoding": {"format": "jsonl", "compression": None},
        "manifest": [manifest],
    }

    with pytest.raises(sqlalchemy.exc.IntegrityError, match="E43") as exc_info:
        target._process_batch_message(batch_message)  # noqa: SLF001

    assert exc_info.value.event_id == "E43"

    assert _rows(
        str(db_path),
        "SELECT event_id, _sdc_deleted_at FROM orders_audit ORDER BY rowid",
    ) == [
        ("E41", "2026-10-01T09:00:00Z"),
        ("E42", "人工撤单"),
    ]
    assert _rows(
        str(db_path),
        "SELECT event_id, warehouse_code, _sdc_deleted_at "
        "FROM orders_cdc ORDER BY rowid",
    ) == [
        ("E41", None, None),
        ("E42", "WH-2", "2026-10-02 08:00:00+00:00"),
    ]

    audit_received = _rows(
        str(db_path),
        "SELECT event_id, _sdc_received_at FROM orders_audit WHERE event_id='E42'",
    )
    cdc_received = _rows(
        str(db_path),
        "SELECT event_id, _sdc_received_at FROM orders_cdc WHERE event_id='E42'",
    )

    fixed_e43 = {**E43, "ledger_ref": "L43-NEW"}
    manifest = _write_batch(batch_path, [E42, fixed_e43, E44])

    replay_target = AtomicOrderTarget(config=config, validate_config=False)
    replay_target._process_schema_message(_schema_message(NEW_SCHEMA))  # noqa: SLF001
    replay_target._process_record_message(  # noqa: SLF001
        {"type": "RECORD", "stream": "orders", "record": E42},
    )
    replay_target._process_batch_message(  # noqa: SLF001
        {
            "type": "BATCH",
            "stream": "orders",
            "encoding": {"format": "jsonl", "compression": None},
            "manifest": [manifest],
        },
    )
    replay_target.process_endofpipe()

    event_order = ["E41", "E42", "E43", "E44"]
    assert [
        row[0]
        for row in _rows(
            str(db_path),
            "SELECT event_id FROM orders_audit ORDER BY rowid",
        )
    ] == event_order
    assert [
        row[0]
        for row in _rows(
            str(db_path),
            "SELECT event_id FROM orders_cdc ORDER BY rowid",
        )
    ] == event_order
    assert _rows(str(db_path), "SELECT COUNT(*) FROM orders_audit") == [(4,)]
    assert _rows(str(db_path), "SELECT COUNT(*) FROM orders_cdc") == [(4,)]
    assert (
        _rows(
            str(db_path),
            "SELECT event_id, _sdc_received_at FROM orders_audit WHERE event_id='E42'",
        )
        == audit_received
    )
    assert (
        _rows(
            str(db_path),
            "SELECT event_id, _sdc_received_at FROM orders_cdc WHERE event_id='E42'",
        )
        == cdc_received
    )
