"""Tests for the dry run Singer writer."""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout

from singer_sdk.singerlib import RecordMessage, StateMessage
from singer_sdk.singerlib.encoding import (
    DryRunSingerWriter,
    SimpleSingerWriter,
    SingerMessageType,
)


def _record(stream: str, record_id: int) -> RecordMessage:
    return RecordMessage(stream=stream, record={"id": record_id})


def test_dry_run_writer_caps_records_per_output_stream():
    inner = SimpleSingerWriter()
    writer = DryRunSingerWriter(inner, record_limit=2)

    buf = io.StringIO()
    with redirect_stdout(buf):
        writer.write_message(_record("sales", 1))
        writer.write_message(_record("bills", 10))
        writer.write_message(_record("sales", 2))
        writer.write_message(_record("bills", 11))
        # Both streams are now at their quota; further records are dropped.
        writer.write_message(_record("sales", 3))
        writer.write_message(_record("bills", 12))

    delivered = [
        (message["stream"], message["record"]["id"])
        for message in (
            json.loads(line) for line in buf.getvalue().splitlines() if line
        )
    ]
    assert delivered == [("sales", 1), ("bills", 10), ("sales", 2), ("bills", 11)]
    assert writer.records_emitted == {"sales": 2, "bills": 2}


def test_dry_run_writer_passes_state_messages_through():
    inner = SimpleSingerWriter()
    writer = DryRunSingerWriter(inner, record_limit=1)

    buf = io.StringIO()
    with redirect_stdout(buf):
        writer.write_message(_record("sales", 1))
        writer.write_message(StateMessage(value={"bookmarks": {}}))
        # Record past the limit is dropped, but the STATE message still writes.
        writer.write_message(_record("sales", 2))
        writer.write_message(StateMessage(value={"bookmarks": {"sales": {}}}))

    messages = [json.loads(line) for line in buf.getvalue().splitlines() if line]
    assert [message["type"] for message in messages] == [
        SingerMessageType.RECORD,
        SingerMessageType.STATE,
        SingerMessageType.STATE,
    ]


def test_dry_run_writer_without_limit_delivers_everything():
    inner = SimpleSingerWriter()
    writer = DryRunSingerWriter(inner, record_limit=None)

    buf = io.StringIO()
    with redirect_stdout(buf):
        for index in range(5):
            writer.write_message(_record("sales", index))

    messages = [json.loads(line) for line in buf.getvalue().splitlines() if line]
    assert len(messages) == 5
    assert writer.records_emitted == {"sales": 5}
