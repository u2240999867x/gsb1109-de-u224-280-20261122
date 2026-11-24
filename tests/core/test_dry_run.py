"""Tests for tap dry runs with per-output-stream sample limits."""

from __future__ import annotations

import io
import json
import sys
import typing as t
from collections import defaultdict
from contextlib import redirect_stdout

import pytest

from singer_sdk import Stream, Tap
from singer_sdk.singerlib.encoding import DryRunSingerWriter

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


def _parse(buf: io.StringIO) -> list[dict]:
    buf.seek(0)
    return [json.loads(line) for line in buf.read().splitlines() if line]


def _tally(messages: list[dict]) -> dict[str, int]:
    tally: dict[str, int] = defaultdict(int)
    for message in messages:
        if message["type"] == "RECORD":
            tally[message["stream"]] += 1
    return dict(tally)


class OrderStream(Stream):
    """Order parent stream. The first three rows are blocked by an ad rule."""

    name = "orders"
    schema: t.ClassVar[dict] = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "updated_at": {"type": "string"},
            "ad_rule": {"type": "boolean"},
        },
    }
    replication_key = "updated_at"

    @override
    def get_child_context(
        self,
        record: Record,
        context: Context | None,
    ) -> Context | None:
        return {"order_id": record["id"]}

    @override
    def get_records(self, context: Context | None):
        for index in range(8):
            yield {
                "id": index + 1,
                "updated_at": f"2026-11-24T00:00:0{index}Z",
                "ad_rule": index < 3,
            }


class BillStream(Stream):
    """Bills derived from every order, including ad-rule-only orders."""

    name = "bills"
    schema: t.ClassVar[dict] = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "order_id": {"type": "integer"},
            "updated_at": {"type": "string"},
        },
    }
    replication_key = "updated_at"
    parent_stream_type = OrderStream

    @override
    def get_records(self, context: Context | None):
        assert context is not None
        order_id = context["order_id"]
        yield {
            "id": order_id * 10,
            "order_id": order_id,
            "updated_at": f"2026-11-24T00:00:0{min(order_id, 9)}Z",
        }


class OrderTap(Tap):
    name = "tap-orders"

    @override
    def discover_streams(self) -> list[Stream]:
        return [OrderStream(self), BillStream(self)]


ORDER_MAPS = {
    # Secondary sales output: ad-rule rows are filtered and must not consume
    # the sales sample quota. The primary orders map still passes every row
    # through so child bills are derived for all orders.
    "sales": {
        "__source__": "orders",
        "__filter__": "not ad_rule",
        "id": "id",
        "updated_at": "updated_at",
    },
    "bills": {},
}

INITIAL_STATE = {
    "bookmarks": {
        "orders": {
            "replication_key": "updated_at",
            "replication_key_value": "2026-11-24T00:00:00Z",
        },
        "bills": {
            "replication_key": "updated_at",
            "replication_key_value": "2026-11-24T00:00:00Z",
        },
    },
}


def test_dry_run_limits_actual_delivered_samples_per_output():
    tap = OrderTap(
        config={"stream_maps": ORDER_MAPS},
        state=INITIAL_STATE,
        parse_env_config=False,
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.run_sync_dry_run(dry_run_record_limit=2)

    messages = _parse(buf)
    tally = _tally(messages)

    # Bills fill from the first two valid parent records...
    assert tally["bills"] == 2
    # ...the default orders output gets its own two samples...
    assert tally["orders"] == 2
    # ...and scanning continues past the three ad-rule rows until sales fills.
    assert tally["sales"] == 2

    # No STATE messages escape the dry run and the bookmarks stay at yesterday.
    assert all(message["type"] != "STATE" for message in messages)
    assert tap.state == INITIAL_STATE

    # SCHEMA messages for the mapped outputs are still emitted.
    schema_streams = {
        message["stream"]
        for message in messages
        if message["type"] == "SCHEMA"
    }
    assert {"orders", "sales", "bills"} <= schema_streams


def test_dry_run_does_not_consume_sales_quota_for_filtered_rows():
    class SingleStream(Stream):
        name = "numbers"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
        }

        @override
        def get_records(self, context: Context | None):
            for index in range(6):
                yield {"id": index + 1}

    class SingleTap(Tap):
        name = "tap-numbers"

        @override
        def discover_streams(self) -> list[Stream]:
            return [SingleStream(self)]

    tap = SingleTap(
        config={
            "stream_maps": {
                "numbers": {"__filter__": "id > 3"},
            },
        },
        parse_env_config=False,
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.run_sync_dry_run(dry_run_record_limit=2)

    records = [
        message["record"]["id"]
        for message in _parse(buf)
        if message["type"] == "RECORD"
    ]
    # The first three source rows are filtered and do not spend the quota,
    # so the two delivered samples are the fourth and fifth source rows.
    assert records == [4, 5]


def test_dry_run_fan_out_caps_each_output_independently():
    class SingleStream(Stream):
        name = "numbers"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
        }

        @override
        def get_records(self, context: Context | None):
            for index in range(6):
                yield {"id": index + 1}

    class SingleTap(Tap):
        name = "tap-numbers"

        @override
        def discover_streams(self) -> list[Stream]:
            return [SingleStream(self)]

    tap = SingleTap(
        config={
            "stream_maps": {
                "numbers_copy": {"__source__": "numbers"},
            },
        },
        parse_env_config=False,
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.run_sync_dry_run(dry_run_record_limit=2)

    tally = _tally(_parse(buf))
    # Each source row fans out into two outputs; each output gets its own quota.
    assert tally == {"numbers": 2, "numbers_copy": 2}


def test_same_tap_runs_full_sync_after_dry_run():
    tap = OrderTap(
        config={"stream_maps": ORDER_MAPS},
        state=INITIAL_STATE,
        parse_env_config=False,
    )

    dry_buf = io.StringIO()
    with redirect_stdout(dry_buf):
        tap.run_sync_dry_run(dry_run_record_limit=2)

    # Dry run settings are fully rolled back.
    assert all(
        stream.ABORT_AT_RECORD_COUNT is None for stream in tap.streams.values()
    )
    assert tap.config["stream_maps"] == ORDER_MAPS

    full_buf = io.StringIO()
    with redirect_stdout(full_buf):
        tap.sync_all()

    messages = _parse(full_buf)
    tally = _tally(messages)

    # 8 orders passthrough, 5 sales (3 ad-rule rows filtered) and 8 bills.
    assert tally == {"orders": 8, "sales": 5, "bills": 8}

    state_messages = [message for message in messages if message["type"] == "STATE"]
    assert state_messages
    orders_bookmark = state_messages[-1]["value"]["bookmarks"]["orders"]
    assert (
        orders_bookmark["replication_key_value"] == "2026-11-24T00:00:07Z"
    ), orders_bookmark


def test_dry_run_without_limit_runs_uncapped():
    tap = OrderTap(
        config={"stream_maps": ORDER_MAPS},
        state=INITIAL_STATE,
        parse_env_config=False,
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        assert tap.run_sync_dry_run(dry_run_record_limit=None)

    tally = _tally(_parse(buf))
    assert tally == {"orders": 8, "sales": 5, "bills": 8}


def test_dry_run_restores_state_after_aborted_stream():
    class BoomStream(Stream):
        name = "boom"
        schema: t.ClassVar[dict] = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
        }

        @override
        def get_records(self, context: Context | None):
            yield {"id": 1}
            raise RuntimeError("boom")

    class BoomTap(Tap):
        name = "tap-boom"

        @override
        def discover_streams(self) -> list[Stream]:
            return [BoomStream(self)]

    tap = BoomTap(parse_env_config=False)
    with pytest.raises(RuntimeError, match="boom"):
        tap.run_sync_dry_run(dry_run_record_limit=1)

    # State and writer must be restored even when the sync blows up.
    assert tap.state == {}
    assert not isinstance(tap.message_writer, DryRunSingerWriter)
    assert tap.streams["boom"].ABORT_AT_RECORD_COUNT is None
