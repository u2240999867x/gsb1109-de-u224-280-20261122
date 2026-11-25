import asyncio
import json
import operator
import time
from contextlib import contextmanager
from copy import copy
from unittest.mock import Mock, call, patch

import pytest
from mode import label, shortlabel

from faust import Event, Record, Stream, Topic, joins
from faust.exceptions import PartitionsMismatch
from faust.stores.base import Store
from faust.tables.base import Collection
from faust.tables.table import Table
from faust.types import TP, Message
from faust.windows import HoppingWindow, TumblingWindow, Window
from tests.helpers import AsyncMock

TP1 = TP("foo", 0)


class User(Record):
    id: str
    name: str


class MyTable(Collection):
    def __post_init__(self, *args, **kwargs):
        self.datas = {}

    def _has_key(self, key):
        return key in self.datas

    def _get_key(self, key):
        return self.datas.get(key)

    def _set_key(self, key, value):
        self.datas[key] = value

    def _del_key(self, key):
        self.datas.pop(key, None)

    def hopping(self, *args, **kwargs):
        raise NotImplementedError()

    def tumbling(self, *args, **kwargs):
        raise NotImplementedError()

    def using_window(self, *args, **kwargs):
        raise NotImplementedError()

    def as_ansitable(self, *args, **kwargs):
        raise NotImplementedError()


class Test_Collection:
    @pytest.fixture
    def table(self, *, app):
        return MyTable(app, name="name")

    def test_key_type_bytes_implies_raw_serializer(self, *, app):
        table = MyTable(app, name="name", key_type=bytes)
        assert table.key_serializer == "raw"

    @pytest.mark.asyncio
    async def test_init_on_recover(self, *, app):
        on_recover = AsyncMock(name="on_recover")
        t = MyTable(app, name="name", on_recover=on_recover)
        assert on_recover in t._recover_callbacks
        await t.call_recover_callbacks()
        on_recover.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_recovery_completed(self, *, table):
        table.call_recover_callbacks = AsyncMock()
        await table.on_recovery_completed(set(), set())
        table.call_recover_callbacks.assert_called_once_with()

    def test_hash(self, *, table):
        assert hash(table)

    @pytest.mark.asyncio
    async def test_on_start(self, *, table):
        table.changelog_topic = Mock(
            name="changelog_topic",
            autospec=Topic,
            maybe_declare=AsyncMock(),
        )
        await table.on_start()
        table.changelog_topic.maybe_declare.assert_called_once_with()

    def test_info(self, *, table):
        assert table.info() == {
            "app": table.app,
            "name": table.name,
            "store": table._store,
            "default": table.default,
            "schema": table.schema,
            "key_type": table.key_type,
            "value_type": table.value_type,
            "changelog_topic": table._changelog_topic,
            "window": table.window,
            "extra_topic_configs": table.extra_topic_configs,
            "on_changelog_event": table._on_changelog_event,
            "recover_callbacks": table._recover_callbacks,
            "partitions": table.partitions,
            "recovery_buffer_size": table.recovery_buffer_size,
            "standby_buffer_size": table.standby_buffer_size,
            "use_partitioner": table.use_partitioner,
        }

    def test_persisted_offset(self, *, table):
        data = table._data = Mock(name="_data")
        assert table.persisted_offset(TP1) == data.persisted_offset()

    @pytest.mark.asyncio
    async def test_need_active_standby_for(self, *, table):
        table._data = Mock(
            name="_data",
            autospec=Store,
            need_active_standby_for=AsyncMock(),
        )
        assert (
            await table.need_active_standby_for(TP1)
            == table._data.need_active_standby_for.return_value
        )

    def test_reset_state(self, *, table):
        data = table._data = Mock(name="_data", autospec=Store)
        table.reset_state()
        data.reset_state.assert_called_once_with()

    def test_send_changelog(self, *, table):
        table.changelog_topic.send_soon = Mock(name="send_soon")
        event = Mock(name="event")
        table._send_changelog(event, "k", "v")
        table.changelog_topic.send_soon.assert_called_once_with(
            key="k",
            value="v",
            partition=event.message.partition,
            key_serializer="json",
            value_serializer="json",
            callback=table._on_changelog_sent,
            eager_partitioning=True,
        )

    def test_send_changelog__custom_serializers(self, *, table):
        event = Mock(name="event")
        table.changelog_topic.send_soon = Mock(name="send_soon")
        table._send_changelog(
            event,
            "k",
            "v",
            key_serializer="raw",
            value_serializer="raw",
        )
        table.changelog_topic.send_soon.assert_called_once_with(
            key="k",
            value="v",
            partition=event.message.partition,
            key_serializer="raw",
            value_serializer="raw",
            callback=table._on_changelog_sent,
            eager_partitioning=True,
        )

    def test_send_changelog__no_current_event(self, *, table):
        with pytest.raises(RuntimeError):
            table._send_changelog(None, "k", "v")

    def test_on_changelog_sent(self, *, table):
        fut = Mock(name="future", autospec=asyncio.Future)
        table._data = Mock(name="data", autospec=Store)
        table._on_changelog_sent(fut)
        table._data.set_persisted_offset.assert_called_once_with(
            fut.result().topic_partition,
            fut.result().offset,
        )

    def test_on_changelog_sent__transactions(self, *, table):
        table.app.in_transaction = True
        table.app.tables = Mock(name="tables")
        fut = Mock(name="fut")
        table._on_changelog_sent(fut)
        table.app.tables.persist_offset_on_commit.assert_called_once_with(
            table.data,
            fut.result().topic_partition,
            fut.result().offset,
        )

    @pytest.mark.asyncio
    async def test_last_closed_window(self, *, table):
        assert table.last_closed_window == 0.0

        table.window = Mock(name="window")
        self.mock_no_ranges(table)
        table._data = {
            ("boo", (1.1, 1.4)): "BOO",
            ("moo", (1.4, 1.6)): "MOO",
            ("faa", (1.9, 2.0)): "FAA",
            ("bar", (4.1, 4.2)): "BAR",
        }
        table._partition_timestamps = {
            TP1: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        }
        table._partition_timestamp_keys = {
            (TP1, 2.0): [
                ("boo", (1.1, 1.4)),
                ("moo", (1.4, 1.6)),
                ("faa", (1.9, 2.0)),
            ],
            (TP1, 5.0): [
                ("bar", (4.1, 4.2)),
            ],
        }

        def get_stale(limit):
            def is_stale(timestamp, latest_timestamp):
                return timestamp < limit

            return is_stale

        table.window.stale.side_effect = get_stale(4.0)

        await table._del_old_keys()

        assert table.last_closed_window == 1.9

    @pytest.mark.asyncio
    async def test_last_closed_window__mock_ranges(self, *, table):
        assert table.last_closed_window == 0.0

        table.window = Mock(name="window")
        self.mock_ranges(table)
        table._data = {
            ("boo", (1.1, 1.4)): "BOO",
            ("moo", (1.4, 1.6)): "MOO",
            ("faa", (1.9, 2.0)): "FAA",
            ("bar", (4.1, 4.2)): "BAR",
        }
        table._partition_timestamps = {
            TP1: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        }
        table._partition_timestamp_keys = {
            (TP1, 2.0): [
                ("boo", (1.1, 1.4)),
                ("moo", (1.4, 1.6)),
                ("faa", (1.9, 2.0)),
            ],
            (TP1, 5.0): [
                ("bar", (4.1, 4.2)),
            ],
        }

        def get_stale(limit):
            def is_stale(timestamp, latest_timestamp):
                return timestamp < limit

            return is_stale

        table.window.stale.side_effect = get_stale(4.0)

        await table._del_old_keys()

        assert table.last_closed_window == 1.9

    @pytest.mark.asyncio
    async def test_del_old_keys__empty(self, *, table):
        table.window = Mock(name="window")
        await table._del_old_keys()

    @pytest.mark.asyncio
    async def test_del_old_keys(self, *, table):
        on_window_close = table._on_window_close = AsyncMock(name="on_window_close")

        table.window = Mock(name="window")
        self.mock_no_ranges(table)
        table._data = {
            ("boo", (1.1, 1.4)): "BOO",
            ("moo", (1.4, 1.6)): "MOO",
            ("faa", (1.9, 2.0)): "FAA",
            ("bar", (4.1, 4.2)): "BAR",
        }
        table._partition_timestamps = {
            TP1: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        }
        table._partition_timestamp_keys = {
            (TP1, 2.0): [
                ("boo", (1.1, 1.4)),
                ("moo", (1.4, 1.6)),
                ("faa", (1.9, 2.0)),
            ],
            (TP1, 5.0): [
                ("bar", (4.1, 4.2)),
            ],
        }

        def get_stale(limit):
            def is_stale(timestamp, latest_timestamp):
                return timestamp < limit

            return is_stale

        table.window.stale.side_effect = get_stale(4.0)

        await table._del_old_keys()

        assert table._partition_timestamps[TP1] == [4.0, 5.0, 6.0, 7.0]
        assert table.data == {("bar", (4.1, 4.2)): "BAR"}

        on_window_close.assert_has_calls(
            [
                call.__bool__(),
                call(("boo", (1.1, 1.4)), "BOO"),
                call.__bool__(),
                call(("moo", (1.4, 1.6)), "MOO"),
                call.__bool__(),
                call(("faa", (1.9, 2.0)), "FAA"),
            ]
        )

        table.last_closed_window = 8.0
        table.window.stale.side_effect = get_stale(6.0)

        await table._del_old_keys()

        assert not table.data

    @pytest.mark.asyncio
    async def test_del_old_keys__mock_ranges(self, *, table):
        on_window_close = table._on_window_close = AsyncMock(name="on_window_close")

        table.window = Mock(name="window")
        self.mock_ranges(table)
        table._data = {
            ("boo", (1.1, 1.4)): "BOO",
            ("moo", (1.4, 1.6)): "MOO",
            ("faa", (1.9, 2.0)): "FAA",
            ("bar", (4.1, 4.2)): "BAR",
        }
        table._partition_timestamps = {
            TP1: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        }
        table._partition_timestamp_keys = {
            (TP1, 2.0): [
                ("boo", (1.1, 1.4)),
                ("moo", (1.4, 1.6)),
                ("faa", (1.9, 2.0)),
            ],
            (TP1, 5.0): [
                ("bar", (4.1, 4.2)),
            ],
        }

        def get_stale(limit):
            def is_stale(timestamp, latest_timestamp):
                return timestamp < limit

            return is_stale

        table.window.stale.side_effect = get_stale(4.0)

        await table._del_old_keys()

        assert table._partition_timestamps[TP1] == [4.0, 5.0, 6.0, 7.0]
        assert table.data == {("bar", (4.1, 4.2)): "BAR"}

        on_window_close.assert_has_calls(
            [
                call.__bool__(),
                call(("boo", (1.1, 1.4)), "BOO"),
                call.__bool__(),
                call(("moo", (1.4, 1.6)), "MOO"),
                call.__bool__(),
                call(("faa", (1.9, 2.0)), "FAA"),
            ]
        )

        table.last_closed_window = 8.0
        table.window.stale.side_effect = get_stale(6.0)

        await table._del_old_keys()

        assert not table.data

    @pytest.mark.asyncio
    async def test_del_old_keys_non_async_cb(self, *, table):
        on_window_close = table._on_window_close = Mock(name="on_window_close")

        table.window = Mock(name="window")
        self.mock_no_ranges(table)
        table._data = {
            ("boo", (1.1, 1.4)): "BOO",
            ("moo", (1.4, 1.6)): "MOO",
            ("faa", (1.9, 2.0)): "FAA",
            ("bar", (4.1, 4.2)): "BAR",
        }
        table._partition_timestamps = {
            TP1: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        }
        table._partition_timestamp_keys = {
            (TP1, 2.0): [
                ("boo", (1.1, 1.4)),
                ("moo", (1.4, 1.6)),
                ("faa", (1.9, 2.0)),
            ],
            (TP1, 5.0): [
                ("bar", (4.1, 4.2)),
            ],
        }

        def get_stale(limit):
            def is_stale(timestamp, latest_timestamp):
                return timestamp < limit

            return is_stale

        table.window.stale.side_effect = get_stale(4.0)

        await table._del_old_keys()

        assert table._partition_timestamps[TP1] == [4.0, 5.0, 6.0, 7.0]
        assert table.data == {("bar", (4.1, 4.2)): "BAR"}

        on_window_close.assert_has_calls(
            [
                call(("boo", (1.1, 1.4)), "BOO"),
                call(("moo", (1.4, 1.6)), "MOO"),
                call(("faa", (1.9, 2.0)), "FAA"),
            ]
        )

        table.last_closed_window = 8.0
        table.window.stale.side_effect = get_stale(6.0)

        await table._del_old_keys()

        assert not table.data

    @pytest.mark.asyncio
    async def test_del_old_keys_non_async_cb__mock_ranges(self, *, table):
        on_window_close = table._on_window_close = Mock(name="on_window_close")

        table.window = Mock(name="window")
        self.mock_ranges(table)
        table._data = {
            ("boo", (1.1, 1.4)): "BOO",
            ("moo", (1.4, 1.6)): "MOO",
            ("faa", (1.9, 2.0)): "FAA",
            ("bar", (4.1, 4.2)): "BAR",
        }
        table._partition_timestamps = {
            TP1: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        }
        table._partition_timestamp_keys = {
            (TP1, 2.0): [
                ("boo", (1.1, 1.4)),
                ("moo", (1.4, 1.6)),
                ("faa", (1.9, 2.0)),
            ],
            (TP1, 5.0): [
                ("bar", (4.1, 4.2)),
            ],
        }

        def get_stale(limit):
            def is_stale(timestamp, latest_timestamp):
                return timestamp < limit

            return is_stale

        table.window.stale.side_effect = get_stale(4.0)

        await table._del_old_keys()

        assert table._partition_timestamps[TP1] == [4.0, 5.0, 6.0, 7.0]
        assert table.data == {("bar", (4.1, 4.2)): "BAR"}

        on_window_close.assert_has_calls(
            [
                call(("boo", (1.1, 1.4)), "BOO"),
                call(("moo", (1.4, 1.6)), "MOO"),
                call(("faa", (1.9, 2.0)), "FAA"),
            ]
        )

        table.last_closed_window = 8.0
        table.window.stale.side_effect = get_stale(6.0)

        await table._del_old_keys()

        assert not table.data

    @pytest.mark.asyncio
    async def test_on_window_close__default(self, *, table):
        assert table._on_window_close is None
        await table.on_window_close(("boo", (1.1, 1.4)), "BOO")

    @pytest.mark.parametrize(
        "source_n,change_n,expect_error",
        [
            (3, 3, False),
            (3, None, False),
            (None, 3, False),
            (3, 6, True),
            (6, 3, True),
        ],
    )
    def test__verify_source_topic_partitions(
        self, source_n, change_n, expect_error, *, app, table
    ):
        event = Mock(name="event", autospec=Event)
        tps = {
            event.message.topic: source_n,
            table.changelog_topic.get_topic_name(): change_n,
        }
        app.consumer.topic_partitions = Mock(side_effect=tps.get)
        if expect_error:
            with pytest.raises(PartitionsMismatch):
                table._verify_source_topic_partitions(event.message.topic)
        else:
            table._verify_source_topic_partitions(event.message.topic)

    @pytest.mark.asyncio
    async def test_clean_data(self, *, table):
        table._should_expire_keys = Mock(name="_should_expire_keys")
        table._should_expire_keys.return_value = False
        await table._clean_data(table)

        table._should_expire_keys.return_value = True
        table._del_old_keys = AsyncMock(name="_del_old_keys")

        def on_sleep(secs, **kwargs):
            if table.sleep.call_count > 2:
                table._stopped.set()

        table.sleep = AsyncMock(name="sleep", side_effect=on_sleep)

        await table._clean_data(table)

        table._del_old_keys.assert_called_once_with()
        table.sleep.assert_called_with(
            pytest.approx(table.app.conf.table_cleanup_interval, rel=1e-1)
        )

    def test_should_expire_keys(self, *, table):
        table.window = None
        assert not table._should_expire_keys()
        table.window = Mock(name="window", autospec=Window)
        table.window.expires = 3600
        assert table._should_expire_keys()

    def test_join(self, *, table):
        table._join = Mock(name="join")
        ret = table.join(User.id, User.name)
        table._join.assert_called_once_with(
            joins.RightJoin(stream=table, fields=(User.id, User.name)),
        )
        assert ret is table._join()

    def test_left_join(self, *, table):
        table._join = Mock(name="join")
        ret = table.left_join(User.id, User.name)
        table._join.assert_called_once_with(
            joins.LeftJoin(stream=table, fields=(User.id, User.name)),
        )
        assert ret is table._join()

    def test_inner_join(self, *, table):
        table._join = Mock(name="join")
        ret = table.inner_join(User.id, User.name)
        table._join.assert_called_once_with(
            joins.InnerJoin(stream=table, fields=(User.id, User.name)),
        )
        assert ret is table._join()

    def test_outer_join(self, *, table):
        table._join = Mock(name="join")
        ret = table.outer_join(User.id, User.name)
        table._join.assert_called_once_with(
            joins.OuterJoin(stream=table, fields=(User.id, User.name)),
        )
        assert ret is table._join()

    def test__join(self, *, table):
        with pytest.raises(NotImplementedError):
            table._join(Mock(name="join_strategy", autospec=joins.Join))

    def test_clone(self, *, table):
        t2 = table.clone()
        assert t2.info() == table.info()

    def test_combine(self, *, table):
        with pytest.raises(NotImplementedError):
            table.combine(Mock(name="joinable", autospec=Stream))

    def test_contribute_to_stream(self, *, table):
        table.contribute_to_stream(Mock(name="stream", autospec=Stream))

    @pytest.mark.asyncio
    async def test_remove_from_stream(self, *, table):
        await table.remove_from_stream(Mock(name="stream", autospec=Stream))

    def test_new_changelog_topic__window_expires(self, *, table):
        table.window = Mock(name="window", autospec=Window)
        table.window.expires = 3600.3
        assert table._new_changelog_topic(retention=None).retention == 3600.3

    def test_new_changelog_topic__default_compacting(self, *, table):
        table._changelog_compacting = True
        assert table._new_changelog_topic(compacting=None).compacting
        table._changelog_compacting = False
        assert not table._new_changelog_topic(compacting=None).compacting
        assert table._new_changelog_topic(compacting=True).compacting

    def test_new_changelog_topic__default_deleting(self, *, table):
        table._changelog_deleting = True
        assert table._new_changelog_topic(deleting=None).deleting
        table._changelog_deleting = False
        assert not table._new_changelog_topic(deleting=None).deleting
        assert table._new_changelog_topic(deleting=True).deleting

    def test_copy(self, *, table):
        assert copy(table).info() == table.info()

    def test_and(self, *, table):
        with pytest.raises(NotImplementedError):
            table & table

    def test__maybe_set_key_ttl(self, *, table):
        table._should_expire_keys = Mock(return_value=False)
        table._maybe_set_key_ttl(("k", (100, 110)), 0)

        table._should_expire_keys = Mock(return_value=True)
        table._maybe_set_key_ttl(("k", (100, 110)), 0)

    def test__maybe_del_key_ttl(self, *, table):
        table._partition_timestamp_keys[(0, 110)] = None

        table._should_expire_keys = Mock(return_value=False)
        table._maybe_del_key_ttl(("k", (100, 110)), 0)

        table._should_expire_keys = Mock(return_value=True)
        table._maybe_del_key_ttl(("k", (100, 110)), 0)

        table._partition_timestamp_keys[(0, 110)] = {
            ("k", (100, 110)),
            ("v", (100, 110)),
        }
        table._maybe_del_key_ttl(("k", (100, 110)), 0)

        assert table._partition_timestamp_keys[(0, 110)] == {("v", (100, 110))}

    def test_apply_window_op(self, *, table):
        self.mock_ranges(table)
        table._set_key(("k", 1.1), 30)
        table._set_key(("k", 1.2), 40)
        table._set_key(("k", 1.3), 50)
        table._apply_window_op(operator.add, "k", 12, 300.3)

        assert table._get_key(("k", 1.1)) == 42
        assert table._get_key(("k", 1.2)) == 52
        assert table._get_key(("k", 1.3)) == 62

    def test_set_del_windowed(self, *, table):
        ranges = self.mock_ranges(table)
        table._set_windowed("k", 11, 300.3)
        for r in ranges:
            assert table._get_key(("k", r)) == 11
        table._del_windowed("k", 300.3)
        for r in ranges:
            assert table._get_key(("k", r)) is None

    def test_window_ranges(self, *, table):
        table.window = Mock(name="window", autospec=Window)
        table.window.ranges.return_value = [1, 2, 3]
        assert list(table._window_ranges(300.3)) == [1, 2, 3]

    def mock_ranges(self, table, ranges=[1.1, 1.2, 1.3]):  # noqa
        table._window_ranges = Mock(name="_window_ranges")
        table._window_ranges.return_value = ranges
        return ranges

    def mock_no_ranges(self, table, ranges=[]):  # noqa
        table._window_ranges = Mock(name="_window_ranges")
        table._window_ranges.return_value = ranges
        return ranges

    def test_relative_now(self, *, table):
        event = Mock(name="event", autospec=Event)
        table._partition_latest_timestamp[event.message.partition] = 30.3
        assert table._relative_now(event) == 30.3

    def test_relative_now__no_event(self, *, table):
        with patch("faust.tables.base.current_event") as ce:
            ce.return_value = None
            with patch("time.time") as time:
                assert table._relative_now(None) is time()

    def test_relative_event(self, *, table):
        event = Mock(name="event", autospec=Event)
        assert table._relative_event(event) is event.message.timestamp

    def test_relative_event__raises_if_no_event(self, *, table):
        with patch("faust.tables.base.current_event") as current_event:
            current_event.return_value = None
            with pytest.raises(RuntimeError):
                table._relative_event(None)

    def test_relative_field(self, *, table):
        user = User("foo", "bar")
        event = Mock(name="event", autospec=Event)
        event.value = user
        assert table._relative_field(User.id)(event) == "foo"

    def test_relative_field__raises_if_no_event(self, *, table):
        with pytest.raises(RuntimeError):
            table._relative_field(User.id)(event=None)

    def test_relative_timestamp(self, *, table):
        assert (
            table._relative_timestamp(303.3)(Mock(name="event", autospec=Event))
            == 303.3
        )

    def test_windowed_now(self, *, table):
        with patch("faust.tables.base.current_event"):
            table.window = Mock(name="window", autospec=Window)
            table.window.earliest.return_value = 42
            table._get_key = Mock(name="_get_key")
            table._windowed_now("k")
            table._get_key.assert_called_once_with(("k", 42))

    def test_windowed_timestamp(self, *, table):
        table.window = Mock(name="window", autospec=Window)
        table.window.current.return_value = 10.1
        assert not table._windowed_contains("k", 303.3)
        table._set_key(("k", 10.1), 101.1)
        assert table._windowed_timestamp("k", 303.3) == 101.1
        assert table._windowed_contains("k", 303.3)

    def test_windowed_delta(self, *, table):
        event = Mock(name="event", autospec=Event)
        table.window = Mock(name="window", autospec=Window)
        table.window.delta.return_value = 10.1
        table._set_key(("k", 10.1), 101.1)
        assert table._windowed_delta("k", 303.3, event=event) == 101.1

    @pytest.mark.asyncio
    async def test_on_rebalance(self, *, table):
        table._data = Mock(
            name="data",
            autospec=Store,
            on_rebalance=AsyncMock(),
        )
        generation_id = 1
        await table.on_rebalance({TP1}, set(), set(), generation_id)
        table._data.on_rebalance.assert_called_once_with(
            {TP1}, set(), set(), generation_id
        )

    @pytest.mark.asyncio
    async def test_on_changelog_event(self, *, table):
        event = Mock(name="event", autospec=Event)
        table._on_changelog_event = None
        await table.on_changelog_event(event)
        table._on_changelog_event = AsyncMock(name="callback")
        await table.on_changelog_event(event)
        table._on_changelog_event.assert_called_once_with(event)

    def test_label(self, *, table):
        assert label(table)

    def test_shortlabel(self, *, table):
        assert shortlabel(table)

    def test_apply_changelog_batch(self, *, table):
        table._data = Mock(name="data", autospec=Store)
        table.apply_changelog_batch([1, 2, 3])
        table._data.apply_changelog_batch.assert_called_once_with(
            [1, 2, 3],
            to_key=table._to_key,
            to_value=table._to_value,
        )

    def test_to_key(self, *, table):
        assert table._to_key([1, 2, 3]) == (1, 2, 3)
        assert table._to_key(1) == 1

    def test_to_value(self, *, table):
        v = Mock(name="v")
        assert table._to_value(v) is v

    def test__human_channel(self, *, table):
        assert table._human_channel()

    def test_repr_info(self, *, table):
        assert table._repr_info() == table.name

    def test_partition_for_key__partitioner(self, *, table, app):
        table.use_partitioner = True

        partition = None
        assert table.partition_for_key("k") is partition


class Test_Collection_window_corrections:
    @pytest.fixture
    def table(self, *, app):
        return app.Table("name")

    def _event(self, app, *, topic="topic", partition=0, offset=0):
        message = Message(
            topic=topic,
            key="k",
            value=1,
            partition=partition,
            offset=offset,
            checksum=None,
            timestamp=0.0,
            timestamp_type=0,
            headers={},
        )
        return Event(app=app, key="k", value=1, headers={}, message=message)

    @contextmanager
    def _current_event(self, event):
        with patch("faust.tables.base.current_event", return_value=event):
            yield

    def test_correction_marker_survives_changelog_roundtrip(self, *, table):
        marker = table._correction_marker("k", (1.1, 1.4), ("topic", 0, 41))
        restored = table._to_key(json.loads(json.dumps(marker)))
        assert restored == marker
        assert hash(restored) == hash(marker)

    def test_open_window_write_is_not_delayed(self, *, app, table):
        table.window = TumblingWindow(60.0, expires=300.0)
        now = time.time()
        event = self._event(app, partition=0, offset=1)
        with self._current_event(event):
            table._set_windowed("k", 5, now)
        window_range = table.window.current(now)
        assert table.data[("k", window_range)] == 5
        assert table._window_delivery_queue.empty()

    @pytest.mark.asyncio
    async def test_correction_redelivers_closed_window_once(self, *, app, table):
        on_window_close = table._on_window_close = AsyncMock(name="on_window_close")
        table.window = TumblingWindow(60.0, expires=300.0)
        now = time.time()
        window_range = table.window.current(now)
        # window was already delivered on this partition
        table._partition_last_closed_window[0] = window_range[0]
        table.data[("k", window_range)] = 10

        event = self._event(app, partition=0, offset=41)
        with self._current_event(event):
            # retract the old reading, record the new one
            table._apply_window_op(operator.sub, "k", 10, now)
            table._apply_window_op(operator.add, "k", 12, now)

        assert table.data[("k", window_range)] == 12

        await table._flush_window_deliveries()

        # both writes of the same correction coalesce into one delivery,
        # carrying the final corrected value
        on_window_close.assert_called_once_with(("k", window_range), 12)
        marker = table._correction_marker("k", window_range, ("topic", 0, 41))
        assert marker in table.data

    @pytest.mark.asyncio
    async def test_correction_replay_after_recovery_keeps_published_version(
        self, *, app, table
    ):
        table._on_window_close = AsyncMock(name="on_window_close")
        table.window = TumblingWindow(60.0, expires=300.0)
        now = time.time()
        window_range = table.window.current(now)
        table._partition_last_closed_window[0] = window_range[0]
        table.data[("k", window_range)] = 10

        event = self._event(app, partition=0, offset=41)
        with self._current_event(event):
            table._apply_window_op(operator.sub, "k", 10, now)
            table._apply_window_op(operator.add, "k", 12, now)
        await table._flush_window_deliveries()
        published = table.data[("k", window_range)]
        assert published == 12

        # reopen the table: the store (including correction markers) is
        # recovered from the changelog, in-memory state starts empty.
        recovered = Table(app, name="name")
        recovered.window = table.window
        recovered._data = table.data
        recovered._partition_last_closed_window[0] = window_range[0]
        on_window_close = recovered._on_window_close = AsyncMock(
            name="on_window_close"
        )

        # the same correction arrives again
        with self._current_event(event):
            recovered._apply_window_op(operator.sub, "k", 10, now)
            recovered._apply_window_op(operator.add, "k", 12, now)
        await recovered._flush_window_deliveries()

        # the published version is kept: not corrected twice, and the
        # correction is not delivered again
        assert recovered.data[("k", window_range)] == published
        on_window_close.assert_not_called()

    @pytest.mark.asyncio
    async def test_correction_redelivers_each_closed_hopping_window_once(
        self, *, app, table
    ):
        on_window_close = table._on_window_close = AsyncMock(name="on_window_close")
        table.window = HoppingWindow(60.0, 10.0, expires=300.0)
        now = time.time()
        ranges = table.window.ranges(now)
        assert len(ranges) > 1
        table._partition_last_closed_window[0] = max(r[0] for r in ranges)
        for window_range in ranges:
            table.data[("k", window_range)] = 10

        event = self._event(app, partition=0, offset=9)
        with self._current_event(event):
            table._apply_window_op(operator.sub, "k", 10, now)
            table._apply_window_op(operator.add, "k", 12, now)
        await table._flush_window_deliveries()

        assert on_window_close.call_count == len(ranges)
        for window_range in ranges:
            assert call(("k", window_range), 12) in on_window_close.mock_calls

    @pytest.mark.asyncio
    async def test_set_windowed_on_closed_window_corrects(self, *, app, table):
        on_window_close = table._on_window_close = AsyncMock(name="on_window_close")
        table.window = TumblingWindow(60.0, expires=300.0)
        now = time.time()
        window_range = table.window.current(now)
        table._partition_last_closed_window[0] = window_range[0]
        table.data[("k", window_range)] = 10

        event = self._event(app, partition=0, offset=3)
        with self._current_event(event):
            table._set_windowed("k", 42, now)

        assert table.data[("k", window_range)] == 42
        await table._flush_window_deliveries()
        on_window_close.assert_called_once_with(("k", window_range), 42)

    @pytest.mark.asyncio
    async def test_late_record_points_at_original_delivery(self, *, app, table):
        on_window_late = table._on_window_late = AsyncMock(name="on_window_late")
        table.window = TumblingWindow(60.0, expires=300.0)
        now = time.time()
        # window closed long ago: beyond the correctable period
        ts = now - 1000.0
        window_range = table.window.current(ts)
        table._partition_last_closed_window[0] = window_range[0]

        event = self._event(app, partition=0, offset=7)
        with self._current_event(event):
            table._apply_window_op(operator.add, "k", 5, ts)

        # the expired window is not recreated
        assert ("k", window_range) not in table.data

        await table._flush_window_deliveries()

        # one late record, pointing at the original delivery
        on_window_late.assert_called_once_with(("k", window_range), 5)

        # the same correction does not produce a second late record
        with self._current_event(event):
            table._apply_window_op(operator.add, "k", 5, ts)
        await table._flush_window_deliveries()
        on_window_late.assert_called_once_with(("k", window_range), 5)

    @pytest.mark.asyncio
    async def test_late_record_reports_latest_value_of_correction(
        self, *, app, table
    ):
        on_window_late = table._on_window_late = AsyncMock(name="on_window_late")
        table.window = TumblingWindow(60.0, expires=300.0)
        now = time.time()
        ts = now - 1000.0
        window_range = table.window.current(ts)
        table._partition_last_closed_window[0] = window_range[0]

        event = self._event(app, partition=0, offset=8)
        with self._current_event(event):
            table._apply_window_op(operator.sub, "k", 10, ts)
            table._apply_window_op(operator.add, "k", 12, ts)

        await table._flush_window_deliveries()
        on_window_late.assert_called_once_with(("k", window_range), 12)

    @pytest.mark.asyncio
    async def test_on_window_late__default(self, *, table):
        assert table._on_window_late is None
        await table.on_window_late(("boo", (1.1, 1.4)), "BOO")

    @pytest.mark.asyncio
    async def test_window_close_progress_is_per_partition(self, *, app, table):
        on_window_close = table._on_window_close = AsyncMock(name="on_window_close")
        table.window = Mock(name="window")
        table._window_ranges = Mock(name="_window_ranges", return_value=[])
        table._data = {
            ("boo", (1.1, 1.4)): "BOO",
            ("zoo", (1.1, 1.4)): "ZOO",
        }
        # partition 1 (line B) closes the period first; partition 0
        # (line A) must still deliver its own keys for the same window.
        table._partition_timestamps = {1: [1.4], 0: [1.4]}
        table._partition_timestamp_keys = {
            (1, 1.4): [("zoo", (1.1, 1.4))],
            (0, 1.4): [("boo", (1.1, 1.4))],
        }
        table.window.stale = Mock(name="stale", return_value=True)

        await table._del_old_keys()

        on_window_close.assert_has_calls(
            [
                call.__bool__(),
                call(("zoo", (1.1, 1.4)), "ZOO"),
                call.__bool__(),
                call(("boo", (1.1, 1.4)), "BOO"),
            ]
        )
        assert table._partition_last_closed_window[0] == 1.1
        assert table._partition_last_closed_window[1] == 1.1
        assert table.last_closed_window == 1.1
