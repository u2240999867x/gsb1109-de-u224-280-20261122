"""Base class Collection for Table and future data structures."""

import abc
import asyncio
import time
from collections import defaultdict
from contextlib import suppress
from datetime import datetime
from heapq import heappop, heappush
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSet,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
    no_type_check,
)

from mode import Seconds, Service
from mode.utils.futures import maybe_async
from yarl import URL

from faust import joins, stores
from faust.exceptions import PartitionsMismatch, TablePartitionNotReady
from faust.streams import current_event
from faust.types import (
    TP,
    AppT,
    CodecArg,
    EventT,
    FieldDescriptorT,
    FutureMessage,
    JoinT,
    RecordMetadata,
    SchemaT,
    TopicT,
)
from faust.types.models import ModelArg, ModelT
from faust.types.stores import StoreT
from faust.types.streams import JoinableT, StreamT
from faust.types.tables import (
    ChangelogEventCallback,
    CollectionT,
    RecoverCallback,
    RelativeHandler,
    TableLookup,
    WindowCloseCallback,
)
from faust.types.windows import WindowRange, WindowT

__all__ = ["Collection"]

TABLE_CLEANING = "CLEANING"

E_SOURCE_PARTITIONS_MISMATCH = """\
The source topic {source_topic!r} for table {table_name!r}
has {source_n} partitions, but the changelog
topic {change_topic!r} has {change_n} partitions.

Please make sure the topics have the same number of partitions
by configuring Kafka correctly.
"""


class Collection(Service, CollectionT):
    """Base class for changelog-backed data structures stored in Kafka."""

    _store: Optional[URL]
    _changelog_topic: Optional[TopicT]
    _partition_timestamp_keys: MutableMapping[
        Tuple[int, float], MutableSet[Tuple[Any, WindowRange]]
    ]
    _partition_timestamps: MutableMapping[int, List[float]]
    _partition_latest_timestamp: MutableMapping[int, float]
    _recover_callbacks: MutableSet[RecoverCallback]
    _data: Optional[StoreT] = None
    _changelog_compacting: Optional[bool] = True
    _changelog_deleting: Optional[bool] = None

    #: Generation of the rebalance that recovery state below belongs to.
    _recovery_generation_id: int = -1
    #: Changelog partitions that did not finish recovery yet.
    _unready_partitions: Set[int]
    #: Changelog partitions owned by this table after the last rebalance.
    _owned_partitions: Set[int]

    @abc.abstractmethod
    def _has_key(self, key: Any) -> bool:  # pragma: no cover
        ...

    @abc.abstractmethod
    def _get_key(self, key: Any) -> Any:  # pragma: no cover
        ...

    @abc.abstractmethod
    def _set_key(self, key: Any, value: Any) -> None:  # pragma: no cover
        ...

    @abc.abstractmethod
    def _del_key(self, key: Any) -> None:  # pragma: no cover
        ...

    def __init__(
        self,
        app: AppT,
        *,
        name: Optional[str] = None,
        default: Optional[Callable[[], Any]] = None,
        store: Optional[Union[str, URL]] = None,
        schema: Optional[SchemaT] = None,
        key_type: Optional[ModelArg] = None,
        value_type: Optional[ModelArg] = None,
        partitions: Optional[int] = None,
        window: Optional[WindowT] = None,
        changelog_topic: Optional[TopicT] = None,
        help: Optional[str] = None,
        on_recover: Optional[RecoverCallback] = None,
        on_changelog_event: Optional[ChangelogEventCallback] = None,
        recovery_buffer_size: int = 1000,
        standby_buffer_size: Optional[int] = None,
        extra_topic_configs: Optional[Mapping[str, Any]] = None,
        recover_callbacks: Optional[Set[RecoverCallback]] = None,
        options: Optional[Mapping[str, Any]] = None,
        use_partitioner: bool = False,
        on_window_close: Optional[WindowCloseCallback] = None,
        is_global: bool = False,
        synchronize_all_active_partitions: bool = False,
        **kwargs: Any,
    ) -> None:
        # Do *not* pass ``loop=app.loop`` here: tables are declared at module
        # scope, and reading ``app.loop`` before a loop is running pins the App
        # to a loop that will never be run.  ``mode.Service`` late-binds the
        # loop on first access.  See the note in ``faust.agents.agent``.
        Service.__init__(self, **kwargs)
        self.app = app
        self.name = cast(str, name)  # set lazily so CAN BE NONE!
        self.default = default
        self._store = URL(store) if store else None
        self.schema = schema
        self.key_type = key_type
        self.value_type = value_type
        self.partitions = partitions
        self.window = window
        self._changelog_topic = changelog_topic
        self.extra_topic_configs = extra_topic_configs or {}
        self.help = help or ""
        self._on_changelog_event = on_changelog_event
        self.recovery_buffer_size = recovery_buffer_size
        self.standby_buffer_size = standby_buffer_size or recovery_buffer_size
        self.use_partitioner = use_partitioner
        self._on_window_close = on_window_close
        self.last_closed_window = 0.0
        self.is_global = is_global
        self.synchronize_all_active_partitions = synchronize_all_active_partitions
        if self.synchronize_all_active_partitions:
            assert self.is_global
        assert self.recovery_buffer_size > 0 and self.standby_buffer_size > 0

        self.options = options

        # Setting Serializers from key_type and value_type
        # Possible values json and raw
        # Fallback to json
        self.key_serializer = self._serializer_from_type(self.key_type)
        self.value_serializer = self._serializer_from_type(self.value_type)

        # Table key expiration
        self._partition_timestamp_keys = defaultdict(set)
        self._partition_timestamps = defaultdict(list)
        self._partition_latest_timestamp = defaultdict(int)

        self._recover_callbacks = set(recover_callbacks or [])
        if on_recover:
            self.on_recover(on_recover)

        self._recovery_generation_id = -1
        self._unready_partitions = set()
        self._owned_partitions = set()

        # Aliases
        self._sensor_on_get = self.app.sensors.on_table_get
        self._sensor_on_set = self.app.sensors.on_table_set
        self._sensor_on_del = self.app.sensors.on_table_del

        self._verified_source_topics_for_partitions: Set[str] = set()

    def _serializer_from_type(self, typ: Optional[ModelArg]) -> Optional[CodecArg]:
        if typ is bytes:
            return "raw"
        serializer = None
        with suppress(AttributeError):
            serializer = typ._options.serializer  # type: ignore
        return serializer or "json"

    def __hash__(self) -> int:
        # We have to override MutableMapping __hash__, so that this table
        # can be registered in the app.tables mapping.
        return object.__hash__(self)

    def _new_store(self) -> StoreT:
        return self._new_store_by_url(self._store or self.app.conf.store)

    def _new_store_by_url(self, url: Union[str, URL]) -> StoreT:
        return stores.by_url(url)(
            url,
            self.app,
            self,
            table_name=self.name,
            key_type=self.key_type,
            key_serializer=self.key_serializer,
            value_serializer=self.value_serializer,
            value_type=self.value_type,
            loop=self.loop,
            options=self.options,
        )

    @property  # type: ignore
    @no_type_check  # XXX https://github.com/python/mypy/issues/4125
    def data(self) -> StoreT:
        """Underlying table storage."""
        if self._data is None:
            self._data = self._new_store()
        return self._data

    async def on_start(self) -> None:
        """Call when table starts."""
        await self.add_runtime_dependency(self.data)
        await self.changelog_topic.maybe_declare()

    def on_recover(self, fun: RecoverCallback) -> RecoverCallback:
        """Add function as callback to be called on table recovery."""
        assert fun not in self._recover_callbacks
        self._recover_callbacks.add(fun)
        return fun

    def info(self) -> Mapping[str, Any]:
        """Return table attributes as dictionary."""
        # Used to recreate object in .clone()
        return {
            "app": self.app,
            "name": self.name,
            "default": self.default,
            "store": self._store,
            "schema": self.schema,
            "key_type": self.key_type,
            "value_type": self.value_type,
            "partitions": self.partitions,
            "window": self.window,
            "changelog_topic": self._changelog_topic,
            "recover_callbacks": self._recover_callbacks,
            "on_changelog_event": self._on_changelog_event,
            "recovery_buffer_size": self.recovery_buffer_size,
            "standby_buffer_size": self.standby_buffer_size,
            "extra_topic_configs": self.extra_topic_configs,
            "use_partitioner": self.use_partitioner,
        }

    def persisted_offset(self, tp: TP) -> Optional[int]:
        """Return the last persisted offset for topic partition."""
        return self.data.persisted_offset(tp)

    async def need_active_standby_for(self, tp: TP) -> bool:
        """Return :const:`False` if we have access to partition data."""
        return await self.data.need_active_standby_for(tp)

    def changelog_partition_for_key(self, key: Any) -> int:
        """Return the changelog partition a key belongs to.

        When the key was seen before, the partition it was last applied
        from is authoritative: it is the partition a lookup result has to
        be matched against.  A deleted key keeps its deletion position for
        exactly this purpose.  Only unknown keys are hashed with the
        producer partitioner.
        """
        key_partition = getattr(self.data, "key_partition", None)
        if callable(key_partition):
            recorded = key_partition(key)
            if isinstance(recorded, int) and not isinstance(recorded, bool):
                return recorded
        recorded = self._deleted_partition(key)
        if recorded is not None:
            return recorded
        try:
            topic = self.changelog_topic.get_topic_name()
            destination = self.app.producer.key_partition(
                topic, self._serialize_key(key)
            )
        except Exception:
            destination = None
        if destination is None:
            return -1
        return cast(int, destination.partition)

    def _deleted_partition(self, key: Any) -> Optional[int]:
        deleted_positions = getattr(self.data, "deleted_positions", None)
        if callable(deleted_positions):
            position = deleted_positions(key)
            if (
                isinstance(position, tuple)
                and len(position) == 2
                and isinstance(position[0], int)
            ):
                return position[0]
        return None

    def _serialize_key(self, key: Any) -> Optional[bytes]:
        return self.changelog_topic.prepare_key(key, self.key_serializer)[0]

    def partition_ready(self, partition: int) -> bool:
        """Return :const:`True` if this partition can answer lookups.

        A partition is ready once recovery has completed for every
        takeover that has assigned it.  A partition that was just handed
        over (or taken back) is not ready until its changelog has been
        replayed to the highwater again; partitions that merely continued
        to be owned by this worker are never blocked by another
        partition's takeover.

        Partitions this table no longer owns are unreadable: the worker is
        no longer allowed to answer for them, even if an older recovery
        callback or a late request still refers to them.
        """
        if self._recovery_generation_id < 0:
            # No rebalance has happened yet: there is no takeover state to
            # check, so the locally applied data is the best answer there is.
            return True
        return (
            partition in self._owned_partitions
            and partition not in self._unready_partitions
        )

    def ensure_partition_ready(self, partition: int) -> None:
        """Raise :exc:`~faust.exceptions.TablePartitionNotReady` if unready."""
        if not self.partition_ready(partition):
            raise TablePartitionNotReady(
                "table partition is being taken over",
                table=self.name,
                partition=partition,
            )

    def partition_offset(self, partition: int) -> int:
        """Return the last changelog offset applied for a partition."""
        return self._applied_offset(partition)

    def lookup(self, key: Any) -> TableLookup:
        """Look up a key in the table, bound to its changelog partition.

        The partition is resolved with the same partitioner used to write
        the changelog, so the result can be validated against the partition
        that answered it.  Values in that partition must have been applied
        for the lookup to happen; otherwise
        :exc:`~faust.exceptions.TablePartitionNotReady` is raised so the
        caller can retry later instead of serving stale data.

        The returned object re-reads the table when its ``value`` and
        ``offset`` are accessed, so a lookup retained while a partition is
        being taken over resolves against the position applied when it is
        finally consumed.
        """
        partition = self.changelog_partition_for_key(key)
        self.ensure_partition_ready(partition)
        return TableLookup(self, key, partition, bound=True)

    def lookup_value(self, key: Any, partition: int) -> Any:
        """Return the current value for a lookup bound to ``partition``."""
        self.ensure_partition_ready(partition)
        return self._data_lookup(key, partition).value

    def lookup_offset(self, key: Any, partition: int) -> int:
        """Return the applied changelog offset for a bound lookup."""
        self.ensure_partition_ready(partition)
        return self._data_lookup(key, partition).offset

    def _data_lookup(self, key: Any, partition: int) -> TableLookup:
        lookup = getattr(self.data, "lookup", None)
        if callable(lookup):
            result = lookup(key, partition)
            if isinstance(result, TableLookup):
                return result
        offset = self._applied_offset(partition)
        value = self.data.get(key)
        return TableLookup(self, key, partition, value, offset)

    def _applied_offset(self, partition: int) -> int:
        applied_offset = getattr(self.data, "applied_offset", None)
        if callable(applied_offset):
            offset = applied_offset(
                TP(self.changelog_topic.get_topic_name(), partition)
            )
            if isinstance(offset, int) and not isinstance(offset, bool):
                return offset
        return -1

    @property
    def recovery_generation_id(self) -> int:
        """Return the rebalance generation recovery state belongs to."""
        return self._recovery_generation_id

    def begin_recovery(
        self, generation_id: int, assigned: Set[TP], newly_assigned: Set[TP]
    ) -> bool:
        """Start recovery state for a new rebalance generation.

        Returns :const:`False` when this call belongs to a rebalance that
        was superseded by a newer one, in which case its callbacks must not
        run: they would otherwise apply results (or prune state) computed
        for an assignment that is no longer current.

        Only partitions this worker did not already own are marked as
        waiting for recovery.  A partition that continued to be assigned
        keeps answering lookups from its current position while another
        partition is being taken over.
        """
        if generation_id < self._recovery_generation_id:
            return False
        topic = self.changelog_topic.get_topic_name()
        assigned_partitions = {tp.partition for tp in assigned if tp.topic == topic}
        if generation_id == self._recovery_generation_id:
            # Same generation re-entered (e.g. recovery restarted): keep the
            # takeover state already computed for it, it has not completed.
            new_partitions = self._unready_partitions.intersection(
                assigned_partitions
            )
        else:
            new_partitions = {
                tp.partition
                for tp in newly_assigned
                if tp.topic == topic and tp.partition in assigned_partitions
            }
        self._recovery_generation_id = generation_id
        self._owned_partitions = assigned_partitions
        self._unready_partitions = new_partitions
        return True

    def complete_recovery(
        self, generation_id: int, active_tps: Set[TP]
    ) -> bool:
        """Mark a generation's recovered partitions as ready.

        Returns :const:`False` for a stale generation, leaving the state of
        the current one untouched.
        """
        if generation_id != self._recovery_generation_id:
            return False
        recovered = {
            tp.partition
            for tp in active_tps
            if tp.topic == self.changelog_topic.get_topic_name()
        }
        self._unready_partitions.difference_update(recovered)
        return True

    def begin_recovery_callbacks(self, generation_id: int) -> bool:
        """Return :const:`True` if recovery callbacks should run now."""
        return generation_id == self._recovery_generation_id

    def end_recovery_callbacks(self, generation_id: int) -> None:
        """Call after recovery callbacks completed."""

    async def run_recover_callbacks(self, generation_id: int) -> None:
        """Run recovery callbacks for a generation, without blocking replay.

        The callbacks are user code and may block for an arbitrary time
        (an ``on_recover`` handler awaiting another node, for example).
        They must therefore not run inside the recovery loop: a callback
        that is still running when the next rebalance starts must not
        prevent that rebalance from applying changelog entries, nor may
        its completion be mistaken for the newer generation's recovery.
        """
        if not self.begin_recovery_callbacks(generation_id):
            return
        try:
            for fun in self._recover_callbacks:
                await fun()
        finally:
            self.end_recovery_callbacks(generation_id)

    def reset_state(self) -> None:
        """Reset local state."""
        self.data.reset_state()

    def send_changelog(
        self,
        partition: Optional[int],
        key: Any,
        value: Any,
        key_serializer: Optional[CodecArg] = None,
        value_serializer: Optional[CodecArg] = None,
    ) -> FutureMessage:
        """Send modification event to changelog topic."""
        if key_serializer is None:
            key_serializer = self.key_serializer
        if value_serializer is None:
            value_serializer = self.value_serializer
        return self.changelog_topic.send_soon(
            key=key,
            value=value,
            partition=partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=self._on_changelog_sent,
            # Ensures final partition number is ready in ret.message.partition
            eager_partitioning=True,
        )

    def _send_changelog(
        self,
        event: Optional[EventT],
        key: Any,
        value: Any,
        key_serializer: Optional[CodecArg] = None,
        value_serializer: Optional[CodecArg] = None,
    ) -> None:
        # XXX compat version of send_changelog that needs event argument.
        if event is None:
            raise RuntimeError("Cannot modify table outside of agent/stream.")
        self.send_changelog(
            event.message.partition, key, value, key_serializer, value_serializer
        )

    def partition_for_key(self, key: Any) -> Optional[int]:
        """Return partition number for table key.

        Always returns :const:`None` when :attr:`use_partitioner`
        is enabled.

        Returns:
            Optional[int]: specific partition or :const:`None` if
                the producer should select partition using its partitioner.
        """
        if self.use_partitioner:
            return None
        else:
            event = current_event()
            if event is None:
                raise TypeError(
                    "Cannot modify table key from outside of stream iteration"
                )
            self._verify_source_topic_partitions(event.message.topic)
            return event.message.partition

    def _verify_source_topic_partitions(self, source_topic: str) -> None:
        # This was formerly wrapped in an lru_cache. The linter sees issues with this
        # as an instance stays cached.
        # This is why we implement a non lru_cached lookup which checks if the
        # function has already been executed once on the instance level.

        if source_topic in self._verified_source_topics_for_partitions:
            return

        change_topic = self.changelog_topic_name
        source_n = self.app.consumer.topic_partitions(source_topic)
        if source_n is not None:
            change_n = self.app.consumer.topic_partitions(change_topic)
            if change_n is not None:
                if source_n != change_n:
                    raise PartitionsMismatch(
                        E_SOURCE_PARTITIONS_MISMATCH.format(
                            source_topic=source_topic,
                            table_name=self.name,
                            source_n=source_n,
                            change_topic=change_topic,
                            change_n=change_n,
                        ),
                    )

        self._verified_source_topics_for_partitions.add(source_topic)

    def _on_changelog_sent(self, fut: FutureMessage) -> None:
        # This is what keeps the offset in RocksDB so that at startup
        # we know what offsets we already have data for in the database.
        #
        # Kafka Streams has a global ".checkpoint" file, but we store
        # it in each individual RocksDB database file.
        # Every partition in the table will have its own database file,
        #  this is required as partitions can easily move from and to
        #  machine as nodes die and recover.
        res: RecordMetadata = fut.result()
        if self.app.in_transaction:
            # for exactly-once semantics we only write the
            # persisted offset to RocksDB on disk when that partition
            # is committed.
            self.app.tables.persist_offset_on_commit(
                self.data, res.topic_partition, res.offset
            )
        else:
            # for normal processing (at-least-once) we just write
            # the persisted offset immediately.
            self.data.set_persisted_offset(res.topic_partition, res.offset)

    @Service.task
    @Service.transitions_to(TABLE_CLEANING)
    async def _clean_data(self) -> None:
        interval = self.app.conf.table_cleanup_interval
        if self._should_expire_keys():
            await self.sleep(interval)
            async for sleep_time in self.itertimer(interval, name="table_cleanup"):
                await self._del_old_keys()

    async def _del_old_keys(self) -> None:
        window = cast(WindowT, self.window)
        assert window
        for partition, timestamps in self._partition_timestamps.items():
            while timestamps and window.stale(timestamps[0], time.time()):
                timestamp = heappop(timestamps)
                triggered_windows = [
                    # XXX bug: this lookup can never hit.
                    # ``_partition_timestamp_keys`` is keyed by
                    # ``(partition, range_end)`` -- a ``(int, float)`` pair,
                    # written that way in ``_maybe_set_key_ttl`` and read that
                    # way in ``_maybe_del_key_ttl``.  Here it is looked up by
                    # ``(partition, window_range)`` where ``window_range`` is
                    # the ``(start, end)`` tuple, so no key ever matches and
                    # ``triggered_windows`` is always ``[None, ...]``.  The
                    # consequence is that ``window_data`` stays empty and
                    # ``on_window_close`` never receives the aggregated window
                    # data, only the raw per-key value.  The correct key is
                    # ``(partition, window_range[1])``; that is a behaviour
                    # change, so it is not made here and the type error is
                    # only silenced.
                    self._partition_timestamp_keys.get(
                        (partition, window_range)  # type: ignore[arg-type]
                    )
                    for window_range in self._window_ranges(timestamp)
                ]
                keys_to_remove = self._partition_timestamp_keys.pop(
                    (partition, timestamp), None
                )
                window_data: Dict[Any, List[Any]] = {}
                if keys_to_remove:
                    for windows in triggered_windows:
                        if windows:
                            for processed_window in windows:
                                # we use set to avoid duplicate element in window's data
                                # window[0] is the window's key
                                # it is not related to window's timestamp
                                # windows are in format:
                                # (key, (window_start, window_end))
                                window_data.setdefault(processed_window[0], []).extend(
                                    self.data.get(processed_window, [])
                                )

                    for key_to_remove in keys_to_remove:
                        value = self.data.pop(key_to_remove, None)
                        if key_to_remove[1][0] > self.last_closed_window:
                            await self.on_window_close(
                                key_to_remove,
                                (
                                    window_data[key_to_remove[0]]
                                    if key_to_remove[0] in window_data
                                    else value
                                ),
                            )
                    self.last_closed_window = max(
                        self.last_closed_window,
                        max(key[1][0] for key in keys_to_remove),
                    )

    async def on_window_close(self, key: Any, value: Any) -> None:
        if self._on_window_close:
            await maybe_async(self._on_window_close(key, value))

    def _should_expire_keys(self) -> bool:
        window = self.window
        return not (window is None or window.expires is None)

    def _maybe_set_key_ttl(self, key: Any, partition: int) -> None:
        if not self._should_expire_keys():
            return
        _, window_range = key
        _, range_end = window_range
        heappush(self._partition_timestamps[partition], range_end)
        self._partition_latest_timestamp[partition] = max(
            self._partition_latest_timestamp[partition], range_end
        )
        self._partition_timestamp_keys[(partition, range_end)].add(key)

    def _maybe_del_key_ttl(self, key: Any, partition: int) -> None:
        if not self._should_expire_keys():
            return
        _, window_range = key
        ts_keys = self._partition_timestamp_keys.get((partition, window_range[1]))
        if ts_keys is not None:
            ts_keys.discard(key)

    def _changelog_topic_name(self) -> str:
        return f"{self.app.conf.id}-{self.name}-changelog"

    def join(self, *fields: FieldDescriptorT) -> StreamT:
        """Right join of this table and another stream/table."""
        return self._join(joins.RightJoin(stream=self, fields=fields))

    def left_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Left join of this table and another stream/table."""
        return self._join(joins.LeftJoin(stream=self, fields=fields))

    def inner_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Inner join of this table and another stream/table."""
        return self._join(joins.InnerJoin(stream=self, fields=fields))

    def outer_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Outer join of this table and another stream/table."""
        return self._join(joins.OuterJoin(stream=self, fields=fields))

    def _join(self, join_strategy: JoinT) -> StreamT:
        # TODO
        raise NotImplementedError("TODO")

    def clone(self, **kwargs: Any) -> Any:
        """Clone table instance."""
        return self.__class__(**{**self.info(), **kwargs})

    def combine(self, *nodes: JoinableT, **kwargs: Any) -> StreamT:
        """Combine tables and streams."""
        # TODO
        raise NotImplementedError("TODO")

    def contribute_to_stream(self, active: StreamT) -> None:
        """Contribute table to stream join."""
        # TODO  See Stream.contribute_to_stream()
        # Should probably connect to Table changelog.
        ...

    async def remove_from_stream(self, stream: StreamT) -> None:
        """Remove table from stream join after stream stopped."""
        # TODO See Stream.remove_from_stream()
        # Should stop any services started to support joining this table
        # with one or more streams.
        ...

    def _new_changelog_topic(
        self,
        *,
        retention: Optional[Seconds] = None,
        compacting: Optional[bool] = None,
        deleting: Optional[bool] = None,
    ) -> TopicT:
        if compacting is None:
            compacting = self._changelog_compacting
        if deleting is None:
            deleting = self._changelog_deleting
        if retention is None and self.window:
            retention = self.window.expires
        return self.app.topic(
            self._changelog_topic_name(),
            schema=self.schema,
            key_type=self.key_type,
            value_type=self.value_type,
            key_serializer=self.key_serializer,
            value_serializer=self.value_serializer,
            partitions=self.partitions,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
            acks=False,
            internal=True,
            config=self.extra_topic_configs,
            # use large buffer size as we do not commit attached messages
            # when reading changelog streams.
            maxsize=131_072,
            allow_empty=True,
        )

    def __copy__(self) -> Any:
        return self.clone()

    def __and__(self, other: Any) -> Any:
        return self.combine(self, other)

    def _apply_window_op(
        self, op: Callable[[Any, Any], Any], key: Any, value: Any, timestamp: float
    ) -> None:
        get_ = self._get_key
        set_ = self._set_key
        for window_range in self._window_ranges(timestamp):
            set_((key, window_range), op(get_((key, window_range)), value))

    def _set_windowed(self, key: Any, value: Any, timestamp: float) -> None:
        for window_range in self._window_ranges(timestamp):
            self._set_key((key, window_range), value)

    def _del_windowed(self, key: Any, timestamp: float) -> None:
        for window_range in self._window_ranges(timestamp):
            self._del_key((key, window_range))

    def _window_ranges(self, timestamp: float) -> Iterator[WindowRange]:
        window = cast(WindowT, self.window)
        for window_range in window.ranges(timestamp):
            yield window_range

    def _relative_now(self, event: Optional[EventT] = None) -> float:
        # get current timestamp
        event = event if event is not None else current_event()
        if event is None:
            return time.time()
        return self._partition_latest_timestamp[event.message.partition]

    def _relative_event(self, event: Optional[EventT] = None) -> float:
        event = event if event is not None else current_event()
        # get event timestamp
        if event is None:
            raise RuntimeError("Operation outside of stream iteration")
        return event.message.timestamp

    def _relative_field(self, field: FieldDescriptorT) -> RelativeHandler:
        def to_value(event: Optional[EventT] = None) -> Union[float, datetime]:
            if event is None:
                raise RuntimeError("Operation outside of stream iteration")
            return field.getattr(cast(ModelT, event.value))

        return to_value

    def _relative_timestamp(self, timestamp: float) -> RelativeHandler:
        def handler(event: Optional[EventT] = None) -> Union[float, datetime]:
            return timestamp

        return handler

    def _windowed_now(self, key: Any) -> Any:
        window = cast(WindowT, self.window)
        return self._get_key((key, window.earliest(self._relative_now())))

    def _windowed_timestamp(self, key: Any, timestamp: float) -> Any:
        window = cast(WindowT, self.window)
        return self._get_key((key, window.current(timestamp)))

    def _windowed_contains(self, key: Any, timestamp: float) -> bool:
        window = cast(WindowT, self.window)
        return self._has_key((key, window.current(timestamp)))

    def _windowed_delta(
        self, key: Any, d: Seconds, event: Optional[EventT] = None
    ) -> Any:
        window = cast(WindowT, self.window)
        return self._get_key(
            (key, window.delta(self._relative_event(event), d)),
        )

    async def on_rebalance(
        self,
        assigned: Set[TP],
        revoked: Set[TP],
        newly_assigned: Set[TP],
        generation_id: int = 0,
    ) -> None:
        """Call when cluster is rebalancing."""
        self.begin_recovery(generation_id, assigned, newly_assigned)
        await self.data.on_rebalance(assigned, revoked, newly_assigned, generation_id)

    async def on_recovery_completed(
        self, active_tps: Set[TP], standby_tps: Set[TP]
    ) -> None:
        """Call when recovery has completed after rebalancing."""
        generation_id = self._recovery_generation_id
        if generation_id >= 0:
            self.complete_recovery(generation_id, active_tps)
        await self.data.on_recovery_completed(active_tps, standby_tps)
        await self.call_recover_callbacks()

    async def on_recovery_completed_for_generation(
        self,
        generation_id: int,
        active_tps: Set[TP],
        standby_tps: Set[TP],
        *,
        run_callbacks: bool = True,
    ) -> None:
        """Complete recovery for one rebalance generation.

        A completion belonging to an older generation is ignored: it would
        prune partitions the current generation has just recovered (or is
        still recovering) and then run callbacks against stale state.
        """
        if generation_id != self._recovery_generation_id:
            return
        self.complete_recovery(generation_id, active_tps)
        await self.data.on_recovery_completed(active_tps, standby_tps)
        if run_callbacks:
            self.schedule_recover_callbacks(generation_id)

    def schedule_recover_callbacks(self, generation_id: int) -> None:
        """Run recovery callbacks in the background.

        Callbacks are user code and may block for an arbitrary time (an
        ``on_recover`` handler waiting for another worker, for example).
        Running them inline would stall changelog replay for the next
        rebalance and make a blocked callback look like a partition that
        has not finished recovering.  They are started as a background
        task instead; a newer generation supersedes an older callback, and
        a callback that finishes after its generation ended is ignored.
        """
        if not self._recover_callbacks:
            return
        asyncio.ensure_future(self.run_recover_callbacks(generation_id))

    async def call_recover_callbacks(self) -> None:
        """Call any configured recovery callbacks after rebalancing."""
        generation_id = self._recovery_generation_id
        if not self.begin_recovery_callbacks(generation_id):
            return
        try:
            for fun in self._recover_callbacks:
                await fun()
        finally:
            self.end_recovery_callbacks(generation_id)

    async def on_changelog_event(self, event: EventT) -> None:
        """Call when a new changelog event is received."""
        if self._on_changelog_event:
            await self._on_changelog_event(event)

    @property
    def label(self) -> str:
        """Return human-readable label used to represent this table."""
        return f"{self.shortlabel}@{self._store}"

    @property
    def shortlabel(self) -> str:
        """Return short label used to represent this table in logs."""
        return f"{type(self).__name__}: {self.name}"

    @property
    def changelog_topic(self) -> TopicT:
        """Return the changelog topic used by this table."""
        if self._changelog_topic is None:
            self._changelog_topic = self._new_changelog_topic()
        return self._changelog_topic

    @changelog_topic.setter
    def changelog_topic(self, topic: TopicT) -> None:
        self._changelog_topic = topic

    @property
    def changelog_topic_name(self) -> str:
        return self.changelog_topic.get_topic_name()

    def apply_changelog_batch(self, batch: Iterable[EventT]) -> None:
        """Apply batch of events from changelog topic local table storage."""
        self.data.apply_changelog_batch(
            batch,
            to_key=self._to_key,
            to_value=self._to_value,
        )

    def _to_key(self, k: Any) -> Any:
        if isinstance(k, list):
            # Lists are not hashable, and windowed-keys are json
            # serialized into a list.
            return tuple(tuple(v) if isinstance(v, list) else v for v in k)
        return k

    def _to_value(self, v: Any) -> Any:
        return v

    def _human_channel(self) -> str:
        return f"{type(self).__name__}: {self.name}"

    def _repr_info(self) -> str:
        return self.name
