import asyncio
import collections
import copy
import logging
import time
from collections.abc import Sequence
from typing import NamedTuple

from aiokafka.errors import (
    KafkaTimeoutError,
    LeaderNotAvailableError,
    NotLeaderForPartitionError,
    ProducerClosed,
    RecordDeliveryUnknownError,
)
from aiokafka.listeners import ProducerListener, _collect_callbacks
from aiokafka.record.default_records import DefaultRecordBatchBuilder
from aiokafka.structs import (
    RecordDelivery,
    RecordErrorDetail,
    RecordMetadata,
    RecordStatus,
)
from aiokafka.util import create_future, get_running_loop

from .rejection import rejection_retry_enabled

log = logging.getLogger(__name__)


class StoredRecord(NamedTuple):
    index: int
    timestamp: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes | None], ...]


def _call_listeners(callbacks, **kwargs):
    for callback in callbacks:
        try:
            callback(**kwargs)
        except Exception:
            log.exception("Producer listener callback failed")


class BatchBuilder:
    def __init__(
        self,
        batch_size,
        compression_type,
        *,
        is_transactional=0,
        key_serializer=None,
        value_serializer=None,
    ):
        self._builder = DefaultRecordBatchBuilder(
            2,
            compression_type,
            is_transactional=is_transactional,
            producer_id=-1,
            producer_epoch=-1,
            base_sequence=0,
            batch_size=batch_size,
        )
        self._relative_offset = 0
        self._buffer = None
        self._closed = False
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._batch_size = batch_size
        self._compression_type = compression_type
        self._is_transactional = is_transactional
        self._stored_records: list[StoredRecord] = []
        self._delivery_report_provider = None

    @property
    def stored_records(self) -> tuple[StoredRecord, ...]:
        return tuple(self._stored_records)

    @property
    def record_results(self):
        provider = self._delivery_report_provider
        return provider.record_results if provider is not None else ()

    @property
    def delivery_results(self):
        return self.record_results

    @property
    def record_delivery_results(self):
        return self.record_results

    @property
    def delivery_report(self):
        provider = self._delivery_report_provider
        return provider if provider is not None else None

    @property
    def results(self):
        return self.record_results

    def result_for_index(self, index):
        provider = self._delivery_report_provider
        return provider._record_states.get(index) if provider is not None else None

    def record_result_for(self, index_or_metadata):
        index = getattr(index_or_metadata, "offset", index_or_metadata)
        return self.result_for_index(index)

    get_record_result = record_result_for
    result_for = record_result_for

    def _serialize(self, key, value):
        if self._key_serializer is None:
            serialized_key = key
        else:
            serialized_key = self._key_serializer(key)
        if self._value_serializer is None:
            serialized_value = value
        else:
            serialized_value = self._value_serializer(value)

        return serialized_key, serialized_value

    def append(self, *, timestamp, key, value, headers: Sequence = []):
        """Add a message to the batch.

        Arguments:
            timestamp (float or None): epoch timestamp in seconds. If None,
                the timestamp will be set to the current time.
            key (bytes or None): the message key. `key` and `value` may not
                both be None.
            value (bytes or None): the message value. `key` and `value` may not
                both be None.

        Returns:
            If the message was successfully added, returns a metadata object
            with crc, offset, size, and timestamp fields. If the batch is full
            or closed, returns None.
        """
        if headers is None:
            headers = []
        if self._closed:
            return None

        key_bytes, value_bytes = self._serialize(key, value)
        metadata = self._builder.append(
            self._relative_offset,
            timestamp,
            key=key_bytes,
            value=value_bytes,
            headers=headers,
        )

        # Check if we could add the message
        if metadata is None:
            self.close()
            return None

        index = self._relative_offset
        self._stored_records.append(
            StoredRecord(
                index=index,
                timestamp=metadata.timestamp,
                key=key_bytes,
                value=value_bytes,
                headers=tuple(headers),
            )
        )
        self._relative_offset += 1
        return metadata

    def copy_with_indices(self, indices):
        """Build an unbuilt copy containing only the given original indices."""
        indices = tuple(indices)
        missing = [index for index in indices if index >= len(self._stored_records)]
        if missing:
            raise IndexError(f"Record indices are out of range: {missing}")

        copy = BatchBuilder(
            self._batch_size,
            self._compression_type,
            is_transactional=self._is_transactional,
        )
        metadata_by_index = {}
        for new_index, original_index in enumerate(indices):
            record = self._stored_records[original_index]
            metadata = copy.append(
                timestamp=record.timestamp,
                key=record.key,
                value=record.value,
                headers=list(record.headers),
            )
            if metadata is None or metadata.offset != new_index:
                raise RuntimeError("Failed to rebuild rejected-record subset")
            metadata_by_index[original_index] = metadata
        return copy, metadata_by_index

    def close(self):
        """Close the batch to further updates.

        Closing the batch before submitting to the producer ensures that no
        messages are added via the ``producer.send()`` interface. To gracefully
        support both the batch and individual message interfaces, leave the
        batch open. For complete control over the batch's contents, close
        before submission. Closing a batch has no effect on when it's sent to
        the broker.

        A batch may not be reopened after it's closed.
        """
        if self._closed:
            return
        self._closed = True

    def _set_producer_state(self, producer_id, producer_epoch, base_sequence):
        assert type(self._builder) is DefaultRecordBatchBuilder
        self._builder.set_producer_state(producer_id, producer_epoch, base_sequence)

    def _build(self):
        self.close()
        if self._buffer is None:
            self._buffer = self._builder.build()
            del self._builder  # We may only call self._builder.build() once!
        return self._buffer

    def size(self):
        """Get the size of batch in bytes."""
        if self._buffer is not None:
            return len(self._buffer)
        else:
            return self._builder.size()

    def record_count(self):
        """Get the number of records in the batch."""
        return self._relative_offset

    def closed(self):
        """Indicates if the builder is already closed"""
        return self._closed


class MessageBatch:
    """This class encapsulate operations with batch of produce messages"""

    def __init__(self, tp, builder, ttl, linger_time, callbacks):
        self._builder = builder
        self._tp = tp
        self._ttl = ttl
        self._linger_time = linger_time
        self._callbacks = callbacks
        self._ctime = time.monotonic()
        self._queued_at = self._ctime
        self._dispatched_at = None

        # Waiters
        # Set when messages are delivered to Kafka based on ACK setting
        self.future = create_future()
        self._msg_futures = []
        # Set when sender takes this batch
        self._drain_waiter = create_future()
        self._retry_count = 0
        self._record_states: dict[int, RecordDelivery] = {}
        self._futures_by_index: dict[int, asyncio.Future] = {}
        self._indices_by_future: dict[asyncio.Future, int] = {}
        self._original_indices = list(
            range(len(getattr(builder, "stored_records", ())))
        )
        self._delivery_uncertain = False
        self._raw_record_errors: tuple[RecordErrorDetail, ...] = ()
        self._rejection_split_applied = False
        self._retry_rejected_records: bool | None = getattr(
            builder, "_retry_rejected_records", None
        )
        self._producer_id = -1
        self._producer_epoch = -1
        self._base_sequence: int | None = None

        for stored_record in getattr(builder, "stored_records", ()):
            self._ensure_record_state(stored_record.index)
        if hasattr(builder, "_delivery_report_provider"):
            builder._delivery_report_provider = self

    def _ensure_record_state(self, index):
        state = self._record_states.get(index)
        if state is None:
            state = RecordDelivery(
                index=index,
                topic=self._tp.topic,
                partition=self._tp.partition,
                topic_partition=self._tp,
            )
            self._record_states[index] = state
        return state

    def _annotate_record_error(self, error, index):
        if error is None:
            return error
        sequence = (
            self._base_sequence + index if self._base_sequence is not None else None
        )
        for name in (
            "index",
            "batch_index",
            "original_index",
            "record_index",
        ):
            try:
                setattr(error, name, index)
            except Exception:
                pass
        for name in ("sequence_number", "original_sequence_number", "sequence"):
            try:
                setattr(error, name, sequence)
            except Exception:
                pass
        return error

    @property
    def record_results(self):
        return tuple(
            self._record_states[index] for index in sorted(self._record_states)
        )

    @property
    def record_states(self):
        return self.record_results

    @property
    def delivery_uncertain(self) -> bool:
        return self._delivery_uncertain or bool(
            getattr(self, "_unknown_delivery", False)
            or getattr(self, "_response_lost", False)
            or getattr(self, "_outcome_unknown", False)
        )

    @delivery_uncertain.setter
    def delivery_uncertain(self, value):
        self._delivery_uncertain = bool(value)

    @property
    def response_lost(self) -> bool:
        return self.delivery_uncertain

    @response_lost.setter
    def response_lost(self, value):
        self.mark_delivery_uncertain() if value else None

    unknown_delivery = response_lost

    @property
    def outcome_unknown(self) -> bool:
        return self.delivery_uncertain

    @outcome_unknown.setter
    def outcome_unknown(self, value):
        self.delivery_uncertain = value

    @property
    def rejection_split_applied(self) -> bool:
        return self._rejection_split_applied

    def mark_delivery_uncertain(self):
        self._delivery_uncertain = True
        for state in self._record_states.values():
            if state.status is RecordStatus.PENDING:
                state.status = RecordStatus.UNKNOWN

    mark_response_lost = mark_delivery_uncertain
    note_response_loss = mark_delivery_uncertain

    @property
    def tp(self):
        return self._tp

    @property
    def record_count(self):
        return self._builder.record_count()

    def append(
        self,
        key,
        value,
        timestamp_ms,
        _create_future=create_future,
        headers: Sequence = [],
    ):
        """Append message (key and value) to batch

        Returns:
            None if batch is full
              or
            asyncio.Future that will resolved when message is delivered
        """
        metadata = self._builder.append(
            timestamp=timestamp_ms, key=key, value=value, headers=headers
        )
        if metadata is None:
            return None

        future = _create_future()
        index = metadata.offset
        self._msg_futures.append((future, metadata))
        self._futures_by_index[index] = future
        self._indices_by_future[future] = index
        if index >= len(self._original_indices):
            self._original_indices.append(index)
        state = self._ensure_record_state(index)
        if self._base_sequence is not None:
            state.original_sequence_number = self._base_sequence + index
            state.sequence_number = state.original_sequence_number
        return future

    def reject_records(self, rejection_by_index, *, error_factory=None):
        """Resolve rejected message futures and remove those records.

        Returns the number of removed records. The batch's original future is
        left unfinished so the retained records can complete it.
        """
        if self._rejection_split_applied:
            return 0

        rejected_indices = set(rejection_by_index)
        for index, detail in rejection_by_index.items():
            state = self._ensure_record_state(index)
            message = None
            error = detail
            if isinstance(detail, tuple) and len(detail) == 2:
                error, message = detail
            elif isinstance(detail, str):
                message = detail
                error = error_factory(message) if error_factory else None

            state.status = RecordStatus.REJECTED
            state.error_message = message or (str(error) if error is not None else None)
            if error is not None:
                state.error = error if isinstance(error, BaseException) else error()
                self._annotate_record_error(state.error, index)
            if self._base_sequence is not None:
                state.original_sequence_number = self._base_sequence + index
                state.sequence_number = state.original_sequence_number

            future = self._futures_by_index.get(index)
            if future is not None and not future.done() and state.error is not None:
                future.set_exception(copy.copy(state.error))

        retained_indices = [
            index
            for index in range(len(self._builder.stored_records))
            if index not in rejected_indices
        ]
        outer_builder = self._builder
        rebuilt, metadata_by_index = outer_builder.copy_with_indices(retained_indices)
        if self._base_sequence is not None:
            rebuilt._set_producer_state(
                self._producer_id, self._producer_epoch, self._base_sequence
            )
        rebuilt.close()
        outer_builder._builder = rebuilt._builder
        outer_builder._buffer = None
        outer_builder._stored_records = rebuilt._stored_records
        outer_builder._relative_offset = rebuilt._relative_offset
        outer_builder._closed = rebuilt._closed
        outer_builder._delivery_report_provider = self

        new_message_futures = []
        new_futures_by_index = {}
        new_indices_by_future = {}
        for new_index, original_index in enumerate(retained_indices):
            future = self._futures_by_index.get(original_index)
            if future is None:
                continue
            metadata = metadata_by_index[original_index]
            new_message_futures.append((future, metadata))
            new_futures_by_index[new_index] = future
            new_indices_by_future[future] = original_index

        self._original_indices = retained_indices
        self._msg_futures = new_message_futures
        self._futures_by_index = new_futures_by_index
        self._indices_by_future = new_indices_by_future
        self._rejection_split_applied = True
        return len(rejected_indices)

    remove_rejected_records = reject_records
    split_out_rejected_records = reject_records

    def set_raw_record_errors(self, record_errors):
        details = []
        for item in record_errors or ():
            try:
                index, message = item
            except (TypeError, ValueError):
                index = getattr(item, "batch_index", None)
                message = getattr(item, "batch_index_error_message", None) or getattr(
                    item, "error_message", None
                )
            details.append(RecordErrorDetail(index, message))
        self._raw_record_errors = tuple(details)
        return self._raw_record_errors

    def note_verified_rejection_details(self, rejection_by_index):
        for index, detail in rejection_by_index.items():
            state = self._ensure_record_state(index)
            error = detail
            message = None
            if isinstance(detail, tuple) and len(detail) == 2:
                error, message = detail
            elif isinstance(detail, str):
                message = detail
            if error is not None:
                state.error = error if isinstance(error, BaseException) else error()
                self._annotate_record_error(state.error, index)
            if message is not None:
                state.error_message = message
            elif state.error is not None:
                state.error_message = str(state.error)
            if not self._delivery_uncertain:
                state.status = RecordStatus.REJECTED

    def done(
        self,
        base_offset,
        timestamp=None,
        log_start_offset=None,
        _record_metadata_class=RecordMetadata,
        *,
        duplicate=False,
    ):
        """Resolve all pending futures"""
        tp = self._tp
        topic = tp.topic
        partition = tp.partition
        if timestamp == -1:
            timestamp_type = 0
        else:
            timestamp_type = 1

        offset_known = base_offset is not None and base_offset >= 0
        batch_offset = base_offset if offset_known else None
        dispatched_at = self._dispatched_at
        should_report = dispatched_at is not None and not self.future.done()

        # Set main batch future
        if not self.future.done():
            self.future.set_result(
                _record_metadata_class(
                    topic,
                    partition,
                    tp,
                    batch_offset,
                    timestamp,
                    timestamp_type,
                    log_start_offset,
                    self.record_results,
                )
            )

        # Set message futures and per-record receipt state.
        stored_records = getattr(self._builder, "stored_records", ())
        for compact_offset, stored_record in enumerate(stored_records):
            original_index = self._original_indices[compact_offset]
            state = self._ensure_record_state(original_index)
            if timestamp == -1:
                record_timestamp = stored_record.timestamp
            else:
                record_timestamp = timestamp

            record_offset = base_offset + compact_offset if offset_known else None
            record_metadata = _record_metadata_class(
                topic,
                partition,
                tp,
                record_offset,
                record_timestamp,
                timestamp_type,
                log_start_offset,
                (),
            )
            state.offset = record_offset
            state.timestamp = record_timestamp
            state.timestamp_type = timestamp_type
            state.log_start_offset = log_start_offset
            state.metadata = record_metadata
            state.offset_known = offset_known
            state.duplicate = duplicate and not offset_known
            if duplicate and not offset_known:
                state.status = RecordStatus.OFFSET_UNKNOWN
            else:
                state.status = RecordStatus.DELIVERED
            if self._base_sequence is not None:
                state.sequence_number = self._base_sequence + compact_offset

            future = self._futures_by_index.get(compact_offset)
            if future is None or future.done():
                continue
            future.set_result(record_metadata)

        if should_report:
            completed_at = time.monotonic()
            _call_listeners(
                self._callbacks.get("on_batch_completed", ()),
                topic=topic,
                partition=partition,
                send_to_completion_seconds=completed_at - dispatched_at,
                record_count=self.record_count,
                acknowledged=True,
                batch_age_seconds=completed_at - self._ctime,
            )

    def done_noack(self):
        """Resolve all pending futures to None"""
        dispatched_at = self._dispatched_at
        should_report = dispatched_at is not None and not self.future.done()

        # Faster resolve for base_offset=None case.
        if not self.future.done():
            self.future.set_result(None)
        for future, _ in self._msg_futures:
            if future.done():
                continue
            future.set_result(None)

        if should_report:
            completed_at = time.monotonic()
            _call_listeners(
                self._callbacks.get("on_batch_completed", ()),
                topic=self._tp.topic,
                partition=self._tp.partition,
                send_to_completion_seconds=completed_at - dispatched_at,
                record_count=self.record_count,
                acknowledged=False,
                batch_age_seconds=completed_at - self._ctime,
            )

    def failure(self, exception):
        should_report = not self.future.done()

        final_results = []
        for future, metadata in self._msg_futures:
            original_index = self._indices_by_future.get(future, metadata.offset)
            state = self._ensure_record_state(original_index)
            if state.status is RecordStatus.REJECTED:
                final_results.append(state)
                continue
            if not (self._delivery_uncertain and state.error is not None):
                state.error = copy.copy(exception)
            state.status = (
                RecordStatus.UNKNOWN
                if self._delivery_uncertain
                else RecordStatus.FAILED
            )
            final_results.append(state)

        for index, state in self._record_states.items():
            if index not in self._indices_by_future.values() and state.error is None:
                state.error = copy.copy(exception)
                state.status = (
                    RecordStatus.UNKNOWN
                    if self._delivery_uncertain
                    else RecordStatus.FAILED
                )
            elif self._delivery_uncertain and state.status is RecordStatus.PENDING:
                state.status = RecordStatus.UNKNOWN
            elif not self._delivery_uncertain and state.status is RecordStatus.PENDING:
                state.status = RecordStatus.FAILED

        record_results = self.record_results
        rejected_details = tuple(
            RecordErrorDetail(
                state.index,
                state.error_message,
                state.error,
                state.original_sequence_number or state.sequence_number,
            )
            for state in record_results
            if state.status is RecordStatus.REJECTED
        )
        exception.record_results = record_results
        exception.record_delivery_results = record_results
        exception.record_error_results = record_results
        exception.delivery_uncertain = self._delivery_uncertain
        if self._raw_record_errors:
            exception.record_errors = self._raw_record_errors
            exception.record_error_details = self._raw_record_errors
        elif rejected_details:
            exception.record_errors = rejected_details
            exception.record_error_details = rejected_details

        if not self.future.done():
            self.future.set_exception(exception)
        for future, metadata in self._msg_futures:
            if future.done():
                continue
            original_index = self._indices_by_future.get(future, metadata.offset)
            state = self._record_states.get(original_index)
            future_exception = exception
            if (
                state is not None
                and state.status is RecordStatus.REJECTED
                and state.error
            ):
                future_exception = state.error
            # we need to copy exception so traceback is not multiplied
            # https://github.com/aio-libs/aiokafka/issues/246
            future.set_exception(copy.copy(future_exception))

        # Consume exception to avoid warnings. We delegate this consumption
        # to user only in case of explicit batch API.
        if self._msg_futures:
            self.future.exception()

        # In case where sender fails and closes batches all waiters have to be
        # reset also.
        if not self._drain_waiter.done():
            self._drain_waiter.set_exception(exception)

        if should_report:
            failed_at = time.monotonic()
            _call_listeners(
                self._callbacks.get("on_batch_failed", ()),
                topic=self._tp.topic,
                partition=self._tp.partition,
                exception=exception,
                record_count=self.record_count,
                batch_age_seconds=failed_at - self._ctime,
            )

    def fail_unknown_delivery(self, exception=None, *, verified_rejections=None):
        if exception is None:
            exception = RecordDeliveryUnknownError(
                "Produce response was lost; record delivery state is unknown"
            )
        self.mark_delivery_uncertain()
        if verified_rejections:
            self.reject_records(verified_rejections)
        self.failure(exception)

    async def wait_drain(self, timeout=None):  # noqa: ASYNC109
        """Wait until all message from this batch is processed"""
        waiter = self._drain_waiter
        await asyncio.wait([waiter], timeout=timeout)
        if waiter.done():
            waiter.result()  # Check for exception

    def remaining_linger(self):
        """Return the eventual remaining linger time"""
        lifetime = time.monotonic() - self._ctime
        # Batch builders reject a message if they reach
        # their maximum capacity
        if self._builder.closed() or lifetime >= self._linger_time:
            return None
        return self._linger_time - lifetime

    def expired(self):
        """Check that batch is expired or not"""
        return (time.monotonic() - self._ctime) > self._ttl

    def drain_ready(self):
        """Mark batch as drained from the accumulator."""
        if not self._drain_waiter.done():
            self._drain_waiter.set_result(None)
        self._retry_count += 1

    def mark_dispatched(self):
        """Record that the batch is handed to the sender."""
        self._dispatched_at = time.monotonic()
        _call_listeners(
            self._callbacks.get("on_batch_dispatched", ()),
            topic=self._tp.topic,
            partition=self._tp.partition,
            queue_time_seconds=self._dispatched_at - self._queued_at,
            batch_size_bytes=self._builder.size(),
            record_count=self.record_count,
            attempt=self._retry_count,
        )

    def reset_drain(self):
        """Reset drain waiter, until we will do another retry"""
        assert self._drain_waiter.done()
        self._drain_waiter = create_future()
        self._queued_at = time.monotonic()
        self._dispatched_at = None

    def set_producer_state(self, producer_id, producer_epoch, base_sequence):
        assert not self._drain_waiter.done()
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._base_sequence = base_sequence
        for index, state in self._record_states.items():
            state.original_sequence_number = base_sequence + index
            if state.sequence_number is None:
                state.sequence_number = state.original_sequence_number
        self._builder._set_producer_state(producer_id, producer_epoch, base_sequence)

    def get_data_buffer(self):
        return self._builder._build()

    def is_empty(self):
        return self._builder.record_count() == 0

    @property
    def retry_count(self):
        return self._retry_count


class MessageAccumulator:
    """Accumulator of messages batched by topic-partition

    Producer adds messages to this accumulator and a background send task
    gets batches per nodes to process it.
    """

    def __init__(
        self,
        cluster,
        batch_size,
        compression_type,
        batch_ttl,
        *,
        txn_manager=None,
        loop=None,
        linger_ms=0,
        listeners=(),
        retry_rejected_records=False,
        **record_rejection_options,
    ):
        self._retry_rejected_records = rejection_retry_enabled(
            retry_rejected_records, **record_rejection_options
        )
        for option_name, option_value in record_rejection_options.items():
            setattr(self, option_name, option_value)
        if loop is None:
            loop = get_running_loop()
        self._loop = loop
        self._batches = collections.defaultdict(collections.deque)
        self._pending_batches = set()
        self._cluster = cluster
        self._batch_size = batch_size
        self._compression_type = compression_type
        self._batch_ttl = batch_ttl
        self._waiter_future = loop.create_future()
        self._wakeup_handle = None
        self._closed = False
        self._txn_manager = txn_manager
        self._linger_time = linger_ms / 1000
        self._callbacks = _collect_callbacks(
            listeners,
            ProducerListener,
            (
                "on_batch_dispatched",
                "on_batch_completed",
                "on_batch_failed",
                "on_buffer_wait",
            ),
        )

        self._exception = None  # Critical exception

    async def flush(self):
        waiters = [
            batch.future for batches in self._batches.values() for batch in batches
        ]
        waiters += [batch.future for batch in self._pending_batches]
        if waiters:
            await asyncio.wait(waiters)

    async def flush_for_commit(self):
        waiters = []
        for batches in self._batches.values():
            for batch in batches:
                # We force all buffers to close to finalize the transaction
                # scope. We should not add anything to this transaction.
                batch._builder.close()
                waiters.append(batch.future)
        # We wake up eventually the sender waiting for lingering batches
        if not self._waiter_future.done():
            self._waiter_future.set_result(None)
        waiters += [batch.future for batch in self._pending_batches]
        # Wait for all waiters to finish. We only wait for the scope we defined
        # above, other batches should not be delivered as part of this
        # transaction
        if waiters:
            await asyncio.wait(waiters)

    def fail_all(self, exception):
        # Close all batches with this exception
        for batches in self._batches.values():
            for batch in batches:
                batch.failure(exception)
        for batch in self._pending_batches:
            batch.failure(exception)
        self._exception = exception

    async def close(self):
        self._closed = True
        await self.flush()

    async def add_message(  # noqa: PLR0917
        self,
        tp,
        key,
        value,
        timeout,  # noqa: ASYNC109
        timestamp_ms=None,
        headers: Sequence = [],
        retry_rejected_records=None,
        **record_rejection_options,
    ):
        """Add message to batch by topic-partition
        If batch is already full this method waits (`timeout` seconds maximum)
        until batch is drained by send task
        """
        total_wait_time = 0.0
        try:
            while True:
                if self._closed:
                    # this can happen when producer is closing but try to send some
                    # messages in async task
                    raise ProducerClosed()
                if self._exception is not None:
                    raise copy.copy(self._exception)

                pending_batches = self._batches.get(tp)
                if not pending_batches:
                    builder = self.create_builder()
                    batch = self._append_batch(builder, tp)
                else:
                    batch = pending_batches[-1]

                future = batch.append(key, value, timestamp_ms, headers=headers)
                if future is not None:
                    if retry_rejected_records is not None or record_rejection_options:
                        batch._retry_rejected_records = rejection_retry_enabled(
                            retry_rejected_records, **record_rejection_options
                        )
                    return future
                # Batch is full, can't append data atm,
                # wake up the sender loop
                # and wait until batch per topic-partition is drained
                if not self._waiter_future.done():
                    self._waiter_future.set_result(None)

                start = time.monotonic()
                try:
                    await batch.wait_drain(timeout)
                finally:
                    wait_time = time.monotonic() - start
                    total_wait_time += wait_time
                    timeout -= wait_time
                if timeout <= 0:
                    raise KafkaTimeoutError()
        finally:
            self._report_buffer_wait(tp, total_wait_time)

    def _report_buffer_wait(self, tp, wait_seconds):
        if wait_seconds > 0:
            _call_listeners(
                self._callbacks.get("on_buffer_wait", ()),
                topic=tp.topic,
                partition=tp.partition,
                wait_seconds=wait_seconds,
            )

    def waiter(self):
        """Return waiter future that will be resolved when accumulator contain
        some data for drain or a batch lingering is ready to be picked
        """
        return self._waiter_future

    def _pop_batch(self, tp):
        batch = self._batches[tp].popleft()
        not_retry = batch.retry_count == 0
        if self._txn_manager is not None and not_retry:
            assert self._txn_manager.has_pid(), (
                "We should have waited for it in sender routine"
            )
            seq = self._txn_manager.sequence_number(batch.tp)
            self._txn_manager.increment_sequence_number(batch.tp, batch.record_count)
            batch.set_producer_state(
                producer_id=self._txn_manager.producer_id,
                producer_epoch=self._txn_manager.producer_epoch,
                base_sequence=seq,
            )
        batch.drain_ready()
        if len(self._batches[tp]) == 0:
            del self._batches[tp]
        self._pending_batches.add(batch)

        if not_retry:

            def cb(fut, batch=batch, self=self):
                self._pending_batches.remove(batch)

            batch.future.add_done_callback(cb)
        return batch

    def reenqueue(self, batch):
        tp = batch.tp
        self._batches[tp].appendleft(batch)
        self._pending_batches.remove(batch)
        batch.reset_drain()

    def drain_by_nodes(self, ignore_nodes, muted_partitions=frozenset()):
        """Group batches by leader to partition nodes."""
        nodes = collections.defaultdict(dict)
        unknown_leaders_exist = False
        remaining_linger_time = None
        for tp in list(self._batches.keys()):
            # Just ignoring by node is not enough, as leader can change during
            # the cycle
            if tp in muted_partitions:
                continue
            leader = self._cluster.leader_for_partition(tp)
            if leader is None or leader == -1:
                if self._batches[tp][0].expired():
                    # batch is for partition is expired and still no leader,
                    # so set exception for batch and pop it
                    batch = self._pop_batch(tp)
                    if leader is None:
                        err = NotLeaderForPartitionError()
                    else:
                        err = LeaderNotAvailableError()
                    batch.failure(exception=err)
                unknown_leaders_exist = True
                continue
            elif ignore_nodes and leader in ignore_nodes:
                continue

            batch_remaining_linger = self._batches[tp][0].remaining_linger()
            if batch_remaining_linger:
                # Batch should linger more
                remaining_linger_time = (
                    min(remaining_linger_time, batch_remaining_linger)
                    if remaining_linger_time
                    else batch_remaining_linger
                )
                continue

            batch = self._pop_batch(tp)
            # We can get an empty batch here if all `append()` calls failed
            # with validation...
            if not batch.is_empty():
                nodes[leader][tp] = batch
                batch.mark_dispatched()
            else:
                # XXX: use something more graceful. We just want to trigger
                # delivery future here, no message futures.
                batch.done_noack()

        # all batches are drained from accumulator
        # so create "wait" future again for waiting new data/linger expired in send
        # task
        if not self._waiter_future.done():
            self._waiter_future.set_result(None)
        self._waiter_future = self._loop.create_future()

        # cleaning up old wakeup task
        if self._wakeup_handle:
            self._wakeup_handle.cancel()
            self._wakeup_handle = None

        # schedule a new wakeup task
        if remaining_linger_time:
            self._wakeup_handle = self._loop.call_later(
                remaining_linger_time, self._wakeup, self._waiter_future
            )

        return nodes, unknown_leaders_exist

    @staticmethod
    def _wakeup(fut):
        if not fut.done():
            fut.set_result(None)

    def create_builder(
        self,
        key_serializer=None,
        value_serializer=None,
        retry_rejected_records=None,
        **record_rejection_options,
    ):
        is_transactional = False
        if (
            self._txn_manager is not None
            and self._txn_manager.transactional_id is not None
        ):
            is_transactional = True
        builder = BatchBuilder(
            self._batch_size,
            self._compression_type,
            is_transactional=is_transactional,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )
        if retry_rejected_records is not None or record_rejection_options:
            builder._retry_rejected_records = rejection_retry_enabled(
                retry_rejected_records, **record_rejection_options
            )
        return builder

    def _append_batch(self, builder, tp):
        # We must do this before actual add takes place to check for errors.
        if self._txn_manager is not None:
            self._txn_manager.maybe_add_partition_to_txn(tp)

        batch = MessageBatch(
            tp,
            builder,
            self._batch_ttl,
            self._linger_time,
            self._callbacks,
        )
        self._batches[tp].append(batch)
        if not self._waiter_future.done():
            self._waiter_future.set_result(None)
        return batch

    async def add_batch(
        self,
        builder,
        tp,
        timeout,
        retry_rejected_records=None,
        **record_rejection_options,
    ):  # noqa: ASYNC109
        """Add BatchBuilder to queue by topic-partition.

        Arguments:
            builder (BatchBuilder): batch object to enqueue.
            tp (TopicPartition): topic and partition to enqueue this batch for.
            timeout (int): time in seconds to wait for a free slot in the batch
                queue.

        Returns:
            MessageBatch: delivery wrapper around the BatchBuilder object.

        Raises:
            aiokafka.errors.ProducerClosed: the accumulator has already been
                closed and flushed.
            aiokafka.errors.KafkaTimeoutError: the batch could not be added
                within the specified timeout.
        """
        if self._closed:
            raise ProducerClosed()
        if self._exception is not None:
            raise copy.copy(self._exception)

        total_wait_time = 0.0
        try:
            while timeout > 0:
                pending = self._batches.get(tp)
                if pending:
                    start = time.monotonic()
                    try:
                        await pending[-1].wait_drain(timeout=timeout)
                    finally:
                        wait_time = time.monotonic() - start
                        total_wait_time += wait_time
                        timeout -= wait_time
                else:
                    batch = self._append_batch(builder, tp)
                    if retry_rejected_records is not None or record_rejection_options:
                        batch._retry_rejected_records = rejection_retry_enabled(
                            retry_rejected_records, **record_rejection_options
                        )
                    return asyncio.shield(batch.future)
            raise KafkaTimeoutError()
        finally:
            self._report_buffer_wait(tp, total_wait_time)
