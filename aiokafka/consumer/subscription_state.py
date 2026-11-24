from __future__ import annotations

import contextlib
import copy
import logging
import time
from asyncio import Event, shield
from collections.abc import Iterable
from enum import Enum
from re import Pattern

from aiokafka.abc import ConsumerRebalanceListener, DeliveryReceipt
from aiokafka.errors import IllegalStateError, StaleDeliveryReceiptError
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from aiokafka.util import create_future, get_running_loop

log = logging.getLogger(__name__)


class SubscriptionType(Enum):
    NONE = 1
    AUTO_TOPICS = 2
    AUTO_PATTERN = 3
    USER_ASSIGNED = 4


class SubscriptionState:
    """Intermediate bridge to coordinate work between Consumer, Coordinator
    and Fetcher primitives.

        The class is different from kafka-python's implementation to provide
    a more friendly way to interact in asynchronous paradigm. The changes
    focus on making the internals less mutable (subscription, topic state etc.)
    paired with futures for when those change.
        Before there was a lot of trouble if user say did a subscribe between
    yield statements of a rebalance or other critical IO operation.
    """

    _subscription_type = SubscriptionType.NONE  # type: SubscriptionType
    _subscribed_pattern = None  # type: str
    _subscription = None  # type: Subscription
    _listener = None  # type: ConsumerRebalanceListener

    def __init__(self, loop=None, enable_store_ack=False):
        if loop is None:
            loop = get_running_loop()
        self._loop = loop

        self._subscription_waiters = []  # type: List[Future]
        self._assignment_waiters = []  # type: List[Future]

        # Fetch contexts
        self._fetch_count = 0
        self._last_fetch_ended = time.monotonic()

        # Store-acknowledge mode: autocommit only advances past records the
        # application explicitly acknowledged with a valid DeliveryReceipt.
        self._enable_store_ack = enable_store_ack
        # Bumped every time the assignment changes. Receipts issued under a
        # previous assignment generation are rejected as stale.
        self._delivery_epoch = 0

    @property
    def store_ack_enabled(self) -> bool:
        return self._enable_store_ack

    @property
    def delivery_epoch(self) -> int:
        return self._delivery_epoch

    def _bump_delivery_epoch(self):
        self._delivery_epoch += 1

    @property
    def subscription(self) -> Subscription:
        return self._subscription

    @property
    def subscribed_pattern(self) -> Pattern:
        return self._subscribed_pattern

    @property
    def listener(self) -> ConsumerRebalanceListener:
        return self._listener

    @property
    def topics(self):
        if self._subscription is not None:
            return self._subscription.topics
        return set()

    def assigned_partitions(self) -> set[TopicPartition]:
        if self._subscription is None:
            return set()
        if self._subscription.assignment is None:
            return set()
        return self._subscription.assignment.tps

    @property
    def reassignment_in_progress(self):
        if self._subscription is None:
            return True
        return self._subscription._reassignment_in_progress

    def partitions_auto_assigned(self) -> bool:
        return self._subscription_type in [
            SubscriptionType.AUTO_TOPICS,
            SubscriptionType.AUTO_PATTERN,
        ]

    def is_assigned(self, tp: TopicPartition) -> bool:
        if self._subscription is None:
            return False
        if self._subscription.assignment is None:
            return False
        return tp in self._subscription.assignment.tps

    def _set_subscription_type(self, subscription_type: SubscriptionType):
        if self._subscription_type in [SubscriptionType.NONE, subscription_type]:
            self._subscription_type = subscription_type
        else:
            raise IllegalStateError(
                "Subscription to topics, partitions and pattern are mutually exclusive"
            )

    def _change_subscription(self, subscription: Subscription):
        log.info("Updating subscribed topics to: %s", subscription.topics)
        # Set old subscription as inactive
        if self._subscription is not None:
            self._subscription._unsubscribe()
        self._subscription = subscription
        self._bump_delivery_epoch()
        self._notify_subscription_waiters()

    def _assigned_state(self, tp: TopicPartition) -> TopicPartitionState:
        assert self._subscription is not None
        assert self._subscription.assignment is not None
        tp_state = self._subscription.assignment.state_value(tp)
        if tp_state is None:
            raise IllegalStateError(f"No current assignment for partition {tp}")
        return tp_state

    def _notify_subscription_waiters(self):
        for waiter in self._subscription_waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._subscription_waiters.clear()

    def _notify_assignment_waiters(self):
        for waiter in self._assignment_waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._assignment_waiters.clear()

    # Consumer callable API:

    def subscribe(self, topics: set[str], listener=None):
        """Subscribe to a list (or tuple) of topics

        Caller: Consumer.
        Affects: SubscriptionState.subscription
        """
        assert isinstance(topics, set)
        assert listener is None or isinstance(listener, ConsumerRebalanceListener)
        self._set_subscription_type(SubscriptionType.AUTO_TOPICS)

        self._change_subscription(Subscription(topics, loop=self._loop))
        self._listener = listener
        self._notify_subscription_waiters()

    def subscribe_pattern(self, pattern: Pattern, listener=None):
        """Subscribe to all topics matching a regex pattern.
        Subsequent calls `subscribe_from_pattern()` by Coordinator will provide
        the actual subscription topics.

        Caller: Consumer.
        Affects: SubscriptionState.subscribed_pattern
        """
        assert hasattr(pattern, "match"), "Expected Pattern type"
        assert listener is None or isinstance(listener, ConsumerRebalanceListener)
        self._set_subscription_type(SubscriptionType.AUTO_PATTERN)

        self._subscribed_pattern = pattern
        self._listener = listener

    def assign_from_user(self, partitions: Iterable[TopicPartition]):
        """Manually assign partitions. After this call automatic assignment
        will be impossible and will raise an ``IllegalStateError``.

        Caller: Consumer.
        Affects: SubscriptionState.subscription
        """
        self._set_subscription_type(SubscriptionType.USER_ASSIGNED)

        # ``_change_subscription`` bumps the delivery epoch as the previous
        # assignment is replaced.
        self._change_subscription(ManualSubscription(partitions, loop=self._loop))
        self._notify_assignment_waiters()

    def unsubscribe(self):
        """Unsubscribe from the last subscription. This will also clear the
        subscription type.

        Caller: Consumer.
        Affects: SubscriptionState.subscription
        """
        # Set old subscription as inactive
        if self._subscription is not None:
            self._subscription._unsubscribe()

        self._subscription = None
        self._subscribed_pattern = None
        self._listener = None
        self._subscription_type = SubscriptionType.NONE

    # Coordinator callable API:

    def subscribe_from_pattern(self, topics: set[str]):
        """Change subscription on cluster metadata update if a new topic
        created or one is removed.

        Caller: Coordinator
        Affects: SubscriptionState.subscription
        """
        assert self._subscription_type == SubscriptionType.AUTO_PATTERN
        self._change_subscription(Subscription(topics))

    def assign_from_subscribed(self, assignment: set[TopicPartition]):
        """Set assignment if automatic assignment is used.

        Caller: Coordinator
        Affects: SubscriptionState.subscription.assignment
        """
        assert self._subscription_type in [
            SubscriptionType.AUTO_PATTERN,
            SubscriptionType.AUTO_TOPICS,
        ]

        self._bump_delivery_epoch()
        self._subscription._assign(assignment)
        self._notify_assignment_waiters()

    def begin_reassignment(self):
        """Signal from Coordinator that a group re-join is needed. For example
        this will be called if a commit or heartbeat fails with an
        InvalidMember error.

        Caller: Coordinator
        """
        if self._subscription is not None:
            self._subscription._begin_reassignment()

    # Fetcher callable API:

    def seek(self, tp: TopicPartition, offset: int):
        """Force reset of position to the specified offset.

        Caller: Consumer, Fetcher
        Affects: TopicPartitionState.position
        """
        self._assigned_state(tp).seek(offset)

    # Store-acknowledge API (enabled with ``enable_store_ack``)

    def issue_receipt(self, tp: TopicPartition, offset: int) -> DeliveryReceipt:
        """Mint a receipt for a record the Fetcher is about to hand out."""
        return self._assigned_state(tp).issue_receipt(tp, offset, self._delivery_epoch)

    def acknowledge(self, receipt: DeliveryReceipt) -> int:
        """Mark the record behind ``receipt`` as durably stored.

        Raises:
            IllegalStateError: if store-ack mode is disabled.
            StaleDeliveryReceiptError: if the receipt was invalidated by a
                re-read or an assignment change.
        """
        if not self._enable_store_ack:
            raise IllegalStateError("Delivery receipts require enable_store_ack=True")
        tp = receipt.topic_partition
        tp_state = self._assigned_state(tp)
        tp_state.acknowledge(receipt, self._delivery_epoch)
        return tp_state.safe_position

    def safe_consumed_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """Offsets safe to commit in store-ack mode, one per assignment."""
        if self._subscription is None or self._subscription.assignment is None:
            return {}
        return self._subscription.assignment.all_safe_offsets()

    def reread(self, tp: TopicPartition, offset: int) -> None:
        """Rewind ``tp`` to ``offset`` and invalidate every receipt issued for
        that offset and everything after it, without touching earlier work.
        """
        tp_state = self._assigned_state(tp)
        tp_state.reread_from(offset)
        self.seek(tp, offset)

    def mark_batch_consumed(self, tp: TopicPartition, last_offset: int) -> None:
        """Report that a fetch batch for ``tp`` was fully drained, its tail
        being ``last_offset``. Offsets without a pending receipt in that range
        are proven empty and may advance the safe position.
        """
        self._assigned_state(tp).mark_empty_upto(last_offset)

    # Waiters

    def wait_for_subscription(self):
        """Wait for subscription change. This will always wait for next
        subscription.
        """
        fut = create_future()
        self._subscription_waiters.append(fut)
        return fut

    def wait_for_assignment(self):
        """Wait for next assignment. Be careful, as this will always wait for
        next assignment, even if the current one is active.
        """
        fut = create_future()
        self._assignment_waiters.append(fut)
        return fut

    def register_fetch_waiters(self, waiters):
        self._fetch_waiters = waiters

    def abort_waiters(self, exc):
        """Critical error occurred, we will abort any pending waiter"""
        for waiter in self._assignment_waiters:
            if not waiter.done():
                waiter.set_exception(copy.copy(exc))
        self._subscription_waiters.clear()

        for waiter in self._fetch_waiters:
            if not waiter.done():
                waiter.set_exception(copy.copy(exc))

    # Pause/Resume API

    def pause(self, tp: TopicPartition) -> None:
        self._assigned_state(tp).pause()

    def paused_partitions(self) -> set[TopicPartition]:
        res = set()
        for tp in self.assigned_partitions():
            if self._assigned_state(tp).paused:
                res.add(tp)
        return res

    def resume(self, tp: TopicPartition) -> None:
        self._assigned_state(tp).resume()

    # Fetch context

    @contextlib.contextmanager
    def fetch_context(self):
        self._fetch_count += 1
        yield
        self._fetch_count -= 1
        if self._fetch_count == 0:
            self._last_fetch_ended = time.monotonic()

    @property
    def fetcher_idle_time(self):
        """How much time (in seconds) spent without consuming any records"""
        if self._fetch_count == 0:
            return time.monotonic() - self._last_fetch_ended
        else:
            return 0


class Subscription:
    """Describes current subscription to a list of topics. In case of pattern
    subscription a new instance of this class will be created if number of
    matched topics change.

    States:
        * Subscribed
        * Assigned (assignment was set)
        * Unsubscribed
    """

    def __init__(self, topics: Iterable[str], loop=None):
        if loop is None:
            loop = get_running_loop()

        self._topics = frozenset(topics)  # type: FrozenSet[str]
        self._assignment = None  # type: Assignment
        self.unsubscribe_future = loop.create_future()  # type: Future
        self._reassignment_in_progress = True

    @property
    def active(self):
        return self.unsubscribe_future.done() is False

    @property
    def topics(self):
        return self._topics

    @property
    def assignment(self):
        return self._assignment

    def _assign(self, topic_partitions: Iterable[TopicPartition]):
        for tp in topic_partitions:
            assert tp.topic in self._topics, (
                f"Received an assignment for unsubscribed topic: {tp}"
            )

        if self._assignment is not None:
            self._assignment._unassign()

        self._assignment = Assignment(topic_partitions)
        self._reassignment_in_progress = False

    def _unsubscribe(self):
        self.unsubscribe_future.set_result(None)
        if self._assignment is not None:
            self._assignment._unassign()

    def _begin_reassignment(self):
        self._reassignment_in_progress = True


class ManualSubscription(Subscription):
    """Describes a user assignment"""

    def __init__(self, user_assignment: Iterable[TopicPartition], loop=None):
        topics = (tp.topic for tp in user_assignment)
        super().__init__(topics, loop=loop)
        self._assignment = Assignment(user_assignment)

    def _assign(self, topic_partitions: set[TopicPartition]):  # pragma: no cover
        raise AssertionError("Should not be called")

    @property
    def _reassignment_in_progress(self):
        return False

    @_reassignment_in_progress.setter
    def _reassignment_in_progress(self, value):
        pass

    def _begin_reassignment(self):  # pragma: no cover
        raise AssertionError("Should not be called")


class Assignment:
    """Describes current partition assignment. New instance will be created
    on each group rebalance if automatic assignment is used.

    States:
        * Assigned
        * Unassigned
    """

    def __init__(self, topic_partitions: Iterable[TopicPartition]):
        assert isinstance(topic_partitions, list | set | tuple)

        self._topic_partitions = frozenset(topic_partitions)

        self._tp_state = {}  # type: Dict[TopicPartition, TopicPartitionState]
        for tp in self._topic_partitions:
            self._tp_state[tp] = TopicPartitionState(self)

        self.unassign_future = create_future()
        self.commit_refresh_needed = Event()

    @property
    def tps(self):
        return self._topic_partitions

    @property
    def active(self):
        return self.unassign_future.done() is False

    def _unassign(self):
        self.unassign_future.set_result(None)

    def state_value(self, tp: TopicPartition) -> TopicPartitionState:
        return self._tp_state.get(tp)

    def all_consumed_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """Returns consumed offsets as {TopicPartition: OffsetAndMetadata}"""
        all_consumed = {}
        for tp in self._topic_partitions:
            state = self.state_value(tp)
            if state.has_valid_position:
                all_consumed[tp] = OffsetAndMetadata(state.position, "")
        return all_consumed

    def all_safe_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """Returns the offsets safe to commit in store-ack mode: the prefix of
        records that has been explicitly acknowledged, including offsets the
        broker proved empty (aborted/control batches).
        """
        all_safe = {}
        for tp in self._topic_partitions:
            state = self.state_value(tp)
            if state.has_valid_position:
                all_safe[tp] = OffsetAndMetadata(state.safe_position, "")
        return all_safe

    def requesting_committed(self):
        """Return all partitions that are requesting commit point fetch"""
        requesting = []
        for tp in self._topic_partitions:
            tp_state = self.state_value(tp)
            if tp_state._committed_futs:
                requesting.append(tp)
        return requesting


class PartitionStatus(Enum):
    AWAITING_RESET = 0
    CONSUMING = 1
    UNASSIGNED = 2


class TopicPartitionState:
    """Shared Partition metadata state.

    After creation the workflow is similar to:

        * Partition assigned to this consumer (AWAITING_RESET)
        * Fetcher either uses commit save point or resets position in respect
          to defined reset policy (AWAITING_RESET -> CONSUMING)
        * Fetcher loads a new batch of records, yields results to consumer
          and updates consumed position (CONSUMING)
        * Assignment changed or subscription changed (CONSUMING -> UNASSIGNED)

    """

    def __init__(self, assignment):
        # Synchronized values
        self._committed_futs = []

        self.highwater = None  # Last fetched highwater mark
        self.lso = None  # Last fetched stable offset mark
        self.timestamp = None  # timestamp of last poll
        self._position = None  # The current position of the topic
        self._position_fut = create_future()

        # Will be set by `seek_to_beginning` or `seek_to_end` if called by user
        # or by Fetcher after confirming that current position is no longer
        # reachable.
        self._reset_strategy = None  # type: int
        self._status = PartitionStatus.AWAITING_RESET  # type: PartitionStatus

        self._assignment = assignment

        self._paused = False
        self._resume_fut = None

        # Store-ack bookkeeping. ``_safe_position`` is the next offset that is
        # safe to commit: every offset before it was either acknowledged or
        # proven empty by a fully consumed fetch batch. ``None`` until the
        # partition gets a valid position.
        self._safe_position: int | None = None
        # Exclusive upper bound up to which complete fetch batches were
        # consumed: every offset below it was either delivered (a receipt was
        # issued) or proven empty (aborted/control/compacted).
        self._verified_upto: int = 0
        # offset -> receipt sequence of the latest valid dispatch for it
        self._pending_acks: dict[int, int] = {}
        # offsets acknowledged out of order, waiting for the prefix to catch up
        self._acked_offsets: set[int] = set()
        self._next_receipt_sequence: int = 0

    @property
    def safe_position(self) -> int | None:
        """Next offset that is safe to commit (``None`` until initialized)."""
        return self._safe_position

    def issue_receipt(
        self, tp: TopicPartition, offset: int, epoch: int
    ) -> DeliveryReceipt:
        receipt = DeliveryReceipt(tp, offset, epoch, self._next_receipt_sequence)
        self._next_receipt_sequence += 1
        self._pending_acks[offset] = receipt.sequence
        return receipt

    def acknowledge(self, receipt: DeliveryReceipt, current_epoch: int) -> None:
        if receipt.epoch != current_epoch:
            raise StaleDeliveryReceiptError(
                f"Receipt {receipt!r} belongs to a revoked assignment"
                f" (current epoch {current_epoch})"
            )
        active_sequence = self._pending_acks.get(receipt.offset)
        if active_sequence is None or active_sequence != receipt.sequence:
            raise StaleDeliveryReceiptError(
                f"Receipt {receipt!r} was superseded by a re-dispatch"
            )
        del self._pending_acks[receipt.offset]
        self._acked_offsets.add(receipt.offset)
        self._advance_safe_position()

    def _advance_safe_position(self) -> None:
        while self._safe_position is not None:
            if self._safe_position in self._acked_offsets:
                self._acked_offsets.discard(self._safe_position)
                self._safe_position += 1
            elif (
                self._safe_position not in self._pending_acks
                and self._safe_position < self._verified_upto
            ):
                # Proven empty by a fully consumed fetch batch.
                self._acked_offsets.discard(self._safe_position)
                self._safe_position += 1
            else:
                break

    def mark_empty_upto(self, offset: int) -> None:
        """Record that a fetch batch was fully consumed up to ``offset`` (the
        last record/control offset seen), so offsets without a pending receipt
        in that range are proven empty and may advance the safe position.
        """
        if self._safe_position is None:
            return
        self._verified_upto = max(self._verified_upto, offset + 1)
        self._advance_safe_position()

    def reread_from(self, offset: int) -> None:
        """Invalidate receipts at ``offset`` and beyond and rewind any out of
        order acknowledgements there. Work dispatched (and possibly
        acknowledged) before ``offset`` keeps its receipts and remains valid.
        """
        if self._safe_position is None:
            self._safe_position = offset
            self._verified_upto = offset
            return
        if offset < self._safe_position:
            raise IllegalStateError(
                f"Cannot re-read from {offset}: offsets up to"
                f" {self._safe_position} are already commit-safe"
            )
        for stale_offset in [
            candidate for candidate in self._pending_acks if candidate >= offset
        ]:
            del self._pending_acks[stale_offset]
        self._acked_offsets = {
            candidate for candidate in self._acked_offsets if candidate < offset
        }
        # The re-read range is no longer verified; it will be proven again as
        # the replacement fetch batches are consumed.
        self._verified_upto = min(self._verified_upto, offset)

    @property
    def paused(self):
        return self._paused

    @property
    def resume_fut(self):
        return self._resume_fut

    @property
    def has_valid_position(self) -> bool:
        return self._position is not None

    @property
    def position(self) -> int:
        assert self._position is not None
        return self._position

    @property
    def awaiting_reset(self):
        return self._reset_strategy is not None

    @property
    def reset_strategy(self) -> int:
        return self._reset_strategy

    def await_reset(self, strategy):
        """Called by either Coonsumer in `seek_to_*` or by Coordinator after
        setting initial committed point.
        """
        self._reset_strategy = strategy
        self._position = None
        if self._position_fut.done():
            self._position_fut = create_future()
        self._status = PartitionStatus.AWAITING_RESET

    # Committed manipulation

    def fetch_committed(self):
        fut = create_future()
        self._committed_futs.append(fut)
        self._assignment.commit_refresh_needed.set()
        return fut

    def update_committed(self, offset_meta: OffsetAndMetadata):
        """Called by Coordinator on successful commit to update commit cache."""
        for fut in self._committed_futs:
            if not fut.done():
                fut.set_result(offset_meta)
        self._committed_futs.clear()

    # Position manipulation

    def consumed_to(self, position: int):
        """Called by Fetcher when yielding results to Consumer"""
        assert self._status == PartitionStatus.CONSUMING
        self._position = position

    def reset_to(self, position: int):
        """Called by Fetcher after performing a reset to force position to
        a new point.
        """
        assert self._status == PartitionStatus.AWAITING_RESET
        self._position = position
        if self._safe_position is None:
            self._safe_position = position
        self._reset_strategy = None
        self._status = PartitionStatus.CONSUMING
        if not self._position_fut.done():
            self._position_fut.set_result(None)

    def seek(self, position: int):
        """Called by Consumer to force position to a specific offset"""
        self._position = position
        if self._safe_position is None:
            self._safe_position = position
        self._reset_strategy = None
        self._status = PartitionStatus.CONSUMING
        if not self._position_fut.done():
            self._position_fut.set_result(None)

    def wait_for_position(self):
        return shield(self._position_fut)

    # Pause/Unpause
    def pause(self):
        if not self._paused:
            self._paused = True
            assert self._resume_fut is None
            self._resume_fut = create_future()

    def resume(self):
        if self._paused:
            self._paused = False
            self._resume_fut.set_result(None)
            self._resume_fut = None

    def __repr__(self):
        return f"TopicPartitionState<Status={self._status} position={self._position}>"
