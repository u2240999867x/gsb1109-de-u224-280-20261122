import re

import pytest

from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.errors import IllegalStateError
from aiokafka.structs import TopicPartition


@pytest.fixture
async def subscription_state():
    return SubscriptionState()


class MockListener(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        pass

    def on_partitions_assigned(self, assigned):
        pass


async def test_subscribe_topic(subscription_state):
    mock_listener = MockListener()
    subscription_state.subscribe({"tp1", "tp2"}, listener=mock_listener)
    assert subscription_state.subscription is not None
    assert subscription_state.subscription.topics == {"tp1", "tp2"}
    assert subscription_state.subscription.assignment is None
    assert subscription_state.subscription.active is True
    assert subscription_state.subscription.unsubscribe_future.done() is False

    # After subscription to topic we can't change the subscription to pattern
    # or user assignment
    with pytest.raises(IllegalStateError):
        subscription_state.subscribe_pattern(
            pattern=re.compile("^tests-.*$"), listener=mock_listener
        )
    with pytest.raises(IllegalStateError):
        subscription_state.assign_from_user([TopicPartition("topic", 0)])

    # Subsciption of the same type can be applied
    old_subsciption = subscription_state.subscription
    subscription_state.subscribe({"tp1", "tp2", "tp3"}, listener=mock_listener)

    assert subscription_state.subscription is not None
    assert subscription_state.subscription.topics == {"tp1", "tp2", "tp3"}

    assert old_subsciption is not subscription_state.subscription
    assert old_subsciption.active is False
    assert old_subsciption.unsubscribe_future.done() is True


async def test_subscribe_pattern(subscription_state):
    mock_listener = MockListener()
    pattern = re.compile("^tests-.*$")
    subscription_state.subscribe_pattern(pattern=pattern, listener=mock_listener)

    assert subscription_state.subscription is None
    assert subscription_state.subscribed_pattern == pattern

    # After subscription to a pattern we can't change the subscription until
    # `unsubscribe` called
    with pytest.raises(IllegalStateError):
        subscription_state.subscribe(
            topics={"tp1", "tp2", "tp3"}, listener=mock_listener
        )
    with pytest.raises(IllegalStateError):
        subscription_state.assign_from_user([TopicPartition("topic", 0)])


async def test_user_assignment(subscription_state):
    topic_partitions = {
        TopicPartition("topic1", 0),
        TopicPartition("topic1", 1),
        TopicPartition("topic2", 0),
    }
    subscription_state.assign_from_user(topic_partitions)
    assert subscription_state.subscription is not None
    assert subscription_state.subscription.topics == {"topic1", "topic2"}
    assert subscription_state.subscription.assignment is not None
    assert subscription_state.subscription.active is True
    assert subscription_state.subscription.unsubscribe_future.done() is False
    assignment = subscription_state.subscription.assignment
    assert assignment.active is True
    assert assignment.unassign_future.done() is False
    assert assignment.tps == topic_partitions

    # After manual assignment no other subscription is possible
    mock_listener = MockListener()
    with pytest.raises(IllegalStateError):
        subscription_state.subscribe(
            topics={"tp1", "tp2", "tp3"}, listener=mock_listener
        )
    with pytest.raises(IllegalStateError):
        subscription_state.subscribe_pattern(
            pattern=re.compile("^tests-.*$"), listener=mock_listener
        )

    # Assignment can be changed manually again thou
    new_tps = {
        TopicPartition("topic3", 0),
        TopicPartition("topic3", 1),
        TopicPartition("topic4", 0),
    }
    subscription_state.assign_from_user(new_tps)

    assert subscription_state.subscription is not None
    assert subscription_state.subscription.topics == {"topic3", "topic4"}
    new_assignment = subscription_state.subscription.assignment
    assert new_assignment.tps == new_tps

    assert assignment is not new_assignment
    assert assignment.active is False
    assert assignment.unassign_future.done() is True


async def test_unsubscribe(subscription_state):
    subscription_state.subscribe({"tp1", "tp2"})
    assert subscription_state.subscription is not None

    subscription_state.unsubscribe()
    assert subscription_state.subscription is None

    # After unsubscribe you can change the type to say pattern.
    subscription_state.subscribe_pattern(re.compile("pattern"))
    subscription_state.subscribe_from_pattern({"tp33"})

    assert subscription_state.subscription is not None


async def test_seek(subscription_state):
    tp = TopicPartition("topic1", 0)
    tp2 = TopicPartition("topic2", 0)
    subscription_state.assign_from_user({tp, tp2})

    assignment = subscription_state.subscription.assignment
    assert assignment.state_value(tp) is not None
    assert not assignment.state_value(tp).has_valid_position
    assert assignment.state_value(tp)._position is None

    subscription_state.seek(tp, 1000)

    assert assignment.state_value(tp).position == 1000


async def test_assigned_partitions(subscription_state):
    assert subscription_state.assigned_partitions() == set()
    subscription_state.subscribe(topics={"tp1"})
    assert subscription_state.assigned_partitions() == set()
    assignment = {TopicPartition("tp1", 0)}
    subscription_state.assign_from_subscribed(assignment)
    assert subscription_state.assigned_partitions() == assignment


async def test_is_assigned(subscription_state):
    tp1 = TopicPartition("topic", 0)
    tp2 = TopicPartition("topic", 1)
    assert not subscription_state.is_assigned(tp1)
    subscription_state.subscribe({"topic"})
    assert not subscription_state.is_assigned(tp1)
    subscription_state.assign_from_subscribed({tp1})
    assert subscription_state.is_assigned(tp1)
    assert not subscription_state.is_assigned(tp2)


async def test_cooperative_reassignment_reuses_retained_state(subscription_state):
    tp0 = TopicPartition("topic", 0)
    tp1 = TopicPartition("topic", 1)
    tp2 = TopicPartition("topic", 2)

    subscription_state.subscribe({"topic"})
    subscription_state.assign_from_subscribed({tp0, tp1, tp2})
    previous = subscription_state.subscription.assignment

    # tp0 is paused for business reasons; tp1 has a consumed position.
    subscription_state.pause(tp0)
    subscription_state.seek(tp1, 5)
    retained_states = {
        tp0: previous.state_value(tp0),
        tp1: previous.state_value(tp1),
    }

    # Cooperative round: tp2 moves away, tp0/tp1 are retained.
    subscription_state.assign_from_subscribed(
        {tp0, tp1}, reused_states=retained_states
    )
    current = subscription_state.subscription.assignment

    assert subscription_state.assigned_partitions() == {tp0, tp1}
    assert subscription_state.retained_partitions == {tp0, tp1}
    # Same state objects, so pause flag and fetch position survive.
    assert current.state_value(tp0) is retained_states[tp0]
    assert current.state_value(tp1) is retained_states[tp1]
    assert current.state_value(tp0).paused is True
    assert current.state_value(tp1).position == 5
    # The moved partition is gone together with its state.
    assert current.state_value(tp2) is None
    assert not previous.active


async def test_eager_reassignment_does_not_retain_state(subscription_state):
    tp0 = TopicPartition("topic", 0)
    tp1 = TopicPartition("topic", 1)

    subscription_state.subscribe({"topic"})
    subscription_state.assign_from_subscribed({tp0, tp1})
    subscription_state.pause(tp0)
    previous = subscription_state.subscription.assignment
    old_tp0_state = previous.state_value(tp0)

    # Eager reassignments pass no reused states, so state starts fresh and
    # the retained-partitions marker stays empty.
    subscription_state.assign_from_subscribed({tp0, tp1})
    current = subscription_state.subscription.assignment

    assert subscription_state.retained_partitions == set()
    assert current.state_value(tp0) is not old_tp0_state
    assert current.state_value(tp0).paused is False
    assert not current.state_value(tp0).has_valid_position


async def test_assigned_state(subscription_state):
    tp1 = TopicPartition("topic", 0)
    tp2 = TopicPartition("topic", 1)

    subscription_state.assign_from_user({tp1})
    with pytest.raises(IllegalStateError):
        subscription_state._assigned_state(tp2)

    tp_state = subscription_state._assigned_state(tp1)
    assert tp_state is not None

    assert repr(tp_state) == (
        "TopicPartitionState<Status=PartitionStatus.AWAITING_RESET position=None>"
    )


async def test_begin_reassignment(subscription_state):
    subscription_state.subscribe({"tp1", "tp2"})
    subscription_state.unsubscribe()

    # After unsubscribe we should not fail begin_reassignment, just ignore it
    subscription_state.begin_reassignment()
