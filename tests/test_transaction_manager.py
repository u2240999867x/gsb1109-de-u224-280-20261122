import pytest

from aiokafka.producer.transaction_manager import TransactionManager
from aiokafka.structs import TopicPartition

NO_PRODUCER_ID = -1
NO_PRODUCER_EPOCH = -1


@pytest.fixture
async def txn_manager():
    return TransactionManager("txn_id", 20000)


def test_txn_manager(txn_manager):
    assert txn_manager._pid_and_epoch.pid == NO_PRODUCER_ID
    assert txn_manager._pid_and_epoch.epoch == NO_PRODUCER_EPOCH
    assert not txn_manager._sequence_numbers
    assert not txn_manager.has_pid()

    txn_manager.set_pid_and_epoch(123, 321)
    assert txn_manager._pid_and_epoch.pid == 123
    assert txn_manager._pid_and_epoch.epoch == 321
    assert not txn_manager._sequence_numbers
    assert txn_manager.has_pid()

    tp1 = TopicPartition("topic", 1)
    tp2 = TopicPartition("topic", 2)

    assert txn_manager.sequence_number(tp1) == 0
    txn_manager.increment_sequence_number(tp1, 1)
    assert txn_manager.sequence_number(tp1) == 1

    # Changing one sequence_number does not change other
    assert txn_manager.sequence_number(tp2) == 0
    txn_manager.increment_sequence_number(tp2, 33)
    assert txn_manager.sequence_number(tp2) == 33
    assert txn_manager.sequence_number(tp1) == 1

    # sequence number should wrap around 32 bit signed integers
    txn_manager.increment_sequence_number(tp1, 2**32 - 5)
    assert txn_manager.sequence_number(tp1) == -4


async def test_txn_manager_transaction_generation(txn_manager):
    # The generation token allows detecting that a transaction, open when an
    # operation started, was closed in the meantime.
    assert txn_manager.transaction_generation == 0

    txn_manager.set_pid_and_epoch(123, 321)
    txn_manager.begin_transaction()
    generation = txn_manager.transaction_generation

    # Committing a transaction changes the token
    txn_manager.committing_transaction()
    txn_manager.complete_transaction()
    assert txn_manager.transaction_generation != generation

    # Aborting after an abortable error changes the token on both steps
    txn_manager.begin_transaction()
    generation = txn_manager.transaction_generation
    txn_manager.error_transaction(ValueError())
    assert txn_manager.transaction_generation != generation
    # Silence "exception was never retrieved" warnings
    txn_manager.wait_for_transaction_end().exception()
    generation = txn_manager.transaction_generation
    txn_manager.aborting_transaction()
    txn_manager.complete_transaction()
    assert txn_manager.transaction_generation != generation

    # Fatal errors also close the transaction for waiting operations
    txn_manager.begin_transaction()
    generation = txn_manager.transaction_generation
    txn_manager.fatal_error(ValueError())
    assert txn_manager.transaction_generation != generation
    txn_manager.wait_for_transaction_end().exception()
