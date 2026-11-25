import asyncio
import contextlib
import io
import struct
import unittest
from unittest import mock

import pytest

from aiokafka.client import AIOKafkaClient
from aiokafka.consumer.fetcher import (
    READ_UNCOMMITTED,
    Fetcher,
    PartitionRecords,
)
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.errors import (
    FencedLeaderEpochError,
    IllegalStateError,
    KafkaError,
    UnknownLeaderEpochError,
)
from aiokafka.protocol.fetch import FetchRequest
from aiokafka.protocol.fetch import FetchResponse_v0 as FetchResponse
from aiokafka.protocol.leader_epoch import (
    OffsetsForLeaderEpochRequest,
    OffsetsForLeaderEpochRequest_v1,
    OffsetsForLeaderEpochResponse_v0,
    OffsetsForLeaderEpochResponse_v1,
)
from aiokafka.record.default_records import (
    _DefaultRecordBatchBuilderPy as DefaultRecordBatchBuilder,
)
from aiokafka.record.memory_records import MemoryRecords
from aiokafka.structs import (
    ConsumerRecord,
    PartitionRecoveryCredential,
    TopicPartition,
)
from aiokafka.util import create_task

from ._testutil import run_until_complete


TP = TopicPartition("test", 0)


def _v2_batch(first_offset, epoch, values):
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=0,
        batch_size=99999999,
    )
    for index, value in enumerate(values):
        builder.append(
            offset=first_offset + index,
            value=value,
            key=None,
            timestamp=1,
            headers=[],
        )
    raw = bytearray(builder.build())
    # PartitionLeaderEpoch lives right after the 32-bit batch length.
    struct.pack_into(">i", raw, 12, epoch)
    return bytes(raw)


def _record(offset, epoch):
    return ConsumerRecord(
        topic=TP.topic,
        partition=TP.partition,
        offset=offset,
        timestamp=1,
        timestamp_type=0,
        key=None,
        value=b"v",
        checksum=None,
        serialized_key_size=-1,
        serialized_value_size=1,
        headers=[],
        leader_epoch=epoch,
    )


@pytest.mark.usefixtures("setup_test_class_serverless")
class TestRecoveryCredential(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._cleanup = []

    def tearDown(self):
        super().tearDown()
        for coro, args, kw in reversed(self._cleanup):
            task = asyncio.wait_for(coro(*args, **kw), 30)
            self.loop.run_until_complete(task)

    def add_cleanup(self, cb_or_coro, *args, **kw):
        self._cleanup.append((cb_or_coro, args, kw))

    async def _make_fetcher(self, auto_offset_reset="latest"):
        client = AIOKafkaClient(bootstrap_servers=[])
        client.ready = mock.AsyncMock(return_value=True)
        client._maybe_wait_metadata = mock.AsyncMock(return_value=None)
        client.cluster.leader_for_partition = mock.Mock(return_value=0)
        client.send = mock.AsyncMock()

        subscriptions = SubscriptionState()
        fetcher = Fetcher(
            client, subscriptions, auto_offset_reset=auto_offset_reset
        )
        # Disable the background fetch routine for deterministic unit tests.
        fetcher._fetch_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await fetcher._fetch_task
        fetcher._fetch_task = create_task(asyncio.sleep(1000000))
        self.add_cleanup(fetcher.close)

        subscriptions.assign_from_user({TP})
        subscriptions.seek(TP, 140)
        return client, subscriptions, fetcher

    def test_leader_epoch_is_attached_per_batch(self):
        # getmany returned 119 (epoch 8) and 120 (epoch 9); 116-118 were
        # compacted away and never arrived.
        message_set = _v2_batch(119, 8, [b"119"]) + _v2_batch(120, 9, [b"120"])
        records = PartitionRecords(
            TP,
            MemoryRecords(message_set),
            [],
            119,
            None,
            None,
            False,
            READ_UNCOMMITTED,
        )
        messages = list(records)
        self.assertEqual([m.offset for m in messages], [119, 120])
        self.assertEqual([m.leader_epoch for m in messages], [8, 9])

    @run_until_complete
    async def test_build_credential_uses_processed_record_epoch(self):
        _client, _subs, fetcher = await self._make_fetcher()
        credential = fetcher.build_recovery_credential(TP, _record(119, 8))
        self.assertEqual(
            credential,
            PartitionRecoveryCredential(
                topic="test",
                partition=0,
                next_offset=120,
                leader_epoch=8,
                verified=True,
            ),
        )
        # Even though the consumer already saw epoch 9 on offset 120, the
        # credential for the processed 119 stays on epoch 8 with next offset 120.
        self.assertEqual(credential.next_offset, 120)
        self.assertEqual(credential.leader_epoch, 8)

    @run_until_complete
    async def test_build_credential_rejects_offset_only_records(self):
        _client, _subs, fetcher = await self._make_fetcher()
        self.assertIsNone(
            fetcher.build_recovery_credential(TP, _record(119, -1))
        )
        old_record = _record(119, None)
        self.assertIsNone(fetcher.build_recovery_credential(TP, old_record))

    def test_credential_dict_roundtrip(self):
        credential = PartitionRecoveryCredential("test", 0, 120, 8)
        self.assertEqual(
            PartitionRecoveryCredential.from_dict(credential.to_dict()),
            credential,
        )

    @run_until_complete
    async def test_validate_without_divergence_resumes_from_credential(self):
        client, _subs, fetcher = await self._make_fetcher()
        client.send.return_value = OffsetsForLeaderEpochResponse_v1(
            0, [("test", [(0, 0, 9, 120)])]
        )
        fetcher._safe_log_start = mock.AsyncMock(return_value=100)

        result = await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8)
        )
        self.assertTrue(result.recoverable)
        self.assertEqual(result.position, 120)
        self.assertEqual(result.divergences, ())
        self.assertIsNone(result.error)
        self.assertEqual(
            _subs.subscription.assignment.state_value(TP).position, 120
        )

    @run_until_complete
    async def test_validate_still_current_epoch_resumes_normally(self):
        client, _subs, fetcher = await self._make_fetcher()
        # end offset -1 means epoch 8 is still the current leader epoch.
        client.send.return_value = OffsetsForLeaderEpochResponse_v1(
            0, [("test", [(0, 0, 8, -1)])]
        )
        fetcher._safe_log_start = mock.AsyncMock(return_value=100)

        result = await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8)
        )
        self.assertTrue(result.recoverable)
        self.assertEqual(result.position, 120)
        self.assertEqual(result.divergences, ())

    @run_until_complete
    async def test_validate_unknown_leader_epoch_keeps_position(self):
        client, subs, fetcher = await self._make_fetcher()
        client.send.return_value = OffsetsForLeaderEpochResponse_v1(
            0, [("test", [(UnknownLeaderEpochError.errno, 0, -1, -1)])]
        )
        result = await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8)
        )
        self.assertFalse(result.recoverable)
        self.assertEqual(result.position, 140)
        self.assertIsInstance(result.error, UnknownLeaderEpochError)
        self.assertEqual(
            subs.subscription.assignment.state_value(TP).position, 140
        )

    @run_until_complete
    async def test_validate_fenced_epoch_reports_divergence_and_keeps_position(self):
        client, subs, fetcher = await self._make_fetcher()
        # The current leader says epoch 8 ended at 105; offset 120 is readable
        # but holds replaced data.
        client.send.return_value = OffsetsForLeaderEpochResponse_v1(
            0, [("test", [(FencedLeaderEpochError.errno, 0, 9, 105)])]
        )
        fetcher._safe_log_start = mock.AsyncMock(return_value=100)

        result = await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8)
        )
        self.assertFalse(result.recoverable)
        self.assertEqual(result.position, 140)
        self.assertEqual(len(result.divergences), 1)
        divergence = result.divergences[0]
        self.assertEqual((divergence.start_offset, divergence.end_offset), (105, 120))
        # The preview position at 140 is untouched.
        self.assertEqual(
            subs.subscription.assignment.state_value(TP).position, 140
        )

    @run_until_complete
    async def test_validate_marks_deleted_ranges_even_when_epoch_is_fenced(self):
        client, _subs, fetcher = await self._make_fetcher()
        client.send.return_value = OffsetsForLeaderEpochResponse_v1(
            0, [("test", [(FencedLeaderEpochError.errno, 0, 9, 105)])]
        )
        # Earliest readable offset moved past the fenced epoch's end.
        fetcher._safe_log_start = mock.AsyncMock(return_value=111)

        result = await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8)
        )
        self.assertFalse(result.recoverable)
        self.assertEqual(result.position, 140)
        reasons = {(d.start_offset, d.end_offset) for d in result.divergences}
        self.assertIn((105, 111), reasons)
        self.assertIn((105, 120), reasons)

    @run_until_complete
    async def test_validate_without_evidence_keeps_position(self):
        client, subs, fetcher = await self._make_fetcher()
        client.send.side_effect = KafkaError("broker unreachable")

        result = await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8)
        )
        self.assertFalse(result.recoverable)
        self.assertEqual(result.position, 140)
        self.assertIsInstance(result.error, KafkaError)
        self.assertEqual(
            subs.subscription.assignment.state_value(TP).position, 140
        )

    @run_until_complete
    async def test_validate_unverified_credential_does_not_contact_broker(self):
        client, subs, fetcher = await self._make_fetcher()
        credential = PartitionRecoveryCredential(
            "test", 0, 120, -1, verified=False
        )
        result = await fetcher.validate_recovery_credential(credential)
        self.assertFalse(result.recoverable)
        self.assertEqual(result.position, 140)
        self.assertIsInstance(result.error, IllegalStateError)
        client.send.assert_not_called()
        self.assertEqual(
            subs.subscription.assignment.state_value(TP).position, 140
        )

    @run_until_complete
    async def test_allow_reread_arms_replay_after_accepted_fetch(self):
        client, subs, fetcher = await self._make_fetcher()

        async def send(node, request):
            if isinstance(request, OffsetsForLeaderEpochRequest):
                return OffsetsForLeaderEpochResponse_v1(
                    0, [("test", [(FencedLeaderEpochError.errno, 0, 9, 105)])]
                )
            # Probe fetch at 105 is accepted.
            return FetchResponse([("test", [(0, 0, 0, b"")])])

        client.send.side_effect = send
        fetcher._safe_log_start = mock.AsyncMock(return_value=100)

        result = await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8),
            allow_reread=True,
        )
        self.assertTrue(result.recoverable)
        self.assertEqual(result.position, 105)
        self.assertEqual(
            subs.subscription.assignment.state_value(TP).position, 105
        )
        state = fetcher._replay_states[TP]
        self.assertEqual(state.leader_node, 0)
        self.assertEqual(state.leader_epoch, 9)
        self.assertEqual(state.lower_bound, 105)

        # The retained lower bound can be raised before the first delivery;
        # leader and epoch stay the same.
        fetcher.update_replay_lower_bound(TP, 111)
        self.assertEqual(fetcher._replay_states[TP].lower_bound, 111)
        self.assertEqual(fetcher._replay_states[TP].leader_epoch, 9)

    @run_until_complete
    async def test_lower_bound_cannot_move_backwards(self):
        _client, _subs, fetcher = await self._make_fetcher()
        fetcher._replay_states  # sanity: attribute exists
        with self.assertRaises(IllegalStateError):
            fetcher.update_replay_lower_bound(TP, 111)

    @run_until_complete
    async def test_replay_filters_records_below_lower_bound(self):
        client, subs, fetcher = await self._make_fetcher()

        async def send(node, request):
            if isinstance(request, OffsetsForLeaderEpochRequest):
                return OffsetsForLeaderEpochResponse_v1(
                    0,
                    [("test", [(FencedLeaderEpochError.errno, 0, 9, 105)])],
                )
            message_set = _v2_batch(
                105, 9, [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"]
            )
            return FetchResponse([("test", [(0, 0, 0, message_set)])])

        client.send.side_effect = send
        fetcher._safe_log_start = mock.AsyncMock(return_value=100)

        result = await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8),
            allow_reread=True,
        )
        self.assertTrue(result.recoverable)
        self.assertEqual(result.position, 105)
        fetcher.update_replay_lower_bound(TP, 111)

        assignment = subs.subscription.assignment
        request = FetchRequest(
            0, 0, 100, 0, [("test", [(0, 105, 100000)])]
        )
        await fetcher._proc_fetch_request(assignment, 0, request)

        first = await fetcher.next_record([])
        self.assertEqual(first.offset, 111)
        self.assertTrue(fetcher._replay_states[TP].confirmed)

    @run_until_complete
    async def test_confirm_replay_position_with_empty_fetch(self):
        client, subs, fetcher = await self._make_fetcher()
        subs.seek(TP, 105)

        async def send(node, request):
            if isinstance(request, OffsetsForLeaderEpochRequest):
                return OffsetsForLeaderEpochResponse_v1(
                    0, [("test", [(FencedLeaderEpochError.errno, 0, 9, 105)])]
                )
            return FetchResponse([("test", [(0, 0, 0, b"")])])

        client.send.side_effect = send
        fetcher._safe_log_start = mock.AsyncMock(return_value=100)

        await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8),
            allow_reread=True,
        )
        self.assertFalse(fetcher._replay_states[TP].confirmed)
        confirmed = await fetcher.confirm_replay_position(TP)
        self.assertTrue(confirmed)
        self.assertTrue(fetcher._replay_states[TP].confirmed)

    @run_until_complete
    async def test_deleted_replay_range_keeps_original_position(self):
        client, subs, fetcher = await self._make_fetcher(
            auto_offset_reset="latest"
        )

        async def send(node, request):
            if isinstance(request, OffsetsForLeaderEpochRequest):
                return OffsetsForLeaderEpochResponse_v1(
                    0, [("test", [(FencedLeaderEpochError.errno, 0, 9, 105)])]
                )
            # Probe at 105 is accepted while arming the replay.
            return FetchResponse([("test", [(0, 0, 0, b"")])])

        client.send.side_effect = send
        fetcher._safe_log_start = mock.AsyncMock(return_value=100)

        result = await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8),
            allow_reread=True,
        )
        self.assertTrue(result.recoverable)
        fetcher.update_replay_lower_bound(TP, 111)

        # The next background fetch finds 105 out of range: the new log starts
        # at 111, so 105-110 can never be back-filled.
        assignment = subs.subscription.assignment
        request = FetchRequest(
            0, 0, 100, 0, [("test", [(0, 105, 100000)])]
        )
        out_of_range = FetchResponse([("test", [(0, 1, 0, b"")])])
        client.send.side_effect = mock.AsyncMock(return_value=out_of_range)
        await fetcher._proc_fetch_request(assignment, 0, request)

        # Auto-reset must not kick in; the preview position at 140 is kept.
        tp_state = assignment.state_value(TP)
        self.assertTrue(tp_state.has_valid_position)
        self.assertEqual(tp_state.position, 140)
        self.assertFalse(tp_state.awaiting_reset)
        ranges = {
            (d.start_offset, d.end_offset)
            for d in fetcher.replay_divergences(TP)
        }
        self.assertIn((105, 111), ranges)

    @run_until_complete
    async def test_term_fence_drops_stale_old_term_fetch(self):
        client, subs, fetcher = await self._make_fetcher()

        ofle_calls = {"count": 0}
        stale = {"value": True}

        async def send(node, request):
            if isinstance(request, OffsetsForLeaderEpochRequest):
                ofle_calls["count"] += 1
                if ofle_calls["count"] == 1:
                    # Validation: epoch 8 ended at 105, current term is epoch 9.
                    return OffsetsForLeaderEpochResponse_v1(
                        0,
                        [("test", [(FencedLeaderEpochError.errno, 0, 9, 105)])],
                    )
                # The same broker is re-elected with a newer epoch (10), and
                # the old epoch-9 fetch arrives only after the client sees it.
                return OffsetsForLeaderEpochResponse_v1(
                    0,
                    [("test", [(FencedLeaderEpochError.errno, 0, 10, 105)])],
                )
            if stale["value"]:
                message_set = _v2_batch(105, 9, [b"stale"])
            else:
                message_set = _v2_batch(105, 10, [b"fresh"])
            return FetchResponse([("test", [(0, 0, 0, message_set)])])

        client.send.side_effect = send
        fetcher._safe_log_start = mock.AsyncMock(return_value=100)

        await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8),
            allow_reread=True,
        )
        state = fetcher._replay_states[TP]
        self.assertEqual(state.leader_epoch, 9)
        self.assertFalse(state.confirmed)

        # The old-term fetch response is processed after the client learned of
        # the new term, so it must be dropped without buffering anything.
        assignment = subs.subscription.assignment
        request = FetchRequest(
            0, 0, 100, 0, [("test", [(0, 105, 100000)])]
        )
        await fetcher._proc_fetch_request(assignment, 0, request)
        self.assertTrue(state.term_rejudged)
        self.assertEqual(state.leader_epoch, 10)
        self.assertNotIn(TP, fetcher._records)

        # A fresh fetch under the new term is delivered normally.
        stale["value"] = False
        await fetcher._proc_fetch_request(assignment, 0, request)
        self.assertIn(TP, fetcher._records)
        message = await fetcher.next_record([])
        self.assertEqual(message.offset, 105)
        self.assertEqual(message.leader_epoch, 10)

    @run_until_complete
    async def test_recovery_does_not_commit_offsets(self):
        client, subs, fetcher = await self._make_fetcher()
        client.send.return_value = OffsetsForLeaderEpochResponse_v1(
            0, [("test", [(0, 0, 9, 120)])]
        )
        fetcher._safe_log_start = mock.AsyncMock(return_value=100)

        result = await fetcher.validate_recovery_credential(
            PartitionRecoveryCredential("test", 0, 120, 8),
            allow_reread=True,
        )
        self.assertTrue(result.recoverable)
        # Only the verification exchange happened; the Fetcher has no commit
        # capability of its own and must never touch the group coordinator.
        sent_requests = [call.args[1] for call in client.send.await_args_list]
        self.assertTrue(
            all(
                isinstance(
                    request,
                    (OffsetsForLeaderEpochRequest, FetchRequest),
                )
                for request in sent_requests
            )
        )


class TestLeaderEpochProtocol(unittest.TestCase):
    def test_request_encode_decode(self):
        request = OffsetsForLeaderEpochRequest([("test", [(0, 8), (1, 9)])])
        encoded = request.build(OffsetsForLeaderEpochRequest_v1).encode()
        decoded = OffsetsForLeaderEpochRequest_v1.decode(io.BytesIO(encoded))
        self.assertEqual(
            decoded.topics,
            [("test", [(0, 8), (1, 9)])],
        )

    def test_response_v0_layout(self):
        response = OffsetsForLeaderEpochResponse_v0(
            0, [("test", [(0, 8, 105)])]
        )
        decoded = OffsetsForLeaderEpochResponse_v0.decode(
            io.BytesIO(response.encode())
        )
        self.assertEqual(decoded.topics, [("test", [(0, 8, 105)])])

    def test_response_v1_layout_carries_error_code(self):
        response = OffsetsForLeaderEpochResponse_v1(
            0, [("test", [(FencedLeaderEpochError.errno, 0, 9, 105)])]
        )
        decoded = OffsetsForLeaderEpochResponse_v1.decode(
            io.BytesIO(response.encode())
        )
        self.assertEqual(
            decoded.topics,
            [("test", [(FencedLeaderEpochError.errno, 0, 9, 105)])],
        )

    def test_error_codes(self):
        self.assertIs(FencedLeaderEpochError, __import__(
            "aiokafka.errors", fromlist=["for_code"]
        ).for_code(74))
        self.assertIs(UnknownLeaderEpochError, __import__(
            "aiokafka.errors", fromlist=["for_code"]
        ).for_code(80))
