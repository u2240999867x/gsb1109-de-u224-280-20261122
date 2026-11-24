"""Tests for streaming publication (`pika.streaming_publish`, channel and connection pumps)."""

import unittest
from collections import deque
from unittest import mock

from pika import channel, connection, exceptions, frame, spec
from pika._utils import override
from pika.adapters.thread_safe_connection import StreamingPublishHandle
from pika.streaming_publish import StreamingPublishState


class ConstructibleConnection(connection.Connection):
    """Connection with adapter hooks replaced by in-memory test doubles."""

    def __init__(self):
        # When True, loop callbacks are queued for manual dispatch instead of
        # running inline, so a pump's retry behavior is observable.
        self.manual_pump = False
        self.pump_callbacks = deque()
        super().__init__()

    @override
    def _adapter_connect_stream(self):
        raise NotImplementedError

    @override
    def _adapter_disconnect_stream(self):
        raise NotImplementedError

    @override
    def _adapter_call_later(self, delay, callback):
        handle = object()
        self.timers.append((handle, delay, callback))
        return handle

    @override
    def _adapter_remove_timeout(self, timeout_id):
        self.timers = [t for t in self.timers if t[0] != timeout_id]

    @override
    def _adapter_add_callback_threadsafe(self, callback):
        if self.manual_pump:
            self.pump_callbacks.append(callback)
        else:
            callback()

    @override
    def _adapter_emit_data(self, data):
        self.emitted.append(data)

    @override
    def _adapter_get_write_buffer_size(self):
        return self.write_buffer_size

    def install(self):
        self.emitted = []
        self.timers = []
        self.write_buffer_size = 0
        self.terminations = []

        def _fake_terminate_stream(error):
            # Mirror the real `_terminate_stream`, which records the reason
            # synchronously before the async transport abort completes.
            self.terminations.append(error)
            self._error = error

        self._terminate_stream = _fake_terminate_stream
        self._set_connection_state(self.CONNECTION_OPEN)
        self._opened = True
        self._body_max_length = 4

    def run_pumps(self):
        """Drain every queued pump callback, including ones queued while draining."""
        while self.pump_callbacks:
            self.pump_callbacks.popleft()()


class ScriptedSource:
    """Serves scripted chunks by offset; an empty chunk models a temporary stall."""

    def __init__(self, chunks):
        self._chunks = deque(chunks)
        self.calls = []

    def read(self, offset, length):
        self.calls.append((offset, length))
        if not self._chunks:
            return b''
        return self._chunks.popleft()


class RaisingSource:

    def __init__(self, error):
        self._error = error

    def read(self, offset, length):
        raise self._error


def decode_frames(blobs):
    data = b''.join(blobs)
    decoded = []
    offset = 0
    while offset < len(data):
        consumed, value = frame.decode_frame(data, offset)
        assert consumed, f'undecodable frame at offset {offset}'
        decoded.append(value)
        offset += consumed
    return decoded


class StreamingPublishTestBase(unittest.TestCase):

    def setUp(self):
        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            self.connection = ConstructibleConnection()
        self.connection.install()
        self.channel = channel.Channel(self.connection, 7,
                                       mock.Mock())
        self.channel._set_state(channel.Channel.OPEN)

    def make_source(self, chunks):
        return ScriptedSource(chunks)


class FrameSequenceTests(StreamingPublishTestBase):

    def test_emits_method_header_and_body_frames_describing_the_full_body(self):
        body = b'0123456789'
        source = self.make_source([body[:4], body[4:8], body[8:]])

        stream = self.channel.basic_publish_stream(
            exchange='ex',
            routing_key='rk',
            body_size=len(body),
            source=source,
            properties=spec.BasicProperties(content_type='text/plain'),
            message_id='biz-1')

        frames = decode_frames(self.connection.emitted)
        self.assertIsInstance(frames[0], frame.Method)
        self.assertEqual(frames[0].channel_number, 7)
        self.assertIsInstance(frames[0].method, spec.Basic.Publish)
        self.assertEqual(frames[0].method.exchange, 'ex')
        self.assertEqual(frames[0].method.routing_key, 'rk')

        self.assertIsInstance(frames[1], frame.Header)
        self.assertEqual(frames[1].body_size, len(body))
        self.assertEqual(frames[1].properties.content_type, 'text/plain')

        body_frames = frames[2:]
        self.assertTrue(all(isinstance(f, frame.Body) for f in body_frames))
        self.assertEqual(b''.join(f.fragment for f in body_frames), body)
        self.assertEqual(stream.state, StreamingPublishState.COMPLETED)
        self.assertEqual(stream.message_id, 'biz-1')
        self.assertIsNone(stream.reason)

    def test_zero_length_body_completes_without_body_frames(self):
        stream = self.channel.basic_publish_stream(
            'ex', 'rk', 0, self.make_source([]))

        frames = decode_frames(self.connection.emitted)
        self.assertEqual(len(frames), 2)
        self.assertIsInstance(frames[0], frame.Method)
        self.assertIsInstance(frames[1], frame.Header)
        self.assertEqual(frames[1].body_size, 0)
        self.assertEqual(stream.state, StreamingPublishState.COMPLETED)

    def test_read_requests_never_exceed_the_negotiated_frame_limit(self):
        source = self.make_source([b'abcd', b'efgh', b'ij'])
        self.channel.basic_publish_stream('ex', 'rk', 10, source)

        self.assertTrue(all(length <= 4 for _offset, length in source.calls))
        self.assertEqual(source.calls[0], (0, 4))
        self.assertEqual(source.calls[-1], (8, 2))

    def test_a_temporary_stall_parks_then_resumes_via_the_retry_timer(self):
        body = b'01234567'
        # The first read yields nothing, so the opening pump pass makes zero
        # progress and must park on a timer rather than busy-spin.
        source = self.make_source([b'', b'0123', b'4567'])

        stream = self.channel.basic_publish_stream('ex', 'rk', 8, source)

        self.assertEqual(stream.state, StreamingPublishState.ACTIVE)
        self.assertEqual(stream.bytes_sent, 0)
        self.assertEqual(len(self.connection.timers), 1)
        _handle, delay, retry = self.connection.timers[0]
        self.assertGreater(delay, 0)

        retry()
        frames = decode_frames(self.connection.emitted)
        self.assertEqual(b''.join(f.fragment for f in frames[2:]), body)
        self.assertEqual(stream.state, StreamingPublishState.COMPLETED)


class ChannelFifoTests(StreamingPublishTestBase):

    def test_a_second_stream_stays_pending_until_the_first_completes(self):
        # Body larger than the first chunk, so the head stalls ACTIVE instead
        # of completing inline.
        first_source = self.make_source([b'aaaa', b''])
        first = self.channel.basic_publish_stream(
            'ex', 'rk', 8, first_source, message_id='first')
        second_source = self.make_source([b'bbbb'])
        second = self.channel.basic_publish_stream(
            'ex', 'rk', 4, second_source, message_id='second')

        self.assertEqual(first.state, StreamingPublishState.ACTIVE)
        self.assertEqual(second.state, StreamingPublishState.PENDING)
        self.assertEqual(second_source.calls, [])
        frames_before = decode_frames(self.connection.emitted)
        self.assertEqual(
            b''.join(f.fragment for f in frames_before[2:]), b'aaaa')

        # Feed the rest of the head and run its parked retry.
        first_source._chunks.append(b'eeee')
        self.connection.timers[0][2]()

        self.assertEqual(first.state, StreamingPublishState.COMPLETED)
        self.assertEqual(second.state, StreamingPublishState.COMPLETED)
        all_frames = decode_frames(self.connection.emitted)
        self.assertEqual(
            b''.join(f.fragment for f in all_frames
                     if isinstance(f, frame.Body)), b'aaaaeeeebbbb')

    def test_regular_publish_is_refused_while_a_stream_is_queued(self):
        # Keep the head ACTIVE so the channel still has a stream in flight.
        self.channel.basic_publish_stream(
            'ex', 'rk', 8, self.make_source([b'aaaa', b'']))
        with self.assertRaises(exceptions.ChannelWrongStateError):
            self.channel.basic_publish('ex', 'rk', b'small')

    def test_validation_rejects_bad_size_and_source(self):
        with self.assertRaises(TypeError):
            self.channel.basic_publish_stream('ex', 'rk', -1,
                                              self.make_source([]))
        with self.assertRaises(TypeError):
            self.channel.basic_publish_stream('ex', 'rk', 4, object())


class PumpFairnessTests(StreamingPublishTestBase):

    def test_round_robin_rotates_active_streams_across_channels(self):
        other_channel = channel.Channel(self.connection, 8, mock.Mock())
        other_channel._set_state(channel.Channel.OPEN)
        first_source = self.make_source([b'aa'])
        second_source = self.make_source([b'bb'])

        stream_one = self.channel.basic_publish_stream(
            'ex', 'rk', 2, first_source)
        stream_two = other_channel.basic_publish_stream(
            'ex', 'rk', 2, second_source)

        self.assertEqual(
            [s.message_id for s in self.connection._stream_publishes], [])
        self.assertEqual(stream_one.state, StreamingPublishState.COMPLETED)
        self.assertEqual(stream_two.state, StreamingPublishState.COMPLETED)

        body_by_channel = {7: [], 8: []}
        for value in decode_frames(self.connection.emitted):
            if isinstance(value, frame.Body):
                body_by_channel[value.channel_number].append(value.fragment)
        self.assertEqual(b''.join(body_by_channel[7]), b'aa')
        self.assertEqual(b''.join(body_by_channel[8]), b'bb')

    def test_high_water_mark_pauses_a_stream_without_reading_more(self):
        self.connection.write_buffer_size = (
            connection.Connection.STREAM_PUMP_BUFFER_HIGH_WATER)
        source = self.make_source([b'0123', b'4567'])
        stream = self.channel.basic_publish_stream('ex', 'rk', 8, source)

        self.assertEqual(stream.state, StreamingPublishState.ACTIVE)
        self.assertEqual(stream.bytes_sent, 0)
        self.assertEqual(source.calls, [])


class FailureAndTeardownTests(StreamingPublishTestBase):

    def test_source_exception_fails_the_stream_and_terminates(self):
        error = RuntimeError('disk gone')
        stream = self.channel.basic_publish_stream(
            'ex',
            'rk',
            8,
            RaisingSource(error),
            message_id='biz-42')

        self.assertEqual(stream.state, StreamingPublishState.FAILED)
        self.assertIs(stream.reason, error)
        self.assertEqual(len(self.connection.terminations), 1)
        terminal = self.connection.terminations[0]
        self.assertIsInstance(terminal, exceptions.MessageSourceUnavailable)
        self.assertEqual(terminal.message_id, 'biz-42')
        self.assertIs(terminal.__cause__, error)
        self.assertEqual(self.connection._stream_publishes, deque())

    def test_oversized_chunk_fails_with_body_too_long(self):
        stream = self.channel.basic_publish_stream(
            'ex', 'rk', 2, self.make_source([b'1234']))
        self.assertEqual(stream.state, StreamingPublishState.FAILED)
        self.assertIsInstance(stream.reason, exceptions.BodyTooLongError)

    def test_non_bytes_chunk_fails_with_type_error(self):

        class BadSource:

            def read(self, offset, length):
                return 'not bytes'

        stream = self.channel.basic_publish_stream('ex', 'rk', 2, BadSource())
        self.assertEqual(stream.state, StreamingPublishState.FAILED)
        self.assertIsInstance(stream.reason, TypeError)

    def test_channel_close_aborts_every_queued_stream(self):
        settled = []
        first = self.channel.basic_publish_stream(
            'ex',
            'rk',
            8,
            self.make_source([b'0123', b'']),
            message_id='one',
            on_settled=lambda s, state, reason: settled.append(
                (s.message_id, state)))
        second = self.channel.basic_publish_stream(
            'ex',
            'rk',
            4,
            self.make_source([b'bbbb']),
            message_id='two',
            on_settled=lambda s, state, reason: settled.append(
                (s.message_id, state)))

        reason = exceptions.StreamLostError('lost')
        self.channel._on_close_meta(reason)

        self.assertEqual(first.state, StreamingPublishState.ABORTED)
        self.assertIs(first.reason, reason)
        self.assertEqual(second.state, StreamingPublishState.ABORTED)
        self.assertEqual(
            [entry[1] for entry in settled],
            [StreamingPublishState.ABORTED, StreamingPublishState.ABORTED])
        self.assertEqual(self.channel._stream_publishes, deque())

    def test_settlement_callback_exception_is_contained(self):

        def boom(stream_publish, state, reason):
            raise RuntimeError('user callback failed')

        stream = self.channel.basic_publish_stream(
            'ex',
            'rk',
            2,
            self.make_source([b'ab']),
            on_settled=boom)
        self.assertEqual(stream.state, StreamingPublishState.COMPLETED)

    def test_failure_from_another_thread_is_scheduled_onto_the_loop(self):
        self.connection.manual_pump = True
        stream = self.channel.basic_publish_stream(
            'ex', 'rk', 2, self.make_source([b'ab']))
        # Drain the pump that completes the body.
        self.connection.run_pumps()
        self.assertEqual(stream.state, StreamingPublishState.COMPLETED)

        # A report on a settled stream is still routed through the loop hook,
        # where _request_stream_publish_failure turns it into a no-op.
        stream.failure(RuntimeError('late'))
        self.assertEqual(len(self.connection.pump_callbacks), 1)
        self.connection.run_pumps()
        self.assertEqual(stream.state, StreamingPublishState.COMPLETED)
        self.assertEqual(self.connection.terminations, [])

    def test_failure_on_an_active_stream_terminates_the_connection(self):
        self.connection.manual_pump = True
        stream = self.channel.basic_publish_stream(
            'ex', 'rk', 8, self.make_source([b'0123']), message_id='biz-7')
        error = RuntimeError('upstream reset')
        stream.failure(error)

        # Nothing terminates until the queued loop callback runs.
        self.assertEqual(self.connection.terminations, [])
        self.connection.run_pumps()
        self.assertEqual(stream.state, StreamingPublishState.FAILED)
        self.assertIs(stream.reason, error)
        self.assertEqual(
            self.connection.terminations[0].message_id, 'biz-7')

    def test_two_sources_failing_in_one_pass_terminate_once(self):
        other_channel = channel.Channel(self.connection, 8, mock.Mock())
        other_channel._set_state(channel.Channel.OPEN)
        first = self.channel.basic_publish_stream(
            'ex', 'rk', 8, RaisingSource(RuntimeError('one')),
            message_id='one')
        second = other_channel.basic_publish_stream(
            'ex', 'rk', 8, self.make_source([b'bb']), message_id='two')

        # The first failure records `_error` and the pump stops the pass, so the
        # transport is torn down exactly once. The second stream is left for the
        # connection's channel-close path to abort (covered separately).
        self.assertEqual(first.state, StreamingPublishState.FAILED)
        self.assertEqual(len(self.connection.terminations), 1)
        self.assertEqual(
            self.connection.terminations[0].message_id, 'one')
        self.assertFalse(second.is_settled)
        self.assertIn(second, self.connection._stream_publishes)


class StreamingPublishHandleTests(unittest.TestCase):
    """The thread-safe handle records a settlement produced on the I/O loop."""

    def _make_handle(self):
        wrapper = mock.Mock()
        scheduled = []
        wrapper._schedule_unchecked.side_effect = scheduled.append
        wrapper_channel = mock.Mock()
        wrapper_channel._wrapper = wrapper
        return StreamingPublishHandle(wrapper_channel), scheduled

    def test_records_terminal_state_and_signals_wait(self):
        handle, _scheduled = self._make_handle()
        self.assertIsNone(handle.state)
        self.assertFalse(handle.is_settled)

        handle._record_settlement(StreamingPublishState.COMPLETED, None)
        self.assertTrue(handle.is_settled)
        self.assertEqual(handle.wait(timeout=1),
                         StreamingPublishState.COMPLETED)
        self.assertIsNone(handle.reason)

    def test_wait_times_out_before_settlement(self):
        handle, _scheduled = self._make_handle()
        with self.assertRaises(TimeoutError):
            handle.wait(timeout=0.01)

    def test_failure_before_bind_is_scheduled_not_applied(self):
        handle, scheduled = self._make_handle()
        handle.failure(RuntimeError('early'))
        self.assertEqual(len(scheduled), 1)
        # Still unsettled until the queued callback runs and finds a bound impl.
        self.assertFalse(handle.is_settled)

    def test_failure_rejects_a_non_exception(self):
        handle, _scheduled = self._make_handle()
        with self.assertRaises(TypeError):
            handle.failure('not an exception')


if __name__ == '__main__':
    unittest.main()
