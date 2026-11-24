"""Streaming publication of a message body from a re-readable source.

A `Channel.basic_publish_stream` call emits the `Basic.Publish` method frame and the content header
up front, then hands body frames to the connection one at a time through
`StreamingPublish._pump`. The connection's pump runs on the I/O loop alongside every other frame,
so a source that is temporarily empty yields the loop instead of blocking the channels behind it,
and heartbeats and small messages on other channels keep going out.

The module owns only the per-message state machine. The connection owns the global pump and the
decision to tear it down after a source failure, and the channel owns serialization of the streams
opened on it.
"""

from __future__ import annotations

import enum
import logging
from typing import TYPE_CHECKING, Any, Callable

from pika import exceptions, frame, spec
from pika._utils import Protocol, runtime_checkable

if TYPE_CHECKING:
    from pika import amqp_object
    from pika.channel import Channel
    from pika.connection import Connection

LOGGER = logging.getLogger(__name__)

OnSettledCallback = Callable[['StreamingPublish', 'StreamingPublishState',
                              BaseException | None], None]


@runtime_checkable
class MessageBodySource(Protocol):
    """A re-readable source of message body bytes.

    Implementations serve the body on demand by absolute offset, so a frame that never made it to
    the broker can be rebuilt after a reconnect by reopening the source at offset zero and
    republishing under a fresh business identifier.

    ``read`` runs on the connection's I/O loop thread and must not block: a source whose bytes are
    not ready yet returns empty ``bytes`` and the pump asks again on a later loop iteration.
    """

    def read(self, offset: int, length: int) -> bytes:
        """
        Return up to *length* body bytes starting at *offset*.

        An empty `bytes` means the bytes are not available yet; the pump asks again later without
        advancing the offset. Raising aborts the publish with that exception. Returning data that
        is not `bytes`, or more bytes than the declared body has left, also aborts it: the frame
        sequence promised in the content header must match the source exactly.

        :param offset: Zero-based byte offset into the body.
        :param length: Maximum number of bytes to return; the negotiated body-frame payload limit.
        :returns: The bytes available at *offset*, never spanning past the end of the body.
        """


class StreamingPublishState(enum.Enum):
    """Lifecycle state of a `StreamingPublish`."""

    # Created and waiting behind another stream on the same channel; the method and header frames
    # have not been sent.
    PENDING = 'pending'
    # The method and header frames are out and body frames are being pumped.
    ACTIVE = 'active'
    # Every declared body byte has been handed to the transport. Broker acknowledgement, when
    # publisher confirms are enabled, still arrives through the existing confirm callback.
    COMPLETED = 'completed'
    # The source failed, returned unusable data, or `StreamingPublish.failure` was called.
    FAILED = 'failed'
    # The channel or connection closed before the body finished; the message needs reissue.
    ABORTED = 'aborted'


_TERMINAL_STATES = frozenset((StreamingPublishState.COMPLETED,
                              StreamingPublishState.FAILED,
                              StreamingPublishState.ABORTED))


class StreamingPublish:
    """A single in-progress streaming publish.

    Returned by `Channel.basic_publish_stream`. The object is the handle a producer thread uses to
    report an upstream failure through `failure`; normal progress is driven by the connection's
    I/O loop and needs no caller action.
    """

    def __init__(self,
                 channel: Channel,
                 source: MessageBodySource,
                 body_size: int,
                 message_id: Any,
                 on_settled: OnSettledCallback | None,
                 method_frame: amqp_object.Method,
                 properties: spec.BasicProperties) -> None:
        """
        :param channel: The channel the publish runs on.
        :param source: Re-readable body source.
        :param body_size: Total body size in bytes, advertised in the content header.
        :param message_id: Application-supplied business identifier, echoed back on settlement so
            the job can reissue the exact object after a failure.
        :param on_settled: Optional callback invoked once on the I/O loop thread with this object,
            the terminal state and the failure reason (``None`` for `COMPLETED`).
        :param method_frame: The `Basic.Publish` method frame held until the stream reaches the
            head of its channel.
        :param properties: Message properties emitted with the content header.
        """
        self._channel = channel
        self._connection: Connection = channel.connection
        self._source = source
        self._body_size = body_size
        self._message_id = message_id
        self._on_settled = on_settled
        self._method_frame = method_frame
        self._properties = properties

        self._offset = 0
        self._state = StreamingPublishState.PENDING
        self._reason: BaseException | None = None

    @property
    def message_id(self) -> Any:
        """The business identifier supplied to `basic_publish_stream`."""
        return self._message_id

    @property
    def state(self) -> StreamingPublishState:
        """Current lifecycle state."""
        return self._state

    @property
    def is_settled(self) -> bool:
        """True once the publish reached a terminal state."""
        return self._state in _TERMINAL_STATES

    @property
    def bytes_sent(self) -> int:
        """Body bytes handed to body frames so far."""
        return self._offset

    @property
    def reason(self) -> BaseException | None:
        """The failure reason in a terminal state, or ``None`` while active."""
        return self._reason

    def failure(self, reason: BaseException) -> None:
        """Report that the body source has failed permanently.

        Safe to call from any thread: the state change is scheduled on the connection's I/O loop
        thread so it cannot race the pump. The content header already promised a fixed-length body,
        so a failed source tears the connection down and settles every other in-flight stream as
        aborted. A settled publish ignores further reports.

        :param reason: The exception describing the failure; handed to the settlement callback.
        """
        if not isinstance(reason, BaseException):
            raise TypeError('reason must be an exception instance, '
                            f'but got {reason!r}')
        self._connection._adapter_add_callback_threadsafe(
            lambda: self._connection._request_stream_publish_failure(
                self, reason))

    def _mark_active(self) -> None:
        """Transition from PENDING to ACTIVE once the method and header frames are emitted."""
        assert self._state == StreamingPublishState.PENDING, self._state
        self._state = StreamingPublishState.ACTIVE

    def _activate(self) -> None:
        """Emit the method and content header frames and register with the connection pump.

        Called by the channel only once this stream reaches the head of its channel's FIFO. The
        header advertises the fixed body size up front, so the broker and the pump agree on how
        many body bytes follow.
        """
        assert self._state == StreamingPublishState.PENDING, self._state
        method_frame = frame.Method(self._channel.channel_number,
                                    self._method_frame)
        header_frame = frame.Header(self._channel.channel_number,
                                    self._body_size, self._properties)
        self._connection._output_marshaled_frames(
            [method_frame.marshal(), header_frame.marshal()])
        self._mark_active()
        self._connection._register_stream_publish(self)

    def _pump(self, should_pause: Callable[[int], bool]) -> int:
        """Emit body frames while the source can supply them.

        :param should_pause: Called with the number of body frames emitted this round; True ends
            the round so the I/O loop can service other channels, heartbeats and socket drains.
        :returns: The number of body frames emitted.
        """
        emitted = 0
        while self._state == StreamingPublishState.ACTIVE:
            remaining = self._body_size - self._offset
            if remaining == 0:
                self._settle(StreamingPublishState.COMPLETED, None)
                return emitted

            if should_pause(emitted):
                return emitted

            try:
                chunk = self._source.read(
                    self._offset, min(self._connection._body_max_length,
                                      remaining))
            except BaseException as exc:
                self._fail(exc)
                return emitted

            if not isinstance(chunk, bytes):
                self._fail(
                    TypeError(
                        'streaming publish source.read must return bytes, '
                        f'but got {type(chunk).__name__}'))
                return emitted

            if not chunk:
                # Temporarily out of bytes; yield without advancing the offset.
                return emitted

            if len(chunk) > remaining:
                self._fail(
                    exceptions.BodyTooLongError(
                        self._offset + len(chunk), self._body_size))
                return emitted

            try:
                marshaled = frame.Body(self._channel.channel_number,
                                       chunk).marshal()
            except BaseException as exc:
                self._fail(exc)
                return emitted

            self._connection._output_marshaled_frames([marshaled])
            self._offset += len(chunk)
            emitted += 1

        return emitted

    def _fail(self, reason: BaseException) -> None:
        """Route a source-side failure through the connection, which settles and terminates."""
        self._connection._request_stream_publish_failure(self, reason)

    def _abort(self, reason: BaseException | None) -> None:
        """Settle as ABORTED during channel or connection teardown."""
        self._settle(StreamingPublishState.ABORTED, reason)

    def _settle(self, state: StreamingPublishState,
                reason: BaseException | None) -> None:
        if self._state in _TERMINAL_STATES:
            return
        self._state = state
        self._reason = reason
        LOGGER.info('Streaming publish %r settled as %s: %r',
                    self._message_id, state.name, reason)
        self._channel._on_stream_publish_settled(self)
        if self._on_settled is not None:
            try:
                self._on_settled(self, state, reason)
            except Exception:
                LOGGER.exception(
                    'Streaming publish settlement callback for %r raised',
                    self._message_id)
