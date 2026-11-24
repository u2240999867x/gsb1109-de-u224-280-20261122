"""
A downstream caller of the streaming publish API.

Pins the types the three entry points return and the shape a custom source must satisfy, so an
annotation change on `pika.channel.Channel.basic_publish_stream`, the thread-safe
`StreamingPublishHandle` or the blocking wrapper fails the type check rather than only the runtime
tests. See `tests/typing/README.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import assert_type

from pika import MessageBodySource, StreamingPublish, StreamingPublishState, spec
from pika.adapters.thread_safe_connection import StreamingPublishHandle

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.adapters.thread_safe_connection import Channel
    from pika.channel import Channel as CoreChannel


class FileRangeSource:
    """A re-readable source; structurally satisfies `MessageBodySource`."""

    def read(self, offset: int, length: int) -> bytes:
        raise NotImplementedError


def start_on_core_channel(
        channel: CoreChannel) -> StreamingPublish:
    source: MessageBodySource = FileRangeSource()
    handle = channel.basic_publish_stream(
        exchange='ex',
        routing_key='rk',
        body_size=1024,
        source=source,
        properties=spec.BasicProperties(),
        message_id='biz-1')
    assert_type(handle, StreamingPublish)
    assert_type(handle.state, StreamingPublishState)
    assert_type(handle.bytes_sent, int)
    assert_type(handle.message_id, Any)
    return handle


def start_on_thread_safe_channel(channel: Channel) -> StreamingPublishHandle:
    handle = channel.basic_publish_stream(
        exchange='ex',
        routing_key='rk',
        body_size=1024,
        source=FileRangeSource(),
        message_id='biz-2')
    assert_type(handle, StreamingPublishHandle)
    assert_type(handle.wait(timeout=5), StreamingPublishState)
    assert_type(handle.is_settled, bool)
    handle.failure(RuntimeError('source lost'))
    return handle


def start_blocking(channel: BlockingChannel) -> StreamingPublish:
    handle = channel.basic_publish_stream(
        exchange='ex',
        routing_key='rk',
        body_size=1024,
        source=FileRangeSource(),
        message_id='biz-3')
    assert_type(handle, StreamingPublish)
    return handle
