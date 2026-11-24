"""
Example: streaming a large, fixed-length body with Connection.

basic_publish_stream publishes a message whose body is pulled from a source by
offset instead of being held whole. This fits a body of tens of megabytes, or
one whose bytes are produced slowly: when the source has nothing ready it
returns empty bytes and the I/O loop moves on, so status packets on another
channel and heartbeats keep going out while the large object waits.

The source is re-readable: read(offset, length) serves any offset, so after a
connection loss the object can be reopened at offset zero and republished under
a fresh business identifier. Here the source is an in-memory buffer; a real job
would back it with a file or object store that supports ranged reads.

Pair a streaming publish with publisher confirms if you need the broker's
Basic.Ack / Basic.Nack in addition to the local "all bytes handed to the
transport" settlement. The two signals are separate: settlement means the body
left this client, a confirm means the broker accepted it.
"""

import logging
import threading

import pika

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

EXCHANGE_NAME = 'test_exchange'
ROUTING_KEY = 'stream_key'


class ReReadableBufferSource:
    """Serve a fixed byte buffer by absolute offset, with an optional slow start.

    The first ``stall_reads`` calls return empty bytes to model a source whose
    next chunk is not available yet. The pump retries later without advancing
    the offset, and other channels keep flowing in the meantime.
    """

    def __init__(self, data: bytes, stall_reads: int = 0) -> None:
        self._data = data
        self._stalls_left = stall_reads
        self.lock = threading.Lock()

    def read(self, offset: int, length: int) -> bytes:
        with self.lock:
            if self._stalls_left:
                self._stalls_left -= 1
                return b''
            return self._data[offset:offset + length]


def main() -> None:
    parameters = pika.ConnectionParameters(
        credentials=pika.PlainCredentials('guest', 'guest'))
    connection = pika.Connection(parameters)
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, durable=True)

    # Business identifiers the job can reissue after a failure. A real job
    # persists these alongside the object so an aborted publish is retried.
    business_id = 'object-0001'
    body = (b'0123456789abcdef' * 4096)  # 64 KiB, large enough to span body frames

    settled = threading.Event()

    def on_settled(handle, state, reason):
        # Runs on the channel's worker thread, not the I/O loop thread.
        LOGGER.info('%s settled as %s (reason=%r)', handle.message_id,
                    state.name, reason)
        settled.set()

    source = ReReadableBufferSource(body, stall_reads=2)
    handle = channel.basic_publish_stream(
        exchange=EXCHANGE_NAME,
        routing_key=ROUTING_KEY,
        body_size=len(body),
        source=source,
        properties=pika.BasicProperties(content_type='application/octet-stream'),
        message_id=business_id,
        on_settled=on_settled)

    state = handle.wait()
    LOGGER.info('wait returned %s; body bytes handed over: %d', state.name,
                handle.bytes_sent())

    # A permanently failed source is reported from any thread. The content
    # header already promised a fixed body, so pika settles this publish as
    # FAILED and closes the connection; every other in-flight stream settles as
    # ABORTED, giving the job a per-object list to reissue by business id.
    #
    #   handle.failure(RuntimeError('object store unreachable'))

    connection.close()


if __name__ == '__main__':
    main()
