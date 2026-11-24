# Streaming Publish

`basic_publish_stream` publishes a message whose body is streamed from a re-readable source instead of being held whole. It is for bodies too large to assemble up front, or produced slowly, where a regular `basic_publish` would either cost the full body in memory or block the connection while the bytes are gathered.

## The source contract

A source is any object with `read(offset, length) -> bytes`. The offset is absolute, so the same byte can be re-read after a failure: a job that loses the connection reopens its object at offset zero and republishes under a fresh business identifier.

`read` runs on the connection's I/O loop thread and must not block. Returning empty `bytes` means the next chunk is not ready yet; the pump retries on a later loop iteration and leaves the offset unchanged. Raising, returning a non-`bytes` value, or returning more bytes than the declared body has left, fails the publish.

## How the frames and fairness work

The `Basic.Publish` method frame and the content header are emitted first, with the header advertising the full `body_size`. Body frames are then pulled one at a time by the pump. A pump pass gives each active stream a bounded, round-robin turn, so:

- A source that runs dry yields the loop; status packets on another channel and heartbeats keep going out.
- Streams on one channel are strictly FIFO. A second stream queues in `PENDING` and emits nothing until the first settles, so two content bodies never interleave. A regular `basic_publish`, itself a content body, is refused while a stream is queued.
- Streams on different channels share the pump fairly, so a small message on another channel is not stuck behind a large object on this one.

## Settlement and failure

The returned `StreamingPublish` moves through `PENDING` and `ACTIVE` before reaching one of the terminal states `COMPLETED`, `FAILED` or `ABORTED`. `COMPLETED` means every declared body byte was handed to the transport; with publisher confirms enabled, the broker's `Basic.Ack`/`Basic.Nack` is still a separate, later signal through the existing confirm callback.

A `FAILED` source cannot be repaired on the wire: the content header already promised a fixed-length body and AMQP 0-9-1 has no frame to retract it. Pika therefore settles that publish as `FAILED` and closes the connection. Every other in-flight stream settles as `ABORTED` through the channel close, each carrying the application-supplied `message_id`, so the job can reissue exactly those objects.

::: pika.streaming_publish
    options:
      members: true
      members_order: source
