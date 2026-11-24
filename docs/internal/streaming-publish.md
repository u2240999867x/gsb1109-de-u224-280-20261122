# Streaming publish

The long-form record for `pika/streaming_publish.py` and the pump wired through `pika/connection.py` and `pika/channel.py`. The published contract is in `docs/modules/streaming_publish.md`; this page covers the invariants a change there must keep and the designs that were rejected.

## How it works

`Channel.basic_publish_stream` builds a `StreamingPublish` holding the `Basic.Publish` method frame, properties, the declared body size and the re-readable source. The object enters a per-channel `deque`. Only the head activates: `StreamingPublish._activate` emits the method and header frames and registers the object with the connection's pump. Every later object stays `PENDING` and has made no socket writes, so a second large object cannot interleave its frames with the first's.

The connection keeps one cross-channel `deque` of active streams and runs `_run_stream_pump` on the I/O loop. One pass pops each stream, calls its `_pump` for at most `STREAM_PUMP_FRAMES_PER_ROUND` body frames, then rotates it to the back if it did not settle. A pass that emitted any frame re-queues itself through `_adapter_add_callback_threadsafe`; a pass where every source returned empty parks on a `STREAM_SOURCE_RETRY_INTERVAL` timer instead of busy-spinning. The transport's write buffer is read through `_adapter_get_write_buffer_size`, and a stream pauses at `STREAM_PUMP_BUFFER_HIGH_WATER` so memory does not grow without bound when the socket is slower than the source.

`read` is called by absolute offset and never on another thread. An empty `bytes` result leaves the offset untouched. The pump therefore never needs a "resume" notification from the source: it simply re-polls on the timer or on the next scheduled pass after another stream made progress.

## Invariants

**Only the head of a channel's deque is ACTIVE.** `_on_stream_publish_settled` removes the settled head and activates the next object only when the head reached `COMPLETED`. A `FAILED` or `ABORTED` head tears the channel or connection down, so the rest are settled as aborted by the close path rather than activated. A regular `basic_publish` is another content body, so `Channel.basic_publish` refuses while the deque is non-empty; method frames that carry no content (acks, nacks, cancels) are not gated and still go out.

**A source failure closes the connection, not just the channel.** The content header has already advertised a fixed body length and AMQP 0-9-1 has no frame to retract a half-sent body. `_request_stream_publish_failure` settles the publish as `FAILED` and calls `_terminate_stream` with `MessageSourceUnavailable`, which carries the application `message_id`. `_terminate_stream` sets `_error` synchronously, and every pump guard reads `_error` rather than `is_open`, because the connection state stays `OPEN` until the async transport abort completes. The channel meta-close then aborts the remaining streams through `Channel._abort_stream_publishes`, each with its own `message_id`, so a job gets one result per object.

**Settlement happens once and is exception-safe.** `_settle` is a no-op once the state is terminal and the user `on_settled` callback is wrapped so a raising callback cannot escape into the pump or block the next object's FIFO advance. The thread-safe wrapper additionally moves its user callback onto the channel's work pool, keeping a slow handler off the I/O loop.

**The pump is single-threaded and re-entrant-safe.** All pump state is touched on the I/O loop thread. `StreamingPublish.failure` from another thread only schedules a callback; it never mutates the deque directly. A pass pops a fixed number of entries captured at entry, and a settle inside the pass removes the object from the connection deque without shortening the list the pass is iterating, because the pass owns its popped copy.

## Rejected designs

**Marshaling every body frame up front was the behavior being replaced.** `Connection._marshal_message` builds the full frame list before any of it reaches the transport, which for a tens-of-megabytes body allocates the whole object twice. The pump emits one frame per `read` and holds no more than a chunk.

**Letting a stalled stream block until its source recovered was rejected.** A blocking `read` on the I/O loop stalls every channel and the heartbeat timer, which under a negotiated heartbeat eventually drops the connection anyway. Empty `bytes` plus a bounded retry timer yields instead; status packets on another channel and heartbeats keep their slot in the same loop.

**Settling a failed publish without closing the connection was rejected.** There is no legal frame sequence that ends a partial content body, so any choice that leaves the connection open forces the broker to read the next channel's frames as the remainder of the broken body and report a protocol error with a confusing cause. The explicit `MessageSourceUnavailable` close gives the application a typed reason and a per-message id.

**Queueing regular `basic_publish` calls behind an active stream instead of refusing them was rejected.** It would require buffering their full marshaled bodies, recreating the allocation the feature exists to avoid, and makes the channel's write ordering depend on an unbounded hidden queue. The caller can wait for settlement or publish from another channel.

## Known gaps

- The buffer high-water mark relies on `_adapter_get_write_buffer_size`. The Twisted transport does not report its queued byte count, so its adapter returns 0 and only the per-round frame cap bounds a turn there.
- There is no end-to-end acceptance test against a broker in this change; the behavior is covered by unit tests on a connection with scripted adapter hooks. A broker-backed test would need a source that stalls mid-body under a second connection's traffic.
