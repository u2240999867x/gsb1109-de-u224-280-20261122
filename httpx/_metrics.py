from __future__ import annotations

import datetime
import enum
import typing

__all__ = ["StreamMetrics", "StreamState"]


class StreamState(enum.Enum):
    """
    The terminal or in-progress state of a metered response stream.
    """

    PENDING = "pending"  # Meter attached, streaming not yet started.
    STREAMING = "streaming"  # Response stream is being consumed.
    COMPLETE = "complete"  # Final consumable data was delivered.
    CLOSED = "closed"  # Response was closed before delivery completed.
    FAILED = "failed"  # Reading the underlying stream raised an error.


class StreamMetrics:
    """
    Optional per-response stream metrics, aimed at archival-style pipelines
    that need to know where the time for a response actually went, rather
    than relying on `response.elapsed`, which conflates source delivery,
    consumer processing, and consumer pauses into a single figure.

    The caller provides a local monotonic `clock`, returning seconds as a
    float, against which three phases are sampled:

    * `headers_elapsed` - From the request being sent to the response
      headers being received.
    * `source_wait_elapsed` - Time spent waiting on the underlying stream
      for the next raw chunk.
    * `consumer_stall_elapsed` - Time between a chunk being delivered to
      the consumer and the consumer requesting more data.

    If a single decoded read requires several underlying pulls, each pull
    is accounted as source wait individually. Decoded output that is
    delivered from the buffer after the underlying stream has been
    exhausted is not counted as source wait again.

    The `state` and `receipt()` provide the terminal outcome. Delivery is
    only `COMPLETE` once the final consumable data has actually been
    delivered. Closing early or a failing raw read keep their own terminal
    state, together with the statistics gathered up to that point.

    Existing `response.elapsed` behaviour is unaffected.
    """

    def __init__(self, clock: typing.Callable[[], float]) -> None:
        self._clock = clock
        self._request_sent_at: float | None = None
        self._headers_elapsed = 0.0
        self._source_wait_elapsed = 0.0
        self._consumer_stall_elapsed = 0.0
        self._num_bytes_downloaded = 0
        self._num_chunks_delivered = 0
        self._delivered_at: float | None = None
        self._started = False
        self._decoded_layer = False
        self._delivery_complete = False
        self._closed = False
        self._failed = False

    # Public phase markers, used by the client or by user code...

    def request_sent(self) -> None:
        """
        Mark the point at which the request was sent.
        """
        self._request_sent_at = self._clock()

    def headers_received(self) -> None:
        """
        Mark the point at which the response headers were received.
        """
        if self._request_sent_at is not None:
            self._headers_elapsed = self._clock() - self._request_sent_at

    # Public readings...

    @property
    def headers_elapsed(self) -> datetime.timedelta:
        """
        Time from the request being sent to the response headers arriving.
        """
        return datetime.timedelta(seconds=self._headers_elapsed)

    @property
    def source_wait_elapsed(self) -> datetime.timedelta:
        """
        Total time spent waiting on the underlying stream for raw chunks.
        """
        return datetime.timedelta(seconds=self._source_wait_elapsed)

    @property
    def consumer_stall_elapsed(self) -> datetime.timedelta:
        """
        Total time the consumer spent paused between being delivered a
        chunk and requesting the next one.
        """
        return datetime.timedelta(seconds=self._consumer_stall_elapsed)

    @property
    def num_bytes_downloaded(self) -> int:
        """
        The number of raw bytes pulled from the underlying stream so far.
        """
        return self._num_bytes_downloaded

    @property
    def num_chunks_delivered(self) -> int:
        """
        The number of consumable chunks delivered so far.
        """
        return self._num_chunks_delivered

    @property
    def state(self) -> StreamState:
        if self._failed:
            return StreamState.FAILED
        if self._delivery_complete:
            return StreamState.COMPLETE
        if self._closed:
            return StreamState.CLOSED
        if self._started:
            return StreamState.STREAMING
        return StreamState.PENDING

    @property
    def is_complete(self) -> bool:
        """
        `True` if the final consumable data was actually delivered.
        """
        return self.state is StreamState.COMPLETE

    def receipt(self) -> dict[str, typing.Any]:
        """
        A snapshot of the metered phases, the terminal state, and the
        read statistics, trackable by source read wait and by stalls
        during which no data was being pulled.
        """
        return {
            "state": self.state,
            "headers_elapsed": self.headers_elapsed,
            "source_wait_elapsed": self.source_wait_elapsed,
            "consumer_stall_elapsed": self.consumer_stall_elapsed,
            "num_bytes_downloaded": self.num_bytes_downloaded,
            "num_chunks_delivered": self.num_chunks_delivered,
        }

    # Instrumentation hooks, used by `Response`...

    def _stream_started(self) -> None:
        self._started = True

    def _decoded_layer_started(self) -> None:
        # A decoded layer such as `iter_bytes()` is delivering to the
        # consumer, so stalls are measured there rather than at the
        # raw layer, to avoid counting the same pause twice.
        self._decoded_layer = True

    def _decoded_layer_finished(self) -> None:
        self._delivery_complete = True

    def _pull_started(self) -> float:
        return self._clock()

    def _raw_chunk_received(self, pull_started_at: float, num_bytes: int) -> None:
        self._source_wait_elapsed += self._clock() - pull_started_at
        self._num_bytes_downloaded += num_bytes

    def _raw_stream_finished(self, pull_started_at: float) -> None:
        self._source_wait_elapsed += self._clock() - pull_started_at
        if not self._decoded_layer:
            # With no decoded layer the raw deliveries are the final
            # deliveries, so exhausting the raw stream completes delivery.
            self._delivery_complete = True

    def _raw_read_failed(self, pull_started_at: float) -> None:
        self._source_wait_elapsed += self._clock() - pull_started_at
        self._failed = True

    def _chunk_delivered(self) -> None:
        self._num_chunks_delivered += 1
        self._delivered_at = self._clock()

    def _consumer_resumed(self) -> None:
        if self._delivered_at is not None:
            self._consumer_stall_elapsed += self._clock() - self._delivered_at
            self._delivered_at = None

    def _response_closed(self) -> None:
        self._closed = True
