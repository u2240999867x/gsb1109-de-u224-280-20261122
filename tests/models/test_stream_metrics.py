import datetime
import typing
import zlib

import pytest

import httpx

CONTENT = b"Hello, world!" * 100


def gzip_compress(content: bytes) -> bytes:
    compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    return compressor.compress(content) + compressor.flush()


COMPRESSED = gzip_compress(CONTENT)
THIRD = len(COMPRESSED) // 3
RAW_CHUNKS = [
    COMPRESSED[:THIRD],
    COMPRESSED[THIRD : 2 * THIRD],
    COMPRESSED[2 * THIRD :],
]
# Transport wait per raw pull, including the final pull that hits EOF.
WAITS = [1.0, 2.0, 1.5, 0.5]
SOURCE_WAIT_TOTAL = sum(WAITS)
HEADERS_ELAPSED = 0.7


class ControlledClock:
    """
    A fake monotonic clock, advanced independently by the transport,
    by response hooks, and by the consumer.
    """

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ControlledStream(httpx.SyncByteStream):
    """
    A raw stream that advances the controlled clock by a given
    transport wait on each pull, optionally failing on a given pull.
    """

    def __init__(
        self,
        clock: ControlledClock,
        chunks: list[bytes],
        waits: list[float],
        error_pull: int | None = None,
    ) -> None:
        self._clock = clock
        self._chunks = chunks
        self._waits = waits
        self._error_pull = error_pull
        self.closed = False

    def __iter__(self) -> typing.Iterator[bytes]:
        for index, chunk in enumerate(self._chunks):
            self._clock.advance(self._waits[index])
            if self._error_pull == index:
                raise RuntimeError("Source read failed.")
            yield chunk
        self._clock.advance(self._waits[len(self._chunks)])
        if self._error_pull == len(self._chunks):
            raise RuntimeError("Source read failed.")

    def close(self) -> None:
        self.closed = True


class AsyncControlledStream(httpx.AsyncByteStream):
    def __init__(
        self,
        clock: ControlledClock,
        chunks: list[bytes],
        waits: list[float],
        error_pull: int | None = None,
    ) -> None:
        self._clock = clock
        self._chunks = chunks
        self._waits = waits
        self._error_pull = error_pull
        self.closed = False

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        for index, chunk in enumerate(self._chunks):
            self._clock.advance(self._waits[index])
            if self._error_pull == index:
                raise RuntimeError("Source read failed.")
            yield chunk
        self._clock.advance(self._waits[len(self._chunks)])
        if self._error_pull == len(self._chunks):
            raise RuntimeError("Source read failed.")

    async def aclose(self) -> None:
        self.closed = True


def streaming_response(
    clock: ControlledClock, stream: httpx.SyncByteStream
) -> tuple[httpx.Response, httpx.StreamMetrics]:
    meter = httpx.StreamMetrics(clock)
    meter.request_sent()
    clock.advance(HEADERS_ELAPSED)
    meter.headers_received()
    response = httpx.Response(
        200,
        headers={"Content-Encoding": "gzip"},
        stream=stream,
        stream_metrics=meter,
    )
    return response, meter


def async_streaming_response(
    clock: ControlledClock, stream: httpx.AsyncByteStream
) -> tuple[httpx.Response, httpx.StreamMetrics]:
    meter = httpx.StreamMetrics(clock)
    meter.request_sent()
    clock.advance(HEADERS_ELAPSED)
    meter.headers_received()
    response = httpx.Response(
        200,
        headers={"Content-Encoding": "gzip"},
        stream=stream,
        stream_metrics=meter,
    )
    return response, meter


def test_stream_metrics_response_hook_and_downstream():
    """
    Two responses with identical compressed content and identical
    transport timings. One has its first chunk read by a response hook
    before the same iterator is handed downstream, the other is consumed
    without any hook. The total time is the same, and the metered phases
    come out identical, with hook and downstream pauses both tracked as
    consumer stalls rather than source wait.
    """

    def handler_factory(
        clock: ControlledClock,
    ) -> typing.Callable[[httpx.Request], httpx.Response]:
        def handler(request: httpx.Request) -> httpx.Response:
            clock.advance(HEADERS_ELAPSED)
            stream = ControlledStream(clock, RAW_CHUNKS, WAITS)
            return httpx.Response(
                200,
                headers={"Content-Encoding": "gzip"},
                stream=stream,
            )

        return handler

    # Response A: the first chunk is read by a response hook.
    clock_a = ControlledClock()
    meter_a = httpx.StreamMetrics(clock_a)
    stash: dict[str, typing.Any] = {}

    def hook(response: httpx.Response) -> None:
        iterator = response.iter_bytes(chunk_size=200)
        stash["iterator"] = iterator
        stash["first"] = next(iterator)
        clock_a.advance(3.0)  # Hook-side processing of the first batch.

    client = httpx.Client(
        transport=httpx.MockTransport(handler_factory(clock_a)),
        event_hooks={"response": [hook]},
    )
    request = client.build_request(
        "GET", "https://example.org", extensions={"stream_metrics": meter_a}
    )
    response_a = client.send(request, stream=True)
    downstream = [stash["first"]]
    for chunk in stash["iterator"]:
        downstream.append(chunk)
        clock_a.advance(1.0)  # Downstream processing per chunk.
    response_a.close()

    # Response B: no hook, the consumer advances the clock after each chunk.
    clock_b = ControlledClock()
    meter_b = httpx.StreamMetrics(clock_b)
    client = httpx.Client(transport=httpx.MockTransport(handler_factory(clock_b)))
    request = client.build_request(
        "GET", "https://example.org", extensions={"stream_metrics": meter_b}
    )
    response_b = client.send(request, stream=True)
    chunks = []
    for chunk in response_b.iter_bytes(chunk_size=200):
        chunks.append(chunk)
        clock_b.advance(3.0 if len(chunks) == 1 else 1.0)
    response_b.close()

    assert b"".join(downstream) == CONTENT
    assert b"".join(chunks) == CONTENT

    assert response_a.stream_metrics is meter_a
    assert response_b.stream_metrics is meter_b

    assert meter_a.state is httpx.StreamState.COMPLETE
    assert meter_b.state is httpx.StreamState.COMPLETE
    assert meter_a.receipt() == meter_b.receipt()

    assert meter_a.headers_elapsed == datetime.timedelta(seconds=HEADERS_ELAPSED)
    assert meter_a.source_wait_elapsed == datetime.timedelta(seconds=SOURCE_WAIT_TOTAL)
    assert meter_a.consumer_stall_elapsed == datetime.timedelta(seconds=9.0)
    assert meter_a.num_bytes_downloaded == len(COMPRESSED)
    assert meter_a.num_chunks_delivered == 7

    # The existing `elapsed` behaviour is unaffected.
    assert isinstance(response_a.elapsed, datetime.timedelta)
    assert isinstance(response_b.elapsed, datetime.timedelta)


@pytest.mark.anyio
async def test_stream_metrics_response_hook_and_downstream_async():
    def handler_factory(
        clock: ControlledClock,
    ) -> typing.Callable[[httpx.Request], httpx.Response]:
        def handler(request: httpx.Request) -> httpx.Response:
            clock.advance(HEADERS_ELAPSED)
            stream = AsyncControlledStream(clock, RAW_CHUNKS, WAITS)
            return httpx.Response(
                200,
                headers={"Content-Encoding": "gzip"},
                stream=stream,
            )

        return handler

    clock_a = ControlledClock()
    meter_a = httpx.StreamMetrics(clock_a)
    stash: dict[str, typing.Any] = {}

    async def hook(response: httpx.Response) -> None:
        iterator = response.aiter_bytes(chunk_size=200)
        stash["iterator"] = iterator
        stash["first"] = await iterator.__anext__()
        clock_a.advance(3.0)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler_factory(clock_a)),
        event_hooks={"response": [hook]},
    )
    request = client.build_request(
        "GET", "https://example.org", extensions={"stream_metrics": meter_a}
    )
    response_a = await client.send(request, stream=True)
    downstream = [stash["first"]]
    async for chunk in stash["iterator"]:
        downstream.append(chunk)
        clock_a.advance(1.0)
    await response_a.aclose()

    clock_b = ControlledClock()
    meter_b = httpx.StreamMetrics(clock_b)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler_factory(clock_b)))
    request = client.build_request(
        "GET", "https://example.org", extensions={"stream_metrics": meter_b}
    )
    response_b = await client.send(request, stream=True)
    chunks = []
    async for chunk in response_b.aiter_bytes(chunk_size=200):
        chunks.append(chunk)
        clock_b.advance(3.0 if len(chunks) == 1 else 1.0)
    await response_b.aclose()

    assert b"".join(downstream) == CONTENT
    assert b"".join(chunks) == CONTENT

    assert meter_a.state is httpx.StreamState.COMPLETE
    assert meter_b.state is httpx.StreamState.COMPLETE
    assert meter_a.receipt() == meter_b.receipt()
    assert meter_a.headers_elapsed == datetime.timedelta(seconds=HEADERS_ELAPSED)
    assert meter_a.source_wait_elapsed == datetime.timedelta(seconds=SOURCE_WAIT_TOTAL)
    assert meter_a.consumer_stall_elapsed == datetime.timedelta(seconds=9.0)
    assert isinstance(response_a.elapsed, datetime.timedelta)
    assert isinstance(response_b.elapsed, datetime.timedelta)


def test_stream_metrics_final_chunk_delivered_after_eof():
    """
    The final decoded chunk is delivered from the buffer after the
    underlying stream has already hit EOF and closed. That delivery is
    not counted as source wait, and completing the iterator counts as
    complete delivery.
    """
    clock = ControlledClock()
    stream = ControlledStream(clock, RAW_CHUNKS, WAITS)
    response, meter = streaming_response(clock, stream)

    received = []
    for chunk in response.iter_bytes(chunk_size=200):
        received.append(chunk)
        if len(received) == 7:
            # The final decoded chunk is only delivered once the
            # underlying stream has already been exhausted and closed.
            assert response.is_closed
            assert stream.closed

    assert b"".join(received) == CONTENT
    assert meter.state is httpx.StreamState.COMPLETE
    assert meter.is_complete
    assert meter.num_chunks_delivered == 7
    assert meter.num_bytes_downloaded == len(COMPRESSED)
    assert meter.source_wait_elapsed == datetime.timedelta(seconds=SOURCE_WAIT_TOTAL)
    assert meter.headers_elapsed == datetime.timedelta(seconds=HEADERS_ELAPSED)


@pytest.mark.anyio
async def test_stream_metrics_final_chunk_delivered_after_eof_async():
    clock = ControlledClock()
    stream = AsyncControlledStream(clock, RAW_CHUNKS, WAITS)
    response, meter = async_streaming_response(clock, stream)

    received = []
    async for chunk in response.aiter_bytes(chunk_size=200):
        received.append(chunk)
        if len(received) == 7:
            assert response.is_closed
            assert stream.closed

    assert b"".join(received) == CONTENT
    assert meter.state is httpx.StreamState.COMPLETE
    assert meter.is_complete
    assert meter.num_chunks_delivered == 7
    assert meter.num_bytes_downloaded == len(COMPRESSED)
    assert meter.source_wait_elapsed == datetime.timedelta(seconds=SOURCE_WAIT_TOTAL)


def test_stream_metrics_close_after_eof_is_not_complete():
    """
    The underlying stream is at EOF and the final decoded chunk has been
    delivered, but the consumer closes directly instead of finishing the
    iterator. The receipt keeps the early-exit terminal state together
    with the read statistics.
    """
    clock = ControlledClock()
    stream = ControlledStream(clock, RAW_CHUNKS, WAITS)
    response, meter = streaming_response(clock, stream)

    received = []
    for chunk in response.iter_bytes(chunk_size=200):
        received.append(chunk)
        if len(received) == 7:
            assert response.is_closed
            response.close()
            break

    assert b"".join(received) == CONTENT
    assert meter.state is httpx.StreamState.CLOSED
    assert not meter.is_complete
    assert meter.num_chunks_delivered == 7
    assert meter.num_bytes_downloaded == len(COMPRESSED)
    assert meter.source_wait_elapsed == datetime.timedelta(seconds=SOURCE_WAIT_TOTAL)


@pytest.mark.anyio
async def test_stream_metrics_close_after_eof_is_not_complete_async():
    clock = ControlledClock()
    stream = AsyncControlledStream(clock, RAW_CHUNKS, WAITS)
    response, meter = async_streaming_response(clock, stream)

    received = []
    async for chunk in response.aiter_bytes(chunk_size=200):
        received.append(chunk)
        if len(received) == 7:
            assert response.is_closed
            await response.aclose()
            break

    assert b"".join(received) == CONTENT
    assert meter.state is httpx.StreamState.CLOSED
    assert not meter.is_complete
    assert meter.num_chunks_delivered == 7
    assert meter.num_bytes_downloaded == len(COMPRESSED)


def test_stream_metrics_early_close():
    """
    The consumer stops after the first batches and closes the response
    before the underlying stream is exhausted.
    """
    clock = ControlledClock()
    stream = ControlledStream(clock, RAW_CHUNKS, WAITS)
    response, meter = streaming_response(clock, stream)

    received = []
    for chunk in response.iter_bytes(chunk_size=200):
        received.append(chunk)
        if len(received) == 2:
            response.close()
            break

    assert stream.closed
    assert response.is_closed
    assert meter.state is httpx.StreamState.CLOSED
    assert not meter.is_complete
    assert meter.num_chunks_delivered == 2
    assert meter.num_bytes_downloaded <= len(COMPRESSED)
    assert meter.source_wait_elapsed < datetime.timedelta(seconds=SOURCE_WAIT_TOTAL)


@pytest.mark.anyio
async def test_stream_metrics_early_close_async():
    clock = ControlledClock()
    stream = AsyncControlledStream(clock, RAW_CHUNKS, WAITS)
    response, meter = async_streaming_response(clock, stream)

    received = []
    async for chunk in response.aiter_bytes(chunk_size=200):
        received.append(chunk)
        if len(received) == 2:
            await response.aclose()
            break

    assert meter.state is httpx.StreamState.CLOSED
    assert not meter.is_complete
    assert meter.num_chunks_delivered == 2
    assert meter.num_bytes_downloaded <= len(COMPRESSED)


def test_stream_metrics_raw_read_failure():
    """
    The underlying stream raises while the consumer is waiting for the
    next raw chunk. The receipt keeps the failure terminal state and the
    statistics gathered up to that point.
    """
    clock = ControlledClock()
    stream = ControlledStream(clock, RAW_CHUNKS, WAITS, error_pull=2)
    response, meter = streaming_response(clock, stream)

    with pytest.raises(RuntimeError):
        for _chunk in response.iter_bytes(chunk_size=200):
            pass

    assert meter.state is httpx.StreamState.FAILED
    assert not meter.is_complete
    assert meter.num_bytes_downloaded == len(RAW_CHUNKS[0]) + len(RAW_CHUNKS[1])
    # The wait on the failing pull is included in the source wait.
    assert meter.source_wait_elapsed == datetime.timedelta(seconds=4.5)

    response.close()
    # The failure terminal state is kept after closing.
    assert meter.state is httpx.StreamState.FAILED
    assert meter.num_bytes_downloaded == len(RAW_CHUNKS[0]) + len(RAW_CHUNKS[1])


@pytest.mark.anyio
async def test_stream_metrics_raw_read_failure_async():
    clock = ControlledClock()
    stream = AsyncControlledStream(clock, RAW_CHUNKS, WAITS, error_pull=2)
    response, meter = async_streaming_response(clock, stream)

    with pytest.raises(RuntimeError):
        async for _chunk in response.aiter_bytes(chunk_size=200):
            pass

    assert meter.state is httpx.StreamState.FAILED
    assert not meter.is_complete
    assert meter.num_bytes_downloaded == len(RAW_CHUNKS[0]) + len(RAW_CHUNKS[1])
    assert meter.source_wait_elapsed == datetime.timedelta(seconds=4.5)

    await response.aclose()
    assert meter.state is httpx.StreamState.FAILED


def test_stream_metrics_iter_raw_complete():
    """
    A consumer reading the raw stream directly is metered at the raw
    layer, and exhausting the raw stream counts as complete delivery.
    """
    clock = ControlledClock()
    stream = ControlledStream(clock, RAW_CHUNKS, WAITS)
    response, meter = streaming_response(clock, stream)

    received = []
    for chunk in response.iter_raw():
        received.append(chunk)
        clock.advance(0.5)

    assert b"".join(received) == COMPRESSED
    assert meter.state is httpx.StreamState.COMPLETE
    assert meter.num_chunks_delivered == 3
    assert meter.num_bytes_downloaded == len(COMPRESSED)
    assert meter.source_wait_elapsed == datetime.timedelta(seconds=SOURCE_WAIT_TOTAL)
    assert meter.consumer_stall_elapsed == datetime.timedelta(seconds=1.5)


@pytest.mark.anyio
async def test_stream_metrics_iter_raw_complete_async():
    clock = ControlledClock()
    stream = AsyncControlledStream(clock, RAW_CHUNKS, WAITS)
    response, meter = async_streaming_response(clock, stream)

    received = []
    async for chunk in response.aiter_raw():
        received.append(chunk)
        clock.advance(0.5)

    assert b"".join(received) == COMPRESSED
    assert meter.state is httpx.StreamState.COMPLETE
    assert meter.num_chunks_delivered == 3
    assert meter.num_bytes_downloaded == len(COMPRESSED)
    assert meter.consumer_stall_elapsed == datetime.timedelta(seconds=1.5)


def test_stream_metrics_client_full_read():
    """
    End-to-end through the client: the meter is passed as a request
    extension, the headers phase is marked by the client, and `elapsed`
    keeps its existing behaviour.
    """
    clock = ControlledClock()
    meter = httpx.StreamMetrics(clock)

    def handler(request: httpx.Request) -> httpx.Response:
        clock.advance(HEADERS_ELAPSED)
        stream = ControlledStream(clock, RAW_CHUNKS, WAITS)
        return httpx.Response(
            200,
            headers={"Content-Encoding": "gzip"},
            stream=stream,
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    response = client.request(
        "GET", "https://example.org", extensions={"stream_metrics": meter}
    )

    assert response.stream_metrics is meter
    assert response.text == CONTENT.decode("utf-8")
    assert meter.state is httpx.StreamState.COMPLETE
    assert meter.headers_elapsed == datetime.timedelta(seconds=HEADERS_ELAPSED)
    assert meter.source_wait_elapsed == datetime.timedelta(seconds=SOURCE_WAIT_TOTAL)
    assert meter.num_bytes_downloaded == len(COMPRESSED)
    assert isinstance(response.elapsed, datetime.timedelta)


@pytest.mark.anyio
async def test_stream_metrics_client_full_read_async():
    clock = ControlledClock()
    meter = httpx.StreamMetrics(clock)

    def handler(request: httpx.Request) -> httpx.Response:
        clock.advance(HEADERS_ELAPSED)
        stream = AsyncControlledStream(clock, RAW_CHUNKS, WAITS)
        return httpx.Response(
            200,
            headers={"Content-Encoding": "gzip"},
            stream=stream,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    response = await client.request(
        "GET", "https://example.org", extensions={"stream_metrics": meter}
    )

    assert response.stream_metrics is meter
    assert response.text == CONTENT.decode("utf-8")
    assert meter.state is httpx.StreamState.COMPLETE
    assert meter.headers_elapsed == datetime.timedelta(seconds=HEADERS_ELAPSED)
    assert meter.source_wait_elapsed == datetime.timedelta(seconds=SOURCE_WAIT_TOTAL)
    assert isinstance(response.elapsed, datetime.timedelta)


def test_stream_metrics_receipt():
    clock = ControlledClock()
    stream = ControlledStream(clock, RAW_CHUNKS, WAITS)
    response, meter = streaming_response(clock, stream)

    assert meter.state is httpx.StreamState.PENDING

    response.read()

    receipt = meter.receipt()
    assert receipt == {
        "state": httpx.StreamState.COMPLETE,
        "headers_elapsed": datetime.timedelta(seconds=HEADERS_ELAPSED),
        "source_wait_elapsed": datetime.timedelta(seconds=SOURCE_WAIT_TOTAL),
        "consumer_stall_elapsed": datetime.timedelta(seconds=0.0),
        "num_bytes_downloaded": len(COMPRESSED),
        "num_chunks_delivered": meter.num_chunks_delivered,
    }
    assert meter.num_chunks_delivered >= 1
