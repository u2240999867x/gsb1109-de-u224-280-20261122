import gzip
import typing

import pytest

import httpx

REPORT = b"A,10\nB,200\nC,300\n"
COMPRESSED = gzip.compress(REPORT, mtime=0)
STRONG_ETAG = '"r7"'
URL = "http://example.org/exports/daily?part=north&mark=%2f&part=south&mark=%2F"


class ChunkStream(httpx.SyncByteStream):
    def __init__(self, chunks: typing.Sequence[bytes], *, disconnect: bool = False):
        self._chunks = list(chunks)
        self._disconnect = disconnect
        self.closed = False

    def __iter__(self) -> typing.Iterator[bytes]:
        yield from self._chunks
        if self._disconnect:
            raise httpx.ReadError("Connection dropped.")

    def close(self) -> None:
        self.closed = True


class AsyncChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks: typing.Sequence[bytes], *, disconnect: bool = False):
        self._chunks = list(chunks)
        self._disconnect = disconnect
        self.closed = False

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk
        if self._disconnect:
            raise httpx.ReadError("Connection dropped.")

    async def aclose(self) -> None:
        self.closed = True


def _partial_response(
    start: int,
    end: int,
    body: bytes,
    *,
    etag: str = STRONG_ETAG,
    content_range: str | None = None,
    content_encoding: str | None = "gzip",
) -> httpx.Response:
    headers = {
        "ETag": etag,
        "Content-Length": str(len(body)),
        "Content-Range": content_range
        if content_range is not None
        else f"bytes {start}-{end}/{len(COMPRESSED)}",
    }
    if content_encoding is not None:
        headers["Content-Encoding"] = content_encoding
    return httpx.Response(206, headers=headers, stream=ChunkStream([body]))


class ReportServer:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if "Range" not in request.headers:
            return httpx.Response(
                200,
                headers={
                    "ETag": STRONG_ETAG,
                    "Content-Encoding": "gzip",
                    "Content-Length": str(len(COMPRESSED)),
                },
                stream=ChunkStream([COMPRESSED[:7]], disconnect=True),
            )

        start = int(request.headers["Range"].split("=", 1)[1].split("-", 1)[0])
        if start == 7:
            return _partial_response(7, 16, COMPRESSED[7:17])
        if start == 17:
            return httpx.Response(
                200,
                headers={
                    "ETag": STRONG_ETAG,
                    "Content-Encoding": "gzip",
                    "Content-Length": str(len(COMPRESSED)),
                },
                stream=ChunkStream(
                    [COMPRESSED[:13], COMPRESSED[13:23]], disconnect=True
                ),
            )
        assert start == 23
        return _partial_response(23, len(COMPRESSED) - 1, COMPRESSED[23:])

    def continuation_headers(self) -> list[tuple[str, str]]:
        return [
            (request.headers["Range"], request.headers["If-Range"])
            for request in self.requests
            if "Range" in request.headers
        ]


def test_resumable_sync_stream() -> None:
    app = ReportServer()
    with httpx.Client(transport=httpx.MockTransport(app)) as client:
        with client.stream("GET", URL, resume=True) as response:
            chunks = list(response.iter_bytes(8))

    assert chunks == [b"A,10\nB,2", b"00\nC,300", b"\n"]
    assert [str(request.url) for request in app.requests] == [URL] * 4
    assert app.continuation_headers() == [
        ("bytes=7-", STRONG_ETAG),
        ("bytes=17-", STRONG_ETAG),
        ("bytes=23-", STRONG_ETAG),
    ]


@pytest.mark.anyio
async def test_resumable_async_stream() -> None:
    app = ReportServer()

    async def handler(request: httpx.Request) -> httpx.Response:
        response = app(request)
        stream = response.stream
        assert isinstance(stream, ChunkStream)
        response.stream = AsyncChunkStream(
            stream._chunks, disconnect=stream._disconnect
        )
        return response

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with client.stream("GET", URL, resume=True) as response:
            chunks = [chunk async for chunk in response.aiter_bytes(8)]

    assert chunks == [b"A,10\nB,2", b"00\nC,300", b"\n"]
    assert app.continuation_headers()[-1] == ("bytes=23-", STRONG_ETAG)


def test_iterator_level_opt_in() -> None:
    app = ReportServer()
    with httpx.Client(transport=httpx.MockTransport(app)) as client:
        with client.stream("GET", URL) as response:
            chunks = list(response.iter_bytes(8, resume=True))

    assert chunks == [b"A,10\nB,2", b"00\nC,300", b"\n"]


@pytest.mark.parametrize(
    "override",
    [
        pytest.param({"etag": '"r8"'}, id="changed-etag"),
        pytest.param({"content_range": "bytes 6-16/35"}, id="wrong-start"),
        pytest.param({"content_range": "bytes 7-16/99"}, id="wrong-total"),
        pytest.param({"content_encoding": "deflate"}, id="wrong-encoding"),
        pytest.param({"content_range": "not-a-range"}, id="bad-content-range"),
    ],
)
def test_invalid_continuation_fails_read(override: dict[str, typing.Any]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "Range" not in request.headers:
            return httpx.Response(
                200,
                headers={
                    "ETag": STRONG_ETAG,
                    "Content-Encoding": "gzip",
                    "Content-Length": str(len(COMPRESSED)),
                },
                stream=ChunkStream([COMPRESSED[:7]], disconnect=True),
            )
        if override["content_range"] == "not-a-range":
            return httpx.Response(
                206,
                headers={"ETag": STRONG_ETAG, "Content-Encoding": "gzip"},
                stream=ChunkStream([b"error body"]),
            )
        return _partial_response(
            7,
            16,
            COMPRESSED[7:17],
            etag=override.get("etag", STRONG_ETAG),
            content_range=override.get("content_range"),
            content_encoding=override.get("content_encoding", "gzip"),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with client.stream("GET", URL, resume=True) as response:
            with pytest.raises(httpx.ResumeError):
                list(response.iter_bytes(8))


def test_resume_request_limit() -> None:
    app = ReportServer()
    with httpx.Client(transport=httpx.MockTransport(app)) as client:
        with client.stream("GET", URL, resume=2) as response:
            with pytest.raises(httpx.ResumeError):
                list(response.iter_bytes(8))

    assert len(app.requests) == 3


def test_requires_strong_etag_and_length() -> None:
    def weak_etag(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"ETag": 'W/"r7"', "Content-Length": "10"},
            stream=ChunkStream([b"0123456789"]),
        )

    def no_length(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"ETag": STRONG_ETAG},
            stream=ChunkStream([b"0123456789"]),
        )

    for handler in (weak_etag, no_length):
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with client.stream("GET", URL, resume=True) as response:
                with pytest.raises(httpx.ResumeError):
                    list(response.iter_bytes(8))


def test_early_close_releases_response_without_more_requests() -> None:
    app = ReportServer()
    with httpx.Client(transport=httpx.MockTransport(app)) as client:
        with client.stream("GET", URL, resume=True) as response:
            chunks = response.iter_bytes(8)
            assert next(chunks) == b"A,10\nB,2"
            response.close()
            requests_after_close = len(app.requests)
            with pytest.raises((httpx.StreamClosed, httpx.ReadError)):
                next(chunks)

    assert len(app.requests) == requests_after_close


def test_no_progress_continuation_fails() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "Range" not in request.headers:
            return httpx.Response(
                200,
                headers={
                    "ETag": STRONG_ETAG,
                    "Content-Encoding": "gzip",
                    "Content-Length": str(len(COMPRESSED)),
                },
                stream=ChunkStream([COMPRESSED[:7]], disconnect=True),
            )
        return httpx.Response(
            200,
            headers={
                "ETag": STRONG_ETAG,
                "Content-Encoding": "gzip",
                "Content-Length": str(len(COMPRESSED)),
            },
            stream=ChunkStream([]),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with client.stream("GET", URL, resume=True) as response:
            with pytest.raises(httpx.ResumeError):
                list(response.iter_bytes(8))


def test_ordinary_streaming_is_unchanged() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"Hello, world!")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with client.stream("GET", URL) as response:
            assert list(response.iter_bytes()) == [b"Hello, world!"]
            assert not isinstance(response.stream, httpx.ResumableSyncStream)


def test_sync_resume_on_async_stream_is_rejected() -> None:
    async def stream() -> typing.AsyncIterator[bytes]:  # pragma: no cover
        yield b""

    response = httpx.Response(200, content=stream())
    response._resume_client = httpx.Client()
    with pytest.raises(RuntimeError):
        list(response.iter_bytes(resume=True))
