from __future__ import annotations

import io
import threading
import typing

import pytest

import httpx
from httpx._multipart import MultipartStream
from httpx._types import RequestFiles

ARCHIVE = bytes(range(256)) * 1024  # 262_144 bytes, offset-verifiable.


def parse_multipart(
    content: bytes, content_type: str
) -> list[tuple[bytes, dict[bytes, bytes], bytes]]:
    boundary = content_type.split("boundary=")[-1].encode("ascii")
    parts = []
    for raw in content.split(b"--" + boundary)[1:-1]:
        raw = raw.removeprefix(b"\r\n").removesuffix(b"\r\n")
        headers_raw, _, body = raw.partition(b"\r\n\r\n")
        headers = {}
        for line in headers_raw.split(b"\r\n"):
            key, _, value = line.partition(b": ")
            headers[key.lower()] = value
        disposition = headers[b"content-disposition"]
        name = disposition.split(b'name="', 1)[1].split(b'"', 1)[0]
        parts.append((name, headers, body))
    return parts


def record(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, content=request.content)


def test_file_range_helper_and_slice_spec():
    archive = io.BytesIO(ARCHIVE)
    for spec in [
        httpx.FileRange(archive, 10, 20),
        httpx.FileRange(archive, slice(10, 20)),
        httpx.FileRange(archive, range(10, 20)),
        httpx.FileRange(archive, (10, 20)),
        httpx.FileRange(archive, 10),
    ]:
        assert spec.start == 10
        assert spec.stop in (20, None)

    with pytest.raises(httpx.MultipartRangeError):
        httpx.FileRange(archive, 20, 10)
    with pytest.raises(httpx.MultipartRangeError):
        httpx.FileRange(archive, -1, 10)
    with pytest.raises(ValueError):
        httpx.FileRange(archive, slice(0, 10, 2))


def test_explicit_byte_range_upload():
    archive = io.BytesIO(ARCHIVE)
    client = httpx.Client(transport=httpx.MockTransport(record))
    files = {
        "segment": httpx.FileRange(archive, 65532, 131080),
        "tail": httpx.FileRange(archive, 65528, 65540),
    }
    response = client.post("http://127.0.0.1/", files=files)
    boundary = response.request.headers["content-type"].split("boundary=")[-1]
    parts = parse_multipart(response.content, response.request.headers["content-type"])
    assert parts[0][2] == ARCHIVE[65532:131080]
    assert parts[1][2] == ARCHIVE[65528:65540]
    declared = int(response.request.headers["content-length"])
    assert declared == len(response.content)
    assert b"--" + boundary.encode() + b"--\r\n" in response.content


def test_range_in_file_tuple_and_offset_only():
    archive = io.BytesIO(ARCHIVE)
    client = httpx.Client(transport=httpx.MockTransport(record))
    files: RequestFiles = {
        "first": ("archive.bin", archive, 100, 200),
        "second": ("archive.bin", archive, slice(300, 320)),
        "third": ("archive.bin", archive, httpx.FileRange(archive, 400, 410)),
    }
    response = client.post("http://127.0.0.1/", files=files)
    parts = parse_multipart(response.content, response.request.headers["content-type"])
    assert [part[2] for part in parts] == [
        ARCHIVE[100:200],
        ARCHIVE[300:320],
        ARCHIVE[400:410],
    ]


def test_complete_upload_still_starts_at_zero():
    archive = io.BytesIO(ARCHIVE)
    archive.seek(1000)
    client = httpx.Client(transport=httpx.MockTransport(record))
    response = client.post("http://127.0.0.1/", files={"whole": archive})
    parts = parse_multipart(response.content, response.request.headers["content-type"])
    assert parts[0][2] == ARCHIVE
    # The caller's original position is not required to survive a complete
    # upload, but the range uploads below must not disturb each other.
    assert archive.tell() in (0, 1000)


def test_concurrent_range_uploads_share_cursor():
    archive = io.BytesIO(ARCHIVE)
    bookmark = archive.tell()
    results: dict[str, bytes] = {}
    content_types: dict[str, str] = {}
    errors: list[BaseException] = []
    started = threading.Event()
    release = threading.Event()

    def upload_a() -> None:
        try:
            stream = MultipartStream(
                {}, {"a1": httpx.FileRange(archive, 65532, 131080)}
            )
            stream.get_headers()
            iterator = iter(stream)
            first = next(iterator)
            started.set()
            release.wait(timeout=5)
            rest = list(iterator)
            results["a"] = first + b"".join(rest)
            content_types["a"] = stream.content_type
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    def upload_b() -> None:
        try:
            started.wait(timeout=5)
            stream = MultipartStream(
                {},
                {
                    "whole": archive,
                    "b1": httpx.FileRange(archive, 65528, 65540),
                },
            )
            results["b"] = b"".join(stream)
            content_types["b"] = stream.content_type
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)
        finally:
            release.set()

    thread_a = threading.Thread(target=upload_a)
    thread_b = threading.Thread(target=upload_b)
    thread_a.start()
    thread_b.start()
    thread_a.join(timeout=10)
    thread_b.join(timeout=10)
    assert not errors

    parts_a = parse_multipart(results["a"], content_types["a"])
    parts_b = parse_multipart(results["b"], content_types["b"])
    assert parts_a[0][2] == ARCHIVE[65532:131080]
    assert parts_b[0][2] == ARCHIVE
    assert parts_b[1][2] == ARCHIVE[65528:65540]
    assert archive.tell() == bookmark


def test_range_beyond_eof_reports_part_name():
    archive = io.BytesIO(ARCHIVE[:100])
    client = httpx.Client(transport=httpx.MockTransport(record))
    files = {"truncated-part": httpx.FileRange(archive, 50, 150)}
    with pytest.raises(httpx.MultipartError) as exc_info:
        client.post("http://127.0.0.1/", files=files)
    assert "truncated-part" in str(exc_info.value)


def test_unknown_length_one_shot_part_replay():
    class OneShot(io.IOBase):
        def __init__(self) -> None:
            self.pending = [b"log-1\n", b"log-2\n", b"log-3\n", b""]

        def read(self, *args: typing.Any) -> bytes:
            return self.pending.pop(0)

    archive = io.BytesIO(ARCHIVE)
    log = OneShot()
    stream = MultipartStream(
        {},
        [
            ("segment", httpx.FileRange(archive, 0, 16)),
            ("sanitizer", typing.cast(typing.IO[bytes], log)),
        ],
    )
    assert stream.get_headers() == {
        "Transfer-Encoding": "chunked",
        "Content-Type": stream.content_type,
    }
    content = b"".join(stream)
    parts = parse_multipart(content, stream.content_type)
    assert parts[0][2] == ARCHIVE[:16]
    assert parts[1][2] == b"log-1\nlog-2\nlog-3\n"

    with pytest.raises(httpx.MultipartStreamConsumed) as exc_info:
        b"".join(stream)
    assert "sanitizer" in str(exc_info.value)


class PartialConsumeTransport(httpx.BaseTransport):
    """
    Consumes the request stream up to a marker before delegating, so a test can
    model an entry point that has already read some of the form.
    """

    def __init__(self, handler: typing.Callable[[httpx.Request], httpx.Response]):
        self.handler = handler

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, httpx.SyncByteStream)
        iterator = iter(request.stream)
        chunks = []
        for chunk in iterator:
            chunks.append(chunk)
            if b"cleaning\n" in chunk or chunk == b"a":
                break
        request._consumed_prefix = b"".join(chunks)  # type: ignore[attr-defined]
        request._remaining = list(iterator)  # type: ignore[attr-defined]
        return self.handler(request)


class StreamingTransport(httpx.BaseTransport):
    """
    A transport that consumes the request body itself, pausing mid-stream so
    another upload can interleave on the same file object.
    """

    def __init__(
        self,
        handler: typing.Callable[[httpx.Request, bytes], httpx.Response],
        pause_after: int | None = None,
        paused: threading.Event | None = None,
        resume: threading.Event | None = None,
    ) -> None:
        self.handler = handler
        self.pause_after = pause_after
        self.paused = paused
        self.resume = resume

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, httpx.SyncByteStream)
        body = []
        for index, chunk in enumerate(request.stream):
            body.append(chunk)
            if self.pause_after is not None and index >= self.pause_after:
                if self.paused is not None:
                    self.paused.set()
                if self.resume is not None:
                    assert self.resume.wait(timeout=5)
        return self.handler(request, b"".join(body))


def test_a_stalls_while_b_uploads_and_a_follows_redirect():
    archive = io.BytesIO(ARCHIVE)
    bookmark = archive.tell()
    received: list[tuple[httpx.Request, bytes]] = []
    paused = threading.Event()
    resume = threading.Event()

    def handler(request: httpx.Request, body: bytes) -> httpx.Response:
        received.append((request, body))
        if request.url.path == "/import":
            return httpx.Response(
                307, headers={"location": "http://127.0.0.1/receiver"}
            )
        return httpx.Response(200)

    entry_then_receiver = StreamingTransport(
        handler, pause_after=1, paused=paused, resume=resume
    )
    client = httpx.Client(transport=entry_then_receiver)

    b_content: dict[str, bytes] = {}
    b_type: dict[str, str] = {}

    def upload_b() -> None:
        assert paused.wait(timeout=5)
        b_client = httpx.Client(transport=httpx.MockTransport(record))
        response = b_client.post(
            "http://127.0.0.1/receiver",
            files=[
                ("whole", archive),
                ("segment", httpx.FileRange(archive, 65528, 65540)),
            ],
        )
        b_content["body"] = response.content
        b_type["type"] = response.request.headers["content-type"]
        resume.set()

    thread_b = threading.Thread(target=upload_b)
    thread_b.start()
    response = client.post(
        "http://127.0.0.1/import",
        files=[("a", httpx.FileRange(archive, 65532, 131080))],
        follow_redirects=True,
    )
    thread_b.join(timeout=10)
    assert not thread_b.is_alive()
    assert response.status_code == 200
    assert len(received) == 2

    entry_request, entry_body = received[0]
    receiver_request, receiver_body = received[1]
    assert (
        entry_request.headers["content-length"]
        == receiver_request.headers["content-length"]
    )
    entry_parts = parse_multipart(entry_body, entry_request.headers["content-type"])
    receiver_parts = parse_multipart(
        receiver_body, receiver_request.headers["content-type"]
    )
    assert entry_parts[0][2] == ARCHIVE[65532:131080]
    assert receiver_parts[0][2] == ARCHIVE[65532:131080]
    assert receiver_parts[0][0] == entry_parts[0][0]

    b_parts = parse_multipart(b_content["body"], b_type["type"])
    assert b_parts[0][2] == ARCHIVE
    assert b_parts[1][2] == ARCHIVE[65528:65540]
    assert archive.tell() == bookmark

    archive = io.BytesIO(ARCHIVE)
    log = io.BytesIO(b"cleaning\n")

    def redirect(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/import":
            # Stop at the boundary before the sanitizer part.
            assert isinstance(request.stream, httpx.SyncByteStream)
            iterator = iter(request.stream)
            prefix = []
            while True:
                chunk = next(iterator)
                prefix.append(chunk)
                if b"sanitizer" in chunk:
                    break
            request._prefix = b"".join(prefix)  # type: ignore[attr-defined]
            return httpx.Response(307, headers={"location": "/receiver"})

        content = request.read()
        parts = parse_multipart(content, request.headers["content-type"])
        return httpx.Response(200, json={"sizes": [len(p[2]) for p in parts]})

    client = httpx.Client(transport=httpx.MockTransport(redirect))
    files: RequestFiles = [
        ("segment", httpx.FileRange(archive, 0, 32)),
        ("sanitizer", log),
    ]
    response = client.post(
        "http://127.0.0.1/import", files=files, follow_redirects=True
    )
    assert response.status_code == 200
    assert response.json() == {"sizes": [32, len(b"cleaning\n")]}


def test_redirect_after_unknown_length_part_started_is_reported():
    archive = io.BytesIO(ARCHIVE)

    class OneShot(io.IOBase):
        def __init__(self) -> None:
            self.pending = [b"a", b"b", b""]

        def read(self, *args: typing.Any) -> bytes:
            return self.pending.pop(0)

    def redirect(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/import":
            return httpx.Response(307, headers={"location": "/receiver"})
        return httpx.Response(200, content=request.content)

    client = httpx.Client(transport=PartialConsumeTransport(redirect))
    files: RequestFiles = [
        ("segment", httpx.FileRange(archive, 0, 8)),
        ("sanitizer", typing.cast(typing.IO[bytes], OneShot())),
    ]
    with pytest.raises(httpx.RequestNotReplayable) as exc_info:
        client.post("http://127.0.0.1/import", files=files, follow_redirects=True)
    assert "sanitizer" in str(exc_info.value)


@pytest.mark.anyio
async def test_async_range_upload_and_redirect_replay():
    archive = io.BytesIO(ARCHIVE)
    bookmark = archive.tell()
    received: list[tuple[httpx.Request, bytes]] = []
    b_bodies: list[bytes] = []
    b_types: list[str] = []
    paused = False

    class AsyncStreamingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            nonlocal paused
            assert isinstance(request.stream, httpx.AsyncByteStream)
            body = []
            async for chunk in request.stream:
                body.append(chunk)
                if request.url.path == "/import" and not paused:
                    paused = True
                    # Let a second async upload run against the same object.
                    async with httpx.AsyncClient(
                        transport=httpx.MockTransport(record)
                    ) as b_client:
                        response = await b_client.post(
                            "http://127.0.0.1/receiver",
                            files=[
                                ("whole", archive),
                                ("segment", httpx.FileRange(archive, 65528, 65540)),
                            ],
                        )
                    b_bodies.append(response.content)
                    b_types.append(response.request.headers["content-type"])
            received.append((request, b"".join(body)))
            if request.url.path == "/import":
                return httpx.Response(307, headers={"location": "/receiver"})
            return httpx.Response(200)

    async with httpx.AsyncClient(transport=AsyncStreamingTransport()) as client:
        response = await client.post(
            "http://127.0.0.1/import",
            files=[("a", httpx.FileRange(archive, 65532, 131080))],
            follow_redirects=True,
        )
    assert response.status_code == 200
    assert len(received) == 2
    entry_request, entry_body = received[0]
    receiver_request, receiver_body = received[1]
    assert (
        entry_request.headers["content-length"]
        == receiver_request.headers["content-length"]
    )
    assert (
        parse_multipart(entry_body, entry_request.headers["content-type"])[0][2]
        == (ARCHIVE[65532:131080])
    )
    assert (
        parse_multipart(receiver_body, receiver_request.headers["content-type"])[0][2]
        == ARCHIVE[65532:131080]
    )

    # B received the whole file from byte zero and its own range.
    b_parts = parse_multipart(b_bodies[0], b_types[0])
    assert b_parts[0][2] == ARCHIVE
    assert b_parts[1][2] == ARCHIVE[65528:65540]
    assert archive.tell() == bookmark


@pytest.mark.anyio
async def test_async_unknown_length_part_replay_error():
    archive = io.BytesIO(ARCHIVE)

    class OneShot(io.IOBase):
        def __init__(self) -> None:
            self.pending = [b"a", b"b", b""]

        def read(self, *args: typing.Any) -> bytes:
            return self.pending.pop(0)

    class PartialTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if request.url.path == "/import":
                assert isinstance(request.stream, httpx.AsyncByteStream)
                async for chunk in request.stream:
                    if chunk == b"a":
                        break
                return httpx.Response(307, headers={"location": "/receiver"})
            return httpx.Response(200)

    async with httpx.AsyncClient(transport=PartialTransport()) as client:
        with pytest.raises(httpx.RequestNotReplayable):
            await client.post(
                "http://127.0.0.1/import",
                files=[
                    ("segment", httpx.FileRange(archive, 0, 8)),
                    ("sanitizer", typing.cast(typing.IO[bytes], OneShot())),
                ],
                follow_redirects=True,
            )


def test_no_follow_redirect_still_returns_next_request():
    archive = io.BytesIO(ARCHIVE)

    class OneShot(io.IOBase):
        def __init__(self) -> None:
            self.pending = [b"a", b"b", b""]

        def read(self, *args: typing.Any) -> bytes:
            return self.pending.pop(0)

    def redirect(request: httpx.Request) -> httpx.Response:
        return httpx.Response(307, headers={"location": "/receiver"})

    client = httpx.Client(transport=PartialConsumeTransport(redirect))
    files: RequestFiles = [
        ("segment", httpx.FileRange(archive, 0, 8)),
        ("sanitizer", typing.cast(typing.IO[bytes], OneShot())),
    ]
    response = client.post(
        "http://127.0.0.1/import", files=files, follow_redirects=False
    )
    assert response.status_code == 307
    assert response.next_request is not None
    with pytest.raises((httpx.RequestNotReplayable, httpx.MultipartStreamConsumed)):
        client.send(response.next_request)
