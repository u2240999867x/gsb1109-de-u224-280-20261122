import json

import anyio
import pytest

import httpx


async def hello_world(scope, receive, send):
    status = 200
    output = b"Hello, World!"
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def echo_path(scope, receive, send):
    status = 200
    output = json.dumps({"path": scope["path"]}).encode("utf-8")
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def echo_raw_path(scope, receive, send):
    status = 200
    output = json.dumps({"raw_path": scope["raw_path"].decode("ascii")}).encode("utf-8")
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def echo_body(scope, receive, send):
    status = 200
    headers = [(b"content-type", "text/plain")]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    more_body = True
    while more_body:
        message = await receive()
        body = message.get("body", b"")
        more_body = message.get("more_body", False)
        await send({"type": "http.response.body", "body": body, "more_body": more_body})


async def echo_headers(scope, receive, send):
    status = 200
    output = json.dumps(
        {"headers": [[k.decode(), v.decode()] for k, v in scope["headers"]]}
    ).encode("utf-8")
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def raise_exc(scope, receive, send):
    raise RuntimeError()


async def raise_exc_after_response(scope, receive, send):
    status = 200
    output = b"Hello, World!"
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})
    raise RuntimeError()


@pytest.mark.anyio
async def test_asgi_transport():
    async with httpx.ASGITransport(app=hello_world) as transport:
        request = httpx.Request("GET", "http://www.example.com/")
        response = await transport.handle_async_request(request)
        await response.aread()
        assert response.status_code == 200
        assert response.content == b"Hello, World!"


@pytest.mark.anyio
async def test_asgi_transport_no_body():
    async with httpx.ASGITransport(app=echo_body) as transport:
        request = httpx.Request("GET", "http://www.example.com/")
        response = await transport.handle_async_request(request)
        await response.aread()
        assert response.status_code == 200
        assert response.content == b""


@pytest.mark.anyio
async def test_asgi():
    transport = httpx.ASGITransport(app=hello_world)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

    assert response.status_code == 200
    assert response.text == "Hello, World!"


@pytest.mark.anyio
async def test_asgi_urlencoded_path():
    transport = httpx.ASGITransport(app=echo_path)
    async with httpx.AsyncClient(transport=transport) as client:
        url = httpx.URL("http://www.example.org/").copy_with(path="/user@example.org")
        response = await client.get(url)

    assert response.status_code == 200
    assert response.json() == {"path": "/user@example.org"}


@pytest.mark.anyio
async def test_asgi_raw_path():
    transport = httpx.ASGITransport(app=echo_raw_path)
    async with httpx.AsyncClient(transport=transport) as client:
        url = httpx.URL("http://www.example.org/").copy_with(path="/user@example.org")
        response = await client.get(url)

    assert response.status_code == 200
    assert response.json() == {"raw_path": "/user@example.org"}


@pytest.mark.anyio
async def test_asgi_raw_path_should_not_include_querystring_portion():
    """
    See https://github.com/encode/httpx/issues/2810
    """
    transport = httpx.ASGITransport(app=echo_raw_path)
    async with httpx.AsyncClient(transport=transport) as client:
        url = httpx.URL("http://www.example.org/path?query")
        response = await client.get(url)

    assert response.status_code == 200
    assert response.json() == {"raw_path": "/path"}


@pytest.mark.anyio
async def test_asgi_upload():
    transport = httpx.ASGITransport(app=echo_body)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.post("http://www.example.org/", content=b"example")

    assert response.status_code == 200
    assert response.text == "example"


@pytest.mark.anyio
async def test_asgi_headers():
    transport = httpx.ASGITransport(app=echo_headers)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

    assert response.status_code == 200
    assert response.json() == {
        "headers": [
            ["host", "www.example.org"],
            ["accept", "*/*"],
            ["accept-encoding", "gzip, deflate, br, zstd"],
            ["connection", "keep-alive"],
            ["user-agent", f"python-httpx/{httpx.__version__}"],
        ]
    }


@pytest.mark.anyio
async def test_asgi_exc():
    transport = httpx.ASGITransport(app=raise_exc)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(RuntimeError):
            await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_exc_after_response():
    transport = httpx.ASGITransport(app=raise_exc_after_response)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(RuntimeError):
            await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_disconnect_after_response_complete():
    disconnect = False

    async def read_body(scope, receive, send):
        nonlocal disconnect

        status = 200
        headers = [(b"content-type", "text/plain")]

        await send(
            {"type": "http.response.start", "status": status, "headers": headers}
        )
        more_body = True
        while more_body:
            message = await receive()
            more_body = message.get("more_body", False)

        await send({"type": "http.response.body", "body": b"", "more_body": False})

        # The ASGI spec says of the Disconnect message:
        # "Sent to the application when a HTTP connection is closed or if receive is
        # called after a response has been sent."
        # So if receive() is called again, the disconnect message should be received
        message = await receive()
        disconnect = message.get("type") == "http.disconnect"

    transport = httpx.ASGITransport(app=read_body)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.post("http://www.example.org/", content=b"example")

    assert response.status_code == 200
    assert disconnect


@pytest.mark.anyio
async def test_asgi_exc_no_raise():
    transport = httpx.ASGITransport(app=raise_exc, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

        assert response.status_code == 500


class StreamingApp:
    """
    A local ASGI application that sends its status and headers, then produces
    pages of data. Between pages it waits until the client has read one, which
    makes the delivery order directly observable.

    `events` records application-side milestones:
    `start`, `sent-N`, and `end`.
    """

    def __init__(self, pages: int = 3, gate_last: bool = False) -> None:
        self.pages = pages
        self.gate_last = gate_last
        self.events: list[str] = []
        self.page_read = anyio.Event()
        self.last_page_read = anyio.Event()
        self.finished = anyio.Event()

    async def __call__(self, scope, receive, send):
        self.events.append("start")
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )

        for page in range(self.pages):
            await send(
                {
                    "type": "http.response.body",
                    "body": f"page-{page}".encode(),
                    "more_body": True,
                }
            )
            self.events.append(f"sent-{page}")

            if page + 1 < self.pages:
                await self.page_read.wait()
                self.page_read = anyio.Event()
            elif self.gate_last:
                await self.last_page_read.wait()

        await send({"type": "http.response.body", "body": b"", "more_body": False})
        self.events.append("end")
        self.finished.set()


class PartialThenWaitApp:
    """
    Sends status/headers and one page, then waits for the client to disconnect.

    Optionally drains a chunked request body first. The application records
    whether it observed `http.disconnect`, either while reading the request or
    while waiting in `receive()`.
    """

    def __init__(self, read_request: bool = False) -> None:
        self.read_request = read_request
        self.events: list[str] = []
        self.request_chunks: list[bytes] = []
        self.disconnect: str | None = None
        self.disconnected = anyio.Event()

    async def __call__(self, scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        self.events.append("start")

        if self.read_request:
            more_body = True
            while more_body:
                message = await receive()
                if message["type"] == "http.disconnect":
                    self.disconnect = "http.disconnect"
                    self.events.append("disconnect")
                    self.disconnected.set()
                    return
                self.request_chunks.append(message.get("body", b""))
                more_body = message.get("more_body", False)

        await send({"type": "http.response.body", "body": b"page-0", "more_body": True})
        self.events.append("sent")

        # The client will now close or cancel the response. `receive()` must
        # eventually report a disconnect.
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                self.disconnect = "http.disconnect"
                self.events.append("disconnect")
                self.disconnected.set()
                return


async def _iter_pages(response: httpx.Response) -> list[bytes]:
    return [chunk async for chunk in response.aiter_raw()]


@pytest.mark.anyio
async def test_asgi_stream_status_before_first_page():
    app = StreamingApp(pages=3)
    transport = httpx.ASGITransport(app=app, max_in_flight=1)

    async with httpx.AsyncClient(transport=transport) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            # The status and headers are available before the application has
            # finished sending the body.
            assert response.status_code == 200
            assert response.headers["content-type"] == "text/plain"
            assert not app.finished.is_set()

            chunks = []
            async for chunk in response.aiter_raw():
                chunks.append(chunk)
                if len(chunks) < app.pages:
                    app.page_read.set()

    assert b"".join(chunks) == b"page-0page-1page-2"
    assert app.events == [
        "start",
        "sent-0",
        "sent-1",
        "sent-2",
        "end",
    ]


@pytest.mark.anyio
async def test_asgi_stream_first_page_delivered_before_app_finishes():
    app = StreamingApp(pages=2, gate_last=True)
    transport = httpx.ASGITransport(app=app, max_in_flight=1)

    async with httpx.AsyncClient(transport=transport) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            iterator = response.aiter_raw()
            assert await iterator.__anext__() == b"page-0"
            app.page_read.set()

            # The final page is delivered while the application is still
            # waiting for the client to read it; it has not finished yet.
            assert await iterator.__anext__() == b"page-1"
            assert not app.finished.is_set()
            assert app.events == ["start", "sent-0", "sent-1"]

            app.last_page_read.set()
            with pytest.raises(StopAsyncIteration):
                await iterator.__anext__()

    assert app.events == ["start", "sent-0", "sent-1", "end"]


@pytest.mark.anyio
async def test_asgi_stream_respects_in_flight_limit():
    app = StreamingApp(pages=3)
    transport = httpx.ASGITransport(app=app, max_in_flight=1)

    async with httpx.AsyncClient(transport=transport) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            iterator = response.aiter_raw()
            assert await iterator.__anext__() == b"page-0"

            # With an in-flight limit of one, the application cannot get ahead
            # of the reader: it has only produced the first page.
            await anyio.sleep(0.01)
            assert app.events == ["start", "sent-0"]

            app.page_read.set()
            assert await iterator.__anext__() == b"page-1"
            app.page_read.set()
            assert await iterator.__anext__() == b"page-2"

            with pytest.raises(StopAsyncIteration):
                await iterator.__anext__()

    assert app.events == ["start", "sent-0", "sent-1", "sent-2", "end"]


@pytest.mark.anyio
async def test_asgi_stream_normal_eof_only_after_app_finishes():
    app = StreamingApp(pages=1, gate_last=True)
    transport = httpx.ASGITransport(app=app, max_in_flight=1)

    async with httpx.AsyncClient(transport=transport) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            iterator = response.aiter_raw()
            assert await iterator.__anext__() == b"page-0"
            assert not app.finished.is_set()

            app.last_page_read.set()
            with pytest.raises(StopAsyncIteration):
                await iterator.__anext__()

    assert app.finished.is_set()
    assert app.events == ["start", "sent-0", "end"]


@pytest.mark.anyio
async def test_asgi_stream_close_lets_app_observe_disconnect():
    app = PartialThenWaitApp()
    transport = httpx.ASGITransport(app=app, max_in_flight=1)

    async with httpx.AsyncClient(transport=transport) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            assert response.status_code == 200
            iterator = response.aiter_raw()
            assert await iterator.__anext__() == b"page-0"
            assert app.events == ["start", "sent"]

        with anyio.fail_after(1):
            await app.disconnected.wait()

    assert app.events == ["start", "sent", "disconnect"]


@pytest.mark.anyio
async def test_asgi_stream_cancel_lets_app_observe_disconnect():
    app = PartialThenWaitApp()
    transport = httpx.ASGITransport(app=app, max_in_flight=1)

    async with httpx.AsyncClient(transport=transport) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            assert response.status_code == 200
            iterator = response.aiter_raw()
            assert await iterator.__anext__() == b"page-0"

            # Cancel the task while it is waiting for the next body chunk.
            with anyio.CancelScope() as scope:
                scope.cancel()
                await anyio.sleep(0)

        with anyio.fail_after(1):
            await app.disconnected.wait()

    assert app.events == ["start", "sent", "disconnect"]


@pytest.mark.anyio
async def test_asgi_stream_read_request_and_disconnect():
    app = PartialThenWaitApp(read_request=True)
    transport = httpx.ASGITransport(app=app, max_in_flight=1)

    async def request_body():
        yield b"one"
        yield b"two"

    async with httpx.AsyncClient(transport=transport) as client:
        request = client.build_request(
            "POST", "http://www.example.org/", content=request_body()
        )
        response = await client.send(request, stream=True)
        try:
            iterator = response.aiter_raw()
            assert await iterator.__anext__() == b"page-0"
        finally:
            await response.aclose()

        with anyio.fail_after(1):
            await app.disconnected.wait()

    assert app.request_chunks == [b"one", b"two", b""]
    assert app.disconnect == "http.disconnect"


@pytest.mark.anyio
async def test_asgi_stream_does_not_consume_unread_request_chunks():
    request_chunks = [b"one", b"two", b"three"]

    async def request_body():
        for chunk in request_chunks:
            yield chunk

    async def app(scope, receive, send):
        # The application never reads the request body.
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send(
            {"type": "http.response.body", "body": b"page-0", "more_body": False}
        )

    transport = httpx.ASGITransport(app=app, max_in_flight=1)
    async with httpx.AsyncClient(transport=transport) as client:
        request = client.build_request(
            "POST", "http://www.example.org/", content=request_body()
        )
        response = await client.send(request, stream=True)
        try:
            assert response.status_code == 200
            assert await response.aread() == b"page-0"
        finally:
            await response.aclose()


@pytest.mark.anyio
async def test_asgi_stream_head_has_no_response_body():
    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", b"11")],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"no body for HEAD",
                "more_body": False,
            }
        )

    transport = httpx.ASGITransport(app=app, max_in_flight=1)

    async with httpx.AsyncClient(transport=transport) as client:
        async with client.stream("HEAD", "http://www.example.org/") as response:
            assert response.status_code == 200
            assert [chunk async for chunk in response.aiter_raw()] == []


@pytest.mark.anyio
async def test_asgi_stream_exception_before_status():
    async def app(scope, receive, send):
        raise RuntimeError("before-status")

    transport = httpx.ASGITransport(app=app, max_in_flight=1)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(RuntimeError, match="before-status"):
            await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_stream_exception_before_status_no_raise():
    async def app(scope, receive, send):
        raise RuntimeError("before-status")

    transport = httpx.ASGITransport(
        app=app, max_in_flight=1, raise_app_exceptions=False
    )
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

    assert response.status_code == 500
    assert response.content == b""


@pytest.mark.anyio
async def test_asgi_stream_exception_after_first_page():
    page_read = anyio.Event()
    raised = anyio.Event()

    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"page-0", "more_body": True})
        await page_read.wait()
        raised.set()
        raise RuntimeError("after-first-page")

    transport = httpx.ASGITransport(app=app, max_in_flight=1)
    async with httpx.AsyncClient(transport=transport) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            assert response.status_code == 200
            iterator = response.aiter_raw()
            # The first page is delivered before the later exception.
            assert await iterator.__anext__() == b"page-0"
            page_read.set()
            with anyio.fail_after(1):
                await raised.wait()
            with pytest.raises(RuntimeError, match="after-first-page"):
                await iterator.__anext__()


@pytest.mark.anyio
async def test_asgi_stream_exception_after_first_page_no_raise():
    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"page-0", "more_body": True})
        raise RuntimeError("after-first-page")

    transport = httpx.ASGITransport(
        app=app, max_in_flight=1, raise_app_exceptions=False
    )
    async with httpx.AsyncClient(transport=transport) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            assert response.status_code == 200
            assert await _iter_pages(response) == [b"page-0"]


@pytest.mark.anyio
async def test_asgi_stream_invalid_max_in_flight():
    with pytest.raises(ValueError):
        httpx.ASGITransport(app=hello_world, max_in_flight=0)
    with pytest.raises(TypeError):
        httpx.ASGITransport(app=hello_world, max_in_flight=True)


class BufferFullThenWaitApp:
    """
    Produces one more page than the in-flight limit so that the application is
    paused inside `send()` when the client stops reading.
    """

    def __init__(self, pages: int = 2) -> None:
        self.pages = pages
        self.events: list[str] = []
        self.disconnected = anyio.Event()

    async def __call__(self, scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        self.events.append("start")

        for page in range(self.pages):
            await send(
                {
                    "type": "http.response.body",
                    "body": f"page-{page}".encode(),
                    "more_body": True,
                }
            )
            self.events.append(f"sent-{page}")

        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                self.events.append("disconnect")
                self.disconnected.set()
                return


@pytest.mark.anyio
async def test_asgi_stream_close_while_send_is_paused():
    # Two pages, but the in-flight limit is one, so the app is paused in
    # `send()` for the second page until the client reads.
    app = BufferFullThenWaitApp(pages=2)
    transport = httpx.ASGITransport(app=app, max_in_flight=1)

    async with httpx.AsyncClient(transport=transport) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            assert response.status_code == 200
            iterator = response.aiter_raw()
            assert await iterator.__anext__() == b"page-0"

            # The app is blocked sending page-1: it has not advanced.
            assert app.events == ["start", "sent-0"]

        # Closing unblocks the paused send and lets the app observe the
        # disconnect, so cleanup does not hang.
        with anyio.fail_after(1):
            await app.disconnected.wait()

    assert app.events == ["start", "sent-0", "sent-1", "disconnect"]


@pytest.mark.anyio
async def test_asgi_stream_transport_close_releases_app():
    app = PartialThenWaitApp()
    transport = httpx.ASGITransport(app=app, max_in_flight=1)

    client = httpx.AsyncClient(transport=transport)
    async with client.stream("GET", "http://www.example.org/") as response:
        assert response.status_code == 200
        assert await response.aiter_raw().__anext__() == b"page-0"

    # Closing the transport while a response is in flight disconnects the app
    # and does not leave any waiter hanging.
    await client.aclose()
    with anyio.fail_after(1):
        await app.disconnected.wait()

    assert app.events == ["start", "sent", "disconnect"]


@pytest.mark.anyio
async def test_asgi_stream_aclose_is_idempotent():
    app = PartialThenWaitApp()
    transport = httpx.ASGITransport(app=app, max_in_flight=1)

    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.send(
            client.build_request("GET", "http://www.example.org/"), stream=True
        )
        assert await response.aiter_raw().__anext__() == b"page-0"

        await response.aclose()
        await response.aclose()

    with anyio.fail_after(1):
        await app.disconnected.wait()

    # The application observed the disconnect exactly once.
    assert app.events.count("disconnect") == 1
