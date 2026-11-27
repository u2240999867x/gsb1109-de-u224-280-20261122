from __future__ import annotations

import contextlib
import math
import typing

import anyio

from .._models import Request, Response
from .._types import AsyncByteStream
from .base import AsyncBaseTransport

if typing.TYPE_CHECKING:  # pragma: no cover
    import asyncio

    import trio
    from anyio.streams.memory import (
        MemoryObjectReceiveStream,
        MemoryObjectSendStream,
    )

    Event = typing.Union[asyncio.Event, trio.Event]


_Message = typing.MutableMapping[str, typing.Any]
_Receive = typing.Callable[[], typing.Awaitable[_Message]]
_Send = typing.Callable[
    [typing.MutableMapping[str, typing.Any]], typing.Awaitable[None]
]
_ASGIApp = typing.Callable[
    [typing.MutableMapping[str, typing.Any], _Receive, _Send], typing.Awaitable[None]
]

__all__ = ["ASGITransport"]

# How long an application is given to observe a disconnect and exit before it
# is cancelled, and how long it is given to run its cancellation cleanup.
DISCONNECT_GRACE = 0.1
CANCEL_GRACE = 1.0


def is_running_trio() -> bool:
    try:
        # sniffio is a dependency of trio.

        # See https://github.com/python-trio/trio/issues/2802
        import sniffio

        if sniffio.current_async_library() == "trio":
            return True
    except ImportError:  # pragma: nocover
        pass

    return False


def create_event() -> Event:
    if is_running_trio():
        import trio

        return trio.Event()

    import asyncio

    return asyncio.Event()


def _normalize_max_in_flight(max_in_flight: float) -> float:
    if max_in_flight is math.inf:
        return max_in_flight
    if isinstance(max_in_flight, bool) or not isinstance(max_in_flight, int):
        raise TypeError("max_in_flight must be an integer or math.inf")
    if max_in_flight < 1:
        raise ValueError("max_in_flight must be at least 1")
    return max_in_flight


class ASGIResponseStream(AsyncByteStream):
    def __init__(self, body: list[bytes]) -> None:
        self._body = body

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        yield b"".join(self._body)


class ASGIStreamingResponseStream(AsyncByteStream):
    """
    A response body that is produced incrementally by an ASGI application.

    The application runs as a background task. Response body chunks are handed
    to the reader through a memory object stream with a bounded in-flight
    limit, so an application producing many chunks is paused until the client
    reads them.

    `http.response.start` is recorded as soon as it is received, which allows
    the status and headers to be observed before the application completes. A
    clean end-of-stream is only reported once the application has sent a final
    `more_body=False` message *and* then returned.

    Exceptions raised by the application before the response starts are
    re-raised by `handle_async_request` (or converted into a 500 response when
    `raise_app_exceptions=False`). Exceptions raised after the response has
    started are raised from the stream after any chunks already delivered, so
    the status and body that were already sent are never reinterpreted.

    Closing or cancelling the stream disconnects the application, lets it
    observe `http.disconnect`, and cancels it if it does not exit. Cleanup is
    idempotent and waits (with a bound) for both sides to finish.
    """

    def __init__(
        self,
        app: _ASGIApp,
        scope: _Message,
        request_body_chunks: typing.AsyncIterator[bytes],
        request_method: str,
        max_in_flight: float,
        raise_app_exceptions: bool,
    ) -> None:
        self._app = app
        self._scope = scope
        self._request_body_chunks = request_body_chunks
        self._request_method = request_method
        self._max_in_flight = max_in_flight
        self._raise_app_exceptions = raise_app_exceptions

        self._status_code: int | None = None
        self._headers: list[tuple[bytes, bytes]] = []

        self._response_ready = anyio.Event()
        self._response_started = anyio.Event()
        self._response_complete = anyio.Event()
        self._disconnected = anyio.Event()
        self._app_done = anyio.Event()

        self._app_error: BaseException | None = None
        self._request_complete = False
        self._pending_request_chunk: bytes | None = None

        self._started = False
        self._closed = False
        self._streams_closed = False

        self._send_stream: MemoryObjectSendStream[bytes] | None = None
        self._receive_stream: MemoryObjectReceiveStream[bytes] | None = None
        self._cancel_scope: anyio.CancelScope | None = None

    @property
    def status_code(self) -> int | None:
        return self._status_code

    @property
    def headers(self) -> list[tuple[bytes, bytes]]:
        return self._headers

    @property
    def app_error(self) -> BaseException | None:
        return self._app_error

    # -- Application lifecycle ------------------------------------------------

    def start(self) -> None:
        """
        Start the application in the background.
        """
        if self._started:
            return
        self._started = True

        self._send_stream, self._receive_stream = anyio.create_memory_object_stream[
            bytes
        ](max_buffer_size=self._max_in_flight)
        self._cancel_scope = anyio.CancelScope()

        if is_running_trio():
            import trio

            trio.lowlevel.spawn_system_task(self._run_app)
        else:
            import asyncio

            asyncio.create_task(self._run_app())

    async def _run_app(self) -> None:
        assert self._cancel_scope is not None
        with self._cancel_scope:
            try:
                await self._app(self._scope, self._receive, self._send)
            except anyio.get_cancelled_exc_class():
                raise
            except BaseException as exc:  # noqa: BLE001
                self._app_error = exc
            finally:
                # Closing the send side is what signals the end of the body to
                # a reader. It must happen even if the app was cancelled. A
                # clean EOF is distinguished by `response_complete`.
                with anyio.CancelScope(shield=True):
                    await self._close_send_stream()
                if not self._response_started.is_set():
                    # Unblock `handle_async_request()`, which is waiting for
                    # either a response to start or the app to exit.
                    self._response_ready.set()
                self._app_done.set()

    async def _close_send_stream(self) -> None:
        if self._send_stream is not None:
            with contextlib.suppress(anyio.ClosedResourceError):
                await self._send_stream.aclose()

    async def _close_streams(self) -> None:
        if self._streams_closed:
            return
        self._streams_closed = True
        if self._send_stream is not None:
            with contextlib.suppress(anyio.ClosedResourceError):
                await self._send_stream.aclose()
        if self._receive_stream is not None:
            with contextlib.suppress(anyio.ClosedResourceError):
                await self._receive_stream.aclose()

    # -- ASGI receive/send ----------------------------------------------------

    async def _receive(self) -> _Message:
        if self._disconnected.is_set() or self._response_complete.is_set():
            return {"type": "http.disconnect"}

        if self._request_complete:
            # The application has read the whole request. Once the response is
            # complete, or the client disconnects, `receive()` reports a
            # disconnect.
            await self._wait_for_receive_disconnect()
            return {"type": "http.disconnect"}

        if self._pending_request_chunk is not None:
            body = self._pending_request_chunk
            self._pending_request_chunk = None
            return {"type": "http.request", "body": body, "more_body": True}

        # Wait for either the next request body chunk or for the client to
        # disconnect. Racing the two ensures that a cooperative application
        # blocked reading a chunked request can still observe a disconnect.
        winner: str | None = None
        result: _Message | None = None
        async with anyio.create_task_group() as task_group:

            async def read_chunk() -> None:
                nonlocal winner, result
                try:
                    body = await self._request_body_chunks.__anext__()
                except StopAsyncIteration:
                    self._request_complete = True
                    if winner is None:
                        winner = "chunk"
                        result = {
                            "type": "http.request",
                            "body": b"",
                            "more_body": False,
                        }
                else:
                    if winner is None:
                        winner = "chunk"
                        result = {
                            "type": "http.request",
                            "body": body,
                            "more_body": True,
                        }
                    else:
                        # The disconnect won the race, but the chunk has
                        # already been pulled from the request stream. Keep it
                        # so it is not silently lost.
                        self._pending_request_chunk = body
                task_group.cancel_scope.cancel()

            async def read_disconnect() -> None:
                nonlocal winner, result
                await self._disconnected.wait()
                if winner is None:
                    winner = "disconnect"
                    result = {"type": "http.disconnect"}
                task_group.cancel_scope.cancel()

            task_group.start_soon(read_chunk)
            task_group.start_soon(read_disconnect)

        if result is None:  # pragma: no cover
            result = {"type": "http.disconnect"}
        return result

    async def _wait_for_receive_disconnect(self) -> None:
        async with anyio.create_task_group() as task_group:

            async def response_finished() -> None:
                await self._response_complete.wait()
                task_group.cancel_scope.cancel()

            async def client_disconnected() -> None:
                await self._disconnected.wait()
                task_group.cancel_scope.cancel()

            task_group.start_soon(response_finished)
            task_group.start_soon(client_disconnected)

    async def _send(self, message: _Message) -> None:
        message_type = message["type"]

        if message_type == "http.response.start":
            assert not self._response_started.is_set()
            self._status_code = message["status"]
            self._headers = message.get("headers", [])
            self._response_started.set()
            self._response_ready.set()
            return

        if message_type == "http.response.body":
            assert self._response_started.is_set()
            assert not self._response_complete.is_set()

            body = message.get("body", b"")
            more_body = message.get("more_body", False)

            if body and self._request_method != "HEAD":
                await self._send_body(body)

            if not more_body:
                self._response_complete.set()
            return

        raise RuntimeError(f"Unexpected ASGI message type: {message_type!r}")

    async def _send_body(self, body: bytes) -> None:
        """
        Hand a body chunk to the reader.

        If the in-flight buffer is full the application is paused. A client
        disconnect releases the pause, so that an application blocked in
        `send()` can continue and observe `http.disconnect` from `receive()`.
        """
        send_stream = self._send_stream
        assert send_stream is not None

        if self._disconnected.is_set():
            return

        try:
            send_stream.send_nowait(body)
            return
        except anyio.WouldBlock:
            pass

        if self._disconnected.is_set():
            return

        async with anyio.create_task_group() as task_group:

            async def send_chunk() -> None:
                with contextlib.suppress(anyio.ClosedResourceError):
                    await send_stream.send(body)
                task_group.cancel_scope.cancel()

            async def wait_for_disconnect() -> None:
                await self._disconnected.wait()
                task_group.cancel_scope.cancel()

            task_group.start_soon(send_chunk)
            task_group.start_soon(wait_for_disconnect)

    # -- Reading the response -------------------------------------------------

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        assert self._receive_stream is not None

        try:
            while True:
                try:
                    body = await self._receive_stream.receive()
                except anyio.EndOfStream:
                    break
                yield body

            # The application has closed the response stream. Deliver any
            # pending cancellation before interpreting the end of the body:
            # a client that cancels while waiting for the next chunk must not
            # be turned into a spurious protocol error.
            await anyio.lowlevel.checkpoint()

            # A clean EOF is only reported once the application has actually
            # returned.
            await self._app_done.wait()

            # An exception raised by the application after the response has
            # started is surfaced after the chunks that were already
            # delivered. It must not cause the status and body that were
            # already sent to be re-interpreted as a whole-response 500 or as
            # a normal end-of-stream. With `raise_app_exceptions=False` the
            # response is simply truncated.
            error = self._app_error
            if error is not None:
                self._app_error = None
                await self._finish()
                if self._raise_app_exceptions:
                    raise error
                return

            if not self._response_complete.is_set():
                # The application exited without sending a final body message.
                # This is a protocol violation, not a normal end-of-stream, and
                # must not be silently treated as a successful completion.
                await self._finish()
                raise RuntimeError(
                    "ASGI application exited without sending a complete response body."
                )

            await self._finish()
        except BaseException:
            await self._shutdown()
            raise

    async def _finish(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Any pending `receive()` calls can now observe a disconnect.
        self._disconnected.set()
        await self._close_streams()

    async def _shutdown(self) -> None:
        # Cleanup must complete even when the reader has been cancelled, so
        # that both sides of the exchange are always released exactly once.
        with anyio.CancelScope(shield=True):
            if self._closed:
                return
            self._closed = True
            self._disconnected.set()

            if self._started and not self._app_done.is_set():
                # Give a cooperative application a chance to observe the
                # disconnect and return before cancelling it. Any chunks it
                # sends in the meantime are discarded, but keeping the send
                # side open lets it reach `receive()` and see the disconnect.
                with anyio.move_on_after(DISCONNECT_GRACE):
                    await self._app_done.wait()

            if self._started and not self._app_done.is_set():
                if self._cancel_scope is not None:
                    self._cancel_scope.cancel()
                with anyio.move_on_after(CANCEL_GRACE):
                    await self._app_done.wait()

            await self._close_streams()
            # Exceptions raised while the client was closing are not reported.
            self._app_error = None

    async def aclose(self) -> None:
        await self._shutdown()


class ASGITransport(AsyncBaseTransport):
    """
    A custom AsyncTransport that handles sending requests directly to an ASGI app.

    ```python
    transport = httpx.ASGITransport(
        app=app,
        root_path="/submount",
        client=("1.2.3.4", 123)
    )
    client = httpx.AsyncClient(transport=transport)
    ```

    Arguments:

    * `app` - The ASGI application.
    * `raise_app_exceptions` - Boolean indicating if exceptions in the application
       should be raised. Default to `True`. Can be set to `False` for use cases
       such as testing the content of a client 500 response.
    * `root_path` - The root path on which the ASGI application should be mounted.
    * `client` - A two-tuple indicating the client IP and port of incoming requests.
    * `stream` - Boolean indicating if response bodies should be streamed
       incrementally rather than buffered until the application completes.
       Default to `False`. Setting a finite `max_in_flight` also enables
       streaming.
    * `max_in_flight` - The maximum number of response body chunks held in flight
       while streaming. The application is paused once this limit is reached,
       providing backpressure. Default to `1` when `stream=True`, otherwise
       `math.inf` (unbounded). Use `math.inf` for an unbounded streaming
       buffer. A finite value enables streaming, even if `stream=False`.
    ```
    """

    def __init__(
        self,
        app: _ASGIApp,
        raise_app_exceptions: bool = True,
        root_path: str = "",
        client: tuple[str, int] = ("127.0.0.1", 123),
        stream: bool | None = None,
        max_in_flight: float | None = None,
    ) -> None:
        self.app = app
        self.raise_app_exceptions = raise_app_exceptions
        self.root_path = root_path
        self.client = client

        if max_in_flight is None:
            # A finite default keeps streaming bounded. When streaming is not
            # requested a finite limit is not needed, because the application
            # is run to completion anyway.
            finite = stream is True
            max_in_flight = 1 if finite else math.inf
        self.max_in_flight = _normalize_max_in_flight(max_in_flight)

        if stream is None:
            stream = self.max_in_flight is not math.inf
        self.stream = stream

    def _scope(self, request: Request) -> _Message:
        return {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": request.method,
            "headers": [(k.lower(), v) for (k, v) in request.headers.raw],
            "scheme": request.url.scheme,
            "path": request.url.path,
            "raw_path": request.url.raw_path.split(b"?")[0],
            "query_string": request.url.query,
            "server": (request.url.host, request.url.port),
            "client": self.client,
            "root_path": self.root_path,
        }

    async def _handle_streaming_request(self, request: Request) -> Response:
        assert isinstance(request.stream, AsyncByteStream)
        response_stream = ASGIStreamingResponseStream(
            app=self.app,
            scope=self._scope(request),
            request_body_chunks=request.stream.__aiter__(),
            request_method=request.method,
            max_in_flight=self.max_in_flight,
            raise_app_exceptions=self.raise_app_exceptions,
        )
        response_stream.start()

        try:
            await response_stream._response_ready.wait()
        except BaseException:
            await response_stream.aclose()
            raise

        if not response_stream._response_started.is_set():
            error = response_stream.app_error
            await response_stream.aclose()

            if self.raise_app_exceptions:
                if error is not None:
                    raise error
                raise RuntimeError(
                    "ASGI application returned without sending a response."
                )

            return Response(500, stream=ASGIResponseStream([]))

        assert response_stream.status_code is not None
        return Response(
            response_stream.status_code,
            headers=response_stream.headers,
            stream=response_stream,
        )

    async def handle_async_request(
        self,
        request: Request,
    ) -> Response:
        assert isinstance(request.stream, AsyncByteStream)

        if self.stream:
            return await self._handle_streaming_request(request)

        # ASGI scope.
        scope = self._scope(request)

        # Request.
        request_body_chunks = request.stream.__aiter__()
        request_complete = False

        # Response.
        status_code = None
        response_headers = None
        body_parts = []
        response_started = False
        response_complete = create_event()

        # ASGI callables.

        async def receive() -> dict[str, typing.Any]:
            nonlocal request_complete

            if request_complete:
                await response_complete.wait()
                return {"type": "http.disconnect"}

            try:
                body = await request_body_chunks.__anext__()
            except StopAsyncIteration:
                request_complete = True
                return {"type": "http.request", "body": b"", "more_body": False}
            return {"type": "http.request", "body": body, "more_body": True}

        async def send(message: typing.MutableMapping[str, typing.Any]) -> None:
            nonlocal status_code, response_headers, response_started

            if message["type"] == "http.response.start":
                assert not response_started

                status_code = message["status"]
                response_headers = message.get("headers", [])
                response_started = True

            elif message["type"] == "http.response.body":
                assert not response_complete.is_set()
                body = message.get("body", b"")
                more_body = message.get("more_body", False)

                if body and request.method != "HEAD":
                    body_parts.append(body)

                if not more_body:
                    response_complete.set()

        try:
            await self.app(scope, receive, send)
        except Exception:  # noqa: PIE-786
            if self.raise_app_exceptions:
                raise

            response_complete.set()
            if status_code is None:
                status_code = 500
            if response_headers is None:
                response_headers = {}

        assert response_complete.is_set()
        assert status_code is not None
        assert response_headers is not None

        stream = ASGIResponseStream(body_parts)

        return Response(status_code, headers=response_headers, stream=stream)
