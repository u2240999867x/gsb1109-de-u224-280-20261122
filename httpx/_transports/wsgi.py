from __future__ import annotations

import sys
import threading
import typing

from .._models import Request, Response
from .._types import SyncByteStream
from .base import BaseTransport

if typing.TYPE_CHECKING:
    from _typeshed import OptExcInfo  # pragma: no cover
    from _typeshed.wsgi import WSGIApplication  # pragma: no cover

__all__ = ["WSGITransport"]

_EOF = object()


class _InputAborted(BaseException):
    """
    Raised inside the WSGI application thread when the response is closed
    before the request body has been fully consumed.

    This intentionally inherits from `BaseException` so that application code
    using `except Exception` cannot accidentally swallow the unwind, while any
    `finally` blocks still get a chance to run.
    """


class _WSGIInput:
    """
    A `wsgi.input` implementation that pulls from the request stream lazily.

    Reads happen on the WSGI application thread. Whenever more data is needed
    the bridge hands control back to the client thread, which is responsible
    for advancing the request stream. This is what allows an application to
    observe a row receipt from the response before the client provides the
    remainder of the upload.
    """

    def __init__(self, bridge: _WSGIBridge) -> None:
        self._bridge = bridge
        self._buffer = b""
        self._eof = False

    def _read_more(self) -> bool:
        if self._eof:
            return False
        chunk = self._bridge._receive_input()
        if chunk is _EOF:
            self._eof = True
            return False
        self._buffer += typing.cast(bytes, chunk)
        return True

    def read(self, size: int = -1) -> bytes:
        if size is None:  # type: ignore[comparison-overlap]
            size = -1
        if size == 0:
            return b""
        if size < 0:
            while self._read_more():
                pass
            content, self._buffer = self._buffer, b""
            return content
        while len(self._buffer) < size and self._read_more():
            pass
        content = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return content

    def readline(self, size: int = -1) -> bytes:
        if size is None:  # type: ignore[comparison-overlap]
            size = -1
        if size == 0:
            return b""
        while True:
            if size < 0:
                index = self._buffer.find(b"\n")
                if index != -1:
                    end = index + 1
                    break
            else:
                index = self._buffer.find(b"\n", 0, size)
                if index != -1:
                    end = index + 1
                    break
                if len(self._buffer) >= size:
                    end = size
                    break
            if self._eof:
                end = len(self._buffer)
                break
            self._read_more()
        content = self._buffer[:end]
        self._buffer = self._buffer[end:]
        return content

    def readlines(self, hint: int = -1) -> list[bytes]:
        lines = []
        total = 0
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
            total += len(line)
            if hint >= 0 and total > hint:
                break
        return lines

    def __iter__(self) -> typing.Iterator[bytes]:
        while True:
            line = self.readline()
            if not line:
                break
            yield line


class _WSGIBridge:
    """
    Coordinates a WSGI application running on a worker thread with the client
    thread reading the response.

    The application thread and the client thread hand control back and forth in
    lock-step:

    * When the application needs more request data it asks the bridge for a
      chunk, which causes the client thread to advance the request stream by
      exactly one chunk.
    * When the application emits response data, either by calling `write()` or
      by yielding from its returned iterable, the chunk is handed to the client
      thread. The application does not continue until the client asks for the
      next chunk.

    That second rule is what defers any work the application does after a
    `write()` (for example reading the rest of a row) until the caller has
    actually consumed the receipt and asked for more.
    """

    def __init__(
        self,
        app: WSGIApplication,
        environ: dict[str, typing.Any],
        request: Request,
    ) -> None:
        self._app = app
        self._environ = environ
        self._request = request

        self._condition = threading.Condition()
        self._thread: threading.Thread | None = None

        self._status: str | None = None
        self._response_headers: list[tuple[str, str]] | None = None
        self._exc_info: OptExcInfo | None = None
        self._status_ready = False

        self._demand = 0
        self._slot: bytes | None = None
        self._error: BaseException | None = None
        self._finished = False
        self._returned = False

        self._input_pending = False
        self._input_data: bytes | object = b""
        self._input_error: BaseException | None = None

        self._close_requested = False
        self._closed = False

        self._result_closed = False

        self._upload_iterator: typing.Iterator[bytes] | None = None
        self._upload_closed = False

    # -- Application thread -------------------------------------------------

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="httpx-wsgi-transport", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        result: typing.Any = None
        returned = False
        try:
            result = self._app(self._environ, self._start_response)
            with self._condition:
                returned = True
                self._returned = True
                self._condition.notify_all()

            for chunk in iter(result):
                if chunk:
                    with self._condition:
                        if not self._status_ready:
                            raise AssertionError(
                                "The application yielded without calling "
                                "start_response"
                            )
                    self._put(chunk)
        except _InputAborted:
            pass
        except BaseException as exc:
            with self._condition:
                if self._error is None and not self._close_requested:
                    self._error = exc
                self._condition.notify_all()
        finally:
            try:
                if returned and result is not None:
                    self._close_result(result)
            finally:
                self._close_upload()
                with self._condition:
                    self._finished = True
                    self._condition.notify_all()

    def _close_result(self, result: typing.Any) -> None:
        with self._condition:
            if self._result_closed:
                return
            self._result_closed = True
        close = getattr(result, "close", None)
        if close is not None:
            close()

    def _put(self, data: bytes) -> None:
        """
        Hand a response chunk to the client, then wait until the client asks
        for the next chunk before allowing the application to continue.
        """
        with self._condition:
            while self._demand <= 0 and not self._closed:
                self._condition.wait()
            if self._closed:
                raise _InputAborted()
            self._demand -= 1
            self._slot = data
            self._condition.notify_all()

            while self._slot is not None and not self._closed:
                self._condition.wait()
            if self._closed:
                raise _InputAborted()

            while self._demand <= 0 and not self._closed:
                self._condition.wait()
            if self._closed:
                raise _InputAborted()

    # -- WSGI callables -----------------------------------------------------

    def _start_response(
        self,
        status: str,
        response_headers: list[tuple[str, str]],
        exc_info: OptExcInfo | None = None,
    ) -> typing.Callable[[bytes], typing.Any]:
        with self._condition:
            self._status = status
            self._response_headers = response_headers
            self._exc_info = exc_info
            self._status_ready = True
            self._condition.notify_all()
        return self._write

    def _write(self, data: bytes) -> None:
        if data:
            self._put(data)

    # -- Cross-thread data flow --------------------------------------------

    def _receive_input(self) -> bytes | object:
        """
        Called on the application thread to obtain the next request chunk.
        """
        with self._condition:
            if self._closed:
                raise _InputAborted()
            self._input_pending = True
            self._condition.notify_all()
            while self._input_pending and not self._closed:
                self._condition.wait()
            if self._closed:
                raise _InputAborted()
            if self._input_error is not None:
                error, self._input_error = self._input_error, None
                raise error
            data, self._input_data = self._input_data, b""
            return data

    def _service_input(self) -> None:
        """
        Called on the client thread to advance the request stream by one chunk.
        """
        with self._condition:
            if not self._input_pending or self._closed:
                return

        try:
            if self._upload_iterator is None:
                self._upload_iterator = iter(self._request.stream)
            while True:
                chunk = next(self._upload_iterator)
                if chunk:
                    break
            data: bytes | object = chunk
        except StopIteration:
            data = _EOF
        except BaseException as exc:
            with self._condition:
                self._input_error = exc
                self._input_pending = False
                if self._error is None:
                    self._error = exc
                self._condition.notify_all()
            return

        with self._condition:
            self._input_data = data
            self._input_pending = False
            self._condition.notify_all()

    def wait_for_response(self) -> tuple[str, list[tuple[str, str]], OptExcInfo | None]:
        """
        Wait until `start_response` has been called, driving request reads as
        required by the application.
        """
        while True:
            with self._condition:
                if self._status_ready:
                    assert self._status is not None
                    assert self._response_headers is not None
                    if self._error is not None and not self._returned:
                        # The application failed synchronously, after calling
                        # `start_response` but before returning its iterable.
                        raise self._error
                    return self._status, self._response_headers, self._exc_info
                if self._error is not None:
                    raise self._error
                if self._finished:
                    raise AssertionError(
                        "WSGI application returned without calling start_response"
                    )
                if not self._input_pending:
                    self._condition.wait()
                    continue
            self._service_input()

    def next_output(self) -> bytes:
        """
        Return the next response chunk, or raise/stop once complete.
        """
        with self._condition:
            self._demand += 1
            self._condition.notify_all()

        while True:
            with self._condition:
                if self._slot is not None:
                    data, self._slot = self._slot, None
                    self._condition.notify_all()
                    return data
                if self._error is not None:
                    raise self._error
                if self._finished and not self._input_pending:
                    raise StopIteration
                if not self._input_pending:
                    self._condition.wait()
                    continue
            self._service_input()

    def close(self) -> None:
        with self._condition:
            self._close_requested = True
            self._closed = True
            self._condition.notify_all()

        thread = self._thread
        if (
            thread is not None
            and thread is not threading.current_thread()
            and thread.is_alive()
        ):
            thread.join()

        self._close_upload()

    def _close_upload(self) -> None:
        with self._condition:
            if self._upload_closed:
                return
            self._upload_closed = True
        self._request.stream.close()


class _WSGIResponseStream(SyncByteStream):
    def __init__(self, bridge: _WSGIBridge) -> None:
        self._bridge = bridge

    def __iter__(self) -> typing.Iterator[bytes]:
        while True:
            try:
                yield self._bridge.next_output()
            except StopIteration:
                return

    def close(self) -> None:
        self._bridge.close()


class WSGITransport(BaseTransport):
    """
    A custom transport that handles sending requests directly to an WSGI app.
    The simplest way to use this functionality is to use the `app` argument.

    ```
    client = httpx.Client(app=app)
    ```

    Alternatively, you can setup the transport instance explicitly.
    This allows you to include any additional configuration arguments specific
    to the WSGITransport class:

    ```
    transport = httpx.WSGITransport(
        app=app,
        script_name="/submount",
        remote_addr="1.2.3.4"
    )
    client = httpx.Client(transport=transport)
    ```

    Arguments:

    * `app` - The WSGI application.
    * `raise_app_exceptions` - Boolean indicating if exceptions in the application
       should be raised. Default to `True`. Can be set to `False` for use cases
       such as testing the content of a client 500 response.
    * `script_name` - The root path on which the WSGI application should be mounted.
    * `remote_addr` - A string indicating the client IP of incoming requests.
    ```
    """

    def __init__(
        self,
        app: WSGIApplication,
        raise_app_exceptions: bool = True,
        script_name: str = "",
        remote_addr: str = "127.0.0.1",
        wsgi_errors: typing.TextIO | None = None,
    ) -> None:
        self.app = app
        self.raise_app_exceptions = raise_app_exceptions
        self.script_name = script_name
        self.remote_addr = remote_addr
        self.wsgi_errors = wsgi_errors

    def handle_request(self, request: Request) -> Response:
        port = request.url.port or {"http": 80, "https": 443}[request.url.scheme]
        environ: dict[str, typing.Any] = {
            "wsgi.version": (1, 0),
            "wsgi.url_scheme": request.url.scheme,
            "wsgi.errors": self.wsgi_errors or sys.stderr,
            "wsgi.multithread": True,
            "wsgi.multiprocess": False,
            "wsgi.run_once": False,
            "REQUEST_METHOD": request.method,
            "SCRIPT_NAME": self.script_name,
            "PATH_INFO": request.url.path,
            "QUERY_STRING": request.url.query.decode("ascii"),
            "SERVER_NAME": request.url.host,
            "SERVER_PORT": str(port),
            "SERVER_PROTOCOL": "HTTP/1.1",
            "REMOTE_ADDR": self.remote_addr,
        }
        for header_key, header_value in request.headers.raw:
            key = header_key.decode("ascii").upper().replace("-", "_")
            if key not in ("CONTENT_TYPE", "CONTENT_LENGTH"):
                key = "HTTP_" + key
            environ[key] = header_value.decode("ascii")

        bridge = _WSGIBridge(
            app=self.app,
            environ=environ,
            request=request,
        )
        environ["wsgi.input"] = _WSGIInput(bridge)
        bridge.start()

        try:
            seen_status, seen_response_headers, seen_exc_info = (
                bridge.wait_for_response()
            )
        except BaseException:
            bridge.close()
            raise

        if seen_exc_info and seen_exc_info[0] and self.raise_app_exceptions:
            bridge.close()
            raise seen_exc_info[1]

        status_code = int(seen_status.split()[0])
        headers = [
            (key.encode("ascii"), value.encode("ascii"))
            for key, value in seen_response_headers
        ]

        stream = _WSGIResponseStream(bridge)
        return Response(status_code, headers=headers, stream=stream)
