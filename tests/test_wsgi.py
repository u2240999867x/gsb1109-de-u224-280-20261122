from __future__ import annotations

import sys
import threading
import typing
import wsgiref.validate
from functools import partial
from io import StringIO

import pytest

import httpx
from httpx._types import SyncByteStream

if typing.TYPE_CHECKING:  # pragma: no cover
    from _typeshed.wsgi import StartResponse, WSGIApplication, WSGIEnvironment


def application_factory(output: typing.Iterable[bytes]) -> WSGIApplication:
    def application(environ, start_response):
        status = "200 OK"

        response_headers = [
            ("Content-type", "text/plain"),
        ]

        start_response(status, response_headers)

        for item in output:
            yield item

    return wsgiref.validate.validator(application)


def echo_body(
    environ: WSGIEnvironment, start_response: StartResponse
) -> typing.Iterable[bytes]:
    status = "200 OK"
    output = environ["wsgi.input"].read()

    response_headers = [
        ("Content-type", "text/plain"),
    ]

    start_response(status, response_headers)

    return [output]


def echo_body_with_response_stream(
    environ: WSGIEnvironment, start_response: StartResponse
) -> typing.Iterable[bytes]:
    status = "200 OK"

    response_headers = [("Content-Type", "text/plain")]

    start_response(status, response_headers)

    def output_generator(f: typing.IO[bytes]) -> typing.Iterator[bytes]:
        while True:
            output = f.read(2)
            if not output:
                break
            yield output

    return output_generator(f=environ["wsgi.input"])


def raise_exc(
    environ: WSGIEnvironment,
    start_response: StartResponse,
    exc: type[Exception] = ValueError,
) -> typing.Iterable[bytes]:
    status = "500 Server Error"
    output = b"Nope!"

    response_headers = [
        ("Content-type", "text/plain"),
    ]

    try:
        raise exc()
    except exc:
        exc_info = sys.exc_info()
        start_response(status, response_headers, exc_info)

    return [output]


def log_to_wsgi_log_buffer(environ, start_response):
    print("test1", file=environ["wsgi.errors"])
    environ["wsgi.errors"].write("test2")
    return echo_body(environ, start_response)


def test_wsgi():
    transport = httpx.WSGITransport(app=application_factory([b"Hello, World!"]))
    client = httpx.Client(transport=transport)
    response = client.get("http://www.example.org/")
    assert response.status_code == 200
    assert response.text == "Hello, World!"


def test_wsgi_upload():
    transport = httpx.WSGITransport(app=echo_body)
    client = httpx.Client(transport=transport)
    response = client.post("http://www.example.org/", content=b"example")
    assert response.status_code == 200
    assert response.text == "example"


def test_wsgi_upload_with_response_stream():
    transport = httpx.WSGITransport(app=echo_body_with_response_stream)
    client = httpx.Client(transport=transport)
    response = client.post("http://www.example.org/", content=b"example")
    assert response.status_code == 200
    assert response.text == "example"


def test_wsgi_exc():
    transport = httpx.WSGITransport(app=raise_exc)
    client = httpx.Client(transport=transport)
    with pytest.raises(ValueError):
        client.get("http://www.example.org/")


def test_wsgi_http_error():
    transport = httpx.WSGITransport(app=partial(raise_exc, exc=RuntimeError))
    client = httpx.Client(transport=transport)
    with pytest.raises(RuntimeError):
        client.get("http://www.example.org/")


def test_wsgi_generator():
    output = [b"", b"", b"Some content", b" and more content"]
    transport = httpx.WSGITransport(app=application_factory(output))
    client = httpx.Client(transport=transport)
    response = client.get("http://www.example.org/")
    assert response.status_code == 200
    assert response.text == "Some content and more content"


def test_wsgi_generator_empty():
    output = [b"", b"", b"", b""]
    transport = httpx.WSGITransport(app=application_factory(output))
    client = httpx.Client(transport=transport)
    response = client.get("http://www.example.org/")
    assert response.status_code == 200
    assert response.text == ""


def test_logging():
    buffer = StringIO()
    transport = httpx.WSGITransport(app=log_to_wsgi_log_buffer, wsgi_errors=buffer)
    client = httpx.Client(transport=transport)
    response = client.post("http://www.example.org/", content=b"example")
    assert response.status_code == 200  # no errors
    buffer.seek(0)
    assert buffer.read() == "test1\ntest2"


@pytest.mark.parametrize(
    "url, expected_server_port",
    [
        pytest.param("http://www.example.org", "80", id="auto-http"),
        pytest.param("https://www.example.org", "443", id="auto-https"),
        pytest.param("http://www.example.org:8000", "8000", id="explicit-port"),
    ],
)
def test_wsgi_server_port(url: str, expected_server_port: str) -> None:
    """
    SERVER_PORT is populated correctly from the requested URL.
    """
    hello_world_app = application_factory([b"Hello, World!"])
    server_port: str | None = None

    def app(environ, start_response):
        nonlocal server_port
        server_port = environ["SERVER_PORT"]
        return hello_world_app(environ, start_response)

    transport = httpx.WSGITransport(app=app)
    client = httpx.Client(transport=transport)
    response = client.get(url)
    assert response.status_code == 200
    assert response.text == "Hello, World!"
    assert server_port == expected_server_port


def test_wsgi_server_protocol():
    server_protocol = None

    def app(environ, start_response):
        nonlocal server_protocol
        server_protocol = environ["SERVER_PROTOCOL"]
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"success"]

    transport = httpx.WSGITransport(app=app)
    with httpx.Client(transport=transport, base_url="http://testserver") as client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.text == "success"
    assert server_protocol == "HTTP/1.1"


class StreamingUpload(SyncByteStream):
    """
    An upload stream that stalls before its third chunk until the gate opens.

    Pulling the third chunk while the gate is closed is a test failure, and it
    is recorded so that the call site can assert it never happened.
    """

    def __init__(self, gate: threading.Event, fail_third: bool = False) -> None:
        self.gate = gate
        self.fail_third = fail_third
        self.pulls: list[int] = []
        self.closed = 0

    def __iter__(self) -> typing.Iterator[bytes]:
        chunks = [b"id,qty\n", b"1,7\n2,", b"9\n"]
        for index, chunk in enumerate(chunks):
            self.pulls.append(index)
            if index == 2:
                assert self.gate.is_set(), "third chunk pulled before gate opened"
                if self.fail_third:
                    raise ValueError("third chunk read failed")
            yield chunk

    def close(self) -> None:
        self.closed += 1


class StreamingReport:
    def __init__(self, write, wsgi_input, log, thread_local, batch):
        self._write = write
        self._input = wsgi_input
        self._log = log
        self._thread_local = thread_local
        self._batch = batch
        self.closed = 0

    def __iter__(self) -> typing.Iterator[bytes]:
        assert self._thread_local.batch == self._batch
        self._log["iter_batch"] = self._thread_local.batch
        try:
            yield self._input.readline()
            remainder = self._input.read(2)
            self._log["read(2)"] = remainder
            self._write(remainder)
            self._log["readline_after_write"] = self._input.readline()
            yield self._log["readline_after_write"]
        finally:
            self._log["finally"] = True
            assert self._thread_local.batch == self._batch
            self._log["finally_batch"] = self._thread_local.batch

    def close(self) -> None:
        self.closed += 1
        self._log.setdefault("closes", []).append("report")
        # The upload is still available while the report is being closed.
        assert self._input.read(0) == b""
        self._log["read(0)"] = True


class StreamingApplication:
    def __init__(self, log: dict[str, typing.Any], batch: int = 200) -> None:
        self.log = log
        self.batch = batch
        self.thread_local = threading.local()

    def __call__(self, environ, start_response):
        self.thread_local.batch = self.batch
        self.log["batch"] = self.batch
        header = environ["wsgi.input"].readline()
        self.log["header"] = header
        write = start_response("200 OK", [("Content-Type", "text/plain")])
        write(header)
        report = StreamingReport(
            write, environ["wsgi.input"], self.log, self.thread_local, self.batch
        )
        self.log["report"] = report
        return report


def build_streaming_request(gate, fail_third=False):
    log: dict[str, typing.Any] = {}
    upload = StreamingUpload(gate, fail_third=fail_third)
    app = StreamingApplication(log)
    transport = httpx.WSGITransport(app=app)
    client = httpx.Client(transport=transport)
    request = httpx.Request(
        "POST",
        "http://testserver/import",
        stream=upload,
        headers={"Content-Length": "15"},
    )
    return client, request, upload, log


def test_wsgi_streaming_upload_is_pulled_on_demand():
    gate = threading.Event()
    client, request, upload, log = build_streaming_request(gate)

    response = client.send(request, stream=True)
    assert response.status_code == 200
    iterator = response.iter_raw()

    assert next(iterator) == b"id,qty\n"
    assert upload.pulls == [0]
    assert next(iterator) == b"1,7\n"
    assert next(iterator) == b"2,"
    # The receipt for b"2," is delivered without pulling the rest of the body.
    assert upload.pulls == [0, 1]
    assert log["read(2)"] == b"2,"

    gate.set()
    assert next(iterator) == b"9\n"
    assert upload.pulls == [0, 1, 2]
    with pytest.raises(StopIteration):
        next(iterator)
    response.close()

    assert log["header"] == b"id,qty\n"
    assert log["readline_after_write"] == b"9\n"
    assert log["closes"] == ["report"]
    assert log["read(0)"] is True
    assert log["finally"] is True
    assert log["batch"] == log["iter_batch"] == log["finally_batch"] == 200
    assert upload.closed == 1
    assert log["report"].closed == 1


def test_wsgi_streaming_upload_error_is_raised_on_next_read():
    gate = threading.Event()
    client, request, upload, log = build_streaming_request(gate, fail_third=True)

    response = client.send(request, stream=True)
    assert response.status_code == 200
    iterator = response.iter_raw()

    assert next(iterator) == b"id,qty\n"
    assert next(iterator) == b"1,7\n"
    assert next(iterator) == b"2,"
    assert upload.pulls == [0, 1]

    gate.set()
    with pytest.raises(ValueError, match="third chunk read failed"):
        next(iterator)
    # The status and already-received receipts remain available.
    assert response.status_code == 200
    response.close()

    assert log["closes"] == ["report"]
    assert log["finally"] is True
    assert log["batch"] == log["iter_batch"] == log["finally_batch"] == 200
    assert upload.closed == 1
    assert log["report"].closed == 1


def test_wsgi_streaming_upload_close_stops_iteration():
    gate = threading.Event()
    client, request, upload, log = build_streaming_request(gate)

    response = client.send(request, stream=True)
    iterator = response.iter_raw()

    assert next(iterator) == b"id,qty\n"
    assert next(iterator) == b"1,7\n"
    assert next(iterator) == b"2,"
    response.close()

    assert upload.pulls == [0, 1]
    assert log["closes"] == ["report"]
    assert log["finally"] is True
    assert log["batch"] == log["iter_batch"] == log["finally_batch"] == 200
    assert upload.closed == 1
    assert log["report"].closed == 1

