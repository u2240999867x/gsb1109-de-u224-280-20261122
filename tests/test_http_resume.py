"""
Resumable http download tests for get_sheet and get_book.

The scripted mock server below replays the supplier order scenario:
routing endpoints are always hit with plain GET requests while the
Range / If-Range headers are only sent to the actual file address.
"""

import threading
from unittest import TestCase

import pyexcel as pe
from pyexcel._compact import request
from pyexcel.exceptions import (
    DownloadInterrupted,
    DownloadResponseMismatch,
    IncompleteDownload,
    StaleDownload,
)
from pyexcel.plugins.sources.resume import (
    STATE_COMPLETE,
    STATE_INCOMPLETE,
    STATE_INTERRUPTED,
    STATE_MISMATCH,
    STATE_STALE,
)

DISPATCH_URL = "https://supplier.example/dispatch"
QUEUE_URL = "https://supplier.example/queue"
FILE_URL = "https://supplier.example/orders.csv?site=north"

V1_CONTENT = 'order,note\nA01,"北仓"\n'.encode("utf-8")
V2_CONTENT = (
    'order,note\nA02,"南仓\n二号门"\n'
).encode("utf-8")

V1_EXPECTED = [["order", "note"], ["A01", "北仓"]]
V2_EXPECTED = [
    ["order", "note"],
    ["A02", "南仓\n二号门"],
]


class FakeInfo:
    def __init__(self, headers, content_type="application/octet-stream"):
        self._headers = {key.lower(): value for key, value in headers.items()}
        self._content_type = content_type

    def get(self, key, default=None):
        return self._headers.get(key.lower(), default)

    def get_content_type(self):
        return self._content_type


class FakeConnection:
    def __init__(self, status, headers, body, url):
        self.status = status
        self._info = FakeInfo(headers)
        self._body = body
        self._url = url
        self._offset = 0
        self.closed = False

    def info(self):
        return self._info

    def geturl(self):
        return self._url

    def getcode(self):
        return self.status

    def read(self, size=-1):
        if self._offset >= len(self._body):
            return b""
        if size is None or size < 0:
            chunk = self._body[self._offset:]
        else:
            chunk = self._body[self._offset:self._offset + size]
        self._offset += len(chunk)
        return chunk

    def close(self):
        self.closed = True


class HangingConnection(FakeConnection):
    """A 206 connection whose body is withheld until release() is called."""

    def __init__(self, status, headers, head, tail, url):
        super().__init__(status, headers, head, url)
        self._tail = tail
        self._release = threading.Event()

    def read(self, size=-1):
        if self._offset >= len(self._body):
            self._release.wait()
            if not self._tail:
                return b""
            chunk = self._tail
            self._tail = b""
            self._body += chunk
            return chunk
        return super().read(size)

    def release(self):
        self._release.set()


class RaisingConnection(FakeConnection):
    def __init__(self, status, headers, head, url, error):
        super().__init__(status, headers, head, url)
        self._error = error

    def read(self, size=-1):
        chunk = super().read(size)
        if chunk:
            return chunk
        raise self._error


class ScriptedServer:
    def __init__(self):
        self._lock = threading.Lock()
        self._script = []
        self.calls = []
        self.hanging = None

    def enqueue(self, response):
        self._script.append(response)

    def urlopen(self, req, *args, **kwargs):
        if isinstance(req, str):
            url = req
            headers = {}
        else:
            url = req.full_url
            headers = dict(req.header_items())
        with self._lock:
            self.calls.append((url, headers))
            response = self._script.pop(0)
        kind = response[0]
        if kind == "redirect":
            return FakeConnection(
                response[1],
                {"Location": response[2]},
                b"",
                url,
            )
        if kind == "connect_error":
            raise response[1]
        if kind == "raising":
            return RaisingConnection(
                response[1],
                response[2],
                response[3],
                url,
                response[4],
            )
        if kind == "hanging":
            connection = HangingConnection(
                response[1],
                response[2],
                response[3],
                response[4],
                url,
            )
            self.hanging = connection
            return connection
        return FakeConnection(
            response[1],
            response[2],
            response[3],
            url,
        )

    def file_requests(self):
        return [
            (url, headers)
            for url, headers in self.calls
            if url == FILE_URL
        ]


def redirect(status, location):
    return ("redirect", status, location)


def full(etag, total_length, body):
    return (
        "full",
        200,
        {"ETag": etag, "Content-Length": str(total_length)},
        body,
    )


def partial(etag, content_range, body, content_length=None, hanging=False):
    headers = {
        "ETag": etag,
        "Content-Range": content_range,
        "Content-Length": str(
            content_length if content_length is not None else len(body)
        ),
    }
    if hanging:
        return ("hanging", 206, headers, body, b"")
    return ("partial", 206, headers, body)


def hanging_partial(etag, content_range, head, tail):
    headers = {
        "ETag": etag,
        "Content-Range": content_range,
        "Content-Length": str(len(head) + len(tail)),
    }
    return ("hanging", 206, headers, head, tail)


def connect_error(error):
    return ("connect_error", error)


def raising_full(etag, total_length, head, error):
    return (
        "raising",
        200,
        {"ETag": etag, "Content-Length": str(total_length)},
        head,
        error,
    )


class TestResumableDownload(TestCase):
    def setUp(self):
        self.server = ScriptedServer()
        self._original_urlopen = request.urlopen
        request.urlopen = self.server.urlopen

    def tearDown(self):
        request.urlopen = self._original_urlopen

    def _enqueue_route(self):
        self.server.enqueue(redirect(302, QUEUE_URL))
        self.server.enqueue(redirect(302, FILE_URL))

    def test_full_supplier_scenario(self):
        progress = pe.DownloadProgress()

        # Step 1: v1 download breaks after the first 18 bytes.
        self._enqueue_route()
        self.server.enqueue(full('"v1"', 24, V1_CONTENT[:18]))
        with self.assertRaises(DownloadInterrupted) as caught:
            pe.get_book(
                url=DISPATCH_URL,
                resumable=True,
                download_progress=progress,
            )
        self.assertEqual(caught.exception.report.state, STATE_INTERRUPTED)
        self.assertEqual(progress.resume_from, 18)
        self.assertEqual(progress.etag, '"v1"')
        self.assertEqual(progress.total_length, 24)
        self.assertEqual(progress.url, FILE_URL)
        self._assert_plain_route_only()

        # Step 2: get_sheet resumes with Range and If-Range, then hangs.
        self._enqueue_route()
        self.server.enqueue(
            hanging_partial(
                '"v1"',
                "bytes 18-23/24",
                b"",
                V1_CONTENT[18:],
            ),
        )
        result = {}

        def hanging_call():
            try:
                pe.get_sheet(
                    url=DISPATCH_URL,
                    resumable=True,
                    download_progress=progress,
                )
            except StaleDownload as exc:
                result["exc"] = exc

        thread = threading.Thread(target=hanging_call)
        thread.start()
        self._wait_until(lambda: self.server.hanging is not None)
        resume_headers = self.server.file_requests()[-1][1]
        self.assertEqual(resume_headers.get("Range"), "bytes=18-")
        self.assertEqual(resume_headers.get("If-range"), '"v1"')

        # Step 3: v2 is published; a fresh full GET breaks after 17 bytes.
        self._enqueue_route()
        self.server.enqueue(full('"v2"', 34, V2_CONTENT[:17]))
        with self.assertRaises(DownloadInterrupted):
            pe.get_book(
                url=DISPATCH_URL,
                resumable=True,
                download_progress=progress,
            )
        self.assertEqual(progress.resume_from, 17)
        self.assertEqual(progress.etag, '"v2"')
        self.assertEqual(progress.total_length, 34)

        # Step 4a: wrong total length in Content-Range.
        self._enqueue_route()
        self.server.enqueue(
            partial('"v2"', "bytes 17-24/35", V2_CONTENT[17:25]),
        )
        with self.assertRaises(DownloadResponseMismatch) as caught:
            pe.get_sheet(
                url=DISPATCH_URL,
                resumable=True,
                download_progress=progress,
            )
        self.assertEqual(caught.exception.report.state, STATE_MISMATCH)
        self.assertEqual(progress.resume_from, 17)

        # Step 4b: Content-Length disagrees with the Content-Range segment.
        self._enqueue_route()
        self.server.enqueue(
            partial(
                '"v2"',
                "bytes 17-24/34",
                V2_CONTENT[17:25],
                content_length=7,
            ),
        )
        with self.assertRaises(DownloadResponseMismatch):
            pe.get_book(
                url=DISPATCH_URL,
                resumable=True,
                download_progress=progress,
            )
        self.assertEqual(progress.resume_from, 17)

        # Step 5: a normal partial response still leaves the file incomplete.
        self._enqueue_route()
        self.server.enqueue(
            partial('"v2"', "bytes 17-24/34", V2_CONTENT[17:25]),
        )
        with self.assertRaises(IncompleteDownload) as caught:
            pe.get_sheet(
                url=DISPATCH_URL,
                resumable=True,
                download_progress=progress,
            )
        self.assertEqual(caught.exception.report.state, STATE_INCOMPLETE)
        self.assertEqual(caught.exception.report.resume_from, 25)
        self.assertEqual(progress.resume_from, 25)

        # Step 6: the final segment completes the v2 file and is parsed.
        self._enqueue_route()
        self.server.enqueue(
            partial('"v2"', "bytes 25-33/34", V2_CONTENT[25:]),
        )
        sheet = pe.get_sheet(
            url=DISPATCH_URL,
            resumable=True,
            download_progress=progress,
        )
        self.assertEqual(sheet.to_array(), V2_EXPECTED)
        self.assertEqual(sheet.download_report.state, STATE_COMPLETE)
        self.assertEqual(progress.completed, True)
        self.assertEqual(progress.resume_from, 34)

        # Step 7: the supplier withdraws v2; v1 restarts and breaks at 18.
        self._enqueue_route()
        self.server.enqueue(full('"v1"', 24, V1_CONTENT[:18]))
        with self.assertRaises(DownloadInterrupted):
            pe.get_book(
                url=DISPATCH_URL,
                resumable=True,
                download_progress=progress,
            )
        self.assertEqual(progress.resume_from, 18)
        self.assertEqual(progress.etag, '"v1"')
        self.assertEqual(progress.total_length, 24)

        # Step 8: the old hanging 206 finishes but is now stale.
        hanging_generation = self.server.hanging
        hanging_generation.release()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(result["exc"], StaleDownload)
        self.assertEqual(result["exc"].report.state, STATE_STALE)
        self.assertEqual(progress.resume_from, 18)

        # Step 9: a fresh resume of v1 delivers the workbook.
        self._enqueue_route()
        self.server.enqueue(
            partial('"v1"', "bytes 18-23/24", V1_CONTENT[18:]),
        )
        book = pe.get_book(
            url=DISPATCH_URL,
            resumable=True,
            download_progress=progress,
        )
        final_headers = dict(self.server.file_requests()[-1][1])
        self.assertEqual(final_headers.get("Range"), "bytes=18-")
        self.assertEqual(final_headers.get("If-range"), '"v1"')
        self.assertEqual(
            list(book.to_dict().values())[0],
            V1_EXPECTED,
        )
        self.assertEqual(book.download_progress, progress)

        # Range headers must never reach the routing endpoints.
        for url, headers in self.server.calls:
            if url != FILE_URL:
                self.assertNotIn("Range", headers)
                self.assertNotIn("If-range", headers)

    def test_fresh_collection_starts_with_plain_get(self):
        progress = pe.DownloadProgress()
        self._enqueue_route()
        self.server.enqueue(full('"v1"', 24, V1_CONTENT))
        sheet = pe.get_sheet(
            url=DISPATCH_URL,
            resumable=True,
            download_progress=progress,
        )
        self.assertEqual(sheet.to_array(), V1_EXPECTED)
        file_headers = self.server.file_requests()[0][1]
        self.assertEqual(file_headers, {})
        self.assertEqual(progress.completed, True)

    def test_resumable_without_progress_creates_a_collection(self):
        self._enqueue_route()
        self.server.enqueue(full('"v1"', 24, V1_CONTENT))
        sheet = pe.get_sheet(url=DISPATCH_URL, resumable=True)
        self.assertEqual(sheet.to_array(), V1_EXPECTED)
        self.assertIsInstance(sheet.download_progress, pe.DownloadProgress)
        self.assertEqual(sheet.download_progress.completed, True)
        self.assertEqual(sheet.download_report.state, STATE_COMPLETE)

    def test_non_resumable_call_keeps_plain_get_behavior(self):
        self.server.enqueue(full('"v1"', 24, V1_CONTENT))
        book = pe.get_book(url=FILE_URL)
        self.assertEqual(
            list(book.to_dict().values())[0],
            V1_EXPECTED,
        )
        url, headers = self.server.calls[0]
        self.assertEqual(url, FILE_URL)
        self.assertEqual(headers, {})
        self.assertFalse(hasattr(book, "download_progress"))

    def test_each_call_requests_the_file_once(self):
        progress = pe.DownloadProgress()
        self._enqueue_route()
        self.server.enqueue(full('"v2"', 34, V2_CONTENT[:17]))
        with self.assertRaises(DownloadInterrupted):
            pe.get_book(
                url=DISPATCH_URL,
                resumable=True,
                download_progress=progress,
            )
        self.assertEqual(len(self.server.file_requests()), 1)
        self.assertEqual(
            [url for url, _ in self.server.calls],
            [DISPATCH_URL, QUEUE_URL, FILE_URL],
        )

    def test_mismatch_keeps_progress_and_is_recorded(self):
        progress = pe.DownloadProgress()
        self._enqueue_route()
        self.server.enqueue(full('"v2"', 34, V2_CONTENT[:17]))
        with self.assertRaises(DownloadInterrupted):
            pe.get_sheet(
                url=DISPATCH_URL,
                resumable=True,
                download_progress=progress,
            )
        self._enqueue_route()
        self.server.enqueue(
            partial('"v2"', "bytes 17-24/35", V2_CONTENT[17:25]),
        )
        with self.assertRaises(DownloadResponseMismatch):
            pe.get_sheet(
                url=DISPATCH_URL,
                resumable=True,
                download_progress=progress,
            )
        self.assertEqual(progress.report.state, STATE_MISMATCH)
        self.assertEqual(progress.resume_from, 17)
        self.assertEqual(progress.reports[-1].state, STATE_MISMATCH)

    def test_socket_failure_during_body_keeps_received_bytes(self):
        progress = pe.DownloadProgress()
        self._enqueue_route()
        self.server.enqueue(
            raising_full(
                '"v2"',
                34,
                V2_CONTENT[:17],
                ConnectionError("connection reset by peer"),
            ),
        )
        with self.assertRaises(DownloadInterrupted) as caught:
            pe.get_sheet(
                url=DISPATCH_URL,
                resumable=True,
                download_progress=progress,
            )
        self.assertEqual(caught.exception.report.state, STATE_INTERRUPTED)
        self.assertEqual(progress.resume_from, 17)
        self.assertEqual(progress.etag, '"v2"')
        self.assertEqual(progress.total_length, 34)

    def test_connect_failure_reports_interrupted_without_progress_change(self):
        progress = pe.DownloadProgress()
        self.server.enqueue(
            connect_error(OSError("network is unreachable")),
        )
        with self.assertRaises(DownloadInterrupted) as caught:
            pe.get_book(
                url=DISPATCH_URL,
                resumable=True,
                download_progress=progress,
            )
        self.assertEqual(caught.exception.report.state, STATE_INTERRUPTED)
        self.assertEqual(progress.resume_from, 0)
        self.assertIsNone(progress.url)

    def _assert_plain_route_only(self):
        for url, headers in self.server.calls[:-1]:
            self.assertNotIn("Range", headers)
        self.assertEqual(self.server.calls[-1][0], FILE_URL)
        self.assertEqual(self.server.calls[-1][1], {})

    def _wait_until(self, predicate, timeout=5):
        import time

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        self.fail("condition was not met in time")
