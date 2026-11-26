from __future__ import annotations

import hashlib
import pickle
import shutil
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from twisted.internet.defer import Deferred

from scrapy import signals
from scrapy.core.downloader._middleware import DownloaderMiddlewareManager
from scrapy.core.engine import ExecutionEngine
from scrapy.exceptions import IgnoreRequest
from scrapy.extensions.httpcache import _parse_range_header
from scrapy.http import Request, Response
from scrapy.utils.asyncio import sleep
from scrapy.utils.defer import deferred_from_coro, maybe_deferred_to_future
from scrapy.utils.test import get_crawler
from tests.utils.decorators import coroutine_test

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from scrapy.crawler import Crawler
    from scrapy.spiders import Spider


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (b"bytes=0-5", (0, 5)),
        (b"bytes = 4-7", (4, 7)),
        (b"bytes=5-4", None),
        (b"bytes=0-1,4-5", None),
        (b"items=0-1", None),
        (b"bytes=-5", None),
    ],
)
def test_parse_range_header(
    value: bytes, expected: tuple[int, int] | None
) -> None:
    assert _parse_range_header(value) == expected


def _range_response(
    request: Request,
    body: bytes,
    etag: bytes,
    start: int,
    end: int,
    total: int,
) -> Response:
    return Response(
        request.url,
        status=206,
        headers={
            b"ETag": etag,
            b"Content-Range": f"bytes {start}-{end}/{total}".encode(),
            b"Content-Type": b"application/octet-stream",
        },
        body=body,
    )


def _full_response(request: Request, body: bytes, etag: bytes) -> Response:
    return Response(
        request.url,
        status=200,
        headers={
            b"ETag": etag,
            b"Content-Length": str(len(body)).encode(),
            b"Content-Type": b"application/octet-stream",
        },
        body=body,
    )


class _Source:
    """A controllable origin server that serves byte ranges of the current
    version."""

    def __init__(self, body: bytes, etag: bytes) -> None:
        self.body = body
        self.etag = etag
        self.urls: list[str] = []
        self.ranges: list[tuple[int, int]] = []
        self.if_ranges: list[bytes | None] = []
        self.hold: dict[tuple[int, int], Deferred[None]] = {}
        self.offline = False

    def set_version(self, body: bytes, etag: bytes) -> None:
        self.body = body
        self.etag = etag

    def pause(self, start: int, end: int) -> Deferred[None]:
        gate: Deferred[None] = Deferred()
        self.hold[(start, end)] = gate
        return gate

    def release(self, start: int, end: int) -> None:
        self.hold.pop((start, end)).callback(None)

    async def __call__(self, request: Request) -> Response:
        self.urls.append(request.url)
        if self.offline:
            raise OSError("The origin server is unavailable")
        range_header = request.headers.get(b"Range")
        assert range_header is not None
        value = range_header.split(b"=", 1)[1]
        start, end = (int(part) for part in value.split(b"-"))
        self.ranges.append((start, end))
        if_range = request.headers.get(b"If-Range")
        self.if_ranges.append(if_range)
        # The response reflects the version at request time, even if the
        # server sends it only later.
        body, etag = self.body, self.etag
        gate = self.hold.get((start, end))
        if gate is not None:
            await maybe_deferred_to_future(gate)
        if if_range is not None and if_range != etag:
            return _full_response(request, body, etag)
        end = min(end, len(body) - 1)
        return _range_response(
            request,
            body[start : end + 1],
            etag,
            start,
            end,
            len(body),
        )


class TestRangeCache:
    url = "http://www.example.com/data.bin"

    def setup_method(self) -> None:
        self.tmpdir = tempfile.mkdtemp()

    def teardown_method(self) -> None:
        shutil.rmtree(self.tmpdir)

    def _settings(self, **new_settings: Any) -> dict[str, Any]:
        settings = {
            "HTTPCACHE_ENABLED": True,
            "HTTPCACHE_DIR": self.tmpdir,
            "HTTPCACHE_IGNORE_HTTP_CODES": [],
            "HTTPCACHE_RANGE_REQUESTS": True,
            "HTTPCACHE_STORAGE": ("scrapy.extensions.httpcache.FilesystemCacheStorage"),
            "DOWNLOADER_MIDDLEWARES": {
                "scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware": 900,
            },
        }
        settings.update(new_settings)
        return settings

    @asynccontextmanager
    async def _manager(
        self, **new_settings: Any
    ) -> AsyncGenerator[tuple[DownloaderMiddlewareManager, Crawler, Spider]]:
        crawler = get_crawler(settings_dict=self._settings(**new_settings))
        crawler.spider = crawler._create_spider("example.com")
        manager = DownloaderMiddlewareManager.from_crawler(crawler)
        crawler.engine = ExecutionEngine(crawler)
        assert crawler.spider
        crawler.signals.send_catch_log(signals.spider_opened, spider=crawler.spider)
        await crawler.engine.open_spider_async()
        try:
            yield manager, crawler, crawler.spider
        finally:
            await crawler.engine.close_spider_async()
            crawler.signals.send_catch_log(
                signals.spider_closed, spider=crawler.spider, reason="finished"
            )

    async def _download(
        self,
        manager: DownloaderMiddlewareManager,
        source: _Source,
        start: int,
        end: int,
    ) -> Response:
        request = Request(self.url, headers={b"Range": f"bytes={start}-{end}".encode()})
        result = await maybe_deferred_to_future(
            deferred_from_coro(manager.download_async(source, request))
        )
        assert isinstance(result, Response)
        return result

    async def _download_full(
        self, manager: DownloaderMiddlewareManager, source: _Source
    ) -> Response:
        result = await maybe_deferred_to_future(
            deferred_from_coro(manager.download_async(source, Request(self.url)))
        )
        assert isinstance(result, Response)
        return result

    @coroutine_test
    async def test_reuse_after_restart(self) -> None:
        source = _Source(b"ABCDEFGH", b'"a"')
        async with self._manager() as (manager, _crawler, _spider):
            first = await self._download(manager, source, 0, 1)
            second = await self._download(manager, source, 4, 5)
        assert first.body == b"AB"
        assert second.body == b"EF"
        assert source.ranges == [(0, 1), (4, 5)]

        async with self._manager() as (manager, _crawler, _spider):
            first = await self._download(manager, source, 0, 1)
            second = await self._download(manager, source, 4, 5)
        assert first.body == b"AB"
        assert second.body == b"EF"
        assert first.headers[b"Content-Range"] == b"bytes 0-1/8"
        assert second.headers[b"Content-Range"] == b"bytes 4-5/8"
        assert first.headers[b"ETag"] == b'"a"'
        assert first.status == 206
        assert source.ranges == [(0, 1), (4, 5)]

    @coroutine_test
    async def test_partial_and_version_change(self) -> None:
        source = _Source(b"ABCDEFGH", b'"a"')
        async with self._manager() as (manager, _crawler, _spider):
            await self._download(manager, source, 0, 1)
            await self._download(manager, source, 4, 5)
            source.ranges.clear()
            source.if_ranges.clear()
            gate = source.pause(2, 3)
            first = deferred_from_coro(
                manager.download_async(
                    source, Request(self.url, headers={b"Range": b"bytes=0-5"})
                )
            )
            for _ in range(100):
                if (2, 3) in source.ranges:
                    break
                await sleep(0)
            assert source.ranges == [(2, 3)]
            assert source.if_ranges == [b'"a"']

            source.set_version(b"abcdefghij", b'"b"')
            second = await self._download(manager, source, 4, 7)
            assert second.body == b"efgh"
            assert second.headers[b"Content-Range"] == b"bytes 4-7/10"
            assert second.headers[b"ETag"] == b'"b"'
            assert source.ranges[-1] == (6, 7)

            gate.callback(None)
            result = await maybe_deferred_to_future(first)
            assert isinstance(result, Response)
            assert result.body == b"ABCDEF"
            assert result.headers[b"Content-Range"] == b"bytes 0-5/8"
            assert result.headers[b"ETag"] == b'"a"'

            cached = await self._download(manager, source, 0, 7)
            assert cached.body == b"abcdefgh"
            assert cached.headers[b"Content-Range"] == b"bytes 0-7/10"
            assert cached.headers[b"ETag"] == b'"b"'

            source.ranges.clear()
            full = await self._download_full(manager, source)
            assert full.body == b"abcdefghij"
            assert not source.ranges

        async with self._manager() as (manager, _crawler, _spider):
            cached = await self._download(manager, source, 0, 7)
            assert cached.body == b"abcdefgh"
            assert cached.headers[b"Content-Range"] == b"bytes 0-7/10"
            source.ranges.clear()
            full = await self._download_full(manager, source)
            assert full.body == b"abcdefghij"
            assert not source.ranges

            # A full request must not be served by a version whose bytes are
            # still incomplete.
            storage = next(
                mw.storage for mw in manager.middlewares if hasattr(mw, "storage")
            )
            for version in (Path(storage.cachedir) / "t").rglob("ranges/*"):
                body = version / "body"
                if body.exists():
                    body.unlink()
            source.ranges.clear()
            full = await self._download_full(manager, source)
            assert full.body == b"abcdefghij"
            assert source.ranges == []

    @coroutine_test
    async def test_disabled_by_default(self) -> None:
        source = _Source(b"ABCDEFGH", b'"a"')
        async with self._manager(HTTPCACHE_RANGE_REQUESTS=False) as (
            manager,
            _crawler,
            spider,
        ):
            await self._download(manager, source, 0, 1)
        storage = next(
            mw.storage for mw in manager.middlewares if hasattr(mw, "storage")
        )
        assert not storage.ranges
        rpath = Path(storage._get_request_path(spider, Request(self.url)))
        assert not (rpath / "ranges").exists()

    @coroutine_test
    async def test_full_request_needs_complete_version(self) -> None:
        source = _Source(b"ABCDEFGH", b'"a"')
        async with self._manager() as (manager, _crawler, _spider):
            await self._download(manager, source, 0, 1)
            await self._download(manager, source, 4, 5)
            source.ranges.clear()
            response = await self._download(manager, source, 0, 7)
            assert response.body == b"ABCDEFGH"
            source.ranges.clear()
            full = await self._download_full(manager, source)
            assert full.body == b"ABCDEFGH"
            assert not source.ranges

    @coroutine_test
    async def test_interrupted_version_write(self) -> None:
        source = _Source(b"ABCDEFGH", b'"a"')
        async with self._manager() as (manager, _crawler, spider):
            await self._download(manager, source, 0, 1)
            await self._download(manager, source, 4, 5)
            storage = next(
                mw.storage for mw in manager.middlewares if hasattr(mw, "storage")
            )
            rpath = Path(storage._get_request_path(spider, Request(self.url)))
            # Bytes of version B were only partially written before the crawl
            # stopped. B's truncated segment cannot satisfy a request, and it
            # must not hide A's complete segments.
            version_b = rpath / "ranges" / hashlib.sha1(b'"b"').hexdigest()
            (version_b / "segments").mkdir(parents=True)
            (version_b / "meta").write_bytes(
                pickle.dumps(
                    {
                        "etag": b'"b"',
                        "total": 10,
                        "headers": [(b"ETag", b'"b"')],
                        "timestamp": 10**6,
                    }
                )
            )
            (version_b / "segments" / "6-7").write_bytes(b"e")

        source.offline = True
        async with self._manager(HTTPCACHE_IGNORE_MISSING=True) as (
            manager,
            _crawler,
            _spider,
        ):
            first = await self._download(manager, source, 0, 1)
            second = await self._download(manager, source, 4, 5)
            assert first.body == b"AB"
            assert second.body == b"EF"
            # The incomplete B segment cannot be combined with A's bytes, and
            # the origin server is unavailable, so 6-7 must be reported as a
            # miss rather than served as a half-complete body.
            with pytest.raises(IgnoreRequest):
                await self._download(manager, source, 6, 7)
