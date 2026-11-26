from __future__ import annotations

import logging
from email.utils import formatdate
from time import time
from typing import TYPE_CHECKING, Self, cast
from weakref import finalize

from twisted.internet.defer import Deferred
from twisted.internet.error import ConnectError, ConnectionDone, ConnectionLost

from scrapy import signals
from scrapy.exceptions import (
    DownloadConnectionRefusedError,
    DownloadFailedError,
    DownloadTimeoutError,
    IgnoreRequest,
    NotConfigured,
)
from scrapy.extensions.httpcache import _parse_content_range, _parse_range_header
from scrapy.http import Headers
from scrapy.utils.decorators import _warn_spider_arg
from scrapy.utils.defer import maybe_deferred_to_future
from scrapy.utils.misc import load_object

if TYPE_CHECKING:
    from scrapy.crawler import Crawler
    from scrapy.http.request import Request
    from scrapy.http.response import Response
    from scrapy.settings import Settings
    from scrapy.spiders import Spider
    from scrapy.statscollectors import StatsCollector
    from scrapy.utils.request import RequestFingerprinterProtocol


logger = logging.getLogger(__name__)


class HttpCacheMiddleware:
    DOWNLOAD_EXCEPTIONS = (
        ConnectionDone,
        ConnectError,
        ConnectionLost,
        OSError,
        DownloadTimeoutError,
        DownloadConnectionRefusedError,
        DownloadFailedError,
    )

    crawler: Crawler
    _fingerprinter: RequestFingerprinterProtocol

    def __init__(self, settings: Settings, stats: StatsCollector) -> None:
        if not settings.getbool("HTTPCACHE_ENABLED"):
            raise NotConfigured
        self.policy = load_object(settings["HTTPCACHE_POLICY"])(settings)
        self.storage = load_object(settings["HTTPCACHE_STORAGE"])(settings)
        self.ignore_missing = settings.getbool("HTTPCACHE_IGNORE_MISSING")
        self.stats = stats
        self._downloading: dict[bytes, list[Deferred[None]]] = {}

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        o = cls(crawler.settings, crawler.stats)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        o.crawler = crawler
        o._fingerprinter = crawler.request_fingerprinter
        return o

    def spider_opened(self, spider: Spider) -> None:
        self.storage.open_spider(spider)

    def spider_closed(self, spider: Spider) -> None:
        for fingerprint in list(self._downloading):
            self._wake(fingerprint)
        self.storage.close_spider(spider)

    @_warn_spider_arg
    async def process_request(
        self, request: Request, spider: Spider | None = None
    ) -> Request | Response | None:
        if request.meta.get("dont_cache", False):
            return None

        # Skip uncacheable requests
        if request.meta.get("body_file") or not self.policy.should_cache_request(
            request
        ):
            request.meta["_dont_cache"] = True  # flag as uncacheable
            return None

        if self._is_range_request(request):
            return self._process_range_request(request)

        # Look for cached response and check if expired
        cachedresponse = self._retrieve(request)
        if cachedresponse is None and await self._wait_for_download(request):
            cachedresponse = self._retrieve(request)

        if cachedresponse is None:
            self.stats.inc_value("httpcache/miss")
            if self.ignore_missing:
                self.stats.inc_value("httpcache/ignore")
                raise IgnoreRequest(f"Ignored request not in cache: {request}")
            self._mark_downloading(request)
            return None  # first time request

        # Return cached response only if not expired
        cachedresponse.flags.append("cached")
        if self.policy.is_cached_response_fresh(cachedresponse, request):
            self.stats.inc_value("httpcache/hit")
            return cachedresponse

        # Keep a reference to cached response to avoid a second cache lookup on
        # process_response hook
        request.meta["cached_response"] = cachedresponse
        self._mark_downloading(request)

        return None

    def _is_range_request(self, request: Request) -> bool:
        if not getattr(self.storage, "ranges", False):
            return False
        if request.method != "GET":
            return False
        if b"Range" not in request.headers:
            return False
        if any(
            header in request.headers
            for header in (b"If-Range", b"If-None-Match", b"If-Modified-Since")
        ):
            return False
        return _parse_range_header(request.headers.get(b"Range")) is not None

    def _process_range_request(self, request: Request) -> Response | None:
        parsed = _parse_range_header(request.headers[b"Range"])
        assert parsed is not None
        start, end = parsed

        cached = self._retrieve(request)
        if cached is not None:
            cached.flags.append("cached")
            self.stats.inc_value("httpcache/hit")
            return cached

        request.meta["_httpcache_range_original"] = (start, end)
        plan = self.storage.range_plan(self.crawler.spider, request, start, end)
        if plan is None:
            self.stats.inc_value("httpcache/miss")
            if self.ignore_missing:
                self.stats.inc_value("httpcache/ignore")
                raise IgnoreRequest(f"Ignored request not in cache: {request}")
            request.meta["_httpcache_range_etag"] = None
            request.meta["_httpcache_range_outgoing"] = (start, end)
            request.headers[b"Accept-Encoding"] = b"identity"
            self._mark_downloading(request)
            return None

        etag, _total, missing = plan
        if not missing:
            cached = self.storage.cached_range_response_for_etag(
                self.crawler.spider, request, etag, start, end
            )
            assert cached is not None
            cached.flags.append("cached")
            self.stats.inc_value("httpcache/hit")
            return cached

        request.meta["_httpcache_range_etag"] = etag
        if len(missing) == 1:
            missing_start, missing_end = missing[0]
        else:
            # Several separate missing intervals cannot be fetched in one
            # response. The enclosing span fetches every gap, re-reading the
            # cached bytes between them.
            missing_start, missing_end = missing[0][0], missing[-1][1]
        request.meta["_httpcache_range_outgoing"] = (missing_start, missing_end)
        request.headers[b"Range"] = f"bytes={missing_start}-{missing_end}".encode()
        request.headers[b"If-Range"] = etag
        request.headers[b"Accept-Encoding"] = b"identity"
        self._mark_downloading(request)
        return None

    def _retrieve(self, request: Request) -> Response | None:
        try:
            return cast(
                "Response | None",
                self.storage.retrieve_response(self.crawler.spider, request),
            )
        except Exception:
            self.stats.inc_value("httpcache/retrieve_error")
            logger.warning(
                f"Could not read the cache entry for {request}, treating it as a "
                f"cache miss.",
                exc_info=True,
                extra={"spider": self.crawler.spider},
            )
            return None

    async def _wait_for_download(self, request: Request) -> bool:
        """Block until an ongoing download of the same resource finishes, and
        return whether such a download was found."""
        fingerprint = self._fingerprinter.fingerprint(request)
        waiters = self._downloading.get(fingerprint)
        # A request already marked as downloading is the one being awaited on,
        # so making it wait would block it forever.
        if waiters is None or request.meta.get("_httpcache_downloading") == fingerprint:
            return False
        self.stats.inc_value("httpcache/wait")
        waiter: Deferred[None] = Deferred(waiters.remove)
        waiters.append(waiter)
        await maybe_deferred_to_future(waiter)
        return True

    def _mark_downloading(self, request: Request) -> None:
        fingerprint = self._fingerprinter.fingerprint(request)
        if fingerprint in self._downloading:
            return
        waiters: list[Deferred[None]] = []
        self._downloading[fingerprint] = waiters
        request.meta["_httpcache_downloading"] = fingerprint
        # Some outcomes reach neither process_response() nor
        # process_exception(), e.g. a middleware with a higher priority
        # returning a request from process_response(), or raising from it while
        # DOWNLOADER_MIDDLEWARE_RESPONSE_EXCEPTIONS is False. Waking waiters up
        # once the request object is gone keeps them from blocking forever in
        # those cases.
        finalize(request, self._wake, fingerprint, waiters)

    def _wake(
        self, fingerprint: bytes, waiters: list[Deferred[None]] | None = None
    ) -> None:
        current = self._downloading.get(fingerprint)
        if current is None or (waiters is not None and current is not waiters):
            return
        del self._downloading[fingerprint]
        while current:
            current.pop(0).callback(None)

    def _downloaded(self, request: Request) -> None:
        fingerprint = request.meta.pop("_httpcache_downloading", None)
        if fingerprint is not None:
            self._wake(fingerprint)

    @_warn_spider_arg
    def process_response(
        self, request: Request, response: Response, spider: Spider | None = None
    ) -> Request | Response:
        # Waiters are woken up only once the response has been cached, so that
        # they can read it from the cache.
        try:
            return self._process_response(request, response)
        finally:
            self._downloaded(request)

    def _process_response(self, request: Request, response: Response) -> Response:
        if request.meta.get("dont_cache", False):
            return response

        if "_httpcache_range_original" in request.meta:
            return self._process_range_response(request, response)

        # Skip cached responses and uncacheable requests
        if "_dont_cache" in request.meta or "cached" in response.flags:
            request.meta.pop("_dont_cache", None)
            return response

        # RFC2616 requires origin server to set Date header,
        # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.18
        if "Date" not in response.headers:
            response.headers["Date"] = formatdate(usegmt=True)

        # Do not validate first-hand responses
        cachedresponse: Response | None = request.meta.pop("cached_response", None)
        if cachedresponse is None:
            self.stats.inc_value("httpcache/firsthand")
            self._cache_response(response, request)
            return response

        if self.policy.is_cached_response_valid(cachedresponse, response, request):
            self.stats.inc_value("httpcache/revalidate")
            if response.status == 304:
                self._freshen_cached_response(cachedresponse, response)
                self._cache_response(cachedresponse, request)
            return cachedresponse

        self.stats.inc_value("httpcache/invalidate")
        request.meta.pop("cache_timestamp", None)
        self._cache_response(response, request)
        return response

    def _process_range_response(self, request: Request, response: Response) -> Response:
        start, end = request.meta.pop("_httpcache_range_original")
        etag: bytes | None = request.meta.pop("_httpcache_range_etag", None)
        outgoing_start, outgoing_end = request.meta.pop(
            "_httpcache_range_outgoing", (start, end)
        )

        if response.status == 206:
            if not self._store_range_segment(
                request, response, etag, outgoing_start, outgoing_end
            ):
                self._restore_range_header(request, start, end)
                return response
            etag = etag or response.headers.get(b"ETag")
        elif response.status == 200:
            # The server ignored the range, or the resource changed under our
            # feet. Store the complete body as a version of its own.
            self._cache_response(response, request)
            etag = response.headers.get(b"ETag")
            if etag is None:
                self._restore_range_header(request, start, end)
                return response
            self.stats.inc_value("httpcache/range_changed")
        else:
            self.stats.inc_value("httpcache/range_error")
            self._restore_range_header(request, start, end)
            return response

        self._restore_range_header(request, start, end)
        if response.status == 200:
            # The body is that of the version the server sent. Caching it
            # above is what lets a later request reuse it at range level, but
            # this response must still be the requested range of that body.
            result = _slice_response(response, start, end)
            if result is not None:
                request.meta["cache_timestamp"] = time()
                return result
        cached = (
            self.storage.cached_range_response_for_etag(
                self.crawler.spider, request, etag, start, end
            )
            if etag is not None
            else None
        )
        if cached is not None:
            cached.flags.append("cached")
            self.stats.inc_value("httpcache/range")
            return cached
        if etag is not None:
            self.stats.inc_value("httpcache/range_partial")
        return response

    def _store_range_segment(
        self,
        request: Request,
        response: Response,
        etag: bytes | None,
        outgoing_start: int,
        outgoing_end: int,
    ) -> bool:
        content_range = _parse_content_range(response.headers.get(b"Content-Range"))
        if content_range is None or (
            content_range[0] != outgoing_start or content_range[1] != outgoing_end
        ):
            self.stats.inc_value("httpcache/range_mismatch")
            return False
        if not self.policy.should_cache_response(response, request):
            self.stats.inc_value("httpcache/range_uncacheable")
            return False
        response_etag = response.headers.get(b"ETag")
        if etag is not None and response_etag != etag:
            # The server ignored If-Range and served another version. Storing
            # these bytes under the requested version would mix the two.
            self.stats.inc_value("httpcache/range_changed")
            return False
        if not self.storage.store_partial_range(self.crawler.spider, request, response):
            self.stats.inc_value("httpcache/range_uncacheable")
            return False
        return True

    @staticmethod
    def _restore_range_header(request: Request, start: int, end: int) -> None:
        request.headers[b"Range"] = f"bytes={start}-{end}".encode()
        request.headers.pop(b"If-Range", None)

    @_warn_spider_arg
    def process_exception(
        self, request: Request, exception: Exception, spider: Spider | None = None
    ) -> Request | Response | None:
        self._downloaded(request)
        self._clear_range_meta(request)
        cachedresponse: Response | None = request.meta.pop("cached_response", None)
        if cachedresponse is not None and isinstance(
            exception, self.DOWNLOAD_EXCEPTIONS
        ):
            self.stats.inc_value("httpcache/errorrecovery")
            return cachedresponse
        return None

    @staticmethod
    def _clear_range_meta(request: Request) -> None:
        for key in (
            "_httpcache_range_original",
            "_httpcache_range_etag",
            "_httpcache_range_outgoing",
        ):
            request.meta.pop(key, None)

    def _freshen_cached_response(
        self, cachedresponse: Response, response: Response
    ) -> None:
        # RFC 7234, section 4.3.4: update the stored response with the
        # header fields from a successful revalidation (304) response.
        warnings = [
            warning
            for warning in cachedresponse.headers.getlist(b"Warning")
            if warning.split(None, 1)[0].startswith(b"2")
        ]
        cachedresponse.headers.update(response.headers)
        if warnings:
            cachedresponse.headers[b"Warning"] = warnings
        else:
            cachedresponse.headers.pop(b"Warning", None)

    def _cache_response(self, response: Response, request: Request) -> None:
        if self.policy.should_cache_response(response, request):
            self.stats.inc_value("httpcache/store")
            self.storage.store_response(self.crawler.spider, request, response)
        else:
            self.stats.inc_value("httpcache/uncacheable")


def _slice_response(response: Response, start: int, end: int) -> Response | None:
    body = response.body
    if end >= len(body):
        return None
    headers = Headers(response.headers)
    headers[b"Content-Range"] = f"bytes {start}-{end}/{len(body)}".encode()
    headers[b"Content-Length"] = str(end - start + 1).encode()
    return response.replace(
        status=206,
        headers=headers,
        body=body[start : end + 1],
    )
