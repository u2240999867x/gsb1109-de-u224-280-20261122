from __future__ import annotations

import gzip
import hashlib
import logging
import pickle
import re
from datetime import UTC
from email.utils import parsedate_to_datetime
from importlib import import_module
from pathlib import Path
from time import time
from typing import IO, TYPE_CHECKING, Any, Concatenate, cast
from urllib.parse import quote
from uuid import uuid4
from weakref import WeakKeyDictionary

from w3lib.http import headers_raw_to_dict

from scrapy.http import Headers, Response
from scrapy.utils.httpobj import urlparse_cached
from scrapy.utils.project import data_path
from scrapy.utils.python import to_unicode
from scrapy.utils.response import response_from_dict

if TYPE_CHECKING:
    import os
    from collections.abc import Callable
    from types import ModuleType

    from scrapy.http.request import Request
    from scrapy.settings import BaseSettings
    from scrapy.spiders import Spider
    from scrapy.utils.request import RequestFingerprinterProtocol


logger = logging.getLogger(__name__)

_CONTENT_RANGE_RE = re.compile(rb"^\s*bytes\s+(\d+)-(\d+)/(\d+)\s*$", re.IGNORECASE)
_RANGE_RE = re.compile(rb"^\s*bytes\s*=\s*(\d+)-(\d+)\s*$", re.IGNORECASE)


def _is_strong_etag(etag: bytes | None) -> bool:
    return bool(etag) and not etag.lstrip().lower().startswith(b"w/")


def _parse_range_header(value: bytes | None) -> tuple[int, int] | None:
    if value is None:
        return None
    match = _RANGE_RE.match(value)
    if not match:
        return None
    start, end = int(match.group(1)), int(match.group(2))
    if start > end:
        return None
    return start, end


def _parse_content_range(value: bytes | None) -> tuple[int, int, int] | None:
    if value is None:
        return None
    match = _CONTENT_RANGE_RE.match(value)
    if not match:
        return None
    start, end, total = (int(group) for group in match.groups())
    if start > end or end >= total:
        return None
    return start, end, total


def _missing_intervals(
    covered: list[tuple[int, int]], start: int, end: int
) -> list[tuple[int, int]]:
    """Return the parts of the closed interval *start*–*end* that *covered*
    does not include."""
    missing: list[tuple[int, int]] = []
    position = start
    for segment_start, segment_end in covered:
        if segment_end < position:
            continue
        if segment_start > position:
            missing.append((position, min(segment_start - 1, end)))
        position = max(position, segment_end + 1)
        if position > end:
            break
    if position <= end:
        missing.append((position, end))
    return missing


def _merge_intervals(
    intervals: list[tuple[int, int]],
) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _slice_cached_response(
    storage: FilesystemCacheStorage,
    rpath: Path,
    metadata: dict[str, Any],
    rawheaders: bytes,
    body: bytes,
    range_value: bytes,
    request: Request,
) -> Response | None:
    headers = Headers(headers_raw_to_dict(rawheaders))
    if (
        metadata.get("status") != 200
        or not _is_strong_etag(headers.get(b"ETag"))
        or b"Content-Encoding" in headers
        or headers.getlist(b"Vary")
    ):
        return None
    parsed = _parse_range_header(range_value)
    if parsed is None:
        return None
    start, end = parsed
    if end >= len(body):
        return None
    headers[b"Content-Range"] = f"bytes {start}-{end}/{len(body)}".encode()
    headers[b"Content-Length"] = str(end - start + 1).encode()
    data: dict[str, Any] = {
        "url": metadata["response_url"],
        "status": 206,
        "headers": {key: list(values) for key, values in headers.items()},
        "body": body[start : end + 1],
    }
    data_path = rpath / "response_data"
    if data_path.exists():
        with storage._open(data_path, "rb") as f:
            data.update(pickle.load(f))  # noqa: S301
    request.meta["cache_timestamp"] = metadata["timestamp"]
    return response_from_dict(data)


def _build_range_response(
    request: Request,
    entry: _RangeCacheEntry,
    start: int,
    end: int,
    body: bytes,
) -> Response:
    headers = Headers(entry.headers)
    headers[b"Content-Range"] = f"bytes {start}-{end}/{entry.total}".encode()
    headers[b"ETag"] = entry.etag
    headers[b"Content-Length"] = str(len(body)).encode()
    data: dict[str, Any] = {
        "url": request.url,
        "status": 206,
        "headers": {key: list(values) for key, values in headers.items()},
        "body": body,
    }
    data.update(entry.data)
    request.meta["cache_timestamp"] = entry.timestamp
    return response_from_dict(data)


class _RangeCacheEntry:
    """One stored version of a resource, identified by its strong ``ETag``."""

    def __init__(self, storage: FilesystemCacheStorage, path: Path):
        self.storage = storage
        self.path = path
        with storage._open(path / "meta", "rb") as f:
            self.meta: dict[str, Any] = pickle.load(f)  # noqa: S301
        self.etag: bytes = self.meta["etag"]
        self.total: int = self.meta["total"]
        self.headers: list[tuple[bytes, bytes]] = self.meta.get("headers", [])
        self.data: dict[str, Any] = self.meta.get("data", {})
        self.timestamp: float = self.meta.get("timestamp", 0)

    def _complete_body(self) -> bytes | None:
        body_path = self.path / "body"
        if not body_path.exists():
            return None
        with self.storage._open(body_path, "rb") as f:
            body = f.read()
        if len(body) != self.total:
            return None
        return body

    def body(self) -> bytes | None:
        """Return the complete body of this version, or ``None`` if some of
        its bytes are missing."""
        body = self._complete_body()
        if body is not None:
            return body
        return self.read(0, self.total - 1)

    def _segments(self) -> list[tuple[int, int, Path]]:
        segments: list[tuple[int, int, Path]] = []
        segments_path = self.path / "segments"
        if not segments_path.exists():
            return segments
        for path in segments_path.iterdir():
            match = re.fullmatch(r"(\d+)-(\d+)", path.name)
            if match is None:
                continue
            start, end = int(match.group(1)), int(match.group(2))
            if start <= end < self.total:
                segments.append((start, end, path))
        return segments

    def covered(self, start: int, end: int) -> list[tuple[int, int]]:
        if start > end:
            return []
        if self._complete_body() is not None:
            return [(start, end)]
        intervals = []
        for segment_start, segment_end, path in self._segments():
            if segment_end < start or segment_start > end:
                continue
            expected = segment_end - segment_start + 1
            with self.storage._open(path, "rb") as f:
                if len(f.read()) != expected:
                    continue
            intervals.append((max(segment_start, start), min(segment_end, end)))
        return _merge_intervals(intervals)

    def read(self, start: int, end: int) -> bytes | None:
        if start < 0 or end >= self.total:
            return None
        body = self._complete_body()
        if body is not None:
            return body[start : end + 1]
        result = bytearray(end - start + 1)
        covered = bytearray(len(result))
        for segment_start, segment_end, path in self._segments():
            read_start = max(segment_start, start)
            read_end = min(segment_end, end)
            if read_start > read_end:
                continue
            with self.storage._open(path, "rb") as f:
                segment = f.read()
            expected = segment_end - segment_start + 1
            if len(segment) != expected:
                continue
            chunk = segment[read_start - segment_start : read_end - segment_start + 1]
            offset = read_start - start
            result[offset : offset + len(chunk)] = chunk
            covered[offset : offset + len(chunk)] = b"\x01" * len(chunk)
        if not all(covered):
            return None
        return bytes(result)


def _get_scope(settings: BaseSettings, supported: tuple[str, ...]) -> str:
    scope: str = settings["HTTPCACHE_SCOPE"]
    if scope not in supported:
        supported_repr = ", ".join(repr(value) for value in supported)
        raise ValueError(
            f"Unsupported HTTPCACHE_SCOPE value: {scope!r}. The cache storage "
            f"in use supports the following values: {supported_repr}."
        )
    return scope


class DummyPolicy:
    def __init__(self, settings: BaseSettings):
        self.ignore_schemes: list[str] = settings.getlist("HTTPCACHE_IGNORE_SCHEMES")
        self.ignore_http_codes: list[int] = [
            int(x) for x in settings.getlist("HTTPCACHE_IGNORE_HTTP_CODES")
        ]

    def should_cache_request(self, request: Request) -> bool:
        return urlparse_cached(request).scheme not in self.ignore_schemes

    def should_cache_response(self, response: Response, request: Request) -> bool:
        return response.status not in self.ignore_http_codes

    def is_cached_response_fresh(
        self, cachedresponse: Response, request: Request
    ) -> bool:
        return True

    def is_cached_response_valid(
        self, cachedresponse: Response, response: Response, request: Request
    ) -> bool:
        return True


class RFC2616Policy:
    MAXAGE = 3600 * 24 * 365  # one year

    def __init__(self, settings: BaseSettings):
        self.always_store: bool = settings.getbool("HTTPCACHE_ALWAYS_STORE")
        self.store_ranges: bool = settings.getbool("HTTPCACHE_RANGE_REQUESTS")
        self.ignore_schemes: list[str] = settings.getlist("HTTPCACHE_IGNORE_SCHEMES")
        self._cc_parsed: WeakKeyDictionary[
            Request | Response, dict[bytes, bytes | None]
        ] = WeakKeyDictionary()
        self.ignore_response_cache_controls: list[bytes] = [
            cc.encode()
            for cc in settings.getlist("HTTPCACHE_IGNORE_RESPONSE_CACHE_CONTROLS")
        ]

    def _parse_cachecontrol(self, r: Request | Response) -> dict[bytes, bytes | None]:
        if r not in self._cc_parsed:
            cch = r.headers.get(b"Cache-Control", b"")
            assert cch is not None
            parsed = parse_cachecontrol(cch)
            if isinstance(r, Response):
                for key in self.ignore_response_cache_controls:
                    parsed.pop(key, None)
            self._cc_parsed[r] = parsed
        return self._cc_parsed[r]

    def should_cache_request(self, request: Request) -> bool:
        if urlparse_cached(request).scheme in self.ignore_schemes:
            return False
        cc = self._parse_cachecontrol(request)
        # obey user-agent directive "Cache-Control: no-store"
        return b"no-store" not in cc

    def should_cache_response(self, response: Response, request: Request) -> bool:
        # What is cacheable - https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9.1
        # Response cacheability - https://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.4
        cc = self._parse_cachecontrol(response)
        # obey directive "Cache-Control: no-store"
        if b"no-store" in cc:
            return False
        # Never cache 304 (Not Modified) responses
        if response.status == 304:
            return False
        # Registering the bytes of a partial response as a reusable range
        # requires a strong validator, even though a partial body is not a
        # complete response on its own.
        if (
            self.store_ranges
            and response.status == 206
            and _is_strong_etag(response.headers.get(b"ETag"))
            and b"Content-Encoding" not in response.headers
            and not response.headers.getlist(b"Vary")
        ):
            return True
        # Cache unconditionally if configured to do so
        if self.always_store:
            return True
        # Any hint on response expiration is good
        if b"max-age" in cc or b"Expires" in response.headers:
            return True
        # Firefox fallbacks this statuses to one year expiration if none is set
        if response.status in {300, 301, 308}:
            return True
        # Other statuses without expiration requires at least one validator
        if response.status in {200, 203, 401}:
            return b"Last-Modified" in response.headers or b"ETag" in response.headers
        # Any other is probably not eligible for caching
        # Makes no sense to cache responses that does not contain expiration
        # info and can not be revalidated
        return False

    def is_cached_response_fresh(
        self, cachedresponse: Response, request: Request
    ) -> bool:
        cc = self._parse_cachecontrol(cachedresponse)
        ccreq = self._parse_cachecontrol(request)
        if b"no-cache" in cc or b"no-cache" in ccreq:
            return False

        now = time()
        freshnesslifetime = self._compute_freshness_lifetime(
            cachedresponse, request, now
        )
        currentage = self._compute_current_age(cachedresponse, request, now)

        reqmaxage = self._get_max_age(ccreq)
        if reqmaxage is not None:
            freshnesslifetime = min(freshnesslifetime, reqmaxage)

        if currentage < freshnesslifetime:
            return True

        if b"max-stale" in ccreq and b"must-revalidate" not in cc:
            # From RFC2616: "Indicates that the client is willing to
            # accept a response that has exceeded its expiration time.
            # If max-stale is assigned a value, then the client is
            # willing to accept a response that has exceeded its
            # expiration time by no more than the specified number of
            # seconds. If no value is assigned to max-stale, then the
            # client is willing to accept a stale response of any age."
            staleage = ccreq[b"max-stale"]
            if staleage is None:
                return True

            try:
                if currentage < freshnesslifetime + max(0, int(staleage)):
                    return True
            except ValueError:
                pass

        # Cached response is stale, try to set validators if any
        self._set_conditional_validators(request, cachedresponse)
        return False

    def is_cached_response_valid(
        self, cachedresponse: Response, response: Response, request: Request
    ) -> bool:
        # Use the cached response if the new response is a server error,
        # as long as the old response didn't specify must-revalidate.
        if response.status >= 500:
            cc = self._parse_cachecontrol(cachedresponse)
            if b"must-revalidate" not in cc:
                return True

        # Use the cached response if the server says it hasn't changed.
        return response.status == 304

    def _set_conditional_validators(
        self, request: Request, cachedresponse: Response
    ) -> None:
        if b"Last-Modified" in cachedresponse.headers:
            request.headers[b"If-Modified-Since"] = cachedresponse.headers[
                b"Last-Modified"
            ]

        if b"ETag" in cachedresponse.headers:
            request.headers[b"If-None-Match"] = cachedresponse.headers[b"ETag"]

    def _get_max_age(self, cc: dict[bytes, bytes | None]) -> int | None:
        try:
            return max(0, int(cc[b"max-age"]))  # type: ignore[arg-type]
        except (KeyError, ValueError):
            return None

    def _compute_freshness_lifetime(
        self, response: Response, request: Request, now: float
    ) -> float:
        # Reference nsHttpResponseHead::ComputeFreshnessLifetime
        # https://dxr.mozilla.org/mozilla-central/source/netwerk/protocol/http/nsHttpResponseHead.cpp#706
        cc = self._parse_cachecontrol(response)
        maxage = self._get_max_age(cc)
        if maxage is not None:
            return maxage

        # Parse date header or synthesize it if none exists
        date = rfc1123_to_epoch(response.headers.get(b"Date")) or now

        # Try HTTP/1.0 Expires header
        if b"Expires" in response.headers:
            expires = rfc1123_to_epoch(response.headers[b"Expires"])
            # When parsing Expires header fails RFC 2616 section 14.21 says we
            # should treat this as an expiration time in the past.
            return max(0, expires - date) if expires else 0

        # Fallback to heuristic using last-modified header
        # This is not in RFC but on Firefox caching implementation
        lastmodified = rfc1123_to_epoch(response.headers.get(b"Last-Modified"))
        if lastmodified and lastmodified <= date:
            return (date - lastmodified) / 10

        # This request can be cached indefinitely
        if response.status in {300, 301, 308}:
            return self.MAXAGE

        # Insufficient information to compute freshness lifetime
        return 0

    def _compute_current_age(
        self, response: Response, request: Request, now: float
    ) -> float:
        # Reference nsHttpResponseHead::ComputeCurrentAge
        # https://dxr.mozilla.org/mozilla-central/source/netwerk/protocol/http/nsHttpResponseHead.cpp#658
        currentage: float = 0
        # If Date header is not set we assume it is a fast connection, and
        # clock is in sync with the server
        date = rfc1123_to_epoch(response.headers.get(b"Date")) or now
        if now > date:
            currentage = now - date

        if b"Age" in response.headers:
            try:
                age = int(response.headers[b"Age"])  # type: ignore[arg-type]
                currentage = max(currentage, age)
            except ValueError:
                pass

        return currentage


class DbmCacheStorage:
    def __init__(self, settings: BaseSettings):
        self.cachedir: str = data_path(settings["HTTPCACHE_DIR"], createdir=True)
        self.expiration_secs: int = settings.getint("HTTPCACHE_EXPIRATION_SECS")
        self.dbmodule: ModuleType = import_module(settings["HTTPCACHE_DBM_MODULE"])
        self.db: Any = None  # the real type is private
        self._scope = _get_scope(settings, ("spider", "none"))

    def open_spider(self, spider: Spider) -> None:
        name = spider.name if self._scope == "spider" else "httpcache"
        dbpath = Path(self.cachedir, f"{name}.db")
        self.db = self.dbmodule.open(str(dbpath), "c")

        logger.debug(
            "Using DBM cache storage in %(cachepath)s",
            {"cachepath": dbpath},
            extra={"spider": spider},
        )

        self._fingerprinter: RequestFingerprinterProtocol = (
            spider.crawler.request_fingerprinter
        )

    def close_spider(self, spider: Spider) -> None:
        self.db.close()

    def retrieve_response(self, spider: Spider, request: Request) -> Response | None:
        result = self._read_data(spider, request)
        if result is None:
            return None  # not cached
        data, ts = result
        request.meta["cache_timestamp"] = ts
        return response_from_dict(data)

    def store_response(
        self, spider: Spider, request: Request, response: Response
    ) -> None:
        key = self._fingerprinter.fingerprint(request).hex()
        self.db[f"{key}_data"] = pickle.dumps(response.to_dict(), protocol=4)
        self.db[f"{key}_time"] = str(time())

    def _read_data(
        self, spider: Spider, request: Request
    ) -> tuple[dict[str, Any], float] | None:
        key = self._fingerprinter.fingerprint(request).hex()
        db = self.db
        tkey = f"{key}_time"
        if tkey not in db:
            return None  # not found

        ts = float(db[tkey])
        if 0 < self.expiration_secs < time() - ts:
            return None  # expired

        data = cast("dict[str, Any]", pickle.loads(db[f"{key}_data"]))  # noqa: S301
        return data, ts


class FilesystemCacheStorage:
    def __init__(self, settings: BaseSettings):
        self.cachedir: str = data_path(settings["HTTPCACHE_DIR"])
        self.expiration_secs: int = settings.getint("HTTPCACHE_EXPIRATION_SECS")
        self.use_gzip: bool = settings.getbool("HTTPCACHE_GZIP")
        self.ranges: bool = settings.getbool("HTTPCACHE_RANGE_REQUESTS")
        self._scope = _get_scope(settings, ("spider", "none", "domain"))
        # https://github.com/python/mypy/issues/10740
        self._open: Callable[
            Concatenate[str | os.PathLike[str], str, ...], IO[bytes]
        ] = (
            gzip.open if self.use_gzip else open  # type: ignore[assignment]
        )

    def open_spider(self, spider: Spider) -> None:
        logger.debug(
            "Using filesystem cache storage in %(cachedir)s",
            {"cachedir": self.cachedir},
            extra={"spider": spider},
        )

        self._fingerprinter = spider.crawler.request_fingerprinter

    def close_spider(self, spider: Spider) -> None:
        pass

    def retrieve_response(self, spider: Spider, request: Request) -> Response | None:
        """Return response if present in cache, or None otherwise."""
        if self.ranges:
            range_value = request.headers.get(b"Range")
            if range_value is not None and self._range_request_supported(request):
                parsed = _parse_range_header(range_value)
                assert parsed is not None
                return self.cached_range_response(spider, request, *parsed)
        metadata = self._read_meta(spider, request)
        if metadata is None:
            if self.ranges and self._full_request_supported(request):
                return self.retrieve_full_range_response(spider, request)
            return None  # not cached
        if self.ranges and self._full_request_supported(request):
            ranged = self.retrieve_full_range_response(spider, request)
            if ranged is not None:
                return ranged
        rpath = Path(self._get_request_path(spider, request))
        with self._open(rpath / "response_body", "rb") as f:
            body = f.read()
        with self._open(rpath / "response_headers", "rb") as f:
            rawheaders = f.read()
        if self.ranges:
            range_value = request.headers.get(b"Range")
            if range_value is not None and self._range_request_supported(request):
                response = _slice_cached_response(
                    self, rpath, metadata, rawheaders, body, range_value, request
                )
                if response is not None:
                    return response
        data = {
            "url": metadata["response_url"],
            "status": metadata["status"],
            "headers": headers_raw_to_dict(rawheaders),
            "body": body,
        }
        datapath = rpath / "response_data"
        if datapath.exists():
            with self._open(datapath, "rb") as f:
                data.update(pickle.load(f))  # noqa: S301
        request.meta["cache_timestamp"] = metadata["timestamp"]
        return response_from_dict(data)

    @staticmethod
    def _range_request_supported(request: Request) -> bool:
        return (
            request.method == "GET"
            and not request.meta.get("body_file")
            and not any(
                header in request.headers
                for header in (b"If-Range", b"If-None-Match", b"If-Modified-Since")
            )
            and _parse_range_header(request.headers.get(b"Range")) is not None
        )

    @staticmethod
    def _full_request_supported(request: Request) -> bool:
        return (
            request.method == "GET"
            and not request.meta.get("body_file")
            and b"Range" not in request.headers
            and not any(
                header in request.headers
                for header in (b"If-Range", b"If-None-Match", b"If-Modified-Since")
            )
        )

    def store_response(
        self, spider: Spider, request: Request, response: Response
    ) -> None:
        """Store the given response in the cache."""
        if response.status == 206:
            if self.ranges and self._is_range_storable(response):
                self._store_range_response(
                    Path(self._get_request_path(spider, request)), response
                )
                return
            if self.ranges:
                return
        if b"Content-Range" in response.headers and self.ranges:
            # A partial body must never become the whole cached response.
            return
        rpath = Path(self._get_request_path(spider, request))
        if not rpath.exists():
            rpath.mkdir(parents=True)
        metadata = {
            "url": request.url,
            "method": request.method,
            "status": response.status,
            "response_url": response.url,
            "timestamp": time(),
        }
        with self._open(rpath / "meta", "wb") as f:
            f.write(repr(metadata).encode())
        with self._open(rpath / "pickled_meta", "wb") as f:
            pickle.dump(metadata, f, protocol=4)
        with self._open(rpath / "response_headers", "wb") as f:
            f.write(response.headers.to_string())
        with self._open(rpath / "response_body", "wb") as f:
            f.write(response.body)
        data = {
            key: value
            for key, value in response.to_dict().items()
            if key not in {"url", "status", "headers", "body"}
        }
        with self._open(rpath / "response_data", "wb") as f:
            pickle.dump(data, f, protocol=4)
        with self._open(rpath / "request_headers", "wb") as f:
            f.write(request.headers.to_string())
        with self._open(rpath / "request_body", "wb") as f:
            f.write(request.body)

        if self.ranges and self._is_range_storable(response):
            self._store_range_response(rpath, response)

    @staticmethod
    def _is_range_storable(response: Response) -> bool:
        return (
            response.status in {200, 206}
            and _is_strong_etag(response.headers.get(b"ETag"))
            and b"Content-Encoding" not in response.headers
            and not response.headers.getlist(b"Vary")
        )

    def _store_range_response(self, rpath: Path, response: Response) -> None:
        etag = cast("bytes", response.headers.get(b"ETag"))
        if response.status == 200:
            if b"Content-Range" in response.headers:
                return
            total = len(response.body)
            content_range = None
        else:
            content_range = _parse_content_range(response.headers.get(b"Content-Range"))
            if content_range is None:
                return
            _, _, total = content_range

        version_path = rpath / "ranges" / hashlib.sha1(etag).hexdigest()  # noqa: S324
        version_path.mkdir(parents=True, exist_ok=True)
        meta_path = version_path / "meta"
        if meta_path.exists():
            with self._open(meta_path, "rb") as f:
                previous = pickle.load(f)  # noqa: S301
            if previous.get("total") != total:
                # The same ETag with a different size cannot describe one
                # resource, so the stored version cannot be trusted.
                return
        meta = {
            "etag": etag,
            "total": total,
            "headers": response.headers.raw,
            "data": {
                key: value
                for key, value in response.to_dict().items()
                if key not in {"url", "status", "headers", "body"}
            },
            "timestamp": time(),
        }

        if response.status == 200:
            self._write_atomic(meta_path, pickle.dumps(meta, protocol=4))
            self._write_atomic(version_path / "body", response.body)
            self._write_atomic(
                version_path.parent / "current", version_path.name.encode()
            )
            return

        assert content_range is not None
        start, end, _ = content_range
        expected = end - start + 1
        if len(response.body) != expected:
            return
        segments_path = version_path / "segments"
        segments_path.mkdir(exist_ok=True)
        # The segment only becomes visible once fully written, so a crash in
        # the middle of the write leaves an unreferenced temporary file.
        self._write_atomic(segments_path / f"{start}-{end}", response.body)
        self._write_atomic(meta_path, pickle.dumps(meta, protocol=4))

    def _write_atomic(self, path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        with self._open(tmp_path, "wb") as f:
            f.write(data)
        tmp_path.replace(path)

    def _range_path(self, spider: Spider, request: Request) -> Path:
        return Path(self._get_request_path(spider, request)) / "ranges"

    def range_versions(
        self, spider: Spider, request: Request
    ) -> list[_RangeCacheEntry]:
        return self._read_range_entries(self._range_path(spider, request))

    def _read_range_entries(self, path: Path) -> list[_RangeCacheEntry]:
        entries: list[_RangeCacheEntry] = []
        if not path.exists():
            return entries
        for version_path in path.iterdir():
            metapath = version_path / "meta"
            if not metapath.exists():
                continue
            if 0 < self.expiration_secs < time() - metapath.stat().st_mtime:
                continue
            try:
                entries.append(_RangeCacheEntry(self, version_path))
            except Exception:
                # An unreadable version must not hide the other stored ones.
                logger.debug(
                    f"Ignoring an unreadable stored range version at {version_path}",
                    exc_info=True,
                )
                continue
        entries.sort(key=lambda entry: entry.timestamp, reverse=True)
        current_path = path / "current"
        if current_path.exists():
            current = current_path.read_bytes()
            entries.sort(key=lambda entry: entry.path.name.encode() != current)
        return entries

    def cached_range_response(
        self, spider: Spider, request: Request, start: int, end: int
    ) -> Response | None:
        """Return a ``206`` response if a stored version has every requested
        byte of *start*–*end*."""
        for entry in self.range_versions(spider, request):
            response = self._range_response(entry, request, start, end)
            if response is not None:
                return response
        return None

    def cached_range_response_for_etag(
        self,
        spider: Spider,
        request: Request,
        etag: bytes,
        start: int,
        end: int,
    ) -> Response | None:
        for entry in self.range_versions(spider, request):
            if entry.etag != etag:
                continue
            return self._range_response(entry, request, start, end)
        return None

    @staticmethod
    def _range_response(
        entry: _RangeCacheEntry, request: Request, start: int, end: int
    ) -> Response | None:
        if end >= entry.total:
            return None
        body = entry.read(start, end)
        if body is None:
            return None
        return _build_range_response(request, entry, start, end, body)

    def range_plan(
        self, spider: Spider, request: Request, start: int, end: int
    ) -> tuple[bytes, int, list[tuple[int, int]]] | None:
        """Return the ``ETag`` and total size of the newest stored version
        that covers part of *start*–*end*, and the intervals it is missing.

        Return ``None`` when no stored version covers any of those bytes."""
        for entry in self.range_versions(spider, request):
            if end >= entry.total:
                continue
            covered = entry.covered(start, end)
            if not covered:
                continue
            return entry.etag, entry.total, _missing_intervals(covered, start, end)
        return None

    def retrieve_full_range_response(
        self, spider: Spider, request: Request
    ) -> Response | None:
        """Return a ``200`` response if a stored version has all its bytes."""
        if not self.ranges or not self._full_request_supported(request):
            return None
        for entry in self.range_versions(spider, request):
            body = entry.body()
            if body is None:
                continue
            headers = Headers(
                (key, value)
                for key, value in entry.headers
                if key.lower() not in {b"content-range", b"content-length"}
            )
            headers[b"ETag"] = entry.etag
            headers[b"Content-Length"] = str(len(body)).encode()
            data: dict[str, Any] = {
                "url": request.url,
                "status": 200,
                "headers": {key: list(values) for key, values in headers.items()},
                "body": body,
            }
            data.update(entry.data)
            request.meta["cache_timestamp"] = entry.timestamp
            return response_from_dict(data)
        return None

    def store_partial_range(
        self, spider: Spider, request: Request, response: Response
    ) -> bool:
        """Store a ``206`` response as a range segment.

        Return ``False`` when the response cannot be stored that way."""
        if not self.ranges or not self._is_range_storable(response):
            return False
        self._store_range_response(
            Path(self._get_request_path(spider, request)), response
        )
        return True

    def _get_request_path(self, spider: Spider, request: Request) -> str:
        key = self._fingerprinter.fingerprint(request).hex()
        scope_path = self._get_scope_path(spider, request)
        return str(Path(self.cachedir, scope_path, key[0:2], key))

    def _get_scope_path(self, spider: Spider, request: Request) -> str:
        if self._scope == "spider":
            return spider.name
        if self._scope == "domain":
            # quote() covers hostnames that are not valid path components,
            # such as IPv6 addresses.
            return quote(urlparse_cached(request).hostname or "_", safe="")
        return ""

    def _read_meta(self, spider: Spider, request: Request) -> dict[str, Any] | None:
        rpath = Path(self._get_request_path(spider, request))
        metapath = rpath / "pickled_meta"
        if not metapath.exists():
            return None  # not found
        mtime = metapath.stat().st_mtime
        if 0 < self.expiration_secs < time() - mtime:
            return None  # expired
        with self._open(metapath, "rb") as f:
            return cast("dict[str, Any]", pickle.load(f))  # noqa: S301


def parse_cachecontrol(header: bytes) -> dict[bytes, bytes | None]:
    """Parse Cache-Control header

    https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9

    >>> parse_cachecontrol(b'public, max-age=3600') == {b'public': None,
    ...                                                 b'max-age': b'3600'}
    True
    >>> parse_cachecontrol(b'') == {}
    True

    """
    directives = {}
    for directive in header.split(b","):
        key, sep, val = directive.strip().partition(b"=")
        if key:
            directives[key.lower()] = val if sep else None
    return directives


def rfc1123_to_epoch(date_str: str | bytes | None) -> int | None:
    # The HTTP time format (which is how we use this function) is actually in
    # RFC 7231 §7.1.1.1, it refers to RFC 5322 and also requires support for
    # two obsolete formats (RFC 850 and asctime()). email.utils evidently
    # supports all three.
    if date_str is None:
        return None
    try:
        date_str = to_unicode(date_str, encoding="ascii")
        date = parsedate_to_datetime(date_str)
        if date.tzinfo is None:
            date = date.replace(tzinfo=UTC)
        return int(date.timestamp())
    except Exception:
        return None
