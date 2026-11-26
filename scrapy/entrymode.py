from __future__ import annotations

import base64
import hashlib
import json
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any
from weakref import WeakKeyDictionary

from twisted.internet.defer import Deferred

from scrapy.exceptions import IgnoreRequest, NotConfigured
from scrapy.http import HtmlResponse, Request, Response, TextResponse
from scrapy.utils.conf import _job_dir
from scrapy.utils.defer import maybe_deferred_to_future
from scrapy.utils.request import fingerprint

if TYPE_CHECKING:
    from collections.abc import Callable

_ENABLED_SETTINGS = (
    "ENTRY_SHARING_ENABLED",
    "ENTRY_COLLECTION_ENABLED",
    "SHARED_ENTRY_MODE",
    "SHARED_ENTRY_COLLECTION_ENABLED",
    "REQUEST_SHARING_ENABLED",
    "DOWNLOAD_COALESCING_ENABLED",
)
_ISSUE_KEYS = (
    "entry_issue",
    "issue_id",
    "issue",
    "edition_id",
    "edition",
    "bulletin_issue",
    "period_id",
)
_ENTRY_KEYS = (
    "entry_id",
    "entry_no",
    "entry_number",
    "entry",
    "directory_entry_id",
    "directory_entry",
)
_PARSER_KEYS = (
    "entry_parser",
    "parser_profile",
    "parsing_convention",
    "parse_convention",
    "parsing_profile",
    "parse_profile",
    "parser_name",
    "parser",
)

_META = {
    "active": "entry_collection",
    "share_key": "entry_share_key",
    "attempt_id": "entry_attempt_id",
    "owner": "entry_attempt_owner",
    "carrier": "entry_attempt_carrier",
    "attempt_number": "entry_attempt_number",
    "delivery": "entry_response_delivery",
    "delivery_key": "entry_delivery_key",
    "article_hit": "entry_article_hit",
    "resume": "entry_resume_request",
}


def _now() -> float:
    return time.time()


def _meta_value(request: Request, keys: tuple[str, ...], default: Any = None) -> Any:
    for key in keys:
        if key in request.meta:
            return request.meta[key]
    return default


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        if isinstance(value, bytes):
            return base64.b64encode(value).decode("ascii")
        if isinstance(value, dict):
            return {str(key): _json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [_json_safe(item) for item in value]
        return repr(value)
    return value


def request_share_key(issue: str, parser: str, request: Request) -> str:
    """Return the key of a download attempt shared by equal requests."""
    header_names = sorted(bytes(name).lower() for name in request.headers)
    request_fingerprint = fingerprint(request, include_headers=header_names).hex()
    raw = json.dumps(
        {
            "issue": issue,
            "parser": parser,
            "fingerprint": request_fingerprint,
        },
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class EntryCollection:
    """Persistent journals, article cache and shared attempt state for entries."""

    _instances: WeakKeyDictionary[Any, EntryCollection] = WeakKeyDictionary()

    def __init__(self, jobdir: str | Path | None = None) -> None:
        self.jobdir = Path(jobdir) if jobdir else None
        self.path = self.jobdir / "entry_collection" / "state.json" if jobdir else None
        self._lock = threading.RLock()
        self.before_retry_return: Callable[[dict[str, Any], Request], None] | None = (
            None
        )
        self.state: dict[str, Any] = {"issues": {}}
        if self.path and self.path.exists():
            self.state = json.loads(self.path.read_text(encoding="utf-8"))

    @classmethod
    def for_crawler(cls, crawler: Any) -> EntryCollection:
        collection = cls._instances.get(crawler)
        if collection is None:
            collection = cls(_job_dir(crawler.settings))
            cls._instances[crawler] = collection
        return collection

    def refresh(self) -> EntryCollection:
        if self.path and self.path.exists():
            with self._lock:
                self.state = json.loads(self.path.read_text(encoding="utf-8"))
        return self

    def save(self) -> None:
        if self.path is None:
            return
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_name(f".{self.path.name}.tmp")
            temporary.write_text(
                json.dumps(self.state, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            temporary.replace(self.path)

    def _issue_state(self, issue: str) -> dict[str, Any]:
        return self.state["issues"].setdefault(
            issue,
            {
                "entries": {},
                "articles": {},
                "attempts": {},
                "active_attempts": {},
                "attempt_generations": {},
            },
        )

    def submit_with_state(
        self,
        issue: str,
        entry_id: str,
        start_url: str | None = None,
        *,
        parser: str = "default",
        max_redirects: int = 2,
        callback: str = "parse_entry",
        errback: str = "entry_on_error",
        **extra: Any,
    ) -> tuple[dict[str, Any], bool]:
        """Create an entry or return its journal and whether it was created."""
        with self._lock:
            issue_state = self._issue_state(issue)
            entries = issue_state["entries"]
            if entry_id in entries:
                entry = entries[entry_id]
                entry["resubmitted_at"] = _now()
                entry["resubmissions"] = int(entry.get("resubmissions", 0)) + 1
                self.save()
                return json.loads(json.dumps(entry)), False

            if start_url is None:
                raise ValueError("start_url is required for a new entry")
            entry = {
                "issue": issue,
                "entry_id": entry_id,
                "start_url": start_url,
                "url": start_url,
                "parser": parser or "default",
                "max_redirects": max_redirects,
                "callback": callback,
                "errback": errback,
                "status": "pending",
                "created_at": _now(),
                "updated_at": _now(),
                "visited_urls": [start_url],
                "redirects": [],
                "responses": [],
                "attempts": [],
                "retries": [],
                "article": None,
                "article_ref": None,
                "result": None,
                "error": None,
                "current_request": None,
                "resubmissions": 0,
                **_json_safe(extra),
            }
            entries[entry_id] = entry
            self.save()
            return json.loads(json.dumps(entry)), True

    def submit(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Idempotently submit an entry and return its current journal."""
        record, _created = self.submit_with_state(*args, **kwargs)
        return record

    def resubmit(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Resubmit an entry ID, returning its original progress."""
        return self.submit(*args, **kwargs)

    def query(self, issue: str, entry_id: str) -> dict[str, Any]:
        """Return an entry journal."""
        return self.get(issue, entry_id)

    def progress(self, issue: str, entry_id: str) -> dict[str, Any]:
        """Return an entry journal."""
        return self.get(issue, entry_id)

    def get(self, issue: str, entry_id: str) -> dict[str, Any]:
        self.refresh()
        with self._lock:
            return json.loads(json.dumps(self._entry(issue, entry_id)))

    def entries(self, issue: str) -> list[dict[str, Any]]:
        self.refresh()
        with self._lock:
            return [
                json.loads(json.dumps(entry))
                for entry in self._issue_state(issue)["entries"].values()
            ]

    def _entry(self, issue: str, entry_id: str) -> dict[str, Any]:
        return self._issue_state(issue)["entries"][entry_id]

    def _touch(self, entry: dict[str, Any], status: str | None = None) -> None:
        entry["updated_at"] = _now()
        if status is not None:
            entry["status"] = status

    def snapshot_request(self, issue: str, entry_id: str, request: Request) -> None:
        with self._lock:
            entry = self._entry(issue, entry_id)
            url = request.url
            visited = entry.setdefault("visited_urls", [])
            if not visited or visited[-1] != url:
                visited.append(url)
            entry["url"] = url
            entry["current_request"] = {
                "url": url,
                "method": request.method,
                "meta": _json_safe(dict(request.meta)),
                "callback": self._callback_name(
                    request, entry.get("callback", "parse_entry")
                ),
                "errback": entry.get("errback", "entry_on_error"),
            }
            self._touch(entry, "running")
            self.save()

    @staticmethod
    def _callback_name(request: Request, default: str) -> str:
        callback = request.callback
        if callback is None:
            return "parse"
        name = getattr(callback, "__name__", None)
        return name or default

    def record_redirect(
        self,
        issue: str,
        entry_id: str,
        *,
        url: str,
        status: int,
        location: str | bytes | None,
        target: str | None,
    ) -> None:
        with self._lock:
            entry = self._entry(issue, entry_id)
            location_text = (
                location.decode("utf-8", errors="replace")
                if isinstance(location, bytes)
                else location
            )
            event = {
                "at": _now(),
                "url": url,
                "status": status,
                "location": location_text,
                "target": target,
            }
            entry.setdefault("redirects", []).append(event)
            entry.setdefault("responses", []).append(
                {
                    "at": event["at"],
                    "url": url,
                    "status": status,
                    "location": location_text,
                }
            )
            self._touch(entry)
            self.save()

    def begin_attempt(
        self,
        issue: str,
        parser: str,
        key: str,
        *,
        url: str,
        method: str,
        entry_id: str,
        attempt_id: str | None = None,
        number: int = 1,
        carrier: bool = False,
        new_generation: bool = False,
    ) -> dict[str, Any]:
        with self._lock:
            issue_state = self._issue_state(issue)
            attempts = issue_state["attempts"]
            if attempt_id is None:
                generation = int(issue_state["attempt_generations"].get(key, 0))
                if new_generation or generation == 0:
                    generation += 1
                    issue_state["attempt_generations"][key] = generation
                attempt_id = key if generation == 1 else f"{key}:g{generation}"
            attempt = attempts.get(attempt_id)
            if attempt is None:
                attempt = {
                    "id": attempt_id,
                    "share_key": key,
                    "issue": issue,
                    "parser": parser,
                    "url": url,
                    "method": method,
                    "number": number - 1,
                    "status": "created",
                    "waiters": [],
                    "events": [],
                    "response": None,
                }
                attempts[attempt_id] = attempt
            if entry_id not in attempt["waiters"]:
                attempt["waiters"].append(entry_id)
            attempt["url"] = url
            attempt["method"] = method
            attempt["number"] = max(int(attempt.get("number", 0)), number)
            attempt["status"] = "downloading"
            attempt["carrier"] = carrier
            issue_state["active_attempts"][key] = attempt_id
            attempt["events"].append(
                {
                    "at": _now(),
                    "type": "request",
                    "number": attempt["number"],
                    "url": url,
                }
            )
            entry = self._entry(issue, entry_id)
            if attempt_id not in entry.setdefault("attempts", []):
                entry["attempts"].append(attempt_id)
            self._touch(entry, "running")
            self.save()
            return json.loads(json.dumps(attempt))

    def active_attempt(self, issue: str, share_key: str) -> dict[str, Any] | None:
        with self._lock:
            issue_state = self._issue_state(issue)
            attempt_id = issue_state.get("active_attempts", {}).get(share_key)
            if not attempt_id:
                return None
            attempt = issue_state["attempts"].get(attempt_id)
            if not attempt or attempt.get("status") not in {
                "created",
                "downloading",
                "retry_queued",
            }:
                return None
            return json.loads(json.dumps(attempt))

    def add_waiter(self, issue: str, attempt_id: str, entry_id: str) -> None:
        with self._lock:
            attempt = self._issue_state(issue)["attempts"][attempt_id]
            if entry_id not in attempt["waiters"]:
                attempt["waiters"].append(entry_id)
            entry = self._entry(issue, entry_id)
            if attempt_id not in entry.setdefault("attempts", []):
                entry["attempts"].append(attempt_id)
            self._touch(entry, "running")
            self.save()

    def attempt(self, issue: str, attempt_id: str) -> dict[str, Any] | None:
        with self._lock:
            attempt = self._issue_state(issue)["attempts"].get(attempt_id)
            return json.loads(json.dumps(attempt)) if attempt else None

    def save_response_snapshot(
        self,
        issue: str,
        key: str,
        response: Response,
        *,
        number: int,
    ) -> dict[str, Any]:
        snapshot = {
            "url": response.url,
            "status": response.status,
            "headers": {
                name.decode("latin1"): [
                    value.decode("latin1", errors="replace") for value in values
                ]
                for name, values in response.headers.items()
            },
            "body_b64": base64.b64encode(response.body).decode("ascii"),
            "flags": list(response.flags),
        }
        with self._lock:
            attempt = self._issue_state(issue)["attempts"][key]
            attempt["response"] = snapshot
            attempt["number"] = number
            if attempt.get("status") not in {"retry_queued", "delivery_queued"}:
                attempt["status"] = "response"
            attempt["events"].append(
                {
                    "at": _now(),
                    "type": "response",
                    "number": number,
                    "status": response.status,
                    "location": response.headers.get("Location", b"").decode(
                        "utf-8", errors="replace"
                    )
                    or None,
                }
            )
            self.save()
            return json.loads(json.dumps(snapshot))

    def record_shared_response(self, issue: str, attempt_id: str) -> None:
        with self._lock:
            attempt = self._issue_state(issue)["attempts"][attempt_id]
            snapshot = attempt.get("response") or {}
            event = {
                "at": _now(),
                "attempt_id": attempt_id,
                "number": attempt.get("number"),
                "url": snapshot.get("url", attempt.get("url")),
                "status": snapshot.get("status"),
                "location": (snapshot.get("headers") or {}).get("Location", [None])[0],
                "shared": True,
            }
            for entry_id in attempt["waiters"]:
                entry = self._entry(issue, entry_id)
                entry.setdefault("responses", []).append(dict(event))
                self._touch(entry)
            self.save()

    def mark_retry_queued(
        self,
        issue: str,
        key: str,
        *,
        reason: Any,
        retry_number: int,
    ) -> None:
        reason_text = reason if isinstance(reason, str) else str(reason)
        with self._lock:
            attempt = self._issue_state(issue)["attempts"][key]
            attempt["status"] = "retry_queued"
            attempt["retry_number"] = retry_number
            attempt["events"].append(
                {
                    "at": _now(),
                    "type": "retry_queued",
                    "number": retry_number,
                    "reason": reason_text,
                }
            )
            for entry_id in attempt["waiters"]:
                entry = self._entry(issue, entry_id)
                retry_event = {
                    "at": _now(),
                    "attempt_key": key,
                    "attempt_number": retry_number,
                    "reason": reason_text,
                    "url": attempt["url"],
                }
                entry.setdefault("retries", []).append(retry_event)
                self._touch(entry, "waiting_retry")
            self.save()

    def mark_delivery_queued(self, issue: str, key: str) -> list[str]:
        with self._lock:
            attempt = self._issue_state(issue)["attempts"][key]
            attempt["status"] = "delivery_queued"
            attempt["delivered_at"] = _now()
            self.save()
            return list(attempt["waiters"])

    def mark_entry_delivery_queued(self, issue: str, entry_id: str, key: str) -> None:
        with self._lock:
            entry = self._entry(issue, entry_id)
            entry["pending_delivery"] = key
            self._touch(entry, "running")
            self.save()

    def clear_pending_delivery(self, issue: str, entry_id: str) -> None:
        with self._lock:
            entry = self._entry(issue, entry_id)
            entry.pop("pending_delivery", None)
            self.save()

    def complete(
        self,
        issue: str,
        entry_id: str,
        *,
        result: dict[str, Any],
        article: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            entry = self._entry(issue, entry_id)
            if article is not None:
                article_key = article["article_key"]
                self._issue_state(issue)["articles"][article_key] = article
                entry["article_ref"] = {
                    "article_key": article_key,
                    "entry_id": article.get("entry_id", entry_id),
                    "issue": issue,
                    "parser": article["parser"],
                    "url": article["url"],
                }
                entry["article"] = {
                    key: value for key, value in article.items() if key != "body_b64"
                }
            entry["result"] = result
            entry["error"] = None
            entry.pop("pending_delivery", None)
            self._touch(entry, "completed")
            self.save()
            return json.loads(json.dumps(entry))

    def reference_article(
        self,
        issue: str,
        entry_id: str,
        article: dict[str, Any],
    ) -> dict[str, Any]:
        with self._lock:
            entry = self._entry(issue, entry_id)
            source_entry_id = article.get("entry_id")
            result = dict(article.get("data") or {})
            result.setdefault("title", article.get("title"))
            result["entry_id"] = entry_id
            result["referenced_from_entry_id"] = source_entry_id
            result["source_entry_id"] = source_entry_id
            result["cached_article"] = True
            result["article_url"] = article["url"]
            entry["result"] = result
            entry["article_ref"] = {
                "article_key": article["article_key"],
                "entry_id": source_entry_id,
                "source_entry_id": source_entry_id,
                "referenced_from_entry_id": source_entry_id,
                "issue": issue,
                "parser": article["parser"],
                "url": article["url"],
            }
            entry["article"] = {
                key: value for key, value in article.items() if key != "body_b64"
            }
            entry["error"] = None
            entry.pop("pending_delivery", None)
            self._touch(entry, "completed")
            self.save()
            return json.loads(json.dumps(entry))

    def article(self, issue: str, parser: str, url: str) -> dict[str, Any] | None:
        article_key = self.article_key(parser, url)
        with self._lock:
            article = self._issue_state(issue)["articles"].get(article_key)
            return json.loads(json.dumps(article)) if article else None

    @staticmethod
    def article_key(parser: str, url: str) -> str:
        raw = json.dumps({"parser": parser or "default", "url": url}, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def save_article_from_response(
        self,
        issue: str,
        entry_id: str,
        response: Response,
        data: dict[str, Any],
        *,
        parser: str | None = None,
    ) -> dict[str, Any]:
        entry = self.get(issue, entry_id)
        profile = parser or entry["parser"]
        url = response.url
        article_key = self.article_key(profile, url)
        article = {
            "article_key": article_key,
            "issue": issue,
            "parser": profile,
            "url": url,
            "entry_id": entry_id,
            "title": data.get("title"),
            "data": _json_safe(data),
            "body_b64": base64.b64encode(response.body).decode("ascii"),
            "encoding": getattr(response, "encoding", "utf-8") or "utf-8",
            "saved_at": _now(),
        }
        result = dict(data)
        result.setdefault("entry_id", entry_id)
        return self.complete(issue, entry_id, result=result, article=article)

    def fail(
        self,
        issue: str,
        entry_id: str,
        *,
        error_type: str,
        message: str,
        url: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            entry = self._entry(issue, entry_id)
            redirects = entry.get("redirects", [])
            last_redirect = redirects[-1] if redirects else None
            error: dict[str, Any] = {
                "type": error_type,
                "message": message,
                "url": url or entry.get("url"),
                "at": _now(),
            }
            if last_redirect and "redirect" in message.lower():
                error.setdefault("status", last_redirect.get("status"))
                error.setdefault("location", last_redirect.get("location"))
                error.setdefault("target", last_redirect.get("target"))
            if extra:
                error.update(_json_safe(extra))
            entry["error"] = error
            entry["result"] = None
            entry.pop("pending_delivery", None)
            self._touch(entry, "failed")
            self.save()
            return json.loads(json.dumps(entry))

    def resumable_entry_requests(self, issue: str) -> list[dict[str, Any]]:
        with self._lock:
            return [
                json.loads(json.dumps(entry["current_request"]))
                for entry in self._issue_state(issue)["entries"].values()
                if entry.get("current_request")
                and entry.get("status") in {"pending", "running", "waiting_retry"}
            ]


class _InflightAttempt:
    def __init__(self, attempt_id: str) -> None:
        self.attempt_id = attempt_id
        self.deferred: Deferred[Response] = Deferred()
        self.settled = False


class EntrySharingMiddleware:
    """Coalesce equal downloads and replay their responses per entry."""

    def __init__(self, crawler: Any) -> None:
        if not any(crawler.settings.getbool(name) for name in _ENABLED_SETTINGS):
            raise NotConfigured
        self.crawler = crawler
        self.collection = EntryCollection.for_crawler(crawler)
        self._inflight: dict[str, _InflightAttempt] = {}

    @classmethod
    def from_crawler(cls, crawler: Any) -> EntrySharingMiddleware:
        return cls(crawler)

    @staticmethod
    def _context(
        request: Request, default_issue: str | None = None
    ) -> tuple[str, str, str] | None:
        if request.meta.get(_META["active"]) is False:
            return None
        issue = _meta_value(request, _ISSUE_KEYS, default_issue)
        entry_id = _meta_value(request, _ENTRY_KEYS)
        parser = _meta_value(request, _PARSER_KEYS, "default") or "default"
        if issue is None or entry_id is None:
            return None
        return str(issue), str(entry_id), str(parser)

    async def process_request(self, request: Request) -> Response | None:
        context = self._context(request)
        if context is None:
            return None
        issue, entry_id, parser = context
        request.headers.pop("Referer", None)
        request.meta["dont_retry"] = True
        request.meta.pop(_META["owner"], None)

        if request.meta.get(_META["delivery"]):
            return self._delivery_response(request)

        if request.meta.get(_META["carrier"]):
            key = request.meta[_META["share_key"]]
            attempt_id = request.meta[_META["attempt_id"]]
            number = int(request.meta.get(_META["attempt_number"], 2))
            request.meta["dont_redirect"] = True
            attempt = self.collection.begin_attempt(
                issue,
                parser,
                key,
                url=request.url,
                method=request.method,
                entry_id=entry_id,
                attempt_id=attempt_id,
                number=number,
                carrier=True,
            )
            request.meta[_META["attempt_id"]] = attempt["id"]
            return None

        self.collection.snapshot_request(issue, entry_id, request)

        cached = self.collection.article(issue, parser, request.url)
        if cached is not None:
            request.meta[_META["article_hit"]] = cached
            request.callback = self._cached_article_callback
            return self._response_from_article(cached, request)

        key = request_share_key(issue, parser, request)
        request.meta[_META["share_key"]] = key
        active = self.collection.active_attempt(issue, key)
        inflight = self._inflight.get(key)

        if inflight is not None:
            attempt_id = inflight.attempt_id
            request.meta[_META["attempt_id"]] = attempt_id
            self.collection.add_waiter(issue, attempt_id, entry_id)
            shared_response = await maybe_deferred_to_future(inflight.deferred)
            return shared_response.replace(request=request)

        if active and active.get("status") == "retry_queued":
            request.meta[_META["attempt_id"]] = active["id"]
            self.collection.add_waiter(issue, active["id"], entry_id)
            raise IgnoreRequest("shared entry retry is already queued")

        attempt = self.collection.begin_attempt(
            issue,
            parser,
            key,
            url=request.url,
            method=request.method,
            entry_id=entry_id,
            new_generation=active is None,
        )
        attempt_id = attempt["id"]
        request.meta[_META["attempt_id"]] = attempt_id
        inflight = _InflightAttempt(attempt_id)
        self._inflight[key] = inflight
        request.meta[_META["owner"]] = True
        return None

    def process_response(
        self,
        request: Request,
        response: Response,
        spider: Any | None = None,
    ) -> Response | Request:
        context = EntrySharingMiddleware._context(request)
        if context is None:
            return response
        issue, entry_id, _parser = context

        if request.meta.get(_META["article_hit"]):
            article = request.meta[_META["article_hit"]]
            self.collection.reference_article(issue, entry_id, article)
            return response

        if request.meta.get(_META["delivery"]):
            if response.status in {301, 302, 303, 307, 308}:
                self._record_redirect_from_response(issue, entry_id, response)
            request.meta.pop(_META["delivery"], None)
            request.meta.pop(_META["delivery_key"], None)
            return response

        share_key = request.meta.get(_META["share_key"])
        attempt_id = request.meta.get(_META["attempt_id"], share_key)
        if not share_key or not attempt_id:
            return response

        number = int(request.meta.get(_META["attempt_number"], 1))
        is_owner = bool(request.meta.get(_META["owner"]))
        is_carrier = bool(request.meta.get(_META["carrier"]))
        if is_owner or is_carrier:
            self.collection.save_response_snapshot(
                issue, attempt_id, response, number=number
            )

        inflight = self._inflight.get(share_key)
        if inflight is not None and not inflight.deferred.called:
            inflight.settled = True
            inflight.deferred.callback(response)
        self._inflight.pop(share_key, None)

        if request.meta.get(_META["carrier"]):
            return self._handle_carrier_response(
                request, response, issue, share_key, attempt_id, number
            )

        if response.status in {301, 302, 303, 307, 308}:
            self._record_redirect_from_response(issue, entry_id, response)

        if response.status in self._retry_codes():
            retry_request = self._retry_owner_or_stop_follower(
                request, response, issue, share_key, attempt_id, number, is_owner
            )
            if retry_request is not None:
                return retry_request

        return response

    def _retry_codes(self) -> set[int]:
        return {
            int(code)
            for code in self.crawler.settings.getlist(
                "RETRY_HTTP_CODES",
                [500, 502, 503, 504, 522, 524, 408, 429],
            )
        }

    def _retry_owner_or_stop_follower(
        self,
        request: Request,
        response: Response,
        issue: str,
        share_key: str,
        attempt_id: str,
        number: int,
        is_owner: bool,
    ) -> Request | None:
        if not is_owner:
            attempt = self.collection.attempt(issue, attempt_id)
            if attempt and attempt.get("status") == "retry_queued":
                request.errback = _noop_errback
                raise IgnoreRequest("shared attempt retry is handled by its owner")
            return None
        self.collection.record_shared_response(issue, attempt_id)
        return self._maybe_retry(
            request, response, issue, share_key, attempt_id, number
        )

    def process_exception(
        self,
        request: Request,
        exception: Exception,
        spider: Any | None = None,
    ) -> None:
        context = self._context(request)
        if context is None:
            return
        if isinstance(exception, IgnoreRequest):
            return
        issue, entry_id, _parser = context
        share_key = request.meta.get(_META["share_key"])
        if share_key and request.meta.get(_META["owner"]):
            inflight = self._inflight.get(share_key)
            if inflight is not None and not inflight.deferred.called:
                inflight.deferred.errback(exception)
        self.collection.fail(
            issue,
            entry_id,
            error_type=type(exception).__name__,
            message=str(exception),
            url=request.url,
        )

    def _maybe_retry(
        self,
        request: Request,
        response: Response,
        issue: str,
        share_key: str,
        attempt_id: str,
        number: int,
    ) -> Request | None:
        retry_codes = {
            int(code)
            for code in self.crawler.settings.getlist(
                "RETRY_HTTP_CODES",
                [500, 502, 503, 504, 522, 524, 408, 429],
            )
        }
        max_retries = int(
            request.meta.get(
                "max_retry_times", self.crawler.settings.getint("RETRY_TIMES", 0)
            )
        )
        if response.status not in retry_codes or number > max_retries:
            return None
        retry_number = number + 1
        retry_request = request.replace()
        retry_request.meta["retry_times"] = retry_number - 1
        retry_request.meta["skip_dupefilter_once"] = True
        retry_request.meta[_META["carrier"]] = True
        retry_request.meta[_META["attempt_number"]] = retry_number
        retry_request.meta[_META["share_key"]] = share_key
        retry_request.meta[_META["attempt_id"]] = attempt_id
        retry_request.meta["dont_redirect"] = True
        retry_request.meta.pop(_META["owner"], None)
        retry_request.dont_filter = True
        retry_request.priority = request.priority + self.crawler.settings.getint(
            "RETRY_PRIORITY_ADJUST", -1
        )
        reason = f"{response.status} {getattr(response, 'status_message', '')}".strip()
        self.collection.mark_retry_queued(
            issue,
            attempt_id,
            reason=reason or response.status,
            retry_number=retry_number,
        )
        hook = self.collection.before_retry_return
        if hook is not None:
            attempt = self.collection.attempt(issue, attempt_id)
            assert attempt is not None
            hook(attempt, retry_request)
        return retry_request

    def _handle_carrier_response(
        self,
        request: Request,
        response: Response,
        issue: str,
        share_key: str,
        attempt_id: str,
        number: int,
    ) -> Response | Request:
        retry_request = self._maybe_retry(
            request, response, issue, share_key, attempt_id, number
        )
        if retry_request is not None:
            return retry_request

        if response.status in {301, 302, 303, 307, 308} or response.status == 200:
            waiter_ids = self.collection.mark_delivery_queued(issue, attempt_id)
            for entry_id in waiter_ids:
                delivery = self._build_delivery(request, issue, entry_id, attempt_id)
                self.collection.mark_entry_delivery_queued(issue, entry_id, attempt_id)
                self.crawler.engine.crawl(delivery)

        request.callback = _noop_callback
        request.errback = _noop_errback
        raise IgnoreRequest("shared carrier response has been delivered to entries")

    def _build_delivery(
        self,
        carrier: Request,
        issue: str,
        entry_id: str,
        attempt_id: str,
    ) -> Request:
        entry = self.collection.get(issue, entry_id)
        snapshot = entry.get("current_request") or {}
        meta = dict(snapshot.get("meta") or {})
        meta[_META["active"]] = True
        meta[_META["delivery"]] = True
        meta[_META["delivery_key"]] = attempt_id
        meta[_META["attempt_id"]] = attempt_id
        meta.pop(_META["carrier"], None)
        meta.pop(_META["owner"], None)
        meta.pop("dont_redirect", None)
        meta["dont_retry"] = True
        callback_name = (
            snapshot.get("callback") or entry.get("callback") or "parse_entry"
        )
        callback = getattr(self.crawler.spider, callback_name, None)
        if not callable(callback):
            callback = getattr(self.crawler.spider, "parse_entry", None)
        errback_name = entry.get("errback") or "entry_on_error"
        errback = getattr(self.crawler.spider, errback_name, None)
        return carrier.replace(
            url=snapshot.get("url") or carrier.url,
            method=snapshot.get("method") or carrier.method,
            meta=meta,
            callback=callback,
            errback=errback,
            dont_filter=True,
        )

    def _delivery_response(self, request: Request) -> Response:
        context = self._context(request)
        assert context is not None
        issue, _entry_id, _parser = context
        key = request.meta[_META["delivery_key"]]
        attempt = self.collection.attempt(issue, key)
        assert attempt is not None
        snapshot = attempt["response"]
        assert snapshot is not None
        return self._response_from_snapshot(snapshot, request)

    @staticmethod
    def _response_from_snapshot(snapshot: dict[str, Any], request: Request) -> Response:
        body = base64.b64decode(snapshot["body_b64"])
        response_cls = Response
        headers = snapshot.get("headers") or {}
        content_type = {str(k).lower(): v for k, v in headers.items()}.get(
            "content-type"
        )
        if content_type or body[:1] == b"<":
            if content_type and "html" in str(content_type).lower():
                response_cls = HtmlResponse
            else:
                response_cls = TextResponse
        return response_cls(
            url=snapshot["url"],
            status=snapshot["status"],
            headers=headers,
            body=body,
            flags=snapshot.get("flags"),
            request=request,
        )

    def _response_from_article(
        self, article: dict[str, Any], request: Request
    ) -> Response:
        body = base64.b64decode(article["body_b64"])
        return Response(
            url=article["url"],
            status=200,
            headers={"Content-Type": "text/html; charset=utf-8"},
            body=body,
            request=request,
        )

    def _cached_article_callback(self, response: Response) -> dict[str, Any]:
        context = self._context(response.request)
        assert context is not None
        issue, entry_id, _parser = context
        article = response.meta[_META["article_hit"]]
        entry = self.collection.reference_article(issue, entry_id, article)
        return entry["result"]

    def _record_redirect_from_response(
        self, issue: str, entry_id: str, response: Response
    ) -> None:
        location = response.headers.get("Location")
        target = None
        if location is not None:
            target = response.urljoin(location.decode("utf-8", errors="replace"))
        self.collection.record_redirect(
            issue,
            entry_id,
            url=response.url,
            status=response.status,
            location=location,
            target=target,
        )


def _noop_callback(response: Response) -> None:
    return None


def _noop_errback(failure: Any) -> None:
    return None


class EntryModeSpider:
    """Spider helpers for idempotent entry submission and journal updates."""

    entry_issue: str = "default"
    entry_parser: str = "default"
    entry_max_redirects: int = 2

    @property
    def entries(self) -> EntryCollection:
        return EntryCollection.for_crawler(self.crawler)  # type: ignore[attr-defined]

    def submit_entry(
        self,
        entry_id: str,
        url: str,
        *,
        issue: str | None = None,
        parser: str | None = None,
        max_redirects: int | None = None,
    ) -> tuple[dict[str, Any], bool]:
        issue_id = issue or self.entry_issue
        profile = parser or self.entry_parser
        limit = self.entry_max_redirects if max_redirects is None else max_redirects
        return self.entries.submit_with_state(
            issue_id,
            entry_id,
            url,
            parser=profile,
            max_redirects=limit,
        )

    def entry_request(self, record: dict[str, Any]) -> Request:
        meta = {
            _META["active"]: True,
            "entry_issue": record["issue"],
            "entry_id": record["entry_id"],
            "entry_parser": record["parser"],
            "entry_max_redirects": record["max_redirects"],
            "redirect_ttl": record["max_redirects"],
            "max_retry_times": record.get("max_retries", 1),
        }
        current = record.get("current_request")
        if current and current.get("url"):
            meta.update(current.get("meta") or {})
            meta.update(
                {
                    _META["active"]: True,
                    "entry_issue": record["issue"],
                    "entry_id": record["entry_id"],
                    "entry_parser": record["parser"],
                }
            )
            url = current["url"]
        else:
            url = record["start_url"]
        return Request(
            url,
            callback=self.parse_entry,  # type: ignore[attr-defined]
            errback=self.entry_on_error,
            meta=meta,
            dont_filter=True,
        )

    def entry_start_requests(self, specs: list[dict[str, Any]]) -> list[Request]:
        requests = []
        for spec in specs:
            record, _created = self.submit_entry(**spec)
            if record["status"] == "completed":
                continue
            if record.get("status") == "waiting_retry":
                attempt_key = None
                if record.get("attempts"):
                    attempt_key = record["attempts"][-1]
                attempt = (
                    self.entries.attempt(record["issue"], attempt_key)
                    if attempt_key
                    else None
                )
                if attempt and attempt.get("status") == "retry_queued":
                    continue
            requests.append(self.entry_request(record))
        return requests

    def start_requests(self) -> list[Request]:
        specs = getattr(self, "entry_specs", [])
        return self.entry_start_requests(specs)

    async def start(self):
        for request in self.start_requests():
            yield request

    def entry_on_error(self, failure: Any) -> None:
        request = getattr(failure, "request", None)
        context = self._failure_context(request)
        if context is None:
            return
        issue, entry_id, _parser = context
        exception = failure.value
        message = str(exception)
        error_type = type(exception).__name__
        if "max redirections" in message.lower():
            error_type = "redirect_limit"
            message = "maximum number of redirects exceeded"
        self.entries.fail(
            issue,
            entry_id,
            error_type=error_type,
            message=message,
            url=getattr(request, "url", None),
        )

    def _failure_context(self, request: Request | None) -> tuple[str, str, str] | None:
        if request is None:
            return None
        return EntrySharingMiddleware._context(request)

    def save_entry_article(
        self,
        response: Response,
        data: dict[str, Any],
        *,
        entry_id: str | None = None,
        parser: str | None = None,
    ) -> dict[str, Any]:
        request = response.request
        assert request is not None
        context = EntrySharingMiddleware._context(request)
        assert context is not None
        issue, current_entry_id, request_parser = context
        return self.entries.save_article_from_response(
            issue,
            entry_id or current_entry_id,
            response,
            data,
            parser=parser or request_parser,
        )


def entry_mode_settings(**overrides: Any) -> dict[str, Any]:
    """Return settings for a two-redirect, no-referer shared entry crawl."""
    settings: dict[str, Any] = {
        "ENTRY_SHARING_ENABLED": True,
        "HTTP_CACHE_ENABLED": False,
        "REFERER_ENABLED": False,
        "REDIRECT_MAX_TIMES": 2,
        "RETRY_TIMES": 1,
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
    }
    settings.update(overrides)
    return settings


EntryJournal = EntryCollection
SharedEntryCollection = EntryCollection
EntryJournalSpider = EntryModeSpider
SharedDownloadMiddleware = EntrySharingMiddleware
entry_collection_settings = entry_mode_settings

__all__ = [
    "EntryCollection",
    "EntryJournal",
    "EntryJournalSpider",
    "EntryModeSpider",
    "EntrySharingMiddleware",
    "SharedDownloadMiddleware",
    "SharedEntryCollection",
    "entry_collection_settings",
    "entry_mode_settings",
    "request_share_key",
]
