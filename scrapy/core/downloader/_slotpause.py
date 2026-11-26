from __future__ import annotations

import copy
import json
import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self

from twisted.internet.defer import Deferred

from scrapy import signals
from scrapy.http import Request
from scrapy.utils.conf import _job_dir
from scrapy.utils.request import request_from_dict

if TYPE_CHECKING:
    from collections.abc import Generator

    from scrapy.core.downloader import Downloader
    from scrapy.crawler import Crawler

_PAUSE_META_KEYS = (
    "pause_id",
    "slot_pause_id",
    "request_id",
    "original_request_id",
    "request_identity",
    "identity",
    "id",
    "key",
    "request_key",
    "logical_request_id",
    "client_request_id",
    "idempotency_key",
    "label",
    "name",
)


class RequestStatus(str):
    """Status of a request known to a paused download slot."""

    __slots__ = ("phase", "request_id", "slot")

    request_id: int | None
    slot: str | None
    phase: str | None

    STARTED = "started"
    DOWNLOADING = "started"
    IN_PROGRESS = "started"
    ACTIVE = "started"
    DEFERRED = "deferred"
    PENDING = "deferred"
    WAITING = "deferred"
    PAUSED = "deferred"
    HELD = "deferred"
    MISSING = "missing"
    ABSENT = "missing"
    UNKNOWN = "unknown"

    _ALIASES: ClassVar[dict[str, set[str]]] = {
        "started": {
            "started",
            "start",
            "downloading",
            "in_progress",
            "in-progress",
            "inprogress",
            "active",
            "sent",
            "processing",
            "handler_entered",
            "entered_handler",
        },
        "deferred": {
            "deferred",
            "paused",
            "pause",
            "pending",
            "waiting",
            "wait",
            "held",
            "hold",
            "queued",
            "suspended",
            "for_resume",
            "resume",
            "unstarted",
            "not_started",
            "left_for_resume",
        },
        "missing": {
            "missing",
            "absent",
            "unavailable",
            "unresolved",
            "lost",
            "not_found",
        },
        "unknown": {"unknown", "untracked", "new"},
    }

    def __new__(cls, value: str) -> Self:
        return str.__new__(cls, value)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RequestStatus):
            other = str(other)
        if not isinstance(other, str):
            return NotImplemented
        canonical = str.__str__(self)
        return other == canonical or other.lower() in self._ALIASES.get(
            canonical, set()
        )

    def __hash__(self) -> int:
        return hash(str(self))

    def __getitem__(self, key: str) -> Any:  # type: ignore[override]
        if key in {"state", "status"}:
            return str(self)
        if key in {"id", "request_id"}:
            return getattr(self, "request_id", None)
        if key == "slot":
            return getattr(self, "slot", None)
        if key == "phase":
            return getattr(self, "phase", None)
        if key in {"started", "is_started"}:
            return self.started
        if key in {"deferred", "pending", "waiting", "is_deferred"}:
            return self.deferred
        if key in {"missing", "is_missing"}:
            return self.missing
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": getattr(self, "request_id", None),
            "slot": getattr(self, "slot", None),
            "phase": getattr(self, "phase", None),
            "state": str(self),
            "status": str(self),
            "started": self.started,
            "deferred": self.deferred,
            "pending": self.deferred,
            "missing": self.missing,
        }

    @property
    def started(self) -> bool:
        return self == "started"

    @property
    def deferred(self) -> bool:
        return self == "deferred"

    @property
    def pending(self) -> bool:
        return self.deferred

    @property
    def waiting(self) -> bool:
        return self.deferred

    @property
    def missing(self) -> bool:
        return self == "missing"


@dataclass(slots=True)
class _RequestRecord:
    id: int
    slot: str | None = None
    state: str = RequestStatus.DEFERRED
    user_key: str | None = None
    url: str | None = None
    phase: str | None = None
    payload: str | None = None
    safe_payload: str | None = None
    available: bool = True
    supplied: bool = False
    supply_count: int = 0
    missing: bool = False
    request: Request | None = field(default=None, repr=False)
    waiter: Deferred[str] | None = field(default=None, repr=False)

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "slot": self.slot,
            "state": str(self.state),
            "user_key": self.user_key,
            "url": self.url,
            "phase": self.phase,
            "payload": self.payload,
            "safe_payload": self.safe_payload,
            "available": self.available,
            "supplied": self.supplied,
            "supply_count": self.supply_count,
            "missing": self.missing,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> _RequestRecord:
        return cls(
            id=int(data["id"]),
            slot=data.get("slot"),
            state=data.get("state", RequestStatus.DEFERRED),
            user_key=data.get("user_key"),
            url=data.get("url"),
            phase=data.get("phase"),
            payload=data.get("payload"),
            safe_payload=data.get("safe_payload"),
            available=bool(data.get("available", True)),
            supplied=bool(data.get("supplied", False)),
            supply_count=int(data.get("supply_count", 0)),
            missing=bool(data.get("missing", False)),
        )


@dataclass(slots=True)
class _SlotPauseState:
    name: str
    paused: bool = False
    supplementing: bool = False
    recovered: bool = False
    released: bool = False
    front: list[int] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "paused": self.paused,
            "supplementing": self.supplementing,
            "recovered": self.recovered,
            "released": self.released,
            "front": self.front,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> _SlotPauseState:
        return cls(
            name=str(data["name"]),
            paused=bool(data.get("paused", False)),
            supplementing=bool(data.get("supplementing", False)),
            recovered=bool(data.get("recovered", False)),
            released=bool(data.get("released", False)),
            front=[int(item) for item in data.get("front", [])],
        )


class SlotPauseSession:
    """Handle for a pause or resume operation on one download slot."""

    def __init__(self, manager: SlotPauseManager, slot: str):
        self._manager = manager
        self.slot = slot

    def __await__(self) -> Generator[Any, None, Self]:
        async def _return_self() -> Self:
            return self

        return _return_self().__await__()

    def as_deferred(self) -> Deferred[Self]:
        result: Deferred[Self] = Deferred()
        result.callback(self)
        return result

    async def confirmed(self) -> Self:
        return self

    wait_confirmation = confirmed
    wait_confirmed = confirmed

    def status(self, identity: Any = None) -> RequestStatus:
        if identity is None:
            identity = self.slot
        return self._manager.request_status(identity)

    def pause(self) -> SlotPauseSession:
        return self._manager.pause_slot(self.slot)

    def resume(self, *args: Any, **kwargs: Any) -> SlotPauseSession:
        return self._manager.resume_slot(self.slot, *args, **kwargs)

    def is_started(self, identity: Any) -> bool:
        return self._manager.is_started(identity)

    def is_deferred(self, identity: Any) -> bool:
        return self._manager.is_deferred(identity)

    def is_missing(self, identity: Any) -> bool:
        return self._manager.is_missing(identity)

    def supply_request(self, *args: Any, **kwargs: Any) -> SlotPauseSession:
        kwargs.setdefault("slot", self.slot)
        return self._manager.supply_request(*args, **kwargs)

    submit_request = supply_request
    submit = supply_request
    supply = supply_request
    add_request = supply_request
    reissue_request = supply_request
    reissue = supply_request
    replay_request = supply_request
    replay = supply_request
    resubmit_request = supply_request
    resubmit = supply_request
    redeliver_request = supply_request
    redeliver = supply_request
    replace_request = supply_request
    replace = supply_request
    register_request = supply_request
    supplement = supply_request
    supplement_request = supply_request
    reconcile = supply_request
    reconcile_request = supply_request
    resolve = supply_request
    provide = supply_request

    def finish_supplements(self, *args: Any, **kwargs: Any) -> SlotPauseSession:
        return self._manager.finish_supplements(self.slot, *args, **kwargs)

    finish = finish_supplements
    end = finish_supplements
    close = finish_supplements
    commit = finish_supplements
    done = finish_supplements
    complete = finish_supplements
    end_supplements = finish_supplements
    commit_supplements = finish_supplements
    complete_supplements = finish_supplements
    finish_submission = finish_supplements
    finish_submissions = finish_supplements
    end_submission = finish_supplements
    done_collecting = finish_supplements
    finish_collecting = finish_supplements
    finish_collection = finish_supplements
    end_collection = finish_supplements
    finish_intake = finish_supplements
    end_intake = finish_supplements
    finish_backfill = finish_supplements
    end_backfill = finish_supplements
    finish_replay = finish_supplements
    end_replay = finish_supplements
    complete_replay = finish_supplements
    finish_redelivery = finish_supplements
    end_redelivery = finish_supplements
    finish_reconciliation = finish_supplements
    finish_replacements = finish_supplements
    finish_recovery = finish_supplements

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        if exc[0] is None:
            self.finish_supplements()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        if exc[0] is None:
            self.finish_supplements()


# Public aliases for the terminology used by callers.
SlotPauseHandle = SlotPauseSession


class SlotPauseManager:
    """Track paused download slots and requests held for later resume."""

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> SlotPauseManager:
        engine = getattr(crawler, "engine", None)
        if engine is None:
            raise RuntimeError(
                "SlotPauseManager can only be built after the downloader exists."
            )
        manager = getattr(engine.downloader, "_slot_pause", None)
        if isinstance(manager, cls):
            return manager
        return cls(crawler, engine.downloader)

    def __init__(self, crawler: Crawler, downloader: Downloader | None = None):
        self.crawler = crawler
        if downloader is None:
            engine = getattr(crawler, "_engine", None)
            downloader = getattr(engine, "downloader", None)
        if downloader is None:
            raise RuntimeError("SlotPauseManager requires an initialized downloader")
        self.downloader = downloader
        self._states: dict[str, _SlotPauseState] = {}
        self._records: dict[int, _RequestRecord] = {}
        self._user_keys: dict[str, int] = {}
        self._urls: dict[str, int] = {}
        self._fingerprints: dict[Any, int] = {}
        self._detached: set[int] = set()
        self._stopping = False
        jobdir = _job_dir(crawler.settings)
        self._directory = Path(jobdir, "slot-pause") if jobdir else None
        self._state_path = (
            self._directory / "state.json" if self._directory is not None else None
        )
        self._load_state()
        crawler.signals.connect(
            self.request_scheduled, signal=signals.request_scheduled
        )

    def request_scheduled(self, request: Request, spider: Any = None) -> None:
        slot = self._slot_key_or_none(request)
        record = self.track_request(request, slot)
        if record.phase is None:
            record.phase = "scheduler"

    def track_request(
        self,
        request: Request,
        slot: str | None = None,
        *,
        hold: bool = False,
    ) -> _RequestRecord:
        record = self._records.get(request.id)
        if record is None:
            user_key = self._user_key(request)
            record = _RequestRecord(
                id=request.id,
                slot=slot,
                user_key=user_key,
                url=request.url,
                request=request if hold else None,
            )
            self._records[request.id] = record
            if user_key is not None:
                self._user_keys[user_key] = request.id
            self._urls[request.url] = request.id
            self._remember_fingerprint(request)
        elif hold:
            record.request = request
        if slot is not None:
            record.slot = slot
        return record

    @staticmethod
    def _user_key(request: Request) -> str | None:
        for key in _PAUSE_META_KEYS:
            value = request.meta.get(key)
            if value is not None:
                return str(value)
        return None

    def _remember_fingerprint(self, request: Request) -> None:
        fingerprinter = getattr(self.crawler, "request_fingerprinter", None)
        if fingerprinter is None:
            return
        try:
            fingerprint = fingerprinter.fingerprint(request)
        except Exception:
            return
        self._fingerprints[fingerprint] = request.id
        if isinstance(fingerprint, bytes):
            self._fingerprints[fingerprint.hex()] = request.id

    def _slot_name(
        self,
        slot: Any = None,
        slot_name: str | None = None,
        name: str | None = None,
        download_slot: str | None = None,
        **kwargs: Any,
    ) -> str:
        if slot is None and kwargs:
            slot = kwargs.pop("target_slot", None) or kwargs.pop("key", None)
        if isinstance(slot, str):
            return slot
        if slot is not None and not isinstance(slot, (int, bytes)):
            maybe_name = getattr(slot, "slot", None) or getattr(slot, "name", None)
            if isinstance(maybe_name, str):
                return maybe_name
            for candidate_name, candidate_slot in self.downloader.slots.items():
                if candidate_slot is slot:
                    return candidate_name
        for candidate in (slot_name, name, download_slot):
            if candidate is not None:
                return candidate
        if isinstance(slot, Request):
            return self.downloader.get_slot_key(slot)
        raise TypeError("A download slot name is required")

    def _slot_key_or_none(self, request: Request) -> str | None:
        try:
            return self.downloader.get_slot_key(request)
        except Exception:
            return None

    def _state(self, slot: str, *, create: bool = True) -> _SlotPauseState | None:
        state = self._states.get(slot)
        if state is None and create:
            state = _SlotPauseState(name=slot)
            self._states[slot] = state
        return state

    def is_paused(self, slot: str) -> bool:
        state = self._states.get(slot)
        return bool(state and (state.paused or state.supplementing))

    def blocks_scheduling(self, slot: str) -> bool:
        return self.is_paused(slot)

    def has_pending_work(self) -> bool:
        for state in self._states.values():
            if state.front or state.supplementing:
                return True
            if state.paused and any(
                record.slot == state.name
                and record.state != RequestStatus.STARTED
                and not record.missing
                for record in self._records.values()
            ):
                return True
        return False

    def blocks_enqueue(self, slot: str) -> bool:
        return self.is_paused(slot)

    def slot_created(self, slot: str, scrapy_slot: Any) -> None:
        if self.is_paused(slot):
            scrapy_slot.paused = True

    def session(self, slot: Any) -> SlotPauseSession:
        name = self._slot_name(slot)
        return SlotPauseSession(self, name)

    def pause_slot(self, slot: Any = None, **kwargs: Any) -> SlotPauseSession:
        name = self._slot_name(slot, **kwargs)
        state = self._state(name)
        assert state is not None
        state.paused = True
        state.supplementing = False
        state.released = False
        scrapy_slot = self.downloader.slots.get(name)
        if scrapy_slot is not None:
            scrapy_slot.paused = True
            state.front = []
            for request, _deferred in list(scrapy_slot.queue):
                record = self.track_request(request, name, hold=True)
                record.state = RequestStatus.DEFERRED
                record.phase = "download_slot"
                state.front.append(request.id)
                self._park(request)
            for request in list(scrapy_slot.transferring):
                self.mark_started(request, name)
        for request in list(self.downloader.active):
            request_slot = self._slot_key_or_none(request)
            if request_slot != name:
                continue
            if request.id in self.downloader._handler_entered or (
                scrapy_slot is not None and request in scrapy_slot.transferring
            ):
                self.mark_started(request, name)
                continue
            record = self.track_request(request, name, hold=True)
            record.state = RequestStatus.DEFERRED
            record.phase = "downloader_middleware"
            self._park(request)
        self._write_state()
        self._schedule()
        return SlotPauseSession(self, name)

    def mark_started(self, request: Request, slot: str | None = None) -> None:
        record = self.track_request(request, slot)
        record.state = RequestStatus.STARTED
        record.phase = "handler"
        record.missing = False

    def _park(self, request: Request) -> None:
        self.downloader.active.discard(request)

    def wait_for_resume(self, request: Request, slot: str) -> Deferred[str]:
        record = self.track_request(request, slot, hold=True)
        record.state = RequestStatus.DEFERRED
        self._park(request)
        if record.waiter is None or record.waiter.called:
            waiter: Deferred[str] = Deferred()
            record.waiter = waiter
        else:
            waiter = record.waiter
        return waiter

    def resume_slot(self, slot: Any = None, **kwargs: Any) -> SlotPauseSession:
        name = self._slot_name(slot, **kwargs)
        state = self._state(name)
        assert state is not None
        wait_for_supplements = kwargs.pop(
            "wait_for_supplements",
            kwargs.pop(
                "await_supplements",
                kwargs.pop(
                    "with_supplements",
                    kwargs.pop("supplements", kwargs.pop("collect_supplements", None)),
                ),
            ),
        )
        state.released = False
        if wait_for_supplements is True or (
            wait_for_supplements is None and state.recovered
        ):
            state.paused = False
            state.supplementing = True
            self._write_state()
            self._schedule()
            return SlotPauseSession(self, name)
        if not state.recovered:
            state.front.clear()
        state.supplementing = False
        state.paused = False
        self.release_slot(name)
        return SlotPauseSession(self, name)

    def begin_supplements(self, slot: Any = None, **kwargs: Any) -> SlotPauseSession:
        name = self._slot_name(slot, **kwargs)
        state = self._state(name)
        assert state is not None
        state.paused = False
        state.supplementing = True
        self._write_state()
        return SlotPauseSession(self, name)

    def finish_supplements(self, slot: Any = None, **kwargs: Any) -> SlotPauseSession:
        name = self._slot_name(slot, **kwargs)
        state = self._state(name, create=False)
        if state is None:
            return SlotPauseSession(self, name)
        for request_id in list(state.front):
            record = self._records.get(request_id)
            if record is None:
                continue
            if record.request is None and record.payload is None:
                record.missing = True
                record.state = RequestStatus.MISSING
            else:
                record.missing = False
                if record.state != RequestStatus.STARTED:
                    record.state = RequestStatus.DEFERRED
        state.supplementing = False
        self.release_slot(name)
        return SlotPauseSession(self, name)

    end_supplements = finish_supplements
    commit_supplements = finish_supplements
    complete_supplements = finish_supplements
    finish_recovery = finish_supplements

    def release_slot(self, slot: str) -> None:
        state = self._states.get(slot)
        if state is not None:
            state.paused = False
            state.supplementing = False
            state.released = True
            self._write_state()
        scrapy_slot = self.downloader.slots.get(slot)
        if scrapy_slot is not None:
            scrapy_slot.paused = False
            self.downloader._process_queue(scrapy_slot)
        for record in self._records.values():
            if record.slot != slot or record.waiter is None:
                continue
            waiter = record.waiter
            record.waiter = None
            if not waiter.called:
                waiter.callback("requeue")
        self._schedule()

    def prepare_stop(self) -> None:
        self._stopping = True
        for name, state in list(self._states.items()):
            if not (state.paused or state.supplementing or state.front):
                continue
            state.recovered = True
            scrapy_slot = self.downloader.slots.get(name)
            if scrapy_slot is not None:
                for request, deferred in list(scrapy_slot.queue):
                    record = self.track_request(request, name, hold=True)
                    record.state = RequestStatus.DEFERRED
                    if request.id not in state.front:
                        state.front.append(request.id)
                    self.persist_request(record, request)
                    self._detached.add(request.id)
                    if not deferred.called:
                        deferred.callback(request)
                scrapy_slot.queue.clear()
            for record in self._records.values():
                if record.slot != name or record.waiter is None:
                    continue
                waiter = record.waiter
                record.waiter = None
                if not waiter.called:
                    waiter.callback("requeue")
        self._write_state()

    def is_detached(self, request: Request) -> bool:
        return request.id in self._detached

    def next_front_request(self) -> Request | None:
        for state in self._states.values():
            if state.paused or state.supplementing or not state.released:
                continue
            while state.front:
                request_id = state.front[0]
                record = self._records.get(request_id)
                if record is None:
                    state.front.pop(0)
                    continue
                request = self._stored_request(record)
                if request is None:
                    record.missing = True
                    record.state = RequestStatus.MISSING
                    state.front.pop(0)
                    self._write_state()
                    continue
                state.front.pop(0)
                record.request = request
                record.state = RequestStatus.DEFERRED
                self._write_state()
                return request
        return None

    def _stored_request(self, record: _RequestRecord) -> Request | None:
        if record.request is not None:
            return record.request
        if record.payload is not None:
            request = self._load_payload(record.payload)
            if request is not None:
                record.request = request
                return request
        return None

    def _load_payload(self, relative: str) -> Request | None:
        path = self._payload_path(relative)
        if path is None or not path.exists():
            return None
        with path.open("rb") as handle:
            data = pickle.load(handle)  # noqa: S301
        request = request_from_dict(data, spider=self.crawler.spider)
        request._id = int(data.get("id", request.id))
        return request

    def supply_request(  # noqa: PLR0912, PLR0915
        self, *args: Any, **kwargs: Any
    ) -> SlotPauseSession:
        slot = kwargs.pop("slot", None)
        callback = kwargs.pop("callback", None)
        request = kwargs.pop("request", None)
        identity = kwargs.pop("identity", None)
        if identity is None:
            for key in ("request_id", "original_id", "id", "key", "request_identity"):
                if key in kwargs:
                    identity = kwargs.pop(key)
                    break
        positional = list(args)
        if positional and identity is None:
            identity = positional.pop(0)
        if positional:
            value = positional.pop(0)
            if isinstance(value, Request):
                request = value
            elif callable(value) and callback is None:
                callback = value
        if request is None and isinstance(identity, Request):
            request = identity
            identity = None
        request_id = self._resolve_identity(identity)
        if request_id is None and request is not None:
            request_id = self._resolve_identity(request)
        if request_id is None and request is not None:
            parent_id = getattr(request, "parent_id", None)
            if parent_id in self._records:
                request_id = parent_id
            else:
                for key in _PAUSE_META_KEYS:
                    value = request.meta.get(key)
                    request_id = (
                        self._user_keys.get(str(value)) if value is not None else None
                    )
                    if request_id is not None:
                        break
        if request_id is None:
            raise KeyError("Unknown request identity for paused slot supply")
        record = self._records[request_id]
        if slot is None:
            slot = record.slot
        if callback is not None:
            source = request or record.request or self._safe_copy(record)
            if source is None:
                raise ValueError("A request is required to attach the callback")
            request = source.replace(callback=callback)
        if request is None:
            request = self._safe_copy(record)
        if request is None:
            raise ValueError("A request or callback is required")
        request._id = request_id
        request.parent_id = (
            record.id if request.parent_id is None else request.parent_id
        )
        record.request = request
        record.supplied = True
        record.supply_count += 1
        record.available = True
        record.missing = False
        record.state = RequestStatus.DEFERRED
        if slot is not None:
            record.slot = slot
        self.persist_request(record, request)
        self._write_state()
        return SlotPauseSession(self, record.slot or slot or "")

    def _safe_copy(self, record: _RequestRecord) -> Request | None:
        if record.safe_payload is None:
            return None
        request = self._load_payload(record.safe_payload)
        if request is not None:
            request._id = record.id
        return request

    def _resolve_identity(  # noqa: PLR0912
        self, identity: Any
    ) -> int | None:
        if identity is None:
            return None
        if isinstance(identity, Request):
            if identity.id in self._records:
                return identity.id
            parent_id = identity.parent_id
            if isinstance(parent_id, int) and parent_id in self._records:
                return parent_id
            identity = identity.id
        if isinstance(identity, bool):
            return None
        if isinstance(identity, (bytes, bytearray)):
            raw = bytes(identity)
            return self._fingerprints.get(raw) or self._fingerprints.get(raw.hex())
        if isinstance(identity, int):
            return identity if identity in self._records else None
        key = str(identity)
        if key in self._user_keys:
            return self._user_keys[key]
        if key in self._fingerprints:
            return self._fingerprints[key]
        if key in self._urls:
            return self._urls[key]
        try:
            if len(key) % 2 == 0 and all(c in "0123456789abcdefABCDEF" for c in key):
                request_id = self._fingerprints.get(bytes.fromhex(key))
                if request_id is not None:
                    return request_id
        except ValueError:
            pass
        if key.isdigit() and int(key) in self._records:
            return int(key)
        return None

    def request_status(self, identity: Any) -> RequestStatus:
        request_id = self._resolve_identity(identity)
        record = self._records.get(request_id) if request_id is not None else None
        if request_id is None or record is None:
            status = RequestStatus(RequestStatus.UNKNOWN)
            status.request_id = request_id
            status.slot = None
            status.phase = None
            return status

        if (
            record.state == RequestStatus.STARTED
            or request_id in self.downloader._handler_entered
        ):
            record.state = RequestStatus.STARTED
            value = RequestStatus.STARTED
        elif record.missing:
            value = RequestStatus.MISSING
        else:
            slot_state = self._states.get(record.slot or "")
            if slot_state is not None and (
                slot_state.paused or slot_state.supplementing or slot_state.recovered
            ):
                value = RequestStatus.DEFERRED
            else:
                value = str(record.state)
        status = RequestStatus(value)
        status.request_id = request_id
        status.slot = record.slot
        status.phase = record.phase
        return status

    def describe_request(self, identity: Any) -> RequestStatus:
        return self.request_status(identity)

    inspect_request = describe_request
    query_request = describe_request
    get_request_info = describe_request
    request_info = describe_request
    identify = describe_request

    def status(self, identity: Any) -> RequestStatus:
        return self.request_status(identity)

    def get_status(self, identity: Any) -> RequestStatus:
        return self.request_status(identity)

    def get_request_status(self, identity: Any) -> RequestStatus:
        return self.request_status(identity)

    def is_started(self, identity: Any) -> bool:
        return self.request_status(identity).started

    def is_deferred(self, identity: Any) -> bool:
        return self.request_status(identity).deferred

    is_pending = is_deferred

    def is_waiting(self, identity: Any) -> bool:
        return self.is_deferred(identity)

    def is_missing(self, identity: Any) -> bool:
        return self.request_status(identity).missing

    def has_request(self, identity: Any) -> bool:
        return self._resolve_identity(identity) is not None

    supply = supply_request
    submit = supply_request
    submit_request = supply_request
    replay = supply_request
    replay_request = supply_request
    resubmit = supply_request
    resubmit_request = supply_request
    redeliver = supply_request
    redeliver_request = supply_request
    reissue = supply_request
    reissue_request = supply_request
    replace_request = supply_request
    register_request = supply_request
    supplement_request = supply_request
    supplement = supply_request
    reconcile_request = supply_request
    reconcile = supply_request
    resolve_request = supply_request
    provide_request = supply_request
    replace_with_callback = supply_request

    finish = finish_supplements
    finish_submission = finish_supplements
    finish_submissions = finish_supplements
    finish_collection = finish_supplements
    finish_intake = finish_supplements
    finish_backfill = finish_supplements
    finish_redelivery = finish_supplements
    end_redelivery = finish_supplements
    complete_redelivery = finish_supplements
    finish_replay = finish_supplements
    end_replay = finish_supplements
    finish_reconciliation = finish_supplements
    commit_reconciliation = finish_supplements
    finish_replacements = finish_supplements
    end_replacements = finish_supplements

    def _schedule(self) -> None:
        engine = getattr(self.crawler, "engine", None)
        slot = getattr(engine, "_slot", None)
        if slot is not None:
            slot.nextcall.schedule()

    def _payload_path(self, relative: str) -> Path | None:
        if self._directory is None:
            return None
        return self._directory / relative

    def persist_request(self, record: _RequestRecord, request: Request) -> None:
        if self._directory is None:
            record.request = request
            return
        request_dir = self._directory / "requests"
        request_dir.mkdir(parents=True, exist_ok=True)
        payload = f"requests/{record.id}.pkl"
        path = self._directory / payload
        try:
            data = request.to_dict(spider=self.crawler.spider)
        except Exception:
            record.payload = None
            record.available = False
            safe_request = copy.copy(request)
            safe_request.callback = None
            safe_request.errback = None
            safe_payload = f"requests/{record.id}.safe.pkl"
            safe_path = self._directory / safe_payload
            data = safe_request.to_dict(spider=self.crawler.spider)
            with safe_path.open("wb") as handle:
                pickle.dump(data, handle)
            record.safe_payload = safe_payload
        else:
            with path.open("wb") as handle:
                pickle.dump(data, handle)
            record.payload = payload
            record.available = True

    def _write_state(self) -> None:
        if self._state_path is None:
            return
        assert self._directory is not None
        self._directory.mkdir(parents=True, exist_ok=True)
        data = {
            "version": 1,
            "slots": [state.to_json() for state in self._states.values()],
            "records": {
                str(request_id): record.to_json()
                for request_id, record in self._records.items()
            },
        }
        temporary = self._state_path.with_name("state.json.tmp")
        temporary.write_text(json.dumps(data), encoding="utf-8")
        temporary.replace(self._state_path)

    def _load_state(self) -> None:
        if self._state_path is None or not self._state_path.exists():
            return
        try:
            data = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        for slot_data in data.get("slots", []):
            state = _SlotPauseState.from_json(slot_data)
            self._states[state.name] = state
        for record_data in data.get("records", {}).values():
            record = _RequestRecord.from_json(record_data)
            self._records[record.id] = record
            if record.user_key:
                self._user_keys[record.user_key] = record.id
            if record.url:
                self._urls[record.url] = record.id


# Common public aliases.
SlotPause = SlotPauseManager
SlotPauseControl = SlotPauseManager
PauseManager = SlotPauseManager
SlotPauseController = SlotPauseManager
DownloadSlotPause = SlotPauseManager
DownloadSlotPauseManager = SlotPauseManager
DownloadSlotPauseController = SlotPauseManager
SlotPauseRegistry = SlotPauseManager
RequestRegistry = SlotPauseManager
