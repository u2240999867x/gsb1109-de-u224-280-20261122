from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

WAREHOUSE_TASK_ID = "warehouse_ingest"
LEGACY_INGEST = "legacy_ingest"
THREADPOOL = "threadpool"
MANUAL_REVIEW = "manual_review"
AWAITING_MANUAL_VERIFICATION = "awaiting_manual_verification"
DEFAULT_RETENTION = timedelta(days=30)
DEFAULT_LEASE = timedelta(minutes=10)


class Protocol(StrEnum):
    V1 = "v1"
    V2 = "v2"


class JobStatus(StrEnum):
    QUEUED = "queued"
    ACQUIRED = "acquired"
    COMPLETED = "completed"
    MANUAL_REVIEW = MANUAL_REVIEW


class RoutingStatus(StrEnum):
    READY = "ready"
    MANUAL_REVIEW = MANUAL_REVIEW


PROTOCOL_V1 = Protocol.V1.value
PROTOCOL_V2 = Protocol.V2.value


def normalize_protocol(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Protocol):
        return value.value

    text = str(value).strip().lower().replace("-", "_")
    if text in {"1", "v1", "legacy", "cents", "minor"}:
        return PROTOCOL_V1
    if text in {"2", "v2", "new", "euros", "major"}:
        return PROTOCOL_V2
    if text in {PROTOCOL_V1, PROTOCOL_V2}:
        return text
    return None


def money_receipt_amount(amount: int, protocol: str | Protocol) -> str:
    version = normalize_protocol(protocol)
    if version == PROTOCOL_V1:
        value = Decimal(amount) / Decimal(100)
    elif version == PROTOCOL_V2:
        value = Decimal(amount)
    else:
        raise ValueError(f"unsupported protocol: {protocol!r}")

    return f"{value:.2f}"


def warehouse_ingest(
    batch_number: str | int,
    amount: int = 1250,
    protocol: Any = PROTOCOL_V1,
    *,
    version: Any = None,
    job_id: str | None = None,
    worker_id: str | None = None,
) -> dict[str, Any]:
    """Convert one batch using the selected supplier protocol."""
    resolved = (
        normalize_protocol(version)
        if version is not None
        else normalize_protocol(protocol)
    )
    if resolved is None:
        resolved = PROTOCOL_V1

    displayed = money_receipt_amount(amount, resolved)
    receipt = WarehouseReceipt(
        job_id=str(job_id or uuid.uuid4()),
        batch_number=str(batch_number),
        input_amount=int(amount),
        protocol_used=resolved,
        amount=displayed,
        worker_id=worker_id,
    )
    return receipt.to_dict()


ingest = warehouse_ingest


@dataclass(frozen=True)
class WarehouseReceipt:
    job_id: str
    batch_number: str
    input_amount: int
    protocol_used: str
    amount: str
    currency: str = "EUR"
    status: str = "completed"
    worker_id: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    expires_at: str | None = None

    @property
    def protocol(self) -> str:
        return self.protocol_used

    @property
    def actual_protocol(self) -> str:
        return self.protocol_used

    @property
    def amount_value(self) -> str:
        return self.amount

    @property
    def amount_display(self) -> str:
        return self.amount

    @property
    def display_amount(self) -> str:
        return self.amount

    @property
    def receipt_text(self) -> str:
        return f"{self.amount} 欧元"

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["protocol"] = self.protocol_used
        data["actual_protocol"] = self.protocol_used
        data["amount_display"] = self.amount
        data["display_amount"] = self.amount
        data["amount_decimal"] = float(self.amount)
        data["amount_numeric"] = float(self.amount)
        data["receipt_text"] = self.receipt_text
        data["localized_receipt"] = self.receipt_text
        data["euro_display"] = f"€{self.amount}"
        return data


@dataclass
class ReviewHold:
    job_id: str
    reason: str
    original_args: tuple[Any, ...] = ()
    original_kwargs: dict[str, Any] = field(default_factory=dict)
    labels: tuple[str, ...] = ()
    task_id: str = WAREHOUSE_TASK_ID
    executor: str = THREADPOOL
    status: str = AWAITING_MANUAL_VERIFICATION
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "state": AWAITING_MANUAL_VERIFICATION,
            "reason": self.reason,
            "task_id": self.task_id,
            "executor": self.executor,
            "labels": list(self.labels),
            "original_args": list(self.original_args),
            "original_kwargs": dict(self.original_kwargs),
            "params": {
                "args": list(self.original_args),
                "kwargs": dict(self.original_kwargs),
            },
            "created_at": self.created_at,
        }


@dataclass
class JobRecord:
    job_id: str
    batch_number: str
    amount: int = 1250
    task_id: str = WAREHOUSE_TASK_ID
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    labels: tuple[str, ...] = ()
    executor: str = THREADPOOL
    source: str = "new_submitter"
    db_role: str | None = None
    requested_protocol: str | None = None
    protocol: str | None = None
    routing_status: str = RoutingStatus.READY.value
    classification_reason: str | None = None
    status: str = JobStatus.QUEUED.value
    sequence: int = 0
    schedule_id: str | None = None
    acquired_by: str | None = None
    acquired_until: datetime | None = None
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    default_protocol_version: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "id": self.job_id,
            "batch_number": self.batch_number,
            "amount": self.amount,
            "task_id": self.task_id,
            "args": list(self.args),
            "kwargs": dict(self.kwargs),
            "labels": list(self.labels),
            "executor": self.executor,
            "source": self.source,
            "db_role": self.db_role,
            "requested_protocol": self.requested_protocol,
            "protocol": self.protocol,
            "actual_protocol": self.protocol,
            "routing_status": self.routing_status,
            "classification_reason": self.classification_reason,
            "status": self.status,
            "sequence": self.sequence,
            "schedule_id": self.schedule_id,
            "acquired_by": self.acquired_by,
            "acquired_until": (
                self.acquired_until.isoformat() if self.acquired_until else None
            ),
            "created_at": self.created_at,
            "default_protocol_version": self.default_protocol_version,
        }


def _labels_set(labels: Iterable[Any] | None) -> set[str]:
    values: set[str] = set()
    for label in labels or ():
        if label is None:
            continue
        values.add(str(label).strip().lower().replace("-", "_"))
    return values


def classify_job(
    *,
    protocol: Any = None,
    api_version: Any = None,
    version: Any = None,
    metadata: dict[str, Any] | None = None,
    job_id: str | None = None,
    task_id: str | None = None,
    labels: Iterable[Any] | None = None,
    executor: str | None = None,
    source: str | None = None,
    db_role: str | None = None,
    old_db_roles: Iterable[str] = ("warehouse_old", "aps_old", "old_worker"),
    default_protocol: str = PROTOCOL_V1,
) -> tuple[str | None, str, str]:
    """Return ``(protocol, routing_status, reason)`` for a queued row."""
    metadata = metadata or {}
    label_set = _labels_set(labels)
    label_set.update(_labels_set(metadata.get("labels")))
    task_text = " ".join(str(value or "") for value in (task_id, job_id)).lower()
    source_text = (source or "").lower()
    role_set = set(old_db_roles)

    explicit = normalize_protocol(
        protocol
        if protocol is not None
        else api_version
        if api_version is not None
        else version
        if version is not None
        else metadata.get("warehouse_protocol")
        or metadata.get("protocol")
        or metadata.get("api_version")
    )
    if explicit is not None:
        return explicit, RoutingStatus.READY.value, "explicit protocol"

    if MANUAL_REVIEW in label_set or "quarantined" in label_set:
        return None, RoutingStatus.MANUAL_REVIEW.value, "marked for manual review"

    if (
        LEGACY_INGEST in label_set
        or LEGACY_INGEST in task_text
        or source_text in {LEGACY_INGEST, "legacy_job", "historical_legacy"}
    ):
        return PROTOCOL_V1, RoutingStatus.READY.value, "legacy_ingest is v1-only"

    if db_role in role_set or source_text in {
        "old_add_job",
        "old_schedule",
        "old_process",
        "legacy_add_job",
        "legacy_schedule",
    }:
        return PROTOCOL_V1, RoutingStatus.READY.value, "unchanged v1 process entry"

    if THREADPOOL in label_set or executor == THREADPOOL or THREADPOOL in task_text:
        if len(label_set - {THREADPOOL}) == 0:
            return (
                None,
                RoutingStatus.MANUAL_REVIEW.value,
                "threadpool was used by both v1 and v2 and business params are ambiguous",
            )

    resolved_default = normalize_protocol(default_protocol) or PROTOCOL_V1
    return resolved_default, RoutingStatus.READY.value, "submitter default protocol"


def extract_batch_amount(
    args: Iterable[Any] | None = None,
    kwargs: dict[str, Any] | None = None,
    *,
    batch_number: str | int | None = None,
    amount: int | None = None,
) -> tuple[str, int]:
    args = tuple(args or ())
    kwargs = dict(kwargs or {})

    if batch_number is None:
        batch_number = kwargs.get("batch_number", kwargs.get("batch"))
    if amount is None:
        amount = kwargs.get("amount", kwargs.get("amount_integer", 1250))

    if batch_number is None and args:
        batch_number = args[0]
    if amount is None and len(args) > 1:
        amount = args[1]

    if batch_number is None:
        raise ValueError("batch_number is required")
    if amount is None:
        amount = 1250

    return str(batch_number), int(amount)


class RolloutConfig:
    def __init__(self, default_protocol: str = PROTOCOL_V2) -> None:
        self._lock = threading.RLock()
        self._default_protocol = normalize_protocol(default_protocol) or PROTOCOL_V2
        self._version = 1
        self.history: list[dict[str, Any]] = [
            {
                "version": self._version,
                "default_protocol": self._default_protocol,
                "committed_at": datetime.now(UTC).isoformat(),
            }
        ]

    @property
    def default_protocol(self) -> str:
        with self._lock:
            return self._default_protocol

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def commit_default_protocol(self, protocol: str | Protocol) -> int:
        resolved = normalize_protocol(protocol)
        if resolved is None:
            raise ValueError(f"unknown protocol: {protocol!r}")

        with self._lock:
            self._default_protocol = resolved
            self._version += 1
            self.history.append(
                {
                    "version": self._version,
                    "default_protocol": resolved,
                    "committed_at": datetime.now(UTC).isoformat(),
                }
            )
            return self._version


class EnqueueGate:
    """Block an unlabeled submission until a default change is committed."""

    def __init__(self) -> None:
        self.waiting = threading.Event()
        self.proceed = threading.Event()
        self.enqueued = threading.Event()

    def enter(self) -> None:
        self.waiting.set()
        if not self.proceed.wait(timeout=10):
            raise TimeoutError("timed out waiting for default protocol commit")

    def release_after_commit(self, config: RolloutConfig, protocol: str) -> int:
        version = config.commit_default_protocol(protocol)
        self.proceed.set()
        return version

    def wait_until_waiting(self, timeout: float = 5) -> None:
        if not self.waiting.wait(timeout):
            raise TimeoutError("submission did not reach the pre-enqueue gate")


class WarehouseStore:
    """Thread-safe durable-looking store used by the rollout harness."""

    def __init__(self, retention: timedelta = DEFAULT_RETENTION) -> None:
        self._lock = threading.RLock()
        self.retention = retention
        self._sequence = 0
        self.jobs: dict[str, JobRecord] = {}
        self.receipts: dict[str, WarehouseReceipt] = {}
        self.review_holds: dict[str, ReviewHold] = {}
        self.writes: list[dict[str, Any]] = []
        self.max_active_writes = 0
        self._active_writes = 0

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    def enqueue(
        self,
        *,
        job_id: str | None = None,
        batch_number: str | int | None = None,
        amount: int | None = None,
        args: Iterable[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        task_id: str = WAREHOUSE_TASK_ID,
        labels: Iterable[Any] | None = None,
        executor: str = THREADPOOL,
        source: str = "new_submitter",
        db_role: str | None = None,
        protocol: Any = None,
        metadata: dict[str, Any] | None = None,
        default_protocol: str = PROTOCOL_V1,
        default_protocol_version: int | None = None,
        schedule_id: str | None = None,
    ) -> JobRecord:
        original_args = tuple(args or ())
        original_kwargs = dict(kwargs or {})
        batch, amount_value = extract_batch_amount(
            original_args,
            original_kwargs,
            batch_number=batch_number,
            amount=amount,
        )
        resolved_job_id = str(job_id or uuid.uuid4())
        label_tuple = tuple(str(label) for label in (labels or ()))
        resolved_protocol, routing_status, reason = classify_job(
            protocol=protocol,
            metadata=metadata,
            job_id=resolved_job_id,
            task_id=task_id,
            labels=label_tuple,
            executor=executor,
            source=source,
            db_role=db_role,
            default_protocol=default_protocol,
        )

        with self._lock:
            record = JobRecord(
                job_id=resolved_job_id,
                batch_number=batch,
                amount=amount_value,
                task_id=task_id,
                args=original_args or (batch, amount_value),
                kwargs=original_kwargs,
                labels=label_tuple,
                executor=executor,
                source=source,
                db_role=db_role,
                requested_protocol=normalize_protocol(protocol)
                if protocol is not None
                else None,
                protocol=resolved_protocol,
                routing_status=routing_status,
                classification_reason=reason,
                status=(
                    JobStatus.MANUAL_REVIEW.value
                    if routing_status == RoutingStatus.MANUAL_REVIEW.value
                    else JobStatus.QUEUED.value
                ),
                sequence=self._next_sequence(),
                schedule_id=schedule_id,
                default_protocol_version=default_protocol_version,
            )
            self.jobs[resolved_job_id] = record
            if routing_status == RoutingStatus.MANUAL_REVIEW.value:
                self.review_holds[resolved_job_id] = ReviewHold(
                    job_id=resolved_job_id,
                    reason=reason,
                    original_args=record.args,
                    original_kwargs=record.kwargs,
                    labels=label_tuple,
                    task_id=task_id,
                    executor=executor,
                )
            return record

    def seed_acquired(
        self,
        *,
        worker_id: str,
        lease: timedelta = DEFAULT_LEASE,
        **enqueue_kwargs: Any,
    ) -> JobRecord:
        record = self.enqueue(**enqueue_kwargs)
        now = datetime.now(UTC)
        with self._lock:
            record.status = JobStatus.ACQUIRED.value
            record.acquired_by = worker_id
            record.acquired_until = now + lease
        return record

    def has_active_warehouse_claim(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(UTC)
        with self._lock:
            for job in self.jobs.values():
                if job.status != JobStatus.ACQUIRED.value:
                    continue
                if job.task_id != WAREHOUSE_TASK_ID:
                    continue
                if job.acquired_until is None or job.acquired_until >= current:
                    return True
        return False

    def claim_next(
        self,
        worker_id: str,
        protocols: Iterable[str | Protocol],
        *,
        lease: timedelta = DEFAULT_LEASE,
        now: datetime | None = None,
    ) -> JobRecord | None:
        allowed = {normalize_protocol(protocol) for protocol in protocols}
        current = now or datetime.now(UTC)
        with self._lock:
            if self.has_active_warehouse_claim(current):
                return None

            candidates = [
                job
                for job in self.jobs.values()
                if job.status == JobStatus.QUEUED.value
                and job.routing_status == RoutingStatus.READY.value
                and job.protocol in allowed
                and (job.acquired_until is None or job.acquired_until < current)
            ]
            if not candidates:
                return None

            job = min(candidates, key=lambda item: item.sequence)
            job.status = JobStatus.ACQUIRED.value
            job.acquired_by = worker_id
            job.acquired_until = current + lease
            return job

    def extend_lease(
        self, job_id: str, worker_id: str, lease: timedelta = DEFAULT_LEASE
    ) -> None:
        with self._lock:
            job = self.jobs[job_id]
            if job.acquired_by != worker_id:
                raise PermissionError(f"job {job_id} is not owned by {worker_id}")
            job.acquired_until = datetime.now(UTC) + lease

    def begin_write(self) -> None:
        with self._lock:
            self._active_writes += 1
            self.max_active_writes = max(self.max_active_writes, self._active_writes)
            if self._active_writes > 1:
                raise RuntimeError("concurrent warehouse writes detected")

    def end_write(self) -> None:
        with self._lock:
            self._active_writes -= 1

    def complete(
        self,
        job: JobRecord,
        receipt: WarehouseReceipt,
        *,
        worker_id: str,
    ) -> WarehouseReceipt:
        with self._lock:
            stored = self.jobs[job.job_id]
            if stored.acquired_by != worker_id:
                raise PermissionError(
                    f"job {job.job_id} is not acquired by {worker_id}"
                )
            stored.status = JobStatus.COMPLETED.value
            stored.acquired_until = None
            expires = datetime.now(UTC) + self.retention
            full_receipt = WarehouseReceipt(
                **{**receipt.__dict__, "expires_at": expires.isoformat()}
            )
            self.receipts[job.job_id] = full_receipt
            self.writes.append(
                {
                    "job_id": job.job_id,
                    "batch_number": job.batch_number,
                    "protocol": full_receipt.protocol_used,
                    "amount": full_receipt.amount,
                    "worker_id": worker_id,
                    "written_at": datetime.now(UTC).isoformat(),
                }
            )
            return full_receipt

    def get_receipt(self, job_id: str) -> WarehouseReceipt | None:
        with self._lock:
            receipt = self.receipts.get(str(job_id))
            if receipt is None:
                return None
            if receipt.expires_at:
                expires = datetime.fromisoformat(receipt.expires_at)
                if expires < datetime.now(UTC):
                    return None
            return receipt

    def get_job_result(self, job_id: str, *, wait: bool = False) -> WarehouseReceipt:
        receipt = self.get_receipt(job_id)
        if receipt is None:
            if str(job_id) in self.review_holds:
                raise LookupError(f"job {job_id} is held for manual verification")
            raise KeyError(job_id)
        return receipt

    def get_review_status(self, job_id: str) -> ReviewHold:
        return self.review_holds[str(job_id)]

    def summary(self) -> dict[str, Any]:
        with self._lock:
            return {
                "jobs": {key: job.to_dict() for key, job in self.jobs.items()},
                "receipts": {
                    key: receipt.to_dict() for key, receipt in self.receipts.items()
                },
                "manual_review": {
                    key: hold.to_dict() for key, hold in self.review_holds.items()
                },
                "writes": list(self.writes),
                "max_concurrent_writes": self.max_active_writes,
            }


class WarehouseGateway:
    """Supplier API gateway with separate v1 and v2 semantics."""

    def __init__(self, store: WarehouseStore) -> None:
        self.store = store

    def ingest(self, job: JobRecord, worker_id: str) -> WarehouseReceipt:
        protocol = normalize_protocol(job.protocol)
        if protocol not in {PROTOCOL_V1, PROTOCOL_V2}:
            raise RuntimeError(f"job {job.job_id} has no routable protocol")

        self.store.begin_write()
        try:
            displayed = money_receipt_amount(job.amount, protocol)
            return WarehouseReceipt(
                job_id=job.job_id,
                batch_number=job.batch_number,
                input_amount=job.amount,
                protocol_used=protocol,
                amount=displayed,
                worker_id=worker_id,
            )
        finally:
            self.store.end_write()


class Worker:
    def __init__(
        self,
        name: str,
        store: WarehouseStore,
        gateway: WarehouseGateway | None = None,
        protocols: Iterable[str | Protocol] = (PROTOCOL_V1,),
    ) -> None:
        self.name = name
        self.store = store
        self.gateway = gateway or WarehouseGateway(store)
        self.protocols = {normalize_protocol(p) for p in protocols}
        self.claimed: list[str] = []
        self.restart_count = 0

    def restart(self) -> Worker:
        replacement = Worker(
            self.name,
            self.store,
            self.gateway,
            protocols=self.protocols,
        )
        replacement.restart_count = self.restart_count + 1
        return replacement

    def claim_next(self) -> JobRecord | None:
        job = self.store.claim_next(self.name, self.protocols)
        if job is not None:
            self.claimed.append(job.job_id)
        return job

    def execute(
        self,
        job: JobRecord,
        *,
        before_write: Callable[[JobRecord], Any] | None = None,
    ) -> WarehouseReceipt:
        if before_write is not None:
            gate = before_write(job)
            if hasattr(gate, "wait"):
                gate.wait()

        provisional = self.gateway.ingest(job, self.name)
        return self.store.complete(job, provisional, worker_id=self.name)

    def process_next(
        self, *, before_write: Callable[[JobRecord], Any] | None = None
    ) -> WarehouseReceipt | None:
        job = self.claim_next()
        if job is None:
            return None
        return self.execute(job, before_write=before_write)


class LegacyV1Scheduler:
    """Stand-in for the unchanged old process calling ``add_job``."""

    def __init__(
        self,
        store: WarehouseStore,
        *,
        db_role: str = "warehouse_old",
        worker_id: str = "worker-a",
    ) -> None:
        self.store = store
        self.db_role = db_role
        self.worker_id = worker_id
        self.schedules: dict[str, dict[str, Any]] = {}

    def add_job(
        self,
        func_or_task_id: Any = WAREHOUSE_TASK_ID,
        *,
        args: Iterable[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        job_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        labels: Iterable[Any] | None = None,
        source: str = "old_add_job",
    ) -> str:
        task_id = (
            func_or_task_id
            if isinstance(func_or_task_id, str)
            else getattr(func_or_task_id, "__name__", WAREHOUSE_TASK_ID)
        )
        record = self.store.enqueue(
            job_id=job_id,
            args=args,
            kwargs=kwargs,
            task_id=task_id,
            labels=labels,
            source=source,
            db_role=self.db_role,
            metadata=metadata,
        )
        return record.job_id

    def add_v1_schedule(
        self,
        schedule_id: str,
        *,
        batch_number: str,
        amount: int = 1250,
        job_id: str | None = None,
    ) -> None:
        self.schedules[schedule_id] = {
            "batch_number": batch_number,
            "amount": amount,
            "job_id": job_id,
        }

    def trigger_schedule(self, schedule_id: str) -> str:
        schedule = self.schedules[schedule_id]
        return self.add_job(
            WAREHOUSE_TASK_ID,
            args=(schedule["batch_number"], schedule["amount"]),
            job_id=schedule.get("job_id"),
            source="old_schedule",
        )


class NewSubmitter:
    def __init__(
        self,
        store: WarehouseStore,
        config: RolloutConfig | None = None,
        *,
        db_role: str = "warehouse_new",
    ) -> None:
        self.store = store
        self.config = config or RolloutConfig(PROTOCOL_V2)
        self.db_role = db_role

    def submit(
        self,
        *,
        batch_number: str,
        amount: int = 1250,
        job_id: str | None = None,
        protocol: Any = None,
        labels: Iterable[Any] | None = None,
        source: str = "new_submitter",
        gate: EnqueueGate | None = None,
    ) -> str:
        requested = normalize_protocol(protocol) if protocol is not None else None
        if requested is None and gate is not None:
            gate.enter()

        effective = requested or self.config.default_protocol
        record = self.store.enqueue(
            job_id=job_id,
            batch_number=batch_number,
            amount=amount,
            protocol=effective,
            labels=labels,
            source=source,
            db_role=self.db_role,
            default_protocol=effective,
            default_protocol_version=self.config.version,
        )
        if gate is not None:
            gate.enqueued.set()
        return record.job_id


def run_rolling_upgrade_scenario(
    *, report_path: str | Path | None = None
) -> dict[str, Any]:
    store = WarehouseStore()
    config = RolloutConfig(PROTOCOL_V2)
    submitter = NewSubmitter(store, config)
    legacy = LegacyV1Scheduler(store)

    inflight_release = threading.Event()
    inflight_entered = threading.Event()

    def hold_before_first_write(job: JobRecord) -> threading.Event:
        inflight_entered.set()
        return inflight_release

    job_a_id = store.seed_acquired(
        job_id="job-a-inflight-v1",
        worker_id="worker-a",
        batch_number="B-203-001",
        amount=1250,
        protocol=PROTOCOL_V1,
        source="old_add_job",
        db_role="warehouse_old",
    ).job_id

    store.enqueue(
        job_id="job-legacy-ingest",
        batch_number="B-203-002",
        amount=1250,
        labels=(LEGACY_INGEST,),
        source="historical_legacy",
        db_role="warehouse_old",
    )
    store.enqueue(
        job_id="job-threadpool-ambiguous",
        batch_number="B-203-003",
        amount=1250,
        labels=(THREADPOOL,),
        executor=THREADPOOL,
        source="historical",
    )

    worker_a = Worker("worker-a", store, protocols=(PROTOCOL_V1,))
    inflight_job = store.jobs[job_a_id]
    inflight_result: dict[str, WarehouseReceipt | BaseException] = {}

    def run_inflight() -> None:
        try:
            inflight_result["receipt"] = worker_a.execute(
                inflight_job, before_write=hold_before_first_write
            )
        except BaseException as exc:  # pragma: no cover - defensive harness output
            inflight_result["error"] = exc

    inflight_thread = threading.Thread(target=run_inflight, daemon=True)
    inflight_thread.start()
    if not inflight_entered.wait(timeout=5):
        raise TimeoutError("worker A did not reach the pre-write barrier")

    v2_job_id = submitter.submit(
        job_id="job-v2-switch", batch_number="B-203-004", protocol=PROTOCOL_V2
    )

    old_add_job_id = legacy.add_job(args=("B-203-005", 1250), job_id="job-old-add-v1")
    legacy.add_v1_schedule(
        "schedule-v1",
        batch_number="B-203-006",
        job_id="job-old-schedule-v1",
    )
    old_schedule_job_id = legacy.trigger_schedule("schedule-v1")

    unlabeled_gate = EnqueueGate()
    unlabeled_result: dict[str, str | BaseException] = {}

    def submit_unlabeled() -> None:
        try:
            unlabeled_result["job_id"] = submitter.submit(
                job_id="job-unlabeled-after-rollback",
                batch_number="B-203-007",
                gate=unlabeled_gate,
            )
        except BaseException as exc:  # pragma: no cover
            unlabeled_result["error"] = exc

    unlabeled_thread = threading.Thread(target=submit_unlabeled, daemon=True)
    unlabeled_thread.start()
    unlabeled_gate.wait_until_waiting()
    rollback_version = unlabeled_gate.release_after_commit(config, PROTOCOL_V1)
    if not unlabeled_gate.enqueued.wait(timeout=5):
        raise TimeoutError("unlabeled submission did not enqueue after rollback")
    unlabeled_thread.join(timeout=5)

    worker_b = Worker("worker-b", store, protocols=(PROTOCOL_V1, PROTOCOL_V2)).restart()

    blocked_b_claim = worker_b.claim_next()
    v2_before_a_finishes = store.jobs[v2_job_id]
    pre_completion_checks = {
        "b_claim_while_a_active_is_none": blocked_b_claim is None,
        "v2_not_acquired_by_b": v2_before_a_finishes.acquired_by is None,
        "v2_still_queued": v2_before_a_finishes.status == JobStatus.QUEUED.value,
        "no_writes_before_release": store.writes == [],
        "a_lease_valid": (
            inflight_job.acquired_until is not None
            and inflight_job.acquired_until > datetime.now(UTC)
        ),
        "b_restart_count": worker_b.restart_count,
    }
    if blocked_b_claim is not None:
        raise AssertionError("worker B claimed work while worker A held the task slot")

    inflight_release.set()
    inflight_thread.join(timeout=5)
    if "receipt" not in inflight_result:
        raise AssertionError(inflight_result.get("error", "missing inflight receipt"))

    legacy_receipt = worker_a.process_next()
    if legacy_receipt is None or legacy_receipt.job_id != "job-legacy-ingest":
        raise AssertionError("worker A did not process legacy_ingest first")

    old_add_receipt = worker_a.process_next()
    if old_add_receipt is None or old_add_receipt.job_id != old_add_job_id:
        raise AssertionError("worker A skipped the old add_job v1 work")

    old_schedule_receipt = worker_a.process_next()
    if (
        old_schedule_receipt is None
        or old_schedule_receipt.job_id != old_schedule_job_id
    ):
        raise AssertionError("worker A skipped the old schedule v1 work")

    v2_receipt = worker_b.process_next()
    if v2_receipt is None or v2_receipt.job_id != v2_job_id:
        raise AssertionError("worker B did not take the earliest confirmed v2 job")

    unlabeled_id = unlabeled_result["job_id"]
    assert isinstance(unlabeled_id, str)
    unlabeled_receipt = worker_b.process_next()
    if unlabeled_receipt is None or unlabeled_receipt.job_id != unlabeled_id:
        raise AssertionError("worker B did not take the post-rollback unlabeled job")

    review = store.get_review_status("job-threadpool-ambiguous")
    final_b_claim = worker_b.claim_next()
    final_a_claim = worker_a.claim_next()

    receipt_ids = [
        job_a_id,
        "job-legacy-ingest",
        old_add_job_id,
        old_schedule_job_id,
        v2_job_id,
        unlabeled_id,
    ]
    receipts_by_original_id = {
        job_id: store.get_receipt(job_id).to_dict()  # type: ignore[union-attr]
        for job_id in receipt_ids
    }

    expected_amounts = {
        job_a_id: "12.50",
        "job-legacy-ingest": "12.50",
        old_add_job_id: "12.50",
        old_schedule_job_id: "12.50",
        v2_job_id: "1250.00",
        unlabeled_id: "12.50",
    }
    for job_id, amount in expected_amounts.items():
        receipt = receipts_by_original_id[job_id]
        if receipt["amount"] != amount:
            raise AssertionError(
                f"{job_id}: expected {amount}, got {receipt['amount']}"
            )

    if store.max_active_writes != 1:
        raise AssertionError("warehouse writes were not serialized")

    report = store.summary()
    report.update(
        {
            "status": "passed",
            "scenario": "apscheduler-postgres-rolling-protocol-upgrade",
            "config_history": config.history,
            "rollback_version": rollback_version,
            "pre_completion_checks": pre_completion_checks,
            "receipts_by_original_job_id": receipts_by_original_id,
            "review_by_original_job_id": {"job-threadpool-ambiguous": review.to_dict()},
            "final_claims": {
                "worker_a": final_a_claim.job_id if final_a_claim else None,
                "worker_b": final_b_claim.job_id if final_b_claim else None,
            },
            "expected_amounts": expected_amounts,
        }
    )

    if report_path is not None:
        path = Path(report_path)
        path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    return report


run_scenario = run_rolling_upgrade_scenario


def main() -> int:
    report_path = Path(__file__).with_name("warehouse_rollout_report.json")
    report = run_rolling_upgrade_scenario(report_path=report_path)
    print(  # noqa: T201
        json.dumps(
            {
                "status": report["status"],
                "report": str(report_path),
                "writes": report["writes"],
                "manual_review_jobs": list(report["manual_review"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
