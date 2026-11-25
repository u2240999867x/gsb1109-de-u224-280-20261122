from __future__ import annotations

import os
from datetime import timedelta
from typing import Any
from uuid import UUID

from warehouse_ingest import (
    DEFAULT_RETENTION,
    WAREHOUSE_TASK_ID,
    WarehouseGateway,
    WarehouseStore,
    money_receipt_amount,
    normalize_protocol,
)

PROTOCOL_METADATA_KEY = "warehouse_protocol"
LABELS_METADATA_KEY = "warehouse_labels"


def add_warehouse_job(
    scheduler: Any,
    batch_number: str,
    amount: int = 1250,
    *,
    protocol: str | None = None,
    labels: list[str] | tuple[str, ...] | None = None,
    job_id: str | None = None,
    result_expiration_time: timedelta = DEFAULT_RETENTION,
) -> Any:
    """Enqueue through the new submitter while recording the resolved protocol."""
    metadata: dict[str, Any] = {}
    if protocol is not None:
        resolved = normalize_protocol(protocol)
        if resolved is None:
            raise ValueError(f"unknown protocol: {protocol!r}")
        metadata[PROTOCOL_METADATA_KEY] = resolved
    if labels:
        metadata[LABELS_METADATA_KEY] = list(labels)

    kwargs: dict[str, Any] = {
        "args": (batch_number, amount),
        "metadata": metadata,
        "result_expiration_time": result_expiration_time,
    }
    if job_id is None:
        return scheduler.add_job(WAREHOUSE_TASK_ID, **kwargs)

    from apscheduler import Job

    job_uuid = job_id if isinstance(job_id, UUID) else UUID(str(job_id))
    job = Job(
        id=job_uuid,
        task_id=WAREHOUSE_TASK_ID,
        args=(batch_number, amount),
        executor="threadpool",
        metadata=metadata,
        result_expiration_time=result_expiration_time,
    )
    portal = scheduler._ensure_services_ready()
    portal.call(scheduler.data_store.add_job, job)
    return job.id


def create_postgres_scheduler(
    dsn: str | None = None,
    *,
    identity: str,
    lease_duration: timedelta = timedelta(seconds=30),
) -> Any:
    """Create an APScheduler sync scheduler backed by PostgreSQL.

    The DSN selects the database role. PostgreSQL RLS from
    ``sql/warehouse_rolling_upgrade.sql`` performs old-worker protocol filtering,
    so the unchanged v1 process can keep using plain ``Scheduler.add_job``.
    """
    from apscheduler import Scheduler
    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    connection = dsn or os.environ["WAREHOUSE_SCHEDULER_DSN"]
    data_store = SQLAlchemyDataStore(connection)
    scheduler = Scheduler(
        data_store=data_store,
        identity=identity,
        lease_duration=lease_duration,
    )
    scheduler.configure_task(
        WAREHOUSE_TASK_ID,
        func=warehouse_ingest,
        max_running_jobs=1,
    )
    return scheduler


def warehouse_ingest(batch_number: str, amount: int = 1250) -> dict[str, Any]:
    """Example APScheduler task used by the new worker.

    Production deployments should replace the in-memory receipt store with
    ``warehouse_receipts`` writes in the same supplier-call transaction.
    """
    from apscheduler._context import current_job

    job = current_job.get()
    protocol = normalize_protocol(job.metadata.get(PROTOCOL_METADATA_KEY))
    if protocol is None:
        raise RuntimeError(
            f"job {job.id} reached the new worker without a resolved protocol"
        )

    displayed = money_receipt_amount(amount, protocol)
    return {
        "job_id": str(job.id),
        "batch_number": str(batch_number),
        "input_amount": amount,
        "protocol_used": protocol,
        "amount": displayed,
        "currency": "EUR",
        "receipt_text": f"{displayed} 欧元",
    }


class PostgresReceipts:
    """Small psycopg/SQLAlchemy helper for receipt and review lookups."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def _connect(self) -> Any:
        try:
            import psycopg
        except ImportError:  # pragma: no cover
            import psycopg2 as psycopg  # type: ignore[no-redef]

        return psycopg.connect(self.dsn)

    def get_receipt(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT receipt FROM warehouse_receipts WHERE job_id = %s",
                    (job_id,),
                )
                row = cur.fetchone()
        return row[0] if row else None

    def get_review_status(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, reason, labels, original_args, original_kwargs
                    FROM warehouse_manual_review
                    WHERE job_id = %s
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return {
            "job_id": job_id,
            "status": row[0],
            "reason": row[1],
            "labels": row[2],
            "original_args": row[3],
            "original_kwargs": row[4],
        }


def new_in_memory_runtime() -> tuple[WarehouseStore, WarehouseGateway]:
    store = WarehouseStore()
    return store, WarehouseGateway(store)
