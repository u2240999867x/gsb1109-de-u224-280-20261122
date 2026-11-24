import asyncio
import contextlib
import sqlite3
import typing
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from databases.core import Connection, Database

if typing.TYPE_CHECKING:  # pragma: no cover
    from collections.abc import AsyncIterable, Awaitable, Callable, Mapping

RUN_OPEN = "open"
RUN_COMPLETED = "completed"

RECEIPT_WRITTEN = "written"
RECEIPT_UNCHANGED = "unchanged"
RECEIPT_REJECTED = "rejected"

_INTAKE_DOCUMENTS_DDL = """
CREATE TABLE IF NOT EXISTS intake_documents (
    source TEXT NOT NULL,
    source_seq INTEGER NOT NULL,
    business_key TEXT NOT NULL,
    version INTEGER NOT NULL,
    content TEXT NOT NULL,
    PRIMARY KEY (source, source_seq)
)
"""

_INTAKE_BATCHES_DDL = """
CREATE TABLE IF NOT EXISTS intake_batches (
    batch_id TEXT PRIMARY KEY,
    received_at TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    rejected INTEGER NOT NULL,
    reject_reason TEXT
)
"""

_INTAKE_STAGING_DDL = """
CREATE TABLE IF NOT EXISTS intake_staging (
    batch_id TEXT NOT NULL,
    source TEXT NOT NULL,
    source_seq INTEGER NOT NULL,
    business_key TEXT NOT NULL,
    version INTEGER NOT NULL,
    content TEXT NOT NULL,
    PRIMARY KEY (batch_id, source, source_seq)
)
"""

_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    document_source TEXT NOT NULL,
    run_no TEXT NOT NULL,
    start_seq INTEGER NOT NULL,
    end_seq INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY (document_source, run_no)
)
"""

_RECEIPTS_DDL = """
CREATE TABLE IF NOT EXISTS reconciliation_receipts (
    document_source TEXT NOT NULL,
    source_seq INTEGER NOT NULL,
    run_no TEXT NOT NULL,
    status TEXT NOT NULL,
    row_id INTEGER,
    version INTEGER,
    reason TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY (document_source, source_seq)
)
"""


class ReconciliationError(Exception):
    pass


class InvalidDocumentError(ReconciliationError):
    pass


class BatchConflictError(ReconciliationError):
    def __init__(self, batch_id: str, reason: str, item_count: int) -> None:
        self.batch_id = batch_id
        self.reason = reason
        self.item_count = item_count
        super().__init__(f"Batch {batch_id} rejected: {reason}")


class RunNotFoundError(ReconciliationError):
    def __init__(self, run_no: str) -> None:
        self.run_no = run_no
        super().__init__(f"Reconciliation run {run_no!r} does not exist")


@dataclass(frozen=True)
class Document:
    source: str
    source_seq: int
    business_key: str
    version: int
    content: str

    @classmethod
    def from_mapping(cls, raw: "Mapping[str, typing.Any]") -> "Document":
        try:
            return cls(
                source=str(raw["source"]),
                source_seq=int(raw["source_seq"]),
                business_key=str(raw["business_key"]),
                version=int(raw["version"]),
                content=str(raw["content"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidDocumentError(f"Invalid intake document: {raw!r}") from exc


@dataclass(frozen=True)
class IntakeResult:
    batch_id: str
    item_count: int
    accepted_count: int


@dataclass(frozen=True)
class ApplyResult:
    changed: bool
    row_id: typing.Optional[int] = None
    version: typing.Optional[int] = None


def written(row_id: int, version: int) -> ApplyResult:
    return ApplyResult(changed=True, row_id=int(row_id), version=int(version))


def unchanged() -> ApplyResult:
    return ApplyResult(changed=False)


@dataclass(frozen=True)
class Receipt:
    source_seq: int
    status: str
    run_no: str
    row_id: typing.Optional[int]
    version: typing.Optional[int]
    reason: typing.Optional[str]


@contextlib.asynccontextmanager
async def _standalone_connection(
    database: Database,
) -> typing.AsyncIterator[Connection]:
    # A core-level Connection gives us the same string/ClauseElement handling
    # callers get, while a manually managed backend connection keeps it out of
    # the task-level connection map and out of any caller transaction.
    connection = Connection(database, database._backend)
    await connection._connection.acquire()
    try:
        yield connection
    finally:
        await connection._connection.release()


@contextlib.asynccontextmanager
async def _standalone_transaction(
    database: Database,
) -> typing.AsyncIterator[Connection]:
    # Drives the backend transaction directly so it never shares the task-level
    # Connection's acquire counter or an outer caller transaction.
    async with _standalone_connection(database) as connection:
        transaction = connection._connection.transaction()
        await transaction.start(is_root=True, extra_options={})
        try:
            yield connection
        except BaseException:
            await transaction.rollback()
            raise
        else:
            await transaction.commit()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class DocumentIntake:
    """Durable, read-once intake of supplier documents in a SQLite database."""

    def __init__(self, database: Database) -> None:
        self._database = database
        self._ready = False
        self._lock = asyncio.Lock()

    async def _ensure_schema(self) -> None:
        if self._ready:
            return
        async with self._lock:
            if self._ready:
                return
            async with _standalone_connection(self._database) as connection:
                await connection.execute(_INTAKE_DOCUMENTS_DDL)
                await connection.execute(_INTAKE_BATCHES_DDL)
                await connection.execute(_INTAKE_STAGING_DDL)
            self._ready = True

    async def receive_many(
        self, documents: typing.List["Mapping[str, typing.Any]"]
    ) -> IntakeResult:
        async def iterator() -> "AsyncIterable[Mapping[str, typing.Any]]":
            for document in documents:
                yield document

        return await self.receive_batch(iterator())

    async def receive_batch(
        self, documents: "AsyncIterable[Mapping[str, typing.Any]]"
    ) -> IntakeResult:
        await self._ensure_schema()
        batch_id = uuid.uuid4().hex
        item_count = 0

        # Stage every item as it streams in. The input iterator is consumed
        # exactly once and memory use stays bounded for very large resupplies.
        try:
            async with _standalone_transaction(self._database) as connection:
                async for raw in documents:
                    document = Document.from_mapping(raw)
                    item_count += 1
                    duplicate = await connection.fetch_one(
                        "SELECT business_key, version, content FROM intake_staging "
                        "WHERE batch_id = :batch_id AND source = :source "
                        "AND source_seq = :source_seq",
                        {
                            "batch_id": batch_id,
                            "source": document.source,
                            "source_seq": document.source_seq,
                        },
                    )
                    if duplicate is not None and not self._matches(
                        duplicate, document
                    ):
                        reason = (
                            f"sequence {document.source_seq} appears twice in "
                            "the batch with different content"
                        )
                        raise BatchConflictError(batch_id, reason, item_count)
                    if duplicate is None:
                        await connection.execute(
                            "INSERT INTO intake_staging "
                            "(batch_id, source, source_seq, business_key, "
                            "version, content) "
                            "VALUES (:batch_id, :source, :source_seq, "
                            ":business_key, :version, :content)",
                            {
                                "batch_id": batch_id,
                                "source": document.source,
                                "source_seq": document.source_seq,
                                "business_key": document.business_key,
                                "version": document.version,
                                "content": document.content,
                            },
                        )
        except BatchConflictError as error:
            # The staging transaction rolled back, so no staged rows remain.
            await self._record_rejection(
                error.batch_id, error.reason, error.item_count
            )
            raise

        try:
            accepted_count = await self._merge_batch(batch_id, item_count)
        except BatchConflictError as error:
            await self._discard_staging(batch_id)
            await self._record_rejection(
                error.batch_id, error.reason, error.item_count
            )
            raise
        return IntakeResult(
            batch_id=batch_id,
            item_count=item_count,
            accepted_count=accepted_count,
        )

    async def _merge_batch(self, batch_id: str, item_count: int) -> int:
        # The whole merge is one transaction, so a conflict rolls back every
        # document in the batch while previously received documents stay put.
        async with _standalone_transaction(self._database) as connection:
            conflict = await connection.fetch_one(
                "SELECT s.source, s.source_seq "
                "FROM intake_staging AS s "
                "JOIN intake_documents AS d "
                "  ON d.source = s.source AND d.source_seq = s.source_seq "
                "WHERE s.batch_id = :batch_id "
                "  AND (d.business_key <> s.business_key "
                "       OR d.version <> s.version "
                "       OR d.content <> s.content) "
                "LIMIT 1",
                {"batch_id": batch_id},
            )
            if conflict is not None:
                reason = (
                    f"sequence {conflict['source_seq']} was already received "
                    "with different content"
                )
                raise BatchConflictError(batch_id, reason, item_count)

            accepted_count = await connection.fetch_val(
                "SELECT COUNT(*) FROM intake_staging WHERE batch_id = :batch_id",
                {"batch_id": batch_id},
            )
            await connection.execute(
                "INSERT OR IGNORE INTO intake_documents "
                "(source, source_seq, business_key, version, content) "
                "SELECT source, source_seq, business_key, version, content "
                "FROM intake_staging WHERE batch_id = :batch_id",
                {"batch_id": batch_id},
            )
            await connection.execute(
                "INSERT INTO intake_batches "
                "(batch_id, received_at, item_count, rejected, reject_reason) "
                "VALUES (:batch_id, :received_at, :item_count, 0, NULL)",
                {
                    "batch_id": batch_id,
                    "received_at": _utcnow(),
                    "item_count": item_count,
                },
            )
            await connection.execute(
                "DELETE FROM intake_staging WHERE batch_id = :batch_id",
                {"batch_id": batch_id},
            )
            return int(accepted_count)

    async def _record_rejection(
        self, batch_id: str, reason: str, item_count: int
    ) -> None:
        async with _standalone_connection(self._database) as connection:
            await connection.execute(
                "INSERT OR REPLACE INTO intake_batches "
                    "(batch_id, received_at, item_count, rejected, reject_reason) "
                    "VALUES (:batch_id, :received_at, :item_count, 1, :reason)",
                {
                    "batch_id": batch_id,
                    "received_at": _utcnow(),
                    "item_count": item_count,
                    "reason": reason,
                },
            )

    async def _discard_staging(self, batch_id: str) -> None:
        async with _standalone_connection(self._database) as connection:
            await connection.execute(
                "DELETE FROM intake_staging WHERE batch_id = :batch_id",
                {"batch_id": batch_id},
            )

    @staticmethod
    def _matches(
        existing: "Mapping[str, typing.Any]", document: Document
    ) -> bool:
        return (
            existing["business_key"] == document.business_key
            and existing["version"] == document.version
            and existing["content"] == document.content
        )

    async def get_document(
        self, source: str, source_seq: int
    ) -> typing.Optional["Mapping[str, typing.Any]"]:
        await self._ensure_schema()
        async with _standalone_connection(self._database) as connection:
            return await connection.fetch_one(
                "SELECT source, source_seq, business_key, version, content "
                    "FROM intake_documents "
                    "WHERE source = :source AND source_seq = :source_seq",
                {"source": source, "source_seq": source_seq},
            )


ApplyCallback = typing.Callable[
    [Connection, "Mapping[str, typing.Any]"],
    "Awaitable[typing.Optional[ApplyResult]]",
]


class Reconciliation:
    """Reconciles received documents into the caller's business database."""

    def __init__(
        self, intake_database: Database, business_database: Database
    ) -> None:
        self._intake = intake_database
        self._business = business_database
        self._ready = False
        self._lock = asyncio.Lock()

    async def _ensure_schema(self) -> None:
        if self._ready:
            return
        async with self._lock:
            if self._ready:
                return
            async with _standalone_connection(self._business) as connection:
                await connection.execute(_RUNS_DDL)
                await connection.execute(_RECEIPTS_DDL)
            self._ready = True

    async def open_run(
        self, run_no: str, start_seq: int, *, source: str
    ) -> int:
        """Open a run and return the upper bound of its contiguous range."""
        await self._ensure_schema()
        async with _standalone_connection(self._business) as connection:
            existing = await connection.fetch_one(
                "SELECT end_seq FROM reconciliation_runs "
                    "WHERE document_source = :source AND run_no = :run_no",
                {"source": source, "run_no": run_no},
            )
            if existing is not None:
                return int(existing["end_seq"])

            end_seq = await self._compute_end_seq(source, start_seq)
            await connection.execute(
                "INSERT INTO reconciliation_runs "
                    "(document_source, run_no, start_seq, end_seq, status, "
                    " created_at, completed_at) "
                    "VALUES (:source, :run_no, :start_seq, :end_seq, :status, "
                    "        :created_at, NULL)",
                {
                    "source": source,
                    "run_no": run_no,
                    "start_seq": start_seq,
                    "end_seq": end_seq,
                    "status": RUN_OPEN,
                    "created_at": _utcnow(),
                },
            )
            return end_seq

    async def _compute_end_seq(
        self, source: str, start_seq: int, page_size: int = 500
    ) -> int:
        end_seq = start_seq - 1
        async with _standalone_connection(self._intake) as connection:
            while True:
                rows = await connection.fetch_all(
                    "SELECT source_seq FROM intake_documents "
                        "WHERE source = :source AND source_seq >= :start_seq "
                        "ORDER BY source_seq LIMIT :page_size",
                    {
                        "source": source,
                        "start_seq": end_seq + 1,
                        "page_size": page_size,
                    },
                )
                if not rows:
                    break
                for row in rows:
                    seq = int(row["source_seq"])
                    if seq != end_seq + 1:
                        return end_seq
                    end_seq = seq
                if len(rows) < page_size:
                    break
        return end_seq

    async def process_run(
        self, run_no: str, *, source: str, apply: ApplyCallback
    ) -> typing.List[Receipt]:
        """Process the run inside the caller's business transaction."""
        await self._ensure_schema()
        connection = self._business.connection()
        if connection._transaction_stack:
            return await self._process(connection, run_no, source, apply)
        async with connection.transaction():
            return await self._process(connection, run_no, source, apply)

    async def _process(
        self,
        connection: Connection,
        run_no: str,
        source: str,
        apply: ApplyCallback,
    ) -> typing.List[Receipt]:
        run = await connection.fetch_one(
            "SELECT start_seq, end_seq, status FROM reconciliation_runs "
                "WHERE document_source = :source AND run_no = :run_no",
            {"source": source, "run_no": run_no},
        )
        if run is None:
            raise RunNotFoundError(run_no)
        if run["status"] == RUN_COMPLETED:
            return await self._load_receipts(connection, source, run_no)

        receipts: typing.List[Receipt] = []
        # One read-only connection to the intake database serves the whole run,
        # so large ranges do not reopen a connection for every sequence.
        async with _standalone_connection(self._intake) as intake_connection:
            for source_seq in range(
                int(run["start_seq"]), int(run["end_seq"]) + 1
            ):
                receipt = await self._process_sequence(
                    connection,
                    intake_connection,
                    source,
                    source_seq,
                    run_no,
                    apply,
                )
                receipts.append(receipt)

        await connection.execute(
            "UPDATE reconciliation_runs "
                "SET status = :status, completed_at = :completed_at "
                "WHERE document_source = :source AND run_no = :run_no",
            {
                "status": RUN_COMPLETED,
                "completed_at": _utcnow(),
                "source": source,
                "run_no": run_no,
            },
        )
        return receipts

    async def _process_sequence(
        self,
        connection: Connection,
        intake_connection: Connection,
        source: str,
        source_seq: int,
        run_no: str,
        apply: ApplyCallback,
    ) -> Receipt:
        existing = await connection.fetch_one(
            "SELECT source_seq, run_no, status, row_id, version, reason "
                "FROM reconciliation_receipts "
                "WHERE document_source = :source AND source_seq = :source_seq",
            {"source": source, "source_seq": source_seq},
        )
        if existing is not None:
            return self._receipt_from_row(existing)

        document = await self._fetch_document(
            intake_connection, source, source_seq
        )
        status = RECEIPT_WRITTEN
        row_id: typing.Optional[int] = None
        version: typing.Optional[int] = None
        reason: typing.Optional[str] = None
        try:
            outcome = await apply(connection, document)
        except sqlite3.IntegrityError as error:
            status = RECEIPT_REJECTED
            reason = str(error)
        else:
            if outcome is None or not outcome.changed:
                status = RECEIPT_UNCHANGED
            else:
                row_id = outcome.row_id
                version = outcome.version

        await connection.execute(
            "INSERT INTO reconciliation_receipts "
                "(document_source, source_seq, run_no, status, row_id, version, "
                " reason, created_at, completed_at) "
                "VALUES (:source, :source_seq, :run_no, :status, :row_id, :version, "
                "        :reason, :created_at, NULL)",
            {
                "source": source,
                "source_seq": source_seq,
                "run_no": run_no,
                "status": status,
                "row_id": row_id,
                "version": version,
                "reason": reason,
                "created_at": _utcnow(),
            },
        )
        return Receipt(
            source_seq=source_seq,
            status=status,
            run_no=run_no,
            row_id=row_id,
            version=version,
            reason=reason,
        )

    async def _fetch_document(
        self, intake_connection: Connection, source: str, source_seq: int
    ) -> "Mapping[str, typing.Any]":
        document = await intake_connection.fetch_one(
            "SELECT source, source_seq, business_key, version, content "
                "FROM intake_documents "
                "WHERE source = :source AND source_seq = :source_seq",
            {"source": source, "source_seq": source_seq},
        )
        if document is None:
            raise ReconciliationError(
                f"document {source}:{source_seq} is missing from the intake database"
            )
        return document

    async def get_run(
        self, run_no: str, *, source: str
    ) -> typing.Optional["Mapping[str, typing.Any]"]:
        await self._ensure_schema()
        async with _standalone_connection(self._business) as connection:
            return await connection.fetch_one(
                "SELECT document_source, run_no, start_seq, end_seq, status, "
                    "       created_at, completed_at "
                    "FROM reconciliation_runs "
                    "WHERE document_source = :source AND run_no = :run_no",
                {"source": source, "run_no": run_no},
            )

    async def get_receipt(
        self, source: str, source_seq: int
    ) -> typing.Optional[Receipt]:
        await self._ensure_schema()
        async with _standalone_connection(self._business) as connection:
            row = await connection.fetch_one(
                "SELECT source_seq, run_no, status, row_id, version, reason "
                    "FROM reconciliation_receipts "
                    "WHERE document_source = :source AND source_seq = :source_seq",
                {"source": source, "source_seq": source_seq},
            )
        return self._receipt_from_row(row) if row is not None else None

    async def get_receipts(
        self, run_no: str, *, source: str
    ) -> typing.List[Receipt]:
        await self._ensure_schema()
        async with _standalone_connection(self._business) as connection:
            run = await connection.fetch_one(
                "SELECT start_seq, end_seq FROM reconciliation_runs "
                    "WHERE document_source = :source AND run_no = :run_no",
                {"source": source, "run_no": run_no},
            )
            if run is None:
                raise RunNotFoundError(run_no)
            return await self._load_receipts(connection, source, run_no)

    async def _load_receipts(
        self, connection: Connection, source: str, run_no: str
    ) -> typing.List[Receipt]:
        run = await connection.fetch_one(
            "SELECT start_seq, end_seq FROM reconciliation_runs "
                "WHERE document_source = :source AND run_no = :run_no",
            {"source": source, "run_no": run_no},
        )
        if run is None:
            raise RunNotFoundError(run_no)
        rows = await connection.fetch_all(
            "SELECT source_seq, run_no, status, row_id, version, reason "
                "FROM reconciliation_receipts "
                "WHERE document_source = :source "
                "  AND source_seq BETWEEN :start_seq AND :end_seq "
                "ORDER BY source_seq",
            {
                "source": source,
                "start_seq": int(run["start_seq"]),
                "end_seq": int(run["end_seq"]),
            },
        )
        return [self._receipt_from_row(row) for row in rows]

    @staticmethod
    def _receipt_from_row(
        row: "Mapping[str, typing.Any]",
    ) -> Receipt:
        return Receipt(
            source_seq=int(row["source_seq"]),
            status=row["status"],
            run_no=row["run_no"],
            row_id=row["row_id"],
            version=row["version"],
            reason=row["reason"],
        )
