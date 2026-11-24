"""
Archive settlement output from a multi-result SQL Server daily-close procedure
into a SQLite target database.

The source procedure is executed exactly once by the caller.  This module only
advances the cursor through the resulting chain of result sets:

* result sets with a column description are settlement details and are numbered
  in order of appearance - an empty detail still takes a number;
* result sets without a column description only report an affected-row count
  and take no number;
* the manifest is the result set whose columns match ``MANIFEST_COLUMNS``; it
  describes each settlement batch and the detail numbers (or an explicit
  "not returned" marker) for its production and cancellation details.

Details are spooled into a temporary SQLite database while the chain is read,
so the number of rows held in Python is bounded by the configured chunk size,
independently of the total settlement volume.  Nothing is written to the target
database until the whole chain has been read and the manifest has been fully
validated, which keeps target writes all-or-nothing.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import Any, NamedTuple, Optional

from etlhelper.etl import CHUNKSIZE
from etlhelper.exceptions import ETLHelperArchiveError

# Columns shared by every production/cancellation detail.
DETAIL_COLUMNS = ("event_id", "valid_batch_no")

# Independent column description of the trailing manifest.  A ``None`` detail
# reference explicitly marks that the procedure did not return that detail.
MANIFEST_COLUMNS = (
    "plant",
    "released",
    "production_detail",
    "production_expected_rows",
    "cancellation_detail",
    "cancellation_expected_rows",
)

# Detail-level outcomes reported on the receipt.
STATUS_APPLIED = "applied"
STATUS_EMPTY = "empty"
STATUS_NOT_RETURNED = "not_returned"
STATUS_REJECTED = "rejected"
STATUS_SKIPPED = "skipped"

SAVEPOINT = "etlhelper_settlement_archive"

# Reference DDL for the target database.  The two business rules and their
# triggers are expected to already exist there; this constant is provided so
# callers and tests can build a matching local database.
TARGET_SCHEMA_SQL = """
CREATE TABLE batch_status (
    valid_batch_no TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('effective', 'cancelled'))
);

CREATE TABLE production_events (
    event_id TEXT NOT NULL,
    plant TEXT NOT NULL,
    valid_batch_no TEXT NOT NULL
);

CREATE TABLE cancellation_events (
    event_id TEXT NOT NULL,
    plant TEXT NOT NULL,
    valid_batch_no TEXT NOT NULL
);

CREATE TABLE event_journal (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    plant TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('production', 'cancellation')),
    valid_batch_no TEXT NOT NULL
);

CREATE TRIGGER trg_production_business_rule
BEFORE INSERT ON production_events
BEGIN
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM batch_status
        WHERE valid_batch_no = NEW.valid_batch_no
          AND status = 'effective'
    ) THEN RAISE(ABORT, 'production event ' || NEW.event_id
        || ' rejected: valid batch ' || NEW.valid_batch_no
        || ' is already effective') END;
END;

CREATE TRIGGER trg_production_journal
AFTER INSERT ON production_events
BEGIN
    INSERT INTO batch_status (valid_batch_no, status)
    VALUES (NEW.valid_batch_no, 'effective')
    ON CONFLICT(valid_batch_no) DO UPDATE SET status = 'effective';

    INSERT INTO event_journal (event_id, plant, event_type, valid_batch_no)
    VALUES (NEW.event_id, NEW.plant, 'production', NEW.valid_batch_no);
END;

CREATE TRIGGER trg_cancellation_business_rule
BEFORE INSERT ON cancellation_events
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM batch_status
        WHERE valid_batch_no = NEW.valid_batch_no
          AND status = 'effective'
    ) THEN RAISE(ABORT, 'cancellation event ' || NEW.event_id
        || ' rejected: valid batch ' || NEW.valid_batch_no
        || ' is not effective') END;
END;

CREATE TRIGGER trg_cancellation_journal
AFTER INSERT ON cancellation_events
BEGIN
    UPDATE batch_status
    SET status = 'cancelled'
    WHERE valid_batch_no = NEW.valid_batch_no;

    INSERT INTO event_journal (event_id, plant, event_type, valid_batch_no)
    VALUES (NEW.event_id, NEW.plant, 'cancellation', NEW.valid_batch_no);
END;
"""

_INSERT_PRODUCTION_SQL = (
    "INSERT INTO production_events (event_id, plant, valid_batch_no) "
    "VALUES (?, ?, ?)"
)
_INSERT_CANCELLATION_SQL = (
    "INSERT INTO cancellation_events (event_id, plant, valid_batch_no) "
    "VALUES (?, ?, ?)"
)


class ManifestEntry(NamedTuple):
    """One row of the trailing manifest, in manifest order."""

    plant: str
    released: bool
    production_detail: Optional[int]
    production_expected_rows: int
    cancellation_detail: Optional[int]
    cancellation_expected_rows: int


class DetailSet(NamedTuple):
    """Metadata for one returned detail (its rows live in the spool)."""

    number: int
    columns: tuple[str, ...]
    row_count: int


class BatchReceipt(NamedTuple):
    """Outcome for a single settlement batch."""

    plant: str
    released: bool
    production_detail: Optional[int]
    production_expected_rows: int
    production_actual_rows: int
    production_status: str
    cancellation_detail: Optional[int]
    cancellation_expected_rows: int
    cancellation_actual_rows: int
    cancellation_status: str
    accepted: bool
    rejected_event_id: Optional[Any]
    rejection_reason: Optional[str]


class ArchiveReceipt:
    """Outcome of a settlement archive run."""

    def __init__(self, batch_receipts: list[BatchReceipt]):
        self.batches = batch_receipts

    @property
    def accepted(self) -> list[BatchReceipt]:
        return [batch for batch in self.batches if batch.accepted]

    @property
    def rejected(self) -> list[BatchReceipt]:
        return [
            batch
            for batch in self.batches
            if batch.released and not batch.accepted
        ]

    @property
    def skipped(self) -> list[BatchReceipt]:
        return [batch for batch in self.batches if not batch.released]


def archive_settlement(
    source_cursor: Any,
    dest_conn: sqlite3.Connection,
    chunk_size: int = CHUNKSIZE,
) -> ArchiveReceipt:
    """
    Read every result set of an executed daily-close procedure and archive the
    released settlement batches into the SQLite target database.

    The caller's transaction is left open: on success the archive writes are
    released from an internal savepoint and committed or rolled back together
    with any earlier changes (e.g. a site correction).  A fatal failure does
    not roll the caller's transaction back.

    :param source_cursor: DBAPI cursor on which the procedure has already been
                          executed; it is only advanced, never re-executed
    :param dest_conn: target SQLite connection, already in a transaction
    :param chunk_size: maximum number of detail rows fetched or inserted at
                       once; Python memory is bounded by this size
    :return: an ArchiveReceipt describing every manifest batch
    :raises ETLHelperArchiveError: if the result chain cannot be read, the
                                   manifest does not match the returned details
                                   or a non-business write error occurs
    """
    if not isinstance(chunk_size, int) or chunk_size < 1:
        raise ValueError("chunk_size must be a positive integer")

    spool, spool_path = _open_spool()
    try:
        details, manifest = _read_result_chain(
            source_cursor, spool, chunk_size
        )
        _validate_manifest(manifest, details, spool)
        return _apply_manifest(manifest, details, spool, dest_conn, chunk_size)
    finally:
        spool.close()
        if os.path.exists(spool_path):
            os.unlink(spool_path)


def _open_spool() -> tuple[sqlite3.Connection, str]:
    """Create a temporary file-backed SQLite spool."""
    spool_file = tempfile.NamedTemporaryFile(
        prefix="etlhelper_settlement_", suffix=".db", delete=False
    )
    spool_path = spool_file.name
    spool_file.close()
    spool = sqlite3.connect(spool_path)
    spool.execute(
        "CREATE TABLE detail_rows ("
        "detail_number INTEGER NOT NULL, row_seq INTEGER NOT NULL, "
        "event_id, valid_batch_no)"
    )
    spool.execute(
        "CREATE TABLE manifest_rows ("
        "row_seq INTEGER PRIMARY KEY, plant TEXT NOT NULL, released INTEGER, "
        "production_detail INTEGER, production_expected_rows INTEGER, "
        "cancellation_detail INTEGER, cancellation_expected_rows INTEGER)"
    )
    spool.commit()
    return spool, spool_path


def _read_result_chain(
    cursor: Any, spool: sqlite3.Connection, chunk_size: int
) -> tuple[dict[int, DetailSet], list[ManifestEntry]]:
    """
    Advance the cursor through its full result chain.

    Described result sets are numbered details, or the manifest when their
    columns match the manifest description.  Undescribed result sets only
    carry a row count and take no number.  A ``None`` description alone does
    not end the chain: the return value of ``nextset`` does, so a row-count
    result followed by more sets is consumed correctly.
    """
    details: dict[int, DetailSet] = {}
    manifest: list[ManifestEntry] = []
    next_number = 1
    manifest_seen = False

    try:
        description = cursor.description
    except Exception as exc:
        raise ETLHelperArchiveError(
            f"Failed to read result set description: {exc}"
        ) from exc

    while True:
        if description is not None:
            columns = tuple(desc[0] for desc in description)

            if columns == MANIFEST_COLUMNS:
                if manifest_seen:
                    raise ETLHelperArchiveError(
                        "A described result set appeared after the manifest"
                    )
                manifest_seen = True
                _spool_manifest(cursor, spool, chunk_size)
                manifest = _load_manifest(spool)
            elif columns == DETAIL_COLUMNS:
                if manifest_seen:
                    raise ETLHelperArchiveError(
                        "A described result set appeared after the manifest"
                    )
                row_count = _spool_detail(
                    cursor, spool, next_number, chunk_size
                )
                details[next_number] = DetailSet(
                    next_number, columns, row_count
                )
                next_number += 1
            else:
                raise ETLHelperArchiveError(
                    f"Unexpected result set columns {columns}; expected "
                    f"{DETAIL_COLUMNS} or {MANIFEST_COLUMNS}"
                )

        # Undescribed result sets only report an affected row count: they take
        # no detail number and are simply skipped.
        has_more = _advance(cursor)
        if not has_more:
            break

        try:
            description = cursor.description
        except Exception as exc:
            raise ETLHelperArchiveError(
                f"Failed to read result set description: {exc}"
            ) from exc

    if not manifest_seen:
        raise ETLHelperArchiveError(
            "The result chain ended without a manifest result set"
        )

    return details, manifest


def _advance(cursor: Any) -> bool:
    """Advance to the next result set; return whether one is available."""
    try:
        has_more = cursor.nextset()
    except Exception as exc:
        raise ETLHelperArchiveError(
            f"Failed to advance the result chain: {exc}"
        ) from exc
    return bool(has_more)


def _iter_chunks(
    cursor: Any, chunk_size: int
) -> Any:
    """Yield rows from the current cursor result set in chunks."""
    while True:
        try:
            chunk = cursor.fetchmany(chunk_size)
        except Exception as exc:
            raise ETLHelperArchiveError(
                f"Failed to fetch detail rows: {exc}"
            ) from exc
        if not chunk:
            return
        yield chunk


def _spool_detail(
    cursor: Any,
    spool: sqlite3.Connection,
    number: int,
    chunk_size: int,
) -> int:
    """Stream one described detail into the spool and return its row count."""
    row_count = 0
    insert_sql = (
        "INSERT INTO detail_rows "
        "(detail_number, row_seq, event_id, valid_batch_no) "
        "VALUES (?, ?, ?, ?)"
    )
    for chunk in _iter_chunks(cursor, chunk_size):
        parameters = [
            (number, row_count + offset, row[0], row[1])
            for offset, row in enumerate(chunk)
        ]
        spool.executemany(insert_sql, parameters)
        row_count += len(parameters)
    spool.commit()
    return row_count


def _spool_manifest(
    cursor: Any, spool: sqlite3.Connection, chunk_size: int
) -> None:
    """Stream the manifest result set into the spool."""
    row_seq = 0
    insert_sql = (
        "INSERT INTO manifest_rows "
        "(row_seq, plant, released, production_detail, "
        "production_expected_rows, cancellation_detail, "
        "cancellation_expected_rows) VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    for chunk in _iter_chunks(cursor, chunk_size):
        parameters = []
        for row in chunk:
            parameters.append((
                row_seq,
                row[0],
                1 if row[1] else 0,
                row[2],
                row[3],
                row[4],
                row[5],
            ))
            row_seq += 1
        spool.executemany(insert_sql, parameters)
    spool.commit()


def _load_manifest(spool: sqlite3.Connection) -> list[ManifestEntry]:
    """Read manifest rows back in their original manifest order."""
    rows = spool.execute(
        "SELECT plant, released, production_detail, production_expected_rows, "
        "cancellation_detail, cancellation_expected_rows "
        "FROM manifest_rows ORDER BY row_seq"
    ).fetchall()
    return [
        ManifestEntry(
            plant=row[0],
            released=bool(row[1]),
            production_detail=_as_detail_ref(row[2]),
            production_expected_rows=row[3],
            cancellation_detail=_as_detail_ref(row[4]),
            cancellation_expected_rows=row[5],
        )
        for row in rows
    ]


def _as_detail_ref(value: Any) -> Optional[int]:
    """
    Normalize a manifest detail reference.  ``None`` (SQL NULL) and any
    non-positive integer are the explicit "not returned" marker; details are
    numbered from one.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        return value
    return value if value > 0 else None


def _validate_manifest(
    manifest: list[ManifestEntry],
    details: dict[int, DetailSet],
    spool: sqlite3.Connection,
) -> None:
    """Check the manifest against the details that were actually returned."""
    if not manifest:
        raise ETLHelperArchiveError("The manifest result set is empty")

    references: dict[int, int] = {}

    for entry in manifest:
        for kind, detail_number, expected_rows in (
            ("production", entry.production_detail,
             entry.production_expected_rows),
            ("cancellation", entry.cancellation_detail,
             entry.cancellation_expected_rows),
        ):
            if detail_number is None:
                continue
            if isinstance(detail_number, bool) or not isinstance(
                detail_number, int
            ) or detail_number < 1:
                raise ETLHelperArchiveError(
                    f"Manifest {kind} detail reference {detail_number!r} for "
                    f"plant {entry.plant!r} is not a positive detail number"
                )
            detail = details.get(detail_number)
            if detail is None:
                raise ETLHelperArchiveError(
                    f"Manifest references {kind} detail {detail_number} for "
                    f"plant {entry.plant!r} but no such detail was returned"
                )
            if detail.row_count != expected_rows:
                raise ETLHelperArchiveError(
                    f"{kind.capitalize()} detail {detail_number} for plant "
                    f"{entry.plant!r} returned {detail.row_count} rows but "
                    f"the manifest expected {expected_rows}"
                )
            references[detail_number] = references.get(detail_number, 0) + 1

    unreferenced = sorted(set(details) - set(references))
    if unreferenced:
        raise ETLHelperArchiveError(
            f"Returned details {unreferenced} are not assigned to any "
            "manifest batch"
        )

    duplicated = sorted(
        number for number, count in references.items() if count != 1
    )
    if duplicated:
        raise ETLHelperArchiveError(
            f"Returned details {duplicated} must each have exactly one "
            "manifest owner"
        )

    participating = [
        number
        for entry in manifest
        if entry.released
        for number in (entry.production_detail, entry.cancellation_detail)
        if number is not None
    ]
    if participating:
        placeholders = ",".join("?" for _ in participating)
        duplicate_rows = spool.execute(
            f"SELECT event_id FROM detail_rows "
            f"WHERE detail_number IN ({placeholders}) "
            f"GROUP BY event_id HAVING COUNT(*) > 1",
            participating,
        ).fetchall()
        if duplicate_rows:
            duplicates = [row[0] for row in duplicate_rows]
            raise ETLHelperArchiveError(
                f"Event ids {duplicates} appear more than once among the "
                "participating details"
            )


def _apply_manifest(
    manifest: list[ManifestEntry],
    details: dict[int, DetailSet],
    spool: sqlite3.Connection,
    dest_conn: sqlite3.Connection,
    chunk_size: int,
) -> ArchiveReceipt:
    """
    Judge every released batch against the pre-archive business state and
    apply the accepted ones.

    Each round applies the still-active batches with their events merged and
    ordered by event id (the event id determines business sequence, not the
    result order).  When the target rejects an event for a business rule the
    whole owning batch is excluded, all writes of the round are undone and the
    remaining batches are re-judged from the pre-archive state.  All target
    writes happen inside a savepoint, keeping them independent of any changes
    the caller already made in the same transaction.
    """
    # A plant can own several settlement batches, so batches are keyed by their
    # manifest row position rather than by plant name.
    plant_by_index: dict[int, str] = {
        index: entry.plant
        for index, entry in enumerate(manifest)
        if entry.released
    }
    batch_by_detail: dict[int, int] = {}
    kind_by_detail: dict[int, str] = {}
    for index, entry in enumerate(manifest):
        if not entry.released:
            continue
        if entry.production_detail is not None:
            batch_by_detail[entry.production_detail] = index
            kind_by_detail[entry.production_detail] = "production"
        if entry.cancellation_detail is not None:
            batch_by_detail[entry.cancellation_detail] = index
            kind_by_detail[entry.cancellation_detail] = "cancellation"

    active_indices = list(plant_by_index.keys())
    rejections: dict[int, tuple[Any, str]] = {}
    dest_cursor = dest_conn.cursor()
    dest_cursor.execute(f"SAVEPOINT {SAVEPOINT}")

    try:
        while active_indices:
            dest_cursor.execute(f"ROLLBACK TO SAVEPOINT {SAVEPOINT}")
            try:
                _apply_events(
                    _iter_ordered_events(
                        spool,
                        batch_by_detail,
                        plant_by_index,
                        set(active_indices),
                        chunk_size,
                    ),
                    kind_by_detail,
                    dest_cursor,
                )
            except _BusinessRejection as rejection:
                # A trigger rule fired: reject the whole batch and re-judge the
                # remaining batches against the pre-archive state.
                rejections[rejection.batch_index] = (
                    rejection.event_id,
                    rejection.reason,
                )
                active_indices = [
                    index
                    for index in active_indices
                    if index != rejection.batch_index
                ]
            except sqlite3.Error as exc:
                raise ETLHelperArchiveError(
                    "Non-business write error during the settlement archive: "
                    f"{exc}"
                ) from exc
            else:
                break
        else:
            # Every batch was rejected: the final round still holds rows
            # inserted before the rule fired, so undo them before releasing.
            dest_cursor.execute(f"ROLLBACK TO SAVEPOINT {SAVEPOINT}")

        # A successful round already holds the final state; otherwise the
        # rollback above removed every archive write.  Releasing now leaves
        # only the caller's earlier changes in the transaction.
        dest_cursor.execute(f"RELEASE SAVEPOINT {SAVEPOINT}")
    except Exception:
        # Discard every archive write while keeping the caller's transaction
        # (and any earlier changes such as a site correction) usable.
        try:
            dest_cursor.execute(f"ROLLBACK TO SAVEPOINT {SAVEPOINT}")
            dest_cursor.execute(f"RELEASE SAVEPOINT {SAVEPOINT}")
        except sqlite3.Error:
            pass
        raise
    finally:
        dest_cursor.close()

    accepted_indices = [
        index
        for index, entry in enumerate(manifest)
        if entry.released and index not in rejections
    ]
    return ArchiveReceipt(
        _build_receipts(manifest, details, accepted_indices, rejections)
    )


class _BusinessRejection(Exception):
    """An insert was rejected by one of the target business rules."""

    def __init__(
        self, batch_index: int, plant: str, event_id: Any, reason: str
    ):
        super().__init__(reason)
        self.batch_index = batch_index
        self.plant = plant
        self.event_id = event_id
        self.reason = reason


def _iter_ordered_events(
    spool: sqlite3.Connection,
    batch_by_detail: dict[int, int],
    plant_by_index: dict[int, str],
    active_indices: set[int],
    chunk_size: int,
):
    """
    Yield ``(batch_index, plant, event_id, valid_batch_no, detail_number)``
    tuples for the active batches, merged across their details and ordered by
    event id.

    Rows are pulled from the spool in chunks, so the number of detail rows held
    in Python stays bounded by chunk_size regardless of total volume.
    """
    detail_numbers = [
        number
        for number, batch_index in batch_by_detail.items()
        if batch_index in active_indices
    ]
    if not detail_numbers:
        return

    placeholders = ",".join("?" for _ in detail_numbers)
    result = spool.execute(
        f"SELECT detail_number, event_id, valid_batch_no FROM detail_rows "
        f"WHERE detail_number IN ({placeholders}) "
        f"ORDER BY event_id, row_seq",
        detail_numbers,
    )
    while True:
        chunk = result.fetchmany(chunk_size)
        if not chunk:
            break
        for detail_number, event_id, valid_batch_no in chunk:
            batch_index = batch_by_detail[detail_number]
            yield (
                batch_index,
                plant_by_index[batch_index],
                event_id,
                valid_batch_no,
                detail_number,
            )


def _apply_events(events, kind_by_detail, dest_cursor) -> None:
    """Apply events in their given order, converting rule failures."""
    for batch_index, plant, event_id, valid_batch_no, detail_number in events:
        is_cancellation = kind_by_detail[detail_number] == "cancellation"
        insert_sql = (
            _INSERT_CANCELLATION_SQL
            if is_cancellation
            else _INSERT_PRODUCTION_SQL
        )
        try:
            dest_cursor.execute(
                insert_sql, (event_id, plant, valid_batch_no)
            )
        except sqlite3.IntegrityError as exc:
            raise _BusinessRejection(
                batch_index, plant, event_id, str(exc)
            ) from exc


def _build_receipts(
    manifest: list[ManifestEntry],
    details: dict[int, DetailSet],
    accepted_indices: list[int],
    rejections: dict[int, tuple[Any, str]],
) -> list[BatchReceipt]:
    """Build one receipt per manifest row."""
    accepted_set = set(accepted_indices)
    receipts: list[BatchReceipt] = []
    for index, entry in enumerate(manifest):
        production_actual = _detail_row_count(details, entry.production_detail)
        cancellation_actual = _detail_row_count(
            details, entry.cancellation_detail
        )

        if not entry.released:
            production_status = STATUS_SKIPPED
            cancellation_status = STATUS_SKIPPED
            accepted = False
        elif index in accepted_set:
            production_status = _detail_status(
                entry.production_detail, production_actual
            )
            cancellation_status = _detail_status(
                entry.cancellation_detail, cancellation_actual
            )
            accepted = True
        else:
            # A rejected batch still reports what each detail actually was:
            # a returned zero-row detail is "empty" rather than "rejected",
            # keeping it distinct from an explicit not-returned marker.
            production_status = (
                STATUS_NOT_RETURNED
                if entry.production_detail is None
                else STATUS_EMPTY
                if production_actual == 0
                else STATUS_REJECTED
            )
            cancellation_status = (
                STATUS_NOT_RETURNED
                if entry.cancellation_detail is None
                else STATUS_EMPTY
                if cancellation_actual == 0
                else STATUS_REJECTED
            )
            accepted = False

        rejected_event = None
        rejection_reason = None
        if index in rejections:
            rejected_event, rejection_reason = rejections[index]

        receipts.append(BatchReceipt(
            plant=entry.plant,
            released=entry.released,
            production_detail=entry.production_detail,
            production_expected_rows=entry.production_expected_rows,
            production_actual_rows=production_actual,
            production_status=production_status,
            cancellation_detail=entry.cancellation_detail,
            cancellation_expected_rows=entry.cancellation_expected_rows,
            cancellation_actual_rows=cancellation_actual,
            cancellation_status=cancellation_status,
            accepted=accepted,
            rejected_event_id=rejected_event,
            rejection_reason=rejection_reason,
        ))
    return receipts


def _detail_row_count(
    details: dict[int, DetailSet], detail_number: Optional[int]
) -> int:
    if detail_number is None:
        return 0
    return details[detail_number].row_count


def _detail_status(
    detail_number: Optional[int], actual_rows: int
) -> str:
    if detail_number is None:
        return STATUS_NOT_RETURNED
    if actual_rows == 0:
        return STATUS_EMPTY
    return STATUS_APPLIED
