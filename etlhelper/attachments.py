"""
Optional large-attachment archiving for `copy_rows`.

Attachments are streamed from source locators (e.g. Oracle BLOB locators)
into an attachment table in the destination SQLite database.  Identical
content is stored only once, however many rows or attachment positions
reference it.

Locators are read via ``read(offset, amount)`` where ``offset`` is 1-based.
A read may return fewer bytes than requested; only empty ``bytes`` marks
the end of the content.  Locators are only readable while the source query
and connection are still valid, so each locator is read exactly once, as
its row is processed.

Attachment content may exceed 256 MiB, so it is never assembled into a
single Python ``bytes`` object.  Data are streamed in chunks of at most
``ATTACHMENT_CHUNK_SIZE`` (1 MiB) and written to the database via SQLite
incremental BLOB I/O (``zeroblob`` + ``blobopen``), keeping the total
Python-side buffer well below 8 MiB.

The destination connection is never committed or rolled back by the copy;
each row is processed inside a SAVEPOINT so that failed rows leave no
complete or partial bodies behind, while the caller's surrounding
transaction (and anything already registered in it) is preserved.
"""
import hashlib
import logging
from typing import (
    Any,
    NamedTuple,
    Optional,
)

from etlhelper.abort import raise_for_abort
from etlhelper.db_helper_factory import DB_HELPER_FACTORY
from etlhelper.etl import (
    FailedRow,
    iter_rows,
    validate_identifier,
)
from etlhelper.exceptions import (
    ETLHelperAttachmentDigestError,
    ETLHelperAttachmentError,
    ETLHelperAttachmentLengthError,
    ETLHelperInsertError,
)
from etlhelper.types import (
    Connection,
    Parameters,
    Row,
)

logger = logging.getLogger('etlhelper')

# Attachments are streamed in 1 MiB chunks so that even attachments larger
# than 256 MiB never require more than a couple of MiB of Python buffers.
ATTACHMENT_CHUNK_SIZE = 1024 * 1024

# Savepoint used to isolate the changes made for each row.
SAVEPOINT_NAME = "etlhelper_attachment_row"


class AttachmentColumn(NamedTuple):
    """
    Describe how one attachment position is mapped from source row fields
    to a reference column in the destination row.

    :param locator: name of the source field holding the locator object
                    (None/NULL means there is no attachment)
    :param target: name of the destination field that receives the
                   archived attachment reference (id)
    :param size: name of the source field holding the expected byte count
    :param sha256: name of the source field holding the expected SHA-256
                   hex digest, if available; the value may still be None
                   for individual rows
    """
    locator: str
    target: str
    size: str
    sha256: Optional[str] = None


class AttachmentArchive(NamedTuple):
    """
    Describe the attachment table used to store archived bodies.

    :param table: name of the attachment table
    :param columns: tuple of AttachmentColumn describing each attachment
                    position in the row
    :param id_column: name of the integer primary key column; its value is
                      used as the reference stored in the observation table
    :param body_column: name of the BLOB column holding the body
    :param size_column: name of the column holding the actual byte count
    :param sha256_column: name of the column holding the SHA-256 hex digest
    """
    table: str
    columns: tuple[AttachmentColumn, ...]
    id_column: str = "id"
    body_column: str = "body"
    size_column: str = "byte_count"
    sha256_column: str = "sha256"


def _read_locator(locator: Any, expected_size: int,
                  expected_sha256: Optional[str],
                  sink: Any = None) -> tuple[int, str]:
    """
    Read locator to its end in chunks, verifying the byte count and, if
    given, the SHA-256 digest.  Chunks are passed to sink(data) if provided.

    :param locator: locator object with 1-based read(offset, amount)
    :param expected_size: expected number of bytes
    :param expected_sha256: expected SHA-256 hex digest or None
    :param sink: callable accepting each chunk of bytes, or None
    :return: (actual byte count, actual SHA-256 hex digest)
    :raises ETLHelperAttachmentLengthError: if the actual byte count does
                                            not match expected_size
    :raises ETLHelperAttachmentDigestError: if the actual digest does not
                                            match expected_sha256
    """
    hasher = hashlib.sha256()
    received = 0
    offset = 1  # locator offsets are 1-based

    while True:
        data = locator.read(offset, ATTACHMENT_CHUNK_SIZE)
        if not data:
            # Empty bytes mark the end of the content
            break

        received += len(data)
        if received > expected_size:
            msg = (f"Attachment content is longer than the expected "
                   f"{expected_size} bytes.")
            raise ETLHelperAttachmentLengthError(msg)

        if sink is not None:
            sink(data)
        hasher.update(data)
        offset += len(data)

    if received != expected_size:
        msg = (f"Attachment content ended after {received} bytes; expected "
               f"{expected_size} bytes.")
        raise ETLHelperAttachmentLengthError(msg)

    digest = hasher.hexdigest()
    if expected_sha256 is not None and digest != expected_sha256:
        msg = (f"Attachment digest {digest} does not match expected "
               f"{expected_sha256}.")
        raise ETLHelperAttachmentDigestError(msg)

    return received, digest


class _AttachmentArchiver:
    """
    Archive attachment bodies into the attachment table on conn, storing
    each distinct content only once.  No changes are committed; callers are
    expected to wrap each row in a savepoint and roll back on failure.
    """

    def __init__(self, conn: Connection, spec: AttachmentArchive):
        self.conn = conn
        self.spec = spec

        identifiers = [spec.table, spec.id_column, spec.body_column,
                       spec.size_column, spec.sha256_column]
        for column in spec.columns:
            identifiers.extend([column.locator, column.target, column.size])
            if column.sha256:
                identifiers.append(column.sha256)
        for identifier in identifiers:
            validate_identifier(identifier)

        self.helper = DB_HELPER_FACTORY.from_conn(conn)

    def archive(self, locator: Any, expected_size: Optional[int],
                expected_sha256: Optional[str]) -> Optional[int]:
        """
        Read locator fully, verify it and ensure its content is archived.
        Return the reference (attachment id) for the content, or None if
        locator is None.

        :param locator: locator object or None
        :param expected_size: expected number of bytes
        :param expected_sha256: expected SHA-256 hex digest or None
        :return: attachment id or None
        """
        if locator is None:
            return None

        if expected_sha256:
            expected_sha256 = str(expected_sha256).lower()
        else:
            expected_sha256 = None

        if expected_sha256 is not None:
            existing = self._find_by_sha256(expected_sha256)
            if existing is not None:
                # Content is already archived, but the locator must still be
                # read fully and verified.
                _read_locator(locator, expected_size, expected_sha256)
                logger.debug("Attachment already archived as %s", existing)
                return existing

        return self._archive_new(locator, expected_size, expected_sha256)

    def _find_by_sha256(self, digest: str) -> Optional[int]:
        """Return id of an archived body with the given digest, or None."""
        spec = self.spec
        query = (f"SELECT {spec.id_column} FROM {spec.table} "
                 f"WHERE {spec.sha256_column} = ?")
        with self.helper.cursor(self.conn) as cursor:
            cursor.execute(query, (digest,))
            row = cursor.fetchone()
        return row[0] if row else None

    def _archive_new(self, locator: Any, expected_size: int,
                     expected_sha256: Optional[str]) -> int:
        """
        Stream locator content into a new attachment row while hashing it,
        then verify and finalise the row.  If the content turns out to be
        archived already, the new row is removed and the existing reference
        returned.
        """
        spec = self.spec

        # Reserve the full body with zeroblob so content can be written
        # incrementally without ever being assembled in Python.
        insert_sql = (f"INSERT INTO {spec.table} "
                      f"({spec.body_column}, {spec.size_column}, "
                      f"{spec.sha256_column}) "
                      f"VALUES (zeroblob(?), ?, ?)")
        with self.helper.cursor(self.conn) as cursor:
            cursor.execute(insert_sql,
                           (expected_size, expected_size, expected_sha256 or ""))
            rowid = cursor.lastrowid

            # Write chunks into the reserved blob as they are read.  The
            # blob is closed before any error propagates so that the row
            # can be rolled back cleanly.
            blob = self.conn.blobopen(spec.table, spec.body_column, rowid)
            try:
                written = 0

                def sink(data: bytes) -> None:
                    nonlocal written
                    blob.seek(written)
                    blob.write(data)
                    written += len(data)

                actual_size, digest = _read_locator(
                    locator, expected_size, expected_sha256, sink=sink)
            finally:
                blob.close()

            existing = self._find_other_by_sha256(cursor, digest, rowid)
            if existing is not None:
                # Identical content is already archived; discard the copy
                # just written and share the existing body.
                cursor.execute(
                    f"DELETE FROM {spec.table} WHERE {spec.id_column} = ?",
                    (rowid,))
                logger.debug("Attachment already archived as %s", existing)
                return existing

            cursor.execute(
                f"UPDATE {spec.table} SET {spec.size_column} = ?, "
                f"{spec.sha256_column} = ? WHERE {spec.id_column} = ?",
                (actual_size, digest, rowid))

        logger.debug("Archived new attachment %s (%s bytes, sha256=%s)",
                     rowid, actual_size, digest)
        return rowid

    def _find_other_by_sha256(self, cursor: Any, digest: str,
                              exclude_id: int) -> Optional[int]:
        """Return id of another archived body with the given digest."""
        spec = self.spec
        query = (f"SELECT {spec.id_column} FROM {spec.table} "
                 f"WHERE {spec.sha256_column} = ? AND {spec.id_column} <> ?")
        cursor.execute(query, (digest, exclude_id))
        row = cursor.fetchone()
        return row[0] if row else None


def _archive_row_attachments(row: Row, spec: AttachmentArchive,
                             archiver: _AttachmentArchiver) -> dict:
    """
    Archive each attachment in row and return a new dict row where locator,
    size and digest fields are replaced by the archived references.

    :param row: source row (dict or namedtuple)
    :param spec: AttachmentArchive describing the attachment positions
    :param archiver: archiver for the destination connection
    :return: dict of values for the insert query
    """
    if hasattr(row, '_asdict'):
        data = dict(row._asdict())
    elif hasattr(row, 'keys'):
        data = dict(row)
    else:
        msg = f"Row is not a dictionary or namedtuple ({type(row)})"
        raise ETLHelperInsertError(msg)

    for column in spec.columns:
        locator = data.pop(column.locator)
        expected_size = data.pop(column.size)
        expected_sha256 = data.pop(column.sha256) if column.sha256 else None
        data[column.target] = archiver.archive(locator, expected_size,
                                               expected_sha256)

    return data


def copy_rows_with_attachments(
        select_query: str,
        source_conn: Connection,
        insert_query: str,
        dest_conn: Connection,
        spec: AttachmentArchive,
        parameters: Parameters = (),
        row_factory: Any = None,
        transform: Any = None,
        on_error: Any = None,
        chunk_size: int = 5000,
        ) -> tuple[int, int]:
    """
    Copy rows from source_conn to dest_conn, archiving attachments.

    See etlhelper.etl.copy_rows for the full documentation.  Each row is
    processed inside a SAVEPOINT: if archiving or inserting fails, all
    changes made for that row (including any new attachment bodies) are
    rolled back without disturbing the caller's surrounding transaction,
    and the row is reported via on_error before processing continues.

    :return: the number of rows processed and the number of rows failed
    """
    from etlhelper.row_factories import dict_row_factory
    if row_factory is None:
        row_factory = dict_row_factory

    logger.info("Copying rows with attachment archiving")
    helper = DB_HELPER_FACTORY.from_conn(dest_conn)
    archiver = _AttachmentArchiver(dest_conn, spec)
    processed = 0
    failed = 0

    rows = iter_rows(select_query, source_conn, parameters=parameters,
                     row_factory=row_factory, transform=transform,
                     chunk_size=chunk_size)

    with helper.cursor(dest_conn) as cursor:
        try:
            for row in rows:
                raise_for_abort(
                    "abort_etlhelper_threads() called during copy_rows")

                cursor.execute(f"SAVEPOINT {SAVEPOINT_NAME}")
                try:
                    new_row = _archive_row_attachments(row, spec, archiver)
                    cursor.execute(insert_query, new_row)
                except (*helper.sql_exceptions, ETLHelperAttachmentError) as exc:
                    # Undo everything done for this row (including any new or
                    # partially written bodies) without touching the caller's
                    # surrounding transaction.
                    cursor.execute(f"ROLLBACK TO SAVEPOINT {SAVEPOINT_NAME}")
                    cursor.execute(f"RELEASE {SAVEPOINT_NAME}")
                    failed += 1
                    logger.warning("Row failed during copy: %s", exc)
                    if on_error:
                        on_error([FailedRow(row, exc)])
                    else:
                        raise
                else:
                    cursor.execute(f"RELEASE {SAVEPOINT_NAME}")

                processed += 1
                logger.info('%s rows processed (%s failed)', processed, failed)
        finally:
            # Close the row generator (and hence the source cursor) even if
            # an error is propagating.
            rows.close()

    logger.info('%s rows processed in total', processed)
    return processed, failed
