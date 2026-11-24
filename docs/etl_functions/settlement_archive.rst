Settlement archive
^^^^^^^^^^^^^^^^^^

Functions
---------

:func:`archive_settlement() <etlhelper.archive_settlement>` archives the output
of a SQL Server daily-close stored procedure that returns several result sets.
The procedure is executed exactly once by the caller; the function only
advances the given cursor through its result chain, so it works with any DBAPI
cursor that supports ``nextset()``.

Result sets fall into three categories:

- result sets with a column description are numbered details in order of
  appearance, and an empty detail still takes a number;
- result sets without a column description only report an affected row count
  and take no number;
- the trailing manifest has its own column description and maps each
  settlement batch to its production and cancellation detail numbers and
  expected row counts.  A ``NULL`` (or non-positive) detail reference marks
  that the detail was not returned.

Details are spooled into a temporary SQLite database while the chain is read,
so the number of rows held in Python is bounded by ``chunk_size`` regardless
of the total settlement volume.  Nothing is written to the target until the
whole chain has been read and the manifest has been validated.

Only released batches participate.  Their events are merged and applied in
event-id order.  When a target business rule rejects an event, the whole
owning batch is rejected and the remaining batches are re-judged against the
pre-archive state.  All writes run inside a savepoint, so the archive can be
committed or rolled back together with other changes the caller already made
in the same transaction.

.. code:: python

    import sqlite3
    from etlhelper import archive_settlement

    cursor = source_conn.cursor()
    cursor.execute("EXEC dbo.daily_close")  # executed exactly once

    with sqlite3.connect("settlement.db") as dest_conn:
        dest_conn.execute(
            "UPDATE site SET name = ? WHERE id = ?", ("new name", 1)
        )
        receipt = archive_settlement(cursor, dest_conn, chunk_size=1000)
        dest_conn.commit()

    for batch in receipt.batches:
        print(batch.plant, batch.accepted, batch.production_status,
              batch.cancellation_status, batch.rejected_event_id,
              batch.rejection_reason)

``receipt.batches`` contains one :class:`BatchReceipt
<etlhelper.BatchReceipt>` per manifest row.  Returned zero-row details are
reported as ``empty`` and details explicitly marked as missing are reported as
``not_returned``.  Use :class:`ArchiveReceipt
<etlhelper.ArchiveReceipt>` properties ``accepted``, ``rejected`` and
``skipped`` to group the batches.  A mismatch between the manifest and the
actual results, a described result after the manifest, or a failure while
reading or advancing the result chain raises
:class:`ETLHelperArchiveError <etlhelper.exceptions.ETLHelperArchiveError>`
and leaves no business rows behind.
