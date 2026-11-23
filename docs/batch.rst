Batch Synchronisation
=====================

``redbird.batch`` coordinates the common workflow where a batch is staged in
a local SQL database and each resource is then marked as processed in a REST
source.  It is built for restarts: an interrupted run continues from the batch
and operation identifiers, and a separate pass can withdraw only the changes
that still belong to that batch.

The two stores keep different guarantees:

- **REST** writes the batch id and operation id in one version-conditional
  request.  A conditional update either writes both fields or fails with a
  version conflict, so a resource cannot be claimed by two batches silently.
- **SQLite** writes the data row and registers the operation in the same
  transaction.  A committed row therefore always has a ledger entry that a
  later resume can find.

The ledger stores one entry per ``(batch, operation, resource)`` with the
final outcome:

- ``retained``: the local value is still the batch's value (or was manually
  corrected and deliberately kept) and the REST marker belongs to the batch.
- ``withdrawn``: both sides were restored because the batch still owned them.
- ``conflict``: another batch owns the REST marker, or the local row was
  changed by someone else, or one side disappeared.  A conflict is reported,
  not overwritten.
- ``pending``: a step could not be confirmed.  Running ``resume`` again
  continues it.

Creating a synchroniser
-----------------------

.. code-block:: python

    from redbird.batch import BatchItem, BatchSynchronizer, RESTBatchStore, SQLiteLedger

    ledger = SQLiteLedger(
        conn_string="sqlite:///sync.db",
        target_table="items",
        id_field="id",
    )
    ledger.ensure_schema()

    source = RESTBatchStore(
        "https://example.test/api/items",
        id_field="id",
        version_field="version",
        batch_field="batch_id",
        operation_field="operation_id",
    )
    sync = BatchSynchronizer(ledger=ledger, source=source)

If the batch markers use other field names, pass them to ``RESTBatchStore``.
``RESTBatchStore`` sends the version in ``If-Match`` by default.  APIs that
expect a version in the request body can use ``expected_version_field``; APIs
that use a bare version header can set ``quote_version=False``.  If the source
also has a boolean "processed" flag, pass ``processed_field="processed"``: the
batch id, operation id and flag are then written in one conditional request and
read back together.

The helpers can also be created from the existing repositories:

.. code-block:: python

    sync = BatchSynchronizer.from_repos(sql_repo, rest_repo)

Recording a batch
-----------------

.. code-block:: python

    sync.record("batch-2026-11-24", "op-1", [
        BatchItem("a", {"name": "Anna", "status": "processed"}),
        BatchItem("b", {"name": "Bob", "status": "processed"}),
    ])

``record`` writes all local rows and their ledger entries in one transaction.
Calling it again with the same batch and operation is idempotent.

Resuming after a stop
---------------------

.. code-block:: python

    report = sync.resume("batch-2026-11-24")

For each resource, ``resume`` reads the REST marker before doing anything:

- A marker already owned by this batch/operation is not written again.  This
  covers the case where the REST request succeeded but the caller lost the
  response.
- A marker owned by another batch is recorded as a conflict and left alone.
- A local row that no longer equals the batch's written value is treated as a
  manual correction.  The value is kept and the entry is still marked as
  retained when the REST marker belongs to the batch.
- An interrupted withdrawal is continued as a withdrawal, never re-marked.

Withdrawing a batch
-------------------

.. code-block:: python

    report = sync.withdraw("batch-2026-11-24")

Only rows that both sides still own are withdrawn.  The REST marker is
cleared conditionally first; the local row is reverted only if it still
matches the value the batch wrote.  The withdrawal intent and every stage are
persisted, so stopping between the REST and SQLite steps is recoverable:

- Intent is recorded before either side is touched.
- ``clearing`` is recorded before the conditional REST request.
- If the response is lost, the next pass checks the remote marker and
  continues clearing or reverts the local row.

Rows already taken over by another batch, or changed locally, are recorded as
conflicts and neither side is modified.

Querying outcomes
-----------------

.. code-block:: python

    sync.outcome("batch-2026-11-24", "a", operation_id="op-1")
    sync.status("batch-2026-11-24")       # resource id -> BatchEntry
    sync.summary("batch-2026-11-24")      # {'retained': 2, 'withdrawn': 0, ...}

    ledger.entries("batch-2026-11-24", outcome="conflict")

The ledger is a normal SQLite table named ``_redbird_batch_entries`` by
default; its ``batch_id``, ``operation_id`` and ``resource_id`` columns are
JSON-normalised so integers, strings and other identifiers remain distinct
and readable.

Failure injection
-----------------

``RESTBatchStore`` raises:

- ``BatchTransportError`` when no HTTP response was received.  The operation
  stays pending because the request may have been applied.
- ``BatchVersionConflict`` when the conditional update fails (HTTP 409, 412
  or 428).
- ``BatchResourceNotFound`` for HTTP 404.
- ``BatchRequestError`` for other unexpected responses.

These exceptions are available from ``redbird.batch``.
