============================
Batch preview and approval
============================

Inbound documents often arrive as one ordered sheet: cold-room items and
quarantine items are listed together, and the warehouse is split only after
every line has been inspected.  Before splitting, an operator needs to know

- which number each line would receive,
- which bin each line would be assigned to, and
- which lines would be returned for review.

:mod:`mongoengine.approval` adds this batch workflow on top of
:class:`~mongoengine.fields.SequenceField` and the document ``clean`` hook.

A manifest
==========

A manifest is an ordered list of rows.  A row is either a document instance, a
``(DocumentClass, values)`` pair, or a mapping naming the model:

.. code-block:: python

    from mongoengine import Document, SequenceField, StringField, ValidationError
    from mongoengine.approval import preview_batch, approve_batch

    class AmbientItem(Document):
        id = SequenceField(primary_key=True)
        name = StringField()
        bin = StringField()

        def clean(self):
            if self.id == 42:
                raise ValidationError("number 42 is reserved")
            self.bin = "AMBIENT-%s" % self.id

    manifest = [
        {"document": AmbientItem, "fields": {"name": "A"}},
        {"document": AmbientItem, "fields": {"name": "B"}},
    ]

Rows that do not name their model can use ``default_document`` (or the
``Model.preview_batch(...)`` / ``Model.approve_batch(...)`` helpers):

.. code-block:: python

    preview = AmbientItem.preview_batch("APPROVAL-1", [{"name": "A"}])

Previewing
==========

:func:`~mongoengine.approval.preview_batch` simulates the whole manifest.  It
reads the counters, writes the candidate number into each in-memory document,
and runs ``validate(clean=True)``.  Nothing is written to MongoDB: counters,
collections and the source instances stay exactly as they were.

.. code-block:: python

    preview = preview_batch("APPROVAL-1", manifest)

    for row in preview:
        print(row.line, row.number, row.bin, row.status, row.error)

Each :class:`~mongoengine.approval.PreviewRow` has:

- ``number`` / ``numbers``: the sequence value(s) the row would receive;
- ``bin``: the first bin-like field set by ``clean`` (or ``bin_fields=``);
- ``status``: ``ready`` or ``rejected``;
- ``error``: the validation error for a rejected row.

A rejected row does not consume its candidate number: the following rows
continue from the same value.  Previewing the same manifest twice yields
identical numbers as long as nothing else moves the counters.

Approving
=========

:func:`~mongoengine.approval.approve_batch` repeats the simulation inside one
transaction, then writes

- the accepted documents,
- the counter increments, and
- the receipt (including the rejected rows)

atomically.  When the approval id was already committed with the same manifest,
the stored receipt is returned and neither documents nor counters are touched.
Reusing the approval id with a different manifest raises
:class:`~mongoengine.errors.ApprovalIDConflict`.

.. code-block:: python

    receipt = approve_batch("APPROVAL-1", preview)

    receipt.status          # "committed"
    receipt.stored          # rows written
    receipt.rejected        # rows returned for review
    receipt.numbers         # numbers recorded at approval time

Stale previews
==============

If the counters move between the preview and the approval (for example an old
collector takes a number), the recorded numbers no longer match.  The approval
is then returned for review instead of being written: the receipt status is
``needs_review``, counters are untouched and no documents are inserted.
Previewing again resolves the new numbers and a fresh approval can be
submitted.

Receipts and retries
====================

The receipt is written in the same transaction as the documents and counters.
A receipt sink passed as ``receipt_sink=`` (or ``deliver_receipt=``) is called
after the commit; a sink failure does not undo the approval.  Re-submitting the
same approval id and manifest returns the original receipt (``replayed`` is
``True``) with the original numbers, bins and rejected rows, even if counters
moved or bins were edited in the meantime.  Use
:func:`~mongoengine.approval.redeliver_receipt` to send a stored receipt again.

Multiple databases and connections
==================================

All documents, counters and the receipt must share one MongoDB client.
Documents may live in different databases of that client (for example the
ambient and quarantine warehouses), and their counters may use their own
``SequenceField(..., db_alias=...)``.  Transactions across databases of one
client are used when the deployment is a replica set; otherwise the approval
compensates a failed write by deleting the documents it inserted and restoring
the counters.

.. note::

   ``clean`` runs with the candidate number already assigned, so validations
   that depend on the number (and bin assignments derived from it) behave the
   same in the preview and in the approval.
