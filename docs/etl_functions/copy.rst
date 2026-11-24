Copy
^^^^

ETL Helper provides three ways to copy data from one database to another.
These are presented in order of increased control or customisability.

copy_table_rows
---------------

:func:`copy_table_rows() <etlhelper.copy_table_rows>` provides a simple way
to copy all the data from one table to another.
It can take a ``transform`` function in case some modification of the data,
e.g. change of case of column names, is required.

.. literalinclude:: ../code_demos/copy/demo_copy_table_rows.py
   :language: python

The ``chunk_size``, ``commit_chunks`` and ``on_error`` parameters can
all be set. A tuple with counts of rows processed and failed is
returned.

Note that the target table must already exist. If it doesn’t, you can
use ``execute`` with a ``CREATE TABLE IF NOT EXISTS ...`` statement to
create it first. See the recipes for examples.

Combining ``iter_rows`` with ``load``
-------------------------------------

For extra control selecting the data to be transferred, ``iter_rows``
can be combined with ``load``.

.. literalinclude:: ../code_demos/copy/demo_copy_iter_rows.py
   :language: python

copy_rows
---------

Customising both queries gives the greatest control on data selection
and loading.
``copy_rows`` takes the results from a SELECT query and applies them as
parameters to an INSERT query.
The source and destination tables must already exist.
For example, here we use GROUP BY and WHERE in the SELECT query and insert extra
auto-generated values via the INSERT query.

.. literalinclude:: ../code_demos/copy/demo_copy_rows.py
   :language: python

``parameters`` can be passed to the SELECT query as before and the
``commit_chunks``, ``chunk_size`` and ``on_error`` options can be set.

A tuple of rows processed and failed is returned.

Archiving large attachments
---------------------------

``copy_rows`` can optionally archive large attachments (e.g. Oracle BLOB
locators) into an attachment table in a destination SQLite database,
storing identical content only once.  Pass an
:class:`AttachmentArchive <etlhelper.attachments.AttachmentArchive>`
describing the attachment table and one
:class:`AttachmentColumn <etlhelper.attachments.AttachmentColumn>` per
attachment position::

    from etlhelper import AttachmentArchive, AttachmentColumn, copy_rows

    attachments = AttachmentArchive(
        table="attachments",
        columns=(
            AttachmentColumn(locator="att1_locator", target="attachment1_id",
                             size="att1_size", sha256="att1_sha"),
            AttachmentColumn(locator="att2_locator", target="attachment2_id",
                             size="att2_size", sha256="att2_sha"),
        ),
    )

    copy_rows(select_sql, oracle_conn, insert_sql, sqlite_conn,
              attachments=attachments, on_error=my_error_handler)

Each non-null locator is read fully via its 1-based
``read(offset, amount)`` method and verified against the expected byte
count and, if given, the expected SHA-256 digest.  Bodies are streamed in
1 MiB chunks and written with SQLite incremental BLOB I/O, so attachments
larger than 256 MiB never need to be held in memory.  A NULL locator
stores a NULL reference, while a zero-length attachment stores a reference
to a shared zero-byte body.

In this mode the destination connection is never committed or rolled back
(``commit_chunks`` is ignored).  Each row is processed inside a savepoint:
rows whose attachments fail verification are reported via ``on_error``
(with :class:`ETLHelperAttachmentLengthError
<etlhelper.exceptions.ETLHelperAttachmentLengthError>` or
:class:`ETLHelperAttachmentDigestError
<etlhelper.exceptions.ETLHelperAttachmentDigestError>`) and leave no
complete or partial bodies behind, while the caller's surrounding
transaction is preserved.
