.. _Insert:

Insert
======

This is used to bulk insert rows into the table:

.. code-block:: python

    await Band.insert(
        Band(name="Pythonistas"),
        Band(name="Darts"),
        Band(name="Gophers")
    )

-------------------------------------------------------------------------------

``add``
-------

If we later decide to insert additional rows, we can use the ``add`` method:

.. code-block:: python

    query = Band.insert(Band(name="Pythonistas"))

    if other_bands:
        query = query.add(
            Band(name="Darts"),
            Band(name="Gophers")
        )

    await query

-------------------------------------------------------------------------------

Query clauses
-------------

on_conflict
~~~~~~~~~~~

See :ref:`On_Conflict`.


returning
~~~~~~~~~

See :ref:`Returning`.

-------------------------------------------------------------------------------

Handover pages
--------------

``insert`` normally writes each row on its own. If rows exchange values
which must stay unique (for example, two devices swapping site numbers and
access ports), use ``insert_manifest``. It analyses the whole page first, so
resources can be temporarily unassigned during the swap:

.. code-block:: python

    result = await Device.insert_manifest(
        [
            Device(device_number="A", site_number="h2", access_port="p1"),
            Device(device_number="B", site_number="h1", access_port="p3"),
        ],
        primary_key="device_number",
        resources=[
            ["warehouse", "site_number"],
            ["warehouse", "access_port"],
        ],
    )

Each input row produces a receipt, in the same order:

.. code-block:: python

    for receipt in result:
        print(receipt.primary_key, receipt.accepted, receipt.rules)

Rows which share a site number or port are grouped together, and the group is
accepted or rejected as a unit. Rows which don't share any resource are
independent. If a group is rejected (for example, by a database trigger),
none of its assignments change; the page is written in a single transaction,
so other connections only see the state before or after it.

The same behaviour is available as ``insert(..., manifest=True)``, or via the
``insert_page`` / ``insert_batch`` aliases.

.. warning::

    ``insert_manifest`` matches existing rows by their primary key, or by a
    permanent identifier such as ``device_number``. It doesn't use site
    numbers or ports for matching, as those are the resources being handed
    over.
