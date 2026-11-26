.. _stdin:

Input via STDIN
===============

STDIN is sent to a process directly by using a command's :ref:`in` special
kwarg:

.. code-block:: python

	print(cat(_in="test"))
	
Any command that takes input from STDIN can be used this way:

.. code-block:: python

	print(tr("[:lower:]", "[:upper:]", _in="sh is awesome"))
	
You're also not limited to using just strings.  You may use a file object, a
:class:`queue.Queue`, or any iterable (list, set, dictionary, etc):

.. code-block:: python

	stdin = ["sh", "is", "awesome"]
	out = tr("[:lower:]", "[:upper:]", _in=stdin)

.. note::

    If you use a queue, you can signal the end of the queue (EOF) with ``None``

Multiplexed input
-----------------

When a single child process has to consume input from several named producers
— and those producers can be replaced while the child keeps running — wrap the
producers in an :class:`InputHub`:

.. code-block:: python

    hub = sh.InputHub()
    a = hub.attach("workstation-a")
    b = hub.attach("workstation-b")

    p = some_command(_in=hub)

    a.send(b"A1\n")
    b.send(b"B001\n")

    # the producer for a drops out; swap it without touching the child
    a_new = hub.attach("workstation-a")
    a_new.send(b"A2\n")

    a_new.end()
    b.end()

An attachment is one producer session for a name.  Attaching a name again
creates a new generation and supersedes the old attachment, but every chunk
that the old attachment already delivered stays accepted.  Late chunks, end
notifications and errors from the old producer remain attached to the old
generation; they are never written to the child nor counted against the new
attachment.

The four things a producer can report are distinct:

- not yet ready — return ``sh.NOT_READY`` (or raise
  :class:`NotYetReadyToRead` from a callback); the child keeps waiting
- an empty chunk — ``send(b"")``; this is a real chunk and is not EOF
- end of this attachment — ``end()``, ``None`` from a callback, or ``None`` in
  a queue; other attachments keep running
- source failure — ``fail(error)`` or an exception raised by a callback; it is
  reported on :attr:`InputHub.failures` for the attachment that failed

Chunks and failures can also arrive through a queue.  Put ``InputControl``
messages on :meth:`InputHub.put` (or attach a queue directly); a control
message carrying ``name`` and optionally ``generation`` is routed to exactly
that attachment, so a producer can address the generation it belongs to even
after the workbench has been re-attached:

.. code-block:: python

    hub.put(sh.InputControl.chunk(b"late\n", name="a", generation=1))
    hub.put(sh.InputControl.end(name="a", generation=1))
    hub.put(sh.InputControl.fail(RuntimeError("boom"), name="a"))

Every ``send``/``end``/``fail`` returns an :class:`InputReceipt` whose
``accepted`` flag says whether the event was taken for the current
attachment.  ``InputHub.events`` and ``InputHub.history`` expose the accepted
and stale events, and ``attachment.accepted_empty`` tells you whether an empty
chunk was accepted.  ``sh.INPUT_END`` is an explicit sentinel usable in a
queue in place of ``None`` when ``None`` has another meaning.

Existing single-source use of a :class:`queue.Queue` or a callable for ``_in``
is unchanged.

For convenience a mapping of names to producers can be passed directly as
``_in``; each value may be a queue, a callback, or a ``(queue, callback)``
pair:

.. code-block:: python

    proc = consumer(_in={
        "a": (queue_a, pull_from_a),
        "b": queue_b,
    })
