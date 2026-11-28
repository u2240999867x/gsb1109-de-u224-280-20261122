.. _athena-work-orders:

#############################
Resumable Athena work orders
#############################

Athena only makes ``ClientRequestToken`` idempotent inside an *execution
target*.  A target is the caller-registered Athena backend identifier
combined with the destination to which the request is actually sent.  Two
backends can therefore recognize the same token independently, and a token
that owns a work order can only be resumed against the target that first
accepted it.

Botocore exposes an opt-in work-order envelope around
``StartQueryExecution``.  The envelope records the work-order identity,
the token owner, the pinned target, and the query execution ID on disk so a
later process can recover the original submission.  Calls that do not use
the envelope keep the normal SDK send and retry behavior.

Registering backends
====================

Each backend identifier maps to the endpoint where that backend receives
requests::

    client.register_athena_backend(
        'west', 'https://west.athena.example.com'
    )
    client.register_athena_backend(
        'east', 'https://east.athena.example.com'
    )

The store defaults to ``~/.aws/boto/cache/athena-work-orders``.  Override
it per client with ``Config(athena_work_order_cache=...)`` or with
``client.set_athena_work_order_cache(path)``.

Submitting a work order
=======================

::

    response = client.start_query_execution_work_order(
        QueryString='SELECT * FROM events WHERE ds = ? AND zone = ?',
        ExecutionParameters=["'2026-11-22'", "'north'"],
        ClientRequestToken=planner_token,
        WorkOrder={
            'TicketId': '17',
            'Line': 'north',
            'AthenaBackendId': 'west',
        },
    )

The work-order identity is the ``Line`` and ``TicketId`` pair; a ticket
only needs to be unique within its line.  The ``ClientRequestToken`` is
the work order's token at its execution target.  When the caller supplies
a token it is used exactly, including tokens shorter than Athena's modeled
minimum.

Resumption and conflicts
========================

After a request may have been accepted, the destination is pinned to the
target recorded in the store.  Retries and later submissions for the same
work order use that target, and once the query ID is known the SDK returns
it without sending another request.

Reusing a token that belongs to a different work order raises
:class:`botocore.exceptions.WorkOrderTokenConflictError`.  Resubmitting a
work order with a changed query, workgroup, parameters, or token raises
:class:`botocore.exceptions.WorkOrderConfigurationConflictError`.  Both
expose ``original_work_order_id``, ``original_ticket_id``, and
``original_line`` so callers can route the conflict back to the owning
line.

``QueryExecutionContext`` key order is irrelevant to conflict detection.
After a process restart, resubmitting the work order against its pinned
backend returns the original ``QueryExecutionId``.
