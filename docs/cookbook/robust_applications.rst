
Robust Applications
===================

This section contains options and 
design patterns for creating robust
applications that has monitors,
persistence and tolerance for errors.

Silence Errors
--------------

One might not want to silence errors in 
development or in test but having error
tolerance in production could be useful.

By default, errors caused by the conditions
or tasks outside their execution crash the
system. However, you can silence those 
errors:

.. code-block:: python

    from rocketry import Rocketry
    from rocketry.log import LogRecord

    app = Rocketry(config={
        'silence_task_prerun': True,
        'silence_task_logging': True,
        'silence_cond_check': True
    })

.. note::

    The errors and their tracebacks are still logged to 
    logger called ``rocketry.scheduler`` even if they are 
    silenced. Setting this logger up helps with diagnosing
    the errors.

.. warning::

    You should first address the errors than silence them. 
    Silencing task logging errors can cause the scheduler to
    be not in sync with the real events.


Log to Database
---------------

Sometimes your application might crash.
If this happens you might want to continue
where it was left previously. You need to
store the log records to a file or to a 
database to achieve this.

Task logging is done using `Red Bird's <https://red-bird.readthedocs.io>`_
repositories. You can use any repository supported
by the library or create your own. In this section
we will go through some of them. Please consult
Red Bird's documentation for more.

To CSV
^^^^^^

Logging to CSV files is a good option
for simple projects: 

.. code-block:: python

    from redbird.repos import SQLRepo
    from rocketry.log import RunRecord

    repo = CSVFileRepo(model=RunRecord, filename="tasks.csv")
    repo.create()

    app.session.set_repo(repo, delete_existing=True)

To SQL
^^^^^^

Logging to relational database is a good 
option for larger projects that:

.. code-block:: python

    from redbird.repos import SQLRepo
    from rocketry.log import RunRecord
    from sqlalchemy import create_engine

    engine = create_engine('sqlite://')
    engine.execute("""CREATE TABLE log (
        id INTEGER PRIMARY KEY,
        created FLOAT,
        task_name TEXT,
        run_id TEXT,
        action TEXT
    )""")

    repo = SQLRepo(model=RunRecord, table="log", engine=engine, id_field="id")

    app.session.set_repo(repo, delete_existing=True)

.. warning::

    SQLAlchemy's `ORM requires primary key <https://stackoverflow.com/a/23771348/13696660>`_.
    It is adviced to create incremential index as ``created`` could have multiple same values
    if your system is fast enough.  

.. warning::

    If logging the task logs fail, the scheduler might not be 
    in sync. Often the errors are caused by temporary connection 
    failures and it might be useful to retry the log insertion:

    .. code-block:: python

        from functools import wraps

        def retry_func(func):

            @wraps(func)
            def wrapper(*args, **kwargs):
                err = None
                for i in range(3):
                    try:
                        return func(*arg, **kwargs)
                    except Exception as exc:
                        err = exc
                raise err
            return wrapper

        repo = SQLRepo(model=LogRecord, table="log", engine=engine, id_field="id")
        repo.emit = retry_func(repo.emit)

Logging Task Errors
-------------------

Logging errors is often cruicial for diagnostic 
purposes and for quickly addressing failures. 
Because Rocketry simply extends logging library,
you can direct the log task records to anywhere you 
wish.

Error Emails
^^^^^^^^^^^^

Recommended way to send errors via email is to use
`Red Mail's <https://red-mail.readthedocs.io>`_
email handler and add it to the logger that handles
the task logs. Red Mail is an advanced email sending
library created by Rocketry's author. 

.. code-block:: python

    import logging
    from redmail.log import EmailHandler

    handler = EmailHandler(
        host="localhost",
        port=0,
        sender="no-reply@example.com",
        receivers=["me@example.com"],

        subject="Task failed",
        html="""
            <h2>Task failed: {{ record.task_name }}</h2>
            <code><pre>{{ record.exc_text }}</pre></code>
        """,
    )
    handler.setLevel(logging.ERROR)

    task_logger = logging.getLogger("rocketry.task")
    task_logger.addHandler(handler)

First we created a logging handler that sends emails, then 
we set the level of this handler to log errors only and then
we set this handler to Rocketry's task logger. 

.. warning::

    Sometimes email sending might fail due to connection
    problems. It might be safer to wrap the ``emit``
    method with a try-except block.
    
Logging Scheduler
-----------------

You can also setup logging for the scheduler. This could 
be useful for additional diagnostics.

.. code-block:: python

    import logging

    sched_logger = logging.getLogger("rocketry.scheduler")
    sched_logger.addHandler(logging.StreamHandler())

Retry Failed
------------

Sometimes you might want to retry a failed task.
Of course, not all tasks are safe to retry
but some might. For example, 

.. code-block:: python

    from rocketry.conds import retry, daily

    @app.task(daily | retry(3))
    def do_things():
        ...

The above runs once a day but it
will retry maximum of three times if it fails.

However, sometimes the task
might run hours before it fails thus it might
be useful to force a time window in which 
the task is allowed to run:

.. code-block:: python

    from rocketry.conds import retry, daily, time_of_day

    @app.task((daily | retry(3)) & time_of_day.between("10:00", "12:00"))
    def do_things():
        ...

The above runs once a day between 10:00 and 12:00. It will also retry
maximum of three times if it fails and time is still between 10:00 and 12:00.

Partitioned Recovery
^^^^^^^^^^^^^^^^^^^^

If a task collects data for several partitions (for example a nightly
collection split into north and south), the ordinary retry counter cannot
tell which partition actually failed, and a task log rotation or a scheduler
restart easily loses the run that should be backfilled.

``rocketry.recovery`` adds an opt-in layer for this.  Every partition keeps
its own declaration batch, the receipts seen for each run and the position
where the task log was last read.  Only a confirmed failure (a failure
receipt) of the *current* batch is eligible for a backfill.  Runs without an
end receipt stay ``pending`` (待确认) and are never backfilled.  Old batches
can still receive late results for auditing, but cannot start a new backfill.

.. code-block:: python

    from rocketry.conditions import Retry
    from rocketry.recovery import Recovery, Receipt

    recovery = Recovery(path="recovery.json")
    recovery.declare_batch("north", "A")
    recovery.declare_batch("south", "B")

    @app.task(Retry(recovery=recovery))
    def collect():
        ...

Receipts can be fed with :meth:`PartitionRecovery.process_receipt` /
:meth:`PartitionRecovery.consume_log`.  A log record carrying ``partition``,
``batch`` and ``run_id`` is also recognized by
:meth:`PartitionRecovery.ingest_task_log`, which reads only the continuation
after the stored cursor.

Backfill results are stored through an idempotent ledger.  The business
transaction id identifies the dispatch; inserting the same id twice (or
submitting another id for a run that already has a transaction) returns the
original success receipt instead of creating a second row.  The success
receipt records ``original_run_id`` so the backfilled run can be traced, and
it is written back to the Rocketry task log by ``Retry`` once the work is
done.

.. code-block:: python

    receipt = recovery.insert_backfill(
        transaction_id="biz-B",
        partition="south",
        batch="B",
        run_id="run-b-17",
    )

    # After an unexpected shutdown, finish registered dispatches and write
    # success receipts that were not yet logged.
    recovery.resume()

The dedicated condition :class:`rocketry.recovery.PartitionRetry` is a
drop-in alternative.  Tasks without ``recovery`` keep the original ``Retry``
behaviour unchanged.

