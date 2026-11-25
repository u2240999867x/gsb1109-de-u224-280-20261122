Generating SQL Scripts (a.k.a. "Offline Mode")
==============================================

A major capability of Alembic is to generate migrations as SQL scripts, instead of running
them against the database - this is also referred to as *offline mode*.
This is a critical feature when working in large organizations
where access to DDL is restricted, and SQL scripts must be handed off to DBAs.   Alembic makes
this easy via the ``--sql`` option passed to any ``upgrade`` or ``downgrade`` command.   We
can, for example, generate a script that revises up to rev ``ae1027a6acf``::

    $ alembic upgrade ae1027a6acf --sql
    INFO  [alembic.context] Context class PostgresqlContext.
    INFO  [alembic.context] Will assume transactional DDL.
    BEGIN;

    CREATE TABLE alembic_version (
        version_num VARCHAR(32) NOT NULL
    );

    INFO  [alembic.context] Running upgrade None -> 1975ea83b712
    CREATE TABLE account (
        id SERIAL NOT NULL,
        name VARCHAR(50) NOT NULL,
        description VARCHAR(200),
        PRIMARY KEY (id)
    );

    INFO  [alembic.context] Running upgrade 1975ea83b712 -> ae1027a6acf
    ALTER TABLE account ADD COLUMN last_transaction_date TIMESTAMP WITHOUT TIME ZONE;

    INSERT INTO alembic_version (version_num) VALUES ('ae1027a6acf');

    COMMIT;


While the logging configuration dumped to standard error, the actual script was dumped to standard output -
so in the absence of further configuration (described later in this section), we'd at first be using output
redirection to generate a script::

    $ alembic upgrade ae1027a6acf --sql > migration.sql

Getting the Start Version
--------------------------

Notice that our migration script started at the base - this is the default when using offline
mode, as no database connection is present and there's no ``alembic_version`` table to read from.

One way to provide a starting version in offline mode is to provide a range to the command line.
This is accomplished by providing the "version" in ``start:end`` syntax::

    $ alembic upgrade 1975ea83b712:ae1027a6acf --sql > migration.sql

The ``start:end`` syntax is only allowed in offline mode; in "online" mode, the ``alembic_version``
table is always used to get at the current version.

It's also possible to have the ``env.py`` script retrieve the "last" version from
the local environment, such as from a local file.   A scheme like this would basically
treat a local file in the same way ``alembic_version`` works::

    if context.is_offline_mode():
        version_file = os.path.join(os.path.dirname(config.config_file_name), "version.txt")
        if os.path.exists(version_file):
            current_version = open(version_file).read()
        else:
            current_version = None
        context.configure(dialect_name=engine.name, starting_rev=current_version)
        context.run_migrations()
        end_version = context.get_revision_argument()
        if end_version and end_version != current_version:
            open(version_file, 'w').write(end_version)

Writing Migration Scripts to Support Script Generation
------------------------------------------------------

The challenge of SQL script generation is that the scripts we generate can't rely upon
any client/server database access.  This means a migration script that pulls some rows
into memory via a ``SELECT`` statement will not work in ``--sql`` mode.   It's also
important that the Alembic directives, all of which are designed specifically to work
in both "live execution" as well as "offline SQL generation" mode, are used.

Customizing the Environment
---------------------------

Users of the ``--sql`` option are encouraged to hack their ``env.py`` files to suit their
needs.  The ``env.py`` script as provided is broken into two sections: ``run_migrations_online()``
and ``run_migrations_offline()``.  Which function is run is determined at the bottom of the
script by reading :meth:`.EnvironmentContext.is_offline_mode`, which basically determines if the
``--sql`` flag was enabled.

For example, a multiple database configuration may want to run through each
database and set the output of the migrations to different named files - the :meth:`.EnvironmentContext.configure`
function accepts a parameter ``output_buffer`` for this purpose.  Below we illustrate
this within the ``run_migrations_offline()`` function::

    from alembic import context
    import myapp
    import sys

    db_1 = myapp.db_1
    db_2 = myapp.db_2

    def run_migrations_offline():
        """Run migrations *without* a SQL connection."""

        for name, engine, file_ in [
            ("db1", db_1, "db1.sql"),
            ("db2", db_2, "db2.sql"),
        ]:
            context.configure(
                        url=engine.url,
                        transactional_ddl=False,
                        output_buffer=open(file_, 'w'))
            context.execute("-- running migrations for '%s'" % name)
            context.run_migrations(name=name)
            sys.stderr.write("Wrote file '%s'" % file_)

    def run_migrations_online():
        """Run migrations *with* a SQL connection."""

        for name, engine in [
            ("db1", db_1),
            ("db2", db_2),
        ]:
            connection = engine.connect()
            context.configure(connection=connection)
            try:
                context.run_migrations(name=name)
                session.commit()
            except:
                session.rollback()
                raise

    if context.is_offline_mode():
        run_migrations_offline()
    else:
        run_migrations_online()



.. _staged_artifacts:

Staged Offline Artifacts
------------------------

The plain ``--sql`` output is a single script.  When that script is handed to
another team, a process stop in the middle of a revision can leave the
database in a state that is hard to reason about: some DDL may have landed
while the version table was not updated, or an object may exist with a
different definition than the deployment intended.

Alembic can instead compile a **staged offline artifact** with the
``--staged`` option::

    $ alembic upgrade head --sql --staged --output deploy.json

Compilation uses no target database connection.  Alembic runs the migration
scripts against an offline context, captures the SQL that would have been
emitted, and splits each revision into phases:

``transaction``
    Ordinary DDL and, for revisions with no autocommit phase, the version
    bookkeeping, all in one database transaction.  If anything fails, both
    the DDL and the progress update roll back, so a transactional phase is
    never considered complete on failure.

``autocommit``
    DDL that must execute outside of a transaction, such as PostgreSQL
    ``CREATE INDEX CONCURRENTLY``.  The preceding transaction is committed
    first; the following transaction begins only after the phase finishes.

``commit``
    Version bookkeeping in a new transaction, immediately after an
    autocommit phase.

Each phase carries read-only check conditions derived from the compiled
definition.  Checks are plain ``SELECT`` statements returning a single
value:

``exists``
    Applied when the value is truthy.

``absent``
    Applied when the value is falsey (the object is gone).

``definition``
    Applied only when the value exactly equals the definition compiled into
    the artifact.  Any other non-empty value is a *conflict*.

``version``
    Applied when the version table exactly matches a declared boundary.

A migration that uses hand-written SQL can declare its check with
:meth:`.MigrationContext.declare_staged_check`::

    def upgrade():
        op.execute("CREATE TABLE audit_log (id INTEGER PRIMARY KEY)")
        op.get_context().declare_staged_check(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'audit_log'",
            kind="exists",
        )

Replaying the Artifact
~~~~~~~~~~~~~~~~~~~~~~

The artifact is deliberately self-contained and does not depend on a Python
project checkout.  It is a JSON document; a generic external runner reads it
and reconciles it against the database::

    $ alembic staged-check deploy.json --url "$DATABASE_URL"
    $ alembic staged-apply deploy.json --url "$DATABASE_URL"

``staged-check`` reports, for every phase, whether it is ``not_applied``,
``applied``, or in ``conflict``, and writes nothing.  ``staged-apply``
evaluates **all** checks for a revision before writing anything for that
revision, then:

* skips phases that report ``applied``;
* executes ``not_applied`` phases in order;
* for a non-transactional backend, completes a phase whose DDL is already
  present but whose version update was not written, after confirming the
  effect check;
* aborts with a :exc:`.StagedConflictError` naming the offending revision and
  phase if any check reports a conflict.

Version advancement is always guarded: the effect checks for a phase run
before the version table is updated, and the update is verified afterwards.

To stop at a phase boundary, for example to demonstrate recovery, use
``--stop-after``::

    $ alembic staged-apply deploy.json --url "$DATABASE_URL" --stop-after 2

Operator serialization
~~~~~~~~~~~~~~~~~~~~~~

Staged artifacts do not add cross-process locking.  Operators are expected to
run migrations serially against a given database.  The runner's version
boundary check prevents an artifact from being applied to a database whose
version is outside the artifact's declared ranges, but concurrent runs from
separate artifacts are not coordinated.

The artifact can also be rendered to plain SQL for review with
``alembic staged-sql deploy.json``.  The rendering preserves the phase
boundaries, placing autocommit phases between ``COMMIT`` and the following
``BEGIN``.
