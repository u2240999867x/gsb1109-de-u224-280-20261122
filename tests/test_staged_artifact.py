from __future__ import annotations

import json
import os

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text

from alembic import command
from alembic import util
from alembic.runtime import staged
from alembic.testing import assert_raises_message
from alembic.testing import eq_
from alembic.testing.env import _no_sql_testing_config
from alembic.testing.env import _sqlite_testing_config
from alembic.testing.env import clear_staging_env
from alembic.testing.env import staging_env
from alembic.testing.env import write_script
from alembic.testing.fixtures import TestBase
from alembic.script import ScriptDirectory


class StagedArtifactTestBase(TestBase):
    def setUp(self):
        staging_env()
        self.script = ScriptDirectory.from_config(
            _sqlite_testing_config()
        )

    def tearDown(self):
        clear_staging_env()

    def _write_rev(self, rev, body, down_revision=None, cfg=None):
        if down_revision is not None:
            self.script.generate_revision(
                rev, None, refresh=True, head=down_revision
            )
        else:
            self.script.generate_revision(rev, None, refresh=True)
        write_script(
            self.script,
            rev,
            """
revision = %r
down_revision = %r

from alembic import op
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

%s
"""
            % (rev, down_revision, body),
        )

    def _artifact(self, cfg, revision, stage_output):
        command.upgrade(
            cfg, revision, sql=True, staged=True, output=stage_output
        )
        return staged.StagedArtifact.load(stage_output)


class StagedCompileTest(StagedArtifactTestBase):
    def test_plain_revision_single_transaction(self):
        cfg = _sqlite_testing_config()
        rev = util.rev_id()
        self._write_rev(
            rev,
            """
def upgrade():
    op.create_table('test', Column('id', Integer, primary_key=True))
    op.create_index('ix_test_id', 'test', ['id'])

def downgrade():
    op.drop_index('ix_test_id', table_name='test')
    op.drop_table('test')
""",
        )
        path = os.path.join(os.path.dirname(cfg.config_file_name), "a.json")
        art = self._artifact(cfg, rev, path)

        eq_(art.dialect, "sqlite")
        eq_(art.expected_start, ())
        eq_(len(art.revisions), 1)
        revision = art.revisions[0]
        eq_(revision.revision, rev)
        eq_(len(revision.phases), 1)
        phase = revision.phases[0]
        eq_(phase.kind, "transaction")
        # version bookkeeping is in the same transaction as the DDL
        eq_([s.role for s in phase.statements].count("version"), 1)
        eq_(phase.version_check is not None, True)
        eq_(phase.version_check.expected, rev)
        # the version table setup precedes the user DDL, and every user
        # DDL statement declares a check before version advancement
        eq_(
            [s.role for s in phase.statements],
            ["setup", "ddl", "ddl", "version"],
        )
        assert all(
            statement.checks
            for statement in phase.statements
            if statement.role == "ddl"
        )

    def test_autocommit_phase_between_transactions(self):
        cfg = _no_sql_testing_config()
        self.script = ScriptDirectory.from_config(cfg)
        rev = util.rev_id()
        self._write_rev(
            rev,
            """
def upgrade():
    op.create_table('test', Column('id', Integer, primary_key=True))
    with op.get_context().autocommit_block():
        op.create_index(
            'ix_test_id', 'test', ['id'], postgresql_concurrently=True
        )

def downgrade():
    op.drop_index('ix_test_id', table_name='test')
    op.drop_table('test')
""",
        )
        path = os.path.join(os.path.dirname(cfg.config_file_name), "a.json")
        art = self._artifact(cfg, rev, path)

        kinds = [p.kind for p in art.revisions[0].phases]
        eq_(kinds, ["transaction", "autocommit", "commit"])

        sql = art.render_sql()
        # concurrent index must be outside any transaction
        assert "COMMIT;\nCREATE INDEX CONCURRENTLY" in sql
        lines = [line.strip() for line in sql.splitlines() if line.strip()]
        commit_before = next(
            i
            for i, line in enumerate(lines)
            if line == "COMMIT;"
            and "CREATE INDEX CONCURRENTLY"
            not in "\n".join(lines[:i])
        )
        index_idx = next(
            i
            for i, line in enumerate(lines)
            if line.startswith("CREATE INDEX CONCURRENTLY")
        )
        begin_after = next(
            i
            for i, line in enumerate(lines)
            if i > index_idx and line == "BEGIN;"
        )
        insert_idx = next(
            i
            for i, line in enumerate(lines)
            if i > begin_after and line.startswith("INSERT INTO alembic_version")
        )
        assert commit_before < index_idx < begin_after < insert_idx

    def test_unsupported_raw_sql_rejected(self):
        cfg = _sqlite_testing_config()
        rev = util.rev_id()
        self._write_rev(
            rev,
            """
def upgrade():
    op.execute("CREATE SOMETHING WEIRD")

def downgrade():
    pass
""",
        )
        path = os.path.join(os.path.dirname(cfg.config_file_name), "a.json")
        assert_raises_message(
            util.CommandError,
            "no read-only check",
            command.upgrade,
            cfg,
            rev,
            sql=True,
            staged=True,
            output=path,
        )
        # the artifact file is not written when compilation fails
        assert not os.path.exists(path)


class StagedSQLiteDrillTest(StagedArtifactTestBase):
    """The required local SQLite process-stop drill."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        staging_env()
        self.cfg = _sqlite_testing_config()
        self.script = ScriptDirectory.from_config(self.cfg)
        self.rev = util.rev_id()
        self._write_rev(
            self.rev,
            """
def upgrade():
    op.create_table(
        'test',
        Column('id', Integer, primary_key=True),
        Column('data', String(50)),
    )
    with op.get_context().autocommit_block():
        op.create_index('ix_test_data', 'test', ['data'])

def downgrade():
    op.drop_index('ix_test_data', table_name='test')
    op.drop_table('test')
"""
        )
        self.path = os.path.join(
            os.path.dirname(self.cfg.config_file_name), "a.json"
        )
        command.upgrade(
            self.cfg, self.rev, sql=True, staged=True, output=self.path
        )
        self.artifact = staged.StagedArtifact.load(self.path)
        self.engine = create_engine(
            self.cfg.get_main_option("sqlalchemy.url")
        )

    def tearDown(self):
        self.engine.dispose()
        clear_staging_env()

    def _heads(self):
        with self.engine.connect() as conn:
            if not inspect(conn).has_table("alembic_version"):
                return []
            return [
                row[0]
                for row in conn.execute(
                    text("SELECT version_num FROM alembic_version")
                )
            ]

    def test_stop_after_index_before_version_then_resume(self):
        config = self.artifact.revisions[0]
        kinds = [p.kind for p in config.phases]
        eq_(kinds, ["transaction", "autocommit", "commit"])

        # execute the transaction phase and the index phase; stop before
        # the version-registration phase
        report = staged.run_staged_artifact(
            self.artifact, self.engine, stop_after=2
        )
        eq_(report.stopped, True)
        eq_(self._heads(), [])

        with self.engine.connect() as conn:
            indexes = inspect(conn).get_indexes("test")
        eq_(
            indexes,
            [
                {
                    "name": "ix_test_data",
                    "column_names": ["data"],
                    "unique": 0,
                    "dialect_options": {},
                }
            ],
        )

        # replay the same artifact: the index definition matches, so the
        # index phase is skipped, and version registration completes
        report = staged.run_staged_artifact(self.artifact, self.engine)
        by_index = {r.phase.index: r for r in report.results}
        eq_(by_index[0].state, "applied")
        eq_(by_index[1].state, "applied")
        eq_(by_index[2].state, "executed")
        eq_(self._heads(), [self.rev])

    def test_conflicting_preexisting_index_stops_and_names_phase(self):
        staged.run_staged_artifact(
            self.artifact, self.engine, stop_after=2
        )
        # replace the index with the same name but a different column
        with self.engine.begin() as conn:
            conn.exec_driver_sql("DROP INDEX ix_test_data")
            conn.exec_driver_sql(
                "CREATE INDEX ix_test_data ON test (id)"
            )

        excinfo = assert_raises_message(
            staged.StagedConflictError,
            "phase 1",
            staged.run_staged_artifact,
            self.artifact,
            self.engine,
        )
        assert "ix_test_data" in str(excinfo)
        assert "expected" in str(excinfo)
        # no version was written
        eq_(self._heads(), [])

    def test_replay_after_full_apply_is_noop(self):
        staged.run_staged_artifact(self.artifact, self.engine)
        report = staged.run_staged_artifact(self.artifact, self.engine)
        eq_([r.state for r in report.results], ["applied"] * 3)
        eq_(self._heads(), [self.rev])

    def test_check_reports_states_without_writing(self):
        report = staged.describe_staged_artifact(
            self.artifact, self.engine
        )
        eq_([r.state for r in report.results], ["not_applied"] * 3)
        eq_(self._heads(), [])

    def test_artifact_round_trip_json(self):
        loaded = staged.StagedArtifact.from_json(
            json.loads(self.artifact.dumps())
        )
        eq_(loaded.to_json(), self.artifact.to_json())


class StagedPostgresqlArtifactTest(TestBase):
    def setUp(self):
        staging_env()
        self.cfg = _no_sql_testing_config()

    def tearDown(self):
        clear_staging_env()

    def test_concurrent_index_between_commit_and_next_begin(self):
        from alembic.script import ScriptDirectory

        script = ScriptDirectory.from_config(self.cfg)
        rev = util.rev_id()
        script.generate_revision(rev, None, refresh=True)
        write_script(
            script,
            rev,
            """
revision = %r
down_revision = None

from alembic import op
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

def upgrade():
    op.create_table(
        'test',
        Column('id', Integer, primary_key=True),
        Column('data', String(50)),
    )
    with op.get_context().autocommit_block():
        op.create_index(
            'ix_test_data', 'test', ['data'], postgresql_concurrently=True
        )

def downgrade():
    op.drop_index('ix_test_data', table_name='test')
    op.drop_table('test')
"""
            % rev,
        )
        path = os.path.join(
            os.path.dirname(self.cfg.config_file_name), "a.json"
        )
        command.upgrade(
            self.cfg, rev, sql=True, staged=True, output=path
        )
        art = staged.StagedArtifact.load(path)

        eq_(
            [p.kind for p in art.revisions[0].phases],
            ["transaction", "autocommit", "commit"],
        )

        # no server connection is used; order is purely from the artifact
        lines = [
            line.strip()
            for line in art.render_sql().splitlines()
            if line.strip()
        ]
        commit_idx = lines.index("COMMIT;")
        index_idx = next(
            i
            for i, line in enumerate(lines)
            if line.startswith("CREATE INDEX CONCURRENTLY")
        )
        begin_after = next(
            i for i, line in enumerate(lines)
            if i > index_idx and line == "BEGIN;"
        )
        insert_idx = next(
            i
            for i, line in enumerate(lines)
            if i > begin_after and line.startswith("INSERT INTO alembic_version")
        )
        assert commit_idx < index_idx < begin_after < insert_idx


class StagedDefinitionReconcileTest(StagedArtifactTestBase):
    def setUp(self):
        staging_env()
        self.cfg = _sqlite_testing_config()
        self.script = ScriptDirectory.from_config(self.cfg)
        self.rev = util.rev_id()
        self._write_rev(
            self.rev,
            """
def upgrade():
    op.create_table('t', Column('id', Integer, primary_key=True))

def downgrade():
    op.drop_table('t')
""",
        )
        self.path = os.path.join(
            os.path.dirname(self.cfg.config_file_name), "a.json"
        )
        command.upgrade(
            self.cfg, self.rev, sql=True, staged=True, output=self.path
        )
        self.artifact = staged.StagedArtifact.load(self.path)
        self.engine = create_engine(
            self.cfg.get_main_option("sqlalchemy.url")
        )

    def tearDown(self):
        self.engine.dispose()
        clear_staging_env()

    def test_identical_definition_is_treated_as_applied(self):
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                "CREATE TABLE t (id INTEGER NOT NULL, PRIMARY KEY (id))"
            )
        report = staged.run_staged_artifact(self.artifact, self.engine)
        eq_([r.state for r in report.results], ["executed"])
        with self.engine.connect() as conn:
            eq_(
                conn.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar(),
                self.rev,
            )

    def test_different_definition_is_conflict_before_write(self):
        with self.engine.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE t (other TEXT)")
        exc = assert_raises_message(
            staged.StagedConflictError,
            "found 'other'",
            staged.run_staged_artifact,
            self.artifact,
            self.engine,
        )
        assert "phase 0" in str(exc)
        with self.engine.connect() as conn:
            eq_(
                inspect(conn).has_table("alembic_version"),
                False,
            )

    def test_transactional_ddl_partial_state_is_conflict(self):
        # simulate a transactional-dialect artifact (e.g. PostgreSQL) where
        # DDL cannot be present without its version update
        self.artifact.transactional_ddl = True
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                "CREATE TABLE t (id INTEGER NOT NULL, PRIMARY KEY (id))"
            )
        exc = assert_raises_message(
            staged.StagedConflictError,
            "cannot leave partial progress",
            staged.run_staged_artifact,
            self.artifact,
            self.engine,
        )
        assert "phase 0" in str(exc)


class StagedDowngradeTest(StagedArtifactTestBase):
    def setUp(self):
        staging_env()
        self.cfg = _sqlite_testing_config()
        self.script = ScriptDirectory.from_config(self.cfg)
        self.rev = util.rev_id()
        self._write_rev(
            self.rev,
            """
def upgrade():
    op.create_table(
        't',
        Column('id', Integer, primary_key=True),
        Column('d', String(20)),
    )

def downgrade():
    op.drop_table('t')
""",
        )
        self.engine = create_engine(
            self.cfg.get_main_option("sqlalchemy.url")
        )

    def tearDown(self):
        self.engine.dispose()
        clear_staging_env()

    def _upgrade_artifact(self):
        path = os.path.join(
            os.path.dirname(self.cfg.config_file_name), "up.json"
        )
        command.upgrade(
            self.cfg, self.rev, sql=True, staged=True, output=path
        )
        return staged.StagedArtifact.load(path)

    def test_downgrade_to_base_round_trip(self):
        up = self._upgrade_artifact()
        staged.run_staged_artifact(up, self.engine)

        path = os.path.join(
            os.path.dirname(self.cfg.config_file_name), "down.json"
        )
        command.downgrade(
            self.cfg, "%s:base" % self.rev, sql=True, staged=True,
            output=path,
        )
        down = staged.StagedArtifact.load(path)
        eq_(down.revisions[0].heads_after, ())
        eq_(down.revisions[0].direction, "downgrade")
        eq_(
            [c.kind for c in down.revisions[0].phases[0].ddl_checks],
            ["absent"],
        )

        report = staged.run_staged_artifact(down, self.engine)
        eq_([r.state for r in report.results], ["executed"])
        with self.engine.connect() as conn:
            eq_(inspect(conn).has_table("t"), False)
            eq_(
                conn.execute(
                    text("SELECT count(*) FROM alembic_version")
                ).scalar(),
                0,
            )

        # replaying the already-applied downgrade is a no-op
        report = staged.run_staged_artifact(down, self.engine)
        eq_([r.state for r in report.results], ["applied"])


class StagedOptionValidationTest(TestBase):
    def test_staged_requires_sql_for_upgrade(self):
        assert_raises_message(
            util.CommandError,
            "--staged requires --sql",
            command.upgrade,
            _sqlite_testing_config(),
            "head",
            staged=True,
        )

    def test_staged_requires_sql_for_downgrade(self):
        assert_raises_message(
            util.CommandError,
            "--staged requires --sql",
            command.downgrade,
            _sqlite_testing_config(),
            "a:b",
            staged=True,
        )

    def test_artifact_rejects_unknown_format(self):
        assert_raises_message(
            util.CommandError,
            "Not an Alembic staged artifact",
            staged.StagedArtifact.from_json,
            {"format": "something-else"},
        )

    def test_declare_check_outside_compilation_raises(self):
        from alembic.migration import MigrationContext

        ctx = MigrationContext.configure(dialect_name="sqlite", opts={})
        assert_raises_message(
            util.CommandError,
            "only available while compiling",
            ctx.declare_staged_check,
            "SELECT 1",
        )


class StagedPartialResumeTest(StagedArtifactTestBase):
    """A non-transactional phase whose DDL landed may complete progress."""

    def setUp(self):
        staging_env()
        self.cfg = _sqlite_testing_config()
        self.script = ScriptDirectory.from_config(self.cfg)
        self.rev = util.rev_id()
        self._write_rev(
            self.rev,
            """
def upgrade():
    op.create_table('t', Column('id', Integer, primary_key=True))

def downgrade():
    op.drop_table('t')
""",
        )
        path = os.path.join(
            os.path.dirname(self.cfg.config_file_name), "a.json"
        )
        command.upgrade(
            self.cfg, self.rev, sql=True, staged=True, output=path
        )
        self.artifact = staged.StagedArtifact.load(path)
        self.engine = create_engine(
            self.cfg.get_main_option("sqlalchemy.url")
        )

    def tearDown(self):
        self.engine.dispose()
        clear_staging_env()

    def test_effect_present_version_missing_completes(self):
        # DDL landed, version table exists but the row was not written
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                "CREATE TABLE alembic_version ("
                "version_num VARCHAR(32) NOT NULL, "
                "CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num))"
            )
            conn.exec_driver_sql(
                "CREATE TABLE t (id INTEGER NOT NULL, PRIMARY KEY (id))"
            )
        report = staged.run_staged_artifact(self.artifact, self.engine)
        eq_([r.state for r in report.results], ["executed"])
        with self.engine.connect() as conn:
            eq_(
                conn.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar(),
                self.rev,
            )
