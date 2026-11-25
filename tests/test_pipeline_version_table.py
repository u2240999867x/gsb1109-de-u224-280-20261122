"""Tests for sharing one pre-built version table across several
migration environments ("pipelines"), and for invoking one Alembic
environment's upgrade from within another environment's revision on
the same connection/transaction.

"""

from __future__ import annotations

import os
import shutil
import tempfile
import textwrap

import sqlalchemy as sa

from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.runtime.migration import HeadMaintainer
from alembic.testing import expect_raises_message
from alembic.testing import eq_
from alembic.testing.fixtures import TestBase
from alembic.util.langhelpers import ModuleClsProxy


class TestPipelineVersionTableStructure(TestBase):
    def test_pipeline_column_and_composite_pk(self):
        context = MigrationContext.configure(
            dialect_name="sqlite",
            opts={
                "version_table": "pipeline_version",
                "version_table_pipeline": "collect",
            },
        )
        eq_(context._version.c.keys(), ["pipeline", "version_num"])
        eq_(
            [c.name for c in context._version.primary_key.columns],
            ["pipeline", "version_num"],
        )
        eq_(context._version.primary_key.name, "pipeline_version_pkc")

    def test_no_pipeline_by_default(self):
        context = MigrationContext.configure(dialect_name="sqlite")
        eq_(context._version.c.keys(), ["version_num"])
        eq_(context.version_table_pipeline, None)


class TestPipelineVersionTableCRUD(TestBase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        engine = sa.create_engine(
            "sqlite:///%s/test.db" % self.tempdir
        )
        self.connection = engine.connect()
        self.connection.execute(
            sa.text(
                "CREATE TABLE pipeline_version ("
                "pipeline VARCHAR(32) NOT NULL, "
                "version_num VARCHAR(32) NOT NULL, "
                "CONSTRAINT pipeline_version_pkc "
                "PRIMARY KEY (pipeline, version_num))"
            )
        )
        self.connection.commit()

    def tearDown(self):
        self.connection.close()
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _context(self, pipeline):
        return MigrationContext.configure(
            connection=self.connection,
            opts={
                "version_table": "pipeline_version",
                "version_table_pipeline": pipeline,
            },
        )

    def _all_rows(self):
        return set(
            self.connection.execute(
                sa.text(
                    "SELECT pipeline, version_num FROM pipeline_version"
                )
            ).all()
        )

    def test_heads_scoped_per_pipeline(self):
        with self.connection.begin():
            self.connection.execute(
                sa.text(
                    "INSERT INTO pipeline_version (pipeline, version_num) "
                    "VALUES ('collect', 'r3'), ('archive', 'r4')"
                )
            )
        eq_(self._context("collect").get_current_heads(), ("r3",))
        eq_(self._context("archive").get_current_heads(), ("r4",))

    def test_head_maintainer_scoped_per_pipeline(self):
        with self.connection.begin():
            self.connection.execute(
                sa.text(
                    "INSERT INTO pipeline_version (pipeline, version_num) "
                    "VALUES ('collect', 'r1'), ('archive', 'r1')"
                )
            )
            HeadMaintainer(self._context("collect"), {"r1"})._update_version(
                "r1", "r2"
            )
            HeadMaintainer(self._context("archive"), {"r1"})._insert_version(
                "r2"
            )
            HeadMaintainer(
                self._context("archive"), {"r1", "r2"}
            )._delete_version("r1")
        eq_(
            self._all_rows(),
            {("collect", "r2"), ("archive", "r2")},
        )

    def test_purge_scoped_per_pipeline(self):
        with self.connection.begin():
            self.connection.execute(
                sa.text(
                    "INSERT INTO pipeline_version (pipeline, version_num) "
                    "VALUES ('collect', 'r1'), ('archive', 'r1')"
                )
            )
            self._context("collect")._ensure_version_table(purge=True)
        eq_(self._all_rows(), {("archive", "r1")})


class TestNestedModuleProxy(TestBase):
    def test_proxy_stack_restores_outer_proxy(self):
        class SomeProxy(ModuleClsProxy):
            label = "unset"

            def __init__(self, name):
                self.label = name

            def get_label(self):
                return self.label

        module_globals: dict = {}
        SomeProxy.create_module_class_proxy(module_globals, module_globals)

        outer = SomeProxy("outer")
        inner = SomeProxy("inner")

        outer._install_proxy()
        eq_(module_globals["get_label"](), "outer")
        eq_(module_globals["label"], "outer")

        inner._install_proxy()
        eq_(module_globals["get_label"](), "inner")
        eq_(module_globals["label"], "inner")

        inner._remove_proxy()
        eq_(module_globals["get_label"](), "outer")
        eq_(module_globals["label"], "outer")

        outer._remove_proxy()
        eq_(module_globals["_proxy"], None)
        assert "label" not in module_globals

    def test_operations_context_restored_after_inner_failure(self):
        import alembic.op as op_module

        outer_ctx = MigrationContext.configure(dialect_name="sqlite")
        inner_ctx = MigrationContext.configure(dialect_name="sqlite")

        preexisting_proxy = getattr(op_module, "_proxy", None)

        with Operations.context(outer_ctx):
            assert op_module._proxy.migration_context is outer_ctx
            with expect_raises_message(RuntimeError, "inner failed"):
                with Operations.context(inner_ctx):
                    assert op_module._proxy.migration_context is inner_ctx
                    raise RuntimeError("inner failed")
            assert op_module._proxy.migration_context is outer_ctx
        assert op_module._proxy is preexisting_proxy


ENV_PY = """\
from alembic import context
from sqlalchemy import create_engine

config = context.config


def _run(connection):
    context.configure(
        connection=connection,
        transactional_ddl=True,
        version_table="pipeline_version",
        version_table_pipeline=config.attributes["pipeline"],
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    connection = config.attributes.get("connection")
    if connection is not None:
        _run(connection)
    else:
        engine = create_engine(config.get_main_option("sqlalchemy.url"))
        with engine.connect() as connection:
            _run(connection)


run_migrations_online()
"""

COLLECT_R1 = """\
revision = 'r1'
down_revision = None

import sqlalchemy as sa
from alembic import op


def upgrade():
    op.create_table(
        "collection_watermark",
        sa.Column("batch_id", sa.Integer, primary_key=True),
        sa.Column("watermark", sa.Integer, nullable=False),
    )


def downgrade():
    op.drop_table("collection_watermark")
"""

COLLECT_R2 = """\
revision = 'r2'
down_revision = None

import sqlalchemy as sa
from alembic import op


def upgrade():
    op.create_table(
        "collection_switch",
        sa.Column("batch_id", sa.Integer, primary_key=True),
        sa.Column("note", sa.String(50), nullable=False),
    )


def downgrade():
    op.drop_table("collection_switch")
"""

COLLECT_R3 = """\
revision = 'r3'
down_revision = ('r1', 'r2')

from alembic import command
from alembic import context
from alembic import op
from sqlalchemy import text


def upgrade():
    ctx = op.get_context()
    cfg = ctx.config
    observations = cfg.attributes["observations"]

    op.execute(
        "INSERT INTO collection_watermark (batch_id, watermark) "
        "VALUES (42, 820)"
    )

    archive_cfg = cfg.attributes["archive_config"]
    archive_cfg.attributes["connection"] = ctx.bind

    try:
        with ctx.bind.begin_nested():
            command.upgrade(archive_cfg, "r4")
    except Exception as exc:
        observations["error"] = str(exc)
        conn = ctx.bind
        observations["watermark"] = conn.execute(
            text(
                "SELECT watermark FROM collection_watermark "
                "WHERE batch_id = 42"
            )
        ).scalar()
        observations["archive_backfill"] = conn.execute(
            text("SELECT COUNT(*) FROM archive_backfill")
        ).scalar()
        observations["versions"] = [
            tuple(row)
            for row in conn.execute(
                text(
                    "SELECT pipeline, version_num FROM pipeline_version "
                    "ORDER BY pipeline, version_num"
                )
            )
        ]
        op.execute("DELETE FROM archive_switch WHERE batch_id = 42")
        with ctx.bind.begin_nested():
            command.upgrade(archive_cfg, "r4")

    observations["resumed_pipeline"] = (
        context.get_context().version_table_pipeline
    )

    op.execute(
        "INSERT INTO collection_switch (batch_id, note) "
        "VALUES (42, 'collect switchover')"
    )

    if cfg.attributes.get("fail_after_archive"):
        raise RuntimeError("collect final step failed")


def downgrade():
    pass
"""

ARCHIVE_R1 = """\
revision = 'r1'
down_revision = None

import sqlalchemy as sa
from alembic import op


def upgrade():
    op.create_table(
        "archive_backfill",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("watermark", sa.Integer, nullable=False),
    )


def downgrade():
    op.drop_table("archive_backfill")
"""

ARCHIVE_R2 = """\
revision = 'r2'
down_revision = None

import sqlalchemy as sa
from alembic import op


def upgrade():
    op.create_table(
        "archive_switch",
        sa.Column("batch_id", sa.Integer, primary_key=True),
        sa.Column("note", sa.String(50), nullable=False),
    )


def downgrade():
    op.drop_table("archive_switch")
"""

ARCHIVE_R3 = """\
revision = 'r3'
down_revision = ('r1', 'r2')

from alembic import op
from sqlalchemy import text


def upgrade():
    row = op.get_bind().execute(
        text(
            "SELECT watermark FROM collection_watermark "
            "WHERE batch_id = 42"
        )
    ).first()
    if row is not None:
        op.execute(
            "INSERT INTO archive_backfill (id, watermark) "
            "VALUES (1, %d)" % row[0]
        )


def downgrade():
    pass
"""

ARCHIVE_R4 = """\
revision = 'r4'
down_revision = 'r3'

from alembic import op


def upgrade():
    op.execute(
        "INSERT INTO archive_switch (batch_id, note) "
        "VALUES (42, 'archive switchover')"
    )


def downgrade():
    pass
"""


class TestPipelineSwitchover(TestBase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.db_url = "sqlite:///%s/test.db" % self.tempdir

        collect_dir = os.path.join(self.tempdir, "collect")
        archive_dir = os.path.join(self.tempdir, "archive")
        for env_dir, revisions in (
            (
                collect_dir,
                {
                    "c_r1.py": COLLECT_R1,
                    "c_r2.py": COLLECT_R2,
                    "c_r3.py": COLLECT_R3,
                },
            ),
            (
                archive_dir,
                {
                    "a_r1.py": ARCHIVE_R1,
                    "a_r2.py": ARCHIVE_R2,
                    "a_r3.py": ARCHIVE_R3,
                    "a_r4.py": ARCHIVE_R4,
                },
            ),
        ):
            versions_dir = os.path.join(env_dir, "versions")
            os.makedirs(versions_dir)
            with open(os.path.join(env_dir, "env.py"), "w") as file_:
                file_.write(ENV_PY)
            for filename, content in revisions.items():
                with open(
                    os.path.join(versions_dir, filename), "w"
                ) as file_:
                    file_.write(textwrap.dedent(content))

        self.collect_cfg = self._config(collect_dir, "collect")
        self.archive_cfg = self._config(archive_dir, "archive")

        engine = sa.create_engine(self.db_url)
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    "CREATE TABLE pipeline_version ("
                    "pipeline VARCHAR(32) NOT NULL, "
                    "version_num VARCHAR(32) NOT NULL, "
                    "CONSTRAINT pipeline_version_pkc "
                    "PRIMARY KEY (pipeline, version_num))"
                )
            )

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _config(self, env_dir, pipeline):
        cfg = Config()
        cfg.set_main_option("script_location", env_dir)
        cfg.set_main_option("sqlalchemy.url", self.db_url)
        cfg.attributes["pipeline"] = pipeline
        return cfg

    def _rows(self, stmt):
        engine = sa.create_engine(self.db_url)
        with engine.connect() as connection:
            return connection.execute(sa.text(stmt)).all()

    def _version_rows(self):
        return self._rows(
            "SELECT pipeline, version_num FROM pipeline_version "
            "ORDER BY pipeline, version_num"
        )

    def _upgrade_to_double_heads(self):
        command.upgrade(self.collect_cfg, "r1")
        command.upgrade(self.collect_cfg, "r2")
        command.upgrade(self.archive_cfg, "r1")
        command.upgrade(self.archive_cfg, "r2")

    def _seed_placeholder(self):
        engine = sa.create_engine(self.db_url)
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    "INSERT INTO archive_switch (batch_id, note) "
                    "VALUES (42, 'old placeholder')"
                )
            )

    def test_switchover_with_nested_upgrade(self):
        self._upgrade_to_double_heads()
        self._seed_placeholder()

        observations = {}
        self.collect_cfg.attributes["observations"] = observations
        self.collect_cfg.attributes["archive_config"] = self.archive_cfg

        command.upgrade(self.collect_cfg, "r3")

        # the first nested archive upgrade failed on the placeholder
        # primary key; while handling it, the collect revision could
        # still see its own uncommitted watermark, the archive
        # backfill was rolled back, and both pipelines were still at
        # their r1/r2 heads in the ledger.
        assert "UNIQUE constraint failed" in observations["error"]
        eq_(observations["watermark"], 820)
        eq_(observations["archive_backfill"], 0)
        eq_(
            observations["versions"],
            [
                ("archive", "r1"),
                ("archive", "r2"),
                ("collect", "r1"),
                ("collect", "r2"),
            ],
        )

        # after the nested upgrade returned, alembic.context pointed
        # back at the collect environment again
        eq_(observations["resumed_pipeline"], "collect")

        eq_(
            self._version_rows(),
            [("archive", "r4"), ("collect", "r3")],
        )
        eq_(
            self._rows("SELECT id, watermark FROM archive_backfill"),
            [(1, 820)],
        )
        eq_(
            self._rows("SELECT batch_id, note FROM archive_switch"),
            [(42, "archive switchover")],
        )
        eq_(
            self._rows("SELECT batch_id, note FROM collection_switch"),
            [(42, "collect switchover")],
        )
        eq_(
            self._rows(
                "SELECT batch_id, watermark FROM collection_watermark"
            ),
            [(42, 820)],
        )

    def test_outer_rollback_restores_pre_switch_state(self):
        self._upgrade_to_double_heads()
        self._seed_placeholder()

        self.collect_cfg.attributes["observations"] = {}
        self.collect_cfg.attributes["archive_config"] = self.archive_cfg
        self.collect_cfg.attributes["fail_after_archive"] = True

        with expect_raises_message(
            RuntimeError, "collect final step failed"
        ):
            command.upgrade(self.collect_cfg, "r3")

        eq_(
            self._version_rows(),
            [
                ("archive", "r1"),
                ("archive", "r2"),
                ("collect", "r1"),
                ("collect", "r2"),
            ],
        )
        eq_(
            self._rows("SELECT batch_id, note FROM archive_switch"),
            [(42, "old placeholder")],
        )
        eq_(self._rows("SELECT COUNT(*) FROM archive_backfill"), [(0,)])
        eq_(self._rows("SELECT COUNT(*) FROM collection_watermark"), [(0,)])
        eq_(self._rows("SELECT COUNT(*) FROM collection_switch"), [(0,)])

    def test_independent_upgrades_use_own_heads(self):
        self._upgrade_to_double_heads()

        command.upgrade(self.archive_cfg, "head")
        eq_(
            self._version_rows(),
            [
                ("archive", "r4"),
                ("collect", "r1"),
                ("collect", "r2"),
            ],
        )

        self.collect_cfg.attributes["observations"] = {}
        self.collect_cfg.attributes["archive_config"] = self.archive_cfg
        command.upgrade(self.collect_cfg, "head")
        eq_(
            self._version_rows(),
            [("archive", "r4"), ("collect", "r3")],
        )
        eq_(
            self._rows("SELECT batch_id, note FROM archive_switch"),
            [(42, "archive switchover")],
        )
        eq_(
            self._rows("SELECT batch_id, note FROM collection_switch"),
            [(42, "collect switchover")],
        )
