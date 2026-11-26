from unittest import TestCase
from unittest.mock import AsyncMock, patch

from piccolo.apps.migrations.auto import (
    DiffableTable,
    MigrationManager,
    SchemaDiffer,
    SchemaSnapshot,
)
from piccolo.columns.column_types import ForeignKey, Serial, Varchar
from piccolo.constraints import Unique
from piccolo.engine.postgres import PostgresEngine
from piccolo.table import Table, create_table_class


class TestSchemaSnaphot(TestCase):
    def test_add_table(self):
        """
        Test adding tables.
        """
        manager_1 = MigrationManager()
        manager_1.add_table(class_name="Manager", tablename="manager")

        manager_2 = MigrationManager()
        manager_2.add_table(class_name="Band", tablename="band")

        schema_snapshot = SchemaSnapshot(managers=[manager_1, manager_2])
        snapshot = schema_snapshot.get_snapshot()

        self.assertTrue(len(snapshot) == 2)

        class_names = [i.class_name for i in snapshot]
        self.assertTrue("Band" in class_names)
        self.assertTrue("Manager" in class_names)

    def test_drop_table(self):
        """
        Test dropping tables.
        """
        manager_1 = MigrationManager()
        manager_1.add_table(class_name="Manager", tablename="manager")
        manager_1.add_table(class_name="Band", tablename="band")

        manager_2 = MigrationManager()
        manager_2.drop_table(class_name="Band", tablename="band")

        schema_snapshot = SchemaSnapshot(managers=[manager_1, manager_2])
        snapshot = schema_snapshot.get_snapshot()

        self.assertTrue(len(snapshot) == 1)

        class_names = [i.class_name for i in snapshot]
        self.assertTrue("Manager" in class_names)

    def test_rename_table(self):
        """
        Test renaming tables.
        """
        manager_1 = MigrationManager()
        manager_1.add_table(class_name="Band", tablename="band")

        manager_2 = MigrationManager()
        manager_2.rename_table(
            old_class_name="Band",
            old_tablename="band",
            new_class_name="Performer",
            new_tablename="performer",
        )

        schema_snapshot = SchemaSnapshot(managers=[manager_1, manager_2])
        snapshot = schema_snapshot.get_snapshot()

        self.assertTrue(snapshot[0].class_name == "Performer")
        self.assertTrue(snapshot[0].tablename == "performer")

    def test_rename_table_updates_foreign_key_references(self):
        """
        Renaming a table must also update the foreign keys on other tables
        which point at it, otherwise the snapshot keeps diffing against the
        renamed model and emits an ``alter_column`` on every ``--auto`` run.
        """

        class Band(Table, tablename="band"):
            pass

        manager_1 = MigrationManager()
        manager_1.add_table(class_name="Band", tablename="band")
        manager_1.add_table(class_name="Concert", tablename="concert")
        manager_1.add_column(
            table_class_name="Concert",
            tablename="concert",
            column_name="band",
            column_class_name="ForeignKey",
            column_class=ForeignKey,
            params={"references": Band},
        )

        manager_2 = MigrationManager()
        manager_2.rename_table(
            old_class_name="Band",
            old_tablename="band",
            new_class_name="Performer",
            new_tablename="performer",
        )

        schema_snapshot = SchemaSnapshot(managers=[manager_1, manager_2])
        concert = schema_snapshot.get_table_from_snapshot("Concert")
        references = concert.columns[0]._meta.params["references"]

        self.assertEqual(references.__name__, "Performer")
        self.assertEqual(references._meta.tablename, "performer")

        # The snapshot now agrees with the renamed model, so there is
        # nothing left to migrate.
        class Performer(Table, tablename="performer"):
            pass

        class Concert(Table, tablename="concert"):
            band = ForeignKey(Performer)

        delta = (
            DiffableTable(
                class_name="Concert",
                tablename="concert",
                columns=[Concert.band],
            )
            - concert
        )

        self.assertEqual(delta.alter_columns, [])

    def test_add_column(self):
        """
        Test adding columns.
        """
        manager = MigrationManager()
        manager.add_table(class_name="Manager", tablename="manager")
        manager.add_column(
            table_class_name="Manager",
            tablename="manager",
            column_name="name",
            column_class_name="Varchar",
            params={"length": 100},
        )

        schema_snapshot = SchemaSnapshot(managers=[manager])
        snapshot = schema_snapshot.get_snapshot()

        self.assertTrue(len(snapshot) == 1)
        self.assertTrue(len(snapshot[0].columns) == 1)

    def test_rename_column(self):
        """
        Test renaming columns.
        """
        manager_1 = MigrationManager()
        manager_1.add_table(class_name="Manager", tablename="manager")
        manager_1.add_column(
            table_class_name="Manager",
            tablename="manager",
            column_name="name",
            column_class_name="Varchar",
            params={"length": 100},
        )

        manager_2 = MigrationManager()
        manager_2.rename_column(
            table_class_name="Manager",
            tablename="manager",
            old_column_name="name",
            new_column_name="title",
        )

        schema_snapshot = SchemaSnapshot(managers=[manager_1, manager_2])
        snapshot = schema_snapshot.get_snapshot()
        self.assertTrue(snapshot[0].columns[0]._meta.name == "title")

        # Make sure double renames still work
        manager_3 = MigrationManager()
        manager_3.rename_column(
            table_class_name="Manager",
            tablename="manager",
            old_column_name="title",
            new_column_name="label",
        )

        schema_snapshot = SchemaSnapshot(
            managers=[manager_1, manager_2, manager_3]
        )
        snapshot = schema_snapshot.get_snapshot()
        self.assertTrue(snapshot[0].columns[0]._meta.name == "label")

    def test_drop_column(self):
        """
        Test dropping columns.
        """
        manager_1 = MigrationManager()
        manager_1.add_table(class_name="Manager", tablename="manager")
        manager_1.add_column(
            table_class_name="Manager",
            tablename="manager",
            column_name="name",
            column_class_name="Varchar",
            params={"length": 100},
        )

        manager_2 = MigrationManager()
        manager_2.drop_column(
            table_class_name="Manager", tablename="manager", column_name="name"
        )

        schema_snapshot = SchemaSnapshot(managers=[manager_1, manager_2])
        snapshot = schema_snapshot.get_snapshot()
        self.assertEqual(len(snapshot[0].columns), 0)

    def test_alter_column(self):
        """
        Test altering columns.
        """
        manager_1 = MigrationManager()
        manager_1.add_table(class_name="Manager", tablename="manager")
        manager_1.add_column(
            table_class_name="Manager",
            tablename="manager",
            column_name="name",
            column_class_name="Varchar",
            params={"length": 100},
        )

        manager_2 = MigrationManager()
        manager_2.alter_column(
            table_class_name="Manager",
            tablename="manager",
            column_name="name",
            params={"unique": True},
            old_params={"unique": False},
        )

        schema_snapshot = SchemaSnapshot(managers=[manager_1, manager_2])
        snapshot = schema_snapshot.get_snapshot()

        self.assertTrue(snapshot[0].columns[0]._meta.unique)

    def test_get_table_from_snapshot(self):
        manager_1 = MigrationManager()
        manager_1.add_table(class_name="Manager", tablename="manager")
        manager_1.add_table(class_name="Band", tablename="band")

        schema_snapshot = SchemaSnapshot(managers=[manager_1])
        table = schema_snapshot.get_table_from_snapshot("Manager")

        self.assertTrue(table.class_name == "Manager")

        with self.assertRaises(ValueError):
            schema_snapshot.get_table_from_snapshot("Foo")

    def test_add_constraint(self):
        """
        Test adding constraints.
        """
        manager = MigrationManager()
        manager.add_table(class_name="Manager", tablename="manager")
        manager.add_column(
            table_class_name="Manager",
            tablename="manager",
            column_name="name",
            column_class_name="Varchar",
            params={"length": 100},
        )
        manager.add_constraint(
            table_class_name="Manager",
            tablename="manager",
            constraint_name="unique_name",
            constraint_class=Unique,
            params={
                "name": "name_unique",
                "columns": ["name"],
            },
        )

        schema_snapshot = SchemaSnapshot(managers=[manager])
        snapshot = schema_snapshot.get_snapshot()

        self.assertTrue(len(snapshot) == 1)
        self.assertTrue(len(snapshot[0].columns) == 1)
        self.assertTrue(len(snapshot[0].constraints) == 1)

    def test_drop_constraint(self):
        """
        Test dropping constraints.
        """
        manager_1 = MigrationManager()
        manager_1.add_table(class_name="Manager", tablename="manager")
        manager_1.add_column(
            table_class_name="Manager",
            tablename="manager",
            column_name="name",
            column_class_name="Varchar",
            params={"length": 100},
        )
        manager_1.add_constraint(
            table_class_name="Manager",
            tablename="manager",
            constraint_name="unique_name",
            constraint_class=Unique,
            params={
                "columns": ["name"],
            },
        )

        manager_2 = MigrationManager()
        manager_2.drop_constraint(
            table_class_name="Manager",
            tablename="manager",
            constraint_name="unique_name",
        )

        schema_snapshot = SchemaSnapshot(managers=[manager_1, manager_2])
        snapshot = schema_snapshot.get_snapshot()
        self.assertEqual(len(snapshot[0].constraints), 0)


class TestRenamedForeignKeyTarget(TestCase):
    """
    A foreign key can point at a non-primary unique column. When the target
    table is moved to another schema, and the class, table, and target column
    are all renamed, historical snapshots must keep using the old values,
    whereas the latest snapshot must use the new ones.

    Two physical tables can share a tablename if they're in different schemas
    - the audit schema has its own ``customer`` table, which must be left
    alone.
    """

    @classmethod
    def setUpClass(cls) -> None:
        # An engine is needed to compile the Postgres SQL, but we don't want
        # to actually connect to a database.
        with (
            patch.object(
                PostgresEngine,
                "get_version",
                new=AsyncMock(return_value=14.0),
            ),
            patch.object(
                PostgresEngine,
                "prep_database",
                new=AsyncMock(return_value=None),
            ),
        ):
            cls.engine = PostgresEngine(config={})

        cls.archive_customer = cls._create_table(
            "ArchiveCustomer", "customer", "archive"
        )
        cls.audit_customer = cls._create_table(
            "CustomerAudit", "customer", "audit"
        )
        cls.customer = create_table_class(
            class_name="Customer",
            class_kwargs={"tablename": "customer", "schema": "archive_v2"},
            class_members={
                "id": Serial(primary_key=True),
                "external_code": Varchar(unique=True, null=False),
            },
        )
        cls.archive_invoice = create_table_class(
            class_name="Invoice",
            class_kwargs={"tablename": "invoice", "schema": "archive"},
            class_members={
                "id": Serial(primary_key=True),
                "customer_code": ForeignKey(
                    cls.customer, target_column="external_code"
                ),
            },
        )
        cls.audit_invoice = create_table_class(
            class_name="AuditInvoice",
            class_kwargs={"tablename": "audit_invoice", "schema": "audit"},
            class_members={
                "id": Serial(primary_key=True),
                "customer_code": ForeignKey(
                    cls.audit_customer, target_column="ext_code"
                ),
            },
        )

    @staticmethod
    def _create_table(class_name: str, tablename: str, schema: str):
        return create_table_class(
            class_name=class_name,
            class_kwargs={"tablename": tablename, "schema": schema},
            class_members={
                "id": Serial(primary_key=True),
                "ext_code": Varchar(unique=True, null=False),
            },
        )

    def setUp(self) -> None:
        archive_customer = self.archive_customer
        audit_customer = self.audit_customer

        self.manager_1 = MigrationManager(migration_id="1", app_name="test")

        self.manager_1.add_table(
            class_name="ArchiveCustomer",
            tablename="customer",
            schema="archive",
            columns=[],
        )
        self.manager_1.add_table(
            class_name="Invoice", tablename="invoice", schema="archive"
        )
        self.manager_1.add_table(
            class_name="CustomerAudit",
            tablename="customer",
            schema="audit",
            columns=[],
        )
        self.manager_1.add_table(
            class_name="AuditInvoice",
            tablename="audit_invoice",
            schema="audit",
        )

        for (
            table_class_name,
            tablename,
            schema,
            column_name,
            column_class,
            params,
        ) in [
            (
                "ArchiveCustomer",
                "customer",
                "archive",
                "id",
                Serial,
                {"primary_key": True},
            ),
            (
                "ArchiveCustomer",
                "customer",
                "archive",
                "ext_code",
                Varchar,
                {"unique": True, "null": False},
            ),
            (
                "Invoice",
                "invoice",
                "archive",
                "id",
                Serial,
                {"primary_key": True},
            ),
            (
                "Invoice",
                "invoice",
                "archive",
                "customer_code",
                ForeignKey,
                {
                    "references": archive_customer,
                    "target_column": "ext_code",
                    "null": True,
                },
            ),
            (
                "CustomerAudit",
                "customer",
                "audit",
                "id",
                Serial,
                {"primary_key": True},
            ),
            (
                "CustomerAudit",
                "customer",
                "audit",
                "ext_code",
                Varchar,
                {"unique": True, "null": False},
            ),
            (
                "AuditInvoice",
                "audit_invoice",
                "audit",
                "id",
                Serial,
                {"primary_key": True},
            ),
            (
                "AuditInvoice",
                "audit_invoice",
                "audit",
                "customer_code",
                ForeignKey,
                {
                    "references": audit_customer,
                    "target_column": "ext_code",
                    "null": True,
                },
            ),
        ]:
            self.manager_1.add_column(
                table_class_name=table_class_name,
                tablename=tablename,
                column_name=column_name,
                column_class=column_class,
                params=params,
                schema=schema,
            )

        self.manager_2 = MigrationManager(migration_id="2", app_name="test")
        self.manager_2.change_table_schema(
            class_name="ArchiveCustomer",
            tablename="customer",
            old_schema="archive",
            new_schema="archive_v2",
        )
        self.manager_2.rename_table(
            old_class_name="ArchiveCustomer",
            old_tablename="customer",
            new_class_name="Customer",
            new_tablename="customer",
            schema="archive_v2",
        )
        self.manager_2.rename_column(
            table_class_name="Customer",
            tablename="customer",
            old_column_name="ext_code",
            new_column_name="external_code",
            schema="archive_v2",
        )

    def _get_snapshot_table(
        self, managers: list[MigrationManager], table_class_name: str
    ) -> DiffableTable:
        return SchemaSnapshot(managers=managers).get_table_from_snapshot(
            table_class_name
        )

    def _get_table_class(self, managers: list[MigrationManager]):
        table = self._get_snapshot_table(managers, "Invoice")
        table_class = table.to_table_class()
        table_class._meta._db = self.engine
        return table_class

    def test_r1_snapshot(self) -> None:
        """
        The historical snapshot must use the original schema, table, and
        target column.
        """
        table_class = self._get_table_class([self.manager_1])
        fk = table_class._meta.foreign_key_columns[0]

        self.assertEqual(
            fk.ddl,
            '"customer_code" VARCHAR(255) REFERENCES "archive"."customer" '
            '("ext_code") ON DELETE CASCADE ON UPDATE CASCADE DEFAULT null',
        )

        select = table_class.select(fk.id).querystrings[0].__str__()
        self.assertIn('FROM "archive"."invoice"', select)
        self.assertIn('LEFT JOIN "archive"."customer"', select)
        self.assertIn(
            '"invoice"."customer_code" = "invoice$customer_code"."ext_code"',
            select,
        )

    def test_r2_snapshot(self) -> None:
        """
        The latest snapshot must use the new schema, table, and physical
        column.
        """
        table_class = self._get_table_class([self.manager_1, self.manager_2])
        fk = table_class._meta.foreign_key_columns[0]

        self.assertEqual(
            fk.ddl,
            '"customer_code" VARCHAR(255) REFERENCES "archive_v2"."customer" '
            '("external_code") ON DELETE CASCADE ON UPDATE CASCADE '
            "DEFAULT null",
        )

        select = table_class.select(fk.id).querystrings[0].__str__()
        self.assertIn('FROM "archive"."invoice"', select)
        self.assertIn('LEFT JOIN "archive_v2"."customer"', select)
        self.assertIn(
            '"invoice"."customer_code" = '
            '"invoice$customer_code"."external_code"',
            select,
        )

    def test_audit_customer_untouched(self) -> None:
        """
        The audit schema has a physical table called ``customer`` too. It
        isn't moved or renamed, and its foreign key still points at its own
        ``ext_code`` column.
        """
        audit_invoice = self._get_snapshot_table(
            [self.manager_1, self.manager_2], "AuditInvoice"
        ).to_table_class()
        audit_invoice._meta._db = self.engine
        audit_fk = audit_invoice._meta.foreign_key_columns[0]

        self.assertEqual(
            audit_fk.ddl,
            '"customer_code" VARCHAR(255) REFERENCES "audit"."customer" '
            '("ext_code") ON DELETE CASCADE ON UPDATE CASCADE DEFAULT null',
        )

        audit_select = str(audit_invoice.select(audit_fk.id).querystrings[0])
        self.assertIn('LEFT JOIN "audit"."customer"', audit_select)
        self.assertIn('"audit_invoice$customer_code"."ext_code"', audit_select)

    def test_snapshot_does_not_modify_managers(self) -> None:
        """
        Building a snapshot shouldn't mutate the managers. Getting the older
        snapshot after the newer one must still return the old values.
        """
        self._get_snapshot_table([self.manager_1, self.manager_2], "Invoice")
        table_class = self._get_table_class([self.manager_1])
        fk = table_class._meta.foreign_key_columns[0]

        self.assertEqual(
            fk.ddl,
            '"customer_code" VARCHAR(255) REFERENCES "archive"."customer" '
            '("ext_code") ON DELETE CASCADE ON UPDATE CASCADE DEFAULT null',
        )

    def test_auto_is_idempotent(self) -> None:
        """
        Building the R2 snapshot, then the R1 snapshot, then R2 again, must
        not produce any repeated changes once R2 is current.
        """
        current_schema = [
            DiffableTable(
                class_name=table_class.__name__,
                tablename=table_class._meta.tablename,
                schema=table_class._meta.schema,
                columns=list(table_class._meta.non_default_columns),
                constraints=list(table_class._meta.constraints),
            )
            for table_class in (
                self.customer,
                self.archive_invoice,
                self.audit_customer,
                self.audit_invoice,
            )
        ]

        def get_statements(managers: list[MigrationManager]) -> list[str]:
            snapshot = SchemaSnapshot(managers=managers).get_snapshot()
            differ = SchemaDiffer(
                schema=current_schema,
                schema_snapshot=snapshot,
                auto_input="y",
            )
            return [
                statement
                for alter_statements in differ.get_alter_statements()
                for statement in alter_statements.statements
            ]

        # Build R2 first, then the older R1 snapshot. R2 matches the current
        # schema, so there's nothing to migrate. R1 doesn't, and must still
        # produce the R1 -> R2 changes - it must not inherit anything from R2.
        self.assertEqual(get_statements([self.manager_1, self.manager_2]), [])
        self.assertTrue(get_statements([self.manager_1]))

        # Now R2 is current, so there should be nothing left to migrate, and
        # it must stay that way.
        self.assertEqual(get_statements([self.manager_1, self.manager_2]), [])
        self.assertEqual(get_statements([self.manager_1, self.manager_2]), [])
