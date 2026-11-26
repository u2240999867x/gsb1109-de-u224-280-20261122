from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from piccolo.apps.migrations.auto.diffable_table import DiffableTable
from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import ForeignKey
from piccolo.columns.reference import LazyTableReference
from piccolo.table import Table

# A table can be identified by more than just its class name - two tables can
# share a tablename, as long as they're in different schemas.
TableIdentity = tuple[str, str, Optional[str]]


def _get_table_identity(table: DiffableTable) -> TableIdentity:
    return (table.class_name, table.tablename, table.schema)


def _get_references_identity(
    references: object,
) -> Optional[TableIdentity]:
    if isinstance(references, LazyTableReference):
        try:
            references = references.resolve()
        except Exception:
            return None

    if isinstance(references, type) and issubclass(references, Table):
        return (
            references.__name__,
            references._meta.tablename,
            references._meta.schema,
        )

    return None


@dataclass
class SchemaSnapshot:
    """
    Adds up a sequence of MigrationManagers, and returns a snapshot of what
    the schema looks like.
    """

    # In ascending order of date created.
    managers: list[MigrationManager] = field(default_factory=list)

    ###########################################################################

    def get_table_from_snapshot(self, table_class_name: str) -> DiffableTable:
        snapshot = self.get_snapshot()
        filtered = [i for i in snapshot if i.class_name == table_class_name]
        if not filtered:
            raise ValueError(f"No match was found for {table_class_name}")
        return filtered[0]

    ###########################################################################

    @staticmethod
    def _copy_table(table: DiffableTable) -> DiffableTable:
        """
        The managers own the tables and columns they were created with. A
        snapshot applies renames and schema changes to them, so we need to
        work on copies - otherwise building a snapshot changes the managers
        themselves, and a later snapshot (for example of an older migration)
        sees the changes from a newer one.
        """
        return DiffableTable(
            class_name=table.class_name,
            tablename=table.tablename,
            schema=table.schema,
            columns=[column.copy() for column in table.columns],
            constraints=list(table.constraints),
        )

    ###########################################################################

    def get_snapshot(self) -> list[DiffableTable]:
        tables: list[DiffableTable] = []

        # Every identity a table has had - used to work out which table a
        # foreign key was pointing at, even if it has since been renamed or
        # moved to a different schema.
        table_identities: dict[int, set[TableIdentity]] = {}

        # For each table, maps every name a column has had to its current
        # name, so foreign keys which still refer to the old name can be
        # updated.
        column_names: dict[int, dict[str, str]] = {}

        def register_table(table: DiffableTable) -> None:
            table_identities[id(table)] = {_get_table_identity(table)}
            column_names[id(table)] = {
                column._meta.name: column._meta.name
                for column in table.columns
            }

        def register_column(table: DiffableTable, column) -> None:
            column_names[id(table)][column._meta.name] = column._meta.name

        # Make sure the managers are sorted correctly:
        sorted_managers = sorted(self.managers, key=lambda x: x.migration_id)

        for manager in sorted_managers:
            for table in manager.add_tables:
                copied_table = self._copy_table(table)
                tables.append(copied_table)
                register_table(copied_table)

            for drop_table in manager.drop_tables:
                remaining = [
                    i
                    for i in tables
                    if not (
                        i.class_name == drop_table.class_name
                        and i.tablename == drop_table.tablename
                        and i.schema == drop_table.schema
                    )
                ]
                if len(remaining) == len(tables):
                    # Fallback for migrations which didn't record the schema.
                    remaining = [
                        i
                        for i in tables
                        if not (
                            i.class_name == drop_table.class_name
                            and i.tablename == drop_table.tablename
                        )
                    ]
                tables = remaining

            # Schema changes happen before renames when the migration runs, so
            # apply them in the same order here. Match on the class name too -
            # different tables can share a tablename if they're in different
            # schemas.
            for change_table_schema in manager.change_table_schemas:
                candidates = [
                    i
                    for i in tables
                    if i.class_name == change_table_schema.class_name
                    and i.tablename == change_table_schema.tablename
                    and (
                        change_table_schema.old_schema is None
                        or i.schema == change_table_schema.old_schema
                    )
                ]
                if not candidates:
                    candidates = [
                        i
                        for i in tables
                        if i.tablename == change_table_schema.tablename
                        and i.schema == change_table_schema.old_schema
                    ]

                if candidates:
                    table = candidates[0]
                    table_identities[id(table)].add(_get_table_identity(table))
                    table.schema = change_table_schema.new_schema
                    table_identities[id(table)].add(_get_table_identity(table))

            for rename_table in manager.rename_tables:
                candidates = [
                    i
                    for i in tables
                    if i.class_name == rename_table.old_class_name
                    and i.tablename == rename_table.old_tablename
                ]

                if not candidates:
                    candidates = [
                        i
                        for i in tables
                        if i.class_name == rename_table.old_class_name
                    ]

                if not candidates:
                    continue

                table = candidates[0]
                table_identities[id(table)].add(_get_table_identity(table))

                table.class_name = rename_table.new_class_name
                table.tablename = rename_table.new_tablename
                if rename_table.schema is not None:
                    table.schema = rename_table.schema

                table_identities[id(table)].add(_get_table_identity(table))

            for table in tables:
                add_columns = manager.add_columns.columns_for_table_class_name(
                    table.class_name
                )
                for column in add_columns:
                    # Copy the column, as renames and alters further down
                    # would otherwise change the manager's column.
                    copied_column = column.copy()
                    table.columns.append(copied_column)
                    register_column(table, copied_column)

                ###############################################################

                drop_columns = manager.drop_columns.for_table_class_name(
                    table.class_name
                )
                for drop_column in drop_columns:
                    table.columns = [
                        i
                        for i in table.columns
                        if i._meta.name != drop_column.column_name
                    ]

                ###############################################################

                alter_columns = manager.alter_columns.for_table_class_name(
                    table.class_name
                )
                for alter_column in alter_columns:
                    for index, column in enumerate(table.columns):
                        if column._meta.name == alter_column.column_name:
                            for key, value in alter_column.params.items():
                                setattr(column._meta, key, value)
                                column._meta.params.update({key: value})

                            # If the column type has changed, we need to update
                            # it.
                            if (
                                alter_column.column_class
                                != alter_column.old_column_class
                            ) and alter_column.column_class is not None:
                                new_column = alter_column.column_class(
                                    **column._meta.params
                                )
                                new_column._meta = column._meta
                                table.columns[index] = new_column
                                register_column(table, new_column)

                ###############################################################

                for (
                    rename_column
                ) in manager.rename_columns.for_table_class_name(
                    table.class_name
                ):
                    for column in table.columns:
                        if column._meta.name == rename_column.old_column_name:
                            column._meta.name = rename_column.new_column_name
                            column._meta.db_column_name = (
                                rename_column.new_db_column_name
                            )
                            column_names[id(table)][
                                rename_column.old_column_name
                            ] = rename_column.new_column_name
                            register_column(table, column)

                add_constraints = (
                    manager.add_constraints.constraints_for_table_class_name(
                        table.class_name
                    )
                )
                table.constraints.extend(add_constraints)

                drop_constraints = (
                    manager.drop_constraints.for_table_class_name(
                        table.class_name
                    )
                )
                for drop_constraint in drop_constraints:
                    table.constraints = [
                        i
                        for i in table.constraints
                        if i._meta.name != drop_constraint.constraint_name
                    ]

        self._update_foreign_key_references(
            tables=tables,
            table_identities=table_identities,
            column_names=column_names,
        )

        for table in tables:
            table.columns_map = {
                column._meta.name: column for column in table.columns
            }

        return tables

    ###########################################################################

    @staticmethod
    def _find_table(
        tables: list[DiffableTable],
        table_identities: dict[int, set[TableIdentity]],
        identity: TableIdentity,
    ) -> Optional[DiffableTable]:
        for table in tables:
            if _get_table_identity(table) == identity:
                return table

        for table in tables:
            if identity in table_identities.get(id(table), set()):
                return table

        return None

    def _update_foreign_key_references(
        self,
        tables: list[DiffableTable],
        table_identities: dict[int, set[TableIdentity]],
        column_names: dict[int, dict[str, str]],
    ) -> None:
        """
        Point every foreign key at the table as it looks in this snapshot.

        A foreign key can outlive a rename of the table it references - it
        still stores the class, tablename, schema, and target column which
        were current when it was created. Update them to match the snapshot,
        otherwise the generated SQL refers to the old schema, table, or
        target column.
        """
        resolved: list[tuple[ForeignKey, DiffableTable]] = []

        for table in tables:
            for column in table.columns:
                if not isinstance(column, ForeignKey):
                    continue

                references = column._meta.params.get("references")
                identity = _get_references_identity(references)
                if identity is None:
                    continue

                target_table = self._find_table(
                    tables=tables,
                    table_identities=table_identities,
                    identity=identity,
                )
                if target_table is None:
                    continue

                # If the target column was renamed, the foreign key still
                # refers to it by its old name.
                target_column = column._meta.params.get("target_column")
                if target_column is not None:
                    old_target_name = (
                        target_column
                        if isinstance(target_column, str)
                        else target_column._meta.name
                    )
                    new_target_name = column_names[id(target_table)].get(
                        old_target_name, old_target_name
                    )
                    column._meta.params["target_column"] = new_target_name
                    column._foreign_key_meta.target_column = new_target_name

                resolved.append((column, target_table))

        # Build a single class for each target table, so all foreign keys
        # referencing it share the same class.
        table_classes: dict[int, type[Table]] = {}

        def get_table_class(table: DiffableTable) -> type[Table]:
            if id(table) not in table_classes:
                table_classes[id(table)] = table.to_table_class()
            return table_classes[id(table)]

        for column, target_table in resolved:
            target_class = get_table_class(target_table)
            column._meta.params["references"] = target_class
            column._foreign_key_meta.references = target_class

            # The proxy columns still refer to the target as it was before.
            for proxy_column in column._foreign_key_meta.proxy_columns:
                try:
                    delattr(column, proxy_column._meta.name)
                except AttributeError:
                    pass
            column._foreign_key_meta.proxy_columns = []
            column.set_proxy_columns()
