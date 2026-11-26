from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

from piccolo.custom_types import Combinable, TableInstance
from piccolo.query.base import Query
from piccolo.query.mixins import (
    ReturningDelegate,
    ValuesDelegate,
    WhereDelegate,
)
from piccolo.querystring import QueryString
from piccolo.utils.sql_values import convert_to_sql_value

if TYPE_CHECKING:  # pragma: no cover
    from piccolo.columns import Column


class UpdateError(Exception):
    pass


class Update(Query[TableInstance, list[Any]]):
    __slots__ = (
        "force",
        "returning_delegate",
        "skip_unchanged",
        "values_delegate",
        "where_delegate",
    )

    def __init__(
        self,
        table: type[TableInstance],
        force: bool = False,
        skip_unchanged: bool = False,
        **kwargs,
    ):
        super().__init__(table, **kwargs)
        self.force = force
        self.skip_unchanged = skip_unchanged
        self.returning_delegate = ReturningDelegate()
        self.values_delegate = ValuesDelegate(table=table)
        self.where_delegate = WhereDelegate()

    ###########################################################################
    # Clauses

    def values(
        self,
        values: Optional[dict[Union[Column, str], Any]] = None,
        **kwargs,
    ) -> Update:
        if values is None:
            values = {}
        values = dict(values, **kwargs)
        self.values_delegate.values(values)
        return self

    def where(self, *where: Union[Combinable, QueryString]) -> Update:
        self.where_delegate.where(*where)
        return self

    def returning(self, *columns: Column) -> Update:
        self.returning_delegate.returning(columns)
        return self

    def skip_unchanged_rows(self, skip: bool = True) -> Update:
        """
        Only write a row when at least one of the new values is actually
        different to the existing row. Rows where every guarded value is
        unchanged (including ``NULL`` compared with ``NULL``) aren't included
        in the ``UPDATE``, so no ``UPDATE`` triggers fire and ``auto_update``
        columns keep their existing values.

        Each expression is evaluated once per row, with the result being
        reused for both the comparison and the assignment.
        """
        self.skip_unchanged = skip
        return self

    ###########################################################################

    def _validate(self):
        """
        Called at the start of :meth:`piccolo.query.base.Query.run` to make
        sure the user has configured the query correctly before running it.
        """
        if len(self.values_delegate._values) == 0:
            raise ValueError("No values were specified to update.")

        for column, _ in self.values_delegate._values.items():
            if len(column._meta.call_chain) > 0:
                raise ValueError(
                    "Related values can't be updated via an update."
                )

        if (not self.where_delegate._where) and (not self.force):
            classname = self.table.__name__
            raise UpdateError(
                "Do you really want to update all rows in "
                f"{classname}? If so, use pass `force=True` into "
                f"`{classname}.update`. Otherwise, add a where clause."
            )

    ###########################################################################

    @property
    def default_querystrings(self) -> Sequence[QueryString]:
        columns_str = ", ".join(
            f'"{col._meta.db_column_name}" = {{}}'
            for col, _ in self.values_delegate._values.items()
        )

        query = f"UPDATE {self.table._meta.get_formatted_tablename()} SET {columns_str}"  # noqa: E501

        querystring = QueryString(
            query, *self.values_delegate.get_sql_values()
        )

        if self.where_delegate._where:
            # The JOIN syntax isn't allowed in SQL UPDATE queries, so we need
            # to write the WHERE clause differently, using a sub select.

            querystring = QueryString(
                "{} WHERE {}",
                querystring,
                self.where_delegate._where.querystring_for_update_and_delete,
            )

        if self.returning_delegate._returning:
            querystring = QueryString(
                "{}{}",
                querystring,
                self.returning_delegate._returning.querystring,
            )

        return [querystring]

    @property
    def sqlite_querystrings(self) -> Sequence[QueryString]:
        if not self.skip_unchanged:
            return self.default_querystrings
        return [self._get_skip_unchanged_querystring(engine_type="sqlite")]

    @property
    def postgres_querystrings(self) -> Sequence[QueryString]:
        if not self.skip_unchanged:
            return self.default_querystrings
        return [self._get_skip_unchanged_querystring(engine_type="postgres")]

    @property
    def cockroach_querystrings(self) -> Sequence[QueryString]:
        if not self.skip_unchanged:
            return self.default_querystrings
        # Cockroach is Postgres compatible for this syntax.
        return [self._get_skip_unchanged_querystring(engine_type="postgres")]

    ###########################################################################

    def _get_skip_unchanged_querystring(self, engine_type: str) -> QueryString:
        """
        Build an ``UPDATE`` which only touches rows where a value actually
        changes.

        The new values are computed once in a sub query, and the outer query
        joins onto it by primary key, skipping any row where every guarded
        value ``IS NOT DISTINCT FROM`` the existing value::

            UPDATE "stock"
            SET "quantity" = "excluded"."new_0"
            FROM (
                SELECT "stock"."id" AS "row_id",
                       (<expression>) AS "new_0"
                FROM "stock"
                WHERE <filters>
            ) AS "excluded"
            WHERE "stock"."id" = "excluded"."row_id"
              AND "excluded"."new_0" IS DISTINCT FROM "stock"."quantity"

        ``auto_update`` columns are still assigned on the rows which change,
        but aren't used to decide whether a row changed.
        """
        table = self.table
        table_name = table._meta.get_formatted_tablename()
        primary_key = table._meta.primary_key
        excluded_alias = "excluded"

        auto_update_column_names = {
            column._meta.db_column_name
            for column in table._meta.auto_update_columns
        }

        guarded_values = [
            (column, value)
            for column, value in self.values_delegate._values.items()
            if column._meta.db_column_name not in auto_update_column_names
        ]

        if not guarded_values:
            # There's nothing to compare, so fall back to a normal update.
            return self.default_querystrings[0]

        if engine_type == "sqlite":
            # `LIMIT -1` stops SQLite from flattening the sub query into the
            # outer query, which would evaluate the expressions more than
            # once.
            barrier = " LIMIT -1"
            difference_operator = "IS NOT"
        elif engine_type == "postgres":
            barrier = ""
            difference_operator = "IS DISTINCT FROM"
        else:  # pragma: no cover
            raise ValueError(
                "`skip_unchanged` is only supported by SQLite, Postgres "
                "and Cockroach."
            )

        select_querystrings: list[QueryString] = [
            QueryString('{} AS "row_id"', primary_key)
        ]
        guard_querystrings: list[QueryString] = []
        set_fragments: list[str] = []
        set_querystrings: list[QueryString] = []

        for index, (column, value) in enumerate(guarded_values):
            new_alias = f"new_{index}"
            column_name = column._meta.db_column_name
            old_value = column._meta.get_full_name(with_alias=False)

            sql_value = convert_to_sql_value(value=value, column=column)
            if isinstance(sql_value, QueryString):
                new_value_querystring = sql_value
            else:
                new_value_querystring = QueryString("{}", sql_value)

            select_querystrings.append(
                QueryString(f'({{}}) AS "{new_alias}"', new_value_querystring)
            )
            guard_querystrings.append(
                QueryString(
                    f'"{excluded_alias}"."{new_alias}" '
                    f"{difference_operator} {old_value}"
                )
            )
            set_fragments.append(
                f'"{column_name}" = "{excluded_alias}"."{new_alias}"'
            )

        # `auto_update` columns (and any other unguarded column) are assigned
        # directly rather than via the sub query.
        for column, value in self.values_delegate._values.items():
            if column._meta.db_column_name in auto_update_column_names:
                sql_value = convert_to_sql_value(value=value, column=column)
                set_fragments.append(f'"{column._meta.db_column_name}" = {{}}')
                set_querystrings.append(QueryString("{}", sql_value))

        select_list = QueryString(
            ", ".join("{}" for _ in select_querystrings),
            *select_querystrings,
        )

        where_template = ""
        where_querystrings: list[QueryString] = []
        if self.where_delegate._where:
            where_template = " WHERE {}"
            where_querystrings.append(
                self.where_delegate._where.querystring_for_update_and_delete
            )

        sub_query = QueryString(
            "SELECT {} FROM " f"{table_name}{where_template}{barrier}",
            select_list,
            *where_querystrings,
        )

        guard = QueryString(
            " OR ".join("{}" for _ in guard_querystrings),
            *guard_querystrings,
        )

        set_clause = QueryString(", ".join(set_fragments), *set_querystrings)

        pk_name = primary_key._meta.get_full_name(with_alias=False)

        querystring = QueryString(
            f"UPDATE {table_name} SET {{}} FROM ({{}}) AS "
            f'"{excluded_alias}" WHERE {pk_name} = '
            f'"{excluded_alias}"."row_id" AND ({{}})',
            set_clause,
            sub_query,
            guard,
        )

        if self.returning_delegate._returning:
            querystring = QueryString(
                "{} {}",
                querystring,
                self.returning_delegate._returning.querystring,
            )

        return querystring
