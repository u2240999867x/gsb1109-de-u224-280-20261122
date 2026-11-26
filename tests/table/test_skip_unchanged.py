"""
Tests for the optional ``skip_unchanged`` write convention on updates.

A batch correction only writes rows where at least one guarded value changes,
so ordinary ``UPDATE`` triggers and ``auto_update`` columns only fire for the
rows which actually changed. Every value expression is evaluated once per row,
and the same result is reused for both the comparison and the assignment.
"""

from __future__ import annotations

import datetime
from unittest import IsolatedAsyncioTestCase, TestCase

from piccolo.columns.column_types import Integer, Timestamp, Varchar
from piccolo.engine.sqlite import TransactionType
from piccolo.querystring import QueryString
from piccolo.table import Table
from tests.base import sqlite_only

INITIAL_MODIFIED_ON = datetime.datetime(
    year=2020, month=1, day=1, hour=0, minute=0, second=0
)


def build_tables(suffix: str):
    class Stock(Table, tablename=f"stock_{suffix}"):
        site = Varchar()
        quantity = Integer(null=True)
        source = Varchar(null=True)
        label_a = Varchar(null=True)
        label_b = Varchar(null=True)
        modified_on = Timestamp(
            null=True, default=None, auto_update=datetime.datetime.now
        )

    class StockAudit(Table, tablename=f"stock_audit_{suffix}"):
        stock_id = Integer()

    return Stock, StockAudit


def build_reordered_tables(suffix: str):
    # The business columns are deliberately declared in a different order.
    class Stock(Table, tablename=f"stock_reordered_{suffix}"):
        modified_on = Timestamp(
            null=True, default=None, auto_update=datetime.datetime.now
        )
        source = Varchar(null=True)
        label_b = Varchar(null=True)
        site = Varchar()
        label_a = Varchar(null=True)
        quantity = Integer(null=True)

    class StockAudit(Table, tablename=f"stock_audit_reordered_{suffix}"):
        stock_id = Integer()

    return Stock, StockAudit


class CallCounter:
    """
    Stands in for a local SQLite function. The first call returns 11, and
    every subsequent call returns one more, so re-evaluating the expression
    within a single update would produce a different (wrong) value.
    """

    def __init__(self):
        self.calls = 0

    def __call__(self, value):
        self.calls += 1
        return 10 + self.calls


@sqlite_only
class TestSkipUnchanged(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.Stock, self.StockAudit = build_tables("main")
        (
            self.ReorderedStock,
            self.ReorderedStockAudit,
        ) = build_reordered_tables("main")
        await self.Stock.create_table()
        await self.StockAudit.create_table()
        await self.ReorderedStock.create_table()
        await self.ReorderedStockAudit.create_table()

        for table, audit in (
            (self.Stock, self.StockAudit),
            (self.ReorderedStock, self.ReorderedStockAudit),
        ):
            await table.raw(f"""
                CREATE TRIGGER "{table._meta.tablename}_audit_trigger"
                AFTER UPDATE ON "{table._meta.tablename}"
                BEGIN
                    INSERT INTO "{audit._meta.tablename}" ("stock_id")
                    VALUES (NEW."id");
                END
                """)

    async def asyncTearDown(self) -> None:
        await self.Stock.alter().drop_table()
        await self.StockAudit.alter().drop_table()
        await self.ReorderedStock.alter().drop_table()
        await self.ReorderedStockAudit.alter().drop_table()

    async def _seed(self, table) -> dict:
        await table.insert(
            # A: both computed values match the originals, including NULL.
            table(
                site="A",
                quantity=10,
                source=None,
                label_a="a-a",
                label_b="b-a",
                modified_on=INITIAL_MODIFIED_ON,
            ),
            # B: the source is still empty/NULL, quantity starts at 10.
            table(
                site="B",
                quantity=10,
                source=None,
                label_a="a-b",
                label_b="b-b",
                modified_on=INITIAL_MODIFIED_ON,
            ),
            # C: quantity is unchanged, source goes from NULL to "".
            table(
                site="C",
                quantity=10,
                source=None,
                label_a="a-c",
                label_b="b-c",
                modified_on=INITIAL_MODIFIED_ON,
            ),
        )
        rows = await table.select().order_by(table.site)
        return {row["site"]: row for row in rows}

    async def _run_batch_with_function(self, table, counter):
        engine = table._meta.db
        async with engine.transaction(
            transaction_type=TransactionType.immediate
        ) as transaction:
            await transaction.connection.create_function(
                "calc_quantity", 1, counter
            )
            await table.update(
                {
                    # Only B recomputes the quantity via the local function;
                    # every other row keeps its existing value.
                    table.quantity: QueryString(
                        "CASE WHEN {} = {} THEN calc_quantity({}) "
                        "ELSE {} END",
                        table.site,
                        "B",
                        table.quantity,
                        table.quantity,
                    ),
                    # Only C changes its source (NULL -> "").
                    table.source: QueryString(
                        "CASE WHEN {} = {} THEN {} ELSE {} END",
                        table.site,
                        "C",
                        "",
                        table.source,
                    ),
                },
                skip_unchanged=True,
            ).where(table.site.is_in(["A", "B", "C"]))

    async def _assert_batch_outcome(self, table, audit, initial):
        rows = {
            row["site"]: row
            for row in await table.select().order_by(table.site)
        }

        # A is never written: values and modified_on are untouched.
        self.assertEqual(rows["A"]["quantity"], 10)
        self.assertIsNone(rows["A"]["source"])
        self.assertEqual(rows["A"]["modified_on"], INITIAL_MODIFIED_ON)

        # B gets the function's first computed value; its source stays NULL.
        self.assertEqual(rows["B"]["quantity"], 11)
        self.assertIsNone(rows["B"]["source"])
        self.assertGreater(rows["B"]["modified_on"], INITIAL_MODIFIED_ON)

        # C keeps its quantity, but NULL -> "" is treated as a change.
        self.assertEqual(rows["C"]["quantity"], 10)
        self.assertEqual(rows["C"]["source"], "")
        self.assertGreater(rows["C"]["modified_on"], INITIAL_MODIFIED_ON)

        # Only B and C fired the ordinary UPDATE trigger.
        audit_rows = await audit.select(audit.stock_id).order_by(audit.id)
        self.assertEqual(
            [row["stock_id"] for row in audit_rows],
            sorted([initial["B"]["id"], initial["C"]["id"]]),
        )

    async def test_batch_only_updates_changed_rows(self):
        initial = await self._seed(self.Stock)
        counter = CallCounter()

        await self._run_batch_with_function(self.Stock, counter)

        await self._assert_batch_outcome(self.Stock, self.StockAudit, initial)

        # The function is only reached for B, and just once per execution -
        # the same calculation drives both the change check and the write.
        self.assertEqual(counter.calls, 1)

    async def test_batch_with_shuffled_column_order(self):
        initial = await self._seed(self.ReorderedStock)
        counter = CallCounter()

        await self._run_batch_with_function(self.ReorderedStock, counter)

        await self._assert_batch_outcome(
            self.ReorderedStock, self.ReorderedStockAudit, initial
        )
        self.assertEqual(counter.calls, 1)

    async def test_function_advances_between_executions(self):
        """
        The local function returns 11 on its first call and 12 on the next.
        Each execution uses a single calculation, but running the batch again
        picks up the function's next value.
        """
        await self._seed(self.Stock)
        counter = CallCounter()

        await self._run_batch_with_function(self.Stock, counter)
        first = await self.Stock.objects().get(self.Stock.site == "B")
        self.assertEqual(first.quantity, 11)
        self.assertEqual(counter.calls, 1)

        # The second execution invokes the function once more, writing 12.
        await self._run_batch_with_function(self.Stock, counter)
        second = await self.Stock.objects().get(self.Stock.site == "B")
        self.assertEqual(second.quantity, 12)
        self.assertEqual(counter.calls, 2)

    async def test_swap_reads_both_old_values(self):
        initial = await self._seed(self.Stock)

        await self.Stock.update(
            {
                self.Stock.label_a: self.Stock.label_b,
                self.Stock.label_b: self.Stock.label_a,
            },
            skip_unchanged=True,
        ).where(self.Stock.site == "B")

        row = await self.Stock.objects().get(self.Stock.site == "B")
        # Each assignment reads the other column's OLD row value.
        self.assertEqual(row.label_a, initial["B"]["label_b"])
        self.assertEqual(row.label_b, initial["B"]["label_a"])

        # A single row changed, so the trigger fires once.
        audit_rows = await self.StockAudit.select().run()
        self.assertEqual(
            [row["stock_id"] for row in audit_rows],
            [initial["B"]["id"]],
        )

    async def test_default_update_is_unchanged(self):
        """
        Without the flag the existing behaviour is preserved: every matched
        row is written, including unchanged ones.
        """
        initial = await self._seed(self.Stock)

        await self.Stock.update(
            {self.Stock.quantity: 10, self.Stock.source: self.Stock.source},
        ).where(self.Stock.site.is_in(["A", "B", "C"]))

        audit_rows = await self.StockAudit.select().run()
        self.assertEqual(
            sorted(row["stock_id"] for row in audit_rows),
            sorted(
                [initial["A"]["id"], initial["B"]["id"], initial["C"]["id"]]
            ),
        )

    async def test_skip_unchanged_rows_clause(self):
        """
        The write convention can also be switched on through the query
        clause rather than the ``Table.update`` keyword argument.
        """
        await self._seed(self.Stock)

        await (
            self.Stock.update(
                {
                    self.Stock.quantity: 10,
                    self.Stock.source: self.Stock.source,
                },
                use_auto_update=False,
            )
            .where(self.Stock.site.is_in(["A", "B", "C"]))
            .skip_unchanged_rows()
        )

        # Nothing changed, so no row was written and no trigger fired.
        audit_rows = await self.StockAudit.select().run()
        self.assertEqual(audit_rows, [])
        rows = {
            row["site"]: row
            for row in await self.Stock.select().order_by(self.Stock.site)
        }
        for site in ("A", "B", "C"):
            self.assertEqual(rows[site]["modified_on"], INITIAL_MODIFIED_ON)


class TestSkipUnchangedQuerystrings(TestCase):
    """
    PostgreSQL isn't available locally, but the generated SQL must still be
    valid, bindable SQL.
    """

    def test_postgres_querystring_is_bindable(self):
        Stock, _ = build_tables("pg")
        query = Stock.update(
            {
                Stock.quantity: QueryString(
                    "calc_quantity({})", Stock.quantity
                ),
                Stock.source: "",
            },
            use_auto_update=False,
            skip_unchanged=True,
        ).where(Stock.site == "B")

        querystring = query._get_skip_unchanged_querystring(
            engine_type="postgres"
        )
        sql, args = querystring.compile_string(engine_type="postgres")

        self.assertIn("IS DISTINCT FROM", sql)
        self.assertNotIn("LIMIT -1", sql)
        self.assertNotIn(" IS NOT ", sql)
        # Every placeholder is a numbered Postgres placeholder.
        self.assertNotIn("?", sql)
        self.assertIn("$1", sql)
        # The arguments are kept separate for binding (the WHERE filter is
        # the second one).
        self.assertEqual(args, ["", "B"])

    def test_sqlite_querystring_is_bindable(self):
        Stock, _ = build_tables("lite")
        query = Stock.update(
            {Stock.quantity: 11, Stock.source: ""},
            use_auto_update=False,
            skip_unchanged=True,
        ).where(Stock.site == "B")

        querystring = query._get_skip_unchanged_querystring(
            engine_type="sqlite"
        )
        sql, args = querystring.compile_string(engine_type="sqlite")

        self.assertIn("LIMIT -1", sql)
        self.assertIn(" IS NOT ", sql)
        self.assertIn("?", sql)
        self.assertEqual(args, [11, "", "B"])
