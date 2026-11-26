import asyncio
import datetime
from unittest import TestCase

from piccolo.columns import Timestamp, Varchar
from piccolo.table import Table
from tests.base import engines_only
from tests.example_apps.music.tables import Manager


class TrackedRow(Table, tablename="tracked_row"):
    name = Varchar()
    modified_on = Timestamp(
        auto_update=datetime.datetime.now, null=True, default=None
    )


@engines_only("sqlite", "postgres", "cockroach")
class TestInstanceTracking(TestCase):
    def setUp(self):
        for table in (Manager, TrackedRow):
            if table.table_exists().run_sync():
                table.alter().drop_table().run_sync()
            table.create_table().run_sync()

    def tearDown(self):
        for table in (Manager, TrackedRow):
            if table.table_exists().run_sync():
                table.alter().drop_table().run_sync()

    def test_nested_savepoints(self):
        """
        Rolling back S2 only undoes the changes made by that layer. Rolling
        back S1 puts A back into a pending state, so saving it again inserts
        a new row rather than updating the row which reused its old PK.
        """

        async def run():
            DB = Manager._meta.db

            async with DB.transaction(track_instances=True) as transaction:
                await Manager.insert(Manager(name="root-keep"))

                P = TrackedRow(name="P")
                await P.save()

                S1 = await transaction.savepoint("S1")

                A = Manager(name="A")
                await A.save()
                A_id = A.id
                self.assertTrue(A._exists_in_db)

                # An edit made by the caller before ``save`` isn't something
                # Piccolo did, so it must survive a savepoint rollback.
                P.name = "P edited upstream"
                previous_modified_on = P.modified_on

                S2 = await transaction.savepoint("S2")
                P.name = "P edited in S2"
                await P.save()

                self.assertIsNotNone(P.modified_on)
                self.assertNotEqual(P.modified_on, previous_modified_on)

                await S2.rollback_to()

                self.assertTrue(P._exists_in_db)
                self.assertEqual(P.name, "P edited in S2")
                self.assertEqual(P.modified_on, previous_modified_on)

                await S1.rollback_to()

                self.assertFalse(A._exists_in_db)
                self.assertNotIsInstance(A.id, int)

                B = Manager(name="B")
                await B.save()

                self.assertEqual(B.id, A_id)

                await A.save()

                self.assertNotEqual(A.id, B.id)
                self.assertTrue(A._exists_in_db)

            rows = Manager.select(Manager.name).run_sync()
            self.assertEqual(
                {row["name"] for row in rows},
                {"root-keep", "A", "B"},
            )

        asyncio.run(run())

    def test_released_savepoint_still_follows_outer_rollback(self):
        """
        Releasing an inner savepoint makes it permanent within the
        transaction - but an outer savepoint must still undo its instance
        changes.
        """

        async def run():
            DB = Manager._meta.db

            async with DB.transaction(track_instances=True) as transaction:
                await Manager.insert(Manager(name="keep"))

                S1 = await transaction.savepoint("S1")

                C = Manager(name="C")
                await C.save()

                S2 = await transaction.savepoint("S2")
                D = Manager(name="D")
                await D.save()
                await S2.release()

                await S1.rollback_to()

                self.assertFalse(C._exists_in_db)
                self.assertFalse(D._exists_in_db)

            rows = Manager.select(Manager.name).run_sync()
            self.assertEqual(rows, [{"name": "keep"}])

        asyncio.run(run())

    def test_root_commit_keeps_keys(self):
        """
        A legitimate primary key assigned during a committed transaction is
        kept on the instance.
        """

        async def run():
            DB = Manager._meta.db

            async with DB.transaction(track_instances=True):
                manager = Manager(name="committed")
                await manager.save()
                assigned_id = manager.id

            self.assertEqual(manager.id, assigned_id)
            self.assertTrue(manager._exists_in_db)
            self.assertEqual(
                Manager.select(Manager.name).run_sync(),
                [{"name": "committed"}],
            )

        asyncio.run(run())

    def test_root_rollback(self):
        """
        Rolling back the root transaction undoes the tracked state, including
        changes from a savepoint which was released.
        """

        async def run():
            DB = Manager._meta.db

            async with DB.transaction(track_instances=True) as transaction:
                first = Manager(name="first")
                await first.save()

                savepoint = await transaction.savepoint()
                second = Manager(name="second")
                await second.save()
                await savepoint.release()

                await transaction.rollback()

            self.assertFalse(first._exists_in_db)
            self.assertFalse(second._exists_in_db)
            self.assertEqual(Manager.select().run_sync(), [])

        asyncio.run(run())

    def test_disabled_by_default(self):
        """
        Transactions which don't opt in keep the existing behaviour.
        """

        async def run():
            DB = Manager._meta.db

            async with DB.transaction() as transaction:
                self.assertFalse(transaction.track_instances)

                manager = Manager(name="untracked")
                await manager.save()
                assigned_id = manager.id

                savepoint = await transaction.savepoint()
                manager._exists_in_db = True
                manager.id = 12345
                await savepoint.rollback_to()

                self.assertEqual(manager.id, 12345)
                self.assertNotEqual(assigned_id, 12345)

        asyncio.run(run())

    def test_enabling_after_writes(self):
        """
        The journal is recorded regardless, so enabling tracking later still
        lets the transaction rollback restore earlier writes.
        """

        async def run():
            DB = Manager._meta.db

            async with DB.transaction() as transaction:
                manager = Manager(name="early")
                await manager.save()
                assigned_id = manager.id

                transaction.enable_instance_tracking()

                await transaction.rollback()

            self.assertFalse(manager._exists_in_db)
            self.assertNotEqual(manager.id, assigned_id)
            self.assertEqual(Manager.select().run_sync(), [])

        asyncio.run(run())

    def test_enable_on_savepoint(self):
        """
        Tracking can be enabled on a per savepoint basis.
        """

        async def run():
            DB = Manager._meta.db

            async with DB.transaction() as transaction:
                savepoint = await transaction.savepoint(track_instances=True)

                manager = Manager(name="later")
                await manager.save()

                await savepoint.rollback_to()

                self.assertFalse(manager._exists_in_db)

        asyncio.run(run())
