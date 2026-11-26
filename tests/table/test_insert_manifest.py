import os
import sqlite3
import tempfile
from unittest import TestCase

from piccolo.columns import Integer, Serial, Varchar
from piccolo.engine.sqlite import SQLiteEngine
from piccolo.query.methods.insert_manifest import MAX_BIND_VALUES
from piccolo.table import Table
from tests.base import engines_only


@engines_only("sqlite")
class TestInsertManifest(TestCase):
    """
    A handover page moves site numbers and ports between devices. A device's
    device number is its permanent identity; its primary key and acquisition
    history must survive a handover.
    """

    class Device(Table):
        id: Serial
        device_number = Varchar(unique=True)
        warehouse = Varchar()
        site_number = Varchar(null=True)
        access_port = Varchar(null=True)
        history = Integer(default=0)

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        self.engine = SQLiteEngine(path=self.path)
        Device = self.Device
        Device._meta.db = self.engine
        Device.create_table().run_sync()

        connection = sqlite3.connect(self.path)
        connection.executescript("""
            CREATE UNIQUE INDEX device_site_unique
                ON device (warehouse, site_number);
            CREATE UNIQUE INDEX device_port_unique
                ON device (warehouse, access_port);
            CREATE TABLE signature (device_number TEXT PRIMARY KEY);
            CREATE TRIGGER acceptance
            BEFORE UPDATE ON device
            WHEN NEW.device_number = 'C' AND NEW.access_port = 'p2'
            BEGIN
                SELECT RAISE(ABORT, 'acceptance: C has not signed')
                WHERE NOT EXISTS (
                    SELECT 1 FROM signature
                    WHERE device_number = NEW.device_number
                );
            END;
            """)
        connection.commit()
        connection.close()

        Device.insert(
            Device(
                device_number="A",
                warehouse="north",
                site_number="h1",
                access_port="p1",
                history=11,
            ),
            Device(
                device_number="B",
                warehouse="north",
                site_number="h2",
                access_port="p2",
                history=22,
            ),
            Device(
                device_number="C",
                warehouse="north",
                site_number="h3",
                access_port="p3",
                history=33,
            ),
        ).run_sync()
        self.original = {
            row["device_number"]: row for row in Device.select().run_sync()
        }

    def tearDown(self) -> None:
        os.remove(self.path)

    def manifest(self, order=("A", "D", "B", "C")) -> list:
        Device = self.Device
        rows = {
            "A": Device(
                device_number="A",
                warehouse="north",
                site_number="h2",
                access_port="p1",
            ),
            "B": Device(
                device_number="B",
                warehouse="north",
                site_number="h1",
                access_port="p3",
            ),
            "C": Device(
                device_number="C",
                warehouse="north",
                site_number="h3",
                access_port="p2",
            ),
            "D": Device(
                device_number="D",
                warehouse="north",
                site_number="h4",
                access_port="p4",
            ),
        }
        output = [rows[name] for name in order]
        for index in range(40):
            output.append(
                Device(
                    device_number=f"X{index}",
                    warehouse="south",
                    site_number=f"h{index}",
                    access_port=f"p{index}",
                )
            )
        return output

    def stored(self) -> dict:
        return {
            row["device_number"]: row
            for row in self.Device.select().run_sync()
        }

    def sign(self) -> None:
        connection = sqlite3.connect(self.path)
        connection.execute("INSERT INTO signature VALUES ('C')")
        connection.commit()
        connection.close()

    def test_rejection_rolls_back_whole_handover(self) -> None:
        result = self.Device.insert_manifest(self.manifest()).run_sync()
        receipts = {receipt.row.device_number: receipt for receipt in result}

        # A, B and C are one connected component, so all three are rejected
        # when C's acceptance trigger fires.
        for name in "ABC":
            self.assertFalse(receipts[name].accepted, name)
            self.assertEqual(receipts[name].rules, ("acceptance", "database"))
            self.assertIn("C", receipts[name].devices)

        # D and the other warehouse are independent, and must be committed.
        self.assertTrue(receipts["D"].accepted)
        self.assertEqual(receipts["D"].primary_key, 4)
        self.assertTrue(
            all(
                receipt.accepted
                for receipt in result
                if receipt.row.device_number.startswith("X")
            )
        )

        stored = self.stored()
        for name in "ABC":
            self.assertEqual(
                stored[name]["site_number"],
                self.original[name]["site_number"],
            )
            self.assertEqual(
                stored[name]["access_port"],
                self.original[name]["access_port"],
            )
            self.assertEqual(stored[name]["id"], self.original[name]["id"])
            self.assertEqual(
                stored[name]["history"], self.original[name]["history"]
            )

    def test_handover_after_signature(self) -> None:
        self.Device.insert_manifest(self.manifest()).run_sync()
        self.sign()

        rows = [
            Device
            for Device in self.manifest(order=("C", "A", "B"))
            if not Device.device_number.startswith("X")
            and Device.device_number != "D"
        ]
        result = self.Device.insert_manifest(rows).run_sync()
        self.assertTrue(all(receipt.accepted for receipt in result))

        stored = self.stored()
        self.assertEqual(stored["A"]["site_number"], "h2")
        self.assertEqual(stored["B"]["site_number"], "h1")
        self.assertEqual(stored["C"]["access_port"], "p2")
        for name in "ABC":
            self.assertEqual(stored[name]["id"], self.original[name]["id"])
            self.assertEqual(
                stored[name]["history"], self.original[name]["history"]
            )

    def test_independent_handovers(self) -> None:
        rows = [
            self.Device(
                device_number="A",
                warehouse="north",
                site_number="h2",
                access_port="p1",
            ),
            self.Device(
                device_number="B",
                warehouse="north",
                site_number="h1",
                access_port="p2",
            ),
        ]
        result = self.Device.insert_manifest(rows).run_sync()
        self.assertTrue(all(receipt.accepted for receipt in result))

    def test_resource_claim_conflicts(self) -> None:
        rows = [
            self.Device(
                device_number="A",
                warehouse="north",
                site_number="h4",
                access_port="p4",
            ),
            self.Device(
                device_number="D",
                warehouse="north",
                site_number="h4",
                access_port="p5",
            ),
        ]
        result = self.Device.insert_manifest(rows).run_sync()
        self.assertFalse(any(receipt.accepted for receipt in result))
        self.assertIn("site_number", result[0].rules[0])
        self.assertIn("site_number", result[1].rules[0])

    def test_external_holder(self) -> None:
        rows = [
            self.Device(
                device_number="D",
                warehouse="north",
                site_number="h1",
                access_port="p4",
            )
        ]
        result = self.Device.insert_manifest(rows).run_sync()
        self.assertFalse(result[0].accepted)
        self.assertIn("site_number", result[0].rules[0])

    def test_bind_limit(self) -> None:
        rows = self.manifest()
        queries = []
        import importlib

        module = importlib.import_module(
            "piccolo.query.methods.insert_manifest"
        )

        original = module._run

        async def audited(table, querystring):
            _, args = querystring.compile_string(
                engine_type=table._meta.db.engine_type
            )
            queries.append(len(args))
            return await original(table, querystring)

        module._run = audited
        try:
            self.Device.insert_manifest(rows).run_sync()
        finally:
            module._run = original

        self.assertTrue(queries)
        self.assertLessEqual(max(queries), MAX_BIND_VALUES)
