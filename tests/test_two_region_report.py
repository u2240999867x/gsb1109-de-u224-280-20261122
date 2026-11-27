"""End-to-end regression test for two-region reconciliation reports.

Reproduces the scenario where a single report collection is read from two
regional databases while references live in either database, some of them
without a database name (legacy pointers).
"""

import unittest

import mongomock
from bson import DBRef

import mongoengine.connection
from mongoengine import (
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    IntField,
    LazyReferenceField,
    ListField,
    ReferenceField,
    StringField,
    connect,
    disconnect_all,
)
from mongoengine.base import LazyReference, UnresolvedReference
from mongoengine.dereference import MISSING_REFERENCE, TYPE_MISMATCH_REFERENCE


class Lot(Document):
    meta = {"allow_inheritance": True, "collection": "lots"}
    number = IntField()
    depot = ReferenceField("Depot")


class ColdChainLot(Lot):
    temperature = IntField()


class Depot(Document):
    meta = {"collection": "depots"}
    number = IntField()
    name = StringField()


class Detail(EmbeddedDocument):
    item = ReferenceField(Lot, dbref=True)


class Report(Document):
    meta = {"collection": "reports", "allow_inheritance": True}
    details = ListField(EmbeddedDocumentField(Detail))
    legacy = ReferenceField(ColdChainLot, dbref=True)
    west = ReferenceField(Lot, dbref=True)
    lazy = LazyReferenceField(Lot, dbref=True)
    missing = ReferenceField(Lot, dbref=True)


class TwoRegionReportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        disconnect_all()
        connect(
            db="region_west",
            alias="region_west",
            host="mongodb://localhost",
            mongo_client_class=mongomock.MongoClient,
        )
        connect(
            db="region_east",
            alias="region_east",
            host="mongodb://localhost",
            mongo_client_class=mongomock.MongoClient,
        )
        client = mongoengine.connection.get_connection("region_west")
        cls.west = client["region_west"]
        cls.east = client["region_east"]

    @classmethod
    def tearDownClass(cls):
        disconnect_all()

    def setUp(self):
        for db in (self.west, self.east):
            for collection in ("lots", "depots", "reports"):
                db.drop_collection(collection)

        self.west["depots"].insert_one({"_id": 2, "number": 2, "name": "west"})
        self.east["depots"].insert_one({"_id": 2, "number": 2, "name": "east"})

        self.west["lots"].insert_many(
            [
                {"_id": 7, "_cls": "Lot", "number": 7, "depot": DBRef("depots", 2)},
                {"_id": 8, "_cls": "Lot", "number": 8, "depot": DBRef("depots", 2)},
            ]
        )
        self.east["lots"].insert_one(
            {
                "_id": 7,
                "_cls": "ColdChainLot",
                "number": 7,
                "temperature": -18,
                "depot": DBRef("depots", 2),
            }
        )

        report = {
            "_id": 1,
            "_cls": "Report",
            "details": [
                {"item": DBRef("lots", 7, "region_east")},
                {"item": DBRef("lots", 7, "region_east")},
            ],
            "legacy": DBRef("lots", 7),
            "west": DBRef("lots", 7, "region_west"),
            "lazy": DBRef("lots", 7, "region_west"),
            "missing": DBRef("lots", 8, "region_east"),
        }
        self.west["reports"].insert_one(report)
        self.east["reports"].insert_one(report)

    def _open(self, database):
        raw = self.west["reports"].find_one({"_id": 1})
        report = Report._from_son(raw)
        report.select_related(database=database)
        return report

    def test_reports_are_isolated_and_database_aware(self):
        west_report = self._open("region_west")
        east_report = self._open("region_east")

        # Interleave access across both reports only after both were opened.
        assert west_report.details[0].item is west_report.details[1].item
        assert east_report.details[0].item is east_report.details[1].item

        for report in (west_report, east_report):
            item = report.details[0].item
            assert isinstance(item, ColdChainLot)
            assert item.temperature == -18
            # depot has no database of its own -> east (from its parent)
            assert item.depot.name == "east"

        # Legacy pointer resolves against each report's default database.
        assert isinstance(west_report.legacy, UnresolvedReference)
        assert west_report.legacy.reason == TYPE_MISMATCH_REFERENCE
        assert west_report.legacy.database == "region_west"
        assert isinstance(east_report.legacy, ColdChainLot)

        # Explicit west refs resolve to the west normal lot in both reports.
        for report in (west_report, east_report):
            assert type(report.west) is Lot
            assert report.west.depot.name == "west"

        # East 8 is missing even though west has 8.
        for report in (west_report, east_report):
            missing = report.missing
            assert isinstance(missing, UnresolvedReference)
            assert missing.reason == MISSING_REFERENCE
            assert missing.database == "region_east"
            assert missing.collection == "lots"
            assert missing.id == 8

        # Lazy references stay lazy, expose pk 7, and fetch from west.
        for report in (west_report, east_report):
            assert isinstance(report.lazy, LazyReference)
            assert report.lazy.pk == 7
            assert type(report.lazy.fetch()) is Lot
            assert type(report.lazy.fetch(force=True)) is Lot
            assert report.lazy.fetch(force=True).depot.name == "west"

    def test_queryset_select_related_database(self):
        raw = self.west["reports"].find_one({"_id": 1})
        report = Report._from_son(raw)
        # A raw document needs no collection access for select_related.
        report.select_related(default_database="region_east")
        assert isinstance(report.legacy, ColdChainLot)


if __name__ == "__main__":
    unittest.main()
