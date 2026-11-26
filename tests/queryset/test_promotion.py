import unittest

import pytest

from mongoengine import (
    Document,
    IntField,
    StringField,
    connect,
    disconnect_all,
)
from mongoengine.errors import (
    CannotPromoteError,
    PromotionNotApplicableError,
    SaveConditionError,
)

try:
    import mongomock
except ImportError:
    mongomock = None

require_mongomock = pytest.mark.skipif(
    mongomock is None, reason="you need mongomock installed to run this testcase"
)


def define_models():
    """Define a waybill staging/formal hierarchy sharing one collection."""

    class Waybill(Document):
        wb = StringField(unique=True)
        meta = {"allow_inheritance": True, "promotion": True}

    class WaybillStaging(Waybill):
        received = IntField(db_field="a")
        declared = IntField(db_field="b")
        collection_note = StringField(db_field="c")
        meta = {"promotion_role": "staging"}

    class WaybillFormal(Waybill):
        received = IntField(db_field="b")
        declared = IntField(db_field="a")
        voucher = StringField(db_field="c")
        meta = {"promotion_role": "formal"}

    class WaybillOther(Waybill):
        received = IntField(db_field="a")
        declared = IntField(db_field="b")
        meta = {"promotion_role": "other"}

    return Waybill, WaybillStaging, WaybillFormal, WaybillOther


@require_mongomock
class TestPromotion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        disconnect_all()
        connect(
            host="mongodb://localhost:27017/mongoenginetest",
            mongo_client_class=mongomock.MongoClient,
        )

    def setUp(self):
        self.Waybill, self.Staging, self.Formal, self.Other = define_models()
        self.collection = self.Waybill._get_collection()
        self.collection.drop()

    def raw(self, wb):
        return self.collection.find_one({"wb": wb})

    def test_promotes_from_base_and_keeps_primary_key(self):
        staged = self.Staging(
            wb="W7", received=80, declared=80, collection_note="old"
        ).save()
        self.collection.update_one({"wb": "W7"}, {"$set": {"audit": "tag"}})

        promoted = self.Waybill.objects(wb="W7").upsert_one(set__voucher="R7")

        assert isinstance(promoted, self.Formal)
        assert promoted.id == staged.id
        assert promoted.received == 80
        assert promoted.declared == 80
        assert promoted.voucher == "R7"

        raw = self.raw("W7")
        assert raw["_cls"] == self.Formal._class_name
        assert raw["_id"] == staged.id
        assert raw["a"] == 80
        assert raw["b"] == 80
        assert raw["c"] == "R7"
        assert raw["audit"] == "tag"

    def test_returns_written_formal_snapshot(self):
        self.Staging(wb="W7", received=80, declared=80, collection_note="old").save()

        promoted = self.Waybill.objects(wb="W7").upsert_one(set__voucher="R7")
        self.collection.update_one({"wb": "W7"}, {"$set": {"a": 55}})

        assert promoted.received == 80
        assert promoted.declared == 80

    def test_creates_formal_document_when_waybill_absent(self):
        created = self.Waybill.objects(wb="W8").upsert_one(
            set__voucher="R8", set__received=90, set__declared=70
        )

        assert isinstance(created, self.Formal)
        assert created.received == 90
        assert created.declared == 70
        assert created.voucher == "R8"
        assert self.raw("W8")["_cls"] == self.Formal._class_name

    def test_rejects_existing_formal_document(self):
        self.Formal(wb="W8", received=80, declared=80, voucher="R8").save()

        with pytest.raises(CannotPromoteError, match="formal document already exists"):
            self.Waybill.objects(wb="W8").upsert_one(set__voucher="X")

        assert self.raw("W8")["c"] == "R8"

    def test_rejects_other_category(self):
        self.Other(wb="W10", received=99, declared=99).save()

        with pytest.raises(CannotPromoteError, match="another category"):
            self.Waybill.objects(wb="W10").upsert_one(set__voucher="X")

        assert self.raw("W10")["_cls"] == self.Other._class_name

    def test_rejects_staged_document_below_threshold(self):
        self.Staging(wb="W9", received=79, declared=79, collection_note="n").save()

        with pytest.raises(CannotPromoteError, match="below the promotion threshold"):
            self.Waybill.objects(wb="W9").upsert_one(set__voucher="X")

        raw = self.raw("W9")
        assert raw["_cls"] == self.Staging._class_name
        assert raw["c"] == "n"

    def test_stale_save_fails_when_declared_was_corrected(self):
        staged = self.Staging(
            wb="W7", received=80, declared=80, collection_note="old"
        ).save()
        stale = self.Staging.objects.get(wb="W7")
        stale.declared = 82

        promoted = self.Waybill.objects(wb="W7").upsert_one(set__voucher="R7")
        assert (promoted.received, promoted.declared, promoted.voucher) == (
            80,
            80,
            "R7",
        )

        self.Formal.objects(wb="W7").update(set__declared=81)
        with pytest.raises(SaveConditionError):
            stale.save(save_condition={"declared": 80})

        raw = self.raw("W7")
        assert raw["_id"] == staged.id
        assert raw["a"] == 81
        assert raw["b"] == 80
        assert raw["c"] == "R7"

    def test_stale_save_succeeds_when_declared_untouched(self):
        self.Staging(wb="W7", received=80, declared=80, collection_note="old").save()
        stale = self.Staging.objects.get(wb="W7")
        stale.declared = 82

        self.Waybill.objects(wb="W7").upsert_one(set__voucher="R7")
        stale.save(save_condition={"declared": 80})

        raw = self.raw("W7")
        assert raw["a"] == 82
        assert raw["b"] == 80
        assert raw["c"] == "R7"

    def test_stale_save_before_promotion_is_carried_over(self):
        self.Staging(wb="W7", received=80, declared=80, collection_note="old").save()
        stale = self.Staging.objects.get(wb="W7")
        stale.declared = 82
        stale.save(save_condition={"declared": 80})

        promoted = self.Waybill.objects(wb="W7").upsert_one(set__voucher="R7")

        assert promoted.received == 80
        assert promoted.declared == 82

    def test_stale_collection_note_correction_is_rejected(self):
        self.Staging(wb="W7", received=80, declared=80, collection_note="old").save()
        stale = self.Staging.objects.get(wb="W7")
        stale.declared = 82
        stale.collection_note = "changed"

        self.Waybill.objects(wb="W7").upsert_one(set__voucher="R7")
        with pytest.raises(PromotionNotApplicableError):
            stale.save(save_condition={"declared": 80})

        raw = self.raw("W7")
        assert raw["a"] == 80
        assert raw["b"] == 80
        assert raw["c"] == "R7"

    def test_ordinary_model_upsert_and_save_condition_are_unchanged(self):
        class Plain(Document):
            name = StringField()
            age = IntField()

        Plain.drop_collection()

        bob = Plain.objects(name="Bob").upsert_one(set__age=30)
        assert (bob.name, bob.age) == ("Bob", 30)

        bob.name = "Bobby"
        bob.save(save_condition={"age": 30})
        bobby = Plain.objects(name="Bobby").upsert_one(set__age=31)

        assert bobby.id == bob.id
        assert bobby.age == 31


if __name__ == "__main__":
    unittest.main()
