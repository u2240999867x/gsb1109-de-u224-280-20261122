"""Tests for revision-aware cached reference corrections.

These tests run against mongomock when MongoDB is unavailable, exercising the
same persistence code paths as a real server for the operations used here.
"""

import mongomock
import pytest

from mongoengine import (
    CachedReferenceField,
    Document,
    IntField,
    StringField,
    connect,
)
from mongoengine.connection import disconnect_all
from mongoengine import cached_reference


@pytest.fixture
def models():
    disconnect_all()
    connect(db="cached_reference_corrections", mongo_client_class=mongomock.MongoClient)

    class Material(Document):
        amount = IntField(db_field="amt", null=True)
        label = StringField(db_field="lbl")

    class Transfer(Document):
        material = CachedReferenceField(Material, fields=["amount", "label"])

    field = next(
        f for f in Transfer._fields.values() if isinstance(f, CachedReferenceField)
    )

    Material.drop_collection()
    Transfer.drop_collection()

    yield Material, Transfer, field

    disconnect_all()


def raw_owner(transfer, owner):
    return transfer._get_collection().find_one({"_id": owner.pk})["material"]


def test_source_correction_receipt_is_durable_and_idempotent(models):
    material, transfer, field = models
    m = material(amount=30, label="old").save()
    t1 = transfer(material=m).save()
    t2 = transfer(material=m).save()

    # Job 1: read amount, delete it, commit, but the caller loses the receipt.
    job1 = material.objects.get(pk=m.pk)
    assert job1.amount == 30
    del job1.amount
    job1.save(import_number="IMPORT-1", source="vendor-A")

    receipt = field.get_receipt("IMPORT-1", m.pk)
    assert receipt.import_number == "IMPORT-1"
    assert receipt.source == "vendor-A"
    assert receipt.unset == {"amt": True}
    assert receipt.set == {}
    assert receipt.revision == 1

    # Corrections are not implicitly synchronized.
    assert raw_owner(transfer, t1)["amt"] == 30
    assert raw_owner(transfer, t2)["amt"] == 30

    # Job 2: change the label and synchronize immediately.
    job2 = material.objects.get(pk=m.pk)
    assert job2.label == "old"
    job2.label = "new"
    job2.save()

    # Job 3: explicitly set amount to null and synchronize to t1 first.
    job3 = material.objects.get(pk=m.pk)
    job3.amount = None
    job3.save(import_number="IMPORT-3")
    null_receipt = field.get_receipt("IMPORT-3", m.pk)
    field.apply_receipt(null_receipt, [t1])
    assert raw_owner(transfer, t1)["amt"] is None
    assert raw_owner(transfer, t2)["amt"] == 30

    # Retry the original deletion with its import number, then replay it.
    retried = field.record_correction(material.objects.get(pk=m.pk), "IMPORT-1")
    assert retried.revision == receipt.revision
    assert retried.unset == receipt.unset
    assert retried.set == receipt.set
    assert material._get_collection().find_one({"_id": m.pk})["amt"] is None

    field.apply_receipt(retried, [t1, t2])
    assert raw_owner(transfer, t1)["amt"] is None
    assert "amt" not in raw_owner(transfer, t2)
    assert raw_owner(transfer, t1)["lbl"] == raw_owner(transfer, t2)["lbl"] == "new"

    # Replay the explicit null on t2: the field reappears with a null value.
    field.apply_receipt(null_receipt, [t2])
    assert "amt" in raw_owner(transfer, t2)
    assert raw_owner(transfer, t2)["amt"] is None

    # A paused sync_all must not overwrite null with its stale snapshot.
    field.sync_all()
    assert raw_owner(transfer, t1)["amt"] is None
    assert raw_owner(transfer, t2)["amt"] is None
    assert raw_owner(transfer, t1)["lbl"] == raw_owner(transfer, t2)["lbl"] == "new"

    # Source keeps the third save's null; retry receipt stayed unchanged.
    assert material._get_collection().find_one({"_id": m.pk})["amt"] is None
    again = field.get_receipt("IMPORT-1", m.pk)
    assert again.revision == receipt.revision
    assert again.unset == receipt.unset


def test_stale_receipt_skips_repointed_owner(models):
    material, transfer, field = models
    m = material(amount=30, label="old").save()
    other = material(amount=99, label="other").save()

    owner = transfer(material=m).save()
    job = material.objects.get(pk=m.pk)
    del job.amount
    receipt = field.record_correction(job, "IMPORT-OLD")
    assert receipt.unset == {"amt": True}

    owner.material = other
    owner.save()
    field.apply_receipt(receipt, [owner])

    cached = raw_owner(transfer, owner)
    assert cached["_id"] == other.pk
    assert cached["amt"] == 99
    assert cached["lbl"] == "other"


def test_no_receipt_without_a_cached_change(models):
    material, transfer, field = models
    m = material(amount=30, label="old").save()

    plain = material.objects.get(pk=m.pk)
    plain.save(import_number="IMPORT-NOOP")

    assert field.get_receipt("IMPORT-NOOP", m.pk) is None


def test_rejected_save_produces_no_syncable_correction(models):
    material, transfer, field = models
    m = material(amount=30, label="old").save()
    owner = transfer(material=m).save()

    class StrictMaterial(Document):
        amount = IntField(db_field="amt", min_value=0)
        label = StringField(db_field="lbl", required=True)
        meta = {"collection": "strict_material"}

    rejected = StrictMaterial(amount=30, label="old").save()
    rejected.amount = -5
    with pytest.raises(Exception):
        rejected.save(import_number="IMPORT-REJECTED")

    assert field.get_receipt("IMPORT-REJECTED", rejected.pk) is None
    assert raw_owner(transfer, owner)["amt"] == 30
