from mongoengine import (
    Document,
    SequenceField,
    StringField,
    ValidationError,
    connect,
    disconnect_all,
)
from mongoengine.approval import (
    ApprovalIDConflict,
    ApprovalReceipt,
    BatchPreview,
    approve,
    approve_batch,
    get_approval_receipt,
    preview,
    preview_batch,
)


class AmbientA(Document):
    id = SequenceField(primary_key=True, sequence_name="ambient")
    name = StringField()
    bin = StringField()
    meta = {"db_alias": "ambient-a"}

    def clean(self):
        if self.id and self.id < 0:
            raise ValidationError("invalid number")


class AmbientB(Document):
    id = SequenceField(primary_key=True, sequence_name="ambient")
    name = StringField()
    bin = StringField()
    meta = {"db_alias": "ambient-b"}

    def clean(self):
        if self.id == 42:
            raise ValidationError("candidate rejected")
        self.bin = "A-%s" % self.id


class AmbientC(Document):
    id = SequenceField(primary_key=True, sequence_name="ambient")
    name = StringField()
    bin = StringField()
    meta = {"db_alias": "ambient-a"}

    def clean(self):
        self.bin = "C-%s" % self.id


class ColdD(Document):
    id = SequenceField(
        primary_key=True,
        sequence_name="cold",
        db_alias="cold",
        value_decorator=lambda value: "Q-%s" % value,
    )
    name = StringField()
    bin = StringField()
    meta = {"db_alias": "cold"}

    def clean(self):
        self.bin = "D-%s" % self.id
        if getattr(self, "fail_write", False):
            raise ValidationError("cold write failed")


def setup_module(module):
    disconnect_all()
    mongomock_client = __import__("mongomock").MongoClient
    connect(db="ambient", alias="default", mongo_client_class=mongomock_client)
    connect(db="ambient", alias="ambient-a", mongo_client_class=mongomock_client)
    connect(db="ambient", alias="ambient-b", mongo_client_class=mongomock_client)
    connect(db="cold", alias="cold", mongo_client_class=mongomock_client)
    from mongoengine.connection import get_connection

    assert get_connection("ambient-a") is get_connection("ambient-b")
    assert get_connection("ambient-a") is get_connection("cold")
    assert get_connection("ambient-a") is get_connection("default")
    for alias in ("ambient-a", "ambient-b", "cold", "default"):
        from mongoengine.connection import get_db

        get_db(alias).client.drop_database(get_db(alias).name)


def reset(counter=40):
    from mongoengine.connection import get_db

    for alias in ("ambient-a", "ambient-b", "cold", "default"):
        db = get_db(alias)
        for name in db.list_collection_names():
            db[name].delete_many({})
    get_db("ambient-a")["mongoengine.counters"].insert_one(
        {"_id": "ambient.id", "next": counter}
    )


def manifest():
    return [
        {"document": AmbientA, "fields": {"name": "A"}},
        {"document": AmbientB, "fields": {"name": "B"}},
        {"document": AmbientC, "fields": {"name": "C"}},
        {"document": ColdD, "fields": {"name": "D"}},
    ]


def numbers(preview_result):
    return [row.number for row in preview_result]


def test_preview_is_read_only_and_rejects_b():
    reset()
    source = [
        AmbientA(name="A"),
        AmbientB(name="B"),
        AmbientC(name="C"),
        ColdD(name="D"),
    ]
    before = [dict(document._data) for document in source]
    result = preview_batch("APP-1", source)
    assert isinstance(result, BatchPreview)
    assert numbers(result) == [41, 42, 42, "Q-1"]
    assert [row.status for row in result] == ["ready", "rejected", "ready", "ready"]
    assert result[1].error["type"] == "ValidationError"
    assert result[3].bin == "D-Q-1"
    assert [dict(document._data) for document in source] == before

    from mongoengine.connection import get_db

    assert get_db("ambient-a")["mongoengine.counters"].find_one(
        {"_id": "ambient.id"}
    ) == {"_id": "ambient.id", "next": 40}
    assert get_db("cold")["mongoengine.counters"].count_documents({}) == 0
    for model in (AmbientA, AmbientB, AmbientC, ColdD):
        assert model.objects.count() == 0


def test_old_manifest_returns_for_review_without_writing():
    reset()
    result = preview_batch("APP-1", manifest())

    from mongoengine.connection import get_db

    get_db("cold")["mongoengine.counters"].insert_one({"_id": "cold.id", "next": 1})

    receipt = approve_batch("APP-1", result)
    assert receipt.status == "needs_review"
    assert receipt.replayed is False
    assert get_db("ambient-a")["mongoengine.counters"].find_one(
        {"_id": "ambient.id"}
    )["next"] == 40
    for model in (AmbientA, AmbientB, AmbientC, ColdD):
        assert model.objects.count() == 0
    assert get_approval_receipt("APP-1") is None

    again = preview_batch("APP-1b", manifest())
    assert numbers(again) == [41, 42, 42, "Q-2"]
    assert again[3].bin == "D-Q-2"


def test_failed_cold_write_rolls_back_everything():
    reset(40)
    from mongoengine.connection import get_db

    get_db("cold")["mongoengine.counters"].insert_one({"_id": "cold.id", "next": 1})
    result = preview_batch("APP-2", manifest())

    original_save = ColdD.save

    def failing_save(self, *args, **kwargs):
        raise RuntimeError("cold storage unavailable")

    ColdD.save = failing_save
    try:
        try:
            approve_batch("APP-2", result)
        except Exception:
            pass
        else:
            raise AssertionError("approval should fail")
    finally:
        ColdD.save = original_save

    assert get_db("ambient-a")["mongoengine.counters"].find_one(
        {"_id": "ambient.id"}
    )["next"] == 40
    assert get_db("cold")["mongoengine.counters"].find_one({"_id": "cold.id"})[
        "next"
    ] == 1
    for model in (AmbientA, AmbientB, AmbientC, ColdD):
        assert model.objects.count() == 0
    assert get_approval_receipt("APP-2") is None


def test_resubmission_replay_survives_restart_and_counter_moves():
    reset(40)
    from mongoengine.connection import get_db

    get_db("cold")["mongoengine.counters"].insert_one({"_id": "cold.id", "next": 1})
    result = preview_batch("APP-3", manifest())

    deliveries = []

    def broken(receipt):
        raise RuntimeError("receipt channel down")

    receipt = approve_batch("APP-3", result, receipt_sink=broken)
    assert receipt.status == "committed"
    assert receipt.delivered is False
    assert [row.status for row in receipt] == [
        "stored",
        "rejected",
        "stored",
        "stored",
    ]
    assert [row.number for row in receipt] == [41, 42, 42, "Q-2"]
    assert receipt[1].error["type"] == "ValidationError"

    assert AmbientA.objects.count() == 1
    assert AmbientC.objects.count() == 1
    assert ColdD.objects.count() == 1
    assert get_db("ambient-a")["mongoengine.counters"].find_one(
        {"_id": "ambient.id"}
    )["next"] == 42
    assert get_db("cold")["mongoengine.counters"].find_one({"_id": "cold.id"})[
        "next"
    ] == 2

    get_db("ambient-a")["mongoengine.counters"].update_one(
        {"_id": "ambient.id"}, {"$set": {"next": 99}}
    )
    get_db("cold")["mongoengine.counters"].update_one(
        {"_id": "cold.id"}, {"$set": {"next": 5}}
    )

    from mongoengine.connection import _clear_session

    replay = approve_batch("APP-3", result, receipt_sink=deliveries.append)
    assert replay.replayed is True
    assert replay.delivered is True
    assert [row.number for row in replay] == [41, 42, 42, "Q-2"]
    assert replay[3].bin == "D-Q-2"
    assert [row.status for row in replay] == [
        "stored",
        "rejected",
        "stored",
        "stored",
    ]
    assert deliveries == [replay]
    assert AmbientA.objects.count() == 1
    assert ColdD.objects.count() == 1
    assert get_db("ambient-a")["mongoengine.counters"].find_one(
        {"_id": "ambient.id"}
    )["next"] == 99


def test_redelivery_keeps_original_numbers_and_bins():
    reset(40)
    from mongoengine.connection import get_db

    get_db("cold")["mongoengine.counters"].insert_one({"_id": "cold.id", "next": 1})
    result = preview_batch("APP-5", manifest())
    receipt = approve_batch("APP-5", result)

    get_db("ambient-a")["mongoengine.counters"].update_one(
        {"_id": "ambient.id"}, {"$set": {"next": 80}}
    )
    get_db("cold")["mongoengine.counters"].update_one(
        {"_id": "cold.id"}, {"$set": {"next": 9}}
    )
    ColdD.objects.update(set__bin="D-moved")

    from mongoengine.approval import redeliver_receipt

    deliveries = []
    resent = redeliver_receipt("APP-5", deliveries.append)
    assert resent.numbers == receipt.numbers
    assert resent[3].bin == "D-Q-2"
    assert resent[1].status == "rejected"
    assert deliveries == [resent]
    assert ColdD.objects.get().bin == "D-moved"


def test_model_level_and_alias_entry_points():
    reset(40)
    preview = AmbientA.preview_batch("APP-6", [{"name": "only-a"}])
    assert preview.numbers == [41]
    receipt = AmbientA.approve_batch("APP-6", [{"name": "only-a"}])
    assert receipt.status == "committed"
    assert AmbientA.objects.count() == 1

    from mongoengine import approval

    assert approval.preview_inbound is approval.preview_batch
    assert approval.read_receipt("APP-6").numbers == [41]
    assert AmbientA.get_approval_receipt("APP-6").status == "committed"


def test_same_approval_id_different_manifest_conflicts():
    reset(40)
    approve_batch("APP-4", manifest())
    try:
        approve_batch(
            "APP-4",
            [{"document": AmbientA, "fields": {"name": "different"}}],
        )
    except ApprovalIDConflict as exc:
        assert "already in use" in str(exc)
    else:
        raise AssertionError("expected ApprovalIDConflict")
