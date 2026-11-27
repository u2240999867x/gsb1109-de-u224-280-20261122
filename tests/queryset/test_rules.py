import mongomock
import pytest
from unittest.mock import MagicMock

from mongoengine import (
    BooleanField,
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    IntField,
    ListField,
    Q,
    StringField,
    connect,
    disconnect_all,
)
from mongoengine.errors import RuleConflictError
from mongoengine.queryset.rules import RuleSet, UpdateRule

DB = "mongoenginerules305"


@pytest.fixture
def form_class():
    """Build a submission model whose fields all use db_field aliases."""

    disconnect_all()
    connect(
        db=DB,
        host="mongodb://localhost",
        mongo_client_class=mongomock.MongoClient,
        uuidRepresentation="pythonLegacy",
    )

    class Reading(EmbeddedDocument):
        no = IntField(db_field="rd_no")
        state = StringField(db_field="rd_state")
        score = IntField(db_field="rd_score")
        note = StringField(db_field="rd_note")
        private_note = StringField(db_field="rd_private")

    class Device(EmbeddedDocument):
        device = StringField(db_field="dv_code")
        readings = ListField(EmbeddedDocumentField(Reading), db_field="dv_readings")

    class Adjustment(EmbeddedDocument):
        no = StringField(db_field="aj_no")
        done = BooleanField(db_field="aj_done")

    class Form(Document):
        devices = ListField(EmbeddedDocumentField(Device), db_field="fm_devices")
        adjustments = ListField(
            EmbeddedDocumentField(Adjustment), db_field="fm_adjustments"
        )

    Form.drop_collection()
    yield Form, Reading, Device, Adjustment
    disconnect_all()


def build_form(Form, Reading, Device, Adjustment, east_score=79):
    return Form(
        devices=[
            Device(
                device="east",
                readings=[
                    Reading(
                        no=7,
                        state="待核",
                        score=east_score,
                        note="n1",
                        private_note="p1",
                    ),
                    Reading(no=8, state="已核", score=85, note="n2", private_note="p2"),
                ],
            ),
            Device(
                device="west",
                readings=[
                    Reading(no=7, state="待核", score=90, note="n3", private_note="p3")
                ],
            ),
        ],
        adjustments=[
            Adjustment(no="07", done=False),
            Adjustment(no="7", done=False),
        ],
    )


def cleaning_rules(second_update):
    return [
        UpdateRule(
            name="甲",
            path="devices.readings",
            match=Q(no="07", state="待核"),
            update={"state": "已核", "note": "复核"},
        ),
        UpdateRule(
            name="乙",
            path="devices.readings",
            scope=Q(device="east"),
            match=Q(no__in=["07", "08"]) & (Q(score__gte=80) | Q(state="已核")),
            update=second_update,
        ),
        UpdateRule(
            name="丙",
            path="adjustments",
            match=Q(no="07"),
            update={"done": True},
        ),
    ]


def raw(Form):
    return Form.objects.as_pymongo().first()


def test_rules_apply_against_pre_image(form_class):
    Form, Reading, Device, Adjustment = form_class
    build_form(Form, Reading, Device, Adjustment).save()

    result = Form.objects.update_rules(cleaning_rules({"unset__note": True}))

    assert result.ok is True
    assert result.conflicts == []
    assert len(result.applied) == 6

    document = raw(Form)
    east, west = document["fm_devices"]
    notes = east["dv_readings"]

    # 甲 applies to the east and west pending "07" readings.
    assert notes[0]["rd_state"] == "已核"
    assert notes[0]["rd_note"] == "复核"
    assert east["dv_readings"][1]["rd_no"] == 8
    assert notes[1]["rd_state"] == "已核"
    assert notes[1]["rd_score"] == 85
    assert "rd_note" not in notes[1]

    west_reading = west["dv_readings"][0]
    assert west_reading["rd_state"] == "已核"
    assert west_reading["rd_note"] == "复核"

    # 丙 only touches the textual "07" adjustment.
    assert document["fm_adjustments"] == [
        {"aj_no": "07", "aj_done": True},
        {"aj_no": "7", "aj_done": False},
    ]


def test_rule_order_does_not_matter(form_class):
    Form, Reading, Device, Adjustment = form_class
    build_form(Form, Reading, Device, Adjustment).save()
    rules = cleaning_rules({"unset__note": True})

    result = Form.objects.update_rules(list(reversed(rules)))

    assert result.ok is True
    assert result.conflicts == []
    assert raw(Form)["fm_devices"][1]["dv_readings"][0]["rd_note"] == "复核"


def test_conflict_reported_and_whole_submission_untouched(form_class):
    Form, Reading, Device, Adjustment = form_class
    build_form(Form, Reading, Device, Adjustment).save()

    # Another job moves the east reading to the inclusive boundary before the
    # batch is submitted; rule 乙 now also matches it on the pre-image.
    Form.objects(devices__readings__no="07").update(
        set__devices__S__readings__S__score=80
    )

    result = Form.objects.update_rules(cleaning_rules({"unset__note": True}))

    assert result.ok is False
    assert len(result.conflicts) == 1
    conflict = result.conflicts[0]
    assert conflict.path == "fm_devices.0.dv_readings.0.rd_note"
    assert conflict.rules == ("乙", "甲")

    # The entire submission is left untouched: not even documents without
    # conflicts (丙's adjustment) are written.
    assert result.applied == []
    document = raw(Form)
    assert document["fm_devices"][0]["dv_readings"][0]["rd_note"] == "n1"
    assert document["fm_adjustments"] == [
        {"aj_no": "07", "aj_done": False},
        {"aj_no": "7", "aj_done": False},
    ]

    with pytest.raises(RuleConflictError) as excinfo:
        Form.objects.update_rules(
            cleaning_rules({"unset__note": True}), raise_on_conflict=True
        )
    assert excinfo.value.conflicts


def test_removing_internal_note_lets_both_rules_apply(form_class):
    Form, Reading, Device, Adjustment = form_class
    build_form(Form, Reading, Device, Adjustment).save()

    Form.objects(devices__readings__no="07").update(
        set__devices__S__readings__S__score=80
    )

    result = Form.objects.update_rules(cleaning_rules({"unset__private_note": True}))

    assert result.ok is True
    assert result.conflicts == []

    document = raw(Form)
    reading = document["fm_devices"][0]["dv_readings"][0]
    assert reading["rd_state"] == "已核"
    assert reading["rd_note"] == "复核"
    assert "rd_private" not in reading
    assert document["fm_adjustments"] == [
        {"aj_no": "07", "aj_done": True},
        {"aj_no": "7", "aj_done": False},
    ]


def test_dry_run_reports_without_writing(form_class):
    Form, Reading, Device, Adjustment = form_class
    build_form(Form, Reading, Device, Adjustment).save()

    result = Form.objects.update_rules(
        cleaning_rules({"unset__note": True}), dry_run=True
    )

    assert result.ok is True
    assert result.dry_run is True
    assert len(result.corrections) == 6
    assert result.applied == []
    assert raw(Form)["fm_adjustments"][0]["aj_done"] is False


def test_rule_set_and_document_wrapper(form_class):
    Form, Reading, Device, Adjustment = form_class
    form = build_form(Form, Reading, Device, Adjustment)
    form.save()

    rules = RuleSet(
        [
            UpdateRule(
                path="devices.readings",
                match={"state": "待核"},
                update={"state": "已核"},
            )
        ]
    )

    result = form.apply_rules(rules)
    assert result.ok is True
    assert raw(Form)["fm_devices"][0]["dv_readings"][0]["rd_state"] == "已核"


def test_integer_and_textual_number_fields_use_their_own_semantics(form_class):
    """The reading ``no`` is an integer; adjustments carry textual numbers.

    Rule 甲's ``"07"`` is coerced to ``7`` for the integer reading field (so it
    matches both devices), while rule 丙 must only match the textual ``"07"``
    adjustment and not the textual ``"7"`` one.
    """
    Form, Reading, Device, Adjustment = form_class
    build_form(Form, Reading, Device, Adjustment).save()

    result = Form.objects.update_rules(
        [
            UpdateRule(
                path="devices.readings",
                match={"no": "07", "state": "待核"},
                update={"state": "已核", "note": "复核"},
                name="甲",
            ),
            UpdateRule(
                path="adjustments",
                match={"no": "07"},
                update={"done": True},
                name="丙",
            ),
        ]
    )

    assert result.ok is True
    document = raw(Form)
    east, west = document["fm_devices"]
    assert east["dv_readings"][0]["rd_no"] == 7
    assert east["dv_readings"][0]["rd_state"] == "已核"
    assert east["dv_readings"][1]["rd_no"] == 8
    assert east["dv_readings"][1]["rd_state"] == "已核"
    assert west["dv_readings"][0]["rd_no"] == 7
    assert west["dv_readings"][0]["rd_state"] == "已核"
    assert document["fm_adjustments"] == [
        {"aj_no": "07", "aj_done": True},
        {"aj_no": "7", "aj_done": False},
    ]


def test_array_filters_calls_still_work(form_class, monkeypatch):
    """Passing ``array_filters`` straight through keeps working."""
    Form, Reading, Device, Adjustment = form_class

    captured = {}
    result = MagicMock()
    result.raw_result = {"n": 1}

    def fake_update_one(collection, query, update, **kwargs):
        captured["array_filters"] = kwargs.get("array_filters")
        return result

    collection_cls = type(Form._get_collection())
    monkeypatch.setattr(collection_cls, "update_one", fake_update_one, raising=False)

    Form.objects().update_one(
        __raw__={"$set": {"fm_devices.$[device].dv_readings.$[reading].rd_note": "x"}},
        array_filters=[
            {"device.dv_code": {"$eq": "east"}},
            {"reading.rd_no": {"$eq": 7}},
        ],
    )

    assert captured["array_filters"] == [
        {"device.dv_code": {"$eq": "east"}},
        {"reading.rd_no": {"$eq": 7}},
    ]
