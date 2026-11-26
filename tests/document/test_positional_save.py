import pytest

from mongoengine import (
    Document,
    EmbeddedDocument,
    EmbeddedDocumentListField,
    IntField,
    StringField,
)
from mongoengine.errors import (
    InvalidDocumentError,
    SaveConditionError,
    SaveCountConflictError,
    SaveGenerationConflictError,
    SaveLocationNotUniqueError,
)
from tests.utils import MongoDBTestCase, requires_mongodb_gte_36


class CountLine(EmbeddedDocument):
    location = StringField(db_field="lc")
    generation = IntField(db_field="gn")
    counted = IntField(db_field="ct")
    note = StringField(db_field="nt")


class Batch(Document):
    status = StringField(db_field="st")
    lines = EmbeddedDocumentListField(CountLine, db_field="ln")


POSITIONAL = {"lines": ("location", "generation", "counted")}
PROJECTED_FIELDS = ("status", "lines.location", "lines.generation", "lines.counted")


def raw_line(location, generation, counted, note=None):
    line = {"lc": location, "gn": generation, "ct": counted}
    if note is not None:
        line["nt"] = note
    return line


class TestPositionalSave(MongoDBTestCase):
    """Identity based saves of embedded rows loaded through only()."""

    def _seed(self):
        Batch.drop_collection()
        Batch(
            status="pending",
            lines=[
                CountLine(location="A", generation=17, counted=10),
                CountLine(location="B", generation=8, counted=8),
            ],
        ).save()

    def _raw_replace(self, raw_lines, status=None):
        collection = Batch._get_collection()
        raw = collection.find_one({})
        raw["ln"] = raw_lines
        if status is not None:
            raw["st"] = status
        collection.replace_one({"_id": raw["_id"]}, raw)

    def _readback(self):
        batch = Batch.objects.first()
        return batch.status, [
            (line.location, line.generation, line.counted, line.note)
            for line in batch.lines
        ]

    def _load_ui(self):
        return Batch.objects.only(*PROJECTED_FIELDS).first()

    @requires_mongodb_gte_36
    def test_projection_metadata_is_recorded(self):
        self._seed()
        ui = self._load_ui()
        assert ui._only_loaded_fields == {"st", "ln.lc", "ln.gn", "ln.ct"}
        assert ui._only_loaded_snapshot["ln"] == [
            {"lc": "A", "gn": 17, "ct": 10},
            {"lc": "B", "gn": 8, "ct": 8},
        ]
        assert Batch.objects.first()._only_loaded_fields is None

    @requires_mongodb_gte_36
    def test_reordered_backend_rows_are_updated_by_identity(self):
        self._seed()
        ui = self._load_ui()
        ui.lines[0].counted = 12
        ui.lines[1].counted = 10

        # Backend reorders B/A, adds a note to B and inserts a new C row
        # before the write reaches the database.
        self._raw_replace(
            [
                raw_line("B", 8, 8, note="backend note"),
                raw_line("A", 17, 10),
                raw_line("C", 8, 8),
            ]
        )

        ui.save(save_condition={"status": "pending"}, positional=POSITIONAL)

        status, rows = self._readback()
        assert status == "pending"
        assert rows == [
            ("B", 8, 10, "backend note"),
            ("A", 17, 12, None),
            ("C", 8, 8, None),
        ]

    @requires_mongodb_gte_36
    def test_repeat_save_on_same_object_tracks_new_state(self):
        self._seed()
        ui = self._load_ui()
        ui.lines[0].counted = 12
        ui.lines[1].counted = 10
        self._raw_replace(
            [raw_line("B", 8, 8), raw_line("A", 17, 10), raw_line("C", 8, 8)]
        )
        ui.save(save_condition={"status": "pending"}, positional=POSITIONAL)

        # The in-memory object keeps the original A/B ordering; B is now edited
        # again while the backend concurrently changes A.
        ui.lines[1].counted = 11
        self._raw_replace(
            [raw_line("B", 8, 10), raw_line("A", 17, 13), raw_line("C", 8, 8)]
        )
        ui.save(save_condition={"status": "pending"}, positional=POSITIONAL)

        _, rows = self._readback()
        assert [(row[0], row[2]) for row in rows] == [
            ("B", 11),
            ("A", 13),
            ("C", 8),
        ]

    @requires_mongodb_gte_36
    def test_generation_change_is_rejected_and_draft_is_kept(self):
        self._seed()
        ui = self._load_ui()
        ui.lines[0].counted = 12
        ui.lines[1].counted = 10

        # B was deleted and recreated with the same location/count but a new
        # generation; C still carries its old generation and count.
        self._raw_replace(
            [raw_line("B", 9, 8), raw_line("A", 17, 10), raw_line("C", 8, 8)]
        )

        with pytest.raises(SaveGenerationConflictError) as exc_info:
            ui.save(save_condition={"status": "pending"}, positional=POSITIONAL)
        assert exc_info.value.location == "B"
        assert exc_info.value.expected_generation == 8
        assert exc_info.value.actual_generation == 9

        _, rows = self._readback()
        assert rows == [
            ("B", 9, 8, None),
            ("A", 17, 10, None),
            ("C", 8, 8, None),
        ]
        # Nothing was written and the unsaved edits survive on the object.
        assert [(line.location, line.counted) for line in ui.lines] == [
            ("A", 12),
            ("B", 10),
        ]

    @requires_mongodb_gte_36
    def test_count_change_is_rejected_and_draft_is_kept(self):
        self._seed()
        ui = self._load_ui()
        ui.lines[0].counted = 12
        ui.lines[1].counted = 10

        self._raw_replace([raw_line("A", 17, 10), raw_line("B", 8, 9)])

        with pytest.raises(SaveCountConflictError) as exc_info:
            ui.save(save_condition={"status": "pending"}, positional=POSITIONAL)
        assert exc_info.value.location == "B"
        assert exc_info.value.expected_count == 8
        assert exc_info.value.actual_count == 9

        _, rows = self._readback()
        assert rows == [("A", 17, 10, None), ("B", 8, 9, None)]
        assert [(line.location, line.counted) for line in ui.lines] == [
            ("A", 12),
            ("B", 10),
        ]

    @requires_mongodb_gte_36
    def test_duplicate_location_is_rejected(self):
        self._seed()
        self._raw_replace(
            [
                raw_line("A", 17, 10),
                raw_line("B", 8, 8),
                raw_line("B", 8, 8),
            ]
        )
        ui = self._load_ui()
        ui.lines[0].counted = 12
        ui.lines[1].counted = 10

        with pytest.raises(SaveLocationNotUniqueError) as exc_info:
            ui.save(save_condition={"status": "pending"}, positional=POSITIONAL)
        assert exc_info.value.location == "B"

        _, rows = self._readback()
        assert rows == [
            ("A", 17, 10, None),
            ("B", 8, 8, None),
            ("B", 8, 8, None),
        ]
        assert [(line.location, line.counted) for line in ui.lines][:2] == [
            ("A", 12),
            ("B", 10),
        ]

    @requires_mongodb_gte_36
    def test_failed_save_condition_keeps_draft(self):
        self._seed()
        ui = self._load_ui()
        ui.lines[0].counted = 12
        ui.lines[1].counted = 10

        self._raw_replace(
            [raw_line("A", 17, 10), raw_line("B", 8, 8)], status="checked"
        )

        with pytest.raises(SaveConditionError):
            ui.save(save_condition={"status": "pending"}, positional=POSITIONAL)

        status, rows = self._readback()
        assert status == "checked"
        assert rows == [("A", 17, 10, None), ("B", 8, 8, None)]
        assert [(line.location, line.counted) for line in ui.lines] == [
            ("A", 12),
            ("B", 10),
        ]

    @requires_mongodb_gte_36
    def test_save_without_condition_and_with_bare_list_projection(self):
        self._seed()
        ui = Batch.objects.only("status", "lines").first()
        ui.lines[0].counted = 12
        ui.lines[1].counted = 10
        self._raw_replace(
            [raw_line("B", 8, 8, note="kept"), raw_line("A", 17, 10)]
        )

        ui.save(positional=POSITIONAL)

        _, rows = self._readback()
        assert rows == [
            ("B", 8, 10, "kept"),
            ("A", 17, 12, None),
        ]

    @requires_mongodb_gte_36
    def test_save_without_any_edit_is_a_noop(self):
        self._seed()
        ui = self._load_ui()
        assert ui._delta() == ({}, {})
        ui.save(save_condition={"status": "pending"}, positional=POSITIONAL)
        _, rows = self._readback()
        assert rows == [("A", 17, 10, None), ("B", 8, 8, None)]

    def test_legacy_save_paths_are_unchanged(self):
        self._seed()
        plain = Batch.objects.first()
        plain.lines[0].counted = 5
        plain.save()
        assert Batch.objects.first().lines[0].counted == 5

        plain.status = "pending"
        plain.save(save_condition={"status": "pending"})

        plain.lines[1].counted = 3
        with pytest.raises(SaveConditionError):
            plain.save(save_condition={"status": "checked"})
        assert Batch.objects.first().lines[1].counted == 8

    def test_positional_save_requires_only_projection(self):
        self._seed()
        plain = Batch.objects.first()
        plain.lines[0].counted = 5
        with pytest.raises(InvalidDocumentError):
            plain.save(positional=POSITIONAL)

    def test_positional_save_validates_its_spec(self):
        self._seed()
        ui = self._load_ui()
        with pytest.raises(InvalidDocumentError):
            ui.save(positional={"unknown": ("location", "generation", "counted")})
        with pytest.raises(InvalidDocumentError):
            ui.save(positional={"lines": ("missing", "generation", "counted")})
        with pytest.raises(InvalidDocumentError):
            ui.save(positional={"status": ("location", "generation", "counted")})
        with pytest.raises(InvalidDocumentError):
            ui.save(positional={"lines": ("location", "generation")})
