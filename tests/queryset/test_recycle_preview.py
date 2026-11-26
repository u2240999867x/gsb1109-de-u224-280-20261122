import unittest

from mongoengine import (
    CASCADE,
    DENY,
    NULLIFY,
    PULL,
    Document,
    ListField,
    ReferenceField,
    StringField,
    connect,
    signals,
)
from mongoengine.queryset.base import BaseQuerySet
from tests.utils import get_as_pymongo


class TestRecyclePreview(unittest.TestCase):
    def setUp(self):
        connect(db="mongoenginetest")

        class CollectTask(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        class SpecialTask(CollectTask):
            peer = ReferenceField("self", reverse_delete_rule=DENY)

        class RawArtifact(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        class ArtifactX(RawArtifact):
            via_a = ReferenceField(CollectTask, reverse_delete_rule=CASCADE)
            via_b = ReferenceField(CollectTask, reverse_delete_rule=CASCADE)
            via_c = ReferenceField(CollectTask, reverse_delete_rule=CASCADE)

        class ArtifactY(RawArtifact):
            via_b = ReferenceField(CollectTask, reverse_delete_rule=CASCADE)
            deny_a = ReferenceField(CollectTask, reverse_delete_rule=DENY)

        class ArtifactZ(RawArtifact):
            via_b = ReferenceField(CollectTask, reverse_delete_rule=CASCADE)

        class AuditForm(Document):
            title = StringField()
            blocks = ReferenceField(RawArtifact, reverse_delete_rule=DENY)

        # Stored field names deliberately differ from the model names.
        class CollectIndex(Document):
            manifest = ListField(
                ReferenceField(RawArtifact, reverse_delete_rule=PULL),
                db_field="stored_manifest",
            )
            primary = ReferenceField(
                RawArtifact,
                reverse_delete_rule=NULLIFY,
                db_field="stored_primary",
            )

        self.CollectTask = CollectTask
        self.SpecialTask = SpecialTask
        self.RawArtifact = RawArtifact
        self.ArtifactX = ArtifactX
        self.ArtifactY = ArtifactY
        self.ArtifactZ = ArtifactZ
        self.AuditForm = AuditForm
        self.CollectIndex = CollectIndex

        for cls in (
            CollectTask,
            RawArtifact,
            AuditForm,
            CollectIndex,
        ):
            cls.drop_collection()

        self.tasks = {}
        for name in ("A", "B", "C"):
            self.tasks[name] = CollectTask(name=name).save()
        self.tasks["D"] = SpecialTask(name="D").save()
        self.tasks["E"] = SpecialTask(name="E").save()

        d, e = self.tasks["D"], self.tasks["E"]
        d.peer = e
        d.save()
        e.peer = d
        e.save()

        a, b, c = self.tasks["A"], self.tasks["B"], self.tasks["C"]
        self.x = ArtifactX(name="X", via_a=a, via_b=b, via_c=c).save()
        self.y = ArtifactY(name="Y", via_b=b, deny_a=a).save()
        self.z = ArtifactZ(name="Z", via_b=b).save()
        self.audit = AuditForm(title="audit", blocks=self.z).save()
        self.index = CollectIndex(
            manifest=[self.x, self.y, self.x], primary=self.y
        ).save()

        self.delete_signals = []
        signals.pre_delete.connect(self._on_pre_delete)
        signals.post_delete.connect(self._on_post_delete)

    def tearDown(self):
        signals.pre_delete.disconnect(self._on_pre_delete)
        signals.post_delete.disconnect(self._on_post_delete)

    def _on_pre_delete(self, sender, document, **kwargs):
        self.delete_signals.append(("pre", document.id))

    def _on_post_delete(self, sender, document, **kwargs):
        self.delete_signals.append(("post", document.id))

    def _by_name(self, documents):
        return {document.name: document for document in documents}

    def test_recycle_preview_is_available_on_queryset(self):
        preview = self.CollectTask.objects.recycle_preview()
        assert isinstance(self.CollectTask.objects, BaseQuerySet)
        assert {task.name for task in preview.candidates} == set("ABCDE")

    def test_select_all_with_audit_deny(self):
        preview = self.CollectTask.objects.order_by("name").recycle_preview()
        result = preview()

        recycled = self._by_name(result.recycled_documents)
        assert set(recycled) == {"C", "D", "E", "X"}
        assert self.x in result.recycled_documents
        # X must only appear once even though three CASCADE rules reach it.
        assert result.recycled_ids.count(self.x.id) == 1

        retained = self._by_name(result.retained_documents)
        assert set(retained) == {"A", "B"}

        # B is stopped by the surviving audit form through Z.
        b_paths = result.blocking_paths_for(retained["B"])
        assert len(b_paths) == 1
        b_chain = b_paths[0].documents
        assert b_chain[0].id == self.z.id
        assert b_chain[-1].id == self.audit.id
        assert b_paths[0].edges[-1].rule == DENY

        # A stays because Y remains with B and Y still references A.
        a_paths = result.blocking_paths_for(retained["A"])
        assert len(a_paths) == 1
        a_edges = a_paths[0].edges
        assert a_edges[0].document.id == self.y.id
        assert a_edges[0].rule == DENY
        assert any(
            edge.document.id == self.y.id and edge.field_name == "via_b"
            for edge in a_edges
        )
        assert a_edges[-1].rule == DENY

        # The index only pulls the two X references; Y and primary stay.
        index_clears = [
            clear
            for clear in result.field_clears
            if clear.document.id == self.index.id
        ]
        assert len(index_clears) == 1
        pull_clear = index_clears[0]
        assert pull_clear.operation == "pull"
        assert pull_clear.db_field == "stored_manifest"
        assert pull_clear.field_name == "manifest"
        assert pull_clear.removed_values == [self.x.id, self.x.id]

    def test_toggle_peer_tasks_and_order_independence(self):
        preview = self.CollectTask.objects.order_by("name").recycle_preview()

        without_e = preview([self.tasks[name] for name in ("A", "B", "C", "D")])
        assert set(without_e.recycled_ids) == {self.tasks["C"].id, self.x.id}
        retained = self._by_name(without_e.retained_documents)
        assert set(retained) == {"A", "B", "D"}

        d_paths = without_e.blocking_paths_for(retained["D"])
        assert any(
            edge.document.id == self.tasks["E"].id
            for path in d_paths
            for edge in path.edges
        )

        for order in (
            ["E", "D", "C", "B", "A"],
            ["C", "E", "A", "D", "B"],
        ):
            result = preview([self.tasks[name] for name in order])
            assert set(result.recycled_ids) == {
                self.tasks[name].id for name in ("C", "D", "E")
            } | {self.x.id}

    def test_preview_only_uses_captured_page_and_ignores_unknown_ids(self):
        page = self.CollectTask.objects.order_by("name")[:3].recycle_preview()
        assert {task.name for task in page.candidates} == {"A", "B", "C"}

        # An id outside the page is silently ignored.
        result = page([self.tasks["A"].id, self.tasks["E"].id])
        assert self.tasks["E"].id not in result.recycled_ids
        assert self.tasks["E"].id not in result.retained_ids

    def test_off_page_referrer_can_block_page_candidate(self):
        class OffPageAudit(Document):
            blocks = ReferenceField(self.CollectTask, reverse_delete_rule=DENY)

        OffPageAudit.drop_collection()
        off_page_form = OffPageAudit(blocks=self.tasks["A"]).save()

        # The audit form lives outside the task page, yet its DENY rule must
        # still be discovered through the model's reverse rules.
        page = self.CollectTask.objects.order_by("name")[:5].recycle_preview()
        result = page()

        retained = self._by_name(result.retained_documents)
        assert "A" in retained
        paths = result.blocking_paths_for(retained["A"])
        assert paths and paths[0].edges[-1].document.id == off_page_form.id
        assert paths[0].edges[-1].rule == DENY

    def test_preview_is_a_snapshot_and_never_writes(self):
        raw_before = get_as_pymongo(self.index)

        preview = self.CollectTask.objects.order_by("name").recycle_preview()
        stale = preview()
        assert set(stale.recycled_ids) == {
            self.tasks[name].id for name in ("C", "D", "E")
        } | {self.x.id}

        # Remove the audit DENY after the preview was opened.
        self.audit.delete()
        assert len(self.delete_signals) == 2

        # The stale preview keeps reporting the old conclusion.
        stale_after = preview()
        assert set(stale_after.recycled_ids) == set(stale.recycled_ids)
        stale_pull = next(
            clear
            for clear in stale_after.field_clears
            if clear.document.id == self.index.id and clear.operation == "pull"
        )
        assert stale_pull.removed_values == [self.x.id, self.x.id]

        # Reopening reflects the new database state.
        fresh = self.CollectTask.objects.order_by("name").recycle_preview()
        fresh_result = fresh()
        assert set(fresh_result.recycled_ids) == {
            self.tasks[name].id for name in ("A", "B", "C", "D", "E")
        } | {self.x.id, self.y.id, self.z.id}
        assert fresh_result.recycled_ids.count(self.x.id) == 1
        assert fresh_result.retained_documents == []

        index_clears = {
            clear.operation: clear
            for clear in fresh_result.field_clears
            if clear.document.id == self.index.id
        }
        assert index_clears["pull"].db_field == "stored_manifest"
        assert index_clears["pull"].removed_values == [
            self.x.id,
            self.y.id,
            self.x.id,
        ]
        assert index_clears["unset"].db_field == "stored_primary"

        # No preview evaluation wrote anything or fired a delete signal.
        raw_after = get_as_pymongo(self.index)
        assert raw_after == raw_before
        assert len(self.delete_signals) == 2

    def test_empty_queryset_preview(self):
        preview = self.CollectTask.objects(name__in=[]).recycle_preview()
        result = preview()
        assert result.recycled_documents == []
        assert result.retained_documents == []
        assert result.field_clears == []
        assert result.blocking_paths == {}

    def test_blocked_candidate_is_not_pulled_back_in_through_another_cascade(self):
        class Task(Document):
            name = StringField()

        class Artifact(Document):
            via_p = ReferenceField(Task, reverse_delete_rule=CASCADE)
            via_q = ReferenceField(Task, reverse_delete_rule=CASCADE)

        class Audit(Document):
            blocks = ReferenceField(Task, reverse_delete_rule=DENY)

        Task.drop_collection()
        Artifact.drop_collection()
        Audit.drop_collection()

        p = Task(name="P").save()
        q = Task(name="Q").save()
        w = Artifact(via_p=p, via_q=q).save()
        Audit(blocks=p).save()

        result = Task.objects.order_by("name").recycle_preview()()

        assert result.retained_ids == [p.id]
        assert set(result.recycled_ids) == {q.id, w.id}
        assert result.blocking_paths_for(result.retained_documents[0])
