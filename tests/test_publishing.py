import os
import tempfile
from unittest import TestCase

from transitions.extensions.publishing import (
    CommandConflict,
    CommandResult,
    DeferredWarehouse,
    Manifest,
    PendingCommandError,
    PublishingMachine,
    Receipt,
    SQLiteCommandStore,
    Warehouse,
)


PARTITION = "east/2026-11-27"


class PublishingModel(object):
    def __init__(self):
        self.state = None
        self.cleaned = []
        self.trials = []
        self.confirmed_version = None
        self.pending_command_id = None

    def clean(self, event_data):
        self.cleaned.append(event_data.candidate)
        return {"input": event_data.candidate, "size": len(event_data.candidate)}

    def trial(self, event_data):
        self.trials.append(event_data.cleaned)
        return event_data.cleaned["size"] > 1

    def before_publish(self, event_data):
        self.before_publish_seen = getattr(event_data, "command_id", None)

    def after_publish(self, event_data):
        self.after_publish_seen = getattr(event_data, "command_id", None)


class PublishingTestCase(TestCase):
    def setUp(self):
        handle, self.db_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(handle)
        os.unlink(self.db_path)
        self.warehouse = Warehouse()
        self.machines = []
        self.models = []

    def tearDown(self):
        for machine in self.machines:
            machine.command_store.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def make_machine(self, warehouse=None, model_class=PublishingModel, **kwargs):
        model = model_class()
        options = {
            "states": ["ready"],
            "initial": "ready",
            "send_event": True,
            "warehouse": warehouse if warehouse is not None else self.warehouse,
            "db_path": self.db_path,
            "partition": PARTITION,
            "publish_trigger": "publish",
            "transitions": [["publish", "ready", "ready"]],
        }
        options.update(kwargs)
        machine = PublishingMachine(model, **options)
        self.machines.append(machine)
        self.models.append(model)
        return model, machine

    def seed(self, version=40):
        model, machine = self.make_machine()
        machine.seed(version)
        return model, machine


class TestPublicationHandover(PublishingTestCase):
    def setUp(self):
        super(TestPublicationHandover, self).setUp()
        self.warehouse = DeferredWarehouse()

    def make_machine(self, **kwargs):
        return super(TestPublicationHandover, self).make_machine(warehouse=self.warehouse, **kwargs)

    def test_handover_replay_and_late_response(self):
        _, seed_machine = self.seed(40)
        self.assertEqual(40, seed_machine.get_confirmed_version())

        _, before_exit = self.make_machine()
        prepared = before_exit.prepare_command("C41", candidates=["a", "bb"])
        self.assertEqual(41, prepared.manifest.version)
        self.assertEqual("prepared", prepared.state)
        self.assertEqual(2, len(before_exit.command_store.load_trials(PARTITION, "C41")))

        model, successor = self.make_machine()
        first = successor.resume_command("C41")
        self.assertTrue(first.pending)
        self.assertEqual(40, model.confirmed_version)
        self.assertEqual("C41", model.pending_command_id)
        self.assertEqual(40, first.confirmed_version)

        blocked_model, blocked_machine = self.make_machine()
        with self.assertRaises(PendingCommandError) as raised:
            blocked_machine.publish_command("C42", candidates=["ccc"])
        self.assertEqual("C41", raised.exception.pending_command_id)
        self.assertEqual(40, raised.exception.confirmed_version)
        self.assertEqual(40, blocked_model.confirmed_version)
        self.assertEqual([40, 41], [manifest.version for manifest in self.warehouse.manifests(PARTITION)])
        self.assertIsNone(self.warehouse.receipt_for_command(PARTITION, "C42"))

        resend = successor.publish_command("C41", candidates=["a", "bb"])
        self.assertFalse(resend.pending)
        self.assertEqual(41, resend.receipt.version)
        self.assertEqual(41, model.confirmed_version)
        self.assertEqual(41, successor.get_confirmed_version())
        self.assertEqual([], model.cleaned)

        published = blocked_machine.publish_command("C42", candidates=["ccc"])
        self.assertEqual(42, published.receipt.version)
        self.assertEqual(resend.receipt.token, published.receipt.previous_digest)
        self.assertEqual(42, blocked_model.confirmed_version)

        self.warehouse.release_first_response()
        late = before_exit.deliver(first.response)
        self.assertEqual(41, late.receipt.version)
        self.assertEqual(41, late.receipt.version)
        self.assertEqual(42, late.confirmed_version)
        self.assertEqual(42, before_exit.get_confirmed_version())

        replay = before_exit.publish_command("C41", candidates=["a", "bb"])
        self.assertEqual(41, replay.receipt.version)
        self.assertEqual(42, replay.confirmed_version)
        self.assertEqual(42, before_exit.get_confirmed_version())
        self.assertEqual([], model.cleaned)

        reopened_model, reopened = self.make_machine()
        self.assertEqual(42, reopened.get_confirmed_version())
        self.assertEqual(42, reopened_model.confirmed_version)
        self.assertIsNone(reopened.get_pending_command_id())

        versions = [manifest.version for manifest in self.warehouse.manifests(PARTITION)]
        self.assertEqual([40, 41, 42], versions)

    def test_command_conflict_is_read_only(self):
        self.warehouse = Warehouse()
        _, machine = self.seed(40)
        _, worker = self.make_machine()
        worker.publish_command("C41", candidates=["a", "bb"])
        self.assertEqual(41, worker.get_confirmed_version())
        before = [manifest.as_dict() for manifest in self.warehouse.manifests(PARTITION)]

        _, conflicting = self.make_machine()
        with self.assertRaises(CommandConflict):
            conflicting.publish_command("C41", candidates=["a", "different"])
        self.assertEqual(41, conflicting.get_confirmed_version())
        self.assertEqual(before, [manifest.as_dict() for manifest in self.warehouse.manifests(PARTITION)])

    def test_replayed_receipt_survives_newer_version(self):
        _, machine = self.seed(40)
        _, worker = self.make_machine()
        first = worker.publish_command("C41", candidates=["a", "bb"], simulate_lost_response=True)
        self.assertTrue(first.pending)
        self.assertEqual(40, worker.get_confirmed_version())
        second = worker.publish_command("C41", candidates=["a", "bb"])
        self.assertEqual(41, second.receipt.version)
        third = worker.publish_command("C42", candidates=["ccc"])
        self.assertEqual(42, third.receipt.version)
        replay = worker.publish_command("C41", candidates=["a", "bb"])
        self.assertEqual(second.receipt.token, replay.receipt.token)
        self.assertEqual(41, replay.receipt.version)
        self.assertEqual(42, replay.confirmed_version)


class TestWarehouse(PublishingTestCase):
    def test_current_receipt_is_the_only_predecessor(self):
        warehouse = Warehouse()
        first = warehouse.seed(PARTITION, 40)
        manifest41 = Manifest.create(PARTITION, 41, "C41", candidates=["a"])
        receipt41 = warehouse.publish(manifest41, previous_receipt=first)
        manifest42 = Manifest.create(PARTITION, 42, "C42", candidates=["b"])
        with self.assertRaises(Exception):
            warehouse.publish(manifest42, previous_receipt=first)
        receipt42 = warehouse.publish(manifest42, previous_receipt=receipt41)
        self.assertEqual(42, receipt42.version)
        self.assertEqual(receipt41.token, receipt42.previous_digest)

    def test_same_command_and_content_returns_first_receipt(self):
        warehouse = Warehouse()
        first = warehouse.seed(PARTITION, 40)
        manifest41 = Manifest.create(PARTITION, 41, "C41", candidates=["a"])
        receipt41 = warehouse.publish(manifest41, previous_receipt=first)
        manifest42 = Manifest.create(PARTITION, 42, "C42", candidates=["b"])
        warehouse.publish(manifest42, previous_receipt=receipt41)
        repeated = warehouse.publish(manifest41, previous_receipt=first)
        self.assertEqual(receipt41.token, repeated.token)
        self.assertEqual(41, repeated.version)
        self.assertEqual(42, warehouse.current_receipt(PARTITION).version)

    def test_same_command_with_different_content_is_rejected(self):
        warehouse = Warehouse()
        first = warehouse.seed(PARTITION, 40)
        warehouse.publish(Manifest.create(PARTITION, 41, "C41", candidates=["a"]), first)
        with self.assertRaises(CommandConflict):
            warehouse.publish(Manifest.create(PARTITION, 41, "C41", candidates=["b"]), first)

    def test_one_manifest_per_version(self):
        warehouse = Warehouse()
        first = warehouse.seed(PARTITION, 40)
        warehouse.publish(Manifest.create(PARTITION, 41, "C41", candidates=["a"]), first)
        with self.assertRaises(Exception):
            warehouse.publish(Manifest.create(PARTITION, 41, "C99", candidates=["b"]), first)
        self.assertEqual(41, warehouse.current_receipt(PARTITION).version)


class TestCandidateSelection(PublishingTestCase):
    def test_only_accepted_candidate_manifest_is_published(self):
        model, machine = self.make_machine(clean="clean", trial="trial")
        machine.seed(40)
        result = machine.publish_command("C41", candidates=["a", "bb", "c"])
        self.assertFalse(result.pending)
        self.assertEqual([{"input": "bb", "size": 2}], list(result.manifest.candidates))
        self.assertEqual(["a", "bb", "c"], model.cleaned)
        self.assertEqual(3, len(model.trials))
        published = self.warehouse.manifest_for(PARTITION, 41)
        self.assertEqual(["bb"], [candidate["input"] for candidate in published.candidates])

    def test_all_rejected_candidates_do_not_reach_warehouse(self):
        _, machine = self.make_machine(clean="clean", trial="trial")
        machine.seed(40)
        with self.assertRaises(Exception):
            machine.publish_command("C41", candidates=["a", "b"])
        self.assertEqual(40, machine.get_confirmed_version())
        self.assertIsNone(self.warehouse.manifest_for(PARTITION, 41))
        self.assertIsNone(machine.get_pending_command_id())

    def test_trial_results_are_persisted(self):
        _, machine = self.make_machine(clean="clean", trial="trial")
        machine.seed(40)
        machine.prepare_command("C41", candidates=["a", "bb"])
        trials = machine.command_store.load_trials(PARTITION, "C41")
        self.assertEqual([False, True], [record["accepted"] for record in trials])
        self.assertEqual(["a", "bb"], [record["raw"] for record in trials])

    def test_transition_conditions_reject_candidates(self):
        class Model(object):
            def __init__(self):
                self.state = None

            def clean(self, event_data):
                return event_data.candidate

            def acceptable(self, event_data):
                return event_data.candidate > 1

        model, machine = self.make_machine(
            model_class=Model, clean="clean",
            transitions=[{"trigger": "publish", "source": "ready", "dest": "ready",
                          "conditions": "acceptable"}])
        machine.seed(40)
        result = machine.publish_command("C41", candidates=[1, 2, 3])
        self.assertEqual((2, 3), tuple(result.manifest.candidates))
        self.assertEqual(41, result.receipt.version)
        published = self.warehouse.manifest_for(PARTITION, 41)
        self.assertEqual([2, 3], list(published.candidates))

        with self.assertRaises(Exception):
            machine.publish_command("C42", candidates=[0, 1])
        self.assertEqual(41, machine.get_confirmed_version())

    def test_command_event_data_exposes_context(self):
        seen = []

        class Model(object):
            def __init__(self):
                self.state = None

            def clean(self, event_data):
                seen.append((event_data.command_id, event_data.partition, event_data.version))
                return event_data.candidate

            def trial(self, event_data):
                seen.append(event_data.store.path)
                return True

        _, machine = self.make_machine(model_class=Model, clean="clean", trial="trial")
        machine.seed(40)
        machine.publish_command("C41", candidates=["a"])
        self.assertEqual(("C41", PARTITION, 41), seen[0])
        self.assertEqual(machine.command_store.path, seen[1])


class TestOrdinaryTrigger(PublishingTestCase):
    def test_regular_trigger_order_and_return_value(self):
        calls = []

        class Model(object):
            def __init__(self):
                self.state = None

            def before(self, event_data):
                calls.append(("before", getattr(event_data, "command_id", None)))

            def after(self, event_data):
                calls.append(("after", getattr(event_data, "command_id", None)))

        model, machine = self.make_machine(
            model_class=Model,
            transitions=[{"trigger": "publish", "source": "ready", "dest": "ready",
                          "before": "before", "after": "after"}])
        self.assertTrue(model.publish())
        self.assertEqual([("before", None), ("after", None)], calls)
        self.assertIsNone(model.confirmed_version)

    def test_regular_trigger_without_command_is_unchanged(self):
        model, _ = self.make_machine()
        self.assertTrue(model.publish())
        self.assertEqual([], model.cleaned)
        self.assertIsNone(model.confirmed_version)


class TestSQLiteStore(PublishingTestCase):
    def test_confirmation_does_not_regress(self):
        store = SQLiteCommandStore(self.db_path)
        first = Receipt.create(Manifest.create(PARTITION, 41, "C41"))
        second = Receipt.create(Manifest.create(PARTITION, 42, "C42"), previous_receipt=first)
        store.set_confirmation(PARTITION, second)
        self.assertEqual(42, store.set_confirmation(PARTITION, first).version)
        self.assertEqual(42, store.confirmed_version(PARTITION))
        store.close()

    def test_result_shape(self):
        _, machine = self.seed(40)
        result = machine.publish_command("C41", candidates=["a", "bb"])
        self.assertIsInstance(result, CommandResult)
        self.assertEqual("confirmed", result.status)
        self.assertEqual(41, result.version)
        self.assertEqual(41, result.confirmed_version)
