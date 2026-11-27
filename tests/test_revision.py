try:
    from builtins import object
except ImportError:  # pragma: no cover
    pass

import threading
from unittest import TestCase, skipIf

from transitions import Machine, MachineError

try:
    from transitions.extensions import HierarchicalMachine
except ImportError:  # pragma: no cover
    HierarchicalMachine = None

try:
    import asyncio
    from transitions.extensions.asyncio import AsyncMachine
except ImportError:  # pragma: no cover
    asyncio = None
    AsyncMachine = None


V1_STATES = ["idle", "read", "review", "raw", "done"]
V2_STATES = ["idle", "read", "accept", "source", "done"]


def make_v1_transitions(before_cb):
    return [
        {"trigger": "start", "source": "idle", "dest": "read"},
        {"trigger": "to_review", "source": "read", "dest": "review"},
        {"trigger": "process_raw", "source": "review", "dest": "raw"},
        {"trigger": "deliver", "source": ["read", "review", "raw"], "dest": "done",
         "before": before_cb},
    ]


def make_v2_transitions(before_cb, after_cb):
    return [
        {"trigger": "start", "source": "idle", "dest": "read"},
        {"trigger": "to_review", "source": "read", "dest": "accept"},
        {"trigger": "process_raw", "source": "accept", "dest": "source"},
        {"trigger": "deliver", "source": ["read", "accept", "source"], "dest": "done",
         "before": before_cb, "after": after_cb},
    ]


class Batch(object):
    """A model whose ``read`` enter callback can block until a gate is released."""

    def __init__(self, gate=None, block=False, trace=None):
        self.gate = gate
        self.block = block
        self.trace = trace

    def on_enter_read(self):
        if not self.block:
            return
        self.trace.append("enter-read:start")
        self.gate.wait()
        self.trace.append("enter-read:done")


class TestDefinitionRevision(TestCase):

    def setUp(self):
        self.events = []
        self.gate = threading.Event()

        def v1_deliver_before():
            self.events.append("v1-deliver-before")

        def v2_deliver_before():
            self.events.append("v2-deliver-before")

        def v2_deliver_after():
            self.events.append("v2-deliver-after")

        self.v1_cb = v1_deliver_before
        self.v2_before = v2_deliver_before
        self.v2_after = v2_deliver_after

        self.machine = Machine(
            model=None, states=V1_STATES, initial="idle",
            transitions=make_v1_transitions(self.v1_cb))

    def _publish_v2(self):
        return self.machine.define_revision(
            "v2", states=V2_STATES, initial="idle",
            transitions=make_v2_transitions(self.v2_before, self.v2_after))

    def test_new_batch_starts_on_latest_revision(self):
        batch = Batch()
        self.machine.add_model(batch)
        self.assertIsNone(self.machine.definition_of(batch))

        self._publish_v2()
        fresh = Batch()
        self.machine.add_model(fresh)
        self.assertEqual(self.machine.definition_of(fresh), "v2")
        self.assertEqual(fresh.state, "idle")
        self.assertTrue(hasattr(fresh, "is_accept"))
        self.assertTrue(hasattr(fresh, "is_source"))
        self.assertFalse(hasattr(fresh, "is_review"))
        # the new revision renamed the manual-decision state
        fresh.start()
        fresh.to_review()
        self.assertEqual(fresh.state, "accept")

    def test_publishing_does_not_touch_existing_models_or_base(self):
        batch = Batch()
        self.machine.add_model(batch)
        batch.start()
        self.assertEqual(batch.state, "read")

        self._publish_v2()
        # existing batch keeps the base definition and its runtime state
        self.assertIsNone(self.machine.definition_of(batch))
        self.assertEqual(batch.state, "read")
        # base machine still owns the original states
        self.assertIn("review", self.machine.states)
        self.assertIn("raw", self.machine.states)
        # old entry points still execute the base definition
        batch.to_review()
        self.assertEqual(batch.state, "review")

    def test_migration_requires_state_mapping_and_is_atomic(self):
        batch = Batch()
        self.machine.add_model(batch)
        batch.start()
        batch.to_review()
        self.assertEqual(batch.state, "review")
        self._publish_v2()

        # no mapping for the active 'review' path -> cannot migrate, with an explanation
        self.assertFalse(self.machine.can_migrate_definition(batch, "v2", {"read": "read"}))
        with self.assertRaises(MachineError) as ctx:
            self.machine.migrate_definition(batch, "v2", {"read": "read"})
        self.assertIn("review", str(ctx.exception))
        self.assertIn("mapping", str(ctx.exception))

        # nothing changed: data state and old definition/entry points are retained
        self.assertEqual(batch.state, "review")
        self.assertIsNone(self.machine.definition_of(batch))
        batch.process_raw()
        self.assertEqual(batch.state, "raw")

        # a mapping pointing at a non-existent target state also aborts cleanly
        with self.assertRaises(MachineError):
            self.machine.migrate_definition(batch, "v2", {"review": "ghost"})
        self.assertEqual(batch.state, "raw")

    def test_unknown_revision_and_unbound_model(self):
        batch = Batch()
        self.machine.add_model(batch)
        with self.assertRaises(MachineError):
            self.machine.migrate_definition(Batch(), "v2", {})
        self._publish_v2()
        with self.assertRaises(MachineError):
            self.machine.migrate_definition(batch, "nope", {})

    def test_duplicate_revision_name_rejected(self):
        self._publish_v2()
        with self.assertRaises(ValueError):
            self._publish_v2()

    def test_can_migrate_reflects_mapping_and_boundary(self):
        batch = Batch()
        self.machine.add_model(batch)
        batch.start()
        self.assertEqual(batch.state, "read")
        self._publish_v2()
        self.assertTrue(self.machine.can_migrate_definition(batch, "v2", {"read": "read"}))
        self.assertFalse(self.machine.can_migrate_definition(batch, "v2", {"review": "accept"}))

    def test_named_trigger_and_may_follow_swapped_definition(self):
        batch = Batch()
        self.machine.add_model(batch)
        batch.start()
        self._publish_v2()
        self.machine.migrate_definition(batch, "v2", {"read": "read"})

        # generic by-name entry point resolves against the active revision
        self.assertTrue(batch.may_trigger("to_review"))
        self.assertTrue(batch.may_to_review())
        batch.trigger("to_review")
        self.assertEqual(batch.state, "accept")
        # to_review is only defined from 'read' in the new revision
        self.assertFalse(batch.may_to_review())

    def test_removed_trigger_and_state_entry_points_are_dropped(self):
        machine = Machine(
            model=None, states=["A", "B", "legacy"], initial="A",
            transitions=[
                ["go", "A", "B"],
                ["legacy_step", "A", "legacy"],
            ])
        batch = type("M", (), {})()
        machine.add_model(batch)
        self.assertTrue(hasattr(batch, "legacy_step"))
        self.assertTrue(hasattr(batch, "is_legacy"))

        machine.define_revision(
            "r2", states=["A", "B"], initial="A",
            transitions=[["go", "A", "B"]])
        machine.migrate_definition(batch, "r2", {"A": "A"})

        self.assertTrue(hasattr(batch, "go"))
        self.assertFalse(hasattr(batch, "legacy_step"))
        self.assertFalse(hasattr(batch, "may_legacy_step"))
        self.assertFalse(hasattr(batch, "is_legacy"))
        with self.assertRaises(AttributeError):
            batch.trigger("legacy_step")

    def test_send_event_revision_routes_event_data(self):
        machine = Machine(
            model=None, states=["A", "B"], initial="A", send_event=True,
            transitions=[["go", "A", "B"]])
        batch = type("M", (), {})()
        machine.add_model(batch)
        machine.define_revision(
            "r2", states=["A", "B"], initial="A",
            transitions=[["go", "A", "B"]])
        machine.migrate_definition(batch, "r2", {"A": "A"})
        self.assertTrue(batch.may_go())
        batch.go()
        self.assertEqual(batch.state, "B")

    @skipIf(HierarchicalMachine is None, "hierarchical extension not available")
    def test_hierarchical_machine_revisions_are_rejected(self):
        machine = HierarchicalMachine(
            model=None, states=["A", "B"], initial="A",
            transitions=[["go", "A", "B"]])
        with self.assertRaises(MachineError):
            machine.define_revision("h2")

    def test_full_revision_swap_scenario(self):
        trace = []
        batch_a = Batch(gate=self.gate, block=True, trace=trace)
        batch_b = Batch(block=False)
        self.machine.add_model(batch_a)
        self.machine.add_model(batch_b)

        # entry point captured before the new version exists, intended for the next order
        captured_deliver = batch_a.deliver

        # B rests on the active path that the new declaration will not map
        batch_b.start()
        batch_b.to_review()
        self.assertEqual(batch_b.state, "review")

        # A enters an event and stalls inside the enter callback
        worker_error = {}

        def run_event():
            try:
                batch_a.start()
            except BaseException as exc:  # pragma: no cover - surfaced by the join assertion
                worker_error["error"] = exc

        worker = threading.Thread(target=run_event)
        worker.start()
        self._wait_until_busy(batch_a)
        self.assertEqual(batch_a.state, "read")

        # publish the new version while A is mid-event; A and B are not restarted or re-run
        self._publish_v2()
        self.assertIsNone(self.machine.definition_of(batch_a))
        self.assertIsNone(self.machine.definition_of(batch_b))
        self.assertEqual(trace, ["enter-read:start"])

        # swapping at a non-boundary is refused and leaves the model untouched
        with self.assertRaises(MachineError):
            self.machine.migrate_definition(batch_a, "v2", {"read": "read"})
        self.assertIsNone(self.machine.definition_of(batch_a))

        # B's active path has no mapping in the declaration, so B cannot be migrated
        with self.assertRaises(MachineError):
            self.machine.migrate_definition(batch_b, "v2", {"read": "read"})
        self.assertEqual(batch_b.state, "review")
        self.assertIsNone(self.machine.definition_of(batch_b))

        # release A: the in-flight event completes with v1 steps only
        marker = len(self.events)
        self.gate.set()
        worker.join(timeout=5)
        self.assertNotIn("error", worker_error)
        self.assertEqual(trace, ["enter-read:start", "enter-read:done"])
        self.assertEqual(self.events[marker:], [])

        # now at a clean boundary A can migrate; no enter/exit is simulated by the swap
        self.machine.migrate_definition(batch_a, "v2", {"read": "read"})
        self.assertEqual(self.machine.definition_of(batch_a), "v2")
        self.assertEqual(batch_a.state, "read")
        self.assertEqual(trace, ["enter-read:start", "enter-read:done"])

        # the previously captured deliver entry now follows the swapped (v2) definition
        captured_deliver()
        self.assertEqual(batch_a.state, "done")
        self.assertEqual(self.events[-2:], ["v2-deliver-before", "v2-deliver-after"])
        self.assertNotIn("v1-deliver-before", self.events)

        # a later declaration deliberately missing A's current target state fails atomically
        with self.assertRaises(MachineError):
            self.machine.migrate_definition(batch_a, "v2", {"read": "read"})
        self.assertEqual(batch_a.state, "done")
        self.assertEqual(self.machine.definition_of(batch_a), "v2")

        # a brand new batch C starts directly on the new revision's initial state
        batch_c = Batch(block=False)
        self.machine.add_model(batch_c)
        self.assertEqual(self.machine.definition_of(batch_c), "v2")
        self.assertEqual(batch_c.state, "idle")
        self.assertTrue(hasattr(batch_c, "is_accept"))

        # B retains its data state and old entry points on the base definition
        self.assertEqual(batch_b.state, "review")
        self.assertIsNone(self.machine.definition_of(batch_b))
        batch_b.process_raw()
        self.assertEqual(batch_b.state, "raw")

    def test_two_distinct_log_markers_for_enter_and_commit(self):
        batch = Batch()
        self.machine.add_model(batch)
        with self.assertLogs("transitions.core", level="DEBUG") as logs:
            self._publish_v2()
            batch.start()
        text = "\n".join(logs.output)
        self.assertIn("[revision] commit definition", text)
        self.assertIn("[revision] enter event", text)

    def _wait_until_busy(self, model, timeout=5.0):
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.machine._is_model_busy(model):
                return
            time.sleep(0.01)
        self.fail("model never became busy with an in-progress event")


@skipIf(asyncio is None, "AsyncMachine requires asyncio and contextvars support")
class TestAsyncDefinitionRevision(TestCase):

    def test_awaited_event_stays_busy_until_boundary(self):
        gate = asyncio.Event()

        class AsyncBatch(object):
            async def on_enter_read(self):
                await gate.wait()

        async def scenario():
            machine = AsyncMachine(
                model=None, states=["idle", "read", "review"], initial="idle",
                transitions=[
                    ["start", "idle", "read"],
                    ["to_review", "read", "review"],
                ])
            batch = AsyncBatch()
            machine.add_model(batch)

            task = asyncio.ensure_future(batch.start())
            await asyncio.sleep(0)
            self.assertTrue(machine._is_model_busy(batch))

            machine.define_revision(
                "v2", states=["idle", "read", "accept"], initial="idle",
                transitions=[
                    ["start", "idle", "read"],
                    ["to_review", "read", "accept"],
                ])
            with self.assertRaises(MachineError):
                machine.migrate_definition(batch, "v2", {"read": "read"})

            gate.set()
            await task
            self.assertFalse(machine._is_model_busy(batch))

            machine.migrate_definition(batch, "v2", {"read": "read"})
            await batch.to_review()
            self.assertEqual(batch.state, "accept")
            self.assertEqual(machine.definition_of(batch), "v2")

        asyncio.run(scenario())

