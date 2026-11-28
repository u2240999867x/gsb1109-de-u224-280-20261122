from typing import TYPE_CHECKING, Sequence, Union, List
from unittest import TestCase
from types import ModuleType
from unittest.mock import MagicMock

from transitions import Machine
from transitions.experimental.utils import generate_base_model
from transitions.experimental.utils import add_transitions, transition, event, with_model_definitions
from transitions.extensions import HierarchicalMachine
from transitions.extensions.markup import MarkupMachine

from .utils import Stuff

if TYPE_CHECKING:
    from transitions.core import MachineConfig, TransitionConfig
    from typing import Type


def import_code(code: str, name: str) -> ModuleType:
    module = ModuleType(name)
    exec(code, module.__dict__)
    return module


class TestExperimental(TestCase):

    def setUp(self) -> None:
        self.machine_cls = Machine  # type: Type[Machine]
        self.create_trigger_class()

    def create_trigger_class(self):
        @with_model_definitions
        class TriggerMachine(self.machine_cls):  # type: ignore
            pass

        self.trigger_machine = TriggerMachine

    def test_model_override(self):

        class Model:

            def trigger(self, name: str) -> bool:
                raise RuntimeError("Should be overridden")

            def is_A(self) -> bool:
                raise RuntimeError("Should be overridden")

            def is_C(self) -> bool:
                raise RuntimeError("Should be overridden")

        model = Model()
        machine = self.machine_cls(model, states=["A", "B"], initial="A", model_override=True)
        self.assertTrue(model.is_A())
        with self.assertRaises(AttributeError):
            model.to_B()  # Should not be assigned to model since its not declared
        self.assertTrue(model.trigger("to_B"))
        self.assertFalse(model.is_A())
        with self.assertRaises(RuntimeError):
            model.is_C()  # not overridden yet
        machine.add_state("C")
        self.assertFalse(model.is_C())  # now it is!
        self.assertTrue(model.trigger("to_C"))
        self.assertTrue(model.is_C())

    def test_generate_base_model(self):
        simple_config = {
            "states": ["A", "B"],
            "transitions": [
                ["go", "A", "B"],
                ["back", "*", "A"]
            ],
            "initial": "A",
            "model_override": True
        }  # type: MachineConfig

        mod = import_code(generate_base_model(simple_config), "base_module")
        model = mod.BaseModel()
        machine = self.machine_cls(model, **simple_config)
        self.assertTrue(model.is_A())
        self.assertTrue(model.go())
        self.assertTrue(model.is_B())
        self.assertTrue(model.back())
        self.assertTrue(model.state == "A")
        with self.assertRaises(AttributeError):
            model.is_C()

    def test_generate_base_model_from_machine(self):
        simple_config = {
            "states": ["A", "B"],
            "transitions": [
                ["go", "A", "B"],
                ["back", "*", "A"]
            ],
            "initial": "A",
            "model_override": True
        }  # type: MachineConfig

        # Cannot generate base model from a Machine instance
        with self.assertRaises(ValueError):
            generate_base_model(Machine(**simple_config))  # type: ignore

        mod = import_code(generate_base_model(MarkupMachine(**simple_config)), "base_module")
        model = mod.BaseModel()
        machine = self.machine_cls(model, **simple_config)
        self.assertTrue(model.is_A())
        self.assertTrue(model.go())
        self.assertTrue(model.is_B())
        self.assertTrue(model.back())
        self.assertTrue(model.state == "A")
        with self.assertRaises(AttributeError):
            model.is_C()

    def test_generate_base_model_callbacks(self):
        simple_config = {
            "states": ["A", "B"],
            "transitions": [
                ["go", "A", "B"],
            ],
            "initial": "A",
            "model_override": True,
            "before_state_change": "call_this"
        }  # type: MachineConfig

        mod = import_code(generate_base_model(simple_config), "base_module")
        mock = MagicMock()

        class Model(mod.BaseModel):  # type: ignore

            @staticmethod
            def call_this() -> None:
                mock()

        model = Model()
        machine = self.machine_cls(model, **simple_config)
        self.assertTrue(model.is_A())
        self.assertTrue(model.go())
        self.assertTrue(mock.called)

    def test_generate_model_no_auto(self):
        simple_config: MachineConfig = {
            "states": ["A", "B"],
            "auto_transitions": False,
            "model_override": True,
            "transitions": [
                ["go", "A", "B"],
                ["back", "*", "A"]
            ],
            "initial": "A"
        }
        mod = import_code(generate_base_model(simple_config), "base_module")
        model = mod.BaseModel()
        machine = self.machine_cls(model, **simple_config)
        self.assertTrue(model.is_A())
        self.assertTrue(model.go())
        with self.assertRaises(AttributeError):
            model.to_B()

    def test_decorator(self):

        class Model:

            state: str = ""

            def is_B(self) -> bool:
                return False

            @add_transitions(transition(source="A", dest="B"))
            @add_transitions([["A", "B"], "C"])
            def go(self) -> bool:
                raise RuntimeError("Should be overridden!")

        model = Model()
        machine = self.trigger_machine(model, states=["A", "B", "C"], initial="A")
        self.assertEqual("A", model.state)
        self.assertTrue(machine.is_state("A", model))
        self.assertTrue(model.go())
        with self.assertRaises(AttributeError):
            model.is_A()
        self.assertEqual("B", model.state)
        self.assertTrue(model.is_B())
        self.assertTrue(model.go())
        self.assertFalse(model.is_B())
        self.assertEqual("C", model.state)

    def test_decorator_complex(self):

        class Model:

            state: str = ""

            def check_param(self, param: bool) -> bool:
                return param

            @add_transitions(transition(source="A", dest="B"),
                             transition(source="B", dest="C", unless=Stuff.this_passes),
                             transition(source="B", dest="A", conditions=Stuff.this_passes, unless=Stuff.this_fails))
            def go(self) -> bool:
                raise RuntimeError("Should be overridden")

            @add_transitions({"source": "A", "dest": "B", "conditions": "check_param"})
            def event(self, param) -> bool:
                raise RuntimeError("Should be overridden")

        model = Model()
        machine = self.trigger_machine(model, states=["A", "B"], initial="A")
        self.assertTrue(model.go())
        self.assertTrue(model.state == "B")
        self.assertTrue(model.go())
        self.assertTrue(model.state == "A")
        self.assertFalse(model.event(param=False))
        self.assertTrue(model.state == "A")
        self.assertTrue(model.event(param=True))
        self.assertTrue(model.state == "B")

    def test_event_definition(self):

        class Model:

            state: str = ""

            def is_B(self) -> bool:
                return False

            go = event(transition(source="A", dest="B"), [["A", "B"], "C"], {"source": "*", "dest": None})

        model = Model()
        machine = self.trigger_machine(model, states=["A", "B", "C"], initial="A")
        self.assertEqual("A", model.state)
        self.assertTrue(machine.is_state("A", model))
        self.assertTrue(model.go())
        with self.assertRaises(AttributeError):
            model.is_A()
        self.assertEqual("B", model.state)
        self.assertTrue(model.is_B())
        self.assertTrue(model.go())
        self.assertFalse(model.is_B())
        self.assertEqual("C", model.state)

    def test_event_definition_complex(self):

        class Model:

            state: str = ""

            go = event(transition(source="A", dest="B"),
                       transition(source="B", dest="C", unless=Stuff.this_passes),
                       transition(source="B", dest="A", conditions=Stuff.this_passes, unless=Stuff.this_fails))

            event = event({"source": "A", "dest": "B", "conditions": "check_param"})

            def check_param(self, param: bool) -> bool:
                return param

        model = Model()
        machine = self.trigger_machine(model, states=["A", "B"], initial="A")
        self.assertTrue(model.go())
        self.assertTrue(model.state == "B")
        self.assertTrue(model.go())
        self.assertTrue(model.state == "A")
        self.assertFalse(model.event(param=False))
        self.assertTrue(model.state == "A")
        self.assertTrue(model.event(param=True))
        self.assertTrue(model.state == "B")

    def test_model_definitions_follow_attachments_and_share_generic_transitions(self):
        class BasePlugin:
            state = "pending"
            generic_log = []

            def __init__(self):
                self.declaration_log = []

            def trigger(self, *args, **kwargs):
                raise RuntimeError("placeholder")

            def may_trigger(self, *args, **kwargs):
                raise RuntimeError("placeholder")

            probe = event(transition(source="pending", dest="accepted", conditions="can_probe",
                                     prepare="prepare_probe"))
            accept = event(transition(source="pending", dest="accepted", conditions="can_accept",
                                      prepare="prepare_base_accept"))

            def can_probe(self, batch_id):
                return batch_id == "probe"

            def can_accept(self, batch_id):
                return batch_id == "parent"

            def prepare_probe(self, batch_id):
                self.declaration_log.append(("probe", batch_id))

            def prepare_base_accept(self, batch_id):
                self.declaration_log.append(("base-accept", batch_id))

            def on_enter_accepted(self, *args, **kwargs):
                pass

        class Plugin(BasePlugin):
            def __init__(self, name):
                super(Plugin, self).__init__()
                self.name = name
                self.preparation_log = []
                self.accepted = []
                self.callback_count = 0

            accept = event(
                transition(source="pending", dest="pending",
                           prepare="prepare_reject", conditions="reject"),
                transition(source="pending", dest="accepted", prepare="prepare_accept",
                           conditions="is_legal"))

            def prepare_reject(self, batch_id):
                self.preparation_log.append(("reject", batch_id))

            def reject(self, batch_id):
                return False

            def prepare_accept(self, batch_id):
                self.preparation_log.append(("accept", batch_id))

            def is_legal(self, batch_id):
                return batch_id.startswith("legal")

            def on_enter_accepted(self, *args, **kwargs):
                self.callback_count += 1
                self.accepted.append(self.name)
                if self.name == "B" and self.callback_count == 1:
                    old_a_accept("old")
                    machine.remove_model(plugin_a)
                    machine.remove_transition("accept", "pending", "accepted")
                    machine.add_model(plugin_a)

        class ThirdParty:
            state = "pending"

            def __init__(self):
                self.normal_accepts = []

            def trigger(self, *args, **kwargs):
                raise RuntimeError("placeholder")

            def may_trigger(self, *args, **kwargs):
                raise RuntimeError("placeholder")

            def accept(self, batch_id):
                self.normal_accepts.append(batch_id)
                return "normal"

            def on_enter_accepted(self, *args, **kwargs):
                pass

        machine = self.trigger_machine(
            model=None, states=["pending", "accepted"], initial="pending", queued=True,
            auto_transitions=False)
        plugin_a = Plugin("A")
        plugin_b = Plugin("B")
        parent = BasePlugin()
        third_party = ThirdParty()

        machine.add_model([plugin_a, plugin_b, parent, third_party])
        old_a_accept = plugin_a.accept
        old_a_trigger = plugin_a.trigger
        old_a_may_accept = plugin_a.may_accept
        old_a_may_trigger = plugin_a.may_trigger
        b_may_accept = plugin_b.may_accept

        self.assertTrue(plugin_a.may_accept("legal-1"))
        self.assertEqual(plugin_a.preparation_log, [("reject", "legal-1"), ("accept", "legal-1")])
        self.assertFalse(plugin_a.may_accept("backfill"))
        self.assertEqual(plugin_a.preparation_log.count(("reject", "backfill")), 1)
        self.assertTrue(plugin_a.may_trigger("probe", "probe"))
        self.assertFalse(plugin_a.may_trigger("probe", "not-probe"))

        machine.add_transition(
            "accept", "pending", "accepted",
            conditions=lambda batch_id: batch_id == "backfill",
            prepare=lambda batch_id: BasePlugin.generic_log.append(batch_id))

        self.assertTrue(plugin_a.may_accept("backfill"))
        self.assertTrue(plugin_a.trigger("accept", "illegal-queued"))
        self.assertEqual(plugin_a.state, "pending")
        self.assertTrue(plugin_a.trigger("accept", "backfill"))
        self.assertEqual(plugin_a.state, "accepted")
        self.assertEqual(third_party.accept("direct"), "normal")
        self.assertEqual(third_party.normal_accepts, ["direct"])
        self.assertTrue(third_party.trigger("accept", "backfill"))
        self.assertEqual(third_party.state, "accepted")
        machine.set_state("pending", third_party)
        self.assertEqual(
            BasePlugin.generic_log,
            ["backfill", "illegal-queued", "backfill", "backfill"])

        machine.set_state("pending", plugin_a)
        plugin_a.accepted.clear()
        self.assertTrue(parent.may_accept("parent"))
        self.assertTrue(parent.trigger("accept", "parent"))
        self.assertEqual(parent.state, "accepted")
        self.assertEqual(
            parent.declaration_log,
            [("base-accept", "parent"), ("base-accept", "parent")])

        self.assertTrue(plugin_b.trigger("accept", "legal-b"))
        self.assertFalse(old_a_accept("old"))
        self.assertFalse(old_a_trigger("accept", "old"))
        self.assertFalse(old_a_may_accept("old"))
        self.assertFalse(old_a_may_trigger("accept", "old"))
        self.assertNotIn(("reject", "old"), plugin_a.preparation_log)
        self.assertIsNot(plugin_a.accept, old_a_accept)
        self.assertIsNot(plugin_a.trigger, old_a_trigger)
        self.assertIsNot(plugin_a.may_accept, old_a_may_accept)
        self.assertIsNot(plugin_a.may_trigger, old_a_may_trigger)
        self.assertTrue(plugin_a.trigger("accept", "legal-new"))

        self.assertEqual(plugin_a.state, "accepted")
        self.assertEqual(plugin_b.state, "accepted")
        self.assertEqual(plugin_b.callback_count, 1)
        self.assertEqual(plugin_a.accepted, ["A"])
        self.assertEqual(plugin_b.accepted, ["B"])
        machine.set_state("pending", plugin_b)
        self.assertTrue(b_may_accept("legal-b-2"))
        self.assertEqual(third_party.accept("still-direct"), "normal")
        self.assertFalse(third_party.trigger("accept", "backfill"))
        self.assertEqual(third_party.state, "pending")


class TestHSMExperimental(TestExperimental):

    def setUp(self):
        self.machine_cls = HierarchicalMachine  # type: Type[HierarchicalMachine]
        self.create_trigger_class()
