from .test_graphviz import TestDiagrams, TestDiagramsNested
from .utils import Stuff, DummyModel
from .test_core import TestTransitions, TYPE_CHECKING

from transitions.extensions import (
    LockedGraphMachine, GraphMachine, HierarchicalGraphMachine, LockedHierarchicalGraphMachine
)
from transitions.extensions.states import add_state_features, Timeout, Tags
from unittest import skipIf
import tempfile
import os
import re
import sys
from unittest import TestCase

try:
    # Just to skip tests if graphviz not installed
    import graphviz as pgv  # @UnresolvedImport
except ImportError:  # pragma: no cover
    pgv = None

if TYPE_CHECKING:
    from typing import Type, List, Collection, Union, Literal


class TestMermaidDiagrams(TestDiagrams):

    graph_engine = "mermaid"
    edge_re = re.compile(r"^\s+(?P<src>\w+)\s*-->\s*(?P<dst>\w+)\s*:\s*(?P<attr>.*)$")
    node_re = re.compile(r"^\s+state \"\S+(\s+(?P<attr>\[.*\]?))?\" as (?P<node>\S+)")

    def test_diagram(self):
        m = self.machine_cls(states=self.states, transitions=self.transitions, initial='A', auto_transitions=False,
                             title='a test', graph_engine=self.graph_engine)
        graph = m.get_graph()
        self.assertIsNotNone(graph)

        _, nodes, edges = self.parse_dot(graph)

        # Test that graph properties match the Machine
        self.assertEqual(set(m.states.keys()), nodes)
        self.assertEqual(len(edges), len(self.transitions))

        # write diagram to temp file
        target = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
        graph.draw(target.name, format='png', prog='dot')
        self.assertTrue(os.path.getsize(target.name) > 0)
        # backwards compatibility check
        m.get_graph().draw(target.name, format='png', prog='dot')
        self.assertTrue(os.path.getsize(target.name) > 0)

        # cleanup temp file
        target.close()
        os.unlink(target.name)

    def test_to_method_filtering(self):
        m = self.machine_cls(states=['A', 'B', 'C'], initial='A', graph_engine=self.graph_engine)
        m.add_transition('to_state_A', 'B', 'A')
        m.add_transition('to_end', '*', 'C')
        _, _, edges = self.parse_dot(m.get_graph())
        self.assertEqual(len([e for e in edges if e == 'to_state_A']), 1)
        self.assertEqual(len([e for e in edges if e == 'to_end']), 3)
        m2 = self.machine_cls(states=['A', 'B', 'C'], initial='A', show_auto_transitions=True,
                              graph_engine=self.graph_engine)
        _, _, edges = self.parse_dot(m2.get_graph())
        self.assertEqual(len(edges), 9)
        self.assertEqual(len([e for e in edges if e == 'to_A']), 3)
        self.assertEqual(len([e for e in edges if e == 'to_C']), 3)

    def test_loops(self):
        m = self.machine_cls(states=['A'], initial='A', graph_engine=self.graph_engine)
        m.add_transition('reflexive', 'A', '=')
        m.add_transition('fixed', 'A', None)
        g1 = m.get_graph()
        dot_string, _, _ = self.parse_dot(g1)
        try:
            self.assertRegex(dot_string, r'A\s+-->\s+A:\s*(fixed|reflexive)')
        except AttributeError:  # Python 2 backwards compatibility

            self.assertRegexpMatches(dot_string, r'A\s+-->\s+A:\s*(fixed|reflexive)')

    def test_roi(self):
        m = self.machine_cls(states=['A', 'B', 'C', 'D', 'E', 'F'], initial='A', graph_engine=self.graph_engine)
        m.add_transition('to_state_A', 'B', 'A')
        m.add_transition('to_state_C', 'B', 'C')
        m.add_transition('to_state_F', 'B', 'F')
        g1 = m.get_graph(show_roi=True)
        dot, nodes, edges = self.parse_dot(g1)
        self.assertEqual(0, len(edges))
        self.assertIn(r'"A"', dot)
        # make sure that generating a graph without ROI has not influence on the later generated graph
        # this has to be checked since graph.custom_style is a class property and is persistent for multiple
        # calls of graph.generate()
        m.to_C()
        m.to_E()
        _ = m.get_graph()
        g2 = m.get_graph(show_roi=True)
        dot, _, _ = self.parse_dot(g2)
        self.assertNotIn(r'label="A\l"', dot)
        m.to_B()
        g3 = m.get_graph(show_roi=True)
        _, nodes, edges = self.parse_dot(g3)
        self.assertEqual(len(edges), 3)  # to_state_{A,C,F}
        self.assertEqual(len(nodes), 5)  # B + A,C,F (edges) + E (previous)

    def test_label_attribute(self):

        class LabelState(self.machine_cls.state_cls):  # type: ignore
            def __init__(self, *args, **kwargs):
                self.label = kwargs.pop('label')
                super(LabelState, self).__init__(*args, **kwargs)

        class CustomMachine(self.machine_cls):  # type: ignore
            state_cls = LabelState

        m = CustomMachine(states=[{'name': 'A', 'label': 'LabelA'},
                                  {'name': 'B', 'label': 'NotLabelA'}],
                          transitions=[{'trigger': 'event', 'source': 'A', 'dest': 'B', 'label': 'LabelEvent'}],
                          initial='A', graph_engine=self.graph_engine)
        dot, _, _ = self.parse_dot(m.get_graph())
        self.assertIn(r'"LabelA"', dot)
        self.assertIn(r'"NotLabelA"', dot)
        self.assertIn("LabelEvent", dot)
        self.assertNotIn(r'"A"', dot)
        self.assertNotIn("event", dot)

    def test_binary_stream(self):
        from io import BytesIO
        m = self.machine_cls(states=['A', 'B', 'C'], initial='A', auto_transitions=True,
                             title='A test', show_conditions=True, graph_engine=self.graph_engine)
        b1 = BytesIO()
        g = m.get_graph()
        g.draw(b1)
        b2 = g.draw(None)
        self.assertEqual(b2, b1.getvalue().decode())
        b1.close()

    def test_update_on_remove_transition(self):
        m = self.machine_cls(states=self.states, transitions=self.transitions, initial='A',
                             graph_engine=self.graph_engine, show_state_attributes=True)
        _, _, edges = self.parse_dot(m.get_graph())
        assert "walk" in edges
        m.remove_transition(trigger="walk", source="A", dest="B")
        _, _, edges = self.parse_dot(m.get_graph())
        assert not any("walk" == t["trigger"] for t in m.markup["transitions"])
        assert "walk" not in edges


class TestMermaidDiagramsNested(TestDiagramsNested, TestMermaidDiagrams):

    machine_cls = HierarchicalGraphMachine \
        # type: Type[Union[HierarchicalGraphMachine, LockedHierarchicalGraphMachine]]

    def test_diagram(self):
        m = self.machine_cls(states=self.states, transitions=self.transitions, initial='A', auto_transitions=False,
                             title='A test', show_conditions=True, graph_engine=self.graph_engine)
        graph = m.get_graph()
        self.assertIsNotNone(graph)

        _, nodes, edges = self.parse_dot(graph)

        self.assertEqual(len(edges), 8)
        # Test that graph properties match the Machine
        self.assertEqual(set(m.get_nested_state_names()), nodes)
        m.walk()
        m.run()

        # write diagram to temp file
        target = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
        m.get_graph().draw(target.name, prog='dot')
        self.assertTrue(os.path.getsize(target.name) > 0)
        # backwards compatibility check
        m.get_graph().draw(target.name, prog='dot')
        self.assertTrue(os.path.getsize(target.name) > 0)

        # cleanup temp file
        target.close()
        os.unlink(target.name)

    def test_roi(self):
        class Model:
            def is_fast(self, *args, **kwargs):
                return True
        model = Model()
        m = self.machine_cls(model, states=self.states, transitions=self.transitions, initial='A', title='A test',
                             graph_engine=self.graph_engine, show_conditions=True)
        model.walk()
        model.run()
        g1 = model.get_graph(show_roi=True)
        _, nodes, edges = self.parse_dot(g1)
        self.assertEqual(len(edges), 4)
        self.assertEqual(len(nodes), 4)
        model.sprint()
        g2 = model.get_graph(show_roi=True)
        dot, nodes, edges = self.parse_dot(g2)
        self.assertEqual(len(edges), 2)
        self.assertEqual(len(nodes), 3)

    def test_roi_parallel(self):
        class Model:
            @staticmethod
            def is_fast(*args, **kwargs):
                return True

        self.states[0] = {"name": "A", "parallel": ["1", "2"]}

        model = Model()
        m = self.machine_cls(model, states=self.states, transitions=self.transitions, initial='A', title='A test',
                             graph_engine=self.graph_engine, show_conditions=True)
        g1 = model.get_graph(show_roi=True)
        _, nodes, edges = self.parse_dot(g1)
        self.assertEqual(2, len(edges))  # reset and walk
        self.assertEqual(4, len(nodes))
        model.walk()
        model.run()
        model.sprint()
        g2 = model.get_graph(show_roi=True)
        _, nodes, edges = self.parse_dot(g2)
        self.assertEqual(len(edges), 2)
        self.assertEqual(len(nodes), 3)

    def test_roi_parallel_deeper(self):
        states = ['A', 'B', 'C', 'D',
                  {'name': 'P',
                   'parallel': [
                       '1',
                       {'name': '2', 'parallel': [
                           {'name': 'a'},
                           {'name': 'b', 'parallel': [
                               {'name': 'x', 'parallel': ['1', '2']}, 'y'
                           ]}
                       ]},
                   ]}]
        transitions = [["go", "A", "P"], ["reset", "*", "A"]]
        m = self.machine_cls(states=states, transitions=transitions, initial='A', title='A test',
                             graph_engine=self.graph_engine, show_conditions=True)
        m.go()
        _, nodes, edges = self.parse_dot(m.get_graph(show_roi=True))
        self.assertEqual(len(edges), 2)
        self.assertEqual(len(nodes), 10)


class TestMermaidHistoryGraph(TestCase):
    """Tests for runtime history graphs rendered from recorded transition attempts."""

    graph_engine = "mermaid"

    def _build_machine(self):
        class BackfillModel:
            def __init__(self):
                self.old_checks = 0
                self.new_checks = 0
                self.exit_errors = 0
                self.acknowledgements = 0

            def old_check(self, event_data):
                self.old_checks += 1
                return False

            def new_check(self, event_data):
                self.new_checks += 1
                return True

            def boom_on_exit(self, event_data):
                self.exit_errors += 1
                raise RuntimeError("exit callback failed")

            def write_acknowledgement(self, event_data):
                self.acknowledgements += 1

        def swallow(event_data):
            self.assertIsInstance(event_data.error, RuntimeError)

        states = [
            "Init",
            {"name": "P", "parallel": [
                {"name": "1", "children": [{"name": "a"}, {"name": "b"}], "initial": "a"},
                {"name": "2", "children": [
                    {"name": "a", "on_exit": "boom_on_exit"}, {"name": "b"}], "initial": "a"},
            ]},
            "Done",
        ]
        transitions = [
            ["enter", "Init", "P"],
            {"trigger": "go", "source": "P_1_a", "dest": "P_1_b", "conditions": "old_check"},
            {"trigger": "go", "source": "P_1_a", "dest": "P_1_b", "conditions": "new_check"},
            ["go", "P_2_a", "P_2_b"],
            ["succeed", "Init", "Done"],
            {"trigger": "ack", "source": "P_1_b", "dest": None, "after": "write_acknowledgement"},
        ]
        model_a = BackfillModel()
        model_b = BackfillModel()
        machine = HierarchicalGraphMachine(
            model=[model_a, model_b], states=states, transitions=transitions, initial="Init",
            auto_transitions=False, send_event=True, on_exception=swallow,
            graph_engine=self.graph_engine)
        return machine, model_a, model_b

    def test_history_graph_records_attempts_without_re_evaluating(self):
        machine, model_a, model_b = self._build_machine()

        model_a.enter()
        # same trigger: one rejected candidate, one passing candidate in region 1 and an
        # exit callback error in region 2
        self.assertTrue(model_a.go("run-42"))
        self.assertEqual(model_a.state, ["P_1_b", "P_2_a"])
        # B completes an event of its own, never touching the parallel state
        self.assertTrue(model_b.succeed())
        self.assertEqual(model_b.state, "Done")
        # internal transition keeps the state but writes an acknowledgement record
        self.assertTrue(model_a.ack("run-42"))
        self.assertEqual(model_a.state, ["P_1_b", "P_2_a"])
        self.assertEqual(model_a.acknowledgements, 1)

        checks = (model_a.old_checks, model_a.new_checks, model_a.exit_errors,
                  model_a.acknowledgements)

        source = model_a.get_history_graph(
            trigger="go", show_roi=True, title="A run-42 replay").source

        # the custom title is used and the ROI excludes states unrelated to the go attempts
        self.assertIn("A run-42 replay", source)
        self.assertNotIn("Init", source)
        self.assertNotIn("Done", source)

        # rejected candidate and the actually executed edge share one source/destination pair
        self.assertIn("P_1_a --> P_1_b: go [rejected] | go [success]", source)
        # error edge keeps the region in its original state
        self.assertIn("P_2_a --> P_2_b: go [error]: RuntimeError", source)
        self.assertIn("Class P_1_a s_rejected", source)
        self.assertIn("Class P_1_b s_success", source)
        self.assertIn("Class P_2_a s_error", source)
        # the filter scope only contains A's go attempts
        self.assertNotIn("ack [", source)
        self.assertNotIn("succeed [", source)
        self.assertIn("linkStyle", source)

        # rendering history again must not re-run guards or state callbacks
        again = model_a.get_history_graph(trigger="go", show_roi=True, title="A run-42 replay").source
        self.assertEqual(source, again)
        self.assertEqual(checks, (model_a.old_checks, model_a.new_checks, model_a.exit_errors,
                                  model_a.acknowledgements))

    def test_internal_transition_does_not_turn_old_edge_into_success(self):
        machine, model_a, model_b = self._build_machine()

        model_a.enter()
        model_a.go("run-42")
        model_b.succeed()
        model_a.ack("run-42")

        source = model_a.get_history_graph(title="A full history").source
        # the internal transition is rendered as a self loop but does not alter earlier outcomes
        self.assertIn("P_1_b --> P_1_b: ack [success] [internal]", source)
        self.assertIn("go [rejected]", source)
        self.assertIn("go [error]", source)
        self.assertEqual(model_a.state, ["P_1_b", "P_2_a"])
        self.assertEqual(model_a.acknowledgements, 1)

    def test_history_is_isolated_per_model(self):
        machine, model_a, model_b = self._build_machine()

        model_a.enter()
        model_a.go("run-42")
        model_b.succeed()
        model_a.ack("run-42")

        graph_a = model_a.get_history_graph().source
        graph_b = model_b.get_history_graph().source

        self.assertIn("go [rejected]", graph_a)
        self.assertIn("go [success]", graph_a)
        self.assertIn("go [error]", graph_a)
        self.assertIn("ack [success]", graph_a)
        self.assertNotIn("succeed", graph_a)

        self.assertIn("Init --> Done: succeed [success]", graph_b)
        self.assertNotIn("go [", graph_b)
        self.assertNotIn("ack [", graph_b)
        self.assertNotIn("[rejected]", graph_b)
        self.assertNotIn("[error]", graph_b)

    def test_static_graph_still_available(self):
        machine, model_a, model_b = self._build_machine()

        model_a.enter()
        model_a.go("run-42")

        static = model_a.get_graph(show_roi=True).source
        # the structural entry keeps rendering configuration edges without runtime annotations
        self.assertIn("P_2_a --> P_2_b: go", static)
        self.assertNotIn("[rejected]", static)
        self.assertNotIn("[error]", static)
        self.assertNotIn("linkStyle", static)

    def test_flat_machine_history_graph(self):
        def reject(event_data):
            return False

        def pass_check(event_data):
            return True

        from transitions.extensions import GraphMachine

        m = GraphMachine(
            states=["A", "B"],
            transitions=[
                {"trigger": "go", "source": "A", "dest": "B", "conditions": reject},
                {"trigger": "go", "source": "A", "dest": "B", "conditions": pass_check},
                {"trigger": "note", "source": "B", "dest": None},
            ],
            initial="A", auto_transitions=False, send_event=True,
            graph_engine=self.graph_engine)
        self.assertTrue(m.go())
        self.assertTrue(m.note())
        source = m.get_history_graph().source
        self.assertIn("A --> B: go [rejected] | go [success]", source)
        self.assertIn("B --> B: note [success] [internal]", source)
        self.assertIn("Class B s_success", source)


class TestDiagramsLockedNested(TestDiagramsNested):

    def setUp(self):
        super(TestDiagramsLockedNested, self).setUp()
        self.machine_cls = LockedHierarchicalGraphMachine  # type: Type[LockedHierarchicalGraphMachine]

    @skipIf(sys.version_info < (3, ), "Python 2.7 cannot retrieve __name__ from partials")
    def test_function_callbacks_annotation(self):
        super(TestDiagramsLockedNested, self).test_function_callbacks_annotation()
