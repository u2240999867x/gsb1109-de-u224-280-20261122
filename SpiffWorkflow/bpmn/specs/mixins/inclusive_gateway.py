# Copyright (C) 2012 Matthew Hampton, 2023 Sartography
#
# This file is part of SpiffWorkflow.
#
# SpiffWorkflow is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3.0 of the License, or (at your option) any later version.
#
# SpiffWorkflow is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301  USA

from uuid import UUID

from SpiffWorkflow.bpmn.exceptions import WorkflowTaskException
from SpiffWorkflow.bpmn.specs.bpmn_task_spec import BpmnTaskSpec
from SpiffWorkflow.util.deep_merge import DeepMerge
from SpiffWorkflow.util.task import TaskState
from SpiffWorkflow.specs.MultiChoice import MultiChoice
from .unstructured_join import UnstructuredJoin



class InclusiveGateway(MultiChoice, UnstructuredJoin):
    """
    Task Spec for a bpmn:parallelGateway node. From the specification of BPMN
    (http://www.omg.org/spec/BPMN/2.0/PDF - document number:formal/2011-01-03):

    The Inclusive Gateway is activated if
     * At least one incoming Sequence Flow has at least one token and
     * For every directed path formed by sequence flow that
        * starts with a Sequence Flow f of the diagram that has a token,
        * ends with an incoming Sequence Flow of the inclusive gateway that has
          no token, and
        * does not visit the Inclusive Gateway.
     * There is also a directed path formed by Sequence Flow that
        * starts with f,
        * ends with an incoming Sequence Flow of the inclusive gateway that has
          a token, and
        * does not visit the Inclusive Gateway.

    Upon execution, a token is consumed from each incoming Sequence Flow that
    has a token. A token will be produced on some of the outgoing Sequence
    Flows.

    TODO: Not implemented: At the moment, we can't handle having more than one
    token at a single incoming sequence
    TODO: At the moment only converging Inclusive Gateways are supported.

    In order to determine the outgoing Sequence Flows that receive a token, all
    conditions on the outgoing Sequence Flows are evaluated. The evaluation
    does not have to respect a certain order.

    For every condition which evaluates to true, a token MUST be passed on the
    respective Sequence Flow.

    If and only if none of the conditions evaluates to true, the token is
    passed on the default Sequence Flow.

    In case all conditions evaluate to false and a default flow has not been
    specified, the Inclusive Gateway throws an exception.
    """

    def test(self):
        MultiChoice.test(self)
        UnstructuredJoin.test(self)

    def _is_join(self):
        """Only gateways with multiple inputs can merge branches."""
        return len(self.inputs) > 1

    def _is_split(self):
        """Gateways that can select more than one output start a new branch set."""
        return len(self.cond_task_specs) > 1

    def _get_split_task(self, my_task, token):
        try:
            return my_task.workflow.tasks.get(UUID(token))
        except (TypeError, ValueError):
            return None

    def _get_expected_branches(self, split_task):
        """Returns the branches this split instance selected that can reach this gateway."""
        selected = split_task.internal_data.get('inclusive_split_outputs')
        if selected is None:
            return None

        expected = []
        for name in selected:
            spec = split_task.workflow.spec.task_specs.get(name)
            if spec is not None and spec not in expected and self._can_reach(spec):
                expected.append(spec)
        return expected

    def _can_reach(self, spec):
        """Checks whether a task spec has a path to this gateway in the process graph."""
        if spec == self:
            return True
        seen, pending = set(), [spec]
        while pending:
            current = pending.pop(0)
            if current == self:
                return True
            if current.name in seen:
                continue
            seen.add(current.name)
            pending.extend(current.outputs)
        return False

    def _get_correlated_split(self, my_task):
        """Finds the innermost open split with a branch that can reach this gateway."""
        for token in reversed(my_task.get_open_branch_tokens()):
            split_task = self._get_split_task(my_task, token)
            if split_task is not None and self._get_expected_branches(split_task):
                return split_task
        return None

    def _matching_tasks(self, my_task, token=None, state=TaskState.ANY_MASK):
        """Find tasks of this spec that belong to the same split instance."""
        if token is None:
            tokens = my_task.get_open_branch_tokens()
            return [
                task for task in my_task.workflow.tasks.values()
                if task.task_spec == self
                and task.has_state(state)
                and task.get_open_branch_tokens() == tokens
            ]
        return [
            task for task in my_task.workflow.tasks.values()
            if task.task_spec == self
            and task.has_state(state)
            and token in task.get_open_branch_tokens()
        ]

    def _check_threshold_token(self, my_task, split_task, expected):
        """Wait only for branches selected by the split that produced this token."""
        token = str(split_task.id)
        join_tasks = self._matching_tasks(my_task, token, TaskState.NOT_FINISHED_MASK)
        for branch in expected:
            branch_task = next((c for c in split_task.children if c.task_spec == branch), None)
            if branch_task is None:
                return False
            arrived = any(
                task.parent is not None
                and task.parent.has_state(TaskState.FINISHED_MASK)
                and task.is_descendant_of(branch_task)
                for task in join_tasks
            )
            if not arrived:
                return False
        return True

    def _check_threshold_legacy(self, my_task):
        # Look at the tree to find all places where this task is used and unfinished tasks that may be ancestors
        # If there are any, we may have to check whether this gateway is reachable from any of them.
        tasks, sources = [], []
        for task in my_task.workflow.get_tasks(end_at_spec=self.name):
            if task.task_spec == self:
                tasks.append(task)
            elif task.has_state(TaskState.READY|TaskState.WAITING):
                sources.append(task.task_spec)

        # Look up which tasks have parents completed.
        completed_inputs = {task.parent.task_spec for task in tasks if task.parent.state == TaskState.COMPLETED}

        # If any parents of this join have not been finished, this task must wait.
        # A parent spec only has to be completed once, even it is on multiple paths
        tasks_waiting = False
        for task in tasks:
            if task.parent.has_state(TaskState.DEFINITE_MASK) and task.parent.task_spec not in completed_inputs:
                tasks_waiting = True
                break

        if tasks_waiting:
            complete = False
        else:
            # Handle the case where there are paths from active tasks that must go through waiting inputs
            waiting_inputs = [i for i in self.inputs if i not in completed_inputs]

            checked = []
            # This will go back through a task spec's ancestors and return the source, if applicable
            def check(spec):
                checked.append(spec)
                for parent in spec.inputs:
                    if parent not in checked:
                        return parent if parent in sources else check(parent)

            # Start with the completed inputs and recurse back through its ancestors, removing any waiting tasks that
            # could reach one of them.
            for spec in completed_inputs:
                source = check(spec)
                if source is not None:
                    sources.remove(source)

            # Now check the rest of the waiting inputs and see if they can be reached from any of the remaining tasks
            unfinished_paths = []
            for spec in waiting_inputs:
                if check(spec) is not None:
                    unfinished_paths.append(spec)
                    break

            complete = len(unfinished_paths) == 0

        return complete

    def _check_threshold_unstructured(self, my_task):
        if not self._is_join():
            return True

        if not my_task.get_open_branch_tokens():
            return self._check_threshold_legacy(my_task)

        split_task = self._get_correlated_split(my_task)
        expected = self._get_expected_branches(split_task) if split_task is not None else None
        if expected is None:
            # The token did not come from an inclusive split we know about; use the
            # legacy behavior for workflows that only use inclusive gateways as joins.
            return self._check_threshold_legacy(my_task)

        return self._check_threshold_token(my_task, split_task, expected)

    def _update_hook(self, my_task):
        if not self._is_join():
            # A diverging gateway never waits for other branches; the generic join
            # bookkeeping would look at every instance of this spec, including
            # unrelated concurrent tokens.
            return BpmnTaskSpec._update_hook(self, my_task)

        if not my_task.get_open_branch_tokens():
            return super()._update_hook(my_task)

        split_task = self._get_correlated_split(my_task)
        may_fire = self._check_threshold_unstructured(my_task)
        if split_task is not None:
            for task in self._matching_tasks(my_task, str(split_task.id), TaskState.WAITING):
                if task != my_task:
                    task.cancel()
        if not may_fire:
            my_task._set_state(TaskState.WAITING)
        else:
            my_task._inherit_data()
        return may_fire

    def _merge_token_branches(self, my_task):
        """Merge data from the branches of this token only."""
        split_task = self._get_correlated_split(my_task)
        token = str(split_task.id) if split_task is not None else None
        for task in sorted(self._matching_tasks(my_task, token), key=lambda t: t.last_state_change):
            if task.parent is None or task == my_task:
                continue
            if my_task.is_descendant_of(task) or task.is_descendant_of(my_task):
                continue
            if not task.parent.has_state(TaskState.COMPLETED):
                continue
            # By inheriting directly from parent tasks, we avoid copying previously merged data.
            DeepMerge.merge(my_task.data, task.parent.data)
            if task.has_state(TaskState.COMPLETED):
                task._drop_children(force=True)
        # My task is not finished, so won't be included above.
        my_task._inherit_data()

    def _set_child_tokens(self, my_task, selected):
        """Propagate or start token lineages on the selected outgoing branches."""
        tokens = my_task.get_branch_tokens()
        open_tokens = my_task.get_open_branch_tokens()
        if self._is_join():
            split_task = self._get_correlated_split(my_task)
            if split_task is not None:
                open_tokens = [token for token in open_tokens if token != str(split_task.id)]
        if self._is_split():
            tokens = tokens + [str(my_task.id)]
            open_tokens = open_tokens + [str(my_task.id)]
            my_task.internal_data['inclusive_split_outputs'] = [spec.name for spec in selected]

        names = {spec.name for spec in selected}
        for child in my_task.children:
            if child.task_spec.name in names:
                child._set_branch_tokens(tokens, open_tokens)

    def _run_hook(self, my_task):
        if self._is_join():
            if self._get_correlated_split(my_task) is not None:
                self._merge_token_branches(my_task)
            else:
                UnstructuredJoin._run_hook(self, my_task)

        matches, defaults = self._get_matching_outputs(my_task)
        if len(matches + defaults) == 0:
            raise WorkflowTaskException('No conditions satisfied on gateway', task=my_task)
        selected = matches or defaults
        my_task._sync_children(selected, TaskState.FUTURE)
        self._set_child_tokens(my_task, selected)
        for child in my_task.children:
            child.task_spec._predict(child, mask=TaskState.FUTURE|TaskState.PREDICTED_MASK)
        return True
