from uuid import UUID

from SpiffWorkflow.exceptions import TaskNotFoundException
from SpiffWorkflow.task import Task, TaskState

class BpmnEvent:
    def __init__(self, event_definition, payload=None, correlations=None, target=None, source=None, **kwargs):
        self.event_definition = event_definition
        self.payload = payload
        self.correlations = correlations or {}
        self.target = target
        self.source = source
        for name, value in kwargs.items():
            setattr(self, name, value)


class PendingBpmnEvent:
    def __init__(self, name, event_type, value=None, correlations=None):
        self.name = name
        self.event_type = event_type
        self.value = value
        self.correlations = correlations or {}


class EventManager:

    def __init__(self, workflow):
        self.workflow = workflow
        self.tasks = {}

    def add_task(self, my_task):
        self.tasks[my_task.id] = my_task

    def remove_task(self, my_task):
        self.tasks.pop(my_task.id, None)

    def get_waiting_tasks(self):
        return [t.task_spec.event_definition.details(t) for t in self.tasks.values()]

    def catch(self, event, *args, internal=True, source=None, **kwargs):
        # Accept both catch(event, internal, source) and the task-first
        # catch(event, source, internal) calling convention.
        for arg in args:
            if isinstance(arg, Task):
                source = arg
            elif isinstance(arg, (UUID, str)):
                source = arg
            elif isinstance(arg, (bool, int)):
                internal = bool(arg)

        if source is None:
            source = kwargs.get('task') or kwargs.get('source_task')

        source = self._resolve_source(event, source)
        if source is not None and self._is_escalation(event):
            tasks = self._catch_escalation(event, source)
            if len(tasks) == 0 and internal:
                self.workflow.bpmn_events.append(event)
            for task in tasks:
                self._deliver_event(task, event)
            return len(tasks)

        if event.target is not None:
            # This limits results to tasks in the specified workflow
            tasks = [t for t in self.tasks.values() if t.workflow == event.target
                     and t.task_spec.event_definition.catches(t, event)]
            if len(tasks) == 0:
                event.target = event.target.parent_workflow
                self.catch(event)
        else:
            self.update_collaboration(event)
            tasks = [t for t in self.tasks.values() if t.task_spec.catches(t, event)]
            # Figure out if we need to create an external event
            if len(tasks) == 0 and internal:
                self.workflow.bpmn_events.append(event)

        for task in tasks:
            self._deliver_event(task, event)

        return len(tasks)

    def _deliver_event(self, task, event):
        task.task_spec.catch(task, event)
        task.task_spec._update(task)

    def _is_escalation(self, event):
        from SpiffWorkflow.bpmn.specs.event_definitions import EscalationEventDefinition
        return isinstance(event.event_definition, EscalationEventDefinition)

    def _resolve_source(self, event, source=None):
        candidates = [source]
        for name in (
            'source', 'source_task', 'source_id', 'task', 'task_id',
            'origin', 'origin_task', 'origin_id', 'origin_task_id', 'target',
        ):
            candidates.append(getattr(event, name, None))
        candidates.extend(value for value in vars(event).values() if value not in candidates)

        for candidate in candidates:
            if isinstance(candidate, Task):
                return candidate
            if isinstance(candidate, UUID):
                return self.workflow.get_task_from_id(candidate)
            if isinstance(candidate, str):
                try:
                    return self.workflow.get_task_from_id(UUID(candidate))
                except (ValueError, AttributeError):
                    continue
        return None

    def _catch_escalation(self, event, source):
        try:
            restored_source = self.workflow.get_task_from_id(source.id)
            if restored_source is not None:
                source = restored_source
        except TaskNotFoundException:
            pass

        workflow = source.workflow
        context = source

        while workflow is not None and context is not None:
            candidates = [
                task for task in self.tasks.values()
                if task.workflow == workflow and task.task_spec.catches(task, event)
            ]
            selected = self._select_boundary_for_context(candidates, context, event)
            if selected is not None:
                return [selected]

            parent_task_id = getattr(workflow, 'parent_task_id', None)
            if parent_task_id is None:
                break
            context = self.workflow.get_task_from_id(parent_task_id)
            workflow = context.workflow if context is not None else None

        return []

    def _select_boundary_for_context(self, candidates, context, event):
        from SpiffWorkflow.bpmn.specs.mixins.events.intermediate_event import BoundaryEvent

        boundaries = {}
        for task in candidates:
            if not isinstance(task.task_spec, BoundaryEvent):
                continue
            host = self._get_boundary_host(task)
            if host is not None:
                boundaries.setdefault(host.id, []).append(task)

        current = context
        while current is not None:
            matches = boundaries.get(current.id)
            if matches:
                exact = [task for task in matches if self._has_exact_code(task.task_spec.event_definition)]
                return exact[0] if exact else matches[0]
            current = current.parent
        return None

    def _get_boundary_host(self, boundary_task):
        from SpiffWorkflow.bpmn.specs.mixins.events.intermediate_event import BoundaryEvent

        parent = boundary_task.parent
        if parent is None:
            return None
        for child in parent.children:
            if child is not None and not isinstance(child.task_spec, BoundaryEvent):
                return child
        return None

    def _has_exact_code(self, event_definition):
        code = getattr(event_definition, 'code', None)
        return code is not None and code != ''

    def update_collaboration(self, event):

        def get_or_create_subprocess(task_spec, wf_spec):
            for sp in self.workflow.subprocesses.values():
                if sp.get_next_task(state=TaskState.WAITING, spec_name=task_spec) is not None:
                    return sp
            child_id = self.workflow.spec.start.trigger_wf(self.workflow.task_tree, wf_spec)
            return self.workflow.subprocesses[child_id]

        # Start a subprocess for known specs with start events that catch this
        for name in self.workflow.spec.start.trigger_specs:
            sp_spec = self.workflow.subprocess_specs.get(name)
            for ts in sp_spec.bpmn_start_events:
                if ts.event_definition == event.event_definition:
                    subprocess = get_or_create_subprocess(ts.name, sp_spec.name)
                    subprocess.correlations.update(event.correlations)
