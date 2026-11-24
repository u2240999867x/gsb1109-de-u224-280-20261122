from copy import deepcopy
from uuid import UUID

from SpiffWorkflow.util.task import TaskState


class CompensationManager:
    """Tracks completed compensable tasks and runs their compensation handlers.

    The manager stores only JSON-compatible state on the top workflow.  Handler
    tasks themselves are ordinary tasks in the affected (sub)workflow, so user
    tasks and process serialization continue to work.
    """

    def __init__(self, workflow, records=None, runs=None):
        self.workflow = workflow
        self.records = records or []
        self.runs = runs or {}

    def task_completed(self, task):
        if task._get_internal_data('compensation_handler_task'):
            self.handler_completed(task)
            return

        handler = getattr(task.task_spec, 'compensation_handler', None)
        if not handler:
            return

        scope_id = self.transaction_scope_id(task)
        record_id = str(task.id)
        if any(record['id'] == record_id for record in self.records):
            return

        self.records.append({
            'id': record_id,
            'task_id': record_id,
            'spec_name': task.task_spec.name,
            'handler': handler,
            'scope_id': scope_id,
            'data': deepcopy(task.data),
            'compensated': False,
            'handler_task_id': None,
        })

    def transaction_scope_id(self, task):
        from SpiffWorkflow.bpmn.specs.defaults import TransactionSubprocess

        top_workflow = task.workflow.top_workflow
        workflow = task.workflow
        while workflow is not top_workflow and getattr(workflow, 'parent_task_id', None):
            parent_task = top_workflow.get_task_from_id(workflow.parent_task_id)
            if isinstance(parent_task.task_spec, TransactionSubprocess):
                return str(parent_task.id)
            workflow = parent_task.workflow
        return None

    def prepare_cancellation(self, end_task):
        scope_id = str(end_task.workflow.parent_task_id)
        record_ids = [
            record['id']
            for record in reversed(self.records)
            if record.get('scope_id') == scope_id
            and record.get('handler')
            and not record.get('compensated')
        ]
        if not record_ids:
            return None

        run = self.runs.get(scope_id)
        if run is None or run.get('completed'):
            run = {
                'scope_id': scope_id,
                'record_ids': record_ids,
                'active_handler_task_id': None,
                'boundary_task_id': None,
                'completed': False,
            }
            self.runs[scope_id] = run
        return run

    def start_cancellation(self, scope_id):
        run = self.runs.get(scope_id)
        if run is None:
            return
        scope_task = self.workflow.get_task_from_id(UUID(scope_id))
        subprocess = self.workflow.get_subprocess(scope_task)
        if subprocess is not None:
            # Transaction.cancel() marks the completed cancellation path as
            # finished.  Compensation handlers are part of the cancellation
            # result and must remain engine-visible until they finish.
            subprocess.completed = False
        self._start_next(run)

    def boundary_allows(self, boundary_task):
        scope_id = self.scope_for_boundary_task(boundary_task)
        if scope_id is None:
            return True
        run = self.runs.get(scope_id)
        if run is None or run.get('completed'):
            return True
        run['boundary_task_id'] = str(boundary_task.id)
        return False

    def scope_for_boundary_task(self, boundary_task):
        from SpiffWorkflow.bpmn.specs.defaults import TransactionSubprocess

        parent = boundary_task.parent
        if parent is not None:
            for child in parent.children:
                if child is boundary_task:
                    continue
                if isinstance(child.task_spec, TransactionSubprocess):
                    return str(child.id)

        attached_to = getattr(boundary_task.task_spec, 'attached_to', None)
        if attached_to:
            for task in self.workflow.get_tasks():
                if task.task_spec.name == attached_to and isinstance(task.task_spec, TransactionSubprocess):
                    return str(task.id)
        return None

    def handler_completed(self, handler_task):
        scope_id = handler_task._get_internal_data('compensation_scope_id')
        record_id = handler_task._get_internal_data('compensation_record_id')
        if not scope_id or not record_id:
            return

        run = self.runs.get(scope_id)
        record = self._record(record_id)
        if record is not None:
            record['compensated'] = True
            record['handler_task_id'] = str(handler_task.id)
        if run is not None:
            run['active_handler_task_id'] = None
            self._start_next(run)

    def _start_next(self, run):
        for record_id in run['record_ids']:
            record = self._record(record_id)
            if record is None or record.get('compensated'):
                continue

            existing_task_id = record.get('handler_task_id') or run.get('active_handler_task_id')
            if existing_task_id:
                try:
                    existing_task = self.workflow.get_task_from_id(UUID(existing_task_id))
                except Exception:
                    existing_task = None
                if existing_task is not None and not existing_task.has_state(TaskState.FINISHED_MASK):
                    run['active_handler_task_id'] = str(existing_task.id)
                    record['handler_task_id'] = str(existing_task.id)
                    return

            handler_task = self._create_handler_task(run, record)
            run['active_handler_task_id'] = str(handler_task.id)
            record['handler_task_id'] = str(handler_task.id)
            return

        run['active_handler_task_id'] = None
        run['completed'] = True
        boundary_task_id = run.get('boundary_task_id')
        if boundary_task_id:
            boundary_task = self.workflow.get_task_from_id(UUID(boundary_task_id))
            boundary_task.task_spec._update(boundary_task)

    def _create_handler_task(self, run, record):
        original_task = self.workflow.get_task_from_id(UUID(record['task_id']))
        workflow = original_task.workflow
        handler_spec = workflow.spec.task_specs[record['handler']]
        handler_spec.is_for_compensation = True

        handler_task = workflow.task_tree._add_child(handler_spec, TaskState.FUTURE)
        handler_task.triggered = True
        workflow.completed = False
        handler_task.data = deepcopy(record.get('data') or {})
        handler_task._set_internal_data(
            compensation_handler_task=True,
            compensation_scope_id=run['scope_id'],
            compensation_record_id=record['id'],
        )
        handler_task.task_spec._update(handler_task)
        return handler_task

    def _record(self, record_id):
        for record in self.records:
            if record['id'] == record_id:
                return record
        return None
