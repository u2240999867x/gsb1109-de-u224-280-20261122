from SpiffWorkflow import TaskState
from SpiffWorkflow.bpmn import BpmnWorkflow

from ..BpmnWorkflowTestCase import BpmnWorkflowTestCase


class CompensationArchiveTest(BpmnWorkflowTestCase):

    def setUp(self):
        self.spec, self.subprocesses = self.load_workflow_spec(
            'compensation_archive.bpmn', 'ArchiveProcess')
        self.workflow = BpmnWorkflow(self.spec, self.subprocesses)
        self.workflow.do_engine_steps()

    def _registration_work_by_call(self):
        call_a = self.workflow.get_tasks(spec_name='RegistrationA')[0]
        call_b = self.workflow.get_tasks(spec_name='RegistrationB')[0]
        work = self.workflow.get_tasks(spec_name='RegistrationWork', state=TaskState.READY)
        by_parent = {str(task.workflow.parent_task_id): task for task in work}
        return call_a, call_b, by_parent[str(call_a.id)], by_parent[str(call_b.id)]

    def _complete_registrations_b_then_a(self):
        call_a, call_b, work_a, work_b = self._registration_work_by_call()

        work_b.set_data(registration_id='B', registration_credential='credential-B')
        work_b.run()
        self.workflow.do_engine_steps()

        work_a.set_data(registration_id='A', registration_credential='credential-A')
        work_a.run()
        self.workflow.do_engine_steps()

        return call_a, call_b

    def test_transaction_cancel_completes_compensation_in_reverse_completion_order(self):
        self._complete_registrations_b_then_a()

        activity_c = self.workflow.get_tasks(spec_name='ActivityC', state=TaskState.READY)
        self.assertEqual(len(activity_c), 1)

        choice = self.workflow.get_tasks(spec_name='ChooseOutcome', state=TaskState.READY)[0]
        choice.set_data(action='cancel')
        choice.run()
        self.workflow.do_engine_steps()

        handlers = self.workflow.get_tasks(spec_name='RegistrationCompensation', state=TaskState.READY)
        self.assertEqual(len(handlers), 1)
        self.assertEqual(handlers[0].get_data('registration_credential'), 'credential-A')
        handlers[0].run()
        self.workflow.do_engine_steps()

        waiting_handlers = self.workflow.get_tasks(spec_name='RegistrationCompensation', state=TaskState.READY)
        self.assertEqual(len(waiting_handlers), 1)
        self.assertEqual(waiting_handlers[0].get_data('registration_credential'), 'credential-B')
        completed_handlers = self.workflow.get_tasks(
            spec_name='RegistrationCompensation', state=TaskState.COMPLETED)
        self.assertEqual(len(completed_handlers), 1)
        self.assertEqual(completed_handlers[0].get_data('registration_credential'), 'credential-A')

        # The second compensation is a user task.  Save and restore while it is waiting.
        self.save_restore()

        self.assertEqual(
            len(self.workflow.get_tasks(spec_name='RegistrationCompensation')), 2)
        self.assertEqual(
            len(self.workflow.get_tasks(spec_name='RegistrationCompensation', state=TaskState.COMPLETED)), 1)
        restored = self.workflow.get_tasks(spec_name='RegistrationCompensation', state=TaskState.READY)
        self.assertEqual(len(restored), 1)
        self.assertEqual(restored[0].get_data('registration_credential'), 'credential-B')

        # Re-running engine steps must not create another A compensation.
        self.workflow.do_engine_steps()
        self.assertEqual(
            len(self.workflow.get_tasks(spec_name='RegistrationCompensation')), 2)

        restored[0].run()
        self.workflow.do_engine_steps()

        self.assertEqual(
            len(self.workflow.get_tasks(spec_name='RegistrationCompensation', state=TaskState.COMPLETED)), 2)
        cancel_branch = self.workflow.get_next_task(spec_name='CancelBranchTask')
        self.assertEqual(cancel_branch.state, TaskState.COMPLETED)
        cancel_boundary = self.workflow.get_next_task(spec_name='ImportCancelBoundary')
        self.assertEqual(cancel_boundary.state, TaskState.COMPLETED)
        activity_c = self.workflow.get_next_task(spec_name='ActivityC')
        self.assertEqual(activity_c.state, TaskState.CANCELLED)

        # The already-completed external transaction is outside this transaction scope.
        self.assertEqual(self.workflow.get_tasks(spec_name='OtherCompensate'), [])
        other_transaction = self.workflow.get_next_task(spec_name='OtherBatchTransaction')
        self.assertEqual(other_transaction.state, TaskState.COMPLETED)

        registration_credentials = [
            record['data'].get('registration_credential')
            for record in self.workflow.compensation.records
            if record['spec_name'] == 'RegistrationWork'
        ]
        other_credentials = [
            record['data'].get('other_credential')
            for record in self.workflow.compensation.records
            if record['spec_name'] == 'OtherRegister'
        ]
        self.assertEqual(other_credentials, ['other-batch-credential'])
        self.assertEqual(registration_credentials, ['credential-B', 'credential-A'])

    def test_transaction_success_does_not_compensate(self):
        self._complete_registrations_b_then_a()

        activity_c = self.workflow.get_tasks(spec_name='ActivityC', state=TaskState.READY)[0]
        activity_c.run()
        self.workflow.do_engine_steps()

        choice = self.workflow.get_tasks(spec_name='ChooseOutcome', state=TaskState.READY)[0]
        choice.set_data(action='success')
        choice.run()
        self.workflow.do_engine_steps()

        transaction = self.workflow.get_next_task(spec_name='ImportTransaction')
        self.assertEqual(transaction.state, TaskState.COMPLETED)
        normal_end = self.workflow.get_next_task(spec_name='ArchiveNormalEnd')
        self.assertEqual(normal_end.state, TaskState.COMPLETED)
        self.assertEqual(self.workflow.get_tasks(spec_name='RegistrationCompensation'), [])

        cancel_branch = self.workflow.get_tasks(spec_name='CancelBranchTask')
        self.assertTrue(all(task.state != TaskState.COMPLETED for task in cancel_branch))
        self.assertFalse(any(record['spec_name'] == 'ActivityC' for record in self.workflow.compensation.records))
        self.assertTrue(all(record.get('handler') for record in self.workflow.compensation.records))
