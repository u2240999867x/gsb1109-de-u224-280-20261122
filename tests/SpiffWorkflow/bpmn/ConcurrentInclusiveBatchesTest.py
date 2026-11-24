from SpiffWorkflow import TaskState
from SpiffWorkflow.bpmn import BpmnWorkflow

from .BpmnWorkflowTestCase import BpmnWorkflowTestCase


class ConcurrentInclusiveBatchesTest(BpmnWorkflowTestCase):
    """Two batches share an inclusive split/join and must merge independently."""

    def setUp(self):
        spec, subprocesses = self.load_workflow_spec('Concurrent-Inclusive-Batches.bpmn', 'concurrent_batches')
        self.workflow = BpmnWorkflow(spec, subprocesses)
        self.workflow.do_engine_steps()
        self.route_ids = {}

    def start_batch(self, batch, include_content=True, include_checklist=True):
        task = self.ready('batch_%d_input' % batch)[0]
        task.data.update(
            batch=batch,
            include_content=include_content,
            include_checklist=include_checklist,
        )
        task.run()
        self.workflow.do_engine_steps()
        route = self.workflow.get_tasks(spec_name='route', state=TaskState.COMPLETED)[-1]
        self.route_ids[batch] = str(route.id)

    def start_both(self, batch_2=None):
        # The order in which batches arrive must not matter.
        self.start_batch(1)
        self.start_batch(2, **(batch_2 or {}))

    def ready(self, name):
        return self.workflow.get_tasks(state=TaskState.READY, spec_name=name)

    def complete(self, name, batch, **data):
        task = next(t for t in self.ready(name) if t.data.get('batch') == batch)
        if data:
            task.data.update(data)
        task.run()
        self.workflow.do_engine_steps()
        return task

    def publish_ready(self):
        return self.ready('publish')

    def publish_batches(self):
        return sorted(t.data.get('batch') for t in self.publish_ready())

    def assert_no_publish_ready(self):
        self.assertEqual([], self.publish_batches())

    def finish_publish(self, batch):
        self.complete('publish', batch)

    def finish_successor(self, batch):
        self.complete('successor', batch)

    def successor_tasks(self):
        return self.workflow.get_tasks(spec_name='successor', state=TaskState.COMPLETED)

    def test_cross_completion_waits_for_each_batch(self):
        self.start_both()

        self.complete('content', 1)
        self.complete('checklist', 2)
        self.assert_no_publish_ready()

        self.save_restore()

        self.complete('checklist', 1)
        self.assertEqual([1], self.publish_batches())
        self.finish_publish(1)
        self.complete('successor', 1)

        self.assertEqual([], self.publish_batches())
        self.complete('content', 2)
        self.assertEqual([2], self.publish_batches())
        self.finish_publish(2)
        self.complete('successor', 2)

        successors = self.successor_tasks()
        self.assertEqual(2, len(successors))
        tokens_by_batch = {t.data.get('batch'): t.get_branch_tokens() for t in successors}
        self.assertEqual([self.route_ids[1]], tokens_by_batch[1])
        self.assertEqual([self.route_ids[2]], tokens_by_batch[2])
        self.assertEqual(2, len(set(tuple(tokens) for tokens in tokens_by_batch.values())))
        self.assertTrue(self.workflow.completed)

    def test_batch_order_can_be_swapped(self):
        self.start_batch(2)
        self.start_batch(1)

        self.complete('checklist', 2)
        self.assert_no_publish_ready()
        self.complete('content', 1)
        self.assert_no_publish_ready()

        self.complete('content', 2)
        self.assertEqual([2], self.publish_batches())
        self.finish_publish(2)
        self.complete('successor', 2)

        self.complete('checklist', 1)
        self.assertEqual([1], self.publish_batches())
        self.finish_publish(1)
        self.complete('successor', 1)

        self.assertTrue(self.workflow.completed)

    def test_single_path_batch_publishes_without_waiting_for_other_batch(self):
        self.start_batch(1, include_content=True, include_checklist=False)
        self.start_batch(2, include_content=False, include_checklist=False)

        # The default-only batch completes first; it must not wait for batch 1.
        self.complete('default_work', 2)
        self.assertEqual([2], self.publish_batches())
        self.finish_publish(2)
        self.complete('successor', 2)

        self.assert_no_publish_ready()
        self.complete('content', 1)
        self.assertEqual([1], self.publish_batches())
        self.finish_publish(1)
        self.complete('successor', 1)

        successors = self.successor_tasks()
        self.assertEqual(2, len(successors))
        self.assertEqual(
            {1: [self.route_ids[1]], 2: [self.route_ids[2]]},
            {t.data.get('batch'): t.get_branch_tokens() for t in successors},
        )
        self.assertTrue(self.workflow.completed)

    def test_single_content_batch_does_not_wait_for_default_batch(self):
        self.start_batch(1, include_content=True, include_checklist=False)
        self.start_batch(2, include_content=False, include_checklist=False)

        self.complete('content', 1)
        self.assertEqual([1], self.publish_batches())
        self.finish_publish(1)
        self.complete('successor', 1)

        self.assert_no_publish_ready()
        self.complete('default_work', 2)
        self.assertEqual([2], self.publish_batches())
        self.finish_publish(2)
        self.complete('successor', 2)

        self.assertTrue(self.workflow.completed)

    def test_save_restore_preserves_token_lineage(self):
        self.start_both()
        self.complete('content', 1)
        self.complete('checklist', 2)

        route_ids = dict(self.route_ids)
        self.save_restore()

        self.complete('checklist', 1)
        self.finish_publish(1)
        self.complete('successor', 1)
        self.complete('content', 2)
        self.finish_publish(2)
        self.complete('successor', 2)

        successors = self.successor_tasks()
        self.assertEqual(
            {1: [route_ids[1]], 2: [route_ids[2]]},
            {t.data.get('batch'): t.get_branch_tokens() for t in successors},
        )
        self.assertTrue(self.workflow.completed)
