from SpiffWorkflow.bpmn.util import BpmnEvent

from .base import EventDefinition

class NoneEventDefinition(EventDefinition):
    """This class defines behavior for NoneEvents.  We override throw to do nothing."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def throw(self, my_task):
        """It's a 'none' event, so nothing to throw."""
        pass

    def reset(self, my_task):
        """It's a 'none' event, so nothing to reset."""
        pass


class CancelEventDefinition(EventDefinition):
    """Cancel events are only handled by the outerworkflow, as they can only be used inside of transaction subprocesses."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def throw(self, my_task, **kwargs):
        top_workflow = my_task.workflow.top_workflow
        compensation = getattr(top_workflow, 'compensation', None)
        if compensation is not None:
            compensation.prepare_cancellation(my_task)
        event = BpmnEvent(self, target=my_task.workflow.parent_workflow)
        top_workflow.catch(event)


class CompensateEventDefinition(EventDefinition):
    """Event definition used by compensation boundary and throw events."""

    def __init__(self, activity_ref=None, wait_for_completion=True, **kwargs):
        super().__init__(**kwargs)
        self.activity_ref = activity_ref
        self.wait_for_completion = wait_for_completion

    def throw(self, my_task, **kwargs):
        event = BpmnEvent(
            self,
            target=my_task.workflow,
            payload={'activity_ref': self.activity_ref},
        )
        my_task.workflow.top_workflow.catch(event)


# BPMN uses the "compensate" event definition, but many APIs name the concept
# "compensation event".  Expose both spellings for compatibility.
CompensationEventDefinition = CompensateEventDefinition


class TerminateEventDefinition(EventDefinition):
    """The TerminateEventDefinition is the implementation of event definition used for Termination Events."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def throw(self, my_task):
        event = BpmnEvent(my_task.task_spec.event_definition, target=my_task.workflow)
        my_task.workflow.top_workflow.catch(event)
