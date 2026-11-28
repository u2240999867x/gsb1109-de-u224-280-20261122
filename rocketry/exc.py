class SchedulerRestart(Exception):
    "Restart the scheduler"


class SchedulerExit(Exception):
    "Shut down the scheduler"


class TaskInactionException(Exception):
    "Task did not succeed nor fail. It did not need to run."


class TaskTerminationException(Exception):
    """Task was terminated.
    This should only be raised by threaded
    tasks to signal that they did indeed
    listen the thread_please_terminate event
    and ended as a result of that"""


# System errors
class TaskSetupError(Exception):
    """Task's setup failed"""

class TaskLoggingError(Exception):
    """Task's logging failed"""


class RunNotAvailable(KeyError):
    """A run accepted by a handoff has no return value available.

    This is raised instead of silently falling back to another run of
    the same task.  It is a ``KeyError`` for backwards compatibility
    with callers that treat missing returns as key errors.
    """

    def __str__(self):
        # KeyError adds quotes around the message; keep it readable.
        return str(self.args[0]) if self.args else ""

