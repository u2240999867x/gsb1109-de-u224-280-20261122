"""Run-scoped handoff between task starts and task parameters.

A handoff (or ticket) is an explicit list of upstream task runs a
downstream task accepts as its inputs.  It is deliberately separate
from ``Return``: the ticket answers *which run* is accepted while
``Return`` answers *how to read a value* for the run it was handed.

The classes here are intentionally dependency free so they can be
pickled and passed to thread/process executions.
"""

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional


def get_task_name(task: Any) -> str:
    """Resolve a task, task function or task name to a task name."""
    if isinstance(task, str):
        return task
    if hasattr(task, "__rocketry__"):
        # FuncTask decorator stores the task info to the function.
        info = task.__rocketry__
        name = info.get("name") if isinstance(info, dict) else None
        if name is not None:
            return name
    name = getattr(task, "name", None)
    if name is not None:
        return str(name)
    raise TypeError(f"Cannot determine task name from: {type(task)}")


@dataclass(frozen=True)
class RunReference:
    """A reference to one concrete run of one task."""

    task_name: str
    run_id: Optional[str] = None
    created: Optional[float] = None

    @classmethod
    def from_value(cls, task: Any, run_id: str = None,
                   created: float = None) -> "RunReference":
        if isinstance(task, RunReference):
            return task
        if isinstance(task, tuple) and run_id is None:
            if len(task) == 2:
                task, run_id = task
            elif len(task) == 3:
                task, run_id, created = task
        if run_id is None and created is None:
            raise TypeError("A run reference requires a task and a run id or timestamp")
        if run_id is not None:
            # ``RunReference(Task, run_id)`` is accepted as well.
            return cls(get_task_name(task), str(run_id), created)
        return cls(get_task_name(task), None, created)

    @property
    def task(self) -> str:
        return self.task_name

    @property
    def id(self) -> str:
        return self.run_id if self.run_id is not None else f"@{self.created}"

    @property
    def token(self) -> str:
        return self.id

    def __iter__(self) -> Iterator[Any]:
        return iter((self.task_name, self.run_id))



@dataclass
class RunReturn:
    """Value returned by one concrete task run."""

    task_name: str
    run_id: str
    value: Any
    created: Optional[float] = None
    withdrawn: bool = False
    reason: Optional[str] = None

    def __bool__(self):
        return not self.withdrawn


class Handoff:
    """Explicit list of upstream runs accepted by a downstream run.

    Examples
    --------
    >>> Handoff("extract", "A1")
    Handoff(extract:A1)
    >>> Handoff({"extract": "A1", "validate": "V1"})
    Handoff(extract:A1, validate:V1)

    The object is normally attached to the downstream task when it is
    queued, for example ``task.run(_handoff_=ticket)``.  It is also
    attached automatically from a start condition that selected a
    concrete upstream run.
    """

    def __init__(self, *runs, task=None, run_id=None, **accepted):
        self._runs: List[RunReference] = []
        self._withdrawn: List[RunReference] = []
        if runs and all(isinstance(run, str) for run in runs):
            if len(runs) == 2 and ":" not in runs[0] and ":" not in runs[1]:
                # Handoff("extract", "A1") as a shorthand for one run.
                self.accept(runs[0], runs[1])
                runs = ()
        for run in runs:
            self.accept(run)
        if task is not None or run_id is not None:
            self.accept(task, run_id)
        for task_name, task_run_id in accepted.items():
            self.accept(task_name, task_run_id)

    def accept(self, task: Any, run_id: str = None) -> "Handoff":
        """Add one or more accepted runs and return self."""
        if task is None:
            return self
        if isinstance(task, Handoff):
            for ref in task:
                self.accept(ref)
        elif isinstance(task, RunReference):
            self._runs.append(task)
        elif isinstance(task, dict):
            for task_name, task_run_id in task.items():
                self.accept(task_name, task_run_id)
        elif isinstance(task, str) and run_id is None and ":" in task:
            task_name, task_run_id = task.split(":", 1)
            self.accept(task_name.strip(), task_run_id.strip())
        elif isinstance(task, (list, tuple, set)) and run_id is None:
            if isinstance(task, tuple) and len(task) == 2 and not isinstance(
                task[0], (list, tuple, set, dict)
            ):
                self._runs.append(RunReference.from_value(task[0], task[1]))
            else:
                for item in task:
                    self.accept(item)
        else:
            self._runs.append(RunReference.from_value(task, run_id))
        return self

    # Common aliases
    add = accept
    add_run = accept
    include = accept
    accept_run = accept
    select = accept
    select_run = accept

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Handoff":
        "Create a ticket from ``{task_name: run_id}``."
        return cls(data)

    @classmethod
    def from_task(cls, task: Any, run_id: Optional[str] = None,
                  created: Optional[float] = None) -> "Handoff":
        return cls(RunReference.from_value(task, run_id, created))

    of = from_task

    def withdraw(self, task: Any, run_id: str = None) -> "Handoff":
        """Remove an accepted run from this ticket."""
        ref = RunReference.from_value(task, run_id)
        return self.withdraw_reference(ref)

    def withdraw_reference(self, ref: RunReference) -> "Handoff":
        "Remove an accepted reference from this ticket."
        remaining = []
        for run in self._runs:
            same = run == ref
            if not same and ref.run_id is not None and run.run_id is not None:
                same = (run.task_name == ref.task_name and run.run_id == ref.run_id)
            if not same and ref.run_id is None and ref.created is not None:
                same = (
                    run.task_name == ref.task_name
                    and run.run_id is None
                    and run.created is not None
                    and abs(run.created - ref.created) <= 0.05
                )
            if same:
                if run not in self._withdrawn:
                    self._withdrawn.append(run)
            else:
                remaining.append(run)
        self._runs = remaining
        return self

    remove = withdraw

    @property
    def withdrawn_runs(self) -> List[RunReference]:
        return list(self._withdrawn)

    def is_withdrawn(self, task: Any, run_id: str = None) -> bool:
        ref = RunReference.from_value(task, run_id)
        return ref in self._withdrawn

    def get_reference(self, task: Any) -> Optional[RunReference]:
        """Get the accepted reference for a task.

        The last acceptance wins, which makes it possible to append a
        supplementary run to an existing ticket.  ``None`` is returned
        if the task has no accepted run left (for example, because it
        was withdrawn); callers must not substitute another run.
        """
        task_name = get_task_name(task)
        for ref in reversed(self._runs):
            if ref.task_name == task_name:
                return ref
        return None

    def get_withdrawn(self, task: Any) -> Optional[RunReference]:
        "Get the latest withdrawn reference for a task."
        task_name = get_task_name(task)
        for ref in reversed(self._withdrawn):
            if ref.task_name == task_name:
                return ref
        return None

    def get_run_id(self, task: Any) -> Optional[str]:
        ref = self.get_reference(task)
        return None if ref is None else ref.run_id

    def is_accepted(self, task: Any, run_id: str = None) -> bool:
        task_name = get_task_name(task)
        return any(
            ref.task_name == task_name and (run_id is None or ref.run_id == str(run_id))
            for ref in self._runs
        )

    def references(self, task: Any = None) -> List[RunReference]:
        if task is None:
            return list(self._runs)
        task_name = get_task_name(task)
        return [ref for ref in self._runs if ref.task_name == task_name]

    @property
    def accepted_runs(self) -> List[RunReference]:
        return list(self._runs)

    @property
    def runs(self) -> List[RunReference]:
        return self.accepted_runs

    def to_dict(self) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {}
        for ref in self._runs:
            result.setdefault(ref.task_name, []).append(ref.run_id)
        return result

    def __iter__(self) -> Iterator[RunReference]:
        return iter(self._runs)

    def __len__(self) -> int:
        return len(self._runs)

    def __bool__(self) -> bool:
        return bool(self._runs)

    def __eq__(self, other) -> bool:
        if isinstance(other, Handoff):
            return self._runs == other._runs
        return False

    def __add__(self, other) -> "Handoff":
        result = Handoff(self)
        if isinstance(other, Handoff):
            result.accept(other)
        elif isinstance(other, RunReference):
            result.accept(other)
        else:
            result.accept(other)
        return result

    def __iadd__(self, other) -> "Handoff":
        self.accept(other)
        return self

    def __repr__(self) -> str:
        content = ", ".join(f"{ref.task_name}:{ref.run_id}" for ref in self._runs)
        return f"{type(self).__name__}({content})"

    def __str__(self) -> str:
        return repr(self)


# Descriptive aliases used by different applications.
RunTicket = Handoff
HandoffTicket = Handoff
Ticket = Handoff
RunHandoff = Handoff
AcceptedRuns = Handoff


def as_handoff(value: Any) -> Optional[Handoff]:
    "Normalize handoff-like values to a Handoff."
    if value is None or isinstance(value, Handoff):
        return value
    if isinstance(value, RunReference):
        return Handoff(value)
    if isinstance(value, dict):
        return Handoff(value)
    if isinstance(value, (list, tuple, set)):
        return Handoff(value)
    return None
