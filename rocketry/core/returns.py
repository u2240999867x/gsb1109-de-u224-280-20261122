"""Storage for task return values.

The legacy API ``session.returns[task]`` still returns the latest
successful return of the task.  In addition, values are stored per
concrete run (``task + run_id``) so a handoff can address exactly the
run it accepted even if another run of the same task finishes later.
"""

from typing import Any, Dict, List, Optional, Tuple

from rocketry.core.handoff import RunReference, get_task_name
from rocketry.core.parameters.parameters import Parameters


class RunReturn:
    """Return value of one concrete run."""

    __slots__ = ("task_name", "run_id", "value", "created", "withdrawn", "reason")

    def __init__(self, task_name: str, run_id: Optional[str], value: Any,
                 created: Optional[float] = None, withdrawn: bool = False,
                 reason: Optional[str] = None):
        self.task_name = task_name
        self.run_id = run_id
        self.value = value
        self.created = created
        self.withdrawn = withdrawn
        self.reason = reason

    @property
    def task(self) -> str:
        return self.task_name

    def withdraw(self, reason: Optional[str] = None) -> "RunReturn":
        self.withdrawn = True
        if reason is not None:
            self.reason = reason
        return self

    def __bool__(self):
        return not self.withdrawn

    def __repr__(self):
        token = self.run_id if self.run_id is not None else self.created
        state = ", withdrawn" if self.withdrawn else ""
        return f"RunReturn({self.task_name!r}, {token!r}, {self.value!r}{state})"


class Returns(Parameters):
    """Session return storage.

    Behaves like the old ``Parameters`` mapping (task -> latest value)
    and additionally keeps values keyed by concrete run.
    """

    def __init__(self, _param=None, **params):
        super().__init__(_param, **params)
        self._runs: Dict[Tuple[str, str], RunReturn] = {}
        self._withdrawn: Dict[Tuple[str, str], RunReturn] = {}

    # Run aware storage
    # -----------------

    def add_run(self, task: Any, run_id: Optional[str], value: Any,
                created: Optional[float] = None) -> RunReturn:
        """Store a return value for a concrete run."""
        task_name = get_task_name(task)
        run_return = RunReturn(task_name, run_id, value, created=created)
        key = self._get_key(task_name, run_id)
        if key is not None and key in self._withdrawn:
            # A withdrawn run stays unavailable even if its value arrives late.
            run_return.withdrawn = True
            run_return.reason = self._withdrawn[key].reason
            self._withdrawn[key] = run_return
        if key is not None:
            if not run_return.withdrawn:
                self._runs[key] = run_return
        return run_return

    def get_run(self, task: Any, run_id: Optional[str] = None,
                created: Optional[float] = None) -> RunReturn:
        """Get the return of one run.

        Raises
        ------
        KeyError
            If the run has no successful return or the run was withdrawn.
        """
        from rocketry.exc import RunNotAvailable

        task_name = get_task_name(task)
        key = self._get_key(task_name, run_id)
        run_return = None
        if key is not None:
            run_return = self._runs.get(key) or self._withdrawn.get(key)
        if run_return is None and run_id is None:
            # Legacy log without run_id: bind at acceptance time to the
            # latest run available then.  Later runs must not shift it.
            run_return = self._find_by_created(task_name, created) if created is not None else self.latest(task_name)
        elif run_return is None and created is not None:
            run_return = self._find_by_created(task_name, created)
        if run_return is None:
            token = run_id if run_id is not None else created
            raise KeyError(
                f"Return value not found for run {token!r} of task {task_name!r}"
            )
        if run_return.withdrawn:
            reason = f" ({run_return.reason})" if run_return.reason else ""
            token = run_return.run_id if run_return.run_id is not None else run_return.created
            raise RunNotAvailable(
                f"Accepted run {token!r} of task {task_name!r} is not available{reason}"
            )
        return run_return

    def get_value(self, task: Any, run_id: Optional[str] = None,
                  created: Optional[float] = None) -> Any:
        "Get the value of one run."
        return self.get_run(task, run_id=run_id, created=created).value

    def withdraw(self, task: Any, run_id: Optional[str] = None,
                 created: Optional[float] = None, reason: str = None) -> bool:
        """Mark a run unavailable.

        Returns ``True`` if the run was known before the withdrawal.
        """
        task_name = get_task_name(task)
        if run_id is None and created is not None:
            existing = self._find_by_created(task_name, created)
            if existing is not None:
                run_id = existing.run_id
        key = self._get_key(task_name, run_id)
        if key is None and created is not None:
            # The run is identified by its success timestamp only.
            existing = self._find_by_created(task_name, created)
            if existing is not None:
                key = self._get_key(task_name, existing.run_id)
                run_id = existing.run_id
        if key is None:
            return False
        known = key in self._runs or key in self._withdrawn
        run_return = self._runs.get(key) or self._withdrawn.get(key)
        if run_return is None:
            run_return = RunReturn(task_name, run_id, None, created=created)
        run_return.withdraw(reason=reason)
        self._withdrawn[key] = run_return
        if key in self._runs:
            del self._runs[key]
        return known

    # Descriptive aliases.
    invalidate = withdraw
    reject = withdraw
    withdraw_run = withdraw
    invalidate_run = withdraw

    def is_available(self, task: Any, run_id: Optional[str] = None) -> bool:
        try:
            self.get_run(task, run_id=run_id)
        except KeyError:
            return False
        return True

    def latest(self, task: Any, include_withdrawn: bool = False) -> Optional[RunReturn]:
        "Get the latest known run of a task (in completion order)."
        task_name = get_task_name(task)
        candidates = []
        for (name, _), run_return in self._runs.items():
            if name == task_name:
                candidates.append(run_return)
        if include_withdrawn:
            for (name, _), run_return in self._withdrawn.items():
                if name == task_name:
                    candidates.append(run_return)
        if not candidates:
            return None
        if not include_withdrawn:
            candidates = [run for run in candidates if not run.withdrawn]
        if not candidates:
            return None
        return max(candidates, key=lambda run: run.created or 0.0)

    def set_created(self, task: Any, run_id: Optional[str], created: float) -> None:
        "Set a run's completion timestamp to match its success log record."
        key = self._get_key(get_task_name(task), run_id)
        if key is None:
            return
        run_return = self._runs.get(key) or self._withdrawn.get(key)
        if run_return is not None:
            run_return.created = created

    def find_by_created(self, task: Any, created: float,
                        tolerance: float = 0.05,
                        include_withdrawn: bool = True) -> Optional[RunReturn]:
        "Find a stored run by the success log timestamp."
        task_name = get_task_name(task)
        return self._find_by_created(
            task_name, created, tolerance=tolerance,
            include_withdrawn=include_withdrawn,
        )

    def resolve_reference(self, task: Any, run_id: Optional[str] = None,
                          created: Optional[float] = None):
        """Resolve a ticket reference to a concrete stored run id when possible.

        A log record may not carry ``run_id`` (legacy ``MinimalRecord``).
        The stored return does know it, so use it to make the ticket
        point at the exact run instead of a timestamp approximation.
        """
        from rocketry.core.handoff import RunReference

        task_name = get_task_name(task)
        if run_id is not None:
            return RunReference(task_name, str(run_id))
        if created is not None:
            stored = self.find_by_created(task_name, created, include_withdrawn=True)
            if stored is not None:
                return RunReference(task_name, stored.run_id, created=created)
        return RunReference(task_name, None, created=created)

    def runs(self, task: Any = None) -> List[RunReturn]:
        "List known run returns (optionally filtered by task)."
        values = list(self._runs.values()) + list(self._withdrawn.values())
        if task is None:
            return values
        task_name = get_task_name(task)
        return [value for value in values if value.task_name == task_name]

    # Helpers
    # -------

    @staticmethod
    def _get_key(task_name: str, run_id: Optional[str]) -> Optional[Tuple[str, str]]:
        if run_id is None:
            return None
        return (task_name, str(run_id))

    def _find_by_created(self, task_name: str, created: float,
                         tolerance: float = 0.05,
                         include_withdrawn: bool = True) -> Optional[RunReturn]:
        candidates = list(self._runs.values())
        if include_withdrawn:
            candidates += list(self._withdrawn.values())
        candidates = [
            run_return
            for run_return in candidates
            if run_return.task_name == task_name
            and run_return.created is not None
            and abs(run_return.created - created) <= tolerance
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda run: abs(run.created - created))

    def __getitem__(self, item):
        if isinstance(item, tuple) and len(item) in (2, 3):
            task, run_id, *rest = item
            created = rest[0] if rest else None
            return self.get_value(task, run_id=run_id, created=created)
        try:
            return super().__getitem__(item)
        except KeyError:
            # Allow lookup by task name as well as by task object.
            pass
        task_name = get_task_name(item)
        latest = self.latest(task_name)
        if latest is None or latest.withdrawn:
            raise KeyError(task_name)
        return latest.value

    def __contains__(self, item):
        if isinstance(item, tuple) and len(item) in (2, 3):
            task, run_id, *rest = item
            created = rest[0] if rest else None
            try:
                self.get_run(task, run_id=run_id, created=created)
            except KeyError:
                return False
            return True
        return super().__contains__(item)


def ensure_returns(value) -> Returns:
    "Turn a Parameters/None value to a Returns instance."
    if isinstance(value, Returns):
        return value
    if value is None:
        return Returns()
    if isinstance(value, Parameters):
        value = value.to_dict()
    return Returns(value)
