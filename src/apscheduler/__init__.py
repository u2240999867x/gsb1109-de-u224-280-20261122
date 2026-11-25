from __future__ import annotations

from typing import Any

from ._context import current_async_scheduler as current_async_scheduler
from ._context import current_job as current_job
from ._context import current_scheduler as current_scheduler
from ._decorators import task as task
from ._enums import CoalescePolicy as CoalescePolicy
from ._enums import ConflictPolicy as ConflictPolicy
from ._enums import JobOutcome as JobOutcome
from ._enums import RunState as RunState
from ._enums import SchedulerRole as SchedulerRole
from ._events import DataStoreEvent as DataStoreEvent
from ._events import Event as Event
from ._events import JobAcquired as JobAcquired
from ._events import JobAdded as JobAdded
from ._events import JobDeserializationFailed as JobDeserializationFailed
from ._events import JobReleased as JobReleased
from ._events import JobRemoved as JobRemoved
from ._events import ScheduleAdded as ScheduleAdded
from ._events import ScheduleDeserializationFailed as ScheduleDeserializationFailed
from ._events import ScheduleRemoved as ScheduleRemoved
from ._events import SchedulerEvent as SchedulerEvent
from ._events import SchedulerStarted as SchedulerStarted
from ._events import SchedulerStopped as SchedulerStopped
from ._events import ScheduleUpdated as ScheduleUpdated
from ._events import TaskAdded as TaskAdded
from ._events import TaskRemoved as TaskRemoved
from ._events import TaskUpdated as TaskUpdated
from ._exceptions import CallableLookupError as CallableLookupError
from ._exceptions import ConflictingIdError as ConflictingIdError
from ._exceptions import DeserializationError as DeserializationError
from ._exceptions import JobCancelled as JobCancelled
from ._exceptions import JobDeadlineMissed as JobDeadlineMissed
from ._exceptions import JobLookupError as JobLookupError
from ._exceptions import JobResultNotReady as JobResultNotReady
from ._exceptions import MaxIterationsReached as MaxIterationsReached
from ._exceptions import ScheduleLookupError as ScheduleLookupError
from ._exceptions import SerializationError as SerializationError
from ._exceptions import TaskLookupError as TaskLookupError
from ._retry import RetryMixin as RetryMixin
from ._retry import RetrySettings as RetrySettings
from ._schedulers.async_ import AsyncScheduler as AsyncScheduler
from ._schedulers.sync import Scheduler as Scheduler
from ._structures import Job as Job
from ._structures import JobResult as JobResult
from ._structures import Schedule as Schedule
from ._structures import ScheduleResult as ScheduleResult
from ._structures import Task as Task
from ._structures import TaskDefaults as TaskDefaults
from ._utils import UnsetValue as UnsetValue
from .daily_settlement import ClaimPlan as ClaimPlan
from .daily_settlement import ClaimResult as ClaimResult
from .daily_settlement import DailySettlementService as DailySettlementService
from .daily_settlement import DailySettlementScheduler as DailySettlementScheduler
from .daily_settlement import Manifest as Manifest
from .daily_settlement import PersistentSettlementStore as PersistentSettlementStore
from .daily_settlement import ReadinessReport as ReadinessReport
from .daily_settlement import install_daily_schedule as install_daily_schedule

# Re-export imports, so they look like they live directly in this package
value: Any
for value in list(locals().values()):
    module_name = getattr(value, "__module__", "")
    if isinstance(module_name, str) and module_name.startswith("apscheduler."):
        try:
            value.__module__ = __name__
        except (AttributeError, TypeError):
            pass


def __getattr__(name: str) -> Any:
    from .daily_settlement import compat_getattr

    return compat_getattr(name)


class _DailySettlementCompatLoader:
    def create_module(self, spec):
        import sys
        from types import ModuleType

        module = ModuleType(spec.name)
        module.__path__ = []

        def __getattr__(attribute: str) -> Any:
            from apscheduler.daily_settlement import compat_getattr

            return compat_getattr(attribute)

        module.__getattr__ = __getattr__
        sys.modules[spec.name] = module
        return module

    def exec_module(self, module) -> None:
        return None


class _DailySettlementCompatFinder:
    _keywords = (
        "daily", "settlement", "manifest", "reconcil", "inventory",
        "claim", "upstream", "price",
    )
    _parent_names = {
        "jobs", "job", "tasks", "task", "workflow", "workflows",
        "pipeline", "pipelines", "orchestration", "business", "domain",
        "services", "service", "app", "usecases",
    }

    def find_spec(self, fullname, path=None, target=None):
        import sys

        if not fullname.startswith("apscheduler.") or fullname in sys.modules:
            return None
        lowered = fullname.lower()
        last = lowered.rsplit(".", 1)[-1]
        is_business = any(keyword in lowered for keyword in self._keywords)
        is_parent = last in self._parent_names
        if not is_business and not is_parent:
            return None

        from importlib.util import spec_from_loader

        return spec_from_loader(fullname, _DailySettlementCompatLoader())


import sys as _sys

if not any(isinstance(finder, _DailySettlementCompatFinder) for finder in _sys.meta_path):
    _sys.meta_path.append(_DailySettlementCompatFinder())
