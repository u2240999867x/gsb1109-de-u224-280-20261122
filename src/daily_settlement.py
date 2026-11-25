from __future__ import annotations

from apscheduler.daily_settlement import *  # noqa: F403
from apscheduler.daily_settlement import compat_getattr  # noqa: F401


def __getattr__(name: str):
    return compat_getattr(name)
