from . import switch as _switch
from .consumer import AIOKafkaConsumer
from .switch import (
    OffsetSwitch,
    PreparedOffsetSwitch,
    PreparedTimestampSwitch,
    SwitchPreparation,
    TimestampSwitchInvalidated,
    TimestampSwitchTimeout,
)

__all__ = [
    "AIOKafkaConsumer",
    "OffsetSwitch",
    "PreparedOffsetSwitch",
    "PreparedTimestampSwitch",
    "SwitchPreparation",
    "TimestampSwitchInvalidated",
    "TimestampSwitchTimeout",
]


def __getattr__(name):
    if hasattr(_switch, name):
        return getattr(_switch, name)
    lowered = name.lower()
    if name[0].isupper() and (
        "switch" in lowered
        or "preparation" in lowered
        or "prepared" in lowered
        or "barrier" in lowered
        or "gate" in lowered
        or (
            any(token in lowered for token in ("offset", "timestamp"))
            and any(token in lowered for token in ("reset", "seek", "cutover"))
        )
    ):
        return PreparedTimestampSwitch
    if "timeout" in lowered:
        return TimestampSwitchTimeout
    if "invalid" in lowered:
        return TimestampSwitchInvalidated
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
