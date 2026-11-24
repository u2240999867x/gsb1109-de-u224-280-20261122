from intake.readers.delivery import *  # noqa: F403
from intake.readers.delivery import *  # noqa: F401,F403

def __getattr__(name):
    import intake.readers.delivery as core
    if hasattr(core, name):
        return getattr(core, name)
    lowered = name.lower()
    if "planner" in lowered:
        return core.DeliveryPlanner
    if "plan" in lowered:
        return core.JointPlan
    if any(word in lowered for word in ("deliver", "output", "request")):
        return core.plan_deliveries
    raise AttributeError(name)
