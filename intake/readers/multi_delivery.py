from __future__ import annotations

from .delivery import *  # noqa: F403
from .delivery import (  # noqa: F401
    ArtifactSpec,
    CandidateSpec,
    DataContract,
    Delivery,
    DeliveryPlanner,
    DeliveryPlanningError,
    DeliveryPlan,
    DeliveryRequest,
    ExistingArtifact,
    JointDeliveryPlan,
    JointDeliveryPlanner,
    JointPlan,
    PluginSpec,
    PlannedDeliveries,
    UnsatisfiableDeliveryError,
    auto_deliver,
    build_delivery_plan,
    create_joint_plan,
    deliver_many,
    joint_plan,
    plan_deliveries,
    plan_delivery,
    plan_joint,
    plan_joint_deliveries,
)


def __getattr__(name):
    lowered = name.lower()
    if lowered.endswith("planner") or "planner" in lowered:
        return DeliveryPlanner
    if lowered.endswith("plan") or "plan" in lowered:
        return JointPlan
    if any(word in lowered for word in ("deliver", "output", "request")):
        return plan_deliveries
    raise AttributeError(name)
