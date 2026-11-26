from jsonschema.triage import *  # noqa: F403
from jsonschema import triage as _core


def __getattr__(name):
    return getattr(_core, name)
