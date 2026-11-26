import warnings

from ._deprecations import _DEPRECATED_CLASS_NAMES
from ._deprecations import _deprecated_class_message
from .graph import Correction  # noqa
from .graph import CorrectionConflict  # noqa
from .graph import correct_graph  # noqa
from .graph import Edge  # noqa
from .graph import find_graph  # noqa
from .graph import GraphCycle  # noqa
from .graph import GraphMatch  # noqa
from .graph import GraphQuery  # noqa
from .jsonpath import *  # noqa
from .parser import parse  # noqa


def __getattr__(name: str) -> object:
    if name in _DEPRECATED_CLASS_NAMES:
        from . import jsonpath

        warnings.warn(_deprecated_class_message(name), DeprecationWarning, stacklevel=2)
        return getattr(jsonpath, f"_{name}")
    if name == "__version__":
        msg = (
            "The `__version__` attribute is deprecated. "
            "It will be removed in version 2.0.0. "
            'Use `importlib.metadata.version("jsonpath-ng")` instead.'
        )
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        import importlib.metadata

        return importlib.metadata.version("jsonpath-ng")
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
