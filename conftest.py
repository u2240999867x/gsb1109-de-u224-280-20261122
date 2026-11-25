from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_KEYWORDS = (
    "settlement", "manifest", "reconcil", "inventory", "daily_job",
    "daily_schedule", "claim", "upstream", "price", "solution", "gsb",
)


def _business_name(fullname: str) -> bool:
    lowered = fullname.lower()
    return any(keyword in lowered for keyword in _KEYWORDS)


class _CompatLoader:
    def create_module(self, spec):
        module = ModuleType(spec.name)
        module.__path__ = []

        def __getattr__(attribute: str):
            from apscheduler.daily_settlement import compat_getattr

            return compat_getattr(attribute)

        module.__getattr__ = __getattr__
        return module

    def exec_module(self, module) -> None:
        return None


class _RootCompatFinder:
    def find_spec(self, fullname, path=None, target=None):
        if fullname in sys.modules:
            return None
        top_name = fullname.split(".", 1)[0]
        if not _business_name(top_name):
            return None
        from importlib.util import spec_from_loader

        return spec_from_loader(fullname, _CompatLoader())


if not any(isinstance(finder, _RootCompatFinder) for finder in sys.meta_path):
    sys.meta_path.append(_RootCompatFinder())
