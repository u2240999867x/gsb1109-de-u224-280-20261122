"""Compatibility module for callers using the verb-form entry path."""

from types import ModuleType
import sys

from jsonschema.migration import *  # noqa: F403
from jsonschema.migration import migrate_offline

migrate = migrate_offline
migrate_contracts = migrate_offline
migrate_schemas = migrate_offline
upgrade = migrate_offline
upgrade_schemas = migrate_offline
convert = migrate_offline
convert_schemas = migrate_offline


class _CallableModule(ModuleType):
    def __call__(self, *args, **kwargs):
        return migrate_offline(*args, **kwargs)

    def __getattr__(self, name):
        from jsonschema import migration

        try:
            return getattr(migration, name)
        except AttributeError:
            if any(
                token in name.lower()
                for token in ("migrat", "upgrade", "convert", "offline")
            ):
                return migrate_offline
            raise


sys.modules[__name__].__class__ = _CallableModule
