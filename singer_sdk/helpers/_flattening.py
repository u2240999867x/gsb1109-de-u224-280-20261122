"""Internal helper library for record flattening functions."""

from __future__ import annotations

__lazy_modules__ = {
    "collections.abc",
    "inflection",
    "singer_sdk.exceptions",
    "singer_sdk.singerlib.json",
}

import collections.abc
import re
import typing as t
from dataclasses import KW_ONLY, dataclass

import inflection

from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.singerlib.json import serialize_json

DEFAULT_FLATTENING_SEPARATOR = "__"
DEFAULT_MAX_KEY_LENGTH = 255

_LOWERCASE_RE = re.compile(r"[a-z]")
_NULL_OBJECT_ARRAY_TYPES: frozenset[str] = frozenset({"null", "object", "array"})

_T = t.TypeVar("_T")


def _first(iterable: t.Iterable[_T]) -> _T | None:
    return next(iter(iterable), None)


class PluginFlatteningConfig(t.TypedDict, total=False):
    """Plugin flattening configuration."""

    flattening_enabled: bool
    flattening_max_depth: int
    flattening_max_key_length: int
    flattening_separator: str


@dataclass
class FlatteningOptions:
    """A stream map which performs the flattening role."""

    _: KW_ONLY

    enabled: bool
    max_level: int = -1
    separator: str = DEFAULT_FLATTENING_SEPARATOR
    max_key_length: int = DEFAULT_MAX_KEY_LENGTH

    def __post_init__(self) -> None:
        """Post-init validation."""
        if self.enabled and self.max_level == -1:
            msg = "Flattening is misconfigured"
            raise ConfigValidationError(
                msg,
                errors=["flattening_max_depth is required when flattening is enabled"],
            )

    @classmethod
    def from_dict(cls, data: PluginFlatteningConfig) -> FlatteningOptions:
        kwargs: dict[str, t.Any] = {}
        if (max_depth := data.get("flattening_max_depth")) is not None:
            kwargs["max_level"] = max_depth

        if (max_key_length := data.get("flattening_max_key_length")) is not None:
            kwargs["max_key_length"] = max_key_length

        if (separator := data.get("flattening_separator")) is not None:
            kwargs["separator"] = separator

        return cls(enabled=data.get("flattening_enabled", False), **kwargs)


def get_flattening_options(plugin_config: PluginFlatteningConfig) -> FlatteningOptions:
    """Get flattening options, if flattening is enabled.

    Args:
        plugin_config: The tap or target config dictionary.

    Returns:
        A new FlatteningOptions object.
    """
    return FlatteningOptions.from_dict(plugin_config)


def _abbreviate_key(key_name: str) -> str:
    """Reduce a single key component to keep flattened names short.

    Args:
        key_name: The key component to abbreviate.

    Returns:
        The abbreviated, lower-cased component.
    """
    reduced_key = _LOWERCASE_RE.sub("", inflection.camelize(key_name))
    return (
        reduced_key if len(reduced_key) > 1 else key_name[:3]
    ).lower()


def flatten_key(
    key_name: str,
    parent_keys: list[str],
    separator: str = "__",
    *,
    max_key_length: int = DEFAULT_MAX_KEY_LENGTH,
) -> str:
    """Concatenate `key_name` with its `parent_keys` using `separator`.

    Args:
        key_name: The node's key.
        parent_keys: A list of parent keys which are ancestors to this node.
        separator: The separator used during concatenation. Defaults to "__".
        max_key_length: The maximum length of the key. Defaults to 255.

    Returns:
        The flattened key name as a string.

    >>> flatten_key("foo", ["bar", "baz"])
    'bar__baz__foo'

    >>> flatten_key("foo", ["bar", "baz"], separator=".")
    'bar.baz.foo'
    """
    full_key = [*parent_keys, key_name]
    inflected_key = full_key.copy()
    reducer_index = 0
    while (
        len(separator.join(inflected_key)) >= max_key_length  # Keep the key short
        and reducer_index < len(inflected_key)
    ):
        inflected_key[reducer_index] = _abbreviate_key(
            inflected_key[reducer_index],
        )
        reducer_index += 1

    return separator.join(inflected_key)


class FlattenedNameRegistry:
    """Resolve and remember flattened column names for a stream.

    The registry guarantees that each source property path maps to a stable,
    unique output column name for the lifetime of the stream map:

    - Explicit property names (top-level properties) are reserved first, so a
      generated (abbreviated) nested name never steals a published column name.
    - Generated names that collide with a reserved or earlier name get a
      deterministic numeric suffix instead of raising an error.
    - Schema flattening and record flattening share the same registry, so the
      values land in exactly the columns declared by the flattened schema.
    """

    def __init__(
        self,
        separator: str = DEFAULT_FLATTENING_SEPARATOR,
        max_key_length: int = DEFAULT_MAX_KEY_LENGTH,
    ) -> None:
        """Initialize the registry.

        Args:
            separator: The separator used when concatenating key names.
            max_key_length: The maximum length of generated key names.
        """
        self.separator = separator
        self.max_key_length = max_key_length
        self._path_to_name: dict[tuple[str, ...], str] = {}
        self._names: set[str] = set()

    def reserve_field(self, field_name: str) -> str:
        """Reserve the flattened name of an explicit top-level property.

        Explicit properties are reserved before nested properties are
        flattened, so a generated (abbreviated) nested name never steals a
        published top-level column name; the nested name gets a suffix instead.

        Args:
            field_name: The top-level property name to reserve.

        Returns:
            The reserved flattened column name.
        """
        path = (field_name,)
        if existing := self._path_to_name.get(path):
            return existing

        candidate = flatten_key(
            field_name,
            [],
            self.separator,
            max_key_length=self.max_key_length,
        )
        if candidate in self._names:
            candidate = self._unique_name(candidate)
        self._path_to_name[path] = candidate
        self._names.add(candidate)
        return candidate

    def register(self, field_name: str, parent_keys: list[str]) -> str:
        """Resolve (once) the output name for a nested property path.

        Args:
            field_name: The leaf property name.
            parent_keys: The ancestor path of the property.

        Returns:
            The stable, unique flattened column name for this path.
        """
        path = (*parent_keys, field_name)
        if existing := self._path_to_name.get(path):
            return existing

        candidate = flatten_key(
            field_name,
            parent_keys,
            self.separator,
            max_key_length=self.max_key_length,
        )
        if candidate in self._names:
            candidate = self._unique_name(candidate)
        self._path_to_name[path] = candidate
        self._names.add(candidate)
        return candidate

    def lookup(self, field_name: str, parent_keys: list[str]) -> str | None:
        """Return the registered output name for a path, if known.

        Args:
            field_name: The leaf property name.
            parent_keys: The ancestor path of the property.

        Returns:
            The registered output name, or None if the path was not registered.
        """
        return self._path_to_name.get((*parent_keys, field_name))

    def leaf_names_under(self, prefix: tuple[str, ...]) -> list[str]:
        """Return the flattened column names registered below a property path.

        Used to expand an explicitly-null object into null values for every
        flattened leaf column it would have produced.

        Args:
            prefix: The object property path to look beneath.

        Returns:
            The flattened column names of descendant property paths, in
            registration order.
        """
        return [
            name
            for path, name in self._path_to_name.items()
            if len(path) > len(prefix) and path[: len(prefix)] == prefix
        ]

    def _unique_name(self, candidate: str) -> str:
        suffix_index = 1
        unique = candidate
        while unique in self._names:
            suffix = f"_{suffix_index}"
            unique = f"{candidate[: self.max_key_length - len(suffix)]}{suffix}"
            suffix_index += 1
        return unique


def flatten_schema(
    schema: dict,
    max_level: int,
    separator: str = "__",
    *,
    max_key_length: int = DEFAULT_MAX_KEY_LENGTH,
    registry: FlattenedNameRegistry | None = None,
) -> dict:
    """Flatten the provided schema up to a depth of max_level.

    Args:
        schema: The schema definition to flatten.
        separator: The string to use when concatenating key names.
        max_level: The max recursion level (zero-based, exclusive).
        max_key_length: The maximum length of the key. Defaults to 255.
        registry: A name registry shared with record flattening. A new one is
            created and attached to the returned schema when not provided.

    Returns:
        A flattened version of the provided schema definition.

    >>> import json
    >>> schema = {
    ...     "type": "object",
    ...     "properties": {
    ...         "id": {"type": "string"},
    ...         "foo": {
    ...             "type": "object",
    ...             "properties": {
    ...                 "bar": {
    ...                     "type": "object",
    ...                     "properties": {
    ...                         "baz": {
    ...                             "type": "object",
    ...                             "properties": {"qux": {"type": "string"}},
    ...                         }
    ...                     },
    ...                 }
    ...             },
    ...         },
    ...     },
    ... }
    >>> print(json.dumps(flatten_schema(schema, 0), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo": {
          "type": "object",
          "properties": {
            "bar": {
              "type": "object",
              "properties": {
                "baz": {
                  "type": "object",
                  "properties": {
                    "qux": {
                      "type": "string"
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    >>> print(json.dumps(flatten_schema(schema, 1), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo__bar": {
          "type": "string"
        }
      }
    }

    >>> print(json.dumps(flatten_schema(schema, 2), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo__bar__baz": {
          "type": "string"
        }
      }
    }

    >>> print(json.dumps(flatten_schema(schema, 3), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo__bar__baz__qux": {
          "type": "string"
        }
      }
    }

    >>> nullable_leaves_schema = {
    ...     "type": "object",
    ...     "properties": {
    ...         "id": {"type": "string"},
    ...         "foo": {
    ...             "type": ["object", "null"],
    ...             "properties": {
    ...                 "bar": {
    ...                     "type": ["object", "null"],
    ...                     "properties": {
    ...                         "baz": {
    ...                             "type": ["object", "null"],
    ...                             "properties": {"qux": {"type": "string"}},
    ...                         }
    ...                     },
    ...                 }
    ...             },
    ...         },
    ...     },
    ... }
    >>> print(json.dumps(flatten_schema(nullable_leaves_schema, 0), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo": {
          "type": [
            "object",
            "null"
          ],
          "properties": {
            "bar": {
              "type": [
                "object",
                "null"
              ],
              "properties": {
                "baz": {
                  "type": [
                    "object",
                    "null"
                  ],
                  "properties": {
                    "qux": {
                      "type": "string"
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    >>> print(json.dumps(flatten_schema(nullable_leaves_schema, 1), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo__bar": {
          "type": [
            "string",
            "null"
          ]
        }
      }
    }

    >>> print(json.dumps(flatten_schema(nullable_leaves_schema, 2), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo__bar__baz": {
          "type": [
            "string",
            "null"
          ]
        }
      }
    }
    """
    new_schema = {k: v for k, v in schema.items() if k != "properties"}
    if registry is None:
        registry = FlattenedNameRegistry(
            separator=separator,
            max_key_length=max_key_length,
        )
    # Reserve explicit top-level property names first so that generated
    # (abbreviated) nested names cannot steal a published column name.
    for field_name in schema.get("properties", {}):
        registry.reserve_field(field_name)
    new_schema["properties"] = _flatten_schema(
        schema_node=schema,
        parent_keys=[],
        max_level=max_level,
        separator=separator,
        max_key_length=max_key_length,
        registry=registry,
    )
    return new_schema


def _flatten_schema(  # noqa: C901, PLR0912
    schema_node: dict,
    parent_keys: list[str],
    separator: str = "__",
    level: int = 0,
    max_level: int = 0,
    *,
    max_key_length: int = DEFAULT_MAX_KEY_LENGTH,
    registry: FlattenedNameRegistry | None = None,
) -> dict:
    """Flatten the provided schema node, recursively up to depth of `max_level`.

    Args:
        schema_node: The schema node to flatten.
        parent_keys: The parent's key, provided as a list of node names.
            Mutated in place during recursion (append/pop); caller owns the list.
        separator: The string to use when concatenating key names.
        level: The current recursion level (zero-based).
        max_level: The max recursion level (zero-based, exclusive).
        max_key_length: The maximum length of the key. Defaults to 255.
        registry: Registry assigning stable, unique flattened column names.
            A new registry is created when not provided.

    Returns:
        A flattened version of the provided node.
    """
    if registry is None:
        registry = FlattenedNameRegistry(
            separator=separator,
            max_key_length=max_key_length,
        )
    items: list[tuple[str, dict]] = []
    if "properties" not in schema_node:
        return {}

    for field_name, field_schema in schema_node["properties"].items():
        new_key = registry.register(field_name, parent_keys)
        if "type" in field_schema:
            if (
                "object" in field_schema["type"]
                and "properties" in field_schema
                and level < max_level
            ):
                parent_keys.append(field_name)
                items.extend(
                    _flatten_schema(
                        field_schema,
                        parent_keys,
                        separator=separator,
                        level=level + 1,
                        max_level=max_level,
                        max_key_length=max_key_length,
                        registry=registry,
                    ).items(),
                )
                parent_keys.pop()
            elif "array" in field_schema["type"] or (
                "object" in field_schema["type"] and max_level > 0
            ):
                types = (
                    ["string", "null"] if "null" in field_schema["type"] else "string"
                )
                items.append((new_key, {"type": types}))
            else:
                items.append((new_key, field_schema))
        # Handle oneOf, anyOf, etc.
        elif (
            (composite := _first(field_schema.values()))
            and isinstance(composite, list)
            and len(composite) > 0
            and (first_element := _first(composite))
        ):
            if first_element["type"] == "string":
                items.append((new_key, {**first_element, "type": ["null", "string"]}))
            elif first_element["type"] == "array":
                items.append((new_key, {**first_element, "type": ["null", "array"]}))
            elif first_element["type"] == "object":
                items.append((new_key, {**first_element, "type": ["null", "object"]}))
        else:
            # Handle typeless properties (e.g., "PropertyName": {})
            # Treat them as string type to allow JSON serialization
            items.append((new_key, {"type": ["null", "string"]}))

    # Check for duplicates in O(n).
    seen: set[str] = set()
    for key, _ in items:
        if key in seen:
            msg = f"Duplicate column name produced in schema: {key}"
            raise ValueError(msg)
        seen.add(key)

    return dict(items)


def flatten_record(
    record: dict,
    flattened_schema: dict,
    max_level: int,
    separator: str = "__",
    *,
    max_key_length: int = DEFAULT_MAX_KEY_LENGTH,
    registry: FlattenedNameRegistry | None = None,
) -> dict:
    """Flatten a record up to max_level.

    Args:
        record: The record to flatten.
        flattened_schema: The already flattened schema.
        separator: The string used to separate concatenated key names. Defaults to "__".
        max_level: The maximum depth of keys to flatten recursively.
        max_key_length: The maximum length of the key. Defaults to 255.
        registry: The name registry used when flattening the schema. When
            provided, record values are written to the exact column names
            declared in the flattened schema, even when names were de-conflicted.

    Returns:
        A flattened version of the record.
    """
    return _flatten_record(
        record_node=record,
        flattened_schema=flattened_schema,
        parent_key=[],
        separator=separator,
        level=0,
        max_level=max_level,
        max_key_length=max_key_length,
        registry=registry,
    )


def _flatten_record(
    record_node: t.MutableMapping[t.Any, t.Any],
    *,
    flattened_schema: dict | None = None,
    parent_key: list[str],
    separator: str = "__",
    level: int = 0,
    max_level: int = 0,
    max_key_length: int = DEFAULT_MAX_KEY_LENGTH,
    registry: FlattenedNameRegistry | None = None,
) -> dict:
    """This recursive function flattens the record node.

    The current invocation is expected to be at `level` and will continue recursively
    until the provided `max_level` is reached.

    Args:
        record_node: The record node to flatten.
        flattened_schema: The already flattened full schema for the record.
        parent_key: The parent's key, provided as a list of node names.
            Mutated in place during recursion (append/pop); caller owns the list.
        separator: The string to use when concatenating key names.
        level: The current recursion level (zero-based).
        max_level: The max recursion level (zero-based, exclusive).
        max_key_length: The maximum length of the key. Defaults to 255.
        registry: The name registry used when flattening the schema.

    Returns:
        A flattened version of the provided node.
    """
    items: list[tuple[str, t.Any]] = []
    for k, v in record_node.items():
        new_key = (
            registry.lookup(k, parent_key)
            if registry is not None
            else None
        ) or flatten_key(k, parent_key, separator, max_key_length=max_key_length)
        # If the value is a dictionary, and the key is not in the schema, and the
        # level is less than the max level, then we should continue to flatten.
        if (
            isinstance(v, collections.abc.MutableMapping)
            and flattened_schema
            and new_key not in flattened_schema.get("properties", {})
            and (level < max_level)
        ):
            parent_key.append(k)
            items.extend(
                _flatten_record(
                    v,
                    flattened_schema=flattened_schema,
                    parent_key=parent_key,
                    separator=separator,
                    level=level + 1,
                    max_level=max_level,
                    max_key_length=max_key_length,
                    registry=registry,
                ).items(),
            )
            parent_key.pop()
        elif v is None and registry is not None and level < max_level:
            # An explicitly-null object expands into null values for every
            # flattened leaf column it would have produced. An omitted object
            # produces no keys at all (handled simply by not being present in
            # the record). Only columns actually declared in the flattened
            # schema are emitted (intermediate object paths are not columns).
            declared_columns = (
                flattened_schema.get("properties", {}) if flattened_schema else {}
            )
            leaf_names = [
                leaf_name
                for leaf_name in registry.leaf_names_under((*parent_key, k))
                if leaf_name in declared_columns
            ]
            if leaf_names:
                items.extend((leaf_name, None) for leaf_name in leaf_names)
            else:
                items.append((new_key, v))
        else:
            items.append(
                (
                    new_key,
                    serialize_json(v)
                    if _should_jsondump_value(k, v, flattened_schema)
                    else v,
                ),
            )

    return dict(items)


def _should_jsondump_value(
    key: str,
    value: t.Any,  # noqa: ANN401
    flattened_schema: dict[str, t.Any] | None = None,
) -> bool:
    """Return True if json.dump() should be used to serialize the value.

    Args:
        key: [description]
        value: [description]
        flattened_schema: [description]. Defaults to None.

    Returns:
        [description]
    """
    if isinstance(value, (dict, list)):
        return True

    return bool(
        flattened_schema
        and key in flattened_schema
        and "type" in flattened_schema[key]
        and frozenset(flattened_schema[key]["type"]) == _NULL_OBJECT_ARRAY_TYPES
    )
