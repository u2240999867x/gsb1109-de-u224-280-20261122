"""
Offline Draft 7 to Draft 2020-12 migration helpers.

This module is intentionally tolerant at its public boundary: settlement
callers historically keep resource mappings, entry contracts and review
console addresses in slightly different shapes.  The implementation below
keeps the migration semantics explicit while exposing several conventional
names for the same entry point.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit

from referencing import Registry
from referencing.jsonschema import DRAFT202012

from jsonschema.validators import Draft202012Validator

DRAFT7_SCHEMA_URI = "http://json-schema.org/draft-07/schema#"
DRAFT202012_SCHEMA_URI = "https://json-schema.org/draft/2020-12/schema"

_DRAFT7_MARKERS = ("draft-07", "draft7")
_DRAFT202012_MARKERS = ("2020-12", "draft2020-12", "draft/2020-12")

# Keywords which Draft 7 did not evaluate when they were siblings of $ref.
# In Draft 2019-09 and later $ref is an ordinary applicator, so leaving these
# in place would change historical decisions.
_IGNORED_REF_SIBLINGS = frozenset(
    {
        "additionalItems",
        "allOf",
        "anyOf",
        "const",
        "contains",
        "dependencies",
        "else",
        "enum",
        "format",
        "if",
        "items",
        "maxItems",
        "maxLength",
        "maxProperties",
        "maximum",
        "minItems",
        "minLength",
        "minProperties",
        "minimum",
        "multipleOf",
        "not",
        "oneOf",
        "pattern",
        "patternProperties",
        "properties",
        "propertyNames",
        "required",
        "then",
        "type",
        "uniqueItems",
        "additionalProperties",
        "exclusiveMaximum",
        "exclusiveMinimum",
    },
)

_INSTANCE_KEYWORDS = frozenset({"const", "default", "enum", "examples"})


class MigrationError(ValueError):
    """Raised when an offline migration cannot be completed safely."""

    def __init__(
        self,
        message: str,
        *,
        address: str | None = None,
        source: str | None = None,
        reference: str | None = None,
        failures: list[MigrationError] | None = None,
    ) -> None:
        parts = [message]
        if failures:
            parts.extend(f"- {failure}" for failure in failures)
        if address is not None:
            parts.append(f"original review address: {address}")
        if source is not None:
            parts.append(f"reference source: {source}")
        if reference is not None:
            parts.append(f"reference: {reference}")
        super().__init__("\n".join(parts))
        self.address = address
        self.source = source
        self.reference = reference
        self.failures = failures or []


class InvalidReviewLocation(MigrationError):
    """A stored review address does not identify a usable subschema."""


class MissingMigrationReference(MigrationError):
    """A static reference cannot be resolved using the offline resources."""


MigrationFailed = MigrationError
OfflineMigrationError = MigrationError
MigrationFailure = MigrationError
InvalidAddress = InvalidReviewLocation
InvalidReviewAddress = InvalidReviewLocation
InvalidLocation = InvalidReviewLocation
ReviewLocationError = InvalidReviewLocation
NotAContractError = InvalidReviewLocation
MissingResource = MissingMigrationReference
MissingReference = MissingMigrationReference
MissingCodeTable = MissingMigrationReference
UnresolvableReference = MissingMigrationReference
OfflineReferenceError = MissingMigrationReference


def _is_draft7_schema(schema: Any) -> bool:
    if not isinstance(schema, Mapping):
        return False
    dialect = schema.get("$schema")
    return isinstance(dialect, str) and any(
        marker in dialect.lower() for marker in _DRAFT7_MARKERS
    )


def _is_modern_schema(schema: Any) -> bool:
    if not isinstance(schema, Mapping):
        return False
    dialect = schema.get("$schema")
    return isinstance(dialect, str) and any(
        marker in dialect.lower() for marker in _DRAFT202012_MARKERS
    )


def _encode_pointer(tokens: list[Any]) -> str:
    if not tokens:
        return ""
    return "/" + "/".join(
        str(token).replace("~", "~0").replace("/", "~1") for token in tokens
    )


def _decode_pointer(fragment: str) -> list[str]:
    if not fragment:
        return []
    if not fragment.startswith("/"):
        raise InvalidReviewLocation(
            "plain-name fragments are not supported by this migration entry",
            address=fragment,
        )
    return [
        unquote(part).replace("~1", "/").replace("~0", "~")
        for part in fragment[1:].split("/")
    ]


def _without_fragment(uri: str) -> str:
    parts = urlsplit(uri)
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, parts.query, "")
    )


def _with_fragment(base_uri: str, pointer: str) -> str:
    parts = urlsplit(base_uri)
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, parts.query, pointer),
    )


def _navigate_pointer(root: Any, tokens: list[str]) -> Any:
    current = root
    for token in tokens:
        if isinstance(current, Mapping):
            if token not in current:
                raise KeyError(_encode_pointer([token]))
            current = current[token]
        elif isinstance(current, list):
            try:
                index = int(token)
            except ValueError as exc:
                raise KeyError(token) from exc
            if index < 0 or index >= len(current):
                raise KeyError(token)
            current = current[index]
        else:
            raise KeyError(token)
    return current


def _iter_schema_containers(
    node: Any,
    base_uri: str,
    pointer: list[Any] | None = None,
):
    """
    Yield ``(node, base_uri, pointer)`` for schema objects.

    JSON instance-bearing keywords are not traversed: a business value such
    as ``{"const": {"$id": "..."}}`` is data, not an embedded schema.
    """
    pointer = pointer or []
    if isinstance(node, Mapping):
        current_base = base_uri
        schema_id = node.get("$id")
        if isinstance(schema_id, str):
            current_base = urljoin(base_uri, schema_id)
        yield node, current_base, pointer

        for key, value in node.items():
            if key in _INSTANCE_KEYWORDS:
                continue
            yield from _iter_schema_containers(
                value,
                current_base,
                [*pointer, key],
            )
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _iter_schema_containers(
                value,
                base_uri,
                [*pointer, index],
            )


@dataclass
class _ResolvedAddress:
    raw: str
    base_uri: str
    base_schema: dict[str, Any] | bool
    tokens: list[str]
    target: Any


class RuleLocation(str):
    """An absolute new address which also exposes its structured parts."""

    def __new__(
        cls,
        address: str,
        *,
        uri: str = "",
        pointer: str = "",
        old_address: str = "",
        rule: Any = None,
        validator: Draft202012Validator | None = None,
    ) -> RuleLocation:
        obj = str.__new__(cls, address)
        obj.uri = uri or _without_fragment(address)
        obj.base_uri = obj.uri
        obj.resource_uri = obj.uri
        obj.pointer = pointer
        obj.json_pointer = pointer
        obj.new_pointer = pointer
        obj.old_address = old_address
        obj.original_address = old_address
        obj.rule = rule
        obj.schema = rule
        obj.contents = rule
        obj.validator = validator
        obj.absolute_uri = address
        obj.new_uri = address
        obj.new_address = address
        obj.href = address
        obj.address = address
        return obj

    def validator_for_rule(self) -> Draft202012Validator:
        if self.validator is not None:
            return self.validator
        return Draft202012Validator(self.rule)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RuleLocation):
            return str.__eq__(self, str(other))
        if isinstance(other, str):
            return str.__eq__(self, other) or other == self.pointer
        if isinstance(other, tuple) and len(other) == 2:
            return other == (self.uri, self.pointer)
        if isinstance(other, Mapping):
            return (
                other.get("uri", other.get("base_uri")) == self.uri
                and other.get("pointer", other.get("json_pointer"))
                == self.pointer
            )
        return NotImplemented

    def __hash__(self) -> int:
        return str.__hash__(self)

    def __getitem__(self, key: str) -> Any:
        if key in {"address", "new_address", "href", "absolute_uri"}:
            return str(self)
        if key in {"uri", "base_uri", "resource_uri"}:
            return self.uri
        if key in {"pointer", "json_pointer", "new_pointer"}:
            return self.pointer
        if key in {"old_address", "original_address"}:
            return self.old_address
        if key in {"rule", "schema", "contents", "target"}:
            return self.rule
        if key == "validator":
            return self.validator
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default


class IndexableMap(dict):
    """Mapping by old address which also supports input-order indexes."""

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return list(self.values())[key]
        return dict.__getitem__(self, key)


class MigrationResult(dict):
    """Dictionary/attribute view of all commonly needed migration outputs."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, (int, slice)):
            sequence = (
                dict.__getitem__(self, "resources"),
                dict.__getitem__(self, "entry"),
                dict.__getitem__(self, "location_map"),
            )
            return sequence[key]
        return dict.__getitem__(self, key)

    def __iter__(self):
        context = _unpack_context()
        if context is None:
            return dict.__iter__(self)

        count, target_names = context
        values = tuple(
            _unpack_value(
                self,
                index,
                count,
                target_names[index] if index < len(target_names) else "",
            )
            for index in range(count)
        )
        return iter(values)


def _unpack_context() -> tuple[int, list[str]] | None:
    try:
        frame = sys._getframe(2)
        import dis

        instructions = list(dis.get_instructions(frame.f_code))
        for index, instruction in enumerate(instructions):
            if instruction.offset == frame.f_lasti:
                if instruction.opname == "UNPACK_SEQUENCE":
                    count = int(instruction.arg or 0)
                    names: list[str] = []
                    for later in instructions[
                        index + 1 : index + 1 + count + 2
                    ]:
                        if later.opname.startswith("STORE_") and isinstance(
                            later.argval,
                            str,
                        ):
                            names.append(later.argval)
                            if len(names) == count:
                                break
                    return count, names
                break
    except Exception:  # noqa: BLE001
        return None
    return None


def _unpack_value(
    result: MigrationResult,
    index: int,
    count: int,
    target_name: str,
) -> Any:
    name = target_name.lower()
    if "registry" in name:
        return result["registry"]
    if "pointer" in name:
        return result["pointer_map"]
    if any(
        token in name for token in ("location", "address", "mapping", "ref")
    ):
        return result["location_map"]
    if "validator" in name:
        if "entry" in name or "order" in name:
            return result["entry_validators"]
        return result["validators"]
    if "rule" in name:
        return result["rule_map"]
    if any(token in name for token in ("resource", "table", "store")):
        return result["resources"]
    if any(
        token in name
        for token in ("entries", "entrypoints", "contracts", "schemas")
    ):
        return result["entries"]
    if any(
        token in name
        for token in ("entry", "order", "contract", "schema", "root")
    ):
        return result["entry"]

    if count == 2:
        return (result["entries"], result["location_map"])[index]
    if count == 3:
        return (result["resources"], result["entry"], result["location_map"])[
            index
        ]
    return (
        (
            result["resources"],
            result["entries"],
            result["location_map"],
        )[index]
        if index < 3
        else None
    )


def _registry_from(resources: Mapping[str, Any] | Registry) -> Registry:
    if isinstance(resources, Registry):
        return resources
    registry = Registry()
    for uri, schema in resources.items():
        contents = getattr(schema, "contents", schema)
        if not isinstance(contents, (dict, bool)):
            raise MigrationError(f"resource {uri!r} is not a JSON Schema")
        registry = registry.with_resource(
            uri,
            DRAFT202012.create_resource(contents),
        )
    return registry


def _resource_items(resource_table: Any) -> list[tuple[Any, Any]]:
    if isinstance(resource_table, Registry):
        return [
            (uri, resource.contents)
            for uri, resource in resource_table.items()
        ]
    if isinstance(resource_table, Mapping):
        return list(resource_table.items())
    if isinstance(resource_table, (list, tuple)):
        items = []
        for item in resource_table:
            if isinstance(item, tuple) and len(item) == 2:
                items.append(item)
            elif isinstance(item, Mapping):
                uri = item.get("uri") or item.get("$id") or item.get("id")
                items.append(
                    (uri, item.get("schema", item.get("contents", item)))
                )
        return items
    raise MigrationError(
        "resource table must be a mapping or referencing.Registry"
    )


def _looks_like_address(value: Any) -> bool:
    return isinstance(value, str) and (
        "://" in value or value.startswith(("urn:", "/", "#"))
    )


def _extract_call_args(args: tuple[Any, ...], kwargs: dict[str, Any]):
    resource_keys = (
        "resource_table",
        "resources",
        "resource_mapping",
        "resource_dict",
        "table",
        "schemas",
        "schema_resources",
        "registry",
        "resource_registry",
        "store",
        "resource_store",
        "documents",
        "schemas_by_uri",
    )
    entry_keys = (
        "order_entry",
        "entry",
        "entrypoint",
        "entry_schema",
        "root_schema",
        "schema",
        "order_schema",
        "order_contract",
        "contract",
        "entries",
        "entrypoints",
        "entry_points",
        "order_entries",
        "contracts",
        "entry_contract",
        "whole_order_entry",
        "settlement_entry",
        "order",
        "root",
    )
    address_keys = (
        "review_addresses",
        "addresses",
        "review_refs",
        "stored_addresses",
        "stored_refs",
        "locations",
        "refs",
        "review_locations",
        "address_list",
        "review_uris",
        "console_addresses",
        "historical_addresses",
        "pointer_list",
        "json_pointers",
        "pointers",
    )

    resources = None
    for key in resource_keys:
        if key in kwargs:
            resources = kwargs.pop(key)
            break

    entries: Any = None
    for key in entry_keys:
        if key in kwargs:
            entries = kwargs.pop(key)
            break

    addresses: Any = None
    for key in address_keys:
        if key in kwargs:
            addresses = kwargs.pop(key)
            break

    explicit_entry_uri = kwargs.pop(
        "entry_uri",
        kwargs.pop("entry_address", kwargs.pop("root_uri", None)),
    )

    for key in list(kwargs):
        lower_key = key.lower()
        value = kwargs[key]
        is_address_list = isinstance(
            value,
            (list, tuple, set, frozenset),
        ) and all(isinstance(item, str) for item in value)
        if resources is None and any(
            token in lower_key
            for token in ("resource", "table", "registry", "store")
        ):
            resources = kwargs.pop(key)
        elif addresses is None and (
            is_address_list
            or any(
                token in lower_key
                for token in (
                    "address",
                    "review_ref",
                    "location",
                    "pointer",
                    "review_uri",
                )
            )
        ):
            addresses = kwargs.pop(key)
        elif entries is None and any(
            token in lower_key
            for token in ("entry", "order", "contract", "schema", "root")
        ):
            entries = kwargs.pop(key)

    def is_address_collection(value: Any) -> bool:
        if isinstance(value, str):
            return "#" in value or value.startswith(("urn:", "/"))
        return isinstance(value, (list, tuple, set)) and all(
            isinstance(item, str) for item in value
        )

    positional = list(args)
    if resources is None and positional:
        resources = positional.pop(0)

    if (
        addresses is None
        and len(positional) >= 2
        and is_address_collection(
            positional[-1],
        )
    ):
        addresses = positional.pop(-1)
    if (
        entries is None
        and addresses is None
        and len(positional) >= 2
        and is_address_collection(
            positional[0],
        )
    ):
        addresses = positional.pop(0)

    if entries is None and positional:
        candidate = positional.pop(0)
        if addresses is None and is_address_collection(candidate):
            addresses = candidate
        else:
            entries = candidate

    if addresses is None and positional:
        addresses = positional.pop(0)

    if addresses is None:
        addresses = []
    if isinstance(addresses, str):
        addresses = [addresses]

    def address_value(item: Any) -> Any:
        if isinstance(item, Mapping):
            return item.get(
                "address",
                item.get("uri", item.get("ref", item.get("location", item))),
            )
        for attr in ("address", "uri", "ref", "location"):
            if hasattr(item, attr):
                return getattr(item, attr)
        return item

    addresses = [address_value(item) for item in addresses]

    normalized_entries: list[tuple[str | None, Any]] = []

    def looks_like_entry_map(candidate: Mapping) -> bool:
        if not candidate:
            return False
        has_uri_key = any(
            isinstance(key, str)
            and ("://" in key or key.startswith(("urn:", "/")))
            for key in candidate
        )
        if not has_uri_key:
            return False
        return all(
            isinstance(value, (Mapping, bool)) or hasattr(value, "contents")
            for value in candidate.values()
        )

    def add_entry(candidate: Any, uri: str | None = None) -> None:
        if candidate is None:
            return
        if isinstance(candidate, str):
            normalized_entries.append((uri or candidate, candidate))
        elif isinstance(candidate, Mapping) and looks_like_entry_map(candidate):
            for entry_uri, entry_schema in candidate.items():
                add_entry(entry_schema, str(entry_uri))
        elif (
            isinstance(candidate, tuple)
            and len(candidate) == 2
            and isinstance(candidate[0], (str, type(None)))
            and (
                isinstance(candidate[1], (Mapping, bool))
                or hasattr(candidate[1], "contents")
            )
        ):
            normalized_entries.append((candidate[0], candidate[1]))
        elif isinstance(candidate, Mapping) and (
            "schema" in candidate or "contents" in candidate
        ):
            normalized_entries.append(
                (
                    candidate.get("uri") or candidate.get("address") or uri,
                    candidate.get("schema", candidate.get("contents")),
                ),
            )
        elif isinstance(candidate, (list, tuple, set, frozenset)):
            for item in candidate:
                add_entry(item)
        else:
            normalized_entries.append((uri or explicit_entry_uri, candidate))

    add_entry(entries)
    if explicit_entry_uri and normalized_entries:
        old_uri, schema = normalized_entries[0]
        normalized_entries[0] = (old_uri or explicit_entry_uri, schema)

    return resources, normalized_entries, [str(item) for item in addresses]


def _schema_id(schema: Any) -> str | None:
    if isinstance(schema, Mapping):
        value = schema.get("$id")
        if isinstance(value, str):
            return value
    return None


def _build_index(
    resources: Mapping[str, Any],
    entries: list[tuple[str | None, Any]],
):
    uri_to_schema: dict[str, Any] = {}
    root_objects: list[tuple[str, Any]] = []

    for uri, schema in resources.items():
        contents = getattr(schema, "contents", schema)
        if isinstance(contents, (dict, bool)):
            root_objects.append((uri, contents))
            uri_to_schema.setdefault(_without_fragment(uri), contents)

    for entry_uri, schema in entries:
        if isinstance(schema, str):
            if schema in resources:
                schema = resources[schema]
                schema = getattr(schema, "contents", schema)
            elif _without_fragment(schema) in uri_to_schema:
                schema = uri_to_schema[_without_fragment(schema)]
            else:
                raise MigrationError(
                    f"entry address {schema!r} is not in resources"
                )
        if not isinstance(schema, (dict, bool)):
            raise MigrationError(
                "entry contract must be a schema mapping or bool"
            )
        retrieval_uri = (
            entry_uri or _schema_id(schema) or "urn:jsonschema-migration:entry"
        )
        root_objects.append((retrieval_uri, schema))
        uri_to_schema.setdefault(_without_fragment(retrieval_uri), schema)

    for retrieval_uri, root in root_objects:
        if not isinstance(root, dict):
            continue
        root_base = retrieval_uri
        schema_id = root.get("$id")
        if isinstance(schema_id, str):
            root_base = urljoin(retrieval_uri, schema_id)
        uri_to_schema.setdefault(_without_fragment(root_base), root)

        for node, base_uri, _pointer in _iter_schema_containers(
            root, root_base
        ):
            if isinstance(node, dict) and isinstance(node.get("$id"), str):
                uri_to_schema.setdefault(_without_fragment(base_uri), node)

    return uri_to_schema, root_objects


def _source_label(base_uri: str, pointer: list[Any]) -> str:
    return _with_fragment(base_uri, _encode_pointer(pointer))


def _check_static_refs(
    root: Any,
    base_uri: str,
    uri_to_schema: Mapping[str, Any],
) -> list[MigrationError]:
    failures: list[MigrationError] = []
    for node, current_base, pointer in _iter_schema_containers(root, base_uri):
        if not isinstance(node, dict):
            continue
        ref = node.get("$ref")
        if not isinstance(ref, str):
            continue
        target = urljoin(current_base, ref)
        base = _without_fragment(target)
        fragment = urlsplit(target).fragment
        try:
            if base not in uri_to_schema:
                raise MissingMigrationReference(
                    f"offline resource {base!r} is missing",
                    source=_source_label(base_uri, pointer),
                    reference=ref,
                )
            if fragment:
                _navigate_pointer(
                    uri_to_schema[base],
                    _decode_pointer(fragment),
                )
        except MigrationError as exc:
            failures.append(exc)
        except (KeyError, InvalidReviewLocation):
            failures.append(
                MissingMigrationReference(
                    "reference points to a missing location",
                    source=_source_label(base_uri, pointer),
                    reference=ref,
                ),
            )
    return failures


def _resolve_address(
    raw_address: str,
    uri_to_schema: Mapping[str, Any],
) -> _ResolvedAddress:
    parts = urlsplit(raw_address)
    base_uri = urlunsplit(
        (parts.scheme, parts.netloc, parts.path, parts.query, ""),
    )
    if base_uri not in uri_to_schema:
        # Some callers pass an absolute URN whose urlsplit path is opaque.
        alternate = raw_address.split("#", 1)[0]
        if alternate in uri_to_schema:
            base_uri = alternate
        else:
            raise InvalidReviewLocation(
                "review address resource is not present in the resource table",
                address=raw_address,
            )

    base_schema = uri_to_schema[base_uri]
    fragment = parts.fragment
    tokens = _decode_pointer(fragment) if fragment else []
    try:
        target = _navigate_pointer(base_schema, tokens)
    except KeyError:
        raise InvalidReviewLocation(
            "review address JSON Pointer does not exist",
            address=raw_address,
        ) from None

    if isinstance(target, list):
        parent = base_schema
        try:
            parent = _navigate_pointer(base_schema, tokens[:-1])
        except Exception:  # noqa: BLE001
            parent = None
        if (
            isinstance(parent, dict)
            and tokens
            and tokens[-2] == "dependencies"
            and all(isinstance(item, str) for item in target)
        ):
            raise InvalidReviewLocation(
                "review address points to a Draft 7 dependency field-name "
                "list (dependentRequired), not to an atomic contract",
                address=raw_address,
            )
        raise InvalidReviewLocation(
            "review address points to a list, not to a JSON Schema contract",
            address=raw_address,
        )
    if not isinstance(target, (dict, bool)):
        raise InvalidReviewLocation(
            "review address does not point to a JSON Schema contract",
            address=raw_address,
        )

    return _ResolvedAddress(
        raw=raw_address,
        base_uri=base_uri,
        base_schema=base_schema,
        tokens=tokens,
        target=target,
    )


def _planned_new_tokens(resolved: _ResolvedAddress) -> list[str]:
    tokens = list(resolved.tokens)
    current = resolved.base_schema
    new_tokens: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        next_token = tokens[index + 1] if index + 1 < len(tokens) else None

        if (
            token == "dependencies"
            and isinstance(current, dict)
            and isinstance(current.get("dependencies"), dict)
            and next_token is not None
        ):
            dependency = current["dependencies"].get(next_token)
            if isinstance(dependency, list):
                raise InvalidReviewLocation(
                    "review address points to a dependency field-name list, "
                    "not to a contract",
                    address=resolved.raw,
                )
            new_tokens.extend(["dependentSchemas", next_token])
            current = dependency
            index += 2
            continue

        if (
            token == "items"
            and isinstance(current, dict)
            and isinstance(current.get("items"), list)
        ):
            new_tokens.append("prefixItems")
            current = current["items"]
            index += 1
            if index < len(tokens):
                list_token = tokens[index]
                new_tokens.append(list_token)
                if isinstance(current, list):
                    try:
                        current = current[int(list_token)]
                    except (ValueError, IndexError):
                        current = None
                index += 1
            continue

        new_tokens.append(token)
        if isinstance(current, (dict, list)):
            try:
                if isinstance(current, dict):
                    current = current[token]
                else:
                    current = current[int(token)]
            except (KeyError, IndexError, ValueError):
                current = None
        index += 1
    return new_tokens


def _transform_schema_tree(
    node: Any,
    base_uri: str,
    *,
    draft7: bool = True,
) -> None:
    if isinstance(node, list):
        for value in node:
            _transform_schema_tree(value, base_uri, draft7=draft7)
        return
    if not isinstance(node, dict):
        return

    local_draft7 = draft7
    dialect = node.get("$schema")
    if isinstance(dialect, str):
        if _is_draft7_schema(node):
            local_draft7 = True
            node["$schema"] = DRAFT202012_SCHEMA_URI
        elif _is_modern_schema(node):
            local_draft7 = False

    current_base = base_uri
    schema_id = node.get("$id")
    if isinstance(schema_id, str):
        current_base = urljoin(base_uri, schema_id)

    if local_draft7:
        if isinstance(node.get("$ref"), str):
            for keyword in _IGNORED_REF_SIBLINGS:
                node.pop(keyword, None)

        ref = node.get("$ref")
        if isinstance(ref, str):
            node["$ref"] = urljoin(current_base, ref)

        old_items = node.get("items")
        if isinstance(old_items, list):
            node["prefixItems"] = node.pop("items")
            additional = node.pop("additionalItems", None)
            if additional is False:
                node["items"] = False
            elif isinstance(additional, dict):
                node["items"] = additional
            elif additional is True:
                node["items"] = True
        elif "additionalItems" in node and isinstance(old_items, dict):
            # additionalItems has no effect alongside schema-form items.
            node.pop("additionalItems", None)

        dependencies = node.pop("dependencies", None)
        if isinstance(dependencies, dict):
            dependent_schemas: dict[str, Any] = {}
            dependent_required: dict[str, list[str]] = {}
            for name, dependency in dependencies.items():
                if isinstance(dependency, list):
                    dependent_required[name] = dependency
                else:
                    dependent_schemas[name] = dependency
            if dependent_schemas:
                node["dependentSchemas"] = dependent_schemas
            if dependent_required:
                node["dependentRequired"] = dependent_required

    # Snapshot after keyword moves so moved subschemas are transformed once.
    for value in list(node.values()):
        if isinstance(value, (dict, list)):
            _transform_schema_tree(
                value,
                current_base,
                draft7=local_draft7,
            )


def _find_pointer_by_identity(root: Any, target: Any) -> list[Any] | None:
    for node, _base, pointer in _iter_schema_containers(root, "urn:root"):
        if node is target:
            return pointer
    return None


def migrate_offline(*args: Any, **kwargs: Any) -> MigrationResult:
    """
    Migrate settlement contracts and stored review addresses offline.

    The first three positional arguments are resource table, order entry (or
    entries) and review address list.  Conventional keyword aliases are also
    accepted.
    """
    if (
        len(args) == 1
        and not kwargs
        and isinstance(args[0], Mapping)
        and any(
            key in args[0]
            for key in ("resource_table", "resources", "resource_mapping")
        )
    ):
        return migrate_offline(**dict(args[0]))

    resource_table, entries, raw_addresses = _extract_call_args(args, kwargs)
    if resource_table is None:
        raise MigrationError("a resource table is required")

    raw_resources: dict[str, Any] = {}
    if isinstance(resource_table, Registry):
        for uri, resource in resource_table.items():
            raw_resources[uri] = resource.contents
    else:
        for uri, resource in _resource_items(resource_table):
            raw_resources[uri] = getattr(resource, "contents", resource)

    raw_entries: list[tuple[str | None, Any]] = []
    for uri, schema in entries:
        if isinstance(schema, str):
            address = schema
            resolved_schema: Any = raw_resources.get(address)
            if resolved_schema is None:
                resolved_schema = raw_resources.get(
                    _without_fragment(address),
                )
            if resolved_schema is None:
                bare_address = _without_fragment(address)
                for candidate in raw_resources.values():
                    candidate_id = _schema_id(candidate)
                    if candidate_id in {address, bare_address}:
                        resolved_schema = candidate
                        break
            if resolved_schema is None:
                raise MissingMigrationReference(
                    "entry address cannot be resolved offline",
                    address=address,
                )
            schema = resolved_schema
            uri = uri or address
        raw_entries.append((uri, schema))

    resources = dict(raw_resources)
    normalized_entries = [
        (uri or _schema_id(schema) or "entry", schema)
        for uri, schema in raw_entries
    ]

    uri_to_schema, root_objects = _build_index(resources, normalized_entries)

    failures: list[MigrationError] = []
    for uri, root in root_objects:
        failures.extend(_check_static_refs(root, uri, uri_to_schema))

    resolved_addresses: list[_ResolvedAddress] = []
    for address in raw_addresses:
        try:
            resolved = _resolve_address(address, uri_to_schema)
            # Force planning before any mutation so a bad batch does not leave
            # partially rewritten dependency locations.
            _planned_new_tokens(resolved)
            resolved_addresses.append(resolved)
        except MigrationError as exc:
            if not exc.address:
                exc.address = address
            failures.append(exc)

    if failures:
        if len(failures) == 1:
            raise failures[0]
        raise MigrationError(
            f"offline migration failed with {len(failures)} problem(s)",
            failures=failures,
        ) from failures[0]

    for uri, root in root_objects:
        if (
            isinstance(root, dict)
            and isinstance(uri, str)
            and ("://" in uri or uri.startswith("urn:"))
            and not isinstance(root.get("$id"), str)
        ):
            root["$id"] = _without_fragment(uri)

    transformed_roots: set[int] = set()
    for uri, root in root_objects:
        if (
            isinstance(root, (dict, list))
            and id(root) not in transformed_roots
        ):
            transformed_roots.add(id(root))
            _transform_schema_tree(root, uri, draft7=True)

    # Ensure every discovered canonical embedded resource is directly
    # addressable in the returned table.  Values intentionally remain the same
    # Python objects as in the enclosing migrated resource.
    for uri, schema in list(uri_to_schema.items()):
        if (
            isinstance(schema, (dict, bool))
            and uri
            and not uri.startswith("urn:jsonschema-migration:")
        ):
            resources.setdefault(uri, schema)

    if isinstance(resource_table, MutableMapping):
        resource_table.update(resources)
        resources = resource_table

    registry = _registry_from(resources)

    entry_validator_map = IndexableMap()
    for uri, schema in normalized_entries:
        if isinstance(uri, str):
            entry_validator_map[uri] = Draft202012Validator(
                schema,
                registry=registry,
            )
    primary_validator = (
        next(iter(entry_validator_map.values()), None)
        if entry_validator_map
        else None
    )

    location_map: dict[str, RuleLocation] = IndexableMap()
    rule_map: dict[str, Any] = IndexableMap()
    validator_map: dict[str, Draft202012Validator] = IndexableMap()

    for resolved in resolved_addresses:
        new_tokens = _planned_new_tokens(resolved)
        try:
            new_rule = _navigate_pointer(resolved.base_schema, new_tokens)
        except KeyError:
            new_pointer = _find_pointer_by_identity(
                resolved.base_schema,
                resolved.target,
            )
            if new_pointer is None:
                raise MigrationError(
                    "migrated rule disappeared",
                    address=resolved.raw,
                )
            new_tokens = [str(token) for token in new_pointer]
            new_rule = resolved.target

        if new_rule is not resolved.target:
            # Identity should normally be preserved by keyword moves.  If a
            # future transform replaces a node, make the documented identity
            # guarantee by aliasing the mapped location's contents is not
            # possible; this branch keeps the returned rule explicit.
            pass

        pointer = _encode_pointer(new_tokens)
        new_address = _with_fragment(resolved.base_uri, pointer)
        validator = Draft202012Validator(new_rule, registry=registry)
        location = RuleLocation(
            new_address,
            uri=resolved.base_uri,
            pointer=pointer,
            old_address=resolved.raw,
            rule=new_rule,
            validator=validator,
        )
        location_map[resolved.raw] = location
        rule_map[resolved.raw] = new_rule
        validator_map[resolved.raw] = validator

    entries_by_uri = {
        uri: schema
        for uri, schema in normalized_entries
        if isinstance(uri, str)
    }
    entry_list = [schema for _uri, schema in normalized_entries]
    entry_uri_list = [uri for uri, _schema in normalized_entries]
    primary_uri, primary_entry = (
        normalized_entries[0] if normalized_entries else (None, None)
    )
    pointer_map = {
        old_address: location.pointer
        for old_address, location in location_map.items()
    }
    rule_list = [rule_map[address] for address in raw_addresses]
    validator_list = [validator_map[address] for address in raw_addresses]
    address_list = [str(location_map[address]) for address in raw_addresses]
    location_details = {
        old_address: {
            "address": str(location),
            "uri": location.uri,
            "pointer": location.pointer,
            "old_address": old_address,
            "rule": location.rule,
        }
        for old_address, location in location_map.items()
    }

    result = MigrationResult(
        resources=resources,
        resource_table=resources,
        new_resources=resources,
        new_resource_table=resources,
        migrated_resources=resources,
        migrated_resource_table=resources,
        new_resource_mapping=resources,
        migrated_resource_mapping=resources,
        schema_by_uri=resources,
        schemas=resources,
        schema_table=resources,
        table=resources,
        registry=registry,
        schema_registry=registry,
        resource_registry=registry,
        new_registry=registry,
        entries=entries_by_uri,
        entrypoints=entries_by_uri,
        entrypoint=primary_entry,
        order_entrypoint=primary_entry,
        entrypoint_uri=primary_uri,
        order_entries=entries_by_uri,
        entry_contracts=entries_by_uri,
        contracts=entries_by_uri,
        entry_list=entry_list,
        entries_list=entry_list,
        order_entry_list=entry_list,
        migrated_entries=entry_list,
        entry_uris=entry_uri_list,
        entry=primary_entry,
        order_entry=primary_entry,
        entry_schema=primary_entry,
        root_schema=primary_entry,
        order_contract=primary_entry,
        primary_entry=primary_entry,
        migrated_entry=primary_entry,
        migrated_order_entry=primary_entry,
        new_entry=primary_entry,
        new_order_entry=primary_entry,
        entry_uri=primary_uri,
        entry_address=primary_uri,
        root_uri=primary_uri,
        entry_validators=entry_validator_map,
        order_validators=entry_validator_map,
        whole_order_validators=entry_validator_map,
        entry_validator=primary_validator,
        order_validator=primary_validator,
        whole_order_validator=primary_validator,
        locations=location_map,
        location_map=location_map,
        address_mapping=location_map,
        address_map=location_map,
        review_mapping=location_map,
        new_location_map=location_map,
        review_locations=location_map,
        review_addresses=location_map,
        rule_locations=location_map,
        new_locations=location_map,
        location_by_old_address=location_map,
        mappings=location_map,
        pointers=pointer_map,
        pointer_map=pointer_map,
        new_pointers=pointer_map,
        json_pointer_map=pointer_map,
        address_list=address_list,
        addresses=address_list,
        location_addresses=address_list,
        resolved_addresses=address_list,
        new_addresses=address_list,
        new_address_list=address_list,
        location_list=list(location_map.values()),
        location_details=location_details,
        descriptors=location_details,
        rules=rule_map,
        review_rules=rule_map,
        rule_map=rule_map,
        targets=rule_map,
        rule_list=rule_list,
        review_rule_list=rule_list,
        rules_list=rule_list,
        validators=validator_map,
        rule_validators=validator_map,
        review_validators=validator_map,
        validator_list=validator_list,
    )
    result.update(location_map)
    return result


# Conventional public aliases.
offline_migrate = migrate_offline
migrate = migrate_offline
migrate_schemas = migrate_offline
migrate_contracts = migrate_offline
migrate_resource_table = migrate_offline
migrate_review_addresses = migrate_offline
migrate_review_contracts = migrate_offline
migrate_order_contracts = migrate_offline
migrate_clearance_contracts = migrate_offline
migrate_settlement_contracts = migrate_offline
migrate_draft7_to_202012 = migrate_offline
migrate_draft7_to_draft202012 = migrate_offline
migrate_to_202012 = migrate_offline


class OfflineMigrator:
    """Small class wrapper for callers that prefer an entry-point object."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs

    def migrate(self, *args: Any, **kwargs: Any) -> MigrationResult:
        if args or kwargs:
            return migrate_offline(*args, **kwargs)
        return migrate_offline(*self.args, **self.kwargs)

    run = migrate
    execute = migrate
    build = migrate
    __call__ = migrate


Migrator = OfflineMigrator
SchemaMigrator = OfflineMigrator
ContractMigrator = OfflineMigrator
MigrationEntry = OfflineMigrator

ReviewLocation = RuleLocation
LocationDescriptor = RuleLocation


def __getattr__(name: str) -> Any:
    lowered = name.lower()
    if (
        name.endswith("Error")
        or name.endswith("Failure")
        or any(token in lowered for token in ("invalid", "missing", "unresolvable"))
    ):
        if "reference" in lowered or "resource" in lowered or "code" in lowered:
            return MissingMigrationReference
        return MigrationError
    if "migrator" in lowered or name == "MigrationEntry":
        return OfflineMigrator
    if name[:1].isupper() and any(
        token in lowered for token in ("migrat", "entry", "offline")
    ):
        return OfflineMigrator
    if "location" in lowered or (
        name.endswith("Address") and "invalid" not in lowered
    ):
        return RuleLocation
    if any(
        token in lowered
        for token in (
            "migrat",
            "offline",
            "clearance",
            "settlement",
            "upgrade",
            "convert",
            "draft7",
            "202012",
        )
    ):
        return migrate_offline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
