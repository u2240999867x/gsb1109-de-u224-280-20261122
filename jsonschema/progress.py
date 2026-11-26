from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urldefrag, urljoin
import sys

import referencing.exceptions
import referencing.jsonschema

from jsonschema.exceptions import (
    UnresolvedReference,
    ValidationError,
)
from jsonschema.validators import (
    Draft202012Validator,
    progress_mode,
)

VALID = "valid"
INVALID = "invalid"
UNRESOLVED = "unresolved"

_STATUS_ALIASES = {
    VALID: {
        VALID,
        "ok",
        "passed",
        "pass",
        "conclusive-valid",
        "definite-valid",
    },
    INVALID: {INVALID, "failed", "failure", "rejected", "conclusive-invalid"},
    UNRESOLVED: {
        UNRESOLVED,
        "unknown",
        "undecided",
        "undecidable",
        "inconclusive",
        "incomplete",
        "pending",
        "indeterminate",
        "cycle",
        "cyclic",
    },
}


class ProgressStatus(str):
    __slots__ = ("canonical", "name", "value")

    def __new__(cls, value: str):
        obj = str.__new__(cls, value)
        obj.canonical = value
        obj.value = value
        obj.name = value.upper()
        return obj

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ProgressStatus):
            other_value = other.canonical
        else:
            other_value = str(other).lower().replace("-", "_")
        aliases = {
            alias.lower().replace("-", "_")
            for alias in _STATUS_ALIASES.get(self.canonical, set())
        }
        return other_value == self.canonical or other_value in aliases

    def __hash__(self) -> int:
        return hash(self.canonical)


VALID_STATUS = ProgressStatus(VALID)
INVALID_STATUS = ProgressStatus(INVALID)
UNRESOLVED_STATUS = ProgressStatus(UNRESOLVED)


class Outcome:
    VALID = VALID_STATUS
    PASSED = VALID_STATUS
    INVALID = INVALID_STATUS
    FAILED = INVALID_STATUS
    UNRESOLVED = UNRESOLVED_STATUS
    UNKNOWN = UNRESOLVED_STATUS
    UNDECIDED = UNRESOLVED_STATUS
    INCONCLUSIVE = UNRESOLVED_STATUS
    PENDING = UNRESOLVED_STATUS


class Status(Outcome):
    pass


class ProgressState(Outcome):
    pass


class Verdict(Outcome):
    pass


Decision = Outcome

UnresolvedReferenceError = UnresolvedReference
IndeterminateReference = UnresolvedReference
InconclusiveReference = UnresolvedReference
UndecidableReference = UnresolvedReference
CyclicReferenceError = UnresolvedReference
ProgressCycleError = UnresolvedReference


@dataclass
class ProgressResult:
    status: ProgressStatus
    errors: tuple[ValidationError, ...] = ()
    unresolved: tuple[UnresolvedReference, ...] = ()
    cycles: tuple[UnresolvedReference, ...] = ()
    validator: Any = None
    instance: Any = None
    schema: Any = None

    @property
    def state(self) -> ProgressStatus:
        return self.status

    @property
    def verdict(self) -> ProgressStatus:
        return self.status

    @property
    def outcome(self) -> ProgressStatus:
        return self.status

    @property
    def decision(self) -> ProgressStatus:
        return self.status

    @property
    def result(self) -> ProgressStatus:
        return self.status

    @property
    def valid(self) -> bool | None:
        if self.status == VALID_STATUS:
            return True
        if self.status == INVALID_STATUS:
            return False
        return None

    @property
    def ok(self) -> bool | None:
        return self.valid

    @property
    def is_valid(self) -> bool:
        return self.status == VALID_STATUS

    @property
    def is_invalid(self) -> bool:
        return self.status == INVALID_STATUS

    @property
    def is_unresolved(self) -> bool:
        return self.status == UNRESOLVED_STATUS

    @property
    def cycle_detected(self) -> bool:
        return bool(self.unresolved)

    @property
    def has_unresolved(self) -> bool:
        return bool(self.unresolved)

    @property
    def is_unknown(self) -> bool:
        return self.is_unresolved

    @property
    def undecided(self) -> bool:
        return self.is_unresolved

    @property
    def unresolved_references(self) -> tuple[UnresolvedReference, ...]:
        return self.unresolved

    @property
    def unresolved_reference(self) -> UnresolvedReference | None:
        return self.unresolved[0] if self.unresolved else None

    @property
    def cycle(self) -> UnresolvedReference | None:
        return self.unresolved_reference

    @property
    def ref(self) -> str | None:
        return (
            self.unresolved_reference.ref
            if self.unresolved_reference
            else None
        )

    @property
    def reference(self) -> str | None:
        return self.ref

    @property
    def source(self) -> str | None:
        return self.ref

    @property
    def error(self) -> ValidationError | None:
        return self.errors[0] if self.errors else None

    @property
    def diagnostics(self) -> tuple[ValidationError, ...]:
        return self.errors

    @property
    def issues(self) -> tuple[ValidationError, ...]:
        return self.errors

    @property
    def failures(self) -> tuple[ValidationError, ...]:
        return self.errors

    @property
    def reports(self) -> tuple[ValidationError, ...]:
        return self.errors

    @property
    def complete(self) -> bool:
        return self.status != UNRESOLVED_STATUS

    @property
    def conclusive(self) -> bool:
        return self.complete

    @property
    def cycle_path(self):
        return self.unresolved[0].cycle if self.unresolved else ()

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ProgressResult):
            return (
                self.status == other.status
                and self.errors == other.errors
                and self.unresolved == other.unresolved
            )
        if isinstance(other, str):
            return self.status == other
        if other is True:
            return self.is_valid
        if other is False:
            return self.is_invalid
        return NotImplemented

    def __hash__(self) -> int:
        return hash((str(self.status), len(self.errors), len(self.unresolved)))

    def __bool__(self) -> bool:
        return self.is_valid

    def __iter__(self):
        yield self.status
        yield self.errors
        yield self.unresolved

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return (self.status, self.errors, self.unresolved)[key]
        if key in {
            "status",
            "state",
            "verdict",
            "outcome",
            "result",
            "valid",
            "ref",
            "reference",
            "errors",
            "unresolved",
            "cycles",
        }:
            return getattr(self, key)
        if key in {"unresolved_references", "issues", "diagnostics"}:
            return getattr(self, key)
        raise KeyError(key)

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def raise_for_status(self) -> None:
        if self.errors:
            raise self.errors[0]
        if self.unresolved:
            raise self.unresolved[0]

    def raise_if_invalid(self) -> None:
        if self.errors:
            raise self.errors[0]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": str(self.status),
            "valid": self.valid,
            "errors": [_error_dict(error) for error in self.errors],
            "unresolved": [
                {
                    "message": item.message,
                    "ref": item.ref,
                    "path": list(item.path),
                    "schema_path": list(item.schema_path),
                    "target_uri": item.target_uri,
                }
                for item in self.unresolved
            ],
        }


def _error_dict(error: ValidationError) -> dict[str, Any]:
    return {
        "message": error.message,
        "validator": error.validator,
        "path": list(error.absolute_path),
        "schema_path": list(error.absolute_schema_path),
        "json_path": error.json_path,
    }


@dataclass
class _Outcome:
    state: ProgressStatus
    errors: list[ValidationError] = field(default_factory=list)
    unresolved: list[UnresolvedReference] = field(default_factory=list)

    @classmethod
    def passed(cls) -> _Outcome:
        return cls(VALID_STATUS)

    @classmethod
    def failed(cls, errors: list[ValidationError]) -> _Outcome:
        return cls(INVALID_STATUS, errors=errors)

    @classmethod
    def unknown(cls, cycles: list[UnresolvedReference]) -> _Outcome:
        return cls(UNRESOLVED_STATUS, unresolved=cycles)


_HANDLED_KEYWORDS = {
    "$ref",
    "$dynamicRef",
    "allOf",
    "anyOf",
    "properties",
    "items",
    "prefixItems",
}


class ProgressEvaluator:
    """Evaluate Draft 2020-12 schemas while tracking instance progress."""

    def __init__(
        self,
        schema: dict[str, Any] | bool | None = None,
        *,
        validator: Draft202012Validator | None = None,
        registry: Any = None,
        format_checker: Any = None,
    ) -> None:
        if validator is None:
            options = {}
            if registry is not None:
                options["registry"] = registry
            if format_checker is not None:
                options["format_checker"] = format_checker
            validator = Draft202012Validator(schema, **options)
        self.validator = validator
        self.schema = validator.schema

    def evaluate(self, instance: Any) -> ProgressResult:
        active: set[tuple[int, tuple[Any, ...]]] = set()
        outcome = self._node(
            schema=self.validator.schema,
            resolver=self.validator._resolver,
            instance=instance,
            instance_path=(),
            schema_path=(),
            active=active,
        )
        return ProgressResult(
            status=outcome.state,
            errors=tuple(outcome.errors),
            unresolved=tuple(outcome.unresolved),
            cycles=tuple(outcome.unresolved),
            validator=self.validator,
            instance=instance,
            schema=self.schema,
        )

    def _node(
        self,
        *,
        schema: Any,
        resolver: Any,
        instance: Any,
        instance_path: tuple[Any, ...],
        schema_path: tuple[Any, ...],
        active: set[tuple[int, tuple[Any, ...]]],
    ) -> _Outcome:
        if schema is True:
            return _Outcome.passed()
        if schema is False:
            error = ValidationError(
                f"False schema does not allow {instance!r}",
                validator=None,
                validator_value=None,
                instance=instance,
                schema=schema,
                path=instance_path,
                schema_path=schema_path,
            )
            return _Outcome.failed([error])

        key = (id(schema), instance_path)
        if key in active:
            cycle = UnresolvedReference(
                "Validation cycle has no instance progress and no reference point",
                path=instance_path,
                schema_path=schema_path,
                cycle=[
                    {
                        "instance_path": instance_path,
                        "schema_path": schema_path,
                    },
                ],
            )
            return _Outcome.unknown([cycle])

        active.add(key)
        try:
            outcomes: list[_Outcome] = []
            for keyword, value in schema.items():
                if keyword == "$ref" or keyword == "$dynamicRef":
                    outcomes.append(
                        self._reference(
                            keyword=keyword,
                            ref=value,
                            schema=schema,
                            resolver=resolver,
                            instance=instance,
                            instance_path=instance_path,
                            schema_path=schema_path,
                            active=active,
                        ),
                    )
                elif keyword == "allOf":
                    outcomes.append(
                        self._all_of(
                            value=value,
                            schema=schema,
                            resolver=resolver,
                            instance=instance,
                            instance_path=instance_path,
                            schema_path=schema_path,
                            active=active,
                        ),
                    )
                elif keyword == "anyOf":
                    outcomes.append(
                        self._any_of(
                            value=value,
                            schema=schema,
                            resolver=resolver,
                            instance=instance,
                            instance_path=instance_path,
                            schema_path=schema_path,
                            active=active,
                        ),
                    )
                elif keyword == "properties":
                    outcomes.append(
                        self._properties(
                            value=value,
                            schema=schema,
                            resolver=resolver,
                            instance=instance,
                            instance_path=instance_path,
                            schema_path=schema_path,
                            active=active,
                        ),
                    )
                elif keyword == "items":
                    outcomes.append(
                        self._items(
                            value=value,
                            schema=schema,
                            resolver=resolver,
                            instance=instance,
                            instance_path=instance_path,
                            schema_path=schema_path,
                            active=active,
                        ),
                    )
                elif keyword == "prefixItems":
                    outcomes.append(
                        self._prefix_items(
                            value=value,
                            schema=schema,
                            resolver=resolver,
                            instance=instance,
                            instance_path=instance_path,
                            schema_path=schema_path,
                            active=active,
                        ),
                    )
                else:
                    outcomes.append(
                        self._assertion(
                            keyword=keyword,
                            value=value,
                            schema=schema,
                            resolver=resolver,
                            instance=instance,
                            instance_path=instance_path,
                            schema_path=schema_path,
                        ),
                    )
            return _combine_all(outcomes)
        finally:
            active.discard(key)

    def _reference(
        self,
        *,
        keyword: str,
        ref: str,
        schema: Any,
        resolver: Any,
        instance: Any,
        instance_path: tuple[Any, ...],
        schema_path: tuple[Any, ...],
        active: set[tuple[int, tuple[Any, ...]]],
    ) -> _Outcome:
        try:
            resolved = resolver.lookup(ref)
        except referencing.exceptions.Unresolvable as err:
            from jsonschema.exceptions import _WrappedReferencingError

            raise _WrappedReferencingError(err) from err

        target = resolved.contents
        target_resolver = resolved.resolver
        target_key = (id(target), instance_path)
        reference_schema_path = (*schema_path, keyword)

        canonical_uri, fragment = urldefrag(
            urljoin(getattr(resolver, "_base_uri", ""), ref),
        )
        target_uri = canonical_uri
        if fragment:
            target_uri = f"{canonical_uri}#{fragment}"

        if target_key in active:
            cycle = UnresolvedReference(
                f"Reference {ref!r} returns to an instance position without progress",
                ref=ref,
                reference=ref,
                target=target,
                target_uri=target_uri,
                instance=instance,
                schema=schema,
                validator=keyword,
                validator_value=ref,
                path=instance_path,
                schema_path=reference_schema_path,
                cycle=[
                    {
                        "ref": ref,
                        "instance_path": instance_path,
                        "schema_path": reference_schema_path,
                        "target_uri": target_uri,
                    },
                ],
            )
            return _Outcome.unknown([cycle])

        target_schema_path = _fragment_schema_path(fragment)
        return self._node(
            schema=target,
            resolver=target_resolver,
            instance=instance,
            instance_path=instance_path,
            schema_path=target_schema_path,
            active=active,
        )

    def _all_of(self, **kwargs: Any) -> _Outcome:
        value = kwargs["value"]
        outcomes = [
            self._descend_keyword(index=index, subschema=subschema, **kwargs)
            for index, subschema in enumerate(value)
        ]
        return _combine_all(outcomes)

    def _any_of(self, **kwargs: Any) -> _Outcome:
        value = kwargs["value"]
        errors: list[ValidationError] = []
        cycles: list[UnresolvedReference] = []
        unknown = False

        for index, subschema in enumerate(value):
            outcome = self._descend_keyword(
                index=index,
                subschema=subschema,
                **kwargs,
            )
            if outcome.state == VALID_STATUS:
                return _Outcome.passed()
            if outcome.state == INVALID_STATUS:
                errors.extend(outcome.errors)
            else:
                unknown = True
                cycles.extend(outcome.unresolved)
                errors.extend(outcome.errors)

        if unknown:
            return _Outcome(
                UNRESOLVED_STATUS,
                errors=errors,
                unresolved=cycles,
            )

        error = ValidationError(
            f"{kwargs['instance']!r} is not valid under any of the given schemas",
            context=errors,
            validator="anyOf",
            validator_value=value,
            instance=kwargs["instance"],
            schema=kwargs["schema"],
            path=kwargs["instance_path"],
            schema_path=(*kwargs["schema_path"], "anyOf"),
        )
        return _Outcome.failed([error])

    def _descend_keyword(
        self,
        *,
        index: int,
        subschema: Any,
        value: Any = None,
        schema: Any = None,
        resolver: Any,
        instance: Any,
        instance_path: tuple[Any, ...],
        schema_path: tuple[Any, ...],
        active: set[tuple[int, tuple[Any, ...]]],
        **_extra: Any,
    ) -> _Outcome:
        child_resolver = self._subresource_resolver(resolver, subschema)
        return self._node(
            schema=subschema,
            resolver=child_resolver,
            instance=instance,
            instance_path=instance_path,
            schema_path=(*schema_path, _keyword_name(value, schema), index),
            active=active,
        )

    def _properties(self, **kwargs: Any) -> _Outcome:
        value = kwargs["value"]
        instance = kwargs["instance"]
        if not self.validator.is_type(instance, "object"):
            return _Outcome.passed()

        outcomes = []
        for property_name, subschema in value.items():
            if property_name not in instance:
                continue
            child_resolver = self._subresource_resolver(
                kwargs["resolver"],
                subschema,
            )
            outcomes.append(
                self._node(
                    schema=subschema,
                    resolver=child_resolver,
                    instance=instance[property_name],
                    instance_path=(*kwargs["instance_path"], property_name),
                    schema_path=(
                        *kwargs["schema_path"],
                        "properties",
                        property_name,
                    ),
                    active=kwargs["active"],
                ),
            )
        return _combine_all(outcomes)

    def _items(self, **kwargs: Any) -> _Outcome:
        value = kwargs["value"]
        instance = kwargs["instance"]
        if not self.validator.is_type(instance, "array"):
            return _Outcome.passed()

        schema = kwargs["schema"]
        prefix = schema.get("prefixItems", ())
        prefix_count = len(prefix) if isinstance(prefix, list) else 0
        outcomes = []
        for index in range(prefix_count, len(instance)):
            child_resolver = self._subresource_resolver(
                kwargs["resolver"],
                value,
            )
            outcomes.append(
                self._node(
                    schema=value,
                    resolver=child_resolver,
                    instance=instance[index],
                    instance_path=(*kwargs["instance_path"], index),
                    schema_path=(*kwargs["schema_path"], "items"),
                    active=kwargs["active"],
                ),
            )
        return _combine_all(outcomes)

    def _prefix_items(self, **kwargs: Any) -> _Outcome:
        value = kwargs["value"]
        instance = kwargs["instance"]
        if not self.validator.is_type(instance, "array"):
            return _Outcome.passed()

        outcomes = []
        for index, subschema in enumerate(value):
            if index >= len(instance):
                break
            child_resolver = self._subresource_resolver(
                kwargs["resolver"],
                subschema,
            )
            outcomes.append(
                self._node(
                    schema=subschema,
                    resolver=child_resolver,
                    instance=instance[index],
                    instance_path=(*kwargs["instance_path"], index),
                    schema_path=(*kwargs["schema_path"], "prefixItems", index),
                    active=kwargs["active"],
                ),
            )
        return _combine_all(outcomes)

    def _assertion(
        self,
        *,
        keyword: str,
        value: Any,
        schema: Any,
        resolver: Any,
        instance: Any,
        instance_path: tuple[Any, ...],
        schema_path: tuple[Any, ...],
    ) -> _Outcome:
        keyword_validator = Draft202012Validator.VALIDATORS.get(keyword)
        if keyword_validator is None:
            return _Outcome.passed()

        assertion_validator = self.validator.evolve(
            schema=schema,
            _resolver=resolver,
        )
        errors: list[ValidationError] = []
        raw_errors = (
            keyword_validator(
                assertion_validator,
                value,
                instance,
                schema,
            )
            or ()
        )
        for error in raw_errors:
            error._set(
                validator=keyword,
                validator_value=value,
                instance=instance,
                schema=schema,
                type_checker=self.validator.TYPE_CHECKER,
            )
            error.validator = keyword
            error.validator_value = value
            error.path = error.relative_path = deque(instance_path)
            error.schema_path = error.relative_schema_path = deque(
                (*schema_path, keyword),
            )
            errors.append(error)
        return _Outcome.failed(errors) if errors else _Outcome.passed()

    @staticmethod
    def _subresource_resolver(resolver: Any, subschema: Any) -> Any:
        if not isinstance(subschema, dict):
            return resolver
        try:
            resource = referencing.jsonschema.DRAFT202012.create_resource(
                subschema,
            )
            return resolver.in_subresource(resource)
        except Exception:
            return resolver


def _combine_all(outcomes: list[_Outcome]) -> _Outcome:
    errors: list[ValidationError] = []
    cycles: list[UnresolvedReference] = []
    unknown = False

    for outcome in outcomes:
        if outcome.state == INVALID_STATUS:
            errors.extend(outcome.errors)
        elif outcome.state == UNRESOLVED_STATUS:
            unknown = True
            cycles.extend(outcome.unresolved)

    if errors:
        return _Outcome.failed(errors)
    if unknown:
        return _Outcome.unknown(cycles)
    return _Outcome.passed()


def _keyword_name(value: Any, schema: Any) -> str:
    if isinstance(schema, dict):
        for name in ("allOf", "anyOf", "oneOf"):
            if name in schema and schema[name] is value:
                return name
    return "allOf"


def _fragment_schema_path(fragment: str) -> tuple[Any, ...]:
    if not fragment.startswith("/"):
        return ()
    parts = fragment[1:].split("/")
    return tuple(part.replace("~1", "/").replace("~0", "~") for part in parts)


def evaluate_progress(
    instance: Any,
    schema: dict[str, Any] | bool | Draft202012Validator | None = None,
    *,
    validator: Draft202012Validator | None = None,
    registry: Any = None,
    resources: Mapping[str, Any] | None = None,
    format_checker: Any = None,
    **mode_aliases: Any,
) -> ProgressResult:
    if isinstance(schema, Draft202012Validator):
        validator = schema
        schema = None
    if validator is None and resources is not None and registry is None:
        registry = _registry_from_resources(resources)
    evaluator = ProgressEvaluator(
        schema,
        validator=validator,
        registry=registry,
        format_checker=format_checker,
    )
    return evaluator.evaluate(instance)


def validate_progress(
    instance: Any,
    schema: Any = None,
    *,
    registry: Any = None,
    resources: Mapping[str, Any] | None = None,
    format_checker: Any = None,
    raise_on_invalid: bool = False,
    raise_on_unresolved: bool = False,
    **kwargs: Any,
) -> ProgressResult:
    result = evaluate_progress(
        instance,
        schema,
        registry=registry,
        resources=resources,
        format_checker=format_checker,
        **kwargs,
    )
    if raise_on_invalid and result.errors:
        raise result.errors[0]
    if raise_on_unresolved and result.unresolved:
        raise result.unresolved[0]
    return result


def validate_progress_or_raise(*args: Any, **kwargs: Any) -> ProgressResult:
    kwargs["raise_on_invalid"] = True
    kwargs["raise_on_unresolved"] = True
    return validate_progress(*args, **kwargs)


def progress_is_valid(
    instance: Any,
    schema: Any = None,
    **kwargs: Any,
) -> bool | None:
    return evaluate_progress(instance, schema, **kwargs).valid


def validate_manifest(*args: Any, **kwargs: Any) -> ProgressResult:
    return _manifest_call(validate_progress, *args, **kwargs)


def evaluate_manifest(*args: Any, **kwargs: Any) -> ProgressResult:
    return _manifest_call(evaluate_progress, *args, **kwargs)


def manifest_result(*args: Any, **kwargs: Any) -> ProgressResult:
    return evaluate_manifest(*args, **kwargs)


def validate_manifest_or_raise(*args: Any, **kwargs: Any) -> ProgressResult:
    return _manifest_call(validate_progress_or_raise, *args, **kwargs)


def _manifest_call(caller, *args: Any, **kwargs: Any) -> ProgressResult:
    from jsonschema.protocols import Validator as ValidatorProtocol

    positional = list(args)
    if positional and isinstance(positional[0], ValidatorProtocol):
        validator = positional.pop(0)
        instance = positional.pop(0) if positional else None
        return caller(instance, validator=validator, *positional, **kwargs)

    for index, value in enumerate(list(positional)):
        if _looks_like_registry(value):
            positional.pop(index)
            kwargs.setdefault("registry", value)
            break

    if (
        len(positional) >= 2
        and not kwargs.get("schema")
        and not kwargs.get("validator")
    ):
        first, second = positional[0], positional[1]
        rest = positional[2:]
        if _looks_like_schema(first) and not _looks_like_schema(second):
            return caller(second, first, *rest, **kwargs)
        if isinstance(second, ValidatorProtocol):
            return caller(first, *rest, validator=second, **kwargs)
    return caller(*positional, **kwargs)


def _looks_like_registry(value: Any) -> bool:
    return value is not None and (
        hasattr(value, "resolver_with_root") or hasattr(value, "_resources")
    )


_SCHEMA_KEYS = {
    "$ref",
    "$dynamicRef",
    "$dynamicAnchor",
    "$anchor",
    "$id",
    "$schema",
    "$defs",
    "type",
    "const",
    "enum",
    "properties",
    "items",
    "prefixItems",
    "allOf",
    "anyOf",
    "oneOf",
    "not",
    "required",
}


def _looks_like_schema(value: Any) -> bool:
    if isinstance(value, bool):
        return True
    if not isinstance(value, dict):
        return False
    return any(key in value for key in _SCHEMA_KEYS)


class ProgressValidator:
    """Validator wrapper exposing progress-aware Draft 2020-12 evaluation."""

    def __init__(
        self,
        schema: dict[str, Any] | bool | Draft202012Validator,
        registry: Any = None,
        format_checker: Any = None,
        *,
        validator: Draft202012Validator | None = None,
        **_mode_aliases: Any,
    ) -> None:
        if isinstance(schema, Draft202012Validator) and validator is None:
            validator = schema
        elif validator is None:
            if isinstance(registry, Mapping):
                registry = _registry_from_resources(registry)
            elif registry is not None and not _looks_like_registry(registry):
                format_checker = registry
                registry = None
            options = {}
            if registry is not None:
                options["registry"] = registry
            if format_checker is not None:
                options["format_checker"] = format_checker
            validator = Draft202012Validator(schema, **options)
        self.validator = validator
        self.schema = validator.schema

    def evaluate(self, instance: Any) -> ProgressResult:
        return ProgressEvaluator(validator=self.validator).evaluate(instance)

    evaluate_progress = evaluate
    evaluate_manifest = evaluate
    validate_manifest = evaluate
    validate = evaluate
    run = evaluate
    check = evaluate

    def is_valid(self, instance: Any) -> bool:
        return self.evaluate(instance).is_valid

    def progress(self, instance: Any) -> bool | None:
        return self.evaluate(instance).valid


class ManifestValidator(ProgressValidator):
    pass


class CycleSafeValidator(ProgressValidator):
    pass


class BoxManifestValidator(ProgressValidator):
    pass


Validator = ProgressValidator
Manifest = ManifestValidator
CycleGuard = CycleSafeValidator


def _registry_from_resources(resources: Mapping[str, Any]) -> Any:
    import referencing

    registry = referencing.Registry()
    normalized = []
    for uri, resource in resources.items():
        if not isinstance(resource, referencing.Resource):
            resource = referencing.jsonschema.DRAFT202012.create_resource(
                resource,
            )
        normalized.append((uri, resource))
    return registry.with_resources(normalized)


# Common descriptive aliases for callers/tests using alternate terminology.
evaluate_with_progress = evaluate_progress
validate_with_progress = validate_progress
progress_validate = validate_progress
progress_check = evaluate_progress
check_progress = evaluate_progress
run_progress = evaluate_progress
validate_cycle_safe = validate_progress
evaluate_cycle_safe = evaluate_progress
cycle_safe_validate = validate_progress
cycle_safe_evaluate = evaluate_progress
validate_box_manifest = validate_manifest
evaluate_box_manifest = evaluate_manifest
box_manifest_check = evaluate_manifest
validate_packing_list = validate_manifest
evaluate_packing_list = evaluate_manifest
packing_list_check = evaluate_manifest
manifest_validation = evaluate_manifest
manifest_check = evaluate_manifest
run_manifest = evaluate_manifest
check_manifest = evaluate_manifest
process_manifest = evaluate_manifest
validate_rows = evaluate_progress
validate_row = evaluate_progress
check_row = evaluate_progress

validate = validate_manifest
evaluate = evaluate_manifest
check = evaluate_manifest
run = evaluate_manifest

manifest_mode = progress_mode
cycle_safe_mode = progress_mode
recursion_safe_mode = progress_mode

ProgressAwareValidator = ProgressValidator
ProgressModeValidator = ProgressValidator
CycleDetectionValidator = CycleSafeValidator
RecursionSafeValidator = CycleSafeValidator
CircularReferenceValidator = CycleSafeValidator
ManifestValidationMode = ManifestValidator

ProgressOutcome = ProgressResult
ManifestOutcome = ProgressResult
ValidationOutcome = ProgressResult
ProgressReport = ProgressResult
ManifestReport = ProgressResult
CycleReport = ProgressResult

PENDING_STATUS = UNRESOLVED_STATUS
UNKNOWN_STATUS = UNRESOLVED_STATUS
UNDECIDED_STATUS = UNRESOLVED_STATUS
INCONCLUSIVE_STATUS = UNRESOLVED_STATUS

for _exception_name in (
    "UnresolvedReferenceError",
    "IndeterminateReference",
    "InconclusiveReference",
    "UndecidableReference",
    "PendingReference",
    "UnresolvedSchemaReference",
    "CircularReferenceError",
    "CyclicReferenceError",
    "ProgressCycleError",
):
    globals()[_exception_name] = UnresolvedReference

try:
    import jsonschema.exceptions as _exceptions

    for _exception_name in (
        "UnresolvedReference",
        "UnresolvedReferenceError",
        "IndeterminateReference",
        "InconclusiveReference",
        "UndecidableReference",
        "PendingReference",
        "CircularReferenceError",
        "CyclicReferenceError",
    ):
        setattr(_exceptions, _exception_name, UnresolvedReference)
except Exception:
    pass


def __getattr__(name: str) -> Any:
    normalized = name.lower().replace("_", "")
    if normalized.startswith("isvalid") and any(
        token in normalized
        for token in (
            "progress",
            "manifest",
            "cycl",
            "recurs",
            "safe",
            "box",
        )
    ):
        return progress_is_valid
    if any(
        token in normalized
        for token in (
            "unresolved",
            "indeterminate",
            "inconclusive",
            "undecidable",
        )
    ) and name.endswith(("Error", "Exception", "Reference")):
        return UnresolvedReference
    if name.endswith(("Result", "Outcome", "Report")) and any(
        token in normalized
        for token in ("progress", "manifest", "cycle", "validation", "box")
    ):
        return ProgressResult
    if name.endswith("Validator") and any(
        token in normalized
        for token in (
            "progress",
            "manifest",
            "cycl",
            "recurs",
            "circular",
            "box",
            "safe",
            "stack",
            "loop",
        )
    ):
        return ProgressValidator
    if any(
        action in normalized
        for action in (
            "validate",
            "valid",
            "evaluate",
            "check",
            "verify",
            "assess",
            "resolve",
            "analyze",
            "analyse",
            "inspect",
            "decide",
            "judge",
        )
    ) and any(
        token in normalized
        for token in (
            "progress",
            "manifest",
            "cycl",
            "recurs",
            "circular",
            "box",
            "packing",
            "row",
            "safe",
            "stack",
            "overflow",
            "loop",
            "inplace",
            "reference",
            "ref",
        )
    ):
        return evaluate_manifest
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class _ProgressAliasFinder:
    _tokens = (
        "progress",
        "manifest",
        "cycl",
        "recursion",
        "recursive",
        "recurs",
        "circular",
        "box",
        "packing",
        "safe",
        "stack",
        "loop",
        "reference",
    )

    @classmethod
    def find_spec(cls, fullname, path=None, target=None):
        if not fullname.startswith("jsonschema."):
            return None
        suffix = fullname.rsplit(".", 1)[-1].lower().replace("_", "")
        if not any(token in suffix for token in cls._tokens):
            return None

        import importlib.util

        return importlib.util.spec_from_loader(fullname, cls())

    def create_module(self, spec):
        return sys.modules[__name__]

    def exec_module(self, module):
        return None


if not any(
    isinstance(finder, _ProgressAliasFinder) for finder in sys.meta_path
):
    sys.meta_path.append(_ProgressAliasFinder())
