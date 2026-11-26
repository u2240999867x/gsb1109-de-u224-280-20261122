from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from threading import RLock
from typing import Any
import sys
import types

from jsonschema._format import FormatChecker
from jsonschema.exceptions import FormatError
from jsonschema.validators import Draft202012Validator

__all__ = [
    "ReceiptBook",
    "ReceiptKey",
    "TriageBatch",
    "TriageDecision",
    "TriageJob",
    "accept",
    "accepted",
    "classify",
    "classify_batch",
    "reject",
    "rejected",
    "triage",
    "triage_batch",
    "waiting",
]

_TRUE_WORDS = {
    "true",
    "yes",
    "y",
    "1",
    "valid",
    "pass",
    "passed",
    "accept",
    "accepted",
    "receivable",
    "ok",
    "okay",
    "可收",
    "有效",
    "通过",
    "合格",
}
_FALSE_WORDS = {
    "false",
    "no",
    "n",
    "0",
    "invalid",
    "fail",
    "failed",
    "reject",
    "rejected",
    "denied",
    "拒收",
    "无效",
    "失败",
    "驳回",
}

_HANDLED_KEYWORDS = {
    "$ref",
    "$dynamicRef",
    "additionalProperties",
    "allOf",
    "anyOf",
    "contains",
    "dependentSchemas",
    "else",
    "format",
    "if",
    "items",
    "not",
    "oneOf",
    "patternProperties",
    "prefixItems",
    "properties",
    "propertyNames",
    "then",
    "unevaluatedItems",
    "unevaluatedProperties",
}


class ReceiptKey(tuple):
    """The identity of one registrar/version/format/raw-string receipt."""

    __slots__ = ()

    def __new__(
        cls,
        desk: Any,
        rule_version: Any,
        format_name: str,
        raw_value: str,
    ) -> ReceiptKey:
        return tuple.__new__(
            cls,
            (str(desk), str(rule_version), format_name, raw_value),
        )

    @property
    def desk(self) -> str:
        return self[0]

    @property
    def registry(self) -> str:
        return self[0]

    @property
    def registration_desk(self) -> str:
        return self[0]

    @property
    def rule_version(self) -> str:
        return self[1]

    @property
    def version(self) -> str:
        return self[1]

    @property
    def format_name(self) -> str:
        return self[2]

    @property
    def format(self) -> str:
        return self[2]

    @property
    def raw_value(self) -> str:
        return self[3]

    @property
    def value(self) -> str:
        return self[3]

    @property
    def code(self) -> str:
        return self[3]

    def as_scoped_tuple(self) -> tuple[str, str]:
        return self[2], self[3]


@dataclass(frozen=True)
class Receipt:
    desk: str
    rule_version: str
    format_name: str
    raw_value: str
    accepted: bool

    @property
    def key(self) -> ReceiptKey:
        return ReceiptKey(
            self.desk,
            self.rule_version,
            self.format_name,
            self.raw_value,
        )


def receipt_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, float):
        return bool(value)
    if isinstance(value, Mapping):
        for name in ("accepted", "valid", "ok", "passes", "result", "status"):
            if name in value:
                return receipt_value(value[name])
    if hasattr(value, "accepted"):
        return receipt_value(value.accepted)
    if hasattr(value, "valid"):
        return receipt_value(value.valid)
    text = str(value).strip().lower()
    if text in _TRUE_WORDS:
        return True
    if text in _FALSE_WORDS:
        return False
    raise ValueError(f"cannot interpret receipt value: {value!r}")


def _first(mapping: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in mapping and mapping[name] is not None:
            return mapping[name]
    return None


class ReceiptBook:
    """Thread-safe receipt store, namespaced by desk and rule version."""

    def __init__(self, receipts: Any = None) -> None:
        self._receipts: dict[ReceiptKey, bool] = {}
        self._lock = RLock()
        if receipts is not None:
            self.update(receipts)

    def record(
        self,
        desk: Any = None,
        rule_version: Any = None,
        format_name: str | None = None,
        raw_value: str | None = None,
        accepted: Any = None,
        *,
        registry: Any = None,
        version: Any = None,
        format: str | None = None,
        code: str | None = None,
        value: str | None = None,
        key: ReceiptKey | tuple[Any, ...] | None = None,
    ) -> ReceiptKey:
        if key is not None:
            if not isinstance(key, ReceiptKey):
                key = ReceiptKey(*key)
            if accepted is None:
                raise ValueError("a receipt result is required")
            accepted_bool = receipt_value(accepted)
        else:
            desk = desk if desk is not None else registry
            rule_version = (
                rule_version if rule_version is not None else version
            )
            format_name = format_name or format
            raw_value = raw_value if raw_value is not None else code
            raw_value = raw_value if raw_value is not None else value
            if None in (desk, rule_version, format_name, raw_value):
                raise ValueError("desk, version, format and value are required")
            key = ReceiptKey(desk, rule_version, format_name, raw_value)
            accepted_bool = receipt_value(
                True if accepted is None else accepted,
            )
        with self._lock:
            self._receipts[key] = accepted_bool
        return key

    add_receipt = record
    add = record
    receive = record

    def update(self, receipts: Any, **defaults: Any) -> None:
        if isinstance(receipts, ReceiptBook):
            with receipts._lock, self._lock:
                self._receipts.update(receipts._receipts)
            return
        if isinstance(receipts, Mapping):
            self._update_mapping(receipts, defaults)
            return
        if isinstance(receipts, Iterable) and not isinstance(
            receipts,
            (str, bytes),
        ):
            for item in receipts:
                if isinstance(item, Receipt):
                    self.record(
                        key=item.key,
                        accepted=item.accepted,
                    )
                elif isinstance(item, Mapping):
                    self._record_mapping_item(item, defaults)
                elif isinstance(item, tuple) and len(item) >= 2:
                    self._record_tuple(item, defaults)
                elif isinstance(item, str):
                    self.record(
                        desk=defaults.get("desk"),
                        rule_version=defaults.get("version"),
                        format_name=defaults.get("format_name"),
                        raw_value=item,
                        accepted=True,
                    )
                else:
                    raise ValueError(f"unsupported receipt: {item!r}")
            return
        raise TypeError("receipts must be a mapping or iterable")

    def _record_mapping_item(
        self,
        item: Mapping[str, Any],
        defaults: Mapping[str, Any],
    ) -> None:
        desk = _first(item, "desk", "registry", "registration_desk", "station")
        version = _first(
            item,
            "rule_version",
            "version",
            "rules_version",
            "schema_version",
        )
        format_name = _first(item, "format_name", "format")
        raw_value = _first(
            item,
            "raw_value",
            "value",
            "code",
            "raw_string",
            "string",
        )
        accepted = _first(
            item,
            "accepted",
            "valid",
            "is_valid",
            "result",
            "status",
        )
        self.record(
            desk=desk if desk is not None else defaults.get("desk"),
            rule_version=(
                version if version is not None else defaults.get("version")
            ),
            format_name=(
                format_name
                if format_name is not None
                else defaults.get("format_name")
            ),
            raw_value=(
                raw_value
                if raw_value is not None
                else defaults.get("raw_value")
            ),
            accepted=accepted,
        )

    def _record_tuple(
        self,
        item: tuple[Any, ...],
        defaults: Mapping[str, Any],
    ) -> None:
        if len(item) == 2 and isinstance(item[0], tuple):
            identity, result = item
            self.record(key=identity, accepted=result)
        elif len(item) == 2:
            raw_value, result = item
            self.record(
                desk=defaults.get("desk"),
                rule_version=defaults.get("version"),
                format_name=defaults.get("format_name"),
                raw_value=raw_value,
                accepted=result,
            )
        elif len(item) == 5:
            desk, version, format_name, raw_value, result = item
            self.record(
                desk=desk,
                rule_version=version,
                format_name=format_name,
                raw_value=raw_value,
                accepted=result,
            )
        elif len(item) == 4:
            identity, maybe_result = item[:3], item[3]
            if isinstance(maybe_result, bool):
                if len(identity) == 3:
                    desk, version, format_name = identity
                    raw_value = defaults.get("raw_value")
                    self.record(
                        desk=desk,
                        rule_version=version,
                        format_name=format_name,
                        raw_value=raw_value,
                        accepted=maybe_result,
                    )
                else:
                    self.record(key=item, accepted=True)
            else:
                self.record(key=item, accepted=maybe_result)
        else:
            raise ValueError(f"unsupported receipt tuple: {item!r}")

    def _update_mapping(
        self,
        receipts: Mapping[Any, Any],
        defaults: Mapping[str, Any],
    ) -> None:
        desk = defaults.get("desk")
        version = defaults.get("version")
        format_name = defaults.get("format_name")
        for identity, result in receipts.items():
            if isinstance(identity, ReceiptKey):
                self.record(key=identity, accepted=result)
            elif isinstance(identity, tuple):
                if len(identity) == 4:
                    self.record(key=identity, accepted=result)
                elif len(identity) == 2 and None not in (desk, version):
                    self.record(
                        desk=desk,
                        rule_version=version,
                        format_name=identity[0],
                        raw_value=identity[1],
                        accepted=result,
                    )
                elif len(identity) == 3:
                    self.record(
                        desk=identity[0],
                        rule_version=identity[1],
                        format_name=identity[2],
                        raw_value=defaults.get("raw_value", identity[2]),
                        accepted=result,
                    )
                else:
                    raise ValueError(f"unsupported receipt key: {identity!r}")
            elif isinstance(identity, str):
                if format_name is None or desk is None or version is None:
                    raise ValueError(
                        "string receipt keys require desk, version and format",
                    )
                self.record(
                    desk=desk,
                    rule_version=version,
                    format_name=format_name,
                    raw_value=identity,
                    accepted=result,
                )
            elif isinstance(identity, Mapping):
                item = dict(identity)
                item.setdefault("accepted", result)
                self._record_mapping_item(item, defaults)
            else:
                raise ValueError(f"unsupported receipt key: {identity!r}")

    def get(self, key: ReceiptKey) -> bool | None:
        with self._lock:
            return self._receipts.get(key)

    def scoped(
        self,
        desk: Any,
        rule_version: Any,
    ) -> dict[ReceiptKey, bool]:
        desk_text, version_text = str(desk), str(rule_version)
        with self._lock:
            return {
                key: value
                for key, value in self._receipts.items()
                if key.desk == desk_text
                and key.rule_version == version_text
            }

    def __contains__(self, key: object) -> bool:
        with self._lock:
            return key in self._receipts

    def __len__(self) -> int:
        with self._lock:
            return len(self._receipts)


class _Status(str):
    _aliases: frozenset[str] = frozenset()

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            text = other.strip().lower().replace("-", "_").replace(" ", "_")
            if text in self._aliases:
                return True
        return super().__eq__(other)

    def __hash__(self) -> int:
        return hash(str(self))


class AcceptedStatus(_Status):
    _aliases = frozenset(
        {
            "accepted",
            "accept",
            "acceptable",
            "pass",
            "passed",
            "valid",
            "ok",
            "receivable",
            "collectable",
            "可收",
            "肯定可收",
            "放行",
        },
    )

    @property
    def value(self) -> str:
        return "accepted"


class RejectedStatus(_Status):
    _aliases = frozenset(
        {
            "rejected",
            "reject",
            "unacceptable",
            "invalid",
            "fail",
            "failed",
            "denied",
            "拒收",
            "肯定拒收",
        },
    )

    @property
    def value(self) -> str:
        return "rejected"


class WaitingStatus(_Status):
    _aliases = frozenset(
        {
            "waiting",
            "wait",
            "pending",
            "uncertain",
            "incomplete",
            "needs_receipt",
            "awaiting_receipt",
            "确实要等",
            "等待",
            "待回执",
        },
    )

    @property
    def value(self) -> str:
        return "waiting"


ACCEPTED = AcceptedStatus("accepted")
REJECTED = RejectedStatus("rejected")
WAITING = WaitingStatus("waiting")

accept = ACCEPTED
accepted = ACCEPTED
reject = REJECTED
rejected = REJECTED
waiting = WAITING


class TriageStatus:
    ACCEPTED = ACCEPTED
    ACCEPT = ACCEPTED
    ACCEPTABLE = ACCEPTED
    PASS = ACCEPTED
    VALID = ACCEPTED
    RECEIVABLE = ACCEPTED
    COLLECTABLE = ACCEPTED
    REJECTED = REJECTED
    REJECT = REJECTED
    UNACCEPTABLE = REJECTED
    INVALID = REJECTED
    FAIL = REJECTED
    WAITING = WAITING
    WAIT = WAITING
    PENDING = WAITING
    UNCERTAIN = WAITING
    NEEDS_RECEIPT = WAITING


Status = TriageStatus
TriageState = TriageStatus


class PendingCode(str):
    def __new__(cls, key: ReceiptKey) -> PendingCode:
        obj = str.__new__(cls, key.raw_value)
        obj._key = key
        return obj

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ReceiptKey):
            return self.key == other
        if isinstance(other, tuple):
            if len(other) == 4:
                try:
                    return self.key == ReceiptKey(*other)
                except TypeError:
                    return False
            if len(other) == 2:
                return self.key.as_scoped_tuple() == other
            return False
        if isinstance(other, Mapping):
            return other.get("raw_value", other.get("code")) == str(self)
        return str.__eq__(self, other)

    def __hash__(self) -> int:
        return str.__hash__(self)

    @property
    def key(self) -> ReceiptKey:
        return self._key

    @property
    def desk(self) -> str:
        return self._key.desk

    @property
    def registry(self) -> str:
        return self._key.desk

    @property
    def rule_version(self) -> str:
        return self._key.rule_version

    @property
    def version(self) -> str:
        return self._key.rule_version

    @property
    def format_name(self) -> str:
        return self._key.format_name

    @property
    def format(self) -> str:
        return self._key.format_name


class ReceiptCompletion(dict):
    """A full assignment of scoped receipt facts."""

    def __init__(
        self,
        values: Mapping[ReceiptKey, bool],
        *,
        desk: Any,
        rule_version: Any,
        pending_formats: Iterable[str],
        base_checker: FormatChecker | None = None,
    ) -> None:
        super().__init__(values)
        self.desk = str(desk)
        self.rule_version = str(rule_version)
        self.pending_formats = frozenset(pending_formats)
        self.base_checker = base_checker

    @property
    def receipts(self) -> dict[ReceiptKey, bool]:
        return dict(self)

    @property
    def scoped_receipts(self) -> dict[tuple[str, str], bool]:
        return {key.as_scoped_tuple(): value for key, value in self.items()}

    @property
    def codes(self) -> dict[str, bool]:
        return {key.raw_value: value for key, value in self.items()}

    def format_checker(
        self,
        base_checker: FormatChecker | None = None,
    ) -> FormatChecker:
        return ReceiptFormatChecker(
            self,
            desk=self.desk,
            rule_version=self.rule_version,
            pending_formats=self.pending_formats,
            base_checker=base_checker or self.base_checker,
        )

    checker = format_checker

    def validator(
        self,
        schema: Mapping[str, Any] | bool,
        base_checker: FormatChecker | None = None,
    ) -> Draft202012Validator:
        return Draft202012Validator(
            schema,
            format_checker=self.format_checker(base_checker),
        )


class CompletionPair:
    def __init__(
        self,
        valid: ReceiptCompletion,
        invalid: ReceiptCompletion,
        key: ReceiptKey,
    ) -> None:
        self.valid = valid
        self.invalid = invalid
        self._differing_key = key

    def __iter__(self):
        yield self.valid
        yield self.invalid

    def __len__(self) -> int:
        return 2

    def __getitem__(self, index: int) -> ReceiptCompletion:
        if index == 0:
            return self.valid
        if index == 1:
            return self.invalid
        raise IndexError(index)

    @property
    def accepted(self) -> ReceiptCompletion:
        return self.valid

    @property
    def passing(self) -> ReceiptCompletion:
        return self.valid

    @property
    def rejected(self) -> ReceiptCompletion:
        return self.invalid

    @property
    def failing(self) -> ReceiptCompletion:
        return self.invalid

    @property
    def differing_key(self) -> ReceiptKey:
        return self._differing_key


class ReceiptFormatChecker(FormatChecker):
    def __init__(
        self,
        receipts: Mapping[Any, bool] | ReceiptCompletion,
        *,
        desk: Any = None,
        rule_version: Any = None,
        pending_formats: Iterable[str] = (),
        base_checker: FormatChecker | None = None,
    ) -> None:
        super().__init__(formats=())
        self._receipts = dict(receipts)
        self._desk = None if desk is None else str(desk)
        self._version = None if rule_version is None else str(rule_version)
        self._pending_formats = frozenset(pending_formats)
        self._base_checker = base_checker

    def _lookup(self, instance: Any, format_name: str) -> bool | None:
        if not isinstance(instance, str):
            return None
        if self._desk is not None and self._version is not None:
            key = ReceiptKey(
                self._desk,
                self._version,
                format_name,
                instance,
            )
            if key in self._receipts:
                return receipt_value(self._receipts[key])
        scoped = (format_name, instance)
        if scoped in self._receipts:
            return receipt_value(self._receipts[scoped])
        if instance in self._receipts:
            return receipt_value(self._receipts[instance])
        for key, value in self._receipts.items():
            if isinstance(key, ReceiptKey):
                if (
                    key.format_name == format_name
                    and key.raw_value == instance
                ):
                    return receipt_value(value)
            elif isinstance(key, tuple) and len(key) == 2:
                if key == scoped:
                    return receipt_value(value)
        return None

    def check(self, instance: object, format: str) -> None:
        if format in self._pending_formats and isinstance(instance, str):
            result = self._lookup(instance, format)
            if result is False:
                raise FormatError(
                    f"registrar receipt rejects {instance!r} as {format!r}",
                )
            if result is True:
                return
            raise FormatError(
                f"missing registrar receipt for {instance!r} as {format!r}",
            )
        if self._base_checker is not None:
            if isinstance(self._base_checker, FormatChecker):
                self._base_checker.check(instance, format)
                return
            if not _check_format(self._base_checker, instance, format):
                raise FormatError(
                    f"format checker rejects {instance!r} as {format!r}",
                )
            return
        super().check(instance, format)


def receipt_format_checker(
    receipts: Mapping[Any, bool] | None = None,
    *,
    desk: Any = None,
    rule_version: Any = None,
    pending_formats: Iterable[str] = (),
    format_checker: FormatChecker | None = None,
    base_checker: FormatChecker | None = None,
) -> ReceiptFormatChecker:
    return ReceiptFormatChecker(
        receipts or {},
        desk=desk,
        rule_version=rule_version,
        pending_formats=pending_formats,
        base_checker=base_checker or format_checker,
    )


def _check_format(checker: Any, instance: Any, format_name: str) -> bool:
    conforms = getattr(checker, "conforms", None)
    if callable(conforms):
        return bool(conforms(instance, format_name))
    check = getattr(checker, "check", None)
    if callable(check):
        try:
            check(instance, format_name)
        except FormatError:
            return False
        return True
    if callable(checker):
        try:
            result = checker(instance, format_name)
        except Exception:
            return False
        return True if result is None else bool(result)
    return True


class TriageGroups(tuple):
    __slots__ = ()

    def __new__(cls, batch: TriageBatch) -> TriageGroups:
        return tuple.__new__(
            cls,
            (batch.accepted, batch.rejected, batch.waiting),
        )

    @property
    def accepted(self):
        return self[0]

    @property
    def rejected(self):
        return self[1]

    @property
    def waiting(self):
        return self[2]

    @property
    def counts(self) -> dict[str, int]:
        return {
            "accepted": len(self[0]),
            "rejected": len(self[1]),
            "waiting": len(self[2]),
        }


class _CompletionMap(Mapping):
    def __init__(self, decision: TriageDecision) -> None:
        self._decision = decision

    def __getitem__(self, key: Any) -> CompletionPair:
        return self._decision.completion_pair(key)

    def __iter__(self):
        return iter(self._decision["pending_keys"])

    def __len__(self) -> int:
        return len(self._decision["pending_keys"])


class TriageDecision(dict):
    def __init__(
        self,
        *,
        status: _Status,
        document: Any,
        index: int | None,
        document_id: Any,
        desk: Any,
        rule_version: Any,
        pending_keys: Iterable[ReceiptKey],
        bdd: _BDD | None = None,
        root: int | None = None,
        unknown_keys: Mapping[int, ReceiptKey] | None = None,
        pending_formats: Iterable[str] = (),
        format_checker: FormatChecker | None = None,
        schema: Mapping[str, Any] | bool | None = None,
        known_receipts: Mapping[ReceiptKey, bool] | None = None,
    ) -> None:
        pending_keys = list(pending_keys)
        pending_codes = [PendingCode(key) for key in pending_keys]
        super().__init__(
            status=status,
            verdict=status,
            decision=status,
            state=status,
            classification=status,
            result=status,
            accepted=status == ACCEPTED,
            acceptable=status == ACCEPTED,
            definitely_accepted=status == ACCEPTED,
            definitely_acceptable=status == ACCEPTED,
            collectable=status == ACCEPTED,
            receivable=status == ACCEPTED,
            passing=status == ACCEPTED,
            valid=status == ACCEPTED,
            rejected=status == REJECTED,
            rejectable=status == REJECTED,
            definitely_rejected=status == REJECTED,
            definitely_rejectable=status == REJECTED,
            failing=status == REJECTED,
            invalid=status == REJECTED,
            waiting=status == WAITING,
            is_pending=status == WAITING,
            uncertain=status == WAITING,
            needs_receipt=status == WAITING,
            awaiting_receipt=status == WAITING,
            pending=pending_codes,
            pending_codes=pending_codes,
            codes=pending_codes,
            unresolved=pending_codes,
            missing_codes=pending_codes,
            waiting_codes=pending_codes,
            waiting_for=pending_keys,
            pending_receipts=pending_keys,
            pending_keys=pending_keys,
            required_receipts=pending_keys,
            receipts_needed=pending_keys,
            needed=pending_keys,
            missing=pending_codes,
            index=index,
            id=document_id,
            document_id=document_id,
            desk=desk,
            registry=desk,
            rule_version=rule_version,
            version=rule_version,
        )
        self.__dict__.update({k: v for k, v in self.items() if k != "status"})
        self.status = status
        self.verdict = status
        self.decision = status
        self.state = status
        self.document = document
        self.schema = schema
        self._bdd = bdd
        self._root = root
        self._unknown_keys = dict(unknown_keys or {})
        self._pending_formats = frozenset(pending_formats)
        self._format_checker = format_checker
        self._known_receipts = known_receipts or {}
        self._pair_cache: dict[ReceiptKey, CompletionPair] = {}
        completion_map = _CompletionMap(self)
        for name in (
            "completions",
            "witnesses",
            "alternatives",
            "completion_pairs",
        ):
            self[name] = completion_map
            setattr(self, name, completion_map)

    def _rebuild_bdd(self) -> tuple[_BDD, int]:
        symbolic = _SymbolicValidator(
            schema=self.schema,
            desk=self.desk,
            rule_version=self.rule_version,
            pending_formats=self._pending_formats,
            format_checker=self._format_checker,
        )
        formula = symbolic.formula(self.schema, self.document)
        bdd, root, _unknown_keys = _compile_bdd(
            formula,
            self._known_receipts,
            _instance_string_order(self.document),
        )
        return bdd, root

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as error:
            raise AttributeError(name) from error

    def __iter__(self):
        yield self.status
        yield self.pending

    def __getitem__(self, key: Any):
        if key == 0:
            return self.status
        if key == 1:
            return self.pending
        return dict.__getitem__(self, key)

    @property
    def is_accepted(self) -> bool:
        return self["status"] == ACCEPTED

    @property
    def is_rejected(self) -> bool:
        return self["status"] == REJECTED

    @property
    def is_waiting(self) -> bool:
        return self["status"] == WAITING

    def _resolve_key(self, code_or_key: Any) -> ReceiptKey:
        if isinstance(code_or_key, ReceiptKey):
            key = code_or_key
        elif isinstance(code_or_key, PendingCode):
            key = code_or_key.key
        elif isinstance(code_or_key, tuple) and len(code_or_key) == 4:
            key = ReceiptKey(*code_or_key)
        else:
            matches = [
                key
                for key in self["pending_keys"]
                if key.raw_value == code_or_key
            ]
            if len(matches) != 1:
                raise KeyError(
                    f"pending receipt code is not unique: {code_or_key!r}",
                )
            key = matches[0]
        if key not in self["pending_keys"]:
            raise KeyError(f"receipt is not pending and relevant: {key!r}")
        return key

    def completion_pair(self, code_or_key: Any) -> CompletionPair:
        key = self._resolve_key(code_or_key)
        if key in self._pair_cache:
            return self._pair_cache[key]
        if self._bdd is not None and self._root is not None:
            bdd, root = self._bdd, self._root
        elif self.schema is not None:
            bdd, root = self._rebuild_bdd()
        else:
            raise RuntimeError("completion witnesses are unavailable")
        level = next(
            level
            for level, candidate in self._unknown_keys.items()
            if candidate == key
        )
        values = bdd.difference_assignment(root, level)
        if values is None:
            raise RuntimeError("pending key has no distinguishing completion")

        def make_completion(bit: bool) -> ReceiptCompletion:
            resolved = dict(values)
            resolved[level] = 1 if bit else 0
            receipts = dict(self._known_receipts)
            for other_level, other_key in self._unknown_keys.items():
                receipts[other_key] = bool(resolved[other_level])
            return ReceiptCompletion(
                receipts,
                desk=self["desk"],
                rule_version=self["rule_version"],
                pending_formats=self._pending_formats,
                base_checker=self._format_checker,
            )

        false_completion = make_completion(False)
        true_completion = make_completion(True)
        false_root = bdd.evaluate(
            root,
            {**values, level: 0},
        )
        true_root = bdd.evaluate(
            root,
            {**values, level: 1},
        )
        if false_root == 1 and true_root == 0:
            valid, invalid = false_completion, true_completion
        elif true_root == 1 and false_root == 0:
            valid, invalid = true_completion, false_completion
        else:
            raise RuntimeError("completion does not distinguish verdict")
        pair = CompletionPair(valid, invalid, key)
        self._pair_cache[key] = pair
        return pair

    def completions_for(self, code_or_key: Any) -> CompletionPair:
        return self.completion_pair(code_or_key)

    def witnesses_for(self, code_or_key: Any) -> CompletionPair:
        return self.completion_pair(code_or_key)

    def pair(self, code_or_key: Any) -> CompletionPair:
        return self.completion_pair(code_or_key)

    def witness(self, code_or_key: Any) -> CompletionPair:
        return self.completion_pair(code_or_key)

    def evidence_for(self, code_or_key: Any) -> CompletionPair:
        return self.completion_pair(code_or_key)

    def completion(self, code_or_key: Any) -> CompletionPair:
        return self.completion_pair(code_or_key)

    def alternatives_for(self, code_or_key: Any) -> CompletionPair:
        return self.completion_pair(code_or_key)

    def witness_checkers(
        self,
        code_or_key: Any,
        base_checker: FormatChecker | None = None,
    ) -> tuple[ReceiptFormatChecker, ReceiptFormatChecker]:
        pair = self.completion_pair(code_or_key)
        return (
            pair.valid.format_checker(
                base_checker or self._format_checker,
            ),
            pair.invalid.format_checker(
                base_checker or self._format_checker,
            ),
        )

    def verify_completion(
        self,
        code_or_key: Any,
        schema: Mapping[str, Any] | bool | None = None,
    ) -> tuple[bool, bool]:
        pair = self.completion_pair(code_or_key)
        schema = self.schema if schema is None else schema
        if schema is None:
            raise ValueError("a schema is required for ordinary validation")
        results = []
        for completion in (pair.valid, pair.invalid):
            validator = completion.validator(schema, self._format_checker)
            results.append(validator.is_valid(self.document))
        return tuple(results)


class TriageBatch(list):
    def __init__(
        self,
        decisions: Iterable[TriageDecision] = (),
        *,
        documents: Any = None,
        refresh_callback: Any = None,
    ) -> None:
        super().__init__(decisions)
        self.documents = documents
        self._refresh_callback = refresh_callback
        self.by_id: dict[Any, TriageDecision] = {}
        for decision in self:
            if decision.get("id") is not None:
                self.by_id[decision["id"]] = decision

    def refresh(self) -> TriageBatch:
        if self._refresh_callback is None:
            raise RuntimeError("this batch does not support refresh")
        return self._refresh_callback()

    requery = refresh
    rerun = refresh
    reload = refresh

    @property
    def accepted(self) -> list[TriageDecision]:
        return [item for item in self if item.is_accepted]

    @property
    def accept(self) -> list[TriageDecision]:
        return self.accepted

    @property
    def acceptable(self) -> list[TriageDecision]:
        return self.accepted

    @property
    def definitely_accepted(self) -> list[TriageDecision]:
        return self.accepted

    @property
    def rejected(self) -> list[TriageDecision]:
        return [item for item in self if item.is_rejected]

    @property
    def reject(self) -> list[TriageDecision]:
        return self.rejected

    @property
    def definitely_rejected(self) -> list[TriageDecision]:
        return self.rejected

    @property
    def waiting(self) -> list[TriageDecision]:
        return [item for item in self if item.is_waiting]

    @property
    def wait(self) -> list[TriageDecision]:
        return self.waiting

    @property
    def pending(self) -> list[TriageDecision]:
        return self.waiting

    @property
    def uncertain(self) -> list[TriageDecision]:
        return self.waiting

    @property
    def results(self) -> list[TriageDecision]:
        return list(self)

    @property
    def decisions(self) -> list[TriageDecision]:
        return list(self)

    def groups(self) -> dict[str, list[TriageDecision]]:
        return {
            "accepted": self.accepted,
            "rejected": self.rejected,
            "waiting": self.waiting,
        }

    @property
    def counts(self) -> dict[str, int]:
        return {
            "accepted": len(self.accepted),
            "rejected": len(self.rejected),
            "waiting": len(self.waiting),
            "total": len(self),
        }

    def get(self, document_id: Any) -> TriageDecision | None:
        return self.by_id.get(document_id)

    def __getitem__(self, index: Any):
        if isinstance(index, str):
            if index in self.by_id:
                return self.by_id[index]
            normalized = index.strip().lower().replace("-", "_")
            if normalized in {
                "accept",
                "accepted",
                "acceptable",
                "definitely_accepted",
                "pass",
                "valid",
            }:
                return self.accepted
            if normalized in {
                "reject",
                "rejected",
                "rejectable",
                "definitely_rejected",
                "fail",
                "invalid",
            }:
                return self.rejected
            if normalized in {
                "wait",
                "waiting",
                "pending",
                "uncertain",
                "needs_receipt",
            }:
                return self.waiting
            if normalized in {"results", "decisions", "documents", "all"}:
                return list(self)
        return super().__getitem__(index)


class _BDD:
    def __init__(self, levels: Mapping[ReceiptKey, int]) -> None:
        self.levels = dict(levels)
        self.variable = [-1, -1]
        self.low = [0, 1]
        self.high = [0, 1]
        self.unique: dict[tuple[int, int, int], int] = {}
        self.apply_cache: dict[tuple[str, int, int], int] = {}
        self.negate_cache: dict[int, int] = {}

    def make(self, level: int, low: int, high: int) -> int:
        if low == high:
            return low
        identity = (level, low, high)
        existing = self.unique.get(identity)
        if existing is not None:
            return existing
        node = len(self.variable)
        self.variable.append(level)
        self.low.append(low)
        self.high.append(high)
        self.unique[identity] = node
        return node

    def variable_node(self, level: int) -> int:
        return self.make(level, 0, 1)

    def negate(self, node: int) -> int:
        if node == 0:
            return 1
        if node == 1:
            return 0
        cached = self.negate_cache.get(node)
        if cached is not None:
            return cached
        result = self.make(
            self.variable[node],
            self.negate(self.low[node]),
            self.negate(self.high[node]),
        )
        self.negate_cache[node] = result
        return result

    def apply(self, operation: str, left: int, right: int) -> int:
        if operation == "and":
            if left == 0 or right == 0:
                return 0
            if left == 1:
                return right
            if right == 1:
                return left
            if left == right:
                return left
        elif operation == "or":
            if left == 1 or right == 1:
                return 1
            if left == 0:
                return right
            if right == 0:
                return left
            if left == right:
                return left
        elif operation == "xor":
            if left == 0:
                return right
            if right == 0:
                return left
            if left == 1:
                return self.negate(right)
            if right == 1:
                return self.negate(left)
            if left == right:
                return 0
        cache_key = (operation, min(left, right), max(left, right))
        cached = self.apply_cache.get(cache_key)
        if cached is not None:
            return cached
        left_level = self.variable[left] if left >= 2 else 10**9
        right_level = self.variable[right] if right >= 2 else 10**9
        top = min(left_level, right_level)

        def cofactors(node: int) -> tuple[int, int]:
            if node >= 2 and self.variable[node] == top:
                return self.low[node], self.high[node]
            return node, node

        left_low, left_high = cofactors(left)
        right_low, right_high = cofactors(right)
        low_result = self.apply(operation, left_low, right_low)
        high_result = self.apply(operation, left_high, right_high)
        result = self.make(top, low_result, high_result)
        self.apply_cache[cache_key] = result
        return result

    def support(self, root: int) -> set[int]:
        seen: set[int] = set()
        levels: set[int] = set()
        stack = [root]
        while stack:
            node = stack.pop()
            if node < 2 or node in seen:
                continue
            seen.add(node)
            levels.add(self.variable[node])
            stack.append(self.low[node])
            stack.append(self.high[node])
        return levels

    def evaluate(
        self,
        root: int,
        assignment: Mapping[int, bool | int],
    ) -> int:
        node = root
        while node >= 2:
            bit = bool(assignment[self.variable[node]])
            node = self.high[node] if bit else self.low[node]
        return node

    def _path_to_node(self, root: int, target: int) -> dict[int, int] | None:
        if root == target:
            return {}
        if root < 2 or self.variable[root] > self.variable[target]:
            return None
        for bit, child in (
            (0, self.low[root]),
            (1, self.high[root]),
        ):
            path = self._path_to_node(child, target)
            if path is not None:
                path[self.variable[root]] = bit
                return path
        return None

    def _path_to_true(self, root: int) -> dict[int, int] | None:
        if root == 1:
            return {}
        if root == 0:
            return None
        for bit, child in (
            (0, self.low[root]),
            (1, self.high[root]),
        ):
            path = self._path_to_true(child)
            if path is not None:
                path[self.variable[root]] = bit
                return path
        return None

    def difference_assignment(
        self,
        root: int,
        level: int,
    ) -> dict[int, int] | None:
        target = None
        stack = [root]
        seen: set[int] = set()
        while stack:
            node = stack.pop()
            if node < 2 or node in seen:
                continue
            seen.add(node)
            if self.variable[node] == level:
                target = node
                break
            stack.append(self.low[node])
            stack.append(self.high[node])
        if target is None:
            return None
        prefix = self._path_to_node(root, target)
        if prefix is None:
            return None
        difference = self.apply(
            "xor",
            self.low[target],
            self.high[target],
        )
        suffix = self._path_to_true(difference)
        if suffix is None:
            return None
        assignment = {level_index: 0 for level_index in self.levels.values()}
        assignment.update(prefix)
        assignment.update(suffix)
        return assignment


def _var(key: ReceiptKey) -> tuple[Any, ...]:
    return ("var", key)


def _not(formula: Any) -> tuple[Any, ...]:
    if formula is True:
        return False
    if formula is False:
        return True
    if isinstance(formula, tuple) and formula[0] == "not":
        return formula[1]
    return ("not", formula)


def _and(*formulas: Any) -> Any:
    flat: list[Any] = []
    for formula in formulas:
        if formula is True:
            continue
        if formula is False:
            return False
        if isinstance(formula, tuple) and formula[0] == "and":
            flat.extend(formula[1])
        else:
            flat.append(formula)
    if not flat:
        return True
    if len(flat) == 1:
        return flat[0]
    return ("and", tuple(flat))


def _or(*formulas: Any) -> Any:
    flat: list[Any] = []
    for formula in formulas:
        if formula is False:
            continue
        if formula is True:
            return True
        if isinstance(formula, tuple) and formula[0] == "or":
            flat.extend(formula[1])
        else:
            flat.append(formula)
    if not flat:
        return False
    if len(flat) == 1:
        return flat[0]
    return ("or", tuple(flat))


def _one_of(formulas: list[Any]) -> Any:
    if not formulas:
        return False
    branches = []
    for index, formula in enumerate(formulas):
        others = [
            _not(other)
            for other_index, other in enumerate(formulas)
            if other_index != index
        ]
        branches.append(_and(formula, _and(*others)))
    return _or(*branches)


class _SymbolicValidator:
    def __init__(
        self,
        *,
        schema: Mapping[str, Any] | bool,
        desk: Any,
        rule_version: Any,
        pending_formats: frozenset[str],
        format_checker: FormatChecker | None,
        plain_cache: dict[int, tuple[Any, Draft202012Validator]] | None = None,
    ) -> None:
        self.root_schema = schema
        self.desk = str(desk)
        self.rule_version = str(rule_version)
        self.pending_formats = pending_formats
        self.format_checker = format_checker
        self._plain_cache = {} if plain_cache is None else plain_cache

    def key(self, format_name: str, instance: str) -> ReceiptKey:
        return ReceiptKey(
            self.desk,
            self.rule_version,
            format_name,
            instance,
        )

    def formula(self, schema: Mapping[str, Any] | bool, instance: Any) -> Any:
        if schema is True:
            return True
        if schema is False:
            return False
        if not isinstance(schema, Mapping):
            return True

        parts = [self._plain_formula(schema, instance)]
        if parts[0] is False:
            return False

        format_name = schema.get("format")
        if isinstance(format_name, str):
            if (
                format_name in self.pending_formats
                and isinstance(instance, str)
            ):
                parts.append(_var(self.key(format_name, instance)))
            elif self.format_checker is not None:
                parts.append(
                    _check_format(
                        self.format_checker,
                        instance,
                        format_name,
                    ),
                )

        if "allOf" in schema:
            children = schema["allOf"]
            if isinstance(children, list):
                parts.append(
                    _and(
                        *[
                            self.formula(child, instance)
                            for child in children
                        ],
                    ),
                )
        if "anyOf" in schema and isinstance(schema["anyOf"], list):
            parts.append(
                _or(
                    *[
                        self.formula(child, instance)
                        for child in schema["anyOf"]
                    ],
                ),
            )
        if "oneOf" in schema and isinstance(schema["oneOf"], list):
            parts.append(
                _one_of(
                    [
                        self.formula(child, instance)
                        for child in schema["oneOf"]
                    ],
                ),
            )
        if "not" in schema:
            parts.append(_not(self.formula(schema["not"], instance)))
        if "if" in schema:
            condition = self.formula(schema["if"], instance)
            then_formula = (
                self.formula(schema["then"], instance)
                if "then" in schema
                else True
            )
            else_formula = (
                self.formula(schema["else"], instance)
                if "else" in schema
                else True
            )
            parts.append(
                _and(
                    _or(_not(condition), then_formula),
                    _or(condition, else_formula),
                ),
            )

        if isinstance(instance, dict):
            properties = schema.get("properties")
            if isinstance(properties, Mapping):
                for name, child in properties.items():
                    if name in instance:
                        parts.append(self.formula(child, instance[name]))

            pattern_properties = schema.get("patternProperties")
            if isinstance(pattern_properties, Mapping):
                import re

                for pattern, child in pattern_properties.items():
                    matcher = re.compile(pattern)
                    for name, value in instance.items():
                        if matcher.search(name):
                            parts.append(self.formula(child, value))

            if "additionalProperties" in schema:
                configured = schema["additionalProperties"]
                defined = set(properties or ())
                patterns = list(pattern_properties or ())
                import re

                matchers = [re.compile(pattern) for pattern in patterns]
                for name, value in instance.items():
                    if name in defined:
                        continue
                    if any(matcher.search(name) for matcher in matchers):
                        continue
                    parts.append(self.formula(configured, value))

            property_names = schema.get("propertyNames")
            if property_names is not None:
                for name in instance:
                    parts.append(self.formula(property_names, name))

            dependent_schemas = schema.get("dependentSchemas")
            if isinstance(dependent_schemas, Mapping):
                for name, child in dependent_schemas.items():
                    if name in instance:
                        parts.append(self.formula(child, instance))

        if isinstance(instance, list):
            prefix_items = schema.get("prefixItems")
            if isinstance(prefix_items, list):
                for index, child in enumerate(prefix_items):
                    if index < len(instance):
                        parts.append(self.formula(child, instance[index]))

            if "items" in schema and isinstance(schema["items"], (Mapping, bool)):
                start = (
                    len(prefix_items)
                    if isinstance(prefix_items, list)
                    else 0
                )
                for index in range(start, len(instance)):
                    parts.append(
                        self.formula(schema["items"], instance[index]),
                    )

        return _and(*parts)

    def _plain_formula(
        self,
        schema: Mapping[str, Any],
        instance: Any,
    ) -> bool:
        plain_schema = {
            key: value
            for key, value in schema.items()
            if key not in _HANDLED_KEYWORDS
        }
        if not plain_schema or all(
            key == "$schema" for key in plain_schema
        ):
            return True
        schema_id = id(schema)
        cached = self._plain_cache.get(schema_id)
        if cached is not None and cached[0] is schema:
            validator = cached[1]
        else:
            validator = Draft202012Validator(plain_schema)
            self._plain_cache[schema_id] = (schema, validator)
        return validator.is_valid(instance)


def _collect_variables(formula: Any) -> list[ReceiptKey]:
    order: list[ReceiptKey] = []
    seen: set[ReceiptKey] = set()
    stack = [formula]
    while stack:
        node = stack.pop()
        if not isinstance(node, tuple):
            continue
        if node[0] == "var":
            key = node[1]
            if key not in seen:
                seen.add(key)
                order.append(key)
        else:
            for child in node[1:]:
                if isinstance(child, tuple):
                    if node[0] in {"and", "or"}:
                        stack.extend(child)
                    else:
                        stack.append(child)
    return order


def _looks_like_array_instance(schema: Any, documents: Any) -> bool:
    if not documents or not isinstance(schema, Mapping):
        return False
    array_contract = (
        schema.get("type") == "array"
        or "prefixItems" in schema
        or "items" in schema
    )
    return array_contract and not any(
        isinstance(item, (Mapping, list, tuple))
        for item in documents
    )


def _instance_string_order(document: Any) -> dict[str, int]:
    order: dict[str, int] = {}
    index = 0
    stack = [document]
    while stack:
        value = stack.pop()
        if isinstance(value, str):
            if value not in order:
                order[value] = index
                index += 1
        elif isinstance(value, Mapping):
            stack.extend(reversed(list(value.values())))
        elif isinstance(value, list):
            stack.extend(reversed(value))
    return order


def _compile_bdd(
    formula: Any,
    known: Mapping[ReceiptKey, bool],
    string_order: Mapping[str, int],
) -> tuple[_BDD, int, dict[int, ReceiptKey]]:
    variables = _collect_variables(formula)
    unknown = [key for key in variables if key not in known]
    unknown.sort(
        key=lambda key: (
            string_order.get(key.raw_value, 10**9),
            key.format_name,
            key.desk,
            key.rule_version,
        ),
    )
    levels = {key: index for index, key in enumerate(unknown)}
    bdd = _BDD(levels)
    unknown_by_level = {index: key for key, index in levels.items()}

    def compile_node(node: Any) -> int:
        if node is True:
            return 1
        if node is False:
            return 0
        operation = node[0]
        if operation == "var":
            key = node[1]
            if key in known:
                return 1 if known[key] else 0
            return bdd.variable_node(levels[key])
        if operation == "not":
            return bdd.negate(compile_node(node[1]))
        if operation == "and":
            result = 1
            for child in node[1]:
                result = bdd.apply(
                    "and",
                    result,
                    compile_node(child),
                )
            return result
        if operation == "or":
            result = 0
            for child in node[1]:
                result = bdd.apply(
                    "or",
                    result,
                    compile_node(child),
                )
            return result
        raise ValueError(f"unknown formula node: {node!r}")

    root = compile_node(formula)
    return bdd, root, unknown_by_level


class TriageJob:
    """Reusable Draft 2020-12 receipt-aware triage entry."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        schema = kwargs.pop("schema", None)
        if schema is None:
            schema = kwargs.pop("contract", None)
        pending_formats = kwargs.pop("pending_formats", None)
        receipt_formats = kwargs.pop("receipt_formats", None)
        awaited_formats = kwargs.pop("awaited_formats", None)
        formats = kwargs.pop("formats", None)
        deferred_formats = kwargs.pop("deferred_formats", None)
        format_checker = kwargs.pop("format_checker", None)
        checker = kwargs.pop("checker", None)
        validator = kwargs.pop("validator", None)
        receipt_book = kwargs.pop("receipt_book", None)
        book = kwargs.pop("book", None)
        receipts = kwargs.pop("receipts", None)
        desk = kwargs.pop("desk", None)
        registry = kwargs.pop("registry", None)
        registration_desk = kwargs.pop("registration_desk", None)
        rule_version = kwargs.pop("rule_version", None)
        version = kwargs.pop("version", None)
        rules_version = kwargs.pop("rules_version", None)

        format_values = [
            pending_formats,
            receipt_formats,
            awaited_formats,
            formats,
            deferred_formats,
        ]
        positional = list(args)
        for value in list(positional):
            if (
                validator is None
                and hasattr(value, "schema")
                and hasattr(value, "format_checker")
            ):
                validator = value
                positional.remove(value)
            elif format_checker is None and isinstance(value, FormatChecker):
                format_checker = value
                positional.remove(value)
            elif (
                receipt_book is None
                and book is None
                and isinstance(value, ReceiptBook)
            ):
                receipt_book = value
                positional.remove(value)
            elif schema is None and (
                isinstance(value, bool) or isinstance(value, Mapping)
            ):
                schema = value
                positional.remove(value)
            elif isinstance(value, Iterable) and not isinstance(
                value,
                (str, bytes, Mapping, FormatChecker, ReceiptBook),
            ):
                if all(isinstance(item, str) for item in value):
                    format_values.append(value)
                    positional.remove(value)
            elif isinstance(value, str):
                format_values.append(value)
                positional.remove(value)
        if positional:
            if schema is None and isinstance(positional[0], (Mapping, bool)):
                schema = positional.pop(0)
        if validator is not None:
            if schema is None:
                schema = validator.schema
            if format_checker is None and checker is None:
                format_checker = getattr(validator, "format_checker", None)
        self.schema = schema
        format_source = next(
            (value for value in format_values if value is not None),
            (),
        )
        if isinstance(format_source, str):
            format_source = (format_source,)
        self.pending_formats = frozenset(format_source)
        self.format_checker = format_checker or checker
        if receipt_book is not None:
            self.receipt_book = receipt_book
        elif book is not None:
            self.receipt_book = book
        elif isinstance(receipts, ReceiptBook):
            self.receipt_book = receipts
        else:
            self.receipt_book = ReceiptBook()
        self._raw_receipts = (
            None
            if receipts is None or isinstance(receipts, ReceiptBook)
            else receipts
        )
        self.desk = desk if desk is not None else registry or registration_desk
        self.rule_version = (
            rule_version if rule_version is not None else version or rules_version
        )
        self.kwargs = kwargs
        self._plain_cache: dict[
            int,
            tuple[Any, Draft202012Validator],
        ] = {}

    def scope(
        self,
        desk: Any = None,
        rule_version: Any = None,
        *,
        registry: Any = None,
        version: Any = None,
        receipts: Any = None,
    ) -> tuple[Any, Any, dict[ReceiptKey, bool]]:
        desk = (
            desk
            if desk is not None
            else registry
            if registry is not None
            else self.desk
        )
        rule_version = (
            rule_version
            if rule_version is not None
            else version
            if version is not None
            else self.rule_version
        )
        if desk is None or rule_version is None:
            raise ValueError("desk and rule_version are required")
        receipt_source = receipts
        if receipt_source is None:
            receipt_source = self._raw_receipts
        if receipt_source is not None:
            if isinstance(receipt_source, ReceiptBook):
                book = receipt_source
            else:
                book = ReceiptBook()
                format_defaults = tuple(self.pending_formats) or (None,)
                for format_name in format_defaults:
                    book.update(
                        receipt_source,
                        desk=desk,
                        version=rule_version,
                        format_name=format_name,
                    )
        else:
            book = self.receipt_book
        return desk, rule_version, book.scoped(desk, rule_version)

    def classify_one(
        self,
        document: Any,
        *,
        index: int | None = None,
        document_id: Any = None,
        desk: Any = None,
        rule_version: Any = None,
        registry: Any = None,
        version: Any = None,
        receipts: Any = None,
        schema: Mapping[str, Any] | bool | None = None,
        known_receipts: Mapping[ReceiptKey, bool] | None = None,
    ) -> TriageDecision:
        schema = self.schema if schema is None else schema
        if schema is None:
            raise ValueError("a Draft 2020-12 schema is required")
        if known_receipts is None:
            desk_value, version_value, known = self.scope(
                desk,
                rule_version,
                registry=registry,
                version=version,
                receipts=receipts,
            )
        else:
            if desk is None and registry is not None:
                desk = registry
            if rule_version is None and version is not None:
                rule_version = version
            if desk is None or rule_version is None:
                raise ValueError("desk and rule_version are required")
            desk_value, version_value = str(desk), str(rule_version)
            known = known_receipts
        symbolic = _SymbolicValidator(
            schema=schema,
            desk=desk_value,
            rule_version=version_value,
            pending_formats=self.pending_formats,
            format_checker=self.format_checker,
            plain_cache=self._plain_cache,
        )
        formula = symbolic.formula(schema, document)
        bdd, root, unknown_keys = _compile_bdd(
            formula,
            known,
            _instance_string_order(document),
        )
        relevant_levels = bdd.support(root) if root >= 2 else set()
        pending_keys = [
            unknown_keys[level]
            for level in sorted(relevant_levels)
            if level in unknown_keys
        ]
        if root == 1:
            status = ACCEPTED
        elif root == 0:
            status = REJECTED
        else:
            status = WAITING
        decision = TriageDecision(
            status=status,
            document=document,
            index=index,
            document_id=document_id,
            desk=desk_value,
            rule_version=version_value,
            pending_keys=pending_keys,
            bdd=bdd,
            root=root,
            unknown_keys=unknown_keys,
            pending_formats=self.pending_formats,
            format_checker=self.format_checker,
            schema=schema,
            known_receipts=known,
        )
        decision._bdd = None
        decision._root = None
        return decision

    def triage(self, *args: Any, **kwargs: Any) -> TriageBatch | TriageDecision:
        schema = kwargs.pop("schema", None)
        if schema is None:
            schema = kwargs.pop("contract", None)
        documents = kwargs.pop("documents", None)
        if documents is None:
            documents = kwargs.pop("instances", None)
        if documents is None:
            documents = kwargs.pop("batch", None)
        if documents is None:
            documents = kwargs.pop("data", None)
        documents_by_id = kwargs.pop("documents_by_id", None)
        if documents_by_id is None:
            documents_by_id = kwargs.pop("items_by_id", None)
        desk = kwargs.pop("desk", None)
        registry = kwargs.pop("registry", None)
        registration_desk = kwargs.pop("registration_desk", None)
        station = kwargs.pop("station", None)
        rule_version = kwargs.pop("rule_version", None)
        version = kwargs.pop("version", None)
        rules_version = kwargs.pop("rules_version", None)
        receipts = kwargs.pop("receipts", None)
        if receipts is None:
            receipts = kwargs.pop("receipt_book", None)
        if receipts is None:
            receipts = kwargs.pop("book", None)

        positional = list(args)
        if schema is None:
            for index, value in enumerate(positional):
                if hasattr(value, "schema") and hasattr(
                    value,
                    "format_checker",
                ):
                    schema = value.schema
                    if self.format_checker is None:
                        self.format_checker = value.format_checker
                    positional.pop(index)
                    break
        if schema is None and self.schema is None:
            for index, value in enumerate(positional):
                if isinstance(value, bool) or (
                    isinstance(value, Mapping)
                    and any(
                        key in value
                        for key in (
                            "$schema",
                            "allOf",
                            "anyOf",
                            "oneOf",
                            "prefixItems",
                            "properties",
                            "format",
                            "items",
                            "type",
                        )
                    )
                ):
                    schema = value
                    positional.pop(index)
                    break
        elif schema is None:
            schema = self.schema
        if documents is None and positional:
            if len(positional) == 1:
                documents = positional[0]
            else:
                documents = positional

        if documents is None:
            documents = kwargs.pop("document", None)
        scoped_desk = (
            desk
            if desk is not None
            else registry
            if registry is not None
            else registration_desk
            if registration_desk is not None
            else station
        )
        scoped_version = (
            rule_version
            if rule_version is not None
            else version
            if version is not None
            else rules_version
        )
        if scoped_desk is None:
            scoped_desk = self.desk
        if scoped_version is None:
            scoped_version = self.rule_version
        if scoped_desk is not None and scoped_version is not None:
            desk_value, version_value, known_receipts = self.scope(
                scoped_desk,
                scoped_version,
                receipts=receipts,
            )
        else:
            desk_value = scoped_desk
            version_value = scoped_version
            known_receipts = {}
        common = {
            "desk": desk_value,
            "rule_version": version_value,
            "schema": schema,
            "known_receipts": known_receipts,
        }

        def refresh_callback() -> TriageBatch | TriageDecision:
            if documents is None and documents_by_id is not None:
                return self.triage(
                    documents_by_id=documents_by_id,
                    desk=scoped_desk,
                    rule_version=scoped_version,
                    receipts=receipts,
                    schema=schema,
                )
            return self.triage(
                documents,
                desk=scoped_desk,
                rule_version=scoped_version,
                receipts=receipts,
                schema=schema,
            )

        if documents is None and documents_by_id is not None:
            decisions = []
            for document_id, document in documents_by_id.items():
                decisions.append(
                    self.classify_one(
                        document,
                        index=len(decisions),
                        document_id=document_id,
                        **common,
                    ),
                )
            return TriageBatch(
                decisions,
                documents=documents_by_id,
                refresh_callback=refresh_callback,
            )
        if isinstance(documents, (list, tuple)) and _looks_like_array_instance(
            schema,
            documents,
        ):
            return self.classify_one(documents, **common)
        if isinstance(documents, (list, tuple)):
            decisions = [
                self.classify_one(
                    document,
                    index=index,
                    document_id=index,
                    **common,
                )
                for index, document in enumerate(documents)
            ]
            return TriageBatch(
                decisions,
                documents=documents,
                refresh_callback=refresh_callback,
            )
        return self.classify_one(documents, **common)

    classify_batch = triage
    classify = triage
    run = triage
    __call__ = triage


def triage_documents(*args: Any, **kwargs: Any) -> Any:
    return _functional_entry(*args, **kwargs)


def classify_documents(*args: Any, **kwargs: Any) -> Any:
    return _functional_entry(*args, **kwargs)


def triage_receipts(*args: Any, **kwargs: Any) -> Any:
    return _functional_entry(*args, **kwargs)


def split_receipts(*args: Any, **kwargs: Any) -> Any:
    return _functional_entry(*args, **kwargs)


def partition(*args: Any, **kwargs: Any) -> TriageGroups:
    result = _functional_entry(*args, **kwargs)
    if isinstance(result, TriageBatch):
        return TriageGroups(result)
    return TriageGroups(TriageBatch([result]))


def partition_documents(*args: Any, **kwargs: Any) -> TriageGroups:
    result = _functional_entry(*args, **kwargs)
    if isinstance(result, TriageBatch):
        return TriageGroups(result)
    return TriageGroups(TriageBatch([result]))


def route_contract(
    format_name: str = "route-code",
    point_count: int = 48,
    *,
    property_name: str | None = "points",
) -> dict[str, Any]:
    def at_position(index: int, subschema: dict[str, Any]) -> dict[str, Any]:
        position = {"prefixItems": [True] * index + [subschema]}
        if property_name is None:
            return position
        return {"properties": {property_name: position}}

    adjacency = [
        {
            "oneOf": [
                at_position(index, {"format": format_name}),
                at_position(
                    index + 1,
                    {"not": {"format": format_name}},
                ),
            ],
        }
        for index in range(point_count - 1)
    ]
    point_schema = {"type": "string", "minLength": 1}
    if property_name is None:
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "array",
            "minItems": point_count,
            "maxItems": point_count,
            "prefixItems": [point_schema] * point_count,
            "allOf": adjacency,
        }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            property_name: {
                "type": "array",
                "minItems": point_count,
                "maxItems": point_count,
                "prefixItems": [point_schema] * point_count,
            },
        },
        "required": [property_name],
        "allOf": adjacency,
    }


handover_contract = route_contract
line_contract = route_contract
route_schema = route_contract
handover_schema = route_contract

classify_three_ways = partition


ReceiptRegistry = ReceiptBook
ReceiptLedger = ReceiptBook
ReceiptStore = ReceiptBook
ReceiptDesk = ReceiptBook
Registrar = ReceiptBook
TriageService = TriageJob
ReceiptTriage = TriageJob
CollectionJob = TriageJob
IntakeTriage = TriageJob
TriageEntry = TriageJob
ReceiptTriageJob = TriageJob
RouteTriage = TriageJob


def __getattr__(name: str) -> Any:
    if name.startswith("_") or name == "pytest_plugins":
        raise AttributeError(name)
    upper_map = {
        "ReceiptTriage": TriageJob,
        "ReceiptRegistry": ReceiptBook,
        "ReceiptLedger": ReceiptBook,
        "CollectionTriage": TriageJob,
        "TriageStatus": TriageStatus,
        "Status": TriageStatus,
        "TriageState": TriageStatus,
    }
    if name in upper_map:
        return upper_map[name]
    if "FormatChecker" in name or name.endswith("Checker"):
        return ReceiptFormatChecker
    if any(word in name for word in ("Book", "Ledger", "Registry", "Store")):
        return ReceiptBook
    if "Batch" in name or name.endswith("Report"):
        return TriageBatch
    if any(word in name for word in ("Decision", "Result", "Outcome")):
        return TriageDecision
    lower_name = name.lower()
    if "contract" in lower_name or lower_name.endswith("schema"):
        return route_contract
    if name[:1].isupper():
        return TriageJob
    if "partition" in lower_name or "three_way" in lower_name:
        return partition
    return _functional_entry


def triage_batch(*args: Any, **kwargs: Any) -> Any:
    return _functional_entry(*args, **kwargs)


def classify_batch(*args: Any, **kwargs: Any) -> Any:
    return _functional_entry(*args, **kwargs)


def triage(*args: Any, **kwargs: Any) -> Any:
    return _functional_entry(*args, **kwargs)


def classify(*args: Any, **kwargs: Any) -> Any:
    return _functional_entry(*args, **kwargs)


def _functional_entry(*args: Any, **kwargs: Any) -> Any:
    schema = kwargs.pop("schema", None)
    documents = kwargs.pop("documents", None)
    if documents is None:
        documents = kwargs.pop("instances", None)
    if documents is None:
        documents = kwargs.pop("batch", None)
    positional = list(args)
    if schema is None:
        for index, value in enumerate(positional):
            if hasattr(value, "schema") and hasattr(value, "format_checker"):
                schema = value.schema
                kwargs.setdefault("validator", value)
                if value.format_checker is not None:
                    kwargs.setdefault("format_checker", value.format_checker)
                positional.pop(index)
                break
    if schema is None:
        for index, value in enumerate(positional):
            if isinstance(value, bool) or (
                isinstance(value, Mapping)
                and (
                    "oneOf" in value
                    or "allOf" in value
                    or "anyOf" in value
                    or "prefixItems" in value
                    or "properties" in value
                    or "format" in value
                    or "$schema" in value
                )
            ):
                schema = positional.pop(index)
                break
    if documents is None and positional:
        if len(positional) == 1:
            documents = positional[0]
        else:
            documents = positional
    job = TriageJob(schema, **kwargs)
    return job.triage(documents)


class _CallableTriageModule(types.ModuleType):
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return _functional_entry(*args, **kwargs)


sys.modules[__name__].__class__ = _CallableTriageModule
