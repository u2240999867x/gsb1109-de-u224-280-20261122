# SPDX-License-Identifier: MIT

"""Transactional bindings for shared mutable field values."""

from __future__ import annotations

import weakref

from collections.abc import Callable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass


_candidate_views: ContextVar[dict[int, ManagedList] | None] = ContextVar(
    "attrs_managed_candidate_views", default=None
)
_rejection_log: ContextVar[tuple[RejectionReason, ...]] = ContextVar(
    "attrs_managed_rejection_log", default=()
)


@dataclass(frozen=True)
class RejectionReason:
    field: str
    raw_value: object
    index: int | slice
    error: Exception


class ManagedRejectionError(Exception):
    def __init__(self, reasons: Sequence[RejectionReason]):
        self.reasons = tuple(reasons)
        super().__init__(repr(self.reasons))


class InconsistentManagedValueError(ValueError):
    def __init__(self, values: dict[str, object]):
        self.values = dict(values)
        super().__init__(repr(self.values))


class _ConfirmedValue:
    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value


def confirmed(value):
    return _ConfirmedValue(value)


class Binding:
    __slots__ = ("converter", "field", "field_name", "instance", "owner")

    def __init__(self, instance, field, owner, converter):
        self.instance = weakref.ref(instance)
        self.field = field
        self.field_name = field.name
        self.owner = weakref.ref(owner)
        self.converter = converter

    def owner_value(self):
        return self.owner()

    def instance_value(self):
        return self.instance()

    def convert(self, value, instance, field):
        from ._make import Converter

        if self.converter is None:
            return value
        if not isinstance(self.converter, Converter):
            return self.converter(value)
        return self.converter(value, instance, field)


class ManagedList(list):
    def __init__(self, values=()):
        super().__init__(values)
        self._bindings: list[Binding] = []

    def _find_binding(self, instance, field):
        for binding in self._bindings:
            if (
                binding.owner_value() is not None
                and binding.instance_value() is instance
                and binding.owner_value() is type(instance)
                and binding.field_name == field.name
            ):
                return binding
        return None

    def _active_bindings(self):
        active = []
        stale = []
        for binding in self._bindings:
            if (
                binding.owner_value() is None
                or binding.instance_value() is None
            ):
                stale.append(binding)
            else:
                active.append(binding)
        for binding in stale:
            self._bindings.remove(binding)
        return active

    def register(self, instance, field, converter):
        self._active_bindings()
        binding = self._find_binding(instance, field)
        created = binding is None
        if binding is None:
            self._bindings.append(
                Binding(instance, field, type(instance), converter)
            )
        else:
            binding.converter = converter
        return self, created

    def unregister(self, instance, field):
        binding = self._find_binding(instance, field)
        if binding is not None:
            self._bindings.remove(binding)
        return self

    def scan(self, instance, field, raw_value, index):
        binding = self._find_binding(instance, field)
        if binding is None:
            msg = f"'{field.name}' is not bound to this managed list"
            raise ValueError(msg)

        common_candidate = self._candidate(raw_value, index)
        super().__setitem__(index, common_candidate[index])

    def __setitem__(self, index, raw_value):
        if not self._active_bindings():
            super().__setitem__(index, raw_value)
            return
        try:
            candidate = self._candidate(raw_value, index)
        except ManagedRejectionError as error:
            _remember(error)
            raise
        super().__setitem__(index, candidate[index])

    def _candidate(self, raw_value, index):
        participants = self._active_bindings()
        candidates: dict[Binding, list] = {}
        reasons: list[RejectionReason] = []

        for participant in participants:
            try:
                candidate = list(self)
                candidate[index] = self._convert_replacement(
                    participant, raw_value, index
                )
                candidates[participant] = candidate
            except Exception as error:  # noqa: BLE001, PERF203
                reasons.append(
                    RejectionReason(
                        participant.field_name, raw_value, index, error
                    )
                )

        if not reasons:
            field_names = [
                participant.field_name for participant in candidates
            ]
            duplicate_field_names = len(field_names) != len(set(field_names))
            if isinstance(index, slice):
                values = {
                    self._conflict_key(participant, duplicate_field_names): (
                        tuple(candidate)
                    )
                    for participant, candidate in candidates.items()
                }
            else:
                values = {
                    self._conflict_key(
                        participant, duplicate_field_names
                    ): candidate[index]
                    for participant, candidate in candidates.items()
                }
            if len(set(values.values())) > 1:
                error = InconsistentManagedValueError(values)
                reasons = [
                    RejectionReason(
                        participant.field_name, raw_value, index, error
                    )
                    for participant in candidates
                ]

        if reasons:
            raise ManagedRejectionError(reasons)

        common_candidate = next(iter(candidates.values()))
        view = self._view(common_candidate)
        previous_views = _candidate_views.get()
        views = dict(previous_views or {})
        views[id(self)] = view
        token = _candidate_views.set(views)
        try:
            for participant in participants:
                validator = participant.field.validator
                if validator is None:
                    continue
                try:
                    validator(
                        participant.instance_value(), participant.field, view
                    )
                except Exception as error:  # noqa: BLE001
                    reasons.append(
                        RejectionReason(
                            participant.field_name, raw_value, index, error
                        )
                    )
        finally:
            _candidate_views.reset(token)

        if reasons:
            raise ManagedRejectionError(reasons)

        return common_candidate

    def _conflict_key(self, binding, include_owner):
        if include_owner:
            owner = binding.owner_value()
            return f"{owner.__name__}.{binding.field_name}"
        return binding.field_name

    def _convert_replacement(self, binding, raw_value, index):
        participant_instance = binding.instance_value()
        if isinstance(index, slice):
            old_values = self[index]
            converted = []
            for position, item in enumerate(raw_value):
                if isinstance(item, _ConfirmedValue):
                    converted.append(old_values[position])
                else:
                    converted.append(
                        binding.convert(
                            item, participant_instance, binding.field
                        )
                    )
            return converted
        if isinstance(raw_value, _ConfirmedValue):
            return self[index]
        return binding.convert(raw_value, participant_instance, binding.field)

    def _view(self, values):
        view = ManagedList(values)
        view._bindings = self._bindings
        return view

    def __iter__(self):
        view = _candidate_views.get()
        if view is not None and id(self) in view:
            return iter(view[id(self)])
        return super().__iter__()

    def __getitem__(self, index):
        view = _candidate_views.get()
        if view is not None and id(self) in view:
            return view[id(self)][index]
        return super().__getitem__(index)


ManagedSequence = ManagedList
ManagedDetails = ManagedList


def current_value(managed_list: ManagedList) -> ManagedList:
    views = _candidate_views.get()
    if views is not None and id(managed_list) in views:
        return views[id(managed_list)]
    return managed_list


def _managed_converter(value, instance, field):
    if not isinstance(value, ManagedList):
        msg = f"'{field.name}' expects a ManagedList"
        raise TypeError(msg)
    existing = value._find_binding(instance, field)
    if existing is None:
        value.register(
            instance,
            field,
            field.metadata.get("attrs:managed-converter"),
        )
    return value


def _on_setattr(instance, field, value):
    if not isinstance(value, ManagedList):
        msg = f"'{field.name}' expects a ManagedList"
        raise TypeError(msg)

    previous = getattr(instance, field.name, None)
    candidate, created = value.register(
        instance,
        field,
        field.metadata.get("attrs:managed-converter"),
    )
    try:
        if field.validator is not None:
            try:
                field.validator(instance, field, current_value(candidate))
            except Exception as error:
                raise ManagedRejectionError(
                    (RejectionReason(field.name, value, None, error),)
                ) from error
        if isinstance(previous, ManagedList) and previous is not value:
            previous.unregister(instance, field)
    except ManagedRejectionError as error:
        if created:
            value.unregister(instance, field)
        _remember(error)
        raise
    else:
        return candidate


def field(*, converter: Callable | None = None) -> dict[str, object]:
    from ._make import Converter

    return {
        "converter": Converter(
            _managed_converter, takes_self=True, takes_field=True
        ),
        "metadata": {
            "attrs:managed": True,
            "attrs:managed-converter": converter,
        },
        "on_setattr": _on_setattr,
    }


def _find_field(instance, field_name: str):
    from ._make import fields_dict

    try:
        return fields_dict(type(instance))[field_name]
    except (TypeError, KeyError):
        raise AttributeError(field_name) from None


def _remember(error: ManagedRejectionError):
    _rejection_log.set((*_rejection_log.get(), *error.reasons))


def scan(instance, field_name: str, raw_value, index: int | slice = 0) -> None:
    field = _find_field(instance, field_name)
    managed_list = current_value(getattr(instance, field_name))
    try:
        managed_list.scan(instance, field, raw_value, index)
    except ManagedRejectionError as error:
        _remember(error)
        raise


def rebind(instance, field_name: str, managed_list: ManagedList) -> None:
    setattr(instance, field_name, managed_list)


def rejection_reasons(
    *,
    field: str | None = None,
    raw_value: object = ...,
    index: int | slice | None = None,
) -> tuple[RejectionReason, ...]:
    return tuple(
        reason
        for reason in _rejection_log.get()
        if (field is None or reason.field == field)
        and (raw_value is ... or reason.raw_value == raw_value)
        and (index is None or reason.index == index)
    )


def clear_rejection_reasons() -> None:
    _rejection_log.set(())
