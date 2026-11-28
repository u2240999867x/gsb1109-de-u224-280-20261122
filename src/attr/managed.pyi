from collections.abc import Callable, Sequence
from typing import Generic, TypeVar

_T = TypeVar("_T")

class RejectionReason:
    field: str
    raw_value: object
    index: int | slice
    error: Exception

class ManagedRejectionError(Exception):
    reasons: tuple[RejectionReason, ...]

class InconsistentManagedValueError(ValueError):
    values: dict[str, object]

class ManagedList(list[_T], Generic[_T]):
    def __init__(self, values: Sequence[_T] = ...) -> None: ...

ManagedSequence = ManagedList
ManagedDetails = ManagedList

def confirmed(value: _T) -> _T: ...
def current_value(managed_list: ManagedList[_T]) -> ManagedList[_T]: ...
def field(
    *, converter: Callable[[object], object] | None = ...
) -> dict[str, object]: ...
def scan(
    instance: object,
    field_name: str,
    raw_value: object,
    index: int | slice = ...,
) -> None: ...
def rebind(
    instance: object,
    field_name: str,
    managed_list: ManagedList[object],
) -> None: ...
def rejection_reasons(
    *,
    field: str | None = ...,
    raw_value: object = ...,
    index: int | slice | None = ...,
) -> tuple[RejectionReason, ...]: ...
def clear_rejection_reasons() -> None: ...
