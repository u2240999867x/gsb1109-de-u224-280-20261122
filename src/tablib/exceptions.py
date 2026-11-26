class TablibException(Exception):
    """Tablib common exception."""


class InvalidDatasetType(TablibException, TypeError):
    """Only Datasets can be added to a Databook."""


class InvalidDimensions(TablibException, ValueError):
    """The size of the column or row doesn't fit the table dimensions."""


class InvalidDatasetIndex(TablibException, IndexError):
    """Outside of Dataset size."""


class HeadersNeeded(TablibException, AttributeError):
    """Header parameter must be given when appending a column to this Dataset."""


class UnsupportedFormat(TablibException, NotImplementedError):
    """Format not supported."""


class MaintenanceError(TablibException, ValueError):
    """Base class for maintainable callable column errors."""


class CorrectionConflictError(MaintenanceError):
    """Two copies changed the same source field to different values."""

    def __init__(self, conflicts=(), message=None):
        self.conflicts = list(conflicts)
        if message is None:
            details = "; ".join(
                f"货号 {item!r} 的字段 {field!r}: {left!r} != {right!r}"
                for item, field, left, right in self.conflicts
            )
            message = f"合表发现来源字段冲突：{details}" if details else "合表发现来源字段冲突"
        super().__init__(message)


class MergeConflictError(CorrectionConflictError):
    """Alias for :class:`CorrectionConflictError`."""


ConflictError = CorrectionConflictError
MergeConflict = CorrectionConflictError


class CircularDependencyError(MaintenanceError):
    """Reference fields form a dependency cycle."""

    def __init__(self, items=(), cycles=(), message=None):
        self.items = list(items)
        self.skus = self.items
        self.products = self.items
        self.cycles = list(cycles)
        self.cycle = self.cycles[0] if self.cycles else self.items
        if message is None:
            message = f"引用关系存在循环依赖，涉及货号: {', '.join(map(str, self.items))}"
        super().__init__(message)


class CircularReferenceError(CircularDependencyError):
    """Alias for :class:`CircularDependencyError`."""


CycleError = CircularDependencyError
DependencyCycleError = CircularDependencyError
CircularDependency = CircularDependencyError


class DerivedCellCalculationError(MaintenanceError, RuntimeError):
    """A maintained calculation callback failed for one cell."""

    def __init__(self, item=None, field=None, original=None, message=None):
        self.item = item
        self.sku = item
        self.product = item
        self.field = field
        self.column = field
        self.original = original
        if message is None:
            message = f"计算货号 {item!r} 的字段 {field!r} 时回调失败"
            if original is not None:
                message = f"{message}: {original}"
        super().__init__(message)


class CalculationCallbackError(DerivedCellCalculationError):
    """Alias for :class:`DerivedCellCalculationError`."""


CalculationError = DerivedCellCalculationError
CellCalculationError = DerivedCellCalculationError
CallbackCalculationError = DerivedCellCalculationError
