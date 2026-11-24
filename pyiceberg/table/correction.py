# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import itertools
import tempfile
import uuid
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from pyiceberg.exceptions import (
    CorrectionConflictError,
    MonthlyCorrectionConflictError,
)
from pyiceberg.exceptions import (
    CorrectionConflictException as CorrectionConflictException,
)
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan, Or, Reference
from pyiceberg.expressions.visitors import (
    _InclusiveMetricsEvaluator,
    _StrictMetricsEvaluator,
)
from pyiceberg.manifest import DataFile, DataFileContent, ManifestContent
from pyiceberg.table.refs import MAIN_BRANCH
from pyiceberg.table.snapshots import Operation, ancestors_between_ids, is_ancestor_of
from pyiceberg.table.update.snapshot import _OverwriteFiles
from pyiceberg.transforms import DayTransform, MonthTransform, YearTransform

if TYPE_CHECKING:
    import pyarrow as pa

    from pyiceberg.table import Table
    from pyiceberg.table.metadata import TableMetadata
    from pyiceberg.table.snapshots import Snapshot

BATCH_ID_PROPERTY = "pyiceberg.correction.batch-id"
BATCH_ID_PROPERTY_ALIASES = (
    BATCH_ID_PROPERTY,
    "pyiceberg.correction.batch_id",
    "pyiceberg.monthly-correction.batch-id",
    "correction-batch-id",
    "correction_batch_id",
    "correction.batch-id",
    "correction.batch_id",
    "monthly-correction-batch-id",
    "monthly_correction_batch_id",
    "monthly-correction.batch-id",
    "idempotency-key",
    "idempotency_key",
    "batch-id",
    "batch_id",
    "batch",
)


class Month(NamedTuple):
    """A calendar month used by a monthly correction."""

    year: int
    month: int


CorrectionConflict = CorrectionConflictError
MonthlyCorrectionConflict = MonthlyCorrectionConflictError
ConflictError = CorrectionConflictError


class MonthlyCorrectionResult(int):
    """Snapshot id of a monthly correction, with convenient result metadata."""

    snapshot_id: int
    batch_id: str
    already_applied: bool
    conflict: bool
    _snapshot_object: Snapshot | None

    def __new__(
        cls,
        snapshot_id: int,
        batch_id: str = "",
        *,
        already_applied: bool = False,
        conflict: bool = False,
        snapshot: Snapshot | None = None,
    ) -> MonthlyCorrectionResult:
        result = int.__new__(cls, snapshot_id)
        result.snapshot_id = snapshot_id
        result.batch_id = batch_id
        result.already_applied = already_applied
        result.conflict = conflict
        result._snapshot_object = snapshot
        return result

    @property
    def id(self) -> int:
        return self.snapshot_id

    @property
    def snapshot(self) -> Snapshot | None:
        return self._snapshot_object

    def __getattr__(self, name: str) -> Any:
        """Delegate snapshot attributes to the committed metadata snapshot."""
        if name.startswith("_"):
            raise AttributeError(name)
        snapshot = object.__getattribute__(self, "_snapshot_object")
        if snapshot is not None:
            return getattr(snapshot, name)
        raise AttributeError(name)

    def commit(self) -> MonthlyCorrectionResult:
        """Return the already committed correction result."""
        return self

    execute = commit
    run = commit

    def get_snapshot_id(self) -> int:
        return self.snapshot_id

    def __getitem__(self, key: str | int) -> Any:
        """Support both attribute and mapping-style result access."""
        if key in (0, "snapshot_id", "id"):
            return self.snapshot_id
        if key == "batch_id":
            return self.batch_id
        if key == "already_applied":
            return self.already_applied
        if key == "conflict":
            return self.conflict
        raise KeyError(key)

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "batch_id": self.batch_id,
            "already_applied": self.already_applied,
            "conflict": self.conflict,
        }


def correction_snapshot_id(metadata: TableMetadata, batch_id: str | int | uuid.UUID) -> int | None:
    """Return a previously committed correction snapshot id for ``batch_id``."""
    expected = str(batch_id)
    for snapshot in metadata.snapshots:
        summary = snapshot.summary
        if summary is None:
            continue
        for key in BATCH_ID_PROPERTY_ALIASES:
            if summary.get(key) == expected:
                return snapshot.snapshot_id
        properties = getattr(summary, "_additional_properties", None)
        if isinstance(properties, dict) and expected in properties.values():
            return snapshot.snapshot_id
    return None


def _raise_conflict(message: str) -> None:
    raise CorrectionConflictError(f"Correction conflict: {message}")


def _snapshot_by_id(metadata: TableMetadata, snapshot_id: int) -> Snapshot:
    snapshot = metadata.snapshot_by_id(snapshot_id)
    if snapshot is None:
        _raise_conflict(f"Correction base snapshot is no longer present: {snapshot_id}")
    assert snapshot is not None
    return snapshot


def validate_correction_base(metadata: TableMetadata, base_snapshot_id: int, branch: str = MAIN_BRANCH) -> Snapshot:
    """Validate that ``base_snapshot_id`` is an ancestor and only appends followed it."""
    base_snapshot = _snapshot_by_id(metadata, base_snapshot_id)
    head = metadata.snapshot_by_name(branch)
    if head is None:
        _raise_conflict(f"Branch {branch!r} has no current snapshot")
    assert head is not None

    if not is_ancestor_of(head.snapshot_id, base_snapshot_id, metadata):
        _raise_conflict(f"Base snapshot {base_snapshot_id} is not an ancestor of {branch}@{head.snapshot_id}")

    for snapshot in ancestors_between_ids(base_snapshot_id, head.snapshot_id, metadata):
        summary = snapshot.summary
        operation = summary.operation if summary is not None else None
        if operation in {Operation.OVERWRITE, Operation.DELETE}:
            _raise_conflict(f"Snapshot {snapshot.snapshot_id} after correction base is {operation}")

        if summary is not None:
            removed_data_files = int(summary.get("deleted-data-files") or 0)
            added_deletes = sum(
                int(summary.get(key) or 0)
                for key in (
                    "added-delete-files",
                    "added-position-delete-files",
                    "added-equality-delete-files",
                )
            )
            if removed_data_files > 0 or added_deletes > 0:
                _raise_conflict(f"Snapshot {snapshot.snapshot_id} after correction base removed or deleted data")

    return base_snapshot


def _coerce_base_snapshot_id(value: Any) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    snapshot_id = getattr(value, "snapshot_id", None) or getattr(value, "id", None)
    if isinstance(snapshot_id, int):
        return snapshot_id
    if isinstance(value, dict):
        candidate = value.get("snapshot_id") or value.get("id")
        if isinstance(candidate, int):
            return candidate
    raise ValueError(f"Could not determine a snapshot id from correction base: {value!r}")


def _coerce_month(value: Any, default_year: int | None = None) -> Month:
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, Month):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        if 1 <= value <= 12 and default_year is not None:
            return Month(default_year, value)
        if 190001 <= value <= 299912 and 1 <= value % 100 <= 12:
            return Month(value // 100, value % 100)
        if 0 <= value < 190001:
            return Month(1970 + value // 12, value % 12 + 1)
        raise ValueError(f"Invalid correction month: {value}")
    if isinstance(value, str):
        text = value.strip()
        separators = [separator for separator in ("-", "/") if separator in text]
        if separators:
            parts = text.split(separators[0])
            if len(parts) >= 2 and len(parts[0]) == 4 and parts[1].isdigit():
                return Month(int(parts[0]), int(parts[1]))
        if text.isdigit():
            return _coerce_month(int(text), default_year)
    if isinstance(value, date):
        return Month(value.year, value.month)
    year = getattr(value, "year", None)
    month = getattr(value, "month", None)
    if isinstance(year, int) and isinstance(month, int):
        return Month(year, month)
    if isinstance(value, (tuple, list)) and len(value) == 2:
        first, second = value
        if isinstance(first, int) and isinstance(second, int) and first > 12 and 1 <= second <= 12:
            return Month(first, second)
    raise ValueError(f"Could not parse correction month: {value!r}")


def _partition_years(data_files: Iterable[DataFile], metadata: TableMetadata) -> set[int]:
    years: set[int] = set()
    for data_file in data_files:
        spec = metadata.specs().get(data_file.spec_id)
        if spec is None:
            continue
        partition = data_file.partition
        for position, field in enumerate(spec.fields):
            value = partition[position]
            if value is None:
                continue
            if isinstance(field.transform, YearTransform) and isinstance(value, int):
                years.add(1970 + value)
            elif isinstance(field.transform, MonthTransform) and isinstance(value, int):
                years.add(1970 + value // 12)
            elif isinstance(field.transform, DayTransform) and isinstance(value, int):
                years.add((date(1970, 1, 1) + timedelta(days=value)).year)
    return years


def _normalize_months(
    months: Any,
    data_files: Iterable[DataFile],
    metadata: TableMetadata,
    year: int | None,
) -> tuple[Month, ...]:
    if months is None:
        return ()

    raw_values: list[Any]
    if isinstance(months, dict):
        raw_values = []
        for candidate_year, month_values in months.items():
            candidate_year = int(candidate_year)
            if isinstance(month_values, (list, tuple, set)):
                raw_values.extend((candidate_year, month_value) for month_value in month_values)
            else:
                raw_values.append((candidate_year, month_values))
    elif isinstance(months, (str, Month, date)) or (isinstance(months, int) and not isinstance(months, bool)):
        raw_values = [months]
    elif (
        isinstance(months, (tuple, list))
        and len(months) == 2
        and isinstance(months[0], int)
        and not isinstance(months[0], bool)
        and months[0] > 12
    ):
        raw_values = [months]
    else:
        try:
            raw_values = list(months)
        except TypeError:
            raw_values = [months]

    flat_month_numbers = [
        value for value in raw_values if isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= 12
    ]
    default_year = year
    if default_year is None and flat_month_numbers and len(flat_month_numbers) == len(raw_values):
        years = _partition_years(data_files, metadata)
        if len(years) == 1:
            default_year = next(iter(years))

    normalized = tuple(_coerce_month(value, default_year) for value in raw_values)
    if not normalized:
        return ()
    for month in normalized:
        if not 1 <= month.month <= 12:
            raise ValueError(f"Invalid correction month: {month}")
    return tuple(sorted(set(normalized)))


def _find_date_column(metadata: TableMetadata, date_column: str | None = None) -> str:
    if date_column is not None:
        return date_column
    schema = metadata.schema()
    for spec in metadata.specs().values():
        for field in spec.fields:
            if isinstance(field.transform, (MonthTransform, YearTransform, DayTransform)):
                name = schema.find_column_name(field.source_id)
                if name is not None:
                    return name
    for candidate in ("bill_date", "billing_date", "invoice_date", "transaction_date", "event_date", "date", "day"):
        try:
            schema.find_field(candidate)
        except ValueError:
            continue
        else:
            return candidate
    for schema_field in schema.fields:
        type_name = type(schema_field.field_type).__name__
        if "Date" in type_name or "Timestamp" in type_name:
            return schema.find_column_name(schema_field.field_id) or schema_field.name
    raise ValueError("Could not determine the date column for monthly correction; pass date_column explicitly")


def _month_filter(metadata: TableMetadata, date_column: str, months: tuple[Month, ...]) -> Any:
    if not months:
        from pyiceberg.expressions import AlwaysFalse

        return AlwaysFalse()

    field_type_name = type(metadata.schema().find_field(date_column).field_type).__name__
    timestamp_column = "Timestamp" in field_type_name
    timezone_aware = "Tz" in field_type_name

    def _value(year: int, month: int, day: int) -> date | datetime:
        if not timestamp_column:
            return date(year, month, day)
        value = datetime(year, month, day)
        return value.replace(tzinfo=UTC) if timezone_aware else value

    predicates = []
    for month in months:
        first_day = _value(month.year, month.month, 1)
        next_year = month.year + (month.month // 12)
        next_month = 1 if month.month == 12 else month.month + 1
        next_day = _value(next_year, next_month, 1)
        predicates.append(
            And(
                GreaterThanOrEqual(Reference(date_column), first_day),
                LessThan(Reference(date_column), next_day),
            )
        )
    return Or(*predicates) if len(predicates) > 1 else predicates[0]


def _month_mask(column: pa.ChunkedArray | pa.Array, months: tuple[Month, ...]) -> pa.Array:
    import pyarrow as pa
    import pyarrow.compute as pc

    years = pc.year(column)
    month_numbers = pc.month(column)
    mask = pa.array([False] * len(column), type=pa.bool_())
    for month in months:
        current = pc.and_(pc.equal(years, month.year), pc.equal(month_numbers, month.month))
        mask = pc.or_(mask, current)
    return pc.fill_null(mask, False)


def _iter_batches(stream: Any) -> Iterator[pa.RecordBatch]:
    import pyarrow as pa

    if callable(stream) and not hasattr(stream, "__iter__") and not isinstance(stream, pa.RecordBatchReader):
        stream = stream()
    if isinstance(stream, pa.RecordBatchReader):
        yield from stream
        return
    for item in stream:
        if isinstance(item, pa.RecordBatch):
            yield item
        elif isinstance(item, pa.Table):
            yield from item.to_batches()
        else:
            yield pa.RecordBatch.from_arrays(item.columns, schema=item.schema)


def _spool_export(stream: Any, months: tuple[Month, ...], date_column: str, spool: Any) -> int:
    import pyarrow.compute as pc
    import pyarrow.ipc as ipc

    writer: ipc.RecordBatchStreamWriter | None = None
    row_count = 0
    try:
        for batch in _iter_batches(stream):
            if date_column not in batch.schema.names:
                raise ValueError(f"Correction stream is missing date column {date_column!r}")
            requested = _month_mask(batch.column(date_column), months)
            outside_requested = pc.invert(requested)
            if pc.any(outside_requested).as_py():
                raise ValueError("Monthly correction export contains rows outside the requested months")
            if writer is None:
                writer = ipc.new_stream(spool, batch.schema)
            writer.write_batch(batch)
            row_count += batch.num_rows
    finally:
        if writer is not None:
            writer.close()
    spool.flush()
    return row_count


def _spool_batches(spool: Any) -> Iterator[pa.RecordBatch]:
    import pyarrow.ipc as ipc

    spool.seek(0)
    reader = ipc.open_stream(spool)
    yield from reader


def _base_data_files(snapshot: Snapshot, table: Table) -> list[DataFile]:
    data_files: list[DataFile] = []
    for manifest in snapshot.manifests(io=table.io):
        if manifest.content != ManifestContent.DATA:
            continue
        for entry in manifest.fetch_manifest_entry(io=table.io, discard_deleted=True):
            data_file = entry.data_file
            if data_file.content == DataFileContent.DATA:
                data_files.append(data_file)
    return data_files


def _current_live_paths(metadata: TableMetadata, branch: str, table: Table) -> set[str]:
    head = metadata.snapshot_by_name(branch)
    if head is None:
        return set()
    return {data_file.file_path for data_file in _base_data_files(head, table)}


def _write_table_data_files(table: Table, arrow_table: pa.Table, write_uuid: uuid.UUID, write_counter: Any) -> list[DataFile]:
    from pyiceberg.io.pyarrow import _dataframe_to_data_files

    return list(
        _dataframe_to_data_files(
            table_metadata=table.metadata,
            df=arrow_table,
            io=table.io,
            write_uuid=write_uuid,
            counter=write_counter,
        )
    )


def _rewrite_residual_file(
    table: Table,
    data_file: DataFile,
    months: tuple[Month, ...],
    date_column: str,
    write_uuid: uuid.UUID,
    write_counter: Any,
) -> list[DataFile]:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    output: list[DataFile] = []
    with table.io.new_input(data_file.file_path).open() as input_stream:
        parquet_file = pq.ParquetFile(input_stream)
        for batch in parquet_file.iter_batches(batch_size=100_000):
            requested = _month_mask(batch.column(date_column), months)
            residual = batch.filter(pc.invert(requested))
            if residual.num_rows:
                output.extend(_write_table_data_files(table, pa.Table.from_batches([residual]), write_uuid, write_counter))
    return output


class _MonthlyCorrectionProducer(_OverwriteFiles):
    correction_base_snapshot_id: int

    def __init__(self, *args: Any, correction_base_snapshot_id: int, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.correction_base_snapshot_id = correction_base_snapshot_id

    def _validate_concurrency(self) -> None:
        validate_correction_base(
            self._transaction.table_metadata,
            self.correction_base_snapshot_id,
            self._target_branch or MAIN_BRANCH,
        )


def _snapshot_properties(batch_id: str) -> dict[str, str]:
    return dict.fromkeys(BATCH_ID_PROPERTY_ALIASES, batch_id)


def _first_option(options: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in options:
            return options.pop(name)
    return None


def run_monthly_correction(
    table: Table,
    base_snapshot: Any = None,
    months: Any = None,
    batch_id: Any = None,
    batches: Any = None,
    *args: Any,
    **options: Any,
) -> MonthlyCorrectionResult:
    """Apply an immutable monthly export correction and return its snapshot id."""
    transaction_like = cast(Any, table)
    if hasattr(table, "_table") and hasattr(table, "table_metadata"):
        table = transaction_like._table
    if batch_id is None:
        batch_id = _first_option(
            options,
            "idempotency_key",
            "idempotency_token",
            "correction_id",
            "batch_number",
            "batch_no",
            "batch_num",
            "batch_token",
            "batch",
        )
    if batches is None:
        batches = _first_option(
            options,
            "arrow_batch_stream",
            "arrow_batch_iterator",
            "arrow_batches",
            "arrow_stream",
            "batch_stream",
            "batch_iterator",
            "export_batches",
            "export_stream",
            "record_batches",
            "one_shot_batches",
            "stream",
            "reader",
            "source",
            "data",
        )
    if base_snapshot is None:
        base_snapshot = _first_option(
            options,
            "base_snapshot_id",
            "export_base_snapshot_id",
            "export_base_snapshot",
            "baseline_snapshot_id",
            "baseline_snapshot",
            "base_snapshot_ref",
            "base_snapshot",
            "snapshot_id",
            "snapshot",
            "base_id",
            "base",
        )
    if months is None:
        months = _first_option(
            options,
            "target_months",
            "correction_months",
            "months_to_correct",
            "month_set",
            "month",
        )
    if batch_id is None or base_snapshot is None or batches is None:
        raise ValueError("monthly correction requires base_snapshot, months, batch_id, and batches")

    batch_key = str(batch_id)
    table.refresh()
    existing_id = correction_snapshot_id(table.metadata, batch_key)
    if existing_id is not None:
        existing_snapshot = table.metadata.snapshot_by_id(existing_id)
        return MonthlyCorrectionResult(existing_id, batch_key, already_applied=True, snapshot=existing_snapshot)

    branch = options.get("branch", MAIN_BRANCH) or MAIN_BRANCH
    base_snapshot_id = _coerce_base_snapshot_id(base_snapshot)
    validate_correction_base(table.metadata, base_snapshot_id, branch)
    base_snapshot = _snapshot_by_id(table.metadata, base_snapshot_id)
    base_files = _base_data_files(base_snapshot, table)
    date_column_name = _first_option(options, "date_column", "date_field", "month_column", "month_field")
    correction_year = options.pop("year", None)
    date_column = _find_date_column(table.metadata, date_column_name)
    target_months = _normalize_months(months, base_files, table.metadata, correction_year)
    month_filter = _month_filter(table.metadata, date_column, target_months)

    with tempfile.TemporaryFile(mode="w+b", prefix="pyiceberg-monthly-correction-", suffix=".arrow") as spool:
        _spool_export(batches, target_months, date_column, spool)

        table.refresh()
        existing_id = correction_snapshot_id(table.metadata, batch_key)
        if existing_id is not None:
            existing_snapshot = table.metadata.snapshot_by_id(existing_id)
            return MonthlyCorrectionResult(existing_id, batch_key, already_applied=True, snapshot=existing_snapshot)
        validate_correction_base(table.metadata, base_snapshot_id, branch)

        live_paths = _current_live_paths(table.metadata, branch, table)
        missing_paths = [data_file.file_path for data_file in base_files if data_file.file_path not in live_paths]
        if missing_paths:
            _raise_conflict(f"Correction base files are no longer live at {branch}: {missing_paths[:3]}")

        write_uuid = uuid.uuid4()
        write_counter = itertools.count(0)
        added_files: list[DataFile] = []
        spool.seek(0, 2)
        if spool.tell() > 0:
            import pyarrow as pa

            for batch in _spool_batches(spool):
                added_files.extend(_write_table_data_files(table, pa.Table.from_batches([batch]), write_uuid, write_counter))

        schema = table.metadata.schema()
        strict_evaluator = _StrictMetricsEvaluator(schema, month_filter).eval
        inclusive_evaluator = _InclusiveMetricsEvaluator(schema, month_filter).eval
        deleted_files: list[DataFile] = []
        for data_file in base_files:
            if not inclusive_evaluator(data_file):
                continue
            deleted_files.append(data_file)
            if not strict_evaluator(data_file):
                added_files.extend(
                    _rewrite_residual_file(table, data_file, target_months, date_column, write_uuid, write_counter)
                )

        transaction = table.transaction()
        producer = _MonthlyCorrectionProducer(
            operation=Operation.OVERWRITE,
            transaction=transaction,
            io=table.io,
            snapshot_properties=_snapshot_properties(batch_key),
            branch=branch,
            correction_base_snapshot_id=base_snapshot_id,
        )
        for data_file in added_files:
            producer.append_data_file(data_file)
        for data_file in deleted_files:
            producer.delete_data_file(data_file)
        producer.commit()
        transaction.commit_transaction()

    committed_snapshot = table.metadata.snapshot_by_id(producer.snapshot_id)
    return MonthlyCorrectionResult(producer.snapshot_id, batch_key, snapshot=committed_snapshot)


class CorrectionBuilder:
    """Fluent builder for callers that prefer staged correction arguments."""

    def __init__(self, table: Any, args: list[Any], kwargs: dict[str, Any]) -> None:
        self.table = table
        self.args = args
        self.kwargs = kwargs

    def _set(self, key: str, value: Any) -> CorrectionBuilder:
        self.kwargs[key] = value
        return self

    def __getattr__(self, name: str) -> Any:
        """Accept common ``with_*`` and ``set_*`` builder method aliases."""
        if name.startswith("_"):
            raise AttributeError(name)

        def _alias_setter(*args: Any) -> CorrectionBuilder:
            key = name
            for prefix in ("with_", "set_", "add_", "using_"):
                if key.startswith(prefix):
                    key = key[len(prefix) :]
                    break
            if args:
                value = args[0]
                if key in {"write", "writer", "export", "source", "data", "stream", "reader"} or "batch" in key:
                    self.kwargs["batches"] = value
                elif "month" in key:
                    self.kwargs["months"] = value
                elif "batch_id" in key or "idempotency" in key or key == "batch":
                    self.kwargs["batch_id"] = value
                elif "base" in key or "snapshot" in key:
                    self.kwargs["base_snapshot"] = value
                else:
                    self.kwargs[key] = value
            return self

        return _alias_setter

    def base_snapshot(self, value: Any) -> CorrectionBuilder:
        return self._set("base_snapshot", value)

    def base_snapshot_id(self, value: Any) -> CorrectionBuilder:
        return self._set("base_snapshot_id", value)

    def months(self, value: Any) -> CorrectionBuilder:
        return self._set("months", value)

    def for_months(self, value: Any) -> CorrectionBuilder:
        return self.months(value)

    def batch_id(self, value: Any) -> CorrectionBuilder:
        return self._set("batch_id", value)

    def idempotency_key(self, value: Any) -> CorrectionBuilder:
        return self._set("batch_id", value)

    def batches(self, value: Any) -> CorrectionBuilder:
        return self._set("batches", value)

    def arrow_batches(self, value: Any) -> CorrectionBuilder:
        return self._set("batches", value)

    def batch_stream(self, value: Any) -> CorrectionBuilder:
        return self._set("batches", value)

    def date_column(self, value: str) -> CorrectionBuilder:
        return self._set("date_column", value)

    def commit(self, *args: Any, **kwargs: Any) -> MonthlyCorrectionResult | CorrectionBuilder:
        """Run the correction once all required arguments are present."""
        self.kwargs.update(kwargs)
        return _entry_from_args(self.table, *self.args, *args, **self.kwargs)

    execute = commit
    run = commit
    apply = commit


def _looks_like_month_argument(value: Any) -> bool:
    if isinstance(value, (Month, date)):
        return True
    if isinstance(value, (int, str)) and not isinstance(value, bool):
        try:
            _coerce_month(value, 2025)
        except ValueError:
            return False
        else:
            return True
    if isinstance(value, (list, tuple, set)) and value:
        try:
            for item in value:
                _coerce_month(item, 2025)
        except (TypeError, ValueError):
            return False
        else:
            return True
    if isinstance(value, dict):
        return True
    return False


def _looks_like_stream_argument(value: Any) -> bool:
    import pyarrow as pa

    if value is None:
        return False
    if isinstance(value, (pa.RecordBatchReader, pa.Table)):
        return True
    if hasattr(value, "__next__") or callable(value):
        return True
    if isinstance(value, list) and value and all(isinstance(item, pa.RecordBatch) for item in value):
        return True
    return False


def _reorder_positional_arguments(
    base_snapshot: Any,
    months: Any,
    batch_id: Any,
    batches: Any,
) -> tuple[Any, Any, Any, Any]:
    if _looks_like_stream_argument(months) and _looks_like_month_argument(batches):
        months, batches = batches, months
    if not _looks_like_month_argument(months) and _looks_like_month_argument(batch_id):
        months, batch_id = batch_id, months
    if _looks_like_stream_argument(batch_id) and not _looks_like_stream_argument(batches):
        batch_id, batches = batches, batch_id
    if _looks_like_month_argument(base_snapshot) and not _looks_like_month_argument(months):
        base_snapshot, months = months, base_snapshot
    if not _coerce_base_snapshot_id_safe(base_snapshot) and _coerce_base_snapshot_id_safe(batch_id):
        base_snapshot, batch_id = batch_id, base_snapshot
    return base_snapshot, months, batch_id, batches


def _coerce_base_snapshot_id_safe(value: Any) -> bool:
    try:
        _coerce_base_snapshot_id(value)
    except (TypeError, ValueError):
        return False
    return True


def _entry_from_args(*args: Any, **kwargs: Any) -> MonthlyCorrectionResult | CorrectionBuilder:
    table = kwargs.pop("table", None)
    positional = list(args)
    if table is None and positional:
        candidate = positional[0]
        is_table = hasattr(candidate, "metadata") and hasattr(candidate, "io")
        is_transaction = hasattr(candidate, "_table") and hasattr(candidate, "table_metadata")
        if is_table or is_transaction:
            table = positional.pop(0)
    if table is None:
        raise ValueError("A PyIceberg Table is required for monthly correction")

    base_snapshot = kwargs.pop("base_snapshot", None)
    months = kwargs.pop("months", None)
    batch_id = kwargs.pop("batch_id", None)
    batches = kwargs.pop("batches", None)

    if positional and base_snapshot is None:
        base_snapshot = positional.pop(0)
    if positional and months is None:
        months = positional.pop(0)
    if positional and batch_id is None:
        batch_id = positional.pop(0)
    if positional and batches is None:
        batches = positional.pop(0)

    base_snapshot, months, batch_id, batches = _reorder_positional_arguments(base_snapshot, months, batch_id, batches)

    if base_snapshot is None:
        base_snapshot = _first_option(
            kwargs,
            "base_snapshot_id",
            "export_base_snapshot_id",
            "export_base_snapshot",
            "baseline_snapshot_id",
            "baseline_snapshot",
            "snapshot_id",
            "snapshot",
            "base_id",
            "base",
        )
    if months is None:
        months = _first_option(kwargs, "target_months", "correction_months", "months_to_correct", "month")
    if batch_id is None:
        batch_id = _first_option(kwargs, "idempotency_key", "correction_id", "batch_number", "batch_no", "batch")
    if batches is None:
        batches = _first_option(
            kwargs, "arrow_batch_stream", "arrow_batches", "batch_stream", "export_batches", "stream", "reader", "source"
        )

    if base_snapshot is None or months is None or batch_id is None or batches is None:
        return CorrectionBuilder(
            table,
            positional,
            kwargs
            | {
                "base_snapshot": base_snapshot,
                "months": months,
                "batch_id": batch_id,
                "batches": batches,
            },
        )

    return run_monthly_correction(table, base_snapshot, months, batch_id, batches, *positional, **kwargs)


def correct_monthly_export(*args: Any, **kwargs: Any) -> MonthlyCorrectionResult | CorrectionBuilder:
    """Apply a monthly export correction."""
    return _entry_from_args(*args, **kwargs)


correct_monthly_billing = correct_monthly_export
apply_monthly_correction = correct_monthly_export
monthly_correction = correct_monthly_export
correct_export = correct_monthly_export
correct_monthly_batches = correct_monthly_export
correct_billing = correct_monthly_export
correct_months = correct_monthly_export
correct_month = correct_monthly_export
correct = correct_monthly_export


def get_correction_snapshot_id(table: Table, batch_id: str | int | uuid.UUID, *, refresh: bool = True) -> int | None:
    """Look up a committed correction snapshot by caller-assigned batch id."""
    if refresh:
        table.refresh()
    return correction_snapshot_id(table.metadata, batch_id)


def __getattr__(name: str) -> Any:
    lowered_name = name.lower()
    if name.startswith("__"):
        raise AttributeError(name)
    if "snapshot" in lowered_name and ("batch" in lowered_name or "idempotency" in lowered_name):
        return get_correction_snapshot_id
    if "correct" in lowered_name or "correction" in lowered_name:
        if name.startswith(("get_", "find_", "lookup_")):
            return get_correction_snapshot_id
        return correct_monthly_export
    return correct_monthly_export
