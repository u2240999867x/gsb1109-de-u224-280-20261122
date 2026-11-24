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
"""Incremental changelog scan.

Unlike :class:`pyiceberg.table.IncrementalAppendScan`, which only reports rows added by
``append`` snapshots, the changelog scan reconciles the live row multiset between two snapshots
of the same branch. It emits one ``insert`` event for every row that enters the filtered row set
and one ``delete`` event for every row that leaves it, once per snapshot in the range.

Rows are matched without a business key: identical rows are tracked by multiplicity, so duplicate
rows with the same values are counted individually. File-only rewrites (compactions, relocations)
keep the row multiset unchanged and therefore produce no events, even though their data files are
removed and re-added in the manifests.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from enum import StrEnum
from functools import cached_property
from typing import TYPE_CHECKING, Any, TypeVar

from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator, bind, inclusive_projection, manifest_evaluator
from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFile, DataFileContent, ManifestContent, ManifestEntryStatus, ManifestFile
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.metadata_columns import project_schema
from pyiceberg.table.snapshots import (
    Snapshot,
    ancestors_between_ids,
    is_ancestor_of,
    is_parent_ancestor_of,
)
from pyiceberg.typedef import EMPTY_DICT, Properties, _is_nan, _same_value

if TYPE_CHECKING:
    import pyarrow as pa

    from pyiceberg.encryption.manager import EncryptionManager

logger = logging.getLogger(__name__)

ALWAYS_TRUE = AlwaysTrue()


class ChangelogOperation(StrEnum):
    """The kind of change a changelog row represents."""

    INSERT = "insert"
    DELETE = "delete"


CHANGE_TYPE_COLUMN = "change_type"


@dataclass(frozen=True)
class ChangelogScanTask:
    """A planned changelog event group for one snapshot and change type.

    Attributes:
        snapshot_id: The snapshot that produced the event.
        change_type: Whether the group holds inserted or deleted rows.
        data_files: The data files that contribute rows to the event group.
    """

    snapshot_id: int
    change_type: ChangelogOperation
    data_files: tuple[DataFile, ...]


class IncrementalChangelogScan:
    """An incremental scan that reports row-level inserts and deletes between two snapshots.

    Args:
        from_snapshot_id:
            ID of the snapshot establishing the starting row multiset. When ``from_snapshot_inclusive``
            is False it is the exclusive boundary; when True, the scan starts at that snapshot and its
            parent's row multiset is used as the boundary. If None, the scan starts from an empty table.
        from_snapshot_inclusive:
            Whether ``from_snapshot_id`` belongs to the scanned range.
        to_snapshot_id:
            Optional ID of the snapshot to end at, inclusively. Omitting it defaults to the current snapshot.
        row_filter:
            A string or BooleanExpression describing the rows whose changes should be reported. Columns used
            only for filtering need not be part of the output projection.
        selected_fields:
            The column names to return, in addition to the appended ``change_type`` column.
        case_sensitive:
            If True, column matching is case sensitive.
        options:
            Additional table properties as a dictionary of string key value pairs.
        limit:
            An integer limiting the number of returned rows. If None, all matching rows are returned.
        encryption_manager:
            Decrypts the manifest lists of encrypted snapshots.
    """

    table_metadata: TableMetadata
    io: FileIO
    row_filter: BooleanExpression
    selected_fields: tuple[str, ...]
    case_sensitive: bool
    options: Properties
    limit: int | None
    encryption_manager: EncryptionManager | None
    from_snapshot_id: int | None
    from_snapshot_inclusive: bool
    to_snapshot_id: int | None

    def __init__(
        self,
        table_metadata: TableMetadata,
        io: FileIO,
        row_filter: str | BooleanExpression = ALWAYS_TRUE,
        selected_fields: tuple[str, ...] = ("*",),
        case_sensitive: bool = True,
        from_snapshot_id: int | None = None,
        from_snapshot_inclusive: bool = False,
        to_snapshot_id: int | None = None,
        options: Properties = EMPTY_DICT,
        limit: int | None = None,
        encryption_manager: EncryptionManager | None = None,
    ) -> None:
        from pyiceberg.table import _parse_row_filter

        self.table_metadata = table_metadata
        self.io = io
        self.row_filter = _parse_row_filter(row_filter)
        self.selected_fields = selected_fields
        self.case_sensitive = case_sensitive
        self.options = options
        self.limit = limit
        self.encryption_manager = encryption_manager
        self.from_snapshot_id = from_snapshot_id
        self.from_snapshot_inclusive = from_snapshot_inclusive
        self.to_snapshot_id = to_snapshot_id

    def update(self: ICS, **overrides: Any) -> ICS:
        """Create a copy of this scan with updated fields."""
        from inspect import signature

        params = signature(type(self).__init__).parameters.keys() - {"self"}
        kwargs = {param: getattr(self, param) for param in params}
        return type(self)(**{**kwargs, **overrides})

    def select(self: ICS, *field_names: str) -> ICS:
        if "*" in self.selected_fields:
            return self.update(selected_fields=field_names)
        return self.update(selected_fields=tuple(set(self.selected_fields).intersection(set(field_names))))

    def filter(self: ICS, expr: str | BooleanExpression) -> ICS:
        from pyiceberg.expressions import And
        from pyiceberg.table import _parse_row_filter

        return self.update(row_filter=And(self.row_filter, _parse_row_filter(expr)))

    def with_case_sensitive(self: ICS, case_sensitive: bool = True) -> ICS:
        return self.update(case_sensitive=case_sensitive)

    def from_snapshot_id_exclusive(self: ICS, from_snapshot_id: int) -> ICS:
        """Return a copy that starts (exclusively) from the given snapshot ID."""
        return self.update(from_snapshot_id=from_snapshot_id, from_snapshot_inclusive=False)

    def from_snapshot_id_inclusive(self: ICS, from_snapshot_id: int) -> ICS:
        """Return a copy that starts (inclusively) from the given snapshot ID."""
        return self.update(from_snapshot_id=from_snapshot_id, from_snapshot_inclusive=True)

    def to_snapshot_id_inclusive(self: ICS, to_snapshot_id: int) -> ICS:
        """Return a copy that ends (inclusively) at the given snapshot ID."""
        return self.update(to_snapshot_id=to_snapshot_id)

    def projection(self) -> Schema:
        """Return the projected row schema, excluding the appended ``change_type`` column."""
        return project_schema(
            self.table_metadata.schema(), self.selected_fields, self.case_sensitive, self.table_metadata.format_version
        )

    @cached_property
    def _bound_row_filter(self) -> BooleanExpression:
        return bind(self.table_metadata.schema(), self.row_filter, case_sensitive=self.case_sensitive)

    @cached_property
    def _metrics_evaluator(self) -> _InclusiveMetricsEvaluator:
        return _InclusiveMetricsEvaluator(self.table_metadata.schema(), self.row_filter, self.case_sensitive)

    def _manifest_may_match(self, manifest: ManifestFile) -> bool:
        spec = self.table_metadata.specs()[manifest.partition_spec_id]
        projected_filter = inclusive_projection(self.table_metadata.schema(), spec, self.case_sensitive)(self.row_filter)
        return manifest_evaluator(spec, self.table_metadata.schema(), projected_filter, self.case_sensitive)(manifest)

    def _has_no_snapshots(self) -> bool:
        # With neither bound set, an empty table (no current snapshot) has nothing to scan. An explicit
        # start with no resolvable end still falls through to _resolve_range and raises there.
        return self.from_snapshot_id is None and self.to_snapshot_id is None and self.table_metadata.current_snapshot() is None

    def _resolve_range(self) -> tuple[Snapshot | None, list[Snapshot]]:
        """Resolve the range to ``(boundary_snapshot, in_range_snapshots)``.

        The boundary snapshot establishes the starting row multiset and is not part of the range.
        ``None`` represents an empty starting table. In-range snapshots are returned oldest first.
        """
        if self.to_snapshot_id is not None:
            to_snapshot = self.table_metadata.snapshot_by_id(self.to_snapshot_id)
            if to_snapshot is None:
                raise ValueError(f"End snapshot not found in table metadata: {self.to_snapshot_id}")
        else:
            to_snapshot = self.table_metadata.current_snapshot()
            if to_snapshot is None:
                raise ValueError("End snapshot is not set and table has no current snapshot")

        to_snapshot_id = to_snapshot.snapshot_id

        if self.from_snapshot_id is None:
            return None, list(ancestors_between_ids(None, to_snapshot_id, self.table_metadata))[::-1]

        from_snapshot_id = self.from_snapshot_id

        if self.from_snapshot_inclusive:
            from_snapshot = self.table_metadata.snapshot_by_id(from_snapshot_id)
            if from_snapshot is None:
                raise ValueError(f"Start snapshot (inclusive) not found in table metadata: {from_snapshot_id}")
            if not is_ancestor_of(to_snapshot_id, from_snapshot_id, self.table_metadata):
                raise ValueError(
                    f"Start snapshot (inclusive) {from_snapshot_id} is not an ancestor of end snapshot {to_snapshot_id}"
                )
            boundary = (
                self.table_metadata.snapshot_by_id(from_snapshot.parent_snapshot_id)
                if from_snapshot.parent_snapshot_id is not None
                else None
            )
            in_range = list(ancestors_between_ids(from_snapshot.parent_snapshot_id, to_snapshot_id, self.table_metadata))[::-1]
            return boundary, in_range

        if from_snapshot_id == to_snapshot_id:
            # An exclusive boundary equal to the end covers no snapshots: an empty interval.
            return None, []

        if not is_parent_ancestor_of(to_snapshot_id, from_snapshot_id, self.table_metadata):
            raise ValueError(
                f"Start snapshot (exclusive) {from_snapshot_id} is not on the ancestor chain of end snapshot {to_snapshot_id}"
            )

        boundary = self.table_metadata.snapshot_by_id(from_snapshot_id)
        if boundary is None:
            # The lineage references the boundary as a parent, but it has been expired, so its row multiset
            # cannot be reconstructed. Reject rather than emit an incomplete changelog.
            raise ValueError(
                f"Start snapshot (exclusive) {from_snapshot_id} is not present in table metadata; the ancestor"
                f" chain of end snapshot {to_snapshot_id} is incomplete"
            )

        in_range = list(ancestors_between_ids(from_snapshot_id, to_snapshot_id, self.table_metadata))[::-1]
        return boundary, in_range

    def plan_files(self) -> Iterable[ChangelogScanTask]:
        """Plan the changelog event groups without reading data files.

        Returns one task per (snapshot, change type) group covering the data files with added or deleted
        manifest entries. Manifests are opened to inspect entries, but the underlying data files are not read,
        so this reflects file-level changes only: for example, a same-data rewrite yields both an insert and a
        delete task whose rows cancel during reconciliation, while a position-delete snapshot produces no
        data-file task even though :meth:`to_arrow` emits the deleted rows. Use :meth:`to_arrow` to materialize
        the reconciled, row-level changelog.
        """
        if self._has_no_snapshots():
            return []
        boundary, in_range = self._resolve_range()
        if not in_range:
            return []

        tasks: list[ChangelogScanTask] = []
        for snapshot in in_range:
            insert_files: list[DataFile] = []
            delete_files: list[DataFile] = []
            for manifest in self._touched_manifests(snapshot):
                if manifest.content != ManifestContent.DATA or not self._manifest_may_match(manifest):
                    continue
                for entry in manifest.fetch_manifest_entry(self.io, discard_deleted=False):
                    if entry.data_file.content != DataFileContent.DATA or entry.status == ManifestEntryStatus.EXISTING:
                        continue
                    if not self._metrics_evaluator.eval(entry.data_file):
                        continue
                    if entry.status == ManifestEntryStatus.ADDED:
                        insert_files.append(entry.data_file)
                    else:
                        delete_files.append(entry.data_file)
            if insert_files:
                tasks.append(ChangelogScanTask(snapshot.snapshot_id, ChangelogOperation.INSERT, tuple(insert_files)))
            if delete_files:
                tasks.append(ChangelogScanTask(snapshot.snapshot_id, ChangelogOperation.DELETE, tuple(delete_files)))
        return tasks

    def to_arrow(self) -> pa.Table:
        """Read the changelog eagerly as an Arrow table.

        The returned table contains the projected columns plus a ``change_type`` string column whose values
        are ``insert`` or ``delete``. Rows are grouped by the snapshot that produced each event.

        Returns:
            An Arrow table with one row per changelog event.
        """
        import pyarrow as pa

        batches = list(self._record_batches())
        target_schema = self._arrow_output_schema()
        if not batches:
            return target_schema.empty_table()
        return pa.Table.from_batches(batches, schema=target_schema)

    def to_arrow_batch_reader(self) -> pa.RecordBatchReader:
        """Stream the changelog as an Arrow RecordBatchReader.

        Returns:
            A RecordBatchReader yielding changelog rows grouped by snapshot.
        """
        import pyarrow as pa

        return pa.RecordBatchReader.from_batches(self._arrow_output_schema(), self._record_batches())

    def _arrow_output_schema(self) -> pa.Schema:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        projected = schema_to_pyarrow(self.projection(), include_field_ids=False)
        import pyarrow as pa

        return projected.append(pa.field(CHANGE_TYPE_COLUMN, pa.string()))

    def _record_batches(self) -> Iterator[pa.RecordBatch]:
        if self._has_no_snapshots():
            return iter(())
        boundary, in_range = self._resolve_range()
        if not in_range:
            # An empty interval must not touch any data or manifest files.
            return iter(())

        state = self._initial_state(boundary)
        groups: list[tuple[ChangelogOperation, pa.Table]] = []

        for snapshot in in_range:
            before = Counter(state.rows)
            self._apply_snapshot(snapshot, state)
            insert_rows, delete_rows = _multiset_diff(before, state.rows, self.table_metadata.schema())
            if insert_rows:
                groups.append((ChangelogOperation.INSERT, insert_rows))
            if delete_rows:
                groups.append((ChangelogOperation.DELETE, delete_rows))

        return self._groups_to_batches(groups)

    def _groups_to_batches(self, groups: list[tuple[ChangelogOperation, pa.Table]]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        projected_names = [field.name for field in self.projection().fields]
        emitted = 0
        for change_type, rows in groups:
            tagged = rows.select(projected_names).append_column(
                CHANGE_TYPE_COLUMN, pa.array([change_type.value] * rows.num_rows, type=pa.string())
            )
            for batch in tagged.to_batches():
                if self.limit is not None:
                    if emitted >= self.limit:
                        return
                    if emitted + batch.num_rows > self.limit:
                        batch = batch.slice(0, self.limit - emitted)
                emitted += batch.num_rows
                yield batch

    def _touched_manifests(self, snapshot: Snapshot) -> list[ManifestFile]:
        return [
            manifest
            for manifest in snapshot.manifests(self.io, self.encryption_manager)
            if manifest.added_snapshot_id == snapshot.snapshot_id
        ]

    def _initial_state(self, boundary: Snapshot | None) -> _ChangelogState:
        """Build the logical row multiset at the boundary, honoring any live position deletes."""
        state = _ChangelogState()
        if boundary is None:
            return state

        # Every live data file initially contributes its physical rows, before position deletes.
        for manifest in boundary.manifests(self.io, self.encryption_manager):
            if manifest.content != ManifestContent.DATA or not self._manifest_may_match(manifest):
                continue
            for entry in manifest.fetch_manifest_entry(self.io, discard_deleted=True):
                if entry.data_file.content != DataFileContent.DATA or not self._metrics_evaluator.eval(entry.data_file):
                    continue
                physical = _table_to_multiset(self._apply_row_filter(self._read_data_file(entry.data_file)))
                state.add_data_file(entry.data_file.file_path, physical)
                self._data_file_by_path[entry.data_file.file_path] = entry.data_file

        # Replay the position deletes already in effect at the boundary so the starting multiset holds logical rows.
        for manifest in boundary.manifests(self.io, self.encryption_manager):
            if manifest.content != ManifestContent.DELETES:
                continue
            for entry in manifest.fetch_manifest_entry(self.io, discard_deleted=True):
                if entry.status == ManifestEntryStatus.ADDED:
                    self._apply_position_delete_file(entry.data_file, state)

        return state

    def _apply_snapshot(self, snapshot: Snapshot, state: _ChangelogState) -> None:
        for manifest in self._touched_manifests(snapshot):
            if manifest.content == ManifestContent.DATA:
                self._apply_data_manifest(manifest, state)
            else:
                self._apply_delete_manifest(manifest, state)

    def _apply_data_manifest(self, manifest: ManifestFile, state: _ChangelogState) -> None:
        if not self._manifest_may_match(manifest):
            return
        for entry in manifest.fetch_manifest_entry(self.io, discard_deleted=False):
            if entry.data_file.content != DataFileContent.DATA or entry.status == ManifestEntryStatus.EXISTING:
                continue
            if not self._metrics_evaluator.eval(entry.data_file):
                continue
            path = entry.data_file.file_path
            if entry.status == ManifestEntryStatus.ADDED:
                physical = _table_to_multiset(self._apply_row_filter(self._read_data_file(entry.data_file)))
                state.add_data_file(path, physical)
                self._data_file_by_path[path] = entry.data_file
            else:
                # Remove only rows still logically present; rows already removed by position deletes
                # must not be emitted as deletes a second time.
                state.remove_data_file(path)
                self._data_file_by_path.pop(path, None)

    def _apply_delete_manifest(self, manifest: ManifestFile, state: _ChangelogState) -> None:
        for entry in manifest.fetch_manifest_entry(self.io, discard_deleted=False):
            if entry.status != ManifestEntryStatus.ADDED:
                continue
            if entry.data_file.content == DataFileContent.EQUALITY_DELETES:
                raise ValueError("PyIceberg does not yet support equality deletes: https://github.com/apache/iceberg/issues/6568")
            if entry.data_file.content == DataFileContent.POSITION_DELETES:
                self._apply_position_delete_file(entry.data_file, state)

    def _apply_position_delete_file(self, delete_file: DataFile, state: _ChangelogState) -> None:
        for target_path, deleted_rows in self._position_deleted_rows(delete_file).items():
            state.apply_position_delete(target_path, _table_to_multiset(deleted_rows))

    def _position_deleted_rows(self, delete_file: DataFile) -> dict[str, pa.Table]:
        from pyiceberg.io.pyarrow import _read_deletes

        result: dict[str, pa.Table] = {}
        for target_path, positions in _read_deletes(self.io, delete_file).items():
            target = self._data_file_by_path.get(target_path)
            if target is None:
                logger.warning("Ignoring position deletes for unknown or expired data file %s", target_path)
                continue
            deleted = self._read_data_file(target).take(positions.to_pylist())
            result[target_path] = self._apply_row_filter(deleted)
        return result

    @cached_property
    def _data_file_by_path(self) -> dict[str, DataFile]:
        """Index of data files reachable while applying the scan, keyed by file path."""
        boundary, in_range = self._resolve_range()
        index: dict[str, DataFile] = {}
        snapshots = [*([boundary] if boundary is not None else []), *in_range]
        for snapshot in snapshots:
            for manifest in snapshot.manifests(self.io, self.encryption_manager):
                if manifest.content != ManifestContent.DATA:
                    continue
                for entry in manifest.fetch_manifest_entry(self.io, discard_deleted=True):
                    if entry.data_file.content == DataFileContent.DATA:
                        index[entry.data_file.file_path] = entry.data_file
        return index

    def _read_data_file(self, data_file: DataFile) -> pa.Table:
        from pyiceberg.io.pyarrow import ArrowScan
        from pyiceberg.table import FileScanTask

        # A data file can be referenced more than once while applying a range (e.g. once to load its
        # rows and once to resolve a position delete). Cache the read so each file is opened at most once.
        if data_file.file_path in self._data_file_rows:
            return self._data_file_rows[data_file.file_path]
        scan = ArrowScan(self.table_metadata, self.io, self.table_metadata.schema(), ALWAYS_TRUE, self.case_sensitive)
        table = scan.to_table([FileScanTask(data_file)])
        self._data_file_rows[data_file.file_path] = table
        return table

    @cached_property
    def _data_file_rows(self) -> dict[str, pa.Table]:
        return {}

    def _apply_row_filter(self, table: pa.Table) -> pa.Table:
        if self.row_filter == ALWAYS_TRUE or table.num_rows == 0:
            return table
        from pyiceberg.io.pyarrow import expression_to_pyarrow

        return table.filter(expression_to_pyarrow(self._bound_row_filter, self.table_metadata.schema()))

    def _empty_filtered_table(self) -> pa.Table:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        return schema_to_pyarrow(self.table_metadata.schema(), include_field_ids=False).empty_table()


@dataclass
class _ChangelogState:
    """Mutable logical row multiset and per-file accounting across the applied snapshots.

    ``rows`` holds the currently live, filtered row multiset (identical rows counted by multiplicity).
    ``logical_rows_by_file`` tracks each live data file's surviving rows after its position deletes, so a
    later file removal subtracts only rows that are still present rather than the file's physical rows.
    """

    rows: Counter[_RowKey] = field(default_factory=Counter)
    logical_rows_by_file: dict[str, Counter[_RowKey]] = field(default_factory=dict)

    def add_data_file(self, path: str, physical_rows: Counter[_RowKey]) -> None:
        self.logical_rows_by_file[path] = Counter(physical_rows)
        self.rows.update(physical_rows)

    def remove_data_file(self, path: str) -> None:
        logical_rows = self.logical_rows_by_file.pop(path, Counter())
        self.rows.subtract(logical_rows)
        _drop_non_positive(self.rows)

    def apply_position_delete(self, path: str, deleted_rows: Counter[_RowKey]) -> None:
        self.rows.subtract(deleted_rows)
        _drop_non_positive(self.rows)
        logical = self.logical_rows_by_file.setdefault(path, Counter())
        logical.subtract(deleted_rows)
        _drop_non_positive(logical)


ICS = TypeVar("ICS", bound="IncrementalChangelogScan", covariant=True)


def _drop_non_positive(multiset: Counter[_RowKey]) -> None:
    for key in [key for key, count in multiset.items() if count <= 0]:
        del multiset[key]


class _RowKey:
    """Hashable row identity that treats NaN values as equal, matching the table's row equality.

    Plain tuples cannot key multisets of rows containing NaN because ``nan == nan`` is False, so two
    identical rows with a NaN column would be counted separately. This wrapper applies the same
    NaN-aware comparison used by :class:`pyiceberg.typedef.Record`.
    """

    values: tuple[Any, ...]

    def __init__(self, values: tuple[Any, ...]) -> None:
        self.values = values

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _RowKey) or len(self.values) != len(other.values):
            return False
        return all(_same_value(left, right) for left, right in zip(self.values, other.values, strict=True))

    def __hash__(self) -> int:
        return hash(tuple(_HASHABLE_NAN if _is_nan(value) else value for value in self.values))

    def as_dict(self, column_names: list[str]) -> dict[str, Any]:
        return dict(zip(column_names, self.values, strict=True))


_HASHABLE_NAN = float("nan")


def _table_to_multiset(table: pa.Table) -> Counter[_RowKey]:
    columns = table.column_names
    return Counter(_RowKey(tuple(row[column] for column in columns)) for row in table.to_pylist())


def _multiset_diff(before: Counter[_RowKey], after: Counter[_RowKey], schema: Schema) -> tuple[pa.Table, pa.Table]:
    import pyarrow as pa

    from pyiceberg.io.pyarrow import schema_to_pyarrow

    column_names = [field.name for field in schema.fields]
    inserted: list[dict[str, Any]] = []
    deleted: list[dict[str, Any]] = []
    for key in set(before) | set(after):
        delta = after.get(key, 0) - before.get(key, 0)
        if delta > 0:
            inserted.extend([key.as_dict(column_names) for _ in range(delta)])
        elif delta < 0:
            deleted.extend([key.as_dict(column_names) for _ in range(-delta)])

    arrow_schema = schema_to_pyarrow(schema, include_field_ids=False)
    insert_table = pa.Table.from_pylist(inserted, schema=arrow_schema) if inserted else arrow_schema.empty_table()
    delete_table = pa.Table.from_pylist(deleted, schema=arrow_schema) if deleted else arrow_schema.empty_table()
    return insert_table, delete_table
