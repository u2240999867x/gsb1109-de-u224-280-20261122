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
"""Tests for manifest rewriting handed off at a snapshot."""

import datetime
import os
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    write_manifest,
    write_manifest_list,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.refs import SnapshotRefType
from pyiceberg.table.snapshots import Operation, Snapshot, Summary
from pyiceberg.table.update import AddSnapshotUpdate, SetSnapshotRefUpdate
from pyiceberg.transforms import MonthTransform, YearTransform
from pyiceberg.typedef import Record
from pyiceberg.types import DateType, LongType, NestedField
from pyiceberg.utils.datetime import date_to_days

FEB_DAY = date_to_days(datetime.date(2024, 2, 15))
JAN_DAY = date_to_days(datetime.date(2024, 1, 15))
TARGET_MANIFEST_SIZE = 20_000


def _schema() -> Schema:
    return Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "d", DateType(), required=False))


def _create_v2_table(catalog: Catalog, identifier: str) -> Schema:
    """Create a v2 table that evolves from a yearly to a monthly partition spec."""
    schema = _schema()
    year_spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=YearTransform(), name="d_year"))
    table = catalog.create_table(identifier, schema=schema, partition_spec=year_spec, properties={"format-version": "2"})
    with table.transaction() as tx:
        spec_update = tx.update_spec()
        spec_update.remove_field("d_year")
        spec_update.add_field("d", MonthTransform(), "d_month")
        spec_update.commit()
    return schema


class ManifestFixture:
    """Builds data/delete files and hand-built snapshots for the rewrite tests."""

    def __init__(self, table, schema: Schema) -> None:
        self.table = table
        self.schema = schema
        self.io = table.io
        self.loc = table.location_provider()
        self.month_spec = table.specs()[1]
        self.year_spec = table.specs()[0]
        self.feb_month = MonthTransform().transform(DateType())(FEB_DAY)
        self.jan_year = YearTransform().transform(DateType())(JAN_DAY)

    def _arrow(self, ids: list[int], day: int) -> pa.Table:
        return pa.table({"id": pa.array(ids, pa.int64()), "d": pa.array([day] * len(ids), pa.int32())}).cast(
            schema_to_pyarrow(self.schema)
        )

    def data_file(self, name: str, ids: list[int], day: int = FEB_DAY, spec_id: int = 1) -> DataFile:
        path = self.loc.new_data_location(f"{name}-{uuid.uuid4()}.parquet")
        with self.io.new_output(path).create() as output:
            pq.write_table(self._arrow(ids, day), output)
        partition = Record(self.feb_month) if spec_id == 1 else Record(self.jan_year)
        return DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=path,
            file_format=FileFormat.PARQUET,
            partition=partition,
            record_count=len(ids),
            file_size_in_bytes=len(self.io.new_input(path)),
            spec_id=spec_id,
        )

    def yearly_data_file(self, name: str) -> DataFile:
        data_file = self.data_file(name, [99], day=JAN_DAY, spec_id=0)
        # Make the single-entry yearly manifest larger than the rewrite target without touching data.
        blob = os.urandom(20000)
        return DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=data_file.file_path,
            file_format=FileFormat.PARQUET,
            partition=Record(self.jan_year),
            record_count=1,
            file_size_in_bytes=data_file.file_size_in_bytes,
            spec_id=0,
            lower_bounds={1: blob, 2: blob},
            upper_bounds={1: blob, 2: blob},
        )

    def delete_file(self, name: str, target: DataFile, pos: int) -> DataFile:
        path = self.loc.new_data_location(f"{name}-{uuid.uuid4()}.parquet")
        frame = pa.table({"file_path": pa.array([target.file_path]), "pos": pa.array([pos], pa.int32())})
        with self.io.new_output(path).create() as output:
            pq.write_table(frame, output)
        return DataFile.from_args(
            content=DataFileContent.POSITION_DELETES,
            file_path=path,
            file_format=FileFormat.PARQUET,
            partition=Record(self.feb_month),
            record_count=1,
            file_size_in_bytes=len(self.io.new_input(path)),
            spec_id=1,
        )

    def entry(self, status: ManifestEntryStatus, snapshot_id: int, seq: int, data_file: DataFile) -> ManifestEntry:
        return ManifestEntry.from_args(
            status=status,
            snapshot_id=snapshot_id,
            sequence_number=seq,
            file_sequence_number=seq,
            data_file=data_file,
        )

    def manifest(self, snapshot_id: int, spec_id: int, content: ManifestContent, entries: list[ManifestEntry]):
        spec = self.table.specs()[spec_id]
        writer = write_manifest(
            format_version=2,
            spec=spec,
            schema=self.schema,
            output_file=self.io.new_output(self.loc.new_metadata_location(f"{uuid.uuid4()}.avro")),
            snapshot_id=snapshot_id,
            avro_compression="zstandard",
            content=content,
        )
        with writer:
            for entry in entries:
                writer.add_entry(entry)
        return writer.to_manifest_file()

    def commit_snapshot(self, snapshot_id: int, parent_id: int | None, seq: int, manifests) -> None:
        manifest_list_path = self.loc.new_metadata_location(f"snap-{snapshot_id}-0-{uuid.uuid4()}.avro")
        with write_manifest_list(
            format_version=2,
            output_file=self.io.new_output(manifest_list_path),
            snapshot_id=snapshot_id,
            parent_snapshot_id=parent_id,
            sequence_number=seq,
            avro_compression="zstandard",
        ) as writer:
            writer.add_manifests(manifests)
        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            parent_snapshot_id=parent_id,
            manifest_list=manifest_list_path,
            sequence_number=seq,
            summary=Summary(Operation.APPEND),
            schema_id=self.table.metadata.current_schema_id,
        )
        with self.table.transaction() as tx:
            tx._stage(
                (
                    AddSnapshotUpdate(snapshot=snapshot),
                    SetSnapshotRefUpdate(
                        snapshot_id=snapshot_id,
                        parent_snapshot_id=parent_id,
                        ref_name="main",
                        type=SnapshotRefType.BRANCH,
                    ),
                )
            )


def _live_entries(table, snapshot_id: int):
    snapshot = table.metadata.snapshot_by_id(snapshot_id)
    return [
        entry
        for manifest in snapshot.manifests(io=table.io)
        for entry in manifest.fetch_manifest_entry(io=table.io, discard_deleted=True)
    ]


def _manifest_paths(table, snapshot_id: int) -> set[str]:
    snapshot = table.metadata.snapshot_by_id(snapshot_id)
    return {manifest.manifest_path for manifest in snapshot.manifests(io=table.io)}


def _data_paths(table, snapshot_id: int) -> set[str]:
    return {entry.data_file.file_path for entry in _live_entries(table, snapshot_id)}


def _rewrite(table, from_snapshot_id: int, branch: str = "main") -> None:
    with table.transaction() as tx:
        tx.update_snapshot(branch=branch).rewrite_manifests(
            from_snapshot_id=from_snapshot_id, target_size_bytes=TARGET_MANIFEST_SIZE
        ).commit()


def _build_handed_off_snapshot(catalog: Catalog, identifier: str, fixture: ManifestFixture, snapshot_id: int = 100):
    """Build snapshot S with A-D, L1-L4 in small manifests and one oversized yearly manifest."""
    data_files = {
        "A": fixture.data_file("A", [10, 11, 12]),
        "B": fixture.data_file("B", [20, 21]),
        "C": fixture.data_file("C", [30, 31]),
        "D": fixture.data_file("D", [40, 41]),
    }
    delete_files = {
        "L1": fixture.delete_file("L1", data_files["A"], 0),
        "L2": fixture.delete_file("L2", data_files["B"], 0),
        "L3": fixture.delete_file("L3", data_files["C"], 0),
        "L4": fixture.delete_file("L4", data_files["D"], 0),
    }
    yearly_file = fixture.yearly_data_file("Y")

    manifests = [
        fixture.manifest(
            snapshot_id,
            1,
            ManifestContent.DATA,
            [fixture.entry(ManifestEntryStatus.ADDED, snapshot_id, 1, data_files[name])],
        )
        for name in ("A", "B", "C", "D")
    ]
    manifests += [
        fixture.manifest(
            snapshot_id,
            1,
            ManifestContent.DELETES,
            [fixture.entry(ManifestEntryStatus.ADDED, snapshot_id, 1, delete_files[name])],
        )
        for name in ("L1", "L2", "L3", "L4")
    ]
    yearly_manifest = fixture.manifest(
        snapshot_id, 0, ManifestContent.DATA, [fixture.entry(ManifestEntryStatus.ADDED, snapshot_id, 1, yearly_file)]
    )
    manifests.append(yearly_manifest)
    fixture.commit_snapshot(snapshot_id, None, 1, manifests)
    return data_files, delete_files, yearly_manifest


def test_rewrite_manifests_merges_small_manifests_without_concurrency(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    identifier = "default.rewrite_manifests_no_concurrency"
    schema = _create_v2_table(catalog, identifier)
    table = catalog.load_table(identifier)
    fixture = ManifestFixture(table, schema)

    _data_files, _delete_files, yearly_manifest = _build_handed_off_snapshot(catalog, identifier, fixture)
    snapshot_id = 100

    handed_off = catalog.load_table(identifier)
    assert handed_off.metadata.current_snapshot_id == snapshot_id
    # February still reads a1,a2,b1,c1,d1 after the position deletes remove the first row of each file.
    assert sorted(handed_off.scan(snapshot_id=snapshot_id).to_arrow()["id"].to_pylist()) == [11, 12, 21, 31, 41, 99]

    before_manifest_paths = _manifest_paths(handed_off, snapshot_id)
    before_data_paths = _data_paths(handed_off, snapshot_id)
    _rewrite(handed_off, snapshot_id)

    rewritten = catalog.load_table(identifier)
    head = rewritten.current_snapshot()
    assert head is not None and head.snapshot_id != snapshot_id
    head_manifests = head.manifests(io=rewritten.io)
    data_manifests = [manifest for manifest in head_manifests if manifest.content == ManifestContent.DATA]
    delete_manifests = [manifest for manifest in head_manifests if manifest.content == ManifestContent.DELETES]

    # The four small deletes manifests merge into a single deletes manifest; the monthly data manifests
    # merge into one, while the oversized yearly manifest stays separate.
    assert len(delete_manifests) == 1
    assert len(data_manifests) == 2
    assert {manifest.partition_spec_id for manifest in delete_manifests} == {1}

    # The oversized old yearly manifest keeps its original path.
    assert yearly_manifest.manifest_path in {manifest.manifest_path for manifest in head_manifests}

    # Every rewritten entry still points at its original file and keeps its source identity.
    for manifest in head_manifests:
        for entry in manifest.fetch_manifest_entry(io=rewritten.io, discard_deleted=False):
            assert entry.snapshot_id == snapshot_id
            assert entry.sequence_number == 1
            assert entry.file_sequence_number == 1
            assert entry.data_file.file_path in before_data_paths

    # The rewrite is metadata-only: no data or delete file was added or removed.
    assert _data_paths(rewritten, head.snapshot_id) == before_data_paths
    assert _manifest_paths(rewritten, head.snapshot_id).isdisjoint(before_manifest_paths - {yearly_manifest.manifest_path})

    assert sorted(rewritten.scan().to_arrow()["id"].to_pylist()) == [11, 12, 21, 31, 41, 99]

    # Repeating the rewrite from S has nothing left to merge, so the current snapshot is reused.
    snapshots_before = len(rewritten.metadata.snapshots)
    head_id_before = rewritten.metadata.current_snapshot_id
    _rewrite(rewritten, snapshot_id)
    reused = catalog.load_table(identifier)
    assert reused.metadata.current_snapshot_id == head_id_before
    assert len(reused.metadata.snapshots) == snapshots_before


def test_rewrite_manifests_retries_against_a_concurrent_head(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    identifier = "default.rewrite_manifests_concurrent"
    schema = _create_v2_table(catalog, identifier)
    table = catalog.load_table(identifier)
    fixture = ManifestFixture(table, schema)

    data_files, delete_files, yearly_manifest = _build_handed_off_snapshot(catalog, identifier, fixture)
    snapshot_id = 100

    # A second writer lands snapshot S2 before the rewrite commits: replace D with a d1-only file,
    # remove L4, add L5 deleting a1, and place L1 and L5 in a shared deletes manifest.
    d2 = fixture.data_file("D2", [41])
    l5 = fixture.delete_file("L5", data_files["A"], 1)
    s2_id = 200
    s2_manifests = [
        fixture.manifest(
            s2_id, 1, ManifestContent.DATA, [fixture.entry(ManifestEntryStatus.EXISTING, snapshot_id, 1, data_files["A"])]
        ),
        fixture.manifest(
            s2_id, 1, ManifestContent.DATA, [fixture.entry(ManifestEntryStatus.EXISTING, snapshot_id, 1, data_files["B"])]
        ),
        fixture.manifest(
            s2_id, 1, ManifestContent.DATA, [fixture.entry(ManifestEntryStatus.EXISTING, snapshot_id, 1, data_files["C"])]
        ),
        fixture.manifest(s2_id, 1, ManifestContent.DATA, [fixture.entry(ManifestEntryStatus.DELETED, s2_id, 2, data_files["D"])]),
        fixture.manifest(s2_id, 1, ManifestContent.DATA, [fixture.entry(ManifestEntryStatus.ADDED, s2_id, 2, d2)]),
        fixture.manifest(
            s2_id,
            1,
            ManifestContent.DELETES,
            [
                fixture.entry(ManifestEntryStatus.EXISTING, snapshot_id, 1, delete_files["L1"]),
                fixture.entry(ManifestEntryStatus.ADDED, s2_id, 2, l5),
            ],
        ),
        fixture.manifest(
            s2_id,
            1,
            ManifestContent.DELETES,
            [fixture.entry(ManifestEntryStatus.EXISTING, snapshot_id, 1, delete_files["L2"])],
        ),
        fixture.manifest(
            s2_id,
            1,
            ManifestContent.DELETES,
            [fixture.entry(ManifestEntryStatus.EXISTING, snapshot_id, 1, delete_files["L3"])],
        ),
        fixture.manifest(
            s2_id,
            1,
            ManifestContent.DELETES,
            [fixture.entry(ManifestEntryStatus.DELETED, s2_id, 2, delete_files["L4"])],
        ),
        yearly_manifest,
    ]

    stale = catalog.load_table(identifier)
    assert stale.metadata.current_snapshot_id == snapshot_id
    # Land the concurrent snapshot so the stale rewrite retries against the new HEAD in the same call.
    fixture.commit_snapshot(s2_id, snapshot_id, 2, s2_manifests)

    _rewrite(stale, snapshot_id)

    rewritten = catalog.load_table(identifier)
    head = rewritten.current_snapshot()
    assert head is not None and head.snapshot_id not in (snapshot_id, s2_id)
    head_manifests = head.manifests(io=rewritten.io)

    delete_manifests = [manifest for manifest in head_manifests if manifest.content == ManifestContent.DELETES]
    groups = [
        sorted(entry.data_file.file_path.rsplit("/", 1)[-1] for entry in manifest.fetch_manifest_entry(io=rewritten.io))
        for manifest in delete_manifests
    ]
    # L1, L2, L3 merge into one manifest while the post-hand-off L5 stays on its own.
    assert sorted(["L1", "L2", "L3"]) == [name.split("-")[0] for name in next(group for group in groups if len(group) == 3)]
    assert any(len(group) == 1 and group[0].startswith("L5") for group in groups)

    live_data_names = {
        entry.data_file.file_path.rsplit("/", 1)[-1].split("-")[0]
        for manifest in head_manifests
        if manifest.content == ManifestContent.DATA
        for entry in manifest.fetch_manifest_entry(io=rewritten.io)
    }
    live_delete_names = {
        entry.data_file.file_path.rsplit("/", 1)[-1].split("-")[0]
        for manifest in delete_manifests
        for entry in manifest.fetch_manifest_entry(io=rewritten.io)
    }
    # D and L4 never reappear as live entries; the replacement file stays live.
    assert "D" not in live_data_names
    assert "D2" in live_data_names
    assert "L4" not in live_delete_names

    # The old yearly manifest still uses its original path, and S still reads its original result.
    assert yearly_manifest.manifest_path in {manifest.manifest_path for manifest in head_manifests}
    assert sorted(rewritten.scan(snapshot_id=snapshot_id).to_arrow()["id"].to_pylist()) == [11, 12, 21, 31, 41, 99]
    # a1 is now deleted by L5, D only keeps d1.
    assert sorted(rewritten.scan().to_arrow()["id"].to_pylist()) == [12, 21, 31, 41, 99]


def test_rewrite_manifests_then_incremental_append_scan(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    identifier = "default.rewrite_manifests_incremental"
    schema = _create_v2_table(catalog, identifier)
    table = catalog.load_table(identifier)
    fixture = ManifestFixture(table, schema)

    _build_handed_off_snapshot(catalog, identifier, fixture)
    snapshot_id = 100

    handed_off = catalog.load_table(identifier)
    _rewrite(handed_off, snapshot_id)

    appended = catalog.load_table(identifier)
    append_frame = pa.table(
        {"id": pa.array([777, 888], pa.int64()), "d": pa.array([datetime.date(2024, 3, 1), datetime.date(2024, 3, 2)])}
    ).cast(schema_to_pyarrow(schema))
    appended.append(append_frame)

    scanned = catalog.load_table(identifier)
    result = scanned.incremental_append_scan().from_snapshot_id_exclusive(snapshot_id).to_arrow()
    assert sorted(result["id"].to_pylist()) == [777, 888]


def test_rewrite_manifests_works_on_a_non_main_branch(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    identifier = "default.rewrite_manifests_branch"
    schema = _create_v2_table(catalog, identifier)
    table = catalog.load_table(identifier)
    fixture = ManifestFixture(table, schema)

    _build_handed_off_snapshot(catalog, identifier, fixture)
    snapshot_id = 100
    table.manage_snapshots().create_branch(snapshot_id=snapshot_id, branch_name="audit").commit()

    branched = catalog.load_table(identifier)
    _rewrite(branched, snapshot_id, branch="audit")

    rewritten = catalog.load_table(identifier)
    audit_head_id = rewritten.metadata.refs["audit"].snapshot_id
    assert audit_head_id != snapshot_id
    audit_manifests = rewritten.metadata.snapshot_by_id(audit_head_id).manifests(io=rewritten.io)
    assert len([manifest for manifest in audit_manifests if manifest.content == ManifestContent.DELETES]) == 1
    # main is untouched by the branch rewrite.
    assert rewritten.metadata.refs["main"].snapshot_id == snapshot_id


def test_rewrite_manifests_rejects_unknown_snapshot_and_target(catalog: Catalog) -> None:
    from pyiceberg.exceptions import ValidationException

    catalog.create_namespace("default")
    identifier = "default.rewrite_manifests_invalid"
    schema = _create_v2_table(catalog, identifier)
    table = catalog.load_table(identifier)
    fixture = ManifestFixture(table, schema)
    _build_handed_off_snapshot(catalog, identifier, fixture)

    with pytest.raises(ValidationException, match="Cannot find handed-off snapshot"):
        with table.transaction() as tx:
            tx.update_snapshot().rewrite_manifests(from_snapshot_id=999, target_size_bytes=TARGET_MANIFEST_SIZE).commit()

    with pytest.raises(ValueError, match="Target manifest size must be a positive number"):
        with table.transaction() as tx:
            tx.update_snapshot().rewrite_manifests(from_snapshot_id=100, target_size_bytes=0).commit()
