"""Restore archived tables to a numbered daily settlement (日结).

This module adds an entry point for restoring python-rocksdb table
archives (for example daily store sales and tax-region summaries) to a
numbered settlement, from the backup/log information provided by the
on-duty rotation:

* :class:`SettlementDefinition` -- the settlement (日结) number and the
  target changelog offset of every table partition.
* :class:`DutyManifest` -- per table partition: the usable backups
  (backup id + location + offset) and the available changelog segments.
* :class:`BatchRestoreCoordinator` -- the restore entry point.  It
  pre-checks for restore gaps (:class:`RestoreGap` tells you exactly
  which table, which partition and which offset range is missing),
  restores every partition into a staging area next to the data
  directory, and only publishes the new settlement once *every* table
  reached its target.  While a restore is in flight -- or fails halfway
  -- queries keep reading the previously published settlement.
* :class:`SettlementRegistry` -- versioned reads.  Readers either use
  the currently published settlement, or pin the settlement selected by
  their first read (e.g. a reconciliation job that fetches sales first
  and the tax summary later) until the job ends.

Progress is journaled inside the data directory, so a restarted process
resumes an interrupted settlement instead of starting over: partitions
already completed are kept even if their log segments have since
expired from the log source.  Re-submitting the same settlement number
with a different definition raises :class:`SettlementConflictError`;
re-submitting it unchanged resumes the restore, or -- if the settlement
was already published and the acknowledgement was simply lost -- returns
the already published settlement without redoing any work.
"""

import hashlib
import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
)

__all__ = [
    "Backup",
    "BatchRestoreCoordinator",
    "ChangelogRecord",
    "DutyManifest",
    "JsonFileBackupStore",
    "JsonFileSegmentSource",
    "LogSegment",
    "PartitionSource",
    "RestoreGap",
    "RestorePlan",
    "SegmentUnavailable",
    "Settlement",
    "SettlementConflictError",
    "SettlementDefinition",
    "SettlementReceipt",
    "SettlementRegistry",
    "SettlementRestoreError",
    "plan_partition",
    "precheck_definition",
]

#: Status of a settlement that is being restored but not published yet.
STATUS_RESTORING = "restoring"
#: Status of a settlement whose tables all reached their target offsets.
STATUS_PUBLISHED = "published"
#: Receipt status when the pre-check found restore gaps.
STATUS_GAP = "gap"


class SettlementConflictError(Exception):
    """Raised when a settlement number is reused with a different definition."""


class SettlementRestoreError(Exception):
    """Raised when a restore cannot finish (e.g. a segment went unreadable)."""


class SegmentUnavailable(Exception):
    """Raised by segment sources when a log segment cannot be read right now."""


class ChangelogRecord(NamedTuple):
    """A single changelog record from a log segment."""

    offset: int
    key: str
    value: Any  # None means the key was deleted.


@dataclass(frozen=True)
class Backup:
    """A table partition backup handed over by the on-duty rotation."""

    backup_id: str
    offset: int
    location: str


@dataclass(frozen=True)
class LogSegment:
    """A continuous changelog segment ``[first_offset, last_offset]``."""

    first_offset: int
    last_offset: int
    location: str


@dataclass(frozen=True)
class PartitionSource:
    """Backups and log segments available for one table partition."""

    backups: Tuple[Backup, ...] = ()
    segments: Tuple[LogSegment, ...] = ()


#: table name -> partition -> sources, as provided by the on-duty rotation.
DutyManifest = Mapping[str, Mapping[int, PartitionSource]]


@dataclass(frozen=True)
class SettlementDefinition:
    """A numbered settlement: target changelog offset per table partition."""

    batch_id: str
    #: table name -> partition -> target offset.
    targets: Mapping[str, Mapping[int, int]]

    def fingerprint(self) -> str:
        """Stable fingerprint of the definition, used to detect conflicts."""
        canonical = json.dumps(
            {
                table: {str(partition): offset for partition, offset in parts.items()}
                for table, parts in sorted(self.targets.items())
            },
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True)
class RestoreGap:
    """A missing piece of a restore: table, partition and offset range."""

    table: str
    partition: int
    first_offset: int
    last_offset: int
    reason: str

    def __str__(self) -> str:
        return (
            f"table={self.table} partition={self.partition} "
            f"offsets={self.first_offset}..{self.last_offset} ({self.reason})"
        )


class RestorePlan(NamedTuple):
    """How to restore one table partition to its target offset."""

    table: str
    partition: int
    target: int
    backup: Optional[Backup]
    segments: Tuple[LogSegment, ...]


def _coverage_gaps(
    table: str,
    partition: int,
    segments: Tuple[LogSegment, ...],
    first: int,
    last: int,
) -> List[RestoreGap]:
    """Return the sub-ranges of ``[first, last]`` not covered by segments."""
    gaps: List[RestoreGap] = []
    need = first
    for segment in sorted(segments, key=lambda s: (s.first_offset, s.last_offset)):
        if segment.last_offset < need:
            continue
        if segment.first_offset > need:
            gaps.append(
                RestoreGap(
                    table=table,
                    partition=partition,
                    first_offset=need,
                    last_offset=min(segment.first_offset, last + 1) - 1,
                    reason="missing-log-segment",
                )
            )
        need = max(need, segment.last_offset + 1)
        if need > last:
            break
    if need <= last:
        gaps.append(
            RestoreGap(
                table=table,
                partition=partition,
                first_offset=need,
                last_offset=last,
                reason="missing-log-segment",
            )
        )
    return gaps


def plan_partition(
    table: str,
    partition: int,
    target: int,
    source: PartitionSource,
) -> RestorePlan:
    """Build the restore plan for one partition.

    Picks the newest backup at or below the target offset, plus the log
    segments needed to replay from the backup up to the target.

    Raises:
        SettlementRestoreError: if the sources cannot reach the target;
            the exception message lists the gaps (use
            :func:`precheck_definition` to report them upfront instead).
    """
    gaps = precheck_partition(table, partition, target, source)
    if gaps:
        raise SettlementRestoreError(
            "restore gaps for "
            f"{table}/{partition}: " + ", ".join(str(gap) for gap in gaps)
        )
    usable = [b for b in source.backups if b.offset <= target]
    backup = max(usable, key=lambda b: b.offset, default=None)
    base = backup.offset if backup is not None else -1
    if base >= target:
        segments: Tuple[LogSegment, ...] = ()
    else:
        segments = tuple(
            segment
            for segment in sorted(
                source.segments, key=lambda s: (s.first_offset, s.last_offset)
            )
            if segment.last_offset >= base + 1 and segment.first_offset <= target
        )
    return RestorePlan(
        table=table,
        partition=partition,
        target=target,
        backup=backup,
        segments=segments,
    )


def precheck_partition(
    table: str,
    partition: int,
    target: int,
    source: PartitionSource,
) -> List[RestoreGap]:
    """Return the restore gaps for one partition (empty if restorable)."""
    usable = [b for b in source.backups if b.offset <= target]
    if not usable:
        return [
            RestoreGap(
                table=table,
                partition=partition,
                first_offset=0,
                last_offset=target,
                reason="missing-backup",
            )
        ]
    base = max(b.offset for b in usable)
    if base >= target:
        return []
    return _coverage_gaps(table, partition, source.segments, base + 1, target)


def precheck_definition(
    definition: SettlementDefinition,
    manifest: DutyManifest,
    skip: Optional[Set[Tuple[str, int]]] = None,
) -> List[RestoreGap]:
    """Check every target of a settlement definition for restore gaps.

    Arguments:
        definition: the settlement to check.
        manifest: backups and log segments provided by the on-duty rotation.
        skip: table/partition pairs to skip (already restored earlier).
    """
    gaps: List[RestoreGap] = []
    for table, partitions in sorted(definition.targets.items()):
        for partition, target in sorted(partitions.items()):
            if skip and (table, partition) in skip:
                continue
            source = manifest.get(table, {}).get(partition, PartitionSource())
            gaps.extend(precheck_partition(table, partition, target, source))
    return gaps


@dataclass(frozen=True)
class Settlement:
    """An immutable, published view of all archived tables."""

    settlement_id: str
    #: table name -> key -> value.
    data: Mapping[str, Mapping[str, Any]]
    #: table name -> partition -> restored changelog offset.
    offsets: Mapping[str, Mapping[int, int]] = field(default_factory=dict)

    def get(self, table: str, key: str, default: Any = None) -> Any:
        """Read ``key`` from ``table`` as of this settlement."""
        return self.data.get(table, {}).get(key, default)


class SettlementRegistry:
    """Tracks the published settlement and pinned reconciliation reads.

    Publishing is a single reference swap, so it is atomic across all
    tables of the settlement: a reader either sees the whole previous
    settlement or the whole new one.  A reconciliation job pins the
    settlement selected by its first read and keeps reading that same
    settlement -- even if a new settlement is published in the meantime
    -- until the ``with`` block ends.
    """

    def __init__(self) -> None:
        self._published: Optional[Settlement] = None
        self._pins: Set[int] = set()

    @property
    def published(self) -> Optional[Settlement]:
        """The currently published settlement, if any."""
        return self._published

    def publish(self, settlement: Settlement) -> None:
        """Atomically make ``settlement`` visible to all new readers."""
        self._published = settlement

    def current(self) -> Settlement:
        """Return the currently published settlement."""
        if self._published is None:
            raise SettlementRestoreError("no settlement has been published yet")
        return self._published

    @contextmanager
    def pin(self) -> Iterator[Settlement]:
        """Pin the published settlement for the duration of a read job.

        Every read inside the ``with`` block uses the settlement that was
        published when the pin was taken, even if a restore publishes a
        newer settlement in the meantime.
        """
        settlement = self.current()
        self._pins.add(id(settlement))
        try:
            yield settlement
        finally:
            self._pins.discard(id(settlement))

    @property
    def pinned_count(self) -> int:
        """Number of settlements currently pinned by readers."""
        return len(self._pins)


@dataclass(frozen=True)
class SettlementReceipt:
    """Result of submitting a settlement restore."""

    batch_id: str
    #: one of "published", "restoring" or "gap".
    status: str
    settlement_id: Optional[str] = None
    gaps: Tuple[RestoreGap, ...] = ()


class JsonFileBackupStore:
    """Loads partition backups from JSON files.

    File layout: ``{"offset": <int>, "state": {<key>: <value>, ...}}``.
    """

    def load(self, location: str) -> Tuple[int, Mapping[str, Any]]:
        """Load backup, returning ``(base_offset, state)``."""
        payload = json.loads(Path(location).read_text())
        return int(payload["offset"]), dict(payload["state"])


class JsonFileSegmentSource:
    """Reads changelog segments from JSON files.

    File layout: ``[{"offset": ..., "key": ..., "value": ...}, ...]``;
    a ``null`` value deletes the key.
    """

    def probe(self, location: str) -> None:
        """Raise :class:`SegmentUnavailable` if the segment is not readable."""
        if not Path(location).is_file():
            raise SegmentUnavailable(f"log segment not readable: {location}")

    def read(self, location: str) -> List[ChangelogRecord]:
        """Read all records of the segment at ``location``."""
        self.probe(location)
        payload = json.loads(Path(location).read_text())
        return [
            ChangelogRecord(
                offset=int(record["offset"]),
                key=record["key"],
                value=record.get("value"),
            )
            for record in payload
        ]


class BatchRestoreCoordinator:
    """Entry point for restoring tables to a numbered settlement.

    Arguments:
        datadir: the table data directory; restore journals, staging
            state and the published settlement pointer are kept inside
            it so a restart with the same directory resumes where the
            previous process left off.
        registry: the registry queries read from; created if omitted.
        backup_store: loads partition backups; defaults to JSON files.
        segment_source: reads changelog segments; defaults to JSON files.
    """

    def __init__(
        self,
        datadir: Any,
        *,
        registry: Optional[SettlementRegistry] = None,
        backup_store: Optional[Any] = None,
        segment_source: Optional[Any] = None,
    ) -> None:
        self.datadir = Path(datadir)
        self.registry = registry if registry is not None else SettlementRegistry()
        self.backup_store = (
            backup_store if backup_store is not None else JsonFileBackupStore()
        )
        self.segment_source = (
            segment_source if segment_source is not None else JsonFileSegmentSource()
        )
        self._journal_dir = self.datadir / "settlement-journal"
        self._staging_dir = self.datadir / "settlement-staging"
        self._published_path = self.datadir / "settlement-published.json"
        self._load_published()

    # -- queries ---------------------------------------------------------

    def current_settlement(self) -> Settlement:
        """Return the settlement queries should read right now."""
        return self.registry.current()

    @contextmanager
    def pin(self) -> Iterator[Settlement]:
        """Pin the published settlement until this read job ends."""
        with self.registry.pin() as settlement:
            yield settlement

    def register_published(self, settlement: Settlement) -> None:
        """Publish an already archived settlement and persist the pointer."""
        self.registry.publish(settlement)
        self._persist_published(settlement)

    # -- pre-check ---------------------------------------------------------

    def precheck(
        self,
        definition: SettlementDefinition,
        manifest: DutyManifest,
    ) -> List[RestoreGap]:
        """Report restore gaps (table, partition, offset range), if any.

        Partitions already restored for this settlement in a previous
        run are skipped, so expired log segments of finished partitions
        do not block a resumed restore.
        """
        journal = self._load_journal(definition.batch_id)
        skip = self._completed_partitions(journal) if journal else set()
        gaps = precheck_definition(definition, manifest, skip=skip)
        return gaps + self._unreadable_segments(definition, manifest, skip)

    def _unreadable_segments(
        self,
        definition: SettlementDefinition,
        manifest: DutyManifest,
        skip: Set[Tuple[str, int]],
    ) -> List[RestoreGap]:
        probe = getattr(self.segment_source, "probe", None)
        if probe is None:
            return []
        gaps: List[RestoreGap] = []
        for table, partitions in sorted(definition.targets.items()):
            for partition, target in sorted(partitions.items()):
                if (table, partition) in skip:
                    continue
                source = manifest.get(table, {}).get(partition, PartitionSource())
                try:
                    plan = plan_partition(table, partition, target, source)
                except SettlementRestoreError:
                    continue  # already reported as a coverage gap
                for segment in plan.segments:
                    try:
                        probe(segment.location)
                    except SegmentUnavailable:
                        gaps.append(
                            RestoreGap(
                                table=table,
                                partition=partition,
                                first_offset=segment.first_offset,
                                last_offset=segment.last_offset,
                                reason="segment-unreadable",
                            )
                        )
        return gaps

    # -- restore -----------------------------------------------------------

    def submit(
        self,
        definition: SettlementDefinition,
        manifest: DutyManifest,
    ) -> SettlementReceipt:
        """Restore all tables to ``definition`` and publish the settlement.

        * Re-submitting a settlement number whose definition differs
          from the journaled one raises :class:`SettlementConflictError`
          and leaves the existing progress untouched.
        * Re-submitting a settlement that is already published returns
          the same published settlement without redoing any work.
        * Otherwise incomplete partitions are restored into staging and
          the settlement is published atomically once every table
          reached its target.  If a log segment becomes unreadable
          mid-restore, :class:`SettlementRestoreError` is raised and
          queries keep reading the previously published settlement.
        """
        journal = self._load_journal(definition.batch_id)
        if journal is not None:
            if journal["fingerprint"] != definition.fingerprint():
                raise SettlementConflictError(
                    f"settlement {definition.batch_id!r} was already submitted "
                    "with a different definition; keeping the original "
                    f"definition (fingerprint {journal['fingerprint']})"
                )
            if journal["status"] == STATUS_PUBLISHED:
                return SettlementReceipt(
                    batch_id=definition.batch_id,
                    status=STATUS_PUBLISHED,
                    settlement_id=journal["settlement_id"],
                )
        else:
            journal = {
                "batch_id": definition.batch_id,
                "fingerprint": definition.fingerprint(),
                "status": STATUS_RESTORING,
                "settlement_id": definition.batch_id,
                "partitions": {},
            }
            self._save_journal(journal)

        gaps = self.precheck(definition, manifest)
        if gaps:
            return SettlementReceipt(
                batch_id=definition.batch_id,
                status=STATUS_GAP,
                gaps=tuple(gaps),
            )

        completed = self._completed_partitions(journal)
        for table, partitions in sorted(definition.targets.items()):
            for partition, target in sorted(partitions.items()):
                if (table, partition) in completed:
                    continue
                source = manifest.get(table, {}).get(partition, PartitionSource())
                plan = plan_partition(table, partition, target, source)
                state = self._restore_partition(plan)
                self._write_staging(definition.batch_id, plan, state)
                journal["partitions"][f"{table}/{partition}"] = {
                    "status": "complete",
                    "offset": target,
                }
                self._save_journal(journal)

        settlement = self._build_settlement(definition)
        self.registry.publish(settlement)
        journal["status"] = STATUS_PUBLISHED
        self._save_journal(journal)
        self._persist_published(settlement)
        return SettlementReceipt(
            batch_id=definition.batch_id,
            status=STATUS_PUBLISHED,
            settlement_id=settlement.settlement_id,
        )

    def _restore_partition(self, plan: RestorePlan) -> Dict[str, Any]:
        state: Dict[str, Any] = {}
        base = -1
        if plan.backup is not None:
            base_offset, backup_state = self.backup_store.load(plan.backup.location)
            base = base_offset
            state.update(backup_state)
        if base >= plan.target:
            return state
        records: Dict[int, ChangelogRecord] = {}
        for segment in plan.segments:
            try:
                segment_records = self.segment_source.read(segment.location)
            except SegmentUnavailable as exc:
                raise SettlementRestoreError(
                    f"log segment {segment.location} for "
                    f"{plan.table}/{plan.partition} became unreadable "
                    f"during replay: {exc}"
                ) from exc
            for record in segment_records:
                if base < record.offset <= plan.target:
                    records[record.offset] = record
        missing = [
            offset
            for offset in range(base + 1, plan.target + 1)
            if offset not in records
        ]
        if missing:
            raise SettlementRestoreError(
                f"log source no longer covers offsets "
                f"{missing[0]}..{missing[-1]} for "
                f"{plan.table}/{plan.partition}"
            )
        for offset in range(base + 1, plan.target + 1):
            record = records[offset]
            if record.value is None:
                state.pop(record.key, None)
            else:
                state[record.key] = record.value
        return state

    def _build_settlement(self, definition: SettlementDefinition) -> Settlement:
        previous = self.registry.published
        data: Dict[str, Dict[str, Any]] = {
            table: dict(values)
            for table, values in (previous.data.items() if previous else [])
        }
        offsets: Dict[str, Dict[int, int]] = {
            table: dict(parts)
            for table, parts in (previous.offsets.items() if previous else [])
        }
        for table, partitions in sorted(definition.targets.items()):
            table_data = data.setdefault(table, {})
            table_offsets = offsets.setdefault(table, {})
            for partition in sorted(partitions):
                staging = self._read_staging(definition.batch_id, table, partition)
                table_data.update(staging["state"])
                table_offsets[partition] = staging["offset"]
        return Settlement(
            settlement_id=definition.batch_id,
            data=data,
            offsets=offsets,
        )

    # -- persistence -------------------------------------------------------

    def _journal_path(self, batch_id: str) -> Path:
        return self._journal_dir / f"{batch_id}.json"

    def _load_journal(self, batch_id: str) -> Optional[Dict[str, Any]]:
        path = self._journal_path(batch_id)
        if not path.is_file():
            return None
        return json.loads(path.read_text())

    def _save_journal(self, journal: Mapping[str, Any]) -> None:
        self._journal_dir.mkdir(parents=True, exist_ok=True)
        path = self._journal_path(journal["batch_id"])
        path.write_text(json.dumps(journal, indent=2, sort_keys=True))

    @staticmethod
    def _completed_partitions(
        journal: Optional[Mapping[str, Any]],
    ) -> Set[Tuple[str, int]]:
        if not journal:
            return set()
        done: Set[Tuple[str, int]] = set()
        for key, entry in journal.get("partitions", {}).items():
            if entry.get("status") == "complete":
                table, _, partition = key.rpartition("/")
                done.add((table, int(partition)))
        return done

    def _staging_path(self, batch_id: str, table: str, partition: int) -> Path:
        return self._staging_dir / batch_id / table / f"{partition}.json"

    def _write_staging(
        self,
        batch_id: str,
        plan: RestorePlan,
        state: Mapping[str, Any],
    ) -> None:
        path = self._staging_path(batch_id, plan.table, plan.partition)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"offset": plan.target, "state": dict(state)}, indent=2)
        )

    def _read_staging(
        self,
        batch_id: str,
        table: str,
        partition: int,
    ) -> Mapping[str, Any]:
        return json.loads(self._staging_path(batch_id, table, partition).read_text())

    def _persist_published(self, settlement: Settlement) -> None:
        self.datadir.mkdir(parents=True, exist_ok=True)
        self._published_path.write_text(
            json.dumps(
                {
                    "settlement_id": settlement.settlement_id,
                    "data": {
                        table: dict(values)
                        for table, values in settlement.data.items()
                    },
                    "offsets": {
                        table: {str(p): o for p, o in parts.items()}
                        for table, parts in settlement.offsets.items()
                    },
                },
                indent=2,
                sort_keys=True,
            )
        )

    def _load_published(self) -> None:
        if not self._published_path.is_file():
            return
        payload = json.loads(self._published_path.read_text())
        self.registry.publish(
            Settlement(
                settlement_id=payload["settlement_id"],
                data=payload["data"],
                offsets={
                    table: {int(p): o for p, o in parts.items()}
                    for table, parts in payload.get("offsets", {}).items()
                },
            )
        )
