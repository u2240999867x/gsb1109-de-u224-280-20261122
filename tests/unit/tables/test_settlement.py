"""Tests for restore-by-settlement (日结) of archived tables."""

import pytest

from faust.tables.settlement import (
    STATUS_GAP,
    STATUS_PUBLISHED,
    Backup,
    BatchRestoreCoordinator,
    LogSegment,
    PartitionSource,
    Settlement,
    SettlementConflictError,
    SettlementDefinition,
    SettlementRegistry,
    SettlementRestoreError,
    SegmentUnavailable,
    precheck_definition,
)

SALES = "sales"
TAX = "tax"

D0 = Settlement(
    settlement_id="D0",
    data={SALES: {"store-1": 80}, TAX: {"region-1": 8}},
    offsets={SALES: {0: 10}, TAX: {0: 12}},
)

D1_TARGETS = {SALES: {0: 17}, TAX: {0: 21}}


class InMemoryBackupStore:
    def __init__(self, backups):
        self.backups = backups
        self.loads = []

    def load(self, location):
        self.loads.append(location)
        offset, state = self.backups[location]
        return offset, dict(state)


class InMemorySegmentSource:
    def __init__(self, segments):
        self.segments = segments
        self.read_failures = set()
        self.probe_failures = set()
        self.reads = []

    def probe(self, location):
        if location in self.probe_failures or location not in self.segments:
            raise SegmentUnavailable(location)

    def read(self, location):
        self.reads.append(location)
        if location in self.read_failures:
            raise SegmentUnavailable(location)
        self.probe(location)
        return list(self.segments[location])


def sales_backups():
    return {
        "sales-b14": (14, {"store-1": 90}),
        "sales-b19": (19, {"store-1": 100}),
    }


def sales_segments():
    return {
        "sales-seg-15-17": [
            (15, "store-1", 95),
            (16, "store-1", 98),
            (17, "store-1", 100),
        ],
    }


def tax_backups():
    return {"tax-b20": (20, {"region-1": 9})}


def tax_segments():
    return {"tax-seg-21-21": [(21, "region-1", 10)]}


def records(seg):
    from faust.tables.settlement import ChangelogRecord

    return [ChangelogRecord(offset=o, key=k, value=v) for o, k, v in seg]


def run1_manifest():
    """On-duty handover for the first D1 attempt."""
    return {
        SALES: {
            0: PartitionSource(
                backups=(
                    Backup("b14", 14, "sales-b14"),
                    Backup("b19", 19, "sales-b19"),
                ),
                segments=(LogSegment(15, 17, "sales-seg-15-17"),),
            )
        },
        TAX: {
            0: PartitionSource(
                backups=(Backup("b20", 20, "tax-b20"),),
                segments=(LogSegment(21, 21, "tax-seg-21-21"),),
            )
        },
    }


def run2_manifest():
    """After restart: sales logs 15-17 expired, backup 19 still around."""
    return {
        SALES: {
            0: PartitionSource(
                backups=(Backup("b19", 19, "sales-b19"),),
                segments=(),
            )
        },
        TAX: {
            0: PartitionSource(
                backups=(Backup("b20", 20, "tax-b20"),),
                segments=(LogSegment(21, 21, "tax-seg-21-21"),),
            )
        },
    }


def d1_definition():
    return SettlementDefinition(batch_id="D1", targets=D1_TARGETS)


@pytest.fixture()
def backup_store():
    return InMemoryBackupStore({**sales_backups(), **tax_backups()})


@pytest.fixture()
def segment_source():
    return InMemorySegmentSource(
        {
            loc: records(seg)
            for loc, seg in {**sales_segments(), **tax_segments()}.items()
        }
    )


@pytest.fixture()
def coordinator(tmp_path, backup_store, segment_source):
    coordinator = BatchRestoreCoordinator(
        tmp_path,
        backup_store=backup_store,
        segment_source=segment_source,
    )
    coordinator.register_published(D0)
    return coordinator


class TestPrecheck:
    def test_no_gaps(self, *, coordinator):
        assert coordinator.precheck(d1_definition(), run1_manifest()) == []

    def test_missing_log_segment_reported(self):
        definition = d1_definition()
        manifest = run1_manifest()
        manifest[SALES] = {
            0: PartitionSource(
                backups=(Backup("b14", 14, "sales-b14"),),
                segments=(LogSegment(15, 16, "sales-seg-15-16"),),
            )
        }
        gaps = precheck_definition(definition, manifest)
        assert len(gaps) == 1
        gap = gaps[0]
        assert gap.table == SALES
        assert gap.partition == 0
        assert (gap.first_offset, gap.last_offset) == (17, 17)
        assert gap.reason == "missing-log-segment"

    def test_missing_backup_reported(self):
        definition = SettlementDefinition(batch_id="D9", targets={TAX: {0: 21}})
        gaps = precheck_definition(definition, {TAX: {0: PartitionSource()}})
        assert len(gaps) == 1
        assert gaps[0].table == TAX
        assert gaps[0].reason == "missing-backup"

    def test_submit_returns_gap_receipt(self, *, coordinator, segment_source):
        segment_source.probe_failures.add("tax-seg-21-21")
        receipt = coordinator.submit(d1_definition(), run1_manifest())
        assert receipt.status == STATUS_GAP
        assert any(
            gap.table == TAX and gap.partition == 0 and gap.last_offset == 21
            for gap in receipt.gaps
        )
        # nothing was restored or published
        assert coordinator.current_settlement() is D0


class TestRestore:
    def test_publish_after_all_tables_reach_target(self, *, coordinator):
        receipt = coordinator.submit(d1_definition(), run1_manifest())
        assert receipt.status == STATUS_PUBLISHED
        assert receipt.settlement_id == "D1"
        settlement = coordinator.current_settlement()
        assert settlement.get(SALES, "store-1") == 100
        assert settlement.get(TAX, "region-1") == 10
        assert settlement.offsets[SALES][0] == 17
        assert settlement.offsets[TAX][0] == 21

    def test_failed_restore_keeps_published_settlement(
        self, *, coordinator, segment_source, tmp_path
    ):
        # tax segment readable at precheck, unreadable during replay.
        segment_source.read_failures.add("tax-seg-21-21")
        with pytest.raises(SettlementRestoreError):
            coordinator.submit(d1_definition(), run1_manifest())
        # queries still read D0
        current = coordinator.current_settlement()
        assert current.settlement_id == "D0"
        assert current.get(SALES, "store-1") == 80
        assert current.get(TAX, "region-1") == 8
        # sales p0 completed and was journaled before the failure
        journal = coordinator._load_journal("D1")
        assert journal["status"] == "restoring"
        assert journal["partitions"]["sales/0"]["status"] == "complete"
        assert journal["partitions"]["sales/0"]["offset"] == 17

    def test_restart_resumes_and_publishes(
        self, *, coordinator, segment_source, tmp_path, backup_store
    ):
        segment_source.read_failures.add("tax-seg-21-21")
        with pytest.raises(SettlementRestoreError):
            coordinator.submit(d1_definition(), run1_manifest())

        # restart with the same data directory; sales logs 15-17 expired.
        backup_store2 = InMemoryBackupStore({**sales_backups(), **tax_backups()})
        segment_source2 = InMemorySegmentSource(
            {"tax-seg-21-21": records(tax_segments()["tax-seg-21-21"])}
        )
        resumed = BatchRestoreCoordinator(
            tmp_path,
            backup_store=backup_store2,
            segment_source=segment_source2,
        )
        # published D0 survived the restart
        assert resumed.current_settlement().get(SALES, "store-1") == 80

        receipt = resumed.submit(d1_definition(), run2_manifest())
        assert receipt.status == STATUS_PUBLISHED
        settlement = resumed.current_settlement()
        assert settlement.settlement_id == "D1"
        assert settlement.get(SALES, "store-1") == 100
        assert settlement.get(TAX, "region-1") == 10
        # sales p0 was already done: no sales backup/log was touched
        assert backup_store2.loads == ["tax-b20"]
        assert segment_source2.reads == ["tax-seg-21-21"]

    def test_conflicting_definition_rejected_and_progress_kept(
        self, *, coordinator, segment_source, tmp_path
    ):
        segment_source.read_failures.add("tax-seg-21-21")
        with pytest.raises(SettlementRestoreError):
            coordinator.submit(d1_definition(), run1_manifest())

        resumed = BatchRestoreCoordinator(
            tmp_path,
            backup_store=InMemoryBackupStore({**sales_backups(), **tax_backups()}),
            segment_source=InMemorySegmentSource(
                {"tax-seg-21-21": records(tax_segments()["tax-seg-21-21"])}
            ),
        )
        # same settlement number, but tax target changed to 22
        conflicting = SettlementDefinition(
            batch_id="D1", targets={SALES: {0: 17}, TAX: {0: 22}}
        )
        with pytest.raises(SettlementConflictError):
            resumed.submit(conflicting, run2_manifest())
        # the completed sales partition of the original D1 is kept
        journal = resumed._load_journal("D1")
        assert journal["partitions"]["sales/0"]["status"] == "complete"
        assert resumed.current_settlement().settlement_id == "D0"

        # resubmitting the original definition finishes the restore
        receipt = resumed.submit(d1_definition(), run2_manifest())
        assert receipt.status == STATUS_PUBLISHED
        settlement = resumed.current_settlement()
        assert settlement.get(SALES, "store-1") == 100
        assert settlement.get(TAX, "region-1") == 10

    def test_resubmit_after_publish_is_idempotent(
        self, *, coordinator, backup_store, segment_source
    ):
        receipt = coordinator.submit(d1_definition(), run1_manifest())
        assert receipt.status == STATUS_PUBLISHED
        backup_store.loads.clear()
        segment_source.reads.clear()

        # the acknowledgement was lost; the submitter retries as-is
        again = coordinator.submit(d1_definition(), run1_manifest())
        assert again.status == STATUS_PUBLISHED
        assert again.settlement_id == receipt.settlement_id
        assert backup_store.loads == []
        assert segment_source.reads == []

    def test_published_settlement_survives_restart(self, *, coordinator, tmp_path):
        coordinator.submit(d1_definition(), run1_manifest())
        restarted = BatchRestoreCoordinator(tmp_path)
        settlement = restarted.current_settlement()
        assert settlement.settlement_id == "D1"
        assert settlement.get(SALES, "store-1") == 100
        assert settlement.get(TAX, "region-1") == 10


class TestPinnedReads:
    def test_reconciliation_keeps_first_read_settlement(self, *, coordinator):
        with coordinator.pin() as pinned:
            assert pinned.get(SALES, "store-1") == 80
            # settlement switches while the reconciliation is still running
            coordinator.submit(d1_definition(), run1_manifest())
            assert coordinator.current_settlement().settlement_id == "D1"
            # the pinned job still reads D0 for its second fetch
            assert pinned.get(TAX, "region-1") == 8
        # a reconciliation started after the switch reads D1
        with coordinator.pin() as new_job:
            assert new_job.get(SALES, "store-1") == 100
            assert new_job.get(TAX, "region-1") == 10

    def test_registry_pin_tracks_active_readers(self):
        registry = SettlementRegistry()
        registry.publish(D0)
        assert registry.pinned_count == 0
        with registry.pin() as pinned:
            assert pinned is D0
            assert registry.pinned_count == 1
            registry.publish(
                Settlement("D1", {SALES: {"store-1": 100}, TAX: {"region-1": 10}})
            )
            assert registry.current().settlement_id == "D1"
            assert pinned.settlement_id == "D0"
        assert registry.pinned_count == 0
