import asyncio
import json

import pytest

from s3fs import S3FileSystem


EAST = "east/raw.csv?versionId=A1"
BACKUP = "backup/raw.csv?versionId=B1"
ARCHIVE = "archive/raw.csv?versionId=C1"
TEMP = "scratch/raw.csv?versionId=D1"

LEDGER = {
    "batches": [
        {"name": "east", "copies": [EAST, BACKUP], "line": "华东批次"},
        {"name": "backfill", "copies": [EAST, ARCHIVE], "line": "补采批次"},
    ]
}


class FakeS3:
    """Only the version-exact calls used by cleanup."""

    def __init__(self):
        self.versions = {
            ("east", "raw.csv"): {"A1": b"a1", "A2": b"a2"},
            ("backup", "raw.csv"): {"B1": b"b1"},
            ("archive", "raw.csv"): {"C1": b"c1"},
            ("scratch", "raw.csv"): {"D1": b"d1"},
        }
        self.deleted = []
        self.on_delete = {}
        self.delete_started = {}

    async def call(self, method, *args, **kwargs):
        if method == "delete_object":
            key = (kwargs["Bucket"], kwargs["Key"])
            version = kwargs["VersionId"]
            started = self.delete_started.get(version)
            if started is not None:
                started.set()
            hook = self.on_delete.get(version)
            if hook is not None:
                await hook(kwargs)
            self.versions.setdefault(key, {}).pop(version, None)
            self.deleted.append((kwargs["Bucket"], kwargs["Key"], version))
            return {}
        if method == "head_object":
            key = (kwargs["Bucket"], kwargs["Key"])
            version = kwargs["VersionId"]
            if version not in self.versions.get(key, {}):
                raise FileNotFoundError(f"{key}?versionId={version}")
            data = self.versions[key][version]
            return {"ContentLength": len(data)}
        raise AssertionError(f"unexpected S3 call {method}")


def make_fs(fake):
    fs = S3FileSystem(asynchronous=True, skip_instance_cache=True)
    fs._call_s3 = fake.call
    return fs


def run(coro):
    return asyncio.run(coro)


def test_inflight_a_protects_b_and_c_and_reports_duplicates():
    async def scenario():
        fake = FakeS3()
        fs = make_fs(fake)
        started = asyncio.Event()
        release = asyncio.Event()

        async def hang(_kwargs):
            started.set()
            await release.wait()

        fake.on_delete["A1"] = hang

        jia = asyncio.create_task(
            fs._cleanup(LEDGER, [{"line": "甲-17", "targets": [EAST]}])
        )
        await started.wait()

        # 乙's line is processed while 甲's delete is still in flight.  A
        # readable A1 is not a safe second copy: a delayed delete may run.
        yi = await fs._cleanup(
            LEDGER,
            [
                {
                    "line": "乙-42",
                    "work_order": "WO-EAST-2",
                    "targets": [BACKUP, ARCHIVE],
                }
            ],
        )
        assert {receipt["line"] for receipt in yi} == {"乙-42"}
        assert all(receipt["status"] == "blocked" for receipt in yi)
        assert {receipt["status"] for receipt in yi} == {"blocked"}
        blocked = {receipt["target"]: receipt for receipt in yi}
        assert blocked[BACKUP]["protected_batches"] == ["east"]
        assert blocked[BACKUP]["blocked_batches"] == ["east"]
        assert blocked[ARCHIVE]["protected_batches"] == ["backfill"]
        assert blocked[ARCHIVE]["blocked_batches"] == ["backfill"]
        assert ("backup", "raw.csv", "B1") not in fake.deleted
        assert ("archive", "raw.csv", "C1") not in fake.deleted
        assert "A1" in fake.versions[("east", "raw.csv")]

        # The same version is reported as already pending, with the original
        # line and work order that owns the outstanding attempt.
        duplicate = await fs._cleanup(
            LEDGER,
            [{"line": "丙-9", "work_order": "WO-DUP", "targets": [EAST, TEMP]}],
        )
        by_target = {receipt["target"]: receipt for receipt in duplicate}
        assert by_target[EAST]["status"] == "pending"
        assert by_target[EAST]["action"] == "waiting"
        assert by_target[EAST]["pending_attempt"]["line"] == "甲-17"
        assert by_target[EAST]["requires_confirmation"] is True
        assert "delete_object" not in by_target[EAST]
        assert fake.deleted.count(("east", "raw.csv", "A1")) == 0

        # The temporary version has no replay dependency and is removed
        # independently, while A1 is still pending.
        assert by_target[TEMP]["status"] == "deleted"
        assert by_target[TEMP]["line"] == "丙-9"
        assert ("scratch", "raw.csv", "D1") in fake.deleted

        # A supplier writes A2 to the same key.  It must not be touched by
        # cleanup of A1.
        fake.versions[("east", "raw.csv")]["A2"] = b"a2-new"
        release.set()
        jia_receipt = (await jia)[0]
        assert jia_receipt["line"] == "甲-17"
        assert jia_receipt["status"] == "deleted"
        assert fake.deleted.count(("east", "raw.csv", "A1")) == 1
        assert fake.versions[("east", "raw.csv")]["A2"] == b"a2-new"

        status = await fs._cleanup_status(LEDGER)
        by_target = status.by_target()
        assert by_target["east/raw.csv?versionId=A1"]["status"] == "deleted"
        assert by_target["east/raw.csv?versionId=A1"]["available"] is False
        assert by_target[BACKUP]["status"] == "available"
        assert by_target[BACKUP]["protected_batches"] == ["east"]
        assert by_target[BACKUP]["can_remove"] is False
        assert by_target[ARCHIVE]["protected_batches"] == ["backfill"]
        assert by_target[ARCHIVE]["can_remove"] is False
        assert status.coverage["east"]["has_copy"] is True
        assert status.coverage["east"]["available"] == [BACKUP]
        assert status.coverage["backfill"]["has_copy"] is True
        assert status.coverage["backfill"]["available"] == [ARCHIVE]

    run(scenario())


def test_unconfirmed_delete_stays_pending_even_when_a1_is_readable():
    async def scenario():
        fake = FakeS3()
        fs = make_fs(fake)
        pending_delivery = asyncio.Event()
        started = asyncio.Event()

        async def timeout_without_response(_kwargs):
            # 甲 disconnected; the service may still execute this delete.
            started.set()
            await pending_delivery.wait()
            fake.versions[("east", "raw.csv")].pop("A1", None)
            fake.deleted.append(("east", "raw.csv", "A1"))

        fake.on_delete["A1"] = timeout_without_response
        first_task = asyncio.create_task(
            fs._cleanup(LEDGER, [{"line": "甲", "targets": [EAST]}])
        )
        # 甲 disconnected: the caller stopped waiting, but the service-side
        # delete is still queued to run later.
        await started.wait()

        # The worker task is still alive but its caller is gone.  Its delete
        # has not run yet, so A1 is directly readable; that does not make it a
        # safe replay copy.
        status = await fs._cleanup_status(LEDGER)
        assert status.by_target()["east/raw.csv?versionId=A1"]["status"] == "pending"
        assert status.by_target()["east/raw.csv?versionId=A1"]["available"] is False
        assert status.by_target()[BACKUP]["can_remove"] is False
        assert status.by_target()[ARCHIVE]["can_remove"] is False
        duplicate = await fs._cleanup(LEDGER, [{"line": "丙", "targets": [EAST]}])
        assert duplicate[0]["status"] == "pending"
        assert duplicate[0]["action"] == "waiting"
        assert duplicate[0]["pending_attempt"]["line"] == "甲"
        assert fake.deleted.count(("east", "raw.csv", "A1")) == 0

        # The service finally executes the original delete.
        pending_delivery.set()
        first = await first_task
        assert first[0]["status"] == "deleted"
        status = await fs._cleanup_status(LEDGER)
        assert status.by_target()["east/raw.csv?versionId=A1"]["status"] == "deleted"
        assert status.by_target()[BACKUP]["can_remove"] is False
        assert status.by_target()[ARCHIVE]["can_remove"] is False

    run(scenario())


def test_confirmation_marks_a_not_readable_but_b_and_c_stay_protected():
    async def scenario():
        fake = FakeS3()
        fs = make_fs(fake)

        async def ambiguous(_kwargs):
            raise asyncio.TimeoutError("connection lost")

        fake.on_delete["A1"] = ambiguous
        first = await fs._cleanup(LEDGER, [{"line": "甲", "targets": [EAST]}])
        assert first[0]["status"] == "pending"

        # The original request did reach S3 even though the reply was lost.
        fake.versions[("east", "raw.csv")].pop("A1")
        status = await fs._cleanup_status(LEDGER)
        by_target = status.by_target()
        assert by_target["east/raw.csv?versionId=A1"]["status"] == "deleted"
        assert by_target["east/raw.csv?versionId=A1"]["readable"] is False
        assert by_target["east/raw.csv?versionId=A1"]["protected_batches"] == [
            "backfill",
            "east",
        ]
        assert by_target[BACKUP]["status"] == "available"
        assert by_target[BACKUP]["protected_batches"] == ["east"]
        assert by_target[BACKUP]["can_remove"] is False
        assert by_target[ARCHIVE]["protected_batches"] == ["backfill"]
        assert by_target[ARCHIVE]["can_remove"] is False

    run(scenario())


def test_explicit_rejection_then_retry_clears_b_and_c():
    async def scenario():
        fake = FakeS3()
        fs = make_fs(fake)

        async def denied(_kwargs):
            raise PermissionError("Access Denied")

        fake.on_delete["A1"] = denied
        first = await fs._cleanup(LEDGER, [{"line": "甲", "targets": [EAST]}])
        assert first[0]["status"] == "rejected"
        assert first[0]["reason"].startswith("S3 explicitly rejected")
        assert first[0]["readable"] is True
        assert fake.versions[("east", "raw.csv")]["A1"] == b"a1"

        retry = await fs._cleanup(
            LEDGER, [{"line": "乙", "targets": [BACKUP, ARCHIVE]}]
        )
        assert [item["status"] for item in retry] == ["deleted", "deleted"]
        assert retry["summary"] == {"deleted": 2}

        status = await fs._cleanup_status(LEDGER)
        by_target = status.by_target()
        assert by_target["east/raw.csv?versionId=A1"]["status"] == "rejected"
        assert by_target["east/raw.csv?versionId=A1"]["readable"] is True
        assert by_target["east/raw.csv?versionId=A1"]["available"] is True
        assert by_target[BACKUP]["status"] == "deleted"
        assert by_target[ARCHIVE]["status"] == "deleted"
        # A1 remains the registered readable copy for both batches.
        assert status.coverage["east"]["has_copy"] is True
        assert status.coverage["backfill"]["has_copy"] is True

    run(scenario())


def test_mixed_receipt_does_not_clear_pending_and_tracks_source_lines():
    async def scenario():
        fake = FakeS3()
        fs = make_fs(fake)

        async def timeout(_kwargs):
            raise asyncio.TimeoutError("connection lost")

        fake.on_delete["A1"] = timeout
        report = await fs._cleanup(
            LEDGER,
            [
                {"line": "甲-1", "work_order": "W-1", "targets": [EAST, TEMP]},
                {"line": "乙-2", "work_order": "W-2", "targets": [BACKUP]},
            ],
        )
        by_target = {receipt["target"]: receipt for receipt in report}
        assert by_target[EAST]["line"] == "甲-1"
        assert by_target[EAST]["status"] == "pending"
        assert by_target[TEMP]["line"] == "甲-1"
        assert by_target[TEMP]["status"] == "deleted"
        assert by_target[BACKUP]["line"] == "乙-2"
        assert by_target[BACKUP]["status"] == "blocked"
        assert [item["target"] for item in report.for_line("甲-1")] == [
            EAST,
            TEMP,
        ]
        assert report["summary"]["pending"] == 1
        assert report["summary"]["deleted"] == 1
        assert report["summary"]["blocked"] == 1

        status = await fs._cleanup_status(LEDGER)
        assert status.by_target()["east/raw.csv?versionId=A1"]["status"] == "pending"
        assert status.by_target()["east/raw.csv?versionId=A1"]["delete_pending"] is True

    run(scenario())


def test_concurrent_work_orders_keep_one_copy_per_batch():
    async def scenario():
        fake = FakeS3()
        fs = make_fs(fake)

        # Give each delete a suspension point so both orders race for the
        # same cleanup decision state.
        async def slow(_kwargs):
            await asyncio.sleep(0)

        fake.on_delete.update({"A1": slow, "B1": slow, "C1": slow})

        first, second = await asyncio.gather(
            fs._cleanup(
                LEDGER, [{"line": "one", "work_order": "W-1", "targets": [EAST]}]
            ),
            fs._cleanup(
                LEDGER,
                [{"line": "two", "work_order": "W-2", "targets": [BACKUP, ARCHIVE]}],
            ),
        )
        results = {item["target"]: item["status"] for item in first + second}
        # A1 is shared by both batches; removing it is only safe while B1 and
        # C1 remain.  At most one of the three can be removed.
        assert list(results.values()).count("deleted") <= 1
        status = await fs._cleanup_status(LEDGER)
        assert status.coverage["east"]["has_copy"] is True
        assert status.coverage["backfill"]["has_copy"] is True

    run(scenario())


def test_unregistered_versions_are_never_deleted_implicitly():
    async def scenario():
        fake = FakeS3()
        fs = make_fs(fake)
        # A2 is the supplier's fresh version at the same key as A1.  It is not
        # registered for any replay batch and must not be touched when A1 is
        # cleaned up.
        await fs._cleanup(LEDGER, [{"line": "甲", "targets": [EAST]}])
        assert ("east", "raw.csv", "A2") not in fake.deleted
        assert fake.versions[("east", "raw.csv")]["A2"] == b"a2"
        assert ("backup", "raw.csv", "B1") not in fake.deleted
        assert ("archive", "raw.csv", "C1") not in fake.deleted

        # An unregistered temporary version is handled only when a work order
        # explicitly names that exact version.
        report = await fs._cleanup(
            LEDGER, [{"line": "临时", "targets": ["scratch/raw.csv?versionId=D1"]}]
        )
        assert report[0]["status"] == "deleted"
        assert report[0]["protected_batches"] == []

    run(scenario())


def test_cleanup_requires_explicit_version_and_accepts_ledger_files(tmp_path):
    async def scenario():
        fake = FakeS3()
        fs = make_fs(fake)
        ledger_file = tmp_path / "ledger.ndjson"
        ledger_file.write_text(
            "\n".join(json.dumps(row) for row in LEDGER["batches"]),
            encoding="utf8",
        )
        report = await fs._cleanup(
            str(ledger_file),
            [{"line": 3, "targets": ["east/raw.csv?versionId=A1"]}],
        )
        assert report[0]["line"] == 3
        assert report[0]["status"] == "deleted"
        assert report[0]["removed"] is True

        with pytest.raises(ValueError, match="explicit VersionId"):
            await fs._cleanup(LEDGER, [{"line": 4, "targets": ["east/raw.csv"]}])

        # A deletion marker at the same key must not satisfy replay.
        assert not hasattr(fs, "_cleanup_file")

    run(scenario())


def test_cleanup_accepts_mapping_and_tuple_forms(tmp_path):
    async def scenario():
        fake = FakeS3()
        fs = make_fs(fake)
        ledger = {
            "east": {"east/raw.csv": "A1", "backup/raw.csv": "B1"},
            "backfill": [("east", "raw.csv", "A1"), "archive/raw.csv?versionId=C1"],
        }
        report = await fs._cleanup(
            ledger,
            [
                {
                    "line": "乙",
                    "work_order": "W-7",
                    "targets": [("backup", "raw.csv", "B1")],
                }
            ],
        )
        assert report[0]["status"] == "deleted"
        assert report[0]["line"] == "乙"
        assert report[0]["work_order"] == "W-7"
        assert ("backup", "raw.csv", "B1") in fake.deleted

        status = await fs._cleanup_status(ledger)
        assert status.coverage["east"]["registered"] == [EAST, BACKUP]
        assert status.coverage["backfill"]["registered"] == [EAST, ARCHIVE]
        assert status.coverage_ok is True
        assert status.coverage["east"]["has_copy"] is True
        assert status.coverage["east"]["available"] == [EAST]

    run(scenario())


def test_cancelled_caller_does_not_orphan_outstanding_delete():
    async def scenario():
        fake = FakeS3()
        fs = make_fs(fake)
        started = asyncio.Event()
        release = asyncio.Event()

        async def hang(_kwargs):
            started.set()
            await release.wait()

        fake.on_delete["A1"] = hang
        task = asyncio.create_task(
            fs._cleanup(LEDGER, [{"line": "甲", "targets": [EAST]}])
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The delete worker must still be tracked.  A status lookup and a
        # duplicate work order both see the pending reservation, and neither
        # sends a second delete.
        status = await fs._cleanup_status(LEDGER)
        assert status.by_target()["east/raw.csv?versionId=A1"]["status"] == "pending"
        duplicate = await fs._cleanup(LEDGER, [{"line": "丙", "targets": [EAST, TEMP]}])
        by_target = {receipt["target"]: receipt for receipt in duplicate}
        assert by_target[EAST]["status"] == "pending"
        assert by_target[EAST]["pending_attempt"]["line"] == "甲"
        assert by_target[TEMP]["status"] == "deleted"

        release.set()
        for _ in range(5):
            await asyncio.sleep(0)
        assert ("east", "raw.csv", "A1") in fake.deleted
        status = await fs._cleanup_status(LEDGER)
        assert status.by_target()["east/raw.csv?versionId=A1"]["status"] == "deleted"

    run(scenario())
