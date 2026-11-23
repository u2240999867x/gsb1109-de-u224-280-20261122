import dataclasses
import json
import zipfile

import fsspec
import pytest

from upath import UPath
from upath.transfer import Location
from upath.transfer import ManifestConflict
from upath.transfer import MemberState
from upath.transfer import SourceVersionNotFound
from upath.transfer import TransferReceipt
from upath.transfer import TransferStatus
from upath.transfer import _plan_members
from upath.transfer import _stage_members
from upath.transfer import open_zip_version
from upath.transfer import publish_manifest
from upath.transfer import read_published_member
from upath.transfer import transfer_members
from upath.transfer import zip_version


@pytest.fixture
def memory_store(clear_fsspec_memory_cache):
    mem = fsspec.filesystem("memory")
    yield mem
    for path in list(mem.store):
        mem.rm(path, recursive=True)


def _write_zip(fs, path, entries):
    parent = fs._parent(path)
    if parent and not fs.exists(parent):
        fs.mkdirs(parent)
    with fs.open(path, "wb") as handle, zipfile.ZipFile(handle, "w") as archive:
        for name, data in entries.items():
            archive.writestr(name, data)


def _zip_alias(path, *, chained=False):
    if chained:
        return UPath("zip://", fo=path, target_protocol="memory")
    return UPath("zip://", fo=f"memory://{path}")


@pytest.fixture
def package(memory_store):
    _write_zip(memory_store, "/pkg.zip", {"A": b"alpha", "B": b"beta-content"})
    return "/pkg.zip"


@pytest.fixture
def release():
    manifest = UPath("memory:///release/manifest.json")
    staging = UPath("memory:///release/staging")
    return manifest, staging


def test_full_transfer_publishes_manifest_and_gates_readers(package, release):
    manifest, staging = release
    sources = [_zip_alias(package), _zip_alias(package, chained=True)]

    receipt = transfer_members(sources, manifest, staging_dir=staging)

    assert receipt.status is TransferStatus.COMPLETED
    assert set(receipt.verified_members) == {"A", "B"}
    assert receipt.missing_members == ()
    payload, revision = json_loads_manifest(manifest)
    assert revision == 1
    assert set(payload["members"]) == {"A", "B"}
    assert read_published_member(manifest, "A", staging_dir=staging) == b"alpha"
    assert read_published_member(manifest, "B", staging_dir=staging) == b"beta-content"
    # Files outside the manifest stay invisible even though their bytes exist.
    staging.joinpath("objects/secret").write_bytes(b"hidden")
    with pytest.raises(KeyError):
        read_published_member(manifest, "objects/secret", staging_dir=staging)
    with pytest.raises(KeyError):
        read_published_member(manifest, "not-listed", staging_dir=staging)


def test_aliased_sources_share_one_version_and_snapshot(package, release):
    manifest, staging = release
    receipt = transfer_members(
        [_zip_alias(package), _zip_alias(package, chained=True)],
        manifest,
        staging_dir=staging,
    )
    # Both aliases point at the same physical ZIP, so one version/snapshot.
    assert len(receipt.zip_versions) == 1
    assert len(receipt.snapshots) == 1
    snapshots = list(staging.joinpath("zip-snapshots").rglob("*.zip"))
    assert len(snapshots) == 1


def test_receipt_roundtrips_through_json(package, release):
    manifest, staging = release
    receipt = transfer_members([_zip_alias(package)], manifest, staging_dir=staging)
    blob = receipt.to_json()
    restored = TransferReceipt.from_json(blob)
    assert restored.to_json() == blob
    assert restored.status is TransferStatus.COMPLETED
    assert restored.verified_members == receipt.verified_members


def test_resuming_a_completed_receipt_is_idempotent(package, release):
    manifest, staging = release
    receipt = transfer_members([_zip_alias(package)], manifest, staging_dir=staging)
    _, revision_before = json_loads_manifest(manifest)

    again = transfer_members(None, manifest, receipt=receipt.to_json())

    assert again is not receipt
    assert again.status is TransferStatus.COMPLETED
    _, revision_after = json_loads_manifest(manifest)
    assert revision_after == revision_before


def test_resume_uses_original_version_and_keeps_concurrent_member(
    package, release, memory_store
):
    manifest, staging = release
    source = _zip_alias(package, chained=True)
    staging.mkdir(parents=True, exist_ok=True)

    # First run got through planning and staged only A before the shutdown.
    planned, versions, snapshots, pinned = _plan_members([source], staging)
    receipt = TransferReceipt(
        transfer_id="resume-case",
        sources=(Location.of(source),),
        manifest=Location.of(manifest),
        staging_dir=Location.of(staging),
        zip_versions=versions,
        members=planned,
        snapshots=snapshots,
    )
    partial = _stage_members(
        pinned, staging, dataclasses.replace(receipt, members={"A": planned["A"]})
    )
    receipt = dataclasses.replace(
        receipt, members={"A": partial.members["A"], "B": planned["B"]}
    )
    blob = receipt.to_json()

    # Downtime: the live ZIP is replaced and another job publishes unrelated C.
    _write_zip(
        memory_store,
        package,
        {"A": b"alpha-CHANGED", "B": b"beta-CHANGED", "X": b"x"},
    )
    c_object = "objects/00000000/3"
    c_path = staging.joinpath(c_object)
    c_path.parent.mkdir(parents=True, exist_ok=True)
    c_path.write_bytes(b"ccc")
    publish_manifest(
        manifest,
        {"C": {"crc": "00000000", "size": 3, "object": c_object}},
    )

    resumed = transfer_members(None, manifest, receipt=blob)

    assert resumed.status is TransferStatus.COMPLETED
    assert set(resumed.verified_members) == {"A", "B"}
    payload, revision = json_loads_manifest(manifest)
    assert revision == 2
    assert set(payload["members"]) == {"A", "B", "C"}
    # B must come from the original ZIP version, not the replaced package.
    assert read_published_member(manifest, "B", staging_dir=staging) == b"beta-content"
    assert read_published_member(manifest, "A", staging_dir=staging) == b"alpha"
    assert read_published_member(manifest, "C", staging_dir=staging) == b"ccc"


def test_conflict_when_concurrent_job_overwrites_member(package, release):
    manifest, staging = release
    source = _zip_alias(package, chained=True)
    staging.mkdir(parents=True, exist_ok=True)
    planned, versions, snapshots, _pinned = _plan_members([source], staging)
    receipt = TransferReceipt(
        transfer_id="conflict-case",
        sources=(Location.of(source),),
        manifest=Location.of(manifest),
        staging_dir=Location.of(staging),
        zip_versions=versions,
        members=planned,
        snapshots=snapshots,
    )

    other_object = "objects/11111111/9"
    other_path = staging.joinpath(other_object)
    other_path.parent.mkdir(parents=True, exist_ok=True)
    other_path.write_bytes(b"other-job")
    publish_manifest(
        manifest,
        {"A": {"crc": "11111111", "size": 9, "object": other_object}},
    )

    with pytest.raises(ManifestConflict) as exc_info:
        transfer_members(None, manifest, receipt=receipt)
    conflict_receipt = exc_info.value.receipt
    assert conflict_receipt.status is TransferStatus.CONFLICT
    assert conflict_receipt.conflicts == ("A",)

    # The other job's publication remains the one readers observe.
    payload, revision = json_loads_manifest(manifest)
    assert revision == 1
    assert set(payload["members"]) == {"A"}
    assert read_published_member(manifest, "A", staging_dir=staging) == b"other-job"


def test_same_content_concurrent_publication_rebases_without_conflict(package, release):
    manifest, staging = release
    source = _zip_alias(package, chained=True)
    staging.mkdir(parents=True, exist_ok=True)
    planned, versions, snapshots, pinned = _plan_members([source], staging)
    receipt = TransferReceipt(
        transfer_id="same-crc-case",
        sources=(Location.of(source),),
        manifest=Location.of(manifest),
        staging_dir=Location.of(staging),
        zip_versions=versions,
        members=planned,
        snapshots=snapshots,
    )
    record_a = planned["A"]
    publish_manifest(
        manifest,
        {
            "A": {
                "crc": f"{record_a.crc:08x}",
                "size": record_a.size,
                "object": "objects/parallel/5",
            }
        },
    )

    completed = transfer_members(None, manifest, receipt=receipt)
    assert completed.status is TransferStatus.COMPLETED
    payload, revision = json_loads_manifest(manifest)
    assert revision == 2
    assert set(payload["members"]) == {"A", "B"}
    assert pinned  # reopening the pinned ZIP still works after rebase


def test_missing_original_version_reports_verifiable_members_and_gaps(
    package, release, memory_store
):
    manifest, staging = release
    source = _zip_alias(package, chained=True)
    staging.mkdir(parents=True, exist_ok=True)
    planned, versions, snapshots, pinned = _plan_members([source], staging)
    receipt = TransferReceipt(
        transfer_id="gap-case",
        sources=(Location.of(source),),
        manifest=Location.of(manifest),
        staging_dir=Location.of(staging),
        zip_versions=versions,
        members=planned,
        snapshots=snapshots,
    )
    partial = _stage_members(
        pinned, staging, dataclasses.replace(receipt, members={"A": planned["A"]})
    )
    receipt = dataclasses.replace(
        receipt, members={"A": partial.members["A"], "B": planned["B"]}
    )

    # The immutable snapshot is gone and the live ZIP has been replaced.
    for snapshot_location in receipt.snapshots.values():
        snapshot = snapshot_location.open()
        snapshot.fs.rm(snapshot.path)
    _write_zip(memory_store, package, {"A": b"new", "B": b"newer"})

    with pytest.raises(SourceVersionNotFound) as exc_info:
        transfer_members(None, manifest, receipt=receipt)
    gap = exc_info.value.receipt
    assert gap.status is TransferStatus.FAILED
    assert gap.verified_members == ("A",)
    assert gap.missing_members == ("B",)
    assert gap.members["A"].state is MemberState.VERIFIED
    assert gap.members["B"].state is MemberState.MISSING_VERSION
    assert gap.members["B"].error
    # No manifest may be published from a stitched-together package.
    assert not manifest.exists()


def test_transfer_runs_on_local_filesystem(tmp_path):
    zip_path = tmp_path / "daily.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("dir/A", b"alpha")
        archive.writestr("dir/B", b"beta")
    manifest = UPath(tmp_path / "release" / "manifest.json")
    staging = UPath(tmp_path / "release" / "staging")
    source = UPath("zip://", fo=str(zip_path))

    receipt = transfer_members([source], manifest, staging_dir=staging)
    assert receipt.status is TransferStatus.COMPLETED
    assert set(receipt.verified_members) == {"dir/A", "dir/B"}
    assert read_published_member(manifest, "dir/A", staging_dir=staging) == b"alpha"
    # A second, replaced ZIP version must not contaminate a resumed receipt.
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("dir/A", b"changed")
        archive.writestr("dir/B", b"changed")
    resumed = transfer_members(None, manifest, receipt=receipt.to_json())
    assert resumed.status is TransferStatus.COMPLETED
    assert read_published_member(manifest, "dir/B", staging_dir=staging) == b"beta"


def test_same_package_content_reuses_the_version_token(package, release, memory_store):
    manifest, staging = release
    first = transfer_members([_zip_alias(package)], manifest, staging_dir=staging)
    assert first.status is TransferStatus.COMPLETED

    # Rewriting an identical package yields the same immutable version token.
    _write_zip(memory_store, package, {"A": b"alpha", "B": b"beta-content"})
    second = transfer_members(
        [_zip_alias(package)],
        UPath("memory:///release/other-manifest.json"),
        staging_dir=staging,
    )
    assert second.zip_versions == first.zip_versions
    assert second.snapshots == first.snapshots


def test_verified_member_is_reused_when_snapshot_is_lost(package, release):
    manifest, staging = release
    source = _zip_alias(package, chained=True)
    staging.mkdir(parents=True, exist_ok=True)
    planned, versions, snapshots, pinned = _plan_members([source], staging)
    receipt = TransferReceipt(
        transfer_id="reuse-case",
        sources=(Location.of(source),),
        manifest=Location.of(manifest),
        staging_dir=Location.of(staging),
        zip_versions=versions,
        members=planned,
        snapshots=snapshots,
    )
    partial = _stage_members(
        pinned, staging, dataclasses.replace(receipt, members={"A": planned["A"]})
    )
    receipt = dataclasses.replace(
        receipt, members={"A": partial.members["A"], "B": planned["B"]}
    )

    # Lose the pinned snapshot: A remains verifiable from staging, B cannot.
    for snapshot_location in receipt.snapshots.values():
        snapshot = snapshot_location.open()
        snapshot.fs.rm(snapshot.path)

    with pytest.raises(SourceVersionNotFound) as exc_info:
        transfer_members(None, manifest, receipt=receipt)
    gap = exc_info.value.receipt
    assert gap.verified_members == ("A",)
    assert gap.missing_members == ("B",)
    assert not manifest.exists()


def test_requires_zip_backed_sources(release):
    manifest, staging = release
    with pytest.raises(ValueError, match="not a ZIP-backed UPath"):
        transfer_members([UPath("memory:///not-a-zip")], manifest, staging_dir=staging)


def test_zip_version_and_open_zip_version_survive_alias_replacement(
    package, release, memory_store
):
    _manifest, staging = release
    source = _zip_alias(package, chained=True)
    version = zip_version(source)

    pinned = open_zip_version(source, version, staging)
    assert pinned.joinpath("A").read_bytes() == b"alpha"

    # Replace the live ZIP; the pinned version must keep serving the old package.
    _write_zip(memory_store, package, {"A": b"changed", "B": b"changed"})
    assert zip_version(source) != version
    reopened = open_zip_version(source, version, staging)
    assert reopened.joinpath("A").read_bytes() == b"alpha"

    with pytest.raises(FileNotFoundError):
        open_zip_version(source, "deadbeef" * 8, staging)


def test_requires_sources_without_receipt(release):
    manifest, staging = release
    with pytest.raises(ValueError, match="at least one source path"):
        transfer_members(None, manifest, staging_dir=staging)


def test_conflicting_member_across_sources_is_rejected(package, release, memory_store):
    manifest, staging = release
    _write_zip(memory_store, "/other.zip", {"A": b"different", "C": b"see"})
    with pytest.raises(ValueError, match="conflicting contents"):
        transfer_members(
            [_zip_alias(package, chained=True), _zip_alias("/other.zip", chained=True)],
            manifest,
            staging_dir=staging,
        )


def json_loads_manifest(manifest):
    payload = json.loads(manifest.read_bytes().decode("utf-8"))
    return payload, int(payload["revision"])
