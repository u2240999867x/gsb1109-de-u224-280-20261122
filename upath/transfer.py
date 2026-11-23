"""Resumable batch transfers of unpacked ZIP members into a manifest-gated store.

The daily package lands as a single ZIP. The ingestion side only exposes members
that are listed in a *release manifest*; files that are not listed stay invisible
to readers. This module copies members out of one or more (possibly aliased)
ZIP-backed :class:`~upath.UPath` locations into a content-addressed staging area
and swaps the release manifest in only when every requested member has been
verified against the *original* ZIP version.

Every run returns a :class:`TransferReceipt` that can be serialized to JSON and
handed back to :func:`transfer_members` to resume an interrupted transfer:

* already staged and verified members are reused without re-reading the source;
* pending members are only ever read from a ZIP whose version token (a hash of
  the backing file) still matches the receipt, so a replaced ZIP can never be
  stitched together with the old package;
* the manifest is replaced conditionally on the revision recorded in the
  receipt, so concurrent publications are either merged (unrelated members) or
  reported as conflicts without clobbering another job's publication.
"""

from __future__ import annotations

import dataclasses
import enum
import hashlib
import json
import logging
import shutil
import uuid
import zlib
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from upath.core import UPath

logger = logging.getLogger(__name__)

_SCHEMA = "upath.transfer/1"
_OBJECTS_DIRNAME = ".transfer-objects"

__all__ = [
    "Location",
    "ManifestConflict",
    "MemberReceipt",
    "MemberState",
    "SourceVersionNotFound",
    "TransferError",
    "TransferReceipt",
    "TransferStatus",
    "open_zip_version",
    "publish_manifest",
    "read_published_member",
    "transfer_members",
    "zip_version",
]


class TransferStatus(str, enum.Enum):
    """Lifecycle state of a whole batch transfer."""

    PENDING = "pending"
    COMPLETED = "completed"
    CONFLICT = "conflict"
    FAILED = "failed"


class MemberState(str, enum.Enum):
    """Lifecycle state of a single member within a transfer."""

    PENDING = "pending"
    VERIFIED = "verified"
    MISSING_VERSION = "missing_version"


class TransferError(Exception):
    """Base class for transfer failures that carry a resumable receipt."""

    def __init__(self, message: str, receipt: TransferReceipt) -> None:
        super().__init__(message)
        self.receipt = receipt


class ManifestConflict(TransferError):
    """Raised when the release manifest changed in a way we would overwrite."""


class SourceVersionNotFound(TransferError):
    """Raised when the original ZIP version can no longer be reopened.

    The attached receipt still lists every member that remains verifiable in the
    staging area and the members that are missing; it never contains a mix of old
    and new package contents.
    """


@dataclass(frozen=True)
class Location:
    """Everything needed to reopen a UPath after a restart."""

    url: str
    storage_options: dict[str, Any]

    @classmethod
    def of(cls, path: UPath) -> Location:
        return cls(url=str(path), storage_options=dict(path.storage_options))

    def open(self) -> UPath:
        return UPath(self.url, **self.storage_options)

    def to_dict(self) -> dict[str, Any]:
        return {"url": self.url, "storage_options": self.storage_options}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Location:
        return cls(url=data["url"], storage_options=dict(data["storage_options"]))


@dataclass(frozen=True)
class MemberReceipt:
    """Receipt record for one transferred member."""

    name: str
    source_idx: int
    size: int
    crc: int
    state: MemberState = MemberState.PENDING
    object_key: str | None = None
    error: str | None = None

    @property
    def is_verified(self) -> bool:
        return self.state is MemberState.VERIFIED

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_idx": self.source_idx,
            "size": self.size,
            "crc": self.crc,
            "state": self.state.value,
            "object_key": self.object_key,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MemberReceipt:
        return cls(
            name=data["name"],
            source_idx=data["source_idx"],
            size=data["size"],
            crc=data["crc"],
            state=MemberState(data["state"]),
            object_key=data.get("object_key"),
            error=data.get("error"),
        )


@dataclass(frozen=True)
class TransferReceipt:
    """Serializable, resumable receipt of a batch transfer.

    Use :meth:`to_json` to persist it between processes and :meth:`from_json`
    together with the ``receipt`` argument of :func:`transfer_members` to resume.
    """

    transfer_id: str
    sources: tuple[Location, ...]
    manifest: Location
    staging_dir: Location
    zip_versions: dict[str, str]
    members: dict[str, MemberReceipt]
    snapshots: dict[str, Location] = field(default_factory=dict)
    status: TransferStatus = TransferStatus.PENDING
    expected_manifest_revision: int | None = None
    conflicts: tuple[str, ...] = ()

    @property
    def verified_members(self) -> tuple[str, ...]:
        """Members whose staged object still matches the recorded ZIP CRC."""
        return tuple(
            name
            for name, member in self.members.items()
            if member.state is MemberState.VERIFIED
        )

    @property
    def missing_members(self) -> tuple[str, ...]:
        """Members that could not be read from the original ZIP version."""
        return tuple(
            name
            for name, member in self.members.items()
            if member.state in (MemberState.PENDING, MemberState.MISSING_VERSION)
        )

    @property
    def is_complete(self) -> bool:
        return self.status is TransferStatus.COMPLETED

    def to_json(self) -> str:
        return json.dumps(
            {
                "schema": _SCHEMA,
                "transfer_id": self.transfer_id,
                "sources": [source.to_dict() for source in self.sources],
                "manifest": self.manifest.to_dict(),
                "staging_dir": self.staging_dir.to_dict(),
                "zip_versions": self.zip_versions,
                "snapshots": {
                    url: location.to_dict() for url, location in self.snapshots.items()
                },
                "members": {
                    name: member.to_dict() for name, member in self.members.items()
                },
                "status": self.status.value,
                "expected_manifest_revision": self.expected_manifest_revision,
                "conflicts": list(self.conflicts),
            },
            indent=2,
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, data: str | bytes | Mapping[str, Any]) -> TransferReceipt:
        payload = json.loads(data) if isinstance(data, (str, bytes)) else dict(data)
        if payload.get("schema") != _SCHEMA:
            raise ValueError(
                f"unsupported transfer receipt schema: {payload.get('schema')!r}"
            )
        return cls(
            transfer_id=payload["transfer_id"],
            sources=tuple(Location.from_dict(item) for item in payload["sources"]),
            manifest=Location.from_dict(payload["manifest"]),
            staging_dir=Location.from_dict(payload["staging_dir"]),
            zip_versions=dict(payload["zip_versions"]),
            snapshots={
                url: Location.from_dict(item)
                for url, item in (payload.get("snapshots") or {}).items()
            },
            members={
                name: MemberReceipt.from_dict(item)
                for name, item in payload["members"].items()
            },
            status=TransferStatus(payload["status"]),
            expected_manifest_revision=payload.get("expected_manifest_revision"),
            conflicts=tuple(payload.get("conflicts") or ()),
        )


def _backing_location(source: UPath) -> Location:
    """Return the location of the physical ZIP behind a (possibly chained) path."""
    if source.protocol == "zip":
        options = dict(source.storage_options)
        fo = options["fo"]
        target_protocol = options.get("target_protocol")
        target_options = dict(options.get("target_options") or {})
        return Location.of(UPath(fo, protocol=target_protocol, **target_options))
    return Location.of(source)


def _content_token(path: UPath) -> str:
    """Stable content hash, used as an immutable version number."""
    digest = hashlib.sha256()
    with path.fs.open(path.path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _zip_version(source: UPath) -> tuple[str, str]:
    backing = _backing_location(source)
    return backing.url, _content_token(backing.open())


def zip_version(source: UPath) -> str:
    """Return the immutable version id (content hash) of a ZIP-backed UPath.

    The id is stable for unchanged bytes, so replacing the ZIP behind an alias
    produces a new id while republishing identical content reuses the old one.
    """
    if source.protocol != "zip":
        raise ValueError(
            f"expected a ZIP-backed UPath, got protocol {source.protocol!r}"
        )
    return _zip_version(source)[1]


def open_zip_version(source: UPath, version: str, staging_dir: UPath) -> UPath:
    """Reopen the whole ZIP pinned at ``version``.

    The returned read-only UPath always resolves to the exact package that had
    this version id, even after the live ZIP behind the alias is replaced.

    Raises
    ------
    FileNotFoundError
        If neither a pinned snapshot nor the live alias matches ``version``.
    """
    if source.protocol != "zip":
        raise ValueError(
            f"expected a ZIP-backed UPath, got protocol {source.protocol!r}"
        )
    snapshot = _snapshot_path(staging_dir, version)
    if not snapshot.exists() and zip_version(source) != version:
        raise FileNotFoundError(
            f"ZIP version {version!r} is no longer available at {str(source)!r}"
        )
    pinned, _ = _pin_zip(source, staging_dir, version)
    return pinned


def _snapshot_path(staging_dir: UPath, version: str) -> UPath:
    return staging_dir.joinpath("zip-snapshots", version[:2], f"{version}.zip")


def _pin_zip(source: UPath, staging_dir: UPath, version: str) -> tuple[UPath, UPath]:
    """Copy the ZIP into a content-addressed snapshot and reopen it read-only.

    Returns the read-only ZIP path and the raw snapshot file path. The snapshot
    is the immutable version handle: subsequent member reads always hit this
    exact package, even if the live ZIP behind the UPath alias is replaced.
    """
    backing = _backing_location(source).open()
    snapshot = _snapshot_path(staging_dir, version)
    if not snapshot.exists():
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = snapshot.parent.joinpath(f".{snapshot.name}.{uuid.uuid4().hex}")
        with (
            backing.fs.open(backing.path, "rb") as src,
            tmp_path.fs.open(tmp_path.path, "wb") as dst,
        ):
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        tmp_path.fs.mv(tmp_path.path, snapshot.path)
    if _content_token(snapshot) != version:
        raise OSError(f"snapshot content token mismatch for version {version!r}")
    pinned = UPath(
        "zip://",
        fo=snapshot.path,
        target_protocol=snapshot.protocol,
        target_options=dict(snapshot.storage_options) or {},
    )
    return pinned, snapshot


def _plan_members(
    sources: Sequence[UPath],
    staging_dir: UPath,
) -> tuple[
    dict[str, MemberReceipt],
    dict[str, str],
    dict[str, Location],
    tuple[UPath, ...],
]:
    """Pin every ZIP version and collect the union of file members."""
    planned: dict[str, MemberReceipt] = {}
    zip_versions: dict[str, str] = {}
    snapshots: dict[str, Location] = {}
    pinned_sources: list[UPath] = []
    for idx, source in enumerate(sources):
        if source.protocol != "zip":
            raise ValueError(
                f"source {str(source)!r} is not a ZIP-backed UPath"
                f" (got protocol {source.protocol!r})"
            )
        url, version = _zip_version(source)
        zip_versions.setdefault(url, version)
        pinned, snapshot = _pin_zip(source, staging_dir, version)
        snapshots.setdefault(url, Location.of(snapshot))
        pinned_sources.append(pinned)
        for member in pinned.rglob("*"):
            if not member.is_file():
                continue
            name = member.path
            info = member.fs.info(member.path)
            if name in planned:
                existing = planned[name]
                if existing.crc != int(info["CRC"]) or existing.size != int(
                    info["size"]
                ):
                    raise ValueError(
                        f"member {name!r} has conflicting contents across sources"
                    )
                continue
            planned[name] = MemberReceipt(
                name=name,
                source_idx=idx,
                size=int(info["size"]),
                crc=int(info["CRC"]),
            )
    if not planned:
        raise ValueError("no file members found in the given source paths")
    return planned, zip_versions, snapshots, tuple(pinned_sources)


def _object_key(crc: int, size: int) -> str:
    return f"objects/{crc:08x}/{size}"


def _crc32(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF


def _read_manifest(manifest_path: UPath) -> tuple[dict[str, Any], int]:
    """Return (payload, revision); an absent manifest is an empty one."""
    if not manifest_path.exists():
        return {"revision": 0, "members": {}}, 0
    payload = json.loads(manifest_path.read_bytes().decode("utf-8"))
    payload.setdefault("revision", 0)
    payload.setdefault("members", {})
    return payload, int(payload["revision"])


def _entry_crc(entry: Mapping[str, Any] | None) -> int | None:
    if not entry:
        return None
    crc = entry.get("crc")
    if crc is None:
        return None
    return int(crc, 16) if isinstance(crc, str) else int(crc)


def _write_manifest_atomic(manifest_path: UPath, payload: dict[str, Any]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = manifest_path.parent.joinpath(
        f".{manifest_path.name}.tmp.{uuid.uuid4().hex}"
    )
    tmp_path.write_bytes(json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"))
    manifest_path.fs.mv(tmp_path.path, manifest_path.path)


def publish_manifest(
    manifest_path: UPath,
    members: Mapping[str, Mapping[str, Any]],
) -> int:
    """Publish a new manifest revision (used by concurrent ingestion jobs).

    Returns the new revision number. This bumps the revision unconditionally, so
    any in-flight transfer that based its work on an older revision will observe
    the change at its next conditional commit.
    """
    payload, revision = _read_manifest(manifest_path)
    merged = dict(payload["members"])
    merged.update({name: dict(meta) for name, meta in members.items()})
    new_payload = {"revision": revision + 1, "members": merged}
    _write_manifest_atomic(manifest_path, new_payload)
    return revision + 1


def _audit_verified_staging(
    staging_dir: UPath,
    receipt: TransferReceipt,
) -> TransferReceipt:
    """Re-hash staged objects so the receipt only claims verifiable members."""
    members = dict(receipt.members)
    for name, record in members.items():
        if record.state is not MemberState.VERIFIED or record.object_key is None:
            continue
        staged = staging_dir.joinpath(record.object_key)
        if not staged.exists():
            members[name] = dataclasses.replace(
                record,
                state=MemberState.MISSING_VERSION,
                error="staged object disappeared before re-verification",
            )
            continue
        data = staged.read_bytes()
        if len(data) != record.size or _crc32(data) != record.crc:
            members[name] = dataclasses.replace(
                record,
                state=MemberState.MISSING_VERSION,
                error="staged object no longer matches the recorded ZIP CRC",
            )
    return dataclasses.replace(receipt, members=members)


def _fail_on_version_loss(
    staging_dir: UPath,
    receipt: TransferReceipt,
    detail: str,
) -> SourceVersionNotFound:
    """Build the gap receipt: verified members kept, everything else a gap."""
    audited = _audit_verified_staging(staging_dir, receipt)
    members = dict(audited.members)
    for name, record in members.items():
        if record.state is MemberState.VERIFIED:
            continue
        members[name] = dataclasses.replace(
            record, state=MemberState.MISSING_VERSION, error=detail
        )
    failed = dataclasses.replace(audited, members=members, status=TransferStatus.FAILED)
    return SourceVersionNotFound(detail, failed)


def _open_pinned_sources(
    staging_dir: UPath,
    receipt: TransferReceipt,
) -> tuple[UPath, ...]:
    """Reopen the exact ZIP versions pinned when the transfer was planned.

    A missing snapshot means the original version cannot be reopened; the gap
    receipt keeps every member that is still verifiable in staging and marks the
    rest, rather than falling back to the (possibly replaced) live ZIP.
    """
    pinned: list[UPath] = []
    for location in receipt.sources:
        source = location.open()
        backing_url = _backing_location(source).url
        snapshot_location = receipt.snapshots.get(backing_url)
        if snapshot_location is None:
            raise _fail_on_version_loss(
                staging_dir,
                receipt,
                "receipt does not record a pinned snapshot for the source ZIP",
            )
        snapshot = snapshot_location.open()
        if not snapshot.exists():
            raise _fail_on_version_loss(
                staging_dir,
                receipt,
                "pinned snapshot of the original ZIP version is gone",
            )
        expected = receipt.zip_versions.get(backing_url)
        if expected is not None and _content_token(snapshot) != expected:
            raise _fail_on_version_loss(
                staging_dir,
                receipt,
                "pinned snapshot no longer matches the recorded ZIP version",
            )
        pinned.append(
            UPath(
                "zip://",
                fo=snapshot.path,
                target_protocol=snapshot.protocol,
                target_options=dict(snapshot.storage_options) or {},
            )
        )
    return tuple(pinned)


def _stage_members(
    sources: Sequence[UPath],
    staging_dir: UPath,
    receipt: TransferReceipt,
) -> TransferReceipt:
    """Stage and verify every member, reusing anything already verified."""
    members = dict(receipt.members)
    for name, record in members.items():
        if record.state is MemberState.VERIFIED:
            continue
        object_key = _object_key(record.crc, record.size)
        target = staging_dir.joinpath(object_key)
        source = sources[record.source_idx].joinpath(name)
        try:
            data = source.read_bytes()
        except (FileNotFoundError, OSError) as exc:
            members[name] = dataclasses.replace(
                record,
                state=MemberState.MISSING_VERSION,
                error=f"member unreadable from the original ZIP version: {exc}",
            )
            continue
        if len(data) != record.size or _crc32(data) != record.crc:
            members[name] = dataclasses.replace(
                record,
                state=MemberState.MISSING_VERSION,
                error="member content does not match the recorded ZIP CRC",
            )
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        members[name] = dataclasses.replace(
            record, object_key=object_key, state=MemberState.VERIFIED
        )
    return dataclasses.replace(receipt, members=members)


def _commit_manifest(
    manifest_path: UPath,
    receipt: TransferReceipt,
) -> TransferReceipt:
    """Conditionally swap the manifest, preserving concurrent publications."""
    payload, current_revision = _read_manifest(manifest_path)
    current_members = payload["members"]

    if receipt.expected_manifest_revision is None:
        # First commit attempt: pin the revision we are basing on.
        receipt = dataclasses.replace(
            receipt, expected_manifest_revision=current_revision
        )
    elif current_revision != receipt.expected_manifest_revision:
        # Another job committed in between our attempts. Rebase onto the new
        # revision instead of blindly overwriting it; the content check below
        # still decides whether any of *our* members were republished.
        logger.info(
            "manifest moved from revision %s to %s before commit; rebasing",
            receipt.expected_manifest_revision,
            current_revision,
        )

    conflicts = [
        name
        for name, record in receipt.members.items()
        if record.state is MemberState.VERIFIED
        and _entry_crc(current_members.get(name)) not in (None, record.crc)
    ]
    if conflicts:
        return dataclasses.replace(
            receipt, status=TransferStatus.CONFLICT, conflicts=tuple(conflicts)
        )

    merged = dict(current_members)
    for name, record in receipt.members.items():
        if record.state is not MemberState.VERIFIED:
            continue
        merged[name] = {
            "crc": f"{record.crc:08x}",
            "size": record.size,
            "object": record.object_key,
        }
    _write_manifest_atomic(
        manifest_path,
        {"revision": current_revision + 1, "members": merged},
    )
    return dataclasses.replace(
        receipt,
        status=TransferStatus.COMPLETED,
        expected_manifest_revision=current_revision + 1,
        conflicts=(),
    )


def _default_staging(manifest_path: UPath) -> UPath:
    return manifest_path.parent.joinpath(_OBJECTS_DIRNAME)


def transfer_members(
    sources: Sequence[UPath] | UPath | None,
    manifest: UPath,
    *,
    staging_dir: UPath | None = None,
    receipt: TransferReceipt | str | None = None,
) -> TransferReceipt:
    """Transfer ZIP members into staging and conditionally publish the manifest.

    Parameters
    ----------
    sources:
        One or more ``UPath`` objects pointing into (possibly aliased) ZIP
        archives. Member names are archive-relative paths; duplicate names
        across sources are transferred once and must agree on size and CRC.
    manifest:
        Writable location of the release manifest JSON. Only members listed in
        it are visible to readers.
    staging_dir:
        Writable directory for content-addressed member objects. Defaults to a
        shared ``.transfer-objects`` directory next to the manifest.
    receipt:
        A :class:`TransferReceipt` (or its JSON form) from an interrupted run.
        When given, verified members are reused and pending members are read
        exclusively from the ZIP version recorded in that receipt.

    Returns
    -------
    TransferReceipt
        The updated receipt, with :attr:`TransferReceipt.status` set to
        :attr:`TransferStatus.COMPLETED` on success.

    Raises
    ------
    SourceVersionNotFound
        If the original ZIP version cannot be reopened anymore. The receipt on
        the exception separates still-verifiable members from gaps and never
        publishes a manifest stitched from old and new package contents.
    ManifestConflict
        If another job republished a manifest entry that this transfer would
        overwrite. The other publication is left intact for readers.
    """
    if isinstance(receipt, str):
        receipt = TransferReceipt.from_json(receipt)

    if sources is None:
        source_paths: tuple[UPath, ...] = ()
    elif isinstance(sources, UPath):
        source_paths = (sources,)
    else:
        source_paths = tuple(sources)
    if receipt is None and not source_paths:
        raise ValueError("at least one source path is required")

    if receipt is None:
        transfer_id = uuid.uuid4().hex
        staging = staging_dir or _default_staging(manifest)
        staging.mkdir(parents=True, exist_ok=True)
        planned, zip_versions, snapshots, pinned_paths = _plan_members(
            source_paths, staging
        )
        receipt = TransferReceipt(
            transfer_id=transfer_id,
            sources=tuple(Location.of(source) for source in source_paths),
            manifest=Location.of(manifest),
            staging_dir=Location.of(staging),
            zip_versions=zip_versions,
            members=planned,
            snapshots=snapshots,
        )
        source_paths = pinned_paths
    else:
        if receipt.status is TransferStatus.COMPLETED:
            return receipt
        manifest = receipt.manifest.open()
        staging = receipt.staging_dir.open()
        staging.mkdir(parents=True, exist_ok=True)
        source_paths = _open_pinned_sources(staging, receipt)
    receipt = _stage_members(source_paths, staging, receipt)

    if receipt.missing_members:
        return dataclasses.replace(receipt, status=TransferStatus.FAILED)

    updated = _commit_manifest(manifest, receipt)
    if updated.status is TransferStatus.CONFLICT:
        raise ManifestConflict(
            "release manifest conflicts on members: " + ", ".join(updated.conflicts),
            updated,
        )
    return updated


def read_published_member(
    manifest: UPath,
    name: str,
    *,
    staging_dir: UPath | None = None,
) -> bytes:
    """Read a member through the release manifest.

    Members that are not listed in the manifest are not visible and raise
    :class:`KeyError`, even if their bytes happen to exist in the staging area.
    """
    payload, _ = _read_manifest(manifest)
    entry = payload["members"].get(name)
    if entry is None:
        raise KeyError(name)
    root = staging_dir or _default_staging(manifest)
    return root.joinpath(entry["object"]).read_bytes()
