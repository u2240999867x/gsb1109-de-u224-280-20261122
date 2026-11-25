"""Publish directories over immutable S3 object versions.

A *publish directory* is an S3 prefix whose visible tree is described by a
publication manifest.  Every manifest pins each relative path to one S3 object
version, so a reader that keeps a generation keeps both its paths and its object
versions after the live objects are overwritten or removed.

Merging one publish directory into another is a three-step operation:

``begin_merge(source, target)``
    Snapshot both directories and report path conflicts.  A file that was never
    catalogued is a first-time entry: it is copied in-bucket from the object
    version named by the work order (or the live version if none was named).  A
    file that is already catalogued is moved using the object version recorded
    in its publication, not whatever happens to be live now.
``draft.keep(...)`` / ``draft.replace(...)``
    Let the warehouse operator resolve each conflict.  ``keep`` leaves the target
    file alone and moves the incoming file to another relative path; ``replace``
    lets the incoming file take the conflicting path.  Each decision records the
    target object version it was made against.
``draft.commit()``
    Re-scan the source for files that arrived after confirmation, re-validate
    every confirmation against the target's object versions and publication
    history, plan the complete prospective tree, and only then copy the files
    that are new to the target.  The new manifest is written last, in one atomic
    pointer update.

All validation happens before any copy, so a stale confirmation or a path that
would contain both a file and a directory leaves the published view untouched.
"""

from __future__ import annotations

import json
import posixpath
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

MANIFEST_FORMAT = "s3fs-publication-v1"
MANIFEST_DIR = ".s3fs-publication"
CURRENT = "current.json"
VERSIONS = "versions"

REPLACE = "replace"
KEEP = "keep"


class PublicationError(Exception):
    """Base class for publication errors."""


class PublicationNotFound(PublicationError, FileNotFoundError):
    """Raised when a publication generation or path does not exist."""


class PublicationLayoutError(PublicationError, ValueError):
    """Raised when one path would be both a file and a directory."""


class MergeConflictError(PublicationError):
    """Raised when a merge has unresolved, stale or colliding entries."""

    def __init__(self, issues: Iterable["MergeIssue"]):
        self.issues = list(issues)
        detail = "; ".join(issue.message() for issue in self.issues)
        super().__init__(detail or "merge has unresolved conflicts")

    @property
    def conflicts(self) -> list["MergeIssue"]:
        return self.issues


def normalise(path: str) -> str:
    """Return ``bucket/key`` with no scheme, leading slash or trailing slash."""
    path = path.strip()
    if path.startswith("s3://"):
        path = path[5:]
    path = path.strip("/")
    return posixpath.normpath(path) if path else ""


def relative_path(root: str, path: str) -> str:
    root = normalise(root)
    path = normalise(path)
    if path == root:
        return ""
    prefix = root + "/"
    if not path.startswith(prefix):
        raise ValueError(f"{path!r} is not below {root!r}")
    return path[len(prefix) :]


def join_path(root: str, relative: str) -> str:
    root = normalise(root)
    relative = normalise(relative)
    return f"{root}/{relative}" if relative else root


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _etag(info: Mapping[str, Any]) -> str | None:
    etag = info.get("ETag")
    return etag.strip('"') if isinstance(etag, str) else etag


def _version_id(info: Mapping[str, Any]) -> str | None:
    version = info.get("VersionId")
    return version if version and version != "null" else None


def _manifest_root(root: str) -> str:
    return f"{normalise(root)}/{MANIFEST_DIR}"


def _manifest_key(root: str, generation: str) -> str:
    return f"{_manifest_root(root)}/{VERSIONS}/{generation}.json"


def _current_key(root: str) -> str:
    return f"{_manifest_root(root)}/{CURRENT}"


def _layout_collisions(paths: Iterable[str]) -> list[tuple[str, str]]:
    """Return ``(ancestor, descendant)`` pairs where both are logical files."""
    paths = sorted(set(paths))
    present = set(paths)
    pairs = []
    for path in paths:
        parts = path.split("/")
        for i in range(1, len(parts)):
            ancestor = "/".join(parts[:i])
            if ancestor in present:
                pairs.append((ancestor, path))
    return pairs


@dataclass(frozen=True)
class Entry:
    """One logical path in a publication, pinned to one S3 object version."""

    path: str
    bucket: str
    key: str
    version_id: str | None = None
    etag: str | None = None
    size: int | None = None

    @property
    def s3path(self) -> str:
        base = f"{self.bucket}/{self.key}"
        return f"{base}?versionId={self.version_id}" if self.version_id else base

    def same_object(self, other: "Entry") -> bool:
        if self.bucket != other.bucket or self.key != other.key:
            return False
        if self.version_id is None or other.version_id is None:
            return True
        return self.version_id == other.version_id

    def retarget(self, path: str) -> "Entry":
        return Entry(
            path=path,
            bucket=self.bucket,
            key=self.key,
            version_id=self.version_id,
            etag=self.etag,
            size=self.size,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "bucket": self.bucket,
            "key": self.key,
            "version_id": self.version_id,
            "etag": self.etag,
            "size": self.size,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Entry":
        return cls(
            path=data["path"],
            bucket=data["bucket"],
            key=data["key"],
            version_id=data.get("version_id"),
            etag=data.get("etag"),
            size=data.get("size"),
        )

    @classmethod
    def at(cls, path: str, fs, info: Mapping[str, Any]) -> "Entry":
        bucket, key, version = fs.split_path(path)
        return cls(
            path=path,
            bucket=bucket,
            key=key,
            version_id=_version_id(info) or version,
            etag=_etag(info),
            size=info.get("size"),
        )


@dataclass
class Publication:
    """An immutable generation of a publish directory.

    ``entries`` is keyed by path relative to ``root``.  Navigation methods accept
    either relative paths or full S3 paths and return full S3 paths, matching
    ``S3FileSystem`` conventions.
    """

    root: str
    generation: str
    entries: dict[str, Entry] = field(default_factory=dict)
    parent: str | None = None
    created: str | None = None
    _fs: Any = field(default=None, repr=False, compare=False)

    # -- serialisation ---------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": MANIFEST_FORMAT,
            "root": self.root,
            "generation": self.generation,
            "parent": self.parent,
            "created": self.created,
            "entries": {
                path: entry.to_dict() for path, entry in sorted(self.entries.items())
            },
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Publication":
        if data.get("format") != MANIFEST_FORMAT:
            raise PublicationError(
                f"unsupported publication manifest format {data.get('format')!r}"
            )
        return cls(
            root=data["root"],
            generation=data["generation"],
            parent=data.get("parent"),
            created=data.get("created"),
            entries={
                path: Entry.from_dict(entry)
                for path, entry in data.get("entries", {}).items()
            },
        )

    # -- validation ------------------------------------------------------

    def validate(self) -> None:
        collisions = _layout_collisions(self.entries)
        if collisions:
            ancestor, descendant = collisions[0]
            raise PublicationLayoutError(
                f"publication {self.root!r} has a file at {ancestor!r} and "
                f"also a file at {descendant!r}"
            )

    # -- tree navigation -------------------------------------------------

    def relative(self, path: str) -> str:
        path = normalise(path)
        if path == self.root:
            return ""
        prefix = self.root + "/"
        return path[len(prefix) :] if path.startswith(prefix) else path

    def full(self, path: str) -> str:
        return join_path(self.root, self.relative(path))

    def _full_child(self, parent: str, name: str) -> str:
        rel = parent.rstrip("/") + "/" + name if parent else name
        full = join_path(self.root, rel.rstrip("/"))
        return full + "/" if name.endswith("/") else full

    def paths(self) -> list[str]:
        """Relative file paths, sorted."""
        return sorted(self.entries)

    def entry(self, path: str) -> Entry:
        try:
            return self.entries[self.relative(path)]
        except KeyError:
            raise PublicationNotFound(path) from None

    def exists(self, path: str) -> bool:
        rel = self.relative(path)
        return rel in self.entries or self.isdir(rel)

    def isfile(self, path: str) -> bool:
        return self.relative(path) in self.entries

    def isdir(self, path: str) -> bool:
        rel = self.relative(path)
        if rel in self.entries:
            return False
        prefix = rel + "/" if rel else ""
        return any(name.startswith(prefix) for name in self.entries)

    def ls(self, path: str = "", detail: bool = False) -> list:
        """List immediate children, returning full S3 paths.

        Directory names carry a trailing ``/``.
        """
        rel = self.relative(path)
        prefix = rel + "/" if rel else ""
        children: dict[str, list[Entry]] = {}
        for name, entry in self.entries.items():
            if not name.startswith(prefix):
                continue
            rest = name[len(prefix) :]
            head, sep, _ = rest.partition("/")
            child = head + sep if sep else head
            children.setdefault(child, []).append(entry)
        names = sorted(children)
        if not detail:
            return [self._full_child(rel, name) for name in names]
        out = []
        for name in names:
            if name.endswith("/"):
                out.append(
                    {
                        "name": self._full_child(rel, name),
                        "path": self._full_child(rel, name),
                        "type": "directory",
                        "size": 0,
                    }
                )
            else:
                entry = children[name][0]
                out.append(
                    {
                        "name": self._full_child(rel, name),
                        "path": self._full_child(rel, name),
                        "type": "file",
                        "size": entry.size,
                        "etag": entry.etag,
                        "version_id": entry.version_id,
                        "entry": entry,
                    }
                )
        return out

    def find(self, path: str = "") -> list[str]:
        """All file paths below ``path``, as full S3 paths."""
        rel = self.relative(path)
        prefix = rel + "/" if rel else ""
        return [self.full(name) for name in sorted(self.entries) if name.startswith(prefix)]

    def info(self, path: str) -> dict[str, Any]:
        rel = self.relative(path)
        if rel in self.entries:
            entry = self.entries[rel]
            return {
                "name": self.full(rel),
                "path": self.full(rel),
                "type": "file",
                "size": entry.size,
                "etag": entry.etag,
                "version_id": entry.version_id,
                "bucket": entry.bucket,
                "key": entry.key,
                "s3path": entry.s3path,
            }
        if self.isdir(rel):
            return {
                "name": self.full(rel) + "/",
                "path": self.full(rel) + "/",
                "type": "directory",
                "size": 0,
            }
        raise PublicationNotFound(path)

    # -- reading ---------------------------------------------------------

    def _filesystem(self):
        if self._fs is None:
            raise RuntimeError(
                "publication is not attached to a filesystem; load it through "
                "PublicationStore"
            )
        return self._fs

    def open(self, path: str, mode: str = "rb"):
        """Open a file pinned to this generation's object version."""
        if "r" not in mode:
            raise ValueError("publications are read-only; open with mode 'rb'")
        return self._filesystem().open(self.entry(path).s3path, "rb")

    def cat_file(self, path: str, start=None, end=None) -> bytes:
        data = self._filesystem().cat_file(self.entry(path).s3path)
        if start is None and end is None:
            return data
        return data[slice(start, end)]

    def get_file(self, path: str, local_path: str) -> str:
        self._filesystem().get_file(self.entry(path).s3path, local_path)
        return local_path

    cat = cat_file
    get = get_file


class PublicationStore:
    """Read and write publication manifests for one S3 filesystem."""

    def __init__(self, fs):
        if not getattr(fs, "version_aware", False):
            raise ValueError("publications require version_aware=True")
        self.fs = fs

    # -- manifest IO -----------------------------------------------------

    def _read_json(self, path: str) -> dict[str, Any]:
        try:
            return json.loads(self.fs.cat_file(path))
        except FileNotFoundError:
            raise PublicationNotFound(path) from None

    def _write_json(self, path: str, data: Mapping[str, Any]) -> None:
        self.fs.pipe_file(path, json.dumps(data, indent=2, sort_keys=True).encode())

    def current_generation(self, root: str) -> str:
        try:
            return self._read_json(_current_key(root))["generation"]
        except (PublicationNotFound, KeyError):
            raise PublicationNotFound(f"no publication for {root!r}") from None

    def is_published(self, root: str) -> bool:
        try:
            self.current_generation(root)
            return True
        except PublicationNotFound:
            return False

    def get(self, root: str, generation: str) -> Publication:
        publication = Publication.from_dict(
            self._read_json(_manifest_key(normalise(root), generation))
        )
        publication._fs = self.fs
        return publication

    def current(self, root: str) -> Publication:
        return self.get(root, self.current_generation(root))

    def publish(self, publication: Publication) -> Publication:
        """Write an immutable generation, then atomically point the root at it."""
        publication.root = normalise(publication.root)
        publication.validate()
        self._write_json(
            _manifest_key(publication.root, publication.generation),
            publication.to_dict(),
        )
        self._write_json(
            _current_key(publication.root),
            {"format": MANIFEST_FORMAT, "generation": publication.generation},
        )
        publication._fs = self.fs
        return publication

    def new_generation(self) -> str:
        return uuid.uuid4().hex

    # -- live tree -------------------------------------------------------

    @staticmethod
    def _is_manifest_path(root: str, path: str) -> bool:
        rel = relative_path(root, path)
        return rel == MANIFEST_DIR or rel.startswith(MANIFEST_DIR + "/")

    def live_entries(self, root: str) -> dict[str, Entry]:
        """Live objects below ``root``, excluding publication metadata."""
        root = normalise(root)
        entries: dict[str, Entry] = {}
        try:
            infos = self.fs.find(root, detail=True, refresh=True)
        except FileNotFoundError:
            return entries
        for path, info in infos.items():
            if info.get("type") == "directory":
                continue
            if self._is_manifest_path(root, path):
                continue
            entries[relative_path(root, path)] = Entry.at(path, self.fs, info)
        return entries

    def base_entries(self, root: str) -> dict[str, Entry]:
        """The tree a new generation should start from.

        Published entries win: a transient overwrite of an already-published path
        does not leak into the next generation.  Live paths that were never
        catalogued (for example, another feed publishing a new path) are included.
        """
        root = normalise(root)
        live = self.live_entries(root)
        base: dict[str, Entry] = {}
        try:
            publication = self.current(root)
        except PublicationNotFound:
            publication = None
        if publication is not None:
            base.update(publication.entries)
        for path, entry in live.items():
            base.setdefault(path, entry)
        return base

    def adopt(self, root: str, entries: dict[str, Entry] | None = None) -> Publication:
        """Publish the current live tree as a new immutable generation."""
        root = normalise(root)
        entries = self.live_entries(root) if entries is None else dict(entries)
        try:
            parent = self.current_generation(root)
        except PublicationNotFound:
            parent = None
        publication = Publication(
            root=root,
            generation=self.new_generation(),
            entries=entries,
            parent=parent,
            created=_utcnow().isoformat(),
        )
        return self.publish(publication)

    def ensure(self, root: str) -> Publication:
        """Return the current publication, adopting the live tree if needed."""
        try:
            return self.current(root)
        except PublicationNotFound:
            return self.adopt(root)

    # -- history ---------------------------------------------------------

    def chain(self, root: str, stop: str | None = None) -> list[Publication]:
        """Generations from current back to ``stop`` (inclusive), newest first."""
        out: list[Publication] = []
        try:
            generation = self.current_generation(root)
        except PublicationNotFound:
            return out
        seen = set()
        while generation and generation not in seen:
            seen.add(generation)
            publication = self.get(root, generation)
            out.append(publication)
            if generation == stop:
                break
            generation = publication.parent
        return out

    def pinned_entry(self, root: str, path: str) -> Entry | None:
        """Most recent catalogued entry for ``path``, or ``None``."""
        for publication in self.chain(root):
            entry = publication.entries.get(path)
            if entry is not None:
                return entry
        return None


@dataclass
class Decision:
    """An operator decision for one incoming path that collides with the target."""

    source_path: str
    action: str
    destination: str | None = None
    target_path: str | None = None
    target_generation: str | None = None
    target_version: str | None = None
    target_etag: str | None = None
    incoming_version: str | None = None
    incoming_etag: str | None = None
    confirmed_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_path": self.source_path,
            "action": self.action,
            "destination": self.destination,
            "target_path": self.target_path,
            "target_generation": self.target_generation,
            "target_version": self.target_version,
            "target_etag": self.target_etag,
            "incoming_version": self.incoming_version,
            "incoming_etag": self.incoming_etag,
            "confirmed_at": self.confirmed_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Decision":
        return cls(**data)


@dataclass
class MergeIssue:
    """A conflict or stale confirmation in a merge."""

    kind: str
    source_path: str
    target_path: str
    reason: str
    counterpart: str | None = None

    def message(self) -> str:
        text = f"{self.source_path} -> {self.target_path}: {self.reason}"
        if self.counterpart:
            text += f"; also involves {self.counterpart}"
        return text

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "source_path": self.source_path,
            "target_path": self.target_path,
            "reason": self.reason,
            "counterpart": self.counterpart,
        }


@dataclass
class PlannedCopy:
    """One source entry becoming visible at a target path.

    ``reuse`` entries are already catalogued files that changed location: the
    target path points at the original object version, with no new copy.  Other
    entries are first-time files, copied in-bucket.
    """

    source_path: str
    destination: str
    action: str
    source_entry: Entry
    reuse: bool = False


@dataclass
class MergePlan:
    """The result of validating a draft against the current objects."""

    copies: list[PlannedCopy] = field(default_factory=list)
    issues: list[MergeIssue] = field(default_factory=list)
    target_base: dict[str, Entry] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.issues


class MergeDraft:
    """A merge of ``source`` into ``target`` awaiting confirmation and commit."""

    def __init__(
        self,
        merger: "PublicationMerger",
        source: str,
        target: str,
        source_entries: dict[str, Entry],
        target_entries: dict[str, Entry],
        source_generation: str | None,
        target_generation: str | None,
        decisions: dict[str, Decision] | None = None,
        assignments: dict[str, str] | None = None,
        pinned_versions: dict[str, str] | None = None,
    ):
        self.merger = merger
        self.source = normalise(source)
        self.target = normalise(target)
        self.source_entries = source_entries
        self.target_entries = target_entries
        self.source_generation = source_generation
        self.target_generation = target_generation
        self.decisions = decisions or {}
        self.assignments = assignments or {path: path for path in source_entries}
        self.pinned_versions = pinned_versions or {}
        self.created = _utcnow().isoformat()

    # -- introspection ---------------------------------------------------

    def destination(self, path: str) -> str:
        source_path = self._source_key(path)
        decision = self.decisions.get(source_path)
        if decision is not None:
            if decision.action == KEEP:
                return decision.destination or self.assignments.get(
                    source_path, source_path
                )
            return decision.target_path or self.assignments.get(source_path, source_path)
        return self.assignments.get(source_path, source_path)

    def conflicts(self, refresh: bool = False) -> list[MergeIssue]:
        """Unresolved conflicts, optionally re-scanned against current objects."""
        if refresh:
            return self.merger.plan(self).issues
        issues = []
        owners: dict[str, str] = {
            path: f"{self.target}/{path}" for path in self.target_entries
        }
        for path in sorted(self.source_entries):
            destination = self.destination(path)
            owners[destination] = f"{self.source}/{path}"
            if destination in self.target_entries and path not in self.decisions:
                issues.append(
                    MergeIssue(
                        kind="existing",
                        source_path=path,
                        target_path=destination,
                        reason="target path exists",
                    )
                )
        issues.extend(self.merger._layout_issues(owners, self.source))
        return issues

    def unresolved(self) -> list[MergeIssue]:
        return self.conflicts(refresh=True)

    def check(self) -> list[MergeIssue]:
        return self.unresolved()

    # -- decisions -------------------------------------------------------

    def _source_key(self, path: str) -> str:
        path = normalise(path)
        prefix = self.source + "/"
        return path[len(prefix) :] if path.startswith(prefix) else path

    def _target_relative(self, destination: str) -> str:
        destination = normalise(destination)
        prefix = self.target + "/"
        return (
            destination[len(prefix) :]
            if destination.startswith(prefix)
            else destination
        )

    def _entry(self, source_path: str) -> Entry:
        try:
            return self.source_entries[source_path]
        except KeyError:
            raise PublicationNotFound(f"{self.source}/{source_path}") from None

    def _current_target_generation(self) -> str | None:
        store = self.merger.store
        return (
            store.current_generation(self.target)
            if store.is_published(self.target)
            else self.target_generation
        )

    def _target_snapshot(self, target_path: str) -> Entry | None:
        """The target object a fresh confirmation is made against.

        Re-confirming after the target changed must bind the decision to the
        object that is current now, not to the snapshot taken when the draft was
        opened.
        """
        current = self.merger.store.base_entries(self.target)
        return current.get(target_path)

    def keep(self, path: str, destination: str) -> Decision:
        """Keep the target file; move the incoming file to ``destination``."""
        source_path = self._source_key(path)
        destination = self._target_relative(destination)
        incoming = self._entry(source_path)
        existing = self.decisions.get(source_path)
        target_path = (
            existing.target_path
            if existing is not None and existing.target_path
            else self.assignments.get(source_path, source_path)
        )
        if destination == target_path:
            raise ValueError(
                "keep must assign a destination different from the conflicting "
                f"target path {target_path!r}"
            )
        target = self._target_snapshot(target_path)
        decision = Decision(
            source_path=source_path,
            action=KEEP,
            destination=destination,
            target_path=target_path,
            target_generation=self._current_target_generation(),
            target_version=target.version_id if target else None,
            target_etag=target.etag if target else None,
            incoming_version=incoming.version_id,
            incoming_etag=incoming.etag,
            confirmed_at=_utcnow().isoformat(),
        )
        self.decisions[source_path] = decision
        self.assignments[source_path] = destination
        return decision

    relocate = keep

    def replace(self, path: str) -> Decision:
        """Replace the target object at the conflicting path."""
        source_path = self._source_key(path)
        incoming = self._entry(source_path)
        existing = self.decisions.get(source_path)
        target_path = (
            existing.target_path
            if existing is not None and existing.target_path
            else self.assignments.get(source_path, source_path)
        )
        target = self._target_snapshot(target_path)
        decision = Decision(
            source_path=source_path,
            action=REPLACE,
            destination=None,
            target_path=target_path,
            target_generation=self._current_target_generation(),
            target_version=target.version_id if target else None,
            target_etag=target.etag if target else None,
            incoming_version=incoming.version_id,
            incoming_etag=incoming.etag,
            confirmed_at=_utcnow().isoformat(),
        )
        self.decisions[source_path] = decision
        self.assignments[source_path] = target_path
        return decision

    def confirm(
        self, path: str, action: str, destination: str | None = None
    ) -> Decision:
        """Generic entry point used by operator UIs."""
        if action == REPLACE:
            return self.replace(path)
        if action == KEEP:
            if destination is None:
                raise ValueError("keep requires an alternative destination")
            return self.keep(path, destination)
        raise ValueError(f"unknown action {action!r}")

    def pin(self, path: str, version_id: str) -> None:
        """Use an explicit source object version for a first-time copy."""
        self.pinned_versions[self._source_key(path)] = version_id

    # -- commit ----------------------------------------------------------

    def commit(self) -> Publication:
        return self.merger.commit(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "source_generation": self.source_generation,
            "target_generation": self.target_generation,
            "decisions": {
                key: decision.to_dict() for key, decision in self.decisions.items()
            },
            "assignments": dict(self.assignments),
            "pinned_versions": dict(self.pinned_versions),
            "created": self.created,
        }

    @classmethod
    def from_dict(
        cls, merger: "PublicationMerger", data: Mapping[str, Any]
    ) -> "MergeDraft":
        return merger.begin(
            normalise(data["source"]), normalise(data["target"]), _resume=data
        )


class PublicationMerger:
    """Build, confirm and commit merges between publish directories."""

    def __init__(self, fs, store: PublicationStore | None = None):
        self.fs = fs
        self.store = store or PublicationStore(fs)

    # -- beginning a merge ----------------------------------------------

    def begin(
        self,
        source: str,
        target: str,
        versions: Mapping[str, str] | None = None,
        _resume: Mapping[str, Any] | None = None,
    ) -> MergeDraft:
        source = normalise(source)
        target = normalise(target)
        source_entries = self.store.live_entries(source)
        target_entries = self.store.base_entries(target)
        source_generation = (
            self.store.current_generation(source)
            if self.store.is_published(source)
            else None
        )
        target_generation = (
            self.store.current_generation(target)
            if self.store.is_published(target)
            else None
        )
        decisions = {}
        assignments = None
        pinned = dict(versions or {})
        created = None
        if _resume:
            decisions = {
                path: Decision.from_dict(value)
                for path, value in _resume.get("decisions", {}).items()
            }
            assignments = dict(_resume.get("assignments", {}))
            pinned = dict(_resume.get("pinned_versions", {}))
            created = _resume.get("created")
        draft = MergeDraft(
            self,
            source,
            target,
            source_entries,
            target_entries,
            source_generation,
            target_generation,
            decisions=decisions,
            assignments=assignments,
            pinned_versions=pinned,
        )
        if created:
            draft.created = created
        return draft

    # -- planning --------------------------------------------------------

    def _source_version(
        self,
        draft: MergeDraft,
        source_path: str,
        source_live: Mapping[str, Entry] | None = None,
    ) -> tuple[Entry, bool]:
        """Return ``(entry, already_catalogued)`` for one source file.

        A file that is already in the source's publication keeps its recorded
        object version and is *not* copied again: moving an already-catalogued
        file only changes the path that points at the original object version.

        A first-time file is copied in-bucket from the version named by the work
        order, falling back to the live object when the work order does not name
        one.
        """
        live = (source_live or draft.source_entries).get(source_path)
        if live is None:
            live = self.store.live_entries(draft.source).get(source_path)
        if live is None:
            raise PublicationNotFound(f"{draft.source}/{source_path}")

        catalogued = self.store.pinned_entry(draft.source, source_path)
        if catalogued is not None and catalogued.version_id is not None:
            return catalogued.retarget(source_path), True

        pinned = draft.pinned_versions.get(source_path)
        if pinned:
            try:
                info = self.fs.info(
                    f"{draft.source}/{source_path}", version_id=pinned
                )
            except FileNotFoundError:
                raise PublicationNotFound(
                    f"{draft.source}/{source_path}?versionId={pinned}"
                ) from None
            return (
                Entry(
                    path=source_path,
                    bucket=live.bucket,
                    key=live.key,
                    version_id=pinned,
                    etag=_etag(info) or live.etag,
                    size=info.get("size", live.size),
                ),
                False,
            )
        return live, False

    def _layout_issues(
        self, owners: Mapping[str, str], source_root: str
    ) -> list[MergeIssue]:
        """Report file/directory clashes, naming the incoming file first."""
        issues = []
        prefix = normalise(source_root) + "/"
        for ancestor, descendant in _layout_collisions(owners):
            ancestor_owner = owners[ancestor]
            descendant_owner = owners[descendant]
            if ancestor_owner.startswith(prefix):
                primary, counterpart = ancestor_owner, descendant_owner
            elif descendant_owner.startswith(prefix):
                primary, counterpart = descendant_owner, ancestor_owner
            else:
                primary, counterpart = descendant_owner, ancestor_owner
            issues.append(
                MergeIssue(
                    kind="path-type",
                    source_path=primary,
                    target_path=ancestor,
                    counterpart=counterpart,
                    reason=(
                        f"the publication would contain {ancestor_owner!r} at "
                        f"{ancestor!r} and also {descendant_owner!r} below it"
                    ),
                )
            )
        return issues

    def plan(self, draft: MergeDraft) -> MergePlan:
        """Validate the draft against current objects and plan every copy."""
        source_live = self.store.live_entries(draft.source)
        target_base = self.store.base_entries(draft.target)
        issues: list[MergeIssue] = []

        for path, decision in sorted(draft.decisions.items()):
            stale = self._stale_reason(draft, decision, target_base)
            if stale:
                issues.append(
                    MergeIssue(
                        kind="stale",
                        source_path=path,
                        target_path=decision.target_path or path,
                        reason=stale,
                    )
                )

        owners: dict[str, str] = {
            path: f"{draft.target}/{path}" for path in target_base
        }
        planned: dict[str, str] = {}
        copies: list[PlannedCopy] = []

        for source_path in sorted(source_live):
            decision = draft.decisions.get(source_path)
            destination = draft.assignments.get(source_path, source_path)
            if decision is not None:
                if decision.action == KEEP:
                    destination = decision.destination or destination
                else:
                    destination = decision.target_path or destination

            if destination in planned:
                issues.append(
                    MergeIssue(
                        kind="collision",
                        source_path=source_path,
                        target_path=destination,
                        counterpart=planned[destination],
                        reason=(
                            "two incoming files were assigned the same "
                            f"destination {destination!r}"
                        ),
                    )
                )
                continue
            planned[destination] = source_path
            owners[destination] = f"{draft.source}/{source_path}"

            if destination in target_base:
                if decision is None:
                    issues.append(
                        MergeIssue(
                            kind="existing",
                            source_path=source_path,
                            target_path=destination,
                            reason="target path exists",
                        )
                    )
                    continue
                if decision.action == KEEP and destination != decision.target_path:
                    issues.append(
                        MergeIssue(
                            kind="existing",
                            source_path=source_path,
                            target_path=destination,
                            reason=(
                                "alternative destination is now occupied by "
                                "the target object"
                            ),
                        )
                    )
                    continue
                if (
                    decision.action == REPLACE
                    and destination != decision.target_path
                ):
                    issues.append(
                        MergeIssue(
                            kind="existing",
                            source_path=source_path,
                            target_path=destination,
                            reason="replacement path changed after confirmation",
                        )
                    )
                    continue

            if decision is None:
                action = "add"
            elif decision.action == REPLACE:
                action = "replace"
            else:
                action = "move"
            source_entry, catalogued = self._source_version(
                draft, source_path, source_live
            )
            copies.append(
                PlannedCopy(
                    source_path=source_path,
                    destination=destination,
                    action=action,
                    source_entry=source_entry,
                    reuse=catalogued,
                )
            )

        for source_path, decision in sorted(draft.decisions.items()):
            if source_path not in source_live:
                issues.append(
                    MergeIssue(
                        kind="missing",
                        source_path=source_path,
                        target_path=decision.target_path or source_path,
                        reason="incoming file disappeared before commit",
                    )
                )

        for ancestor, descendant in _layout_collisions(owners):
            ancestor_owner = owners[ancestor]
            descendant_owner = owners[descendant]
            if ancestor_owner.startswith(draft.source + "/"):
                primary, counterpart = ancestor_owner, descendant_owner
            else:
                primary, counterpart = descendant_owner, ancestor_owner
            issues.append(
                MergeIssue(
                    kind="path-type",
                    source_path=primary,
                    target_path=ancestor,
                    counterpart=counterpart,
                    reason=(
                        f"the publication would contain {ancestor_owner!r} at "
                        f"{ancestor!r} and also {descendant_owner!r} below it"
                    ),
                )
            )

        return MergePlan(copies=copies, issues=issues, target_base=target_base)

    def _stale_reason(
        self,
        draft: MergeDraft,
        decision: Decision,
        target_base: dict[str, Entry],
    ) -> str | None:
        target_path = decision.target_path or decision.source_path
        current = target_base.get(target_path)
        if current is None:
            return "target object disappeared after confirmation"
        if (
            decision.target_version is not None
            and current.version_id != decision.target_version
        ):
            return (
                "target object was replaced after confirmation "
                f"({decision.target_version!r} -> {current.version_id!r}); "
                "confirm it again"
            )
        if decision.target_etag is not None and current.etag != decision.target_etag:
            return "target object changed after confirmation"

        # A later write can replace an object without publishing a generation
        # (and a delete-then-recreate yields a *new* version id even when the
        # content is the old one).  The live object must still be the version the
        # operator confirmed.
        live = self.store.live_entries(draft.target).get(target_path)
        if (
            live is not None
            and decision.target_version is not None
            and live.version_id != decision.target_version
        ):
            return (
                "target object has a newer version than the confirmation "
                f"({decision.target_version!r} -> {live.version_id!r}); "
                "confirm it again"
            )

        # A replace-then-restore can also be reflected only in a publication
        # generation.  Walk the history between the current generation and the
        # one confirmed against.  Re-confirming sets target_generation to the
        # current generation, which clears this condition.
        if decision.target_generation:
            try:
                generation = self.store.current_generation(draft.target)
            except PublicationNotFound:
                generation = None
            seen = set()
            while generation and generation != decision.target_generation:
                if generation in seen:
                    break
                seen.add(generation)
                publication = self.store.get(draft.target, generation)
                entry = publication.entries.get(target_path)
                version = entry.version_id if entry else None
                etag = entry.etag if entry else None
                if (
                    version != decision.target_version
                    or etag != decision.target_etag
                ):
                    return (
                        "target object was replaced and restored after "
                        "confirmation; confirm it again"
                    )
                generation = publication.parent

        return None

    # -- commit ----------------------------------------------------------

    def commit(self, draft: MergeDraft) -> Publication:
        plan = self.plan(draft)
        if plan.issues:
            raise MergeConflictError(plan.issues)

        final_entries: dict[str, Entry] = dict(plan.target_base)
        prospective = {path: path for path in final_entries}
        prospective.update({copy.destination: copy.destination for copy in plan.copies})
        collisions = _layout_collisions(prospective)
        if collisions:
            ancestor, descendant = collisions[0]
            raise MergeConflictError(
                [
                    MergeIssue(
                        kind="path-type",
                        source_path=descendant,
                        target_path=ancestor,
                        counterpart=ancestor,
                        reason=(
                            "publication would contain a file and a directory "
                            f"at {ancestor!r}"
                        ),
                    )
                ]
            )

        written: list[str] = []
        try:
            for copy in plan.copies:
                if copy.reuse:
                    # Already catalogued: the destination points at the original
                    # object version, so no new object is created.
                    final_entries[copy.destination] = copy.source_entry.retarget(
                        copy.destination
                    )
                    continue
                target_path = f"{draft.target}/{copy.destination}"
                self.fs.cp_file(copy.source_entry.s3path, target_path)
                written.append(target_path)
                info = self.fs.info(target_path, refresh=True)
                final_entries[copy.destination] = Entry.at(
                    target_path, self.fs, info
                ).retarget(copy.destination)

            publication = Publication(
                root=draft.target,
                generation=self.store.new_generation(),
                entries=dict(final_entries),
                parent=self.store.current_generation(draft.target)
                if self.store.is_published(draft.target)
                else draft.target_generation,
                created=_utcnow().isoformat(),
            )
            self.store.publish(publication)
        except Exception:
            # The published view was never switched, so remove the partial
            # copies and leave the previous generation untouched.
            for path in written:
                try:
                    self.fs.rm_file(path)
                except Exception:
                    pass
            raise

        self._retire_source(draft, {copy.source_path for copy in plan.copies})
        return self.store.get(draft.target, publication.generation)

    def _retire_source(self, draft: MergeDraft, migrated: set[str]) -> None:
        """Remove the files this merge took from the source directory.

        Older source generations keep their version pins, so their readers are
        unaffected by the removals.
        """
        for path in sorted(migrated):
            try:
                self.fs.rm_file(f"{draft.source}/{path}")
            except FileNotFoundError:
                continue
        try:
            parent = self.store.current_generation(draft.source)
        except PublicationNotFound:
            parent = None
        publication = Publication(
            root=draft.source,
            generation=self.store.new_generation(),
            entries=dict(self.store.live_entries(draft.source)),
            parent=parent,
            created=_utcnow().isoformat(),
        )
        self.store.publish(publication)


# -- module-level helpers ------------------------------------------------


def publication_for(fs, root: str, generation: str | None = None) -> Publication:
    """Load the current (or a named) generation of a publish directory."""
    store = PublicationStore(fs)
    return store.get(root, generation) if generation else store.current(root)


def publish_directory(fs, root: str) -> Publication:
    """Adopt the current live tree of ``root`` as a new publication generation."""
    return PublicationStore(fs).adopt(root)


def begin_merge(
    fs,
    source: str,
    target: str,
    versions: Mapping[str, str] | None = None,
) -> MergeDraft:
    """Start merging ``source`` into ``target``."""
    return PublicationMerger(fs).begin(source, target, versions=versions)


def check_merge(draft: MergeDraft) -> list[MergeIssue]:
    """Return the conflicts that still block ``draft`` at commit time."""
    return draft.check()


def commit_merge(draft: MergeDraft) -> Publication:
    """Commit a confirmed draft."""
    return draft.commit()


def merge_directories(
    fs,
    source: str,
    target: str,
    resolutions: Mapping[str, Any] | None = None,
    versions: Mapping[str, str] | None = None,
) -> Publication:
    """Convenience wrapper: begin, apply resolutions, commit."""
    draft = begin_merge(fs, source, target, versions=versions)
    for path, resolution in (resolutions or {}).items():
        if isinstance(resolution, str):
            draft.keep(path, resolution)
        elif resolution == REPLACE:
            draft.replace(path)
        elif isinstance(resolution, Mapping):
            draft.confirm(path, resolution["action"], resolution.get("destination"))
        else:
            raise ValueError(f"unsupported resolution for {path!r}: {resolution!r}")
    return draft.commit()


merge_publications = merge_directories


__all__ = [
    "Decision",
    "Entry",
    "KEEP",
    "MergeConflictError",
    "MergeDraft",
    "MergeIssue",
    "MergePlan",
    "Publication",
    "PublicationError",
    "PublicationLayoutError",
    "PublicationMerger",
    "PublicationNotFound",
    "PublicationStore",
    "REPLACE",
    "begin_merge",
    "check_merge",
    "commit_merge",
    "join_path",
    "merge_directories",
    "merge_publications",
    "normalise",
    "publication_for",
    "publish_directory",
    "relative_path",
]
