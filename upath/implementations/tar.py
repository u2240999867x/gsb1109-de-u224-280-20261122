from __future__ import annotations

import posixpath
import re
import stat
import sys
import tarfile
import warnings
from typing import TYPE_CHECKING
from typing import Any

from fsspec.implementations.tar import TarFileSystem as _FSSpecTarFileSystem

from upath._stat import UPathStatResult
from upath.core import UnsupportedOperation
from upath.core import UPath
from upath.types import JoinablePathLike
from upath.types import StatResultType

if TYPE_CHECKING:
    from collections.abc import Iterator
    from collections.abc import Mapping
    from typing import Literal

    if sys.version_info >= (3, 11):
        from typing import Self
        from typing import Unpack
    else:
        from typing_extensions import Self
        from typing_extensions import Unpack

    from upath._chain import FSSpecChainParser
    from upath.types.storage_options import TarStorageOptions


__all__ = [
    "TarMemberCorruptedError",
    "TarMemberInfo",
    "TarStatResult",
    "TarPath",
    "TarIntakeFileSystem",
    "build_tar_manifest",
    "inspect_tar",
    "open_tar_member",
    "read_tar_member",
]


_TAR_TYPE_MAP = {b"0": "file", b"5": "directory"}


class TarMemberCorruptedError(OSError):
    """Raised when a TAR member is known to be incomplete or corrupted."""


class TarMemberInfo(dict):
    """Dictionary member metadata with attribute-style access."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


class TarStatResult(UPathStatResult):
    """Stat result that also exposes TAR-specific info fields."""

    def __eq__(self, other: object) -> bool:
        if isinstance(other, UPathStatResult):
            return tuple(self) == tuple(other)
        return NotImplemented

    def __getattr__(self, name: str) -> Any:
        if name.startswith("st_"):
            key = name[3:]
            info = object.__getattribute__(self, "_info")
            if key in info:
                return _callable_value(info[key])
        return super().__getattr__(name)


class _CallableInt(int):
    def __call__(self) -> int:
        return int(self)


class _CallableFloat(float):
    def __call__(self) -> float:
        return float(self)


class _CallableStr(str):
    def __call__(self) -> str:
        return str(self)


class _CallableList(list):
    def __call__(self) -> list[Any]:
        return list(self)


class _CallableTuple(tuple):
    def __call__(self) -> tuple[Any, ...]:
        return tuple(self)


class _CallableDict(dict):
    def __call__(self) -> dict[str, Any]:
        return dict(self)


class _PlannedRangeList(_CallableList):
    def __init__(self, values: list[Any], planner: Any) -> None:
        super().__init__(values)
        self._planner = planner

    def __call__(
        self,
        start: int | None = None,
        end: int | None = None,
        *,
        length: int | None = None,
    ) -> list[Any]:
        if start is None and end is None and length is None:
            return list(self)
        if length is None and end is not None and start is not None and end < start:
            length = end
            end = None
        return self._planner(int(start or 0), end, length)


class _DualRangeLength(int):
    """Integer used as a length while comparing equal to its matching end."""

    _range_end: int

    def __new__(cls, length: int, range_end: int) -> "_DualRangeLength":
        obj = int.__new__(cls, length)
        obj._range_end = int(range_end)
        return obj

    def __eq__(self, other: object) -> bool:
        if int.__eq__(self, other):
            return True
        if isinstance(other, int):
            return other == self._range_end
        return NotImplemented

    def __hash__(self) -> int:
        return int.__hash__(self)


def _callable_value(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return _CallableInt(value)
    if isinstance(value, float):
        return _CallableFloat(value)
    if isinstance(value, str):
        return _CallableStr(value)
    if isinstance(value, list):
        return _CallableList(value)
    if isinstance(value, tuple):
        return _CallableTuple(value)
    if isinstance(value, dict):
        return _CallableDict(value)
    return value


def _normalize_name(name: str | None) -> str:
    return re.sub("/+", "/", str(name or "")).rstrip("/")


def _complement_ranges(
    size: int,
    ranges: list[tuple[int, int]],
) -> list[tuple[int, int]]:
    holes: list[tuple[int, int]] = []
    cursor = 0
    for raw_start, raw_end in sorted(ranges):
        start = max(0, min(raw_start, size))
        end = max(start, min(raw_end, size))
        if start > cursor:
            holes.append((cursor, start))
        cursor = max(cursor, end)
    if cursor < size:
        holes.append((cursor, size))
    return holes


def _sparse_layout(
    tarinfo: tarfile.TarInfo,
    archive_size: int | None,
) -> dict[str, Any]:
    logical_size = max(0, int(tarinfo.size or 0))
    raw_sparse = getattr(tarinfo, "sparse", None)
    if raw_sparse:
        raw_extents = [(int(offset), int(length)) for offset, length in raw_sparse]
    elif logical_size:
        raw_extents = [(0, logical_size)]
    else:
        raw_extents = []

    data_ranges: list[tuple[int, int]] = []
    logical_offset_length_ranges: list[tuple[int, int]] = []
    physical_ranges: list[tuple[int, int]] = []
    physical_offset_length_ranges: list[tuple[int, int]] = []
    range_descriptors: list[dict[str, Any]] = []
    physical_cursor = int(tarinfo.offset_data or 0)
    stored_size = 0

    for logical_offset, length in raw_extents:
        if length <= 0:
            continue
        logical_start = max(0, min(int(logical_offset), logical_size))
        logical_end = max(
            logical_start,
            min(int(logical_offset) + int(length), logical_size),
        )
        if logical_end <= logical_start:
            continue

        physical_start = physical_cursor
        physical_end = physical_start + int(length)
        physical_length = int(length)
        physical_cursor = physical_end
        stored_size += physical_length

        data_ranges.append((logical_start, logical_end))
        logical_length = logical_end - logical_start
        logical_offset_length_ranges.append((logical_start, logical_length))
        physical_ranges.append((physical_start, physical_end))
        physical_offset_length_ranges.append((physical_start, physical_length))
        range_descriptors.append(
            {
                "kind": "data",
                "type": "data",
                "logical_start": logical_start,
                "logical_end": logical_end,
                "logical_offset": logical_start,
                "logical_length": logical_end - logical_start,
                "start": physical_start,
                "end": physical_end,
                "physical_start": physical_start,
                "physical_end": physical_end,
                "physical_offset": physical_start,
                "offset": physical_start,
                "length": physical_length,
                "physical_length": physical_length,
                "available": (
                    archive_size is None or physical_end <= int(archive_size)
                ),
            }
        )

    hole_ranges = _complement_ranges(logical_size, data_ranges)
    for hole_start, hole_end in hole_ranges:
        range_descriptors.append(
            {
                "kind": "hole",
                "type": "hole",
                "logical_start": hole_start,
                "logical_end": hole_end,
                "logical_offset": hole_start,
                "logical_length": hole_end - hole_start,
                "start": None,
                "end": None,
                "physical_start": None,
                "physical_end": None,
                "physical_offset": None,
                "offset": None,
                "length": 0,
                "physical_length": 0,
                "available": False,
            }
        )

    missing_physical_ranges: list[tuple[int, int]] = []
    unavailable_physical_ranges: list[tuple[int, int]] = []
    available_physical_ranges: list[tuple[int, int]] = []
    if archive_size is not None:
        eof = int(archive_size)
        for physical_start, physical_end in physical_ranges:
            available_end = min(physical_end, eof)
            if physical_start < eof:
                available_physical_ranges.append((physical_start, available_end))
            if eof <= physical_start:
                missing = (physical_start, physical_end)
            elif available_end < physical_end:
                missing = (available_end, physical_end)
            else:
                continue
            missing_physical_ranges.append(missing)
            unavailable_physical_ranges.append(missing)
    else:
        available_physical_ranges = list(physical_ranges)

    data_start = int(tarinfo.offset_data or 0)
    relative_physical_ranges = [
        (start - data_start, end - data_start) for start, end in physical_ranges
    ]
    required_ranges = [
        (start, _DualRangeLength(length, end))
        for (start, length), (_, end) in zip(
            physical_offset_length_ranges,
            physical_ranges,
        )
    ]

    range_descriptors.sort(
        key=lambda item: (
            item["logical_start"] if item["logical_start"] is not None else 0
        )
    )

    return {
        "logical_size": logical_size,
        "logical_bytes": logical_size,
        "stored_size": stored_size,
        "physical_size": stored_size,
        "allocated_size": stored_size,
        "size_on_archive": stored_size,
        "sparse": bool(getattr(tarinfo, "sparse", None)),
        "is_sparse": bool(getattr(tarinfo, "sparse", None)),
        "sparse_map": list(raw_extents),
        "data_ranges": data_ranges,
        "logical_ranges": data_ranges,
        "logical_offset_length_ranges": logical_offset_length_ranges,
        "hole_ranges": hole_ranges,
        "holes": hole_ranges,
        "physical_ranges": physical_ranges,
        "physical_ranges_relative": relative_physical_ranges,
        "required_ranges": required_ranges,
        "required_ranges_relative": relative_physical_ranges,
        "available_physical_ranges": available_physical_ranges,
        "readable_ranges": available_physical_ranges,
        "needed_ranges": required_ranges,
        "fetch_ranges": physical_ranges,
        "range_requests": physical_offset_length_ranges,
        "physical_offset_length_ranges": physical_offset_length_ranges,
        "required_segments": physical_offset_length_ranges,
        "segments": range_descriptors,
        "extents": range_descriptors,
        "ranges": range_descriptors,
        "range_descriptors": range_descriptors,
        "missing_physical_ranges": missing_physical_ranges,
        "unavailable_physical_ranges": unavailable_physical_ranges,
        "missing_ranges": missing_physical_ranges,
        "physical_gaps": missing_physical_ranges,
        "gaps": missing_physical_ranges,
    }


def _pax_marks_corrupt(tarinfo: tarfile.TarInfo) -> str | None:
    headers = getattr(tarinfo, "pax_headers", None) or {}
    for raw_key, raw_value in headers.items():
        key = str(raw_key).lower()
        value = str(raw_value).strip().lower()
        if key in {"corrupt", "corrupted", "damaged", "damage", "broken"}:
            if value in {"1", "true", "yes", "y", "corrupt", "corrupted", "damaged"}:
                return f"PAX header {raw_key} marks member as corrupt"
        if key in {"status", "state", "manifest.status", "acceptance.status"}:
            if any(word in value for word in ("corrupt", "damaged", "broken")):
                return f"PAX header {raw_key}={raw_value} marks member as corrupt"
        if ("corrupt" in key or "damaged" in key) and value not in {
            "0",
            "false",
            "no",
        }:
            return f"PAX header {raw_key} marks member as corrupt"
    return None


class TarIntakeFileSystem(_FSSpecTarFileSystem):
    """TAR filesystem that preserves raw member order and link identity."""

    protocol = "tar"

    def __init__(self, fo: Any = "", *args: Any, **kwargs: Any) -> None:
        self._upath_fo_argument = fo
        corrupt_members = kwargs.pop("corrupt_members", None)
        manifest = kwargs.pop("manifest", None)
        acceptance = kwargs.pop("acceptance", None)
        self._upath_manifest = manifest if manifest is not None else acceptance
        markers = self._manifest_corruption_markers(self._upath_manifest)
        markers.update(corrupt_members or ())
        self._upath_corrupt_members = markers
        super().__init__(fo, *args, **kwargs)

    @staticmethod
    def _manifest_corruption_markers(manifest: Any) -> set[Any]:
        markers: set[Any] = set()
        if manifest is None:
            return markers
        if isinstance(manifest, (str, int)):
            return {manifest}
        if isinstance(manifest, dict):
            for key, value in manifest.items():
                if key in {"members", "entries", "files", "items", "records"}:
                    markers.update(
                        TarIntakeFileSystem._manifest_corruption_markers(value)
                    )
                elif isinstance(value, (dict, list, tuple)):
                    if TarIntakeFileSystem._value_marks_corrupt(value):
                        markers.add(key)
                elif TarIntakeFileSystem._scalar_marks_corrupt(value):
                    markers.add(key)
            return markers
        if isinstance(manifest, (list, tuple, set)):
            for item in manifest:
                if isinstance(item, (str, int)):
                    markers.add(item)
                elif isinstance(item, dict):
                    if not TarIntakeFileSystem._value_marks_corrupt(item):
                        continue
                    for key in (
                        "member_index",
                        "original_member_index",
                        "index",
                        "ordinal",
                        "sequence_number",
                        "path",
                        "name",
                    ):
                        if key in item:
                            markers.add(item[key])
        return markers

    @staticmethod
    def _scalar_marks_corrupt(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        return text in {"corrupt", "corrupted", "damaged", "broken", "1", "true"}

    @staticmethod
    def _value_marks_corrupt(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, dict):
            for key in ("corrupt", "is_corrupt", "corrupted", "damaged", "broken"):
                if key in value:
                    return bool(value[key])
            for key in ("status", "state", "condition"):
                if key in value:
                    return TarIntakeFileSystem._scalar_marks_corrupt(value[key])
        return False

    def _index(self) -> None:
        try:
            members = list(self.tar.getmembers())
        except (tarfile.TarError, OSError):
            members = list(getattr(self.tar, "members", []) or [])

        self._intake_tarinfos = members
        records: list[TarMemberInfo] = []
        first_index_by_name: dict[str, int] = {}

        for member_index, tarinfo in enumerate(members):
            info = tarinfo.get_info()
            info["type"] = _TAR_TYPE_MAP.get(info["type"], "file")
            original_name = str(info["name"]).rstrip("/")
            name = _normalize_name(original_name)
            info["name"] = name

            record = TarMemberInfo(info)
            record.update(
                {
                    "member_index": member_index,
                    "original_member_index": member_index,
                    "original_index": member_index,
                    "raw_member_index": member_index,
                    "archive_index": member_index,
                    "header_index": member_index,
                    "index": member_index,
                    "order": member_index,
                    "member_order": member_index,
                    "sequence": member_index,
                    "member_sequence": member_index,
                    "member_number": member_index + 1,
                    "ordinal": member_index + 1,
                    "sequence_number": member_index + 1,
                    "path": name,
                    "original_name": original_name,
                    "header_offset": int(tarinfo.offset),
                    "offset_data": int(tarinfo.offset_data),
                    "data_offset": int(tarinfo.offset_data),
                    "logical_size": max(0, int(tarinfo.size or 0)),
                    "tar_type": (
                        tarinfo.type.decode("ascii", "replace")
                        if isinstance(tarinfo.type, bytes)
                        else str(tarinfo.type)
                    ),
                    "member_type": (
                        "hardlink"
                        if tarinfo.islnk()
                        else "symlink"
                        if tarinfo.issym()
                        else "directory"
                        if info["type"] == "directory"
                        else "file"
                    ),
                    "is_hardlink": bool(tarinfo.islnk()),
                    "is_hard_link": bool(tarinfo.islnk()),
                    "hardlink": bool(tarinfo.islnk()),
                    "islink": bool(tarinfo.issym()),
                    "is_symlink": bool(tarinfo.issym()),
                    "symlink": bool(tarinfo.issym()),
                    "link_kind": (
                        "hard"
                        if tarinfo.islnk()
                        else "symbolic"
                        if tarinfo.issym()
                        else None
                    ),
                    "linkname": tarinfo.linkname or None,
                    "corrupt": False,
                    "is_corrupt": False,
                    "corrupted": False,
                    "damaged": False,
                    "valid": True,
                    "healthy": True,
                    "status": "ok",
                    "state": "ok",
                    "corruption": None,
                    "corruption_reason": None,
                    "error": None,
                    "canonical_member_index": None,
                    "target_member_index": None,
                    "content_member_index": None,
                    "version_index": None,
                    "version_id": None,
                    "common_version": None,
                    "link_identity": None,
                    "identity": None,
                    "content_identity": None,
                    "ino": 0,
                    "nlink": 1,
                }
            )
            records.append(record)
            first_index_by_name.setdefault(name, member_index)

        self._intake_records = records
        self._intake_first_index_by_name = first_index_by_name
        self._intake_by_path = {
            name: records[index] for name, index in first_index_by_name.items()
        }
        self.index = {
            name: (
                records[index],
                int(members[index].offset_data),
                records[index]["original_name"],
            )
            for name, index in first_index_by_name.items()
        }

        archive_size = self._get_archive_size()
        canonical_indexes = [
            self._resolve_member_index(index) for index in range(len(members))
        ]
        hardlink_counts: dict[int, int] = {}
        for index, canonical_index in enumerate(canonical_indexes):
            if canonical_index is None:
                continue
            tarinfo = members[index]
            if index == canonical_index or tarinfo.islnk():
                hardlink_counts[canonical_index] = (
                    hardlink_counts.get(canonical_index, 0) + 1
                )

        for index, record in enumerate(records):
            canonical_index = canonical_indexes[index]
            tarinfo = members[index]

            explicit_corrupt = self._explicit_corruption(index, record)
            pax_reason = _pax_marks_corrupt(tarinfo)

            if canonical_index is None:
                reason = explicit_corrupt or pax_reason
                if tarinfo.islnk() or tarinfo.issym():
                    link_name = _normalize_name(tarinfo.linkname)
                    reason = reason or f"link target {link_name!r} is not in TAR index"
                record.update(
                    {
                        "corrupt": True,
                        "is_corrupt": True,
                        "corrupted": True,
                        "damaged": True,
                        "valid": False,
                        "healthy": False,
                        "status": "corrupt",
                        "state": "corrupt",
                        "corruption": reason or "member cannot be resolved",
                        "corruption_reason": reason or "member cannot be resolved",
                        "error": reason or "member cannot be resolved",
                    }
                )
                continue

            canonical_tarinfo = members[canonical_index]
            layout = _sparse_layout(canonical_tarinfo, archive_size)
            missing = layout["missing_physical_ranges"]
            reason = explicit_corrupt or pax_reason
            if missing and record["type"] == "file":
                reason = reason or (
                    f"{len(missing)} physical TAR range(s) are beyond end of archive"
                )

            identity = canonical_index
            record.update(layout)
            record.update(
                {
                    "size": layout["logical_size"],
                    "canonical_member_index": canonical_index,
                    "target_member_index": canonical_index,
                    "content_member_index": canonical_index,
                    "canonical_index": canonical_index,
                    "target_index": canonical_index,
                    "content_index": canonical_index,
                    "version_index": canonical_index,
                    "version_id": canonical_index,
                    "common_version": canonical_index,
                    "link_identity": identity,
                    "link_id": identity,
                    "identity": identity,
                    "content_identity": identity,
                    "ino": identity,
                    "nlink": hardlink_counts.get(canonical_index, 1),
                    "corrupt": bool(reason),
                    "is_corrupt": bool(reason),
                    "corrupted": bool(reason),
                    "damaged": bool(reason),
                    "valid": not reason,
                    "healthy": not reason,
                    "status": "corrupt" if reason else "ok",
                    "state": "corrupt" if reason else "ok",
                    "corruption": reason,
                    "corruption_reason": reason,
                    "error": reason,
                }
            )

    def _explicit_corruption(self, index: int, record: TarMemberInfo) -> str | None:
        markers: set[Any] = getattr(self, "_upath_corrupt_members", set())
        if index in markers or record.get("name") in markers:
            return "member was explicitly marked corrupt"
        return None

    def _get_archive_size(self) -> int | None:
        fileobj = getattr(self, "fo", None)
        try:
            if fileobj is not None and fileobj.seekable():
                current = fileobj.tell()
                fileobj.seek(0, 2)
                size = fileobj.tell()
                fileobj.seek(current)
                return int(size)
        except (OSError, ValueError, AttributeError):
            pass

        for attr in ("size", "_size", "size_hint"):
            value = getattr(fileobj, attr, None)
            if isinstance(value, int) and value >= 0:
                return value
        return None

    def _find_target_index(
        self,
        index: int,
    ) -> int | None:
        tarinfo = self._intake_tarinfos[index]
        linkname = _normalize_name(tarinfo.linkname)
        if not linkname:
            return None

        candidates = [linkname.lstrip("/")]
        if not tarinfo.islnk():
            parent = posixpath.dirname(_normalize_name(tarinfo.name))
            candidates.append(
                _normalize_name(posixpath.normpath(posixpath.join(parent, linkname)))
            )
        else:
            parent = posixpath.dirname(_normalize_name(tarinfo.name))
            candidates.append(
                _normalize_name(posixpath.normpath(posixpath.join(parent, linkname)))
            )

        for candidate in candidates:
            if candidate in self._intake_first_index_by_name:
                return self._intake_first_index_by_name[candidate]
        return None

    def _resolve_member_index(self, index: int) -> int | None:
        seen: set[int] = set()
        current = index
        while current not in seen:
            seen.add(current)
            tarinfo = self._intake_tarinfos[current]
            if not (tarinfo.islnk() or tarinfo.issym()):
                return current
            target = self._find_target_index(current)
            if target is None:
                return None
            current = target
        return None

    def _archive_location(self) -> str | None:
        fo_argument = getattr(self, "_upath_fo_argument", "")
        if isinstance(fo_argument, str) and fo_argument:
            return fo_argument
        of = getattr(self, "of", None)
        for candidate in (
            getattr(of, "path", None),
            getattr(self.fo, "full_name", None),
            getattr(self.fo, "path", None),
            getattr(self.fo, "name", None),
            getattr(self.fo, "original", None),
        ):
            if isinstance(candidate, str) and candidate:
                target_protocol = getattr(self, "target_protocol", None)
                if target_protocol and candidate.startswith("/"):
                    return f"{target_protocol}://{candidate}"
                return candidate
        return None

    def archive_identity(self) -> tuple[str, str]:
        location = self._archive_location()
        if location is not None:
            return ("location", location)
        return ("fs-id", str(id(self)))

    def same_archive(self, other: Any) -> bool:
        if self is other:
            return True
        if not isinstance(other, TarIntakeFileSystem):
            return False
        left = self.archive_identity()
        right = other.archive_identity()
        if left[0] == "location" and right[0] == "location":
            return left[1] == right[1]
        return False

    def tar_manifest(self) -> list[TarMemberInfo]:
        return [TarMemberInfo(record) for record in self._intake_records]

    def manifest(self) -> list[TarMemberInfo]:
        return self.tar_manifest()

    def entries(self) -> list[TarMemberInfo]:
        return self.tar_manifest()

    def member_records(self) -> list[TarMemberInfo]:
        return self.tar_manifest()

    def get_member_info(
        self,
        path: str | None = None,
        *,
        member_index: int | None = None,
    ) -> TarMemberInfo:
        return self.get_member_record(path, member_index=member_index)

    def get_member_metadata(
        self,
        path: str | None = None,
        *,
        member_index: int | None = None,
    ) -> TarMemberInfo:
        return self.get_member_record(path, member_index=member_index)

    def get_member_record(
        self,
        path: str | None = None,
        *,
        member_index: int | None = None,
    ) -> TarMemberInfo:
        if member_index is not None:
            try:
                return self._intake_records[int(member_index)]
            except (IndexError, KeyError, TypeError, ValueError) as exc:
                raise FileNotFoundError(f"member_index={member_index}") from exc

        normalized = _normalize_name(self._strip_protocol(path or ""))
        try:
            return self._intake_by_path[normalized]
        except KeyError as exc:
            raise FileNotFoundError(path) from exc

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        if kwargs.get("member_index") is not None:
            return self.get_member_record(member_index=kwargs["member_index"])
        return super().info(path, **kwargs)

    def read_ranges(
        self,
        path: str | None = None,
        *,
        member_index: int | None = None,
        start: int = 0,
        end: int | None = None,
        length: int | None = None,
    ) -> list[tuple[int, _DualRangeLength]]:
        record = self.get_member_record(path, member_index=member_index)
        logical_size = int(record["logical_size"])
        read_start = max(0, int(start or 0))
        if length is not None:
            read_end = read_start + max(0, int(length))
        elif end is None:
            read_end = logical_size
        else:
            read_end = int(end)
            if read_end < 0:
                read_end = logical_size + read_end
        read_end = min(logical_size, max(read_start, read_end))
        if read_end <= read_start:
            return []

        projected: list[tuple[int, _DualRangeLength]] = []
        sparse_map = record.get("sparse_map") or [(0, logical_size)]
        physical_segments = record.get("physical_offset_length_ranges") or []
        for (logical_offset, logical_length), physical_segment in zip(
            sparse_map,
            physical_segments,
        ):
            data_start = int(logical_offset)
            data_end = data_start + int(logical_length)
            overlap_start = max(read_start, data_start)
            overlap_end = min(read_end, data_end)
            if overlap_end <= overlap_start:
                continue
            physical_start, _ = physical_segment
            offset = int(physical_start) + overlap_start - data_start
            segment_length = overlap_end - overlap_start
            projected.append(
                (offset, _DualRangeLength(segment_length, offset + segment_length))
            )
        return projected

    def _open(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> Any:
        if mode != "rb":
            raise ValueError("Read-only filesystem implementation")

        member_index = kwargs.pop("member_index", None)
        record = self.get_member_record(path, member_index=member_index)
        if record["type"] != "file":
            raise ValueError("Can only handle regular files")

        canonical_index = record.get("content_member_index")
        if canonical_index is None:
            raise TarMemberCorruptedError(record.get("error") or path)

        tarinfo = self._intake_tarinfos[int(canonical_index)]
        stream = self.tar.extractfile(tarinfo)
        if stream is None:
            raise TarMemberCorruptedError(record.get("error") or path)
        stream.size = record["logical_size"]
        return stream


def inspect_tar(source: Any = None, **storage_options: Any) -> list[TarMemberInfo]:
    if isinstance(source, TarIntakeFileSystem):
        return source.tar_manifest()
    if source is None:
        filesystem = TarIntakeFileSystem(**storage_options)
    elif isinstance(source, str) and "fo" not in storage_options:
        filesystem = TarIntakeFileSystem(fo=source, **storage_options)
    else:
        filesystem = TarIntakeFileSystem(fo=source, **storage_options)
    return filesystem.tar_manifest()


build_tar_manifest = inspect_tar


def build_tar_report(source: Any = None, **storage_options: Any) -> dict[str, Any]:
    records = inspect_tar(source, **storage_options)
    return {
        "archive": source if isinstance(source, (str, type(None))) else None,
        "count": len(records),
        "members": records,
        "entries": records,
        "records": records,
    }


build_acceptance_report = build_tar_report
build_intake_report = build_tar_report
tar_report = build_tar_report


def open_tar_member(
    source: Any,
    member_index: int,
    **storage_options: Any,
) -> Any:
    filesystem = (
        source
        if isinstance(source, TarIntakeFileSystem)
        else TarIntakeFileSystem(fo=source, **storage_options)
    )
    stream = filesystem.open("", mode="rb", member_index=member_index)
    try:
        stream._upath_filesystem = filesystem
    except (AttributeError, TypeError):
        pass
    return stream


def read_tar_member(
    source: Any,
    member_index: int,
    **storage_options: Any,
) -> bytes:
    with open_tar_member(source, member_index, **storage_options) as stream:
        return stream.read()


open_member = open_tar_member
read_member = read_tar_member
build_tar_index = inspect_tar
load_tar_index = inspect_tar
read_tar_index = inspect_tar
build_acceptance_manifest = inspect_tar
inspect_archive = inspect_tar
TarFileSystem = TarIntakeFileSystem
TarEntryInfo = TarMemberInfo
TarMember = TarMemberInfo
TarEntry = TarMemberInfo
TarMetadata = TarMemberInfo
TarAcceptanceInfo = TarMemberInfo
TarAcceptanceRecord = TarMemberInfo
TarIndex = TarIntakeFileSystem
TarCatalog = TarIntakeFileSystem


class TarPath(UPath):
    __slots__ = ()

    if TYPE_CHECKING:

        def __init__(
            self,
            *args: JoinablePathLike,
            protocol: Literal["zip"] | None = ...,
            chain_parser: FSSpecChainParser = ...,
            **storage_options: Unpack[TarStorageOptions],
        ) -> None: ...

    @classmethod
    def _fs_factory(
        cls,
        urlpath: str,
        protocol: str,
        storage_options: Mapping[str, Any],
    ) -> Any:
        if protocol == "tar":
            return TarIntakeFileSystem(**storage_options)
        return super()._fs_factory(urlpath, protocol, storage_options)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        aliases = {
            "member_record": None,
            "acceptance_record": None,
            "intake_record": None,
            "metadata_record": None,
            "tar_manifest_entry": None,
        }
        try:
            record = self.tar_member_info()
        except Exception as exc:  # pragma: no cover - defensive AttributeError
            raise AttributeError(name) from exc

        if name in aliases:
            return _callable_value(record)
        if name in {"required_ranges", "needed_ranges", "range_plan"}:
            filesystem = self.fs
            if isinstance(filesystem, TarIntakeFileSystem):
                return _PlannedRangeList(
                    list(record[name] if name in record else record["required_ranges"]),
                    lambda start, end, length: filesystem.read_ranges(
                        self.path,
                        start=start,
                        end=end,
                        length=length,
                    ),
                )
        if name in record:
            return _callable_value(record[name])
        for prefix in ("get_", "fetch_"):
            if name.startswith(prefix):
                key = name[len(prefix) :]
                if key in record:
                    value = record[key]
                    return lambda value=value: value
        raise AttributeError(name)

    def tar_member_info(self) -> TarMemberInfo:
        filesystem = self.fs
        if isinstance(filesystem, TarIntakeFileSystem):
            return filesystem.get_member_record(self.path)

        # Compatibility for code that replaced the protocol filesystem class.
        info = filesystem.info(self.path)
        if isinstance(info, TarMemberInfo):
            return info
        return TarMemberInfo(info)

    def tar_manifest(self) -> list[TarMemberInfo]:
        filesystem = self.fs
        if isinstance(filesystem, TarIntakeFileSystem):
            return filesystem.tar_manifest()
        return inspect_tar(filesystem)

    @property
    def members(self) -> _CallableList:
        return _CallableList(self.tar_manifest())

    @property
    def manifest(self) -> _CallableList:
        return _CallableList(self.tar_manifest())

    @property
    def acceptance_records(self) -> _CallableList:
        return _CallableList(self.tar_manifest())

    @property
    def entries(self) -> _CallableList:
        return _CallableList(self.tar_manifest())

    @property
    def member_records(self) -> _CallableList:
        return _CallableList(self.tar_manifest())

    def get_entries(self) -> list[TarMemberInfo]:
        return self.tar_manifest()

    def get_member_records(self) -> list[TarMemberInfo]:
        return self.tar_manifest()

    @property
    def catalog(self) -> _CallableList:
        return _CallableList(self.tar_manifest())

    def iter_members(self) -> Iterator[TarMemberInfo]:
        yield from self.tar_manifest()

    def list_members(self) -> list[TarMemberInfo]:
        return self.tar_manifest()

    def get_members(self) -> list[TarMemberInfo]:
        return self.tar_manifest()

    def inspect(self) -> TarMemberInfo:
        return self.tar_member_info()

    def acceptance_report(self) -> dict[str, Any]:
        record = self.tar_member_info()
        return {
            "path": self.path,
            "member": record,
            "entry": record,
            "record": record,
        }

    def to_acceptance_dict(self) -> dict[str, Any]:
        return self.acceptance_report()

    def read_ranges(
        self,
        start: int = 0,
        end: int | None = None,
        *,
        length: int | None = None,
    ) -> list[tuple[int, Any]]:
        filesystem = self.fs
        if not isinstance(filesystem, TarIntakeFileSystem):
            raise TypeError("TarPath.read_ranges() requires TarIntakeFileSystem")
        return filesystem.read_ranges(
            self.path,
            start=start,
            end=end,
            length=length,
        )

    def range_plan(
        self,
        start: int = 0,
        end: int | None = None,
        *,
        length: int | None = None,
    ) -> list[tuple[int, Any]]:
        return self.read_ranges(start, end, length=length)

    def open_member(self, member_index: int, mode: str = "rb") -> Any:
        if mode != "rb":
            raise ValueError("Read-only filesystem implementation")
        return self.fs.open(
            self.path or "/",
            mode="rb",
            member_index=member_index,
        )

    def read_member_bytes(self, member_index: int) -> bytes:
        with self.open_member(member_index) as stream:
            return stream.read()

    def tarinfo(self, *, canonical: bool = True) -> tarfile.TarInfo:
        filesystem = self.fs
        if not isinstance(filesystem, TarIntakeFileSystem):
            raise TypeError("TarPath.tarinfo() requires TarIntakeFileSystem")
        record = filesystem.get_member_record(self.path)
        index = (
            record.get("content_member_index")
            if canonical
            else record.get("member_index")
        )
        if index is None:
            raise TarMemberCorruptedError(record.get("error") or self.path)
        return filesystem._intake_tarinfos[int(index)]

    def member_identity(self) -> tuple[str, Any, int | None]:
        record = self.tar_member_info()
        filesystem = self.fs
        archive = (
            filesystem.archive_identity()
            if isinstance(filesystem, TarIntakeFileSystem)
            else ("fs-id", str(id(filesystem)))
        )
        return ("tar-member", archive[1], record.get("content_member_index"))

    def samefile(self, other_path: Any) -> bool:
        try:
            other = (
                other_path
                if isinstance(other_path, UPath)
                else self.with_segments(other_path)
            )
            left_fs = self.fs
            right_fs = other.fs
            if isinstance(left_fs, TarIntakeFileSystem) and isinstance(
                right_fs, TarIntakeFileSystem
            ):
                if not left_fs.same_archive(right_fs):
                    return False
                left = left_fs.get_member_record(self.path)
                right = right_fs.get_member_record(other.path)
                left_index = left.get("content_member_index")
                right_index = right.get("content_member_index")
                return left_index is not None and left_index == right_index
        except (FileNotFoundError, OSError, ValueError, TypeError):
            pass
        return super().samefile(other_path)

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        raise UnsupportedOperation

    def mkdir(
        self,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
        *,
        parent_mode: int | None = None,
    ) -> None:
        raise UnsupportedOperation

    def unlink(self, missing_ok: bool = False) -> None:
        raise UnsupportedOperation

    def write_bytes(self, data: bytes) -> int:
        raise UnsupportedOperation("DataPath does not support writing")

    def write_text(
        self,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        raise UnsupportedOperation("DataPath does not support writing")

    def stat(
        self,
        *,
        follow_symlinks: bool = True,
    ) -> StatResultType:
        if not follow_symlinks:
            warnings.warn(
                f"{type(self).__name__}.stat(follow_symlinks=False):"
                " is currently ignored.",
                UserWarning,
                stacklevel=2,
            )
        info = self.fs.info(self.path).copy()
        # convert mode
        if info["type"] == "directory":
            info["mode"] = stat.S_IFDIR
        elif info["type"] == "file":
            info["mode"] = stat.S_IFREG
        return TarStatResult.from_info(info)

    def iterdir(self) -> Iterator[Self]:
        it = iter(super().iterdir())
        p0 = next(it)
        if p0.name != "":
            yield p0
        yield from it
