from __future__ import annotations

import io
import mimetypes
import os
import re
import stat
import threading
import typing
import weakref
from pathlib import Path

from ._exceptions import (
    MultipartError,
    MultipartRangeError,
    MultipartStreamConsumed,
    RequestNotReplayable,
)
from ._types import (
    AsyncByteStream,
    FileContent,
    FileTypes,
    RequestData,
    RequestFiles,
    SyncByteStream,
)
from ._utils import (
    primitive_value_to_str,
    to_bytes,
)

_HTML5_FORM_ENCODING_REPLACEMENTS = {'"': "%22", "\\": "\\\\"}
_HTML5_FORM_ENCODING_REPLACEMENTS.update(
    {chr(c): "%{:02X}".format(c) for c in range(0x1F + 1) if c != 0x1B}
)
_HTML5_FORM_ENCODING_RE = re.compile(
    r"|".join([re.escape(c) for c in _HTML5_FORM_ENCODING_REPLACEMENTS.keys()])
)

__all__ = ["FileRange"]


def _format_form_param(name: str, value: str) -> bytes:
    """
    Encode a name/value pair within a multipart form.
    """

    def replacer(match: typing.Match[str]) -> str:
        return _HTML5_FORM_ENCODING_REPLACEMENTS[match.group(0)]

    value = _HTML5_FORM_ENCODING_RE.sub(replacer, value)
    return f'{name}="{value}"'.encode()


def _guess_content_type(filename: str | None) -> str | None:
    """
    Guesses the mimetype based on a filename. Defaults to `application/octet-stream`.

    Returns `None` if `filename` is `None` or empty.
    """
    if filename:
        return mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return None


def _is_byte_count(value: typing.Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_file_value(value: typing.Any) -> bool:
    return isinstance(value, (bytes, str)) or hasattr(value, "read")


def _as_range_bounds(value: typing.Any) -> tuple[int, int | None] | None:
    """
    Interpret a `slice`, `range`, or `(start, stop)` tuple as a half-open byte
    range. Returns `None` if the value is not a range spec.
    """
    if isinstance(value, slice):
        if value.step not in (None, 1):
            raise MultipartRangeError(
                "Multipart byte ranges do not support a slice step."
            )
        return value.start or 0, value.stop
    if isinstance(value, range):
        if value.step != 1:
            raise MultipartRangeError(
                "Multipart byte ranges do not support a range step."
            )
        return value.start, value.stop
    if (
        isinstance(value, tuple)
        and len(value) == 2
        and _is_byte_count(value[0])
        and (value[1] is None or _is_byte_count(value[1]))
    ):
        return value[0], value[1]
    return None


def _validate_range(start: int, stop: int | None) -> tuple[int, int | None]:
    if not _is_byte_count(start):
        raise TypeError(
            f"Invalid type for range start. Expected int, got {type(start)}: {start!r}"
        )
    if stop is not None and not _is_byte_count(stop):
        raise TypeError(
            f"Invalid type for range stop. Expected int or None, got "
            f"{type(stop)}: {stop!r}"
        )
    if start < 0 or (stop is not None and stop < 0):
        raise MultipartRangeError(
            "Multipart byte ranges must use non-negative offsets."
        )
    if stop is not None and stop < start:
        raise MultipartRangeError(
            f"Invalid multipart byte range [{start}, {stop}): "
            "the stop offset is before the start offset."
        )
    return start, stop


class FileRange:
    """
    An explicit byte range from a file, for use as a multipart `files` value.

    The range is half-open: `[start, stop)`.

    ```python
    with open("archive.bin", "rb") as archive:
        files = {"segment": httpx.FileRange(archive, 65532, 131080)}
        response = httpx.post("https://example.org/import", files=files)
    ```

    Range uploads are read using positional I/O where the file object provides
    a file descriptor, or under a per-file lock with the shared cursor restored
    after every chunk. This allows several concurrent uploads to read different
    ranges, or a range and the complete file, from the same underlying object
    without moving each other's position.

    A `FileRange` may also be used as a range specification inside a file tuple,
    such as `("archive.bin", archive, "application/octet-stream", FileRange(0, 10))`.
    A plain `slice`, `range`, or `(start, stop)` tuple is accepted in the same
    positions.
    """

    file: FileContent
    start: int
    stop: int | None

    def __init__(self, *args: typing.Any) -> None:
        resolved_file: FileContent
        resolved_start: int
        resolved_stop: int | None

        if len(args) == 1 and args[0] is not None and isinstance(args[0], FileRange):
            existing = typing.cast("FileRange", args[0])
            resolved_file = existing.file
            resolved_start = existing.start
            resolved_stop = existing.stop
        else:
            items = list(args)
            file_indexes = [
                index for index, item in enumerate(items) if _is_file_value(item)
            ]
            if len(file_indexes) != 1:
                raise TypeError(
                    "Invalid arguments for FileRange. Expected a file object "
                    f"and a byte range, got {args!r}."
                )
            file_index = file_indexes[0]
            resolved_file = items.pop(file_index)

            resolved_start = 0
            resolved_stop = None
            if len(items) == 1:
                bounds = _as_range_bounds(items[0])
                if bounds is not None:
                    resolved_start, resolved_stop = bounds
                elif _is_byte_count(items[0]):
                    resolved_start = items[0]
                else:
                    raise TypeError(
                        "Invalid byte range for FileRange. Expected a slice, "
                        f"range, or integer offset, got {items[0]!r}."
                    )
            elif len(items) == 2:
                if not (
                    _is_byte_count(items[0])
                    and (items[1] is None or _is_byte_count(items[1]))
                ):
                    raise TypeError(
                        "Invalid byte range for FileRange. Expected integer "
                        f"start and stop offsets, got {items!r}."
                    )
                resolved_start, resolved_stop = items[0], items[1]
            elif len(items) != 0:
                raise TypeError(
                    "Invalid arguments for FileRange. Expected a file object "
                    f"and a byte range, got {args!r}."
                )

        start, stop = _validate_range(resolved_start, resolved_stop)
        self.file = resolved_file
        self.start = start
        self.stop = stop

    @property
    def slice(self) -> slice:
        return slice(self.start, self.stop)

    @property
    def length(self) -> int | None:
        if self.stop is None:
            return None
        return self.stop - self.start

    def __repr__(self) -> str:
        return f"FileRange({self.file!r}, {self.start!r}, {self.stop!r})"


_FILE_LOCKS: dict[int, tuple[typing.Any, threading.RLock]] = {}
_FILE_LOCKS_GUARD = threading.Lock()


def _get_file_lock(fileobj: typing.Any) -> threading.RLock:
    """
    Return the process-wide lock associated with a file object.

    The lock is shared between separate `FileField` instances so that distinct
    requests reading from the same underlying object coordinate when they
    cannot use positional reads.
    """
    key = id(fileobj)
    with _FILE_LOCKS_GUARD:
        entry = _FILE_LOCKS.get(key)
        if entry is not None:
            existing_reference, existing_lock = entry
            resolved = (
                existing_reference()
                if isinstance(existing_reference, weakref.ref)
                else existing_reference
            )
            if resolved is fileobj:
                return existing_lock

        reference: typing.Any
        try:
            reference = weakref.ref(fileobj, _make_lock_finalizer(key))
        except TypeError:
            reference = fileobj
        lock = threading.RLock()
        _FILE_LOCKS[key] = (reference, lock)
        return lock


def _make_lock_finalizer(key: int) -> typing.Callable[..., None]:
    def finalize(_reference: typing.Any) -> None:
        _FILE_LOCKS.pop(key, None)

    return finalize


def _file_descriptor(fileobj: typing.Any) -> int | None:
    try:
        fd = fileobj.fileno()
    except (AttributeError, OSError, ValueError):
        return None
    return fd if isinstance(fd, int) and fd >= 0 else None


def _is_regular_file(fileobj: typing.Any, fd: int) -> bool:
    try:
        return stat.S_ISREG(os.fstat(fd).st_mode)
    except OSError:
        return False


def _supports_positional_reads(fileobj: typing.Any) -> bool:
    if not hasattr(os, "pread"):
        return False
    fd = _file_descriptor(fileobj)
    if fd is None or not _is_regular_file(fileobj, fd):
        # Pipes and sockets expose a descriptor, but `pread` cannot seek in
        # them and their bytes cannot be replayed.
        return False
    # Restrict positional reads to standard binary file implementations. Other
    # file-like wrappers may expose a descriptor while transforming the bytes
    # returned by `read()`, in which case `os.pread` would bypass that logic.
    module = type(fileobj).__module__
    if module in ("io", "_io", "tempfile"):
        return True
    return isinstance(fileobj, (io.FileIO, io.BufferedReader, io.BufferedRandom))


def _is_seekable(fileobj: typing.Any) -> bool:
    try:
        if hasattr(fileobj, "seekable") and not fileobj.seekable():
            return False
        position = fileobj.tell()
        fileobj.seek(position)
    except (AttributeError, OSError, ValueError):
        return False
    return True


def _flush_pending_writes(fileobj: typing.Any) -> None:
    """
    Ensure buffered writes are visible to positional reads on the descriptor.
    """
    flush = getattr(fileobj, "flush", None)
    if flush is None:
        return
    try:
        flush()
    except (OSError, ValueError):
        pass


def _file_length(fileobj: typing.Any) -> int | None:
    """
    Return the file's length without disturbing its shared cursor.
    """
    fd = _file_descriptor(fileobj)
    if fd is not None and _is_regular_file(fileobj, fd):
        _flush_pending_writes(fileobj)
        try:
            return os.fstat(fd).st_size
        except OSError:
            pass
    if not _is_seekable(fileobj):
        return None
    lock = _get_file_lock(fileobj)
    with lock:
        try:
            position = fileobj.tell()
            length = fileobj.seek(0, os.SEEK_END)
            fileobj.seek(position)
            return int(length)
        except (AttributeError, OSError, ValueError):
            return None


def _read_at(fileobj: typing.Any, offset: int, size: int) -> bytes:
    """
    Read up to `size` bytes at `offset` without moving the shared cursor.
    """
    if _supports_positional_reads(fileobj):
        fd = _file_descriptor(fileobj)
        assert fd is not None
        _flush_pending_writes(fileobj)
        try:
            return os.pread(fd, size, offset)
        except OSError:
            pass

    lock = _get_file_lock(fileobj)
    with lock:
        position = fileobj.tell()
        try:
            fileobj.seek(offset)
            chunk = fileobj.read(size)
        finally:
            fileobj.seek(position)
    return to_bytes(chunk)


def get_multipart_boundary_from_content_type(
    content_type: bytes | None,
) -> bytes | None:
    if not content_type or not content_type.startswith(b"multipart/form-data"):
        return None
    # parse boundary according to
    # https://www.rfc-editor.org/rfc/rfc2046#section-5.1.1
    if b";" in content_type:
        for section in content_type.split(b";"):
            if section.strip().lower().startswith(b"boundary="):
                return section.strip()[len(b"boundary=") :].strip(b'"')
    return None


class DataField:
    """
    A single form field item, within a multipart form field.
    """

    def __init__(self, name: str, value: str | bytes | int | float | None) -> None:
        if not isinstance(name, str):
            raise TypeError(
                f"Invalid type for name. Expected str, got {type(name)}: {name!r}"
            )
        if value is not None and not isinstance(value, (str, bytes, int, float)):
            raise TypeError(
                "Invalid type for value. Expected primitive type,"
                f" got {type(value)}: {value!r}"
            )
        self.name = name
        self.value: str | bytes = (
            value if isinstance(value, bytes) else primitive_value_to_str(value)
        )

    def render_headers(self) -> bytes:
        if not hasattr(self, "_headers"):
            name = _format_form_param("name", self.name)
            self._headers = b"".join(
                [b"Content-Disposition: form-data; ", name, b"\r\n\r\n"]
            )

        return self._headers

    def render_data(self) -> bytes:
        if not hasattr(self, "_data"):
            self._data = to_bytes(self.value)

        return self._data

    def get_length(self) -> int:
        headers = self.render_headers()
        data = self.render_data()
        return len(headers) + len(data)

    def render(self) -> typing.Iterator[bytes]:
        yield self.render_headers()
        yield self.render_data()


class FileField:
    """
    A single file field item, within a multipart form field.
    """

    CHUNK_SIZE = 64 * 1024

    def __init__(self, name: str, value: FileTypes) -> None:
        self.name = name

        fileobj: FileContent
        filename: str | None

        headers: dict[str, str] = {}
        content_type: str | None = None
        byte_range: tuple[int, int | None] | None = None

        # This large tuple based API largely mirror's requests' API
        # It would be good to think of better APIs for this that we could
        # include in httpx 2.0 since variable length tuples(especially of 4 elements)
        # are quite unwieldly
        if isinstance(value, FileRange):
            filename = Path(str(getattr(value.file, "name", "upload"))).name
            fileobj = value.file
            byte_range = (value.start, value.stop)
        elif isinstance(value, tuple):
            if not value:
                raise TypeError("Multipart file tuples cannot be empty.")
            filename, fileobj, content_type, headers, byte_range = (
                self._parse_file_tuple(value)
            )
        else:
            filename = Path(str(getattr(value, "name", "upload"))).name
            fileobj = value

        if content_type is None:
            content_type = _guess_content_type(filename)

        has_content_type_header = any("content-type" in key.lower() for key in headers)
        if content_type is not None and not has_content_type_header:
            # note that unlike requests, we ignore the content_type provided in the 3rd
            # tuple element if it is also included in the headers requests does
            # the opposite (it overwrites the headerwith the 3rd tuple element)
            headers["Content-Type"] = content_type

        if isinstance(fileobj, io.StringIO):
            raise TypeError(
                "Multipart file uploads require 'io.BytesIO', not 'io.StringIO'."
            )
        if isinstance(fileobj, io.TextIOBase):
            raise TypeError(
                "Multipart file uploads must be opened in binary mode, not text mode."
            )

        if byte_range is not None:
            start, stop = _validate_range(byte_range[0], byte_range[1])
            byte_range = (start, stop)

        self.filename = filename
        self.file = fileobj
        self.headers = headers
        self.byte_range = byte_range
        self._started = False
        self._replayable = self._is_replayable(fileobj)

    @staticmethod
    def _parse_file_tuple(
        value: tuple[typing.Any, ...],
    ) -> tuple[
        str | None,
        FileContent,
        str | None,
        dict[str, str],
        tuple[int, int | None] | None,
    ]:
        """
        Parse the historic `(filename, file, content_type, headers)` tuple,
        additionally accepting a byte range descriptor in either of the two
        optional trailing positions.

        The short `(file, range)` form, and the `(file, ..., range)` form
        without a filename, are both accepted too.
        """
        items = list(value)

        if isinstance(items[0], str) or items[0] is None:
            filename: str | None = items.pop(0)
        else:
            filename = None

        if not items:
            raise TypeError("Multipart file tuples must include a file value.")

        fileobj = items.pop(0)
        if isinstance(fileobj, FileRange):
            byte_range: tuple[int, int | None] | None = (
                fileobj.start,
                fileobj.stop,
            )
            fileobj = fileobj.file
        else:
            byte_range = None

        if (
            byte_range is None
            and len(items) == 2
            and _is_byte_count(items[0])
            and (items[1] is None or _is_byte_count(items[1]))
        ):
            byte_range = _validate_range(items[0], items[1])
            items = []

        content_type: str | None = None
        headers: dict[str, str] = {}
        extras: list[typing.Any] = []
        for item in items:
            if isinstance(item, FileRange):
                byte_range = (item.start, item.stop)
            elif _as_range_bounds(item) is not None:
                byte_range = _as_range_bounds(item)
            elif item is None or isinstance(item, str):
                content_type = item
            elif isinstance(item, typing.Mapping):
                headers = dict(item)
            else:
                extras.append(item)

        if extras:
            raise TypeError(f"Invalid multipart file tuple elements: {extras!r}")

        if byte_range is not None:
            byte_range = _validate_range(byte_range[0], byte_range[1])

        return filename, fileobj, content_type, headers, byte_range

    @staticmethod
    def _is_replayable(fileobj: FileContent) -> bool:
        if isinstance(fileobj, (bytes, str)):
            return True
        if _supports_positional_reads(fileobj):
            return True
        return _is_seekable(fileobj)

    @property
    def started(self) -> bool:
        return self._started

    @property
    def replayable(self) -> bool:
        return self._replayable

    def can_replay(self) -> bool:
        return self._replayable or not self._started

    def _file_size(self) -> int | None:
        if isinstance(self.file, (bytes, str)):
            return len(to_bytes(self.file))
        return _file_length(self.file)

    def _range_length(self) -> int | None:
        if self.byte_range is None:
            return None
        start, stop = self.byte_range
        if stop is not None:
            return stop - start
        file_size = self._file_size()
        if file_size is None:
            return None
        return max(file_size - start, 0)

    def get_length(self) -> int | None:
        headers = self.render_headers()

        if isinstance(self.file, (bytes, str)):
            data_length = len(to_bytes(self.file))
            if self.byte_range is not None:
                start, stop = self.byte_range
                data_length = len(to_bytes(self.file)[start:stop])
            return len(headers) + data_length

        if self.byte_range is not None:
            range_length = self._range_length()
            if range_length is None:
                return None
            return len(headers) + range_length

        file_length = _file_length(self.file)

        # If we can't determine the filesize without reading it into memory,
        # then return `None` here, to indicate an unknown file length.
        if file_length is None:
            return None

        return len(headers) + file_length

    def render_headers(self) -> bytes:
        if not hasattr(self, "_headers"):
            parts = [
                b"Content-Disposition: form-data; ",
                _format_form_param("name", self.name),
            ]
            if self.filename:
                filename = _format_form_param("filename", self.filename)
                parts.extend([b"; ", filename])
            for header_name, header_value in self.headers.items():
                key, val = f"\r\n{header_name}: ".encode(), header_value.encode()
                parts.extend([key, val])
            parts.append(b"\r\n\r\n")
            self._headers = b"".join(parts)

        return self._headers

    def _iter_replayable_data(self) -> typing.Iterator[bytes]:
        """
        Yield the requested bytes using positional reads where possible, so
        that concurrent uploads sharing a file object do not disturb each
        other's cursor.
        """
        start, stop = self.byte_range or (0, None)

        if isinstance(self.file, (bytes, str)):
            data = to_bytes(self.file)
            yield data[start:stop]
            return

        size = self._file_size()
        if stop is not None and size is not None and stop > size:
            raise MultipartError(
                f"The {self.name!r} file part requested bytes [{start}, {stop}), "
                f"but the file only contains {size} bytes."
            )

        offset = start
        remaining = None if stop is None else stop - start

        while remaining is None or remaining > 0:
            size_to_read = self.CHUNK_SIZE
            if remaining is not None:
                size_to_read = min(size_to_read, remaining)

            supported = _supports_positional_reads(self.file)
            if supported:
                chunk = _read_at(self.file, offset, size_to_read)
            else:
                chunk = self._read_seekable(size_to_read, offset)

            if not chunk:
                if self.byte_range is not None and stop is not None:
                    raise MultipartError(
                        f"The {self.name!r} file part ended before the requested "
                        f"range [{start}, {stop}) was complete."
                    )
                return

            yield to_bytes(chunk)
            offset += len(chunk)
            if remaining is not None:
                remaining -= len(chunk)

    def _read_seekable(self, size: int, offset: int) -> bytes:
        """
        Read from a seekable file, restoring its shared cursor afterwards.
        """
        fileobj: typing.Any = self.file
        lock = _get_file_lock(fileobj)
        with lock:
            position = fileobj.tell()
            try:
                fileobj.seek(offset)
                return to_bytes(fileobj.read(size))
            finally:
                fileobj.seek(position)

    def _iter_streaming_data(self) -> typing.Iterator[bytes]:
        """
        Yield bytes from a file that cannot be randomly accessed. This is a
        one-shot operation; the shared cursor cannot be rewound.
        """
        fileobj: typing.Any = self.file
        start = 0 if self.byte_range is None else self.byte_range[0]
        stop = None if self.byte_range is None else self.byte_range[1]
        remaining = None if stop is None else stop - start

        if start and hasattr(fileobj, "seek"):
            try:
                fileobj.seek(start)
            except (io.UnsupportedOperation, OSError, ValueError):
                skipped = 0
                while skipped < start:
                    to_skip = min(self.CHUNK_SIZE, start - skipped)
                    chunk = fileobj.read(to_skip)
                    if not chunk:
                        raise MultipartError(
                            f"The {self.name!r} file part ended before the "
                            f"requested range start offset {start}."
                        )
                    skipped += len(chunk)

        while remaining is None or remaining > 0:
            size_to_read = self.CHUNK_SIZE
            if remaining is not None:
                size_to_read = min(size_to_read, remaining)
            chunk = fileobj.read(size_to_read)
            if not chunk:
                if self.byte_range is not None and stop is not None:
                    raise MultipartError(
                        f"The {self.name!r} file part ended before the requested "
                        f"range [{start}, {stop}) was complete."
                    )
                return
            yield to_bytes(chunk)
            if remaining is not None:
                remaining -= len(chunk)

    def render_data(self) -> typing.Iterator[bytes]:
        if isinstance(self.file, (bytes, str)):
            if self.byte_range is None:
                yield to_bytes(self.file)
            else:
                start, stop = self.byte_range
                yield to_bytes(self.file)[start:stop]
            return

        if not self._replayable:
            iterator = self._iter_streaming_data()
            try:
                for chunk in iterator:
                    self._started = True
                    yield chunk
            finally:
                self._started = True
            return

        if self.byte_range is None and not _supports_positional_reads(self.file):
            # Preserve the historic rewinding behaviour for seekable complete
            # uploads, under the same lock that range reads use.
            fileobj: typing.Any = self.file
            lock = _get_file_lock(fileobj)
            with lock:
                try:
                    fileobj.seek(0)
                except (io.UnsupportedOperation, OSError, ValueError):
                    pass

        yield from self._iter_replayable_data()

    def render(self) -> typing.Iterator[bytes]:
        yield self.render_headers()
        yield from self.render_data()


class MultipartStream(SyncByteStream, AsyncByteStream):
    """
    Request content as streaming multipart encoded form data.
    """

    def __init__(
        self,
        data: RequestData,
        files: RequestFiles,
        boundary: bytes | None = None,
    ) -> None:
        if boundary is None:
            boundary = os.urandom(16).hex().encode("ascii")

        self.boundary = boundary
        self.content_type = "multipart/form-data; boundary=%s" % boundary.decode(
            "ascii"
        )
        self.fields = list(self._iter_fields(data, files))
        self._boundary_rendered = False

    @property
    def file_fields(self) -> list[FileField]:
        return [field for field in self.fields if isinstance(field, FileField)]

    def can_replay(self) -> bool:
        """
        Return `True` if the complete form can be produced again from the start.
        """
        return all(field.can_replay() for field in self.file_fields)

    def assert_replayable(self) -> None:
        for file_field in self.file_fields:
            if not file_field.can_replay():
                raise RequestNotReplayable(
                    f"the {file_field.name!r} form part has already been read and "
                    "cannot be sent again, because it has no known length and "
                    "cannot be rewound."
                )

    def _iter_fields(
        self, data: RequestData, files: RequestFiles
    ) -> typing.Iterator[FileField | DataField]:
        for name, value in data.items():
            if isinstance(value, (tuple, list)):
                for item in value:
                    yield DataField(name=name, value=item)
            else:
                yield DataField(name=name, value=value)

        file_items = files.items() if isinstance(files, typing.Mapping) else files
        for name, value in file_items:
            yield FileField(name=name, value=value)

    def iter_chunks(self) -> typing.Iterator[bytes]:
        if self._boundary_rendered:
            # A second iteration must start from the beginning of the form.
            # Any one-shot part that was already touched makes that impossible.
            for file_field in self.file_fields:
                if not file_field.can_replay():
                    raise MultipartStreamConsumed(file_field.name)
        self._boundary_rendered = True

        for field in self.fields:
            yield b"--%s\r\n" % self.boundary
            yield from field.render()
            yield b"\r\n"
        yield b"--%s--\r\n" % self.boundary

    def get_content_length(self) -> int | None:
        """
        Return the length of the multipart encoded content, or `None` if
        any of the files have a length that cannot be determined upfront.
        """
        boundary_length = len(self.boundary)
        length = 0

        for field in self.fields:
            field_length = field.get_length()
            if field_length is None:
                return None

            length += 2 + boundary_length + 2  # b"--{boundary}\r\n"
            length += field_length
            length += 2  # b"\r\n"

        length += 2 + boundary_length + 4  # b"--{boundary}--\r\n"
        return length

    # Content stream interface.

    def get_headers(self) -> dict[str, str]:
        content_length = self.get_content_length()
        content_type = self.content_type
        if content_length is None:
            return {"Transfer-Encoding": "chunked", "Content-Type": content_type}
        return {"Content-Length": str(content_length), "Content-Type": content_type}

    def __iter__(self) -> typing.Iterator[bytes]:
        for chunk in self.iter_chunks():
            yield chunk

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        for chunk in self.iter_chunks():
            yield chunk
