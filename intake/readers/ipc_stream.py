"""Resumable, batch-by-batch reading of Arrow IPC streams and files.

The ordinary :class:`~intake.readers.readers.PyArrowIPCReader` loads a whole
Arrow IPC payload into one table.  This module adds an ingestion-oriented mode
where complete record batches are *delivered as soon as they arrive*, without
waiting for the stream to finish, and where an interrupted non-seekable stream
can be resumed from the start of any previously sent record-batch message.

Two payload shapes are handled:

* the non-seekable IPC **stream** format used by device uploads (schema message,
  interleaved dictionary and record-batch messages, explicit EOS marker);
* the seekable IPC **file** format used for archive backfill (``ARROW1``
  framing with a footer); a truncated/corrupt archive is reported as a corrupt
  file and never yields a stream resume position.

Resume information is intentionally decoupled from warehouse acknowledgements.
Seeing (delivering) a batch and the warehouse confirming that batch landed are
different events, so the saved :class:`StreamResumeInfo` records every message
boundary (plus the schema and dictionary message bytes needed to rebuild
state), while the actual contiguous prefix confirmed by the warehouse is
supplied separately when a new reader is created.
"""

from __future__ import annotations

import base64
import struct

# Arrow IPC continuation marker and framing constants.
_CONTINUATION = b"\xff\xff\xff\xff"
_ARROW_MAGIC = b"ARROW1"
_PREFIX_LEN = len(_CONTINUATION) + 4  # continuation marker + metadata length int
_FILE_PADDING = 8

# MessageHeader union values in the flatbuffers Message table.
_MSG_SCHEMA = 1
_MSG_DICTIONARY_BATCH = 2
_MSG_RECORD_BATCH = 3

__all__ = [
    "ArrowIPCError",
    "CorruptArrowIPCFile",
    "ArrowIPCInterrupted",
    "StaleResumeInfo",
    "StreamResumeInfo",
    "ResumableIPCReader",
    "check_ipc_file_magic",
    "open_ipc_file",
]


class ArrowIPCError(Exception):
    """Base class for errors raised while parsing an Arrow IPC payload."""


class CorruptArrowIPCFile(ArrowIPCError):
    """A seekable Arrow IPC *file* (``ARROW1`` framing) or a frame is damaged.

    File corruption is never resumable: no stream position is offered because
    the archive format is not a message stream that can be replayed from a
    message boundary.
    """


class ArrowIPCInterrupted(ArrowIPCError):
    """Raised when an IPC stream ends in the middle of a message.

    Attributes
    ----------
    message_position:
        Byte offset (in the coordinate space of the original stream) of the
        first message whose framing could not be fully read.
    last_delivered_batch:
        Sequence number of the last record batch that was completely delivered
        to the caller (``0`` means none).  Delivery is not an acknowledgement.
    resume_at:
        Message start to ask the service to replay from, computed from the
        warehouse-confirmed prefix supplied to the reader (``None`` when no
        record batch is confirmed yet).
    """

    def __init__(self, message_position, last_delivered_batch, resume_at):
        self.message_position = message_position
        self.last_delivered_batch = last_delivered_batch
        self.resume_at = resume_at
        target = "stream start" if resume_at is None else f"batch {resume_at} message start"
        super().__init__(
            f"Arrow IPC stream interrupted inside the message at byte {message_position}; "
            f"last fully delivered batch is {last_delivered_batch}, "
            f"request replay from {target}"
        )


class StaleResumeInfo(ValueError):
    """Saved resume info does not match the stream being read.

    Raised before any rows are delivered when the stream identifier or stream
    version differs, so data from one stream version can never leak into
    another.
    """


def _read_exact(feed, size):
    """Read up to *size* bytes from *feed*, batching short reads."""
    pieces = []
    remaining = size
    while remaining:
        chunk = feed(remaining)
        if not chunk:
            break
        pieces.append(chunk)
        remaining -= len(chunk)
    return b"".join(pieces)


def _vtable_field(metadata, slot, fmt):
    """Read one flatbuffers table field through its vtable.

    Returns ``None`` when the vtable predates the field or the field is absent.
    The buffer starts with a uoffset to the root table; the table's first
    soffset points backwards to its vtable.  The Message vtable stores, in
    order, ``version`` (slot 0), the ``MessageHeader`` union type byte
    (slot 1), the union offset (slot 2), ``bodyLength`` (slot 3) and
    ``custom_metadata`` (slot 4).
    """
    (root_table,) = struct.unpack_from("<I", metadata, 0)
    (soffset,) = struct.unpack_from("<i", metadata, root_table)
    vtable = root_table - soffset
    (vtable_size,) = struct.unpack_from("<H", metadata, vtable)
    field_offset_pos = vtable + 4 + 2 * slot
    if field_offset_pos + 2 > vtable + vtable_size:
        return None
    (field_offset,) = struct.unpack_from("<H", metadata, field_offset_pos)
    if field_offset == 0:
        return None
    (value,) = struct.unpack_from(fmt, metadata, root_table + field_offset)
    return value


def _message_type(metadata):
    return _vtable_field(metadata, 1, "<B")


def _message_body_length(metadata):
    body_length = _vtable_field(metadata, 3, "<q")
    return body_length or 0


class StreamResumeInfo:
    """Serializable, version-safe bookmark of a partially read IPC stream.

    The bookmark records message boundaries plus the raw schema and dictionary
    message bytes required to rebuild decoding state for a replay fragment.
    Replay fragments start at a record-batch message and contain neither the
    schema message nor a full dictionary, so both must come from here.  It
    never decides which batches the warehouse has stored: the confirmed prefix
    is passed separately when resuming.

    Attributes
    ----------
    stream_id, stream_version:
        Identity of the stream; bookmarks are rejected when either changes.
    schema_frame:
        Raw encapsulated bytes of the schema message.
    dictionary_frames:
        ``(absolute byte offset, encapsulated message bytes)`` for every fully
        received dictionary message, in arrival order.
    batch_starts:
        Mapping ``record batch sequence number -> absolute byte offset of that
        batch's message start``.  Batch numbering starts at 1 and is dense.
    next_message_start:
        Absolute start of the first message that has not been completely read
        yet.  A message boundary is known as soon as its framing begins, even
        if the body never finishes, so this captures the start of the batch
        that was interrupted — exactly the replay point when every earlier
        batch is already confirmed.  ``None`` once the EOS marker is seen.
    basis_offset:
        Absolute stream offset corresponding to local offset 0 of the bytes
        fed to the reader that produced this bookmark (0 on a fresh stream,
        the replay start on a reconnected one).
    next_batch:
        Sequence number the next record batch will receive.
    last_delivered_batch:
        Highest batch number fully handed to a caller so far.
    """

    def __init__(
        self,
        stream_id,
        stream_version,
        schema_frame=b"",
        dictionary_frames=None,
        batch_starts=None,
        next_message_start=None,
        basis_offset=0,
        next_batch=1,
        last_delivered_batch=0,
    ):
        self.stream_id = stream_id
        self.stream_version = stream_version
        self.schema_frame = bytes(schema_frame)
        self.dictionary_frames = list(dictionary_frames or [])
        self.batch_starts = dict(batch_starts or {})
        self.next_message_start = next_message_start
        self.basis_offset = basis_offset
        self.next_batch = next_batch
        self.last_delivered_batch = last_delivered_batch

    def check(self, stream_id, stream_version):
        """Raise :class:`StaleResumeInfo` unless identity and version match."""
        if stream_id != self.stream_id or stream_version != self.stream_version:
            raise StaleResumeInfo(
                f"resume info is for stream {self.stream_id!r} version "
                f"{self.stream_version!r}, but got stream {stream_id!r} version "
                f"{stream_version!r}"
            )

    def copy(self):
        """Independent duplicate, so a new reader cannot mutate the original."""
        return StreamResumeInfo(
            self.stream_id,
            self.stream_version,
            schema_frame=self.schema_frame,
            dictionary_frames=[(offset, frame) for offset, frame in self.dictionary_frames],
            batch_starts=self.batch_starts,
            next_message_start=self.next_message_start,
            basis_offset=self.basis_offset,
            next_batch=self.next_batch,
            last_delivered_batch=self.last_delivered_batch,
        )

    def to_dict(self):
        """Plain JSON-friendly representation, e.g. for persisting externally."""
        return {
            "stream_id": self.stream_id,
            "stream_version": self.stream_version,
            "schema_frame": base64.b64encode(self.schema_frame).decode("ascii"),
            "dictionary_frames": [
                [offset, base64.b64encode(frame).decode("ascii")]
                for offset, frame in self.dictionary_frames
            ],
            "batch_starts": list(self.batch_starts.items()),
            "next_message_start": self.next_message_start,
            "basis_offset": self.basis_offset,
            "next_batch": self.next_batch,
            "last_delivered_batch": self.last_delivered_batch,
        }

    @classmethod
    def from_dict(cls, payload):
        return cls(
            stream_id=payload["stream_id"],
            stream_version=payload["stream_version"],
            schema_frame=base64.b64decode(payload.get("schema_frame", b"")),
            dictionary_frames=[
                [offset, base64.b64decode(frame)]
                for offset, frame in payload.get("dictionary_frames", [])
            ],
            batch_starts=payload.get("batch_starts"),
            next_message_start=payload.get("next_message_start"),
            basis_offset=payload.get("basis_offset", 0),
            next_batch=payload.get("next_batch", 1),
            last_delivered_batch=payload.get("last_delivered_batch", 0),
        )

    def __eq__(self, other):
        if not isinstance(other, StreamResumeInfo):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __repr__(self):
        return (
            f"StreamResumeInfo(stream_id={self.stream_id!r}, "
            f"stream_version={self.stream_version!r}, "
            f"last_delivered_batch={self.last_delivered_batch}, "
            f"batch_starts={self.batch_starts})"
        )


class _FrameParser:
    """Byte-level state machine over one (possibly truncated) IPC fragment.

    Only IPC framing and flatbuffers vtable navigation are understood here —
    no pyarrow dependency — so message positions and truncation points can be
    tracked precisely.  Partial frame bytes are retained so
    :attr:`pending_start` always reports the first message that did not finish.
    """

    def __init__(self, feed):
        if hasattr(feed, "read") and not callable(feed):
            read = feed.read

            def feed(size):
                return read(size)

        self._feed = feed
        self._offset = 0
        self._pending = b""
        self._pending_start = 0

    @property
    def offset(self):
        return self._offset

    @property
    def pending_start(self):
        """Local start offset of the partially received frame, if any."""
        if self._pending:
            return self._pending_start
        return self._offset

    def _take(self, size):
        data = _read_exact(self._feed, size)
        self._offset += len(data)
        return data

    def frames(self):
        """Yield ``(kind, local_start, frame_bytes)`` for complete frames.

        ``kind`` is one of ``"schema"``, ``"dictionary"``, ``"record"`` or
        ``"eos"`` and ``frame_bytes`` are the exact on-wire encapsulated bytes.
        If the feed stops mid-frame the partial bytes (and the frame start) are
        retained and the generator returns, letting the caller report the
        interruption instead of pretending the message completed.
        """
        while True:
            frame_start = self._offset - len(self._pending)
            prefix = self._pending + self._take(_PREFIX_LEN - len(self._pending))
            if len(prefix) < _PREFIX_LEN:
                self._pending = prefix
                self._pending_start = frame_start
                return
            if prefix == _CONTINUATION + struct.pack("<i", 0):
                self._pending = b""
                yield "eos", frame_start, prefix
                return
            if prefix[:4] != _CONTINUATION:
                raise CorruptArrowIPCFile(
                    f"expected IPC continuation marker at byte {frame_start}, "
                    f"found {prefix[:4]!r}"
                )
            (metadata_length,) = struct.unpack("<i", prefix[4:8])
            if metadata_length <= 0:
                raise CorruptArrowIPCFile(
                    f"invalid IPC metadata length {metadata_length} at byte {frame_start}"
                )
            metadata = self._take(metadata_length)
            if len(metadata) < metadata_length:
                self._pending = prefix + metadata
                self._pending_start = frame_start
                return
            padding = (-metadata_length) % _FILE_PADDING
            pad = self._take(padding)
            if len(pad) < padding:
                self._pending = prefix + metadata + pad
                self._pending_start = frame_start
                return
            metadata = metadata[:metadata_length]
            body_length = _message_body_length(metadata)
            body_padding = (-body_length) % _FILE_PADDING
            body = self._take(body_length + body_padding)
            if len(body) < body_length + body_padding:
                self._pending = prefix + metadata + pad + body
                self._pending_start = frame_start
                return
            self._pending = b""
            message_type = _message_type(metadata)
            if message_type == _MSG_SCHEMA:
                kind = "schema"
            elif message_type == _MSG_DICTIONARY_BATCH:
                kind = "dictionary"
            elif message_type == _MSG_RECORD_BATCH:
                kind = "record"
            else:
                raise CorruptArrowIPCFile(
                    f"unknown Arrow IPC MessageHeader type {message_type} at byte {frame_start}"
                )
            yield kind, frame_start, prefix + metadata + pad + body


class _BufferedWindow:
    """Readable view over an accumulating byte buffer.

    pyarrow's stream decoder reads from this while frame bytes are appended
    only after a whole frame has been verified complete, so its reads never
    underrun and a truncated message never reaches the decoder.
    """

    def __init__(self, initial=b""):
        self.buffer = bytearray(initial)
        self.position = 0
        self.closed = False

    def append(self, data):
        self.buffer += data

    def compact(self):
        """Drop bytes the decoder has already consumed.

        The IPC stream decoder only reads forward, so everything before
        :attr:`position` can never be needed again; compacting keeps the buffer
        bounded to the in-flight frames instead of the whole stream.
        """
        if self.position:
            del self.buffer[: self.position]
            self.position = 0

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False

    def tell(self):
        return self.position

    def close(self):
        self.closed = True

    @property
    def mode(self):
        return "rb"

    def read(self, size=-1):
        if size is None or size < 0:
            data = bytes(self.buffer[self.position:])
            self.position = len(self.buffer)
            return data
        end = min(self.position + size, len(self.buffer))
        data = bytes(self.buffer[self.position:end])
        self.position = end
        return data


class ResumableIPCReader:
    """Iterate an Arrow IPC stream one complete record batch at a time.

    Parameters
    ----------
    feed:
        Zero-argument callable returning the next chunk of stream bytes, or an
        empty ``bytes``/``None`` at end of data (the same contract as a
        non-seekable socket read).  A plain file-like object with ``read`` is
        also accepted.
    stream_id, stream_version:
        Identity of the stream; checked against *resume_info*.
    resume_info:
        Optional :class:`StreamResumeInfo` saved by an earlier reader instance.
    confirmed_batch:
        The highest batch number the warehouse has confirmed as part of the
        contiguous stored prefix (``0``/``None`` = nothing confirmed).  Replayed
        batches at or below this number are parsed to keep dictionary state
        correct but are not delivered again; the next delivered batch is
        ``confirmed_batch + 1``.
    """

    def __init__(self, feed, stream_id, stream_version, resume_info=None, confirmed_batch=None):
        if hasattr(feed, "read") and not callable(feed):
            read = feed.read

            def feed(size):
                return read(size)

        self._feed = feed
        self.stream_id = stream_id
        self.stream_version = stream_version
        if resume_info is not None:
            # Reject a mismatched stream/version before touching any bytes.
            resume_info.check(stream_id, stream_version)
            self.resume_info = resume_info.copy()
        else:
            self.resume_info = StreamResumeInfo(stream_id, stream_version)
        self.confirmed_batch = confirmed_batch or 0
        if self.confirmed_batch and self.confirmed_batch not in self.resume_info.batch_starts:
            raise StaleResumeInfo(
                f"batch {self.confirmed_batch} is claimed confirmed but the resume info "
                f"only knows about batches {sorted(self.resume_info.batch_starts)}"
            )
        self._target_batch = self.confirmed_batch + 1
        if self.resume_info.batch_starts or self.resume_info.next_message_start is not None:
            if self._target_batch in self.resume_info.batch_starts:
                # Replay from a batch that had been completely received.
                self._basis = self.resume_info.batch_starts[self._target_batch]
            elif (
                self._target_batch == self.resume_info.next_batch
                and self.resume_info.next_message_start is not None
            ):
                # The target batch is the one whose body was cut short: its
                # message start is known even though it never completed.
                self._basis = self.resume_info.next_message_start
            else:
                raise StaleResumeInfo(
                    f"cannot resume: message start for batch {self._target_batch} was not "
                    f"recorded in the resume info"
                )
            # The replay fragment restarts numbering at the target batch and
            # re-sends dictionary messages from the replay point onward; drop
            # any state that belonged to bytes at or after the replay point.
            self.resume_info.batch_starts = {
                number: start
                for number, start in self.resume_info.batch_starts.items()
                if number < self._target_batch
            }
            self.resume_info.dictionary_frames = [
                (offset, frame)
                for offset, frame in self.resume_info.dictionary_frames
                if offset < self._basis
            ]
            self.resume_info.next_batch = self._target_batch
            self.resume_info.last_delivered_batch = min(
                self.resume_info.last_delivered_batch, self.confirmed_batch
            )
        else:
            self._basis = self.resume_info.basis_offset
        self.resume_info.basis_offset = self._basis
        self._parser = _FrameParser(self._feed)
        self._window = None
        self._decoder = None
        self._closed_clean = False

    @property
    def last_delivered_batch(self):
        return self.resume_info.last_delivered_batch

    @property
    def saw_end_of_stream(self):
        """True only after the explicit IPC EOS marker has been consumed."""
        return self._closed_clean

    def _build_prefix(self):
        """Schema + dictionary messages strictly before the replay point."""
        if not self.resume_info.schema_frame:
            return b""
        cutoff = self._basis
        pieces = [self.resume_info.schema_frame]
        pieces.extend(
            frame
            for offset, frame in self.resume_info.dictionary_frames
            if offset < cutoff
        )
        return b"".join(pieces)

    def _ensure_decoder(self):
        import pyarrow.ipc as ipc

        if self._decoder is None:
            self._window = _BufferedWindow(self._build_prefix())
            try:
                self._decoder = ipc.open_stream(self._window)
            except Exception as err:
                raise CorruptArrowIPCFile(
                    f"could not initialise IPC stream decoder: {err}"
                ) from err

    def _decode_next(self):
        try:
            return self._decoder.read_next_batch()
        except StopIteration:
            raise
        except Exception as err:
            raise CorruptArrowIPCFile(f"could not decode Arrow IPC record batch: {err}") from err

    def iter_batches(self):
        """Yield ``(batch_number, record_batch)`` for each newly complete batch.

        Replayed batches at or below the confirmed prefix are consumed (and
        applied to the local dictionary state) but not yielded.  A truncated
        message raises :class:`ArrowIPCInterrupted`; framing or payload damage
        raises :class:`CorruptArrowIPCFile`.
        """
        for kind, local_start, frame_bytes in self._parser.frames():
            absolute_start = self._basis + local_start
            # Boundary of the message currently in flight: if the feed stops
            # inside this frame, this offset is exactly the first unfinished
            # message position to report and (when applicable) replay from.
            self.resume_info.next_message_start = absolute_start
            if kind == "eos":
                self._closed_clean = True
                self.resume_info.next_message_start = None
                return
            if kind == "schema":
                # A replay fragment must not carry a schema message; the
                # bookmark prefix already supplied one.
                if self._basis:
                    raise CorruptArrowIPCFile(
                        "unexpected schema message in a replay fragment; services must resume "
                        "from a record-batch message start"
                    )
                self.resume_info.schema_frame = frame_bytes
                self._window = _BufferedWindow(frame_bytes)
                self._ensure_decoder()
                continue
            self._ensure_decoder()
            self._window.append(frame_bytes)
            if kind == "dictionary":
                self.resume_info.dictionary_frames.append((absolute_start, frame_bytes))
                continue
            # record batch
            batch_number = self.resume_info.next_batch
            self.resume_info.batch_starts[batch_number] = absolute_start
            self.resume_info.next_batch = batch_number + 1
            batch = self._decode_next()
            # The decoder stops exactly on the frame boundary and never seeks a
            # non-seekable stream, so consumed prefix bytes are safe to drop.
            self._window.compact()
            if batch_number <= self.confirmed_batch:
                # Consumed for dictionary consistency; must not be redelivered.
                continue
            self.resume_info.last_delivered_batch = batch_number
            yield batch_number, batch

        if not self._closed_clean:
            # The parser keeps the partial frame's start (or the offset where
            # the next message would have begun); a complete frame never
            # overwrites this with a boundary further on.
            self.resume_info.next_message_start = self._basis + self._parser.pending_start
            raise self.interruption()

    def __iter__(self):
        return self.iter_batches()

    def resume_position(self):
        """Message start to request from the service given the confirmed prefix.

        Returns the absolute byte offset of batch ``confirmed_batch + 1``, or
        ``None`` when no batch is confirmed (replay must begin at the stream
        start, where the service re-sends schema and the initial dictionary).
        """
        if self.confirmed_batch == 0:
            return None
        target = self.confirmed_batch + 1
        if target in self.resume_info.batch_starts:
            return self.resume_info.batch_starts[target]
        if target == self.resume_info.next_batch:
            return self.resume_info.next_message_start
        return None

    def interruption(self):
        """Build the :class:`ArrowIPCInterrupted` describing a broken stream."""
        # pending_start is the partial frame's start when the feed stopped
        # inside a message, otherwise the offset where the next message would
        # have begun (a transport close without the explicit EOS marker).
        message_position = self._basis + self._parser.pending_start
        return ArrowIPCInterrupted(
            message_position,
            self.resume_info.last_delivered_batch,
            self.resume_position(),
        )

    def export_resume_info(self):
        """Return the bookmark to hand to a new reader after reconnection."""
        return self.resume_info


def check_ipc_file_magic(path, storage_options=None):
    """Open *path* just far enough to verify the ``ARROW1`` file magic.

    Raises :class:`CorruptArrowIPCFile` without importing pyarrow, so a damaged
    archive can be reported even in minimal environments; the error never
    carries a stream resume position.
    """
    import fsspec

    with fsspec.open(path, "rb", **(storage_options or {})) as f:
        head = f.read(len(_ARROW_MAGIC))
    if head != _ARROW_MAGIC:
        raise CorruptArrowIPCFile(
            f"{path} does not start with the ARROW1 magic of an Arrow IPC file"
        )


def open_ipc_file(path, storage_options=None, **kwargs):
    """Open a seekable Arrow IPC *file*, raising on truncation/corruption.

    The ``ARROW1`` magic is checked up front so that a damaged archive is
    reported as :class:`CorruptArrowIPCFile` rather than silently falling back
    to stream parsing (which would invent resumable positions that do not
    exist for the file format).
    """
    import fsspec
    import pyarrow.ipc as ipc

    check_ipc_file_magic(path, storage_options)
    f = fsspec.open(path, "rb", **(storage_options or {})).open()
    try:
        # pyarrow retains the wrapped source for lazy batch reads itself.
        return ipc.open_file(f, **kwargs)
    except Exception as err:
        f.close()
        raise CorruptArrowIPCFile(f"Arrow IPC file {path} is corrupt: {err}") from err
