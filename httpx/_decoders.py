"""
Handlers for Content-Encoding.

See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding
"""

from __future__ import annotations

import codecs
import io
import typing
import zlib

from ._exceptions import DecodingError

# Brotli support is optional
try:
    # The C bindings in `brotli` are recommended for CPython.
    import brotli
except ImportError:  # pragma: no cover
    try:
        # The CFFI bindings in `brotlicffi` are recommended for PyPy
        # and other environments.
        import brotlicffi as brotli
    except ImportError:
        brotli = None


# Zstandard support is optional
try:
    import zstandard
except ImportError:  # pragma: no cover
    zstandard = None  # type: ignore


class DecoderStatus(typing.NamedTuple):
    """
    The outcome of one encoding layer, or one gzip member.

    `status` is one of "complete", "incomplete", "checksum", or "error".
    """

    encoding: str
    status: str
    member: int | None = None
    detail: str = ""

    @property
    def layer(self) -> str:
        if self.member is None:
            return self.encoding
        return f"{self.encoding} member {self.member}"

    @property
    def complete(self) -> bool:
        return self.status == "complete"

    @property
    def incomplete(self) -> bool:
        return self.status == "incomplete"

    @property
    def missing_tail(self) -> bool:
        return self.status == "incomplete"

    @property
    def checksum_failed(self) -> bool:
        return self.status == "checksum"

    @property
    def message(self) -> str:
        if self.status == "complete":
            return f"{self.layer} is complete"
        if self.status == "incomplete":
            return f"{self.layer} is incomplete: missing tail"
        if self.status == "checksum":
            message = f"{self.layer} checksum failed"
        else:
            message = f"{self.layer} decoding error"
        if self.detail:
            message = f"{message}: {self.detail}"
        return message

    def describe(self) -> str:
        return self.message


def _status_from_zlib_error(exc: zlib.error) -> str:
    message = str(exc)
    if "incorrect data check" in message or "incorrect length check" in message:
        return "checksum"
    return "error"


class ContentDecoder:
    def decode(self, data: bytes) -> bytes:
        raise NotImplementedError()  # pragma: no cover

    def flush(self) -> bytes:
        raise NotImplementedError()  # pragma: no cover

    def get_statuses(self) -> list[DecoderStatus]:
        return []

    def is_finalized(self) -> bool:
        return True

    def get_decoding_error(self) -> DecodingError | None:
        if not self.is_finalized():
            return None
        statuses = self.get_statuses()
        if not any(status.status != "complete" for status in statuses):
            return None
        message = "Content decoding failed: " + "; ".join(
            status.describe() for status in statuses
        )
        exc = DecodingError(message)
        exc.statuses = statuses
        return exc


class IdentityDecoder(ContentDecoder):
    """
    Handle unencoded data.
    """

    def decode(self, data: bytes) -> bytes:
        return data

    def flush(self) -> bytes:
        return b""


def _looks_like_zlib(data: bytes) -> bool:
    # A zlib stream starts with a CMF/FLG pair. The low nibble of CMF is
    # the compression method (8 for deflate), and the two-byte header is
    # zero modulo 31.
    if len(data) < 2:
        return False
    cmf, flg = data[0], data[1]
    return (cmf & 0x0F) == 8 and ((cmf << 8) | flg) % 31 == 0


class DeflateDecoder(ContentDecoder):
    """
    Handle 'deflate' decoding.

    See: https://stackoverflow.com/questions/1838699

    The zlib-wrapped form carries a four-byte Adler-32 trailer. We deliberately
    hold those four bytes back until the next decode call or `flush()`, so that
    a bad checksum cannot discard decompressed output that was produced from the
    same input block. A stream that ends without a complete trailer is reported
    as incomplete instead of being treated as a successful response.
    """

    ZLIB_TRAILER_SIZE = 4

    def __init__(self) -> None:
        self.wrapper: str | None = None
        self.decompressor: typing.Any = None
        self.pending = b""
        self.seen_data = False
        self.finished = False
        self.failed = False
        self.failure_status: str | None = None
        self.failure_detail = ""
        self._finalized = False

    def _start(self, data: bytes) -> None:
        if _looks_like_zlib(data):
            self.wrapper = "zlib"
            self.decompressor = zlib.decompressobj()
        else:
            self.wrapper = "raw"
            self.decompressor = zlib.decompressobj(-zlib.MAX_WBITS)

    def _feed(self, data: bytes) -> bytes:
        assert self.decompressor is not None
        try:
            output: bytes = self.decompressor.decompress(data)
        except zlib.error as exc:
            self.failed = True
            self.failure_status = _status_from_zlib_error(exc)
            self.failure_detail = str(exc)
            return b""

        if self.decompressor.eof:
            self.finished = True
        return output

    def decode(self, data: bytes) -> bytes:
        if not data:
            return b""

        self.seen_data = True
        self.pending += data

        if self.wrapper is None:
            if len(self.pending) < 2:
                return b""
            self._start(self.pending)

        if self.failed:
            self.pending = b""
            return b""

        # Feed the stream body first, then the last four bytes separately.
        # That way a bad Adler-32 value cannot throw away output that was
        # decompressed from the earlier part of the same input block.
        if self.wrapper == "zlib":
            chunk = self.pending[: -self.ZLIB_TRAILER_SIZE]
            trailer = self.pending[-self.ZLIB_TRAILER_SIZE :]
        else:
            chunk = self.pending
            trailer = b""
        self.pending = b""

        output = b""
        if chunk:
            output += self._feed(chunk)
        if trailer and not self.failed:
            output += self._feed(trailer)
        return output

    def flush(self) -> bytes:
        if not self.seen_data:
            return b""
        if self._finalized:
            return b""
        self._finalized = True

        output = b""
        if self.wrapper is None:
            # Compressed data too short to identify the wrapper. Treat it as
            # raw deflate and let zlib report the malformed stream.
            self._start(self.pending)
        assert self.decompressor is not None

        if self.pending and not self.failed:
            chunk = self.pending
            self.pending = b""
            output += self._feed(chunk)

        if not self.failed and not self.finished:
            try:
                output += self.decompressor.flush()
            except zlib.error as exc:  # pragma: no cover
                self.failed = True
                self.failure_status = _status_from_zlib_error(exc)
                self.failure_detail = str(exc)

        if self.decompressor.eof:
            self.finished = True
        return output

    def get_statuses(self) -> list[DecoderStatus]:
        if not self.seen_data:
            return []
        if self.failed:
            status = self.failure_status or "error"
            return [DecoderStatus("deflate", status, detail=self.failure_detail)]
        if self.finished:
            return [DecoderStatus("deflate", "complete")]
        return [DecoderStatus("deflate", "incomplete")]

    def is_finalized(self) -> bool:
        return self._finalized


class GZipDecoder(ContentDecoder):
    """
    Handle 'gzip' decoding.

    See: https://stackoverflow.com/questions/1838699

    A gzip payload may contain multiple consecutive members. Each member has
    its own eight-byte trailer, and each member is validated independently. As
    with deflate, we hold trailer bytes back so that output is still available
    if a checksum is bad, and we report a truncated final member as incomplete
    rather than silently ending the response.
    """

    TRAILER_SIZE = 8

    def __init__(self) -> None:
        self.decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
        self.pending = b""
        self.seen_data = False
        self.member = 1
        self.member_active = False
        self.failed = False
        self.finished = False
        self.statuses: list[DecoderStatus] = []
        self._finalized = False

    def _feed(self, data: bytes) -> bytes:
        output = b""
        while data and not self.failed:
            self.member_active = True
            try:
                output += self.decompressor.decompress(data)
            except zlib.error as exc:
                self.failed = True
                self.statuses.append(
                    DecoderStatus(
                        "gzip",
                        _status_from_zlib_error(exc),
                        self.member,
                        str(exc),
                    )
                )
                return output

            if not self.decompressor.eof:
                return output

            self.statuses.append(DecoderStatus("gzip", "complete", self.member))
            self.member_active = False
            leftover = self.decompressor.unused_data
            self.member += 1
            self.decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
            data = leftover
        return output

    def decode(self, data: bytes) -> bytes:
        if not data:
            return b""
        self.seen_data = True
        if self.failed:
            return b""
        self.pending += data

        # Feed the stream body without the final eight-byte gzip trailer, then
        # feed the trailer separately. A bad CRC/ISIZE then cannot discard the
        # output that was decompressed from the same input block.
        if len(self.pending) <= self.TRAILER_SIZE:
            return b""
        chunk = self.pending[: -self.TRAILER_SIZE]
        trailer = self.pending[-self.TRAILER_SIZE :]
        self.pending = b""

        output = self._feed(chunk)
        if trailer and not self.failed:
            output += self._feed(trailer)
        return output

    def flush(self) -> bytes:
        if not self.seen_data:
            return b""
        if self._finalized:
            return b""
        self._finalized = True

        output = b""
        if self.pending and not self.failed:
            chunk = self.pending
            self.pending = b""
            output += self._feed(chunk)

        if not self.failed:
            if self.decompressor.eof:
                self.finished = True
                if self.member_active:
                    self.statuses.append(DecoderStatus("gzip", "complete", self.member))
                    self.member_active = False
            elif self.member_active:
                self.statuses.append(DecoderStatus("gzip", "incomplete", self.member))
                self.member_active = False

        return output

    def get_statuses(self) -> list[DecoderStatus]:
        if not self.seen_data:
            return []
        return list(self.statuses)

    def is_finalized(self) -> bool:
        return self._finalized


class BrotliDecoder(ContentDecoder):
    """
    Handle 'brotli' decoding.

    Requires `pip install brotlipy`. See: https://brotlipy.readthedocs.io/
        or   `pip install brotli`. See https://github.com/google/brotli
    Supports both 'brotlipy' and 'Brotli' packages since they share an import
    name. The top branches are for 'brotlipy' and bottom branches for 'Brotli'
    """

    def __init__(self) -> None:
        if brotli is None:  # pragma: no cover
            raise ImportError(
                "Using 'BrotliDecoder', but neither of the 'brotlicffi' or 'brotli' "
                "packages have been installed. "
                "Make sure to install httpx using `pip install httpx[brotli]`."
            ) from None

        self.decompressor = brotli.Decompressor()
        self.seen_data = False
        self._decompress: typing.Callable[[bytes], bytes]
        if hasattr(self.decompressor, "decompress"):
            # The 'brotlicffi' package.
            self._decompress = self.decompressor.decompress  # pragma: no cover
        else:
            # The 'brotli' package.
            self._decompress = self.decompressor.process  # pragma: no cover

    def decode(self, data: bytes) -> bytes:
        if not data:
            return b""
        self.seen_data = True
        try:
            return self._decompress(data)
        except brotli.error as exc:
            raise DecodingError(str(exc)) from exc

    def flush(self) -> bytes:
        if not self.seen_data:
            return b""
        try:
            if hasattr(self.decompressor, "finish"):
                # Only available in the 'brotlicffi' package.

                # As the decompressor decompresses eagerly, this
                # will never actually emit any data. However, it will potentially throw
                # errors if a truncated or damaged data stream has been used.
                self.decompressor.finish()  # pragma: no cover
            return b""
        except brotli.error as exc:  # pragma: no cover
            raise DecodingError(str(exc)) from exc


class ZStandardDecoder(ContentDecoder):
    """
    Handle 'zstd' RFC 8878 decoding.

    Requires `pip install zstandard`.
    Can be installed as a dependency of httpx using `pip install httpx[zstd]`.
    """

    # inspired by the ZstdDecoder implementation in urllib3
    def __init__(self) -> None:
        if zstandard is None:  # pragma: no cover
            raise ImportError(
                "Using 'ZStandardDecoder', ..."
                "Make sure to install httpx using `pip install httpx[zstd]`."
            ) from None

        self.decompressor = zstandard.ZstdDecompressor().decompressobj()
        self.seen_data = False

    def decode(self, data: bytes) -> bytes:
        assert zstandard is not None
        self.seen_data = True
        output = io.BytesIO()
        try:
            output.write(self.decompressor.decompress(data))
            while self.decompressor.eof and self.decompressor.unused_data:
                unused_data = self.decompressor.unused_data
                self.decompressor = zstandard.ZstdDecompressor().decompressobj()
                output.write(self.decompressor.decompress(unused_data))
        except zstandard.ZstdError as exc:
            raise DecodingError(str(exc)) from exc
        return output.getvalue()

    def flush(self) -> bytes:
        if not self.seen_data:
            return b""
        ret = self.decompressor.flush()  # note: this is a no-op
        if not self.decompressor.eof:
            raise DecodingError("Zstandard data is incomplete")  # pragma: no cover
        return bytes(ret)


class MultiDecoder(ContentDecoder):
    """
    Handle the case where multiple encodings have been applied.
    """

    def __init__(self, children: typing.Sequence[ContentDecoder]) -> None:
        """
        'children' should be a sequence of decoders in the order in which
        each was applied.
        """
        # Note that we reverse the order for decoding.
        self.children = list(reversed(children))

    def decode(self, data: bytes) -> bytes:
        for child in self.children:
            data = child.decode(data)
        return data

    def flush(self) -> bytes:
        data = b""
        for child in self.children:
            data = child.decode(data) + child.flush()
        return data

    def get_statuses(self) -> list[DecoderStatus]:
        statuses: list[DecoderStatus] = []
        for child in self.children:
            statuses.extend(child.get_statuses())
        return statuses

    def is_finalized(self) -> bool:
        return all(child.is_finalized() for child in self.children)


class ByteChunker:
    """
    Handles returning byte content in fixed-size chunks.
    """

    def __init__(self, chunk_size: int | None = None) -> None:
        self._buffer = io.BytesIO()
        self._chunk_size = chunk_size

    def decode(self, content: bytes) -> list[bytes]:
        if self._chunk_size is None:
            return [content] if content else []

        self._buffer.write(content)
        if self._buffer.tell() >= self._chunk_size:
            value = self._buffer.getvalue()
            chunks = [
                value[i : i + self._chunk_size]
                for i in range(0, len(value), self._chunk_size)
            ]
            if len(chunks[-1]) == self._chunk_size:
                self._buffer.seek(0)
                self._buffer.truncate()
                return chunks
            else:
                self._buffer.seek(0)
                self._buffer.write(chunks[-1])
                self._buffer.truncate()
                return chunks[:-1]
        else:
            return []

    def flush(self) -> list[bytes]:
        value = self._buffer.getvalue()
        self._buffer.seek(0)
        self._buffer.truncate()
        return [value] if value else []


class TextChunker:
    """
    Handles returning text content in fixed-size chunks.
    """

    def __init__(self, chunk_size: int | None = None) -> None:
        self._buffer = io.StringIO()
        self._chunk_size = chunk_size

    def decode(self, content: str) -> list[str]:
        if self._chunk_size is None:
            return [content] if content else []

        self._buffer.write(content)
        if self._buffer.tell() >= self._chunk_size:
            value = self._buffer.getvalue()
            chunks = [
                value[i : i + self._chunk_size]
                for i in range(0, len(value), self._chunk_size)
            ]
            if len(chunks[-1]) == self._chunk_size:
                self._buffer.seek(0)
                self._buffer.truncate()
                return chunks
            else:
                self._buffer.seek(0)
                self._buffer.write(chunks[-1])
                self._buffer.truncate()
                return chunks[:-1]
        else:
            return []

    def flush(self) -> list[str]:
        value = self._buffer.getvalue()
        self._buffer.seek(0)
        self._buffer.truncate()
        return [value] if value else []


class TextDecoder:
    """
    Handles incrementally decoding bytes into text
    """

    def __init__(self, encoding: str = "utf-8") -> None:
        self.decoder = codecs.getincrementaldecoder(encoding)(errors="replace")

    def decode(self, data: bytes) -> str:
        return self.decoder.decode(data)

    def flush(self) -> str:
        return self.decoder.decode(b"", True)


class LineDecoder:
    """
    Handles incrementally reading lines from text.

    Has the same behaviour as the stdllib splitlines,
    but handling the input iteratively.
    """

    def __init__(self) -> None:
        self.buffer: list[str] = []
        self.trailing_cr: bool = False

    def decode(self, text: str) -> list[str]:
        # See https://docs.python.org/3/library/stdtypes.html#str.splitlines
        NEWLINE_CHARS = "\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029"

        # We always push a trailing `\r` into the next decode iteration.
        if self.trailing_cr:
            text = "\r" + text
            self.trailing_cr = False
        if text.endswith("\r"):
            self.trailing_cr = True
            text = text[:-1]

        if not text:
            # NOTE: the edge case input of empty text doesn't occur in practice,
            # because other httpx internals filter out this value
            return []  # pragma: no cover

        trailing_newline = text[-1] in NEWLINE_CHARS
        lines = text.splitlines()

        if len(lines) == 1 and not trailing_newline:
            # No new lines, buffer the input and continue.
            self.buffer.append(lines[0])
            return []

        if self.buffer:
            # Include any existing buffer in the first portion of the
            # splitlines result.
            lines = ["".join(self.buffer) + lines[0]] + lines[1:]
            self.buffer = []

        if not trailing_newline:
            # If the last segment of splitlines is not newline terminated,
            # then drop it from our output and start a new buffer.
            self.buffer = [lines.pop()]

        return lines

    def flush(self) -> list[str]:
        if not self.buffer and not self.trailing_cr:
            return []

        lines = ["".join(self.buffer)]
        self.buffer = []
        self.trailing_cr = False
        return lines


SUPPORTED_DECODERS = {
    "identity": IdentityDecoder,
    "gzip": GZipDecoder,
    "deflate": DeflateDecoder,
    "br": BrotliDecoder,
    "zstd": ZStandardDecoder,
}


if brotli is None:
    SUPPORTED_DECODERS.pop("br")  # pragma: no cover
if zstandard is None:
    SUPPORTED_DECODERS.pop("zstd")  # pragma: no cover
