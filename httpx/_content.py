from __future__ import annotations

import codecs
import inspect
import unicodedata
import warnings
from json import dumps as json_dumps
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Iterable,
    Iterator,
    Mapping,
)
from urllib.parse import quote_plus, urlencode

from ._exceptions import (
    FormValueLimitExceeded,
    StreamClosed,
    StreamConsumed,
)
from ._multipart import MultipartStream
from ._types import (
    AsyncByteStream,
    FormValueChunk,
    FormValueSource,
    RequestContent,
    RequestData,
    RequestFiles,
    ResponseContent,
    StreamingFormData,
    SyncByteStream,
)
from ._utils import peek_filelike_length, primitive_value_to_str

__all__ = [
    "AsyncURLencodedFormStream",
    "ByteStream",
    "URLencodedFormStream",
    "encode_streaming_urlencoded_data",
]

_FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"


class ByteStream(AsyncByteStream, SyncByteStream):
    def __init__(self, stream: bytes) -> None:
        self._stream = stream

    def __iter__(self) -> Iterator[bytes]:
        yield self._stream

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._stream


class IteratorByteStream(SyncByteStream):
    CHUNK_SIZE = 65_536

    def __init__(self, stream: Iterable[bytes]) -> None:
        self._stream = stream
        self._is_stream_consumed = False
        self._is_generator = inspect.isgenerator(stream)

    def __iter__(self) -> Iterator[bytes]:
        if self._is_stream_consumed and self._is_generator:
            raise StreamConsumed()

        self._is_stream_consumed = True
        if hasattr(self._stream, "read"):
            # File-like interfaces should use 'read' directly.
            chunk = self._stream.read(self.CHUNK_SIZE)
            while chunk:
                yield chunk
                chunk = self._stream.read(self.CHUNK_SIZE)
        else:
            # Otherwise iterate.
            for part in self._stream:
                yield part


class AsyncIteratorByteStream(AsyncByteStream):
    CHUNK_SIZE = 65_536

    def __init__(self, stream: AsyncIterable[bytes]) -> None:
        self._stream = stream
        self._is_stream_consumed = False
        self._is_generator = inspect.isasyncgen(stream)

    async def __aiter__(self) -> AsyncIterator[bytes]:
        if self._is_stream_consumed and self._is_generator:
            raise StreamConsumed()

        self._is_stream_consumed = True
        if hasattr(self._stream, "aread"):
            # File-like interfaces should use 'aread' directly.
            chunk = await self._stream.aread(self.CHUNK_SIZE)
            while chunk:
                yield chunk
                chunk = await self._stream.aread(self.CHUNK_SIZE)
        else:
            # Otherwise iterate.
            async for part in self._stream:
                yield part


class UnattachedStream(AsyncByteStream, SyncByteStream):
    """
    If a request or response is serialized using pickle, then it is no longer
    attached to a stream for I/O purposes. Any stream operations should result
    in `httpx.StreamClosed`.
    """

    def __iter__(self) -> Iterator[bytes]:
        raise StreamClosed()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        raise StreamClosed()
        yield b""  # pragma: no cover


def encode_content(
    content: str | bytes | Iterable[bytes] | AsyncIterable[bytes],
) -> tuple[dict[str, str], SyncByteStream | AsyncByteStream]:
    if isinstance(content, (bytes, str)):
        body = content.encode("utf-8") if isinstance(content, str) else content
        content_length = len(body)
        headers = {"Content-Length": str(content_length)} if body else {}
        return headers, ByteStream(body)

    elif isinstance(content, Iterable) and not isinstance(content, dict):
        # `not isinstance(content, dict)` is a bit oddly specific, but it
        # catches a case that's easy for users to make in error, and would
        # otherwise pass through here, like any other bytes-iterable,
        # because `dict` happens to be iterable. See issue #2491.
        content_length_or_none = peek_filelike_length(content)

        if content_length_or_none is None:
            headers = {"Transfer-Encoding": "chunked"}
        else:
            headers = {"Content-Length": str(content_length_or_none)}
        return headers, IteratorByteStream(content)  # type: ignore

    elif isinstance(content, AsyncIterable):
        headers = {"Transfer-Encoding": "chunked"}
        return headers, AsyncIteratorByteStream(content)

    raise TypeError(f"Unexpected type for 'content', {type(content)!r}")


def encode_urlencoded_data(
    data: RequestData,
) -> tuple[dict[str, str], ByteStream]:
    plain_data = []
    for key, value in data.items():
        if isinstance(value, (list, tuple)):
            plain_data.extend([(key, primitive_value_to_str(item)) for item in value])
        else:
            plain_data.append((key, primitive_value_to_str(value)))
    body = urlencode(plain_data, doseq=True).encode("utf-8")
    content_length = str(len(body))
    content_type = "application/x-www-form-urlencoded"
    headers = {"Content-Length": content_length, "Content-Type": content_type}
    return headers, ByteStream(body)


class _FormValueNormalizer:
    """
    Incrementally applies Unicode NFC and CRLF newline normalization to the
    chunked text source of a single form field.

    Only the current combining sequence is retained between chunks, so the
    full field value is never buffered. The retained sequence is bounded by
    the caller-supplied limit on its length.
    """

    def __init__(self, field_name: str, combining_limit: int) -> None:
        self._field_name = field_name
        self._combining_limit = combining_limit
        self._ready = ""
        self._base = ""
        self._marks: list[str] = []
        self._combining_count = 0
        self._pending_cr = False

    def feed(self, text: str) -> str:
        for char in text:
            if self._pending_cr:
                self._pending_cr = False
                self._accept_line_break()
                if char == "\n":
                    # The LF completes an already-counted CRLF pair; drop it
                    # so the pair collapses to a single CRLF.
                    continue

            if char == "\r":
                self._pending_cr = True
            elif char == "\n":
                self._accept_line_break()
            else:
                self._accept_char(char)

        ready = self._ready
        self._ready = ""
        return ready

    def finish(self) -> str:
        if self._pending_cr:
            self._pending_cr = False
            self._accept_line_break()
        self._flush_combining()
        ready = self._ready
        self._ready = ""
        return ready

    def _accept_line_break(self) -> None:
        # Line breaks are starters, so they close any open combining sequence.
        self._flush_combining()
        self._ready += "\r\n"

    def _accept_char(self, char: str) -> None:
        if unicodedata.category(char).startswith("M"):
            # Marks are kept in the open run even when there is no preceding
            # base, e.g. when a field begins with a combining mark.
            self._marks.append(char)
            self._combining_count += 1
            if self._combining_count > self._combining_limit:
                raise FormValueLimitExceeded(
                    "Form field "
                    f"{self._field_name!r} contains a combining sequence of "
                    f"{self._combining_count} marks, exceeding the limit of "
                    f"{self._combining_limit}.",
                    field_name=self._field_name,
                    limit=self._combining_limit,
                )
        else:
            self._flush_combining()
            self._base = char

    def _flush_combining(self) -> None:
        if self._base or self._marks:
            sequence = self._base + "".join(self._marks)
            # Normalizing each run independently is equivalent to normalizing
            # the whole value, because NFC boundaries occur at starter chars.
            self._ready += unicodedata.normalize("NFC", sequence)
        self._base = ""
        self._marks = []
        self._combining_count = 0


def _is_async_source(source: FormValueSource) -> bool:
    if isinstance(source, (str, bytes)):
        return False
    return isinstance(source, AsyncIterable) or hasattr(source, "aread")


def _normalize_form_name(name: str) -> str:
    # Field names are short and known up front, so they are normalized whole,
    # using the same NFC + CRLF contract as the field values.
    collapsed = name.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\r\n")
    return unicodedata.normalize("NFC", collapsed)


def _decode_form_chunk(
    decoder: codecs.IncrementalDecoder, chunk: FormValueChunk
) -> str:
    if isinstance(chunk, bytes):
        # Incremental decoding keeps a multi-byte character split across
        # chunks intact instead of failing on each chunk individually.
        return decoder.decode(chunk)
    return decoder.decode(b"") + chunk


def _validate_form_fields(
    fields: StreamingFormData,
) -> list[tuple[str, FormValueSource]]:
    plain_fields = list(fields)
    for field in plain_fields:
        if not isinstance(field, tuple) or len(field) != 2:
            raise TypeError(
                "Streaming form fields must be (name, value) two-tuples."
            )
        name, source = field
        if not isinstance(name, str):
            raise TypeError(
                f"Invalid type for form field name. Expected str, got "
                f"{type(name)}: {name!r}"
            )
        if not isinstance(source, (str, bytes, Iterable, AsyncIterable)) and not (
            hasattr(source, "read") or hasattr(source, "aread")
        ):
            raise TypeError(
                "Invalid type for streaming form field value. Expected str, "
                "bytes, a text iterable, or a file-like object, got "
                f"{type(source)}."
            )
    return plain_fields


class URLencodedFormStream(SyncByteStream):
    """
    Streaming `application/x-www-form-urlencoded` request content.

    Each field value is pulled from its chunked text source one field at a
    time. Unicode NFC and CRLF newline normalization are applied to a value
    before it is percent-encoded, so neither a whole field value nor the
    whole form body ever needs to be held in memory.

    The encoded length is not known up front, so the stream is always sent
    with `Transfer-Encoding: chunked`.
    """

    content_type = _FORM_CONTENT_TYPE

    def __init__(
        self, fields: list[tuple[str, FormValueSource]], combining_limit: int
    ) -> None:
        self.fields = fields
        self.combining_limit = combining_limit

    def get_headers(self) -> dict[str, str]:
        return {
            "Transfer-Encoding": "chunked",
            "Content-Type": self.content_type,
        }

    def iter_chunks(self) -> Iterator[bytes]:
        for index, (name, source) in enumerate(self.fields):
            if index:
                yield b"&"
            yield quote_plus(_normalize_form_name(name)).encode("utf-8") + b"="
            normalizer = _FormValueNormalizer(name, self.combining_limit)
            decoder = codecs.getincrementaldecoder("utf-8")()
            for chunk in self.iter_source_chunks(source):
                ready = normalizer.feed(_decode_form_chunk(decoder, chunk))
                if ready:
                    yield quote_plus(ready).encode("utf-8")
            tail = normalizer.feed(decoder.decode(b"", final=True))
            tail += normalizer.finish()
            if tail:
                yield quote_plus(tail).encode("utf-8")

    @staticmethod
    def iter_source_chunks(
        source: FormValueSource,
    ) -> Iterator[FormValueChunk]:
        if isinstance(source, (str, bytes)):
            if source:
                yield source
            return
        if hasattr(source, "read"):
            chunk = source.read(IteratorByteStream.CHUNK_SIZE)
            while chunk:
                yield chunk
                chunk = source.read(IteratorByteStream.CHUNK_SIZE)
            return
        for chunk in source:  # type: ignore[union-attr]
            if not isinstance(chunk, (str, bytes)):
                raise TypeError(
                    "Streaming form field chunks must be str or bytes, "
                    f"got {type(chunk)}."
                )
            yield chunk

    def __iter__(self) -> Iterator[bytes]:
        yield from self.iter_chunks()


class AsyncURLencodedFormStream(AsyncByteStream):
    """
    Async counterpart of `URLencodedFormStream`, for field values backed by
    async chunk iterables or async file-like objects.
    """

    content_type = _FORM_CONTENT_TYPE

    def __init__(
        self, fields: list[tuple[str, FormValueSource]], combining_limit: int
    ) -> None:
        self.fields = fields
        self.combining_limit = combining_limit

    def get_headers(self) -> dict[str, str]:
        return {
            "Transfer-Encoding": "chunked",
            "Content-Type": self.content_type,
        }

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        for index, (name, source) in enumerate(self.fields):
            if index:
                yield b"&"
            yield quote_plus(_normalize_form_name(name)).encode("utf-8") + b"="
            normalizer = _FormValueNormalizer(name, self.combining_limit)
            decoder = codecs.getincrementaldecoder("utf-8")()
            async for chunk in self.iter_source_chunks(source):
                ready = normalizer.feed(_decode_form_chunk(decoder, chunk))
                if ready:
                    yield quote_plus(ready).encode("utf-8")
            tail = normalizer.feed(decoder.decode(b"", final=True))
            tail += normalizer.finish()
            if tail:
                yield quote_plus(tail).encode("utf-8")

    @staticmethod
    async def iter_source_chunks(
        source: FormValueSource,
    ) -> AsyncIterator[FormValueChunk]:
        if isinstance(source, (str, bytes)):
            if source:
                yield source
            return
        if hasattr(source, "aread"):
            chunk = await source.aread(AsyncIteratorByteStream.CHUNK_SIZE)
            while chunk:
                yield chunk
                chunk = await source.aread(AsyncIteratorByteStream.CHUNK_SIZE)
            return
        if hasattr(source, "read"):
            chunk = source.read(AsyncIteratorByteStream.CHUNK_SIZE)
            while chunk:
                yield chunk
                chunk = source.read(AsyncIteratorByteStream.CHUNK_SIZE)
            return
        async for chunk in source:  # type: ignore[union-attr]
            if not isinstance(chunk, (str, bytes)):
                raise TypeError(
                    "Streaming form field chunks must be str or bytes, "
                    f"got {type(chunk)}."
                )
            yield chunk

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self.iter_chunks():
            yield chunk


def encode_streaming_urlencoded_data(
    fields: StreamingFormData,
    *,
    combining_limit: int,
) -> tuple[dict[str, str], SyncByteStream | AsyncByteStream]:
    """
    Build a one-shot streaming urlencoded form from an ordered sequence of
    `(field name, chunked text source)` pairs.

    `combining_limit` is the maximum number of consecutive combining marks
    allowed within a single field value. A field exceeding it raises
    `FormValueLimitExceeded` while the body is being streamed, by which point
    earlier fields may already have been sent.
    """
    if not isinstance(combining_limit, int) or isinstance(
        combining_limit, bool
    ) or combining_limit < 0:
        raise ValueError("'combining_limit' must be a non-negative integer.")

    plain_fields = _validate_form_fields(fields)
    if any(_is_async_source(source) for _, source in plain_fields):
        async_stream = AsyncURLencodedFormStream(
            plain_fields, combining_limit=combining_limit
        )
        return async_stream.get_headers(), async_stream

    sync_stream = URLencodedFormStream(
        plain_fields, combining_limit=combining_limit
    )
    return sync_stream.get_headers(), sync_stream


def encode_multipart_data(
    data: RequestData, files: RequestFiles, boundary: bytes | None
) -> tuple[dict[str, str], MultipartStream]:
    multipart = MultipartStream(data=data, files=files, boundary=boundary)
    headers = multipart.get_headers()
    return headers, multipart


def encode_text(text: str) -> tuple[dict[str, str], ByteStream]:
    body = text.encode("utf-8")
    content_length = str(len(body))
    content_type = "text/plain; charset=utf-8"
    headers = {"Content-Length": content_length, "Content-Type": content_type}
    return headers, ByteStream(body)


def encode_html(html: str) -> tuple[dict[str, str], ByteStream]:
    body = html.encode("utf-8")
    content_length = str(len(body))
    content_type = "text/html; charset=utf-8"
    headers = {"Content-Length": content_length, "Content-Type": content_type}
    return headers, ByteStream(body)


def encode_json(json: Any) -> tuple[dict[str, str], ByteStream]:
    body = json_dumps(
        json, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    content_length = str(len(body))
    content_type = "application/json"
    headers = {"Content-Length": content_length, "Content-Type": content_type}
    return headers, ByteStream(body)


def encode_request(
    content: RequestContent | None = None,
    data: RequestData | None = None,
    files: RequestFiles | None = None,
    json: Any | None = None,
    boundary: bytes | None = None,
) -> tuple[dict[str, str], SyncByteStream | AsyncByteStream]:
    """
    Handles encoding the given `content`, `data`, `files`, and `json`,
    returning a two-tuple of (<headers>, <stream>).
    """
    if data is not None and not isinstance(data, Mapping):
        # We prefer to separate `content=<bytes|str|byte iterator|bytes aiterator>`
        # for raw request content, and `data=<form data>` for url encoded or
        # multipart form content.
        #
        # However for compat with requests, we *do* still support
        # `data=<bytes...>` usages. We deal with that case here, treating it
        # as if `content=<...>` had been supplied instead.
        message = "Use 'content=<...>' to upload raw bytes/text content."
        warnings.warn(message, DeprecationWarning, stacklevel=2)
        return encode_content(data)

    if content is not None:
        return encode_content(content)
    elif files:
        return encode_multipart_data(data or {}, files, boundary)
    elif data:
        return encode_urlencoded_data(data)
    elif json is not None:
        return encode_json(json)

    return {}, ByteStream(b"")


def encode_response(
    content: ResponseContent | None = None,
    text: str | None = None,
    html: str | None = None,
    json: Any | None = None,
) -> tuple[dict[str, str], SyncByteStream | AsyncByteStream]:
    """
    Handles encoding the given `content`, returning a two-tuple of
    (<headers>, <stream>).
    """
    if content is not None:
        return encode_content(content)
    elif text is not None:
        return encode_text(text)
    elif html is not None:
        return encode_html(html)
    elif json is not None:
        return encode_json(json)

    return {}, ByteStream(b"")
