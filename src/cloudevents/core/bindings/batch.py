#  Copyright 2018-Present The CloudEvents Authors
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Streaming batch reader for HTTP CloudEvent responses.

The warehouse collector hands a batch of HTTP responses to the SDK in one call.
Response bodies are one-shot byte streams that may be hundreds of megabytes, so
structured envelopes are parsed incrementally without buffering the whole body.

Routes describe how an envelope with a registered outer media type is decoded.
Two routes may share an outer media type during migration:

* a generic route accepts the regular CloudEvents versions and payload
  representations of that envelope;
* a specialized route (e.g. the warehouse route) adds conditions on the
  envelope version, the declared ``datacontenttype`` and the payload
  representation. A more specific route wins; equally specific routes produce
  an ambiguity result and neither is executed.
"""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, field
from json import dumps, loads
from tempfile import SpooledTemporaryFile
from typing import Any, Callable, Final, Iterable, Protocol

from cloudevents.core.bindings.common import (
    CONTENT_TYPE_HEADER,
    DATACONTENTTYPE_ATTR,
)
from cloudevents.core.bindings.http import CE_PREFIX, _decode_header_value
from cloudevents.core.formats.json import JSONFormat
from cloudevents.core.spec import SPECVERSION_V0_3

STRUCTURED_MEDIA_TYPE: Final[str] = "application/cloudevents+json"
WAREHOUSE_MEDIA_TYPE: Final[str] = (
    "application/cloudevents+json;profile=warehouse"
)
WAREHOUSE_DATACONTENTTYPE: Final[str] = "application/vnd.wms+json;rev=1"
BASE64_ENCODING: Final[str] = "base64"

_DEFAULT_CHUNK_SIZE: Final[int] = 64 * 1024


class BatchReadError(Exception):
    """Raised when a single batch response cannot be fully read or validated."""


class AmbiguousRouteError(BatchReadError):
    """
    Raised when two equally specific registered routes match the same response.

    The matching routes and their reasons are preserved on the exception so
    callers can inspect the conflict without executing either route.
    """

    def __init__(self, matches: list["BatchRouteMatch"]) -> None:
        self.matches: list[BatchRouteMatch] = list(matches)
        names = ", ".join(match.route_name for match in matches)
        super().__init__(
            f"Equally specific routes match the response: {names}; "
            "neither route was executed"
        )


class BusinessDecoder(Protocol):
    """Decoder turning fully validated raw payload bytes into business data."""

    def __call__(self, payload: bytes) -> Any: ...


@dataclass(frozen=True)
class BatchRouteRegistration:
    """
    An immutable snapshot of a registered batch route.

    Attributes:
        name: Registration name returned with read results.
        media_type: Outer envelope media type the route accepts.
        version: Registration version, captured in snapshots and results.
        generic: When ``True`` the route accepts regular CloudEvents versions
            and payload representations; when ``False`` the extra warehouse
            conditions apply.
        required_specversion: Envelope ``specversion`` required by a
            specialized route.
        required_datacontenttype: Envelope ``datacontenttype`` required by a
            specialized route.
        require_base64: Whether the v0.3 base64 payload representation is
            required by a specialized route.
        decoder: Optional business decoder invoked only after the whole
            response has been read and validated.
    """

    name: str
    media_type: str
    version: int
    generic: bool = True
    required_specversion: str | None = None
    required_datacontenttype: str | None = None
    require_base64: bool = False
    decoder: Callable[[bytes], Any] | None = None


@dataclass(frozen=True)
class BatchRouteMatch:
    """A route that matched a response together with the matching evidence."""

    route_name: str
    route_version: int
    generic: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class BatchReadResult:
    """
    The outcome of reading one response stream exactly once.

    Attributes:
        success: Whether the full response was read and validated.
        attributes: CloudEvent attributes carried by the response. Residual
            ``ce-*`` headers of structured responses never appear here.
        envelope: The selected envelope representation, or ``None`` when the
            mode could not be determined.
        payload_route: How the body was decoded (route name or content mode).
        data: Payload in its regular CloudEvents representation, or the raw
            bytes handed to a specialized decoder.
        business_data: Output of the route's business decoder, if any.
        route_version: Version of the registration that was executed.
        matches: Routes that matched before specificity resolution.
        error: Failure reason for unsuccessful responses.
    """

    success: bool
    attributes: dict[str, Any] = field(default_factory=dict)
    envelope: str | None = None
    payload_route: str | None = None
    data: Any = None
    business_data: Any = None
    route_version: int | None = None
    matches: tuple[BatchRouteMatch, ...] = ()
    error: str | None = None


class BatchRouteRegistry:
    """
    Registry of batch decoding routes.

    Registration is name based. Replacing a route keeps its name and bumps the
    version. A snapshot freezes the registrations visible to a batch so a route
    swap while a batch is still streaming cannot change in-flight decoding.
    """

    def __init__(self) -> None:
        self._routes: list[BatchRouteRegistration] = []
        self._versions: dict[str, int] = {}

    def register_generic(
        self,
        name: str,
        media_type: str = WAREHOUSE_MEDIA_TYPE,
        decoder: Callable[[bytes], Any] | None = None,
    ) -> BatchRouteRegistration:
        """
        Register a generic route accepting regular CloudEvents versions and
        payload representations for the given structured envelope media type.
        """
        return self._register(
            name=name,
            media_type=media_type,
            generic=True,
            required_specversion=None,
            required_datacontenttype=None,
            require_base64=False,
            decoder=decoder,
        )

    def register_warehouse(
        self,
        name: str,
        decoder: Callable[[bytes], Any],
        media_type: str = WAREHOUSE_MEDIA_TYPE,
    ) -> BatchRouteRegistration:
        """
        Register the warehouse route.

        The route only matches envelopes declaring specversion ``0.3``,
        ``datacontenttype`` of ``application/vnd.wms+json;rev=1`` and a v0.3
        base64 payload representation.
        """
        return self._register(
            name=name,
            media_type=media_type,
            generic=False,
            required_specversion=SPECVERSION_V0_3,
            required_datacontenttype=WAREHOUSE_DATACONTENTTYPE,
            require_base64=True,
            decoder=decoder,
        )

    def _register(
        self,
        name: str,
        media_type: str,
        generic: bool,
        required_specversion: str | None,
        required_datacontenttype: str | None,
        require_base64: bool,
        decoder: Callable[[bytes], Any] | None,
    ) -> BatchRouteRegistration:
        version = self._versions.get(name, 0) + 1
        self._versions[name] = version
        registration = BatchRouteRegistration(
            name=name,
            media_type=normalize_media_type(media_type),
            version=version,
            generic=generic,
            required_specversion=required_specversion,
            required_datacontenttype=required_datacontenttype,
            require_base64=require_base64,
            decoder=decoder,
        )
        self._routes = [route for route in self._routes if route.name != name]
        self._routes.append(registration)
        return registration

    def registration_version(self, name: str) -> int | None:
        """Return the currently registered version of ``name``."""
        return self._versions.get(name)

    def registration(self, name: str) -> BatchRouteRegistration | None:
        """Return the current registration with ``name``."""
        for route in self._routes:
            if route.name == name:
                return route
        return None

    def routes_for(self, media_type: str) -> list[BatchRouteRegistration]:
        """Return all registrations accepting ``media_type`` in registration order."""
        return [
            route
            for route in self._routes
            if media_type_matches(media_type, route.media_type)
        ]

    def snapshot(self) -> "BatchRouteRegistry":
        """Return an isolated copy of the registry as visible right now."""
        frozen = BatchRouteRegistry()
        frozen._routes = list(self._routes)
        frozen._versions = dict(self._versions)
        return frozen


def normalize_media_type(value: str) -> str:
    """Lower-case a media type and strip surrounding whitespace."""
    return value.strip().lower()


def _parse_media_type(value: str) -> tuple[str, frozenset[str]]:
    parts = [part.strip().lower() for part in value.split(";") if part.strip()]
    if not parts:
        return "", frozenset()
    return parts[0], frozenset(parts[1:])


def media_type_matches(actual: str, expected: str) -> bool:
    """
    Compare two media types ignoring parameter order.

    All parameters declared by ``expected`` must be present on ``actual`` with
    the same values; additional parameters on ``actual`` are accepted.
    """
    actual_type, actual_params = _parse_media_type(actual)
    expected_type, expected_params = _parse_media_type(expected)
    return actual_type == expected_type and expected_params <= actual_params


def _media_type_without_parameters(content_type: str) -> str:
    return content_type.split(";", 1)[0].strip().lower()


class _ChunkReader:
    """Yields fixed-size chunks from a one-shot stream until exhaustion."""

    def __init__(self, stream: Any, chunk_size: int) -> None:
        self._stream = stream
        self._chunk_size = chunk_size
        self._eof = False

    def __iter__(self) -> "_ChunkReader":
        return self

    def __next__(self) -> bytes:
        if self._eof:
            raise StopIteration
        chunk = self._stream.read(self._chunk_size)
        if not chunk:
            self._eof = True
            raise StopIteration
        return chunk


class ByteAccumulator:
    """
    Collects bytes with bounded working memory.

    Small payloads stay in memory; payloads crossing the spill threshold are
    written to a spooled temporary file. Working memory therefore does not grow
    with the payload size while streaming, and the complete bytes can still be
    delivered once reading and validation have finished.
    """

    def __init__(self, spill_size: int = _DEFAULT_CHUNK_SIZE) -> None:
        self._memory = bytearray()
        self._file: SpooledTemporaryFile[bytes] | None = None
        self._spill_size = spill_size
        self._length = 0

    def add(self, chunk: bytes) -> None:
        if not chunk:
            return
        self._length += len(chunk)
        if self._file is not None:
            self._file.write(chunk)
            return
        self._memory.extend(chunk)
        if len(self._memory) > self._spill_size:
            spooled = SpooledTemporaryFile(max_size=self._spill_size, mode="w+b")
            spooled.write(bytes(self._memory))
            self._file = spooled
            self._memory.clear()

    def getvalue(self) -> bytes:
        if self._file is not None:
            self._file.seek(0)
            return self._file.read()
        return bytes(self._memory)

    def iter_chunks(self, chunk_size: int = _DEFAULT_CHUNK_SIZE) -> Iterable[bytes]:
        if self._file is None:
            yield bytes(self._memory)
            return
        self._file.seek(0)
        while True:
            block = self._file.read(chunk_size)
            if not block:
                return
            yield block

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
        self._memory.clear()

    def __len__(self) -> int:
        return self._length


class _Base64Pipe:
    """
    Incremental base64 decoder with bounded working memory.

    At most four encoded bytes (one pending quantum plus output buffering) are
    held regardless of payload size. Decoded bytes are forwarded to a sink.
    """

    def __init__(self, sink: ByteAccumulator) -> None:
        self._sink = sink
        self._pending = bytearray()

    def feed(self, chunk: bytes) -> None:
        self._pending.extend(chunk)
        usable = len(self._pending) - len(self._pending) % 4
        if usable:
            block = bytes(self._pending[:usable])
            del self._pending[:usable]
            try:
                self._sink.add(base64.b64decode(block))
            except (binascii.Error, ValueError) as exc:
                raise BatchReadError(f"Invalid base64 payload: {exc}") from exc

    def finish(self) -> None:
        if self._pending:
            try:
                self._sink.add(base64.b64decode(bytes(self._pending)))
            except (binascii.Error, ValueError) as exc:
                raise BatchReadError(f"Invalid base64 payload: {exc}") from exc
            self._pending.clear()


class _StructuredEnvelopeParser:
    """
    Streaming parser for a single JSON structured envelope.

    The parser only keeps the current token, a small tail of undecoded bytes
    and the payload accumulators. Envelope fields may be split across arbitrary
    read boundaries and ``data`` may appear before the identifying attributes.
    """

    def __init__(self, reader: _ChunkReader) -> None:
        self._reader = reader
        self._buffer = bytearray()
        self._index = 0
        self._eof = False
        self.attributes: dict[str, Any] = {}
        self.data_present = False
        self.data_is_string = False
        self.data_is_base64_field = False
        self._data_value: Any = None
        self._data_string: ByteAccumulator | None = None
        self._decoded_payload: ByteAccumulator | None = None

    def parse(self) -> None:
        self._consume_whitespace()
        self._expect_char(ord("{"))
        while True:
            self._consume_whitespace()
            if self._peek() == ord("}"):
                self._advance()
                return
            key = self._read_string()
            self._consume_whitespace()
            self._expect_char(ord(":"))
            self._consume_whitespace()
            if key == "data":
                self._read_data_value()
            elif key == "data_base64":
                self.data_present = True
                self.data_is_base64_field = True
                self._decoded_payload = ByteAccumulator()
                self._read_string_into(
                    self._decoded_payload, _Base64Pipe(self._decoded_payload)
                )
            else:
                self.attributes[key] = self._read_value()
            self._consume_whitespace()
            separator = self._peek()
            if separator == ord(","):
                self._advance()
                continue
            if separator == ord("}"):
                self._advance()
                return
            raise BatchReadError(
                "Malformed structured envelope: expected ',' or '}'"
            )

    def string_data_value(self) -> str:
        """Return the decoded value of a ``data`` JSON string."""
        assert self._data_string is not None
        return self._data_string.getvalue().decode("utf-8")

    def decoded_payload_bytes(self) -> bytes:
        """Return raw payload bytes of a ``data_base64`` field."""
        assert self._decoded_payload is not None
        return self._decoded_payload.getvalue()

    def decode_base64_string_data(self) -> bytes:
        """
        Decode a v0.3 base64 ``data`` string without materializing the whole
        base64 text in working memory.
        """
        assert self._data_string is not None
        decoded = ByteAccumulator()
        pipe = _Base64Pipe(decoded)
        for chunk in self._data_string.iter_chunks():
            pipe.feed(chunk)
        pipe.finish()
        return decoded.getvalue()

    def close(self) -> None:
        if self._data_string is not None:
            self._data_string.close()
        if self._decoded_payload is not None:
            self._decoded_payload.close()

    def _fill(self) -> bool:
        if self._index < len(self._buffer):
            return True
        if self._eof:
            return False
        try:
            chunk = next(self._reader)
        except StopIteration:
            self._eof = True
            self._buffer.clear()
            self._index = 0
            return False
        del self._buffer[: self._index]
        self._buffer.extend(chunk)
        self._index = 0
        return True

    def _peek(self) -> int:
        if not self._fill():
            raise BatchReadError(
                "Truncated structured envelope: unexpected end of stream"
            )
        return self._buffer[self._index]

    def _advance(self) -> None:
        self._index += 1

    def _expect_char(self, expected: int) -> None:
        actual = self._peek()
        if actual != expected:
            raise BatchReadError(
                "Malformed structured envelope: expected "
                f"'{chr(expected)}' but found '{chr(actual)}'"
            )
        self._advance()

    def _consume_whitespace(self) -> None:
        while self._fill() and self._buffer[self._index] in b" \t\r\n":
            self._advance()

    def _read_string(self) -> str:
        self._expect_char(ord('"'))
        raw = bytearray()
        while True:
            if not self._fill():
                raise BatchReadError(
                    "Truncated structured envelope: unterminated string"
                )
            start = self._index
            idx = start
            while idx < len(self._buffer):
                current = self._buffer[idx]
                if current == ord('"') or current == ord("\\") or current < 0x20:
                    break
                idx += 1
            raw.extend(self._buffer[start:idx])
            self._index = idx
            if self._index < len(self._buffer):
                byte = self._buffer[self._index]
                self._advance()
                if byte == ord('"'):
                    return raw.decode("utf-8")
                if byte == ord("\\"):
                    raw.extend(self._read_escape().encode("utf-8"))
                    continue
                raise BatchReadError(
                    "Malformed structured envelope: unescaped control character"
                )

    def _read_string_into(
        self,
        raw_sink: ByteAccumulator,
        pipe: _Base64Pipe | None = None,
    ) -> None:
        """
        Stream a JSON string out of the envelope without buffering it whole.

        ``raw_sink`` receives the decoded string bytes. When ``pipe`` is given
        (the v1.0 ``data_base64`` case) its base64 output goes to the same sink
        instead, and JSON escapes are rejected as invalid base64.
        """
        self._expect_char(ord('"'))
        allow_escapes = pipe is None
        while True:
            if not self._fill():
                raise BatchReadError(
                    "Truncated structured envelope: unterminated string"
                )
            start = self._index
            idx = start
            while idx < len(self._buffer):
                current = self._buffer[idx]
                if current == ord('"') or current == ord("\\") or current < 0x20:
                    break
                idx += 1
            block = bytes(self._buffer[start:idx])
            if pipe is not None:
                pipe.feed(block)
            else:
                raw_sink.add(block)
            self._index = idx
            if self._index >= len(self._buffer):
                continue
            byte = self._buffer[self._index]
            self._advance()
            if byte == ord('"'):
                if pipe is not None:
                    pipe.finish()
                return
            if byte == ord("\\"):
                if not allow_escapes:
                    raise BatchReadError(
                        "Malformed structured envelope: escape in base64 string"
                    )
                raw_sink.add(self._read_escape().encode("utf-8"))
                continue
            raise BatchReadError(
                "Malformed structured envelope: unescaped control character"
            )

    def _read_escape(self) -> str:
        escape = chr(self._peek())
        self._advance()
        simple = {
            '"': '"',
            "\\": "\\",
            "/": "/",
            "b": "\b",
            "f": "\f",
            "n": "\n",
            "r": "\r",
            "t": "\t",
        }
        if escape in simple:
            return simple[escape]
        if escape == "u":
            code = self._read_hex_code_point()
            if 0xD800 <= code <= 0xDBFF:
                if self._peek() != ord("\\"):
                    raise BatchReadError(
                        "Malformed structured envelope: invalid surrogate pair"
                    )
                self._advance()
                if chr(self._peek()) != "u":
                    raise BatchReadError(
                        "Malformed structured envelope: invalid surrogate pair"
                    )
                self._advance()
                low = self._read_hex_code_point()
                if not 0xDC00 <= low <= 0xDFFF:
                    raise BatchReadError(
                        "Malformed structured envelope: invalid surrogate pair"
                    )
                return chr(0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00))
            return chr(code)
        raise BatchReadError(
            f"Malformed structured envelope: invalid escape '\\{escape}'"
        )

    def _read_hex_code_point(self) -> int:
        code = 0
        for _ in range(4):
            digit = chr(self._peek())
            self._advance()
            if digit not in "0123456789abcdefABCDEF":
                raise BatchReadError(
                    "Malformed structured envelope: invalid unicode escape"
                )
            code = code * 16 + int(digit, 16)
        return code

    def _read_data_value(self) -> None:
        self.data_present = True
        if self._peek() == ord('"'):
            self.data_is_string = True
            self._data_string = ByteAccumulator()
            self._read_string_into(self._data_string)
        else:
            self._data_value = self._read_generic_json()

    def _read_generic_json(self) -> Any:
        depth = 0
        in_string = False
        escaped = False
        terminated = False
        collected = bytearray()

        def consume_from(pos: int) -> int:
            nonlocal depth, in_string, escaped, terminated
            idx = pos
            while idx < len(self._buffer):
                byte = self._buffer[idx]
                if in_string:
                    if escaped:
                        escaped = False
                    elif byte == ord("\\"):
                        escaped = True
                    elif byte == ord('"'):
                        in_string = False
                elif byte == ord('"'):
                    in_string = True
                elif depth == 0 and byte in b",} \t\r\n":
                    # Top-level scalar ends at a structural delimiter or whitespace.
                    terminated = True
                    return idx
                elif byte in b"{[":
                    depth += 1
                elif byte in b"}]":
                    depth -= 1
                    if depth == 0:
                        terminated = True
                        return idx + 1
                idx += 1
            return idx

        end = consume_from(self._index)
        collected.extend(self._buffer[self._index : end])
        self._index = end
        while not terminated:
            if not self._fill():
                raise BatchReadError(
                    "Truncated structured envelope: unterminated JSON value"
                )
            end = consume_from(self._index)
            collected.extend(self._buffer[self._index : end])
            self._index = end
        try:
            return loads(bytes(collected))
        except ValueError as exc:
            raise BatchReadError(f"Malformed JSON value: {exc}") from exc

    def _read_value(self) -> Any:
        if self._peek() == ord('"'):
            return self._read_string()
        return self._read_generic_json()

    @property
    def generic_data_value(self) -> Any:
        return self._data_value


def _route_matches(
    route: BatchRouteRegistration,
    specversion: str | None,
    datacontenttype: str | None,
    base64_representation: bool,
) -> BatchRouteMatch | None:
    if route.generic:
        return BatchRouteMatch(
            route_name=route.name,
            route_version=route.version,
            generic=True,
            reasons=("generic route accepts regular CloudEvents representations",),
        )
    reasons: list[str] = []
    if specversion != route.required_specversion:
        return None
    reasons.append(f"specversion is '{specversion}'")
    if datacontenttype != route.required_datacontenttype:
        return None
    reasons.append(f"datacontenttype is '{datacontenttype}'")
    if route.require_base64 and not base64_representation:
        return None
    reasons.append("payload uses the v0.3 base64 representation")
    return BatchRouteMatch(
        route_name=route.name,
        route_version=route.version,
        generic=False,
        reasons=tuple(reasons),
    )


def _select_route(matches: list[BatchRouteMatch]) -> BatchRouteMatch:
    """
    Resolve matching routes by specificity.

    Specialized routes outrank generic ones. Equally specific matches raise
    :class:`AmbiguousRouteError` and none of them is executed.
    """
    if not matches:
        raise BatchReadError(
            "No registered route accepts this envelope version and payload "
            "representation"
        )
    specialized = [match for match in matches if not match.generic]
    candidates = specialized or matches
    if len(candidates) == 1:
        return candidates[0]
    raise AmbiguousRouteError(candidates)


def _read_structured(
    stream: Any,
    content_type: str,
    registry: BatchRouteRegistry,
    chunk_size: int,
) -> BatchReadResult:
    reader = _ChunkReader(stream, chunk_size)
    parser = _StructuredEnvelopeParser(reader)
    try:
        try:
            parser.parse()
        except BatchReadError as exc:
            return BatchReadResult(
                success=False, envelope="structured", error=str(exc)
            )

        attributes = dict(parser.attributes)
        specversion = attributes.get("specversion")
        datacontenttype = attributes.get(DATACONTENTTYPE_ATTR)
        base64_representation = (
            parser.data_is_base64_field
            or attributes.get("datacontentencoding", "").lower() == BASE64_ENCODING
        )

        candidate_routes = registry.routes_for(content_type)
        matches = [
            match
            for route in candidate_routes
            if (
                match := _route_matches(
                    route, specversion, datacontenttype, base64_representation
                )
            )
            is not None
        ]
        try:
            selected = _select_route(matches)
        except AmbiguousRouteError as exc:
            return BatchReadResult(
                success=False,
                envelope="structured",
                matches=tuple(exc.matches),
                error=str(exc),
            )
        except BatchReadError as exc:
            return BatchReadResult(
                success=False,
                envelope="structured",
                matches=tuple(matches),
                error=str(exc),
            )

        route = registry.registration(selected.route_name)
        assert route is not None

        if not selected.generic:
            if not parser.data_present or not base64_representation:
                return BatchReadResult(
                    success=False,
                    envelope="structured",
                    payload_route=route.name,
                    route_version=route.version,
                    matches=tuple(matches),
                    error="Warehouse route requires a base64 encoded payload",
                )
            try:
                if parser.data_is_base64_field:
                    raw_payload = parser.decoded_payload_bytes()
                else:
                    raw_payload = parser.decode_base64_string_data()
            except BatchReadError as exc:
                return BatchReadResult(
                    success=False,
                    envelope="structured",
                    payload_route=route.name,
                    route_version=route.version,
                    matches=tuple(matches),
                    error=str(exc),
                )
            try:
                assert route.decoder is not None
                business_data = route.decoder(raw_payload)
            except Exception as exc:  # decoder failures fail only this response
                return BatchReadResult(
                    success=False,
                    envelope="structured",
                    payload_route=route.name,
                    route_version=route.version,
                    matches=tuple(matches),
                    error=f"Business decoder failed: {exc}",
                )
            return BatchReadResult(
                success=True,
                attributes=attributes,
                envelope="structured",
                payload_route=route.name,
                data=raw_payload,
                business_data=business_data,
                route_version=route.version,
                matches=tuple(matches),
            )

        data: Any = None
        if parser.data_present:
            if parser.data_is_base64_field:
                data = parser.decoded_payload_bytes()
            elif parser.data_is_string:
                if (
                    specversion == SPECVERSION_V0_3
                    and attributes.get("datacontentencoding", "").lower()
                    == BASE64_ENCODING
                ):
                    try:
                        data = parser.decode_base64_string_data()
                    except BatchReadError as exc:
                        return BatchReadResult(
                            success=False,
                            envelope="structured",
                            payload_route=route.name,
                            route_version=route.version,
                            matches=tuple(matches),
                            error=str(exc),
                        )
                else:
                    data = parser.string_data_value()
            else:
                data = parser.generic_data_value

        business_data = None
        if route.decoder is not None:
            raw_payload = (
                data if isinstance(data, bytes) else dumps(data).encode("utf-8")
            )
            try:
                business_data = route.decoder(raw_payload)
            except Exception as exc:
                return BatchReadResult(
                    success=False,
                    envelope="structured",
                    payload_route=route.name,
                    route_version=route.version,
                    matches=tuple(matches),
                    error=f"Business decoder failed: {exc}",
                )

        return BatchReadResult(
            success=True,
            attributes=attributes,
            envelope="structured",
            payload_route=route.name,
            data=data,
            business_data=business_data,
            route_version=route.version,
            matches=tuple(matches),
        )
    finally:
        parser.close()


def _read_binary(
    stream: Any,
    headers: dict[str, str],
    chunk_size: int,
) -> BatchReadResult:
    accumulator = ByteAccumulator()
    for chunk in _ChunkReader(stream, chunk_size):
        accumulator.add(chunk)
    body = accumulator.getvalue()
    accumulator.close()

    attributes: dict[str, Any] = {}
    for header_name, header_value in headers.items():
        normalized = header_name.lower()
        if normalized.startswith(CE_PREFIX):
            attr_name = normalized[len(CE_PREFIX) :]
            attributes[attr_name] = _decode_header_value(attr_name, header_value)
        elif normalized == CONTENT_TYPE_HEADER:
            attributes[DATACONTENTTYPE_ATTR] = header_value

    datacontenttype = attributes.get(DATACONTENTTYPE_ATTR)
    data = JSONFormat().read_data(body, datacontenttype)
    return BatchReadResult(
        success=True,
        attributes=attributes,
        envelope="binary",
        payload_route=datacontenttype or "binary",
        data=data,
    )


def read_http_response(
    headers: dict[str, str],
    stream: Any,
    registry: BatchRouteRegistry,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
) -> BatchReadResult:
    """
    Read a single HTTP response stream exactly once and resolve its route.

    A response whose outer Content-Type explicitly declares a structured
    CloudEvents envelope is always parsed as structured, even when residual
    ``ce-*`` headers are present. Other responses are treated as binary mode and
    their event attributes come from the ``ce-*`` headers while Content-Type
    selects the payload format.

    :param headers: Outer HTTP response headers (case insensitive).
    :param stream: One-shot readable byte stream.
    :param registry: Route registry snapshot used for this response.
    :param chunk_size: Read boundary size; working memory stays bounded by it
        for segmentable payloads.
    """
    content_type = ""
    for header_name, header_value in headers.items():
        if header_name.lower() == CONTENT_TYPE_HEADER:
            content_type = header_value
            break

    outer_media_type = _media_type_without_parameters(content_type)
    if outer_media_type == STRUCTURED_MEDIA_TYPE:
        return _read_structured(stream, content_type, registry, chunk_size)
    return _read_binary(stream, headers, chunk_size)


@dataclass(frozen=True)
class HTTPBatchResponse:
    """One response in an HTTP batch: its headers and one-shot body stream."""

    headers: dict[str, str]
    stream: Any


class BatchResponseReader:
    """
    Reads a batch of HTTP responses, one stream per response.

    The registry is snapshotted when the reader is created. Route replacements
    performed while the batch is in flight never affect responses of this batch,
    while later batches observe the new registrations.
    """

    def __init__(
        self,
        responses: Iterable[HTTPBatchResponse | tuple[dict[str, str], Any]],
        registry: BatchRouteRegistry | None = None,
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
    ) -> None:
        self._responses = list(responses)
        self._registry = (registry or default_registry()).snapshot()
        self._chunk_size = chunk_size
        self._consumed = False

    @property
    def registry(self) -> BatchRouteRegistry:
        """Frozen registry snapshot used by this batch."""
        return self._registry

    def read_responses(self) -> list[BatchReadResult]:
        """
        Read every response exactly once.

        A failure of one response (truncation, invalid envelope, decoder error
        or ambiguous routes) yields a failed :class:`BatchReadResult` while the
        remaining responses stay readable.
        """
        if self._consumed:
            raise BatchReadError("Each batch response stream can only be read once")
        self._consumed = True
        results: list[BatchReadResult] = []
        for response in self._responses:
            headers, stream = _unpack_response(response)
            try:
                result = read_http_response(
                    headers, stream, self._registry, self._chunk_size
                )
            except BatchReadError as exc:
                result = BatchReadResult(success=False, error=str(exc))
            except Exception as exc:
                result = BatchReadResult(
                    success=False, error=f"Unexpected read failure: {exc}"
                )
            results.append(result)
        return results

    def __iter__(self) -> Iterable[BatchReadResult]:
        return iter(self.read_responses())


def _unpack_response(
    response: HTTPBatchResponse | tuple[dict[str, str], Any],
) -> tuple[dict[str, str], Any]:
    if isinstance(response, HTTPBatchResponse):
        return response.headers, response.stream
    return response[0], response[1]


_DEFAULT_REGISTRY_SINGLETON: BatchRouteRegistry | None = None


def default_registry() -> BatchRouteRegistry:
    """
    Return the process-wide default route registry.

    Applications register their generic and warehouse routes here and pass the
    registry explicitly when deterministic ownership is required.
    """
    global _DEFAULT_REGISTRY_SINGLETON
    if _DEFAULT_REGISTRY_SINGLETON is None:
        _DEFAULT_REGISTRY_SINGLETON = BatchRouteRegistry()
    return _DEFAULT_REGISTRY_SINGLETON


def read_http_batch(
    responses: Iterable[HTTPBatchResponse | tuple[dict[str, str], Any]],
    registry: BatchRouteRegistry | None = None,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
) -> list[BatchReadResult]:
    """
    Read a whole HTTP batch and return one result per response.

    :param responses: Responses as ``(headers, stream)`` tuples or
        :class:`HTTPBatchResponse` instances.
    :param registry: Route registry; defaults to the process registry.
    :param chunk_size: Maximum bytes held per read boundary.
    """
    return BatchResponseReader(responses, registry, chunk_size).read_responses()
