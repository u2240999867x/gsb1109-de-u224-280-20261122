from __future__ import annotations

import io
import typing
import unicodedata
from urllib.parse import parse_qsl, urlencode

import pytest

import httpx

method = "POST"
url = "https://www.example.com"
COMBINING_LIMIT = 8

FormValueChunk = typing.Union[str, bytes]
StreamingFormData = typing.Sequence[tuple[str, typing.Any]]


def read_body(request: httpx.Request) -> bytes:
    assert isinstance(request.stream, typing.Iterable)
    return b"".join(request.stream)


async def read_async_body(request: httpx.Request) -> bytes:
    assert isinstance(request.stream, typing.AsyncIterable)
    return b"".join([chunk async for chunk in request.stream])


def normalize_whole_value(value: str) -> str:
    # Independent whole-value reference implementation of the contract:
    # newline normalization followed by Unicode NFC over the entire field.
    value = value.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\r\n")
    return unicodedata.normalize("NFC", value)


def reference_body(pairs: list[tuple[str, str]]) -> bytes:
    return urlencode(
        [(name, normalize_whole_value(value)) for name, value in pairs]
    ).encode("utf-8")


def nfd(text: str) -> str:
    return unicodedata.normalize("NFD", text)


class RecordingTransport(httpx.BaseTransport):
    """Pulls the request stream one chunk at a time, like a real transport."""

    def __init__(self) -> None:
        self.received: list[bytes] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, typing.Iterable)
        for chunk in request.stream:
            self.received.append(chunk)
        return httpx.Response(200)


class PartialTransport(httpx.BaseTransport):
    """Pulls only a fixed number of chunks, then stops without exhausting."""

    def __init__(self, max_chunks: int) -> None:
        self.max_chunks = max_chunks
        self.received: list[bytes] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, typing.Iterable)
        for index, chunk in enumerate(request.stream):
            self.received.append(chunk)
            if index + 1 >= self.max_chunks:
                break
        return httpx.Response(200)


def tracking_source(
    chunks: typing.Iterable[FormValueChunk], state: dict[str, typing.Any]
) -> typing.Iterator[FormValueChunk]:
    for chunk in chunks:
        state["yielded"] += 1
        yield chunk
    state["exhausted"] = True


def rechunk(text: str, sizes: list[int]) -> list[str]:
    chunks: list[str] = []
    index = 0
    for size in sizes + [len(text)]:
        if index >= len(text):
            break
        chunks.append(text[index : index + size])
        index += size
    return chunks


@pytest.mark.anyio
async def test_streaming_form_matches_whole_value_reference():
    note_one = nfd("Café note") + "\r\nline2\rlast"
    note_two = "plus+space &separator"
    whole_pairs = [("note", note_one), ("biz", ""), ("note", note_two)]
    expected = reference_body(whole_pairs)

    # The accent combination and the CRLF both straddle chunk boundaries.
    note_one_chunks = [
        nfd("Café")[:-1],  # base letter only, combining mark in next chunk
        nfd("é")[1] + " note\r",
        "\nline2\r",
        "last",
    ]
    note_two_chunks = ["plus+", "space ", "&separator"]
    fields: StreamingFormData = [
        ("note", iter(note_one_chunks)),
        ("biz", iter([""])),
        ("note", iter(note_two_chunks)),
    ]

    request = httpx.Request(
        method,
        url,
        form=fields,
        form_combining_limit=COMBINING_LIMIT,
    )
    assert request.headers == {
        "Host": "www.example.com",
        "Transfer-Encoding": "chunked",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    assert "Content-Length" not in request.headers

    sync_body = read_body(request)
    assert sync_body == expected

    # Pair order, repeated names and the empty value are all preserved.
    decoded = parse_qsl(sync_body.decode("utf-8"), keep_blank_values=True)
    assert decoded == [
        ("note", normalize_whole_value(note_one)),
        ("biz", ""),
        ("note", normalize_whole_value(note_two)),
    ]
    assert b"&biz=&note=" in sync_body

    # The same fields through a second, independently chunked source give the
    # exact same canonical bytes.
    flattened_one = "".join(note_one_chunks)
    flattened_two = "".join(note_two_chunks)
    alternate = [
        ("note", iter(rechunk(flattened_one, [2, 1, 4]))),
        ("biz", iter([""])),
        ("note", iter(rechunk(flattened_two, [7]))),
    ]
    alternate_request = httpx.Request(
        method, url, form=alternate, form_combining_limit=COMBINING_LIMIT
    )
    assert read_body(alternate_request) == expected

    # A transport pulling chunk by chunk receives the same canonical body.
    transport = RecordingTransport()
    client = httpx.Client(transport=transport)
    pulled_request = httpx.Request(
        method,
        url,
        form=[
            ("note", iter(rechunk(flattened_one, [3, 3, 3, 3]))),
            ("biz", iter([""])),
            ("note", iter(rechunk(flattened_two, [1, 1, 1]))),
        ],
        form_combining_limit=COMBINING_LIMIT,
    )
    client.send(pulled_request)
    assert b"".join(transport.received) == expected

    # Async sources produce the same bytes through the async stream.
    async def async_source(
        chunks: list[str],
    ) -> typing.AsyncIterator[str]:
        for chunk in chunks:
            yield chunk

    async_request = httpx.Request(
        method,
        url,
        form=[
            ("note", async_source(note_one_chunks)),
            ("biz", async_source([""])),
            ("note", async_source(note_two_chunks)),
        ],
        form_combining_limit=COMBINING_LIMIT,
    )
    assert isinstance(async_request.stream, httpx.AsyncByteStream)
    assert await read_async_body(async_request) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("a\r\nb", "a\r\nb"),  # existing CRLF collapses to one
        ("a\rb", "a\r\nb"),  # lone CR -> one CRLF
        ("a\nb", "a\r\nb"),  # lone LF -> one CRLF
        ("a\r\r\nb", "a\r\n\r\nb"),  # lone CR then CRLF
        ("a\n\r\nb", "a\r\n\r\nb"),  # lone LF then CRLF
        ("\r\n\r\n", "\r\n\r\n"),
        ("end\r", "end\r\n"),  # trailing lone CR, finished after chunks
    ],
)
def test_newline_normalization_across_chunks(raw: str, expected: str) -> None:
    for split in range(1, len(raw)):
        chunks = [raw[:split], raw[split:]]
        request = httpx.Request(
            method,
            url,
            form=[("field", iter(chunks))],
            form_combining_limit=COMBINING_LIMIT,
        )
        body = read_body(request).decode("utf-8")
        assert body == "field=" + urlencode([("", expected)])[1:], split


def test_nfc_equivalence_with_arbitrary_chunk_boundaries():
    text = nfd("Ångström æøå 日本語 ") + "z" + "̀" * 4
    reference = reference_body([("field", text)])

    # Every possible split point must give identical canonical bytes.
    for split in range(1, len(text)):
        request = httpx.Request(
            method,
            url,
            form=[("field", iter([text[:split], text[split:]]))],
            form_combining_limit=COMBINING_LIMIT,
        )
        assert read_body(request) == reference, split


def test_bytes_chunks_split_multibyte_character():
    text = nfd("éê€") + "\r\nx"
    raw = text.encode("utf-8")
    byte_chunks = [raw[:1], raw[1:3], raw[3:5], raw[5:]]

    request = httpx.Request(
        method,
        url,
        form=[("field", iter(byte_chunks))],
        form_combining_limit=COMBINING_LIMIT,
    )
    assert read_body(request) == reference_body([("field", text)])


def test_filelike_source_is_read_in_blocks():
    text = nfd("streamed é\r\nvalue")
    stream = io.StringIO(text)
    request = httpx.Request(
        method,
        url,
        form=[("field", stream)],
        form_combining_limit=COMBINING_LIMIT,
    )
    assert read_body(request) == reference_body([("field", text)])


class BlockReader:
    """A readable object that is not itself iterable."""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)
        self.reads = 0

    def read(self, size: int) -> bytes:
        self.reads += 1
        return self._chunks.pop(0) if self._chunks else b""


def test_non_iterable_filelike_source():
    text = nfd("é") + "\r\nend"
    raw = text.encode("utf-8")
    reader = BlockReader([raw[:2], raw[2:4], raw[4:]])
    assert not isinstance(reader, typing.Iterable)

    request = httpx.Request(
        method,
        url,
        form=[("field", reader)],
        form_combining_limit=COMBINING_LIMIT,
    )
    assert read_body(request) == reference_body([("field", text)])
    assert reader.reads >= 3


def test_field_name_is_nfc_and_newline_normalized():
    name = nfd("é") + "\r\nk"
    value = "v"
    request = httpx.Request(
        method,
        url,
        form=[(name, iter([value]))],
        form_combining_limit=COMBINING_LIMIT,
    )
    expected_name = normalize_whole_value(name)
    assert read_body(request) == urlencode([(expected_name, value)]).encode()


class AsyncReader:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)

    async def aread(self, size: int) -> bytes:
        return self._chunks.pop(0) if self._chunks else b""


def test_async_filelike_source_uses_aread():
    text = nfd("abé")
    raw = text.encode("utf-8")

    async def run() -> bytes:
        request = httpx.Request(
            method,
            url,
            form=[("field", AsyncReader([raw[:3], raw[3:]]))],
            form_combining_limit=COMBINING_LIMIT,
        )
        assert isinstance(request.stream, httpx.AsyncByteStream)
        return await read_async_body(request)

    import asyncio

    body = asyncio.run(run())
    assert body == reference_body([("field", text)])


def test_partial_pull_does_not_read_whole_source():
    first_chunks = ["a\r\n", "b\r\n", "c\r\n", "d\r\n"]
    second_chunks = ["e\r\n", "f\r\n", "g\r\n"]
    state_one = {"yielded": 0, "exhausted": False}
    state_two = {"yielded": 0, "exhausted": False}
    fields = [
        ("first", tracking_source(first_chunks, state_one)),
        ("second", tracking_source(second_chunks, state_two)),
    ]
    request = httpx.Request(
        method, url, form=fields, form_combining_limit=COMBINING_LIMIT
    )

    # Output chunk 1 is the field header; each flushed source chunk then maps
    # to exactly one further output chunk. Stop after that second pull.
    transport = PartialTransport(max_chunks=2)
    client = httpx.Client(transport=transport)
    client.send(request)

    # Only one source chunk was read, and neither text source was consumed in
    # full; the second field never started.
    assert state_one["yielded"] == 1
    assert state_one["exhausted"] is False
    assert state_two["yielded"] == 0
    assert state_two["exhausted"] is False
    received = b"".join(transport.received)
    assert received == b"first=a%0D%0A"
    full_body = reference_body(
        [("first", "".join(first_chunks)), ("second", "".join(second_chunks))]
    )
    assert full_body.startswith(received)

    # A full pull on a fresh request does exhaust every source.
    full_state = {"yielded": 0, "exhausted": False}
    full_request = httpx.Request(
        method,
        url,
        form=[
            (
                "first",
                tracking_source(["a\r\n", "b\r\n"], full_state),
            )
        ],
        form_combining_limit=COMBINING_LIMIT,
    )
    full_request.read()
    assert full_state["exhausted"] is True


def test_combining_limit_exceeded_reports_field_and_sent_prefix():
    mark = "̀"
    fields = [
        ("ok", iter(["fine"])),
        ("bad", iter(["base" + mark * 2, mark * 3])),
    ]
    request = httpx.Request(method, url, form=fields, form_combining_limit=3)

    sent: list[bytes] = []
    assert isinstance(request.stream, typing.Iterable)
    with pytest.raises(httpx.FormValueLimitExceeded) as exc_info:
        for chunk in request.stream:
            sent.append(chunk)

    exception = exc_info.value
    assert exception.field_name == "bad"
    assert exception.limit == 3
    assert isinstance(exception, httpx.RequestError)
    # The legal preceding field was sent in full, and only a normalized prefix
    # of the offending field went out; its combining marks never shipped, so
    # the field as a whole was not submitted.
    sent_bytes = b"".join(sent)
    assert sent_bytes == b"ok=fine&bad=bas"
    assert b"ok=fine" in sent_bytes
    assert "̀".encode("utf-8") not in sent_bytes


def test_combining_limit_boundary_values():
    mark = "̀"
    accepted = httpx.Request(
        method,
        url,
        form=[("field", iter(["a" + mark * 3]))],
        form_combining_limit=3,
    )
    assert read_body(accepted) == reference_body([("field", "a" + mark * 3)])

    rejected = httpx.Request(
        method,
        url,
        form=[("field", iter(["a" + mark * 4]))],
        form_combining_limit=3,
    )
    with pytest.raises(httpx.FormValueLimitExceeded) as exc_info:
        read_body(rejected)
    assert exc_info.value.field_name == "field"


def test_limit_exception_attaches_request_through_client():
    def source() -> typing.Iterator[str]:
        yield "x"
        yield "̀" * 5
    request = httpx.Request(
        method, url, form=[("field", source())], form_combining_limit=2
    )
    client = httpx.Client(transport=RecordingTransport())
    with pytest.raises(httpx.FormValueLimitExceeded) as exc_info:
        client.send(request)
    assert exc_info.value.request is request


def test_combining_limit_is_per_field():
    mark = "̀"
    # Two fields each at the limit are fine; only a single run is bounded.
    request = httpx.Request(
        method,
        url,
        form=[
            ("a", iter(["x" + mark * 2])),
            ("b", iter(["y" + mark * 2])),
        ],
        form_combining_limit=2,
    )
    assert read_body(request) == reference_body(
        [("a", "x" + mark * 2), ("b", "y" + mark * 2)]
    )


def test_missing_combining_limit_is_an_error():
    with pytest.raises(ValueError, match="form_combining_limit"):
        httpx.Request(method, url, form=[("a", iter(["b"]))])


@pytest.mark.parametrize("bad_limit", (-1, False, 1.5))
def test_invalid_combining_limit(bad_limit: typing.Any) -> None:
    with pytest.raises(ValueError):
        httpx.Request(
            method,
            url,
            form=[("a", iter(["b"]))],
            form_combining_limit=bad_limit,
        )


def test_form_cannot_be_combined_with_other_bodies():
    with pytest.raises(TypeError):
        httpx.Request(
            method,
            url,
            form=[("a", iter(["b"]))],
            form_combining_limit=1,
            data={"a": "b"},
        )


def test_invalid_field_descriptions():
    with pytest.raises(TypeError):
        httpx.Request(
            method,
            url,
            form=[("a",)],  # type: ignore[list-item]
            form_combining_limit=1,
        )

    with pytest.raises(TypeError):
        httpx.Request(
            method,
            url,
            form=[(1, iter(["b"]))],  # type: ignore[list-item]
            form_combining_limit=1,
        )

    with pytest.raises(TypeError):
        httpx.Request(
            method,
            url,
            form=[("a", object())],  # type: ignore[list-item]
            form_combining_limit=1,
        )


def test_dict_form_data_remains_backward_compatible():
    request = httpx.Request(method, url, data={"Hello": "world!"})
    assert request.headers["Content-Length"] == "14"
    assert "Transfer-Encoding" not in request.headers
    assert read_body(request) == b"Hello=world%21"

    listed = httpx.Request(method, url, data={"note": ["a", "b"]})
    assert read_body(listed) == b"note=a&note=b"


def test_empty_streaming_form():
    request = httpx.Request(
        method, url, form=[], form_combining_limit=COMBINING_LIMIT
    )
    assert read_body(request) == b""
    assert request.headers["Transfer-Encoding"] == "chunked"


def test_plain_string_and_bytes_sources():
    request = httpx.Request(
        method,
        url,
        form=[("s", "a+b"), ("b", b"c d")],
        form_combining_limit=COMBINING_LIMIT,
    )
    assert read_body(request) == b"s=a%2Bb&b=c+d"


def test_encode_entry_point_returns_unknown_length_stream():
    headers, stream = httpx.encode_streaming_urlencoded_data(
        [("field", iter(["a", "b"]))], combining_limit=COMBINING_LIMIT
    )
    assert headers == {
        "Transfer-Encoding": "chunked",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    assert isinstance(stream, httpx.URLencodedFormStream)
    assert read_body(httpx.Request(method, url, headers={}, stream=stream)) == (
        b"field=ab"
    )
