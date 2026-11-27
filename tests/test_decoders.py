from __future__ import annotations

import io
import typing
import zlib

import chardet
import pytest
import zstandard as zstd

import httpx


def test_deflate():
    """
    Deflate encoding may use either 'zlib' or 'deflate' in the wild.

    https://stackoverflow.com/questions/1838699/how-can-i-decompress-a-gzip-stream-with-zlib#answer-22311297
    """
    body = b"test 123"
    compressor = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
    compressed_body = compressor.compress(body) + compressor.flush()

    headers = [(b"Content-Encoding", b"deflate")]
    response = httpx.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_zlib():
    """
    Deflate encoding may use either 'zlib' or 'deflate' in the wild.

    https://stackoverflow.com/questions/1838699/how-can-i-decompress-a-gzip-stream-with-zlib#answer-22311297
    """
    body = b"test 123"
    compressed_body = zlib.compress(body)

    headers = [(b"Content-Encoding", b"deflate")]
    response = httpx.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_gzip():
    body = b"test 123"
    compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    compressed_body = compressor.compress(body) + compressor.flush()

    headers = [(b"Content-Encoding", b"gzip")]
    response = httpx.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_brotli():
    body = b"test 123"
    compressed_body = b"\x8b\x03\x80test 123\x03"

    headers = [(b"Content-Encoding", b"br")]
    response = httpx.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_zstd():
    body = b"test 123"
    compressed_body = zstd.compress(body)

    headers = [(b"Content-Encoding", b"zstd")]
    response = httpx.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_zstd_decoding_error():
    compressed_body = "this_is_not_zstd_compressed_data"

    headers = [(b"Content-Encoding", b"zstd")]
    with pytest.raises(httpx.DecodingError):
        httpx.Response(
            200,
            headers=headers,
            content=compressed_body,
        )


def test_zstd_empty():
    headers = [(b"Content-Encoding", b"zstd")]
    response = httpx.Response(200, headers=headers, content=b"")
    assert response.content == b""


def test_zstd_truncated():
    body = b"test 123"
    compressed_body = zstd.compress(body)

    headers = [(b"Content-Encoding", b"zstd")]
    with pytest.raises(httpx.DecodingError):
        httpx.Response(
            200,
            headers=headers,
            content=compressed_body[1:3],
        )


def test_zstd_multiframe():
    # test inspired by urllib3 test suite
    data = (
        # Zstandard frame
        zstd.compress(b"foo")
        # skippable frame (must be ignored)
        + bytes.fromhex(
            "50 2A 4D 18"  # Magic_Number (little-endian)
            "07 00 00 00"  # Frame_Size (little-endian)
            "00 00 00 00 00 00 00"  # User_Data
        )
        # Zstandard frame
        + zstd.compress(b"bar")
    )
    compressed_body = io.BytesIO(data)

    headers = [(b"Content-Encoding", b"zstd")]
    response = httpx.Response(200, headers=headers, content=compressed_body)
    response.read()
    assert response.content == b"foobar"


def test_multi():
    body = b"test 123"

    deflate_compressor = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
    compressed_body = deflate_compressor.compress(body) + deflate_compressor.flush()

    gzip_compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    compressed_body = (
        gzip_compressor.compress(compressed_body) + gzip_compressor.flush()
    )

    headers = [(b"Content-Encoding", b"deflate, gzip")]
    response = httpx.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_multi_with_identity():
    body = b"test 123"
    compressed_body = b"\x8b\x03\x80test 123\x03"

    headers = [(b"Content-Encoding", b"br, identity")]
    response = httpx.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body

    headers = [(b"Content-Encoding", b"identity, br")]
    response = httpx.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


@pytest.mark.anyio
async def test_streaming():
    body = b"test 123"
    compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)

    async def compress(body: bytes) -> typing.AsyncIterator[bytes]:
        yield compressor.compress(body)
        yield compressor.flush()

    headers = [(b"Content-Encoding", b"gzip")]
    response = httpx.Response(
        200,
        headers=headers,
        content=compress(body),
    )
    assert not hasattr(response, "body")
    assert await response.aread() == body


@pytest.mark.parametrize("header_value", (b"deflate", b"gzip", b"br", b"identity"))
def test_empty_content(header_value):
    headers = [(b"Content-Encoding", header_value)]
    response = httpx.Response(
        200,
        headers=headers,
        content=b"",
    )
    assert response.content == b""


@pytest.mark.parametrize("header_value", (b"deflate", b"gzip", b"br", b"identity"))
def test_decoders_empty_cases(header_value):
    headers = [(b"Content-Encoding", header_value)]
    response = httpx.Response(content=b"", status_code=200, headers=headers)
    assert response.read() == b""


@pytest.mark.parametrize("header_value", (b"deflate", b"gzip", b"br"))
def test_decoding_errors(header_value):
    headers = [(b"Content-Encoding", header_value)]
    compressed_body = b"invalid"
    with pytest.raises(httpx.DecodingError):
        request = httpx.Request("GET", "https://example.org")
        httpx.Response(200, headers=headers, content=compressed_body, request=request)

    with pytest.raises(httpx.DecodingError):
        httpx.Response(200, headers=headers, content=compressed_body)


@pytest.mark.parametrize(
    ["data", "encoding"],
    [
        ((b"Hello,", b" world!"), "ascii"),
        ((b"\xe3\x83", b"\x88\xe3\x83\xa9", b"\xe3", b"\x83\x99\xe3\x83\xab"), "utf-8"),
        ((b"Euro character: \x88! abcdefghijklmnopqrstuvwxyz", b""), "cp1252"),
        ((b"Accented: \xd6sterreich abcdefghijklmnopqrstuvwxyz", b""), "iso-8859-1"),
    ],
)
@pytest.mark.anyio
async def test_text_decoder_with_autodetect(data, encoding):
    async def iterator() -> typing.AsyncIterator[bytes]:
        nonlocal data
        for chunk in data:
            yield chunk

    def autodetect(content):
        return chardet.detect(content).get("encoding")

    # Accessing `.text` on a read response.
    response = httpx.Response(200, content=iterator(), default_encoding=autodetect)
    await response.aread()
    assert response.text == (b"".join(data)).decode(encoding)

    # Streaming `.aiter_text` iteratively.
    # Note that if we streamed the text *without* having read it first, then
    # we won't get a `charset_normalizer` guess, and will instead always rely
    # on utf-8 if no charset is specified.
    text = "".join([part async for part in response.aiter_text()])
    assert text == (b"".join(data)).decode(encoding)


@pytest.mark.anyio
async def test_text_decoder_known_encoding():
    async def iterator() -> typing.AsyncIterator[bytes]:
        yield b"\x83g"
        yield b"\x83"
        yield b"\x89\x83x\x83\x8b"

    response = httpx.Response(
        200,
        headers=[(b"Content-Type", b"text/html; charset=shift-jis")],
        content=iterator(),
    )

    await response.aread()
    assert "".join(response.text) == "トラベル"


def test_text_decoder_empty_cases():
    response = httpx.Response(200, content=b"")
    assert response.text == ""

    response = httpx.Response(200, content=[b""])
    response.read()
    assert response.text == ""


@pytest.mark.parametrize(
    ["data", "expected"],
    [((b"Hello,", b" world!"), ["Hello,", " world!"])],
)
def test_streaming_text_decoder(
    data: typing.Iterable[bytes], expected: list[str]
) -> None:
    response = httpx.Response(200, content=iter(data))
    assert list(response.iter_text()) == expected


def test_line_decoder_nl():
    response = httpx.Response(200, content=[b""])
    assert list(response.iter_lines()) == []

    response = httpx.Response(200, content=[b"", b"a\n\nb\nc"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    # Issue #1033
    response = httpx.Response(
        200, content=[b"", b"12345\n", b"foo ", b"bar ", b"baz\n"]
    )
    assert list(response.iter_lines()) == ["12345", "foo bar baz"]


def test_line_decoder_cr():
    response = httpx.Response(200, content=[b"", b"a\r\rb\rc"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    response = httpx.Response(200, content=[b"", b"a\r\rb\rc\r"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    # Issue #1033
    response = httpx.Response(
        200, content=[b"", b"12345\r", b"foo ", b"bar ", b"baz\r"]
    )
    assert list(response.iter_lines()) == ["12345", "foo bar baz"]


def test_line_decoder_crnl():
    response = httpx.Response(200, content=[b"", b"a\r\n\r\nb\r\nc"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    response = httpx.Response(200, content=[b"", b"a\r\n\r\nb\r\nc\r\n"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    response = httpx.Response(200, content=[b"", b"a\r", b"\n\r\nb\r\nc"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    # Issue #1033
    response = httpx.Response(200, content=[b"", b"12345\r\n", b"foo bar baz\r\n"])
    assert list(response.iter_lines()) == ["12345", "foo bar baz"]


def test_invalid_content_encoding_header():
    headers = [(b"Content-Encoding", b"invalid-header")]
    body = b"test 123"

    response = httpx.Response(
        200,
        headers=headers,
        content=body,
    )
    assert response.content == body


def _gzip_member(body: bytes) -> bytes:
    compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    return compressor.compress(body) + compressor.flush()


def _nested_report(
    *,
    second_member_truncated: bool = False,
    outer_truncated: bool = False,
    outer_bad_checksum: bool = False,
) -> bytes:
    first = _gzip_member(b"A,10\n")
    second = _gzip_member(b"B,200\n")
    if second_member_truncated:
        second = second[:-2]
    body = zlib.compress(first + second)
    if outer_truncated:
        return body[:-2]
    if outer_bad_checksum:
        return body[:-1] + bytes([body[-1] ^ 0xFF])
    return body


def _byte_stream(body: bytes, parts: int) -> typing.Iterator[bytes]:
    step = max(1, len(body) // parts)
    for index in range(0, len(body), step):
        yield body[index : index + step]


REPORT_HEADERS = [(b"Content-Encoding", b"gzip, deflate")]


def test_multi_member_gzip_report_decodes_every_member():
    body = _nested_report()
    response = httpx.Response(200, headers=REPORT_HEADERS, content=body)
    assert response.read() == b"A,10\nB,200\n"


def test_multi_member_gzip_report_is_chunked_across_members():
    body = _nested_report()
    response = httpx.Response(
        200, headers=REPORT_HEADERS, content=_byte_stream(body, parts=1)
    )
    assert list(response.iter_bytes(8)) == [b"A,10\nB,2", b"00\n"]


@pytest.mark.parametrize(
    "options, expected_incomplete, expected_checksum",
    [
        ({"second_member_truncated": True}, ["gzip member 2"], []),
        ({"outer_truncated": True}, ["deflate"], []),
        ({"outer_bad_checksum": True}, [], ["deflate"]),
        (
            {"second_member_truncated": True, "outer_bad_checksum": True},
            ["gzip member 2"],
            ["deflate"],
        ),
    ],
)
def test_nested_report_reports_each_layer(
    options: dict[str, bool],
    expected_incomplete: list[str],
    expected_checksum: list[str],
):
    body = _nested_report(**options)

    response = httpx.Response(
        200, headers=REPORT_HEADERS, stream=httpx.ByteStream(body)
    )
    iterator = response.iter_bytes(8)
    assert next(iterator) == b"A,10\nB,2"
    assert next(iterator) == b"00\n"

    with pytest.raises(httpx.DecodingError) as exc_info:
        next(iterator)

    error = exc_info.value
    assert [status.layer for status in error.incomplete_layers] == expected_incomplete
    assert [
        status.layer for status in error.checksum_failed_layers
    ] == expected_checksum


def test_nested_report_bad_outer_keeps_inner_members_complete():
    body = _nested_report(outer_bad_checksum=True)
    response = httpx.Response(
        200, headers=REPORT_HEADERS, stream=httpx.ByteStream(body)
    )

    with pytest.raises(httpx.DecodingError) as exc_info:
        response.read()

    error = exc_info.value
    assert [status.layer for status in error.checksum_failed_layers] == ["deflate"]
    assert [status.layer for status in error.complete_layers] == [
        "gzip member 1",
        "gzip member 2",
    ]


def test_nested_report_read_raises_on_tail_failure():
    for options in (
        {"second_member_truncated": True},
        {"outer_truncated": True},
        {"outer_bad_checksum": True},
    ):
        response = httpx.Response(
            200,
            headers=REPORT_HEADERS,
            stream=httpx.ByteStream(_nested_report(**options)),
        )
        with pytest.raises(httpx.DecodingError):
            response.read()


def test_nested_report_close_after_first_chunk_is_a_stop():
    body = _nested_report(outer_truncated=True)
    response = httpx.Response(
        200, headers=REPORT_HEADERS, content=_byte_stream(body, parts=2)
    )
    iterator = response.iter_bytes(8)
    assert next(iterator) == b"A,10\nB,2"

    response.close()

    with pytest.raises(httpx.StreamClosed):
        next(iterator)


def test_nested_report_close_after_short_chunk_keeps_failure():
    body = _nested_report(outer_truncated=True)
    response = httpx.Response(
        200, headers=REPORT_HEADERS, content=_byte_stream(body, parts=2)
    )
    iterator = response.iter_bytes(8)
    assert next(iterator) == b"A,10\nB,2"
    assert next(iterator) == b"00\n"

    response.close()

    with pytest.raises(httpx.DecodingError):
        next(iterator)


def test_nested_report_raw_bytes_are_preserved():
    for body in (_nested_report(), _nested_report(outer_truncated=True)):
        response = httpx.Response(
            200, headers=REPORT_HEADERS, content=_byte_stream(body, parts=3)
        )
        assert b"".join(response.iter_raw()) == body


@pytest.mark.anyio
async def test_async_multi_member_gzip_report():
    body = _nested_report()

    async def stream() -> typing.AsyncIterator[bytes]:
        yield body

    response = httpx.Response(200, headers=REPORT_HEADERS, content=stream())
    assert await response.aread() == b"A,10\nB,200\n"


@pytest.mark.anyio
async def test_async_multi_member_gzip_report_is_chunked():
    body = _nested_report()

    async def stream() -> typing.AsyncIterator[bytes]:
        for index in range(0, len(body), 4):
            yield body[index : index + 4]

    response = httpx.Response(200, headers=REPORT_HEADERS, content=stream())
    assert [part async for part in response.aiter_bytes(8)] == [
        b"A,10\nB,2",
        b"00\n",
    ]


@pytest.mark.anyio
async def test_async_nested_report_reports_each_layer():
    body = _nested_report(second_member_truncated=True, outer_bad_checksum=True)

    async def stream() -> typing.AsyncIterator[bytes]:
        yield body

    response = httpx.Response(200, headers=REPORT_HEADERS, content=stream())
    parts = []
    with pytest.raises(httpx.DecodingError) as exc_info:
        async for part in response.aiter_bytes(8):
            parts.append(part)

    assert parts == [b"A,10\nB,2", b"00\n"]
    error = exc_info.value
    assert [status.layer for status in error.incomplete_layers] == ["gzip member 2"]
    assert [status.layer for status in error.checksum_failed_layers] == ["deflate"]


@pytest.mark.anyio
async def test_async_nested_report_read_raises_on_tail_failure():
    async def stream() -> typing.AsyncIterator[bytes]:
        yield _nested_report(outer_truncated=True)

    response = httpx.Response(200, headers=REPORT_HEADERS, content=stream())
    with pytest.raises(httpx.DecodingError):
        await response.aread()


@pytest.mark.anyio
async def test_async_nested_report_close_after_first_chunk_is_a_stop():
    body = _nested_report(outer_truncated=True)

    async def stream() -> typing.AsyncIterator[bytes]:
        yield body[:-2]
        yield body[-2:]

    response = httpx.Response(200, headers=REPORT_HEADERS, content=stream())
    iterator = response.aiter_bytes(8)
    assert await iterator.__anext__() == b"A,10\nB,2"

    await response.aclose()

    with pytest.raises(httpx.StreamClosed):
        await iterator.__anext__()


@pytest.mark.anyio
async def test_async_nested_report_close_keeps_failure():
    body = _nested_report(outer_truncated=True)

    async def stream() -> typing.AsyncIterator[bytes]:
        yield body[:-2]
        yield body[-2:]

    response = httpx.Response(200, headers=REPORT_HEADERS, content=stream())
    iterator = response.aiter_bytes(8)
    assert await iterator.__anext__() == b"A,10\nB,2"
    assert await iterator.__anext__() == b"00\n"

    await response.aclose()

    with pytest.raises(httpx.DecodingError):
        await iterator.__anext__()
