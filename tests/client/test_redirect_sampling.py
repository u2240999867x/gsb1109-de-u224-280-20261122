"""
Tests for optional raw-byte sampling of followed redirect bodies.
"""

import gzip
import typing

import pytest

import httpx

MANDATORY_NOTE = b"mandatory audit note"


class ChunkThenErrorSyncStream(httpx.SyncByteStream):
    """
    Yields a single chunk, then raises on the next pull. Closing while
    suspended at the first yield stays clean.
    """

    def __init__(self, first: bytes, error: Exception) -> None:
        self._first = first
        self._error = error
        self._delivered = False
        self.closed = False

    def __iter__(self) -> typing.Iterator[bytes]:
        if not self._delivered:
            self._delivered = True
            yield self._first
        # Reached only by a second ``next()``; ``close()`` injects
        # GeneratorExit at the yield above and never executes this line.
        raise self._error

    def close(self) -> None:
        self.closed = True


class ChunkThenErrorAsyncStream(httpx.AsyncByteStream):
    def __init__(self, first: bytes, error: Exception) -> None:
        self._first = first
        self._error = error
        self._delivered = False
        self.closed = False

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        if not self._delivered:
            self._delivered = True
            yield self._first
        raise self._error

    async def aclose(self) -> None:
        self.closed = True


class FinalSyncStream(httpx.SyncByteStream):
    def __init__(self) -> None:
        self.closed = False

    def __iter__(self) -> typing.Iterator[bytes]:
        yield b"monthly,csv"

    def close(self) -> None:
        self.closed = True


class FinalAsyncStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.closed = False

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        yield b"monthly,csv"

    async def aclose(self) -> None:
        self.closed = True


class ChallengeAuth(httpx.Auth):
    """
    Reads the 401 challenge body, then resends the very request that was
    challenged.
    """

    def __init__(self) -> None:
        self.challenges: list[bytes] = []

    def auth_flow(
        self, request: httpx.Request
    ) -> typing.Generator[httpx.Request, httpx.Response, None]:
        response = yield request
        if response.status_code == 401:
            self.challenges.append(response.read())
            challenged = response.request
            challenged.headers["Authorization"] = "Bearer monthly-token"
            yield challenged


class GzipSyncStream(httpx.SyncByteStream):
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.closed = False

    def __iter__(self) -> typing.Iterator[bytes]:
        yield self._body

    def close(self) -> None:
        self.closed = True


class GzipAsyncStream(httpx.AsyncByteStream):
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.closed = False

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        yield self._body

    async def aclose(self) -> None:
        self.closed = True


def make_sampling(budget: int) -> httpx.RedirectSampling:
    def rule(
        request: httpx.Request, response: httpx.Response
    ) -> tuple[int, bool]:
        if response.request.url.path == "/monthly":
            return 2, True
        return 8, False

    return httpx.RedirectSampling(budget=budget, rule=rule)


class ScenarioState:
    def __init__(self) -> None:
        self.gate_calls = 0
        self.mirror_calls = 0
        self.request_paths: list[str] = []


def build_sync_handler(state: ScenarioState):
    def handler(request: httpx.Request) -> httpx.Response:
        state.request_paths.append(request.url.path)

        if request.url.path == "/monthly":
            body = gzip.compress(MANDATORY_NOTE)
            return httpx.Response(
                302,
                headers={"location": "/gate", "content-encoding": "gzip"},
                stream=GzipSyncStream(body),
            )

        if request.url.path == "/gate":
            if state.gate_calls == 0:
                state.gate_calls += 1
                return httpx.Response(401, content=b"authenticate first")
            state.gate_calls += 1
            return httpx.Response(
                307,
                headers={"location": "/mirror"},
                stream=ChunkThenErrorSyncStream(
                    b"ABCD", httpx.ReadError("second pull failed")
                ),
            )

        if request.url.path == "/mirror":
            state.mirror_calls += 1
            return httpx.Response(302, headers={"location": "/csv"}, content=b"")

        if request.url.path == "/csv":
            return httpx.Response(200, stream=FinalSyncStream())

        raise AssertionError(f"unexpected path {request.url.path!r}")

    return handler


def build_async_handler(state: ScenarioState):
    async def handler(request: httpx.Request) -> httpx.Response:
        state.request_paths.append(request.url.path)

        if request.url.path == "/monthly":
            body = gzip.compress(MANDATORY_NOTE)
            return httpx.Response(
                302,
                headers={"location": "/gate", "content-encoding": "gzip"},
                stream=GzipAsyncStream(body),
            )

        if request.url.path == "/gate":
            if state.gate_calls == 0:
                state.gate_calls += 1
                return httpx.Response(401, content=b"authenticate first")
            state.gate_calls += 1
            return httpx.Response(
                307,
                headers={"location": "/mirror"},
                stream=ChunkThenErrorAsyncStream(
                    b"ABCD", httpx.ReadError("second pull failed")
                ),
            )

        if request.url.path == "/mirror":
            state.mirror_calls += 1
            return httpx.Response(302, headers={"location": "/csv"}, content=b"")

        if request.url.path == "/csv":
            return httpx.Response(200, stream=FinalAsyncStream())

        raise AssertionError(f"unexpected path {request.url.path!r}")

    return handler


def audit_hook(response: httpx.Response) -> None:
    if response.request.url.path == "/monthly":
        # The existing hook still reads and checks the whole decoded note.
        assert response.read() == MANDATORY_NOTE


async def async_audit_hook(response: httpx.Response) -> None:
    if response.request.url.path == "/monthly":
        # The existing hook still reads and checks the whole decoded note.
        assert await response.aread() == MANDATORY_NOTE


def test_sync_successful_fetch_with_six_byte_budget():
    state = ScenarioState()
    events: list[str] = []
    auth = ChallengeAuth()

    def on_request(request: httpx.Request) -> None:
        events.append(f"request:{request.url.path}")

    def on_response(response: httpx.Response) -> None:
        events.append(f"response:{response.url.path}:{response.status_code}")

    client = httpx.Client(
        transport=httpx.MockTransport(build_sync_handler(state)),
        follow_redirects=True,
        redirect_sampling=make_sampling(budget=6),
        event_hooks={"request": [on_request], "response": [on_response]},
    )
    client.event_hooks["response"].insert(0, audit_hook)

    request = client.build_request("GET", "https://example.org/monthly")
    response = client.send(request, stream=True, auth=auth)

    try:
        assert response.status_code == 200
        assert response.url.path == "/csv"
        # The final product stream is handed over unread.
        assert not hasattr(response, "_content")

        # Request/response hooks fire once per actual request, in order.
        assert events == [
            "request:/monthly",
            "response:/monthly:302",
            "request:/gate",
            "response:/gate:401",
            "request:/gate",
            "response:/gate:307",
            "request:/mirror",
            "response:/mirror:302",
            "request:/csv",
            "response:/csv:200",
        ]

        # History keeps every hop both before and after the auth retry.
        assert [item.url.path for item in response.history] == [
            "/monthly",
            "/gate",
            "/gate",
            "/mirror",
        ]
        assert [item.status_code for item in response.history] == [302, 401, 307, 302]

        sampler = response.redirect_sampler
        assert sampler is not None
        assert sampler.remaining == 0

        mandatory, limited, exhausted = sampler.samples
        gzip_body = gzip.compress(MANDATORY_NOTE)

        assert mandatory.mandatory is True
        assert mandatory.max_bytes == 2
        assert mandatory.sample == gzip_body[:2] == b"\x1f\x8b"
        assert mandatory.bytes_pulled == len(gzip_body)
        assert mandatory.stop_reason == httpx.RedirectSampleReason.COMPLETE

        assert limited.mandatory is False
        assert limited.max_bytes == 8
        assert limited.sample == b"ABCD"
        assert limited.bytes_pulled == 4
        assert limited.stop_reason == httpx.RedirectSampleReason.LIMITED

        # The shared budget is exhausted, so this hop is never pulled; its
        # stream end is not observed and it counts as limited.
        assert exhausted.mandatory is False
        assert exhausted.sample == b""
        assert exhausted.bytes_pulled == 0
        assert exhausted.stop_reason == httpx.RedirectSampleReason.LIMITED

        # The challenge body was consumed by the auth flow but not sampled.
        assert auth.challenges == [b"authenticate first"]
        assert len(sampler.samples) == 3

        # Every intermediate response is closed before the next hop.
        for item in response.history:
            assert item.is_closed
    finally:
        response.close()
        client.close()


def test_sync_budget_of_seven_surfaces_the_real_read_error():
    state = ScenarioState()
    client = httpx.Client(
        transport=httpx.MockTransport(build_sync_handler(state)),
        follow_redirects=True,
        redirect_sampling=make_sampling(budget=7),
        event_hooks={"response": [audit_hook]},
    )

    request = client.build_request("GET", "https://example.org/monthly")
    with pytest.raises(httpx.ReadError) as exc_info:
        client.send(request, auth=ChallengeAuth())

    # The fetch cannot continue past the failing hop.
    assert state.mirror_calls == 0
    assert state.request_paths == ["/monthly", "/gate", "/gate"]

    sampler = exc_info.value.redirect_sampler
    assert sampler is not None
    mandatory, failed = sampler.samples

    assert mandatory.sample == b"\x1f\x8b"
    assert mandatory.stop_reason == httpx.RedirectSampleReason.COMPLETE

    assert failed.stop_reason == httpx.RedirectSampleReason.ERRORED
    assert isinstance(failed.error, httpx.ReadError)
    assert str(failed.error) == "second pull failed"
    assert failed.sample == b"ABCD"
    assert failed.bytes_pulled == 4

    client.close()


class FailAfterChunkSyncStream(httpx.SyncByteStream):
    def __init__(self, first: bytes, error: Exception) -> None:
        self._first = first
        self._error = error
        self._delivered = False
        self.closed = False

    def __iter__(self) -> typing.Iterator[bytes]:
        if not self._delivered:
            self._delivered = True
            yield self._first
        raise self._error

    def close(self) -> None:
        self.closed = True


class FailAfterChunkAsyncStream(httpx.AsyncByteStream):
    def __init__(self, first: bytes, error: Exception) -> None:
        self._first = first
        self._error = error
        self._delivered = False
        self.closed = False

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        if not self._delivered:
            self._delivered = True
            yield self._first
        raise self._error

    async def aclose(self) -> None:
        self.closed = True


def test_sync_mandatory_read_failure_keeps_tapped_bytes():
    streams: list[FailAfterChunkSyncStream] = []
    raw = b"\x1f\x8b\x08\x00abcd"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/mandatory-fail":
            stream = FailAfterChunkSyncStream(raw, httpx.ReadError("boom"))
            streams.append(stream)
            return httpx.Response(
                302,
                headers={"location": "/done", "content-encoding": "gzip"},
                stream=stream,
            )
        raise AssertionError("the fetch must not continue")

    def rule(
        request: httpx.Request, response: httpx.Response
    ) -> tuple[int, bool]:
        return 3, True

    def hook(response: httpx.Response) -> None:
        response.read()

    client = httpx.Client(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
        redirect_sampling=httpx.RedirectSampling(budget=6, rule=rule),
        event_hooks={"response": [hook]},
    )

    with pytest.raises(httpx.ReadError) as exc_info:
        client.get("https://example.org/mandatory-fail")

    sampler = exc_info.value.redirect_sampler
    assert sampler is not None
    (record,) = sampler.samples
    assert record.mandatory is True
    assert record.max_bytes == 3
    # The tap retains the raw encoded bytes already delivered.
    assert record.sample == raw[:3]
    assert record.bytes_pulled == len(raw)
    assert record.stop_reason == httpx.RedirectSampleReason.ERRORED
    assert isinstance(record.error, httpx.ReadError)
    assert str(record.error) == "boom"
    # The body is closed before the exception bubbles up.
    assert streams[0].closed is True

    client.close()


@pytest.mark.anyio
async def test_async_mandatory_read_failure_keeps_tapped_bytes():
    streams: list[FailAfterChunkAsyncStream] = []
    raw = b"\x1f\x8b\x08\x00abcd"

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/mandatory-fail":
            stream = FailAfterChunkAsyncStream(raw, httpx.ReadError("boom"))
            streams.append(stream)
            return httpx.Response(
                302,
                headers={"location": "/done", "content-encoding": "gzip"},
                stream=stream,
            )
        raise AssertionError("the fetch must not continue")

    def rule(
        request: httpx.Request, response: httpx.Response
    ) -> tuple[int, bool]:
        return 3, True

    async def hook(response: httpx.Response) -> None:
        await response.aread()

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
        redirect_sampling=httpx.RedirectSampling(budget=6, rule=rule),
        event_hooks={"response": [hook]},
    ) as client:
        with pytest.raises(httpx.ReadError) as exc_info:
            await client.get("https://example.org/mandatory-fail")

        sampler = exc_info.value.redirect_sampler
        assert sampler is not None
        (record,) = sampler.samples
        assert record.mandatory is True
        assert record.sample == raw[:3]
        assert record.bytes_pulled == len(raw)
        assert record.stop_reason == httpx.RedirectSampleReason.ERRORED
        assert isinstance(record.error, httpx.ReadError)
        assert streams[0].closed is True


def test_sync_independent_fetches_have_independent_budgets():
    first_state = ScenarioState()
    client = httpx.Client(
        transport=httpx.MockTransport(build_sync_handler(first_state)),
        follow_redirects=True,
        redirect_sampling=make_sampling(budget=6),
        event_hooks={"response": [audit_hook]},
    )

    first = client.send(
        client.build_request("GET", "https://example.org/monthly"),
        stream=True,
        auth=ChallengeAuth(),
    )
    assert first.redirect_sampler.remaining == 0
    first.close()

    second_state = ScenarioState()
    client._transport = httpx.MockTransport(build_sync_handler(second_state))
    second = client.send(
        client.build_request("GET", "https://example.org/monthly"),
        stream=True,
        auth=ChallengeAuth(),
    )
    try:
        # A fresh fetch gets a fresh budget and a fresh set of records.
        assert second.redirect_sampler is not first.redirect_sampler
        assert second.redirect_sampler.remaining == 0
        assert len(second.redirect_sampler.samples) == 3
        assert second_state.request_paths == [
            "/monthly",
            "/gate",
            "/gate",
            "/mirror",
            "/csv",
        ]
    finally:
        second.close()
        client.close()


class CountingSyncStream(httpx.SyncByteStream):
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.pulls = 0
        self.closed = False

    def __iter__(self) -> typing.Iterator[bytes]:
        self.pulls += 1
        yield self._body

    def close(self) -> None:
        self.closed = True


class CountingAsyncStream(httpx.AsyncByteStream):
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.pulls = 0
        self.closed = False

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        self.pulls += 1
        yield self._body

    async def aclose(self) -> None:
        self.closed = True


def no_drain_sync_handler(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/monthly":
        return httpx.Response(
            302,
            headers={"location": "/csv"},
            stream=CountingSyncStream(b"redirect note"),
        )
    return httpx.Response(200, content=b"ok")


async def no_drain_async_handler(
    request: httpx.Request,
) -> httpx.Response:
    if request.url.path == "/monthly":
        return httpx.Response(
            302,
            headers={"location": "/csv"},
            stream=CountingAsyncStream(b"redirect note"),
        )
    return httpx.Response(200, content=b"ok")


def test_sync_redirect_following_disabled_does_not_sample():
    client = httpx.Client(
        transport=httpx.MockTransport(no_drain_sync_handler),
        follow_redirects=False,
        redirect_sampling=make_sampling(budget=6),
    )

    response = client.send(
        client.build_request("GET", "https://example.org/monthly"), stream=True
    )
    try:
        assert response.status_code == 302
        assert response.next_request is not None
        # Sampling neither drains the body nor creates a hop record.
        assert response.redirect_sample is None
        assert response.stream._stream.pulls == 0
    finally:
        response.close()
        client.close()


def test_sync_sampling_disabled_keeps_original_drain_behavior():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/note":
            return httpx.Response(
                302, headers={"location": "/done"}, content=b"full note body"
            )
        return httpx.Response(200, content=b"ok")

    client = httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=True
    )
    response = client.get("https://example.org/note")
    assert response.history[0].content == b"full note body"
    assert response.redirect_sampler is None
    client.close()


@pytest.mark.anyio
async def test_async_successful_fetch_with_six_byte_budget():
    state = ScenarioState()
    events: list[str] = []
    auth = ChallengeAuth()

    async def on_request(request: httpx.Request) -> None:
        events.append(f"request:{request.url.path}")

    async def on_response(response: httpx.Response) -> None:
        events.append(f"response:{response.url.path}:{response.status_code}")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(build_async_handler(state)),
        follow_redirects=True,
        redirect_sampling=make_sampling(budget=6),
        event_hooks={"request": [on_request], "response": [on_response]},
    ) as client:
        client.event_hooks["response"].insert(0, async_audit_hook)

        request = client.build_request("GET", "https://example.org/monthly")
        response = await client.send(request, stream=True, auth=auth)

        try:
            assert response.status_code == 200
            assert response.url.path == "/csv"
            assert not hasattr(response, "_content")

            assert events == [
                "request:/monthly",
                "response:/monthly:302",
                "request:/gate",
                "response:/gate:401",
                "request:/gate",
                "response:/gate:307",
                "request:/mirror",
                "response:/mirror:302",
                "request:/csv",
                "response:/csv:200",
            ]

            assert [item.url.path for item in response.history] == [
                "/monthly",
                "/gate",
                "/gate",
                "/mirror",
            ]

            sampler = response.redirect_sampler
            assert sampler is not None
            assert sampler.remaining == 0

            mandatory, limited, exhausted = sampler.samples
            gzip_body = gzip.compress(MANDATORY_NOTE)

            assert mandatory.sample == b"\x1f\x8b"
            assert mandatory.bytes_pulled == len(gzip_body)
            assert mandatory.stop_reason == httpx.RedirectSampleReason.COMPLETE

            assert limited.sample == b"ABCD"
            assert limited.bytes_pulled == 4
            assert limited.stop_reason == httpx.RedirectSampleReason.LIMITED

            assert exhausted.bytes_pulled == 0
            assert exhausted.sample == b""
            assert exhausted.stop_reason == httpx.RedirectSampleReason.LIMITED

            assert auth.challenges == [b"authenticate first"]
            for item in response.history:
                assert item.is_closed
        finally:
            await response.aclose()


@pytest.mark.anyio
async def test_async_budget_of_seven_surfaces_the_real_read_error():
    state = ScenarioState()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(build_async_handler(state)),
        follow_redirects=True,
        redirect_sampling=make_sampling(budget=7),
        event_hooks={"response": [async_audit_hook]},
    ) as client:
        request = client.build_request("GET", "https://example.org/monthly")
        with pytest.raises(httpx.ReadError) as exc_info:
            await client.send(request, auth=ChallengeAuth())

        assert state.mirror_calls == 0
        assert state.request_paths == ["/monthly", "/gate", "/gate"]

        sampler = exc_info.value.redirect_sampler
        assert sampler is not None
        mandatory, failed = sampler.samples
        assert mandatory.stop_reason == httpx.RedirectSampleReason.COMPLETE
        assert failed.stop_reason == httpx.RedirectSampleReason.ERRORED
        assert isinstance(failed.error, httpx.ReadError)
        assert failed.sample == b"ABCD"
        assert failed.bytes_pulled == 4


@pytest.mark.anyio
async def test_async_redirect_following_disabled_does_not_sample():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(no_drain_async_handler),
        follow_redirects=False,
        redirect_sampling=make_sampling(budget=6),
    ) as client:
        response = await client.send(
            client.build_request("GET", "https://example.org/monthly"), stream=True
        )
        try:
            assert response.status_code == 302
            assert response.next_request is not None
            assert response.redirect_sample is None
            assert response.stream._stream.pulls == 0
        finally:
            await response.aclose()
