from __future__ import annotations

import typing
from pathlib import Path

import anyio
import pytest

import httpx
from httpx import _exports

pytestmark = pytest.mark.anyio

PROGRESS = "https://platform.test/exports/progress"
MIGRATED = "https://platform.test/exports/replica/progress"
ID_HEADER = "X-Export-Id"
REV_HEADER = "X-Export-Revision"


@pytest.fixture
def anyio_backend() -> str:
    # The harness relies on asyncio task scheduling semantics.
    return "asyncio"


class Clock:
    """A controllable monotonic clock; it only advances through scheduled waits."""

    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.value += seconds
        # Yield control without spending any real time.
        await anyio.sleep(0)


class GatedStream(httpx.AsyncByteStream):
    """Yields a first chunk, signals, blocks on a gate, then yields the rest."""

    def __init__(
        self,
        first: bytes,
        remainder: bytes,
        gate: anyio.Event,
        *,
        signal: anyio.Event | None = None,
        fail_at_end: bool = False,
    ) -> None:
        self._first = first
        self._remainder = remainder
        self._gate = gate
        self._signal = signal
        self._fail_at_end = fail_at_end

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        yield self._first
        if self._signal is not None:
            self._signal.set()
        await self._gate.wait()
        if self._fail_at_end:
            raise httpx.ReadError("pickup stream closed by the platform")
        yield self._remainder

    async def aclose(self) -> None:
        pass


def tracking_headers(export_id: str, revision: int) -> dict[str, str]:
    return {ID_HEADER: export_id, REV_HEADER: str(revision)}


def make_response(
    status_code: int, export_id: str, revision: int, **kwargs: typing.Any
) -> httpx.Response:
    headers = tracking_headers(export_id, revision)
    headers.update(kwargs.pop("headers", {}))
    return httpx.Response(status_code, headers=headers, **kwargs)


class Platform:
    """
    A scripted export platform.

    The first progress responses are handed out from `progress_script`, each
    parked on its own gate so the test fixes the completion order even though
    several requests are in flight at once. Later requests are answered by
    `progress_fallback`. Pickup (artifact) responses come from
    `pickup_overrides`, keyed by URL.
    """

    def __init__(self, progress_script: list[dict[str, typing.Any]]) -> None:
        self._script = list(progress_script)
        self.requests: list[tuple[str, str, str]] = []
        self.pickup_calls: dict[str, int] = {}
        self.pickup_overrides: dict[str, httpx.Response] = {}

    async def handler(self, request: httpx.Request) -> httpx.Response:
        export_id = request.headers[ID_HEADER]
        url = str(request.url)
        self.requests.append((export_id, request.method, url))

        if "/artifact" in url or "/pickup" in url:
            return await self._handle_pickup(url)
        return await self._handle_progress(export_id, url)

    async def _handle_progress(self, export_id: str, url: str) -> httpx.Response:
        if self._script:
            step = self._script.pop(0)
            await step["gate"].wait()
            return step["response"]
        fallback = self.progress_fallback(export_id, url)
        if fallback is not None:
            return fallback
        raise AssertionError(f"unexpected progress request {url} for {export_id}")

    def progress_fallback(self, export_id: str, url: str) -> httpx.Response | None:
        return None

    async def _handle_pickup(self, url: str) -> httpx.Response:
        self.pickup_calls[url] = self.pickup_calls.get(url, 0) + 1
        try:
            return self.pickup_overrides[url]
        except KeyError:
            raise AssertionError(f"unscripted pickup request {url}") from None


def make_manager(
    client: httpx.AsyncClient, clock: Clock
) -> _exports.AsyncExportManager:
    return _exports.AsyncExportManager(client, clock=clock, sleep=clock.sleep)


async def test_overlapping_polls_confirm_only_latest_revision(tmp_path: Path) -> None:
    clock_a = Clock()
    clock_b = Clock()

    g_resp7 = anyio.Event()
    g_resp6 = anyio.Event()
    g_resp8 = anyio.Event()
    g_pickup7 = anyio.Event()
    g_pickup9 = anyio.Event()
    pickup7_midflight = anyio.Event()
    pickup9_midflight = anyio.Event()

    body_rev9 = b"revision-nine-report" * 8

    class APlatform(Platform):
        def progress_fallback(self, export_id: str, url: str) -> httpx.Response | None:
            # The migrated address keeps offering the revision 9 candidate.
            return make_response(303, "A", 9, headers={"Location": "artifact/9"})

    platform = APlatform(
        [
            {
                "gate": g_resp7,
                "response": make_response(
                    303, "A", 7, headers={"Location": "artifact/7"}
                ),
            },
            {
                "gate": g_resp6,
                "response": make_response(202, "A", 6, headers={"Retry-After": "60"}),
            },
            {
                "gate": g_resp8,
                "response": make_response(
                    202,
                    "A",
                    8,
                    headers={"Location": "replica/progress", "Retry-After": "3"},
                ),
            },
        ]
    )
    platform.pickup_overrides = {
        "https://platform.test/exports/artifact/7": httpx.Response(
            200,
            headers=tracking_headers("A", 7),
            stream=GatedStream(
                b"half-of-7",
                b"rest-of-7",
                g_pickup7,
                signal=pickup7_midflight,
                fail_at_end=True,
            ),
        ),
        "https://platform.test/exports/replica/artifact/9": httpx.Response(
            200,
            headers=tracking_headers("A", 9),
            stream=GatedStream(
                body_rev9[:8],
                body_rev9[8:],
                g_pickup9,
                signal=pickup9_midflight,
            ),
        ),
    }

    # Export B runs its own independent schedule on the original address.
    b_gate = anyio.Event()
    b_progress_calls = 0

    class BPlatform(Platform):
        def progress_fallback(self, export_id: str, url: str) -> httpx.Response | None:
            nonlocal b_progress_calls
            b_progress_calls += 1
            if b_progress_calls == 1:
                return make_response(202, "B", 1, headers={"Retry-After": "2"})
            return make_response(303, "B", 2, headers={"Location": "artifact/B"})

    b_platform = BPlatform([{"gate": b_gate, "response": make_response(202, "B", 0)}])
    b_platform.pickup_overrides = {
        "https://platform.test/exports/artifact/B": httpx.Response(
            200, headers=tracking_headers("B", 2), content=b"b-report"
        ),
    }

    async def routed_handler(request: httpx.Request) -> httpx.Response:
        if request.headers[ID_HEADER] == "B":
            return await b_platform.handler(request)
        return await platform.handler(request)

    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    dest_a = archive_dir / "report-A.bin"
    dest_b = archive_dir / "report-B.bin"

    transport = httpx.MockTransport(routed_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        manager_a = make_manager(client, clock_a)
        manager_b = make_manager(client, clock_b)

        a_results: list[_exports.ExportResult] = []

        async def collect_a() -> None:
            a_results.append(await manager_a.archive("A", PROGRESS, dest_a))

        async def run_b() -> None:
            await manager_b.archive("B", PROGRESS, dest_b)

        async with anyio.create_task_group() as task_group:
            # Three overlapping pollers for A, mirroring several in-flight polls.
            for _ in range(3):
                task_group.start_soon(collect_a)
            task_group.start_soon(run_b)

            # Revision 7's 303 completes first and starts its half-finished
            # pickup stream.
            g_resp7.set()
            await pickup7_midflight.wait()

            # Revision 8 lands at monotonic second 10 and migrates A's polling.
            clock_a.value = 10.0
            g_resp8.set()

            # The revision 9 pickup reaches half way and stays in flight.
            await pickup9_midflight.wait()

            # Only hidden temporary files may exist while downloads run: the
            # final archive filename must never appear partially written.
            visible = sorted(p.name for p in archive_dir.iterdir())
            assert "report-A.bin" not in visible
            assert all(name.startswith(".") for name in visible)

            # Let revision 9 finish and become the confirmed artifact.
            g_pickup9.set()

            # Only now do the late revision 6 progress and the failing old
            # revision 7 pickup complete.
            g_resp6.set()
            g_pickup7.set()
            b_gate.set()

    # All three A pollers converge on the single cached revision 9 result.
    assert len(a_results) == 3
    assert all(result.revision == 9 for result in a_results)
    assert all(result.path == dest_a for result in a_results)
    assert dest_a.read_bytes() == body_rev9
    assert sorted(p.name for p in archive_dir.iterdir()) == [
        "report-A.bin",
        "report-B.bin",
    ]

    # Revision 9 was picked up exactly once despite the repeated 303s.
    assert (
        platform.pickup_calls["https://platform.test/exports/replica/artifact/9"] == 1
    )

    a_urls = [url for export_id, _, url in platform.requests if export_id == "A"]
    b_urls = [url for export_id, _, url in platform.requests if export_id == "B"]
    # A migrated onto the relative progress address; B never did.
    assert MIGRATED in a_urls
    assert MIGRATED not in b_urls
    assert all("/exports/replica/" not in url for url in b_urls)

    # Re-querying A returns the cached confirmation with no new traffic.
    requests_before = len(platform.requests)
    cached = await manager_a.archive("A", PROGRESS, dest_a)
    assert cached.revision == 9
    assert cached.path == dest_a
    assert len(platform.requests) == requests_before


async def test_repeated_303_for_current_revision_downloads_once(
    tmp_path: Path,
) -> None:
    clock = Clock()
    release_pickup = anyio.Event()
    body = b"x" * 64

    class DedupPlatform(Platform):
        def progress_fallback(self, export_id: str, url: str) -> httpx.Response | None:
            return make_response(303, "A", 9, headers={"Location": "artifact/9"})

    platform = DedupPlatform([])
    platform.pickup_overrides = {
        "https://platform.test/exports/artifact/9": httpx.Response(
            200,
            headers=tracking_headers("A", 9),
            stream=GatedStream(body[:16], body[16:], release_pickup),
        ),
    }

    dest = tmp_path / "a.bin"
    transport = httpx.MockTransport(platform.handler)
    async with httpx.AsyncClient(transport=transport) as client:
        manager = make_manager(client, clock)

        async with anyio.create_task_group() as task_group:
            for _ in range(3):
                task_group.start_soon(manager.archive, "A", PROGRESS, dest)
            await anyio.sleep(0)
            await anyio.sleep(0)
            # Give duplicate pollers time to prove they park rather than spin.
            await anyio.sleep(0)
            release_pickup.set()

    assert dest.read_bytes() == body
    assert platform.pickup_calls["https://platform.test/exports/artifact/9"] == 1
    # Each archive sends exactly one progress request; duplicate 303s park on
    # the in-flight download instead of hammering the progress endpoint.
    progress_calls = sum(1 for _, _, url in platform.requests if "/artifact" not in url)
    assert progress_calls == 3


async def test_wrong_pickup_identifier_fails_and_recovery_archives(
    tmp_path: Path,
) -> None:
    clock = Clock()

    class WrongIdPlatform(Platform):
        def progress_fallback(self, export_id: str, url: str) -> httpx.Response | None:
            return make_response(303, "A", 9, headers={"Location": "artifact/9"})

    platform = WrongIdPlatform([])
    # The first revision 9 pickup returns a 200 carrying the wrong identifier.
    platform.pickup_overrides = {
        "https://platform.test/exports/artifact/9": httpx.Response(
            200,
            headers=tracking_headers("WRONG", 9),
            content=b"not-for-A",
        ),
    }

    dest = tmp_path / "report-A.bin"
    transport = httpx.MockTransport(platform.handler)
    async with httpx.AsyncClient(transport=transport) as client:
        manager = make_manager(client, clock)

        with pytest.raises(httpx.ExportConfirmationError) as exc_info:
            await manager.archive("A", PROGRESS, dest)

        message = str(exc_info.value)
        assert "Pickup 9" in message
        assert "'WRONG'" in message
        assert exc_info.value.request is not None
        assert str(exc_info.value.request.url).endswith("/artifact/9")

    # Nothing was left behind under the archive name or as a temp file.
    assert not dest.exists()
    assert all(not p.name.endswith(".tmp") for p in tmp_path.iterdir())

    # Re-send the same revision's 303 with a correctly identified pickup.
    platform.pickup_overrides = {
        "https://platform.test/exports/artifact/9": httpx.Response(
            200,
            headers=tracking_headers("A", 9),
            content=b"correct-revision-nine",
        ),
    }
    async with httpx.AsyncClient(transport=transport) as client:
        manager = make_manager(client, clock)
        result = await manager.archive("A", PROGRESS, dest)

    assert result.revision == 9
    assert dest.read_bytes() == b"correct-revision-nine"


async def test_retry_after_scheduled_on_monotonic_clock(tmp_path: Path) -> None:
    clock = Clock()
    seen: list[float] = []

    class SchedulingPlatform(Platform):
        def progress_fallback(self, export_id: str, url: str) -> httpx.Response | None:
            seen.append(clock())
            if len(seen) == 1:
                # A deliberately wrong Date must be ignored; Retry-After alone
                # drives the schedule.
                return make_response(
                    202,
                    "A",
                    1,
                    headers={
                        "Location": "replica/progress",
                        "Retry-After": "3",
                        "Date": "Wed, 01 Jan 2020 00:00:00 GMT",
                    },
                )
            return make_response(303, "A", 2, headers={"Location": "artifact/2"})

    platform = SchedulingPlatform([])
    platform.pickup_overrides = {
        "https://platform.test/exports/replica/artifact/2": httpx.Response(
            200, headers=tracking_headers("A", 2), content=b"ok"
        ),
    }

    dest = tmp_path / "a.bin"
    transport = httpx.MockTransport(platform.handler)
    async with httpx.AsyncClient(transport=transport) as client:
        manager = make_manager(client, clock)
        clock.value = 10.0
        result = await manager.archive("A", PROGRESS, dest)

    assert result.revision == 2
    # First poll at second 10, the Retry-After=3 wait lands the next at 13.
    assert seen == [10.0, 13.0]
    assert any(url == MIGRATED for _, _, url in platform.requests)


async def test_relative_locations_resolved_against_request(tmp_path: Path) -> None:
    class RelativePlatform(Platform):
        def progress_fallback(self, export_id: str, url: str) -> httpx.Response | None:
            if url == PROGRESS:
                return make_response(
                    202, "A", 1, headers={"Location": "replica/progress"}
                )
            return make_response(303, "A", 2, headers={"Location": "artifact/2"})

    platform = RelativePlatform([])
    platform.pickup_overrides = {
        "https://platform.test/exports/replica/artifact/2": httpx.Response(
            200, headers=tracking_headers("A", 2), content=b"ok"
        ),
    }

    dest = tmp_path / "a.bin"
    transport = httpx.MockTransport(platform.handler)
    async with httpx.AsyncClient(transport=transport) as client:
        manager = make_manager(client, Clock())
        await manager.archive("A", PROGRESS, dest)

    urls = [url for _, _, url in platform.requests]
    assert MIGRATED in urls
    assert "https://platform.test/exports/replica/artifact/2" in urls


async def test_regular_requests_keep_their_redirect_behaviour() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/redirect":
            return httpx.Response(303, headers={"Location": "/target"})
        return httpx.Response(200, text="target")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        # Default behaviour: redirects are not followed automatically.
        response = await client.get("https://platform.test/redirect")
        assert response.status_code == 303
        assert response.next_request is not None

        # The export entry point is optional and lazily created.
        manager = client.export_manager()
        assert client.export_manager() is manager

    async with httpx.AsyncClient(
        transport=transport, follow_redirects=True
    ) as following:
        followed = await following.get("https://platform.test/redirect")
        assert followed.status_code == 200
        assert followed.text == "target"
