"""Tests for OAuth token generation tracking and coordinated refresh/retry.

The simulated OAuth and report endpoints in this module stand in for two
report endpoints that share credentials and a third authenticator with a
different scope. They verify that:

- every authenticated request can be attributed to the token generation it
  actually carried and the number of applied refreshes is tracked,
- concurrent expiry on shared credentials results in a single token update
  while different scopes still refresh independently,
- a late 401 for an already-rotated token only retries that request and never
  discards the token currently in effect,
- a 401 for the current token triggers one refresh and a successful retry,
- rotating, single-use refresh tokens are never sent twice,
- a request that keeps failing does not silently advance pagination.
"""

from __future__ import annotations

import datetime
import sys
import threading
import typing as t
from http import HTTPStatus
from urllib.parse import parse_qsl

import backoff
import pytest
import requests_mock

from singer_sdk.authenticators import (
    REQUEST_TOKEN_GENERATION_ATTR,
    OAuthAuthenticator,
)
from singer_sdk.exceptions import RetriableAuthAPIError
from singer_sdk.helpers._util import utc_now
from singer_sdk.pagination import SimpleHeaderPaginator
from singer_sdk.streams.rest import RESTStream

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests

    from singer_sdk.tap_base import Tap

OAUTH_ENDPOINT = "https://example.com/oauth"
REPORT_ENDPOINT_A = "https://example.com/reports/a"
REPORT_ENDPOINT_B = "https://example.com/reports/b"


class SimulatedOAuthServer:
    """In-memory OAuth 2.0 server with rotating, single-use refresh tokens."""

    def __init__(self, *, expires_in: int = 3600) -> None:
        self.expires_in = expires_in
        self._access_tokens: dict[str, str] = {}
        self._refresh_tokens: dict[str, str] = {}
        self._counters: dict[str, int] = {}
        self.token_requests: list[dict[str, str]] = []
        # Snapshotted at request time, since retries mutate the same prepared
        # request object and request history would only show its final state.
        self.report_calls: list[dict[str, t.Any]] = []
        self.force_reject = False
        self.reject_tokens: set[str] = set()

    def _scope_key(self, scope: str | None) -> str:
        return scope or "default"

    def seed(self, scope: str | None = None) -> tuple[str, str]:
        """Seed an initial token pair, as if provisioned out of band."""
        key = self._scope_key(scope)
        self._counters[key] = 1
        access_token = f"access-{key}-1"
        refresh_token = f"refresh-{key}-1"
        self._access_tokens[key] = access_token
        self._refresh_tokens[key] = refresh_token
        return access_token, refresh_token

    def token_handler(
        self,
        request: requests.Request,
        context: t.Any,
    ) -> dict[str, t.Any]:
        """Handle a token request, enforcing refresh token rotation."""
        body = dict(parse_qsl(request.body))
        scope = body.get("scope")
        key = self._scope_key(scope)
        self.token_requests.append(body)

        if body.get("grant_type") == "refresh_token":
            presented = body.get("refresh_token")
            if presented != self._refresh_tokens.get(key):
                context.status_code = HTTPStatus.UNAUTHORIZED
                return {"error": "invalid_grant"}

        number = self._counters.get(key, 0) + 1
        self._counters[key] = number
        access_token = f"access-{key}-{number}"
        refresh_token = f"refresh-{key}-{number}"
        self._access_tokens[key] = access_token
        self._refresh_tokens[key] = refresh_token
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": self.expires_in,
        }

    def report_handler(
        self,
        request: requests.Request,
        context: t.Any,
    ) -> list[dict[str, t.Any]]:
        """Return 401 for any token other than the current one."""
        scope = request.headers.get("X-OAuth-Scope")
        expected = self._access_tokens.get(self._scope_key(scope))
        presented = request.headers.get("Authorization", "").removeprefix("Bearer ")
        self.report_calls.append(
            {
                "url": request.url,
                "generation": getattr(
                    request,
                    REQUEST_TOKEN_GENERATION_ATTR,
                    None,
                ),
                "authorization": request.headers.get("Authorization"),
                "accepted": presented == expected,
            },
        )
        if (
            presented != expected
            or self.force_reject
            or presented in self.reject_tokens
        ):
            context.status_code = HTTPStatus.UNAUTHORIZED
            return [{"error": "invalid_token"}]
        return [{"id": 1}]


class _TestOAuthAuthenticator(OAuthAuthenticator):
    """OAuth authenticator with an explicit scope in its token request body."""

    def __init__(self, *args: t.Any, scope: str | None = None, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self._scope = scope

    @override
    @property
    def oauth_request_body(self) -> dict:
        grant_type = "refresh_token" if self.refresh_token else "client_credentials"
        body: dict[str, t.Any] = {"grant_type": grant_type}
        if self.refresh_token:
            body["refresh_token"] = self.refresh_token
        if self._scope:
            body["scope"] = self._scope
        return body


class _ReportStream(RESTStream):
    """Report stream with pluggable authenticator and no backoff delay."""

    url_base = "https://example.com"
    schema: t.ClassVar[dict] = {
        "type": "object",
        "properties": {"id": {"type": "integer"}},
    }
    backoff_max_tries = 3

    def __init__(
        self,
        *args: t.Any,
        authenticator: OAuthAuthenticator,
        path: str,
        scope: str | None = None,
        paginated: bool = False,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(*args, name=path.strip("/").replace("/", "-"), **kwargs)
        self.path = path
        self._test_authenticator = authenticator
        self._scope = scope
        self._paginated = paginated

    @property
    def http_headers(self) -> dict:
        headers = dict(super().http_headers)
        if self._scope:
            headers["X-OAuth-Scope"] = self._scope
        return headers

    @override
    @property
    def authenticator(self) -> OAuthAuthenticator:
        return self._test_authenticator

    @override
    def get_new_paginator(self) -> SimpleHeaderPaginator | None:
        if not self._paginated:
            return None
        return SimpleHeaderPaginator("X-Next-Page")

    @override
    def backoff_wait_generator(self) -> t.Generator[float, None, None]:
        return backoff.constant(0)

    @override
    def backoff_jitter(self, value: float) -> float:
        return 0


def _make_authenticator(
    rest_tap: Tap,
    server: SimulatedOAuthServer,
    *,
    scope: str | None = None,
    auth_endpoint: str = OAUTH_ENDPOINT,
) -> _TestOAuthAuthenticator:
    authenticator = _TestOAuthAuthenticator(
        stream=rest_tap.streams["some_stream"],
        auth_endpoint=auth_endpoint,
        oauth_scopes=scope,
        scope=scope,
        default_expiration=3600,
    )
    access_token, refresh_token = server.seed(scope)
    authenticator.access_token = access_token
    authenticator.refresh_token = refresh_token
    authenticator.expires_in = 3600
    authenticator.last_refreshed = utc_now()
    authenticator.token_generation = 1
    return authenticator


def _expire(authenticator: OAuthAuthenticator) -> None:
    authenticator.last_refreshed = utc_now() - datetime.timedelta(hours=1)


def _report_generations(
    server: SimulatedOAuthServer,
    url: str,
) -> list[int | None]:
    return [call["generation"] for call in server.report_calls if call["url"] == url]


def _send_stale_then_delegate(stream: _ReportStream, stale_request: requests.Request):
    """Build a request function whose first attempt uses an in-flight request.

    The stale request is sent exactly as authenticated before the token was
    rotated (so its 401 response carries the old generation), while subsequent
    retries go through the stream's normal request path and re-authenticate
    with the token currently held.
    """
    real_request = stream._request
    attempts = {"count": 0}

    def _request(prepared_request: requests.PreparedRequest, context: t.Any = None):
        attempts["count"] += 1
        if attempts["count"] == 1:
            response = stream.requests_session.send(stale_request)
            stream.validate_response(response)
            return response
        return real_request(prepared_request, context)

    return _request


def test_authenticated_request_carries_token_generation(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
):
    """Each request is tagged with the generation of the token it carries."""
    server = SimulatedOAuthServer()
    requests_mock.post(OAUTH_ENDPOINT, json=server.token_handler)
    requests_mock.get(REPORT_ENDPOINT_A, json=server.report_handler)

    authenticator = _make_authenticator(rest_tap, server)
    stream = _ReportStream(rest_tap, authenticator=authenticator, path="/reports/a")

    assert authenticator.token_generation == 1
    assert authenticator.refresh_count == 0

    records = list(stream.get_records(None))

    assert records == [{"id": 1}]
    assert authenticator.token_generation == 1
    assert authenticator.refresh_count == 0  # Seeded token was still valid
    assert _report_generations(server, REPORT_ENDPOINT_A) == [1]
    assert server.report_calls[-1]["authorization"] == (
        f"Bearer {authenticator.access_token}"
    )


def test_concurrent_expiry_refreshes_once_for_shared_credentials(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
):
    """Concurrent callers on one authenticator perform a single refresh."""
    server = SimulatedOAuthServer()
    requests_mock.post(OAUTH_ENDPOINT, json=server.token_handler)
    requests_mock.get(REPORT_ENDPOINT_A, json=server.report_handler)
    requests_mock.get(REPORT_ENDPOINT_B, json=server.report_handler)

    authenticator = _make_authenticator(rest_tap, server)
    stream_a = _ReportStream(rest_tap, authenticator=authenticator, path="/reports/a")
    stream_b = _ReportStream(rest_tap, authenticator=authenticator, path="/reports/b")
    _expire(authenticator)

    thread_count = 8
    barrier = threading.Barrier(thread_count)
    errors: list[Exception] = []

    def _run(stream: _ReportStream) -> None:
        try:
            barrier.wait()
            list(stream.get_records(None))
        except Exception as exc:  # noqa: BLE001, pragma: no cover - assertion
            errors.append(exc)

    threads = [
        threading.Thread(
            target=_run,
            args=(stream_a if index % 2 == 0 else stream_b,),
        )
        for index in range(thread_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(server.token_requests) == 1
    assert authenticator.refresh_count == 1
    assert authenticator.token_generation == 2
    generations = _report_generations(server, REPORT_ENDPOINT_A)
    generations += _report_generations(server, REPORT_ENDPOINT_B)
    assert generations == [2] * thread_count


def test_different_scopes_refresh_independently(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
):
    """Separate scopes keep separate authenticators and separate refreshes."""
    server = SimulatedOAuthServer()
    requests_mock.post(OAUTH_ENDPOINT, json=server.token_handler)
    requests_mock.get(REPORT_ENDPOINT_A, json=server.report_handler)
    requests_mock.get(REPORT_ENDPOINT_B, json=server.report_handler)

    auth_read = _make_authenticator(rest_tap, server, scope="read")
    auth_write = _make_authenticator(rest_tap, server, scope="write")
    stream_read = _ReportStream(
        rest_tap,
        authenticator=auth_read,
        path="/reports/a",
        scope="read",
    )
    stream_write = _ReportStream(
        rest_tap,
        authenticator=auth_write,
        path="/reports/b",
        scope="write",
    )
    _expire(auth_read)
    _expire(auth_write)

    barrier = threading.Barrier(2)
    errors: list[Exception] = []

    def _run(stream: _ReportStream) -> None:
        try:
            barrier.wait()
            list(stream.get_records(None))
        except Exception as exc:  # noqa: BLE001, pragma: no cover - assertion
            errors.append(exc)

    threads = [
        threading.Thread(target=_run, args=(stream_read,)),
        threading.Thread(target=_run, args=(stream_write,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    scopes = [body.get("scope") for body in server.token_requests]
    assert sorted(scopes) == ["read", "write"]
    assert auth_read.refresh_count == 1
    assert auth_write.refresh_count == 1
    assert auth_read.access_token != auth_write.access_token


def test_stale_401_retries_without_discarding_current_token(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
):
    """A late 401 for an old token only retries; the new token stays in effect."""
    server = SimulatedOAuthServer()
    requests_mock.post(OAUTH_ENDPOINT, json=server.token_handler)
    requests_mock.get(REPORT_ENDPOINT_A, json=server.report_handler)

    authenticator = _make_authenticator(rest_tap, server)
    stream = _ReportStream(rest_tap, authenticator=authenticator, path="/reports/a")

    # The request is built and authenticated while generation 1 is current.
    stale_request = stream.build_prepared_request(method="GET", url=REPORT_ENDPOINT_A)
    authenticator.authenticate_request(stale_request)
    assert getattr(stale_request, REQUEST_TOKEN_GENERATION_ATTR) == 1
    stale_token = authenticator.access_token

    # Another flow rotates the token before the stale response arrives.
    _expire(authenticator)
    authenticator.ensure_valid_token()
    assert authenticator.token_generation == 2
    assert authenticator.access_token != stale_token
    token_requests_before = len(server.token_requests)

    # refresh_if_stale on the old generation must not touch the current token.
    assert authenticator.refresh_if_stale(1) is False
    assert len(server.token_requests) == token_requests_before
    assert authenticator.token_generation == 2

    # End to end: the stale request 401s, then retries with the current token.
    decorated = stream.request_decorator(
        _send_stale_then_delegate(stream, stale_request),
    )
    fresh_request = stream.build_prepared_request(
        method="GET",
        url=REPORT_ENDPOINT_A,
    )
    response = decorated(fresh_request)

    assert response.status_code == HTTPStatus.OK
    assert len(server.token_requests) == token_requests_before
    assert authenticator.token_generation == 2
    data_calls = [
        call for call in server.report_calls if call["url"] == REPORT_ENDPOINT_A
    ]
    assert [call["generation"] for call in data_calls] == [1, 2]
    assert data_calls[0]["authorization"] == f"Bearer {stale_token}"
    assert data_calls[1]["authorization"] == (f"Bearer {authenticator.access_token}")


def test_401_on_current_token_refreshes_and_retries(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
):
    """A genuine rejection of the current token refreshes it then succeeds."""
    server = SimulatedOAuthServer()
    requests_mock.post(OAUTH_ENDPOINT, json=server.token_handler)

    authenticator = _make_authenticator(rest_tap, server)
    original_refresh_token = authenticator.refresh_token
    # Reject the seeded generation once; the rotated token is accepted normally.
    server.reject_tokens.add("access-default-1")

    requests_mock.get(REPORT_ENDPOINT_A, json=server.report_handler)
    stream = _ReportStream(rest_tap, authenticator=authenticator, path="/reports/a")

    records = list(stream.get_records(None))

    assert records == [{"id": 1}]
    assert authenticator.token_generation == 2
    assert authenticator.refresh_count == 1
    assert len(server.token_requests) == 1
    refresh_body = server.token_requests[0]
    assert refresh_body["grant_type"] == "refresh_token"
    assert refresh_body["refresh_token"] == original_refresh_token
    assert authenticator.refresh_token == "refresh-default-2"
    assert _report_generations(server, REPORT_ENDPOINT_A) == [1, 2]


def test_rotating_refresh_token_is_never_reused(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
):
    """The simulated server rejects reused refresh tokens; dedup keeps us valid."""
    server = SimulatedOAuthServer()
    requests_mock.post(OAUTH_ENDPOINT, json=server.token_handler)

    authenticator = _make_authenticator(rest_tap, server)
    _expire(authenticator)
    authenticator.ensure_valid_token()
    assert authenticator.refresh_token == "refresh-default-2"

    # A second, genuinely needed refresh uses the rotated token and succeeds.
    _expire(authenticator)
    authenticator.ensure_valid_token()
    assert authenticator.refresh_token == "refresh-default-3"
    assert [body["refresh_token"] for body in server.token_requests] == [
        "refresh-default-1",
        "refresh-default-2",
    ]


def test_refresh_if_stale_deduplicates_concurrent_callers(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
):
    """Concurrent 401 handling for one generation performs a single refresh."""
    server = SimulatedOAuthServer()
    requests_mock.post(OAUTH_ENDPOINT, json=server.token_handler)

    authenticator = _make_authenticator(rest_tap, server)
    assert authenticator.token_generation == 1

    entered = threading.Event()
    release = threading.Event()
    original_update = authenticator.update_access_token

    def blocking_update() -> None:
        entered.set()
        assert release.wait(timeout=5)
        original_update()

    authenticator.update_access_token = blocking_update  # type: ignore[method-assign]

    barrier = threading.Barrier(2)
    results: list[bool] = []
    results_lock = threading.Lock()

    def _handle_401() -> None:
        barrier.wait()
        refreshed = authenticator.refresh_if_stale(1)
        with results_lock:
            results.append(refreshed)

    threads = [threading.Thread(target=_handle_401) for _ in range(2)]
    for thread in threads:
        thread.start()
    assert entered.wait(timeout=5)
    release.set()
    for thread in threads:
        thread.join()

    assert sorted(results) == [False, True]
    assert len(server.token_requests) == 1
    assert authenticator.refresh_count == 1
    assert authenticator.token_generation == 2


def test_repeated_401_does_not_advance_pagination(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
):
    """A request that exhausts retries leaves the paginator exactly where it was."""
    server = SimulatedOAuthServer()
    requests_mock.post(OAUTH_ENDPOINT, json=server.token_handler)
    requests_mock.get(REPORT_ENDPOINT_A, json=server.report_handler)

    authenticator = _make_authenticator(rest_tap, server)
    server.force_reject = True
    stream = _ReportStream(
        rest_tap,
        authenticator=authenticator,
        path="/reports/a",
        paginated=True,
    )
    paginator = stream.get_new_paginator()
    assert paginator is not None
    # Simulate being midway through a sync.
    paginator._value = "cursor-2"  # noqa: SLF001
    prepared = stream._prepare_request(context=None, page=paginator)
    decorated = stream.request_decorator(stream._request)

    with pytest.raises(RetriableAuthAPIError):
        decorated(prepared, None)

    assert paginator.current_value == "cursor-2"
    assert paginator.count == 0
    assert not paginator.finished
    # Each 401 on a current generation triggers one refresh, then retries.
    assert authenticator.refresh_count == 2
    assert _report_generations(server, REPORT_ENDPOINT_A) == [1, 2, 3]


def test_shared_credentials_two_report_endpoints_stale_401_scenario(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
):
    """Endpoint B refreshes first; endpoint A's stale 401 only retries its request."""
    server = SimulatedOAuthServer()
    requests_mock.post(OAUTH_ENDPOINT, json=server.token_handler)
    requests_mock.get(REPORT_ENDPOINT_A, json=server.report_handler)
    requests_mock.get(REPORT_ENDPOINT_B, json=server.report_handler)

    authenticator = _make_authenticator(rest_tap, server)
    stream_a = _ReportStream(rest_tap, authenticator=authenticator, path="/reports/a")
    stream_b = _ReportStream(rest_tap, authenticator=authenticator, path="/reports/b")

    # Endpoint A's request is prepared with the old, about-to-be-replaced token.
    stale_request = stream_a.build_prepared_request(method="GET", url=REPORT_ENDPOINT_A)
    authenticator.authenticate_request(stale_request)

    # Endpoint B hits an expired token first and performs the single refresh.
    _expire(authenticator)
    records_b = list(stream_b.get_records(None))
    assert records_b == [{"id": 1}]
    assert authenticator.token_generation == 2
    assert len(server.token_requests) == 1

    # Endpoint A's stale response arrives late, 401s, then retries on gen 2.
    decorated = stream_a.request_decorator(
        _send_stale_then_delegate(stream_a, stale_request),
    )
    fresh_request = stream_a.build_prepared_request(
        method="GET",
        url=REPORT_ENDPOINT_A,
    )
    response = decorated(fresh_request)
    assert response.status_code == HTTPStatus.OK

    assert len(server.token_requests) == 1
    assert authenticator.refresh_count == 1
    assert _report_generations(server, REPORT_ENDPOINT_A) == [1, 2]
    assert _report_generations(server, REPORT_ENDPOINT_B) == [2]
