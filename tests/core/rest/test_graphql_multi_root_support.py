"""Tests for aliased, multi-root GraphQL response handling."""

from __future__ import annotations

import json
import typing as t

import pytest
import requests

from singer_sdk.exceptions import FatalAPIError
from singer_sdk.streams.graphql import (
    GraphQLRootTracker,
    MultiRootGraphQLStream,
    analyze_graphql_response,
)


class ProductStream(MultiRootGraphQLStream):
    """GraphQL stream with two aliased store roots."""

    name = "products"
    url_base = "https://example.com"
    query = "query Products { alpha: storeA { products { nodes { id } } } }"
    graphql_roots = ("alpha", "beta")
    records_jsonpath = "$.data.{root}.products.nodes[*]"
    page_info_jsonpath = "$.data.{root}.products.pageInfo"
    primary_keys = ("id",)
    schema: t.ClassVar[dict] = {
        "type": "object",
        "properties": {"id": {"type": "string"}},
    }


def _response(body: dict) -> requests.Response:
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps(body).encode()
    return response


def _page(
    records: list[dict],
    *,
    start: str,
    end: str,
    has_next: bool,
) -> dict:
    return {
        "nodes": records,
        "pageInfo": {
            "startCursor": start,
            "endCursor": end,
            "hasNextPage": has_next,
        },
    }


def test_complete_and_incomplete_roots_have_independent_cursors() -> None:
    payload = {
        "data": {
            "alpha": {
                "products": _page(
                    [{"id": "a1"}],
                    start="a1",
                    end="a2",
                    has_next=True,
                )
            },
            "beta": None,
        },
        "errors": [
            {
                "message": "beta unavailable",
                "path": ["beta", "products", "nodes", 0, "name"],
            },
        ],
    }

    analysis = analyze_graphql_response(
        payload,
        ["alpha", "beta"],
        selected_fields={"*": {"id"}},
        required_fields={"*": ("id",)},
        cursors={"alpha": None, "beta": "beta-old"},
        strict=False,
    )

    assert analysis.complete_roots["alpha"].records == [{"id": "a1"}]
    assert not analysis.results["beta"].complete
    assert analysis.cursors == {"alpha": "a2", "beta": "beta-old"}
    assert analysis.pending_cursors == {"beta": "beta-old"}


def test_unselected_non_required_field_error_does_not_block_root() -> None:
    payload = {
        "data": {
            "beta": {
                "products": _page(
                    [{"id": "b1"}],
                    start="b1",
                    end="b2",
                    has_next=False,
                )
            },
        },
        "errors": [
            {
                "message": "name failed",
                "path": ["beta", "products", "nodes", 0, "name"],
            },
        ],
    }

    analysis = analyze_graphql_response(
        payload,
        ["beta"],
        selected_fields={"beta": {"id"}},
        required_fields={"beta": ("id",)},
    )

    assert analysis.results["beta"].complete
    assert analysis.records == [{"id": "b1"}]


def test_unattributed_error_stops_response() -> None:
    payload = {
        "data": {"alpha": None},
        "errors": [{"message": "query rate limit"}],
    }

    with pytest.raises(FatalAPIError, match="could not be attributed"):
        analyze_graphql_response(payload, ["alpha"])


def test_tracker_deduplicates_replayed_root_page() -> None:
    tracker = GraphQLRootTracker(["alpha", "beta"], key_properties=("id",))
    first = _response(
        {
            "data": {
                "alpha": {
                    "products": _page(
                        [{"id": "a1"}],
                        start="a1",
                        end="a2",
                        has_next=True,
                    )
                },
                "beta": None,
            },
            "errors": [{"path": ["beta"], "message": "retry"}],
        },
    )
    replay = _response(
        {
            "data": {
                "alpha": {
                    "products": _page(
                        [{"id": "a1"}],
                        start="a1",
                        end="a2",
                        has_next=True,
                    )
                },
                "beta": {
                    "products": _page(
                        [{"id": "b1"}],
                        start="b0",
                        end="b1",
                        has_next=False,
                    )
                },
            },
        },
    )

    first_analysis = tracker.handle(first, strict=False)
    assert first_analysis.records == [{"id": "a1"}]
    tracker.states["beta"].cursor = "beta-old"

    replay_analysis = tracker.handle(replay, strict=False)
    assert replay_analysis.results["alpha"].new_records == []
    assert replay_analysis.results["beta"].new_records == [{"id": "b1"}]


def test_stream_request_loop_advances_only_successful_root(rest_tap) -> None:
    stream = ProductStream(rest_tap)
    first = _response(
        {
            "data": {
                "alpha": {
                    "products": _page(
                        [{"id": "a1"}],
                        start="a1",
                        end="a2",
                        has_next=True,
                    )
                },
                "beta": None,
            },
            "errors": [{"path": ["beta"], "message": "retry beta"}],
        },
    )
    retry = _response(
        {
            "data": {
                "alpha": {
                    "products": _page(
                        [{"id": "a1"}],
                        start="a1",
                        end="a2",
                        has_next=True,
                    )
                },
                "beta": {
                    "products": _page(
                        [{"id": "b1"}],
                        start="b0",
                        end="b1",
                        has_next=False,
                    )
                },
            },
        },
    )
    next_alpha = _response(
        {
            "data": {
                "alpha": {
                    "products": _page(
                        [{"id": "a2"}],
                        start="a2",
                        end="a3",
                        has_next=False,
                    )
                },
                "beta": {
                    "products": _page(
                        [],
                        start="b1",
                        end="b1",
                        has_next=False,
                    )
                },
            },
        },
    )
    responses = iter([first, retry, next_alpha])
    variables: list[dict] = []

    def fake_request(prepared_request, context=None):  # noqa: ANN001
        response = next(responses)
        body = json.loads(prepared_request.body)
        variables.append(body["variables"])
        stream.validate_response(response)
        return response

    stream._request = fake_request  # type: ignore[method-assign]

    records = list(stream.request_records(None))

    assert records == [{"id": "a1"}, {"id": "b1"}, {"id": "a2"}]
    assert variables == [
        {"alpha": None, "beta": None},
        {"alpha": "a2", "beta": None},
        {"alpha": "a2", "beta": "b1"},
    ]
