"""Abstract base class for API-type streams."""

from __future__ import annotations

__lazy_modules__ = {"requests", "singer_sdk.exceptions", "singer_sdk.helpers.types"}

import abc
import sys
import typing as t

from singer_sdk.exceptions import FatalAPIError
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams.graphql_multiroot import (
    GraphQLFieldSelection,
    GraphQLMultiRootPaginator,
    GraphQLMultiRootResponse,
    GraphQLRootConfig,
    GraphQLRootPaginator,
    GraphQLRootResult,
    GraphQLRootState,
    GraphQLRootTracker,
    _normalize_roots,
    analyze_graphql_response,
    attribute_graphql_error,
)
from singer_sdk.streams.rest import RESTStream

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests

    from singer_sdk.helpers.types import Context

_TToken = t.TypeVar("_TToken")


def _coerce_required_stream_fields(
    value: t.Any,
    root: str,
) -> tuple[str, ...] | None:
    if isinstance(value, t.Mapping):
        root_value = value.get(root) or value.get("*")
        if isinstance(root_value, str):
            return (root_value,)
        if root_value:
            return tuple(root_value)
        return None
    if isinstance(value, str):
        return (value,)
    return tuple(value)


class GraphQLStream(RESTStream, abc.ABC, t.Generic[_TToken]):
    """Abstract base class for API-type streams.

    GraphQL streams inherit from the class `GraphQLStream`, which in turn inherits from
    the `RESTStream` class. GraphQL streams are very similar to REST API-based streams,
    but instead of specifying a `path` and `url_params`, developers override the
    GraphQL query text.
    """

    path = ""
    http_method = "POST"

    graphql_multi_root: t.ClassVar[bool] = False
    multi_root: t.ClassVar[bool] = False
    multiple_roots: t.ClassVar[bool] = False
    graphql_roots: t.ClassVar[t.Any] = None
    graphql_root_configs: t.ClassVar[t.Any] = None
    root_configs: t.ClassVar[t.Any] = None
    graphql_root_aliases: t.ClassVar[t.Sequence[str] | None] = None
    root_aliases: t.ClassVar[t.Sequence[str] | None] = None
    aliases: t.ClassVar[t.Sequence[str] | None] = None
    root_cursors: t.Mapping[str, t.Any] | None = None
    required_fields: t.ClassVar[t.Any] = None
    graphql_required_fields: t.ClassVar[t.Any] = None
    root_state_key: t.ClassVar[str] = "graphql_root_cursors"

    _graphql_root_tracker: GraphQLRootTracker | None
    _root_paginator: GraphQLRootPaginator | None
    _root_analysis_cache: tuple[int, GraphQLMultiRootResponse] | None
    _root_context: Context | None

    @property
    def multi_root_enabled(self) -> bool:
        """Whether response handling should isolate aliased GraphQL roots."""
        return (
            self.graphql_multi_root
            or self.multi_root
            or self.multiple_roots
            or bool(self.get_graphql_roots())
        )

    def get_graphql_roots(self) -> dict[str, GraphQLRootConfig]:
        """Return normalized aliased root configuration.

        Returns:
            Root configurations keyed by response alias.
        """
        for attr in (
            "graphql_roots",
            "roots",
            "root_names",
            "graphql_root_names",
            "graphql_root_configs",
            "root_configs",
            "graphql_root_aliases",
            "root_aliases",
            "alias_names",
            "root_fields",
            "aliases",
        ):
            value = getattr(self, attr, None)
            if callable(value):
                value = value()
            if value:
                roots = _normalize_roots(value)
                self._apply_root_path_defaults(roots)
                return roots
        return {}

    def _apply_root_path_defaults(
        self,
        roots: dict[str, GraphQLRootConfig],
    ) -> None:
        records_by_root = getattr(self, "root_records_jsonpath", None) or getattr(
            self,
            "records_jsonpath_by_root",
            None,
        )
        page_info_by_root = getattr(self, "root_page_info_jsonpath", None) or getattr(
            self,
            "page_info_jsonpath_by_root",
            None,
        )
        records_expression = getattr(self, "records_jsonpath", None)
        page_info_expression = getattr(self, "page_info_jsonpath", None)

        for key, config in roots.items():
            if not config.records_jsonpath:
                if isinstance(records_by_root, t.Mapping) and key in records_by_root:
                    config.records_jsonpath = records_by_root[key]
                elif (
                    isinstance(records_expression, str)
                    and "{root}" in records_expression
                ):
                    config.records_jsonpath = records_expression
            if not config.page_info_jsonpath:
                if (
                    isinstance(page_info_by_root, t.Mapping)
                    and key in page_info_by_root
                ):
                    config.page_info_jsonpath = page_info_by_root[key]
                elif isinstance(page_info_expression, str) and (
                    "{root}" in page_info_expression
                ):
                    config.page_info_jsonpath = page_info_expression

    def get_required_fields(self, root: str) -> tuple[str, ...]:
        """Return business-required fields for a root.

        Args:
            root: Root alias.

        Returns:
            Field selectors required to prove root completeness.
        """
        roots = self.get_graphql_roots()
        root_config = roots.get(root)
        if root_config and root_config.required_fields:
            return root_config.required_fields

        for attr in (
            "graphql_required_fields",
            "business_required_fields",
            "required_paths",
            "required_properties",
            "business_required_properties",
            "required_fields",
        ):
            value = getattr(self, attr, None)
            if callable(value):
                value = value(root)
            if not value:
                continue
            fields = _coerce_required_stream_fields(value, root)
            if fields is not None:
                return fields

        schema = getattr(self, "_input_schema", None) or getattr(self, "_schema", None)
        if isinstance(schema, dict) and isinstance(schema.get("required"), list):
            return tuple(schema["required"])
        return ()

    def get_selected_root_fields(self, root: str) -> set[str] | None:
        """Return catalog-selected top-level fields for a root.

        Args:
            root: Root alias.

        Returns:
            Selected field names, or ``None`` when selection is unknown.
        """
        explicit = None
        for attr in (
            "selected_fields",
            "selected_properties",
            "catalog_selected_fields",
        ):
            explicit = getattr(self, attr, None)
            if explicit is not None:
                break
        if explicit is not None:
            if callable(explicit):
                explicit = explicit(root)
            if isinstance(explicit, t.Mapping):
                value = explicit.get(root) or explicit.get("*")
                return set(value) if value is not None else None
            if isinstance(explicit, t.Collection) and not isinstance(explicit, str):
                return set(explicit)

        schema = getattr(self, "_input_schema", None) or getattr(self, "_schema", None)
        if not isinstance(schema, dict):
            return None
        properties = schema.get("properties")
        if not isinstance(properties, dict) or not properties:
            return None
        return {
            field_name
            for field_name in properties
            if self.mask[("properties", field_name)]
        }

    def get_root_field_selection(self, root: str) -> GraphQLFieldSelection:
        """Return selected and known schema fields for error classification.

        Args:
            root: Root alias.

        Returns:
            A field selection descriptor.
        """
        selected = self.get_selected_root_fields(root)
        schema = getattr(self, "_input_schema", None) or getattr(self, "_schema", None)
        properties = schema.get("properties") if isinstance(schema, dict) else None
        known = set(properties) if isinstance(properties, dict) and properties else None
        return GraphQLFieldSelection(selected=selected, known=known)

    def get_initial_root_cursors(
        self,
        context: Context | None = None,
    ) -> dict[str, t.Any]:
        """Return initial per-root cursors from configuration or state.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            Cursors keyed by root alias.
        """
        for attr in (
            "initial_root_cursors",
            "graphql_root_cursors",
            "root_cursors",
        ):
            value = getattr(self, attr, None)
            if callable(value):
                value = value(context)
            if isinstance(value, t.Mapping):
                return dict(value)

        state = (
            self.get_context_state(context)
            if context is not None
            else self.stream_state
        )
        saved = state.get(self.root_state_key)
        if isinstance(saved, t.Mapping):
            return {
                key: value.get("cursor") if isinstance(value, t.Mapping) else value
                for key, value in saved.items()
            }
        return {}

    def get_root_tracker(self, context: Context | None = None) -> GraphQLRootTracker:
        """Get or create the per-root cursor/deduplication tracker.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            The active root tracker.
        """
        tracker = getattr(self, "_graphql_root_tracker", None)
        if tracker is None:
            tracker = GraphQLRootTracker(
                roots=list(self.get_graphql_roots().values()) or None,
                cursors=self.get_initial_root_cursors(context),
                key_properties=self.primary_keys,
            )
            self._graphql_root_tracker = tracker
        return tracker

    @property
    @override
    def records_jsonpath(self) -> str:
        """JSONPath expression to extract records from an API response."""
        return f"$.data.{self.name}[*]"

    @property
    def query(self) -> str:
        """Set or return the GraphQL query string.

        Raises:
            NotImplementedError: If the derived class doesn't define this property.
        """
        msg = "GraphQLStream `query` is not defined."
        raise NotImplementedError(msg)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: _TToken | None,
    ) -> dict[str, t.Any] | str:
        """Build GraphQL variables, using root cursors when provided.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Pagination token, or a mapping of root cursors.

        Returns:
            GraphQL variables.
        """
        if self.multi_root_enabled and isinstance(next_page_token, t.Mapping):
            return dict(next_page_token)
        return super().get_url_params(context, next_page_token)

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: _TToken | None,
    ) -> dict | None:
        """Prepare the data payload for the GraphQL API request.

        Developers generally should generally not need to override this method.
        Instead, developers set the payload by properly configuring the `query`
        attribute.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Dictionary with the body to use for the request.

        Raises:
            ValueError: If the `query` property is not set in the request body.
        """
        params = self.get_url_params(context, next_page_token)
        query = self.query

        if query is None:
            msg = "Graphql `query` property not set."  # type: ignore[unreachable]
            raise ValueError(msg)

        if not query.lstrip().startswith("query"):
            # Wrap text in "query { }" if not already wrapped
            query = "query { " + query + " }"

        query = query.lstrip()
        request_data = {
            "query": (" ".join([line.strip() for line in query.splitlines()])),
            "variables": params,
        }
        self.logger.debug("Attempting query:\n%s", query)
        return request_data

    @override
    def validate_response(self, response: requests.Response) -> None:
        """Validate the GraphQL response.

        GraphQL APIs typically return HTTP 200 even on failure, reporting errors
        in the response body under an ``errors`` key. This extends the parent
        status-code checks to surface those body-level errors.

        Args:
            response: A :class:`requests.Response` object.

        Raises:
            FatalAPIError: If the response body is not valid JSON, is not a
                JSON object, or reports GraphQL errors with no data.
        """
        super().validate_response(response)

        try:
            json_response = response.json()
        except ValueError as e:
            msg = "GraphQL response body is not valid JSON"
            raise FatalAPIError(msg) from e

        if not isinstance(json_response, dict):
            msg = "GraphQL response body must be a JSON object"
            raise FatalAPIError(msg)

        errors = json_response.get("errors")
        if not errors:
            return

        if self.multi_root_enabled:
            analyze_graphql_response(
                json_response,
                roots=list(self.get_graphql_roots().values()) or None,
                strict=True,
            )

        if isinstance(errors, list):
            messages = ", ".join(
                str(e.get("message", e)) if isinstance(e, dict) else str(e)
                for e in errors
            )
        else:
            messages = str(errors)

        msg = f"GraphQL errors in response: {messages}"

        # Partial success: data is present alongside errors. Records can still be
        # extracted, so warn rather than abort. Taps needing retriable handling
        # (e.g. RATE_LIMITED in extensions) should override this method.
        if "data" in json_response and json_response["data"] is not None:
            self.logger.warning(msg)
        else:
            raise FatalAPIError(msg)

    def analyze_root_response(
        self,
        response: requests.Response,
        context: Context | None = None,
    ) -> GraphQLMultiRootResponse:
        """Analyze one response and update per-root continuation positions.

        Args:
            response: A GraphQL HTTP response.
            context: Stream partition or context dictionary.

        Returns:
            The root analysis.
        """
        return self._analyze_root_response(response, context=context)

    process_response = analyze_root_response
    handle_response = analyze_root_response

    def _analyze_root_response(
        self,
        response: requests.Response,
        context: Context | None = None,
    ) -> GraphQLMultiRootResponse:
        cache = getattr(self, "_root_analysis_cache", None)
        response_id = id(response)
        if cache is not None and cache[0] == response_id:
            return t.cast("GraphQLMultiRootResponse", cache[1])

        active_context = context or getattr(self, "_root_context", None)
        tracker = self.get_root_tracker(active_context)
        paginator = getattr(self, "_root_paginator", None)
        request_cursors = (
            paginator.current_value
            if paginator is not None and isinstance(paginator.current_value, dict)
            else tracker.cursors
        )
        roots = set(tracker.configs)
        try:
            payload = response.json()
            if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                roots.update(payload["data"])
        except ValueError:
            pass

        selected_fields = {root: self.get_root_field_selection(root) for root in roots}
        required_fields = {root: self.get_required_fields(root) for root in roots}
        analysis = tracker.handle(
            response,
            request_cursors=request_cursors,
            selected_fields=selected_fields,
            required_fields=required_fields,
            strict=True,
        )
        self._root_analysis_cache = (response_id, analysis)

        state = self.get_context_state(active_context)
        checkpoint = tracker.checkpoint()
        state[self.root_state_key] = checkpoint
        self.root_checkpoint = checkpoint
        self.root_cursors = tracker.cursors
        return analysis

    @override
    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse records, isolating complete aliased roots when enabled.

        Args:
            response: A raw :class:`requests.Response`.

        Yields:
            Records from roots which are complete and not already delivered.
        """
        if not self.multi_root_enabled:
            yield from super().parse_response(response)
            return

        analysis = self._analyze_root_response(response)
        for result in analysis.results.values():
            if result.complete:
                yield from result.new_records

    @override
    def get_new_paginator(self) -> BaseAPIPaginator[t.Any] | None:
        """Get a paginator that advances each GraphQL root independently.

        Returns:
            A root-aware paginator, or the default GraphQL/REST paginator.
        """
        if not self.multi_root_enabled:
            return super().get_new_paginator()

        context = getattr(self, "_root_context", None)
        tracker = self.get_root_tracker(context)
        paginator = GraphQLRootPaginator(self, tracker)
        self._root_paginator = paginator
        return paginator

    @override
    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records with a fresh per-context root tracker.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Records from complete GraphQL roots.
        """
        if self.multi_root_enabled:
            self._root_context = context
            self._graphql_root_tracker = None
            self._root_paginator = None
            self._root_analysis_cache = None
        yield from super().request_records(context)


class MultiRootGraphQLStream(GraphQLStream, abc.ABC):
    """GraphQL stream with independent aliased-root pagination enabled."""

    graphql_multi_root = True


GraphQLMultiRootStream = MultiRootGraphQLStream
GraphQLMultiAliasStream = MultiRootGraphQLStream
MultiAliasGraphQLStream = MultiRootGraphQLStream
GraphQLAliasStream = MultiRootGraphQLStream
AliasedGraphQLStream = MultiRootGraphQLStream
GraphQLRootStream = MultiRootGraphQLStream

GraphQLPaginator = GraphQLRootPaginator
RootConfig = GraphQLRootConfig
RootState = GraphQLRootState
RootResult = GraphQLRootResult
RootTracker = GraphQLRootTracker
MultiRootPaginator = GraphQLMultiRootPaginator
process_graphql_response = analyze_graphql_response
parse_multi_root_response = analyze_graphql_response
get_error_root = attribute_graphql_error
attribute_error_to_root = attribute_graphql_error

__all__ = [
    "AliasedGraphQLStream",
    "GraphQLAliasStream",
    "GraphQLFieldSelection",
    "GraphQLMultiAliasStream",
    "GraphQLMultiRootPaginator",
    "GraphQLMultiRootResponse",
    "GraphQLMultiRootStream",
    "GraphQLPaginator",
    "GraphQLRootConfig",
    "GraphQLRootPaginator",
    "GraphQLRootResult",
    "GraphQLRootState",
    "GraphQLRootTracker",
    "GraphQLStream",
    "MultiAliasGraphQLStream",
    "MultiRootGraphQLStream",
    "MultiRootPaginator",
    "RootConfig",
    "RootResult",
    "RootState",
    "RootTracker",
    "attribute_graphql_error",
    "parse_multi_root_response",
    "process_graphql_response",
]
