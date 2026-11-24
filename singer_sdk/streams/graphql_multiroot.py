"""Helpers for GraphQL responses that contain several aliased root fields."""

from __future__ import annotations

import json
import sys
import typing as t
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal

from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests

__all__ = [
    "GraphQLFieldSelection",
    "GraphQLMultiRootPaginator",
    "GraphQLMultiRootResponse",
    "GraphQLRootCheckpoint",
    "GraphQLRootConfig",
    "GraphQLRootPaginator",
    "GraphQLRootResult",
    "GraphQLRootState",
    "GraphQLRootTracker",
    "analyze_graphql_response",
    "attribute_graphql_error",
]

RootKey: t.TypeAlias = str
Cursor: t.TypeAlias = t.Any
FieldSelector: t.TypeAlias = str | t.Sequence[str]


@dataclass(frozen=True, slots=True)
class GraphQLFieldSelection:
    """Catalog selection and all known schema properties for one root."""

    selected: t.Collection[str] | None = None
    known: t.Collection[str] | None = None


SelectedFields: t.TypeAlias = (
    t.Collection[str]
    | GraphQLFieldSelection
    | t.Mapping[str, t.Collection[str] | GraphQLFieldSelection]
    | None
)
RequiredFields: t.TypeAlias = (
    FieldSelector
    | t.Collection[FieldSelector]
    | t.Mapping[str, FieldSelector | t.Collection[FieldSelector]]
    | None
)

_STRUCTURAL_PATH_TOKENS = frozenset({"edges", "node", "nodes", "items", "records"})


@dataclass(slots=True)
class GraphQLRootConfig:
    """Configuration describing one aliased GraphQL root field."""

    key: RootKey
    records_jsonpath: str | None = None
    page_info_jsonpath: str | None = None
    cursor_key: str = "endCursor"
    has_next_page_key: str = "hasNextPage"
    required_fields: tuple[str, ...] = ()

    def __init__(  # noqa: PLR0913
        self,
        key: RootKey | None = None,
        *,
        alias: RootKey | None = None,
        name: RootKey | None = None,
        root: RootKey | None = None,
        root_alias: RootKey | None = None,
        records_jsonpath: str | None = None,
        records_path: str | None = None,
        page_info_jsonpath: str | None = None,
        page_info_path: str | None = None,
        cursor_key: str | None = None,
        has_next_page_key: str | None = None,
        required_fields: t.Collection[str] | None = None,
        required: t.Collection[str] | None = None,
        **_extra_options: t.Any,
    ) -> None:
        """Initialize a root configuration.

        Args:
            key: Key of the root in the GraphQL ``data`` object.
            alias: Alias for ``key``.
            name: Alias for ``key``.
            root: Alias for ``key``.
            root_alias: Alias for ``key``.
            records_jsonpath: JSONPath to records for this root.
            records_path: Alias for ``records_jsonpath``.
            page_info_jsonpath: JSONPath to the root's ``pageInfo`` object.
            page_info_path: Alias for ``page_info_jsonpath``.
            cursor_key: Key in ``pageInfo`` containing the continuation cursor.
            has_next_page_key: Key in ``pageInfo`` indicating more pages.
            required_fields: Fields which must be present for a complete root.
            required: Alias for ``required_fields``.
            **_extra_options: Additional ignored configuration options.
        """
        resolved_key = key or alias or name or root or root_alias
        if resolved_key is None:
            msg = "A GraphQL root key is required"
            raise ValueError(msg)

        records_jsonpath = records_jsonpath or records_path
        records_jsonpath = (
            records_jsonpath
            or _extra_options.get("record_jsonpath")
            or _extra_options.get("records_json_path")
            or _extra_options.get("record_path")
        )
        page_info_jsonpath = page_info_jsonpath or page_info_path
        page_info_jsonpath = (
            page_info_jsonpath
            or _extra_options.get("pageInfo")
            or _extra_options.get("page_info")
        )
        self.key = resolved_key
        self.records_jsonpath = records_jsonpath
        self.page_info_jsonpath = page_info_jsonpath
        self.cursor_key = cursor_key or "endCursor"
        self.has_next_page_key = has_next_page_key or "hasNextPage"
        required_value = required_fields if required_fields is not None else required
        self.required_fields = tuple(required_value or ())


GraphQLRootCheckpoint = t.NewType("GraphQLRootCheckpoint", dict[str, t.Any])


@dataclass(slots=True)
class GraphQLRootState:
    """Mutable pagination and de-duplication state for one root."""

    cursor: Cursor = None
    request_cursor: Cursor = None
    next_cursor: Cursor = None
    completed: bool = False
    attempts: int = 0
    seen_pages: set[t.Hashable] = field(default_factory=set)
    seen_records: set[str] = field(default_factory=set)

    def checkpoint(self) -> dict[str, t.Any]:
        """Return a JSON-serializable checkpoint for this root.

        Returns:
            A checkpoint dictionary.
        """
        return {
            "cursor": self.cursor,
            "request_cursor": self.request_cursor,
            "next_cursor": self.next_cursor,
            "completed": self.completed,
            "attempts": self.attempts,
        }


@dataclass(slots=True)
class GraphQLRootResult:
    """The analyzed outcome for one GraphQL root field."""

    root: RootKey
    records: list[dict[str, t.Any]]
    errors: list[dict[str, t.Any]]
    blocking_errors: list[dict[str, t.Any]]
    missing_required_fields: tuple[str, ...]
    request_cursor: Cursor
    next_cursor: Cursor = None
    page_cursor: Cursor = None
    has_next_page: bool | None = None
    data_present: bool = True
    stale: bool = False
    new_records: list[dict[str, t.Any]] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        """Whether this root can be safely delivered from this response."""
        return (
            self.data_present
            and not self.blocking_errors
            and not self.missing_required_fields
        )

    def is_complete(self) -> bool:
        """Return whether this root is complete.

        Returns:
            True when the root can be delivered.
        """
        return self.complete

    @property
    def deliverable(self) -> bool:
        """Whether records from this root should be emitted."""
        return self.complete

    @property
    def finished(self) -> bool:
        """Whether this root has no further pages."""
        return self.complete and self.has_next_page is False

    @property
    def resume_cursor(self) -> Cursor:
        """Cursor to use when this root is requested again."""
        if not self.complete:
            return self.request_cursor
        return self.next_cursor

    def checkpoint(self) -> dict[str, t.Any]:
        """Return a checkpoint describing this result's continuation position.

        Returns:
            A checkpoint dictionary.
        """
        return {
            "complete": self.complete,
            "finished": self.finished,
            "cursor": self.resume_cursor,
            "next_cursor": self.next_cursor,
            "request_cursor": self.request_cursor,
            "has_next_page": self.has_next_page,
        }


GraphQLRootOutcome = GraphQLRootResult


@dataclass(slots=True)
class GraphQLMultiRootResponse:
    """Analysis of all roots in one GraphQL response."""

    results: dict[RootKey, GraphQLRootResult]
    unattributed_errors: list[t.Any] = field(default_factory=list)
    data: dict[str, t.Any] | None = None

    @property
    def roots(self) -> dict[RootKey, GraphQLRootResult]:
        """Root results keyed by alias."""
        return self.results

    @property
    def complete_roots(self) -> dict[RootKey, GraphQLRootResult]:
        """Roots which are safe to deliver."""
        return {key: result for key, result in self.results.items() if result.complete}

    @property
    def delivered_roots(self) -> dict[RootKey, GraphQLRootResult]:
        """Alias for :attr:`complete_roots`."""
        return self.complete_roots

    @property
    def incomplete_roots(self) -> dict[RootKey, GraphQLRootResult]:
        """Roots which are not safe to deliver."""
        return {
            key: result for key, result in self.results.items() if not result.complete
        }

    @property
    def failed_roots(self) -> dict[RootKey, GraphQLRootResult]:
        """Alias for :attr:`incomplete_roots`."""
        return self.incomplete_roots

    @property
    def successful_roots(self) -> dict[RootKey, GraphQLRootResult]:
        """Alias for :attr:`complete_roots`."""
        return self.complete_roots

    @property
    def ready_roots(self) -> dict[RootKey, GraphQLRootResult]:
        """Alias for :attr:`complete_roots`."""
        return self.complete_roots

    @property
    def records(self) -> list[dict[str, t.Any]]:
        """All new records from complete roots, in root order."""
        records: list[dict[str, t.Any]] = []
        for result in self.complete_roots.values():
            records.extend(result.new_records or result.records)
        return records

    @property
    def cursors(self) -> dict[RootKey, Cursor]:
        """Resume cursor for every root."""
        return {key: result.resume_cursor for key, result in self.results.items()}

    @property
    def resume_cursors(self) -> dict[RootKey, Cursor]:
        """Alias for :attr:`cursors`."""
        return self.cursors

    @property
    def next_cursors(self) -> dict[RootKey, Cursor]:
        """Alias for :attr:`cursors`."""
        return self.cursors

    @property
    def incomplete_cursors(self) -> dict[RootKey, Cursor]:
        """Resume cursors for roots which were not delivered."""
        return {
            key: result.resume_cursor for key, result in self.incomplete_roots.items()
        }

    @property
    def pending_cursors(self) -> dict[RootKey, Cursor]:
        """Alias for :attr:`incomplete_cursors`."""
        return self.incomplete_cursors

    @property
    def continuation(self) -> dict[str, t.Any]:
        """JSON-serializable continuation checkpoint for all roots."""
        return {
            "cursors": self.cursors,
            "pending_cursors": self.pending_cursors,
            "all_finished": self.all_finished,
        }

    @property
    def all_finished(self) -> bool:
        """Whether every root is complete and has no further pages."""
        return bool(self.results) and all(
            result.finished for result in self.results.values()
        )

    def cursor_for(self, root: RootKey) -> Cursor:
        """Return the resume cursor for one root.

        Args:
            root: Root alias.

        Returns:
            The root's continuation cursor.
        """
        return self.results[root].resume_cursor

    def is_complete(self, root: RootKey) -> bool:
        """Return whether one root is complete.

        Args:
            root: Root alias.

        Returns:
            True when the root can be delivered.
        """
        return self.results[root].complete

    def raise_for_unattributed_errors(self) -> None:
        """Raise if an error cannot be assigned to a configured root.

        Raises:
            FatalAPIError: If one or more errors have no attributable root path.
        """
        if not self.unattributed_errors:
            return

        messages = []
        for error in self.unattributed_errors:
            if isinstance(error, dict):
                messages.append(str(error.get("message", error)))
            else:
                messages.append(str(error))
        joined = ", ".join(messages)
        msg = f"GraphQL errors could not be attributed to a root: {joined}"
        raise FatalAPIError(msg)


GraphQLResponseAnalysis = GraphQLMultiRootResponse


class GraphQLRootTracker:
    """Track per-root cursors, attempts, and emitted record identities."""

    def __init__(  # noqa: PLR0913
        self,
        roots: t.Sequence[RootKey | GraphQLRootConfig | Mapping[str, t.Any]]
        | Mapping[str, t.Any]
        | None = None,
        *,
        cursors: Mapping[RootKey, Cursor] | None = None,
        key_properties: t.Sequence[str] | None = None,
        selected_fields: SelectedFields = None,
        required_fields: RequiredFields = None,
    ) -> None:
        """Create a tracker.

        Args:
            roots: Root aliases or configurations.
            cursors: Initial per-root cursors.
            key_properties: Record keys used for de-duplication.
            selected_fields: Catalog-selected fields, optionally keyed by root.
            required_fields: Business-required fields, optionally keyed by root.
        """
        self.configs = _normalize_roots(roots)
        self.key_properties = tuple(key_properties or ())
        self.selected_fields = selected_fields
        self.required_fields = required_fields
        initial_cursors = dict(cursors or {})
        self.states: dict[RootKey, GraphQLRootState] = {
            key: GraphQLRootState(cursor=initial_cursors.get(key))
            for key in self.configs
        }
        for key, cursor in initial_cursors.items():
            if key not in self.states:
                self.states[key] = GraphQLRootState(cursor=cursor)

    @property
    def cursors(self) -> dict[RootKey, Cursor]:
        """Current request/resume cursor for each known root."""
        return {key: state.cursor for key, state in self.states.items()}

    @property
    def all_finished(self) -> bool:
        """Whether every known root has finished pagination."""
        return bool(self.states) and all(
            state.completed for state in self.states.values()
        )

    def checkpoint(self) -> dict[RootKey, dict[str, t.Any]]:
        """Return JSON-serializable per-root continuation state.

        Returns:
            Checkpoint dictionary keyed by root alias.
        """
        return {key: state.checkpoint() for key, state in self.states.items()}

    def handle(
        self,
        payload: Mapping[str, t.Any] | requests.Response,
        *,
        request_cursors: Mapping[RootKey, Cursor] | None = None,
        selected_fields: SelectedFields = None,
        required_fields: RequiredFields = None,
        strict: bool = True,
    ) -> GraphQLMultiRootResponse:
        """Analyze a response and advance or preserve per-root state.

        Args:
            payload: GraphQL JSON body or a response object with ``json()``.
            request_cursors: Cursors used for this request.
            selected_fields: Catalog-selected fields, optionally keyed by root.
            required_fields: Business-required fields, optionally keyed by root.
            strict: Raise on unattributed errors when true.

        Returns:
            The analyzed response with de-duplicated records.
        """
        body = _coerce_payload(payload)
        if selected_fields is None:
            selected_fields = self.selected_fields
        if required_fields is None:
            required_fields = self.required_fields
        data = body.get("data")
        data_mapping = data if isinstance(data, dict) else {}
        errors = body.get("errors", [])
        error_list = errors if isinstance(errors, list) else [errors]

        known_roots = tuple(self.configs) or tuple(data_mapping)
        unattributed: list[t.Any] = []
        errors_by_root: dict[RootKey, list[dict[str, t.Any]]] = {
            key: [] for key in known_roots
        }

        for error in error_list:
            root = attribute_graphql_error(error, known_roots)
            if root is None:
                unattributed.append(error)
                continue
            errors_by_root.setdefault(root, []).append(
                error if isinstance(error, dict) else {"message": str(error)}
            )

        results: dict[RootKey, GraphQLRootResult] = {}
        root_keys = list(self.configs)
        for key in data_mapping:
            if key not in root_keys:
                root_keys.append(key)

        for root in root_keys:
            config = self.configs.get(root) or GraphQLRootConfig(key=root)
            root_required = _required_fields_for_root(
                root,
                required_fields,
                config.required_fields,
            )
            root_selected = _selected_fields_for_root(root, selected_fields)
            request_state = self.states.get(root)
            fallback_cursor = request_state.cursor if request_state else None
            request_cursor = (
                request_cursors.get(root)
                if request_cursors and root in request_cursors
                else fallback_cursor
            )
            result = _build_root_result(
                root=root,
                data_root=data_mapping.get(root),
                errors=errors_by_root.get(root, []),
                request_cursor=request_cursor,
                config=config,
                selected_fields=root_selected,
                required_fields=root_required,
            )
            results[root] = result
            self._update_state(root, result)

        analysis = GraphQLMultiRootResponse(
            results=results,
            unattributed_errors=unattributed,
            data=data_mapping,
        )
        if strict:
            analysis.raise_for_unattributed_errors()
        return analysis

    def _update_state(self, root: RootKey, result: GraphQLRootResult) -> None:
        state = self.states.setdefault(root, GraphQLRootState())
        state.request_cursor = result.request_cursor

        if not result.complete:
            state.cursor = result.request_cursor
            state.completed = False
            state.attempts += 1
            result.new_records = []
            return

        state.attempts = 0
        page_identity = result.page_cursor
        duplicate_page = page_identity is not None and page_identity in state.seen_pages
        if page_identity is not None:
            state.seen_pages.add(page_identity)

        new_records: list[dict[str, t.Any]] = []
        for record in result.records:
            fingerprint = _record_fingerprint(record, self.key_properties)
            if fingerprint in state.seen_records:
                continue
            state.seen_records.add(fingerprint)
            new_records.append(record)

        result.new_records = new_records
        result.stale = duplicate_page

        if duplicate_page:
            return

        state.next_cursor = result.next_cursor
        state.completed = result.finished
        state.cursor = result.next_cursor


class GraphQLMultiRootPaginator(BaseAPIPaginator[dict[RootKey, Cursor]]):
    """Paginator which keeps an independent cursor for every GraphQL root."""

    def __init__(  # noqa: PLR0913
        self,
        roots: t.Any = None,
        tracker: GraphQLRootTracker | None = None,
        *,
        stream: t.Any = None,
        cursors: Mapping[RootKey, Cursor] | None = None,
        key_properties: t.Sequence[str] | None = None,
        selected_fields: SelectedFields = None,
        required_fields: RequiredFields = None,
        max_attempts: int = 5,
    ) -> None:
        """Create a multi-root paginator.

        This accepts either root configuration for standalone use, or a stream and
        existing tracker for integration with :class:`GraphQLStream`.

        Args:
            roots: Root aliases/configurations, or a stream when paired with tracker.
            tracker: Existing tracker used by a GraphQL stream.
            stream: Optional stream providing catalog selection and retry policy.
            cursors: Initial cursors keyed by root alias.
            key_properties: Keys used to de-duplicate replayed records.
            selected_fields: Catalog-selected fields, optionally keyed by root.
            required_fields: Business-required fields, optionally keyed by root.
            max_attempts: Maximum consecutive failed attempts for one root.
        """
        if tracker is not None:
            self.stream = stream or (
                roots if hasattr(roots, "_analyze_root_response") else None
            )
            self.tracker = tracker
        elif isinstance(roots, GraphQLRootTracker):
            self.stream = stream
            self.tracker = roots
        else:
            self.stream = stream
            self.tracker = GraphQLRootTracker(
                roots=roots,
                cursors=cursors,
                key_properties=key_properties,
            )
        self.selected_fields = selected_fields
        self.required_fields = required_fields
        self.max_attempts = max_attempts
        self._analysis_cache: tuple[int, GraphQLMultiRootResponse] | None = None
        super().__init__(self.tracker.cursors)

    @override
    def continue_if_empty(self, response: requests.Response) -> bool:  # noqa: ARG002
        """Continue when only failed or duplicate roots produced no records.

        Args:
            response: API response object.

        Returns:
            Always true.
        """
        return True

    def analyze(self, response: requests.Response) -> GraphQLMultiRootResponse:
        """Analyze a response once and cache the result for this response object.

        Args:
            response: GraphQL response or JSON mapping.

        Returns:
            The analyzed response.
        """
        response_id = id(response)
        if self._analysis_cache and self._analysis_cache[0] == response_id:
            return self._analysis_cache[1]

        analysis = self.tracker.handle(
            response,
            request_cursors=self.current_value,
            selected_fields=self.selected_fields,
            required_fields=self.required_fields,
            strict=True,
        )
        self._analysis_cache = (response_id, analysis)
        return analysis

    @override
    def has_more(self, response: requests.Response) -> bool:
        """Return whether any root still has pages.

        Args:
            response: API response object.

        Returns:
            True when at least one root is unfinished.
        """
        if self.stream is not None:
            self.stream._analyze_root_response(response)  # noqa: SLF001
        else:
            self.analyze(response)
        return not self.tracker.all_finished

    @override
    def get_next(
        self,
        response: requests.Response,  # noqa: ARG002
    ) -> dict[RootKey, Cursor]:
        """Return cursors for all roots.

        Args:
            response: API response object.

        Returns:
            Cursor mapping.
        """
        return self.tracker.cursors

    @override
    def advance(self, response: requests.Response) -> None:
        """Advance successful roots and retain failed-root cursors.

        Args:
            response: API response object.

        Raises:
            RetriableAPIError: If one root exceeds its retry budget.
        """
        if self.stream is not None:
            self.stream._analyze_root_response(response)  # noqa: SLF001
        else:
            self.analyze(response)
        self._page_count += 1

        max_attempts = (
            self.stream.backoff_max_tries()
            if self.stream is not None
            else self.max_attempts
        )
        try:
            if self.tracker.all_finished:
                self._finished = True
                return

            exhausted = [
                root
                for root, state in self.tracker.states.items()
                if not state.completed and state.attempts >= max_attempts
            ]
            if exhausted:
                msg = (
                    "GraphQL root(s) failed after "
                    f"{max_attempts} attempts: {', '.join(exhausted)}"
                )
                raise RetriableAPIError(msg, response=response)

            self._finished = False
            self._value = self.tracker.cursors
        finally:
            self._analysis_cache = None
            if self.stream is not None:
                self.stream._root_analysis_cache = None  # noqa: SLF001


GraphQLRootPaginator = GraphQLMultiRootPaginator


def attribute_graphql_error(
    error: t.Any,
    roots: t.Collection[RootKey] | None = None,
) -> RootKey | None:
    """Return the root alias addressed by a GraphQL error path.

    Args:
        error: An entry from the GraphQL ``errors`` array.
        roots: Known root aliases. When omitted, any first path segment is accepted.

    Returns:
        The root alias, or ``None`` if the error cannot be attributed.
    """
    if not isinstance(error, dict):
        return None

    path = error.get("path")
    if isinstance(path, str):
        path = path.split(".")
    if not isinstance(path, (list, tuple)) or not path:
        return None

    if path[0] == "data" and len(path) > 1:
        path = path[1:]

    root = path[0]
    if not isinstance(root, str):
        return None
    if roots is not None and root not in roots:
        return None
    return root


def analyze_graphql_response(  # noqa: PLR0913
    payload: Mapping[str, t.Any] | requests.Response,
    roots: t.Sequence[RootKey | GraphQLRootConfig | Mapping[str, t.Any]]
    | Mapping[str, t.Any]
    | None = None,
    *,
    selected_fields: SelectedFields = None,
    required_fields: RequiredFields = None,
    cursors: Mapping[RootKey, Cursor] | None = None,
    key_properties: t.Sequence[str] | None = None,
    strict: bool = True,
) -> GraphQLMultiRootResponse:
    """Analyze GraphQL roots without retaining de-duplication state.

    Args:
        payload: GraphQL JSON body or response object.
        roots: Root aliases or configurations.
        selected_fields: Catalog-selected fields, optionally keyed by root.
        required_fields: Required fields, optionally keyed by root.
        cursors: Cursors used for the request.
        key_properties: Record keys used for de-duplication.
        strict: Raise on unattributed errors when true.

    Returns:
        The analyzed response.
    """
    tracker = GraphQLRootTracker(
        roots=roots,
        cursors=cursors,
        key_properties=key_properties,
    )
    return tracker.handle(
        payload,
        request_cursors=cursors,
        selected_fields=selected_fields,
        required_fields=required_fields,
        strict=strict,
    )


def _normalize_roots(
    roots: t.Sequence[RootKey | GraphQLRootConfig | Mapping[str, t.Any]]
    | Mapping[str, t.Any]
    | None,
) -> dict[RootKey, GraphQLRootConfig]:
    if roots is None:
        return {}

    if isinstance(roots, Mapping):
        normalized: dict[RootKey, GraphQLRootConfig] = {}
        for key, value in roots.items():
            if isinstance(value, GraphQLRootConfig):
                normalized[key] = value
            elif isinstance(value, Mapping):
                config_kwargs = dict(value)
                config_kwargs.setdefault("key", key)
                normalized[key] = GraphQLRootConfig(**config_kwargs)
            else:
                normalized[key] = GraphQLRootConfig(key=key)
        return normalized

    result: dict[RootKey, GraphQLRootConfig] = {}
    for item in roots:
        if isinstance(item, GraphQLRootConfig):
            result[item.key] = item
        elif isinstance(item, Mapping):
            root_config = GraphQLRootConfig(**dict(item))
            result[root_config.key] = root_config
        else:
            result[item] = GraphQLRootConfig(key=item)
    return result


def _coerce_payload(
    payload: Mapping[str, t.Any] | requests.Response,
) -> dict[str, t.Any]:
    if hasattr(payload, "json") and callable(payload.json):
        try:
            value = payload.json(parse_float=Decimal)
        except TypeError:
            value = payload.json()
    else:
        value = payload

    if not isinstance(value, dict):
        msg = "GraphQL response body must be a JSON object"
        raise FatalAPIError(msg)
    return value


def _selected_fields_for_root(
    root: RootKey,
    selected_fields: SelectedFields,
) -> t.Collection[str] | GraphQLFieldSelection | None:
    if selected_fields is None:
        return None
    if isinstance(selected_fields, Mapping):
        value = selected_fields.get(root) or selected_fields.get("*")
        if isinstance(value, GraphQLFieldSelection):
            return value
        if isinstance(value, t.Collection) and not isinstance(value, str):
            return value
        return None
    return selected_fields


def _required_fields_for_root(
    root: RootKey,
    required_fields: RequiredFields,
    root_defaults: t.Sequence[str],
) -> tuple[str, ...]:
    values: list[t.Any] = list(root_defaults)
    if required_fields is None:
        return tuple(values)
    if isinstance(required_fields, Mapping):
        value = required_fields.get(root)
        if value is None:
            value = required_fields.get("*")
    else:
        value = required_fields

    if value is None:
        return tuple(values)
    if isinstance(value, str):
        values.append(value)
    else:
        values.extend(value)
    return tuple(dict.fromkeys(values))


def _build_root_result(  # noqa: PLR0913
    *,
    root: RootKey,
    data_root: t.Any,
    errors: list[dict[str, t.Any]],
    request_cursor: Cursor,
    config: GraphQLRootConfig,
    selected_fields: t.Collection[str] | GraphQLFieldSelection | None,
    required_fields: t.Sequence[str],
) -> GraphQLRootResult:
    data_present = data_root is not None
    records = _extract_records(data_root, config) if data_present else []
    page_info = _extract_page_info(data_root, config) if data_present else {}
    has_next_page = _get_first(
        page_info,
        [config.has_next_page_key, "hasNextPage", "has_next_page"],
    )
    next_cursor = _get_first(
        page_info,
        [config.cursor_key, "endCursor", "end_cursor", "cursor"],
    )
    page_cursor = _get_first(
        page_info,
        ["startCursor", "start_cursor", config.cursor_key],
    )

    blocking_errors = [
        error
        for error in errors
        if _is_blocking_error(
            error,
            root=root,
            selected_fields=selected_fields,
            required_fields=required_fields,
        )
    ]

    missing = tuple(
        field
        for field in required_fields
        if any(_dig(record, field) is None for record in records)
    )

    if has_next_page is True and next_cursor is None:
        blocking_errors.append(
            {"message": f"GraphQL root {root!r} did not provide a next cursor"}
        )

    return GraphQLRootResult(
        root=root,
        records=records,
        errors=errors,
        blocking_errors=blocking_errors,
        missing_required_fields=missing,
        request_cursor=request_cursor,
        next_cursor=next_cursor,
        page_cursor=page_cursor,
        has_next_page=has_next_page if isinstance(has_next_page, bool) else None,
        data_present=data_present,
    )


def _is_blocking_error(  # noqa: PLR0911, PLR0912
    error: dict[str, t.Any],
    *,
    root: RootKey,
    selected_fields: t.Collection[str] | GraphQLFieldSelection | None,
    required_fields: t.Sequence[str],
) -> bool:
    path = error.get("path")
    if isinstance(path, str):
        path = path.split(".")
    if not isinstance(path, (list, tuple)) or not path:
        return True
    if path[0] == "data" and len(path) > 1:
        path = path[1:]
    if path[0] != root:
        return True

    segments = [part for part in path[1:] if isinstance(part, str)]
    if any(segment.lower() == "pageinfo" for segment in segments):
        return True

    field_path = _record_field_path(path[1:])
    dotted = ".".join(field_path)
    top_level_field = field_path[0] if field_path else None

    if any(
        _field_matches(selector, field_path, dotted) for selector in required_fields
    ):
        return True

    known_fields: set[str] | None = None
    if isinstance(selected_fields, GraphQLFieldSelection):
        if selected_fields.known is not None:
            known_fields = set(selected_fields.known)
        selected_field_names: set[str] | None = (
            set(selected_fields.selected)
            if selected_fields.selected is not None
            else None
        )
    elif selected_fields is None:
        selected_field_names = None
    else:
        selected_field_names = set(selected_fields)

    if top_level_field is None:
        return True
    if known_fields is not None and top_level_field not in known_fields:
        return True
    if selected_field_names is None:
        return True

    return top_level_field in selected_field_names


def _record_field_path(path_parts: t.Sequence[t.Any]) -> list[str]:
    parts = list(path_parts)
    marker_index: int | None = None
    for index, part in enumerate(parts):
        if isinstance(part, str) and part.lower() in _STRUCTURAL_PATH_TOKENS:
            marker_index = index

    if marker_index is not None:
        return [part for part in parts[marker_index + 1 :] if isinstance(part, str)]

    return [part for part in parts if isinstance(part, str)]


def _field_matches(
    selector: FieldSelector,
    field_path: t.Sequence[str],
    dotted: str,
) -> bool:
    if isinstance(selector, str):
        selector_parts = selector.split(".")
    else:
        selector_parts = list(selector)
    selector_dotted = ".".join(selector_parts)
    if dotted == selector_dotted or dotted.endswith(f".{selector_dotted}"):
        return True
    return any(part == selector_parts[0] for part in field_path)


def _extract_records(
    data_root: t.Any,
    config: GraphQLRootConfig,
) -> list[dict[str, t.Any]]:
    if config.records_jsonpath:
        expression = config.records_jsonpath
        if "{root}" in expression or "{alias}" in expression or "{name}" in expression:
            expression = expression.format(
                root=config.key,
                alias=config.key,
                name=config.key,
            )
        input_value: t.Any = data_root
        if expression.startswith("$.data"):
            input_value = {"data": {config.key: data_root}}
        return [
            item
            for item in _extract_jsonpath(expression, input_value)
            if isinstance(item, dict)
        ]

    if isinstance(data_root, list):
        return [item for item in data_root if isinstance(item, dict)]
    if not isinstance(data_root, dict):
        return []

    for expression in (
        "$.nodes[*]",
        "$.edges[*].node",
        "$.items[*]",
        "$.*.nodes[*]",
        "$.*.edges[*].node",
        "$.*.items[*]",
        "$..nodes[*]",
        "$..edges[*].node",
        "$..items[*]",
    ):
        matches = [
            item
            for item in _extract_jsonpath(expression, data_root)
            if isinstance(item, dict)
        ]
        if matches:
            return matches
    return []


def _extract_page_info(
    data_root: t.Any,
    config: GraphQLRootConfig,
) -> dict[str, t.Any]:
    if not isinstance(data_root, dict):
        return {}

    if config.page_info_jsonpath:
        expression = config.page_info_jsonpath.format(
            root=config.key,
            alias=config.key,
            name=config.key,
        )
        input_value: t.Any = data_root
        if expression.startswith("$.data"):
            input_value = {"data": {config.key: data_root}}
        matches = list(_extract_jsonpath(expression, input_value))
        first = next(iter(matches), None)
        return first if isinstance(first, dict) else {}

    direct = data_root.get("pageInfo")
    if isinstance(direct, dict):
        return direct

    for value in data_root.values():
        if isinstance(value, dict) and isinstance(value.get("pageInfo"), dict):
            page_info = value["pageInfo"]
            return t.cast("dict[str, t.Any]", page_info)

    matches = list(_extract_jsonpath("$..pageInfo", data_root))
    first = next(iter(matches), None)
    return first if isinstance(first, dict) else {}


def _extract_jsonpath(expression: str, value: t.Any) -> t.Generator[t.Any, None, None]:
    if not expression.startswith("$"):
        expression = f"$.{expression}"
    yield from extract_jsonpath(expression, value)


def _get_first(mapping: Mapping[str, t.Any], keys: t.Sequence[str]) -> t.Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _dig(value: t.Any, path: FieldSelector) -> t.Any:
    parts = path.split(".") if isinstance(path, str) else list(path)
    current = value
    for part in parts:
        if isinstance(current, Mapping):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            current = current[index] if index < len(current) else None
        else:
            return None
        if current is None:
            return None
    return current


def _record_fingerprint(
    record: dict[str, t.Any],
    key_properties: t.Sequence[str],
) -> str:
    identity: t.Any
    if key_properties and all(key in record for key in key_properties):
        identity = tuple(record.get(key) for key in key_properties)
    else:
        identity = record
    return json.dumps(identity, sort_keys=True, default=str, separators=(",", ":"))


RootConfig = GraphQLRootConfig
RootState = GraphQLRootState
RootResult = GraphQLRootResult
RootTracker = GraphQLRootTracker
RootPaginator = GraphQLMultiRootPaginator
MultiRootResponse = GraphQLMultiRootResponse
PartialGraphQLResponse = GraphQLMultiRootResponse
GraphQLRootHandler = GraphQLRootTracker
GraphQLRootProcessor = GraphQLRootTracker
GraphQLCursorTracker = GraphQLRootTracker
process_graphql_response = analyze_graphql_response
parse_multi_root_response = analyze_graphql_response
split_graphql_response = analyze_graphql_response
