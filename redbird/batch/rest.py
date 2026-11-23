"""Version-aware REST access used by batch synchronisation.

The REST side of a batch operation has to do two things atomically:

- claim a resource for a batch/operation pair, and
- make that claim conditional on the version that was read.

A single conditional PATCH writes both marker fields.  Reading the resource
back returns the marker fields and the version together.
"""
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from redbird.packages import requests


class BatchSourceError(Exception):
    """Base class for errors from a batch source."""


class BatchTransportError(BatchSourceError):
    """The request did not produce an HTTP response.

    The caller cannot know whether the remote side applied the change.  The
    operation must stay pending and be reconciled by reading the resource on
    the next attempt.
    """


class BatchRequestError(BatchSourceError):
    """The remote side returned an unexpected response."""

    def __init__(self, message: str, status_code: Optional[int] = None, response=None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class BatchResourceNotFound(BatchRequestError):
    """The resource does not exist in the source."""


class BatchVersionConflict(BatchSourceError):
    """A conditional update failed because the resource version moved.

    ``marker`` is filled when the caller has already read the current state.
    """

    def __init__(self, message: str, marker=None):
        super().__init__(message)
        self.marker = marker


def _same_id(left, right) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return str(left) == str(right)


def _strip_etag(value: str) -> str:
    value = value.strip()
    if value.startswith("W/"):
        value = value[2:].strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1]
    return value


def _format_etag(value) -> str:
    value = str(value)
    if value.startswith('"') or value.startswith("W/"):
        return value
    return '"{}"'.format(value)


@dataclass(frozen=True)
class RemoteMarker:
    """Marker state of one REST resource.

    ``batch_id`` and ``operation_id`` are read together so the caller can tell
    which operation currently owns the resource.  ``version`` is the version
    that a conditional update must use.  ``marked`` is the value of the
    optional processed flag, read in the same response.
    """

    resource_id: Any
    version: Optional[Any]
    batch_id: Optional[Any]
    operation_id: Optional[Any]
    raw: Mapping[str, Any]
    marked: Optional[Any] = None

    @property
    def present(self) -> bool:
        return bool(self.marked) or self.batch_id is not None or self.operation_id is not None

    def owned_by(self, batch_id, operation_id) -> bool:
        if not self.present:
            return False
        return _same_id(self.batch_id, batch_id) and _same_id(self.operation_id, operation_id)

    @property
    def owner(self):
        if not self.present:
            return None
        return (self.batch_id, self.operation_id)


class RESTBatchStore:
    """Version-aware REST resource store.

    Parameters
    ----------
    url : str
        Base URL of the resource collection.  The item URL defaults to
        ``{url}/{id}``.
    id_field : str
        Field containing the resource id.  Only used to build the item URL.
    version_field : str
        Field containing the resource version in the JSON representation.
        If it is absent, the ``ETag`` response header is used.
    batch_field, operation_field : str
        JSON fields used for the batch and operation markers.
    session : requests.Session, optional
        Session to use.  A new session is created when omitted.
    headers : dict, optional
        Headers sent with every request.
    url_template : str, optional
        Template for the item URL.  It receives ``id``, for example
        ``"https://example.test/items/{id}"``.
    version_header : str
        Header used for conditional updates.
    quote_version : bool
        Whether a JSON version is quoted as an HTTP entity tag.  Set this to
        ``False`` for APIs that use a bare version in ``If-Match``.
    expected_version_field : str, optional
        If given, the expected version is also included in the JSON body.
    processed_field : str, optional
        Optional boolean field written to ``True`` when a resource is marked
        and to ``False`` when the marker is cleared.  It is written in the
        same conditional request as the batch and operation fields.
    timeout : float, optional
        Request timeout.
    """

    def __init__(
        self,
        url: str,
        *,
        id_field: str = "id",
        version_field: str = "version",
        batch_field: str = "batch_id",
        operation_field: str = "operation_id",
        processed_field: Optional[str] = None,
        session=None,
        headers: Optional[Dict[str, str]] = None,
        url_template: Optional[str] = None,
        version_header: str = "If-Match",
        quote_version: bool = True,
        expected_version_field: Optional[str] = None,
        timeout: Optional[float] = 30,
    ):
        self.url = url
        self.id_field = id_field
        self.version_field = version_field
        self.batch_field = batch_field
        self.operation_field = operation_field
        self.processed_field = processed_field
        self.session = session if session is not None else requests.Session()
        self.headers = dict(headers or {})
        self.url_template = url_template
        self.version_header = version_header
        self.quote_version = quote_version
        self.expected_version_field = expected_version_field
        self.timeout = timeout

    @classmethod
    def from_repo(cls, repo, **kwargs):
        """Create a store from a :class:`redbird.repos.RESTRepo`."""
        settings = dict(
            id_field=repo.id_field,
            session=repo.session,
            headers=repo.headers,
            url=repo.url,
        )
        settings.update(kwargs)
        return cls(**settings)

    def item_url(self, resource_id) -> str:
        if self.url_template is not None:
            return self.url_template.format(id=urllib.parse.quote(str(resource_id), safe=""))
        return "{}/{}".format(
            self.url.rstrip("/"),
            urllib.parse.quote(str(resource_id), safe=""),
        )

    def _request(self, method: str, url: str, **kwargs):
        headers = dict(self.headers)
        headers.update(kwargs.pop("headers", {}))
        try:
            return self.session.request(
                method,
                url,
                headers=headers,
                timeout=self.timeout,
                **kwargs
            )
        except requests.RequestException as exc:
            raise BatchTransportError(
                "Request {} {} did not complete: {}".format(method, url, exc)
            ) from exc

    def _marker_from_response(self, resource_id, response) -> RemoteMarker:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not isinstance(payload, Mapping):
            payload = {}

        version = payload.get(self.version_field)
        if version is None:
            etag = response.headers.get("ETag")
            if etag is not None:
                version = _strip_etag(etag)

        return RemoteMarker(
            resource_id=resource_id,
            version=version,
            batch_id=payload.get(self.batch_field),
            operation_id=payload.get(self.operation_field),
            raw=dict(payload),
            marked=payload.get(self.processed_field) if self.processed_field is not None else None,
        )

    def read_item(self, resource_id) -> Optional[Dict[str, Any]]:
        response = self._request("GET", self.item_url(resource_id))
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            raise BatchRequestError(
                "Reading resource {} failed with status {}".format(resource_id, response.status_code),
                status_code=response.status_code,
                response=response,
            )
        return response.json()

    def read_marker(self, resource_id) -> RemoteMarker:
        response = self._request("GET", self.item_url(resource_id))
        if response.status_code == 404:
            raise BatchResourceNotFound(
                "Resource {} was not found".format(resource_id),
                status_code=404,
                response=response,
            )
        if response.status_code >= 400:
            raise BatchRequestError(
                "Reading resource {} failed with status {}".format(resource_id, response.status_code),
                status_code=response.status_code,
                response=response,
            )
        return self._marker_from_response(resource_id, response)

    def _conditional_update(
        self,
        method: str,
        resource_id,
        payload: Dict[str, Any],
        expected_version,
        operation: str,
    ) -> RemoteMarker:
        payload = dict(payload)
        headers = {}
        if expected_version is not None:
            value = _format_etag(expected_version) if self.quote_version else str(expected_version)
            headers[self.version_header] = value
            if self.expected_version_field is not None:
                payload[self.expected_version_field] = expected_version

        response = self._request(
            method,
            self.item_url(resource_id),
            json=payload,
            headers=headers,
        )

        if response.status_code in (409, 412, 428):
            raise BatchVersionConflict(
                "{} of resource {} conflicted with version {}".format(
                    operation, resource_id, expected_version
                )
            )
        if response.status_code == 404:
            raise BatchResourceNotFound(
                "Resource {} was not found".format(resource_id),
                status_code=404,
                response=response,
            )
        if response.status_code >= 400:
            raise BatchRequestError(
                "{} of resource {} failed with status {}".format(
                    operation, resource_id, response.status_code
                ),
                status_code=response.status_code,
                response=response,
            )

        if response.status_code == 204 or not response.content:
            return self.read_marker(resource_id)
        return self._marker_from_response(resource_id, response)

    def mark(self, resource_id, batch_id, operation_id, expected_version) -> RemoteMarker:
        """Claim ``resource_id`` for a batch/operation in one request."""
        payload = {
            self.batch_field: batch_id,
            self.operation_field: operation_id,
        }
        if self.processed_field is not None:
            payload[self.processed_field] = True
        marker = self._conditional_update(
            "PATCH",
            resource_id,
            payload,
            expected_version=expected_version,
            operation="Marking",
        )
        return marker

    def clear_marker(self, resource_id, batch_id, operation_id, expected_version) -> RemoteMarker:
        """Clear a marker owned by ``batch_id``/``operation_id``.

        The caller should verify ownership first.  The conditional request
        makes sure the resource was not taken over between that check and this
        request.
        """
        payload = {
            self.batch_field: None,
            self.operation_field: None,
        }
        if self.processed_field is not None:
            payload[self.processed_field] = False
        return self._conditional_update(
            "PATCH",
            resource_id,
            payload,
            expected_version=expected_version,
            operation="Clearing marker from",
        )
