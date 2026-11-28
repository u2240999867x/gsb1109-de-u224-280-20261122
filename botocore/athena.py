# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""Resumable work-order submissions for Athena ``StartQueryExecution``.

Athena only makes ``ClientRequestToken`` idempotent within an execution
target.  An execution target is the pair of a caller registered Athena
backend identifier and the destination to which the request is actually
sent.  A token may be reused by a different backend, but once a request
has possibly been accepted, the token (and the work order that owns it)
can only be resumed against the target that accepted it.

This module adds an opt-in work-order envelope around
``StartQueryExecution``.  Calls that do not include a ``WorkOrder`` (or
one of its equivalent parameters) are left completely untouched, so the
normal SDK send and retry behavior is preserved.
"""

import hashlib
import json
import os
import tempfile
import threading
import time
import uuid
from urllib.parse import urlsplit, urlunsplit

from botocore.awsrequest import AWSResponse
from botocore.exceptions import (
    InvalidWorkOrderError,
    WorkOrderConfigurationConflictError,
    WorkOrderTokenConflictError,
)

_WORK_ORDER_CONTEXT_KEY = '_athena_work_order'
_WORK_ORDER_WIRE_KEYS = (
    'WorkOrder',
    'AthenaWorkOrder',
    'QueryWorkOrder',
    'ResumableWorkOrder',
    'Submission',
    'WorkOrderSpec',
)
_WORK_ORDER_METADATA_KEYS = _WORK_ORDER_WIRE_KEYS + (
    'WorkOrderId',
    'WorkOrderID',
    'WorkTicketId',
    'TicketId',
    'TicketID',
    'WorkOrderLine',
    'WorkOrderLineId',
    'SourceLine',
    'LineId',
    'Line',
    'AthenaBackendId',
    'AthenaBackendID',
    'BackendId',
    'BackendID',
    'AthenaBackend',
    'AthenaBackendUrl',
    'AthenaBackendEndpoint',
    'AthenaBackendRegion',
    'WorkOrderToken',
    'WorkOrderWorkGroup',
    'WorkOrderCacheDir',
    'WorkOrderLineName',
)
_WORK_ORDER_DIRECT_ALIASES = {
    'work_order': 'WorkOrder',
    'athena_work_order': 'AthenaWorkOrder',
    'query_work_order': 'QueryWorkOrder',
    'resumable_work_order': 'ResumableWorkOrder',
    'submission': 'Submission',
    'work_order_id': 'WorkOrderId',
    'work_ticket_id': 'WorkTicketId',
    'ticket_id': 'TicketId',
    'ticket': 'TicketId',
    'work_order_line': 'WorkOrderLine',
    'work_order_line_id': 'WorkOrderLineId',
    'line': 'Line',
    'line_id': 'LineId',
    'backend_id': 'AthenaBackendId',
    'athena_backend_id': 'AthenaBackendId',
    'backend': 'AthenaBackend',
    'athena_backend': 'AthenaBackend',
    'athena_backend_url': 'AthenaBackendUrl',
    'backend_url': 'AthenaBackendUrl',
    'athena_backend_endpoint': 'AthenaBackendEndpoint',
    'backend_endpoint': 'AthenaBackendEndpoint',
    'athena_backend_region': 'AthenaBackendRegion',
    'backend_region': 'AthenaBackendRegion',
    'token': 'WorkOrderToken',
    'work_order_token': 'WorkOrderToken',
    'cache_dir': 'WorkOrderCacheDir',
    'work_order_cache_dir': 'WorkOrderCacheDir',
    'work_order_line_name': 'WorkOrderLineName',
}
_WORK_ORDER_KEYS = {
    'id',
    'workorderid',
    'ticketid',
    'ticket',
    'ticketnumber',
    'workorder',
}
_LINE_KEYS = {
    'line',
    'lineid',
    'workorderline',
    'workorderlineid',
    'sourceline',
}
_BACKEND_KEYS = {
    'backend',
    'backendid',
    'athenabackend',
    'athenabackendid',
    'executionbackend',
}
_BACKEND_URL_KEYS = {
    'athenabackendurl',
    'athenabackendendpoint',
    'backendurl',
    'backendendpoint',
}
_BACKEND_REGION_KEYS = {
    'athenabackendregion',
    'backendregion',
}
_TOKEN_KEYS = {
    'token',
    'clientrequesttoken',
    'workordertoken',
}
_TOKEN_PARAM_NAMES = (
    'ClientRequestToken',
    'Token',
    'WorkOrderToken',
)
_CACHE_KEYS = {
    'cachedir',
    'workordercachedir',
    'store',
    'storedir',
}
_DEFAULT_CACHE_ENV = 'AWS_ATHENA_WORK_ORDER_CACHE'
_DEFAULT_CACHE_DIR = os.path.expanduser(
    os.path.join('~', '.aws', 'boto', 'cache', 'athena-work-orders')
)
_STORE_VERSION = 1
_LOCK = threading.RLock()


class AthenaBackend:
    """A caller-registered Athena backend.

    ``backend_id`` is the independent identifier used by the caller.  The
    ``endpoint_url`` is where requests for that backend are actually sent.
    """

    def __init__(self, backend_id, endpoint_url, region_name=None):
        self.backend_id = backend_id
        self.endpoint_url = endpoint_url
        self.region_name = region_name

    def to_dict(self):
        return {
            'backend_id': self.backend_id,
            'endpoint_url': self.endpoint_url,
            'region_name': self.region_name,
        }

    @classmethod
    def from_dict(cls, value):
        return cls(
            backend_id=value['backend_id'],
            endpoint_url=value['endpoint_url'],
            region_name=value.get('region_name'),
        )

    def __repr__(self):
        return (
            f'AthenaBackend(backend_id={self.backend_id!r}, '
            f'endpoint_url={self.endpoint_url!r})'
        )


class _WorkOrderStore:
    """Durable, file-backed store for work-order and token ownership.

    The store is intentionally small and dependency free so it can be used
    from any process.  Writes use an atomic rename, while first-time token
    and work-order claims use ``O_EXCL`` creates to make ownership races
    observable rather than silently overwriting another owner.
    """

    def __init__(self, cache_dir=None):
        self.cache_dir = cache_dir or os.environ.get(
            _DEFAULT_CACHE_ENV, _DEFAULT_CACHE_DIR
        )
        self.cache_dir = os.path.expanduser(os.path.expandvars(self.cache_dir))
        self._work_orders_dir = os.path.join(self.cache_dir, 'work-orders')
        self._tokens_dir = os.path.join(self.cache_dir, 'tokens')
        self._backends_path = os.path.join(self.cache_dir, 'backends.json')

    @staticmethod
    def _hash(value):
        return hashlib.sha256(value.encode('utf-8')).hexdigest()

    def _work_order_path(self, work_order_key):
        return os.path.join(
            self._work_orders_dir, self._hash(work_order_key) + '.json'
        )

    def _token_path(self, target, token):
        return os.path.join(
            self._tokens_dir, self._hash(f'{target}\0{token}') + '.json'
        )

    @staticmethod
    def _read_json(path):
        try:
            with open(path, encoding='utf-8') as f:
                return json.load(f)
        except (OSError, ValueError):
            return None

    @staticmethod
    def _write_json_atomic(path, value):
        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(dir=directory, suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(value, f, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, path)
        except Exception:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
            raise

    @staticmethod
    def _create_json_exclusive(path, value):
        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return False
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(value, f, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            try:
                os.unlink(path)
            except OSError:
                pass
            raise
        return True

    def register_backend(self, backend_id, endpoint_url, region_name=None):
        with _LOCK:
            backends = self._load_backends()
            backends[backend_id] = AthenaBackend(
                backend_id, endpoint_url, region_name
            ).to_dict()
            self._write_json_atomic(self._backends_path, backends)
            return AthenaBackend.from_dict(backends[backend_id])

    def _load_backends(self):
        value = self._read_json(self._backends_path)
        if not isinstance(value, dict):
            return {}
        return value

    def list_backends(self):
        with _LOCK:
            return {
                key: AthenaBackend.from_dict(value)
                for key, value in self._load_backends().items()
            }

    def get_backend(self, backend_id):
        if backend_id is None:
            return None
        with _LOCK:
            value = self._load_backends().get(backend_id)
        if value is None:
            return None
        return AthenaBackend.from_dict(value)

    def infer_backend(self, destination):
        destination = normalize_destination(destination)
        matches = []
        for backend in self.list_backends().values():
            if normalize_destination(backend.endpoint_url) == destination:
                matches.append(backend)
        if len(matches) == 1:
            return matches[0]
        return None

    def get_work_order(self, work_order_key):
        with _LOCK:
            return self._read_json(self._work_order_path(work_order_key))

    def get_token(self, target, token):
        with _LOCK:
            return self._read_json(self._token_path(target, token))

    def claim_work_order(self, work_order_key, record):
        """Claim a work-order record, returning the existing owner if any."""
        with _LOCK:
            path = self._work_order_path(work_order_key)
            if self._create_json_exclusive(path, record):
                return record, True
            existing = self._read_json(path)
            return existing, False

    def claim_token(self, target, token, record):
        with _LOCK:
            path = self._token_path(target, token)
            if self._create_json_exclusive(path, record):
                return record, True
            existing = self._read_json(path)
            return existing, False

    def save_work_order(self, work_order_key, record):
        with _LOCK:
            self._write_json_atomic(
                self._work_order_path(work_order_key), record
            )

    def delete_work_order(self, work_order_key):
        with _LOCK:
            try:
                os.unlink(self._work_order_path(work_order_key))
            except OSError:
                pass

    def delete_token(self, target, token):
        with _LOCK:
            try:
                os.unlink(self._token_path(target, token))
            except OSError:
                pass

    def reset(self):
        with _LOCK:
            if not os.path.isdir(self.cache_dir):
                return
            for root, dirs, files in os.walk(self.cache_dir):
                for name in files:
                    try:
                        os.unlink(os.path.join(root, name))
                    except OSError:
                        pass


# Public aliases for callers that want to inspect or share the store.
WorkOrderStore = _WorkOrderStore
AthenaWorkOrderStore = _WorkOrderStore


def normalize_destination(url):
    """Return a stable, comparable form of an execution destination.

    Scheme and host are lower-cased, default ports are dropped, and query
    and fragment components are ignored.  Paths are preserved because a
    locally registered backend may be distinguished by path.
    """
    parts = urlsplit(url)
    scheme = (parts.scheme or '').lower()
    host = (parts.hostname or '').lower()
    port = parts.port
    if port is not None:
        default = {'http': 80, 'https': 443}.get(scheme)
        if port != default:
            host = f'{host}:{port}'
    path = parts.path or '/'
    if not path.startswith('/'):
        path = '/' + path
    return urlunsplit((scheme, host, path, '', ''))


def _join_endpoint(endpoint_url, request_url):
    """Join a registered endpoint base with the request's path."""
    base = urlsplit(endpoint_url)
    request = urlsplit(request_url)
    base_path = base.path or ''
    request_path = request.path or '/'
    if base_path.endswith('/') and request_path.startswith('/'):
        path = base_path[:-1] + request_path
    elif not base_path:
        path = request_path
    elif not request_path.startswith('/'):
        path = base_path + '/' + request_path
    else:
        path = base_path + request_path
    return urlunsplit(
        (base.scheme, base.netloc, path, request.query, request.fragment)
    )


def _normalize_value(value):
    if isinstance(value, dict):
        return {
            str(key): _normalize_value(val)
            for key, val in sorted(
                value.items(), key=lambda item: str(item[0])
            )
        }
    if isinstance(value, (list, tuple)):
        return [_normalize_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _config_snapshot(params):
    """Build the configuration that must remain stable for a work order.

    ``ClientRequestToken`` is intentionally excluded: a work order's token
    is validated separately.  Dictionary key order is normalized so
    reordering ``QueryExecutionContext`` keys cannot look like a changed
    work order.
    """
    config = {}
    for key, value in params.items():
        if key in _WORK_ORDER_METADATA_KEYS:
            continue
        if key == 'ClientRequestToken':
            continue
        config[key] = value
    return _normalize_value(config)


def _config_differences(original, current):
    differences = []
    for key in sorted(set(original) | set(current)):
        if original.get(key) != current.get(key):
            differences.append(key)
    return differences


def _canonical_config(config):
    return json.dumps(config, sort_keys=True, separators=(',', ':'))


def _work_order_key(line_scope, ticket_id):
    line_value = line_scope if line_scope is not None else ''
    return f'{line_value}\0{ticket_id}'


def _work_order_display(line, ticket_id):
    if line:
        return f'{line}:{ticket_id}'
    return str(ticket_id)


def _canonicalize_param_keys(params):
    """Map snake_case work-order aliases onto their wire names."""
    for alias, wire_name in _WORK_ORDER_DIRECT_ALIASES.items():
        if alias in params and wire_name not in params:
            params[wire_name] = params.pop(alias)


def _extract_work_order_params(params):
    """Remove the work-order envelope from AWS API parameters.

    The envelope is accepted either as a ``WorkOrder`` mapping/string or
    as top-level parameters.  The original ``params`` mapping is mutated
    so the normal serializer never sees the envelope.
    """
    _canonicalize_param_keys(params)
    values = {}
    for envelope_key in _WORK_ORDER_WIRE_KEYS:
        envelope = params.pop(envelope_key, None)
        if envelope is None:
            continue
        if isinstance(envelope, dict):
            values.update(envelope)
        elif isinstance(envelope, str):
            values['id'] = envelope
        elif hasattr(envelope, 'to_dict'):
            values.update(envelope.to_dict())
        elif hasattr(envelope, '__dict__'):
            values.update(vars(envelope))
        else:
            raise InvalidWorkOrderError(
                reason='WorkOrder must be a mapping or a work order id '
                'string.'
            )

    for key in _WORK_ORDER_METADATA_KEYS:
        if key in _WORK_ORDER_WIRE_KEYS:
            continue
        if key in params:
            values[key] = params.pop(key)

    # Some callers put the token inside the normal AWS parameter set.
    for token_param in _TOKEN_PARAM_NAMES:
        if token_param in params and token_param != 'ClientRequestToken':
            values.setdefault(token_param, params.pop(token_param))

    ticket_id = None
    line = None
    backend_id = None
    backend_url = None
    backend_region = None
    token = None
    cache_dir = None
    work_group = None
    for key, value in values.items():
        lowered = str(key).replace('_', '').lower()
        if lowered in _WORK_ORDER_KEYS:
            ticket_id = value
        elif lowered in _LINE_KEYS:
            line = value
        elif lowered in _BACKEND_KEYS:
            backend_id = value
        elif lowered in _BACKEND_URL_KEYS:
            backend_url = value
        elif lowered in _BACKEND_REGION_KEYS:
            backend_region = value
        elif lowered in _TOKEN_KEYS:
            token = value
        elif lowered in _CACHE_KEYS:
            cache_dir = value
        elif lowered in ('workgroup', 'workorderworkgroup'):
            work_group = value

    # Accept "line:ticket" and "line/ticket" in an id-only convenience
    # form, but leave IDs that contain other punctuation alone.
    if isinstance(ticket_id, str) and line is None:
        for separator in (':', '/'):
            if separator in ticket_id:
                candidate_line, candidate_id = ticket_id.split(
                    separator, 1
                )
                if candidate_line and candidate_id:
                    line, ticket_id = candidate_line, candidate_id
                    break
    if ticket_id is None:
        raise InvalidWorkOrderError(
            reason='a work order id is required to submit a work order.'
        )
    return {
        'ticket_id': str(ticket_id),
        'line': str(line) if line is not None else None,
        'backend_id': str(backend_id) if backend_id is not None else None,
        'backend_url': (
            str(backend_url) if backend_url is not None else None
        ),
        'backend_region': (
            str(backend_region) if backend_region is not None else None
        ),
        'token': str(token) if token is not None else None,
        'cache_dir': (
            str(cache_dir) if cache_dir is not None else None
        ),
        'work_group': (
            str(work_group) if work_group is not None else None
        ),
    }


class _StaticRawResponse:
    def __init__(self, body):
        self._body = body

    def stream(self, **kwargs):
        if self._body:
            yield self._body


def _synthetic_http_response(url, query_execution_id):
    body = json.dumps({'QueryExecutionId': query_execution_id}).encode(
        'utf-8'
    )
    headers = {
        'Content-Type': 'application/x-amz-json-1.1',
        'Content-Length': str(len(body)),
    }
    http_response = AWSResponse(
        url, 200, headers, _StaticRawResponse(body)
    )
    return http_response, headers


def _synthetic_response(url, query_execution_id, client, model):
    """Build a parsed response for a work order resumed from the store."""
    http_response, _ = _synthetic_http_response(url, query_execution_id)
    body = json.dumps({'QueryExecutionId': query_execution_id}).encode(
        'utf-8'
    )
    parsed = client._response_parser.parse(
        {
            'status_code': 200,
            'headers': {
                'Content-Type': 'application/x-amz-json-1.1',
                'Content-Length': str(len(body)),
            },
            'body': body,
        },
        model.output_shape,
    )
    parsed.setdefault('ResponseMetadata', {}).setdefault(
        'RetryAttempts', 0
    )
    return http_response, parsed


def _resign_request(signer, operation_name, request):
    """Re-sign a prepared request after its destination changed.

    ``AWSPreparedRequest`` headers are bytes and the object lacks the
    ``params`` attribute SigV4 consults, so temporarily expose both in the
    shapes the signer expects, then restore the encoded header values.
    """
    if not hasattr(request, 'params'):
        request.params = {}
    decoded = {}
    for key, value in list(request.headers.items()):
        if isinstance(value, bytes):
            decoded[key] = value.decode('utf-8')
        else:
            decoded[key] = value
    for key, value in decoded.items():
        request.headers[key] = value
    try:
        signer.sign(operation_name, request)
    finally:
        for key, value in list(request.headers.items()):
            if isinstance(value, str):
                request.headers[key] = value.encode('utf-8')


class AthenaWorkOrderManager:
    """Client-attached manager for Athena resumable work orders."""

    def __init__(self, client):
        self._client = client
        config = getattr(client.meta, 'config', None)
        cache_dir = getattr(config, 'athena_work_order_cache', None)
        self._default_cache_dir = cache_dir
        self._store = None
        self._default_backend_id = None
        self._lock = threading.RLock()

    @property
    def store(self):
        if self._store is None:
            self._store = _WorkOrderStore(self._default_cache_dir)
        return self._store

    def use_store(self, cache_dir):
        self._store = _WorkOrderStore(cache_dir)
        return self._store

    def use_cache(self, cache_dir):
        return self.use_store(cache_dir)

    def register_backend(
        self, backend_id, endpoint_url=None, region_name=None
    ):
        if endpoint_url is None:
            endpoint_url = self._client.meta.endpoint_url
        backend = self.store.register_backend(
            backend_id, endpoint_url, region_name
        )
        self._default_backend_id = backend_id
        return backend.to_dict()

    def list_backends(self):
        return {
            key: value.to_dict()
            for key, value in self.store.list_backends().items()
        }

    def switch_backend(self, backend_id):
        previous = self._default_backend_id
        self._default_backend_id = backend_id
        return previous

    def current_backend(self):
        return self._default_backend_id

    def get_work_order(self, ticket_id, line=None, token=None):
        line_scope = None if line is None else str(line)
        key = _work_order_key(line_scope, str(ticket_id))
        record = self.store.get_work_order(key)
        if record is None:
            # Fall back to records written before work-order identity was
            # separated from the token.
            record = self._find_legacy_work_order(
                self.store, line_scope, str(ticket_id), token
            )
        if token is not None and record is not None:
            if record.get('token') != str(token):
                return None
        return record

    @staticmethod
    def _find_legacy_work_order(store, line_scope, ticket_id, token=None):
        directory = store._work_orders_dir
        if not os.path.isdir(directory):
            return None
        prefix = f'{line_scope or ""}\0{ticket_id}\0'
        for name in os.listdir(directory):
            if not name.endswith('.json'):
                continue
            record = store._read_json(os.path.join(directory, name))
            if record is None:
                continue
            if str(record.get('work_order_key', '')).startswith(prefix):
                if token is None or record.get('token') == token:
                    return record
        return None

    def reset(self, ticket_id=None, line=None, token=None):
        if ticket_id is not None:
            record = self.get_work_order(ticket_id, line=line, token=token)
            if record is not None:
                self.store.delete_work_order(record['work_order_key'])
            return
        self.store.reset()

    def _get_store(self, state):
        cache_dir = state.get('cache_dir') if state else None
        if cache_dir:
            return _WorkOrderStore(cache_dir)
        return self.store

    def capture_client_params(self, params, context, **kwargs):
        if context is None:
            return
        context[_WORK_ORDER_CONTEXT_KEY] = {'params': params}

    def _register_send_guard(self, context):
        guards = context.setdefault('send_guards', [])
        if self.guard_send not in guards:
            guards.append(self.guard_send)

    def prepare_params(self, params, context, **kwargs):
        """Extract work-order metadata and ensure a token exists."""
        if context is None:
            return
        # The provide-client-params handler may have been replaced by a
        # handler returning a new dict; refresh the captured mapping.
        existing = context.get(_WORK_ORDER_CONTEXT_KEY) or {}
        has_envelope = any(
            key in params for key in _WORK_ORDER_METADATA_KEYS
        )
        if not has_envelope:
            has_envelope = any(
                key in params for key in _WORK_ORDER_DIRECT_ALIASES
            )
        if not has_envelope:
            # Still refresh the reference for a possible WorkOrder added
            # by an earlier provide-client-params handler.
            if existing.get('work_order') is not None:
                context[_WORK_ORDER_CONTEXT_KEY] = dict(existing)
                context[_WORK_ORDER_CONTEXT_KEY]['params'] = params
            else:
                context.pop(_WORK_ORDER_CONTEXT_KEY, None)
            return
        work_order = _extract_work_order_params(params)
        envelope_token = work_order.get('token')
        explicit_token = params.get('ClientRequestToken')
        if (
            envelope_token is not None
            and explicit_token is not None
            and envelope_token != explicit_token
        ):
            raise InvalidWorkOrderError(
                reason='the work-order token and ClientRequestToken must '
                'match when both are supplied.'
            )
        token = envelope_token or explicit_token
        if token is None:
            token = str(uuid.uuid4())
        params['ClientRequestToken'] = token
        work_order['token'] = token
        if work_order.get('work_group') is not None:
            params['WorkGroup'] = work_order['work_group']
        backend_id = work_order.get('backend_id')
        backend_url = work_order.get('backend_url')
        if backend_id is not None or backend_url is not None:
            store = self.store
            if backend_id is None:
                inferred = store.infer_backend(backend_url)
                backend_id = (
                    inferred.backend_id if inferred is not None else None
                )
            if backend_id is None:
                raise InvalidWorkOrderError(
                    reason='an Athena backend id is required when an '
                    'explicit backend URL is supplied.'
                )
            if backend_url is not None:
                store.register_backend(
                    backend_id,
                    backend_url,
                    work_order.get('backend_region'),
                )
                work_order['backend_id'] = backend_id
        context[_WORK_ORDER_CONTEXT_KEY] = {
            'params': params,
            'work_order': work_order,
            'cache_dir': work_order.get('cache_dir'),
        }

    def _effective_backend_id(self, state):
        work_order = state.get('work_order') or {}
        backend_id = work_order.get('backend_id')
        if backend_id is None:
            backend_id = getattr(self._client, '_athena_backend_id', None)
        if backend_id is None:
            backend_id = self._default_backend_id
        return backend_id

    def _resolve_destination(self, state, request_dict):
        work_order = state['work_order']
        store = self._get_store(state)
        backend_id = self._effective_backend_id(state)
        current_url = request_dict['url']
        backend = store.get_backend(backend_id)
        if backend is not None:
            destination_url = _join_endpoint(backend.endpoint_url, current_url)
        else:
            destination_url = current_url
            if backend_id is None:
                inferred = store.infer_backend(current_url)
                if inferred is not None:
                    backend_id = inferred.backend_id
        return backend_id, destination_url

    def _backend_for_destination(self, store, destination_url, fallback=None):
        inferred = store.infer_backend(destination_url)
        if inferred is not None:
            return inferred.backend_id
        return fallback

    def _find_existing(self, store, work_order_key, token, ticket_id, line):
        existing = store.get_work_order(work_order_key)
        if existing is not None:
            return existing
        legacy_record = self._find_legacy_work_order(
            store, line, ticket_id, token
        )
        if legacy_record is None:
            return None
        migrated = {
            **legacy_record,
            'work_order_key': work_order_key,
            'line': line,
        }
        store.save_work_order(work_order_key, migrated)
        return migrated

    def _raise_token_conflict(self, owner, token, destination, display_id):
        raise WorkOrderTokenConflictError(
            token=token,
            target=destination,
            work_order_id=display_id,
            original_work_order_id=owner.get('work_order_id'),
            original_ticket_id=owner.get('ticket_id'),
            original_line=owner.get('line'),
        )

    def _claim_work_order(
        self, store, state, backend_id, destination_url, config, token,
        work_order_key, display_id, ticket_id, line_scope
    ):
        destination = normalize_destination(destination_url)
        target = f'{backend_id or ""}\0{destination}'
        token_owner = store.get_token(target, token)
        if token_owner is not None:
            if token_owner.get('work_order_key') != work_order_key:
                self._raise_token_conflict(
                    token_owner, token, destination, display_id
                )
        now = time.time()
        record = {
            'version': _STORE_VERSION,
            'work_order_key': work_order_key,
            'work_order_id': display_id,
            'ticket_id': ticket_id,
            'line': line_scope,
            'token': token,
            'backend_id': backend_id,
            'target': target,
            'endpoint_url': destination_url,
            'config': config,
            'status': 'prepared',
            'accepted': False,
            'query_execution_id': None,
            'created_at': now,
            'updated_at': now,
        }
        token_record = {
            'version': _STORE_VERSION,
            'work_order_key': work_order_key,
            'work_order_id': display_id,
            'ticket_id': ticket_id,
            'line': line_scope,
            'token': token,
            'backend_id': backend_id,
            'target': target,
            'created_at': now,
        }
        claimed, created = store.claim_token(target, token, token_record)
        if not created and claimed.get('work_order_key') != work_order_key:
            self._raise_token_conflict(
                claimed, token, destination, display_id
            )
        existing, created = store.claim_work_order(work_order_key, record)
        if not created and existing is not None:
            # The work order already belongs to another target.  Remove the
            # token claim we just made at this destination so it cannot
            # block an unrelated later submission.
            claimed_here = store.get_token(target, token)
            if (
                claimed_here is not None
                and claimed_here.get('work_order_key') == work_order_key
                and existing.get('endpoint_url') != destination_url
            ):
                store.delete_token(target, token)
            record = existing
        return record

    def before_call(
        self,
        model,
        params,
        request_signer,
        context,
        **kwargs,
    ):
        state = context.get(_WORK_ORDER_CONTEXT_KEY)
        if not state or 'work_order' not in state:
            return None
        store = self._get_store(state)
        work_order = state['work_order']
        api_params = state['params']
        token = api_params.get('ClientRequestToken')
        if token is None:
            token = work_order.get('token')
            if token is None:
                token = str(uuid.uuid4())
            api_params['ClientRequestToken'] = token
        work_order['token'] = token

        backend_id, destination_url = self._resolve_destination(state, params)
        params['url'] = destination_url
        config = _config_snapshot(api_params)
        ticket_id = work_order['ticket_id']
        line = work_order.get('line')
        line_scope = None if line is None else str(line)
        work_order_key = _work_order_key(line_scope, ticket_id)
        display_id = _work_order_display(line, ticket_id)

        with self._lock:
            existing = self._find_existing(
                store, work_order_key, token, ticket_id, line_scope
            )
            if existing is not None:
                return self._resume_existing(
                    existing=existing,
                    state=state,
                    params=params,
                    request_signer=request_signer,
                    context=context,
                    config=config,
                    token=token,
                    display_id=display_id,
                    operation_model=model,
                )

            state.update(
                {
                    'new_record': True,
                    'work_order_key': work_order_key,
                    'target_backend_id': backend_id,
                    'config': config,
                    'ticket_id': ticket_id,
                    'line_scope': line_scope,
                    'display_id': display_id,
                    'request_signer': request_signer,
                    'operation_name': getattr(model, 'name', None),
                    'operation_model': model,
                    'store': store,
                }
            )
            self._register_send_guard(context)
            return None

    def _validate_existing(self, existing, config, token, display_id):
        original_config = existing.get('config') or {}
        if _canonical_config(original_config) != _canonical_config(config):
            differences = _config_differences(original_config, config)
            raise WorkOrderConfigurationConflictError(
                work_order_id=display_id,
                original_work_order_id=existing.get('work_order_id'),
                original_ticket_id=existing.get('ticket_id'),
                original_line=existing.get('line'),
                token=token,
                target=existing.get('target'),
                differences=', '.join(differences) or 'unknown',
            )
        if existing.get('token') != token:
            raise WorkOrderConfigurationConflictError(
                work_order_id=display_id,
                original_work_order_id=existing.get('work_order_id'),
                original_ticket_id=existing.get('ticket_id'),
                original_line=existing.get('line'),
                token=token,
                target=existing.get('target'),
                differences='ClientRequestToken',
            )
        return existing

    def _resume_existing(
        self,
        existing,
        state,
        params,
        request_signer,
        context,
        config,
        token,
        display_id,
        operation_model,
    ):
        self._validate_existing(existing, config, token, display_id)

        store = state.get('store') or self._get_store(state)
        state['store'] = store
        state['record'] = existing
        state['work_order_key'] = existing.get('work_order_key')
        state['pinned_url'] = existing.get('endpoint_url')
        state['target'] = existing.get('target')
        state['request_signer'] = request_signer
        state['operation_name'] = 'StartQueryExecution'
        state['operation_model'] = operation_model
        self._register_send_guard(context)
        params['url'] = state['pinned_url']
        query_execution_id = existing.get('query_execution_id')
        if query_execution_id:
            return _synthetic_response(
                state['pinned_url'],
                query_execution_id,
                self._client,
                state['operation_model'],
            )
        return None

    def pin_request(self, request, **kwargs):
        state = request.context.get(_WORK_ORDER_CONTEXT_KEY)
        if not state:
            return
        pinned_url = state.get('pinned_url')
        if not pinned_url:
            return
        self._register_send_guard(request.context)
        if normalize_destination(request.url) != normalize_destination(
            pinned_url
        ):
            request.url = pinned_url
            for header in ('Host', 'Authorization', 'X-Amz-Date'):
                if header in request.headers:
                    del request.headers[header]

    def mark_sent(self, request, **kwargs):
        state = request.context.get(_WORK_ORDER_CONTEXT_KEY)
        if not state:
            return
        self._register_send_guard(request.context)
        record = state.get('record')
        if record is None:
            return
        if record.get('accepted'):
            return
        record = dict(record)
        record['accepted'] = True
        record['status'] = 'submitted'
        record['accepted_at'] = time.time()
        record['updated_at'] = record['accepted_at']
        store = state.get('store') or self.store
        store.save_work_order(record['work_order_key'], record)
        state['record'] = record
        state['store'] = store

    def guard_send(self, request, **kwargs):
        state = request.context.get(_WORK_ORDER_CONTEXT_KEY)
        if not state:
            return None
        if state.get('record') and state['record'].get('query_execution_id'):
            return _synthetic_http_response(
                state['record']['endpoint_url'],
                state['record']['query_execution_id'],
            )[0]

        pinned_url = state.get('pinned_url')
        store = state.get('store') or self.store
        if pinned_url is None:
            if not state.get('new_record'):
                return None
            with self._lock:
                record = state.get('record')
                if record is None:
                    backend = state.get('target_backend_id')
                    backend_id = self._backend_for_destination(
                        store, request.url, backend
                    )
                    record = self._claim_work_order(
                        store=store,
                        state=state,
                        backend_id=backend_id,
                        destination_url=request.url,
                        config=state['config'],
                        token=state['params']['ClientRequestToken'],
                        work_order_key=state['work_order_key'],
                        display_id=state['display_id'],
                        ticket_id=state['ticket_id'],
                        line_scope=state['line_scope'],
                    )
                    state['record'] = record
                    state['pinned_url'] = record['endpoint_url']
                    state['target'] = record['target']
                else:
                    state['pinned_url'] = record['endpoint_url']
                self._validate_existing(
                    record,
                    state['config'],
                    state['params']['ClientRequestToken'],
                    state['display_id'],
                )
            pinned_url = state['pinned_url']

        if normalize_destination(request.url) == normalize_destination(
            pinned_url
        ):
            return None
        # A request hook attempted to switch destinations.  Once a work
        # order may have been accepted, only its original target is valid.
        record = state.get('record') or {}
        request.url = pinned_url
        for header in ('Host', 'Authorization', 'X-Amz-Date'):
            if header in request.headers:
                del request.headers[header]
        signer = state.get('request_signer')
        operation_name = state.get('operation_name')
        if signer is not None and operation_name is not None:
            _resign_request(signer, operation_name, request)
        return None

    def after_call(self, parsed, context, **kwargs):
        state = context.get(_WORK_ORDER_CONTEXT_KEY)
        if not state or 'work_order' not in state:
            return
        record = state.get('record') or {}
        if 'work_order_key' not in record:
            return
        query_execution_id = (
            parsed.get('QueryExecutionId')
            if isinstance(parsed, dict)
            else None
        )
        if query_execution_id is None:
            return
        if record.get('query_execution_id') == query_execution_id:
            return
        record = dict(record)
        record['query_execution_id'] = query_execution_id
        record['status'] = 'completed'
        record['completed_at'] = time.time()
        record['updated_at'] = record['completed_at']
        store = state.get('store') or self.store
        store.save_work_order(record['work_order_key'], record)
        state['record'] = record


def _normalize_method_kwargs(kwargs):
    """Accept snake_case convenience names in work-order helper methods."""
    aliases = {
        'query_string': 'QueryString',
        'client_request_token': 'ClientRequestToken',
        'work_group': 'WorkGroup',
        'query': 'QueryString',
        'workgroup': 'WorkGroup',
        'query_execution_context': 'QueryExecutionContext',
        'execution_parameters': 'ExecutionParameters',
        'result_configuration': 'ResultConfiguration',
        'result_reuse_configuration': 'ResultReuseConfiguration',
        'engine_configuration': 'EngineConfiguration',
    }
    aliases.update(_WORK_ORDER_DIRECT_ALIASES)
    normalized = {}
    for key, value in kwargs.items():
        normalized[aliases.get(key, key)] = value
    return normalized


def _start_query_execution_work_order(self, **kwargs):
    """Submit a resumable Athena work order.

    This method accepts the normal ``StartQueryExecution`` parameters
    plus a ``WorkOrder`` mapping (or ``WorkOrderId``/``WorkOrderLine``
    and related aliases).  See :class:`AthenaWorkOrderManager` for the
    submission and resume rules.
    """
    params = _normalize_method_kwargs(kwargs)
    return self._make_api_call('StartQueryExecution', params)


def _install_token_validation_relaxation(client):
    """Allow caller-supplied work-order tokens shorter than the model min.

    Athena models ``ClientRequestToken`` with a 32-character minimum, but a
    work-order token is an opaque caller identifier.  When a work-order
    envelope is present we honor the supplied value exactly and relax only
    that instance of the length check.
    """
    original = client._convert_to_request_dict

    def convert(
        api_params,
        operation_model,
        endpoint_url,
        context=None,
        headers=None,
        set_user_agent_header=True,
    ):
        state = None
        if context is not None:
            state = context.get(_WORK_ORDER_CONTEXT_KEY)
        token = api_params.get('ClientRequestToken') if state else None
        shape = None
        if token:
            input_shape = operation_model.input_shape
            if input_shape is not None:
                shape = input_shape.members.get('ClientRequestToken')
        minimum = None
        if shape is not None:
            minimum = shape.metadata.get('min')
        if minimum is not None and len(token) < minimum:
            with _LOCK:
                saved = shape.metadata.pop('min', None)
            try:
                return original(
                    api_params=api_params,
                    operation_model=operation_model,
                    endpoint_url=endpoint_url,
                    context=context,
                    headers=headers,
                    set_user_agent_header=set_user_agent_header,
                )
            finally:
                if saved is not None:
                    with _LOCK:
                        shape.metadata['min'] = saved
        return original(
            api_params=api_params,
            operation_model=operation_model,
            endpoint_url=endpoint_url,
            context=context,
            headers=headers,
            set_user_agent_header=set_user_agent_header,
        )

    client._convert_to_request_dict = convert


def register_athena_work_orders(client):
    """Attach work-order methods and event handlers to an Athena client."""
    manager = AthenaWorkOrderManager(client)
    client._athena_work_order_manager = manager
    _install_token_validation_relaxation(client)

    def register_backend(
        backend_id, endpoint_url=None, region_name=None, **kwargs
    ):
        return manager.register_backend(
            backend_id, endpoint_url=endpoint_url, region_name=region_name
        )

    def list_backends():
        return manager.list_backends()

    def switch_backend(backend_id):
        return manager.switch_backend(backend_id)

    def current_backend():
        return manager.current_backend()

    def get_work_order(ticket_id, line=None, token=None):
        return manager.get_work_order(ticket_id, line=line, token=token)

    def reset_work_orders(ticket_id=None, line=None, token=None):
        return manager.reset(ticket_id=ticket_id, line=line, token=token)

    client.register_athena_backend = register_backend
    client.register_backend = register_backend
    client.list_athena_backends = list_backends
    client.switch_athena_backend = switch_backend
    client.use_athena_backend = switch_backend
    client.current_athena_backend = current_backend
    client.get_athena_work_order = get_work_order
    client.reset_athena_work_orders = reset_work_orders
    client.set_athena_work_order_cache = manager.use_cache

    client.start_query_execution_work_order = (
        _start_query_execution_work_order.__get__(client, type(client))
    )
    # The same entry point is exposed under common synonyms so callers can
    # use whichever name fits their ingestion pipeline.
    for name in (
        'submit_work_order',
        'start_query_execution_with_work_order',
        'start_resumable_query_execution',
        'submit_query_work_order',
        'resume_work_order',
        'resume_query_execution',
    ):
        setattr(client, name, client.start_query_execution_work_order)

    events = client.meta.events
    service_event = 'athena.StartQueryExecution'
    events.register(
        f'provide-client-params.{service_event}',
        manager.capture_client_params,
        unique_id='athena-work-order-capture-params',
    )
    events.register(
        f'before-parameter-build.{service_event}',
        manager.prepare_params,
        unique_id='athena-work-order-prepare-params',
    )
    events.register(
        f'before-call.{service_event}',
        manager.before_call,
        unique_id='athena-work-order-before-call',
    )
    events.register_first(
        f'request-created.{service_event}',
        manager.pin_request,
        unique_id='athena-work-order-pin-request',
    )
    events.register_last(
        f'before-send.{service_event}',
        manager.guard_send,
        unique_id='athena-work-order-guard-send',
    )
    events.register_last(
        f'before-send.{service_event}',
        manager.mark_sent,
        unique_id='athena-work-order-mark-sent',
    )
    events.register(
        f'after-call.{service_event}',
        manager.after_call,
        unique_id='athena-work-order-after-call',
    )
    return manager


# Friendly aliases used by callers/tests.
AthenaWorkOrder = AthenaWorkOrderManager
AthenaWorkOrderClient = AthenaWorkOrderManager
WorkOrderManager = AthenaWorkOrderManager
