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
"""Batching helpers built on top of the DynamoDB ``BatchGetItem`` API.

The :class:`BatchGetItemReader` gathers individual read requests (table,
full primary key, projection and consistency requirement), merges requests
that target the same key into a single wire read, and drives a bounded
number of actual transmissions per round.  Every transmission counts
against the round budget, including the SDK's own retries for service
errors and follow-up reads of keys the service reports as unprocessed.

Strongly consistent and eventually consistent requests are kept in
separate tiers so their capacity is accounted for separately.  After a
response is handled, waiting strongly consistent requests are scheduled
before eventually consistent unprocessed keys are re-read.
"""

import copy
import json

STRONG = True
EVENTUAL = False


class Projection:
    """A projection expression attached to a single read request.

    :type expression: str
    :param expression: The ``ProjectionExpression`` as the caller wrote
        it, including its own ``#name`` placeholders.

    :type expression_attribute_names: dict
    :param expression_attribute_names: The
        ``ExpressionAttributeNames`` map belonging to this expression.
        The same placeholder (for example ``#v``) may mean different
        things in different requests.
    """

    def __init__(self, expression, expression_attribute_names=None):
        self.expression = expression
        self.expression_attribute_names = dict(
            expression_attribute_names or {}
        )


class ReadResult:
    """Outcome of a single :class:`ReadRequest` after a round."""

    #: An item was returned; it is available on :attr:`item`.
    ITEM = 'item'
    #: The key was processed but no item exists for it this round.
    NOT_FOUND = 'not_found'
    #: The key was sent at least once but the service never finished
    #: processing it before the round ended.
    UNPROCESSED = 'unprocessed'
    #: The round ran out of transmissions before the key was ever sent.
    NOT_SENT = 'not_sent'

    def __init__(self, status, item=None):
        self.status = status
        self.item = item

    def __repr__(self):
        return f'ReadResult(status={self.status!r}, item={self.item!r})'


class ReadRequest:
    """Handle returned when a read request is added to a reader."""

    def __init__(self, table_name, key, projection, consistent_read):
        self.table_name = table_name
        self.key = key
        self.projection = projection
        self.consistent_read = consistent_read
        self.selected_paths = (
            None
            if projection is None
            else parse_projection(
                projection.expression,
                projection.expression_attribute_names,
            )
        )

    def __repr__(self):
        return (
            f'ReadRequest(table_name={self.table_name!r}, key={self.key!r}, '
            f'consistent_read={self.consistent_read!r})'
        )


class _KeyGroup:
    """Requests waiting on the same table/key/tier combination."""

    def __init__(self, table_name, key, consistent_read):
        self.table_name = table_name
        self.key = key
        self.consistent_read = consistent_read
        self.requests = []
        self.sent = False

    @property
    def key_attribute_names(self):
        return list(self.key.keys())

    def add(self, request):
        self.requests.append(request)


def parse_projection(expression, expression_attribute_names=None):
    """Parse a projection expression into normalized document paths.

    A path is a tuple of ``str`` attribute names and ``int`` list
    indexes.  ``#name`` placeholders are resolved through
    ``expression_attribute_names``.
    """
    names = expression_attribute_names or {}
    paths = []
    for raw_path in expression.split(','):
        raw_path = raw_path.strip()
        if not raw_path:
            continue
        segments = []
        for raw_segment in raw_path.split('.'):
            token = raw_segment
            while token:
                bracket = token.find('[')
                if bracket == -1:
                    segments.append(_resolve_name(token, names))
                    break
                if bracket > 0:
                    segments.append(_resolve_name(token[:bracket], names))
                closing = token.index(']', bracket)
                segments.append(int(token[bracket + 1 : closing]))
                token = token[closing + 1 :]
        paths.append(tuple(segments))
    return paths


def _resolve_name(token, names):
    if token.startswith('#'):
        return names[token]
    return token


def merge_paths(paths):
    """Merge a path set, dropping paths covered by a shorter one."""
    unique_paths = list(dict.fromkeys(paths))
    merged = []
    for path in unique_paths:
        covered_by_other = any(
            other != path and path[: len(other)] == other
            for other in unique_paths
        )
        if not covered_by_other:
            merged.append(path)
    return merged


def build_wire_projection(paths, key_attribute_names):
    """Build the wire projection for one table/tier combination.

    Only the selected paths and the primary key attributes needed to
    pair responses back to requests are included.  Every attribute name
    gets its own placeholder so that placeholders from distinct
    requests (including two different meanings of ``#v``) never collide.
    """
    ordered = list(paths)
    present = {path[0] for path in ordered if path}
    for name in key_attribute_names:
        if name not in present:
            ordered.append((name,))
            present.add(name)
    tokens = {}
    expression_paths = []
    for path in ordered:
        rendered = []
        for segment in path:
            if isinstance(segment, int):
                rendered[-1] = f'{rendered[-1]}[{segment}]'
            else:
                token = tokens.get(segment)
                if token is None:
                    token = f'#a{len(tokens)}'
                    tokens[segment] = token
                rendered.append(token)
        expression_paths.append('.'.join(rendered))
    attribute_names = {token: name for name, token in tokens.items()}
    return {
        'ProjectionExpression': ','.join(expression_paths),
        'ExpressionAttributeNames': attribute_names,
    }


def project_item(item, selected_paths):
    """Return only the selected paths from a parsed DynamoDB item.

    The item itself is a bare map of attribute name to AttributeValue;
    nested maps and lists are wrapped in their ``M``/``L`` containers.
    """
    if selected_paths is None:
        return copy.deepcopy(item)
    projected = {}
    for path in selected_paths:
        current_source = item
        current_target = projected
        for depth, segment in enumerate(path):
            last = depth == len(path) - 1
            if isinstance(segment, int):
                container = current_source.get('L')
                if not isinstance(container, list) or segment >= len(
                    container
                ):
                    break
                next_source = container[segment]
                target_list = current_target.setdefault('L', [])
                if len(target_list) <= segment:
                    target_list.extend(
                        {} for _ in range(segment + 1 - len(target_list))
                    )
                if last:
                    target_list[segment] = copy.deepcopy(next_source)
                    break
                if not isinstance(target_list[segment], dict):
                    target_list[segment] = {}
                current_source = next_source
                current_target = target_list[segment]
            elif depth == 0:
                if (
                    not isinstance(current_source, dict)
                    or segment not in current_source
                ):
                    break
                next_source = current_source[segment]
                if last:
                    current_target[segment] = copy.deepcopy(next_source)
                    break
                current_source = next_source
                current_target = current_target.setdefault(segment, {})
            else:
                container = current_source.get('M')
                if not isinstance(container, dict) or segment not in container:
                    break
                next_source = container[segment]
                target_map = current_target.setdefault('M', {})
                if last:
                    target_map[segment] = copy.deepcopy(next_source)
                    break
                current_source = next_source
                current_target = target_map.setdefault(segment, {})
    return projected


def _key_signature(key):
    return json.dumps(key, sort_keys=True, separators=(',', ':'), default=str)


class BatchGetItemReader:
    """Merge read requests into bounded ``BatchGetItem`` rounds.

    :type client: botocore.client.BaseClient
    :param client: A DynamoDB client.  It is called through its normal
        ``batch_get_item`` method, so its existing retry configuration
        keeps applying to every transmission.

    :type max_sends: int
    :param max_sends: Maximum number of actual transmissions allowed in
        a single :meth:`run_round` call.  Initial sends, SDK retries of
        service errors and follow-up reads of unprocessed keys all
        count against this limit.
    """

    def __init__(self, client, max_sends):
        self._client = client
        self._max_sends = max_sends
        self._groups = {}
        self._results = {}
        self._delivered = set()
        self.transmissions = []
        self._current_tier = None

    @property
    def max_sends(self):
        return self._max_sends

    @property
    def send_count(self):
        return len(self.transmissions)

    @property
    def results(self):
        return dict(self._results)

    def add_request(
        self, table_name, key, projection=None, consistent_read=False
    ):
        """Queue a read request.

        May be called while a round is in flight; the request joins the
        pending work and is picked up by the next scheduling decision.

        :type table_name: str
        :param table_name: Table to read from.

        :type key: dict
        :param key: The full primary key as DynamoDB AttributeValues.

        :type projection: Projection
        :param projection: The projection to apply, or ``None`` to read
            every attribute.

        :type consistent_read: bool
        :param consistent_read: ``True`` for a strongly consistent read,
            ``False`` (the default) for an eventually consistent one.

        :rtype: ReadRequest
        """
        request = ReadRequest(table_name, key, projection, consistent_read)
        group_key = (table_name, _key_signature(key), consistent_read)
        group = self._groups.get(group_key)
        if group is None:
            group = _KeyGroup(table_name, key, consistent_read)
            self._groups[group_key] = group
        group.add(request)
        return request

    def run_round(self):
        """Run one bounded round and return each request's result.

        :rtype: dict
        :returns: A map of every :class:`ReadRequest` handed out by this
            reader to its :class:`ReadResult`.
        """
        events = self._client.meta.events
        events.register('before-send', self._count_transmission)
        round_start = len(self.transmissions)
        try:
            while len(self.transmissions) - round_start < self._max_sends:
                tier = self._next_tier()
                if tier is None:
                    break
                request_items, sent_groups = self._build_request_items(tier)
                for group in sent_groups:
                    group.sent = True
                self._current_tier = tier
                try:
                    response = self._client.batch_get_item(
                        RequestItems=request_items
                    )
                finally:
                    self._current_tier = None
                self._process_response(response, sent_groups)
        finally:
            events.unregister('before-send', self._count_transmission)
        self._finalize()
        return self.results

    def _count_transmission(self, request, **kwargs):
        body = request.body
        if isinstance(body, bytes):
            body = body.decode('utf-8')
        payload = json.loads(body)
        self.transmissions.append(
            {
                'consistent_read': self._current_tier,
                'request_items': copy.deepcopy(
                    payload.get('RequestItems', {})
                ),
            }
        )

    def _next_tier(self):
        if any(
            group.consistent_read == STRONG and self._has_pending(group)
            for group in self._groups.values()
        ):
            return STRONG
        if any(
            group.consistent_read == EVENTUAL and self._has_pending(group)
            for group in self._groups.values()
        ):
            return EVENTUAL
        return None

    def _has_pending(self, group):
        return any(
            request not in self._delivered for request in group.requests
        )

    def _build_request_items(self, tier):
        request_items = {}
        sent_groups = []
        table_paths = {}
        table_key_names = {}
        for group in self._groups.values():
            if group.consistent_read != tier or not self._has_pending(group):
                continue
            sent_groups.append(group)
            entry = request_items.setdefault(
                group.table_name, {'Keys': [], 'ConsistentRead': tier}
            )
            entry['Keys'].append(copy.deepcopy(group.key))
            paths = table_paths.setdefault(group.table_name, [])
            for request in group.requests:
                if request in self._delivered:
                    continue
                if request.selected_paths is None:
                    paths.append(None)
                else:
                    paths.extend(request.selected_paths)
            table_key_names.setdefault(group.table_name, []).extend(
                group.key_attribute_names
            )
        for table_name, entry in request_items.items():
            path_lists = table_paths[table_name]
            if any(paths is None for paths in path_lists):
                continue
            merged = merge_paths(path_lists)
            unique_key_names = list(dict.fromkeys(table_key_names[table_name]))
            entry.update(build_wire_projection(merged, unique_key_names))
        return request_items, sent_groups

    def _process_response(self, response, sent_groups):
        responses = response.get('Responses', {}) or {}
        unprocessed = response.get('UnprocessedKeys', {}) or {}
        unprocessed_signatures = self._unprocessed_signatures(unprocessed)
        for group in sent_groups:
            signature = _key_signature(group.key)
            if signature in unprocessed_signatures.get(
                group.table_name, set()
            ):
                continue
            item = self._find_item(responses.get(group.table_name, []), group)
            pending = [
                request
                for request in group.requests
                if request not in self._delivered
            ]
            if item is None:
                for request in pending:
                    self._deliver(request, ReadResult(ReadResult.NOT_FOUND))
            else:
                for request in pending:
                    projected = project_item(item, request.selected_paths)
                    self._deliver(
                        request, ReadResult(ReadResult.ITEM, projected)
                    )

    def _unprocessed_signatures(self, unprocessed):
        signatures_by_table = {}
        for table_name, entry in unprocessed.items():
            signatures = signatures_by_table.setdefault(table_name, set())
            for key in entry.get('Keys', []):
                signatures.add(_key_signature(key))
        return signatures_by_table

    def _find_item(self, items, group):
        key_names = group.key_attribute_names
        for item in items:
            if all(
                name in item and item[name] == group.key[name]
                for name in key_names
            ):
                return item
        return None

    def _deliver(self, request, result):
        self._results[request] = result
        self._delivered.add(request)

    def _finalize(self):
        for group in self._groups.values():
            for request in group.requests:
                if request in self._delivered:
                    continue
                status = (
                    ReadResult.UNPROCESSED
                    if group.sent
                    else ReadResult.NOT_SENT
                )
                self._deliver(request, ReadResult(status))
        self._groups = {}
