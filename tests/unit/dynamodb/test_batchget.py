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
import json
from unittest import TestCase

from botocore.dynamodb.batchget import (
    EVENTUAL,
    STRONG,
    BatchGetItemReader,
    Projection,
    ReadResult,
    build_wire_projection,
    merge_paths,
    parse_projection,
    project_item,
)

TABLE = 'invoices'


def key(number, year=2025):
    return {'invoice.id': {'S': str(number)}, 'year': {'N': str(year)}}


class FakeRequest:
    def __init__(self, request_items):
        self.body = json.dumps({'RequestItems': request_items}).encode()


class FakeEvents:
    def __init__(self):
        self._handlers = []

    def register(self, event_name, handler):
        self._handlers.append(handler)

    def unregister(self, event_name, handler):
        self._handlers.remove(handler)

    def emit(self, request):
        for handler in list(self._handlers):
            handler(request=request)


class FakeMeta:
    def __init__(self):
        self.events = FakeEvents()


class FakeClient:
    """Minimal stand-in that re-emits before-send like a real client."""

    def __init__(self, script, on_send=None):
        self.meta = FakeMeta()
        self._script = list(script)
        self._on_send = on_send
        self.calls = []

    def batch_get_item(self, RequestItems):
        self.calls.append(RequestItems)
        response = None
        while self._script:
            outcome = self._script.pop(0)
            self.meta.events.emit(FakeRequest(RequestItems))
            if self._on_send is not None:
                self._on_send(RequestItems)
            if isinstance(outcome, Exception):
                # An SDK-retryable service error: another transmission.
                continue
            response = outcome
            break
        if response is None:
            raise AssertionError('fake client ran out of scripted responses')
        return response


class ParseProjectionTest(TestCase):
    def test_resolves_placeholders_and_nested_paths(self):
        paths = parse_projection(
            '#v, #v.net.#u, items[2].name',
            {'#v': 'profile', '#u': 'usd'},
        )
        self.assertEqual(
            paths,
            [
                ('profile',),
                ('profile', 'net', 'usd'),
                ('items', 2, 'name'),
            ],
        )

    def test_same_placeholder_keeps_its_own_meaning(self):
        first = parse_projection('#v', {'#v': 'profile'})
        second = parse_projection('#v.net.usd, status', {'#v': 'profile'})
        self.assertEqual(first, [('profile',)])
        self.assertEqual(second, [('profile', 'net', 'usd'), ('status',)])


class MergePathsTest(TestCase):
    def test_drops_paths_covered_by_shorter_one(self):
        merged = merge_paths(
            [
                ('profile', 'net', 'usd'),
                ('profile',),
                ('status',),
            ]
        )
        self.assertEqual(merged, [('profile',), ('status',)])


class BuildWireProjectionTest(TestCase):
    def test_adds_only_missing_key_attributes_and_aliases_names(self):
        wire = build_wire_projection(
            [('profile', 'net', 'usd'), ('status',)],
            ['invoice.id', 'year'],
        )
        names = wire['ExpressionAttributeNames']
        self.assertEqual(
            names,
            {
                '#a0': 'profile',
                '#a1': 'net',
                '#a2': 'usd',
                '#a3': 'status',
                '#a4': 'invoice.id',
                '#a5': 'year',
            },
        )
        self.assertEqual(
            wire['ProjectionExpression'],
            '#a0.#a1.#a2,#a3,#a4,#a5',
        )

    def test_reuses_placeholder_for_shared_attribute(self):
        wire = build_wire_projection(
            [('status',), ('profile',), ('profile', 'net')],
            ['year'],
        )
        names = wire['ExpressionAttributeNames']
        self.assertEqual(names['#a0'], 'status')
        self.assertEqual(names['#a1'], 'profile')
        self.assertIn('#a1.#a2', wire['ProjectionExpression'])


class ProjectItemTest(TestCase):
    ITEM = {
        'invoice.id': {'S': '17'},
        'year': {'N': '2025'},
        'status': {'S': 'pending'},
        'profile': {
            'M': {
                'net': {'M': {'usd': {'N': '100'}, 'eur': {'N': '90'}}},
                'owner': {'S': 'ops'},
            }
        },
    }

    def test_full_item_when_no_projection(self):
        self.assertEqual(project_item(self.ITEM, None), self.ITEM)

    def test_nested_path_only_returns_selected_content(self):
        projected = project_item(
            self.ITEM, [('profile', 'net', 'usd'), ('status',)]
        )
        self.assertEqual(
            projected,
            {
                'status': {'S': 'pending'},
                'profile': {'M': {'net': {'M': {'usd': {'N': '100'}}}}},
            },
        )

    def test_full_profile_does_not_leak_into_subfield_request(self):
        full = project_item(self.ITEM, [('profile',)])
        sub = project_item(self.ITEM, [('profile', 'net', 'usd')])
        self.assertIn('owner', full['profile']['M'])
        self.assertNotIn('owner', sub['profile']['M']['net']['M'])
        self.assertNotIn('eur', sub['profile']['M']['net']['M'])

    def test_key_only_returned_to_request_that_selected_it(self):
        with_key = project_item(self.ITEM, [('status',), ('invoice.id',)])
        without_key = project_item(self.ITEM, [('status',)])
        self.assertIn('invoice.id', with_key)
        self.assertNotIn('invoice.id', without_key)
        self.assertNotIn('year', without_key)


class BatchGetItemReaderTest(TestCase):
    def _reader(self, script, max_sends, on_send=None):
        client = FakeClient(script, on_send=on_send)
        return BatchGetItemReader(client, max_sends), client

    def test_same_key_requests_share_one_transmission_with_aliased_v(self):
        response = {
            'Responses': {
                TABLE: [
                    {
                        'invoice.id': {'S': '17'},
                        'year': {'N': '2025'},
                        'status': {'S': 'pending'},
                        'profile': {
                            'M': {
                                'net.usd': {'N': '100'},
                                'owner': {'S': 'ops'},
                            }
                        },
                    }
                ]
            },
            'UnprocessedKeys': {},
        }
        reader, client = self._reader([response], max_sends=3)
        full_profile = reader.add_request(
            TABLE,
            key(17),
            Projection('#v', {'#v': 'profile'}),
            consistent_read=EVENTUAL,
        )
        sub_field = reader.add_request(
            TABLE,
            key(17),
            Projection(
                '#p.#v, #s',
                {'#p': 'profile', '#v': 'net.usd', '#s': 'status'},
            ),
            consistent_read=EVENTUAL,
        )
        results = reader.run_round()

        self.assertEqual(len(reader.transmissions), 1)
        wire = reader.transmissions[0]['request_items'][TABLE]
        self.assertEqual(len(wire['Keys']), 1)
        self.assertFalse(wire['ConsistentRead'])
        names = wire['ExpressionAttributeNames']
        self.assertEqual(len(set(names.values())), len(names))
        self.assertEqual(
            set(names.values()),
            {'profile', 'status', 'invoice.id', 'year'},
        )

        full_item = results[full_profile].item
        sub_item = results[sub_field].item
        self.assertEqual(results[full_profile].status, ReadResult.ITEM)
        self.assertIn('owner', full_item['profile']['M'])
        self.assertEqual(
            sub_item,
            {
                'profile': {'M': {'net.usd': {'N': '100'}}},
                'status': {'S': 'pending'},
            },
        )
        self.assertNotIn('owner', sub_item['profile']['M'])
        self.assertNotIn('invoice.id', sub_item)

    def test_tiers_are_sent_separately_and_strong_goes_first(self):
        ok = {'Responses': {TABLE: []}, 'UnprocessedKeys': {}}
        reader, client = self._reader([ok, ok], max_sends=3)
        eventual_request = reader.add_request(
            TABLE, key(18), consistent_read=False
        )
        strong_request = reader.add_request(
            TABLE,
            key(17),
            Projection('#s', {'#s': 'status'}),
            consistent_read=True,
        )
        results = reader.run_round()

        self.assertEqual(len(reader.transmissions), 2)
        self.assertEqual(reader.transmissions[0]['consistent_read'], STRONG)
        self.assertEqual(reader.transmissions[1]['consistent_read'], EVENTUAL)
        self.assertTrue(
            reader.transmissions[0]['request_items'][TABLE]['ConsistentRead']
        )
        self.assertFalse(
            reader.transmissions[1]['request_items'][TABLE]['ConsistentRead']
        )
        self.assertEqual(
            results[eventual_request].status, ReadResult.NOT_FOUND
        )
        self.assertEqual(results[strong_request].status, ReadResult.NOT_FOUND)

    def test_strong_requests_arriving_mid_round_skip_eventual_reread_line(
        self,
    ):
        first_response = {
            'Responses': {
                TABLE: [{'invoice.id': {'S': '17'}, 'year': {'N': '2025'}}]
            },
            'UnprocessedKeys': {
                TABLE: {
                    'Keys': [key(19)],
                    'ConsistentRead': False,
                }
            },
        }
        second_response = {
            'Responses': {
                TABLE: [
                    {
                        'invoice.id': {'S': '17'},
                        'year': {'N': '2025'},
                        'status': {'S': 'posted'},
                    }
                ]
            },
            'UnprocessedKeys': {
                TABLE: {'Keys': [key(19)], 'ConsistentRead': True}
            },
        }

        def add_strong_requests(request_items):
            if not getattr(add_strong_requests, 'fired', False):
                add_strong_requests.fired = True
                reader.add_request(
                    TABLE,
                    key(17),
                    Projection('#s', {'#s': 'status'}),
                    consistent_read=STRONG,
                )
                reader.add_request(
                    TABLE,
                    key(19),
                    Projection('#s', {'#s': 'status'}),
                    consistent_read=STRONG,
                )

        reader, client = self._reader(
            [first_response, second_response],
            max_sends=2,
            on_send=add_strong_requests,
        )
        reader.add_request(TABLE, key(17), consistent_read=EVENTUAL)
        reader.add_request(TABLE, key(18), consistent_read=EVENTUAL)
        reader.add_request(TABLE, key(19), consistent_read=EVENTUAL)
        results = reader.run_round()

        tiers = [t['consistent_read'] for t in reader.transmissions]
        self.assertEqual(tiers, [EVENTUAL, STRONG])
        second_keys = [
            k['invoice.id']['S']
            for k in reader.transmissions[1]['request_items'][TABLE]['Keys']
        ]
        self.assertEqual(sorted(second_keys), ['17', '19'])
        self.assertEqual(
            [result.status for result in results.values()].count(
                ReadResult.NOT_FOUND
            ),
            1,
        )
        self.assertEqual(
            [result.status for result in results.values()].count(
                ReadResult.UNPROCESSED
            ),
            2,
        )

    def test_retry_transmissions_count_against_the_budget(self):
        reader, client = self._reader(
            [
                RuntimeError('service error'),
                {'Responses': {}, 'UnprocessedKeys': {}},
            ],
            max_sends=2,
        )
        reader.add_request(TABLE, key(17))
        reader.run_round()
        self.assertEqual(reader.send_count, 2)
        self.assertEqual(len(client.calls), 1)

    def test_unprocessed_requests_are_reread_then_marked_unprocessed(self):
        first = {
            'Responses': {},
            'UnprocessedKeys': {
                TABLE: {'Keys': [key(17), key(18)], 'ConsistentRead': False}
            },
        }
        second = {
            'Responses': {},
            'UnprocessedKeys': {
                TABLE: {'Keys': [key(18)], 'ConsistentRead': False}
            },
        }
        reader, client = self._reader([first, second], max_sends=2)
        first_request = reader.add_request(TABLE, key(17))
        second_request = reader.add_request(TABLE, key(18))
        results = reader.run_round()

        self.assertEqual(reader.send_count, 2)
        self.assertEqual(results[first_request].status, ReadResult.NOT_FOUND)
        self.assertEqual(
            results[second_request].status, ReadResult.UNPROCESSED
        )

    def test_budget_exhausted_marks_late_tier_not_sent(self):
        def add_strong_requests(request_items):
            reader.add_request(
                TABLE,
                key(17),
                Projection('#s', {'#s': 'status'}),
                consistent_read=STRONG,
            )
            reader.add_request(
                TABLE,
                key(19),
                Projection('#s', {'#s': 'status'}),
                consistent_read=STRONG,
            )

        reader, client = self._reader(
            [{'Responses': {}, 'UnprocessedKeys': {}}],
            max_sends=1,
            on_send=add_strong_requests,
        )
        eventual_request = reader.add_request(
            TABLE, key(18), consistent_read=EVENTUAL
        )
        results = reader.run_round()

        self.assertEqual(reader.send_count, 1)
        late = [
            result
            for request, result in results.items()
            if request.consistent_read == STRONG
        ]
        self.assertEqual(len(late), 2)
        self.assertTrue(
            all(result.status == ReadResult.NOT_SENT for result in late)
        )
        self.assertEqual(
            results[eventual_request].status, ReadResult.NOT_FOUND
        )

    def test_missing_item_is_reported_not_found(self):
        reader, client = self._reader(
            [{'Responses': {}, 'UnprocessedKeys': {}}], max_sends=1
        )
        request = reader.add_request(TABLE, key(404))
        results = reader.run_round()
        self.assertEqual(results[request].status, ReadResult.NOT_FOUND)

    def test_budget_resets_for_each_round(self):
        reader, client = self._reader(
            [
                {'Responses': {}, 'UnprocessedKeys': {}},
                {'Responses': {}, 'UnprocessedKeys': {}},
            ],
            max_sends=1,
        )
        first = reader.add_request(TABLE, key(17))
        first_results = reader.run_round()
        self.assertEqual(first_results[first].status, ReadResult.NOT_FOUND)

        second = reader.add_request(TABLE, key(18))
        second_results = reader.run_round()
        self.assertEqual(second_results[second].status, ReadResult.NOT_FOUND)
        self.assertEqual(reader.send_count, 2)
        self.assertEqual(len(reader.transmissions), 2)
