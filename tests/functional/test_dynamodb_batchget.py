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

from botocore.config import Config
from botocore.dynamodb.batchget import (
    BatchGetItemReader,
    Projection,
    ReadResult,
)
from tests import BaseSessionTest, ClientHTTPStubber, mock

TABLE = 'invoices'


def key(number, year=2025):
    return {'invoice.id': {'S': str(number)}, 'year': {'N': str(year)}}


def wire_body(request):
    body = request.body
    if isinstance(body, bytes):
        body = body.decode('utf-8')
    return json.loads(body)


class TestBatchGetItemReader(BaseSessionTest):
    def setUp(self):
        super().setUp()
        self.region = 'us-west-2'
        self.sleep_patch = mock.patch('time.sleep')
        self.sleep_patch.start()
        self.client = self.session.create_client(
            'dynamodb',
            self.region,
            config=Config(retries={'max_attempts': 3, 'mode': 'legacy'}),
        )
        self.http_stubber = ClientHTTPStubber(self.client)

    def tearDown(self):
        super().tearDown()
        self.sleep_patch.stop()

    def _eventual_requests(self, reader):
        full_profile = reader.add_request(
            TABLE,
            key(17),
            Projection('#v', {'#v': 'profile'}),
            consistent_read=False,
        )
        sub_field = reader.add_request(
            TABLE,
            key(17),
            Projection(
                '#p.#v, #s',
                {'#p': 'profile', '#v': 'net.usd', '#s': 'status'},
            ),
            consistent_read=False,
        )
        eighteen = reader.add_request(
            TABLE,
            key(18),
            Projection('#s', {'#s': 'status'}),
            consistent_read=False,
        )
        nineteen = reader.add_request(
            TABLE,
            key(19),
            Projection('#s', {'#s': 'status'}),
            consistent_read=False,
        )
        return full_profile, sub_field, eighteen, nineteen

    def _inject_strong_requests(self, reader):
        state = {'attempt': 0}

        def handler(request, **kwargs):
            state['attempt'] += 1
            if state['attempt'] != 2:
                return
            reader.add_request(
                TABLE,
                key(17),
                Projection('#s', {'#s': 'status'}),
                consistent_read=True,
            )
            reader.add_request(
                TABLE,
                key(19),
                Projection('#s', {'#s': 'status'}),
                consistent_read=True,
            )

        self.client.meta.events.register('before-send', handler)
        return handler

    def _eventual_partial_response(self):
        return {
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
            'UnprocessedKeys': {
                TABLE: {'Keys': [key(19)], 'ConsistentRead': False}
            },
        }

    def _strong_response(self):
        return {
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

    def test_round_of_three_merges_retries_and_prioritizes_strong(self):
        reader = BatchGetItemReader(self.client, max_sends=3)
        (
            full_profile,
            sub_field,
            eighteen,
            nineteen,
        ) = self._eventual_requests(reader)
        inject_handler = self._inject_strong_requests(reader)

        with self.http_stubber as stubber:
            stubber.add_response(status=500, body=b'{}')
            stubber.add_response(
                status=200,
                body=json.dumps(self._eventual_partial_response()).encode(),
            )
            stubber.add_response(
                status=200,
                body=json.dumps(self._strong_response()).encode(),
            )
            results = reader.run_round()
        self.client.meta.events.unregister('before-send', inject_handler)

        self.assertEqual(len(self.http_stubber.requests), 3)
        self.assertEqual(reader.send_count, 3)
        first_body = wire_body(self.http_stubber.requests[0])
        retry_body = wire_body(self.http_stubber.requests[1])
        strong_body = wire_body(self.http_stubber.requests[2])

        self.assertEqual(first_body, retry_body)
        self.assertEqual(reader.transmissions[0]['consistent_read'], False)
        self.assertEqual(reader.transmissions[1]['consistent_read'], False)
        self.assertEqual(reader.transmissions[2]['consistent_read'], True)

        eventual_entry = first_body['RequestItems'][TABLE]
        self.assertFalse(eventual_entry['ConsistentRead'])
        self.assertEqual(
            sorted(item['invoice.id']['S'] for item in eventual_entry['Keys']),
            ['17', '18', '19'],
        )
        self.assertEqual(
            set(eventual_entry['ExpressionAttributeNames'].values()),
            {'profile', 'status', 'invoice.id', 'year'},
        )

        strong_entry = strong_body['RequestItems'][TABLE]
        self.assertTrue(strong_entry['ConsistentRead'])
        self.assertEqual(
            sorted(item['invoice.id']['S'] for item in strong_entry['Keys']),
            ['17', '19'],
        )
        self.assertEqual(
            set(strong_entry['ExpressionAttributeNames'].values()),
            {'status', 'invoice.id', 'year'},
        )

        self.assertEqual(results[full_profile].status, ReadResult.ITEM)
        self.assertEqual(
            results[full_profile].item,
            {
                'profile': {
                    'M': {
                        'net.usd': {'N': '100'},
                        'owner': {'S': 'ops'},
                    }
                }
            },
        )
        self.assertEqual(results[sub_field].status, ReadResult.ITEM)
        self.assertEqual(
            results[sub_field].item,
            {
                'profile': {'M': {'net.usd': {'N': '100'}}},
                'status': {'S': 'pending'},
            },
        )
        self.assertNotIn('invoice.id', results[sub_field].item)
        self.assertNotIn('year', results[sub_field].item)
        self.assertNotIn('owner', results[sub_field].item['profile']['M'])

        self.assertEqual(results[eighteen].status, ReadResult.NOT_FOUND)
        self.assertEqual(results[nineteen].status, ReadResult.UNPROCESSED)

        strong_results = [
            result
            for request, result in results.items()
            if request.consistent_read
        ]
        strong_items = [
            result
            for result in strong_results
            if result.status == ReadResult.ITEM
        ]
        self.assertEqual(len(strong_items), 1)
        self.assertEqual(strong_items[0].item, {'status': {'S': 'posted'}})
        self.assertEqual(
            sum(
                result.status == ReadResult.UNPROCESSED
                for result in strong_results
            ),
            1,
        )

    def test_round_of_two_stops_before_sending_strong_requests(self):
        reader = BatchGetItemReader(self.client, max_sends=2)
        (
            full_profile,
            sub_field,
            eighteen,
            nineteen,
        ) = self._eventual_requests(reader)
        inject_handler = self._inject_strong_requests(reader)

        with self.http_stubber as stubber:
            stubber.add_response(status=500, body=b'{}')
            stubber.add_response(
                status=200,
                body=json.dumps(self._eventual_partial_response()).encode(),
            )
            results = reader.run_round()
        self.client.meta.events.unregister('before-send', inject_handler)

        self.assertEqual(len(self.http_stubber.requests), 2)
        self.assertEqual(reader.send_count, 2)
        for transmission in reader.transmissions:
            self.assertFalse(transmission['consistent_read'])

        strong_requests = [
            request for request in results if request.consistent_read
        ]
        self.assertEqual(len(strong_requests), 2)
        for request in strong_requests:
            self.assertEqual(results[request].status, ReadResult.NOT_SENT)
            self.assertIsNone(results[request].item)

        self.assertEqual(results[full_profile].status, ReadResult.ITEM)
        self.assertEqual(results[sub_field].status, ReadResult.ITEM)
        self.assertEqual(results[eighteen].status, ReadResult.NOT_FOUND)
        self.assertEqual(results[nineteen].status, ReadResult.UNPROCESSED)

    def test_direct_client_retry_configuration_still_applies(self):
        with self.http_stubber as stubber:
            stubber.add_response(status=500, body=b'{}')
            stubber.add_response(status=500, body=b'{}')
            stubber.add_response(status=200, body=b'{}')
            response = self.client.batch_get_item(
                RequestItems={
                    TABLE: {
                        'Keys': [key(17)],
                        'ConsistentRead': False,
                    }
                }
            )
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(len(self.http_stubber.requests), 3)
