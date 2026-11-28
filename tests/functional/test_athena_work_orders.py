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
import shutil
import tempfile

from botocore.exceptions import (
    WorkOrderConfigurationConflictError,
    WorkOrderTokenConflictError,
)
from tests import BaseSessionTest

from .athena_doubles import LocalAthena

QUERY = 'SELECT * FROM events WHERE ds = ? AND zone = ?'
BINDINGS = ["'2026-11-22'", "'north'"]
TOKEN = 'planner-ticket-token-17-000000000000'


class TestAthenaResumableWorkOrders(BaseSessionTest):
    def setUp(self):
        super().setUp()
        self.cache_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.cache_dir, True)
        self.athena = LocalAthena(timeouts={'west.example.com': 1})
        self.west = 'https://west.example.com'
        self.east = 'https://east.example.com'
        self.athena.timeouts = {'west.example.com': 1}

        self.north = self._client(self.west)
        self.south = self._client(self.west)

        self.north.register_athena_backend('west', self.west)
        self.north.register_athena_backend('east', self.east)
        self.south.register_athena_backend('west', self.west)

        # Parameter hooks set the actual workgroup for each line while the
        # request records the destination chosen by the planner.
        self.north.meta.events.register(
            'before-parameter-build.athena.StartQueryExecution',
            self._set_north_workgroup,
        )
        self.south.meta.events.register(
            'before-parameter-build.athena.StartQueryExecution',
            self._set_south_workgroup,
        )

        # Retry hook: only a retried request is redirected to the east
        # backend, mirroring a request hook that inspects SDK retry state.
        self.retry_destinations = []

        def redirect_north(request, **kwargs):
            retries = request.context.get('retries', {})
            if retries.get('attempt', 1) > 1:
                self.east_seen_by_retry_hook = True
                request.url = request.url.replace(
                    'west.example.com', 'east.example.com'
                )

        self.north.meta.events.register(
            'before-send.athena.StartQueryExecution', redirect_north
        )
        self._register_athena()

    def _register_athena(self):
        for client in (self.north, self.south):
            self._stub_client(client)

    def _stub_client(self, client):
        # Replace only the transport, not the event layer, so request hooks
        # and the work-order send guard both run as they would in production.
        client._endpoint.http_session = self.athena

    def _client(self, endpoint_url):
        client = self.session.create_client(
            'athena',
            region_name='us-east-1',
            endpoint_url=endpoint_url,
            aws_access_key_id='access',
            aws_secret_access_key='secret',
        )
        client._athena_work_order_manager.use_store(self.cache_dir)
        return client

    def _set_north_workgroup(self, params, **kwargs):
        if 'ClientRequestToken' in params:
            params['WorkGroup'] = 'north-line'

    def _set_south_workgroup(self, params, **kwargs):
        if 'ClientRequestToken' in params:
            params['WorkGroup'] = 'south-line'

    def _submit(self, client, *, token=TOKEN, line=None, query=QUERY,
                bindings=BINDINGS, work_group=None, backend_id=None,
                context=None):
        work_order = {'TicketId': '17'}
        if line is not None:
            work_order['Line'] = line
        if backend_id is not None:
            work_order['AthenaBackendId'] = backend_id
        if work_group is not None:
            work_order['WorkGroup'] = work_group
        params = {
            'QueryString': query,
            'ExecutionParameters': list(bindings),
            'ClientRequestToken': token,
            'WorkOrder': work_order,
            'QueryExecutionContext': context or {'Database': 'analytics'},
        }
        return client.start_query_execution_work_order(**params)

    def test_work_order_redirected_retry_stays_on_original_target(self):
        response = self._submit(
            self.north,
            line='north',
            backend_id='west',
            work_group='north-line',
        )
        # The first send was accepted at west but the response timed out.
        # The retry must stay on west and reuse the query id.
        self.assertEqual(
            response['QueryExecutionId'], 'west.example.com-query-1'
        )
        north_west = self.athena.requests_for('west.example.com')
        north_east = self.athena.requests_for('east.example.com')
        self.assertEqual(len(north_west), 2)
        self.assertEqual(north_east, [])

    def test_first_send_hook_may_choose_destination_before_acceptance(self):
        self.athena.timeouts = {}

        def choose_east(request, **kwargs):
            request.url = request.url.replace(
                'west.example.com', 'east.example.com'
            )

        self.north.meta.events.register(
            'before-send.athena.StartQueryExecution', choose_east
        )
        response = self._submit(
            self.north,
            line='north',
            backend_id='west',
            work_group='north-line',
        )
        self.assertEqual(
            response['QueryExecutionId'], 'east.example.com-query-1'
        )
        self.assertEqual(self.athena.requests_for('west.example.com'), [])

    def test_resume_after_acceptance_returns_original_id_without_send(self):
        first = self._submit(
            self.north,
            line='north',
            backend_id='west',
            work_group='north-line',
        )
        requests_before = len(self.athena.requests)
        resumed = self._submit(
            self.north,
            line='north',
            backend_id='east',
            work_group='north-line',
        )
        self.assertEqual(
            resumed['QueryExecutionId'], first['QueryExecutionId']
        )
        self.assertEqual(len(self.athena.requests), requests_before)
        self.assertEqual(self.athena.requests_for('east.example.com'), [])

    def test_conflicting_token_submission_reports_original_work_order(self):
        self._submit(
            self.north,
            line='north',
            backend_id='west',
            work_group='north-line',
        )
        with self.assertRaises(WorkOrderTokenConflictError) as ctx:
            self._submit(
                self.south,
                line='south',
                backend_id='west',
                work_group='south-line',
            )
        self.assertIn('north:17', str(ctx.exception))
        self.assertEqual(ctx.exception.original_ticket_id, '17')
        self.assertEqual(ctx.exception.original_line, 'north')
        # South must not have sent the token to west.
        for request in self.athena.requests_for('west.example.com'):
            if request['payload'].get('WorkGroup') == 'south-line':
                self.fail('South request reached west with the shared token')

    def test_resume_after_restart_reuses_id_and_ignores_context_order(self):
        first = self._submit(
            self.north,
            line='north',
            backend_id='west',
            work_group='north-line',
            context={'Database': 'analytics', 'Catalog': 'AwsDataCatalog'},
        )
        restarted = self._client(self.west)
        restarted.register_athena_backend('west', self.west)
        restarted.meta.events.register(
            'before-parameter-build.athena.StartQueryExecution',
            self._set_north_workgroup,
        )
        self.athena.timeouts = {}
        resumed = self._submit(
            restarted,
            line='north',
            backend_id='west',
            work_group='north-line',
            context={'Catalog': 'AwsDataCatalog', 'Database': 'analytics'},
        )
        self.assertEqual(
            resumed['QueryExecutionId'], first['QueryExecutionId']
        )
        self.assertEqual(
            len(self.athena.requests_for('west.example.com')),
            2,
            'resume should not re-execute the query',
        )

    def test_changed_workgroup_reports_original_configuration_conflict(self):
        self.north.meta.events.unregister(
            'before-parameter-build.athena.StartQueryExecution',
            self._set_north_workgroup,
        )
        self._submit(
            self.north,
            line='north',
            backend_id='west',
            work_group='north-line',
        )
        with self.assertRaises(WorkOrderConfigurationConflictError) as ctx:
            self._submit(
                self.north,
                line='north',
                backend_id='west',
                work_group='north-line-v2',
            )
        self.assertIn('north:17', str(ctx.exception))

    def test_changed_bindings_report_configuration_conflict(self):
        self._submit(
            self.north,
            line='north',
            backend_id='west',
            work_group='north-line',
        )
        with self.assertRaises(WorkOrderConfigurationConflictError):
            self._submit(
                self.north,
                line='north',
                backend_id='west',
                work_group='north-line',
                bindings=list(reversed(BINDINGS)),
            )

    def test_same_ticket_with_changed_token_reports_conflict(self):
        self._submit(
            self.north,
            line='north',
            backend_id='west',
            work_group='north-line',
        )
        with self.assertRaises(WorkOrderConfigurationConflictError) as ctx:
            self._submit(
                self.north,
                token='changed-token-000000000000000000',
                line='north',
                backend_id='west',
                work_group='north-line',
            )
        self.assertIn('north:17', str(ctx.exception))
        self.assertEqual(len(self.athena.requests_for('west.example.com')), 2)

    def test_envelope_can_carry_token_and_workgroup(self):
        client = self._client(self.west)
        client.register_athena_backend('west', self.west)
        self.athena.timeouts = {}
        self._stub_client(client)
        response = client.start_query_execution_work_order(
            QueryString=QUERY,
            ExecutionParameters=list(BINDINGS),
            WorkOrder={
                'TicketId': '17',
                'Line': 'north',
                'AthenaBackendId': 'west',
                'WorkGroup': 'north-line',
                'ClientRequestToken': 'nested-token-000000000000000000',
            },
        )
        self.assertTrue(response['QueryExecutionId'])
        payload = self.athena.requests[-1]['payload']
        self.assertEqual(payload['WorkGroup'], 'north-line')
        self.assertEqual(
            payload['ClientRequestToken'],
            'nested-token-000000000000000000',
        )

    def test_south_with_new_token_executes_independently(self):
        self._submit(
            self.north,
            line='north',
            backend_id='west',
            work_group='north-line',
        )
        south = self._submit(
            self.south,
            token='south-ticket-17-0000000000000000',
            line='south',
            backend_id='west',
            work_group='south-line',
        )
        self.assertNotEqual(
            south['QueryExecutionId'], 'west.example.com-query-1'
        )
        self.assertEqual(len(self.athena.requests_for('west.example.com')), 3)

    def test_short_planner_token_is_honored_exactly(self):
        response = self._submit(
            self.north,
            token='ticket-17',
            line='north',
            backend_id='west',
            work_group='north-line',
        )
        self.assertTrue(response['QueryExecutionId'])
        self.assertEqual(
            self.athena.requests[-1]['payload']['ClientRequestToken'],
            'ticket-17',
        )

    def test_calls_without_work_order_use_normal_sdk_behavior(self):
        response = self.north.start_query_execution(
            QueryString=QUERY,
            ExecutionParameters=list(BINDINGS),
            ClientRequestToken=TOKEN,
            WorkGroup='north-line',
        )
        self.assertTrue(response['QueryExecutionId'])
        self.assertEqual(len(self.athena.requests_for('west.example.com')), 1)
        self.assertEqual(
            self.athena.requests[0]['payload']['QueryString'], QUERY
        )
