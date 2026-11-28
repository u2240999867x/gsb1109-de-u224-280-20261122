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
from collections import defaultdict

from botocore.awsrequest import AWSResponse
from botocore.compat import urlsplit
from botocore.exceptions import ReadTimeoutError


class _Body:
    def __init__(self, body):
        self._body = body

    def stream(self, **kwargs):
        if self._body:
            yield self._body


class LocalAthena:
    """A tiny in-process Athena replacement for functional tests.

    Requests are routed by endpoint host.  Each endpoint remembers the
    tokens it has accepted and returns the same query execution id for a
    repeated token, mirroring Athena's target-scoped idempotency.
    """

    def __init__(self, timeouts=None):
        self.requests = []
        self.executions = defaultdict(dict)
        self.next_id = 0
        # ``timeouts`` maps an endpoint host to the number of initial
        # requests that should be accepted but fail before the response is
        # read.
        self.timeouts = dict(timeouts or {})

    def send(self, request):
        return self(request)

    def close(self):
        pass

    def __call__(self, request, **kwargs):
        request_body = request.body
        if isinstance(request_body, (bytes, bytearray)):
            request_body = request_body.decode('utf-8')
        payload = json.loads(request_body or '{}')
        host = urlsplit(request.url).netloc
        self.requests.append(
            {
                'host': host,
                'url': request.url,
                'payload': payload,
                'headers': dict(request.headers),
            }
        )
        endpoint = self.executions[host]
        token = payload.get('ClientRequestToken')
        if host in self.timeouts and self.timeouts[host] > 0:
            self.timeouts[host] -= 1
            if token is not None and token not in endpoint:
                self.next_id += 1
                endpoint[token] = f'{host}-query-{self.next_id}'
            raise ReadTimeoutError(endpoint_url=request.url)
        if token is None:
            self.next_id += 1
            query_id = f'{host}-query-{self.next_id}'
        elif token not in endpoint:
            self.next_id += 1
            endpoint[token] = f'{host}-query-{self.next_id}'
            query_id = endpoint[token]
        else:
            query_id = endpoint[token]
        body = json.dumps({'QueryExecutionId': query_id}).encode('utf-8')
        return AWSResponse(
            request.url,
            200,
            {'Content-Type': 'application/x-amz-json-1.1'},
            _Body(body),
        )

    def requests_for(self, host):
        return [r for r in self.requests if r['host'] == host]
