# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for specific
# language governing permissions and limitations under the License.
import json
import os
import shutil
import tempfile

import pytest

import botocore.session
from botocore.exceptions import (
    DataNotFoundError,
    ModelReleaseBaselineChangedError,
    ModelReleaseValidationError,
    UnknownModelReleaseError,
    UnknownServiceError,
)
from botocore.loaders import Loader
from botocore.model_releases import (
    MANIFEST_FILENAME,
    REMOVED,
    RETAINED,
    REPLACED,
    ModelReleaseCatalog,
    ModelReleaseManifest,
    resource_digest,
)
from tests import BaseEnvVar

SOURCE_SERVICE = 'source-svc'
LOAD_SERVICE = 'load-svc'
API_VERSION = '2020-01-01'


def _write(path, data):
    directory = os.path.dirname(path)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory)
    with open(path, 'w', encoding='utf-8') as fp:
        json.dump(data, fp)


def _service_model(field):
    return {
        "version": "2.0",
        "metadata": {
            "apiVersion": API_VERSION,
            "endpointPrefix": "svc",
            "protocol": "json",
            "serviceId": "Svc",
            "signatureVersion": "v4",
            "signingName": "svc",
            "jsonVersion": "1.1",
        },
        "operations": {
            "ListThings": {
                "name": "ListThings",
                "http": {"method": "POST", "requestUri": "/"},
                "input": {"shape": "ListThingsInput"},
                "output": {"shape": "ListThingsOutput"},
            }
        },
        "shapes": {
            "ListThingsInput": {
                "type": "structure",
                "members": {field: {"shape": "Str"}},
            },
            "ListThingsOutput": {
                "type": "structure",
                "members": {
                    "Things": {"type": "list", "member": {"shape": "Str"}},
                    "NextToken": {"shape": "Str"},
                    "PaginationToken": {"shape": "Str"},
                },
            },
            "Str": {"type": "string"},
        },
    }


def _paginators(token):
    return {
        "pagination": {
            "ListThings": {
                "input_token": token,
                "output_token": token,
                "result_key": "Things",
            }
        }
    }


def _waiters(max_attempts):
    return {
        "version": 2,
        "waiters": {
            "ThingsReady": {
                "delay": 10,
                "maxAttempts": max_attempts,
                "operation": "ListThings",
                "acceptors": [
                    {"matcher": "status", "expected": 200, "state": "success"}
                ],
            }
        },
    }


def _ruleset(host):
    return {
        "version": "1.3",
        "parameters": {
            "Region": {
                "builtIn": "AWS::Region",
                "required": True,
                "type": "String",
            }
        },
        "rules": [
            {
                "conditions": [
                    {
                        "fn": "aws.partition",
                        "argv": [{"ref": "Region"}],
                        "assign": "PartitionResult",
                    }
                ],
                "type": "tree",
                "rules": [
                    {
                        "conditions": [],
                        "endpoint": {
                            "url": (
                                f"https://{host}.{{Region}}."
                                "{PartitionResult#dnsSuffix}"
                            ),
                            "properties": {},
                            "headers": {},
                        },
                        "type": "endpoint",
                    }
                ],
            }
        ],
    }


def _partitions(suffix):
    return {
        "version": 3,
        "partitions": [
            {
                "id": "aws",
                "regionRegex": r"^(us|eu)\-\w+\-\d+$",
                "regions": {"us-east-1": {}},
                "outputs": {
                    "dnsSuffix": suffix,
                    "dualStackDnsSuffix": suffix,
                    "name": "aws",
                    "supportsDualStack": True,
                    "supportsFIPS": True,
                    "implicitGlobalRegion": "us-east-1",
                },
            }
        ],
    }


def _extras():
    # Native sdk-extras envelope: only the "merge" document is applied.
    return {
        "merge": {
            "shapes": {
                "ListThingsInput": {
                    "members": {"ExtraFromExtras": {"shape": "Str"}}
                }
            }
        }
    }


def _build_deployment(root, label, field, token, suffix,
                      load_waiter_attempts=None):
    base = os.path.join(root, 'deploy', label)
    _write(
        f'{base}/{SOURCE_SERVICE}/{API_VERSION}/service-2.json',
        _service_model(field),
    )
    _write(
        f'{base}/{SOURCE_SERVICE}/{API_VERSION}/service-2.sdk-extras.json',
        _extras(),
    )
    _write(
        f'{base}/{SOURCE_SERVICE}/{API_VERSION}/paginators-1.json',
        _paginators(token),
    )
    _write(
        f'{base}/{SOURCE_SERVICE}/{API_VERSION}/endpoint-rule-set-1.json',
        _ruleset('src'),
    )
    _write(
        f'{base}/{LOAD_SERVICE}/{API_VERSION}/service-2.json',
        _service_model('KeepField'),
    )
    _write(
        f'{base}/{LOAD_SERVICE}/{API_VERSION}/endpoint-rule-set-1.json',
        _ruleset('load'),
    )
    if load_waiter_attempts is not None:
        _write(
            f'{base}/{LOAD_SERVICE}/{API_VERSION}/waiters-2.json',
            _waiters(load_waiter_attempts),
        )
    _write(f'{base}/partitions.json', _partitions(suffix))
    return base


def _entry(state, data=None):
    entry = {"state": state, "digest": None}
    if data is not None:
        entry["digest"] = resource_digest(data)
    return entry


def _data_at(deploy_dir, resource):
    path = os.path.join(deploy_dir, resource + '.json')
    with open(path, encoding='utf-8') as fp:
        return json.load(fp)


def _write_manifest(release_dir, release_id, document):
    _write(
        os.path.join(release_dir, 'manifests', f'{release_id}.json'),
        document,
    )


def _baseline_manifest(deploy_dir):
    resources = {}
    for resource in (
        f'{SOURCE_SERVICE}/{API_VERSION}/service-2',
        f'{SOURCE_SERVICE}/{API_VERSION}/service-2.sdk-extras',
        f'{SOURCE_SERVICE}/{API_VERSION}/paginators-1',
        f'{SOURCE_SERVICE}/{API_VERSION}/endpoint-rule-set-1',
        f'{LOAD_SERVICE}/{API_VERSION}/service-2',
        f'{LOAD_SERVICE}/{API_VERSION}/endpoint-rule-set-1',
        f'{LOAD_SERVICE}/{API_VERSION}/waiters-2',
        'partitions',
    ):
        resources[resource] = _entry(REPLACED, _data_at(deploy_dir, resource))
    return {"baseline": None, "resources": resources}


class ReleaseFixture:
    """Builds a G1/G2/G3 deployment + release tree in a temp directory."""

    def __init__(self):
        self.root = tempfile.mkdtemp(prefix='model-release-')
        self.g1 = _build_deployment(
            self.root, 'G1', 'OldField', 'NextToken', 'g1.example', 5
        )
        self.g2 = _build_deployment(
            self.root,
            'G2',
            'NewField',
            'PaginationToken',
            'g2.example',
            load_waiter_attempts=None,
        )
        self.g3 = _build_deployment(
            self.root, 'G3', 'OldField', 'NextToken', 'g1.example', 7
        )
        # A lower priority directory that still ships the load waiter.
        self.low_priority = os.path.join(self.root, 'lowpriority')
        _write(
            f'{self.low_priority}/{LOAD_SERVICE}/{API_VERSION}/waiters-2.json',
            _waiters(5),
        )
        self.release_dir = os.path.join(self.root, 'releases')
        _write_manifest(
            self.release_dir, 'G1', _baseline_manifest(self.g1)
        )
        self._write_g2_manifest()
        self._write_g3_manifest()

    def _write_g2_manifest(self):
        resources = {
            f'{SOURCE_SERVICE}/{API_VERSION}/service-2': _entry(
                REPLACED,
                _data_at(
                    self.g2, f'{SOURCE_SERVICE}/{API_VERSION}/service-2'
                ),
            ),
            f'{SOURCE_SERVICE}/{API_VERSION}/service-2.sdk-extras': {
                "state": RETAINED,
                "digest": None,
            },
            f'{SOURCE_SERVICE}/{API_VERSION}/paginators-1': _entry(
                REPLACED,
                _data_at(
                    self.g2, f'{SOURCE_SERVICE}/{API_VERSION}/paginators-1'
                ),
            ),
            'partitions': _entry(
                REPLACED, _data_at(self.g2, 'partitions')
            ),
            f'{LOAD_SERVICE}/{API_VERSION}/waiters-2': {
                "state": REMOVED,
                "digest": None,
            },
            f'{SOURCE_SERVICE}/{API_VERSION}/endpoint-rule-set-1': {
                "state": RETAINED,
                "digest": None,
            },
            f'{LOAD_SERVICE}/{API_VERSION}/service-2': {
                "state": RETAINED,
                "digest": None,
            },
            f'{LOAD_SERVICE}/{API_VERSION}/endpoint-rule-set-1': {
                "state": RETAINED,
                "digest": None,
            },
        }
        _write_manifest(
            self.release_dir, 'G2', {"baseline": "G1", "resources": resources}
        )

    def _write_g3_manifest(self):
        resources = {
            f'{LOAD_SERVICE}/{API_VERSION}/waiters-2': _entry(
                REPLACED,
                _data_at(
                    self.g3, f'{LOAD_SERVICE}/{API_VERSION}/waiters-2'
                ),
            ),
            'partitions': {"state": RETAINED, "digest": None},
            f'{SOURCE_SERVICE}/{API_VERSION}/service-2': {
                "state": RETAINED,
                "digest": None,
            },
            f'{SOURCE_SERVICE}/{API_VERSION}/service-2.sdk-extras': {
                "state": RETAINED,
                "digest": None,
            },
            f'{SOURCE_SERVICE}/{API_VERSION}/paginators-1': {
                "state": RETAINED,
                "digest": None,
            },
            f'{SOURCE_SERVICE}/{API_VERSION}/endpoint-rule-set-1': {
                "state": RETAINED,
                "digest": None,
            },
            f'{LOAD_SERVICE}/{API_VERSION}/service-2': {
                "state": RETAINED,
                "digest": None,
            },
            f'{LOAD_SERVICE}/{API_VERSION}/endpoint-rule-set-1': {
                "state": RETAINED,
                "digest": None,
            },
        }
        _write_manifest(
            self.release_dir, 'G3', {"baseline": "G1", "resources": resources}
        )

    def loader(self, deploy_dir):
        # The deployment path outranks the low priority customer path.
        return Loader(extra_search_paths=[deploy_dir, self.low_priority])

    def catalog(self):
        return ModelReleaseCatalog(self.release_dir)

    def cleanup(self):
        shutil.rmtree(self.root, ignore_errors=True)


class TestResourceDigest:
    def test_digest_is_stable_and_content_sensitive(self):
        data = {"b": 1, "a": [1, 2]}
        reordered = {"a": [1, 2], "b": 1}
        assert resource_digest(data) == resource_digest(reordered)
        changed = {"a": [1, 2], "b": 2}
        assert resource_digest(data) != resource_digest(changed)


class TestManifestValidation:
    def test_rejects_unknown_state(self):
        document = {
            "baseline": "G1",
            "resources": {"partitions": {"state": "bogus", "digest": "x"}},
        }
        try:
            ModelReleaseManifest.from_document('GX', document)
        except ModelReleaseValidationError as error:
            assert error.kwargs['mismatched_resources'] == ['partitions']
        else:
            raise AssertionError('expected ModelReleaseValidationError')

    def test_rejects_replaced_without_digest(self):
        document = {
            "baseline": "G1",
            "resources": {"partitions": {"state": REPLACED, "digest": None}},
        }
        with pytest.raises(ModelReleaseValidationError):
            ModelReleaseManifest.from_document('GX', document)

    def test_rejects_retained_without_baseline(self):
        document = {
            "baseline": None,
            "resources": {"partitions": {"state": RETAINED, "digest": None}},
        }
        with pytest.raises(ModelReleaseValidationError):
            ModelReleaseManifest.from_document('GX', document)

    def test_round_trip_document(self):
        document = {
            "baseline": "G1",
            "resources": {
                "partitions": {"state": REPLACED, "digest": "abc"}
            },
        }
        manifest = ModelReleaseManifest.from_document('G2', document)
        assert manifest.baseline == 'G1'
        assert manifest.state('partitions') == REPLACED
        assert manifest.to_document() == document


class TestCatalog:
    def setup_method(self):
        self.fixture = ReleaseFixture()

    def teardown_method(self):
        self.fixture.cleanup()

    def test_list_releases_and_unknown_release(self):
        catalog = self.fixture.catalog()
        assert catalog.list_releases() == ['G1', 'G2', 'G3']
        with pytest.raises(UnknownModelReleaseError):
            catalog.get_manifest('nope')

    def test_prepare_stages_matching_replacements(self):
        catalog = self.fixture.catalog()
        catalog.prepare('G1', self.fixture.loader(self.fixture.g1))
        assert catalog.is_prepared('G1')
        staged = os.path.join(
            self.fixture.release_dir, 'G1', MANIFEST_FILENAME
        )
        assert os.path.isfile(staged)

    def test_prepare_flags_every_mismatched_resource(self):
        catalog = self.fixture.catalog()
        # The baseline release is already staged, so retained resources can
        # resolve and only the replaced files read from the wrong deployment
        # are reported.
        catalog.prepare('G1', self.fixture.loader(self.fixture.g1))
        with pytest.raises(ModelReleaseValidationError) as caught:
            catalog.prepare('G2', self.fixture.loader(self.fixture.g3))
        mismatched = caught.value.kwargs['mismatched_resources']
        assert set(mismatched) == {
            f'{SOURCE_SERVICE}/{API_VERSION}/service-2',
            f'{SOURCE_SERVICE}/{API_VERSION}/paginators-1',
            'partitions',
        }
        # A failed preparation must not be marked ready.
        assert not catalog.is_prepared('G2')

    def test_retained_resources_are_copied_from_baseline(self):
        catalog = self.fixture.catalog()
        catalog.prepare('G1', self.fixture.loader(self.fixture.g1))
        catalog.prepare('G2', self.fixture.loader(self.fixture.g2))
        retained = os.path.join(
            self.fixture.release_dir,
            'G2',
            f'{LOAD_SERVICE}/{API_VERSION}/service-2',
        )
        assert os.path.isfile(retained)

    def test_removed_resources_are_not_staged(self):
        catalog = self.fixture.catalog()
        catalog.prepare('G1', self.fixture.loader(self.fixture.g1))
        catalog.prepare('G2', self.fixture.loader(self.fixture.g2))
        removed = os.path.join(
            self.fixture.release_dir,
            'G2',
            f'{LOAD_SERVICE}/{API_VERSION}/waiters-2',
        )
        assert not os.path.exists(removed)

    def test_activate_requires_a_prepared_release(self):
        catalog = self.fixture.catalog()
        with pytest.raises(UnknownModelReleaseError):
            catalog.activate('G2')

    def test_activate_rejects_changed_baseline(self):
        catalog = self.fixture.catalog()
        catalog.prepare('G1', self.fixture.loader(self.fixture.g1))
        catalog.activate('G1')
        catalog.prepare('G2', self.fixture.loader(self.fixture.g2))
        catalog.prepare('G3', self.fixture.loader(self.fixture.g3))
        # Another window activates G3 first.
        catalog.activate('G3')
        with pytest.raises(ModelReleaseBaselineChangedError) as caught:
            catalog.activate('G2')
        assert caught.value.kwargs == {
            'release_id': 'G2',
            'baseline_id': 'G1',
            'active_id': 'G3',
        }

    def test_active_release_survives_a_new_catalog_instance(self):
        catalog = self.fixture.catalog()
        catalog.prepare('G1', self.fixture.loader(self.fixture.g1))
        catalog.activate('G1')
        # Simulate a restart / different machine reopening the same dir.
        reopened = self.fixture.catalog()
        assert reopened.active_release == 'G1'
        assert reopened.is_prepared('G1')

    def test_create_loader_requires_prepared_release(self):
        catalog = self.fixture.catalog()
        with pytest.raises(UnknownModelReleaseError):
            catalog.create_loader('G1', self.fixture.loader(self.fixture.g1))


class TestPinnedModelLoader:
    def setup_method(self):
        self.fixture = ReleaseFixture()
        self.catalog = self.fixture.catalog()
        self.base_loader = self.fixture.loader(self.fixture.g1)
        self.catalog.prepare('G1', self.base_loader)
        self.catalog.activate('G1')
        self.catalog.prepare('G2', self.fixture.loader(self.fixture.g2))

    def teardown_method(self):
        self.fixture.cleanup()

    def _pinned(self, release_id):
        return self.catalog.create_loader(release_id, self.base_loader)

    def test_managed_global_resource_is_served_from_snapshot(self):
        pinned = self._pinned('G1')
        suffix = pinned.load_data('partitions')['partitions'][0]['outputs'][
            'dnsSuffix'
        ]
        assert suffix == 'g1.example'

    def test_replaced_partition_takes_effect(self):
        pinned = self._pinned('G2')
        suffix = pinned.load_data('partitions')['partitions'][0]['outputs'][
            'dnsSuffix'
        ]
        assert suffix == 'g2.example'

    def test_removed_service_resource_raises_not_found(self):
        pinned = self._pinned('G2')
        with pytest.raises(UnknownServiceError):
            pinned.load_service_model(
                LOAD_SERVICE, 'waiters-2', API_VERSION
            )
        # Version resolution must also report the tool as unavailable rather
        # than falling back to a same named file on a lower priority path.
        with pytest.raises(UnknownServiceError):
            pinned.load_service_model(LOAD_SERVICE, 'waiters-2')

    def test_removed_resource_masks_lower_priority_file(self):
        # The low priority directory still ships this waiter, but the
        # release withdrawal wins over the normal search path.
        pinned = self._pinned('G2')
        assert LOAD_SERVICE not in pinned.list_available_services('waiters-2')
        with pytest.raises(DataNotFoundError):
            pinned.load_data(
                f'{LOAD_SERVICE}/{API_VERSION}/waiters-2'
            )

    def test_replaced_service_model_is_served(self):
        pinned = self._pinned('G2')
        model = pinned.load_service_model(
            SOURCE_SERVICE, 'service-2', API_VERSION
        )
        members = model['shapes']['ListThingsInput']['members']
        assert 'NewField' in members
        assert 'OldField' not in members

    def test_extras_merge_envelope_is_applied(self):
        # The {"merge": ...} envelope must be unwrapped: the added member is
        # present but the envelope itself must not leak into the model.
        for release_id in ('G1', 'G2'):
            pinned = self._pinned(release_id)
            model = pinned.load_service_model(
                SOURCE_SERVICE, 'service-2', API_VERSION
            )
            members = model['shapes']['ListThingsInput']['members']
            assert 'ExtraFromExtras' in members
            assert 'merge' not in model

    def test_retained_service_model_matches_baseline(self):
        g2 = self._pinned('G2')
        g1 = self._pinned('G1')
        retained = g2.load_service_model(
            LOAD_SERVICE, 'service-2', API_VERSION
        )
        baseline = g1.load_service_model(
            LOAD_SERVICE, 'service-2', API_VERSION
        )
        assert retained == baseline

    def test_unmanaged_resource_delegates_to_base_loader(self):
        pinned = self._pinned('G1')
        # _retry is global botocore data not managed by any manifest.
        assert pinned.load_data('_retry') == self.base_loader.load_data(
            '_retry'
        )

    def test_list_api_versions_reflects_managed_resource(self):
        pinned = self._pinned('G2')
        assert API_VERSION in pinned.list_api_versions(
            SOURCE_SERVICE, 'paginators-1'
        )
        with pytest.raises(DataNotFoundError):
            pinned.list_api_versions(LOAD_SERVICE, 'waiters-2')


class TestSessionReleases(BaseEnvVar):
    def setUp(self):
        super().setUp()
        self.environ['AWS_ACCESS_KEY_ID'] = 'access'
        self.environ['AWS_SECRET_ACCESS_KEY'] = 'secret'
        self.environ['AWS_EC2_METADATA_DISABLED'] = 'true'
        self.fixture = ReleaseFixture()

    def tearDown(self):
        self.fixture.cleanup()
        super().tearDown()

    def _session(self, deploy_dir):
        session = botocore.session.Session()
        session.configure_model_release_directory(
            self.fixture.release_dir
        )
        session.register_component(
            'data_loader', self.fixture.loader(deploy_dir)
        )
        return session

    def _prepare(self, session, release_id, deploy_dir):
        # Preparation reads replaced files through the normal search path,
        # which must point at the matching deployment directory.
        session.register_component(
            'data_loader', self.fixture.loader(deploy_dir)
        )
        session.prepare_model_release(release_id)

    def _resolved_endpoint(self, client, operation='ListThings'):
        operation_model = client.meta.service_model.operation_model(operation)
        return client._ruleset_resolver.construct_endpoint(
            operation_model=operation_model,
            call_args={},
            request_context={},
        ).url

    def test_pinned_client_uses_release_paginator_and_partitions(self):
        session = self._session(self.fixture.g1)
        self._prepare(session, 'G1', self.fixture.g1)
        session.activate_model_release('G1')
        self._prepare(session, 'G2', self.fixture.g2)
        session.activate_model_release('G2')
        client = session.create_client(
            SOURCE_SERVICE,
            'us-east-1',
            model_release_id='G2',
        )
        token = client.get_paginator('list_things')._input_token
        assert token == ['PaginationToken']
        assert self._resolved_endpoint(client).endswith('.g2.example')

    def test_pinned_client_has_no_withdrawn_waiter(self):
        session = self._session(self.fixture.g1)
        self._prepare(session, 'G1', self.fixture.g1)
        session.activate_model_release('G1')
        self._prepare(session, 'G2', self.fixture.g2)
        session.activate_model_release('G2')
        client = session.create_client(
            LOAD_SERVICE, 'us-east-1', model_release_id='G2'
        )
        assert client.waiter_names == []
        with pytest.raises(ValueError):
            client.get_waiter('things_ready')

    def test_unpinned_client_keeps_default_directory_priority(self):
        session = self._session(self.fixture.g2)
        client = session.create_client(LOAD_SERVICE, 'us-east-1')
        # The deployment withdraws the waiter, but the unpinned loader never
        # consults the release and finds the low priority same-named file.
        assert client.waiter_names == ['things_ready']

    def test_g3_keeps_g1_source_but_uses_its_own_waiter(self):
        session = self._session(self.fixture.g1)
        self._prepare(session, 'G1', self.fixture.g1)
        session.activate_model_release('G1')
        self._prepare(session, 'G3', self.fixture.g3)
        session.activate_model_release('G3')
        source = session.create_client(
            SOURCE_SERVICE, 'us-east-1', model_release_id='G3'
        )
        load = session.create_client(
            LOAD_SERVICE, 'us-east-1', model_release_id='G3'
        )
        assert source.get_paginator('list_things')._input_token == [
            'NextToken'
        ]
        assert self._resolved_endpoint(source).endswith('.g1.example')
        waiter = load.get_waiter('things_ready')
        assert waiter.config.max_attempts == 7

    def test_activating_g2_after_g3_raises_and_pins_stay_intact(self):
        g2_session = self._session(self.fixture.g1)
        self._prepare(g2_session, 'G1', self.fixture.g1)
        g2_session.activate_model_release('G1')
        self._prepare(g2_session, 'G2', self.fixture.g2)

        g3_session = self._session(self.fixture.g3)
        self._prepare(g3_session, 'G3', self.fixture.g3)
        g3_session.activate_model_release('G3')

        with pytest.raises(ModelReleaseBaselineChangedError):
            g2_session.activate_model_release('G2')
        # The active release is still G3, never reverted to G1.
        assert g3_session.get_active_model_release() == 'G3'

    def test_pin_survives_session_restart(self):
        first = self._session(self.fixture.g1)
        self._prepare(first, 'G1', self.fixture.g1)
        first.activate_model_release('G1')
        client = first.create_client(
            SOURCE_SERVICE, 'us-east-1', model_release_id='G1'
        )
        assert client.get_paginator('list_things')._input_token == [
            'NextToken'
        ]

        # A brand new session (new machine/process) reopens the same dir.
        restarted = self._session(self.fixture.g2)
        assert restarted.get_active_model_release() == 'G1'
        restarted_client = restarted.create_client(
            SOURCE_SERVICE, 'us-east-1', model_release_id='G1'
        )
        # The G1 snapshot is immutable even though the deployment now looks
        # like G2.
        assert restarted_client.get_paginator(
            'list_things'
        )._input_token == ['NextToken']
