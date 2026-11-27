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
"""Local, approved model releases.

A *model release* is a frozen, reviewed set of botocore data files that
lives on disk instead of being fetched from a service.  Ingestion batches
that keep running across restarts (and possibly across machines) can pin
their clients to a specific approved release so that a service model,
pagination keys, waiter configuration, endpoint rules and partition data all
stay identical for the lifetime of the batch.

A release is described by a manifest.  Each manifest records:

* the identifier of the release it is based on (``baseline``);
* one entry per managed resource, keyed by the same logical data path the
  :class:`botocore.loaders.Loader` uses (for example
  ``myservice/2020-01-01/paginators-1`` or ``partitions``), saying whether
  the resource is ``replaced`` by an approved file, ``retained`` from the
  baseline release or ``removed`` in this release.

Releases are first prepared (validated against their manifest and copied
into a self contained, machine independent snapshot) and then activated.
Only one release is active at a time.  Activating a release whose baseline
is no longer the active release fails, so a release can never silently
revert another release's data or mix data from two releases together.
"""

import hashlib
import os
import shutil

from botocore.compat import OrderedDict, json
from botocore.exceptions import (
    DataNotFoundError,
    ModelReleaseBaselineChangedError,
    ModelReleaseValidationError,
    UnknownModelReleaseError,
    UnknownServiceError,
)
from botocore.loaders import ExtrasProcessor

# The well known files inside a release directory.
MANIFEST_FILENAME = 'manifest.json'
PREPARED_MARKER = '.prepared'
ACTIVE_MARKER = 'active'

# Resource states used by a manifest.
REPLACED = 'replaced'
RETAINED = 'retained'
REMOVED = 'removed'
VALID_STATES = (REPLACED, RETAINED, REMOVED)

# Keys used in the manifest document.
BASELINE_KEY = 'baseline'
RESOURCES_KEY = 'resources'
STATE_KEY = 'state'
DIGEST_KEY = 'digest'


def resource_digest(data):
    """Return a stable digest of a loaded model resource.

    The digest is computed over the canonical JSON serialization so that the
    same logical content always produces the same digest regardless of key
    ordering or whitespace in the original file.
    """
    payload = json.dumps(
        data, sort_keys=True, separators=(',', ':')
    ).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


def _read_json(path):
    for candidate in (path, path + '.json'):
        if os.path.isfile(candidate):
            with open(candidate, 'rb') as fp:
                payload = fp.read().decode('utf-8')
            return json.loads(payload, object_pairs_hook=OrderedDict)
    return None


def _write_json(path, data):
    directory = os.path.dirname(path)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory)
    with open(path, 'w', encoding='utf-8') as fp:
        fp.write(json.dumps(data, indent=2))


class ModelReleaseManifest:
    """The validated description of a single model release."""

    def __init__(self, release_id, baseline, resources):
        self.release_id = release_id
        self.baseline = baseline
        # Mapping of logical data path -> {"state": ..., "digest": ...}
        self.resources = resources

    @classmethod
    def from_document(cls, release_id, document):
        if not isinstance(document, dict):
            raise ModelReleaseValidationError(
                release_id=release_id,
                mismatched_resources=[],
                msg=f"Release '{release_id}' manifest is not a JSON object.",
            )
        baseline = document.get(BASELINE_KEY)
        raw_resources = document.get(RESOURCES_KEY)
        if not isinstance(raw_resources, dict) or not raw_resources:
            raise ModelReleaseValidationError(
                release_id=release_id,
                mismatched_resources=[],
                msg=(
                    f"Release '{release_id}' manifest is missing its "
                    f"'{RESOURCES_KEY}' section."
                ),
            )
        resources = OrderedDict()
        for resource, entry in raw_resources.items():
            if not isinstance(entry, dict):
                raise ModelReleaseValidationError(
                    release_id=release_id,
                    mismatched_resources=[resource],
                    msg=(
                        f"Release '{release_id}' resource '{resource}' must "
                        'be an object.'
                    ),
                )
            state = entry.get(STATE_KEY)
            if state not in VALID_STATES:
                raise ModelReleaseValidationError(
                    release_id=release_id,
                    mismatched_resources=[resource],
                    msg=(
                        f"Release '{release_id}' resource '{resource}' has an "
                        f"unknown state '{state}'."
                    ),
                )
            digest = entry.get(DIGEST_KEY)
            if state == REPLACED and not digest:
                raise ModelReleaseValidationError(
                    release_id=release_id,
                    mismatched_resources=[resource],
                    msg=(
                        f"Release '{release_id}' resource '{resource}' is "
                        'replaced but has no approved content digest.'
                    ),
                )
            if state == RETAINED and not baseline:
                raise ModelReleaseValidationError(
                    release_id=release_id,
                    mismatched_resources=[resource],
                    msg=(
                        f"Release '{release_id}' resource '{resource}' is "
                        'retained but the release declares no baseline.'
                    ),
                )
            resources[resource] = OrderedDict(
                [(STATE_KEY, state), (DIGEST_KEY, digest)]
            )
        return cls(release_id, baseline, resources)

    def to_document(self):
        return OrderedDict(
            [
                (BASELINE_KEY, self.baseline),
                (
                    RESOURCES_KEY,
                    OrderedDict(
                        (
                            resource,
                            OrderedDict(
                                [
                                    (STATE_KEY, entry[STATE_KEY]),
                                    (DIGEST_KEY, entry[DIGEST_KEY]),
                                ]
                            ),
                        )
                        for resource, entry in self.resources.items()
                    ),
                ),
            ]
        )

    def entry(self, resource):
        return self.resources.get(resource)

    def state(self, resource):
        entry = self.entry(resource)
        return entry[STATE_KEY] if entry else None


class ModelReleaseCatalog:
    """Prepares, validates and activates the releases in a release directory.

    A release directory is laid out as::

        <release_dir>/
          manifests/
            G1.json
            G2.json
          G1/
            <staged resource files mirroring their logical data paths>
            manifest.json
          G2/
            ...
          active

    The same release directory is reused operationally and may be reopened on
    another machine, so the only state this class relies on is the staged
    snapshots and marker files it writes itself.
    """

    def __init__(self, release_dir):
        self.release_dir = release_dir
        self.manifests_dir = os.path.join(release_dir, 'manifests')

    # -- discovery ------------------------------------------------------

    def list_releases(self):
        """Return the identifiers of every manifest in the directory."""
        if not os.path.isdir(self.manifests_dir):
            return []
        releases = [
            name[: -len('.json')]
            for name in os.listdir(self.manifests_dir)
            if name.endswith('.json')
        ]
        return sorted(releases)

    def get_manifest(self, release_id):
        document = _read_json(
            os.path.join(self.manifests_dir, release_id)
        )
        if document is None:
            raise UnknownModelReleaseError(
                release_id=release_id,
                known_releases=', '.join(self.list_releases()) or '(none)',
            )
        return ModelReleaseManifest.from_document(release_id, document)

    # -- preparation ----------------------------------------------------

    def prepare(self, release_id, source_loader):
        """Validate a release and stage a self contained snapshot of it.

        Every replaced file is read through ``source_loader`` (which follows
        the normal search path priority) and checked against the approved
        digest in the manifest.  Retained resources are copied out of the
        baseline's already staged snapshot and removed resources are recorded
        as absent.  Because the snapshot only contains files that matched the
        manifest, it is independent of the mutable deployment directory and
        can be reopened on another machine.

        :raises ModelReleaseValidationError: if any staged resource does not
            match the manifest.  The exception lists every mismatched
            resource and the release is left unprepared.
        """
        manifest = self.get_manifest(release_id)
        stage_dir = self._stage_dir(release_id)
        if os.path.isdir(stage_dir):
            shutil.rmtree(stage_dir)
        os.makedirs(stage_dir)

        mismatched = []
        for resource, entry in manifest.resources.items():
            state = entry[STATE_KEY]
            if state == REPLACED:
                data = self._read_source(source_loader, resource)
                if (
                    data is None
                    or resource_digest(data) != entry[DIGEST_KEY]
                ):
                    mismatched.append(resource)
                    continue
                _write_json(os.path.join(stage_dir, resource), data)
            elif state == RETAINED:
                data = self._read_retained(manifest, resource)
                if data is None:
                    mismatched.append(resource)
                    continue
                _write_json(os.path.join(stage_dir, resource), data)
            # REMOVED resources are intentionally not staged.

        _write_json(
            os.path.join(stage_dir, MANIFEST_FILENAME),
            manifest.to_document(),
        )
        if mismatched:
            raise ModelReleaseValidationError(
                release_id=release_id,
                mismatched_resources=mismatched,
            )
        # Only a fully validated release gets the prepared marker.
        with open(os.path.join(stage_dir, PREPARED_MARKER), 'w') as fp:
            fp.write('')
        return manifest

    def is_prepared(self, release_id):
        return os.path.isfile(
            os.path.join(self._stage_dir(release_id), PREPARED_MARKER)
        )

    def _read_source(self, source_loader, resource):
        try:
            data, _ = source_loader.load_data_with_path(resource)
        except DataNotFoundError:
            return None
        return data

    def _read_retained(self, manifest, resource):
        if not manifest.baseline:
            return None
        baseline_stage = self._stage_dir(manifest.baseline)
        return _read_json(os.path.join(baseline_stage, resource))

    # -- activation -----------------------------------------------------

    @property
    def active_release(self):
        marker = os.path.join(self.release_dir, ACTIVE_MARKER)
        if not os.path.isfile(marker):
            return None
        with open(marker, encoding='utf-8') as fp:
            release_id = fp.read().strip()
        return release_id or None

    def activate(self, release_id):
        """Make a prepared release the single active release.

        A release may only be activated if it is fully prepared and either no
        release is active yet or its baseline is the currently active release.
        This guarantees that activating a release never reverts another
        release's data and that data from two releases is never mixed.
        """
        if not self.is_prepared(release_id):
            raise UnknownModelReleaseError(
                release_id=release_id,
                known_releases=', '.join(self.list_releases()) or '(none)',
            )
        manifest = self.get_staged_manifest(release_id)
        active_id = self.active_release
        if active_id is not None and manifest.baseline != active_id:
            raise ModelReleaseBaselineChangedError(
                release_id=release_id,
                baseline_id=manifest.baseline,
                active_id=active_id,
            )
        with open(
            os.path.join(self.release_dir, ACTIVE_MARKER),
            'w',
            encoding='utf-8',
        ) as fp:
            fp.write(release_id)
        return release_id

    def get_staged_manifest(self, release_id):
        document = _read_json(
            os.path.join(self._stage_dir(release_id), MANIFEST_FILENAME)
        )
        if document is None:
            raise UnknownModelReleaseError(
                release_id=release_id,
                known_releases=', '.join(self.list_releases()) or '(none)',
            )
        return ModelReleaseManifest.from_document(release_id, document)

    def create_loader(self, release_id, base_loader):
        """Build a loader pinned to a prepared, validated release."""
        if not self.is_prepared(release_id):
            raise UnknownModelReleaseError(
                release_id=release_id,
                known_releases=', '.join(self.list_releases()) or '(none)',
            )
        manifest = self.get_staged_manifest(release_id)
        return PinnedModelLoader(
            stage_dir=self._stage_dir(release_id),
            manifest=manifest,
            base_loader=base_loader,
        )

    def _stage_dir(self, release_id):
        return os.path.join(self.release_dir, release_id)


class PinnedModelLoader:
    """A loader view that serves only one validated release's resources.

    Managed resources are resolved from the immutable staged snapshot
    according to the release manifest: replaced and retained files are served
    from the snapshot while removed resources behave as if they do not exist,
    even when a lower priority search path happens to contain a same named
    file.  Every other data path is delegated to the wrapped loader so that
    global botocore data such as retry configuration keeps loading normally.

    Instances are cheap to create, so a batch can open one per client on a
    restart or a different machine simply by pointing at the same staged
    release.
    """

    def __init__(self, stage_dir, manifest, base_loader):
        self.stage_dir = stage_dir
        self.manifest = manifest
        self._base_loader = base_loader
        self._extras_processor = ExtrasProcessor()
        self._cache = {}
        self._service_model_cache = {}

    # -- passthrough surface used by the rest of botocore --------------

    @property
    def search_paths(self):
        return self._base_loader.search_paths

    @property
    def extras_types(self):
        return self._base_loader.extras_types

    def is_builtin_path(self, path):
        return self._base_loader.is_builtin_path(path)

    # -- release aware loading -----------------------------------------

    def load_data_with_path(self, name):
        state = self.manifest.state(name)
        if state is not None:
            return self._load_managed(name), os.path.join(
                self.stage_dir, name
            )
        return self._base_loader.load_data_with_path(name)

    def load_data(self, name):
        data, _ = self.load_data_with_path(name)
        return data

    def load_service_model(self, service_name, type_name, api_version=None):
        matching = self._manifest_entries(service_name, type_name)
        if not matching:
            # The release makes no decision for this service/type, so the
            # wrapped loader's normal search path applies unchanged.
            return self._base_loader.load_service_model(
                service_name, type_name, api_version
            )
        if api_version is None:
            api_version = self._resolve_api_version(
                service_name, type_name, matching
            )
        resource = f'{service_name}/{api_version}/{type_name}'
        if self.manifest.state(resource) == REMOVED:
            raise UnknownServiceError(
                service_name=service_name,
                known_service_names=', '.join(
                    self.list_available_services(type_name)
                ),
            )
        cache_key = (service_name, type_name, api_version)
        if cache_key in self._service_model_cache:
            return self._service_model_cache[cache_key]
        model = self._load_managed(resource)
        self._apply_extras(model, service_name, type_name, api_version)
        self._service_model_cache[cache_key] = model
        return model

    def _manifest_entries(self, service_name, type_name):
        prefix = f'{service_name}/'
        suffix = '/' + type_name
        return {
            resource: entry
            for resource, entry in self.manifest.resources.items()
            if resource.startswith(prefix) and resource.endswith(suffix)
        }

    def _resolve_api_version(self, service_name, type_name, matching):
        available = [
            resource.split('/')[1]
            for resource, entry in matching.items()
            if entry[STATE_KEY] != REMOVED
        ]
        if available:
            return max(available)
        # Every staged version is withdrawn, so the tool is unavailable in
        # this release regardless of what lower priority paths still contain.
        raise UnknownServiceError(
            service_name=service_name,
            known_service_names=', '.join(
                self.list_available_services(type_name)
            ),
        )

    def _apply_extras(self, model, service_name, type_name, api_version):
        extras_resource = (
            f'{service_name}/{api_version}/{type_name}.sdk-extras'
        )
        extras_state = self.manifest.state(extras_resource)
        if extras_state in (REPLACED, RETAINED):
            # The staged extras document keeps the native {"merge": ...}
            # envelope, so it is applied through the same processor the
            # default loader uses.
            self._extras_processor.process(
                model, [self._load_managed(extras_resource)]
            )
        elif extras_state is None:
            # The release makes no decision about extras, so keep the default
            # search path behavior of the wrapped loader.
            self._extras_processor.process(
                model,
                self._base_loader._find_extras(
                    service_name, type_name, api_version
                ),
            )
            # REMOVED: the release explicitly withdraws the extras file.

    def list_available_services(self, type_name):
        try:
            services = set(
                self._base_loader.list_available_services(type_name)
            )
        except DataNotFoundError:
            services = set()
        for resource, entry in self.manifest.resources.items():
            if not resource.endswith('/' + type_name):
                continue
            service_name = resource.split('/')[0]
            if entry[STATE_KEY] == REMOVED:
                services.discard(service_name)
            else:
                services.add(service_name)
        return sorted(services)

    def list_api_versions(self, service_name, type_name):
        try:
            versions = set(
                self._base_loader.list_api_versions(service_name, type_name)
            )
        except DataNotFoundError:
            versions = set()
        for resource, entry in self.manifest.resources.items():
            prefix = f'{service_name}/'
            if not resource.startswith(prefix):
                continue
            if not resource.endswith('/' + type_name):
                continue
            api_version = resource[len(prefix):].split('/')[0]
            if entry[STATE_KEY] == REMOVED:
                versions.discard(api_version)
            else:
                versions.add(api_version)
        if not versions:
            raise DataNotFoundError(
                data_path=f'{service_name}/{type_name}'
            )
        return sorted(versions)

    def determine_latest_version(self, service_name, type_name):
        return max(self.list_api_versions(service_name, type_name))

    # -- helpers --------------------------------------------------------

    def _load_managed(self, resource):
        state = self.manifest.state(resource)
        if state in (None, REMOVED):
            raise DataNotFoundError(data_path=resource)
        if resource not in self._cache:
            data = _read_json(os.path.join(self.stage_dir, resource))
            if data is None:
                raise DataNotFoundError(data_path=resource)
            self._cache[resource] = data
        return self._cache[resource]
