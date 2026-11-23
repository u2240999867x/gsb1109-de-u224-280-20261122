'''Confluence convention for merging streams from independent Taps.

A confluence lets several Taps keep their own locally named streams (two
Taps may both call a stream ``items``) while publishing them under one
public stream name, typically via ``stream_alias``. It records, in a
manifest that can be checked before a pull:

  * which :class:`~singer.source.SourceId` owns which stream mapping,
  * which :class:`~singer.contracts.FieldContract` version decodes each
    source's records,
  * which per-source bookmark slot (the existing ``tap_stream_id``
    bookmark namespace) resumes that source.

At run time records are tagged with their source and contract version, and
a :class:`KeyConflictTracker` lets a caller find out which sources produced
a colliding public key instead of treating same-named streams as one
redelivery.
'''

import json
from collections import namedtuple

from . import state as state_module
from .contracts import ContractRegistry, FieldContract
from .exceptions import SingerError, SingerSyncError
from .logger import get_logger
from .source import SourceId

LOGGER = get_logger()

MANIFEST_VERSION = 1

CheckResult = namedtuple('CheckResult', ['ok', 'message'])


class ConfluenceError(SingerError):
    '''Base class for confluence convention errors.'''


class ManifestValidationError(ConfluenceError):
    '''Raised when the manifest and/or catalog checklist fails.'''


class KeyConflictError(SingerSyncError):
    '''Raised when two distinct sources emit the same public key.'''

    def __init__(self, public_stream, key, sources):
        self.public_stream = public_stream
        self.key = key
        self.sources = sorted(sources)
        super().__init__(
            'Public key {!r} of merged stream {!r} was emitted by more '
            'than one source: {}. Same-named streams from different '
            'sources are not redeliveries of one another.'.format(
                key, public_stream, ', '.join(self.sources)))


class StreamMapping(
        namedtuple('StreamMapping',
                   ['source', 'tap_stream_id', 'local_stream',
                    'public_stream', 'contract_version'])):
    '''One source's contribution to a merged stream.

    ``local_stream`` is the stream name the Tap itself uses (``items``).
    ``public_stream`` is the name the merged stream is published under
    (the alias handed to the ingestion flow).
    '''

    def to_dict(self):
        return {
            'source': self.source.to_dict(),
            'tap_stream_id': self.tap_stream_id,
            'local_stream': self.local_stream,
            'public_stream': self.public_stream,
            'contract_version': self.contract_version,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            source=SourceId.from_dict(data['source']),
            tap_stream_id=data['tap_stream_id'],
            local_stream=data['local_stream'],
            public_stream=data['public_stream'],
            contract_version=data['contract_version'])


class KeyConflictTracker:
    '''Records which sources have emitted which public keys.

    A key repeated by the same source is a normal redelivery and is
    ignored. A key seen from two different sources raises
    :class:`KeyConflictError`; the conflicting source names are also
    available through :meth:`sources_for`.
    '''

    def __init__(self):
        self._keys = {}

    def observe(self, public_stream, key, source, raise_on_conflict=True):
        '''Record that ``source`` emitted ``key`` on ``public_stream``.

        A key repeated by the same source is a normal redelivery. When a
        second distinct source emits the same key, :class:`KeyConflictError`
        is raised by default so the run fails fast with both sources named.
        Pass ``raise_on_conflict=False`` to keep collecting and inspect all
        collisions afterwards via :meth:`conflicts` / :meth:`sources_for`.
        '''
        source_name = source.name if isinstance(source, SourceId) else source
        bucket = self._keys.setdefault((public_stream, key), set())
        bucket.add(source_name)
        if len(bucket) > 1 and raise_on_conflict:
            raise KeyConflictError(public_stream, key, bucket)
        return sorted(bucket)

    def sources_for(self, public_stream, key):
        '''Return the sources that have emitted ``key`` (possibly empty).'''
        return sorted(self._keys.get((public_stream, key), set()))

    def conflicts(self):
        '''Return ``(public_stream, key, sources)`` tuples with >1 source.'''
        return [(stream, key, sorted(sources))
                for (stream, key), sources in self._keys.items()
                if len(sources) > 1]

    def raise_for_conflicts(self):
        '''Raise the first recorded conflict, if any.'''
        for stream, key, sources in self.conflicts():
            raise KeyConflictError(stream, key, sources)


class Confluence:
    '''The merged view of several Taps' streams.'''

    def __init__(self, name, mappings=None, registry=None,
                 manifest_path=None):
        self.name = name
        self.mappings = list(mappings or [])
        self.registry = registry or ContractRegistry()
        self.manifest_path = manifest_path

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def add_stream(self, source, entry):
        '''Register one catalog entry under its ``stream_alias``.

        The entry's selected fields become field contract version 1. If
        the mapping already exists (for example when reloading a saved
        manifest) it is returned unchanged.
        '''
        source = SourceId.from_dict(source)
        mapping = self._find_mapping(source.name, entry.tap_stream_id)
        if mapping:
            return mapping

        public_stream = entry.stream_alias or entry.stream
        version = self.registry.latest_version(source.name, public_stream) or 1
        contract = FieldContract.from_catalog_entry(source, entry, version)
        self.registry.add(contract)
        mapping = StreamMapping(
            source=source,
            tap_stream_id=entry.tap_stream_id,
            local_stream=entry.stream,
            public_stream=public_stream,
            contract_version=version)
        self.mappings.append(mapping)
        LOGGER.info(
            'Confluence %s: source %s contributes %s as %s under contract v%s',
            self.name, source.name, entry.stream, public_stream, version)
        return mapping

    def refresh_contract(self, source, entry):
        '''Compare an entry with its latest contract and version a change.

        Returns the ``(mapping, bumped)`` tuple. A field rename/removal is
        incompatible with the frozen contract, so a new version is created
        that only applies to records emitted from now on. Records resumed
        under an old bookmark keep decoding against the old version, which
        remains in the registry.
        '''
        source = SourceId.from_dict(source)
        mapping = self._find_mapping(source.name, entry.tap_stream_id)
        if not mapping:
            return self.add_stream(source, entry), True

        public_stream = entry.stream_alias or entry.stream
        previous = self.registry.latest(source.name, public_stream)
        candidate = FieldContract.from_catalog_entry(
            source, entry, previous.version + 1)
        if candidate.field_set() == previous.field_set():
            return mapping, False

        # Frozen contract versions are immutable: any field-set change
        # gets a new version. Purely additive changes stay decodable for
        # older records; removals or renames do not, and are logged louder.
        compatible = candidate.is_compatible_with(previous)
        self.registry.add(candidate)
        index = self.mappings.index(mapping)
        mapping = mapping._replace(contract_version=candidate.version)
        self.mappings[index] = mapping
        if compatible:
            LOGGER.info(
                'Confluence %s: fields added for %s/%s; new records use '
                'v%s, earlier records keep v%s',
                self.name, source.name, public_stream,
                candidate.version, previous.version)
        else:
            LOGGER.warning(
                'Confluence %s: field contract for %s/%s changed; new '
                'records use v%s, earlier records continue to use v%s',
                self.name, source.name, public_stream,
                candidate.version, previous.version)
        return mapping, True

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    def _find_mapping(self, source_name, tap_stream_id):
        for mapping in self.mappings:
            if mapping.source.name == source_name and \
                    mapping.tap_stream_id == tap_stream_id:
                return mapping
        return None

    def resolve(self, source, stream):
        '''Resolve a mapping by source plus tap_stream_id or local stream.'''
        source_name = source.name if isinstance(source, SourceId) else source
        for mapping in self.mappings:
            if mapping.source.name != source_name:
                continue
            if mapping.tap_stream_id == stream or \
                    mapping.local_stream == stream:
                return mapping
        raise ConfluenceError(
            'No confluence mapping for source {!r} and stream {!r}'.format(
                source_name, stream))

    def mappings_for(self, public_stream):
        return [m for m in self.mappings
                if m.public_stream == public_stream]

    def sources(self):
        return sorted({m.source for m in self.mappings},
                      key=lambda source: source.name)

    def contract(self, source, stream, version=None):
        mapping = self.resolve(source, stream)
        version = version or mapping.contract_version
        contract = self.registry.get(
            mapping.source.name, mapping.public_stream, version)
        if not contract:
            raise ConfluenceError(
                'Contract v{} missing for {}/{}'.format(
                    version, mapping.source.name, mapping.public_stream))
        return contract

    # ------------------------------------------------------------------
    # Pre-pull checklist
    # ------------------------------------------------------------------

    def checklist(self, catalog=None):
        '''Return a list of :class:`CheckResult` items without raising.

        When ``catalog`` (a merged :class:`~singer.catalog.Catalog`) is
        supplied, every mapping is checked against its entry, including
        drift between the catalog field selection and the frozen contract.
        '''
        results = []

        def check(ok, message):
            results.append(CheckResult(ok, message))

        if not self.mappings:
            check(False, 'Confluence has no stream mappings')
        seen = set()
        for mapping in self.mappings:
            identity = (mapping.source.name, mapping.tap_stream_id)
            check(identity not in seen,
                  'Mapping {} / {} is unique'.format(*identity))
            seen.add(identity)
            check(bool(mapping.public_stream),
                  'Mapping {} / {} has a public stream name'.format(*identity))
            contract = self.registry.get(
                mapping.source.name, mapping.public_stream,
                mapping.contract_version)
            check(contract is not None,
                  'Contract v{} registered for {} / {}'.format(
                      mapping.contract_version, mapping.source.name,
                      mapping.public_stream))

        for public_stream in {m.public_stream for m in self.mappings}:
            partners = self.mappings_for(public_stream)
            key_sets = {tuple(self._key_properties(m)) for m in partners}
            check(len(key_sets) == 1,
                  'All sources of merged stream {!r} agree on key '
                  'properties: {}'.format(
                      public_stream,
                      sorted({tuple(self._key_properties(m))
                              for m in partners})))

        if catalog is not None:
            results.extend(self._check_catalog(catalog))
        return results

    def _key_properties(self, mapping):
        contract = self.registry.get(
            mapping.source.name, mapping.public_stream,
            mapping.contract_version)
        return contract.key_properties if contract else []

    def _check_catalog(self, catalog):
        results = []
        for mapping in self.mappings:
            entry = catalog.get_stream(mapping.tap_stream_id)
            if entry is None:
                results.append(CheckResult(
                    False,
                    'Catalog is missing stream {}'.format(
                        mapping.tap_stream_id)))
                continue
            results.append(CheckResult(
                entry.is_selected(),
                'Stream {} is selected'.format(mapping.tap_stream_id)))
            current = FieldContract.from_catalog_entry(
                mapping.source, entry, mapping.contract_version)
            frozen = self.registry.get(
                mapping.source.name, mapping.public_stream,
                mapping.contract_version)
            if frozen is None:
                continue
            if current.field_set() == frozen.field_set():
                results.append(CheckResult(
                    True,
                    'Fields of {} match frozen contract v{}'.format(
                        mapping.tap_stream_id, mapping.contract_version)))
            else:
                results.append(CheckResult(
                    False,
                    'Fields of {} drifted from contract v{}: added {}, '
                    'removed {}; call refresh_contract() before syncing'.format(
                        mapping.tap_stream_id,
                        mapping.contract_version,
                        sorted(current.field_set() - frozen.field_set()),
                        sorted(frozen.field_set() - current.field_set()))))
        return results

    def validate(self, catalog=None):
        '''Run the checklist and raise on the first failure.'''
        failures = [item.message for item in self.checklist(catalog)
                    if not item.ok]
        if failures:
            raise ManifestValidationError(
                'Confluence {!r} checklist failed:\n - {}'.format(
                    self.name, '\n - '.join(failures)))

    # ------------------------------------------------------------------
    # Per-source bookmarks (standard state layout, keyed by tap_stream_id)
    # ------------------------------------------------------------------

    def get_bookmark(self, state, source, stream, key, default=None):
        mapping = self.resolve(source, stream)
        return state_module.get_bookmark(
            state, mapping.tap_stream_id, key, default)

    def set_bookmark(self, state, source, stream, key, value):
        mapping = self.resolve(source, stream)
        return state_module.set_bookmark(
            state, mapping.tap_stream_id, key, value)

    # ------------------------------------------------------------------
    # Run-time tagging and key checks
    # ------------------------------------------------------------------

    def tag_record(self, source, stream, record):
        '''Validate a record against its contract and return its mapping.'''
        mapping = self.resolve(source, stream)
        contract = self.registry.get(
            mapping.source.name, mapping.public_stream,
            mapping.contract_version)
        contract.validate_record(record)
        return mapping

    def write_record(self, source, stream, record, time_extracted=None):
        '''Validate and emit a record tagged with its source and contract.

        The record is written under the public stream name (the alias) with
        ``source`` and ``contract_version`` attached, so a Target can route
        and decode it without confusing two same-named source streams.
        '''
        from .messages import write_record
        mapping = self.tag_record(source, stream, record)
        write_record(
            mapping.local_stream,
            record,
            stream_alias=mapping.public_stream,
            time_extracted=time_extracted,
            source=mapping.source.name,
            contract_version=mapping.contract_version)
        return mapping

    def key_tracker(self):
        return KeyConflictTracker()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def to_dict(self):
        return {
            'confluence_version': MANIFEST_VERSION,
            'name': self.name,
            'mappings': [mapping.to_dict() for mapping in self.mappings],
            'contracts': self.registry.to_list(),
        }

    @classmethod
    def from_dict(cls, data):
        if data.get('confluence_version') != MANIFEST_VERSION:
            raise ManifestValidationError(
                'Unsupported confluence_version: {!r}'.format(
                    data.get('confluence_version')))
        return cls(
            name=data['name'],
            mappings=[StreamMapping.from_dict(item)
                      for item in data.get('mappings', [])],
            registry=ContractRegistry.from_list(data.get('contracts')))

    def dump(self, fp):
        json.dump(self.to_dict(), fp, indent=2)

    def save(self, path=None):
        target = path or self.manifest_path
        if not target:
            raise ConfluenceError('No manifest path configured')
        with open(target, 'w', encoding='utf-8') as fp:
            self.dump(fp)

    @classmethod
    def load(cls, path):
        with open(path, encoding='utf-8') as fp:
            confluence = cls.from_dict(json.load(fp))
        confluence.manifest_path = path
        return confluence
