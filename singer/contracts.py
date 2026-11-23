'''Field contracts that pin how one Source decodes one merged stream.

When several Taps merge streams under one public stream name (often via
``stream_alias``), each Tap still owns its own field layout. A
:class:`FieldContract` snapshots that layout together with the field
selection in effect for the source.

Contracts are versioned and frozen into the confluence manifest when a
source first joins. On a later run a source may rename an optional field;
that produces a new contract version that only applies to records emitted
afterwards, while records resumed under an old bookmark are still decoded
with the contract version that was current when they were extracted.
'''

from . import metadata as metadata_module
from .logger import get_logger
from .schema import Schema

LOGGER = get_logger()


def _schema_to_dict(schema):
    if isinstance(schema, Schema):
        return schema.to_dict()
    return schema


def _schema_from_dict(data):
    if data is None:
        return None
    return Schema.from_dict(data)


def _selected_field_set(schema, metadata):
    '''Return the property names this source is allowed to emit.

    Fields are read from the schema properties and narrowed by the catalog
    metadata selection, mirroring ``should_sync_field`` semantics.
    '''
    properties = (schema.properties or {}) if isinstance(schema, Schema) \
        else (schema or {}).get('properties', {})
    selected = set()
    if not metadata:
        return set(properties.keys())

    mdata = metadata_module.to_map(metadata)
    for field_name in properties:
        inclusion = metadata_module.get(
            mdata, ('properties', field_name), 'inclusion')
        is_selected = metadata_module.get(
            mdata, ('properties', field_name), 'selected')
        if inclusion == 'automatic' or is_selected is True:
            selected.add(field_name)
        elif is_selected is None and inclusion != 'unsupported':
            selected.add(field_name)
    return selected


class FieldContract:
    '''A versioned field layout for one ``(source, stream)`` pair.

    ``version`` is assigned by the :class:`~singer.confluence.Confluence`
    and is 1-based within a pair. ``fields`` is the ordered list of field
    names the source emits under this version. ``schema`` is the JSON schema
    (as a dict or :class:`~singer.schema.Schema`) describing those fields.
    '''

    def __init__(self, source, stream, version, fields, schema=None,
                 key_properties=None, description=None):
        from .source import SourceId
        self.source = SourceId.from_dict(source)
        self.stream = stream
        self.version = int(version)
        self.fields = list(fields)
        self.schema = _schema_to_dict(schema) if schema is not None else None
        self.key_properties = list(key_properties) if key_properties else []
        self.description = description

    @property
    def pair_key(self):
        return (self.source.name, self.stream)

    def field_set(self):
        return set(self.fields)

    def is_compatible_with(self, previous):
        '''True if records written under ``previous`` still decode.

        A new contract is compatible when every field the previous contract
        promised is still present. Renaming or removing an optional field is
        therefore a breaking change and must take a new version; key fields
        may never be dropped.
        '''
        if previous is None:
            return True
        removed = previous.field_set() - self.field_set()
        if not removed:
            return True
        dropped_keys = removed & set(previous.key_properties)
        if dropped_keys:
            LOGGER.warning(
                'Contract v%s for %s/%s drops key properties %s',
                self.version, self.source, self.stream, sorted(dropped_keys))
        return False

    def validate_record(self, record):
        '''Raise ``ValueError`` if a record carries fields outside the contract.'''
        if not isinstance(record, dict):
            raise ValueError(
                'Record for {}/{} must be an object, got {}'.format(
                    self.source, self.stream, type(record).__name__))
        unknown = set(record.keys()) - self.field_set()
        if unknown:
            raise ValueError(
                'Record from {} for stream {} contains fields not in '
                'contract v{}: {} (allowed: {})'.format(
                    self.source, self.stream, self.version,
                    sorted(unknown), self.fields))
        missing_keys = [key for key in self.key_properties
                        if key not in record]
        if missing_keys:
            raise ValueError(
                'Record from {} for stream {} is missing key properties '
                'required by contract v{}: {}'.format(
                    self.source, self.stream, self.version, missing_keys))

    def to_dict(self):
        result = {
            'source': self.source.to_dict(),
            'stream': self.stream,
            'version': self.version,
            'fields': self.fields,
        }
        if self.schema is not None:
            result['schema'] = self.schema
        if self.key_properties:
            result['key_properties'] = self.key_properties
        if self.description:
            result['description'] = self.description
        return result

    @classmethod
    def from_dict(cls, data):
        return cls(
            source=data['source'],
            stream=data['stream'],
            version=data['version'],
            fields=data['fields'],
            schema=_schema_from_dict(data.get('schema')),
            key_properties=data.get('key_properties'),
            description=data.get('description'))

    @classmethod
    def from_catalog_entry(cls, source, entry, version, metadata=None):
        '''Build a contract from a :class:`~singer.catalog.CatalogEntry`.

        Only fields selected for this source (via the entry metadata) are
        retained, so tap A keeping its amount field and tap B keeping its
        quantity field produce different contracts even though both streams
        are called ``items``.
        '''
        schema = entry.schema
        fields = sorted(_selected_field_set(schema, metadata or entry.metadata))
        return cls(
            source=source,
            stream=entry.stream_alias or entry.stream,
            version=version,
            fields=fields,
            schema=schema,
            key_properties=entry.key_properties)

    def __eq__(self, other):
        return isinstance(other, FieldContract) and \
            self.to_dict() == other.to_dict()

    def __repr__(self):
        return 'FieldContract(source={!r}, stream={!r}, version={}, fields={!r})'.format(
            self.source, self.stream, self.version, self.fields)


class ContractRegistry:
    '''Holds every known contract version, keyed by ``(source, stream)``.'''

    def __init__(self, contracts=None):
        self._contracts = {}
        for contract in contracts or []:
            self.add(contract)

    def add(self, contract):
        bucket = self._contracts.setdefault(contract.pair_key, [])
        for existing in bucket:
            if existing.version == contract.version:
                if existing != contract:
                    raise ValueError(
                        'Conflicting contract v{} for {}/{}'.format(
                            contract.version, contract.source,
                            contract.stream))
                return existing
        bucket.append(contract)
        bucket.sort(key=lambda item: item.version)
        return contract

    def latest_version(self, source_name, stream):
        bucket = self._contracts.get((source_name, stream))
        if not bucket:
            return None
        return bucket[-1].version

    def get(self, source_name, stream, version):
        for contract in self._contracts.get((source_name, stream), []):
            if contract.version == version:
                return contract
        return None

    def latest(self, source_name, stream):
        bucket = self._contracts.get((source_name, stream))
        return bucket[-1] if bucket else None

    def pairs(self):
        return list(self._contracts.keys())

    def to_list(self):
        result = []
        for bucket in self._contracts.values():
            result.extend(contract.to_dict() for contract in bucket)
        return result

    @classmethod
    def from_list(cls, data):
        return cls(FieldContract.from_dict(item) for item in (data or []))

    def __eq__(self, other):
        return isinstance(other, ContractRegistry) and \
            self.to_list() == other.to_list()
