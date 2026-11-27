"""Tests for the experimental manifest module."""

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import BaseModel, Field, HttpUrl, TypeAdapter, ValidationError, field_validator

from pydantic.experimental.manifest import (
    ManifestBase,
    ManifestConfig,
    ManifestContext,
    resolve_reference,
)

ARCHIVE = 'https://archive.example/v1/day/index?rev=9'
EAST = 'https://east.example/v2/day/index?rev=7'
WEST = 'https://west.example/v3/day/index?rev=8'
NORTH_BASE_REFERENCE = '../%2e/north/list?tag=a&tag=b'
AUDIT_BASE = 'https://archive.example/v1/%2e/north/list?tag=a&tag=b'
DOWNLOAD_REFERENCES = ['', '?', './lot.csv']


def _north_urls(origin: str) -> list[str]:
    return [
        f'{origin}/%2e/north/list?tag=a&tag=b',
        f'{origin}/%2e/north/list?',
        f'{origin}/%2e/north/lot.csv',
    ]


class SubManifest(ManifestBase):
    name: str
    downloads: list[str]
    base_url: str | None = None
    manifest = ManifestConfig(base_field='base_url', url_fields=('downloads',))


class NorthManifest(SubManifest):
    manifest = ManifestConfig(base_field='base_url', url_fields=('downloads',), relative=True)


class AuditManifest(SubManifest):
    manifest = ManifestConfig(base_field='base_url', url_fields=('downloads',), relative=False)


def _station_factory(*, station: str | None) -> str:
    return {'west': WEST, 'east': EAST}[station]  # type: ignore[index]


class DayManifest(ManifestBase):
    # The sub-manifest list is declared before the base column.
    submanifests: list[SubManifest]
    entry: str | None = Field(default=None, validation_alias='entry')
    manifest = ManifestConfig(
        base_field='entry',
        children_field='submanifests',
        default_factory=_station_factory,
    )


def _payload() -> dict[str, Any]:
    return {
        'submanifests': [
            {'name': 'north', 'base_url': NORTH_BASE_REFERENCE, 'downloads': list(DOWNLOAD_REFERENCES)},
            {'name': 'audit', 'base_url': AUDIT_BASE, 'downloads': list(DOWNLOAD_REFERENCES)},
        ]
    }


# --------------------------------------------------------------------------
# Reference resolution text preservation
# --------------------------------------------------------------------------


def test_resolve_reference_preserves_wire_text() -> None:
    base = 'https://west.example/v3/day/index?rev=8'
    north_base = resolve_reference(base, NORTH_BASE_REFERENCE)
    assert north_base == 'https://west.example/v3/%2e/north/list?tag=a&tag=b'
    assert [resolve_reference(north_base, ref) for ref in DOWNLOAD_REFERENCES] == _north_urls(
        'https://west.example/v3'
    )


def test_resolve_reference_keeps_encoded_dots_and_query_order() -> None:
    base = 'https://h.test/a/b/c?one=1&one=2'
    # Percent-encoded dots are never treated as directory segments.
    assert resolve_reference(base, '%2e%2e/x') == 'https://h.test/a/b/%2e%2e/x'
    assert resolve_reference(base, './%2e/lot') == 'https://h.test/a/b/%2e/lot'
    # Literal dots remain directory segments.
    assert resolve_reference(base, '../x') == 'https://h.test/a/x'
    # Empty reference keeps the base verbatim, including its repeated query.
    assert resolve_reference(base, '') == base
    # An empty query marker is preserved as actual wire text.
    assert resolve_reference(base, '?') == 'https://h.test/a/b/c?'
    assert resolve_reference(base, './lot?') == 'https://h.test/a/b/lot?'


def test_resolve_reference_requires_absolute_base() -> None:
    with pytest.raises(ValueError):
        resolve_reference('not-a-url', './x')


# --------------------------------------------------------------------------
# Validation against the archive site
# --------------------------------------------------------------------------


def test_validate_submanifests_at_archive() -> None:
    context = ManifestContext(entry=ARCHIVE, station='archive')
    north = NorthManifest.model_validate(
        {'name': 'north', 'base_url': NORTH_BASE_REFERENCE, 'downloads': list(DOWNLOAD_REFERENCES)},
        context=context,
    )
    audit = AuditManifest.model_validate(
        {'name': 'audit', 'base_url': AUDIT_BASE, 'downloads': list(DOWNLOAD_REFERENCES)},
        context=context,
    )
    archive_origin = 'https://archive.example/v1'
    assert north.manifest_base == f'{archive_origin}/%2e/north/list?tag=a&tag=b'
    assert north.request_urls() == _north_urls(archive_origin)
    assert audit.manifest_base == AUDIT_BASE
    assert audit.request_urls() == _north_urls(archive_origin)


def test_relative_base_requires_parent() -> None:
    with pytest.raises(
        ValidationError,
        check=lambda err: err.errors()[0]['type'] == 'manifest_relative_base_without_parent',
    ):
        NorthManifest.model_validate(
            {'name': 'north', 'base_url': NORTH_BASE_REFERENCE, 'downloads': []}
        )


# --------------------------------------------------------------------------
# Reusing validated Python sub-manifests across stations
# --------------------------------------------------------------------------


@pytest.fixture()
def validated_submanifests() -> tuple[NorthManifest, AuditManifest]:
    context = ManifestContext(entry=ARCHIVE, station='archive')
    north = NorthManifest.model_validate(
        {'name': 'north', 'base_url': NORTH_BASE_REFERENCE, 'downloads': list(DOWNLOAD_REFERENCES)},
        context=context,
    )
    audit = AuditManifest.model_validate(
        {'name': 'audit', 'base_url': AUDIT_BASE, 'downloads': list(DOWNLOAD_REFERENCES)},
        context=context,
    )
    return north, audit


def test_reuse_python_submanifests_at_west(
    validated_submanifests: tuple[NorthManifest, AuditManifest],
) -> None:
    north, audit = validated_submanifests
    west = DayManifest.model_validate(
        {'submanifests': [north, audit]},
        context=ManifestContext(station='west'),
    )
    west_north, west_audit = west.submanifests
    assert west.manifest_base == WEST
    assert west_north.manifest_base == 'https://west.example/v3/%2e/north/list?tag=a&tag=b'
    assert west_north.request_urls() == _north_urls('https://west.example/v3')
    # The absolute audit base keeps pointing at the original archive site.
    assert west_audit.manifest_base == AUDIT_BASE
    assert west_audit.request_urls() == _north_urls('https://archive.example/v1')


def test_reuse_python_submanifests_at_east(
    validated_submanifests: tuple[NorthManifest, AuditManifest],
) -> None:
    north, audit = validated_submanifests
    east = DayManifest.model_validate(
        {'submanifests': [north, audit], 'entry': EAST},
        context=ManifestContext(station='east'),
    )
    east_north, east_audit = east.submanifests
    assert east.manifest_base == EAST
    assert east_north.manifest_base == 'https://east.example/v2/%2e/north/list?tag=a&tag=b'
    assert east_north.request_urls() == _north_urls('https://east.example/v2')
    assert east_audit.manifest_base == AUDIT_BASE
    assert east_audit.request_urls() == _north_urls('https://archive.example/v1')


def test_reuse_does_not_mutate_original_manifest(
    validated_submanifests: tuple[NorthManifest, AuditManifest],
) -> None:
    north, audit = validated_submanifests
    original_north_urls = north.request_urls()
    DayManifest.model_validate(
        {'submanifests': [north, audit]}, context=ManifestContext(station='west')
    )
    DayManifest.model_validate(
        {'submanifests': [north, audit], 'entry': EAST}, context=ManifestContext(station='east')
    )
    # The already validated archive manifests stay usable at the original site.
    assert north.request_urls() == original_north_urls
    assert north.manifest_base == 'https://archive.example/v1/%2e/north/list?tag=a&tag=b'
    assert audit.manifest_base == AUDIT_BASE


def test_reuse_preserves_defaults_fields_set_and_validation(
    validated_submanifests: tuple[NorthManifest, AuditManifest],
) -> None:
    class Annotated(SubManifest):
        flavor: str = 'vanilla'

        @field_validator('name')
        @classmethod
        def _upper(cls, value: str) -> str:
            return value.upper()

    context = ManifestContext(entry=ARCHIVE, station='archive')
    original = Annotated.model_validate(
        {'name': 'north', 'base_url': NORTH_BASE_REFERENCE, 'downloads': ['']},
        context=context,
    )
    assert original.model_fields_set == {'name', 'base_url', 'downloads'}
    west = DayManifest.model_validate(
        {'submanifests': [original]}, context=ManifestContext(station='west')
    )
    child = west.submanifests[0]
    assert child is not original
    assert isinstance(child, Annotated)
    assert child.model_fields_set == {'name', 'base_url', 'downloads'}
    assert child.flavor == 'vanilla'
    assert child.name == 'NORTH'


def test_same_instance_can_be_reused_more_than_once(
    validated_submanifests: tuple[NorthManifest, AuditManifest],
) -> None:
    north, audit = validated_submanifests
    west = DayManifest.model_validate(
        {'submanifests': [north, north]}, context=ManifestContext(station='west')
    )
    first, second = west.submanifests
    assert first is not second
    assert first is not north
    assert first.request_urls() == second.request_urls() == _north_urls('https://west.example/v3')


# --------------------------------------------------------------------------
# Equivalent raw JSON input
# --------------------------------------------------------------------------


def test_json_input_matches_python_input() -> None:
    west = DayManifest.model_validate_json(
        json.dumps(_payload()), context=ManifestContext(station='west')
    )
    north, audit = west.submanifests
    assert north.request_urls() == _north_urls('https://west.example/v3')
    assert audit.request_urls() == _north_urls('https://archive.example/v1')


def test_type_adapter_python_and_json() -> None:
    adapter = TypeAdapter(DayManifest)
    from_python = adapter.validate_python(_payload(), context=ManifestContext(station='west'))
    from_json = adapter.validate_json(
        json.dumps(_payload()), context=ManifestContext(station='west')
    )
    assert from_python.submanifests[0].request_urls() == from_json.submanifests[0].request_urls()


# --------------------------------------------------------------------------
# Inbound alias vs validated default factory precedence
# --------------------------------------------------------------------------


def test_entry_alias_outranks_default_factory() -> None:
    east = DayManifest.model_validate(
        {'submanifests': [], 'entry': EAST}, context=ManifestContext(station='east')
    )
    assert east.manifest_base == EAST


def test_default_factory_used_without_entry() -> None:
    west = DayManifest.model_validate(
        {'submanifests': []}, context=ManifestContext(station='west')
    )
    assert west.manifest_base == WEST


def test_factory_without_station_parameter() -> None:
    class Root(ManifestBase):
        entry: str | None = None
        manifest = ManifestConfig(base_field='entry', default_factory=lambda: 'https://f.test/idx')

    assert Root.model_validate({}).manifest_base == 'https://f.test/idx'


def test_missing_base_and_parent_raises() -> None:
    class Root(ManifestBase):
        value: str = 'x'
        entry: str | None = None
        manifest = ManifestConfig(base_field='entry')

    with pytest.raises(
        ValidationError,
        check=lambda err: err.errors()[0]['type'] == 'manifest_base_missing',
    ):
        Root.model_validate({})


# --------------------------------------------------------------------------
# Base inheritance for sub-manifests without their own base
# --------------------------------------------------------------------------


def test_submanifest_without_base_inherits_parent() -> None:
    class Child(SubManifest):
        manifest = ManifestConfig(base_field='base_url', url_fields=('downloads',))

    class Parent(ManifestBase):
        children: list[Child]
        entry: str | None = Field(default=None, validation_alias='entry')
        manifest = ManifestConfig(base_field='entry', children_field='children')

    parent = Parent.model_validate(
        {'entry': ARCHIVE, 'children': [{'name': 'a', 'downloads': ['./lot.csv']}]},
        context=ManifestContext(station='archive'),
    )
    child = parent.children[0]
    assert child.manifest_base == ARCHIVE
    assert child.manifest_base_owned is False
    assert child.request_urls() == ['https://archive.example/v1/day/lot.csv']


# --------------------------------------------------------------------------
# Download address verification against final request URLs
# --------------------------------------------------------------------------


class RestrictedNorth(NorthManifest):
    def manifest_verify_url(self, url: str, *, field_name: str, index: int | None) -> None:
        if self.manifest_station == 'west' and 'archive.example' in url:
            raise ValueError(f'west station cannot request the archive host: {url}')


def test_verify_hook_receives_final_request_url_and_loc() -> None:
    context = ManifestContext(entry=ARCHIVE, station='archive')
    audit = RestrictedNorth.model_validate(
        {'name': 'audit', 'base_url': AUDIT_BASE, 'downloads': list(DOWNLOAD_REFERENCES)},
        context=context,
    )
    # The archive site itself is allowed and stays usable.
    assert audit.request_urls()[0].startswith('https://archive.example')

    with pytest.raises(
        ValidationError,
        check=lambda err: [error['loc'] for error in err.errors()]
        == [
            ('submanifests', 0, 'downloads', 0),
            ('submanifests', 0, 'downloads', 1),
            ('submanifests', 0, 'downloads', 2),
        ]
        and all(error['type'] == 'manifest_url_denied' for error in err.errors()),
    ):
        DayManifest.model_validate(
            {'submanifests': [audit]}, context=ManifestContext(station='west')
        )
    # The validated original manifest remains usable after the rejection.
    assert audit.request_urls()[0].startswith('https://archive.example')


def test_field_validator_sees_final_resolved_url() -> None:
    observed: list[str] = []

    class Observing(SubManifest):
        manifest = ManifestConfig(base_field='base_url', url_fields=('downloads',), relative=True)

        @field_validator('downloads')
        @classmethod
        def _observe(cls, value: list[str]) -> list[str]:
            observed.extend(value)
            return value

    Observing.model_validate(
        {'name': 'n', 'base_url': NORTH_BASE_REFERENCE, 'downloads': ['', '?']},
        context=ManifestContext(entry=ARCHIVE, station='archive'),
    )
    assert observed == [
        'https://archive.example/v1/%2e/north/list?tag=a&tag=b',
        'https://archive.example/v1/%2e/north/list?',
    ]


# --------------------------------------------------------------------------
# Plain HttpUrl fields keep their original behaviour
# --------------------------------------------------------------------------


def test_inbound_aliases_are_resolved() -> None:
    class Aliased(ManifestBase):
        downloads: list[str] = Field(validation_alias='dl')
        base_url: str | None = Field(default=None, validation_alias='bu')
        manifest = ManifestConfig(
            base_field='base_url', url_fields=('downloads',), relative=True
        )

    class Root(ManifestBase):
        kids: list[Aliased]
        entry: str | None = Field(default=None, validation_alias='entry')
        manifest = ManifestConfig(base_field='entry', children_field='kids')

    root = Root.model_validate(
        {
            'entry': ARCHIVE,
            'kids': [
                {
                    'bu': NORTH_BASE_REFERENCE,
                    'dl': list(DOWNLOAD_REFERENCES),
                }
            ],
        },
        context=ManifestContext(station='archive'),
    )
    assert root.kids[0].request_urls() == _north_urls('https://archive.example/v1')


def test_plain_http_url_still_works() -> None:
    class Plain(BaseModel):
        website: HttpUrl

    assert str(Plain(website='https://example.com/path').website) == 'https://example.com/path'
    with pytest.raises(ValidationError):
        Plain(website='not-a-url')


# --------------------------------------------------------------------------
# Serialization keeps resolved request URLs and hides manifest internals
# --------------------------------------------------------------------------


def test_serialization_exposes_resolved_urls_without_private_state() -> None:
    west = DayManifest.model_validate_json(
        json.dumps(_payload()), context=ManifestContext(station='west')
    )
    dumped = west.model_dump()
    north = dumped['submanifests'][0]
    assert north['downloads'] == _north_urls('https://west.example/v3')
    assert all(not key.startswith('_manifest') for key in dumped)
    assert all(not key.startswith('_manifest') for key in north)
    json.loads(west.model_dump_json())


def test_deep_tree_verify_error_location_stays_absolute() -> None:
    class Leaf(ManifestBase):
        refs: list[str] = []
        base_url: str | None = None
        manifest = ManifestConfig(base_field='base_url', url_fields=('refs',))

        def manifest_verify_url(self, url: str, *, field_name: str, index: int | None) -> None:
            if self.manifest_station == 'west' and 'bad' in url:
                raise ValueError('blocked')

    class Mid(ManifestBase):
        leaves: list[Leaf]
        base_url: str | None = None
        manifest = ManifestConfig(
            base_field='base_url', children_field='leaves', relative=True
        )

    class Root(ManifestBase):
        mids: list[Mid]
        entry: str | None = Field(default=None, validation_alias='entry')
        manifest = ManifestConfig(base_field='entry', children_field='mids')

    tree = Root.model_validate(
        {
            'entry': ARCHIVE,
            'mids': [
                {
                    'base_url': '../%2e/n/list',
                    'leaves': [{'refs': ['./bad.csv']}, {'refs': ['./ok.csv']}],
                }
            ],
        },
        context=ManifestContext(station='archive'),
    )
    with pytest.raises(
        ValidationError,
        check=lambda err: err.errors()[0]['loc'] == ('mids', 0, 'leaves', 0, 'refs', 0),
    ):
        Root.model_validate(tree, context=ManifestContext(entry=ARCHIVE, station='west'))
