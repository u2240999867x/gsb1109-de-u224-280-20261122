import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout

from singer import (
    Catalog,
    CatalogEntry,
    Confluence,
    FieldContract,
    KeyConflictError,
    KeyConflictTracker,
    ManifestValidationError,
    RecordMessage,
    SourceId,
    StreamMapping,
    parse_message,
)
from singer import metadata as metadata_module
from singer.metadata import get_standard_metadata
from singer.schema import Schema

TAP_A = SourceId('tap_a', 'Tap A')
TAP_B = SourceId('tap_b', 'Tap B')
PUBLIC_STREAM = 'warehouse_items'


def make_entry(tap_stream_id, stream, alias, properties, key_properties,
               deselected=()):
    schema = Schema(
        type='object',
        properties={name: Schema(type=spec) for name, spec in properties.items()})
    mdata = metadata_module.to_map(
        get_standard_metadata(schema=schema.to_dict(),
                              schema_name=stream,
                              key_properties=key_properties))
    metadata_module.write(mdata, (), 'selected', True)
    for field in deselected:
        metadata_module.write(
            mdata, ('properties', field), 'selected', False)
    return CatalogEntry(
        tap_stream_id=tap_stream_id,
        stream=stream,
        stream_alias=alias,
        schema=schema,
        key_properties=key_properties,
        metadata=metadata_module.to_list(mdata))


def tap_a_entry(deselected=()):
    return make_entry(
        'tap_a_items', 'items', PUBLIC_STREAM,
        {'id': 'string', 'sku': 'string', 'amount': 'number'},
        ['id'], deselected=deselected)


def tap_b_entry(deselected=()):
    return make_entry(
        'tap_b_items', 'items', PUBLIC_STREAM,
        {'id': 'string', 'sku': 'string', 'quantity': 'integer'},
        ['id'], deselected=deselected)


class TestSourceId(unittest.TestCase):
    def test_rejects_empty_name(self):
        with self.assertRaises(ValueError):
            SourceId('')

    def test_rejects_dotted_name(self):
        with self.assertRaises(ValueError):
            SourceId('a.b')

    def test_equality_and_dict_roundtrip(self):
        self.assertEqual(SourceId('tap_a'), SourceId('tap_a', 'Tap A'))
        self.assertEqual(
            SourceId.from_dict(SourceId('tap_a').to_dict()),
            SourceId('tap_a'))
        self.assertEqual(SourceId.from_dict('tap_b'), SourceId('tap_b'))


class TestConfluenceBuild(unittest.TestCase):
    def setUp(self):
        self.confluence = Confluence('warehouse')
        self.entry_a = tap_a_entry()
        self.entry_b = tap_b_entry()
        self.mapping_a = self.confluence.add_stream(TAP_A, self.entry_a)
        self.mapping_b = self.confluence.add_stream(TAP_B, self.entry_b)

    def test_same_local_name_merges_under_one_public_stream(self):
        self.assertEqual(self.mapping_a.local_stream, 'items')
        self.assertEqual(self.mapping_b.local_stream, 'items')
        self.assertEqual(self.mapping_a.public_stream, PUBLIC_STREAM)
        self.assertEqual(self.mapping_b.public_stream, PUBLIC_STREAM)
        self.assertNotEqual(
            self.mapping_a.tap_stream_id, self.mapping_b.tap_stream_id)
        self.assertEqual(
            len(self.confluence.mappings_for(PUBLIC_STREAM)), 2)

    def test_contracts_keep_each_source_selected_fields(self):
        contract_a = self.confluence.contract(TAP_A, 'items')
        contract_b = self.confluence.contract(TAP_B, 'items')
        self.assertEqual(contract_a.fields, ['amount', 'id', 'sku'])
        self.assertEqual(contract_b.fields, ['id', 'quantity', 'sku'])
        self.assertIn('amount', contract_a.field_set())
        self.assertNotIn('amount', contract_b.field_set())
        self.assertIn('quantity', contract_b.field_set())
        self.assertNotIn('quantity', contract_a.field_set())
        self.assertEqual(contract_a.version, 1)
        self.assertEqual(contract_b.version, 1)

    def test_resolve_accepts_tap_stream_id_or_local_name(self):
        self.assertEqual(
            self.confluence.resolve(TAP_A, 'items'),
            self.confluence.resolve(TAP_A, 'tap_a_items'))

    def test_adding_same_mapping_is_idempotent(self):
        again = self.confluence.add_stream(TAP_A, self.entry_a)
        self.assertEqual(again, self.mapping_a)
        self.assertEqual(
            len(self.confluence.mappings_for(PUBLIC_STREAM)), 2)

    def test_checklist_passes_against_merged_catalog(self):
        catalog = Catalog([self.entry_a, self.entry_b])
        failures = [item.message for item in
                    self.confluence.checklist(catalog) if not item.ok]
        self.assertEqual(failures, [])
        self.confluence.validate(catalog)

    def test_checklist_flags_unselected_and_drifted_stream(self):
        drifted_a = tap_a_entry(deselected=('amount',))
        catalog = Catalog([drifted_a, self.entry_b])
        messages = [item.message for item in
                    self.confluence.checklist(catalog) if not item.ok]
        self.assertTrue(any('tap_a_items' in msg for msg in messages))
        self.assertTrue(any('drifted' in msg for msg in messages))
        with self.assertRaises(ManifestValidationError):
            self.confluence.validate(catalog)

    def test_checklist_flags_disagreeing_key_properties(self):
        other = Confluence('other')
        source_x = SourceId('x')
        source_y = SourceId('y')
        other.registry.add(FieldContract(
            source_x, PUBLIC_STREAM, 1, ['id'], key_properties=['id']))
        other.registry.add(FieldContract(
            source_y, PUBLIC_STREAM, 1, ['id'], key_properties=['sku']))
        other.mappings.append(StreamMapping(
            source_x, 'x_items', 'items', PUBLIC_STREAM, 1))
        other.mappings.append(StreamMapping(
            source_y, 'y_items', 'items', PUBLIC_STREAM, 1))
        failures = [item.message for item in other.checklist() if not item.ok]
        self.assertTrue(any('key properties' in msg for msg in failures))


class TestManifestPersistence(unittest.TestCase):
    def test_dict_roundtrip(self):
        confluence = Confluence('warehouse')
        confluence.add_stream(TAP_A, tap_a_entry())
        confluence.add_stream(TAP_B, tap_b_entry())
        restored = Confluence.from_dict(confluence.to_dict())
        self.assertEqual(restored.mappings, confluence.mappings)
        self.assertEqual(restored.registry, confluence.registry)

    def test_save_and_load_file(self):
        confluence = Confluence('warehouse')
        confluence.add_stream(TAP_A, tap_a_entry())
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'confluence.json')
            confluence.save(path)
            restored = Confluence.load(path)
        self.assertEqual(restored.name, 'warehouse')
        self.assertEqual(
            restored.contract(TAP_A, 'items').fields,
            ['amount', 'id', 'sku'])

    def test_unknown_manifest_version_rejected(self):
        with self.assertRaises(ManifestValidationError):
            Confluence.from_dict({'confluence_version': 99, 'name': 'x'})


class TestPerSourceBookmarks(unittest.TestCase):
    def setUp(self):
        self.confluence = Confluence('warehouse')
        self.confluence.add_stream(TAP_A, tap_a_entry())
        self.confluence.add_stream(TAP_B, tap_b_entry())
        self.state = {}

    def test_bookmarks_are_namespaced_per_source(self):
        self.confluence.set_bookmark(
            self.state, TAP_A, 'items', 'updated_at', '2026-11-01')
        self.confluence.set_bookmark(
            self.state, TAP_B, 'items', 'updated_at', '2026-11-20')
        self.assertEqual(
            self.confluence.get_bookmark(
                self.state, TAP_A, 'items', 'updated_at'),
            '2026-11-01')
        self.assertEqual(
            self.confluence.get_bookmark(
                self.state, TAP_B, 'items', 'updated_at'),
            '2026-11-20')
        self.assertEqual(
            self.state['bookmarks']['tap_a_items']['updated_at'],
            '2026-11-01')
        self.assertEqual(
            self.state['bookmarks']['tap_b_items']['updated_at'],
            '2026-11-20')

    def test_b_resumes_its_own_bookmark_after_a_finished(self):
        self.confluence.set_bookmark(
            self.state, TAP_A, 'items', 'updated_at', '2026-11-01')
        self.confluence.set_bookmark(
            self.state, TAP_B, 'items', 'updated_at', '2026-10-01')
        resumed = self.confluence.get_bookmark(
            self.state, TAP_B, 'items', 'updated_at')
        self.assertEqual(resumed, '2026-10-01')
        record_b = {'id': 'b-1', 'sku': 'SKU1', 'quantity': 7}
        mapping = self.confluence.tag_record(TAP_B, 'items', record_b)
        self.assertEqual(mapping.contract_version, 1)


class TestContractEvolution(unittest.TestCase):
    def setUp(self):
        self.confluence = Confluence('warehouse')
        self.confluence.add_stream(TAP_A, tap_a_entry())
        self.confluence.add_stream(TAP_B, tap_b_entry())

    def test_renamed_optional_field_bumps_only_the_renaming_source(self):
        renamed_a = make_entry(
            'tap_a_items', 'items', PUBLIC_STREAM,
            {'id': 'string', 'sku': 'string', 'amount_value': 'number'},
            ['id'])
        mapping_a, bumped = self.confluence.refresh_contract(
            TAP_A, renamed_a)
        self.assertTrue(bumped)
        self.assertEqual(mapping_a.contract_version, 2)
        self.assertEqual(
            self.confluence.contract(TAP_B, 'items').version, 1)

        v1 = self.confluence.contract(TAP_A, 'items', version=1)
        v2 = self.confluence.contract(TAP_A, 'items', version=2)
        self.assertEqual(v1.fields, ['amount', 'id', 'sku'])
        self.assertEqual(v2.fields, ['amount_value', 'id', 'sku'])

    def test_old_records_still_decode_under_frozen_contract(self):
        renamed_a = make_entry(
            'tap_a_items', 'items', PUBLIC_STREAM,
            {'id': 'string', 'sku': 'string', 'amount_value': 'number'},
            ['id'])
        self.confluence.refresh_contract(TAP_A, renamed_a)

        old_contract = self.confluence.contract(TAP_A, 'items', version=1)
        old_contract.validate_record(
            {'id': 'a-old', 'sku': 'SKU1', 'amount': 12.5})
        with self.assertRaises(ValueError):
            old_contract.validate_record(
                {'id': 'a-new', 'sku': 'SKU1', 'amount_value': 12.5})

        new_contract = self.confluence.contract(TAP_A, 'items', version=2)
        new_contract.validate_record(
            {'id': 'a-new', 'sku': 'SKU1', 'amount_value': 12.5})
        with self.assertRaises(ValueError):
            new_contract.validate_record(
                {'id': 'a-old', 'sku': 'SKU1', 'amount': 12.5})

    def test_b_contract_is_unaffected_by_a_rename(self):
        renamed_a = make_entry(
            'tap_a_items', 'items', PUBLIC_STREAM,
            {'id': 'string', 'sku': 'string', 'amount_value': 'number'},
            ['id'])
        self.confluence.refresh_contract(TAP_A, renamed_a)
        contract_b = self.confluence.contract(TAP_B, 'items')
        contract_b.validate_record(
            {'id': 'b-1', 'sku': 'SKU2', 'quantity': 3})
        self.assertEqual(contract_b.version, 1)

    def test_unchanged_fields_do_not_bump_version(self):
        mapping, bumped = self.confluence.refresh_contract(
            TAP_A, tap_a_entry())
        self.assertFalse(bumped)
        self.assertEqual(mapping.contract_version, 1)

    def test_record_with_unknown_field_rejected(self):
        with self.assertRaises(ValueError):
            self.confluence.tag_record(
                TAP_B, 'items',
                {'id': 'b-1', 'sku': 'SKU2', 'quantity': 3, 'bogus': 1})


class TestKeyConflicts(unittest.TestCase):
    def test_same_key_from_two_sources_raises_and_names_both(self):
        tracker = KeyConflictTracker()
        tracker.observe(PUBLIC_STREAM, 'dup-1', TAP_A)
        with self.assertRaises(KeyConflictError) as ctx:
            tracker.observe(PUBLIC_STREAM, 'dup-1', TAP_B)
        self.assertEqual(ctx.exception.sources, ['tap_a', 'tap_b'])
        self.assertEqual(ctx.exception.key, 'dup-1')
        self.assertEqual(
            tracker.sources_for(PUBLIC_STREAM, 'dup-1'),
            ['tap_a', 'tap_b'])

    def test_repeat_from_same_source_is_not_a_conflict(self):
        tracker = KeyConflictTracker()
        tracker.observe(PUBLIC_STREAM, 'k-1', TAP_A)
        tracker.observe(PUBLIC_STREAM, 'k-1', TAP_A)
        self.assertEqual(
            tracker.sources_for(PUBLIC_STREAM, 'k-1'), ['tap_a'])
        self.assertEqual(tracker.conflicts(), [])

    def test_conflicts_listing(self):
        tracker = KeyConflictTracker()
        tracker.observe(PUBLIC_STREAM, 'k-1', TAP_A, raise_on_conflict=False)
        tracker.observe(PUBLIC_STREAM, 'k-2', TAP_A, raise_on_conflict=False)
        tracker.observe(PUBLIC_STREAM, 'k-1', TAP_B, raise_on_conflict=False)
        self.assertEqual(
            tracker.conflicts(),
            [(PUBLIC_STREAM, 'k-1', ['tap_a', 'tap_b'])])
        with self.assertRaises(KeyConflictError):
            tracker.raise_for_conflicts()


class TestRecordTagging(unittest.TestCase):
    def setUp(self):
        self.confluence = Confluence('warehouse')
        self.confluence.add_stream(TAP_A, tap_a_entry())
        self.confluence.add_stream(TAP_B, tap_b_entry())

    def test_write_record_is_tagged_and_round_trips(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.confluence.write_record(
                TAP_B, 'items',
                {'id': 'b-1', 'sku': 'SKU2', 'quantity': 4})
        message = parse_message(buf.getvalue().strip())
        self.assertEqual(message.stream, PUBLIC_STREAM)
        self.assertEqual(message.source, 'tap_b')
        self.assertEqual(message.contract_version, 1)
        self.assertEqual(message.record['quantity'], 4)

    def test_single_tap_record_messages_are_untagged(self):
        message = RecordMessage(stream='items', record={'id': 'x'})
        encoded = message.asdict()
        self.assertNotIn('source', encoded)
        self.assertNotIn('contract_version', encoded)
        parsed = parse_message(json.dumps(encoded))
        self.assertIsNone(parsed.source)
        self.assertIsNone(parsed.contract_version)

    def test_tagged_record_round_trips_through_json(self):
        message = RecordMessage(
            stream=PUBLIC_STREAM,
            record={'id': 'a-1'},
            source='tap_a',
            contract_version=2)
        parsed = parse_message(json.dumps(message.asdict()))
        self.assertEqual(parsed.source, 'tap_a')
        self.assertEqual(parsed.contract_version, 2)


class TestStagedRolloutScenario(unittest.TestCase):
    '''Round 1 ships tap A only; round 2 resumes B and renames an A field.'''

    def test_full_scenario_across_persisted_manifest_and_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = os.path.join(tmp, 'confluence.json')
            state = {}

            # Round 1: only tap A is onboarded.
            round_one = Confluence('warehouse')
            entry_a = tap_a_entry()
            round_one.add_stream(TAP_A, entry_a)
            round_one.validate(Catalog([entry_a]))
            round_one.set_bookmark(
                state, TAP_A, 'items', 'updated_at', '2026-11-01')
            round_one.save(manifest_path)

            # Round 2: reload the manifest, onboard tap B under its own
            # bookmark slot, and rename an optional field of tap A.
            round_two = Confluence.load(manifest_path)
            entry_b = tap_b_entry()
            round_two.add_stream(TAP_B, entry_b)
            round_two.validate(Catalog([entry_a, entry_b]))
            round_two.set_bookmark(
                state, TAP_B, 'items', 'updated_at', '2026-10-01')

            renamed_a = make_entry(
                'tap_a_items', 'items', PUBLIC_STREAM,
                {'id': 'string', 'sku': 'string',
                 'amount_value': 'number'},
                ['id'])
            mapping_a, bumped = round_two.refresh_contract(
                TAP_A, renamed_a)
            self.assertTrue(bumped)
            self.assertEqual(mapping_a.contract_version, 2)
            round_two.save(manifest_path)

            # B resumes from its own original bookmark and is still
            # decoded by the contract version it started with.
            self.assertEqual(
                round_two.get_bookmark(
                    state, TAP_B, 'items', 'updated_at'),
                '2026-10-01')
            mapping_b = round_two.tag_record(
                TAP_B, 'items',
                {'id': 'b-1', 'sku': 'SKU9', 'quantity': 5})
            self.assertEqual(mapping_b.contract_version, 1)

            # A's rename only applies to records A emits after the bump;
            # the v1 contract remains available to decode earlier data.
            round_two.tag_record(
                TAP_A, 'items',
                {'id': 'a-9', 'sku': 'SKU8', 'amount_value': 9.5})
            with self.assertRaises(ValueError):
                round_two.tag_record(
                    TAP_A, 'items',
                    {'id': 'a-old', 'sku': 'SKU8', 'amount': 9.5})
            restored = Confluence.load(manifest_path)
            self.assertEqual(
                restored.contract(TAP_A, 'items', version=1).fields,
                ['amount', 'id', 'sku'])
            self.assertEqual(
                restored.contract(TAP_A, 'items', version=2).fields,
                ['amount_value', 'id', 'sku'])
            self.assertEqual(
                restored.contract(TAP_B, 'items').fields,
                ['id', 'quantity', 'sku'])


if __name__ == '__main__':
    unittest.main()
