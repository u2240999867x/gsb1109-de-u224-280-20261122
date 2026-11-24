#!/usr/bin/env python
#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
"""Tests for the opt-in transactional handoff editing layer."""
import unittest
from decimal import Decimal

from xmlschema import XMLSchema11, DataElement, HandoffEditor, \
    HandoffValidationError, HandoffChannelConflict
from xmlschema.names import XSI_TYPE
from xmlschema.testing import run_xmlschema_tests

NS = 'http://xmlschema.test/shipment'

XSD = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:t="http://xmlschema.test/shipment"
           targetNamespace="http://xmlschema.test/shipment"
           elementFormDefault="qualified">

  <xs:complexType name="manifest-base">
    <xs:sequence>
      <xs:element name="id" type="xs:string"/>
    </xs:sequence>
    <xs:attribute name="channel" type="xs:string" inheritable="true"/>
  </xs:complexType>

  <xs:complexType name="manifest-land">
    <xs:complexContent>
      <xs:extension base="t:manifest-base">
        <xs:sequence>
          <xs:element name="fee" type="xs:string"/>
        </xs:sequence>
      </xs:extension>
    </xs:complexContent>
  </xs:complexType>

  <xs:complexType name="manifest-sea">
    <xs:complexContent>
      <xs:extension base="t:manifest-base">
        <xs:sequence>
          <xs:element name="fee" type="xs:decimal"/>
        </xs:sequence>
      </xs:extension>
    </xs:complexContent>
  </xs:complexType>

  <xs:element name="manifest" type="t:manifest-base">
    <xs:alternative test="@channel='land'" type="t:manifest-land"/>
    <xs:alternative test="@channel='sea'" type="t:manifest-sea"/>
  </xs:element>

  <xs:element name="group">
    <xs:complexType>
      <xs:choice maxOccurs="unbounded">
        <xs:element ref="t:group" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="t:manifest" minOccurs="0" maxOccurs="unbounded"/>
      </xs:choice>
      <xs:attribute name="channel" type="xs:string" inheritable="true"/>
      <xs:attribute name="name" type="xs:string"/>
    </xs:complexType>
  </xs:element>

  <xs:element name="batch">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="t:group" maxOccurs="unbounded"/>
      </xs:sequence>
      <xs:attribute name="channel" type="xs:string" inheritable="true"/>
    </xs:complexType>
  </xs:element>

</xs:schema>
"""

XML = """<t:batch xmlns:t="http://xmlschema.test/shipment" channel="land"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <t:group name="source">
    <t:group name="mid" channel="land">
      <t:manifest xsi:type="t:manifest-land">
        <t:id>A</t:id><t:fee>12.50</t:fee>
      </t:manifest>
      <t:group name="override" channel="land">
        <t:manifest>
          <t:id>D</t:id><t:fee>9.00</t:fee>
        </t:manifest>
      </t:group>
    </t:group>
    <t:group name="leftover">
      <t:manifest>
        <t:id>A</t:id><t:fee>7.00</t:fee>
      </t:manifest>
    </t:group>
  </t:group>
  <t:group name="target" channel="sea">
    <t:group name="receiver" channel="sea"/>
    <t:manifest>
      <t:id>B</t:id><t:fee>2.5</t:fee>
    </t:manifest>
  </t:group>
</t:batch>
"""


def group_named(node, name):
    for elem in node.iter():
        if elem.local_name == 'group' and elem.attrib.get('name') == name:
            return elem
    raise KeyError(name)


def manifests(node):
    return [e for e in node.iter() if e.local_name == 'manifest']


def manifest_with_id(node, manifest_id):
    for elem in manifests(node):
        if any(c.local_name == 'id' and c.value == manifest_id for c in elem):
            return elem
    raise KeyError(manifest_id)


def fee_of(manifest):
    return next(c for c in manifest if c.local_name == 'fee')


class TestHandoffEditing(unittest.TestCase):

    def setUp(self):
        self.schema = XMLSchema11(XSD)
        self.root = self.schema.to_objects(XML)
        self.assertTrue(self.root.is_valid())

    # -- basic decoded bindings ------------------------------------------- #
    def test_initial_decode_bindings(self):
        a = manifest_with_id(self.root, 'A')
        b = manifest_with_id(self.root, 'B')
        d = manifest_with_id(self.root, 'D')

        self.assertTrue(a.xsd_type.name.endswith('manifest-land'))
        self.assertTrue(b.xsd_type.name.endswith('manifest-sea'))
        self.assertTrue(d.xsd_type.name.endswith('manifest-land'))
        self.assertEqual(fee_of(a).value, '12.50')
        self.assertIsInstance(fee_of(a).value, str)
        self.assertEqual(fee_of(b).value, Decimal('2.5'))
        self.assertEqual(fee_of(d).value, '9.00')

    def test_dataelement_binding_guard_unchanged(self):
        # The handoff layer must not weaken the bind-once DataElement guard.
        with self.assertRaises(ValueError):
            self.root.xsd_type = self.root[0].xsd_type

    # -- draft isolation -------------------------------------------------- #
    def test_draft_does_not_touch_confirmed_tree_before_commit(self):
        editor = HandoffEditor(self.root)
        draft = editor.begin_draft()
        mid = group_named(self.root, 'mid')
        w_mid = draft.working_node(mid)
        del w_mid.attrib['channel']
        # The confirmed node still carries its local channel.
        self.assertEqual(mid.attrib.get('channel'), 'land')
        self.assertNotIn('channel', w_mid.attrib)
        draft.discard()
        self.assertFalse(editor.has_draft())
        self.assertEqual(mid.attrib.get('channel'), 'land')

    def test_only_one_open_draft(self):
        editor = HandoffEditor(self.root)
        editor.begin_draft()
        with self.assertRaises(ValueError):
            editor.begin_draft()

    # -- the full handoff scenario --------------------------------------- #
    def _stage_handoff(self, editor, keep_a_xsi_type, new_a_fee='13.25'):
        root = editor.root
        mid = group_named(root, 'mid')
        target = group_named(root, 'target')
        source = group_named(root, 'source')
        a = manifest_with_id(root, 'A')
        b = manifest_with_id(root, 'B')

        draft = editor.begin_draft()
        w_mid = draft.working_node(mid)
        w_target = draft.working_node(target)
        w_source = draft.working_node(source)
        w_a = draft.working_node(a)
        w_b = draft.working_node(b)

        del w_mid.attrib['channel']
        w_source.remove(w_mid)
        w_target.append(w_mid)
        w_target.remove(w_b)
        w_source.append(w_b)
        fee_of(w_a).value = new_a_fee
        if not keep_a_xsi_type:
            del w_a.attrib['xsi:type']
        return draft, a, b

    def test_failed_commit_with_old_xsi_type_reports_manifest_and_location(self):
        editor = HandoffEditor(self.root)
        draft, a, b = self._stage_handoff(editor, keep_a_xsi_type=True)

        with self.assertRaises(HandoffValidationError) as caught:
            draft.commit()
        self.assertEqual(caught.exception.manifest_id, 'A')
        self.assertIsNotNone(caught.exception.location)
        self.assertIn('xsi:type', str(caught.exception))

        # Confirmed tree keeps its order, attributes, values and bindings.
        source = group_named(self.root, 'source')
        target = group_named(self.root, 'target')
        mid = group_named(self.root, 'mid')
        self.assertIn(mid, list(source))
        self.assertIn(b, list(target))
        self.assertEqual(mid.attrib.get('channel'), 'land')
        self.assertEqual(fee_of(a).value, '12.50')
        self.assertEqual(fee_of(b).value, Decimal('2.5'))
        self.assertTrue(a.xsd_type.name.endswith('manifest-land'))
        self.assertTrue(b.xsd_type.name.endswith('manifest-sea'))
        self.assertIn('xsi:type', a.attrib)
        # No candidate XML: the confirmed tree still encodes the old content.
        self.assertTrue(self.schema.is_valid(self.root.tostring()))
        # The failed draft stays open and can be discarded.
        self.assertTrue(editor.has_draft())
        draft.discard()

    def test_successful_commit_rebinds_in_place_and_preserves_identities(self):
        editor = HandoffEditor(self.root)
        draft, a, b = self._stage_handoff(editor, keep_a_xsi_type=False)
        a_fee, b_fee = fee_of(a), fee_of(b)
        draft.commit()

        # Same object references now expose the new declarations/types/values.
        self.assertEqual(a_fee.value, Decimal('13.25'))
        self.assertIsInstance(a_fee.value, Decimal)
        self.assertEqual(b_fee.value, '2.5')
        self.assertIsInstance(b_fee.value, str)
        self.assertTrue(a.xsd_type.name.endswith('manifest-sea'))
        self.assertTrue(b.xsd_type.name.endswith('manifest-land'))
        self.assertIsNotNone(a.xsd_element)
        self.assertEqual(a.xsd_element.local_name, 'manifest')
        self.assertTrue(a_fee.xsd_type.name.endswith('decimal'))
        self.assertTrue(b_fee.xsd_type.name.endswith('string'))
        self.assertNotIn('xsi:type', a.attrib)

        # Inner override group stays land, source leftover manifest untouched.
        d = manifest_with_id(self.root, 'D')
        self.assertTrue(d.xsd_type.name.endswith('manifest-land'))
        self.assertEqual(fee_of(d).value, '9.00')
        leftover = manifest_with_id(group_named(self.root, 'leftover'), 'A')
        self.assertIsNot(leftover, a)
        self.assertTrue(leftover.xsd_type.name.endswith('manifest-land'))
        self.assertEqual(fee_of(leftover).value, '7.00')

        # mid no longer has a local channel and none was materialized on
        # descendants that lacked one.
        mid = group_named(self.root, 'mid')
        self.assertNotIn('channel', mid.attrib)
        self.assertTrue(self.root.is_valid())

    def test_roundtrip_and_detail_level_encoding_use_new_bindings(self):
        editor = HandoffEditor(self.root)
        draft, a, _ = self._stage_handoff(editor, keep_a_xsi_type=False)
        draft.commit()

        encoded = self.root.tostring()
        self.assertTrue(self.schema.is_valid(encoded))
        redecoded = self.schema.to_objects(encoded)
        selection = {
            next(c for c in m if c.local_name == 'id').value:
                m.xsd_type.name.split('}')[-1]
            for m in manifests(redecoded)
        }
        self.assertEqual(selection['A'], 'manifest-sea')
        self.assertEqual(selection['B'], 'manifest-land')
        self.assertEqual(selection['D'], 'manifest-land')

        # A detail object validates/encodes through its current location.
        detail = fee_of(a)
        self.assertTrue(detail.is_valid())
        encoded_fee = detail.encode()
        self.assertTrue(encoded_fee.tag.endswith('fee'))
        self.assertEqual(encoded_fee.text, '13.25')

    def test_discarded_reverse_handoff_keeps_committed_version(self):
        editor = HandoffEditor(self.root)
        draft, _, _ = self._stage_handoff(editor, keep_a_xsi_type=False)
        draft.commit()

        b = manifest_with_id(self.root, 'B')
        b_fee = fee_of(b)
        reverse = editor.begin_draft()
        w_b = reverse.working_node(b)
        reverse.working_node(group_named(self.root, 'source')).remove(w_b)
        reverse.working_node(group_named(self.root, 'target')).append(w_b)
        reverse.discard()

        self.assertEqual(b_fee.value, '2.5')
        self.assertIsInstance(b_fee.value, str)
        self.assertTrue(b.xsd_type.name.endswith('manifest-land'))

    # -- concurrent drafts ------------------------------------------------ #
    def _prepare_move_a_only(self):
        root = self.root
        target = group_named(root, 'target')
        mid = group_named(root, 'mid')
        source = group_named(root, 'source')
        a = manifest_with_id(root, 'A')
        editor = HandoffEditor(root)
        draft = editor.begin_draft()
        w_mid = draft.working_node(mid)
        w_target = draft.working_node(target)
        w_source = draft.working_node(source)
        w_a = draft.working_node(a)
        del w_mid.attrib['channel']
        w_source.remove(w_mid)
        w_target.append(w_mid)
        del w_a.attrib['xsi:type']
        return editor, draft, a

    def test_concurrent_fee_correction_is_preserved(self):
        editor, draft_a, a = self._prepare_move_a_only()
        b = manifest_with_id(self.root, 'B')

        other = HandoffEditor(self.root)
        draft_b = other.begin_draft()
        fee_of(draft_b.working_node(b)).value = '2.50'
        draft_b.commit()
        self.assertEqual(fee_of(b).value, '2.50')
        self.assertIsInstance(fee_of(b).value, str)

        draft_a.commit()
        self.assertEqual(fee_of(b).value, '2.50')
        self.assertEqual(fee_of(a).value, Decimal('12.50'))
        self.assertTrue(a.xsd_type.name.endswith('manifest-sea'))
        self.assertTrue(self.root.is_valid())

    def test_channel_removal_conflict_blocks_silent_land_handoff(self):
        editor, draft_a, a = self._prepare_move_a_only()
        target = group_named(self.root, 'target')

        other = HandoffEditor(self.root)
        draft_b = other.begin_draft()
        del draft_b.working_node(target).attrib['channel']
        draft_b.commit()
        self.assertNotIn('channel', target.attrib)

        with self.assertRaises(HandoffChannelConflict) as caught:
            draft_a.commit()
        self.assertEqual(caught.exception.manifest_id, 'A')
        self.assertEqual(caught.exception.prepared_channel, 'sea')
        self.assertEqual(caught.exception.current_channel, 'land')

        # Even though the content would pass land validation, nothing changed.
        self.assertTrue(a.xsd_type.name.endswith('manifest-land'))
        self.assertEqual(fee_of(a).value, '12.50')
        draft_a.discard()

    def test_conflict_can_be_accepted_in_the_same_draft(self):
        editor, draft_a, a = self._prepare_move_a_only()
        target = group_named(self.root, 'target')

        other = HandoffEditor(self.root)
        draft_b = other.begin_draft()
        del draft_b.working_node(target).attrib['channel']
        draft_b.commit()

        with self.assertRaises(HandoffChannelConflict):
            draft_a.commit()
        draft_a.commit(accept_current_channel=True)
        self.assertTrue(a.xsd_type.name.endswith('manifest-land'))
        self.assertEqual(fee_of(a).value, '12.50')
        self.assertIsInstance(fee_of(a).value, str)
        self.assertNotIn('channel', target.attrib)
        self.assertTrue(self.root.is_valid())

    def test_inner_explicit_sea_receiver_avoids_conflict(self):
        root = self.root
        target = group_named(root, 'target')
        receiver = group_named(root, 'receiver')
        mid = group_named(root, 'mid')
        source = group_named(root, 'source')
        a = manifest_with_id(root, 'A')

        editor = HandoffEditor(root)
        draft = editor.begin_draft()
        w_mid = draft.working_node(mid)
        w_source = draft.working_node(source)
        w_receiver = draft.working_node(receiver)
        w_a = draft.working_node(a)
        del w_mid.attrib['channel']
        w_source.remove(w_mid)
        w_mid.remove(w_a)
        w_receiver.append(w_a)
        del w_a.attrib['xsi:type']

        other = HandoffEditor(root)
        draft_b = other.begin_draft()
        del draft_b.working_node(target).attrib['channel']
        draft_b.commit()

        draft.commit()  # receiver keeps explicit channel="sea"
        self.assertTrue(a.xsd_type.name.endswith('manifest-sea'))
        self.assertEqual(fee_of(a).value, Decimal('12.50'))
        self.assertTrue(root.is_valid())

    def test_invalid_final_composition_fails_with_collected_errors(self):
        # Even with the xsi:type removed and the correct sea target type, a
        # non-decimal fee makes the final composition invalid.
        editor = HandoffEditor(self.root)
        draft, a, b = self._stage_handoff(
            editor, keep_a_xsi_type=False, new_a_fee='not-a-decimal')
        a_fee, b_fee = fee_of(a), fee_of(b)

        with self.assertRaises(HandoffValidationError) as caught:
            draft.commit()
        self.assertEqual(caught.exception.manifest_id, 'A')
        self.assertTrue(caught.exception.errors)

        # Confirmed tree untouched, no candidate XML is returned.
        self.assertEqual(a_fee.value, '12.50')
        self.assertIsInstance(a_fee.value, str)
        self.assertEqual(b_fee.value, Decimal('2.5'))
        self.assertTrue(a.xsd_type.name.endswith('manifest-land'))
        self.assertTrue(self.schema.is_valid(self.root.tostring()))
        draft.discard()

    def test_transient_intermediate_invalidity_does_not_abort(self):
        # The editor never validates while edits are staged; only the final
        # composition is judged at commit time.
        editor = HandoffEditor(self.root)
        draft = editor.begin_draft()
        target = draft.working_node(group_named(self.root, 'target'))
        b = manifest_with_id(self.root, 'B')
        w_b = draft.working_node(b)
        # Temporarily place B under a group, then move it back before commit.
        target.remove(w_b)
        draft.working.append(w_b)
        draft.working.remove(w_b)
        target.append(w_b)
        draft.discard()
        self.assertEqual(fee_of(b).value, Decimal('2.5'))

    def test_value_only_edit_keeps_python_type(self):
        d = manifest_with_id(self.root, 'D')
        editor = HandoffEditor(self.root)
        draft = editor.begin_draft()
        w_d = draft.working_node(d)
        fee_of(w_d).value = '10.75'
        draft.commit()
        # Land fee stays a string even though the text is numeric.
        self.assertEqual(fee_of(d).value, '10.75')
        self.assertIsInstance(fee_of(d).value, str)
        self.assertTrue(d.xsd_type.name.endswith('manifest-land'))
        self.assertTrue(self.root.is_valid())

    def test_attribute_edit_does_not_add_inherited_channel(self):
        target = group_named(self.root, 'target')
        # The target group has a local channel; the source group does not.
        source = group_named(self.root, 'source')
        self.assertNotIn('channel', source.attrib)
        editor = HandoffEditor(self.root)
        draft = editor.begin_draft()
        draft.working_node(target).set('name', 'renamed-target')
        draft.commit()
        self.assertEqual(target.attrib.get('name'), 'renamed-target')
        self.assertNotIn('channel', source.attrib)

    def test_inherited_channel_is_read_but_not_written(self):
        # A node lacking a local channel follows its branch but never gets a
        # materialized value after an unrelated commit.
        source = group_named(self.root, 'source')
        self.assertNotIn('channel', source.attrib)
        editor = HandoffEditor(self.root)
        draft = editor.begin_draft()
        draft.working_node(source).set('name', 'source-renamed')
        draft.commit()
        self.assertNotIn('channel', source.attrib)
        self.assertEqual(source.attrib.get('name'), 'source-renamed')

    def test_child_removal_failure_leaves_confirmed_tree_intact(self):
        d = manifest_with_id(self.root, 'D')
        d_fee = fee_of(d)
        editor = HandoffEditor(self.root)
        draft = editor.begin_draft()
        w_d = draft.working_node(d)
        removed = w_d.pop()
        self.assertEqual(removed.local_name, 'fee')
        with self.assertRaises(HandoffValidationError):
            draft.commit()
        # The confirmed manifest still has both children.
        self.assertEqual([c.local_name for c in d], ['id', 'fee'])
        self.assertIs(fee_of(d), d_fee)
        draft.discard()

    def test_new_nested_group_with_manifest_is_adopted(self):
        target = group_named(self.root, 'target')
        editor = HandoffEditor(self.root)
        draft = editor.begin_draft()
        w_target = draft.working_node(target)
        new_group = draft.create_element(
            f'{{{NS}}}group', attrib={'name': 'new-inner', 'channel': 'sea'})
        new_manifest = draft.create_element(f'{{{NS}}}manifest')
        new_manifest.append(DataElement(f'{{{NS}}}id', value='E'))
        new_manifest.append(DataElement(f'{{{NS}}}fee', value='8.75'))
        new_group.append(new_manifest)
        w_target.append(new_group)
        draft.commit()

        created_group = group_named(self.root, 'new-inner')
        created = manifest_with_id(created_group, 'E')
        self.assertTrue(created.xsd_type.name.endswith('manifest-sea'))
        self.assertEqual(fee_of(created).value, Decimal('8.75'))
        self.assertTrue(self.root.is_valid())

    def test_new_node_created_in_draft_is_adopted(self):
        editor = HandoffEditor(self.root)
        draft = editor.begin_draft()
        target = draft.working_node(group_named(self.root, 'target'))
        new_manifest = draft.create_element(
            f'{{{NS}}}manifest',
            attrib={'channel': 'sea'},
        )
        new_id = DataElement(f'{{{NS}}}id', value='C')
        new_fee = DataElement(f'{{{NS}}}fee', value='4.25')
        new_manifest.append(new_id)
        new_manifest.append(new_fee)
        target.append(new_manifest)
        draft.commit()

        created = manifest_with_id(self.root, 'C')
        self.assertTrue(created.xsd_type.name.endswith('manifest-sea'))
        self.assertEqual(fee_of(created).value, Decimal('4.25'))
        self.assertTrue(self.root.is_valid())


if __name__ == '__main__':
    run_xmlschema_tests('handoff editing')
