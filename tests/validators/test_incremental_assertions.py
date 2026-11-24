#!/usr/bin/env python
#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import pathlib
from textwrap import dedent

from xmlschema import XMLSchema11, XMLResource
from xmlschema.validators import IncrementalAssertionValidator, \
    AssertionRevalidationResult, FullRevalidationRequirement
from xmlschema.testing import XsdValidatorTestCase


SCHEMA_SOURCE = dedent("""\
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:complexType name="areaType">
            <xs:sequence>
                <xs:element name="waybill" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:attribute name="id" type="xs:string"/>
                        <xs:attribute name="cancelled" type="xs:boolean"/>
                        <xs:attribute name="amount" type="xs:decimal"/>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
            <xs:attribute name="first" type="xs:string"/>
            <xs:assert test="waybill[not(@cancelled = true())][1]/@id = @first"/>
        </xs:complexType>
        <xs:complexType name="billType">
            <xs:sequence>
                <xs:element name="area" type="areaType" maxOccurs="unbounded"/>
            </xs:sequence>
            <xs:attribute name="total" type="xs:decimal"/>
            <xs:assert test="sum(area/waybill[not(@cancelled = true())]/@amount) = @total"/>
        </xs:complexType>
        <xs:element name="bill" type="billType"/>
    </xs:schema>
""")


class TestIncrementalAssertions(XsdValidatorTestCase):

    cases_dir = pathlib.Path(__file__).parent.joinpath('../test_cases')
    schema_class = XMLSchema11

    def setUp(self):
        self.schema = self.schema_class(SCHEMA_SOURCE)

    def get_resource(self):
        source = dedent("""\
            <bill total="40">
                <area first="a">
                    <waybill id="a" amount="10"/>
                    <waybill id="b" amount="20"/>
                </area>
                <area first="x">
                    <waybill id="x" amount="5"/>
                    <waybill id="y" amount="5"/>
                </area>
            </bill>
        """)
        return XMLResource(source)

    def test_incremental_api(self):
        resource = self.get_resource()
        validator = self.schema.increment(resource)
        self.assertIsInstance(validator, IncrementalAssertionValidator)

        result = validator.revalidate([])
        self.assertIsInstance(result, AssertionRevalidationResult)
        self.assertEqual(result.errors, [])
        self.assertTrue(result.is_valid)
        self.assertTrue(result.is_complete)
        self.assertEqual(len(result.skipped), 3)

    def test_first_waybill_after_cancellation(self):
        resource = self.get_resource()
        validator = self.schema.increment(resource)

        # Make the initial first-waybill assertion fail.  The bill total is
        # corrected in the same batch, otherwise the sum assertion fails too.
        resource.root[0][0].set('cancelled', 'true')
        resource.root.set('total', '30')
        result = validator.revalidate([
            (resource.root[0][0], '@cancelled'),
            (resource.root, '@total'),
        ])
        self.assertEqual(len(result.errors), 1)
        self.assertEqual(len(result.added), 1)
        self.assertIn('waybill[not(@cancelled', result.added[0].validator.path)

        # The next waybill is now the first non-cancelled one.
        resource.root[0].set('first', 'b')
        result = validator.revalidate([(resource.root[0], '@first')])
        self.assertEqual(result.errors, [])
        self.assertEqual(len(result.removed), 1)
        self.assertEqual(len(result.added), 0)

    def test_batch_uses_final_values(self):
        resource = self.get_resource()
        validator = self.schema.increment(resource)

        # The first waybill is cancelled, the second amount is corrected
        # from 20 to 25, and the total is adjusted in the same batch.
        resource.root[0][0].set('cancelled', 'true')
        resource.root[0][1].set('amount', '25')
        resource.root[0].set('first', 'b')
        resource.root.set('total', '35')

        result = validator.revalidate([
            (resource.root[0][0], '@cancelled'),
            (resource.root[0][1], '@amount'),
            (resource.root[0], '@first'),
            (resource.root, '@total'),
        ])

        # The sum uses 25 + 5 + 5 = 35 and not the intermediate 20.
        self.assertEqual(result.errors, [])
        self.assertEqual(result.removed, [])
        self.assertEqual(result.added, [])

    def test_batch_is_not_evaluated_at_intermediate_states(self):
        resource = self.get_resource()
        validator = self.schema.increment(resource)

        resource.root[0][0].set('cancelled', 'true')
        resource.root[0][1].set('amount', '25')
        resource.root[0].set('first', 'b')
        resource.root.set('total', '35')
        result = validator.revalidate([
            (resource.root[0][0], '@cancelled'),
            (resource.root[0][1], '@amount'),
            (resource.root[0], '@first'),
            (resource.root, '@total'),
        ])
        self.assertEqual(result.errors, [])

        # A later batch with only the old amount would be invalid.  The
        # first batch must not have evaluated that intermediate state.
        resource.root[0][1].set('amount', '20')
        result = validator.revalidate([(resource.root[0][1], '@amount')])
        self.assertEqual(len(result.errors), 1)
        self.assertIn('sum(', result.errors[0].validator.path)

    def test_untouched_area_is_not_revalidated(self):
        resource = self.get_resource()
        validator = self.schema.increment(resource)

        resource.root[0][0].set('cancelled', 'true')
        resource.root[0][1].set('amount', '25')
        resource.root[0].set('first', 'b')
        resource.root.set('total', '35')

        result = validator.revalidate([
            (resource.root[0][0], '@cancelled'),
            (resource.root[0][1], '@amount'),
            (resource.root[0], '@first'),
            (resource.root, '@total'),
        ])

        self.assertEqual(result.errors, [])
        self.assertEqual(len(result.revalidated), 2)
        self.assertEqual(len(result.skipped), 1)
        self.assertIs(result.skipped[0].parent, self.schema.types['areaType'])

    def test_stale_error_removed_and_retained_error_keeps_path(self):
        resource = self.get_resource()
        validator = self.schema.increment(resource)

        # Create two assertion errors: area 1 and the bill total.
        resource.root[0].set('first', 'wrong')
        resource.root.set('total', '999')
        result = validator.revalidate([
            (resource.root[0], '@first'),
            (resource.root, '@total'),
        ])
        self.assertEqual(len(result.errors), 2)
        paths = {error.path for error in result.errors}
        self.assertEqual(paths, {'/bill/area[1]', '/bill'})

        # Fix only the first area.  Its error must be removed from the current
        # errors, while the bill error stays in place with its original path.
        resource.root[0].set('first', 'a')
        result = validator.revalidate([(resource.root[0], '@first')])

        self.assertEqual(len(result.errors), 1)
        self.assertEqual(len(result.removed), 1)
        self.assertIn('waybill[not(@cancelled', result.removed[0].validator.path)
        self.assertEqual(result.errors[0].path, '/bill')
        self.assertIn('sum(', result.errors[0].validator.path)

        # Fixing the total removes the remaining error.
        resource.root.set('total', '40')
        result = validator.revalidate([(resource.root, '@total')])
        self.assertEqual(result.errors, [])
        self.assertEqual(len(result.removed), 1)

    def test_path_strings_and_queued_changes(self):
        resource = self.get_resource()
        validator = self.schema.increment(resource)

        resource.root[0][0].set('cancelled', 'true')
        resource.root.set('total', '30')
        validator.mark_changed('/bill/area[1]/waybill[1]/@cancelled')
        validator.mark_changed('/bill/@total')
        result = validator.revalidate()
        self.assertEqual(len(result.added), 1)

        resource.root[0].set('first', 'b')
        result = validator.revalidate('/bill/area[1]/@first')
        self.assertEqual(result.errors, [])

    def test_unsupported_xpath_requests_full_revalidation(self):
        schema = self.schema_class(dedent("""\
            <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
                <xs:complexType name="rootType">
                    <xs:sequence>
                        <xs:element name="item" maxOccurs="unbounded">
                            <xs:complexType>
                                <xs:attribute name="code" type="xs:string"/>
                            </xs:complexType>
                        </xs:element>
                    </xs:sequence>
                    <xs:attribute name="first" type="xs:string"/>
                    <xs:assert test="count(item) = 1 or item[1]/@code = @first"/>
                </xs:complexType>
                <xs:element name="root" type="rootType"/>
            </xs:schema>
        """))

        resource = XMLResource('<root first="a"><item code="a"/></root>')
        validator = schema.increment(resource)
        result = validator.revalidate([])
        self.assertEqual(result.errors, [])

        resource.root[0].set('code', 'b')
        result = validator.revalidate([(resource.root[0], '@code')])
        self.assertTrue(result.requires_full_revalidation)
        self.assertEqual(len(result.full_revalidation), 1)
        requirement = result.full_revalidation[0]
        self.assertIsInstance(requirement, FullRevalidationRequirement)
        self.assertEqual(requirement.path, '/root')
        self.assertIn('full revalidation', str(requirement))

    def test_qualified_assertion_paths(self):
        schema = self.schema_class(dedent("""\
            <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    xmlns:t="urn:test" targetNamespace="urn:test"
                    elementFormDefault="qualified">
                <xs:complexType name="areaType">
                    <xs:sequence>
                        <xs:element name="waybill" maxOccurs="unbounded">
                            <xs:complexType>
                                <xs:attribute name="id" type="xs:string"/>
                                <xs:attribute name="cancelled" type="xs:boolean"/>
                                <xs:attribute name="amount" type="xs:decimal"/>
                            </xs:complexType>
                        </xs:element>
                    </xs:sequence>
                    <xs:attribute name="first" type="xs:string"/>
                    <xs:assert test="t:waybill[not(@cancelled = true())][1]/@id = @first"/>
                </xs:complexType>
                <xs:complexType name="billType">
                    <xs:sequence>
                        <xs:element name="area" type="t:areaType" maxOccurs="unbounded"/>
                    </xs:sequence>
                    <xs:attribute name="total" type="xs:decimal"/>
                    <xs:assert test="sum(t:area/t:waybill[
                        not(@cancelled = true())]/@amount) = @total"/>
                </xs:complexType>
                <xs:element name="bill" type="t:billType"/>
            </xs:schema>
        """))

        resource = XMLResource(dedent("""\
            <bill xmlns="urn:test" xmlns:t="urn:test" total="30">
                <area first="a">
                    <waybill id="a" amount="10"/>
                    <waybill id="b" amount="20"/>
                </area>
            </bill>
        """))
        validator = schema.increment(resource)
        self.assertEqual(validator.revalidate([]).errors, [])

        resource.root[0][0].set('cancelled', 'true')
        resource.root[0][1].set('amount', '20')
        resource.root[0].set('first', 'b')
        resource.root.set('total', '20')
        result = validator.revalidate([
            (resource.root[0][0], '@cancelled'),
            (resource.root[0][1], '@amount'),
            (resource.root[0], '@first'),
            (resource.root, '@total'),
        ])
        self.assertEqual(result.errors, [])

    def test_result_matches_full_validation(self):
        resource = self.get_resource()
        errors = list(self.schema.iter_errors(resource))
        self.assertEqual(errors, [])

        # Introduce two assertion errors, then hand the errors of the real
        # validation pass to the incremental session.
        resource.root[0].set('first', 'wrong')
        resource.root.set('total', '999')
        errors = list(self.schema.iter_errors(resource))
        self.assertEqual(len(errors), 2)

        # Actual validation errors don't carry a persistent reference to
        # this resource tree, so they're only used as initial error state.
        for error in errors:
            _ = error.path

        validator = self.schema.increment(resource, errors=errors)
        self.assertEqual(len(validator.errors), 2)

        resource.root[0][0].set('cancelled', 'true')
        resource.root[0][1].set('amount', '25')
        resource.root[0].set('first', 'b')
        resource.root.set('total', '35')

        result = validator.revalidate([
            (resource.root[0][0], '@cancelled'),
            (resource.root[0][1], '@amount'),
            (resource.root[0], '@first'),
            (resource.root, '@total'),
        ])

        self.assertEqual(result.errors, [])
        self.assertEqual(len(result.removed), 2)

        # Cross-check against a complete validation of the final tree.
        self.assertEqual(list(self.schema.iter_errors(resource.tostring())), [])

        resource.root.set('total', '31')
        result = validator.revalidate([(resource.root, '@total')])
        self.assertEqual(len(result.errors), 1)
        full_errors = list(self.schema.iter_errors(resource.tostring()))
        self.assertEqual(len(full_errors), 1)

    def test_schema_prefix_binding(self):
        schema = self.schema_class(dedent("""\
            <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    xmlns:t="urn:test" targetNamespace="urn:test"
                    elementFormDefault="qualified">
                <xs:complexType name="areaType">
                    <xs:sequence>
                        <xs:element name="waybill" maxOccurs="unbounded">
                            <xs:complexType>
                                <xs:attribute name="id" type="xs:string"/>
                                <xs:attribute name="cancelled" type="xs:boolean"/>
                                <xs:attribute name="amount" type="xs:decimal"/>
                            </xs:complexType>
                        </xs:element>
                    </xs:sequence>
                    <xs:attribute name="first" type="xs:string"/>
                    <xs:assert test="t:waybill[not(@cancelled = true())][1]/@id = @first"/>
                </xs:complexType>
                <xs:complexType name="billType">
                    <xs:sequence>
                        <xs:element name="area" type="t:areaType" maxOccurs="unbounded"/>
                    </xs:sequence>
                    <xs:attribute name="total" type="xs:decimal"/>
                    <xs:assert test="sum(t:area/t:waybill[
                        not(@cancelled = true())]/@amount) = @total"/>
                </xs:complexType>
                <xs:element name="bill" type="t:billType"/>
            </xs:schema>
        """))

        # The instance rebinds "t" to another URI: assertion prefixes are bound
        # to the schema, so the incremental path must ignore the instance one.
        resource = XMLResource(dedent("""\
            <bill xmlns="urn:test" xmlns:t="urn:other" total="30">
                <area first="a">
                    <waybill id="a" amount="10"/>
                    <waybill id="b" amount="20"/>
                </area>
            </bill>
        """))
        validator = schema.increment(resource)
        self.assertEqual(validator.revalidate([]).errors, [])

        resource.root[0][0].set('cancelled', 'true')
        resource.root[0].set('first', 'b')
        resource.root.set('total', '20')
        result = validator.revalidate([
            (resource.root[0][0], '@cancelled'),
            (resource.root[0], '@first'),
            (resource.root, '@total'),
        ])
        self.assertEqual(result.errors, [])
        self.assertEqual(list(schema.iter_errors(resource.tostring())), [])

    def test_lazy_resource_is_rejected(self):
        resource = XMLResource(self.get_resource().tostring(), lazy=True)
        with self.assertRaises(ValueError):
            self.schema.increment(resource)

    def test_inserted_and_removed_waybill(self):
        from xml.etree import ElementTree

        resource = self.get_resource()
        validator = self.schema.increment(resource)

        new_waybill = ElementTree.Element(
            resource.root[0][0].tag, {'id': 'z', 'amount': '5'}
        )
        resource.root[0].insert(0, new_waybill)
        resource.root[0].set('first', 'z')
        resource.root.set('total', '45')

        result = validator.revalidate([
            (resource.root[0], None),
            (resource.root[0], '@first'),
            (resource.root, '@total'),
        ])
        self.assertEqual(result.errors, [])

        resource.root[0].remove(new_waybill)
        resource.root[0].set('first', 'a')
        resource.root.set('total', '40')

        result = validator.revalidate([
            (resource.root[0], None),
            (resource.root[0], '@first'),
            (resource.root, '@total'),
        ])
        self.assertEqual(result.errors, [])

    def test_typed_first_item_comparison(self):
        schema = self.schema_class(dedent("""\
            <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
                <xs:complexType name="rootType">
                    <xs:sequence>
                        <xs:element name="waybill" maxOccurs="unbounded">
                            <xs:complexType>
                                <xs:attribute name="id" type="xs:int"/>
                                <xs:attribute name="cancelled" type="xs:boolean"/>
                            </xs:complexType>
                        </xs:element>
                    </xs:sequence>
                    <xs:attribute name="first" type="xs:int"/>
                    <xs:assert test="waybill[not(@cancelled = true())][1]/@id = @first"/>
                </xs:complexType>
                <xs:element name="bill" type="rootType"/>
            </xs:schema>
        """))

        # XSD integer equality is on typed values, so '01' equals '1'.
        resource = XMLResource(
            '<bill first="01"><waybill id="1"/></bill>'
        )
        validator = schema.increment(resource)
        self.assertEqual(validator.revalidate([]).errors, [])

        resource.root[0].set('id', '2')
        result = validator.revalidate([(resource.root[0], '@id')])
        self.assertEqual(len(result.added), 1)

    def test_boolean_lexical_predicate(self):
        schema = self.schema_class(dedent("""\
            <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
                <xs:complexType name="rootType">
                    <xs:sequence>
                        <xs:element name="waybill" maxOccurs="unbounded">
                            <xs:complexType>
                                <xs:attribute name="id" type="xs:string"/>
                                <xs:attribute name="cancelled" type="xs:boolean"/>
                            </xs:complexType>
                        </xs:element>
                    </xs:sequence>
                    <xs:attribute name="first" type="xs:string"/>
                    <xs:assert test="waybill[not(@cancelled = true())][1]/@id = @first"/>
                </xs:complexType>
                <xs:element name="bill" type="rootType"/>
            </xs:schema>
        """))

        resource = XMLResource(
            '<bill first="b"><waybill id="a" cancelled="1"/><waybill id="b"/></bill>'
        )
        validator = schema.increment(resource)
        self.assertEqual(validator.revalidate([]).errors, [])

        # 'false' is a valid lexical form of xs:boolean and makes item 'a'
        # the first non-cancelled waybill again.
        resource.root[0].set('cancelled', 'false')
        result = validator.revalidate([(resource.root[0], '@cancelled')])
        self.assertEqual(len(result.added), 1)

    def test_xpath_default_namespace(self):
        schema = self.schema_class(dedent("""\
            <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    xmlns:t="urn:test" targetNamespace="urn:test"
                    elementFormDefault="qualified"
                    xpathDefaultNamespace="##targetNamespace">
                <xs:complexType name="areaType">
                    <xs:sequence>
                        <xs:element name="waybill" maxOccurs="unbounded">
                            <xs:complexType>
                                <xs:attribute name="id" type="xs:string"/>
                                <xs:attribute name="cancelled" type="xs:boolean"/>
                                <xs:attribute name="amount" type="xs:decimal"/>
                            </xs:complexType>
                        </xs:element>
                    </xs:sequence>
                    <xs:attribute name="first" type="xs:string"/>
                    <xs:assert test="waybill[not(@cancelled = true())][1]/@id = @first"/>
                </xs:complexType>
                <xs:complexType name="billType">
                    <xs:sequence>
                        <xs:element name="area" type="t:areaType" maxOccurs="unbounded"/>
                    </xs:sequence>
                    <xs:attribute name="total" type="xs:decimal"/>
                    <xs:assert test="sum(area/waybill[
                        not(@cancelled = true())]/@amount) = @total"/>
                </xs:complexType>
                <xs:element name="bill" type="t:billType"/>
            </xs:schema>
        """))

        resource = XMLResource(dedent("""\
            <bill xmlns="urn:test" total="30">
                <area first="a">
                    <waybill id="a" amount="10"/>
                    <waybill id="b" amount="20"/>
                </area>
            </bill>
        """))
        validator = schema.increment(resource)
        self.assertEqual(validator.revalidate([]).errors, [])

        resource.root[0][0].set('cancelled', 'true')
        resource.root[0][1].set('amount', '20')
        resource.root[0].set('first', 'b')
        resource.root.set('total', '20')
        result = validator.revalidate([
            (resource.root[0][0], '@cancelled'),
            (resource.root[0][1], '@amount'),
            (resource.root[0], '@first'),
            (resource.root, '@total'),
        ])
        self.assertEqual(result.errors, [])


if __name__ == '__main__':
    from xmlschema.testing import run_xmlschema_tests
    run_xmlschema_tests('incremental XSD assertions')
