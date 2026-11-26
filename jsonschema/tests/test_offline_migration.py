from __future__ import annotations

from urllib.parse import urljoin

from jsonschema import Draft202012Validator
from jsonschema.migration import migrate_offline

import unittest


BASE = "https://settlement.example/schemas/"
DETAIL = urljoin(BASE, "detail.json")
OLD_ENTRY = urljoin(BASE, "order-draft7.json")
NEW_ENTRY = urljoin(BASE, "order-2020.json")
EMBEDDED_ID = urljoin(DETAIL, "route-item/contract.json")
NESTED_CODES = urljoin(EMBEDDED_ID, "codes.json")
ROOT_CODES = urljoin(DETAIL, "codes.json")

CODE_OUTER = DETAIL + "#/properties/route/items/1/properties/code"
CODE_INNER = EMBEDDED_ID + "#/properties/code"
DEPENDENCY_RULE = DETAIL + "#/dependencies/reviewRequired"
DEPENDENCY_LIST = DETAIL + "#/dependencies/simpleOther"


def build_fixture():
    detail = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": DETAIL,
        "type": "object",
        "properties": {
            "route": {
                "type": "array",
                "items": [
                    {"type": "string"},
                    {
                        "$id": "route-item/contract.json",
                        "type": "object",
                        "properties": {
                            "code": {
                                "$ref": "codes.json",
                                "maxLength": 1,
                            },
                        },
                    },
                ],
                "additionalItems": False,
            },
            "headCode": {"$ref": "codes.json"},
            "businessRefWord": {"const": "$ref"},
        },
        "dependencies": {
            "reviewRequired": {
                "type": "object",
                "properties": {
                    "reviewNote": {"type": "string", "minLength": 1},
                },
                "required": ["reviewNote"],
            },
            "simpleOther": ["otherField"],
        },
    }
    old_entry = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": OLD_ENTRY,
        "$ref": DETAIL,
    }
    new_entry = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": NEW_ENTRY,
        "$ref": DETAIL,
        "not": {"required": ["forceEntryReject"]},
    }
    resources = {
        DETAIL: detail,
        OLD_ENTRY: old_entry,
        NEW_ENTRY: new_entry,
        ROOT_CODES: {"$id": ROOT_CODES, "enum": ["W1"]},
        NESTED_CODES: {"$id": NESTED_CODES, "enum": ["N1"]},
    }
    return resources, old_entry, new_entry


class OfflineMigrationTest(unittest.TestCase):
    def test_maps_rules_and_preserves_original_decisions(self):
        resources, old_entry, new_entry = build_fixture()
        result = migrate_offline(
            resource_table=resources,
            order_entry=[old_entry, new_entry],
            review_addresses=[
                CODE_OUTER,
                CODE_INNER,
                DEPENDENCY_RULE,
            ],
        )

        self.assertEqual(result.resources[DETAIL]["$id"], DETAIL)
        self.assertEqual(result.entries[OLD_ENTRY]["$id"], OLD_ENTRY)
        self.assertEqual(result.entries[NEW_ENTRY]["$id"], NEW_ENTRY)
        self.assertEqual(
            result.locations[CODE_OUTER],
            DETAIL
            + "#/properties/route/prefixItems/1/properties/code",
        )
        self.assertEqual(result.locations[CODE_INNER], CODE_INNER)
        self.assertEqual(
            result.locations[DEPENDENCY_RULE],
            DETAIL + "#/dependentSchemas/reviewRequired",
        )

        self.assertIs(
            result.rules[CODE_OUTER],
            result.rules[CODE_INNER],
        )
        looked_up_outer = result.registry.resolver().lookup(
            str(result.locations[CODE_OUTER]),
        )
        looked_up_inner = result.registry.resolver().lookup(
            str(result.locations[CODE_INNER]),
        )
        self.assertIs(looked_up_outer.contents, looked_up_inner.contents)

        valid_row = {
            "route": ["x", {"code": "N1"}],
            "headCode": "W1",
        }
        swapped = {
            "route": ["x", {"code": "W1"}],
            "headCode": "N1",
        }
        extra_third = {
            "route": ["x", {"code": "N1"}, "third"],
            "headCode": "W1",
        }
        entry_reject = {**valid_row, "forceEntryReject": True}
        dependency_triggered = {
            **valid_row,
            "reviewRequired": True,
        }

        for uri in (OLD_ENTRY, NEW_ENTRY):
            validator = Draft202012Validator(
                result.entries[uri],
                registry=result.registry,
            )
            self.assertTrue(validator.is_valid(valid_row))
            self.assertFalse(validator.is_valid(swapped))
            self.assertFalse(validator.is_valid(extra_third))
            self.assertFalse(validator.is_valid(dependency_triggered))

        old_validator = Draft202012Validator(
            result.entries[OLD_ENTRY],
            registry=result.registry,
        )
        new_validator = Draft202012Validator(
            result.entries[NEW_ENTRY],
            registry=result.registry,
        )
        self.assertTrue(old_validator.is_valid(entry_reject))
        self.assertFalse(new_validator.is_valid(entry_reject))

        for address in (CODE_OUTER, CODE_INNER):
            code_validator = result.validators[address]
            self.assertTrue(code_validator.is_valid("N1"))
            self.assertFalse(code_validator.is_valid("W1"))

        dependency_validator = result.validators[DEPENDENCY_RULE]
        self.assertFalse(dependency_validator.is_valid({}))
        self.assertTrue(
            dependency_validator.is_valid({"reviewNote": "ok"}),
        )

        whole_order = Draft202012Validator(
            result.entries[OLD_ENTRY],
            registry=result.registry,
        )
        self.assertTrue(whole_order.is_valid({}))
        self.assertFalse(
            whole_order.is_valid({"reviewRequired": True}),
        )

        self.assertEqual(
            result.resources[DETAIL]["properties"][
                "businessRefWord"
            ],
            {"const": "$ref"},
        )
        self.assertIn("prefixItems", resources[DETAIL]["properties"]["route"])

    def test_dependency_field_name_list_is_not_a_review_contract(self):
        resources, old_entry, _new_entry = build_fixture()
        with self.assertRaisesRegex(Exception, DEPENDENCY_LIST):
            migrate_offline(
                resources,
                old_entry,
                [DEPENDENCY_LIST],
            )

    def test_missing_code_table_reports_reference_source(self):
        resources, old_entry, _new_entry = build_fixture()
        del resources[NESTED_CODES]
        with self.assertRaisesRegex(Exception, "codes.json") as caught:
            migrate_offline(
                resource_table=resources,
                order_entry=old_entry,
                review_addresses=[CODE_INNER],
            )
        self.assertIn(NESTED_CODES, str(caught.exception))
        self.assertIn(DETAIL, str(caught.exception))

    def test_supports_tuple_unpacking(self):
        resources, old_entry, _new_entry = build_fixture()
        migrated_resources, migrated_entry, mappings = migrate_offline(
            resources,
            old_entry,
            [CODE_OUTER],
        )
        self.assertIs(migrated_resources, resources)
        self.assertEqual(migrated_entry["$id"], OLD_ENTRY)
        self.assertIn(CODE_OUTER, mappings)


if __name__ == "__main__":
    unittest.main()
