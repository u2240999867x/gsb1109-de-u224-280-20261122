from __future__ import annotations

from decimal import Decimal
from unittest import TestCase
import json

from jsonschema.amounts import (
    AmountAdmission,
    AmountContract,
    AmountSegment,
    AmountStatus,
    audit_amount_contract,
)
from jsonschema.exceptions import SchemaError


def overlapping_tiers(*, tier_two_upper: str, tier_two_inclusive: bool):
    tier_two = {
        "minimum": 0,
        "multipleOf": Decimal("0.05"),
    }
    if tier_two_inclusive:
        tier_two["maximum"] = Decimal(tier_two_upper)
    else:
        tier_two["exclusiveMaximum"] = Decimal(tier_two_upper)
    return {
        "oneOf": [
            {
                "minimum": 0,
                "exclusiveMaximum": Decimal("0.30"),
                "multipleOf": Decimal("0.03"),
            },
            tier_two,
        ],
    }


class AmountContractTest(TestCase):
    def setUp(self):
        self.contract = audit_amount_contract(
            overlapping_tiers(
                tier_two_upper="0.25",
                tier_two_inclusive=True,
            ),
            lower=0,
            upper=Decimal("0.30"),
            unit=Decimal("0.01"),
            upper_inclusive=False,
        )

    def test_individual_tier_amounts_are_admissible(self):
        for amount in ("0.03", "0.05", "0.06", "0.25", "0.27"):
            with self.subTest(amount=amount):
                admission = self.contract.classify(Decimal(amount))
                self.assertIs(admission.status, AmountStatus.ADMISSIBLE)
                self.assertTrue(admission.accepted)
                self.assertTrue(admission.registry_compatible)

    def test_shared_amount_is_an_exclusive_overlap(self):
        admission = self.contract.classify(Decimal("0.15"))
        self.assertIs(
            admission.status,
            AmountStatus.EXCLUSIVE_OVERLAP,
        )
        self.assertFalse(admission.accepted)
        self.assertIsNone(admission.registry_compatible)
        self.assertEqual(
            admission.conflicting_branches,
            ("oneOf[0]", "oneOf[1]"),
        )

    def test_open_upper_bound_is_excluded(self):
        admission = self.contract.classify(Decimal("0.30"))
        self.assertIs(admission.status, AmountStatus.OUT_OF_RANGE)

    def test_amount_between_steps_is_a_step_mismatch(self):
        admission = self.contract.classify(Decimal("0.16"))
        self.assertIs(admission.status, AmountStatus.STEP_MISMATCH)
        self.assertFalse(admission.accepted)

    def test_segments_partition_every_grid_point_once(self):
        grid = self.contract.grid_unit
        covered = set()
        for segment in self.contract.segments():
            point = segment.start
            while point <= segment.end:
                self.assertNotIn(point, covered)
                covered.add(point)
                if segment.step is None:
                    break
                point += segment.step
        expected = {
            Decimal(position) * grid
            for position in range(
                self.contract.window_lower_int,
                self.contract.window_upper_int + 1,
            )
        }
        self.assertEqual(covered, expected)

    def test_segment_lookup_matches_classification(self):
        for position in range(0, 30):
            amount = Decimal(position) / Decimal(100)
            self.assertIs(
                self.contract.segment_status_at(amount),
                self.contract.classify(amount).status,
            )

    def test_every_cent_is_registrable_for_a_cent_unit(self):
        self.assertIsNone(self.contract.warehouse_counterexample())
        for segment in self.contract.segments():
            if segment.status is AmountStatus.ADMISSIBLE:
                self.assertTrue(segment.registry_compatible)


class MovedUpperBoundTest(TestCase):
    def contract(self, *, upper, inclusive):
        return audit_amount_contract(
            overlapping_tiers(
                tier_two_upper=upper,
                tier_two_inclusive=inclusive,
            ),
            lower=0,
            upper=Decimal("0.30"),
            unit=Decimal("0.01"),
            upper_inclusive=False,
        )

    def test_boundary_amount_flips_to_admissible(self):
        moved = self.contract(upper="0.15", inclusive=False)
        admission = moved.classify(Decimal("0.15"))
        self.assertIs(admission.status, AmountStatus.ADMISSIBLE)
        self.assertEqual(admission.matched_branches, ("oneOf[0]",))

    def test_only_the_origin_overlaps_after_the_move(self):
        moved = self.contract(upper="0.15", inclusive=False)
        self.assertIs(
            moved.classify(Decimal("0.00")).status,
            AmountStatus.EXCLUSIVE_OVERLAP,
        )
        self.assertIs(
            moved.classify(Decimal("0.14")).status,
            AmountStatus.STEP_MISMATCH,
        )

    def test_closed_and_open_bounds_attribute_differently(self):
        closed = self.contract(upper="0.15", inclusive=True)
        opened = self.contract(upper="0.15", inclusive=False)
        self.assertIs(
            closed.classify(Decimal("0.15")).status,
            AmountStatus.EXCLUSIVE_OVERLAP,
        )
        self.assertIs(
            opened.classify(Decimal("0.15")).status,
            AmountStatus.ADMISSIBLE,
        )


class WarehouseUnitTest(TestCase):
    def setUp(self):
        schema = {
            "minimum": Decimal("0.01"),
            "maximum": Decimal("0.10"),
            "multipleOf": Decimal("0.03"),
        }
        self.contract = audit_amount_contract(
            schema,
            lower=0,
            upper=Decimal("0.50"),
            unit=Decimal("0.02"),
        )

    def test_precise_counterexample_is_smallest(self):
        self.assertEqual(
            self.contract.warehouse_counterexample(),
            Decimal("0.03"),
        )

    def test_individual_registry_verdicts(self):
        rejected = self.contract.classify(Decimal("0.03"))
        accepted = self.contract.classify(Decimal("0.06"))
        self.assertFalse(rejected.registry_compatible)
        self.assertTrue(accepted.registry_compatible)

    def test_no_counterexample_when_unit_divides_the_step(self):
        schema = {"minimum": 0, "multipleOf": Decimal("0.04")}
        contract = audit_amount_contract(
            schema,
            lower=0,
            upper=Decimal("0.40"),
            unit=Decimal("0.02"),
        )
        self.assertIsNone(contract.warehouse_counterexample())


class HugeWindowTest(TestCase):
    def setUp(self):
        schema = overlapping_tiers(
            tier_two_upper="0.25",
            tier_two_inclusive=True,
        )
        self.small = audit_amount_contract(
            schema,
            lower=0,
            upper=Decimal("0.30"),
            unit=Decimal("0.01"),
            upper_inclusive=False,
        )
        self.huge = audit_amount_contract(
            schema,
            lower=0,
            upper=Decimal("1e15"),
            unit=Decimal("0.01"),
            upper_inclusive=False,
        )

    def test_overlap_conclusion_is_window_independent(self):
        for amount in ("0.03", "0.05", "0.15", "0.16", "0.27"):
            with self.subTest(amount=amount):
                self.assertIs(
                    self.huge.classify(Decimal(amount)).status,
                    self.small.classify(Decimal(amount)).status,
                )

    def test_segment_count_is_bounded_by_step_period(self):
        # A 1e15-wide window must not produce 1e15 segments: the count stays
        # bounded by the lattice period (plus one trailing out-of-range slab).
        self.assertLess(len(self.huge.segments()), 100)
        self.assertLessEqual(
            len(self.huge.segments()),
            len(self.small.segments()) + 1,
        )

    def test_amounts_beyond_the_tiers_stay_out_of_range(self):
        self.assertIs(
            self.huge.classify(Decimal("1e14")).status,
            AmountStatus.OUT_OF_RANGE,
        )


class DecimalSpellingTest(TestCase):
    text_one = (
        '{"oneOf": ['
        '{"minimum": 0, "exclusiveMaximum": 0.30, "multipleOf": 0.03}, '
        '{"minimum": 0, "maximum": 0.25, "multipleOf": 0.05}]}'
    )
    text_two = (
        '{"oneOf": ['
        '{"minimum": 0.000, "exclusiveMaximum": 0.3000, '
        '"multipleOf": 0.0300}, '
        '{"minimum": 0, "maximum": 0.25000, "multipleOf": 5E-2}]}'
    )

    def audit(self, text):
        schema = json.loads(text, parse_float=Decimal)
        return audit_amount_contract(
            schema,
            lower=0,
            upper=Decimal("0.30"),
            unit=Decimal("0.01"),
            upper_inclusive=False,
        )

    def test_equivalent_spellings_agree(self):
        one = self.audit(self.text_one)
        two = self.audit(self.text_two)
        # Probe amounts finer than either audit grid too: point classification
        # is not restricted to the internal grid.
        for position in range(0, 3000):
            amount = Decimal(position) / Decimal(10000)
            if amount >= Decimal("0.30"):
                break
            first = one.classify(amount)
            second = two.classify(amount)
            self.assertIs(first.status, second.status)
            self.assertEqual(
                first.registry_compatible,
                second.registry_compatible,
            )
        self.assertEqual(
            one.warehouse_counterexample(),
            two.warehouse_counterexample(),
        )

    def test_segment_boundaries_keep_their_membership(self):
        one = self.audit(self.text_one)
        two = self.audit(self.text_two)
        # The audits may use grids of different decimal precision, but every
        # amount on their shared cent grid resolves to the same status.
        for position in range(0, 30):
            amount = Decimal(position) / Decimal(100)
            self.assertEqual(
                one.segment_status_at(amount),
                two.segment_status_at(amount),
            )


class NestedApplicatorTest(TestCase):
    def test_allof_distributes_steps_and_bounds(self):
        schema = {
            "allOf": [
                {"type": "integer", "minimum": 0, "maximum": 10},
                {
                    "anyOf": [
                        {"multipleOf": 2},
                        {"multipleOf": 3},
                    ],
                },
            ],
        }
        contract = audit_amount_contract(
            schema,
            lower=0,
            upper=10,
            unit=1,
        )
        self.assertTrue(contract.classify(Decimal("2")).accepted)
        self.assertTrue(contract.classify(Decimal("3")).accepted)
        self.assertIs(
            contract.classify(Decimal("2.5")).status,
            AmountStatus.STEP_MISMATCH,
        )

    def test_outer_bound_masks_inner_exclusive_overlap(self):
        schema = {
            "allOf": [
                {"exclusiveMaximum": Decimal("0.10")},
                {
                    "oneOf": [
                        {"multipleOf": Decimal("0.02")},
                        {"multipleOf": Decimal("0.04")},
                    ],
                },
            ],
        }
        contract = audit_amount_contract(
            schema,
            lower=0,
            upper=Decimal("0.30"),
            unit=Decimal("0.01"),
            upper_inclusive=False,
        )
        self.assertIs(
            contract.classify(Decimal("0.20")).status,
            AmountStatus.OUT_OF_RANGE,
        )
        self.assertIs(
            contract.classify(Decimal("0.04")).status,
            AmountStatus.EXCLUSIVE_OVERLAP,
        )


class DuplicateDisjunctTest(TestCase):
    def test_duplicate_anyof_keeps_a_single_partition(self):
        schema = {
            "anyOf": [
                {
                    "minimum": 0,
                    "maximum": 1,
                    "multipleOf": Decimal("0.02"),
                },
                {
                    "minimum": 0,
                    "maximum": 1,
                    "multipleOf": Decimal("0.02"),
                },
            ],
        }
        contract = audit_amount_contract(
            schema,
            lower=0,
            upper=Decimal("0.10"),
            unit=Decimal("0.01"),
        )
        grid = contract.grid_unit
        for position in range(
            contract.window_lower_int,
            contract.window_upper_int + 1,
        ):
            amount = Decimal(position) * grid
            hits = [
                segment
                for segment in contract.segments()
                if segment.start <= amount <= segment.end
                and (amount - segment.start) % segment.step == 0
            ]
            self.assertEqual(len(hits), 1)

    def test_identical_oneof_branches_always_overlap(self):
        schema = {
            "oneOf": [
                {"minimum": 0, "multipleOf": Decimal("0.02")},
                {"minimum": 0, "multipleOf": Decimal("0.02")},
            ],
        }
        contract = audit_amount_contract(
            schema,
            lower=0,
            upper=Decimal("0.10"),
            unit=Decimal("0.01"),
        )
        self.assertIs(
            contract.classify(Decimal("0.02")).status,
            AmountStatus.EXCLUSIVE_OVERLAP,
        )
        self.assertIs(
            contract.classify(Decimal("0.01")).status,
            AmountStatus.STEP_MISMATCH,
        )


class BooleanSchemaTest(TestCase):
    def test_true_accepts_every_window_point(self):
        contract = audit_amount_contract(
            True,
            lower=0,
            upper=5,
            unit=1,
        )
        self.assertTrue(contract.classify(Decimal("3")).accepted)
        self.assertTrue(
            all(
                segment.status is AmountStatus.ADMISSIBLE
                for segment in contract.segments()
            ),
        )

    def test_false_rejects_every_window_point(self):
        contract = audit_amount_contract(
            False,
            lower=0,
            upper=5,
            unit=1,
        )
        self.assertIs(
            contract.classify(Decimal("3")).status,
            AmountStatus.OUT_OF_RANGE,
        )


class InvalidContractTest(TestCase):
    def test_unsupported_keyword_is_rejected(self):
        with self.assertRaises(SchemaError):
            audit_amount_contract(
                {"minimum": 0, "pattern": "^x$"},
                lower=0,
                upper=5,
                unit=1,
            )

    def test_non_positive_multiple_of_is_rejected(self):
        with self.assertRaises(SchemaError):
            audit_amount_contract(
                {"multipleOf": 0},
                lower=0,
                upper=5,
                unit=1,
            )

    def test_inverted_window_is_rejected(self):
        with self.assertRaises(SchemaError):
            audit_amount_contract(
                {"minimum": 0},
                lower=5,
                upper=5,
                unit=1,
            )

    def test_non_numeric_unit_is_rejected(self):
        with self.assertRaises(SchemaError):
            audit_amount_contract(
                True,
                lower=0,
                upper=5,
                unit="0.01",
            )


class ReturnShapeTest(TestCase):
    def test_admission_and_segment_shapes(self):
        contract = audit_amount_contract(
            overlapping_tiers(
                tier_two_upper="0.25",
                tier_two_inclusive=True,
            ),
            lower=0,
            upper=Decimal("0.30"),
            unit=Decimal("0.01"),
            upper_inclusive=False,
        )
        admission = contract.classify(Decimal("0.15"))
        self.assertIsInstance(admission, AmountAdmission)
        self.assertEqual(admission.value, Decimal("0.15"))
        self.assertIn("0.15", admission.reason)

        segments = contract.segments()
        self.assertTrue(segments)
        self.assertIsInstance(segments[0], AmountSegment)
        self.assertIsInstance(contract, AmountContract)

    def test_float_inputs_are_converted_exactly(self):
        contract = audit_amount_contract(
            {"minimum": 0, "multipleOf": 0.03},
            lower=0,
            upper=0.3,
            unit=0.01,
            upper_inclusive=False,
        )
        self.assertTrue(contract.classify(0.03).accepted)
        self.assertIs(
            contract.classify(0.04).status,
            AmountStatus.STEP_MISMATCH,
        )
