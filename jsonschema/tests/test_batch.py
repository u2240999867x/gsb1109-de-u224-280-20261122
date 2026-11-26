import itertools

import pytest

from jsonschema import Draft202012Validator, batch


def kinded(kind):
    return {
        "type": "object",
        "properties": {"kind": {"const": kind}},
        "required": ["kind"],
    }


CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "prefixItems": [kinded("header")],
    "contains": kinded("sample"),
    "minContains": 0,
    "anyOf": [
        {"contains": kinded("A")},
        {"contains": kinded("B"), "maxContains": 1},
    ],
    "unevaluatedItems": False,
    "uniqueItems": True,
}


def batch_records(samples, tail=()):
    return [
        {"kind": "header", "seq": 0},
        {"kind": "A", "v": 1},
        {"kind": "B", "v": True},
        *[{"kind": "sample", "seq": i} for i in range(samples)],
        *tail,
    ]


def validator(schema=CONTRACT):
    return Draft202012Validator(schema)


class TestContract:
    def test_valid_batch_is_accepted(self):
        records = batch_records(samples=20)
        assert validator().is_valid_batch(iter(records))
        assert list(validator().iter_batch_errors(iter(records))) == []
        assert validator().is_valid(records)

    def test_extra_b_leaves_both_bs_unevaluated(self):
        records = batch_records(samples=20, tail=[{"kind": "B", "v": 1}])
        errors = list(validator().iter_batch_errors(iter(records)))

        assert not validator().is_valid(records)
        unevaluated = [
            error for error in errors if error.validator == "unevaluatedItems"
        ]
        assert sorted(error.path[0] for error in unevaluated) == [2, 23]
        assert all(error.validator != "uniqueItems" for error in errors)
        assert all(error.validator != "anyOf" for error in errors)

    def test_duplicate_a_is_rejected_with_original_indices(self):
        records = batch_records(samples=20, tail=[{"v": 1.0, "kind": "A"}])
        errors = list(validator().iter_batch_errors(iter(records)))

        assert not validator().is_valid(records)
        duplicates = [
            error for error in errors if error.validator == "uniqueItems"
        ]
        assert len(duplicates) == 1
        assert "indexes 1, 23" in duplicates[0].message
        assert all(error.validator != "unevaluatedItems" for error in errors)

    def test_truncated_stream_reports_incomplete_input(self):
        def stream():
            yield from batch_records(samples=5)
            raise OSError("collector lost the source")

        errors = list(validator().iter_batch_errors(stream()))
        assert len(errors) == 1
        assert "fully read" in errors[0].message
        assert not validator().is_valid_batch(stream())

    def test_read_error_mid_stream_suppresses_final_verdicts(self):
        def stream():
            yield from batch_records(samples=5)[:2]
            raise OSError("boom")

        errors = list(validator().iter_batch_errors(stream()))
        assert len(errors) == 1
        assert "fully read" in errors[0].message

    def test_spool_dir_is_used_and_cleaned_up(self, tmp_path):
        assert validator().is_valid_batch(
            iter(batch_records(samples=10)),
            spool_dir=tmp_path,
        )
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.parametrize(
        "records",
        [
            batch_records(samples=3),
            batch_records(samples=3, tail=[{"kind": "B", "v": 1}]),
            batch_records(samples=3, tail=[{"v": 1.0, "kind": "A"}]),
            batch_records(samples=0),
            [{"kind": "nope"}, *batch_records(samples=2)[1:]],
            [*batch_records(samples=2), {"kind": "sample", "seq": 0}],
        ],
    )
    def test_verdicts_match_plain_list_validation(self, records):
        assert validator().is_valid_batch(
            iter(records),
        ) == validator().is_valid(records)


class TestKeywords:
    def test_bad_header_reports_index_zero(self):
        records = [{"kind": "nope"}, *batch_records(samples=2)[1:]]
        errors = list(validator().iter_batch_errors(iter(records)))
        assert [error.validator for error in errors] == ["const"]
        assert list(errors[0].path) == [0, "kind"]

    def test_items_false(self):
        schema = {"prefixItems": [{"const": 1}], "items": False}
        assert validator(schema).is_valid_batch(iter([1]))
        errors = list(validator(schema).iter_batch_errors(iter([1, 2, 3])))
        assert len(errors) == 1
        assert errors[0].validator == "items"
        assert not validator(schema).is_valid([1, 2, 3])

    def test_min_and_max_items(self):
        schema = {"minItems": 2, "maxItems": 3}
        for records, expected in [
            ([1], False),
            ([1, 2], True),
            ([1, 2, 3], True),
            ([1, 2, 3, 4], False),
        ]:
            assert validator(schema).is_valid_batch(iter(records)) == expected
            assert validator(schema).is_valid(records) == expected

    def test_one_of(self):
        schema = {"oneOf": [{"contains": {"const": 1}}, {"minItems": 1}]}
        for records, expected in [([1], False), ([2], True), ([], False)]:
            assert validator(schema).is_valid_batch(iter(records)) == expected
            assert validator(schema).is_valid(records) == expected

    def test_all_of(self):
        schema = {"allOf": [{"minItems": 1}, {"contains": {"const": "x"}}]}
        for records, expected in [
            (["x"], True),
            (["y"], False),
            ([], False),
        ]:
            assert validator(schema).is_valid_batch(iter(records)) == expected
            assert validator(schema).is_valid(records) == expected

    def test_any_of_failure_carries_context(self):
        schema = {"anyOf": [{"contains": {"const": 1}}, {"minItems": 5}]}
        errors = list(validator(schema).iter_batch_errors(iter([2, 3])))
        assert len(errors) == 1
        assert errors[0].validator == "anyOf"
        assert errors[0].context
        assert not validator(schema).is_valid([2, 3])

    def test_boolean_schemas(self):
        assert validator(True).is_valid_batch(iter([1, 2, 3]))
        assert not validator(False).is_valid_batch(iter([1, 2, 3]))
        assert not validator(False).is_valid_batch(iter([]))

    def test_type_mismatch(self):
        schema = {"type": "object"}
        assert not validator(schema).is_valid_batch(iter([1]))
        assert not validator(schema).is_valid([1])

    def test_module_level_api(self):
        assert batch.is_valid(validator(), iter(batch_records(samples=2)))
        errors = batch.iter_errors(validator({"minItems": 2}), iter([1]))
        assert len(list(errors)) == 1

    def test_unsupported_keyword_is_rejected(self):
        with pytest.raises(batch.UnsupportedBatchKeyword):
            validator({"not": {}}).is_valid_batch(iter([]))
        with pytest.raises(batch.UnsupportedBatchKeyword):
            validator({"unevaluatedItems": {"type": "object"}}).is_valid_batch(
                iter([]),
            )


class TestScale:
    def test_many_unique_records(self):
        schema = {
            "items": {"type": "object"},
            "uniqueItems": True,
            "unevaluatedItems": False,
        }
        records = ({"n": i} for i in range(100_000))
        assert validator(schema).is_valid_batch(records)

    def test_many_records_duplicate_detected(self):
        schema = {"items": {"type": "object"}, "uniqueItems": True}
        records = itertools.chain(
            ({"n": i} for i in range(100_000)),
            [{"n": 0}],
        )
        errors = [
            error
            for error in validator(schema).iter_batch_errors(records)
            if error.validator == "uniqueItems"
        ]
        assert len(errors) == 1
        assert "indexes 0, 100000" in errors[0].message
