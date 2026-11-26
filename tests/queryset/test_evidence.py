import unittest

import pytest

import mongoengine.connection
from mongoengine import (
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    EvidenceCondition,
    EvidenceRule,
    IntField,
    ListField,
    Q,
    StringField,
    connect,
    disconnect_all,
)

try:
    import mongomock
except ImportError:  # pragma: no cover
    mongomock = None


class Reading(EmbeddedDocument):
    tag = StringField(db_field="tg")
    value = IntField(db_field="val", default=0)
    supplier = StringField(db_field="sup")

    meta = {"collection": "audit_reading"}


class Group(EmbeddedDocument):
    kind = StringField(db_field="k")
    readings = ListField(EmbeddedDocumentField(Reading), db_field="rs")


class Report(Document):
    number = StringField(db_field="no", primary_key=True)
    groups = ListField(EmbeddedDocumentField(Group), db_field="gs")

    meta = {"collection": "audit_report"}


R31_RAW = {
    "_id": "R31",
    "gs": [
        {
            "k": "coldchain",
            "rs": [
                {"tg": "HOLD"},
                {"tg": "pass", "val": 0},
                {"tg": "hold", "val": None},
            ],
        },
        {
            "k": "room",
            "rs": [
                {"tg": "hold", "val": None},
                {"tg": "hold", "val": 4},
            ],
        },
    ],
}

CI = {"locale": "en", "strength": 2}
CS = {"locale": "en", "strength": 3}


def rule_a():
    return EvidenceRule(
        "甲",
        Q(groups__readings__tag="hold") & Q(groups__readings__value=0),
    )


def rule_b():
    return EvidenceRule(
        "乙",
        {
            "gs": {
                "$elemMatch": {
                    "k": "coldchain",
                    "rs": {"$elemMatch": {"tg": "hold", "val": None}},
                }
            }
        },
    )


def rule_c():
    return EvidenceRule(
        "丙",
        {
            "gs": {
                "$elemMatch": {
                    "k": "coldchain",
                    "rs": {"$elemMatch": {"tg": "hold", "val": 0}},
                }
            }
        },
    )


@pytest.mark.skipif(mongomock is None, reason="mongomock is required")
class TestEvidence(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        disconnect_all()
        connect(
            "audit_evidence",
            host="mongodb://localhost",
            mongo_client_class=mongomock.MongoClient,
        )

    @classmethod
    def tearDownClass(cls):
        disconnect_all()

    def setUp(self):
        Report.drop_collection()
        Report._get_collection().insert_one(R31_RAW)
        self.rules = [rule_a(), rule_b(), rule_c()]

    def test_r31_reports_each_rule_with_positions(self):
        results = list(
            Report.objects(Q(groups__readings__tag="hold"))
            .collation(CI)
            .evidence(self.rules)
        )

        assert len(results) == 1
        result = results[0]
        assert result.report_number == "R31"
        assert [rule.name for rule in result.rules] == ["甲", "乙", "丙"]
        assert [rule.matched for rule in result.rules] == [True, True, False]

        rule_a_evidence, rule_b_evidence, rule_c_evidence = result.rules

        assert rule_a_evidence.cross_row[0].positions == (
            (0, 0),
            (0, 2),
            (1, 0),
            (1, 1),
        )
        assert rule_a_evidence.cross_row[1].positions == ((0, 1),)
        assert rule_b_evidence.positions == ((0, 0), (0, 2))
        assert rule_b_evidence.conditions[0].kinds == ("missing", "null")
        assert rule_c_evidence.positions == ()

    def test_case_sensitive_query_does_not_change_earlier_evidence(self):
        base = (
            Report.objects(Q(groups__readings__tag="hold"))
            .collation(CI)
        )
        insensitive = base.evidence(self.rules)
        sensitive = base.collation(CS).evidence(self.rules)

        insensitive_result = next(iter(insensitive))
        sensitive_result = next(iter(sensitive))
        # Interleave reads: the first queryset has already been evaluated,
        # the second one is evaluated afterwards.
        assert insensitive_result.rules[0].cross_row[0].positions == (
            (0, 0),
            (0, 2),
            (1, 0),
            (1, 1),
        )
        assert sensitive_result.rules[0].cross_row[0].positions == (
            (0, 2),
            (1, 0),
            (1, 1),
        )
        assert sensitive_result.rules[1].positions == ((0, 2),)
        assert insensitive_result.rules[1].positions == ((0, 0), (0, 2))

    def test_to_dict_only_exposes_report_number_and_reasons(self):
        result = next(
            iter(Report.objects.collation(CI).evidence([rule_a(), rule_b()]))
        )
        payload = result.to_dict()

        assert payload["report_number"] == "R31"
        assert payload["rules"][0]["name"] == "甲"
        assert "HOLD" not in repr(payload)
        assert "coldchain" not in repr(payload)
        assert result.document.groups[0].readings[0].value == 0

        transmit = result.transmit()
        assert set(transmit) == {"report_number", "rules"}
        assert "HOLD" not in repr(transmit)
        assert "coldchain" not in repr(transmit)

    def test_evidence_is_bound_to_the_original_document(self):
        queryset = Report.objects.collation(CI)
        result = next(iter(queryset.evidence([rule_b()])))

        # Simulate a concurrent writer changing the original after the
        # evidence-bearing cursor has read it.
        Report._get_collection().update_one(
            {"_id": "R31"},
            {
                "$set": {
                    "gs": [
                        {"k": "room", "rs": [{"tg": "hold", "val": 4}]},
                    ]
                }
            },
        )

        assert result.report_number == "R31"
        assert result.rules[0].positions == ((0, 0), (0, 2))
        assert result.rules[0].conditions[0].kinds == ("missing", "null")
        assert len(result.document.groups) == 2

    def test_normal_read_keeps_projection_and_defaults(self):
        projected = Report.objects.only("number").collation(CI)
        normal = next(iter(projected))
        evidence = next(iter(projected.evidence([rule_a()])))

        assert isinstance(normal, Report)
        assert normal.groups == []
        assert normal.number == "R31"
        # The evidence read must not inherit the projection that removed the
        # fields the rule needs.
        assert evidence.document.groups[0].readings[0].value == 0
        assert evidence.rules[0].matched is True
        assert projected._loaded_fields.as_dict() == {"_id": 1}

    def test_first_and_get_return_evidence_results(self):
        queryset = Report.objects.collation(CI)
        assert queryset.evidence([rule_a()]).first().report_number == "R31"
        assert queryset.evidence([rule_a()]).get(number="R31").rules[0].matched

    def test_rule_boolean_combinations(self):
        rules = rule_a() | (rule_b() & EvidenceRule("no", Q(number="missing")))
        result = next(iter(Report.objects.collation(CI).evidence(rules)))

        assert result.matched is True
        assert [rule.matched for rule in result.rules] == [True, True, False]

    def test_same_row_condition(self):
        rule = EvidenceRule.same_row(
            "same-row",
            Q(groups__kind="coldchain")
            & Q(groups__readings__tag="hold")
            & Q(groups__readings__value=None),
        )
        result = next(iter(Report.objects.collation(CI).evidence([rule])))

        assert result.rules[0].matched is True
        assert result.rules[0].positions == ((0, 0), (0, 2))

    def test_condition_scope_is_explicit(self):
        condition = EvidenceCondition.same_row(
            {
                "gs": {
                    "$elemMatch": {
                        "k": "coldchain",
                        "rs": {"$elemMatch": {"tg": "hold", "val": None}},
                    }
                }
            }
        )
        result = next(
            iter(
                Report.objects.collation(CI).evidence(
                    [EvidenceRule("explicit", condition)]
                )
            )
        )

        assert result.rules[0].matched is True
        assert result.rules[0].conditions[0].scope == "same_row"
        assert result.rules[0].positions == ((0, 0), (0, 2))


@pytest.mark.skipif(mongomock is None, reason="mongomock is required")
class TestEvidenceAliases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        disconnect_all()
        connect(
            "audit_evidence_aliases",
            host="mongodb://localhost",
            mongo_client_class=mongomock.MongoClient,
        )

    @classmethod
    def tearDownClass(cls):
        disconnect_all()

    def setUp(self):
        Report.drop_collection()
        Report._get_collection().insert_one(R31_RAW)

    def test_aliases_share_behavior(self):
        from mongoengine import Evidence

        queryset = Report.objects.collation(CI)
        result = next(iter(queryset.with_evidence([rule_a()])))
        assert isinstance(result, Evidence)
        assert result["甲"].matched is True
        assert result.rules["甲"].matched is True
        assert result.reasons[0].name == "甲"

    def test_rule_names_can_be_used_for_lookup(self):
        result = next(
            iter(
                Report.objects.collation(CI).evidence(
                    {"甲": rule_a(), "乙": rule_b(), "丙": rule_c()}
                )
            )
        )
        assert result["乙"].matched is True
        assert result["丙"].matched is False
        assert result.reason_text == "甲, 乙"

    def test_rule_specification_accepts_tuple_and_mapping_forms(self):
        mapping = next(
            iter(
                Report.objects.collation(CI).evidence(
                    [
                        ("甲", {"groups__readings__tag": "hold"}),
                        ("乙", {"groups__kind": "coldchain"}),
                    ],
                    combine="and",
                )
            )
        )
        assert [rule.matched for rule in mapping.rules] == [True, True]

    def test_same_row_condition_classmethod(self):
        rule = EvidenceRule.same_row(
            "乙",
            {
                "gs": {
                    "$elemMatch": {
                        "k": "coldchain",
                        "rs": {"$elemMatch": {"tg": "hold", "val": None}},
                    }
                }
            },
        )
        result = next(iter(Report.objects.collation(CI).evidence([rule])))
        assert result.rules[0].conditions[0].scope == "same_row"
        assert result.rules[0].positions == ((0, 0), (0, 2))


    def test_explicit_cross_row_is_not_narrowed_by_group_condition(self):
        rule = EvidenceRule(
            "cross",
            [
                EvidenceCondition.cross_row({"gs.rs.tg": "hold"}),
                EvidenceCondition.cross_row({"gs.k": "coldchain"}),
            ],
        )
        result = next(iter(Report.objects.collation(CI).evidence([rule])))
        assert result.rules[0].matched is True
        assert result.rules[0].conditions[0].positions == (
            (0, 0),
            (0, 2),
            (1, 0),
            (1, 1),
        )

    def test_db_field_aliases_are_used_in_evidence(self):
        rule = EvidenceRule("alias", {"gs.rs.tg": "hold"})
        result = next(iter(Report.objects.collation(CI).evidence([rule])))
        condition = result.rules[0].conditions[0]
        assert condition.field == "gs.rs.tg"
        assert condition.fields == ("gs.rs.tg",)
        assert condition.positions == ((0, 0), (0, 2), (1, 0), (1, 1))

    def test_nested_match_form_is_same_row(self):
        rules = [
            EvidenceRule(
                "乙",
                Q(
                    groups__match={
                        "kind": "coldchain",
                        "readings__match": {"tag": "hold", "value": None},
                    }
                ),
            ),
            EvidenceRule(
                "丙",
                Q(
                    groups__match={
                        "kind": "coldchain",
                        "readings__match": {"tag": "hold", "value": 0},
                    }
                ),
            ),
        ]
        result = next(iter(Report.objects.collation(CI).evidence(rules)))
        assert [rule.matched for rule in result.rules] == [True, False]
        assert result.rules[0].positions == ((0, 0), (0, 2))
        assert result.rules[0].conditions[0].kinds == ("missing", "null")
