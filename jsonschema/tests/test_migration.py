import itertools
import random
import string

import pytest

from jsonschema.migration import (
    Contract,
    NameDomain,
    RenamePlan,
    UnsupportedPattern,
    ValueRule,
    _CompiledContract,
    accepts_row,
    check_migration,
)

ALPHABET = string.ascii_letters + string.digits + "-"
DOMAIN = NameDomain(ALPHABET, 4)
PLAN = RenamePlan(
    prefix_rewrites=(("B-", "D-"),),
    case_normalization="lower",
)


def old_contract_v1():
    return {
        "properties": {"A-01": {"type": "string"}},
        "patternProperties": {
            "^[A-Za-z]-[0-9]{2}$": {"type": "string"},
            "^[Aa]-[0-9]{2}$": {"enum": ["ok", "ng"]},
            "^B-[0-9][0-9]$": {"enum": ["x", "y"]},
        },
        "additionalProperties": False,
    }


def old_contract_v2():
    return {
        "patternProperties": {
            "^[A-C]-[0-9]{2}$": {"type": "string"},
            "^A-[0-9]{2}$": {"enum": ["ok", "ng"]},
            "^B-[0-9][0-9]$": {"enum": ["x", "y"]},
        },
        "additionalProperties": False,
    }


def new_contract():
    return {
        "properties": {"a-01": {"type": "string"}},
        "patternProperties": {
            "^[acd]-[0-9]{2}$": {"type": "string"},
        },
        "additionalProperties": False,
    }


def test_colliding_plan_is_rejected_with_merge_witnesses():
    report = check_migration(old_contract_v1(), new_contract(), PLAN, DOMAIN)
    assert report.verdict == "collision"
    assert not report.faithful

    merges = {c.target: c for c in report.collisions}
    assert "a-01" in merges
    merge = merges["a-01"]
    assert set(merge.sources) == {"A-01", "a-01"}

    # the merge is reviewable: both source names may appear together in
    # one original row accepted by the old contract
    assert merge.cooccurs
    assert accepts_row(old_contract_v1(), merge.source_row, DOMAIN)
    assert set(merge.source_row) == {"A-01", "a-01"}

    # after renaming, the two old columns collapse onto a single name
    assert list(merge.renamed_row) == ["a-01"]

    # the prefix rewrite can merge columns across rewrite classes too
    assert any(c.target.startswith("d-") for c in report.collisions)


def test_fixed_plan_is_faithful_with_reverse_witnesses():
    report = check_migration(old_contract_v2(), new_contract(), PLAN, DOMAIN)
    assert report.verdict == "faithful"
    assert report.faithful
    assert report.forward_witnesses == []
    assert report.collisions == []

    # the rename lands columns from a two-rule position (type + enum)
    # onto a one-rule position (type only), so the target accepts more
    assert report.reverse_witnesses
    for witness in report.reverse_witnesses:
        assert witness.kind == "target_only_value"
        assert accepts_row(new_contract(), witness.target_row, DOMAIN)
    by_name = {w.target_name: w for w in report.reverse_witnesses}
    assert "a-01" in by_name
    witness = by_name["a-01"]
    assert not accepts_row(
        old_contract_v2(),
        {"A-01": witness.value},
        DOMAIN,
    )

    # the conclusion covers the whole declared domain, not a sample
    assert str(DOMAIN.size()) in report.coverage
    assert "finite automata" in report.coverage


def test_prefix_rewrite_from_two_rules_to_one_rule():
    old = {
        "patternProperties": {
            "^B-[0-9]{2}$": {"type": "string"},
            "^B-[0-9][0-9]$": {"enum": ["x", "y"]},
        },
        "additionalProperties": False,
    }
    new = {
        "patternProperties": {"^d-[0-9]{2}$": {"type": "string"}},
        "additionalProperties": False,
    }
    report = check_migration(old, new, PLAN, DOMAIN)
    assert report.verdict == "faithful"
    assert report.forward_witnesses == []

    [witness] = report.reverse_witnesses
    assert witness.kind == "target_only_value"
    assert witness.target_name.startswith("d-")
    assert witness.value not in {"x", "y"}
    assert accepts_row(new, witness.target_row, DOMAIN)
    source_name = "B-" + witness.target_name[2:]
    assert not accepts_row(old, {source_name: witness.value}, DOMAIN)


def test_difference_only_at_max_length_forward():
    old = {
        "patternProperties": {"^E-[0-9]{1,2}$": {"type": "string"}},
        "additionalProperties": False,
    }
    new = {
        "patternProperties": {"^e-[0-9]$": {"type": "string"}},
        "additionalProperties": False,
    }
    plan = RenamePlan(case_normalization="lower")
    report = check_migration(old, new, plan, DOMAIN)
    assert report.verdict == "unfaithful"

    [witness] = report.forward_witnesses
    assert witness.kind == "name_not_accepted"
    [name] = witness.source_row
    # the divergence only shows up on names reaching the length cap
    assert len(name) == DOMAIN.max_length
    assert accepts_row(old, witness.source_row, DOMAIN)
    renamed = {plan.rename(name): witness.source_row[name]}
    assert not accepts_row(new, renamed, DOMAIN)


def test_difference_only_at_max_length_reverse():
    old = {
        "patternProperties": {"^F-[0-9]$": {"type": "string"}},
        "additionalProperties": False,
    }
    new = {
        "patternProperties": {"^f-[0-9]{1,2}$": {"type": "string"}},
        "additionalProperties": False,
    }
    plan = RenamePlan(case_normalization="lower")
    report = check_migration(old, new, plan, DOMAIN)
    assert report.verdict == "faithful"
    assert report.forward_witnesses == []

    [witness] = report.reverse_witnesses
    assert witness.kind == "target_only_name"
    assert len(witness.target_name) == DOMAIN.max_length
    assert accepts_row(new, witness.target_row, DOMAIN)


def test_value_failure_forward_witness():
    old = {
        "patternProperties": {"^G-[0-9]{2}$": {"type": "string"}},
        "additionalProperties": False,
    }
    new = {
        "patternProperties": {"^g-[0-9]{2}$": {"enum": ["ok"]}},
        "additionalProperties": False,
    }
    plan = RenamePlan(case_normalization="lower")
    report = check_migration(old, new, plan, DOMAIN)
    assert report.verdict == "unfaithful"

    [witness] = report.forward_witnesses
    assert witness.kind == "value_not_accepted"
    assert accepts_row(old, witness.source_row, DOMAIN)
    renamed = {
        plan.rename(name): value for name, value in witness.source_row.items()
    }
    assert not accepts_row(new, renamed, DOMAIN)


@pytest.mark.parametrize(
    "pattern",
    [
        "^E-\\d{2}$",
        "^E-..$",
        "^E-[^0-9]$",
        "^E-[0-9]{2}$|^F-$",
        "E-^01",
    ],
)
def test_unsupported_patterns_are_unknown(pattern):
    old = {
        "patternProperties": {pattern: {"type": "string"}},
        "additionalProperties": False,
    }
    report = check_migration(old, new_contract(), PLAN, DOMAIN)
    assert report.verdict == "unknown"
    assert pattern in report.unknown_patterns


def test_case_insensitive_policy_merges_case_variants():
    old = {
        "properties": {"A-01": {"type": "string"}},
        "additionalProperties": False,
    }
    new = {
        "properties": {"a-01": {"type": "string"}},
        "additionalProperties": False,
    }
    plan = RenamePlan(case_normalization="lower")

    assert accepts_row(old, {"a-01": "x"}, DOMAIN, "insensitive")
    assert not accepts_row(old, {"a-01": "x"}, DOMAIN, "sensitive")

    report = check_migration(old, new, plan, DOMAIN, "insensitive")
    assert report.verdict == "collision"
    [merge] = report.collisions
    assert set(merge.sources) == {"A-01", "a-01"}
    assert merge.target == "a-01"
    assert merge.cooccurs

    report = check_migration(old, new, plan, DOMAIN, "sensitive")
    assert report.verdict == "faithful"
    assert report.reverse_witnesses == []


def test_rename_plan_longest_prefix_wins():
    plan = RenamePlan(
        prefix_rewrites=(("A-", "X-"), ("A-0", "Y-")),
        case_normalization="lower",
    )
    assert plan.rename("A-01") == "y-1"
    assert plan.rename("A-11") == "x-11"
    assert plan.rename("B-01") == "b-01"


def test_rename_plan_rejects_bad_input():
    with pytest.raises(ValueError, match="non-empty"):
        RenamePlan(prefix_rewrites=(("", "X-"),))
    with pytest.raises(ValueError, match="duplicate"):
        RenamePlan(prefix_rewrites=(("A-", "X-"), ("A-", "Y-")))
    with pytest.raises(ValueError, match="case_normalization"):
        RenamePlan(case_normalization="shuffle")


def test_mapping_specs_are_accepted():
    report = check_migration(
        old_contract_v2(),
        new_contract(),
        {"prefix_rewrites": {"B-": "D-"}, "case_normalization": "lower"},
        {"alphabet": ALPHABET, "max_length": 4},
    )
    assert report.verdict == "faithful"


def test_contract_objects_are_accepted():
    report = check_migration(
        Contract.from_schema(old_contract_v2()),
        Contract.from_schema(new_contract()),
        PLAN,
        DOMAIN,
    )
    assert report.verdict == "faithful"


def test_value_rule_meet_and_difference():
    strings = ValueRule(types=["string"])
    enum = ValueRule(enum=["ok", "ng"])
    meet = strings.meet(enum)
    assert meet.is_satisfiable()
    assert meet.witness() in {"ok", "ng"}
    assert meet.difference_witness(strings) is None
    assert strings.difference_witness(meet) == ""

    empty = ValueRule(types=["integer"]).meet(enum)
    assert not empty.is_satisfiable()


def test_report_summary_and_to_dict():
    report = check_migration(old_contract_v2(), new_contract(), PLAN, DOMAIN)
    summary = report.summary()
    assert "faithful" in summary
    as_dict = report.to_dict()
    assert as_dict["verdict"] == "faithful"
    assert as_dict["domain"]["max_length"] == 4


def test_accepts_row_rejects_unsupported_patterns():
    old = {
        "patternProperties": {"^E-\\d{2}$": {"type": "string"}},
        "additionalProperties": False,
    }
    with pytest.raises(UnsupportedPattern):
        accepts_row(old, {"E-01": "x"}, DOMAIN)


TINY = NameDomain("aA0-", 3)
TINY_VALUES = ["", "x", "ok", 0, 1, 5, 0.5, None, True, [], {}]
TINY_NAMES = [
    "".join(chars)
    for length in range(TINY.max_length + 1)
    for chars in itertools.product("aA0-", repeat=length)
]
TINY_PATTERNS = [
    "^[A-Za-z]-[0-9]$",
    "^[Aa]-[0-9]$",
    "^A[0-9]$",
    "^[A-]{2,3}$",
    "^a[0-9]$",
    "^[Aa][0-9]?$",
    "^[0-9][Aa]$",
    "A",
    "[0-9]$",
]
TINY_RULES = [
    {"type": "string"},
    {"type": "integer"},
    {"enum": ["ok", "x"]},
    {"enum": [0, 1]},
    {"type": ["string", "null"]},
    {},
]


def tiny_contract(rng):
    contract = {"additionalProperties": False}
    if rng.random() < 0.6:
        contract["properties"] = {
            rng.choice(["A-0", "a-0", "A0", "-0"]): rng.choice(TINY_RULES)
            for _ in range(rng.randint(1, 2))
        }
    if rng.random() < 0.9:
        contract["patternProperties"] = {
            rng.choice(TINY_PATTERNS): rng.choice(TINY_RULES)
            for _ in range(rng.randint(1, 3))
        }
    return contract


def tiny_plan(rng):
    rewrites = [
        (prefix, rng.choice(["a", "A0", "x-", "", "0"]))
        for prefix in rng.sample(["A", "a", "A-", "0", "-"], rng.randint(0, 2))
    ]
    return RenamePlan(
        prefix_rewrites=tuple(rewrites),
        case_normalization=rng.choice([None, "lower", "upper"]),
    )


def brute_force(old, new, plan):
    old_c = _CompiledContract(Contract.from_schema(old), TINY, "sensitive")
    new_c = _CompiledContract(Contract.from_schema(new), TINY, "sensitive")
    failures = []
    for name in TINY_NAMES:
        for value in TINY_VALUES:
            if not old_c.accepts_row({name: value}):
                continue
            if not new_c.accepts_row({plan.rename(name): value}):
                failures.append(name)
                break
    usable = [
        name
        for name in TINY_NAMES
        if any(old_c.accepts_row({name: v}) for v in TINY_VALUES)
    ]
    merges = [
        (first, second)
        for first, second in itertools.combinations(usable, 2)
        if plan.rename(first) == plan.rename(second)
    ]
    reverse = []
    for target in TINY_NAMES:
        preimages = [n for n in TINY_NAMES if plan.rename(n) == target]
        for value in TINY_VALUES:
            if not new_c.accepts_row({target: value}):
                continue
            if not any(old_c.accepts_row({n: value}) for n in preimages):
                reverse.append((target, value))
                break
    return failures, merges, reverse


def test_symbolic_verdicts_match_exhaustive_enumeration():
    # on a tiny domain the automata-based verdicts must agree with
    # brute-force enumeration of every name, value and name pair
    rng = random.Random(20261127)
    verdicts = set()
    for _ in range(60):
        old = tiny_contract(rng)
        new = tiny_contract(rng)
        plan = tiny_plan(rng)
        report = check_migration(old, new, plan, TINY)
        failures, merges, reverse = brute_force(old, new, plan)
        verdicts.add(report.verdict)
        assert bool(merges) == (report.verdict == "collision")
        if report.verdict == "collision":
            continue
        assert bool(failures) == (report.verdict == "unfaithful")
        assert bool(reverse) == bool(report.reverse_witnesses)
    assert verdicts == {"collision", "unfaithful", "faithful"}
