"""
Tests for opt-in object graph queries and corrections.

The fixture mirrors a collector that keeps pending jobs in Python ``dict``
and ``list`` objects, where one job template is mounted at several sites,
review records reference details and jobs, and some references loop back.
"""

import pytest

from jsonpath_ng import CorrectionConflict
from jsonpath_ng import GraphQuery
from jsonpath_ng import correct_graph
from jsonpath_ng import find_graph
from jsonpath_ng import parse
from jsonpath_ng.exceptions import JSONPathError


@pytest.fixture()
def graph():
    rules = {"v": 1}
    detail = {"score": 7}
    review = {"item": detail, "links": []}
    task = {"items": [detail], "review": review, "rules": rules}
    review["links"].append(task)
    root = {
        "jobs": {"east": task, "west": task, "spare": task},
        "audit": review,
    }
    return {
        "root": root,
        "task": task,
        "review": review,
        "detail": detail,
        "rules": rules,
    }


def test_graph_query_is_opt_in_and_tree_behavior_is_unchanged():
    tree = {"jobs": {"east": {"score": 1}, "west": {"score": 2}}}
    # The standard engine is untouched and still returns DatumInContext.
    standard = parse("$..score").find(tree)
    assert [match.value for match in standard] == [1, 2]
    # The graph engine is a separate, explicitly enabled entry point.
    query = GraphQuery("$..score")
    assert isinstance(query, GraphQuery)
    assert [match.value for match in query.find(tree)] == [1, 2]


def test_find_lists_every_path_to_the_same_detail(graph):
    matches = find_graph("$..score", graph["root"])
    assert [str(match.path) for match in matches] == [
        "jobs.east.items.[0].score",
        "jobs.east.review.item.score",
        "jobs.west.items.[0].score",
        "jobs.west.review.item.score",
        "jobs.spare.items.[0].score",
        "jobs.spare.review.item.score",
        "audit.item.score",
        "audit.links.[0].items.[0].score",
    ]
    assert all(match.value == 7 for match in matches)
    # Every occurrence is the same underlying field of the same detail.
    assert all(match.chain[-1].parent is graph["detail"] for match in matches)
    assert all(not match.is_cycle for match in matches)


def test_find_accepts_ast_and_graph_query_objects(graph):
    expression = parse("$..score")
    via_ast = find_graph(expression, graph["root"])
    via_query = GraphQuery(expression).find(graph["root"])
    via_string = GraphQuery("$..score").find(graph["root"])
    expected = [
        "jobs.east.items.[0].score",
        "jobs.east.review.item.score",
        "jobs.west.items.[0].score",
        "jobs.west.review.item.score",
        "jobs.spare.items.[0].score",
        "jobs.spare.review.item.score",
        "audit.item.score",
        "audit.links.[0].items.[0].score",
    ]
    for result in (via_ast, via_query, via_string):
        assert [str(match.path) for match in result] == expected


def test_cycle_reports_link_and_target_and_does_not_expand(graph):
    matches = find_graph("$..links.[0]", graph["root"])
    cycles = [match for match in matches if match.is_cycle]
    assert [
        (match.cycle.link, match.cycle.target) for match in cycles
    ] == [
        ("jobs.east.review.links.[0]", "jobs.east"),
        ("jobs.west.review.links.[0]", "jobs.west"),
        ("jobs.spare.review.links.[0]", "jobs.spare"),
    ]
    # From the audit walk the link is initially a fresh reference ...
    audit_link = next(
        match
        for match in matches
        if str(match.path) == "audit.links.[0]"
    )
    assert not audit_link.is_cycle
    assert audit_link.value is graph["task"]
    # ... and its own back edge is reported when the review field is read.
    audit_review = find_graph("audit.links.[0].review", graph["root"])
    assert len(audit_review) == 1
    assert audit_review[0].is_cycle
    assert audit_review[0].cycle.link == "audit.links.[0].review"
    assert audit_review[0].cycle.target == "audit"


def test_audit_walk_reads_through_the_fresh_link(graph):
    matches = find_graph(
        "audit.links.[0].items.[0].score", graph["root"]
    )
    assert len(matches) == 1
    assert str(matches[0].path) == "audit.links.[0].items.[0].score"
    assert matches[0].value == 7


def test_recursive_descent_terminates_on_loops(graph):
    # Without cycle handling this would recurse forever.
    matches = find_graph("$..*", graph["root"])
    assert matches
    # Every reported back edge stops the walk; nothing is visited twice on
    # a single walk.
    for match in matches:
        if match.is_cycle:
            ids = {id(edge.child) for edge in match.chain[:-1]}
            assert id(match.value) in ids


def test_self_referential_list_cycle():
    loop = [1, 2]
    loop.append(loop)
    matches = find_graph("$..*", loop)
    values = [match.value for match in matches if not match.is_cycle]
    # The back edge is reported but its contents are not walked again.
    assert values == [1, 2]
    cycles = [match for match in matches if match.is_cycle]
    assert len(cycles) == 1
    assert cycles[0].cycle.link == "[2]"
    assert cycles[0].cycle.target == "$"


def test_interned_scalars_are_not_cycles():
    data = {"a": {"x": 1}, "b": {"x": 1}}
    matches = find_graph("$..*", data)
    assert not any(match.is_cycle for match in matches)
    assert [match.value for match in find_graph("$..x", data)] == [1, 1]


def test_correct_graph_produces_independent_mount_copies(graph):
    result = correct_graph(
        graph["root"],
        {
            "jobs.east.items.[0].score": 9,
            "jobs.west.review.item.score": 11,
        },
        mounts=GraphQuery("jobs.*"),
        query=GraphQuery("$..score"),
    )
    assert [str(item.mount.path) for item in result] == [
        "jobs.east",
        "jobs.west",
    ]
    east, west = result[0].value, result[1].value

    # Both paths in each station read the corrected value ...
    assert [match.value for match in find_graph("$..score", east)] == [9, 9]
    assert [match.value for match in find_graph("$..score", west)] == [11, 11]
    # ... and still converge on the same detail object.
    east_matches = find_graph("$..score", east)
    west_matches = find_graph("$..score", west)
    assert east_matches[0].chain[-1].parent is east_matches[1].chain[-1].parent
    assert west_matches[0].chain[-1].parent is west_matches[1].chain[-1].parent

    # The review links rewire onto each new task object.
    assert east["review"]["links"][0] is east
    assert west["review"]["links"][0] is west
    east_cycle = find_graph("$..links.[0]", east)[0]
    west_cycle = find_graph("$..links.[0]", west)[0]
    assert east_cycle.is_cycle and east_cycle.cycle.target == "$"
    assert west_cycle.is_cycle and west_cycle.cycle.target == "$"


def test_correction_preserves_shared_and_original_references(graph):
    root = graph["root"]
    result = correct_graph(
        root,
        {
            "jobs.east.items.[0].score": 9,
            "jobs.west.review.item.score": 11,
        },
        mounts=GraphQuery("jobs.*"),
        query=GraphQuery("$..score"),
    )
    east, west = result[0].value, result[1].value

    # The spare mount and the audit table keep the original objects.
    assert root["jobs"]["spare"] is graph["task"]
    assert root["audit"] is graph["review"]
    assert root["audit"]["links"][0] is graph["task"]
    # The original input is untouched.
    assert all(match.value == 7 for match in find_graph("$..score", root))
    # The shared rule table stays the same object everywhere.
    assert east["rules"] is graph["rules"]
    assert west["rules"] is graph["rules"]
    assert root["jobs"]["east"]["rules"] is graph["rules"]
    # Each branch gets its own review and detail copies.
    assert east["review"] is not graph["review"]
    assert west["review"] is not graph["review"]
    assert east["items"][0] is not graph["detail"]
    assert west["items"][0] is not graph["detail"]
    assert east is not west


def test_consistent_duplicate_hits_apply_once(graph):
    # Two paths to the same field requesting the same value are fine.
    result = correct_graph(
        graph["root"],
        {
            "jobs.east.items.[0].score": 9,
            "jobs.east.review.item.score": 9,
        },
        mounts=GraphQuery("jobs.*"),
        query=GraphQuery("$..score"),
    )
    assert len(result) == 1
    assert [
        match.value for match in find_graph("$..score", result[0].value)
    ] == [9, 9]


def test_conflicting_hits_reject_the_whole_commit(graph):
    with pytest.raises(CorrectionConflict) as exc_info:
        correct_graph(
            graph["root"],
            {
                "jobs.east.items.[0].score": 9,
                "jobs.east.review.item.score": 10,
            },
            mounts=GraphQuery("jobs.*"),
            query=GraphQuery("$..score"),
        )
    error = exc_info.value
    assert error.mount_path == "jobs.east"
    assert set(error.conflicting_paths) == {
        "jobs.east.items.[0].score",
        "jobs.east.review.item.score",
    }
    assert error.values == [9, 10]
    # No partial correction: the input is unchanged.
    assert all(
        match.value == 7 for match in find_graph("$..score", graph["root"])
    )
    assert graph["root"]["jobs"]["east"] is graph["task"]


def test_conflict_in_one_mount_blocks_the_other_mount(graph):
    with pytest.raises(CorrectionConflict):
        correct_graph(
            graph["root"],
            {
                "jobs.west.review.item.score": 11,
                "jobs.east.items.[0].score": 9,
                "jobs.east.review.item.score": 10,
            },
            mounts=GraphQuery("jobs.*"),
            query=GraphQuery("$..score"),
        )
    assert all(
        match.value == 7 for match in find_graph("$..score", graph["root"])
    )


def test_cycle_references_cannot_be_correction_targets(graph):
    with pytest.raises(JSONPathError, match="cycle reference"):
        correct_graph(
            graph["root"],
            {"jobs.east.review.links.[0]": "blocked"},
            mounts=GraphQuery("jobs.*"),
            query=GraphQuery("$..*"),
        )


def test_unknown_targets_and_mounts_are_rejected(graph):
    with pytest.raises(JSONPathError, match="did not match"):
        correct_graph(
            graph["root"],
            {"jobs.north.items.[0].score": 1},
            mounts=["jobs.east"],
            query=GraphQuery("$..score"),
        )
    with pytest.raises(JSONPathError, match="did not match"):
        correct_graph(
            graph["root"],
            {"jobs.east.items.[0].score": 1},
            mounts=["jobs.north"],
        )


def test_mounts_may_be_given_as_paths_or_matches(graph):
    query = GraphQuery("$..score")
    mount_match = GraphQuery("jobs.*").find(graph["root"])[0]
    from_paths = correct_graph(
        graph["root"],
        {"jobs.east.items.[0].score": 9},
        mounts=["jobs.east"],
        query=query,
    )
    from_matches = correct_graph(
        graph["root"],
        {"jobs.east.items.[0].score": 9},
        mounts=[mount_match],
        query=query,
    )
    assert from_paths[0].value["items"][0]["score"] == 9
    assert from_matches[0].value["items"][0]["score"] == 9


def test_target_outside_every_mount_is_rejected(graph):
    with pytest.raises(JSONPathError, match="outside every selected mount"):
        correct_graph(
            graph["root"],
            {"audit.item.score": 5},
            mounts=["jobs.east"],
            query=GraphQuery("$..score"),
        )


def test_mount_root_itself_cannot_be_a_target(graph):
    with pytest.raises(JSONPathError, match="outside every selected mount"):
        correct_graph(
            graph["root"],
            {"jobs.east": {"replaced": True}},
            mounts=["jobs.east"],
            query=GraphQuery("$..*"),
        )


def test_empty_corrections_return_nothing(graph):
    assert (
        correct_graph(
            graph["root"], {}, mounts=GraphQuery("jobs.*")
        )
        == []
    )


def test_corrections_accept_pairs_and_graph_match_targets(graph):
    target = next(
        match
        for match in GraphQuery("$..score").find(graph["root"])
        if str(match.path) == "jobs.east.items.[0].score"
    )
    result = correct_graph(
        graph["root"],
        [(target, 9)],
        mounts=GraphQuery("jobs.*"),
        query=GraphQuery("$..score"),
    )
    assert result[0].value["items"][0]["score"] == 9


@pytest.mark.parametrize(
    "expression",
    [
        "$",
        "$..score",
        "$..*",
        "$.*",
        "$[*]",
        "$..[0]",
        "jobs.east",
        "$.jobs..review",
        "jobs.east,jobs.west",
    ],
)
def test_graph_matches_tree_order_and_values_on_plain_trees(expression):
    tree = {
        "jobs": {
            "east": {"items": [{"score": 1}], "review": {"score": 2}},
            "west": {"items": [{"score": 3}], "review": {"score": 4}},
        }
    }
    standard = parse(expression).find(tree)
    graph_matches = find_graph(expression, tree)
    assert [match.value for match in graph_matches] == [
        match.value for match in standard
    ]
    assert [str(match.path) for match in graph_matches] == [
        str(match.full_path) for match in standard
    ]
    assert all(not match.is_cycle for match in graph_matches)


def test_where_and_parent_keep_graph_semantics(graph):
    where_matches = find_graph("jobs.* where review", graph["root"])
    assert {str(match.path) for match in where_matches} == {
        "jobs.east",
        "jobs.west",
        "jobs.spare",
    }
    parent_matches = find_graph("$..score.`parent`", graph["root"])
    assert len(parent_matches) == 8
    assert all(match.value is graph["detail"] for match in parent_matches)


def test_wherenot_is_distinct_from_where_on_graphs():
    from jsonpath_ng.jsonpath import Fields
    from jsonpath_ng.jsonpath import Where
    from jsonpath_ng.jsonpath import WhereNot
    from jsonpath_ng.jsonpath import Wildcard

    data = {
        "jobs": {
            "east": {"review": {}},
            "west": {"other": {}},
        }
    }
    left = Fields("jobs").child(Wildcard())
    where_expr = Where(left, Fields("review"))
    where_not_expr = WhereNot(left, Fields("review"))
    assert {
        str(match.path) for match in find_graph(where_expr, data)
    } == {"jobs.east"}
    assert {
        str(match.path) for match in find_graph(where_not_expr, data)
    } == {"jobs.west"}


def test_extended_filter_sees_every_walk(graph):
    from jsonpath_ng.ext import parse as ext_parse

    matches = GraphQuery(ext_parse("$..[?@.score]")).find(graph["root"])
    assert {str(match.path) for match in matches} == {
        "jobs.east.items.[0]",
        "jobs.east.review.item",
        "jobs.west.items.[0]",
        "jobs.west.review.item",
        "jobs.spare.items.[0]",
        "jobs.spare.review.item",
        "audit.item",
        "audit.links.[0].items.[0]",
    }
