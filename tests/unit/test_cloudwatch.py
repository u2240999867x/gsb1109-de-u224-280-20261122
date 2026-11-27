# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import random

import pytest

from botocore.cloudwatch import (
    BatchPlan,
    CloudWatchBatchingError,
    CloudWatchMetricBatcher,
    ItemRef,
)
from botocore.session import get_session
from botocore.stub import Stubber

START = "2026-11-28T00:00:00Z"
END = "2026-11-28T01:00:00Z"


def metric_stat(
    metric_name, period, unit="None", dimensions=None, account=None
):
    item = {
        "group": "shared",
        "name": metric_name,
        "metric_stat": {
            "Metric": {
                "Namespace": "ns",
                "MetricName": metric_name,
                "Dimensions": dimensions or [],
            },
            "Period": period,
            "Stat": "Average",
            "Unit": unit,
        },
    }
    if account is not None:
        item["account_id"] = account
    return item


def canonical_manifest():
    # Three 60-second metrics x, y, z plus 300-second variants of y and z.
    manifest = [
        metric_stat("x", 60),
        metric_stat("y", 60),
        metric_stat("z", 60),
        metric_stat("y300", 300),
        metric_stat("z300", 300),
        {
            "group": "shared",
            "name": "r",
            "expression": "x * 2",
            "label": "rate",
            "deliver": True,
        },
        {
            "group": "lineA",
            "name": "A",
            "label": "rate",
            "deliver": True,
            "expression": "shared:r + shared:y + shared:z",
        },
        {
            "group": "lineB",
            "name": "B",
            "label": "rate",
            "deliver": True,
            "expression": "shared:r + shared:y - shared:z",
        },
        {
            "group": "lineC",
            "name": "C",
            "label": "rate",
            "deliver": True,
            "expression": "shared:r + shared:y + shared:z300",
        },
        {
            "group": "lineD",
            "name": "D",
            "label": "rate",
            "deliver": True,
            "expression": "shared:r + shared:z + shared:y300",
        },
    ]
    return manifest


DELIVERY_REFS = [
    ItemRef("shared", "r"),
    ItemRef("lineA", "A"),
    ItemRef("lineB", "B"),
    ItemRef("lineC", "C"),
    ItemRef("lineD", "D"),
]


def plan_fingerprint(plan):
    # Order-independent description of who is delivered where, plus the exact
    # query contents, so two plans are equal only if grouping and ownership
    # match as well as the two counts.
    groups = frozenset(
        frozenset(str(ref) for ref in request.delivered_refs)
        for request in plan.requests
    )
    queries = frozenset(
        (
            request.index,
            frozenset(
                (
                    query.id,
                    query.kind,
                    query.return_data,
                    str(query.ref),
                    query.expression,
                )
                for query in request.queries
            ),
        )
        for request in plan.requests
    )
    return (
        groups,
        plan.request_count,
        plan.total_query_count,
        {str(ref): plan.delivery[ref] for ref in plan.delivery},
        queries,
    )


class TestCanonicalScenario:
    @pytest.mark.parametrize("limit,expected_total", [(7, 14), (8, 13)])
    def test_minimal_requests_and_counts(self, limit, expected_total):
        plan = CloudWatchMetricBatcher().build_plan(
            canonical_manifest(),
            start_time=START,
            end_time=END,
            max_queries_per_request=limit,
        )
        assert isinstance(plan, BatchPlan)
        assert plan.request_count == 2
        assert plan.total_query_count == expected_total
        for request in plan.requests:
            assert request.size <= limit

    def test_limit_eight_saves_one_support_item(self):
        seven = CloudWatchMetricBatcher().build_plan(
            canonical_manifest(),
            start_time=START,
            end_time=END,
            max_queries_per_request=7,
        )
        eight = CloudWatchMetricBatcher().build_plan(
            canonical_manifest(),
            start_time=START,
            end_time=END,
            max_queries_per_request=8,
        )
        assert seven.request_count == eight.request_count == 2
        assert seven.total_query_count - eight.total_query_count == 1

    @pytest.mark.parametrize("limit", [7, 8])
    def test_plan_is_independent_of_manifest_order(self, limit):
        batcher = CloudWatchMetricBatcher()
        expected = plan_fingerprint(
            batcher.build_plan(
                canonical_manifest(),
                start_time=START,
                end_time=END,
                max_queries_per_request=limit,
            )
        )
        shuffled = canonical_manifest()
        rng = random.Random(1234)
        for _ in range(25):
            rng.shuffle(shuffled)
            plan = batcher.build_plan(
                list(shuffled),
                start_time=START,
                end_time=END,
                max_queries_per_request=limit,
            )
            assert plan_fingerprint(plan) == expected

    def test_every_delivery_is_owned_by_exactly_one_request(self):
        plan = CloudWatchMetricBatcher().build_plan(
            canonical_manifest(),
            start_time=START,
            end_time=END,
            max_queries_per_request=7,
        )
        returned = [
            query
            for request in plan.requests
            for query in request.queries
            if query.return_data
        ]
        returned_refs = [str(query.ref) for query in returned]
        # Four formulas plus r, each returned a single time.
        assert sorted(returned_refs) == sorted(
            str(ref) for ref in DELIVERY_REFS
        )
        for ref in DELIVERY_REFS:
            request_index, query_id = plan.delivery[ref]
            assert plan.requests[request_index].query_ids.count(query_id) == 1

    def test_query_ids_unique_and_expressions_self_contained(self):
        plan = CloudWatchMetricBatcher().build_plan(
            canonical_manifest(),
            start_time=START,
            end_time=END,
            max_queries_per_request=7,
        )
        all_ids = []
        for request in plan.requests:
            local_ids = set(request.query_ids)
            assert len(local_ids) == len(request.query_ids)
            all_ids.extend(request.query_ids)
            for query in request.queries:
                if query.expression is None:
                    continue
                referenced = {
                    token
                    for token in query.expression.replace("*", " ").split()
                    if token.startswith("q")
                }
                # Every dependency must resolve to a query in the same request.
                assert referenced <= local_ids
        assert len(all_ids) == len(set(all_ids))

    def test_r_runs_twice_but_is_returned_once(self):
        plan = CloudWatchMetricBatcher().build_plan(
            canonical_manifest(),
            start_time=START,
            end_time=END,
            max_queries_per_request=7,
        )
        r_queries = [
            (request.index, query)
            for request in plan.requests
            for query in request.queries
            if query.ref == ItemRef("shared", "r")
        ]
        # r is a support formula in both requests and keeps its full
        # dependency (x) in each.
        assert len(r_queries) == 2
        for request_index, query in r_queries:
            assert query.kind == "formula"
            request = plan.requests[request_index]
            assert any(
                support.ref == ItemRef("shared", "x")
                for support in request.queries
            )
        returning = [index for index, query in r_queries if query.return_data]
        assert returning == [plan.request_delivering(ItemRef("shared", "r"))]
        assert len(returning) == 1

    def test_each_request_carries_full_formula_dependencies(self):
        plan = CloudWatchMetricBatcher().build_plan(
            canonical_manifest(),
            start_time=START,
            end_time=END,
            max_queries_per_request=7,
        )
        expected_support = {
            "lineA": {"shared:r", "shared:x", "shared:y", "shared:z"},
            "lineB": {"shared:r", "shared:x", "shared:y", "shared:z"},
            "lineC": {"shared:r", "shared:x", "shared:y", "shared:z300"},
            "lineD": {"shared:r", "shared:x", "shared:z", "shared:y300"},
        }
        for request in plan.requests:
            present = {str(query.ref) for query in request.queries}
            for ref in request.delivered_refs:
                if ref.group in expected_support:
                    assert expected_support[ref.group] <= present
                    assert str(ref) in present


class TestSupportSharingAndIntermediates:
    def _metric(self, item_name, metric_name, **overrides):
        item = metric_stat(metric_name, 60, **overrides)
        item["name"] = item_name
        return item

    def test_identical_fetching_meaning_is_shared(self):
        manifest = [
            self._metric("first", "cpu"),
            self._metric("second", "cpu"),
            {
                "group": "line",
                "name": "f",
                "expression": "shared:first + shared:second",
                "deliver": True,
            },
        ]
        plan = CloudWatchMetricBatcher().build_plan(
            manifest,
            start_time=START,
            end_time=END,
            max_queries_per_request=50,
        )
        refs = {
            str(query.ref)
            for request in plan.requests
            for query in request.queries
        }
        # The two aliases collapse to a single metric query.
        assert refs == {"shared:first", "line:f"}
        formula = next(
            query
            for request in plan.requests
            for query in request.queries
            if query.ref == ItemRef("line", "f")
        )
        shared_id = next(
            query.id
            for request in plan.requests
            for query in request.queries
            if query.ref == ItemRef("shared", "first")
        )
        assert formula.expression.count(shared_id) == 2

    @pytest.mark.parametrize(
        "change",
        [
            lambda item: item["metric_stat"].__setitem__("Period", 300),
            lambda item: item["metric_stat"].__setitem__("Unit", "Bytes"),
            lambda item: item["metric_stat"].__setitem__("Stat", "Sum"),
            lambda item: item["metric_stat"]["Metric"].__setitem__(
                "Namespace", "other"
            ),
            lambda item: item["metric_stat"]["Metric"].__setitem__(
                "MetricName", "other"
            ),
            lambda item: item["metric_stat"]["Metric"].__setitem__(
                "Dimensions", [{"Name": "d", "Value": "v"}]
            ),
            lambda item: item.__setitem__("account_id", "other-account"),
        ],
    )
    def test_different_meaning_is_not_shared(self, change):
        first = self._metric("first", "cpu")
        second = self._metric("second", "cpu")
        change(second)
        manifest = [
            first,
            second,
            {
                "group": "line",
                "name": "f",
                "expression": "shared:first + shared:second",
                "deliver": True,
            },
        ]
        plan = CloudWatchMetricBatcher().build_plan(
            manifest,
            start_time=START,
            end_time=END,
            max_queries_per_request=50,
        )
        refs = {
            str(query.ref)
            for request in plan.requests
            for query in request.queries
        }
        assert {"shared:first", "shared:second", "line:f"} <= refs

    def test_intermediate_formula_is_a_real_support_query(self):
        manifest = [
            self._metric("x", "x"),
            {"group": "shared", "name": "s", "expression": "x * 3"},
            {
                "group": "shared",
                "name": "t",
                "expression": "s + 1",
                "label": "rate",
                "deliver": True,
            },
        ]
        plan = CloudWatchMetricBatcher().build_plan(
            manifest,
            start_time=START,
            end_time=END,
            max_queries_per_request=7,
        )
        request = plan.requests[0]
        by_ref = {str(query.ref): query for query in request.queries}
        assert set(by_ref) == {"shared:x", "shared:s", "shared:t"}
        assert by_ref["shared:s"].return_data is False
        assert by_ref["shared:t"].return_data is True
        assert (
            by_ref["shared:t"].expression.split()[0] == by_ref["shared:s"].id
        )
        assert by_ref["shared:s"].expression.startswith(by_ref["shared:x"].id)

    def test_delivered_intermediate_still_usable_as_support_elsewhere(self):
        # s is delivered in its own right and supports two formulas, t and u.
        # The limit keeps t and u in separate requests, so s is executed in
        # both (returned once) and locally referenceable as a support in each.
        manifest = [
            self._metric("x", "x"),
            {
                "group": "shared",
                "name": "s",
                "expression": "x * 3",
                "deliver": True,
            },
            {
                "group": "line",
                "name": "t",
                "expression": "shared:s + 1",
                "deliver": True,
            },
            {
                "group": "line",
                "name": "u",
                "expression": "shared:s + 2",
                "deliver": True,
            },
        ]
        plan = CloudWatchMetricBatcher().build_plan(
            manifest,
            start_time=START,
            end_time=END,
            max_queries_per_request=3,
        )
        assert plan.request_count == 2
        s_queries = [
            query
            for request in plan.requests
            for query in request.queries
            if query.ref == ItemRef("shared", "s")
        ]
        assert len(s_queries) == 2
        assert sum(query.return_data for query in s_queries) == 1
        for request in plan.requests:
            local = {query.ref: query.id for query in request.queries}
            assert ItemRef("shared", "s") in local
            for dependent in (ItemRef("line", "t"), ItemRef("line", "u")):
                dependent_query = next(
                    (
                        query
                        for query in request.queries
                        if query.ref == dependent
                    ),
                    None,
                )
                if dependent_query is not None:
                    assert local[ItemRef("shared", "s")] in (
                        dependent_query.expression
                    )


class TestPlanningErrors:
    def _build(self, manifest, limit=7):
        return CloudWatchMetricBatcher().build_plan(
            manifest,
            start_time=START,
            end_time=END,
            max_queries_per_request=limit,
        )

    def _expect_issue(self, manifest, kind, limit=7):
        with pytest.raises(CloudWatchBatchingError) as exc_info:
            self._build(manifest, limit=limit)
        issue = exc_info.value.issue
        assert issue.kind == kind
        return issue

    def test_ambiguous_bare_reference(self):
        manifest = [
            metric_stat("x", 60),
            {
                "group": "other",
                "name": "x",
                "metric_stat": {
                    "Metric": {"Namespace": "ns", "MetricName": "x"},
                    "Period": 60,
                    "Stat": "Average",
                },
            },
            {
                "group": "third",
                "name": "f",
                "expression": "x + 1",
                "deliver": True,
            },
        ]
        issue = self._expect_issue(manifest, "AmbiguousReference")
        assert issue.group == "third"
        assert issue.name == "f"
        assert issue.token == "x"

    def test_cycle_is_located(self):
        manifest = [
            {
                "group": "g",
                "name": "p",
                "expression": "g:q + 1",
                "deliver": True,
            },
            {
                "group": "g",
                "name": "q",
                "expression": "g:p + 1",
                "deliver": True,
            },
        ]
        issue = self._expect_issue(manifest, "CyclicReference")
        assert issue.name in {"p", "q"}

    def test_unknown_cross_group_reference(self):
        manifest = [
            {
                "group": "g",
                "name": "f",
                "expression": "missing:thing + 1",
                "deliver": True,
            }
        ]
        issue = self._expect_issue(manifest, "UnknownReference")
        assert issue.token == "missing:thing"

    def test_unsupported_expression_token(self):
        manifest = [
            metric_stat("x", 60),
            {
                "group": "shared",
                "name": "f",
                "expression": "x % 2",
                "deliver": True,
            },
        ]
        issue = self._expect_issue(manifest, "InvalidExpression")
        assert issue.token == "%"

    def test_delivery_alone_over_limit(self):
        # A's closure needs five queries; a limit of four cannot hold it even
        # in a dedicated request.
        issue = self._expect_issue(
            canonical_manifest(), "QueryLimitExceeded", limit=4
        )
        assert issue.name == "A"

    def test_duplicate_name_in_group(self):
        manifest = [metric_stat("x", 60), metric_stat("x", 60)]
        issue = self._expect_issue(manifest, "DuplicateItem")
        assert issue.group == "shared"
        assert issue.name == "x"

    def test_manifest_entry_must_have_one_query_kind(self):
        manifest = [
            {
                "group": "g",
                "name": "f",
                "expression": "1",
                "metric_stat": {},
            }
        ]
        issue = self._expect_issue(manifest, "InvalidManifest")
        assert issue.group == "g"


class TestExecutionAndReconciliation:
    def _recording_client(self, plan):
        sent = []

        def get_metric_data(**kwargs):
            sent.append(kwargs)
            ids = [query["Id"] for query in kwargs["MetricDataQueries"]]
            # The service is free to reorder the result packets; shuffle them.
            random.Random(len(sent)).shuffle(ids)
            return {
                "MetricDataResults": [
                    {
                        "Id": query_id,
                        "Label": "rate",
                        "Timestamps": [START],
                        "Values": [query_id],
                        "StatusCode": "Complete",
                    }
                    for query_id in ids
                ]
            }

        class FakeClient:
            def get_metric_data(self, **kwargs):
                return get_metric_data(**kwargs)

        return FakeClient(), sent

    def test_same_label_results_return_to_their_lines(self):
        batcher = CloudWatchMetricBatcher()
        plan = batcher.build_plan(
            canonical_manifest(),
            start_time=START,
            end_time=END,
            max_queries_per_request=8,
            ScanBy="TimestampDescending",
        )
        client, sent = self._recording_client(plan)
        execution = batcher.execute_plan(plan, client=client)

        assert len(sent) == plan.request_count
        for kwargs, request in zip(sent, plan.requests):
            assert kwargs["StartTime"] == START
            assert kwargs["EndTime"] == END
            assert kwargs["ScanBy"] == "TimestampDescending"
            assert [
                query["Id"] for query in kwargs["MetricDataQueries"]
            ] == request.query_ids

        for ref in DELIVERY_REFS:
            result = execution.result_for(ref.group, ref.name)
            assert result.label == "rate"
            assert result.result["Values"] == [result.query_id]
            assert result.request_index == plan.request_delivering(
                ref
            ) and result.query_id == plan.query_id_delivering(ref)

        r_result = execution.result_for("shared", "r")
        delivering_request = plan.request_delivering(ItemRef("shared", "r"))
        assert r_result.request_index == delivering_request

        lines = execution.line_results(
            {
                "lineA": ["A"],
                "lineB": ["B"],
                "lineC": ["C"],
                "lineD": ["D"],
            }
        )
        assert {
            group: [result.ref.name for result in results]
            for group, results in lines.items()
        } == {
            "lineA": ["A"],
            "lineB": ["B"],
            "lineC": ["C"],
            "lineD": ["D"],
        }

    def test_generated_request_validates_against_service_model(self):
        manifest = [
            metric_stat(
                "x",
                60,
                dimensions=[{"Name": "InstanceId", "Value": "i-1"}],
                account="111122223333",
            ),
            {"group": "shared", "name": "r", "expression": "x * 2"},
            {
                "group": "line",
                "name": "A",
                "expression": "shared:r + 1",
                "label": "rate",
                "deliver": True,
            },
        ]
        batcher = CloudWatchMetricBatcher()
        plan = batcher.build_plan(
            manifest,
            start_time=START,
            end_time=END,
            max_queries_per_request=7,
        )
        client = get_session().create_client(
            "cloudwatch",
            region_name="us-east-1",
            aws_access_key_id="k",
            aws_secret_access_key="s",
        )
        stubber = Stubber(client)
        request = plan.requests[0]
        delivered_id = plan.query_id_delivering(ItemRef("line", "A"))
        stubber.add_response(
            "get_metric_data",
            {
                "MetricDataResults": [
                    {
                        "Id": delivered_id,
                        "Label": "rate",
                        "Timestamps": [],
                        "Values": [],
                        "StatusCode": "Complete",
                    }
                ]
            },
            expected_params=request.to_client_kwargs(START, END),
        )
        stubber.activate()
        execution = batcher.execute_plan(plan, client=client)
        stubber.assert_no_pending_responses()
        assert execution.result_for("line", "A").query_id == delivered_id

    def test_collect_builds_and_executes_in_one_call(self):
        batcher = CloudWatchMetricBatcher()
        client, _ = self._recording_client(None)
        execution = batcher.collect(
            canonical_manifest(),
            start_time=START,
            end_time=END,
            client=client,
            max_queries_per_request=8,
        )
        assert len(execution.results) == len(DELIVERY_REFS)

    def test_executing_without_a_client_raises(self):
        plan = CloudWatchMetricBatcher().build_plan(
            canonical_manifest(),
            start_time=START,
            end_time=END,
            max_queries_per_request=8,
        )
        with pytest.raises(ValueError):
            CloudWatchMetricBatcher().execute_plan(plan)
