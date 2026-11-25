import pandas as pd
import pytest

import pyranges as pr
from pyranges.collection_audit import (
    CollectionAuditor,
    CollectionObservation,
    CountSpec,
    audit_collection_records,
)


def example_auditor():
    auditor = CollectionAuditor(ring_length=23, bin_width=10, zero=0, query=(4, 7))
    auditor.add_observation(
        "example",
        readings=[21, 2, 22],
        directions=["forward", "reverse"],
        min_distances=[4, 3],
        max_distances=[27, 26],
    )
    auditor.add_count("example", 0, 2, exact=30)
    return auditor


def test_prompt_example_extrema_and_evidence():
    report = example_auditor().audit()

    frame = report.bins.set_index("bin_index")
    assert frame["min_meters"].tolist() == [2, 0, 0]
    assert frame["max_meters"].tolist() == [3, 0, 1]
    assert frame["is_tail"].tolist() == [False, False, True]

    tail_max = report.evidence("example", 2, "max")
    assert tail_max.value == 1
    assert tail_max.lengths == [4, 26]
    assert [(item["interval"], item["meters"]) for item in tail_max.intervals] == [(1, 3)]
    assert tail_max.intervals[0]["overlap_start"] == 4
    assert tail_max.intervals[0]["overlap_end"] == 7
    assert tail_max.bin_meters == [2, 0, 1]

    first_bin_min = report.evidence("example", 0, "min")
    assert first_bin_min.lengths == [4, 26]

    first_bin_max = report.evidence("example", 0, "max")
    assert first_bin_max.value == 3
    assert first_bin_max.lengths == [27, 3]
    assert [(item["interval"], item["meters"]) for item in first_bin_max.intervals] == [(0, 3)]


def test_added_counts_tighten_and_then_contradict():
    auditor = example_auditor()
    auditor.add_count("example", 0, 1, exact=27)
    report = auditor.audit()
    assert report.min_bins["example"] == [3, 0, 0]
    assert report.max_bins["example"] == [3, 0, 0]

    auditor.add_count("example", 1, 2, exact=26)
    report = auditor.audit()
    assert "example" in report.invalid_ids
    assert report.bins.empty
    assert "negative cycle" in report.invalid.iloc[0]["reason"]


def test_outside_query_count_still_constrains_result():
    observations = [
        CollectionObservation(
            observation_id="outside-count",
            ring_length=10,
            positions=[0, 1, 2],
            directions=[1, 1],
            min_distances=[1, 1],
            max_distances=[91, 91],
            counts=[CountSpec(0, 2, lower=2, upper=2)],
            zero=0,
            bin_width=10,
            query_start=0,
            query_end=1,
        )
    ]
    report = audit_collection_records(observations, bin_width=10, query=(0, 1))
    assert report.min_bins["outside-count"] == [1]
    assert report.max_bins["outside-count"] == [1]

    contradictory = CollectionObservation(
        observation_id="outside-count",
        ring_length=10,
        positions=[0, 1, 2],
        directions=[1, 1],
        min_distances=[11, 1],
        max_distances=[11, 1],
        counts=[CountSpec(0, 2, lower=2, upper=2)],
        zero=0,
        bin_width=10,
        query_start=0,
        query_end=1,
    )
    report = audit_collection_records([contradictory], bin_width=10, query=(0, 1))
    assert report.invalid_ids == ["outside-count"]


def test_reverse_zero_crossing_with_nonzero_grid_origin():
    obs = CollectionObservation(
        observation_id="reverse",
        ring_length=23,
        positions=[2, 22],
        directions=[-1],
        min_distances=[3],
        max_distances=[26],
        zero=0,
        bin_width=10,
        query_start=0,
        query_end=3,
    )
    report = audit_collection_records([obs], bin_width=10, query=(0, 3))
    assert report.min_bins["reverse"] == [2, 0, 1]
    assert report.max_bins["reverse"] == [2, 0, 1]
    evidence = report.evidence("reverse", 2)
    assert evidence.lengths in ([3], [26])
    assert evidence.bin_meters == [2, 0, 1]


def test_dataframe_ingestion_and_multiple_observations():
    intervals = pd.DataFrame(
        [
            {"observation_id": "a", "interval_index": 0, "start_position": 21, "end_position": 2, "direction": 1, "min_meters": 4, "max_meters": 27},
            {"observation_id": "a", "interval_index": 1, "start_position": 2, "end_position": 22, "direction": -1, "min_meters": 3, "max_meters": 26},
            {"observation_id": "b", "interval_index": 0, "start_position": 0, "end_position": 5, "direction": 1, "min_meters": 5, "max_meters": 5},
        ]
    )
    counts = pd.DataFrame(
        [{"observation_id": "a", "start": 0, "end": 2, "distance": 30}]
    )
    report = pr.audit_collection_records(
        intervals,
        counts,
        ring_length=23,
        bin_width=10,
        zero=0,
        query=(4, 7),
    )
    assert set(report.results) == {"a", "b"}
    assert report.max_bins["a"] == [3, 0, 1]
    assert report.min_bins["b"] == [1, 0, 0]


def test_thousands_of_intervals_do_not_expand_laps():
    n = 3000
    positions = [(index * 7) % 997 for index in range(n + 1)]
    directions = [1] * n
    minima = []
    maxima = []
    for index in range(n):
        residue = (positions[index + 1] - positions[index]) % 997
        minima.append(residue)
        maxima.append(residue + 997 * 5_000_000)
    obs = CollectionObservation(
        observation_id="large",
        ring_length=997,
        positions=positions,
        directions=directions,
        min_distances=minima,
        max_distances=maxima,
        counts=[],
        zero=0,
        bin_width=100,
        query_start=4,
        query_end=7,
    )
    report = audit_collection_records([obs], bin_width=100, query=(4, 7))
    result = report.results["large"]
    assert len(result.min_meters) == 10
    evidence = result.max_evidence[0]
    assert len(evidence.lengths) == n
    assert sum(item["meters"] for item in evidence.intervals) == 3
