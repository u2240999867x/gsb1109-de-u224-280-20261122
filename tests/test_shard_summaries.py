from __future__ import annotations

import pathlib

import pytest

import pystac
from pystac import (
    NOT_APPLICABLE,
    UNKNOWN,
    Collection,
    Extent,
    FullShardSummary,
    ShardCorrection,
    ShardCorrectionEntry,
    ShardSummaries,
    SpatialExtent,
    Summaries,
    TemporalExtent,
)

PROVIDER = "vendor"
PARTITION_P = "P"
PARTITION_Q = "Q"
THRESHOLD = 50


def make_collection(shard_summaries: ShardSummaries | None = None) -> Collection:
    return Collection(
        id="test-collection",
        description="test",
        extent=Extent(
            spatial=SpatialExtent([-90, -180, 90, 180]),
            temporal=TemporalExtent([None, None]),
        ),
        summaries=Summaries({"platform": ["A", "B", "C"]}),
        shard_summaries=shard_summaries,
    )


def full_summary_10() -> FullShardSummary:
    return FullShardSummary(
        PROVIDER,
        PARTITION_P,
        10,
        [("A", 90), ("A", 8), ("A", UNKNOWN)],
    )


def correction_11() -> ShardCorrection:
    return ShardCorrection(
        PROVIDER,
        PARTITION_P,
        11,
        [ShardCorrectionEntry("A", 90, "B", 90)],
    )


def correction_12_withdraw() -> ShardCorrection:
    return ShardCorrection(
        PROVIDER,
        PARTITION_P,
        12,
        [ShardCorrectionEntry.withdraw("A", 8)],
    )


def correction_12_reclassify() -> ShardCorrection:
    return ShardCorrection(
        PROVIDER,
        PARTITION_P,
        12,
        [ShardCorrectionEntry("A", 8, "A", 70)],
    )


def correction_13() -> ShardCorrection:
    return ShardCorrection(
        PROVIDER,
        PARTITION_P,
        13,
        [ShardCorrectionEntry("A", UNKNOWN, "A", NOT_APPLICABLE)],
    )


def full_summary_12_vendor() -> FullShardSummary:
    # Vendor confirms the withdrawal of the cloud-8 record.
    return FullShardSummary(
        PROVIDER,
        PARTITION_P,
        12,
        [("A", UNKNOWN), ("B", 90)],
    )


def full_summary_q() -> FullShardSummary:
    return FullShardSummary(PROVIDER, PARTITION_Q, 1, [("C", 5)])


def build_replica_1() -> ShardSummaries:
    summaries = ShardSummaries()
    summaries.register_partition(PROVIDER, PARTITION_P, ["A", "B"])
    summaries.register_partition(PROVIDER, PARTITION_Q, ["C"])
    summaries.apply_full(full_summary_q())
    summaries.apply_full(full_summary_10())
    summaries.apply_correction(correction_12_withdraw())
    summaries.apply_correction(correction_12_reclassify())
    return summaries


def build_replica_2() -> ShardSummaries:
    summaries = ShardSummaries()
    summaries.register_partition(PROVIDER, PARTITION_P, ["A", "B"])
    summaries.register_partition(PROVIDER, PARTITION_Q, ["C"])
    summaries.apply_full(full_summary_q())
    summaries.apply_full(full_summary_10())
    summaries.apply_correction(correction_13())
    return summaries


def test_cloud_cover_state_validation() -> None:
    with pytest.raises(ValueError):
        ShardCorrectionEntry("A", 101, "A", 50)
    with pytest.raises(ValueError):
        ShardCorrectionEntry("A", -1, "A", 50)
    with pytest.raises(ValueError):
        ShardCorrectionEntry("A", "cloudy", "A", 50)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        ShardCorrectionEntry("A", True, "A", 50)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        ShardCorrectionEntry("A", 8, None, 50)
    entry = ShardCorrectionEntry.withdraw("A", 8)
    assert entry.is_withdrawal
    assert entry.to_dict() == {
        "before_device": "A",
        "before_state": 8,
    }


def test_unregistered_partition_and_device_rejected() -> None:
    summaries = ShardSummaries()
    with pytest.raises(KeyError):
        summaries.apply_full(full_summary_10())
    summaries.register_partition(PROVIDER, PARTITION_P, ["A", "B"])
    foreign = FullShardSummary(PROVIDER, PARTITION_P, 1, [("Z", 5)])
    with pytest.raises(ValueError):
        summaries.apply_full(foreign)
    with pytest.raises(ValueError):
        summaries.apply_correction(
            ShardCorrection(
                PROVIDER,
                PARTITION_P,
                2,
                [ShardCorrectionEntry("A", 5, "Z", 5)],
            )
        )


def test_conflict_and_gap_block_partition_but_not_others() -> None:
    replica_1 = build_replica_1()
    replica_2 = build_replica_2()

    assert not replica_1.is_partition_usable(PROVIDER, PARTITION_P)
    assert replica_1.conflicted_sequences(PROVIDER, PARTITION_P) == [12]
    assert not replica_2.is_partition_usable(PROVIDER, PARTITION_P)
    assert replica_2.conflicted_sequences(PROVIDER, PARTITION_P) == []

    # The independent partition Q stays usable the whole time and C is skippable.
    for replica in (replica_1, replica_2):
        assert replica.is_partition_usable(PROVIDER, PARTITION_Q)
        assert replica.can_skip_device(PROVIDER, PARTITION_Q, "C", THRESHOLD)
        # While P is flagged it must not be used to exclude either device.
        assert not replica.can_skip_device(PROVIDER, PARTITION_P, "A", THRESHOLD)
        assert not replica.can_skip_device(PROVIDER, PARTITION_P, "B", THRESHOLD)


def test_full_summary_replacing_corrections_resolves_conflict() -> None:
    summaries = build_replica_1()
    summaries.apply_correction(correction_11())
    state = summaries._require_partition(PROVIDER, PARTITION_P)
    assert set(state.correction_versions) == {11, 12}

    summaries.apply_full(full_summary_12_vendor())

    # The full summary independently replaces every correction it covers,
    # including the divergent mirrors of correction 12.
    assert state.full_sequence == 12
    assert state.correction_versions == {}
    assert summaries.is_partition_usable(PROVIDER, PARTITION_P)
    # A still holds an unknown cloud cover record, so A must be queried; B
    # holds a cloud-90 record and can never be skipped at threshold 50.
    assert not summaries.can_skip_device(PROVIDER, PARTITION_P, "A", THRESHOLD)
    assert not summaries.can_skip_device(PROVIDER, PARTITION_P, "B", THRESHOLD)


def test_end_to_end_replica_scenario() -> None:
    replica_1 = build_replica_1()
    replica_2 = build_replica_2()

    # Save and reload each replica independently before merging.
    replica_1 = ShardSummaries.from_dict(replica_1.to_dict())
    replica_2 = ShardSummaries.from_dict(replica_2.to_dict())

    def merged() -> ShardSummaries:
        merged_1 = replica_1.clone().merge(replica_2.clone())
        merged_2 = replica_2.clone().merge(replica_1.clone())
        # Merge order does not affect the resulting state.
        assert merged_1 == merged_2
        return merged_1

    combined = merged()
    combined.apply_correction(correction_11())
    combined.apply_full(full_summary_12_vendor())

    # A replica that never saw correction 13 still holds the unknown record
    # on A, so A must be read even after the vendor full summary 12.
    without_13 = build_replica_1()
    without_13.apply_correction(correction_11())
    without_13.apply_full(full_summary_12_vendor())
    assert not without_13.can_skip_device(PROVIDER, PARTITION_P, "A", THRESHOLD)
    assert not without_13.can_skip_device(PROVIDER, PARTITION_P, "B", THRESHOLD)

    # The merged replica carries correction 13, which resolves the unknown.
    combined.apply_correction(correction_13())
    assert combined.is_partition_usable(PROVIDER, PARTITION_P)

    # Replaying old files after sealing changes nothing.
    combined.apply_full(full_summary_10())
    combined.apply_correction(correction_12_reclassify())
    combined = ShardSummaries.from_dict(combined.to_dict())

    assert combined.can_skip_device(PROVIDER, PARTITION_P, "A", THRESHOLD)
    assert not combined.can_skip_device(PROVIDER, PARTITION_P, "B", THRESHOLD)
    assert combined.can_skip_device(PROVIDER, PARTITION_Q, "C", THRESHOLD)
    assert combined.devices_to_query(PROVIDER, PARTITION_P, THRESHOLD) == frozenset(
        {"B"}
    )
    assert combined.devices_to_query(PROVIDER, PARTITION_Q, THRESHOLD) == frozenset()


def test_unknown_never_skipped_and_not_applicable_excluded() -> None:
    summaries = ShardSummaries()
    summaries.register_partition(PROVIDER, PARTITION_P, ["A", "B"])
    summaries.apply_full(
        FullShardSummary(
            PROVIDER,
            PARTITION_P,
            1,
            [("A", UNKNOWN), ("B", NOT_APPLICABLE)],
        )
    )
    assert not summaries.can_skip_device(PROVIDER, PARTITION_P, "A", 0)
    assert summaries.can_skip_device(PROVIDER, PARTITION_P, "B", 0)
    with pytest.raises(ValueError):
        summaries.can_skip_device(PROVIDER, PARTITION_P, "B", 101)


def test_sealed_data_is_histogram_not_per_scene() -> None:
    summaries = ShardSummaries()
    summaries.register_partition(PROVIDER, PARTITION_P, ["A", "B"])
    summaries.apply_full(
        FullShardSummary(
            PROVIDER,
            PARTITION_P,
            1,
            [
                ("A", 50),
                ("A", 50),
                ("A", 20),
                ("A", UNKNOWN),
                ("B", 50),
                ("B", 20),
            ],
        )
    )
    payload = summaries.to_dict()
    partition = payload["partitions"][0]
    assert "full_versions" not in partition
    device_a = next(entry for entry in partition["sealed"] if entry["device"] == "A")
    cloud_cover = {entry["state"]: entry["count"] for entry in device_a["cloud_cover"]}
    assert cloud_cover == {20: 1, 50: 2, UNKNOWN: 1}
    # Storage size follows the range of cloud values, not the scene count.
    assert sum(cloud_cover.values()) == 4
    assert len(device_a["cloud_cover"]) == 3

    reloaded = ShardSummaries.from_dict(payload)
    assert reloaded == summaries
    # Unknown cloud cover can never be skipped, regardless of the threshold.
    assert not reloaded.can_skip_device(PROVIDER, PARTITION_P, "A", 51)
    # A fully known device supports exact threshold boundaries.
    assert not reloaded.can_skip_device(PROVIDER, PARTITION_P, "B", 50)
    assert reloaded.can_skip_device(PROVIDER, PARTITION_P, "B", 51)


def test_only_unsealed_corrections_are_retained() -> None:
    summaries = ShardSummaries()
    summaries.register_partition(PROVIDER, PARTITION_P, ["A", "B"])
    summaries.apply_full(full_summary_10())
    summaries.apply_correction(correction_11())
    summaries.apply_correction(correction_12_withdraw())
    summaries.apply_full(full_summary_12_vendor())
    summaries.apply_correction(correction_13())

    payload = summaries.to_dict()
    partition = payload["partitions"][0]
    assert partition["full_sequence"] == 12
    assert [item["sequence"] for item in partition["corrections"]] == [13]
    assert "full_versions" not in partition


def test_conflicted_full_summary_survives_save_and_reload() -> None:
    summaries = ShardSummaries()
    summaries.register_partition(PROVIDER, PARTITION_P, ["A"])
    summaries.apply_full(FullShardSummary(PROVIDER, PARTITION_P, 3, [("A", 90)]))
    summaries.apply_full(FullShardSummary(PROVIDER, PARTITION_P, 3, [("A", 10)]))
    assert not summaries.is_partition_usable(PROVIDER, PARTITION_P)
    reloaded = ShardSummaries.from_dict(summaries.to_dict())
    assert not reloaded.is_partition_usable(PROVIDER, PARTITION_P)
    assert reloaded.conflicted_sequences(PROVIDER, PARTITION_P) == [3]

    # A later covering full summary resolves the conflict.
    reloaded.apply_full(FullShardSummary(PROVIDER, PARTITION_P, 4, [("A", 10)]))
    assert reloaded.is_partition_usable(PROVIDER, PARTITION_P)
    assert reloaded.can_skip_device(PROVIDER, PARTITION_P, "A", THRESHOLD)


def test_full_summary_conflict_detected_after_replica_reload_and_merge() -> None:
    # Two mirrors deliver divergent full summaries with the same sequence to
    # different replicas, each of which is saved and reloaded (dropping the
    # per-scene payloads, keeping only histogram plus content hashes).
    first = ShardSummaries()
    first.register_partition(PROVIDER, PARTITION_P, ["A"])
    first.apply_full(FullShardSummary(PROVIDER, PARTITION_P, 3, [("A", 90)]))
    second = ShardSummaries()
    second.register_partition(PROVIDER, PARTITION_P, ["A"])
    second.apply_full(FullShardSummary(PROVIDER, PARTITION_P, 3, [("A", 10)]))

    first = ShardSummaries.from_dict(first.to_dict())
    second = ShardSummaries.from_dict(second.to_dict())
    payload = first.to_dict()["partitions"][0]
    assert "full_versions" not in payload

    merged = first.merge(second)
    assert not merged.is_partition_usable(PROVIDER, PARTITION_P)
    assert merged.conflicted_sequences(PROVIDER, PARTITION_P) == [3]
    assert not merged.can_skip_device(PROVIDER, PARTITION_P, "A", THRESHOLD)

    # A covering full summary on either replica resolves it after merging.
    first.apply_full(FullShardSummary(PROVIDER, PARTITION_P, 4, [("A", 10)]))
    resolved = second.merge(first)
    assert resolved.is_partition_usable(PROVIDER, PARTITION_P)
    assert resolved.can_skip_device(PROVIDER, PARTITION_P, "A", THRESHOLD)


def test_gap_detection() -> None:
    summaries = ShardSummaries()
    summaries.register_partition(PROVIDER, PARTITION_P, ["A"])
    summaries.apply_full(FullShardSummary(PROVIDER, PARTITION_P, 10, [("A", 5)]))
    summaries.apply_correction(
        ShardCorrection(
            PROVIDER,
            PARTITION_P,
            12,
            [ShardCorrectionEntry("A", 5, "A", 6)],
        )
    )
    assert not summaries.is_partition_usable(PROVIDER, PARTITION_P)
    summaries.apply_correction(
        ShardCorrection(
            PROVIDER,
            PARTITION_P,
            11,
            [ShardCorrectionEntry("A", 5, "A", 5)],
        )
    )
    assert summaries.is_partition_usable(PROVIDER, PARTITION_P)


def test_correction_moves_device_within_registered_partition() -> None:
    summaries = ShardSummaries()
    summaries.register_partition(PROVIDER, PARTITION_P, ["A", "B"])
    summaries.apply_full(full_summary_10())
    summaries.apply_correction(correction_11())
    summaries.apply_correction(correction_12_withdraw())
    summaries.apply_correction(correction_13())

    histogram = summaries._working_histogram(
        summaries._require_partition(PROVIDER, PARTITION_P)
    )
    assert histogram == {"A": {NOT_APPLICABLE: 1}, "B": {90: 1}}
    assert summaries.partitions_for_device("B") == [(PROVIDER, PARTITION_P)]


def test_collection_round_trip_keeps_both_summary_kinds() -> None:
    shard_summaries = build_replica_2()
    collection = make_collection(shard_summaries)
    payload = collection.to_dict()
    assert "summaries" in payload
    assert "shard_summaries" in payload

    reloaded = Collection.from_dict(payload)
    assert reloaded.summaries.to_dict() == {"platform": ["A", "B", "C"]}
    assert reloaded.shard_summaries == shard_summaries
    assert not reloaded.shard_summaries.is_partition_usable(PROVIDER, PARTITION_P)
    assert reloaded.shard_summaries.can_skip_device(
        PROVIDER, PARTITION_Q, "C", THRESHOLD
    )

    cloned = collection.clone()
    assert cloned.shard_summaries == shard_summaries
    cloned.shard_summaries.apply_correction(correction_11())
    assert collection.shard_summaries != cloned.shard_summaries

    full_copy = collection.full_copy()
    assert full_copy.shard_summaries == shard_summaries


def test_collection_without_shard_summaries_unchanged() -> None:
    collection = make_collection()
    payload = collection.to_dict()
    assert "shard_summaries" not in payload
    reloaded = Collection.from_dict(payload)
    assert reloaded.shard_summaries.is_empty()
    assert reloaded.summaries.to_dict() == {"platform": ["A", "B", "C"]}


def test_collection_save_and_reload(tmp_path: pathlib.Path) -> None:
    collection = make_collection(build_replica_1())
    path = tmp_path / "collection.json"
    pystac.write_file(collection, dest_href=str(path))
    reloaded = pystac.read_file(str(path))
    assert isinstance(reloaded, Collection)
    assert reloaded.shard_summaries == collection.shard_summaries
    assert reloaded.shard_summaries.conflicted_sequences(PROVIDER, PARTITION_P) == [12]
