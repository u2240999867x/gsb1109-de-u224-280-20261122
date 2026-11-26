import pickle

import pytest

import tablib
from tablib import Dataset, ReviewInbox


def make_dataset():
    dataset = Dataset(headers=("批号", "电压", "温度"))
    dataset.append((9, 220, 18))
    dataset.append((9, 221, 19))
    dataset.append((7, 215, 16))
    return dataset


def publish_three_versions(dataset):
    first = dataset.publish_for_review(
        version=1,
        tags=["甲", "乙", "丙", "丁"],
        source_rows=[1, 0, 1, 2],
    )

    dataset[1] = (9, 221, 29)
    second = dataset.publish_for_review(
        version=2,
        tags=["甲", "乙", "丙", "丁"],
        source_rows=[1, 0, 1, 2],
    )

    dataset[1] = (9, 221, 19)
    third = dataset.publish_for_review(
        version=3,
        tags=["甲", "乙", "丙", "丁"],
        source_rows=[1, 0, 1, 2],
        order=["丙", "甲", "丁", "乙"],
    )
    return first, second, third


def test_banded_review_full_out_of_order_restart_scenario():
    dataset = make_dataset()
    calls = []

    def identity_formatter(value):
        calls.append(value)
        return value

    dataset.add_formatter("温度", identity_formatter)
    first, second, third = publish_three_versions(dataset)

    assert len(calls) == 5
    assert first.bands == (("甲", "乙"), ("丙", "丁"))
    assert third.bands == (("丙", "甲"), ("丁", "乙"))

    sender = ReviewInbox()
    sender.receive_release(first)
    sender.receive_approval(version=1, group="温控组", band=1, approved=True)
    sender.receive_approval(version=1, group="温控组", band=2, approved=True)
    sender.receive_approval(version=1, group="电气组", band=1, approved=True)

    receiver = ReviewInbox()
    receiver.receive_release(third)
    receiver = pickle.loads(pickle.dumps(receiver))

    receiver.receive_release(second)
    receiver.receive_release(first)
    for approval in sender.approvals:
        receiver.approvals.append(pickle.loads(pickle.dumps(approval)))

    receiver.receive_approval(version=1, group="电气组", band=2, approved=True)

    status = receiver.status()
    assert status.missing_releases == ()
    assert status.missing == {"温控组": ["丙", "甲"]}
    assert ("温控组", "丙") in status.missing

    before_duplicate = len(receiver.approvals)
    duplicate = receiver.receive_approval(version=1, group="温控组", band=1, approved=True)
    assert duplicate.duplicate is True
    assert len(receiver.approvals) == before_duplicate

    receiver.receive_approval(version=3, group="温控组", band=1, approved=True)
    expected = [(19, 221), (19, 221), (16, 215), (18, 220)]
    assert receiver.deliver() == expected

    dataset._review_ledger = receiver
    summary = dataset.review_summary()
    assert summary == expected
    assert expected == summary
    assert isinstance(summary, Dataset)
    assert summary["温度"] == [19, 19, 16, 18]
    assert len(calls) == 5


def test_formatter_counts_deduplicate_source_cells_and_withdrawal():
    dataset = make_dataset()
    calls = []
    dataset.add_formatter("温度", lambda value: calls.append(value) or value)

    first = dataset.publish([1, 0, 1, 2])
    assert len(calls) == 3

    dataset.distribute_band(first.version, "温控组", 1)
    dataset.distribute_band(first.version, "温控组", 1)
    assert len(calls) == 3

    dataset.receive_approval(1, "温控组", 1, True)
    dataset.revoke_approval(1, "温控组", 1)
    assert len(calls) == 3


def test_conflicting_band_approval_reports_tags():
    dataset = make_dataset()
    dataset.publish(version=1, tags=["甲", "乙"], source_rows=[1, 0])
    dataset.receive_approval(1, "温控组", 1, {"甲": True, "乙": True})

    with pytest.raises(tablib.ApprovalConflictError) as exc_info:
        dataset.receive_approval(1, "温控组", 1, {"甲": False, "乙": True})

    assert exc_info.value.tags == ("甲",)
    assert "甲" in str(exc_info.value)


def test_group_packets_hide_other_measurement_column():
    dataset = make_dataset()
    release = dataset.publish(version=1, tags=["甲", "乙", "丙", "丁"], source_rows=[1, 0, 1, 2])

    electrical = release.packet("电气组", 1).to_dataset()
    thermal = release.packet("温控组", 2).to_dataset()

    assert electrical.headers == ["批号", "电压"]
    assert electrical[:] == [(9, 221), (9, 220)]
    assert thermal.headers == ["批号", "温度"]
    assert thermal[:] == [(9, 19), (7, 16)]
