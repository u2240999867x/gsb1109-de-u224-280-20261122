import pandas as pd
import pytest

import pyranges as pr
from pyranges import (
    ChannelCapacity,
    ChannelHandover,
    ExecutedWrite,
    InfeasibleSchedule,
    Task,
    schedule_channels,
)


def north_capacity():
    return [
        ChannelCapacity("北线", "X", 0, 8, 1),
        ChannelCapacity("北线", "Y", 6, 12, 1),
        ChannelCapacity("北线", "Z", 6, 12, 1),
    ]


def task_a():
    return Task(
        "A",
        "北线",
        0,
        12,
        1,
        ("X",),
        (
            ChannelHandover("A", 8, "X", "Y", 1),
            ChannelHandover("A", 8, "X", "Z", 2),
        ),
    )


def task_b():
    return Task("B", "北线", 7, 10, 1, ("Y",))


def row(frame, task, channel, kind, start, end=None):
    matching = frame[
        (frame.Task == task)
        & (frame.Channel == channel)
        & (frame.Kind == kind)
        & (frame.Start == start)
    ]
    if end is not None:
        matching = matching[matching.End == end]
    assert len(matching) == 1, frame
    return matching.iloc[0]


def test_only_a_chooses_shorter_warmup_y():
    result = schedule_channels([task_a()], north_capacity())

    assert result.feasible
    assert result.future_handovers == 1
    assert result.future_warmup_slot_seconds == 1

    handover = result.handovers.iloc[0]
    assert handover.Target == "Y"
    assert (handover.WarmupStart, handover.WarmupEnd) == (7, 8)

    row(result.writes, "A", "X", "primary", 0, 8)
    row(result.writes, "A", "Y", "warmup", 7, 8)
    row(result.writes, "A", "Y", "primary", 8, 12)


def test_a_and_b_hand_over_a_to_z_and_keep_b_on_y():
    result = schedule_channels([task_a(), task_b()], north_capacity())

    assert result.feasible
    assert result.future_handovers == 1
    assert result.future_warmup_slot_seconds == 2

    handover = result.handovers.iloc[0]
    assert (handover.Source, handover.Target) == ("X", "Z")
    assert (handover.WarmupStart, handover.WarmupEnd) == (6, 8)

    row(result.writes, "A", "X", "primary", 0, 8)
    row(result.writes, "A", "Z", "warmup", 6, 8)
    row(result.writes, "A", "Z", "primary", 8, 12)
    row(result.writes, "B", "Y", "primary", 7, 10)


def test_missing_warmup_before_cutoff_is_infeasible_with_conflicts():
    result = schedule_channels(
        [task_a(), task_b()],
        north_capacity(),
        executed=[ExecutedWrite("A", "北线", "X", 0, 7, "primary", 1)],
        cutoff=7,
    )

    assert not result.feasible
    assert result.future_handovers == 0

    conflicts = result.conflicts
    y_conflict = conflicts[
        (conflicts.Channel == "Y")
        & (conflicts.Start <= 7)
        & (conflicts.End >= 8)
        & (conflicts.Kind == "occupied")
    ]
    assert len(y_conflict)
    assert "B" in set(y_conflict.OccupiedBy)

    warmup_conflict = conflicts[conflicts.Kind == "warmup_missing"]
    assert len(warmup_conflict)
    assert (warmup_conflict.iloc[0].Channel) == "Z"
    assert (warmup_conflict.iloc[0].Start, warmup_conflict.iloc[0].End) == (6, 7)


def test_executed_warmup_prefix_is_continued_after_cutoff():
    result = schedule_channels(
        [task_a(), task_b()],
        north_capacity(),
        executed=[
            ExecutedWrite("A", "北线", "X", 0, 7, "primary", 1),
            ExecutedWrite("A", "北线", "Z", 6, 7, "warmup", 1, handover_at=8),
        ],
        cutoff=7,
    )

    assert result.feasible
    assert result.future_handovers == 1
    assert result.future_warmup_slot_seconds == 1

    row(result.writes, "A", "Z", "warmup", 7, 8)
    row(result.writes, "A", "Z", "primary", 8, 12)
    row(result.writes, "B", "Y", "primary", 7, 10)

    handover = result.handovers.iloc[0]
    assert (handover.Source, handover.Target) == ("X", "Z")
    assert (handover.WarmupStart, handover.WarmupEnd) == (6, 8)
    assert handover.WarmupExecutedUntil == 7

    executed = result.writes[result.writes.Executed]
    assert set(zip(executed.Task, executed.Channel, executed.Kind, executed.Start)) == {
        ("A", "X", "primary", 0),
        ("A", "Z", "warmup", 6),
    }


def test_half_open_boundaries_allow_back_to_back_tasks():
    result = schedule_channels(
        [Task("early", "line", 0, 5, 1, ("Q",)), Task("late", "line", 5, 9, 1, ("Q",))],
        [ChannelCapacity("line", "Q", 0, 9, 1)],
    )

    assert result.feasible
    row(result.writes, "early", "Q", "primary", 0, 5)
    row(result.writes, "late", "Q", "primary", 5, 9)


def test_unlisted_capacity_is_zero():
    result = schedule_channels(
        [Task("A", "line", 0, 10, 1, ("Q",))],
        [ChannelCapacity("line", "Q", 0, 5, 1)],
    )

    assert not result.feasible
    assert {"occupied", "warmup_missing", "primary_missing"} & set(result.conflicts.Kind)
    assert (result.conflicts.Available == 0).any()


def test_two_slots_need_two_free_slots():
    result = schedule_channels(
        [Task("A", "line", 0, 4, 2, ("Q",))],
        [ChannelCapacity("line", "Q", 0, 4, 1)],
    )
    assert not result.feasible
    assert (result.conflicts.Available == 1).any()


def test_objective_prefers_fewer_future_handovers_then_less_warmup():
    # One handover P->Q needs 3 warm-up seconds; two handovers P->S->R need
    # only 1+1 seconds.  The handover count dominates the warm-up total.
    # P is only available early, so at least one handover is required.
    capacity = [
        ChannelCapacity("line", "P", 0, 4, 1),
        ChannelCapacity("line", "Q", 1, 10, 1),
        ChannelCapacity("line", "S", 3, 10, 1),
        ChannelCapacity("line", "R", 7, 10, 1),
    ]
    result = schedule_channels(
        [
            Task(
                "A",
                "line",
                0,
                10,
                1,
                ("P",),
                (
                    ChannelHandover("A", 4, "P", "Q", 3),
                    ChannelHandover("A", 4, "P", "S", 1),
                    ChannelHandover("A", 7, "S", "R", 1),
                ),
            )
        ],
        capacity,
    )

    # Both routes are capacity-feasible and both use one handover.  The
    # shorter warm-up to S wins on the second objective.
    assert result.future_handovers == 1
    assert result.future_warmup_slot_seconds == 1
    assert result.handovers.iloc[0].Target == "S"


def test_objective_uses_handover_count_before_warmup_total():
    # One handover P->Q needs 3 warm-up seconds; two handovers P->S->R need
    # only 1+1 seconds.  The handover count is the primary objective.
    capacity = [
        ChannelCapacity("line", "P", 0, 4, 1),
        ChannelCapacity("line", "Q", 1, 10, 1),
        ChannelCapacity("line", "S", 3, 7, 1),
        ChannelCapacity("line", "R", 7, 10, 1),
    ]
    result = schedule_channels(
        [
            Task(
                "A",
                "line",
                0,
                10,
                1,
                ("P",),
                (
                    ChannelHandover("A", 4, "P", "Q", 3),
                    ChannelHandover("A", 4, "P", "S", 1),
                    ChannelHandover("A", 7, "S", "R", 1),
                ),
            )
        ],
        capacity,
    )

    # S is unusable from 7 onwards, so the only one-handover route is P->Q.
    assert result.future_handovers == 1
    assert result.future_warmup_slot_seconds == 3
    assert result.handovers.iloc[0].Target == "Q"


def test_uneexecuted_previous_schedule_is_ignored():
    tasks = [task_a(), task_b()]
    previous = schedule_channels(tasks, north_capacity())

    result = schedule_channels(tasks, north_capacity(), previous=previous)

    assert result.feasible
    assert result.future_handovers == 1


def test_incompatible_executed_warmup_is_rejected():
    # A only allows X->Y with a 1-second warm-up and X->Z with 2 seconds.
    # A Y warm-up from 6 to 7 prepares an 8-second X->Y handover that needs
    # [7, 8) instead, so it cannot be continued.
    result = schedule_channels(
        [task_a()],
        north_capacity(),
        executed=[
            ExecutedWrite("A", "北线", "X", 0, 7, "primary", 1),
            ExecutedWrite("A", "北线", "Y", 6, 7, "warmup", 1, handover_at=8),
        ],
        cutoff=7,
    )

    assert not result.feasible
    assert (result.conflicts.Kind == "warmup_missing").any()


def test_raise_on_infeasible_exposes_conflicts():
    with pytest.raises(InfeasibleSchedule) as error:
        schedule_channels(
            [task_a(), task_b()],
            north_capacity(),
            executed=[ExecutedWrite("A", "北线", "X", 0, 7, "primary", 1)],
            cutoff=7,
            raise_on_infeasible=True,
        )
    assert len(error.value.conflicts)


def test_dataframe_inputs_and_as_pyranges():
    tasks = pd.DataFrame(
        {
            "Chromosome": ["北线", "北线"],
            "Start": [0, 7],
            "End": [12, 10],
            "Task": ["A", "B"],
            "Slots": [1, 1],
            "Channel": ["X", "Y"],
        }
    )
    capacity = pd.DataFrame(
        {
            "Chromosome": ["北线"] * 3,
            "Start": [0, 6, 6],
            "End": [8, 12, 12],
            "Channel": ["X", "Y", "Z"],
            "Slots": [1, 1, 1],
        }
    )
    handovers = pd.DataFrame(
        {
            "Task": ["A", "A"],
            "At": [8, 8],
            "Source": ["X", "X"],
            "Target": ["Y", "Z"],
            "Warmup": [1, 2],
        }
    )

    result = schedule_channels(tasks, capacity, handovers)
    assert result.feasible
    assert len(result.as_pyranges()) == len(result.writes)
    assert isinstance(result.as_pyranges(), pr.PyRanges)


def test_pyranges_method_entry_point():
    tasks = pr.from_dict(
        {
            "Chromosome": ["北线", "北线"],
            "Start": [0, 7],
            "End": [12, 10],
            "Task": ["A", "B"],
            "Slots": [1, 1],
            "Channel": ["X", "Y"],
        }
    )
    capacity = plot_capacity_frame()
    handovers = pd.DataFrame(
        {
            "Task": ["A", "A"],
            "At": [8, 8],
            "Source": ["X", "X"],
            "Target": ["Y", "Z"],
            "Warmup": [1, 2],
        }
    )

    result = tasks.schedule_channels(capacity, handovers)
    assert result.feasible
    assert result.future_warmup_slot_seconds == 2


def plot_capacity_frame():
    return pd.DataFrame(
        {
            "Chromosome": ["北线"] * 3,
            "Start": [0, 6, 6],
            "End": [8, 12, 12],
            "Channel": ["X", "Y", "Z"],
            "Slots": [1, 1, 1],
        }
    )
