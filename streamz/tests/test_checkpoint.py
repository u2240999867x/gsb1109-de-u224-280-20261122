import asyncio
import os
import subprocess
import sys

import pytest

from streamz import Stream
from streamz.checkpoint import (
    Checkpoint,
    CheckpointError,
    CheckpointManager,
    PositionedSource,
    checkpoint_transform,
)
from streamz.utils_test import tmpfile


LEFT_VALUES = ["L1", "L2"]
RIGHT_VALUES = ["R1", "R2", "R3"]


def pair_diff(pair):
    # Use the index digits so that mismatched pairings produce a non-zero diff.
    return int(pair[0][1]) - int(pair[1][1])


def build_pipeline():
    """Two positioned sources, per-route transforms, zip reconciliation and
    a running difference accumulator. Every node is named for checkpointing."""
    left = PositionedSource(LEFT_VALUES, stream_name="left", asynchronous=True)
    right = PositionedSource(RIGHT_VALUES, stream_name="right", asynchronous=True)
    left_t = checkpoint_transform(
        left, lambda x: x + "*", stream_name="left-t"
    )
    right_t = checkpoint_transform(
        right, lambda x: x + "*", stream_name="right-t"
    )
    zipped = Stream.zip(left_t, right_t, stream_name="zip")
    pairs = zipped.map(lambda pair: pair, stream_name="pairs").sink_to_list()
    accumulator = zipped.accumulate(
        lambda state, pair: state + pair_diff(pair),
        start=0, stream_name="acc",
    )
    total = accumulator.sink_to_list()
    return {
        "left": left,
        "right": right,
        "left_t": left_t,
        "right_t": right_t,
        "zip": zipped,
        "acc": accumulator,
        "pairs": pairs,
        "total": total,
    }


async def drive(pipeline):
    await asyncio.gather(
        pipeline["left"].wait_drained(),
        pipeline["right"].wait_drained(),
    )
    await asyncio.sleep(0.05)


def stop(pipeline):
    pipeline["left"].stop()
    pipeline["right"].stop()


@pytest.mark.asyncio
async def test_positioned_source_replays_from_offset():
    source = PositionedSource(["a", "b", "c"], stream_name="s", asynchronous=True)
    out = source.sink_to_list()
    source.replay(1)
    await source.wait_drained()
    assert out == ["b", "c"]
    assert source.emitted_through(2) and not source.emitted_through(3)
    source.stop()


@pytest.mark.asyncio
async def test_checkpoint_transform_preserves_order_with_gated_work():
    async def slow_double(x):
        await asyncio.sleep(0.02 if x % 2 == 0 else 0.001)
        return x * 2

    source = PositionedSource(range(5), stream_name="s", asynchronous=True)
    transformed = checkpoint_transform(
        source, slow_double, stream_name="t"
    )
    out = transformed.sink_to_list()
    source.replay(0)
    await source.wait_drained()
    await asyncio.sleep(0.05)
    assert out == [0, 2, 4, 6, 8]
    source.stop()


@pytest.mark.asyncio
async def test_inflight_input_accepted_while_earlier_position_hangs():
    pipeline = build_pipeline()
    manager = CheckpointManager.from_sources(
        {"left": pipeline["left"], "right": pipeline["right"]}
    )

    pipeline["left_t"].gate(0)  # L1 hangs inside the transform
    pipeline["right_t"].gate(2)  # R3 is accepted but hangs past the R2 cut
    pipeline["left"].replay(0)
    pipeline["right"].replay(0)
    await drive(pipeline)

    # L2 was already accepted behind the hanging L1
    assert pipeline["left_t"].inflight_positions() == [0, 1]
    assert pipeline["right_t"].inflight_positions() == [2]

    save = asyncio.create_task(
        manager.save_checkpoint(
            {"left": 0, "right": 1}, release_held=False
        )
    )
    await asyncio.sleep(0.05)
    # The cut cannot overtake the earlier in-flight input of its own route.
    assert not save.done()

    pipeline["left_t"].release(0)
    checkpoint = await asyncio.wait_for(save, timeout=2)

    # Only L1/R1 reached the exit; the snapshot keeps unpaired R2 and the
    # accepted-but-after-cut L2 and R3 as pending inputs.
    assert pipeline["pairs"] == [("L1*", "R1*")]
    assert pipeline["total"] == [0]
    assert checkpoint.pending == {"left": ["L2"], "right": ["R3"]}
    assert checkpoint.nodes["zip"]["buffers"] == {
        "left-t": [], "right-t": ["R2*"],
    }
    assert checkpoint.nodes["acc"]["state"] == 0
    assert checkpoint.positions == {"left": 0, "right": 1}

    # Frozen after-cut inputs do not cross the boundary after the snapshot.
    await asyncio.sleep(0.02)
    assert pipeline["pairs"] == [("L1*", "R1*")]
    assert pipeline["left_t"].inflight_positions() == [1]
    assert pipeline["right_t"].inflight_positions() == [2]
    stop(pipeline)


@pytest.mark.asyncio
async def test_restore_replays_from_cut_positions_without_reemitting():
    pipeline = build_pipeline()
    manager = CheckpointManager.from_sources(
        {"left": pipeline["left"], "right": pipeline["right"]}
    )
    pipeline["left_t"].gate(0)
    pipeline["right_t"].gate(2)
    pipeline["left"].replay(0)
    pipeline["right"].replay(0)
    await drive(pipeline)

    save = asyncio.create_task(
        manager.save_checkpoint(
            {"left": 0, "right": 1}, release_held=False
        )
    )
    await asyncio.sleep(0.03)
    assert not save.done()
    pipeline["left_t"].release(0)
    checkpoint = await asyncio.wait_for(save, timeout=2)
    stop(pipeline)

    rebuilt = build_pipeline()
    CheckpointManager.restore(
        checkpoint,
        {"left": rebuilt["left"], "right": rebuilt["right"]},
    )
    await drive(rebuilt)

    # First post-restore pair is L2/R2: the buffered R2 is consumed, R3 stays
    # buffered, and L1/R1 is never delivered to the exit again.
    assert rebuilt["pairs"] == [("L2*", "R2*")]
    assert rebuilt["total"] == [0]
    assert rebuilt["acc"].state == 0
    buffers = {
        upstream.name: [value for value, _ in queue]
        for upstream, queue in rebuilt["zip"].buffers.items()
    }
    assert buffers == {"left-t": [], "right-t": ["R3*"]}
    stop(rebuilt)


@pytest.mark.asyncio
async def test_checkpoint_roundtrip_to_file():
    pipeline = build_pipeline()
    manager = CheckpointManager.from_sources(
        {"left": pipeline["left"], "right": pipeline["right"]}
    )
    pipeline["left_t"].gate(0)
    pipeline["right_t"].gate(2)
    pipeline["left"].replay(0)
    pipeline["right"].replay(0)
    await drive(pipeline)
    save = asyncio.create_task(
        manager.save_checkpoint(
            {"left": 0, "right": 1}, release_held=False
        )
    )
    await asyncio.sleep(0.03)
    pipeline["left_t"].release(0)
    await asyncio.wait_for(save, timeout=2)

    with tmpfile("pkl") as path:
        await manager.save_checkpoint(
            {"left": 0, "right": 1}, path=path, release_held=False
        )
        reloaded = Checkpoint.load(path)
        assert reloaded.positions == {"left": 0, "right": 1}
        assert reloaded.pending == {"left": ["L2"], "right": ["R3"]}

        rebuilt = build_pipeline()
        CheckpointManager.restore(
            path, {"left": rebuilt["left"], "right": rebuilt["right"]}
        )
        await drive(rebuilt)
        assert rebuilt["pairs"] == [("L2*", "R2*")]
        stop(rebuilt)
    stop(pipeline)


@pytest.mark.asyncio
async def test_reversed_cut_order_gives_identical_result():
    async def scenario(right_first):
        pipeline = build_pipeline()
        manager = CheckpointManager.from_sources(
            {"left": pipeline["left"], "right": pipeline["right"]}
        )
        pipeline["left_t"].gate(0)
        pipeline["right_t"].gate(2)
        pipeline["left"].replay(0)
        pipeline["right"].replay(0)
        await drive(pipeline)

        positions = (
            {"right": 1, "left": 0} if right_first
            else {"left": 0, "right": 1}
        )
        save = asyncio.create_task(
            manager.save_checkpoint(positions, release_held=False)
        )
        await asyncio.sleep(0.03)
        assert not save.done()
        pipeline["left_t"].release(0)
        checkpoint = await asyncio.wait_for(save, timeout=2)
        stop(pipeline)

        rebuilt = build_pipeline()
        CheckpointManager.restore(
            checkpoint,
            {"left": rebuilt["left"], "right": rebuilt["right"]},
        )
        await drive(rebuilt)
        result = (list(rebuilt["pairs"]), list(rebuilt["total"]),
                  checkpoint.pending, checkpoint.positions)
        stop(rebuilt)
        return result

    first = await scenario(right_first=True)
    second = await scenario(right_first=False)
    assert first == second
    assert first[0] == [("L2*", "R2*")]


@pytest.mark.asyncio
async def test_hold_boundary_does_not_swallow_after_cut_inputs():
    pipeline = build_pipeline()
    manager = CheckpointManager.from_sources(
        {"left": pipeline["left"], "right": pipeline["right"]}
    )
    pipeline["right_t"].gate(2)  # R3 hangs inside the right transform
    pipeline["left"].replay(0)
    pipeline["right"].replay(0)
    await drive(pipeline)

    checkpoint = await manager.save_checkpoint(
        {"left": 0, "right": 1}, release_held=False
    )
    # R3 was received but stays strictly after the right cut.
    assert checkpoint.pending["right"] == ["R3"]
    assert pipeline["right_t"].inflight_positions() == [2]
    assert all("R3" not in pair for pair in pipeline["pairs"])
    stop(pipeline)


@pytest.mark.asyncio
async def test_cut_cannot_pass_earlier_inflight_input():
    pipeline = build_pipeline()
    manager = CheckpointManager.from_sources(
        {"left": pipeline["left"], "right": pipeline["right"]}
    )
    pipeline["left_t"].gate(0)
    pipeline["left"].replay(0)
    pipeline["right"].replay(0)
    await drive(pipeline)

    with pytest.raises(CheckpointError, match="did not process cut position"):
        await manager.save_checkpoint(
            {"left": 0, "right": 1}, timeout=0.2, release_held=False
        )
    stop(pipeline)


@pytest.mark.asyncio
async def test_missing_cut_position_rejected():
    pipeline = build_pipeline()
    manager = CheckpointManager.from_sources(
        {"left": pipeline["left"], "right": pipeline["right"]}
    )
    with pytest.raises(CheckpointError, match="Missing cut positions"):
        await manager.save_checkpoint({"left": 0})
    stop(pipeline)


@pytest.mark.asyncio
async def test_replacing_named_operator_fails_before_restore():
    pipeline = build_pipeline()
    manager = CheckpointManager.from_sources(
        {"left": pipeline["left"], "right": pipeline["right"]}
    )
    pipeline["left_t"].gate(0)
    pipeline["right_t"].gate(2)
    pipeline["left"].replay(0)
    pipeline["right"].replay(0)
    await drive(pipeline)
    save = asyncio.create_task(
        manager.save_checkpoint(
            {"left": 0, "right": 1}, release_held=False
        )
    )
    await asyncio.sleep(0.03)
    pipeline["left_t"].release(0)
    checkpoint = await asyncio.wait_for(save, timeout=2)
    stop(pipeline)

    # Rebuild the same connections but swap the named left transform for a
    # plain map node carrying the same name.
    left = PositionedSource(LEFT_VALUES, stream_name="left", asynchronous=True)
    right = PositionedSource(RIGHT_VALUES, stream_name="right", asynchronous=True)
    left_t = left.map(lambda x: x + "*", stream_name="left-t")
    right_t = checkpoint_transform(
        right, lambda x: x + "*", stream_name="right-t"
    )
    zipped = Stream.zip(left_t, right_t, stream_name="zip")
    pairs = zipped.map(lambda pair: pair, stream_name="pairs").sink_to_list()
    accumulator = zipped.accumulate(
        lambda state, pair: state + pair_diff(pair),
        start=0, stream_name="acc",
    )
    total = accumulator.sink_to_list()

    with pytest.raises(CheckpointError):
        CheckpointManager.restore(
            checkpoint, {"left": left, "right": right}
        )

    # Validation happens before any state mutation or source start.
    assert accumulator.state == 0
    assert left.emitted_count == 0 and right.emitted_count == 0
    assert pairs == [] and total == []
    left.stop()
    right.stop()


@pytest.mark.asyncio
async def test_changed_connections_fail_before_restore():
    pipeline = build_pipeline()
    manager = CheckpointManager.from_sources(
        {"left": pipeline["left"], "right": pipeline["right"]}
    )
    pipeline["left"].replay(0)
    pipeline["right"].replay(0)
    await drive(pipeline)
    checkpoint = await manager.save_checkpoint(
        {"left": 0, "right": 1}, release_held=False
    )
    stop(pipeline)

    left = PositionedSource(LEFT_VALUES, stream_name="left", asynchronous=True)
    right = PositionedSource(RIGHT_VALUES, stream_name="right", asynchronous=True)
    left_t = checkpoint_transform(left, lambda x: x + "*", stream_name="left-t")
    right_t = checkpoint_transform(
        right, lambda x: x + "*", stream_name="right-t"
    )
    # Extra operator changes the structural signature of the graph.
    extra = left_t.map(lambda x: x, stream_name="extra")
    zipped = Stream.zip(extra, right_t, stream_name="zip")
    zipped.accumulate(
        lambda state, pair: state + pair_diff(pair),
        start=0, stream_name="acc",
    ).sink_to_list()

    with pytest.raises(CheckpointError, match="topology"):
        CheckpointManager.restore(
            checkpoint, {"left": left, "right": right}
        )
    assert left.emitted_count == 0 and right.emitted_count == 0
    left.stop()
    right.stop()


@pytest.mark.asyncio
async def test_duplicate_node_names_rejected():
    left = PositionedSource(LEFT_VALUES, stream_name="source", asynchronous=True)
    right = PositionedSource(RIGHT_VALUES, stream_name="source", asynchronous=True)
    with pytest.raises(CheckpointError, match="registered as"):
        CheckpointManager.from_sources({"left": left, "right": right})
    left.stop()
    right.stop()


@pytest.mark.asyncio
async def test_checkpoint_pickle_contents():
    checkpoint = Checkpoint(
        positions={"left": 0},
        nodes={"acc": {"state": 3}},
        pending={"left": ["L2"]},
        topology=[("acc", "streamz.core.accumulate", ("left",))],
    )
    with tmpfile("pkl") as path:
        checkpoint.save(path)
        reloaded = Checkpoint.load(path)
        assert reloaded.to_dict() == checkpoint.to_dict()


def test_checkpoint_restores_in_a_fresh_process():
    with tmpfile("pkl") as checkpoint_path:

        async def prepare():
            pipeline = build_pipeline()
            manager = CheckpointManager.from_sources(
                {"left": pipeline["left"], "right": pipeline["right"]}
            )
            pipeline["left_t"].gate(0)
            pipeline["right_t"].gate(2)
            pipeline["left"].replay(0)
            pipeline["right"].replay(0)
            await drive(pipeline)
            save = asyncio.create_task(
                manager.save_checkpoint(
                    {"left": 0, "right": 1},
                    path=checkpoint_path, release_held=False,
                )
            )
            await asyncio.sleep(0.03)
            pipeline["left_t"].release(0)
            await asyncio.wait_for(save, timeout=2)
            stop(pipeline)

        asyncio.run(prepare())

        script = (
            "import asyncio\n"
            "from streamz import Stream\n"
            "from streamz.checkpoint import (\n"
            "    CheckpointManager, PositionedSource, checkpoint_transform)\n"
            "LEFT_VALUES = ['L1', 'L2']\n"
            "RIGHT_VALUES = ['R1', 'R2', 'R3']\n"
            "def pair_diff(pair):\n"
            "    return int(pair[0][1]) - int(pair[1][1])\n"
            "def build():\n"
            "    left = PositionedSource(LEFT_VALUES, stream_name='left', asynchronous=True)\n"
            "    right = PositionedSource(RIGHT_VALUES, stream_name='right', asynchronous=True)\n"
            "    lt = checkpoint_transform(left, lambda x: x + '*', stream_name='left-t')\n"
            "    rt = checkpoint_transform(right, lambda x: x + '*', stream_name='right-t')\n"
            "    zipped = Stream.zip(lt, rt, stream_name='zip')\n"
            "    pairs = zipped.map(lambda p: p, stream_name='pairs').sink_to_list()\n"
            "    total = zipped.accumulate(lambda s, p: s + pair_diff(p), start=0, stream_name='acc').sink_to_list()\n"
            "    return left, right, pairs, total\n"
            "async def main():\n"
            "    left, right, pairs, total = build()\n"
            "    CheckpointManager.restore(%r, {'left': left, 'right': right})\n"
            "    await asyncio.gather(left.wait_drained(), right.wait_drained())\n"
            "    await asyncio.sleep(0.05)\n"
            "    print('PAIRS', pairs)\n"
            "    print('TOTAL', total)\n"
            "    left.stop(); right.stop()\n"
            "asyncio.run(asyncio.wait_for(main(), timeout=10))\n"
            % checkpoint_path
        )

        env = dict(os.environ)
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True, text=True, env=env, timeout=30,
        )
        assert result.returncode == 0, result.stderr
        assert "PAIRS [('L2*', 'R2*')]" in result.stdout
        assert "TOTAL [0]" in result.stdout


def test_uncheckpointed_streams_keep_original_behaviour():
    source = Stream()
    out = source.map(lambda x: x + 1).accumulate(
        lambda acc, x: acc + x, start=0
    ).sink_to_list()
    for value in range(4):
        source.emit(value)
    assert out == [1, 3, 6, 10]
    # Stateless base nodes still report no checkpoint state.
    assert source._checkpoint_state() is None
    assert source._restore_checkpoint_state({"whatever": 1}) is None


@pytest.mark.asyncio
async def test_live_pipeline_continues_after_default_checkpoint_release():
    pipeline = build_pipeline()
    manager = CheckpointManager.from_sources(
        {"left": pipeline["left"], "right": pipeline["right"]}
    )
    pipeline["left_t"].gate(0)
    pipeline["right_t"].gate(2)
    pipeline["left"].replay(0)
    pipeline["right"].replay(0)
    await drive(pipeline)

    save = asyncio.create_task(
        manager.save_checkpoint({"left": 0, "right": 1})
    )
    await asyncio.sleep(0.03)
    pipeline["left_t"].release(0)
    await asyncio.wait_for(save, timeout=2)
    # release_held defaults to True, so the live pipeline drains afterwards.
    await drive(pipeline)

    assert pipeline["pairs"] == [("L1*", "R1*"), ("L2*", "R2*")]
    stop(pipeline)


@pytest.mark.asyncio
async def test_source_barrier_blocks_until_released():
    source = PositionedSource(["a", "b"], stream_name="s", asynchronous=True)
    out = source.sink_to_list()
    source.barrier(0)
    source.replay(0)
    await asyncio.sleep(0.05)
    assert out == []
    source.release(0)
    await source.wait_drained()
    assert out == ["a", "b"]
    source.stop()


@pytest.mark.asyncio
async def test_hold_boundary_arms_before_inputs_arrive():
    source = PositionedSource([10, 20, 30], stream_name="s", asynchronous=True)
    transformed = checkpoint_transform(
        source, lambda x: x, stream_name="t"
    )
    out = transformed.sink_to_list()
    transformed.hold_from(1)  # positions 1 and 2 must stay parked
    source.replay(0)
    await asyncio.sleep(0.05)
    assert out == [10]
    assert transformed.inflight_positions() == [1, 2]

    transformed.release_hold()
    await source.wait_drained()
    await asyncio.sleep(0.05)
    assert out == [10, 20, 30]
    source.stop()


@pytest.mark.asyncio
async def test_second_checkpoint_after_restore():
    pipeline = build_pipeline()
    manager = CheckpointManager.from_sources(
        {"left": pipeline["left"], "right": pipeline["right"]}
    )
    pipeline["left_t"].gate(0)
    pipeline["right_t"].gate(2)
    pipeline["left"].replay(0)
    pipeline["right"].replay(0)
    await drive(pipeline)
    save = asyncio.create_task(
        manager.save_checkpoint(
            {"left": 0, "right": 1}, release_held=False
        )
    )
    await asyncio.sleep(0.03)
    pipeline["left_t"].release(0)
    first = await asyncio.wait_for(save, timeout=2)
    stop(pipeline)

    rebuilt = build_pipeline()
    rebuilt_manager = CheckpointManager.restore(
        first, {"left": rebuilt["left"], "right": rebuilt["right"]}
    )
    await drive(rebuilt)
    assert rebuilt["pairs"] == [("L2*", "R2*")]

    # A second snapshot at the advanced position holds R3 only.
    second = await rebuilt_manager.save_checkpoint(
        {"left": 1, "right": 2}, release_held=False
    )
    assert second.positions == {"left": 1, "right": 2}
    assert second.pending == {"left": [], "right": []}
    assert second.nodes["acc"]["state"] == 0
    stop(rebuilt)


@pytest.mark.asyncio
async def test_invalid_source_registrations_rejected():
    plain = Stream()
    with pytest.raises(CheckpointError, match="mapping"):
        CheckpointManager.from_sources([plain])

    source = PositionedSource([1], stream_name="s", asynchronous=True)
    with pytest.raises(CheckpointError, match="PositionedSource"):
        CheckpointManager.from_sources({"s": plain})

    with pytest.raises(CheckpointError, match="registered as"):
        CheckpointManager.from_sources({"other": source})
    source.stop()


@pytest.mark.asyncio
async def test_replay_beyond_available_values_rejected():
    source = PositionedSource([1], stream_name="s", asynchronous=True)
    with pytest.raises(CheckpointError, match="beyond"):
        source.replay(5)
    source.stop()


@pytest.mark.asyncio
async def test_unknown_cut_position_rejected():
    pipeline = build_pipeline()
    manager = CheckpointManager.from_sources(
        {"left": pipeline["left"], "right": pipeline["right"]}
    )
    with pytest.raises(CheckpointError, match="Unknown source positions"):
        await manager.save_checkpoint(
            {"left": 0, "right": 1, "middle": 0}
        )
    stop(pipeline)
