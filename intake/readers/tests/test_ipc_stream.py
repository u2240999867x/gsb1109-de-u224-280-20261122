"""Tests for resumable, batch-wise Arrow IPC stream reading.

The end-to-end scenario mirrors a device upload: a region dictionary that grows
(沪/苏/浙/皖), record batches delivered one at a time, a disconnect halfway
through batch 3, and two reconnect paths that differ only in the warehouse's
confirmed prefix.
"""

import io

import pytest

pa = pytest.importorskip("pyarrow")
ipc = pytest.importorskip("pyarrow.ipc")

from intake.readers import datatypes
from intake.readers.ipc_stream import (
    ArrowIPCInterrupted,
    CorruptArrowIPCFile,
    ResumableIPCReader,
    StaleResumeInfo,
    StreamResumeInfo,
    _FrameParser,
)
from intake.readers.readers import PyArrowIPCReader

STREAM_ID = "device-7"
STREAM_VERSION = "v2026.11"
REGIONS = ["沪", "苏", "浙", "皖"]


def _schema():
    return pa.schema(
        [
            pa.field("order_id", pa.int64()),
            pa.field("region", pa.dictionary(pa.int8(), pa.string())),
        ]
    )


def _batch(order_ids, indices, dictionary):
    region = pa.DictionaryArray.from_arrays(
        pa.array(indices, pa.int8()), pa.array(dictionary, pa.string())
    )
    return pa.record_batch(
        [pa.array(order_ids, pa.int64()), region], names=["order_id", "region"]
    )


def _write_steps(steps, close=True):
    """Serialize (dictionary, batch) steps into IPC stream bytes.

    pyarrow emits a dictionary message automatically whenever the dictionary
    changes.  With ``close=False`` the explicit EOS marker is omitted, which is
    exactly the state of a transport that drops before end-of-stream.
    """
    sink = io.BytesIO()
    writer = ipc.new_stream(sink, _schema())
    for dictionary, order_ids, indices in steps:
        writer.write_batch(_batch(order_ids, indices, dictionary))
    if close:
        writer.close()
    # Without close(), every message is already flushed and only the explicit
    # EOS marker is missing — the state of a transport that drops early.
    return sink.getvalue()


def _frame_index(data):
    """Return ``(kind, start, end)`` for every frame in *data*."""
    parser = _FrameParser(io.BytesIO(data))
    return [(kind, start, start + len(frame)) for kind, start, frame in parser.frames()]


def _rows(batch):
    return [
        (batch.column(0)[i].as_py(), batch.column(1)[i].as_py())
        for i in range(batch.num_rows)
    ]


def _initial_steps():
    return [
        (REGIONS[:1], [101], [0]),  # batch 1; dict 沪 established first
        (REGIONS[:2], [102], [1]),  # batch 2 after appending 苏
        (REGIONS[:3], [103, 104], [2, 1]),  # batch 3 after appending 浙
    ]


def _continuation_tail():
    """Bytes a reconnecting service sends after batch 3's message start.

    Append 皖, send batch 4 (order 105, index 3) and the explicit EOS marker.
    No schema or full dictionary is included.
    """
    sink = io.BytesIO()
    writer = ipc.new_stream(sink, _schema())
    writer.write_batch(_batch([105], [3], REGIONS[:4]))
    writer.close()
    raw = sink.getvalue()
    first_non_schema = next(
        start for kind, start, _ in _frame_index(raw) if kind != "schema"
    )
    return raw[first_non_schema:]


def _drive_until_break():
    data = _write_steps(_initial_steps(), close=False)
    records = [item for item in _frame_index(data) if item[0] == "record"]
    _, batch3_start, batch3_end = records[2]
    cut = batch3_start + (batch3_end - batch3_start) // 2
    reader = ResumableIPCReader(io.BytesIO(data[:cut]), STREAM_ID, STREAM_VERSION)
    delivered = []
    with pytest.raises(ArrowIPCInterrupted) as exc:
        for number, batch in reader:
            delivered.append((number, _rows(batch)))
    return reader, exc.value, delivered, data, batch3_start


def test_batches_are_delivered_before_stream_ends():
    data = _write_steps(_initial_steps(), close=False) + _continuation_tail()
    reader = ResumableIPCReader(io.BytesIO(data), STREAM_ID, STREAM_VERSION)
    delivered = []
    for number, batch in reader:
        delivered.append((number, _rows(batch)))
        assert not reader.saw_end_of_stream
    assert reader.saw_end_of_stream
    assert [row for _, rows in delivered for row in rows] == [
        (101, "沪"),
        (102, "苏"),
        (103, "浙"),
        (104, "苏"),
        (105, "皖"),
    ]


def test_interruption_reports_position_delivered_and_replay_start():
    reader, err, delivered, _, batch3_start = _drive_until_break()
    # First unread message = the batch-3 message that was cut in half.
    assert err.message_position == batch3_start
    # Batches 1 and 2 were fully delivered; batch 3 never was.
    assert err.last_delivered_batch == 2
    assert delivered == [(1, [(101, "沪")]), (2, [(102, "苏")])]
    # With nothing confirmed, replay must start at the stream start.
    assert err.resume_at is None
    info = reader.export_resume_info()
    assert info.next_message_start == batch3_start
    assert info.batch_starts[1] < info.batch_starts[2] < batch3_start
    # Confirmed prefix 1 -> ask for batch 2's message start.
    reader.confirmed_batch = 1
    assert reader.resume_position() == info.batch_starts[2]
    # Confirmed prefix 2 -> ask for the interrupted batch 3's start.
    reader.confirmed_batch = 2
    assert reader.resume_position() == batch3_start


def test_resume_info_round_trips():
    reader, _, _, _, _ = _drive_until_break()
    info = reader.export_resume_info()
    assert StreamResumeInfo.from_dict(info.to_dict()) == info


def test_resume_after_only_batch_one_confirmed():
    broken, _, _, data, batch3_start = _drive_until_break()
    info = broken.export_resume_info()
    batch2_start = info.batch_starts[2]
    # Replay from batch 2's message start: batch 2, the 浙 dictionary message,
    # the full batch 3, then the live 皖/batch-4/EOS continuation.
    fragment = data[batch2_start:] + _continuation_tail()
    reader = ResumableIPCReader(
        io.BytesIO(fragment),
        STREAM_ID,
        STREAM_VERSION,
        resume_info=info,
        confirmed_batch=1,
    )
    numbers = []
    rows = []
    for number, batch in reader:
        numbers.append(number)
        rows.extend(_rows(batch))
    assert reader.saw_end_of_stream
    assert numbers == [2, 3, 4]
    assert rows == [(102, "苏"), (103, "浙"), (104, "苏"), (105, "皖")]


def test_resume_after_batch_two_confirmed():
    broken, _, _, data, batch3_start = _drive_until_break()
    info = broken.export_resume_info()
    fragment = data[batch3_start:] + _continuation_tail()
    reader = ResumableIPCReader(
        io.BytesIO(fragment),
        STREAM_ID,
        STREAM_VERSION,
        resume_info=info,
        confirmed_batch=2,
    )
    numbers = []
    rows = []
    for number, batch in reader:
        numbers.append(number)
        rows.extend(_rows(batch))
    assert reader.saw_end_of_stream
    assert numbers == [3, 4]
    assert rows == [(103, "浙"), (104, "苏"), (105, "皖")]


def test_stale_resume_info_rejected_before_any_rows():
    broken, _, _, data, batch3_start = _drive_until_break()
    info = broken.export_resume_info()
    fragment = data[batch3_start:] + _continuation_tail()
    # Same address, new stream version: reject before a single row is delivered.
    with pytest.raises(StaleResumeInfo):
        ResumableIPCReader(
            io.BytesIO(fragment),
            STREAM_ID,
            STREAM_VERSION + "-new",
            resume_info=info,
            confirmed_batch=2,
        )
    with pytest.raises(StaleResumeInfo):
        ResumableIPCReader(
            io.BytesIO(fragment),
            STREAM_ID + "-other",
            STREAM_VERSION,
            resume_info=info,
            confirmed_batch=2,
        )


def test_explicit_eos_is_required():
    data = _write_steps(_initial_steps(), close=False)
    last_record_end = max(
        end for kind, _, end in _frame_index(data) if kind == "record"
    )
    # Transport closes after complete batches but without the EOS marker.
    reader = ResumableIPCReader(
        io.BytesIO(data[:last_record_end]), STREAM_ID, STREAM_VERSION
    )
    with pytest.raises(ArrowIPCInterrupted):
        list(reader)


def test_corrupt_arrow1_file_has_no_stream_position(tmp_path):
    path = tmp_path / "archive.arrow"
    good = _write_steps(_initial_steps())
    path.write_bytes(b"ARROW1" + good[6:][: len(good) // 2])
    reader = PyArrowIPCReader(datatypes.ArrowIPC(str(path)))
    with pytest.raises(CorruptArrowIPCFile) as exc:
        reader.read()
    assert getattr(exc.value, "resume_at", None) is None
    with pytest.raises(CorruptArrowIPCFile):
        reader.discover()


def test_complete_file_read_and_discover_unchanged(tmp_path):
    table = pa.table(
        {
            "order_id": pa.int64(),
            "region": pa.array(["沪", "苏", "浙"], pa.string()),
        }
    )
    sink = io.BytesIO()
    writer = ipc.new_file(sink, table.schema)
    writer.write_table(table)
    writer.close()
    path = tmp_path / "good.arrow"
    path.write_bytes(sink.getvalue())
    reader = PyArrowIPCReader(datatypes.ArrowIPC(str(path)))
    assert reader.read().num_rows == 3
    assert reader.discover().num_rows == 3


def test_reader_read_stream_entrypoint(tmp_path):
    path = tmp_path / "upload.arrow"
    path.write_bytes(
        _write_steps(_initial_steps(), close=False) + _continuation_tail()
    )
    stream = PyArrowIPCReader(datatypes.ArrowIPC(str(path))).read_stream(
        STREAM_ID, STREAM_VERSION
    )
    assert [number for number, _ in stream] == [1, 2, 3, 4]
    assert stream.saw_end_of_stream
