from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import sys
import threading
import traceback

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

import sh  # noqa: E402

sh_real = sys.modules["sh"]._SelfWrapper__self_module

ART = ROOT / "handoff_artifacts"
ERR_HEX = b"MODE\nCRC=ok".hex()

A8 = b"A,00000\n"
B_E7 = b"B,\xe7"
TAIL = b"\x94\xb2\nC,3\n"


class DiskFlushError(Exception):
    pass


class TextSink:
    """UTF-8 text sink without a file descriptor."""

    encoding = "utf-8"

    def __init__(self, path: Path, fail: str | None):
        self.path = path
        self.fail = fail
        self.calls: list[dict] = []
        self.pending = ""
        self.writes_since_flush = 0
        self.nonempty_writes = 0
        self.flush_count = 0

    def write(self, text: str) -> int:
        self.calls.append({"op": "write", "text": text, "hex": text.encode("utf-8").hex()})
        self.pending += text
        self.writes_since_flush += 1
        if text:
            self.nonempty_writes += 1
        return len(text)

    def flush(self) -> None:
        self.flush_count += 1
        paired = self.writes_since_flush > 0
        self.calls.append(
            {
                "op": "flush_enter",
                "n": self.flush_count,
                "paired_write": paired,
                "pending": self.pending,
            }
        )

        if self.fail == "second" and self.flush_count == 2:
            err = DiskFlushError("simulated disk flush failure: sink commit failed")
            self.calls.append({"op": "flush_error", "n": self.flush_count, "error": str(err)})
            raise err

        if self.fail == "second_nonempty" and paired and self.nonempty_writes == 2:
            err = DiskFlushError("simulated disk flush failure: sink commit failed")
            self.calls.append({"op": "flush_error", "n": self.flush_count, "error": str(err)})
            raise err

        if self.fail == "unpaired" and not paired:
            err = DiskFlushError("simulated disk flush failure: sink commit failed")
            self.calls.append({"op": "flush_error", "n": self.flush_count, "error": str(err)})
            raise err

        with self.path.open("a", encoding="utf-8", newline="") as fh:
            fh.write(self.pending)
        self.calls.append({"op": "append", "n": self.flush_count, "text": self.pending})
        self.pending = ""
        self.writes_since_flush = 0
        self.calls.append({"op": "flush_ok", "n": self.flush_count})


CONVERTER = ART / "converter_child.py"
CONVERTER_TEXT = '''
import json
import os
from pathlib import Path
import time
import sys

spec_path = Path(sys.argv[1])
d = spec_path.parent
spec = json.loads(spec_path.read_text())
trace_path = d / "child_writes.jsonl"

def trace(rec):
    with trace_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\\n")

def write_all(fd, data, name):
    view = memoryview(data)
    offset = 0
    while view:
        n = os.write(fd, view)
        trace({"name": name, "requested": len(data), "offset": offset, "n": n,
               "hex": bytes(view[:n]).hex()})
        view = view[n:]
        offset += n

for i, chunk_hex in enumerate(spec["early"]):
    write_all(1, bytes.fromhex(chunk_hex), f"early-{i}")
(d / "child_pre_done").write_text("done\\n")

deadline = time.time() + 10
while not (d / "read_barrier").exists():
    if time.time() > deadline:
        raise SystemExit("timed out waiting for read_barrier")
    time.sleep(0.002)

write_all(2, bytes.fromhex(spec["err"]), "stderr-mode-crc")

deadline = time.time() + 10
while not (d / "release_barrier").exists():
    if time.time() > deadline:
        raise SystemExit("timed out waiting for release_barrier")
    time.sleep(0.002)

for i, chunk_hex in enumerate(spec["late"]):
    write_all(1, bytes.fromhex(chunk_hex), f"late-{i}")

trace({"name": "exit", "code": 0})
os._exit(0)
'''

READS: list[dict] = []
READ_HOOKS: list = []
THREAD_ERRORS: list[str] = []
_orig_no_interrupt = sh_real.no_interrupt
_orig_thread_excepthook = threading.excepthook


def traced_no_interrupt(func, *args, **kwargs):
    result = _orig_no_interrupt(func, *args, **kwargs)
    if func is os.read and args:
        fd = args[0]
        requested = args[1] if len(args) > 1 else None
        chunk = bytes(result)
        READS.append({"fd": fd, "requested": requested, "n": len(chunk), "hex": chunk.hex(), "repr": repr(chunk)})
        for hook in list(READ_HOOKS):
            hook(fd, chunk)
    return result


def recording_excepthook(args):
    THREAD_ERRORS.append(
        "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
    )


sh_real.no_interrupt = traced_no_interrupt
threading.excepthook = recording_excepthook


def bhex(data: bytes) -> str:
    return data.hex()


def buffer_bytes(stream) -> bytes:
    if stream is None:
        return b""
    return b"".join(stream.stream_bufferer.buffer)


def run_case(case: dict) -> dict:
    case_id = case["id"]
    d = ART / case_id
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)

    detail = d / "details.txt"
    detail.write_bytes(b"")

    early_bytes = b"".join(case["early"])
    late_bytes = b"".join(case["late"])
    spec = {
        "early": [bhex(x) for x in case["early"]],
        "err": ERR_HEX,
        "late": [bhex(x) for x in case["late"]],
    }
    spec_path = d / "spec.json"
    spec_path.write_text(json.dumps(spec, indent=2), encoding="utf-8")

    READS.clear()
    THREAD_ERRORS.clear()
    read_acc: dict[int, bytes] = {}
    barrier = d / "read_barrier"

    def read_hook(fd: int, chunk: bytes) -> None:
        read_acc[fd] = read_acc.get(fd, b"") + chunk
        if any(acc == early_bytes for acc in read_acc.values()):
            barrier.write_text("parent has exactly read all early bytes\n", encoding="utf-8")

    READ_HOOKS.append(read_hook)

    err_calls: list[str] = []

    def on_err(text: str, stdin, process) -> None:
        err_calls.append(text)
        if "MODE" in text:
            if case.get("switch", True):
                process.change_out_bufsize(0)
            (d / "release_barrier").write_text("released after MODE handling\n", encoding="utf-8")

    sink = TextSink(detail, case.get("fail"))
    py = sh.Command(sys.executable)

    p = py(
        str(CONVERTER),
        str(spec_path),
        _tty_out=False,
        _out=sink,
        _err=on_err,
        _out_bufsize=8,
        _err_bufsize=1,
        _tee=("out", "err"),
        _internal_bufsize=1,
        _no_pipe=True,
        _decode_errors="ignore",
        _return_cmd=True,
    )

    READ_HOOKS.remove(read_hook)
    p.wait()

    proc = p.process
    out_fd = proc._stdout_stream.stream if proc._stdout_stream else None
    err_fd = proc._stderr_stream.stream if proc._stderr_stream else None
    out_reads = [r for r in READS if r["fd"] == out_fd]
    err_reads = [r for r in READS if r["fd"] == err_fd]

    exc_items = []
    while not proc._output_thread_exc_queue.empty():
        exc = proc._output_thread_exc_queue.get_nowait()
        exc_items.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))

    file_bytes = detail.read_bytes()
    result = {
        "id": case_id,
        "description": case.get("description", ""),
        "switch": case.get("switch", True),
        "fail": case.get("fail"),
        "exit_code": p.exit_code,
        "early_expected_hex": early_bytes.hex(),
        "late_expected_hex": late_bytes.hex(),
        "complete_expected_text": case.get("expected_text", ""),
        "child_writes": [
            json.loads(line)
            for line in (d / "child_writes.jsonl").read_text(encoding="utf-8").splitlines()
        ],
        "parent_out_reads": out_reads,
        "parent_err_reads": err_reads,
        "err_callback_calls": err_calls,
        "sink_calls": sink.calls,
        "detail_file_hex": file_bytes.hex(),
        "detail_file_text": file_bytes.decode("utf-8", "replace"),
        "tee_out_hex": bytes(p.stdout).hex(),
        "tee_out_repr": repr(bytes(p.stdout)),
        "tee_err_hex": bytes(p.stderr).hex(),
        "tee_err_repr": repr(bytes(p.stderr)),
        "stdout_deque": [repr(x) for x in list(proc._stdout)],
        "stderr_deque": [repr(x) for x in list(proc._stderr)],
        "stdout_streamer_buffer_hex": buffer_bytes(proc._stdout_stream).hex(),
        "stderr_streamer_buffer_hex": buffer_bytes(proc._stderr_stream).hex(),
        "output_thread_exc_queue": exc_items,
        "threading_excepthook": list(THREAD_ERRORS),
        "artifacts_dir": str(d),
    }

    (d / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (d / "tee_out.bin").write_bytes(bytes(p.stdout))
    (d / "tee_err.bin").write_bytes(bytes(p.stderr))
    return result


def main() -> None:
    ART.mkdir(exist_ok=True)
    CONVERTER.write_text(CONVERTER_TEXT, encoding="utf-8")

    jia = "甲".encode("utf-8")
    cases = [
        {
            "id": "01_second_flush_fails",
            "description": "Base alignment; paired second flush after B prefix fails",
            "early": [A8, B_E7],
            "late": [TAIL],
            "fail": "second",
            "switch": True,
            "expected_text": "A,00000\nB,甲\nC,3\n",
        },
        {
            "id": "01b_second_nonempty_paired_flush_fails",
            "description": "A committed; B prefix reaches write() but its paired flush fails",
            "early": [A8, B_E7],
            "late": [TAIL],
            "fail": "second_nonempty",
            "switch": True,
            "expected_text": "A,00000\nB,甲\nC,3\n",
        },
        {
            "id": "02_unpaired_closing_flush_fails",
            "description": "All paired flushes succeed; final finish()-only flush fails",
            "early": [A8, B_E7],
            "late": [TAIL],
            "fail": "unpaired",
            "switch": True,
            "expected_text": "A,00000\nB,甲\nC,3\n",
        },
        {
            "id": "03_switched_healthy_sink",
            "description": "Base alignment with switch and no sink error",
            "early": [A8, B_E7],
            "late": [TAIL],
            "fail": None,
            "switch": True,
            "expected_text": "A,00000\nB,甲\nC,3\n",
        },
        {
            "id": "04_no_switch_healthy_sink",
            "description": "Fixed 8-byte buffering reassembles the split character",
            "early": [A8, B_E7],
            "late": [TAIL],
            "fail": None,
            "switch": False,
            "expected_text": "A,00000\nB,甲\nC,3\n",
        },
        {
            "id": "05_len8_zero_jia_before_switch",
            "description": "First line length 8; all three JIA bytes arrive after switch",
            "early": [A8, b"B,"],
            "late": [jia + b"\nC,3\n"],
            "fail": None,
            "switch": True,
            "expected_text": "A,00000\nB,甲\nC,3\n",
        },
        {
            "id": "06_len8_two_jia_before_switch",
            "description": "First line length 8; two JIA bytes before switch, one after",
            "early": [A8, b"B," + jia[:2]],
            "late": [jia[2:] + b"\nC,3\n"],
            "fail": None,
            "switch": True,
            "expected_text": "A,00000\nB,甲\nC,3\n",
        },
        {
            "id": "07_len8_three_jia_before_switch",
            "description": "First line length 8; all three JIA bytes buffered before switch",
            "early": [A8, b"B," + jia],
            "late": [b"\nC,3\n"],
            "fail": None,
            "switch": True,
            "expected_text": "A,00000\nB,甲\nC,3\n",
        },
        {
            "id": "08_len7_one_jia_byte_before_switch",
            "description": "Shorter first line shifts the 8-byte boundary into B",
            "early": [b"A,0000\n", B_E7],
            "late": [TAIL],
            "fail": None,
            "switch": True,
            "expected_text": "A,0000\nB,甲\nC,3\n",
        },
        {
            "id": "09_len9_one_jia_byte_before_switch",
            "description": "Longer first line shifts newline after the first 8-byte chunk",
            "early": [b"A,000000\n", B_E7],
            "late": [TAIL],
            "fail": None,
            "switch": True,
            "expected_text": "A,000000\nB,甲\nC,3\n",
        },
    ]

    summaries = []
    for case in cases:
        result = run_case(case)
        summaries.append(
            {
                "id": result["id"],
                "exit": result["exit_code"],
                "detail": result["detail_file_text"],
                "tee_out": result["tee_out_repr"],
                "tee_err": result["tee_err_repr"],
                "err_cb": result["err_callback_calls"],
                "stderr_buffer": result["stderr_streamer_buffer_hex"],
                "exc": bool(result["output_thread_exc_queue"]),
            }
        )
        print(f"\n===== {result['id']} =====")
        print("exit:", result["exit_code"], "exc:", bool(result["output_thread_exc_queue"]))
        print("out reads:", [(r["n"], r["repr"]) for r in result["parent_out_reads"]])
        print("err reads:", [(r["n"], r["repr"]) for r in result["parent_err_reads"]])
        print("err callbacks:", result["err_callback_calls"])
        print("sink calls:", [(c["op"], c.get("n"), c.get("text", c.get("paired_write"))) for c in result["sink_calls"]])
        print("file:", repr(result["detail_file_text"]))
        print("tee out:", result["tee_out_repr"])
        print("tee err:", result["tee_err_repr"])
        print("stderr buffer hex:", result["stderr_streamer_buffer_hex"])

    (ART / "summary.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
