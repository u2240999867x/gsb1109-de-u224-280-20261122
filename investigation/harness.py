"""Investigation harness for batch B314 handoff failures.

Reproduces, against the sh snapshot in ../src, the combinations the operator
sees: detail/receipt files fully on disk, child exit code 0, yet one observer
sees end-of-iteration, another sees only EWOULDBLOCK, and an `await` coroutine
waits forever.  Each run writes one evidence JSON under evidence/ so the same
work ticket can be re-reviewed later.

Mechanics under test (src/sh/__init__.py):
  - get_file_chunk_consumer: every chunk -> handler.write() + flush();
    StreamReader.close() -> finish() -> one final (contentless) flush.
  - StreamReader.close(): pipe_queue.put(None) happens only AFTER finish().
  - output_thread: stdout.close() then stderr.close() then output_complete().
  - thread exceptions land in OProc._output_thread_exc_queue, never re-raised.
  - RunningCommand.wait(timeout=...) only polls is_alive(); a later plain
    wait() is a no-op.
"""

import asyncio
import errno
import hashlib
import json
import os
import shutil
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
import sh  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
EVIDENCE_DIR = os.path.join(HERE, "evidence")
os.makedirs(EVIDENCE_DIR, exist_ok=True)

BATCH = "B314"
DETAIL_LINES = ["A,7", "B,4"]
CK1 = hashlib.md5(b"A,7\n").hexdigest()[:12]
CK2 = hashlib.md5(b"B,4\n").hexdigest()[:12]
RECEIPT_LINE = f"receipt batch={BATCH} lines=1,2 ck1={CK1} ck2={CK2}"
EXPECTED_DETAILS = "A,7\nB,4\n"
EXPECTED_RECEIPT = RECEIPT_LINE + "\n"
FAULT_MSG = "handoff fault: flush failed"  # identical text on both streams

CHILD_PROGRAM = f"""
import sys, time
sys.stderr.write({RECEIPT_LINE!r} + "\\n")
sys.stderr.flush()
time.sleep(0.3)
sys.stdout.write("A,7\\n")
sys.stdout.flush()
time.sleep(0.3)
sys.stdout.write("B,4\\n")
sys.stdout.flush()
time.sleep(0.2)
"""

# exceptions escaping sh's daemon threads (what the operator would see on the
# terminal: one OSError, thread name does not say which stream/flush failed)
THREAD_EXC = []
THREAD_EXC_LOCK = threading.Lock()


def _excepthook(args):
    with THREAD_EXC_LOCK:
        THREAD_EXC.append(
            {
                "thread": args.thread.name if args.thread else "?",
                "exc": type(args.exc_value).__name__,
                "msg": str(args.exc_value),
            }
        )


threading.excepthook = _excepthook


class FaultPlan:
    """Per-stream fault injection.

    modes: ok | fail_empty | park_then_ok | park_then_fail | fail_last_content
    """

    def __init__(self, out_mode="ok", err_mode="ok"):
        self.mode = {"out": out_mode, "err": err_mode}
        self.last_line = {"out": "B,4\n", "err": RECEIPT_LINE + "\n"}
        self.parked = {"out": threading.Event(), "err": threading.Event()}
        self.release = {"out": threading.Event(), "err": threading.Event()}

    def describe(self):
        return {"out": self.mode["out"], "err": self.mode["err"]}


class Receiver:
    """Text receiver without a file descriptor (has encoding/write/flush).

    Content is appended to its file only by a *successful* flush that has
    pending content.  Contentless flushes may be parked and later released
    (success) or fail with FAULT_MSG.  Content written by earlier successful
    flushes stays readable after a later failure.
    """

    def __init__(self, stream, path, plan, flush_log):
        self.encoding = "utf-8"  # makes sh decode chunks to str for us
        self.stream = stream
        self.path = path
        self.plan = plan
        self.pending = []
        self.flush_log = flush_log  # list shared with the run log

    def write(self, chunk):
        self.pending.append(chunk)

    def _record(self, had, content, result):
        self.flush_log.append(
            {"stream": self.stream, "had_content": had, "content": content, "result": result}
        )

    def flush(self):
        content = "".join(self.pending)
        had = bool(content)
        mode = self.plan.mode[self.stream]

        if mode == "fail_last_content" and had and self.plan.last_line[self.stream] in content:
            self._record(had, content, "OSError")
            raise OSError(FAULT_MSG)

        if not had and mode in ("park_then_ok", "park_then_fail"):
            self._record(had, content, "parked")
            self.plan.parked[self.stream].set()
            self.plan.release[self.stream].wait(timeout=60)
            if mode == "park_then_fail":
                self._record(had, content, "OSError")
                raise OSError(FAULT_MSG)
            self._record(had, content, "ok (released)")
            return

        if not had and mode == "fail_empty":
            self._record(had, content, "OSError")
            raise OSError(FAULT_MSG)

        if had:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(content)
        self.pending = []
        self._record(had, content, "ok")


def read_file(path):
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return ""


def verify_receipt(details_text, receipt_text):
    """Which detail lines are on disk AND confirmed by this batch's receipt."""
    result = {"receipt_on_disk": receipt_text == EXPECTED_RECEIPT, "lines": {}}
    disk_lines = details_text.splitlines()
    for idx, (line, ck) in enumerate((("A,7", CK1), ("B,4", CK2)), start=1):
        on_disk = len(disk_lines) >= idx and disk_lines[idx - 1] == line
        confirmed = (
            on_disk
            and result["receipt_on_disk"]
            and hashlib.md5((line + "\n").encode()).hexdigest()[:12] == ck
        )
        result["lines"][f"line{idx}:{line}"] = {
            "on_disk": on_disk,
            "confirmed_by_receipt": confirmed,
        }
    return result


def wait_until(pred, timeout=15.0, interval=0.02):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return True
        time.sleep(interval)
    return False


def drain_exc_queue(cmd):
    out = []
    q = getattr(cmd.process, "_output_thread_exc_queue", None)
    if q is not None:
        while not q.empty():
            e = q.get()
            out.append(f"{type(e).__name__}: {e}")
    return out


class Run:
    def __init__(self, scenario, run_kind, plan):
        self.scenario = scenario
        self.run_kind = run_kind
        self.plan = plan
        self.dir = os.path.join(EVIDENCE_DIR, "tmp", f"{scenario}__{run_kind}")
        shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir, exist_ok=True)
        self.out_path = os.path.join(self.dir, "details.txt")
        self.err_path = os.path.join(self.dir, "receipt.txt")
        self.log = {
            "batch": BATCH,
            "scenario": scenario,
            "run": run_kind,
            "fault_plan": plan.describe(),
            "flushes": [],
            "thread_exceptions": [],
        }

    def make_cmd(self, **extra):
        recv_out = Receiver("out", self.out_path, self.plan, self.log["flushes"])
        recv_err = Receiver("err", self.err_path, self.plan, self.log["flushes"])
        cmd = sh.Command(sys.executable)(
            "-u",
            "-c",
            CHILD_PROGRAM,
            _out=recv_out,
            _err=recv_err,
            _tee=("out", "err"),  # NOTE: _tee=True tees stdout only (TEE_STDERR={"err",2})
            _tty_out=False,
            **extra,
        )
        return cmd

    def files(self):
        return {"details": read_file(self.out_path), "receipt": read_file(self.err_path)}

    def child_exited(self, cmd):
        return not cmd.is_alive()

    def files_complete(self):
        f = self.files()
        return f["details"] == EXPECTED_DETAILS and f["receipt"] == EXPECTED_RECEIPT

    def coordinate_observe(self, cmd):
        """Use receiver + child progress to reach the observation point.

        Does NOT release a parked flush.  Returns the phase label.
        """
        parked_streams = [s for s in ("out", "err") if self.plan.mode[s].startswith("park")]
        if parked_streams:
            stream = parked_streams[0]
            wait_until(lambda: self.child_exited(cmd))
            wait_until(self.files_complete)
            wait_until(lambda: self.plan.parked[stream].is_set())
            time.sleep(0.6)  # let observers reach steady state
            return f"parked at {stream} contentless flush"
        wait_until(lambda: self.child_exited(cmd))
        if all(m == "ok" for m in self.plan.mode.values()):
            wait_until(self.files_complete)
        time.sleep(0.8)  # let the output thread reach/attempt its final flushes
        return "child exited, flushes attempted"

    def coordinate_release(self):
        for stream in ("out", "err"):
            if self.plan.mode[stream].startswith("park"):
                self.plan.release[stream].set()

    def coordinate(self, cmd):
        phase = self.coordinate_observe(cmd)
        self.coordinate_release()
        return phase, None

    def snapshot_thread_exc(self):
        with THREAD_EXC_LOCK:
            self.log["thread_exceptions"] = list(THREAD_EXC)
            THREAD_EXC.clear()

    def save(self, cmd=None):
        self.snapshot_thread_exc()
        self.log["files"] = self.files()
        self.log["receipt_check"] = verify_receipt(
            self.log["files"]["details"], self.log["files"]["receipt"]
        )
        if cmd is not None:
            self.log["exit_code"] = cmd.process.exit_code
            self.log["output_thread_exc_queue"] = drain_exc_queue(cmd)
            self.log["output_thread_alive"] = cmd.process._output_thread.is_alive()
        path = os.path.join(EVIDENCE_DIR, f"{self.scenario}__{self.run_kind}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.log, f, indent=2, ensure_ascii=False)
        return path


def observe_iter(run, cmd, stream, settle_timeout=4.0):
    rec = {"stream": stream, "lines": [], "ewouldblock": 0, "outcome": "running"}
    stop_note = []

    def worker():
        try:
            for chunk in cmd:
                if chunk == errno.EWOULDBLOCK:
                    rec["ewouldblock"] += 1
                    time.sleep(0.01)
                else:
                    rec["lines"].append(chunk)
            rec["outcome"] = "ended (StopIteration)"
        except Exception as e:  # noqa: BLE001
            rec["outcome"] = f"raised {type(e).__name__}: {e}"
        stop_note.append(True)

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    phase, _released = run.coordinate(cmd)
    rec["phase_at_observation"] = phase
    rec["ewouldblock_at_observation"] = rec["ewouldblock"]
    rec["files_at_observation"] = run.files()
    rec["exit_code_at_observation"] = cmd.process.exit_code
    t.join(timeout=settle_timeout)
    if rec["outcome"] == "running":
        rec["outcome"] = "still only EWOULDBLOCK (no StopIteration)"
    rec["ewouldblock_total"] = rec["ewouldblock"]
    run.log["iter"] = rec
    return rec


def run_iter(scenario, plan, stream):
    run = Run(scenario, f"iter-{stream}", plan)
    cmd = run.make_cmd(_iter_noblock=stream)
    observe_iter(run, cmd, stream)
    return run.save(cmd)


def observe_wait_plain(run, cmd, settle_timeout=4.0):
    rec = {"outcome": "running"}
    done = threading.Event()

    def worker():
        t0 = time.time()
        try:
            cmd.wait()
            rec["outcome"] = f"returned after {time.time() - t0:.2f}s"
        except Exception as e:  # noqa: BLE001
            rec["outcome"] = f"raised {type(e).__name__}: {e}"
        done.set()

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    phase, _ = run.coordinate(cmd)
    rec["phase_at_observation"] = phase
    rec["blocked_at_observation"] = not done.is_set()
    rec["files_at_observation"] = run.files()
    rec["exit_code_at_observation"] = cmd.process.exit_code
    done.wait(timeout=settle_timeout)
    if rec["outcome"] == "running":
        rec["outcome"] = "still blocked"
    run.log["wait_plain"] = rec


def run_wait_plain(scenario, plan):
    run = Run(scenario, "wait-plain", plan)
    cmd = run.make_cmd(_bg=True)
    observe_wait_plain(run, cmd)
    return run.save(cmd)


def run_wait_timeout_first(scenario, plan):
    run = Run(scenario, "wait-timeout-first", plan)
    cmd = run.make_cmd(_bg=True)
    rec = {}
    phase = run.coordinate_observe(cmd)  # observe while flush still parked
    rec["phase_at_observation"] = phase
    t0 = time.time()
    try:
        cmd.wait(timeout=20)
        rec["wait_timeout"] = (
            f"returned after {time.time() - t0:.2f}s, exit_code={cmd.process.exit_code}"
        )
    except Exception as e:  # noqa: BLE001
        rec["wait_timeout"] = f"raised {type(e).__name__}: {e}"
    rec["files_when_wait_timeout_returned"] = run.files()
    rec["flush_log_when_wait_timeout_returned"] = list(run.log["flushes"])
    run.coordinate_release()
    t0 = time.time()
    try:
        cmd.wait()
        rec["plain_wait_afterwards"] = f"returned after {time.time() - t0:.3f}s"
    except Exception as e:  # noqa: BLE001
        rec["plain_wait_afterwards"] = f"raised {type(e).__name__}: {e}"
    rec["files_at_end"] = run.files()
    run.log["wait_timeout_first"] = rec
    return run.save(cmd)


def run_await(scenario, plan):
    run = Run(scenario, "await", plan)
    holder = {}

    async def main():
        cmd = run.make_cmd(_async=True, _return_cmd=True)
        holder["cmd"] = cmd
        task = asyncio.ensure_future(cmd)
        rec = {}

        # coordinate using receiver + child progress, without leaving the loop
        parked_streams = [s for s in ("out", "err") if plan.mode[s].startswith("park")]
        if parked_streams:
            stream = parked_streams[0]
            while cmd.process.exit_code is None:
                cmd.is_alive()
                await asyncio.sleep(0.05)
            while not run.files_complete():
                await asyncio.sleep(0.05)
            while not plan.parked[stream].is_set():
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.6)
            rec["phase_at_observation"] = f"parked at {stream} contentless flush"
        else:
            while cmd.process.exit_code is None:
                cmd.is_alive()
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.8)
            rec["phase_at_observation"] = "child exited, flushes attempted"

        rec["await_done_at_observation"] = task.done()
        rec["files_at_observation"] = run.files()
        rec["exit_code_at_observation"] = cmd.process.exit_code

        if parked_streams:
            plan.release[parked_streams[0]].set()

        done, _pending = await asyncio.wait({task}, timeout=4.0)
        if done:
            try:
                result = task.result()
                rec["final"] = f"await returned, exit_code={result.exit_code}"
            except Exception as e:  # noqa: BLE001
                rec["final"] = f"await raised {type(e).__name__}: {e}"
        else:
            rec["final"] = "await still waiting (aio_output_complete never set)"
            task.cancel()
        run.log["await"] = rec

    asyncio.run(main())
    return run.save(holder["cmd"])


SCENARIOS = {
    "S0-clean": FaultPlan("ok", "ok"),
    "S1-out-empty-flush-fails": FaultPlan("fail_empty", "ok"),
    "S2-err-empty-flush-fails": FaultPlan("ok", "fail_empty"),
    "S3-out-park-then-ok": FaultPlan("park_then_ok", "ok"),
    "S4-out-park-then-fail": FaultPlan("park_then_fail", "ok"),
    "S5-out-last-line-flush-fails": FaultPlan("fail_last_content", "ok"),
    "S6-err-receipt-flush-fails": FaultPlan("ok", "fail_last_content"),
}


def main():
    only = sys.argv[1:] or None
    paths = []
    for scenario, plan in SCENARIOS.items():
        if only and scenario not in only:
            continue
        fresh = lambda: FaultPlan(plan.mode["out"], plan.mode["err"])  # noqa: E731
        paths.append(run_iter(scenario, fresh(), "out"))
        paths.append(run_iter(scenario, fresh(), "err"))
        paths.append(run_await(scenario, fresh()))
        paths.append(run_wait_plain(scenario, fresh()))
        paths.append(run_wait_timeout_first(scenario, fresh()))
        print(f"done {scenario}", flush=True)
    print("\n".join(paths))


if __name__ == "__main__":
    main()
