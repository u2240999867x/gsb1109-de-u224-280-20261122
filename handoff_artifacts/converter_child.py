
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
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

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
(d / "child_pre_done").write_text("done\n")

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
