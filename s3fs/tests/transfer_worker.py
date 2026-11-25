"""Subprocess worker for cross-process resumable download tests.

Each invocation is a brand-new interpreter that only shares the destination and
its ``.s3dl`` sidecar directory with the previous process. The in-memory S3
stand-in is rebuilt per process from the same fixed object versions.
"""

import argparse
import asyncio
import json
import os
import sys
from io import BytesIO

from s3fs import S3FileSystem

V1 = b"AAAABBBBCCCC"
V2 = b"DDDDEEEEFFFF"


class Body:
    def __init__(self, data):
        self.stream = BytesIO(data)
        self.closed = False

    async def read(self, size=-1):
        return self.stream.read(size)

    def close(self):
        self.closed = True
        self.stream.close()


class KilledBody:
    def __init__(self, parts_ready):
        self.parts_ready = parts_ready
        self.closed = False

    async def read(self, size=-1):
        while not self.parts_ready():
            await asyncio.sleep(0)
        raise RuntimeError("process terminated mid-download")

    def close(self):
        self.closed = True


class FakeS3:
    def __init__(self, target, kill_middle):
        self.versions = [("v1", V1), ("v2", V2)]
        self.ranges = []
        self._sidecar = target + ".s3dl"
        self._kill_middle = kill_middle

    def _parts_on_disk(self):
        count = 0
        for _root, _dirs, files in os.walk(self._sidecar):
            count += sum(1 for name in files if name.startswith("part-"))
        return count

    def head_object(self, Bucket, Key, VersionId=None, **kwargs):
        vid = VersionId or self.versions[-1][0]
        data = dict(self.versions)[vid]
        return {"ContentLength": len(data), "ETag": '"et"', "VersionId": vid}

    def get_object(self, Bucket, Key, Range=None, VersionId=None, **kwargs):
        vid = VersionId or self.versions[-1][0]
        data = dict(self.versions)[vid]
        if Range:
            start, end = map(int, Range.removeprefix("bytes=").split("-"))
            self.ranges.append((start, end))
            if self._kill_middle and (start, end) == (4, 7):
                return {
                    "Body": KilledBody(lambda: self._parts_on_disk() >= 2),
                    "ContentLength": 0,
                }
            data = data[start : end + 1]
        return {"Body": Body(data), "ContentLength": len(data)}

    async def __call__(self, method, **kwargs):
        return getattr(self, method)(**kwargs)


async def _run(args):
    backend = FakeS3(args.target, kill_middle=args.stage == "partial")
    fs = S3FileSystem(asynchronous=True, skip_instance_cache=True)
    fs._call_s3 = backend
    try:
        await fs._get_file(
            "bucket/incoming/daily.dat",
            args.target,
            chunksize=args.chunksize,
            max_concurrency=3,
            transfer_id="B",
        )
    except RuntimeError as exc:
        if "terminated" in str(exc):
            print(json.dumps({"killed": True, "ranges": backend.ranges}))
            return 2
        raise
    print(json.dumps({"killed": False, "ranges": backend.ranges}))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True)
    parser.add_argument("--stage", choices=["partial", "resume", "confirm"])
    parser.add_argument("--chunksize", type=int, default=4)
    args = parser.parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
