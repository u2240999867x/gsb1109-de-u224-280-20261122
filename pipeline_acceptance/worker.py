from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline_acceptance.control import append_event, read_control, send_control
from pipeline_acceptance.credentials import (
    JobCredential,
    PipeIdentity,
    PreviewConfirmation,
    StopPermit,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="pipeline-acceptance-worker")
    sub = parser.add_subparsers(dest="stage", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--job-id", required=True)
    common.add_argument("--job-token")
    common.add_argument("--event-file")
    common.add_argument("--pipe-id", required=True)

    unpack = sub.add_parser("unpack", parents=[common])
    unpack.add_argument("--archive", required=True)
    unpack.add_argument("--control-fd", type=int)

    parse = sub.add_parser("parse", parents=[common])
    parse.add_argument("--up-control-fd", type=int)
    parse.add_argument("--down-control-fd", type=int)
    parse.add_argument(
        "--behavior",
        choices=("authorized", "missing-upstream", "bad-record"),
        default="authorized",
    )
    parse.add_argument("--full", action="store_true")

    preview = sub.add_parser("preview", parents=[common])
    preview.add_argument("--control-fd", type=int)
    preview.add_argument("--limit", type=int, default=2)
    preview.add_argument("--full", action="store_true")

    return parser.parse_args(argv)


def _job(args: argparse.Namespace) -> JobCredential:
    token = args.job_token or os.environ["PIPELINE_ACCEPTANCE_JOB_TOKEN"]
    return JobCredential(job_id=args.job_id, token=token)


def _write_json(record: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    sys.stdout.flush()


def _unpack(args: argparse.Namespace) -> int:
    job = _job(args)
    pipe = PipeIdentity(
        segment="unpack->parse",
        pipe_id=args.pipe_id + "-up",
        writer_pid=os.getpid(),
    )
    sent = 0
    broken_pipe = False
    permit_data: dict[str, Any] | None = None

    try:
        with gzip.open(args.archive, "rt", encoding="utf-8") as archive:
            for line in archive:
                sys.stdout.write(line if line.endswith("\n") else line + "\n")
                sys.stdout.flush()
                sent += 1
                if sent % 25 == 0:
                    time.sleep(0.002)
    except BrokenPipeError:
        broken_pipe = True
        try:
            sys.stdout.close()
        except OSError:
            pass

    if broken_pipe:
        permit_data = read_control(args.control_fd, timeout=3.0)
        valid = False
        permit: StopPermit | None = None
        if permit_data is not None:
            permit = StopPermit.from_mapping(permit_data)
            valid = permit.authorizes(job, pipe, secret=job.token)
        append_event(
            args.event_file,
            {
                "type": "writer_broken_pipe",
                "segment": pipe.segment,
                "pipe_id": pipe.pipe_id,
                "sent": sent,
                "permit": permit.to_dict() if permit else None,
                "permit_valid": valid,
            },
        )
        print(
            f"unpack: broken pipe after {sent} records; permit_valid={valid}",
            file=sys.stderr,
            flush=True,
        )

    return 0


def _parse(args: argparse.Namespace) -> int:
    job = _job(args)
    upstream_pipe = PipeIdentity(
        segment="unpack->parse",
        pipe_id=args.pipe_id + "-up",
        reader_pid=os.getpid(),
    )
    forwarded = 0
    upstream_permit_sent = False

    def send_upstream_permit() -> StopPermit:
        permit = StopPermit.issue(job, upstream_pipe, secret=job.token)
        send_control(args.up_control_fd, permit.to_dict())
        append_event(
            args.event_file,
            {"type": "stop_permit", "permit": permit.to_dict()},
        )
        print("parser: sent unpack stop permit", file=sys.stderr, flush=True)
        return permit

    for line in sys.stdin:
        try:
            raw = json.loads(line)
            if not isinstance(raw, dict) or "id" not in raw:
                raise ValueError("record is missing id")
        except (json.JSONDecodeError, ValueError) as exc:
            print(f"parser error: bad record after {forwarded}: {exc}", file=sys.stderr)
            print(
                f"parser diagnostics: offending_line={line.strip()!r}", file=sys.stderr
            )
            if not upstream_permit_sent:
                send_upstream_permit()
            append_event(
                args.event_file,
                {
                    "type": "parse_error",
                    "exit_code": 23,
                    "forwarded": forwarded,
                    "error": str(exc),
                    "line": line.strip(),
                },
            )
            return 23

        _write_json({"id": raw["id"], "payload": raw.get("payload", "")})
        forwarded += 1

        if forwarded == 2 and not args.full:
            preview_permit = read_control(args.down_control_fd, timeout=3.0)
            if preview_permit is not None:
                append_event(
                    args.event_file,
                    {"type": "preview_stop_permit_received", "permit": preview_permit},
                )

            if args.behavior == "authorized":
                send_upstream_permit()
                upstream_permit_sent = True
                sys.stdin.close()
                return 0
            if args.behavior == "missing-upstream":
                print("parser: closing input without upstream permit", file=sys.stderr)
                sys.stdin.close()
                return 0
            if args.behavior == "bad-record":
                continue

    return 0


def _preview(args: argparse.Namespace) -> int:
    job = _job(args)
    downstream_pipe = PipeIdentity(
        segment="parse->preview",
        pipe_id=args.pipe_id + "-down",
        reader_pid=os.getpid(),
    )

    records: list[dict[str, Any]] = []
    for line in sys.stdin:
        record = json.loads(line)
        records.append(record)
        print(f"DISPLAY {json.dumps(record, sort_keys=True)}", flush=True)
        if not args.full and len(records) >= args.limit:
            break

    kind = "full" if args.full else "preview"
    ack = PreviewConfirmation.from_records(
        job.job_id, records, kind=kind, secret=job.token
    )
    append_event(
        args.event_file,
        {
            "type": "confirmation",
            "ack": ack.to_dict(),
            "records": records,
        },
    )
    print(f"ACK {json.dumps(ack.public_dict(), sort_keys=True)}", flush=True)

    if not args.full:
        permit = StopPermit.issue(job, downstream_pipe, secret=job.token)
        send_control(args.control_fd, permit.to_dict())
        append_event(
            args.event_file,
            {"type": "stop_permit", "permit": permit.to_dict()},
        )
        time.sleep(0.05)

    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.stage == "unpack":
        return _unpack(args)
    if args.stage == "parse":
        return _parse(args)
    if args.stage == "preview":
        return _preview(args)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
