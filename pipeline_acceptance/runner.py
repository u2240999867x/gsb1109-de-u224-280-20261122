from __future__ import annotations

import gzip
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import sh

from .control import make_control_pair
from .credentials import JobCredential, PreviewConfirmation, StopPermit
from .report import AcceptanceReport
from .verifier import PipelineAcceptance

ROOT = Path(__file__).resolve().parent.parent


def _python_command() -> sh.Command:
    return sh.Command(sys.executable)


def _write_archive(path: Path, *, bad_third: bool = False, records: int = 160) -> Path:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for index in range(1, records + 1):
            if bad_third and index == 3:
                handle.write("{not-json\n")
            else:
                payload = "x" * 2048
                handle.write(json.dumps({"id": index, "payload": payload}) + "\n")
    return path


def _read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            events.append(json.loads(line))
    return events


class PipelineRunner:
    def __init__(self, workdir: str | Path | None = None) -> None:
        self.workdir = Path(workdir or tempfile.mkdtemp(prefix="pipe-acceptance-"))
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.python = _python_command()

    def close(self) -> None:
        if self.workdir.name.startswith("pipe-acceptance-"):
            shutil.rmtree(self.workdir, ignore_errors=True)

    def __enter__(self) -> "PipelineRunner":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def run(
        self,
        behavior: str = "authorized",
        *,
        job_id: str | None = None,
        archive: str | Path | None = None,
        mode: str = "preview",
        timeout: float = 20.0,
    ) -> AcceptanceReport:
        job = JobCredential.create(job_id)
        run_dir = self.workdir / job.job_id
        run_dir.mkdir(parents=True, exist_ok=True)
        event_file = run_dir / "events.jsonl"
        event_file.write_text("", encoding="utf-8")

        if archive is None:
            archive_path = run_dir / "records.jsonl.gz"
            _write_archive(archive_path, bad_third=(behavior == "bad-record"))
        else:
            archive_path = Path(archive)

        up_writer, up_reader = make_control_pair()
        down_writer, down_reader = make_control_pair()
        up_writer_fd = up_writer.fileno()
        up_reader_fd = up_reader.fileno()
        down_writer_fd = down_writer.fileno()
        down_reader_fd = down_reader.fileno()

        env = os.environ.copy()
        pythonpath_parts = [str(ROOT), str(ROOT / "src")]
        if env.get("PYTHONPATH"):
            pythonpath_parts.append(env["PYTHONPATH"])
        env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
        env["PIPELINE_ACCEPTANCE_JOB_TOKEN"] = job.token

        common = {
            "_env": env,
            "_tty_out": False,
            "_bg": True,
            "_bg_exc": False,
            "_ok_code": list(range(-128, 256)),
        }

        pass_fds_unpack = {up_writer_fd}
        unpack_cmd = self.python(
            "-u",
            "-m",
            "pipeline_acceptance.worker",
            "unpack",
            "--job-id",
            job.job_id,
            "--event-file",
            str(event_file),
            "--pipe-id",
            job.job_id,
            "--archive",
            str(archive_path),
            "--control-fd",
            str(up_writer_fd),
            _piped=True,
            _pass_fds=pass_fds_unpack,
            **common,
        )
        up_writer.close()

        parser_fds = {up_reader_fd, down_writer_fd}
        parser_cmd = self.python(
            "-u",
            "-m",
            "pipeline_acceptance.worker",
            "parse",
            "--job-id",
            job.job_id,
            "--event-file",
            str(event_file),
            "--pipe-id",
            job.job_id,
            "--up-control-fd",
            str(up_reader_fd),
            "--down-control-fd",
            str(down_writer_fd),
            "--behavior",
            behavior,
            *(["--full"] if mode == "full" else []),
            _in=unpack_cmd,
            _piped=True,
            _pass_fds=parser_fds,
            **common,
        )
        up_reader.close()
        down_writer.close()

        preview_fds = {down_reader_fd}
        preview_args = [
            "-u",
            "-m",
            "pipeline_acceptance.worker",
            "preview",
            "--job-id",
            job.job_id,
            "--event-file",
            str(event_file),
            "--pipe-id",
            job.job_id,
            "--control-fd",
            str(down_reader_fd),
        ]
        if mode == "full":
            preview_args.append("--full")
        preview_cmd = self.python(
            *preview_args,
            _in=parser_cmd,
            _pass_fds=preview_fds,
            **common,
        )
        down_reader.close()

        commands = {
            "unpack->parse": unpack_cmd,
            "parse->preview": parser_cmd,
            "preview": preview_cmd,
        }

        for command in commands.values():
            process = command.process
            assert process is not None
            process.wait()

        events = _read_events(event_file)
        acceptance = PipelineAcceptance(job, mode=mode, secret=job.token)

        records: list[Any] = []
        for event in events:
            event_type = event.get("type")
            if event_type == "confirmation":
                ack = PreviewConfirmation.from_mapping(event["ack"])
                records = list(event.get("records", []))
                acceptance.retain_records(records)
                if ack.kind == "full":
                    acceptance.acknowledge_full(ack)
                else:
                    acceptance.acknowledge_preview(ack)
            elif event_type == "stop_permit":
                acceptance.add_permit(StopPermit.from_mapping(event["permit"]))
            elif event_type == "writer_broken_pipe":
                permit = event.get("permit")
                acceptance.record_segment(
                    event["segment"],
                    broken_pipe=True,
                    permit=permit,
                    permit_valid=event.get("permit_valid"),
                    sent=event.get("sent"),
                )
            elif event_type == "parse_error":
                acceptance._extra["parse_error"] = event

        for segment, command in commands.items():
            if segment == "preview":
                continue
            process = command.process
            stderr_bytes = process.stderr if process else b""
            stdout_bytes = process.stdout if process else b""
            acceptance.record_segment(
                segment,
                exit_code=command.exit_code,
                stderr=stderr_bytes.decode("utf-8", "replace"),
                stdout=stdout_bytes.decode("utf-8", "replace"),
            )

        preview_process = preview_cmd.process
        acceptance.record_segment(
            "preview",
            exit_code=preview_cmd.exit_code,
            stderr=(preview_process.stderr if preview_process else b"").decode(
                "utf-8", "replace"
            ),
            stdout=(preview_process.stdout if preview_process else b"").decode(
                "utf-8", "replace"
            ),
        )
        acceptance.mark_credentials_complete(True)
        acceptance.mark_processes_complete(True)
        return acceptance.finalize()

    def twin_previews(self) -> tuple[AcceptanceReport, AcceptanceReport]:
        archive = self.workdir / "same-archive.jsonl.gz"
        _write_archive(archive)
        first = self.run("authorized", job_id="preview-first", archive=archive)
        second = self.run("missing-upstream", job_id="preview-second", archive=archive)
        return first, second


def run_scenario(behavior: str = "authorized", **kwargs: Any) -> AcceptanceReport:
    with PipelineRunner() as runner:
        return runner.run(behavior, **kwargs)
