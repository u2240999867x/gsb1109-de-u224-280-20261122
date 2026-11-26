from __future__ import annotations

from pipeline_acceptance import (
    JobCredential,
    PipelineAcceptance,
    PipeIdentity,
    PreviewConfirmation,
    StopPermit,
    UNPACK_TO_PARSE,
    PARSE_TO_PREVIEW,
)
from pipeline_acceptance.runner import PipelineRunner


def test_permit_is_scoped_to_job_segment_and_pipe() -> None:
    job = JobCredential.create("job-scope")
    verifier = PipelineAcceptance(job, secret=job.token)

    parse_pipe = type(
        "Pipe",
        (),
        {"segment": PARSE_TO_PREVIEW, "pipe_id": "pipe-down"},
    )()
    wrong_pipe = type(
        "Pipe",
        (),
        {"segment": UNPACK_TO_PARSE, "pipe_id": "pipe-up"},
    )()
    permit = StopPermit.issue(job, parse_pipe, secret=job.token)

    assert permit.authorizes(job, parse_pipe, secret=job.token)
    assert not permit.authorizes(job, wrong_pipe, secret=job.token)
    assert not permit.authorizes(JobCredential.create("other-job"), parse_pipe)

    verifier.add_permit(permit)
    assert verifier.segment(PARSE_TO_PREVIEW).permit == permit
    assert verifier.segment(PARSE_TO_PREVIEW).exit_code is None


def test_verdict_is_independent_of_segment_insertion_order() -> None:
    job = JobCredential.create("job-order")
    records = [{"id": 1}, {"id": 2}]

    preview_ack = PreviewConfirmation.from_records(
        job.job_id, records, secret=job.token
    )
    upstream_permit = StopPermit.issue(
        job,
        PipeIdentity(segment=UNPACK_TO_PARSE, pipe_id="up"),
        secret=job.token,
    )
    downstream_permit = StopPermit.issue(
        job,
        PipeIdentity(segment=PARSE_TO_PREVIEW, pipe_id="down"),
        secret=job.token,
    )

    first = PipelineAcceptance(job, secret=job.token)
    first.retain_records(records)
    first.acknowledge_preview(preview_ack)
    first.record_segment("preview", exit_code=0)
    first.add_permit(downstream_permit)
    first.add_permit(upstream_permit)
    first.mark_processes_complete()

    second = PipelineAcceptance(job, secret=job.token)
    second.retain_records(records)
    second.acknowledge_preview(preview_ack)
    second.add_permit(upstream_permit)
    second.add_permit(downstream_permit)
    second.record_segment("preview", exit_code=0)
    second.mark_processes_complete()

    first_report = first.finalize().to_dict()
    second_report = second.finalize().to_dict()

    assert first_report["status"] == "delivered"
    assert [segment["segment"] for segment in first_report["segments"]] == [
        "unpack->parse",
        "parse->preview",
        "preview",
    ]
    assert first_report["segments"] == second_report["segments"]
    assert first_report["verdict"] == second_report["verdict"]


def test_preview_permit_does_not_satisfy_full_mode() -> None:
    job = JobCredential.create("job-full")
    records = [{"id": index} for index in range(5)]
    acceptance = PipelineAcceptance(job, mode="full", secret=job.token)
    acceptance.retain_records(records)
    acceptance.acknowledge_preview(
        PreviewConfirmation.from_records(job.job_id, records[:2], secret=job.token)
    )
    acceptance.mark_processes_complete()

    report = acceptance.finalize()
    assert not report.delivered
    assert "missing_valid_full_confirmation" in report.verdict.reasons
    assert "preview_permit_cannot_replace_full_confirmation" in report.verdict.reasons


def test_twin_previews_only_authorized_chain_is_delivered() -> None:
    with PipelineRunner() as runner:
        first, second = runner.twin_previews()

    first_dict = first.to_dict()
    second_dict = second.to_dict()

    assert first.delivered
    assert first.status == "delivered"
    assert second.status == "missing_permit"
    assert not second.delivered
    assert second.verdict.missing_permits == ("unpack->parse",)

    assert len(first.records) == 2
    assert len(second.records) == 2
    assert [record["id"] for record in first.records] == [1, 2]
    assert [record["id"] for record in second.records] == [1, 2]
    assert first_dict["preview_ack"]["job_id"] == "preview-first"
    assert second_dict["preview_ack"]["job_id"] == "preview-second"
    assert first_dict["preview_ack"]["count"] == 2
    assert second_dict["preview_ack"]["count"] == 2
    assert first_dict["preview_ack"]["digest"] == second_dict["preview_ack"]["digest"]

    first_codes = {
        segment["segment"]: segment["exit_code"] for segment in first_dict["segments"]
    }
    second_codes = {
        segment["segment"]: segment["exit_code"] for segment in second_dict["segments"]
    }
    assert (
        first_codes
        == second_codes
        == {
            "unpack->parse": 0,
            "parse->preview": 0,
            "preview": 0,
        }
    )
    assert second.segment("unpack->parse").broken_pipe
    assert "unpack->parse" in second_dict["diagnostics"]


def test_bad_record_remains_pending_with_diagnostics() -> None:
    with PipelineRunner() as runner:
        report = runner.run("bad-record", job_id="bad-record-job")

    data = report.to_dict()
    parser = report.segment("parse->preview")
    unpack = report.segment("unpack->parse")

    assert report.status == "pending"
    assert not report.delivered
    assert parser.exit_code == 23
    assert "parser error" in parser.stderr
    assert "offending_line" in parser.stderr
    assert unpack.broken_pipe
    assert unpack.permit_valid is True
    assert len(report.records) == 2
    assert data["preview_ack"]["count"] == 2


def test_full_pipeline_requires_terminal_confirmation() -> None:
    with PipelineRunner() as runner:
        report = runner.run("authorized", job_id="full-job", mode="full")

    assert report.delivered
    assert report.full_ack is not None
    assert report.full_ack.kind == "full"
    assert report.full_ack.count == 160
