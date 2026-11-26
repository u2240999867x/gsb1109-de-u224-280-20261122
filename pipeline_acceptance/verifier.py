from __future__ import annotations

import threading
from typing import Any, Iterable, Mapping

from .credentials import (
    JobCredential,
    PreviewConfirmation,
    StopPermit,
    normalize_segment,
)
from .report import (
    DELIVERED,
    FAILED,
    INCOMPLETE,
    MISSING_PERMIT,
    PENDING,
    AcceptanceReport,
    AcceptanceVerdict,
    SegmentDiagnostics,
)

REQUIRED_PREVIEW_PERMITS = ("parse->preview", "unpack->parse")
CANONICAL_SEGMENTS = ("unpack->parse", "parse->preview")


class PipelineAcceptance:
    """Collect facts first and derive one order-independent verdict."""

    def __init__(
        self,
        job: JobCredential | str,
        *,
        mode: str = "preview",
        secret: str | bytes | None = None,
        expected_preview_count: int = 2,
    ) -> None:
        self.job = job if isinstance(job, JobCredential) else JobCredential(str(job))
        self.mode = "full" if mode == "full" else "preview"
        self.secret = secret
        self.expected_preview_count = expected_preview_count
        self._lock = threading.RLock()
        self._segments: dict[str, SegmentDiagnostics] = {}
        self._permits: dict[str, StopPermit] = {}
        self._preview_ack: PreviewConfirmation | None = None
        self._full_ack: PreviewConfirmation | None = None
        self._records: list[Any] = []
        self._displayed_records: list[Any] = []
        self._credentials_complete = False
        self._processes_complete = False
        self._waited_commands: list[Any] = []
        self._extra: dict[str, Any] = {}

    @property
    def job_id(self) -> str:
        return self.job.job_id

    def retain_records(self, records: Iterable[Any]) -> "PipelineAcceptance":
        with self._lock:
            self._records = list(records)
            if not self._displayed_records:
                self._displayed_records = list(self._records)
        return self

    record_preview = retain_records

    def displayed(self, records: Iterable[Any]) -> "PipelineAcceptance":
        with self._lock:
            self._displayed_records = list(records)
            if not self._records:
                self._records = list(records)
        return self

    def display_record(self, record: Any) -> "PipelineAcceptance":
        with self._lock:
            self._displayed_records.append(record)
            self._records.append(record)
        return self

    def display(self, record: Any) -> "PipelineAcceptance":
        return self.display_record(record)

    def acknowledge_preview(
        self, ack: PreviewConfirmation | Mapping[str, Any]
    ) -> "PipelineAcceptance":
        confirmation = (
            ack
            if isinstance(ack, PreviewConfirmation)
            else PreviewConfirmation.from_mapping(ack)
        )
        with self._lock:
            self._preview_ack = confirmation
        return self

    def acknowledge_full(
        self, ack: PreviewConfirmation | Mapping[str, Any]
    ) -> "PipelineAcceptance":
        confirmation = (
            ack
            if isinstance(ack, PreviewConfirmation)
            else PreviewConfirmation.from_mapping({**dict(ack), "kind": "full"})
            if isinstance(ack, Mapping)
            else ack
        )
        object.__setattr__(confirmation, "kind", "full")
        with self._lock:
            self._full_ack = confirmation
        return self

    acknowledge = acknowledge_preview

    def add_permit(
        self, permit: StopPermit | Mapping[str, Any]
    ) -> "PipelineAcceptance":
        stop_permit = (
            permit
            if isinstance(permit, StopPermit)
            else StopPermit.from_mapping(permit)
        )
        with self._lock:
            self._permits[stop_permit.segment] = stop_permit
            existing = self._segments.get(stop_permit.segment)
            valid = stop_permit.authorizes(self.job, secret=self.secret)
            if existing is not None:
                self._segments[stop_permit.segment] = SegmentDiagnostics(
                    **{
                        **existing.to_dict(),
                        "permit": stop_permit,
                        "permit_valid": valid,
                    }
                )
            else:
                self._segments[stop_permit.segment] = SegmentDiagnostics(
                    segment=stop_permit.segment,
                    permit=stop_permit,
                    permit_valid=valid,
                )
        return self

    def record_segment(
        self,
        segment_or_report: str | SegmentDiagnostics | Mapping[str, Any],
        *,
        exit_code: int | None = None,
        stderr: str = "",
        stdout: str = "",
        broken_pipe: bool | None = None,
        permit: StopPermit | Mapping[str, Any] | None = None,
        error: str | None = None,
        **extra: Any,
    ) -> "PipelineAcceptance":
        if isinstance(segment_or_report, SegmentDiagnostics):
            report = segment_or_report
        elif isinstance(segment_or_report, Mapping):
            report = SegmentDiagnostics.from_mapping(segment_or_report)
        else:
            segment = normalize_segment(segment_or_report)
            permit_obj = None
            if permit is not None:
                permit_obj = (
                    permit
                    if isinstance(permit, StopPermit)
                    else StopPermit.from_mapping(permit)
                )
            had_broken_pipe = broken_pipe
            if had_broken_pipe is None:
                had_broken_pipe = exit_code in (-13, 141) or "BrokenPipe" in stderr
            report = SegmentDiagnostics(
                segment=segment,
                exit_code=exit_code,
                stderr=stderr,
                stdout=stdout,
                broken_pipe=bool(had_broken_pipe),
                permit=permit_obj,
                error=error,
                extra=extra,
            )

        with self._lock:
            previous = self._segments.get(report.segment)
            if previous is not None:
                merged_extra = {**previous.extra, **report.extra}
                report = SegmentDiagnostics(
                    segment=report.segment,
                    exit_code=(
                        report.exit_code
                        if report.exit_code is not None
                        else previous.exit_code
                    ),
                    stderr=(report.stderr if report.stderr else previous.stderr),
                    stdout=report.stdout or previous.stdout,
                    broken_pipe=report.broken_pipe or previous.broken_pipe,
                    permit=report.permit or previous.permit,
                    permit_valid=(
                        report.permit_valid
                        if report.permit_valid is not None
                        else previous.permit_valid
                    ),
                    error=report.error or previous.error,
                    extra=merged_extra,
                )

            if report.permit is not None:
                self._permits[report.permit.segment] = report.permit
            elif report.segment in self._permits:
                report = SegmentDiagnostics(
                    **{
                        **report.to_dict(),
                        "permit": self._permits[report.segment],
                    }
                )

            if report.permit is not None and report.permit_valid is None:
                valid = report.permit.authorizes(self.job, secret=self.secret)
                report = SegmentDiagnostics(
                    **{**report.to_dict(), "permit_valid": valid}
                )

            self._segments[report.segment] = report
        return self

    def attach(self, segment: str, command: Any) -> "PipelineAcceptance":
        with self._lock:
            self._waited_commands.append((segment, command))
        return self

    def mark_credentials_complete(self, value: bool = True) -> "PipelineAcceptance":
        with self._lock:
            self._credentials_complete = value
        return self

    def mark_processes_complete(self, value: bool = True) -> "PipelineAcceptance":
        with self._lock:
            self._processes_complete = value
        return self

    def segment(self, name: str) -> SegmentDiagnostics | None:
        with self._lock:
            return self._segments.get(normalize_segment(name))

    def get_segment(self, name: str) -> SegmentDiagnostics | None:
        return self.segment(name)

    def _wait_attached(self) -> None:
        for segment, command in list(self._waited_commands):
            try:
                wait = getattr(command, "wait", None)
                if callable(wait):
                    wait()
                exit_code = getattr(command, "exit_code", None)
                stderr_value = getattr(command, "stderr", b"")
                if isinstance(stderr_value, bytes):
                    stderr = stderr_value.decode("utf-8", "replace")
                else:
                    stderr = str(stderr_value or "")
                stdout_value = getattr(command, "stdout", b"")
                if isinstance(stdout_value, bytes):
                    stdout = stdout_value.decode("utf-8", "replace")
                else:
                    stdout = str(stdout_value or "")
                if self.segment(segment) is None:
                    self.record_segment(
                        segment,
                        exit_code=exit_code,
                        stderr=stderr,
                        stdout=stdout,
                    )
            except Exception as exc:  # preserve wait/diagnostic failure
                if self.segment(segment) is None:
                    self.record_segment(segment, error=repr(exc))
        self._processes_complete = True

    def wait(self, timeout: float | None = None) -> AcceptanceReport:
        self._wait_attached()
        return self.finalize()

    def _valid_ack(self, ack: PreviewConfirmation | None, *, full: bool) -> bool:
        if ack is None:
            return False
        records = self._records if full else self._displayed_records or self._records
        if not records:
            return False
        return ack.is_valid_for(
            self.job_id,
            records,
            secret=self.secret,
            require_full=full,
        )

    def finalize(self) -> AcceptanceReport:
        self._wait_attached()
        with self._lock:
            reasons: list[str] = []
            missing: list[str] = []

            preview_ack_valid = self._valid_ack(self._preview_ack, full=False)
            full_ack_valid = self._valid_ack(self._full_ack, full=True)

            if self.mode == "full":
                if not full_ack_valid:
                    reasons.append("missing_valid_full_confirmation")
                if self._preview_ack is not None and not full_ack_valid:
                    reasons.append("preview_permit_cannot_replace_full_confirmation")
            else:
                if not preview_ack_valid:
                    reasons.append("missing_valid_preview_confirmation")
                if (
                    len(self._displayed_records or self._records)
                    != self.expected_preview_count
                ):
                    reasons.append("preview_record_count_mismatch")
                for segment in REQUIRED_PREVIEW_PERMITS:
                    permit = self._permits.get(segment)
                    if permit is None:
                        missing.append(segment)
                        reasons.append(f"missing_stop_permit:{segment}")
                    elif not permit.authorizes(self.job, secret=self.secret):
                        missing.append(segment)
                        reasons.append(f"invalid_stop_permit:{segment}")

            parser = self._segments.get("parse->preview")
            if parser is not None and parser.exit_code not in (0, None):
                reasons.append(f"parser_exit_{parser.exit_code}")
                if parser.stderr:
                    reasons.append("parser_diagnostics_available")

            for segment_name, segment in self._segments.items():
                if segment.permit is not None and segment.permit_valid is False:
                    if segment_name not in missing:
                        missing.append(segment_name)
                    reasons.append(f"invalid_stop_permit:{segment_name}")
                if segment.exit_code not in (0, None, -13, 141):
                    if segment_name == "parse->preview":
                        continue
                    reasons.append(f"segment_exit_{segment_name}:{segment.exit_code}")

            ordered = tuple(
                self._segments[name]
                for name in CANONICAL_SEGMENTS
                if name in self._segments
            ) + tuple(
                report
                for name, report in sorted(self._segments.items())
                if name not in CANONICAL_SEGMENTS
            )

            if not self._processes_complete:
                status = PENDING
                reasons.append("processes_not_complete")
                delivered = False
            elif reasons:
                if any(reason.startswith("parser_exit_") for reason in reasons):
                    status = PENDING
                elif missing:
                    status = MISSING_PERMIT
                elif any("confirmation" in reason for reason in reasons):
                    status = INCOMPLETE
                else:
                    status = FAILED
                delivered = False
            else:
                status = DELIVERED
                delivered = True

            verdict = AcceptanceVerdict(
                status=status,
                delivered=delivered,
                reasons=tuple(dict.fromkeys(reasons)),
                missing_permits=tuple(missing),
            )
            summary = {
                "retained_records": len(self._records),
                "displayed_records": len(self._displayed_records),
                "segment_count": len(self._segments),
                "credentials_complete": self._credentials_complete,
                "processes_complete": self._processes_complete,
            }
            return AcceptanceReport(
                job_id=self.job_id,
                mode=self.mode,
                verdict=verdict,
                records=tuple(self._records),
                preview_ack=self._preview_ack,
                full_ack=self._full_ack,
                segments=ordered,
                diagnostics=ordered,
                summary=summary,
            )

    def report(self) -> AcceptanceReport:
        return self.finalize()

    def conclude(self) -> AcceptanceReport:
        return self.wait()

    def verdict(self) -> AcceptanceVerdict:
        return self.finalize().verdict

    def inspect_segment(self, name: str) -> SegmentDiagnostics | None:
        return self.segment(name)


AcceptanceVerifier = PipelineAcceptance
Scheduler = PipelineAcceptance
PipelineScheduler = PipelineAcceptance
