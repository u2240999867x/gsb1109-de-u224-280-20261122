from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .credentials import (
    PreviewConfirmation,
    StopPermit,
    normalize_segment,
)

DELIVERED = "delivered"
PENDING = "pending"
MISSING_PERMIT = "missing_permit"
FAILED = "failed"
INCOMPLETE = "incomplete"


@dataclass(frozen=True)
class SegmentDiagnostics:
    segment: str
    exit_code: int | None = None
    stderr: str = ""
    stdout: str = ""
    broken_pipe: bool = False
    permit: StopPermit | None = None
    permit_valid: bool | None = None
    error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "segment", normalize_segment(self.segment))
        if isinstance(self.permit, Mapping):
            object.__setattr__(self, "permit", StopPermit.from_mapping(self.permit))

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "SegmentDiagnostics":
        permit_data = data.get("permit")
        permit = (
            StopPermit.from_mapping(permit_data)
            if isinstance(permit_data, Mapping)
            else None
        )
        return cls(
            segment=str(data["segment"]),
            exit_code=data.get("exit_code"),
            stderr=str(data.get("stderr", "") or ""),
            stdout=str(data.get("stdout", "") or ""),
            broken_pipe=bool(data.get("broken_pipe", False)),
            permit=permit,
            permit_valid=data.get("permit_valid"),
            error=data.get("error"),
            extra=dict(data.get("extra", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "segment": self.segment,
            "exit_code": self.exit_code,
            "stderr": self.stderr,
            "stdout": self.stdout,
            "broken_pipe": self.broken_pipe,
            "permit": self.permit.to_dict() if self.permit else None,
            "permit_valid": self.permit_valid,
            "error": self.error,
            "extra": self.extra,
        }


SegmentReport = SegmentDiagnostics


@dataclass(frozen=True)
class AcceptanceVerdict:
    status: str
    delivered: bool
    reasons: tuple[str, ...] = ()
    missing_permits: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "delivered": self.delivered,
            "reasons": list(self.reasons),
            "missing_permits": list(self.missing_permits),
        }


@dataclass(frozen=True)
class AcceptanceReport:
    job_id: str
    mode: str
    verdict: AcceptanceVerdict
    records: tuple[Any, ...] = ()
    preview_ack: PreviewConfirmation | None = None
    full_ack: PreviewConfirmation | None = None
    segments: tuple[SegmentDiagnostics, ...] = ()
    diagnostics: tuple[SegmentDiagnostics, ...] = ()
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def delivered(self) -> bool:
        return self.verdict.delivered

    @property
    def status(self) -> str:
        return self.verdict.status

    def segment(self, name: str) -> SegmentDiagnostics | None:
        normalized = normalize_segment(name)
        for segment in self.segments:
            if segment.segment == normalized:
                return segment
        return None

    def get_segment(self, name: str) -> SegmentDiagnostics | None:
        return self.segment(name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "mode": self.mode,
            "status": self.status,
            "delivered": self.delivered,
            "verdict": self.verdict.to_dict(),
            "reasons": list(self.verdict.reasons),
            "missing_permits": list(self.verdict.missing_permits),
            "records": list(self.records),
            "record_count": len(self.records),
            "preview_ack": self.preview_ack.to_dict() if self.preview_ack else None,
            "full_ack": self.full_ack.to_dict() if self.full_ack else None,
            "segments": [segment.to_dict() for segment in self.segments],
            "diagnostics": {
                segment.segment: segment.to_dict() for segment in self.segments
            },
            "summary": self.summary,
        }
