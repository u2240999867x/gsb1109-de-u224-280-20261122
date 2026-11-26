from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Mapping

JOB_SCOPE = "pipeline-acceptance/v1"

_SEGMENT_ALIASES = {
    "unpack->parse": "unpack->parse",
    "unpack_to_parse": "unpack->parse",
    "unpack-parse": "unpack->parse",
    "unpack:parse": "unpack->parse",
    "unpacker->parser": "unpack->parse",
    "unpacker_to_parser": "unpack->parse",
    "parse->preview": "parse->preview",
    "parse_to_preview": "parse->preview",
    "parse-preview": "parse->preview",
    "parse:preview": "parse->preview",
    "parser->previewer": "parse->preview",
    "parser_to_previewer": "parse->preview",
}


def normalize_segment(segment: str) -> str:
    key = str(segment).strip().lower().replace(" ", "")
    if key in _SEGMENT_ALIASES:
        return _SEGMENT_ALIASES[key]
    key = key.replace("_", "->").replace("-", "->").replace(":", "->")
    return _SEGMENT_ALIASES.get(key, str(segment))


def canonical_digest(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def summarize_records(records: Iterable[Mapping[str, Any] | Any]) -> dict[str, Any]:
    material = [
        dict(record) if isinstance(record, Mapping) else record for record in records
    ]
    digest = canonical_digest(material)
    return {
        "count": len(material),
        "digest": digest,
        "summary": digest.removeprefix("sha256:")[:16],
    }


def sign_message(secret: str | bytes, message: Mapping[str, Any]) -> str:
    if isinstance(secret, str):
        secret = secret.encode("utf-8")
    body = json.dumps(
        message, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hmac.new(secret, body, hashlib.sha256).hexdigest()


def verify_signature(
    secret: str | bytes, message: Mapping[str, Any], signature: str | None
) -> bool:
    if not signature:
        return False
    expected = sign_message(secret, message)
    return hmac.compare_digest(expected, str(signature))


@dataclass(frozen=True)
class JobCredential:
    job_id: str
    token: str = field(default_factory=lambda: secrets.token_urlsafe(24))
    scope: str = JOB_SCOPE

    @classmethod
    def create(cls, job_id: str | None = None) -> "JobCredential":
        return cls(job_id=job_id or f"job-{secrets.token_hex(8)}")

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "JobCredential":
        return cls(
            job_id=str(data["job_id"]),
            token=str(data.get("token", "")),
            scope=str(data.get("scope", JOB_SCOPE)),
        )

    def public_dict(self) -> dict[str, Any]:
        return {"job_id": self.job_id, "scope": self.scope}

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def matches(self, job_id: str | None) -> bool:
        return job_id is not None and hmac.compare_digest(str(job_id), self.job_id)


@dataclass(frozen=True)
class PipeIdentity:
    segment: str
    pipe_id: str
    reader_pid: int | None = None
    writer_pid: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "segment", normalize_segment(self.segment))

    @classmethod
    def create(
        cls,
        segment: str,
        *,
        reader_pid: int | None = None,
        writer_pid: int | None = None,
    ) -> "PipeIdentity":
        return cls(
            segment=segment,
            pipe_id=f"pipe-{secrets.token_hex(12)}",
            reader_pid=reader_pid,
            writer_pid=writer_pid,
        )

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "PipeIdentity":
        return cls(
            segment=str(data["segment"]),
            pipe_id=str(data["pipe_id"]),
            reader_pid=data.get("reader_pid"),
            writer_pid=data.get("writer_pid"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _signed_body(data: Mapping[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {name: data.get(name) for name in fields}


@dataclass(frozen=True)
class PreviewConfirmation:
    job_id: str
    count: int
    digest: str
    summary: str | None = None
    kind: str = "preview"
    scope: str = JOB_SCOPE
    issued_at: float = field(default_factory=time.time)
    signature: str | None = None

    @classmethod
    def from_records(
        cls,
        job_id: str,
        records: Iterable[Any],
        *,
        kind: str = "preview",
        secret: str | bytes | None = None,
    ) -> "PreviewConfirmation":
        summary = summarize_records(records)
        confirmation = cls(
            job_id=job_id,
            count=summary["count"],
            digest=summary["digest"],
            summary=summary["summary"],
            kind=kind,
        )
        if secret is not None:
            return issue_preview_ack(confirmation, secret)
        return confirmation

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "PreviewConfirmation":
        summary = data.get("summary")
        digest = str(data.get("digest", ""))
        if not summary and digest:
            summary = digest.removeprefix("sha256:")[:16]
        return cls(
            job_id=str(data["job_id"]),
            count=int(data["count"]),
            digest=digest,
            summary=summary,
            kind=str(data.get("kind", "preview")),
            scope=str(data.get("scope", JOB_SCOPE)),
            issued_at=float(data.get("issued_at", time.time())),
            signature=data.get("signature"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def public_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("signature", None)
        return data

    def signed_fields(self) -> tuple[str, ...]:
        return ("job_id", "count", "digest", "summary", "kind", "scope")

    def is_valid_for(
        self,
        job_id: str,
        records: Iterable[Any],
        *,
        secret: str | bytes | None = None,
        require_full: bool = False,
    ) -> bool:
        expected = summarize_records(records)
        if not hmac.compare_digest(self.job_id, str(job_id)):
            return False
        if int(self.count) != expected["count"]:
            return False
        if not hmac.compare_digest(self.digest, expected["digest"]):
            return False
        if require_full and self.kind != "full":
            return False
        if secret is not None:
            body = _signed_body(self.to_dict(), self.signed_fields())
            if not verify_signature(secret, body, self.signature):
                return False
        return True


PreviewAck = PreviewConfirmation


def issue_preview_ack(
    confirmation: PreviewConfirmation, secret: str | bytes
) -> PreviewConfirmation:
    body = _signed_body(confirmation.to_dict(), confirmation.signed_fields())
    signature = sign_message(secret, body)
    return PreviewConfirmation(**{**confirmation.to_dict(), "signature": signature})


@dataclass(frozen=True)
class StopPermit:
    job_id: str
    segment: str
    pipe_id: str
    reader_pid: int | None = None
    scope: str = JOB_SCOPE
    issued_at: float = field(default_factory=time.time)
    signature: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "segment", normalize_segment(self.segment))

    @classmethod
    def issue(
        cls,
        credential: JobCredential,
        pipe: PipeIdentity,
        *,
        secret: str | bytes | None = None,
    ) -> "StopPermit":
        permit = cls(
            job_id=credential.job_id,
            segment=pipe.segment,
            pipe_id=pipe.pipe_id,
            reader_pid=getattr(pipe, "reader_pid", None),
        )
        if secret is not None:
            return issue_stop_permit(permit, secret)
        return permit

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "StopPermit":
        return cls(
            job_id=str(data["job_id"]),
            segment=normalize_segment(str(data["segment"])),
            pipe_id=str(data["pipe_id"]),
            reader_pid=data.get("reader_pid"),
            scope=str(data.get("scope", JOB_SCOPE)),
            issued_at=float(data.get("issued_at", time.time())),
            signature=data.get("signature"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def signed_fields(self) -> tuple[str, ...]:
        return ("job_id", "segment", "pipe_id", "reader_pid", "scope")

    def authorizes(
        self,
        credential_or_job: JobCredential | str,
        pipe: PipeIdentity | Mapping[str, Any] | None = None,
        *,
        secret: str | bytes | None = None,
    ) -> bool:
        if isinstance(credential_or_job, JobCredential):
            job_id = credential_or_job.job_id
        else:
            job_id = str(credential_or_job)
        if not hmac.compare_digest(self.job_id, job_id):
            return False
        if pipe is not None:
            if isinstance(pipe, PipeIdentity):
                expected = pipe
            elif isinstance(pipe, Mapping):
                expected = PipeIdentity.from_mapping(pipe)
            else:
                expected = PipeIdentity(
                    segment=str(pipe.segment),
                    pipe_id=str(pipe.pipe_id),
                    reader_pid=getattr(pipe, "reader_pid", None),
                    writer_pid=getattr(pipe, "writer_pid", None),
                )
            if self.segment != expected.segment:
                return False
            if not hmac.compare_digest(self.pipe_id, expected.pipe_id):
                return False
        if secret is not None:
            body = _signed_body(self.to_dict(), self.signed_fields())
            if not verify_signature(secret, body, self.signature):
                return False
        return True


def issue_stop_permit(permit: StopPermit, secret: str | bytes) -> StopPermit:
    body = _signed_body(permit.to_dict(), permit.signed_fields())
    signature = sign_message(secret, body)
    return StopPermit(**{**permit.to_dict(), "signature": signature})
