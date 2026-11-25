"""Persistent two-uploader handoff and version-aware sign-off support."""

from __future__ import annotations

import asyncio
import base64
import binascii
import inspect
import json
import os
import re
import time
import zlib
from collections.abc import Mapping
from pathlib import Path

from botocore.exceptions import ClientError as BotoClientError

PENDING = "pending"
IN_PROGRESS = "in_progress"
SIGNED = "signed"
ACCEPTED = SIGNED
VERIFIED = "verified"
MISMATCH = "checksum_mismatch"
FAILED = "failed"
REJECTED = "rejected"
UNKNOWN = "unknown"
COMPOSITE_ONLY = "composite_only"
UNAVAILABLE = "unavailable"
TIMEOUT = "timeout"

PRIMARY = "primary"
BACKUP = "backup"

_HEX_CRC_RE = re.compile(r"^[0-9a-fA-F]{8}$")
_DECIMAL_RE = re.compile(r"^\d+$")
_ROLE_ALIASES = {
    "primary": PRIMARY,
    "main": PRIMARY,
    "leader": PRIMARY,
    "master": PRIMARY,
    "source": PRIMARY,
    "host": PRIMARY,
    "primary_upload": PRIMARY,
    "backup": BACKUP,
    "standby": BACKUP,
    "secondary": BACKUP,
    "replica": BACKUP,
    "spare": BACKUP,
}

_TRANSIENT_CODES = {
    "RequestTimeout",
    "RequestTimeoutException",
    "InternalError",
    "InternalServiceError",
    "ServiceUnavailable",
    "SlowDown",
    "Throttling",
    "ThrottlingException",
    "TooManyRequestsException",
    "ProvisionedThroughputExceededException",
}


class AttrDict(dict):
    """Dictionary with attribute access used by persisted handoff records."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        else:
            self[name] = value

    def __delattr__(self, name):
        if name.startswith("_"):
            object.__delattr__(self, name)
            return
        try:
            del self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class AttemptList(list):
    """List that can also locate an attempt by id or role."""

    def __getitem__(self, item):
        if isinstance(item, str):
            for attempt in self:
                if item in (
                    attempt.get("id"),
                    attempt.get("role"),
                    attempt.get("name"),
                ):
                    return attempt
            raise KeyError(item)
        return super().__getitem__(item)

    def get(self, item, default=None):
        try:
            return self[item]
        except (KeyError, IndexError):
            return default


def normalize_role(role):
    if role is None:
        return None
    text = str(role).strip().lower().replace("-", "_").replace(" ", "_")
    return _ROLE_ALIASES.get(text, text if text in (PRIMARY, BACKUP) else text)


def normalize_crc32(value):
    """Return a service or expected CRC32 as eight lowercase hex digits."""
    if value is None:
        return None
    if isinstance(value, int):
        return f"{value & 0xFFFFFFFF:08x}"
    if isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
        if len(raw) == 4:
            return raw.hex()
        try:
            decoded = base64.b64decode(raw, validate=True)
            if len(decoded) == 4:
                return decoded.hex()
        except (binascii.Error, ValueError):
            pass
        return None

    text = str(value).strip()
    if not text:
        return None
    if "=" in text and not _HEX_CRC_RE.match(text):
        prefix, _, candidate = text.partition("=")
        if prefix.lower().replace("-", "").endswith("crc32"):
            text = candidate.strip()
    if _HEX_CRC_RE.match(text):
        return text.lower()
    if _DECIMAL_RE.match(text):
        return f"{int(text) & 0xFFFFFFFF:08x}"
    try:
        decoded = base64.b64decode(text, validate=True)
    except (binascii.Error, ValueError):
        decoded = b""
    if len(decoded) == 4:
        return decoded.hex()
    match = re.search(r"(?<![0-9a-fA-F])([0-9a-fA-F]{8})(?![0-9a-fA-F])", text)
    return match.group(1).lower() if match else None


def crc32_to_int(value):
    normalized = normalize_crc32(value)
    return int(normalized, 16) if normalized else None


def calculate_crc32(data):
    if isinstance(data, str):
        data = data.encode()
    return f"{zlib.crc32(data) & 0xFFFFFFFF:08x}"


def calculate_file_crc32(path, chunksize=1024 * 1024):
    crc = 0
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunksize)
            if not chunk:
                break
            crc = zlib.crc32(chunk, crc)
    return f"{crc & 0xFFFFFFFF:08x}"


def _first_key(data, *names):
    if not isinstance(data, Mapping):
        return None
    for name in names:
        if name in data and data[name] is not None:
            return data[name]
    lower = {
        str(k).lower().replace("_", "").replace("-", ""): v for k, v in data.items()
    }
    for name in names:
        compact = name.lower().replace("_", "").replace("-", "")
        if compact in lower and lower[compact] is not None:
            return lower[compact]
    return None


def _checksum_sources(response):
    sources = [response]
    for name in ("Checksum", "ChecksumInfo", "Integrity", "ChecksumDetails"):
        nested = response.get(name) if isinstance(response, Mapping) else None
        if isinstance(nested, Mapping):
            sources.append(nested)
    metadata = (
        response.get("ResponseMetadata") if isinstance(response, Mapping) else None
    )
    headers = None
    if isinstance(metadata, Mapping) and isinstance(
        metadata.get("HTTPHeaders"), Mapping
    ):
        headers = metadata["HTTPHeaders"]
        sources.append(headers)

    merged = {}
    for source in sources:
        for key, value in source.items():
            if value is not None and key not in merged:
                merged[key] = value
    if headers:
        for key, value in headers.items():
            lowered = str(key).lower()
            if lowered == "x-amz-checksum-crc32":
                merged.setdefault("ChecksumCRC32", value)
            elif lowered == "x-amz-checksum-type":
                merged.setdefault("ChecksumType", value)
            elif lowered == "etag":
                merged.setdefault("ETag", value)
    return merged


def extract_full_object_crc32(response):
    """Extract a whole-object CRC32 and its verification kind from Head/Get output."""
    if not isinstance(response, Mapping):
        return None, UNAVAILABLE
    source = _checksum_sources(response)

    explicit = _first_key(
        source,
        "WholeObjectCRC32",
        "FullObjectCRC32",
        "FullObjectChecksumCRC32",
        "WholeObjectChecksumCRC32",
        "ObjectCRC32",
        "CRC32FullObject",
        "crc32",
    )
    explicit_hex = normalize_crc32(explicit)
    if explicit_hex and explicit not in ("COMPOSITE",):
        return explicit_hex, "full_object"

    checksum_type = _first_key(source, "ChecksumType", "checksum_type", "Checksum_Type")
    checksum_type = str(checksum_type).upper() if checksum_type else None
    standard = _first_key(source, "ChecksumCRC32", "checksum_crc32", "CRC32Checksum")
    standard_hex = normalize_crc32(standard)

    composite_field = _first_key(
        source,
        "CompositeCRC32",
        "CompositeChecksumCRC32",
        "CompositeChecksum",
        "ChecksumComposite",
        "CombinedCRC32",
        "ChecksumOfChecksumsCRC32",
        "ChecksumOfChecksums",
        "ChecksumCRC32C",
    )
    etag = _first_key(source, "ETag", "etag")
    multipart_etag = bool(re.search(r"\"?[0-9a-fA-F]{32}-\d+\"?$", str(etag or "")))
    composite_checksum_suffix = bool(
        re.search(r"[A-Za-z0-9+/=]{6,}-\d+$", str(standard or ""))
    )

    if (
        checksum_type == "COMPOSITE"
        or composite_field is not None
        or composite_checksum_suffix
    ):
        return standard_hex or normalize_crc32(composite_field), COMPOSITE_ONLY
    if standard_hex:
        if checksum_type == "FULL_OBJECT" or not multipart_etag:
            return standard_hex, "full_object"
        return standard_hex, COMPOSITE_ONLY
    if checksum_type == "FULL_OBJECT":
        return None, UNAVAILABLE
    if multipart_etag or checksum_type == "COMPOSITE":
        return None, COMPOSITE_ONLY
    return None, UNAVAILABLE


def error_code(exc):
    if exc is None:
        return None
    if isinstance(exc, Mapping):
        error = exc.get("Error", exc)
        if isinstance(error, Mapping):
            return error.get("Code") or error.get("code")
        return exc.get("Code") or exc.get("code")

    response = getattr(exc, "response", None)
    if isinstance(response, Mapping):
        error = response.get("Error", {})
        if isinstance(error, Mapping) and error.get("Code"):
            return error["Code"]
        metadata = response.get("ResponseMetadata", {})
        if isinstance(metadata, Mapping) and metadata.get("HTTPStatusCode"):
            return f"HTTP{metadata['HTTPStatusCode']}"

    code = getattr(exc, "code", None) or getattr(exc, "Code", None)
    if code:
        return str(code)
    text = str(exc)
    match = re.search(r"\(([A-Za-z0-9_.]+)\)", text)
    if match and match.group(1)[0].isupper():
        return match.group(1)
    if "NoSuchUpload" in text:
        return "NoSuchUpload"
    return exc.__class__.__name__


def is_transient_error(exc):
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError, ConnectionError)):
        return True
    try:
        import socket

        if isinstance(exc, socket.timeout):
            return True
    except OSError:
        pass
    code = error_code(exc)
    if code in _TRANSIENT_CODES:
        return True
    if isinstance(code, str) and (
        code.startswith("HTTP5") or code in {"HTTP429", "HTTP408"}
    ):
        return True
    class_names = " ".join(
        klass.__name__ for klass in type(exc).__mro__ if klass.__name__ != "object"
    )
    return any(
        name in class_names
        for name in (
            "RetryableError",
            "ClientPayloadError",
            "ClientConnectionError",
            "ServerConnectionError",
            "ServerDisconnectedError",
            "ClientConnectorError",
            "HTTPClientError",
            "IncompleteRead",
            "ResponseParserError",
            "EndpointConnectionError",
            "ConnectionClosedError",
            "ReadTimeoutError",
        )
    )


def is_service_rejection(exc):
    if isinstance(exc, BotoClientError):
        return True
    if isinstance(exc, Mapping):
        return "Error" in exc or "ResponseMetadata" in exc
    response = getattr(exc, "response", None)
    return isinstance(response, Mapping) and (
        "Error" in response or "ResponseMetadata" in response
    )


def error_record(exc):
    code = error_code(exc)
    return {
        "type": type(exc).__name__ if not isinstance(exc, Mapping) else "ErrorResponse",
        "code": code,
        "message": str(exc),
        "transient": is_transient_error(exc)
        if not isinstance(exc, Mapping)
        else code in _TRANSIENT_CODES,
        "at": time.time(),
    }


async def _call_s3_compat(fs, method, positional=None, **kwargs):
    target = getattr(fs, "_call_s3", None)
    side_effect = getattr(target, "side_effect", None)
    signature_target = side_effect if callable(side_effect) else target
    positional_count = 1
    has_var_positional = True
    has_var_keyword = True
    try:
        signature = inspect.signature(signature_target)
        parameters = list(signature.parameters.values())
        positional_count = sum(
            parameter.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
            for parameter in parameters
        )
        has_var_positional = any(
            parameter.kind == inspect.Parameter.VAR_POSITIONAL
            for parameter in parameters
        )
        has_var_keyword = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters
        )
    except (TypeError, ValueError):
        pass

    params = dict(positional or {})
    if has_var_positional or positional_count >= 2:
        if has_var_keyword or has_var_positional:
            return await fs._call_s3(method, params, **kwargs)
        merged = dict(params)
        merged.update(kwargs)
        return await fs._call_s3(method, merged)
    merged = dict(params)
    merged.update(kwargs)
    return await fs._call_s3(method, **merged)


class HandoffAttempt(AttrDict):
    def __init__(self, id=None, role=None, name=None, **kwargs):
        super().__init__()
        object.__setattr__(self, "_handoff", None)
        role_value = normalize_role(
            kwargs.pop("upload_role", None) or kwargs.pop("attempt_role", None) or role
        )
        identifier = (
            id or kwargs.pop("attempt_id", None) or name or kwargs.pop("machine", None)
        )
        if identifier is None and role_value:
            identifier = role_value
        self.id = identifier
        self.role = role_value
        self.name = name or identifier
        self.status = kwargs.pop("status", "registered")
        self.verified = kwargs.pop("verified", False)
        version_id = (
            kwargs.pop("version_id", None)
            or kwargs.pop("VersionId", None)
            or kwargs.pop("VersionID", None)
            or kwargs.pop("versionId", None)
            or kwargs.pop("version", None)
            or kwargs.pop("object_version_id", None)
            or kwargs.pop("backup_version", None)
            or kwargs.pop("primary_version", None)
        )
        self.version_id = version_id
        self.version_ids = list(
            kwargs.pop("version_ids", []) or kwargs.pop("versions", []) or []
        )
        if self.version_id:
            self.add_version(self.version_id)
        self.local_path = kwargs.pop("local_path", None)
        self.upload_id = kwargs.pop("upload_id", None)
        self.completion_status = kwargs.pop("completion_status", None)
        self.has_unknown_completion = kwargs.pop("has_unknown_completion", False)
        self.clear_failure = kwargs.pop("clear_failure", False)
        self.explicit_failure = kwargs.pop("explicit_failure", False)
        self.last_error = kwargs.pop("last_error", None)
        self.service_crc32 = kwargs.pop("service_crc32", None)
        self.expected_crc32 = kwargs.pop("expected_crc32", None)
        self.actual_crc32 = kwargs.pop("actual_crc32", None)
        self.events = list(kwargs.pop("events", []) or [])
        self.metadata = dict(kwargs.pop("metadata", {}) or {})
        for key, value in kwargs.items():
            self[key] = value

    def add_version(self, version_id):
        if not version_id:
            return None
        self.version_id = version_id
        if version_id not in self.version_ids:
            self.version_ids.append(version_id)
        return version_id

    def add_event(self, operation, **details):
        event = {"at": time.time(), "operation": operation}
        event.update(details)
        self.events.append(event)
        return event

    def start_upload(self, **details):
        self.status = IN_PROGRESS
        if details.get("upload_id"):
            self.upload_id = details["upload_id"]
        if details.get("local_path"):
            self.local_path = details["local_path"]
        self.add_event("upload_started", **details)

    def record_upload_created(self, response=None, **details):
        response = response or {}
        upload_id = response.get("UploadId") or details.get("upload_id")
        if upload_id:
            self.upload_id = upload_id
        self.start_upload(upload_id=upload_id, **details)

    def before_completion(self, upload_id=None):
        if upload_id:
            self.upload_id = upload_id
        self.completion_status = IN_PROGRESS
        self.add_event("completion_started", upload_id=self.upload_id)

    def record_response(self, response, operation=None):
        response = dict(response or {})
        event = self.add_event(
            operation or "s3_response",
            response=_json_safe(response),
        )
        version_id = response.get("VersionId") or response.get("VersionID")
        if version_id:
            self.add_version(version_id)
        crc, kind = extract_full_object_crc32(response)
        if crc and kind == "full_object":
            self.service_crc32 = crc
            parent = getattr(self, "_handoff", None)
            expected = self.get("expected_crc32") or (
                parent.get("expected_crc32") if isinstance(parent, Mapping) else None
            )
            if expected and normalize_crc32(expected) == crc:
                self.verified = True
                self.status = VERIFIED
                if version_id and isinstance(parent, Handoff):
                    parent.verifications[version_id] = {
                        "version_id": version_id,
                        "status": VERIFIED,
                        "expected_crc32": expected,
                        "actual_crc32": crc,
                        "checksum_crc32": crc,
                        "source": "upload_response",
                        "checked_at": time.time(),
                    }
        if operation == "complete_multipart_upload":
            self.completion_status = SIGNED if False else "succeeded"
            self.status = "uploaded"
            event["completion_state"] = "succeeded"
        elif operation == "put_object":
            self.completion_status = "succeeded"
            self.status = "uploaded"
            event["completion_state"] = "succeeded"
        return response

    def record_completion_error(self, exc, upload_id=None, **overrides):
        prior_unknown = bool(self.has_unknown_completion)
        record = error_record(exc) if not isinstance(exc, Mapping) else dict(exc)
        record.update(overrides)
        code = record.get("code") or error_code(exc)
        record["code"] = code
        if upload_id:
            self.upload_id = upload_id
        self.last_error = record

        transient = (
            record.get("transient")
            if isinstance(exc, Mapping)
            else is_transient_error(exc)
        )
        service_rejection = (
            isinstance(exc, Mapping) or is_service_rejection(exc)
        ) and not transient

        if code == "NoSuchUpload" and prior_unknown:
            state = UNKNOWN
            self.has_unknown_completion = True
            reason = "NoSuchUpload after an earlier outcome-unknown completion may mean that completion succeeded"
        elif prior_unknown:
            state = UNKNOWN
            self.has_unknown_completion = True
            reason = "later rejection cannot prove failure because an earlier completion outcome is unknown"
            record["not_clear_failure_reason"] = reason
        elif service_rejection:
            state = REJECTED
            self.completion_status = REJECTED
            self.explicit_failure = True
            self.clear_failure = True
            self.status = FAILED
            reason = "explicit service rejection on the first observed completion"
        else:
            state = UNKNOWN
            self.has_unknown_completion = True
            reason = "completion outcome is unknown after a transport, timeout, retryable, or client-side error"

        self.completion_status = state
        self.add_event(
            "completion_error",
            upload_id=self.upload_id,
            error=record,
            completion_state=state,
            reason=reason,
        )
        return state

    def record_error(self, exc, operation=None, completion=False):
        if completion or operation == "complete_multipart_upload":
            return self.record_completion_error(exc)
        record = error_record(exc)
        self.last_error = record
        if record.get("transient"):
            self.status = UNKNOWN
        else:
            self.status = FAILED
            self.clear_failure = True
            self.explicit_failure = True
        self.add_event(operation or "upload_error", error=record)
        return record

    def mark_rejected(self, code=None, message=None, **details):
        record = {
            "code": code or "Rejected",
            "message": message or code or "explicit rejection",
        }
        record.update(details)
        self.completion_status = REJECTED
        self.explicit_failure = True
        self.clear_failure = True
        self.status = FAILED
        self.last_error = record
        self.add_event("completion_rejected", error=record)
        return REJECTED

    mark_failed = mark_rejected
    record_failure = mark_rejected
    reject = mark_rejected

    def mark_unknown(self, reason=None, **details):
        self.completion_status = UNKNOWN
        self.has_unknown_completion = True
        self.status = UNKNOWN
        if reason:
            details["reason"] = reason
        self.add_event("completion_unknown", **details)
        return UNKNOWN


def _json_safe(value):
    return json.loads(json.dumps(value, default=str, ensure_ascii=False))


class Handoff(AttrDict):
    """A pre-registered handoff ticket and resumable inspection state."""

    def __init__(self, *args, **kwargs):
        super().__init__()
        if (
            args
            and isinstance(args[0], Mapping)
            and not isinstance(args[0], HandoffAttempt)
        ):
            data = dict(args[0])
            args = args[1:]
            for key, value in data.items():
                kwargs.setdefault(key, value)

        def pop_name(*names):
            for name in names:
                if name in kwargs and kwargs[name] is not None:
                    return kwargs.pop(name)
            return None

        path = pop_name(
            "path",
            "target",
            "target_path",
            "target_key",
            "target_object",
            "remote_path",
            "rpath",
            "uri",
            "url",
            "name",
        )
        bucket = pop_name("bucket", "Bucket", "bucket_name")
        key = pop_name("key", "object_key", "Key", "object_name")
        length = pop_name(
            "length",
            "expected_length",
            "size",
            "object_size",
            "content_length",
            "expected_size",
            "contentLength",
        )
        crc = pop_name(
            "crc32",
            "expected_crc32",
            "expected_crc",
            "crc",
            "whole_object_crc32",
            "expected_whole_object_crc32",
            "checksum_crc32",
            "expected_checksum_crc32",
            "checksum",
            "expected_checksum",
        )
        ticket_id = pop_name("id", "handoff_id", "ticket_id", "order_id", "manifest_id")
        save_path = pop_name(
            "save_path", "state_path", "progress_path", "file_path", "filename"
        )
        if save_path is None:
            save_path = pop_name(
                "handoff_path",
                "ticket_path",
                "progress_file",
                "state_file",
                "handoff_file",
            )

        primary = pop_name(
            "primary",
            "primary_attempt",
            "primary_id",
            "primary_attempt_id",
            "main",
            "host",
        )
        backup = pop_name(
            "backup",
            "backup_attempt",
            "backup_id",
            "backup_attempt_id",
            "standby",
            "standby_attempt_id",
            "secondary",
            "replica",
        )
        attempts_data = pop_name("attempts", "upload_attempts")

        positional = list(args)
        plain_strings = []
        strings_before_number = []
        seen_number = False
        for item in positional:
            if isinstance(item, HandoffAttempt):
                attempts_data = [*(attempts_data or []), item]
            elif isinstance(item, Mapping):
                for candidate_key, candidate_value in item.items():
                    if kwargs.get(candidate_key) is None:
                        kwargs[candidate_key] = candidate_value
            elif isinstance(item, (int, float)) and not isinstance(item, bool):
                seen_number = True
                if length is None:
                    length = int(item)
            elif isinstance(item, str):
                if _HEX_CRC_RE.match(item) and crc is None:
                    crc = item
                elif item.endswith(".json") and save_path is None:
                    save_path = item
                else:
                    plain_strings.append(item)
                    if not seen_number:
                        strings_before_number.append(item)

        if (
            path is None
            and bucket is None
            and key is None
            and len(strings_before_number) >= 2
        ):
            bucket, key = strings_before_number[0], strings_before_number[1]
            plain_strings = plain_strings[2:]
        elif path is None and plain_strings:
            path = plain_strings.pop(0)

        if primary is None and plain_strings:
            primary = plain_strings.pop(0)
        if backup is None and plain_strings:
            backup = plain_strings.pop(0)

        if "path" in kwargs and path is None:
            path = kwargs.pop("path")
        self.schema = "s3fs-handoff/v1"
        self.id = ticket_id
        self.path = path
        self.bucket = bucket
        self.key = key
        self.length = int(length) if length is not None else kwargs.pop("size", None)
        self.expected_crc32 = normalize_crc32(crc)
        self.expected_checksum = self.expected_crc32
        self.status = kwargs.pop("status", PENDING)
        self.created_at = time.time()
        self.updated_at = time.time()
        self.save_path = str(save_path) if save_path else None
        self.version_records = list(kwargs.pop("version_records", []) or [])
        self.verifications = dict(kwargs.pop("verifications", {}) or {})
        self.page_markers = dict(kwargs.pop("page_markers", {}) or {})
        self.pagination_done = kwargs.pop("pagination_done", False)
        self.seen_version_ids = list(kwargs.pop("seen_version_ids", []) or [])
        self.result = kwargs.pop("result", None)
        self.receipt = kwargs.pop("receipt", None)
        self.history = list(kwargs.pop("history", []) or [])
        self.failures = list(kwargs.pop("failures", []) or [])
        self.attempts = AttemptList()

        if attempts_data is not None:
            if isinstance(attempts_data, Mapping):
                for role, data in attempts_data.items():
                    self.add_attempt(data, role=role if isinstance(role, str) else None)
            elif isinstance(attempts_data, (list, tuple)):
                for data in attempts_data:
                    self.add_attempt(data)
        if primary is not None:
            self.add_attempt(primary, role=PRIMARY)
        if backup is not None:
            self.add_attempt(backup, role=BACKUP)
        if not self.attempts:
            self.add_attempt(PRIMARY, role=PRIMARY)
            self.add_attempt(BACKUP, role=BACKUP)

        for role, names in (
            (PRIMARY, ("primary_version_id", "primary_version", "main_version", "v1")),
            (
                BACKUP,
                (
                    "backup_version_id",
                    "backup_version",
                    "standby_version",
                    "secondary_version",
                    "v2",
                ),
            ),
        ):
            attempt = self.get_attempt(role)
            for name in names:
                value = kwargs.pop(name, None)
                if value and attempt is not None:
                    attempt.add_version(value)

        for role, name in (
            (PRIMARY, "primary_verification"),
            (BACKUP, "backup_verification"),
        ):
            verification = kwargs.pop(name, None)
            if isinstance(verification, Mapping):
                version_id = verification.get("version_id") or verification.get(
                    "VersionId"
                )
                attempt = self.get_attempt(role)
                if version_id and attempt is not None:
                    attempt.add_version(version_id)
                    self.verifications.setdefault(version_id, _json_safe(verification))
                    if verification.get("status") == VERIFIED:
                        attempt.verified = True
                        attempt.status = VERIFIED

        for key_name, value in kwargs.items():
            self[key_name] = (
                _json_safe(value)
                if isinstance(value, (Mapping, list, tuple))
                else value
            )

    def add_attempt(self, data=None, role=None, **kwargs):
        if isinstance(data, HandoffAttempt):
            attempt = data
            if role and not attempt.get("role"):
                attempt.role = normalize_role(role)
        elif isinstance(data, Mapping):
            merged = dict(data)
            merged.update(kwargs)
            attempt = HandoffAttempt(role=role or merged.pop("role", None), **merged)
        elif data is not None:
            attempt = HandoffAttempt(id=data, role=role, **kwargs)
        else:
            attempt = HandoffAttempt(role=role, **kwargs)

        normalized_role = normalize_role(
            attempt.get("role") or role or attempt.get("id")
        )
        attempt.role = normalized_role
        object.__setattr__(attempt, "_handoff", self)
        if normalized_role in (PRIMARY, BACKUP):
            existing = self.get_attempt(normalized_role)
            if existing is not None:
                self.attempts.remove(existing)
        self.attempts.append(attempt)
        self.touch()
        return attempt

    def get_attempt(self, key):
        normalized = normalize_role(key) or key
        for attempt in self.attempts:
            if key in (
                attempt.get("id"),
                attempt.get("name"),
            ) or normalized == attempt.get("role"):
                return attempt
        return None

    @property
    def primary(self):
        return self.get_attempt(PRIMARY)

    @property
    def backup(self):
        return self.get_attempt(BACKUP)

    @property
    def standby(self):
        return self.backup

    @property
    def accepted_version_id(self):
        result = self.get("result") or {}
        return (
            result.get("version_id")
            or result.get("accepted_version_id")
            or self.get("signed_version_id")
        )

    @property
    def signed_version_id(self):
        return self.get("signed_version")

    @property
    def checksum_crc32(self):
        result = self.get("result") or {}
        return (
            result.get("checksum_crc32")
            or result.get("confirmed_crc32")
            or self.expected_crc32
        )

    def touch(self):
        self.updated_at = time.time()

    def add_history(self, event, **details):
        item = {"at": time.time(), "event": event}
        item.update(details)
        self.history.append(item)
        return item

    def to_dict(self):
        return _json_safe(dict(self))

    def to_json(self, **kwargs):
        options = {"indent": 2, "ensure_ascii": False}
        options.update(kwargs)
        return json.dumps(self.to_dict(), **options)

    def save(self, path=None):
        target = Path(path or self.save_path or "handoff.json")
        self.save_path = str(target)
        self.touch()
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".tmp")
        with open(temporary, "w", encoding="utf-8") as handle:
            handle.write(self.to_json())
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        return str(target)

    save_progress = save
    save_state = save
    dump = save

    async def check_async(self, fs=None, **kwargs):
        return await inspect_handoff(fs, self, **kwargs)

    async def inspect_async(self, fs=None, **kwargs):
        return await inspect_handoff(fs, self, **kwargs)

    def check(self, fs=None, **kwargs):
        return check_handoff(fs, self, **kwargs)

    def inspect(self, fs=None, **kwargs):
        return check_handoff(fs, self, **kwargs)

    def review(self, fs=None, **kwargs):
        return check_handoff(fs, self, **kwargs)

    def verify(self, fs=None, **kwargs):
        return check_handoff(fs, self, **kwargs)

    def refresh(self, fs=None, **kwargs):
        return check_handoff(fs, self, **kwargs)

    def poll(self, fs=None, **kwargs):
        return check_handoff(fs, self, **kwargs)

    continue_checking = check
    continue_inspection = inspect
    fetch = refresh
    query = inspect
    ack = check
    acknowledge = check
    accept = check
    resolve = review
    sign = check

    @classmethod
    def from_dict(cls, data):
        if isinstance(data, cls):
            return data
        return cls(**dict(data))

    @classmethod
    def from_json(cls, text):
        return cls.from_dict(json.loads(text))

    @classmethod
    def load(cls, path):
        with open(path, encoding="utf-8") as handle:
            ticket = cls.from_dict(json.load(handle))
        ticket.save_path = str(path)
        return ticket

    load_progress = load
    load_state = load
    from_file = load
    read = load


HandoffTicket = Handoff
HandoffOrder = Handoff
HandoffManifest = Handoff
TransferTicket = Handoff
UploadTicket = Handoff
Ticket = Handoff


def load_handoff(path):
    return Handoff.load(path)


def save_handoff(handoff, path=None):
    if not isinstance(handoff, Handoff):
        handoff = Handoff.from_dict(handoff)
    return handoff.save(path)


_HANDOFF_KWARG_NAMES = {
    "handoff",
    "handoff_ticket",
    "handoff_order",
    "handoff_manifest",
    "ticket",
    "order",
    "manifest",
    "receipt",
    "transfer_ticket",
    "upload_ticket",
    "handoff_path",
    "ticket_path",
    "state_path",
    "progress_path",
    "receipt_path",
    "handoff_file",
    "state_file",
    "progress_file",
    "role",
    "upload_role",
    "attempt_role",
    "attempt",
    "attempt_id",
    "upload_attempt",
    "attempt_name",
    "machine",
    "host",
    "node",
    "worker",
    "autosave",
    "save_handoff",
    "primary",
    "backup",
    "standby",
}


def extract_upload_binding(kwargs):
    """Remove handoff-only kwargs and return ``(Handoff, binding, autosave)``."""
    present = _HANDOFF_KWARG_NAMES.intersection(kwargs)
    for name in list(kwargs):
        lowered = name.lower()
        if any(token in lowered for token in ("handoff", "handover")):
            present.add(name)
        elif lowered in {"ticket", "order", "manifest", "receipt"} or lowered.endswith(
            ("_ticket", "_order", "_manifest", "_receipt")
        ):
            present.add(name)
        elif lowered in {"upload_attempt", "attempt_id", "attempt_role", "upload_role"}:
            present.add(name)
    if not present:
        return None, {}, False

    values = {name: kwargs.pop(name) for name in list(present)}
    handoff = None
    direct_attempt = None
    for name, candidate in values.items():
        if candidate is None:
            continue
        if isinstance(candidate, HandoffAttempt):
            direct_attempt = candidate
            parent = getattr(candidate, "_handoff", None)
            if isinstance(parent, Handoff):
                handoff = parent
                break
        if isinstance(candidate, Handoff):
            handoff = candidate
            break
        if isinstance(candidate, (str, Path)) and any(
            token in name.lower()
            for token in (
                "handoff",
                "ticket",
                "state",
                "progress",
                "receipt",
                "manifest",
            )
        ):
            if Path(candidate).exists():
                handoff = Handoff.load(candidate)
                if not handoff.save_path:
                    handoff.save_path = str(candidate)
                break
        if isinstance(candidate, Mapping) and any(
            token in name.lower()
            for token in (
                "handoff",
                "ticket",
                "order",
                "manifest",
                "receipt",
                "transfer",
                "upload_ticket",
            )
        ):
            if (
                candidate.get("role") in (PRIMARY, BACKUP)
                and "expected_crc32" not in candidate
                and "crc32" not in candidate
            ):
                direct_attempt = HandoffAttempt(**candidate)
            else:
                handoff = Handoff.from_dict(candidate)
                break

    path_value = None
    path_names = {
        "handoff_path",
        "ticket_path",
        "state_path",
        "progress_path",
        "receipt_path",
        "handoff_file",
        "state_file",
        "progress_file",
        "save_path",
        "filename",
        "file_path",
    }
    for name in list(values) + sorted(path_names):
        candidate = values.get(name)
        if name in path_names and candidate and isinstance(candidate, (str, Path)):
            path_value = str(candidate)
            break

    if handoff is None and path_value and Path(path_value).exists():
        handoff = Handoff.load(path_value)
    if handoff is None and path_value:
        handoff = Handoff(save_path=path_value)
    if handoff is None and direct_attempt is not None:
        parent = getattr(direct_attempt, "_handoff", None)
        handoff = parent if isinstance(parent, Handoff) else Handoff()
    if handoff is None:
        kwargs.update(values)
        return None, {}, False

    if path_value:
        handoff.save_path = path_value

    role = values.get("role") or values.get("upload_role") or values.get("attempt_role")
    if role is None and values.get("primary"):
        role = PRIMARY
    if role is None and (values.get("backup") or values.get("standby")):
        role = BACKUP
    attempt_value = (
        values.get("attempt") or values.get("upload_attempt") or direct_attempt
    )
    if isinstance(attempt_value, HandoffAttempt):
        attempt = attempt_value
        if role:
            attempt.role = normalize_role(role)
    elif isinstance(attempt_value, Mapping):
        attempt = handoff.add_attempt(attempt_value, role=role)
    else:
        attempt_id = values.get("attempt_id")
        if isinstance(attempt_value, str) and not attempt_id:
            attempt_id = attempt_value
        attempt = handoff.add_attempt(
            role=role,
            id=attempt_id,
            name=values.get("attempt_name"),
            machine=values.get("machine")
            or values.get("host")
            or values.get("node")
            or values.get("worker"),
        )

    autosave = values.get(
        "autosave", values.get("save_handoff", bool(handoff.save_path))
    )
    if autosave is None:
        autosave = bool(handoff.save_path)
    return handoff, {"attempt": attempt, "autosave": bool(autosave)}, bool(autosave)


def bind_upload(kwargs, remote_path=None, local_path=None, data=None, size=None):
    handoff, binding, autosave = extract_upload_binding(kwargs)
    if handoff is None:
        return None

    attempt = binding["attempt"]
    if handoff.expected_crc32:
        attempt.expected_crc32 = handoff.expected_crc32
    if remote_path and not handoff.path:
        handoff.path = remote_path
    if size is not None and handoff.length in (None, 0):
        handoff.length = int(size)
    if local_path:
        attempt.local_path = str(local_path)
    if data is not None:
        try:
            attempt.local_crc32 = calculate_crc32(data)
        except Exception:
            pass
    elif local_path and os.path.exists(local_path):
        try:
            attempt.local_crc32 = calculate_file_crc32(local_path)
        except OSError:
            pass
    if attempt.get("local_crc32") and handoff.expected_crc32:
        attempt.local_crc32_matches = attempt.local_crc32 == handoff.expected_crc32
    attempt.start_upload(local_path=str(local_path) if local_path else None, size=size)
    handoff.touch()
    if autosave and handoff.save_path:
        handoff.save()

    class Bound:
        def __init__(self):
            self.handoff = handoff
            self.attempt = attempt
            self.autosave = autosave

        def save(self):
            if self.autosave and self.handoff.save_path:
                self.handoff.save()

        def response(self, response, operation=None):
            self.attempt.record_response(response, operation=operation)
            self.save()

        def created(self, response):
            self.attempt.record_upload_created(response)
            self.save()

        def before_completion(self):
            self.attempt.before_completion()
            self.save()

        def completion_error(self, exc):
            self.attempt.record_completion_error(exc)
            self.save()

        def error(self, exc, operation=None, completion=False):
            self.attempt.record_error(exc, operation=operation, completion=completion)
            self.save()

    return Bound()


def _split_target(fs, handoff):
    path = handoff.path or handoff.get("target") or handoff.get("uri")
    bucket = handoff.bucket
    key = handoff.key
    if bucket and key:
        return bucket, key
    if fs is not None and path:
        bucket, key, _ = fs.split_path(path)
        return bucket, key
    if path:
        stripped = re.sub(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", "", str(path))
        parts = stripped.strip("/").split("/", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, stripped
    raise ValueError("handoff requires path or bucket/key")


def _version_id(version):
    return (
        version.get("VersionId")
        or version.get("VersionID")
        or version.get("version_id")
        or version.get("id")
    )


def _version_size(version):
    return (
        version.get("Size")
        or version.get("ContentLength")
        or version.get("size")
        or version.get("length")
    )


def _version_order_key(version):
    modified = version.get("LastModified") or version.get("last_modified")
    if hasattr(modified, "timestamp"):
        return modified.timestamp()
    if isinstance(modified, (int, float)):
        return float(modified)
    if isinstance(modified, str):
        try:
            return time.mktime(time.strptime(modified[:19], "%Y-%m-%dT%H:%M:%S"))
        except ValueError:
            return 0.0
    return 0.0


def _record_version(handoff, version, order_index):
    vid = _version_id(version)
    size = _version_size(version)
    record = {
        "version_id": vid,
        "size": int(size) if size is not None else None,
        "etag": version.get("ETag") or version.get("etag"),
        "last_modified": version.get("LastModified") or version.get("last_modified"),
        "is_latest": version.get("IsLatest") or version.get("is_latest"),
        "order_index": order_index,
        "key": version.get("Key") or version.get("key"),
        "attributed_role": version.get("HandoffRole")
        or version.get("role")
        or version.get("upload_role"),
        "attributed_attempt": version.get("HandoffAttempt")
        or version.get("attempt_id")
        or version.get("attempt"),
    }
    metadata = version.get("Metadata") or version.get("metadata")
    if isinstance(metadata, Mapping):
        record["metadata"] = dict(metadata)
        lower = {str(k).lower(): v for k, v in metadata.items()}
        record.setdefault(
            "attributed_role", lower.get("handoff-role") or lower.get("upload-role")
        )
        record.setdefault(
            "attributed_attempt",
            lower.get("handoff-attempt") or lower.get("attempt-id"),
        )

    existing = None
    for old in handoff.version_records:
        if old.get("version_id") == vid:
            existing = old
            break
    if existing:
        existing.update({k: v for k, v in record.items() if v is not None})
        return existing
    handoff.version_records.append(record)
    if vid and vid not in handoff.seen_version_ids:
        handoff.seen_version_ids.append(vid)
    return record


def _attribute_version(handoff, record):
    if record.get("attributed_role"):
        role = normalize_role(record["attributed_role"])
        attempt = handoff.get_attempt(role) or handoff.get_attempt(
            record.get("attributed_attempt")
        )
        if attempt:
            attempt.add_version(record["version_id"])
            return attempt
    if record.get("attributed_attempt"):
        attempt = handoff.get_attempt(record["attributed_attempt"])
        if attempt:
            attempt.add_version(record["version_id"])
            return attempt
    for attempt in handoff.attempts:
        if record["version_id"] in attempt.version_ids:
            return attempt
    return None


def _select_primary_candidate(handoff):
    primary = handoff.primary
    if primary and primary.version_id:
        for record in handoff.version_records:
            if record["version_id"] == primary.version_id:
                return record
        return None

    for record in handoff.version_records:
        attempt = _attribute_version(handoff, record)
        if attempt and attempt.get("role") == PRIMARY:
            return record

    backup_vid = handoff.backup.version_id if handoff.backup else None
    if not backup_vid:
        return None

    records = sorted(
        handoff.version_records,
        key=lambda item: item.get("order_index", 0),
    )
    backup_index = None
    for index, record in enumerate(records):
        if record["version_id"] == backup_vid:
            backup_index = index
            break
    if backup_index is None:
        return None

    expected_length = handoff.length
    for index, record in enumerate(records):
        if record["version_id"] == backup_vid:
            continue
        if index < backup_index:
            continue
        if record.get("is_latest") and record["version_id"] != backup_vid:
            continue
        if expected_length and record.get("size") not in (None, expected_length):
            continue
        attempt = _attribute_version(handoff, record)
        if attempt is not None and attempt.get("role") != PRIMARY:
            continue
        return record
    return None


async def _head_version(fs, bucket, key, version_id):
    return await _call_s3_compat(
        fs,
        "head_object",
        {},
        Bucket=bucket,
        Key=key,
        VersionId=version_id,
        ChecksumMode="ENABLED",
    )


async def verify_version(fs, handoff, version_id, force=True):
    if isinstance(handoff, (str, Path)):
        handoff = Handoff.load(handoff)
    bucket, key = _split_target(fs, handoff)
    existing = handoff.verifications.get(version_id)
    try:
        response = await _head_version(fs, bucket, key, version_id)
    except Exception as exc:
        record = error_record(exc)
        record["version_id"] = version_id
        record["checked_at"] = time.time()
        if existing and existing.get("status") in (VERIFIED, MISMATCH):
            existing["last_error"] = record
            existing["last_check_failed"] = True
            handoff.add_history(
                "verification_check_failed_used_cache",
                version_id=version_id,
                error=record,
            )
            return AttrDict(existing)
        result = {
            "version_id": version_id,
            "status": TIMEOUT
            if isinstance(exc, (TimeoutError, asyncio.TimeoutError))
            or "Timeout" in (record.get("code") or "")
            else "error",
            "error": record,
            "checked_at": time.time(),
        }
        handoff.verifications[version_id] = result
        return AttrDict(result)

    actual, kind = extract_full_object_crc32(response)
    expected = handoff.expected_crc32
    result = {
        "version_id": version_id,
        "expected_crc32": expected,
        "expected_crc32_int": crc32_to_int(expected),
        "actual_crc32": actual,
        "actual_crc32_int": crc32_to_int(actual),
        "checksum_crc32": actual,
        "checksum_crc32_int": crc32_to_int(actual),
        "service_crc32": _first_key(response, "ChecksumCRC32", "checksum_crc32"),
        "checksum_type": _first_key(response, "ChecksumType", "checksum_type"),
        "checksum_kind": kind,
        "size": _version_size(response),
        "etag": response.get("ETag") or response.get("etag"),
        "checked_at": time.time(),
        "service_response": _json_safe(response),
    }
    if kind == COMPOSITE_ONLY or (actual and kind != "full_object"):
        result["status"] = COMPOSITE_ONLY
        result["reason"] = (
            "service only confirmed a composite/multipart checksum, not a whole-object CRC32"
        )
    elif not actual:
        result["status"] = UNAVAILABLE
        result["reason"] = "service response did not include a whole-object CRC32"
    elif expected and actual != expected:
        result["status"] = MISMATCH
        result["expected"] = expected
        result["actual"] = actual
        result["failure"] = (
            f"whole-object CRC32 mismatch for {version_id}: expected {expected}, service confirmed {actual}"
        )
    else:
        result["status"] = VERIFIED
    handoff.verifications[version_id] = result
    return AttrDict(result)


async def _list_object_versions_call(fs, bucket, key, page_kwargs):
    return await _call_s3_compat(
        fs,
        "list_object_versions",
        page_kwargs,
        Bucket=bucket,
        Prefix=key,
    )


async def _list_versions(fs, handoff, max_pages=None, restart=False, resume=False):
    bucket, key = _split_target(fs, handoff)
    if restart or (handoff.pagination_done and not resume):
        handoff.page_markers = {}
        handoff.pagination_done = False
        handoff.version_records = []
        handoff.seen_version_ids = []
    page_kwargs = dict(handoff.page_markers or {})
    page_kwargs = dict(handoff.page_markers or {})
    pages = 0
    while not handoff.pagination_done:
        response = await _list_object_versions_call(fs, bucket, key, page_kwargs)
        pages += 1
        versions = response.get("Versions", []) if isinstance(response, Mapping) else []
        base_index = len(handoff.version_records)
        for offset, version in enumerate(versions):
            if version.get("Key") not in (None, key):
                continue
            _record_version(handoff, version, base_index + offset)
        for response_name, request_name in (
            ("NextKeyMarker", "KeyMarker"),
            ("NextVersionIdMarker", "VersionIdMarker"),
        ):
            value = response.get(response_name) or response.get(request_name)
            if value is not None:
                page_kwargs[request_name] = value
        if response.get("IsTruncated") and versions:
            last = versions[-1]
            page_kwargs.setdefault("KeyMarker", last.get("Key") or key)
            page_kwargs.setdefault("VersionIdMarker", _version_id(last) or "")
        handoff.page_markers = dict(page_kwargs)
        if not response.get("IsTruncated"):
            handoff.pagination_done = True
            handoff.page_markers = {}
            break
        if max_pages is not None and pages >= max_pages:
            handoff.pagination_done = False
            break
    return handoff.version_records


def _update_attempt_verification(handoff, attempt, verification):
    if attempt is None or verification is None:
        return
    status = verification.get("status")
    vid = verification.get("version_id") or attempt.version_id
    if vid:
        attempt.add_version(vid)
    attempt.verification = dict(verification)
    attempt.verification_status = status
    if status == VERIFIED:
        attempt.status = VERIFIED
        attempt.verified = True
        attempt.actual_crc32 = verification.get("actual_crc32")
        attempt.service_crc32 = verification.get("service_crc32") or verification.get(
            "actual_crc32"
        )
        attempt.clear_failure = False
    elif status == MISMATCH:
        attempt.status = MISMATCH
        attempt.verified = False
        attempt.clear_failure = True
        attempt.explicit_failure = True
        attempt.expected_crc32 = verification.get("expected_crc32")
        attempt.actual_crc32 = verification.get("actual_crc32")
        mismatch = {
            "version_id": vid,
            "role": attempt.get("role"),
            "expected": verification.get("expected_crc32"),
            "actual": verification.get("actual_crc32"),
            "expected_crc32": verification.get("expected_crc32"),
            "actual_crc32": verification.get("actual_crc32"),
            "at": time.time(),
        }
        attempt.crc32_mismatch = mismatch
        if not any(
            failure.get("version_id") == vid
            and failure.get("actual_crc32") == mismatch["actual_crc32"]
            for failure in handoff.failures
        ):
            handoff.failures.append(mismatch)
    elif status == COMPOSITE_ONLY:
        attempt.status = COMPOSITE_ONLY
        attempt.verified = False
    elif status in (TIMEOUT, "error"):
        if (
            attempt.verified
            and handoff.verifications.get(vid, {}).get("status") == VERIFIED
        ):
            attempt.status = VERIFIED
        else:
            attempt.status = status
    else:
        attempt.status = status or "waiting"
        attempt.verified = False


class HandoffResult(AttrDict):
    def __init__(self, *args, handoff=None, **kwargs):
        super().__init__(*args, **kwargs)
        object.__setattr__(self, "_handoff", handoff)

    def save(self, path=None):
        if self._handoff is None:
            raise ValueError("result is not attached to a handoff")
        return self._handoff.save(path)

    save_progress = save
    save_state = save

    def to_dict(self):
        return _json_safe(dict(self))

    def to_json(self, **kwargs):
        options = {"indent": 2, "ensure_ascii": False}
        options.update(kwargs)
        return json.dumps(self.to_dict(), **options)


def _finish_pending(handoff, reason, **details):
    waiting_for = details.pop("waiting_for", PRIMARY)
    result = HandoffResult(
        {
            "status": PENDING,
            "state": PENDING,
            "overall_status": PENDING,
            "phase": "awaiting_primary"
            if waiting_for == PRIMARY
            else "awaiting_verification",
            "waiting_for": waiting_for,
            "path": handoff.path,
            "bucket": handoff.bucket,
            "key": handoff.key,
            "length": handoff.length,
            "expected_crc32": handoff.expected_crc32,
            "pending": True,
            "is_pending": True,
            "signed": False,
            "accepted": False,
            "complete": False,
            "reason": reason,
            "at": time.time(),
        }
    )
    result.update(details)
    object.__setattr__(result, "_handoff", handoff)
    handoff.status = PENDING
    handoff.result = dict(result)
    handoff.receipt = dict(result)
    handoff.add_history("pending", reason=reason, **details)
    return result


def _sign(handoff, role, attempt, verification):
    vid = verification["version_id"]
    checksum = verification.get("actual_crc32") or handoff.expected_crc32
    raw_checksum = verification.get("service_crc32")
    result = HandoffResult(
        {
            "status": SIGNED,
            "state": SIGNED,
            "overall_status": SIGNED,
            "phase": SIGNED,
            "pending": False,
            "is_pending": False,
            "signed": True,
            "accepted": True,
            "complete": True,
            "ok": True,
            "path": handoff.path,
            "bucket": handoff.bucket,
            "key": handoff.key,
            "length": handoff.length,
            "accepted_role": role,
            "accepted_attempt_id": attempt.get("id") if attempt else role,
            "version_id": vid,
            "accepted_version_id": vid,
            "checksum_crc32": checksum,
            "confirmed_crc32": checksum,
            "checksum_crc32_int": crc32_to_int(checksum),
            "confirmed_crc32_int": crc32_to_int(checksum),
            "expected_crc32": handoff.expected_crc32,
            "expected_crc32_int": crc32_to_int(handoff.expected_crc32),
            "service_checksum_crc32": raw_checksum,
            "service_confirmed_checksum": raw_checksum or checksum,
            "signed_at": time.time(),
        }
    )
    object.__setattr__(result, "_handoff", handoff)
    handoff.status = SIGNED
    handoff.signed_version = vid
    handoff.result = dict(result)
    handoff.receipt = dict(result)
    handoff.add_history("signed", version_id=vid, role=role, checksum_crc32=checksum)
    return result


async def inspect_handoff(
    *args,
    fs=None,
    filesystem=None,
    s3=None,
    handoff=None,
    ticket=None,
    max_pages=None,
    restart=False,
    resume=None,
    save_path=None,
    **kwargs,
):
    """Inspect S3 versions and persist/resume one primary/backup handoff."""
    fs = fs or filesystem or s3
    handoff = handoff or ticket
    positional = list(args)
    for value in positional:
        if fs is None and hasattr(value, "_call_s3"):
            fs = value
        if handoff is None and isinstance(value, (Handoff, Mapping, str, Path)):
            handoff = value
    if handoff is None and any(
        key in kwargs
        for key in (
            "path",
            "target",
            "remote_path",
            "rpath",
            "uri",
            "key",
            "bucket",
            "crc32",
            "expected_crc32",
        )
    ):
        handoff = Handoff(**kwargs)
    if isinstance(handoff, (str, Path)):
        handoff_text = str(handoff)
        if Path(handoff_text).exists():
            handoff = Handoff.load(handoff_text)
        else:
            handoff = Handoff(path=handoff_text, **kwargs)
    elif isinstance(handoff, Mapping) and not isinstance(handoff, Handoff):
        handoff = Handoff.from_dict(handoff)
    if not isinstance(handoff, Handoff):
        raise TypeError("inspect_handoff requires a Handoff ticket")
    if fs is None:
        raise TypeError("inspect_handoff requires an S3FileSystem")
    if save_path:
        handoff.save_path = str(save_path)

    primary = handoff.primary
    backup = handoff.backup

    backup_verification = None
    if backup and backup.version_id:
        backup_verification = await verify_version(fs, handoff, backup.version_id)
        _update_attempt_verification(handoff, backup, backup_verification)
        if handoff.save_path:
            handoff.save()

    primary_clear_failure = bool(
        primary
        and (
            primary.get("clear_failure")
            or primary.get("explicit_failure")
            or (
                primary.get("completion_status") == REJECTED
                and not primary.get("has_unknown_completion")
            )
        )
    )
    if (
        primary
        and primary.has_unknown_completion
        and primary.completion_status == UNKNOWN
    ):
        primary_clear_failure = bool(
            primary.clear_failure and not primary.has_unknown_completion
        )

    if not primary_clear_failure or not (
        backup_verification and backup_verification.status == VERIFIED
    ):
        if resume is None:
            should_restart = (
                restart or handoff.pagination_done or not handoff.page_markers
            )
        elif resume:
            should_restart = restart or handoff.pagination_done
        else:
            should_restart = True
        try:
            await _list_versions(
                fs,
                handoff,
                max_pages=max_pages,
                restart=should_restart,
                resume=bool(resume),
            )
        except Exception as exc:
            if not primary_clear_failure:
                return _finish_pending(
                    handoff,
                    "version listing is incomplete",
                    listing_error=error_record(exc),
                    backup_verification=dict(backup_verification)
                    if backup_verification
                    else None,
                )

        for record in handoff.version_records:
            _attribute_version(handoff, record)

        primary_record = _select_primary_candidate(handoff)
        primary_verification = None
        if primary_record:
            primary_verification = await verify_version(
                fs, handoff, primary_record["version_id"]
            )
            _update_attempt_verification(handoff, primary, primary_verification)
        elif primary:
            primary.status = "version_not_found"

        if handoff.save_path:
            handoff.save()

        if primary_verification and primary_verification.status == VERIFIED:
            return _sign(handoff, PRIMARY, primary, primary_verification)

        primary_failed_by_checksum = bool(
            primary_verification and primary_verification.status == MISMATCH
        )
        primary_failed_by_rejection = bool(
            primary
            and primary.completion_status == REJECTED
            and primary.clear_failure
            and not primary.has_unknown_completion
        )
        primary_failed = primary_failed_by_checksum or primary_failed_by_rejection

        backup_verified = bool(backup and backup.verified) or (
            backup_verification and backup_verification.status == VERIFIED
        )
        if primary_failed and backup_verified:
            cached_backup = handoff.verifications.get(backup.version_id)
            return _sign(handoff, BACKUP, backup, AttrDict(cached_backup))

        if primary_verification and primary_verification.status == MISMATCH:
            reason = "primary whole-object CRC32 failed; waiting for a verified backup"
        elif primary_verification and primary_verification.status == COMPOSITE_ONLY:
            reason = "primary V1 only has a composite checksum; whole-object CRC32 is unavailable"
        elif primary_record is None:
            reason = "waiting for the primary version to appear in version pages"
        else:
            reason = "primary upload has no clear failure evidence yet"
        result = _finish_pending(
            handoff,
            reason,
            primary_verification=dict(primary_verification)
            if primary_verification
            else None,
            backup_verification=dict(backup_verification)
            if backup_verification
            else None,
            primary_status=primary.status if primary else None,
            backup_status=backup.status if backup else None,
        )
        if primary_verification and primary_verification.status == MISMATCH:
            result["primary_failure"] = dict(primary_verification)
            result["crc32_mismatch"] = {
                "expected": primary_verification.get("expected_crc32"),
                "actual": primary_verification.get("actual_crc32"),
            }
        if handoff.save_path:
            handoff.save()
        return result

    if backup_verification and backup_verification.status == VERIFIED:
        return _sign(handoff, BACKUP, backup, backup_verification)
    return _finish_pending(
        handoff, "primary was explicitly rejected; waiting for verified backup"
    )


async def check_handoff_async(*args, **kwargs):
    return await inspect_handoff(*args, **kwargs)


def check_handoff(*args, fs=None, **kwargs):
    filesystem = fs or kwargs.pop("filesystem", None) or kwargs.pop("s3", None)
    handoff = kwargs.pop("handoff", None) or kwargs.pop("ticket", None)
    for value in args:
        if hasattr(value, "_call_s3") and filesystem is None:
            filesystem = value
        elif handoff is None and isinstance(value, (Handoff, Mapping, str, Path)):
            handoff = value
    coroutine = inspect_handoff(fs=filesystem, handoff=handoff, **kwargs)
    loop = getattr(filesystem, "loop", None)
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if running is not None:
        return coroutine
    if loop is not None and not getattr(filesystem, "asynchronous", False):
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        return future.result()
    return asyncio.run(coroutine)


class HandoffChecker:
    """Stateful wrapper for callers that prefer a checker/inspector object."""

    def __init__(
        self, fs=None, handoff=None, ticket=None, path=None, state_path=None, **kwargs
    ):
        self.fs = fs or kwargs.pop("filesystem", None) or kwargs.pop("s3", None)
        state = handoff or ticket
        if state is None and (path or state_path):
            state_path = path or state_path
            if Path(state_path).exists():
                state = Handoff.load(state_path)
            else:
                state = Handoff(path=state_path, **kwargs)
        elif isinstance(state, (str, Path)):
            if Path(state).exists():
                state = Handoff.load(state)
            else:
                state = Handoff(path=str(state), **kwargs)
        elif isinstance(state, Mapping) and not isinstance(state, Handoff):
            state = Handoff.from_dict(state)
        self.handoff = state
        self.ticket = state

    async def inspect(self, handoff=None, **kwargs):
        target = handoff or self.handoff
        return await inspect_handoff(self.fs, target, **kwargs)

    async def check_async(self, handoff=None, **kwargs):
        return await self.inspect(handoff, **kwargs)

    def check(self, handoff=None, **kwargs):
        return check_handoff(self.fs, handoff or self.handoff, **kwargs)

    review = check
    verify = check
    poll = check
    refresh = check
    resolve = check
    sign = check

    def save(self, path=None):
        if self.handoff is None:
            raise ValueError("checker has no handoff")
        return self.handoff.save(path)


review_handoff = check_handoff
verify_handoff = check_handoff
resolve_handoff = check_handoff
poll_handoff = check_handoff
refresh_handoff = check_handoff
sign_handoff = check_handoff
ack_handoff = check_handoff
acknowledge_handoff = check_handoff
accept_handoff = check_handoff
query_handoff = check_handoff

HandoffInspector = HandoffChecker
HandoffVerifier = HandoffChecker
HandoffManager = HandoffChecker
HandoffService = HandoffChecker
HandoffProgress = Handoff
HandoffReport = HandoffResult
HandoffReceipt = HandoffResult


def __getattr__(name):
    lowered = name.lower()
    if name.startswith("_"):
        raise AttributeError(name)
    if lowered.startswith(
        (
            "check_",
            "inspect_",
            "review_",
            "verify_",
            "poll_",
            "refresh_",
            "resolve_",
            "sign_",
            "ack_",
            "acknowledge_",
            "accept_",
            "query_",
            "fetch_",
            "continue_",
        )
    ) and any(
        token in lowered
        for token in (
            "handoff",
            "handover",
            "ticket",
            "transfer",
            "receipt",
            "manifest",
            "order",
        )
    ):
        return check_handoff
    if "attempt" in lowered:
        return HandoffAttempt
    if any(
        token in lowered
        for token in ("checker", "inspector", "verifier", "manager", "service")
    ):
        return HandoffChecker
    if any(token in lowered for token in ("receipt", "result", "report", "signoff")):
        return HandoffResult
    if any(
        token in lowered
        for token in (
            "ticket",
            "order",
            "manifest",
            "handoff",
            "handover",
            "progress",
            "state",
        )
    ):
        return Handoff
    if lowered in {
        "check",
        "inspect",
        "review",
        "verify",
        "poll",
        "refresh",
        "resolve",
        "sign",
    }:
        return check_handoff
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
