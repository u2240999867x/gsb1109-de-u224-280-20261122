import copy
import json
from dataclasses import dataclass, fields, is_dataclass, replace
from typing import Any, Final, Iterable, Sequence

from cloudevents.archive.exceptions import (
    CommitConflictError,
    InvalidLaneTypeError,
    ManifestStateError,
    ReceiptStateError,
    UnknownCommitError,
    UnknownLineError,
    UnknownManifestError,
    UnknownReceiptError,
)
from cloudevents.archive.operations import (
    LANE_INTEGER,
    LANE_STRING,
    IngestAMQP,
    IngestHTTP,
    IngestJSON,
    Operation,
    RegisterManifest,
    RevokeManifest,
    WithdrawReceipt,
)
from cloudevents.core.bindings import amqp as amqp_binding
from cloudevents.core.bindings import http as http_binding
from cloudevents.core.bindings.amqp import AMQPMessage
from cloudevents.core.bindings.http import HTTPMessage
from cloudevents.core.formats.json import JSONFormat

BINDING_HTTP: Final[str] = "http"
BINDING_AMQP: Final[str] = "amqp"
BINDING_JSON: Final[str] = "json"

BODY_TEXT: Final[str] = "text"
BODY_BINARY: Final[str] = "binary"
BODY_JSON: Final[str] = "json"

STATUS_ASSIGNED: Final[str] = "assigned"
STATUS_PENDING: Final[str] = "pending"
STATUS_CONFLICTED: Final[str] = "conflicted"
STATUS_WITHDRAWN: Final[str] = "withdrawn"

_LANE_BOOLEAN: Final[str] = "boolean"
_STRUCTURED_CONTENT_TYPE_PREFIX: Final[str] = "application/cloudevents"


@dataclass(frozen=True)
class LineKey:
    """
    Identity of a business line.

    Two receipts share a business line only when source, event type, the
    confirmed type and value of the 'lane' extension, and the kind and
    value of the body are all equal. Bindings and event ids are not part
    of the key, so different bindings and different event ids may share
    a line.
    """

    source: str | None
    event_type: str | None
    lane_type: str | None
    lane_value: int | str | None
    body_kind: str | None
    body_value: str | bytes | None


@dataclass(frozen=True)
class ManifestView:
    """Active (non-revoked) source manifest attached to a receipt."""

    manifest_id: str
    lane_type: str


@dataclass(frozen=True)
class ReceiptView:
    """
    Current state of a receipt.

    status is one of:
    - "assigned": the receipt currently belongs to the business line
      identified by line_no.
    - "pending": the lane type is unconfirmed and no active manifest
      declares it yet.
    - "conflicted": active manifests declare contradictory lane types;
      the conflicting manifests are listed in manifests.
    - "withdrawn": the receipt is out of the current ingestion scope;
      its raw content remains queryable.
    """

    receipt_id: int
    binding: str
    raw: HTTPMessage | AMQPMessage | bytes
    event_id: str | None
    source: str | None
    event_type: str | None
    status: str
    line_no: int | None
    lane_type: str | None
    lane_value: int | str | None
    manifests: tuple[ManifestView, ...]


@dataclass(frozen=True)
class LineView:
    """Current state of a business line."""

    line_no: int
    key: LineKey
    members: tuple[int, ...]


@dataclass(frozen=True)
class LineChange:
    """Membership change of one business line within a single commit."""

    line_no: int
    added: tuple[int, ...]
    removed: tuple[int, ...]
    created: bool


@dataclass(frozen=True)
class CommitResult:
    """
    Outcome of one ingestion commit.

    receipt_ids lists the receipts ingested by the commit in operation
    order. changes lists the business line membership changes caused by
    the commit. replayed is True when the commit id was re-sent with
    identical content and the commit was recorded only once.
    """

    commit_id: str
    receipt_ids: tuple[int, ...]
    changes: tuple[LineChange, ...]
    replayed: bool = False


@dataclass
class _Receipt:
    receipt_id: int
    binding: str
    raw: HTTPMessage | AMQPMessage | bytes
    event_id: str | None
    source: str | None
    event_type: str | None
    lane_present: bool
    lane_raw: Any
    lane_type: str | None
    body_kind: str | None
    body_value: str | bytes | None
    withdrawn: bool = False
    line_no: int | None = None


@dataclass
class _Manifest:
    manifest_id: str
    receipt_id: int
    lane_type: str
    revoked: bool = False


@dataclass
class _Line:
    line_no: int
    key: LineKey
    members: list[int]


@dataclass
class _Extracted:
    binding: str
    raw: HTTPMessage | AMQPMessage | bytes
    event_id: str | None
    source: str | None
    event_type: str | None
    lane_present: bool
    lane_raw: Any
    lane_type: str | None
    body_kind: str | None
    body_value: str | bytes | None


@dataclass(frozen=True)
class _CommitEntry:
    fingerprint: str
    result: CommitResult


def _lane_type_of(value: Any) -> str:
    if isinstance(value, bool):
        return _LANE_BOOLEAN
    if isinstance(value, int):
        return LANE_INTEGER
    if isinstance(value, str):
        return LANE_STRING
    raise InvalidLaneTypeError(f"unsupported lane value type: {type(value).__name__}")


def _normalize_lane_value(lane_type: str, raw: Any) -> int | str:
    if lane_type == LANE_INTEGER:
        if isinstance(raw, bool):
            raise InvalidLaneTypeError("lane value is not an integer")
        try:
            return int(raw)
        except (TypeError, ValueError):
            raise InvalidLaneTypeError(
                f"lane value {raw!r} cannot be confirmed as integer"
            ) from None
    if lane_type == LANE_STRING:
        return raw if isinstance(raw, str) else str(raw)
    raise InvalidLaneTypeError(f"unsupported lane type: {lane_type!r}")


def _binary_mode_body(
    body: bytes, datacontenttype: str | None
) -> tuple[str | None, str | bytes | None]:
    if not body:
        return None, None
    media_type = (datacontenttype or "").split(";")[0].strip().lower()
    if media_type.startswith("text/"):
        return BODY_TEXT, body.decode("utf-8")
    return BODY_BINARY, bytes(body)


def _structured_body(data: Any) -> tuple[str | None, str | bytes | None]:
    if data is None:
        return None, None
    if isinstance(data, (bytes, bytearray)):
        return BODY_BINARY, bytes(data)
    if isinstance(data, str):
        return BODY_TEXT, data
    return BODY_JSON, json.dumps(data, sort_keys=True, separators=(",", ":"))


def _extract_structured(
    binding: str, raw: HTTPMessage | AMQPMessage | bytes, body: bytes
) -> _Extracted:
    event = JSONFormat().read(None, body)
    attributes = event.get_attributes()
    lane_present = "lane" in attributes
    lane_raw = attributes.get("lane")
    body_kind, body_value = _structured_body(event.get_data())
    return _Extracted(
        binding=binding,
        raw=raw,
        event_id=attributes.get("id"),
        source=attributes.get("source"),
        event_type=attributes.get("type"),
        lane_present=lane_present,
        lane_raw=lane_raw,
        lane_type=_lane_type_of(lane_raw) if lane_present else None,
        body_kind=body_kind,
        body_value=body_value,
    )


def _extract_http(message: HTTPMessage) -> _Extracted:
    has_ce_headers = any(name.lower().startswith("ce-") for name in message.headers)
    if not has_ce_headers:
        return _extract_structured(BINDING_HTTP, message, message.body)
    event = http_binding.from_binary(message, JSONFormat())
    attributes = event.get_attributes()
    body_kind, body_value = _binary_mode_body(
        message.body, attributes.get("datacontenttype")
    )
    return _Extracted(
        binding=BINDING_HTTP,
        raw=message,
        event_id=attributes.get("id"),
        source=attributes.get("source"),
        event_type=attributes.get("type"),
        lane_present="lane" in attributes,
        lane_raw=attributes.get("lane"),
        # HTTP header values are plain strings; the original lane type
        # stays unconfirmed until source manifests declare it.
        lane_type=None,
        body_kind=body_kind,
        body_value=body_value,
    )


def _extract_amqp(message: AMQPMessage) -> _Extracted:
    content_type = message.properties.get("content-type", "")
    if isinstance(content_type, str) and content_type.lower().startswith(
        _STRUCTURED_CONTENT_TYPE_PREFIX
    ):
        return _extract_structured(BINDING_AMQP, message, message.application_data)
    event = amqp_binding.from_binary(message, JSONFormat())
    attributes = event.get_attributes()
    body_kind, body_value = _binary_mode_body(
        message.application_data, attributes.get("datacontenttype")
    )
    lane_present = "lane" in attributes
    lane_raw = attributes.get("lane")
    return _Extracted(
        binding=BINDING_AMQP,
        raw=message,
        event_id=attributes.get("id"),
        source=attributes.get("source"),
        event_type=attributes.get("type"),
        lane_present=lane_present,
        lane_raw=lane_raw,
        lane_type=_lane_type_of(lane_raw) if lane_present else None,
        body_kind=body_kind,
        body_value=body_value,
    )


def _canonical(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return (
            type(value).__qualname__,
            tuple(
                (field.name, _canonical(getattr(value, field.name)))
                for field in fields(value)
            ),
        )
    if isinstance(value, dict):
        return tuple(sorted((key, _canonical(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_canonical(item) for item in value)
    return value


class Archive:
    """
    Deduplicating ingestion archive for CloudEvents receipts.

    Receipts arrive inside commits. Each commit carries a client-chosen
    commit id and an ordered list of operations (ingestions, manifest
    registrations/revocations, withdrawals). A commit is applied
    atomically: queries only observe the state before or after the whole
    commit. Re-sending a commit id with identical content is recorded
    only once. Business lines are numbered in creation order and keep
    their numbers for the lifetime of the archive; they are never
    deleted and recreated. Replaying the commits in their original order
    from an empty archive reproduces the same line numbers, assignments
    and changes.
    """

    def __init__(self) -> None:
        self._receipts: dict[int, _Receipt] = {}
        self._manifests: dict[str, _Manifest] = {}
        self._lines: dict[LineKey, _Line] = {}
        self._lines_by_no: dict[int, _Line] = {}
        self._next_receipt_id = 1
        self._next_line_no = 1
        self._commits: dict[str, _CommitEntry] = {}
        self._commit_order: list[str] = []

    @classmethod
    def replay(cls, commits: Iterable[tuple[str, Sequence[Operation]]]) -> "Archive":
        """
        Rebuild an archive by re-applying commits in their original order.

        :param commits: (commit_id, operations) pairs in commit order
        :return: Archive with the same line numbers, assignments and
            per-commit changes as the original submission order produced
        """
        archive = cls()
        for commit_id, operations in commits:
            archive.submit(commit_id, operations)
        return archive

    def submit(self, commit_id: str, operations: Iterable[Operation]) -> CommitResult:
        """
        Apply a commit atomically.

        :param commit_id: Client-chosen commit id. Re-sending the same id
            with identical content is a no-op recorded only once.
        :param operations: Ordered operations of the commit
        :return: CommitResult with ingested receipt ids and line changes
        :raises CommitConflictError: If the commit id was already recorded
            with different content
        """
        ops = tuple(operations)
        fingerprint = repr(_canonical(ops))
        existing = self._commits.get(commit_id)
        if existing is not None:
            if existing.fingerprint != fingerprint:
                raise CommitConflictError(
                    f"commit {commit_id!r} was already recorded with different content"
                )
            return replace(existing.result, replayed=True)

        snapshot = copy.deepcopy(
            (
                self._receipts,
                self._manifests,
                self._lines,
                self._lines_by_no,
                self._next_receipt_id,
                self._next_line_no,
            )
        )
        before = self._membership()
        try:
            receipt_ids = self._apply(ops)
        except Exception:
            (
                self._receipts,
                self._manifests,
                self._lines,
                self._lines_by_no,
                self._next_receipt_id,
                self._next_line_no,
            ) = snapshot
            raise

        result = CommitResult(
            commit_id=commit_id,
            receipt_ids=receipt_ids,
            changes=self._diff(before),
        )
        self._commits[commit_id] = _CommitEntry(fingerprint=fingerprint, result=result)
        self._commit_order.append(commit_id)
        return result

    def receipt(self, receipt_id: int) -> ReceiptView:
        """
        Query the current business line assignment of a receipt.

        :param receipt_id: Archive-assigned receipt id
        :return: ReceiptView with status, line number and active manifests
        :raises UnknownReceiptError: If the receipt id does not exist
        """
        receipt = self._receipts.get(receipt_id)
        if receipt is None:
            raise UnknownReceiptError(f"unknown receipt id: {receipt_id!r}")
        status, key = self._resolve(receipt)
        manifests = tuple(
            ManifestView(manifest_id=m.manifest_id, lane_type=m.lane_type)
            for m in self._active_manifests(receipt_id)
        )
        return ReceiptView(
            receipt_id=receipt.receipt_id,
            binding=receipt.binding,
            raw=receipt.raw,
            event_id=receipt.event_id,
            source=receipt.source,
            event_type=receipt.event_type,
            status=status,
            line_no=receipt.line_no if status == STATUS_ASSIGNED else None,
            lane_type=key.lane_type if key is not None else None,
            lane_value=key.lane_value if key is not None else None,
            manifests=manifests,
        )

    def line(self, line_no: int) -> LineView:
        """
        Query a business line by its stable line number.

        :param line_no: Business line number
        :return: LineView with the line key and current member receipt ids
        :raises UnknownLineError: If the line number does not exist
        """
        line = self._lines_by_no.get(line_no)
        if line is None:
            raise UnknownLineError(f"unknown line number: {line_no!r}")
        return LineView(line_no=line.line_no, key=line.key, members=tuple(line.members))

    def lines(self) -> tuple[LineView, ...]:
        """List all business lines ordered by line number."""
        return tuple(self.line(line_no) for line_no in sorted(self._lines_by_no))

    def commit(self, commit_id: str) -> CommitResult:
        """
        Read the business line and member changes of one commit.

        :param commit_id: Commit id passed to submit
        :return: The recorded CommitResult of the commit
        :raises UnknownCommitError: If the commit id does not exist
        """
        entry = self._commits.get(commit_id)
        if entry is None:
            raise UnknownCommitError(f"unknown commit id: {commit_id!r}")
        return entry.result

    @property
    def commit_ids(self) -> tuple[str, ...]:
        """Commit ids in the order they were first recorded."""
        return tuple(self._commit_order)

    def _apply(self, ops: tuple[Operation, ...]) -> tuple[int, ...]:
        receipt_ids: list[int] = []
        for op in ops:
            receipt_id = self._apply_op(op)
            if receipt_id is not None:
                receipt_ids.append(receipt_id)
        return tuple(receipt_ids)

    def _apply_op(self, op: Operation) -> int | None:
        if isinstance(op, IngestHTTP):
            return self._ingest(_extract_http(op.message))
        if isinstance(op, IngestAMQP):
            return self._ingest(_extract_amqp(op.message))
        if isinstance(op, IngestJSON):
            return self._ingest(_extract_structured(BINDING_JSON, op.body, op.body))
        if isinstance(op, RegisterManifest):
            if op.lane_type not in (LANE_INTEGER, LANE_STRING):
                raise InvalidLaneTypeError(
                    f"manifest lane type must be {LANE_INTEGER!r} or "
                    f"{LANE_STRING!r}, got {op.lane_type!r}"
                )
            if op.manifest_id in self._manifests:
                raise ManifestStateError(
                    f"manifest id {op.manifest_id!r} is already registered"
                )
            receipt = self._receipts.get(op.receipt_id)
            if receipt is None:
                raise UnknownReceiptError(f"unknown receipt id: {op.receipt_id!r}")
            self._manifests[op.manifest_id] = _Manifest(
                manifest_id=op.manifest_id,
                receipt_id=op.receipt_id,
                lane_type=op.lane_type,
            )
            self._place(receipt)
            return None
        if isinstance(op, RevokeManifest):
            manifest = self._manifests.get(op.manifest_id)
            if manifest is None:
                raise UnknownManifestError(f"unknown manifest id: {op.manifest_id!r}")
            if manifest.revoked:
                raise ManifestStateError(
                    f"manifest {op.manifest_id!r} is already revoked"
                )
            manifest.revoked = True
            self._place(self._receipts[manifest.receipt_id])
            return None
        if isinstance(op, WithdrawReceipt):
            receipt = self._receipts.get(op.receipt_id)
            if receipt is None:
                raise UnknownReceiptError(f"unknown receipt id: {op.receipt_id!r}")
            if receipt.withdrawn:
                raise ReceiptStateError(
                    f"receipt {op.receipt_id!r} is already withdrawn"
                )
            receipt.withdrawn = True
            self._place(receipt)
            return None
        raise TypeError(f"unsupported operation: {op!r}")

    def _ingest(self, extracted: _Extracted) -> int:
        receipt_id = self._next_receipt_id
        self._next_receipt_id += 1
        receipt = _Receipt(
            receipt_id=receipt_id,
            binding=extracted.binding,
            raw=extracted.raw,
            event_id=extracted.event_id,
            source=extracted.source,
            event_type=extracted.event_type,
            lane_present=extracted.lane_present,
            lane_raw=extracted.lane_raw,
            lane_type=extracted.lane_type,
            body_kind=extracted.body_kind,
            body_value=extracted.body_value,
        )
        self._receipts[receipt_id] = receipt
        self._place(receipt)
        return receipt_id

    def _active_manifests(self, receipt_id: int) -> list[_Manifest]:
        return [
            manifest
            for manifest in self._manifests.values()
            if manifest.receipt_id == receipt_id and not manifest.revoked
        ]

    def _resolve(self, receipt: _Receipt) -> tuple[str, LineKey | None]:
        if receipt.withdrawn:
            return STATUS_WITHDRAWN, None
        if not receipt.lane_present:
            return STATUS_ASSIGNED, self._make_key(receipt, None)
        if receipt.lane_type is not None:
            return STATUS_ASSIGNED, self._make_key(receipt, receipt.lane_type)
        active = self._active_manifests(receipt.receipt_id)
        if not active:
            return STATUS_PENDING, None
        declared = {manifest.lane_type for manifest in active}
        if len(declared) != 1:
            return STATUS_CONFLICTED, None
        return STATUS_ASSIGNED, self._make_key(receipt, declared.pop())

    def _make_key(self, receipt: _Receipt, lane_type: str | None) -> LineKey:
        lane_value: int | str | None = None
        if receipt.lane_present and lane_type is not None:
            lane_value = _normalize_lane_value(lane_type, receipt.lane_raw)
        return LineKey(
            source=receipt.source,
            event_type=receipt.event_type,
            lane_type=lane_type if receipt.lane_present else None,
            lane_value=lane_value,
            body_kind=receipt.body_kind,
            body_value=receipt.body_value,
        )

    def _place(self, receipt: _Receipt) -> None:
        _, key = self._resolve(receipt)
        if key is None:
            if receipt.line_no is not None:
                self._lines_by_no[receipt.line_no].members.remove(receipt.receipt_id)
                receipt.line_no = None
            return
        line = self._lines.get(key)
        if line is None:
            line = _Line(line_no=self._next_line_no, key=key, members=[])
            self._next_line_no += 1
            self._lines[key] = line
            self._lines_by_no[line.line_no] = line
        if receipt.line_no == line.line_no:
            return
        if receipt.line_no is not None:
            self._lines_by_no[receipt.line_no].members.remove(receipt.receipt_id)
        line.members.append(receipt.receipt_id)
        receipt.line_no = line.line_no

    def _membership(self) -> dict[int, tuple[int, ...]]:
        return {
            line_no: tuple(line.members) for line_no, line in self._lines_by_no.items()
        }

    def _diff(self, before: dict[int, tuple[int, ...]]) -> tuple[LineChange, ...]:
        after = self._membership()
        changes = []
        for line_no in sorted(set(before) | set(after)):
            old = before.get(line_no, ())
            new = after.get(line_no, ())
            if old == new:
                continue
            changes.append(
                LineChange(
                    line_no=line_no,
                    added=tuple(member for member in new if member not in old),
                    removed=tuple(member for member in old if member not in new),
                    created=line_no not in before,
                )
            )
        return tuple(changes)
