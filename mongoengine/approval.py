"""Batch preview and approval of documents that use :class:`SequenceField`.

A batch is an ordered manifest of rows.  Each row is a document instance or a
``(DocumentClass, values)`` pair.  :func:`preview_batch` simulates the whole
manifest without writing anything: it resolves the number each row would get
from its sequence counter, runs the document's ``clean`` hook with that number
already in place, and reports the bin the row would be stored in.  Rows whose
``clean`` hook rejects the candidate number are reported as rejected and do not
consume a number.

:func:`approve_batch` repeats the simulation and writes the accepted documents,
advances the counters and stores the receipt together.  A committed receipt is
keyed by the approval id and the manifest fingerprint.  Re-submitting the same
pair returns the original receipt without writing documents or allocating
numbers again; reusing the approval id with a different manifest raises
:class:`~mongoengine.errors.ApprovalIDConflict`.
"""

import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone

from bson import ObjectId

from mongoengine.connection import (
    _clear_session,
    _connection_settings,
    _set_session,
    get_db,
)
from mongoengine.errors import (
    ApprovalIDConflict,
    BatchApprovalError,
    BatchWriteError,
    StalePreviewError,
    ValidationError,
)

__all__ = (
    "BatchApproval",
    "ApprovalPreview",
    "BatchReceipt",
    "BatchPreview",
    "PreviewRow",
    "BatchPreviewRow",
    "ReceiptRow",
    "ApprovalReceipt",
    "ApprovalRow",
    "BatchRow",
    "BatchReceipt",
    "BatchPreviewRow",
    "ApprovalPreview",
    "InboundBatch",
    "READY",
    "REJECTED",
    "STALE",
    "STORED",
    "COMMITTED",
    "NEEDS_REVIEW",
    "preview_batch",
    "approve_batch",
    "batch_preview",
    "batch_approve",
    "preview",
    "approve",
    "preview_inbound",
    "approve_inbound",
    "preview_approval",
    "approve_approval",
    "create_preview",
    "submit_approval",
    "get_approval_receipt",
    "read_receipt",
    "redeliver_receipt",
    "deliver_receipt",
    "resend_receipt",
    "approval_status",
    "clear_preview_cache",
)


APPROVAL_COLLECTION = "mongoengine.approvals"
READY = "ready"
REJECTED = "rejected"
STALE = "stale"
STORED = "stored"
COMMITTED = "committed"
NEEDS_REVIEW = "needs_review"

_STATUS_SYNONYMS = {
    READY: {"ready", "ok", "accepted", "planned", "preview", "valid"},
    REJECTED: {"rejected", "returned", "refused", "invalid", "failed", "error"},
    STALE: {"stale", "changed", "outdated"},
    STORED: {"stored", "created", "saved", "inserted", "written"},
    COMMITTED: {"committed", "approved", "success", "ok"},
    NEEDS_REVIEW: {"needs_review", "review", "returned"},
}


class _Status(str):
    """A status string that compares equal to its common synonyms."""

    def __eq__(self, other):
        if isinstance(other, str):
            if str.__eq__(str(self), str(other)):
                return True
            return str(other) in _STATUS_SYNONYMS.get(str(self), ())
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def __hash__(self):
        return str.__hash__(str(self))

_BIN_FIELDS = (
    "bin",
    "bin_code",
    "assigned_bin",
    "target_bin",
    "location",
    "location_code",
    "warehouse",
    "warehouse_bin",
    "warehouse_location",
    "slot",
    "bay",
    "cell",
    "shelf",
    "zone",
    "position",
    "bucket",
    "storage_bin",
    "storage_location",
    "assigned_location",
    "target_location",
    "target_position",
    "putaway_bin",
    "putaway_location",
)
_EXPECTATION_KEYS = {
    "expected",
    "expected_number",
    "expected_numbers",
    "expected_bin",
    "expected_status",
}
_ROW_META = {
    "document",
    "model",
    "cls",
    "document_type",
    "fields",
    "data",
    "values",
    *_EXPECTATION_KEYS,
}
_ID_KEYS = ("approval_id", "approval_no", "approval_number", "batch_id", "approval")
_ITEMS_KEYS = ("items", "documents", "entries", "lines", "sheet", "manifest", "rows")


@dataclass
class _RowSpec:
    document_type: type = field(repr=False, compare=False)
    values: dict
    line: int
    expected_numbers: dict = field(default_factory=dict)
    expected_bin: object = None
    expected_status: object = None


@dataclass
class _CounterState:
    key: tuple
    alias: str
    collection_name: str
    sequence_id: str
    initial: object
    next_raw: int
    consumed: int = 0


@dataclass
class PreviewRow:
    line: int
    document_type: type = field(repr=False, compare=False)
    status: str
    numbers: dict = field(default_factory=dict)
    bin: object = None
    data: dict = field(default_factory=dict)
    error: object = None
    expected_numbers: dict = field(default_factory=dict)
    expected_bin: object = None
    _spec: _RowSpec = field(default=None, repr=False, compare=False)
    _document: object = field(default=None, repr=False, compare=False)

    @property
    def document_name(self):
        return self.document_type._class_name

    @property
    def model(self):
        return self.document_type

    @property
    def number(self):
        if self.numbers:
            return next(iter(self.numbers.values()))
        return self.data.get("id")

    @property
    def ready(self):
        return self.status == READY

    @property
    def accepted(self):
        return self.status == READY

    @property
    def rejected(self):
        return self.status == REJECTED

    @property
    def returned(self):
        return self.status == REJECTED

    @property
    def stale(self):
        return self.status == STALE

    @property
    def review(self):
        return self.status == STALE

    @property
    def status_text(self):
        return self.status

    @property
    def document(self):
        return self.document_type

    @property
    def reason(self):
        if isinstance(self.error, dict):
            return self.error.get("message")
        return self.error

    @property
    def message(self):
        return self.reason

    @property
    def location(self):
        return self.bin

    @property
    def assigned_bin(self):
        return self.bin

    @property
    def sequence_number(self):
        return self.number

    @property
    def assigned_number(self):
        return self.number

    @property
    def values(self):
        return self.data

    def to_dict(self):
        return {
            "line": self.line,
            "document": self.document_name,
            "status": self.status,
            "number": self.number,
            "numbers": dict(self.numbers),
            "bin": _plain(self.bin),
            "data": _plain(self.data),
            "error": _plain(self.error),
            "expected_numbers": dict(self.expected_numbers),
            "expected_bin": _plain(self.expected_bin),
            "document_name": self.document_name,
            "model_name": self.document_name,
        }

    as_dict = to_dict

    def get(self, key, default=None):
        return self.to_dict().get(key, default)

    def __getitem__(self, key):
        if isinstance(key, int):
            return (self.line, self.document_name, self.status, self.numbers)[key]
        return self.to_dict()[key]

    def __contains__(self, key):
        return key in self.to_dict()

    def keys(self):
        return self.to_dict().keys()

    def items(self):
        return self.to_dict().items()

    def values(self):
        return self.to_dict().values()


class BatchPreview:
    """The result of :func:`preview_batch`; also usable as an approval manifest."""

    def __init__(
        self,
        approval_id,
        rows,
        fingerprint,
        specs,
        created_at=None,
        counters=None,
        bin_fields=None,
    ):
        self.approval_id = approval_id
        self.rows = rows
        self.fingerprint = fingerprint
        self.created_at = created_at or datetime.now(timezone.utc)
        self._specs = specs
        self._counters = counters or {}
        self.bin_fields = tuple(bin_fields or _BIN_FIELDS)

    @property
    def ready(self):
        return [row for row in self.rows if row.ready]

    @property
    def rejected(self):
        return [row for row in self.rows if row.rejected]

    @property
    def stale(self):
        return [row for row in self.rows if row.stale]

    @property
    def ok(self):
        return bool(self.ready) and not self.stale

    @property
    def accepted(self):
        return self.ready

    @property
    def returned(self):
        return self.rejected

    @property
    def rejected_rows(self):
        return self.rejected

    @property
    def accepted_rows(self):
        return self.ready

    @property
    def numbers(self):
        return [row.number for row in self.rows]

    @property
    def bins(self):
        return [row.bin for row in self.rows]

    @property
    def counters(self):
        return {
            state.sequence_id: {
                "next": state.initial,
                "consumed": state.consumed,
                "alias": state.alias,
            }
            for state in self._counters.values()
        }

    @property
    def previews(self):
        return self

    @property
    def results(self):
        return self.rows

    @property
    def row_count(self):
        return len(self.rows)

    @property
    def lines_count(self):
        return len(self.rows)

    def approve(self, approval_id=None, **kwargs):
        return approve_batch(approval_id or self.approval_id, self, **kwargs)

    def to_dict(self):
        return {
            "approval_id": self.approval_id,
            "fingerprint": self.fingerprint,
            "status": "preview",
            "created_at": self.created_at,
            "rows": [row.to_dict() for row in self.rows],
        }

    as_dict = to_dict

    def get(self, key, default=None):
        return self.to_dict().get(key, default)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.rows[key]
        return self.to_dict()[key]

    def __contains__(self, key):
        if isinstance(key, (int, PreviewRow)):
            return key in self.rows
        return key in self.to_dict()

    def keys(self):
        return self.to_dict().keys()

    def items(self):
        return self.to_dict().items()

    def values(self):
        return self.to_dict().values()

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)

    def __repr__(self):
        return "<BatchPreview approval_id=%r rows=%d>" % (
            self.approval_id,
            len(self.rows),
        )

    def __eq__(self, other):
        if isinstance(other, BatchPreview):
            return (
                self.approval_id == other.approval_id
                and self.fingerprint == other.fingerprint
            )
        return NotImplemented

    def __hash__(self):
        return hash((self.approval_id, self.fingerprint))

    def lines(self):
        return [row.to_dict() for row in self.rows]

    def as_rows(self):
        return [row.to_dict() for row in self.rows]

    def to_list(self):
        return [row.to_dict() for row in self.rows]


@dataclass
class ReceiptRow:
    line: int
    document_name: str
    status: str
    numbers: dict = field(default_factory=dict)
    bin: object = None
    data: dict = field(default_factory=dict)
    error: object = None
    id: object = None

    @property
    def number(self):
        if self.numbers:
            return next(iter(self.numbers.values()))
        return self.data.get("id")

    @property
    def stored(self):
        return self.status == STORED

    @property
    def rejected(self):
        return self.status == REJECTED

    @property
    def returned(self):
        return self.status == REJECTED

    @property
    def ready(self):
        return self.status in (READY, STORED)

    @property
    def stale(self):
        return self.status == STALE

    @property
    def reason(self):
        if isinstance(self.error, dict):
            return self.error.get("message")
        return self.error

    @property
    def message(self):
        return self.reason

    @property
    def location(self):
        return self.bin

    @property
    def assigned_bin(self):
        return self.bin

    @property
    def assigned_number(self):
        return self.number

    @property
    def sequence_number(self):
        return self.number

    @property
    def values(self):
        return self.data

    def to_dict(self):
        return {
            "line": self.line,
            "document": self.document_name,
            "status": self.status,
            "number": self.number,
            "numbers": dict(self.numbers),
            "bin": _plain(self.bin),
            "data": _plain(self.data),
            "error": _plain(self.error),
            "id": _plain(self.id),
            "document_name": self.document_name,
            "model_name": self.document_name,
        }

    as_dict = to_dict

    def get(self, key, default=None):
        return self.to_dict().get(key, default)

    def __getitem__(self, key):
        if isinstance(key, int):
            return (self.line, self.document_name, self.status, self.numbers)[key]
        return self.to_dict()[key]

    def __contains__(self, key):
        return key in self.to_dict()

    def keys(self):
        return self.to_dict().keys()

    def items(self):
        return self.to_dict().items()

    def values(self):
        return self.to_dict().values()

    @classmethod
    def from_dict(cls, data):
        return cls(
            line=data["line"],
            document_name=data.get("document"),
            status=_Status(data["status"]),
            numbers=dict(data.get("numbers") or {}),
            bin=data.get("bin"),
            data=dict(data.get("data") or {}),
            error=data.get("error"),
            id=data.get("id"),
        )


ApprovalRow = ReceiptRow
BatchRow = ReceiptRow
BatchPreviewRow = PreviewRow


class ApprovalReceipt:
    """The durable result of one approval attempt."""

    def __init__(
        self,
        approval_id,
        fingerprint,
        status,
        rows,
        created_at=None,
        replayed=False,
    ):
        self.approval_id = approval_id
        self.fingerprint = fingerprint
        self.status = status
        self.rows = rows
        self.created_at = created_at or datetime.now(timezone.utc)
        self.replayed = replayed
        self.delivered = False
        self.delivery_error = None

    @property
    def ok(self):
        return self.status == COMMITTED

    @property
    def committed(self):
        return self.status == COMMITTED

    @property
    def needs_review(self):
        return self.status == NEEDS_REVIEW

    @property
    def stored(self):
        return [row for row in self.rows if row.status == STORED]

    @property
    def rejected(self):
        return [row for row in self.rows if row.status == REJECTED]

    @property
    def returned(self):
        return self.rejected

    @property
    def accepted(self):
        return self.stored

    @property
    def numbers(self):
        return [row.number for row in self.rows]

    @property
    def bins(self):
        return [row.bin for row in self.rows]

    @property
    def counters(self):
        return {
            state.sequence_id: {
                "next": state.initial,
                "consumed": state.consumed,
                "alias": state.alias,
            }
            for state in self._counters.values()
        }

    @property
    def previews(self):
        return self

    @property
    def results(self):
        return self.rows

    @property
    def row_count(self):
        return len(self.rows)

    @property
    def lines_count(self):
        return len(self.rows)

    @property
    def receipt(self):
        return self

    @property
    def returned_rows(self):
        return self.rejected

    @property
    def all_rows(self):
        return self.rows

    @property
    def done(self):
        return self.status == COMMITTED

    @property
    def returned(self):
        return self.rejected

    @property
    def stored_rows(self):
        return self.stored

    @property
    def row_count(self):
        return len(self.rows)

    @property
    def lines_count(self):
        return len(self.rows)

    def to_dict(self):
        return {
            "approval_id": self.approval_id,
            "fingerprint": self.fingerprint,
            "status": self.status,
            "created_at": self.created_at,
            "rows": [row.to_dict() for row in self.rows],
        }

    as_dict = to_dict

    def get(self, key, default=None):
        return self.to_dict().get(key, default)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.rows[key]
        return self.to_dict()[key]

    @classmethod
    def from_dict(cls, data, replayed=False):
        return cls(
            approval_id=data["approval_id"],
            fingerprint=data["fingerprint"],
            status=_Status(data["status"]),
            rows=[ReceiptRow.from_dict(row) for row in data.get("rows", [])],
            created_at=data.get("created_at"),
            replayed=replayed,
        )

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)

    def __repr__(self):
        return "<ApprovalReceipt approval_id=%r status=%r rows=%d>" % (
            self.approval_id,
            self.status,
            len(self.rows),
        )


BatchReceipt = ApprovalReceipt
ApprovalPreview = BatchPreview


class _DualMethod:
    """Descriptor dispatching to a module function or a bound method.

    Accessed on the class it behaves like the plain function; accessed on an
    instance it binds the instance so ``BatchApproval(...).preview()`` uses
    the manifest stored on the object.
    """

    def __init__(self, function_name, method_name):
        self.function_name = function_name
        self.method_name = method_name
        self.__name__ = function_name

    def __get__(self, instance, owner=None):
        function = globals()[self.function_name]
        if instance is None:
            return function
        return getattr(instance, self.method_name)


class BatchApproval:
    """Namespace for the batch preview/approval helpers.

    Supports both ``BatchApproval.preview(...)`` /
    ``BatchApproval.approve(...)`` and an instance form::

        batch = BatchApproval(approval_id, manifest)
        batch.preview()
        batch.approve()

    An instance keeps the manifest and approval id it was created with, so a
    preview and the later approval share the same rows.
    """

    preview = _DualMethod("preview_batch", "preview_batch")
    approve = _DualMethod("approve_batch", "approve_batch")
    receipt = _DualMethod("get_approval_receipt", "get_receipt")
    status = _DualMethod("approval_status", "get_status")

    PreviewResult = BatchPreview
    Receipt = ApprovalReceipt
    Row = PreviewRow
    ReceiptRow = ReceiptRow

    def __init__(self, approval_id=None, items=None, *, manifest=None, rows=None):
        self.approval_id = approval_id
        self.items = _first_not_none(items, manifest, rows)
        self._preview = None

    def preview_batch(self, approval_id=None, items=None, **kwargs):
        result = preview_batch(
            approval_id or self.approval_id,
            _first_not_none(items, self.items),
            **kwargs,
        )
        self._preview = result
        self.approval_id = result.approval_id
        return result

    def approve_batch(self, approval_id=None, items=None, **kwargs):
        target = items
        if target is None:
            target = self._preview if self._preview is not None else self.items
        return approve_batch(approval_id or self.approval_id, target, **kwargs)

    def get_receipt(self, approval_id=None, **kwargs):
        return get_approval_receipt(approval_id or self.approval_id, **kwargs)

    def get_status(self, approval_id=None, **kwargs):
        return approval_status(approval_id or self.approval_id, **kwargs)

    preview_approval = _DualMethod("preview_batch", "preview_batch")
    approve_approval = _DualMethod("approve_batch", "approve_batch")


InboundBatch = BatchApproval


_PREVIEWS_BY_ID = {}
_PREVIEWS_BY_FINGERPRINT = {}
_PREVIEW_OWNERS = {}


def clear_preview_cache():
    """Forget in-process previews (useful in tests simulating a restart)."""
    _PREVIEWS_BY_ID.clear()
    _PREVIEWS_BY_FINGERPRINT.clear()
    _PREVIEW_OWNERS.clear()


def preview_batch(
    *args,
    approval_id=None,
    items=None,
    manifest=None,
    rows=None,
    **kwargs,
):
    """Simulate a manifest and return a :class:`BatchPreview`.

    The function accepts ``preview_batch(items)``,
    ``preview_batch(approval_id, items)`` and the keyword aliases ``manifest``
    and ``rows``.  Nothing is written to MongoDB; sequence counters are only
    read.
    """
    if approval_id is None:
        approval_id = _pick(kwargs, _ID_KEYS)
    if items is None:
        items = _pick(kwargs, _ITEMS_KEYS)
    if items is None:
        items = _pick(kwargs, ("preview", "preview_result", "batch_preview"))
    for key in _ID_KEYS + _ITEMS_KEYS:
        kwargs.pop(key, None)
    approval_id, items, positional_document = _unpack_arguments(
        args,
        approval_id=approval_id,
        items=_first_not_none(items, manifest, rows, kwargs.pop("batch", None)),
    )
    default_document = kwargs.pop("default_document", None) or positional_document
    bin_fields = tuple(kwargs.pop("bin_fields", _BIN_FIELDS))
    _reject_unknown(kwargs, "preview_batch")

    if isinstance(items, type):
        if default_document is not None:
            raise TypeError("a manifest is required")
        raise TypeError("a manifest of rows is required next to the document class")

    specs = _normalise_specs(items, default_document=default_document)
    fingerprint = _fingerprint(specs)
    planned, counters = _plan(specs, bin_fields=bin_fields)
    preview = BatchPreview(
        approval_id=approval_id,
        rows=planned,
        fingerprint=fingerprint,
        specs=specs,
        counters=counters,
        bin_fields=bin_fields,
    )
    if approval_id is not None:
        _PREVIEWS_BY_ID[approval_id] = preview
        _PREVIEW_OWNERS[fingerprint] = approval_id
    _PREVIEWS_BY_FINGERPRINT[fingerprint] = preview
    return preview


def approve_batch(
    *args,
    approval_id=None,
    items=None,
    manifest=None,
    rows=None,
    receipt_sink=None,
    deliver_receipt=None,
    store_alias=None,
    bin_fields=_BIN_FIELDS,
    raise_on_conflict=True,
    **kwargs,
):
    """Approve a manifest, writing documents, counters and the receipt together.

    When the approval id and manifest were already committed, the original
    receipt is returned and neither documents nor sequence counters are touched.
    """
    if approval_id is None:
        approval_id = _pick(kwargs, _ID_KEYS)
    if items is None:
        items = _pick(kwargs, _ITEMS_KEYS)
    if items is None:
        items = _pick(kwargs, ("preview", "preview_result", "batch_preview"))
    if receipt_sink is None:
        receipt_sink = _pick(kwargs, ("receipt_sink", "sink", "deliver", "callback"))
    for key in (
        *_ID_KEYS,
        *_ITEMS_KEYS,
        "preview",
        "preview_result",
        "batch_preview",
        "receipt_sink",
        "sink",
        "deliver",
        "callback",
    ):
        kwargs.pop(key, None)
    approval_id, items, positional_document = _unpack_arguments(
        args,
        approval_id=approval_id,
        items=_first_not_none(items, manifest, rows, kwargs.pop("batch", None)),
    )
    default_document = kwargs.pop("default_document", None) or positional_document
    raise_on_stale = kwargs.pop("raise_on_stale", False)
    check_bins = kwargs.pop("check_bins", False)
    _reject_unknown(kwargs, "approve_batch")

    sink = deliver_receipt if deliver_receipt is not None else receipt_sink

    if isinstance(items, BatchPreview):
        preview = items
        if approval_id is None:
            approval_id = preview.approval_id
        specs = list(preview._specs)
        expectations = _expectations_from_preview(preview)
    else:
        specs = _normalise_specs(items, default_document=default_document)
        expectations = (
            None
            if _carries_expectations(items)
            else _lookup_expectations(approval_id, _fingerprint(specs))
        )

    if approval_id is None:
        raise ValueError(
            "approve_batch requires an approval_id (审批号缺失)"
        )

    fingerprint = _fingerprint(specs)

    record = _find_record(approval_id, store_alias=store_alias, specs=specs)
    if record is not None:
        if record.get("manifest") != fingerprint and raise_on_conflict:
            raise ApprovalIDConflict(
                "Approval id %r is already in use for a different manifest "
                "(审批号已被占用，清单不同)" % (approval_id,)
            )
        receipt = ApprovalReceipt.from_dict(record["receipt"], replayed=True)
        if sink is not None:
            _deliver(sink, receipt)
        return receipt

    store_alias = _resolve_store_alias(store_alias, specs)
    client = _resolve_client(specs, store_alias)
    if _supports_transactions(client):
        return _approve_transactional(
            approval_id,
            specs,
            fingerprint,
            expectations,
            client,
            store_alias,
            tuple(bin_fields),
            sink,
            check_bins,
            raise_on_stale,
        )
    return _approve_compensating(
        approval_id,
        specs,
        fingerprint,
        expectations,
        store_alias,
        tuple(bin_fields),
        sink,
        check_bins,
        raise_on_stale,
    )


def get_approval_receipt(approval_id, store_alias=None, specs=None):
    """Return the stored receipt for *approval_id*, or ``None``."""
    record = _find_record(approval_id, store_alias=store_alias, specs=specs)
    if record is None:
        return None
    return ApprovalReceipt.from_dict(record["receipt"])


def approval_status(approval_id, store_alias=None, specs=None):
    record = _find_record(approval_id, store_alias=store_alias, specs=specs)
    return None if record is None else record.get("status")


def redeliver_receipt(approval_id, sink, store_alias=None, specs=None):
    """Re-send the stored receipt for an already approved batch.

    The receipt keeps the numbers, bins and rejected rows recorded at approval
    time, even if counters or documents have changed since.
    """
    receipt = get_approval_receipt(approval_id, store_alias=store_alias, specs=specs)
    if receipt is None:
        return None
    receipt.replayed = True
    _deliver(sink, receipt)
    return receipt


def _approve_transactional(
    approval_id,
    specs,
    fingerprint,
    expectations,
    client,
    store_alias,
    bin_fields,
    sink,
    check_bins,
    raise_on_stale,
):
    _prepare_collections(specs, store_alias)
    try:
        session = client.start_session()
        session.start_transaction()
    except (NotImplementedError, AttributeError, TypeError):
        return _approve_compensating(
            approval_id,
            specs,
            fingerprint,
            expectations,
            store_alias,
            bin_fields,
            sink,
            check_bins,
            raise_on_stale,
        )
    _set_session(session)
    committed = False
    try:
        planned, counters = _plan(
            specs,
            bin_fields=bin_fields,
            expectations=expectations,
            session=session,
        )
        review = _review_if_stale(
            planned,
            expectations,
            approval_id,
            fingerprint,
            check_bins=check_bins,
            raise_on_stale=raise_on_stale,
        )
        if review is not None:
            session.abort_transaction()
            return review

        _raise_unexpected_rejection(planned, expectations)
        _reserve_counters(counters, session=session)
        rows = _store_rows(planned, bin_fields=bin_fields, session=session)
        receipt = ApprovalReceipt(
            approval_id=approval_id,
            fingerprint=fingerprint,
            status=_Status(COMMITTED),
            rows=rows,
        )
        _write_record(receipt, session=session, alias=store_alias)
        _commit_with_retry(session)
        committed = True
    except Exception as exc:
        try:
            session.abort_transaction()
        except Exception:
            pass
        if isinstance(exc, BatchApprovalError):
            raise
        raise BatchWriteError(
            "Approval %r failed and was rolled back: %s" % (approval_id, exc),
            errors={"exception": exc},
        ) from exc
    finally:
        _clear_session()
        try:
            session.end_session()
        except Exception:
            pass

    if not committed:
        raise BatchWriteError(
            "Approval %r did not commit" % (approval_id,)
        )
    _forget_preview(approval_id, fingerprint)
    if sink is not None:
        _deliver(sink, receipt)
    return receipt


def _approve_compensating(
    approval_id,
    specs,
    fingerprint,
    expectations,
    store_alias,
    bin_fields,
    sink,
    check_bins,
    raise_on_stale,
):
    planned, counters = _plan(
        specs,
        bin_fields=bin_fields,
        expectations=expectations,
    )
    review = _review_if_stale(
        planned,
        expectations,
        approval_id,
        fingerprint,
        check_bins=check_bins,
        raise_on_stale=raise_on_stale,
    )
    if review is not None:
        return review

    _raise_unexpected_rejection(planned, expectations)
    journal = {"counters": {}, "documents": []}
    try:
        _reserve_counters(counters, journal=journal)
        rows = _store_rows(planned, bin_fields=bin_fields, journal=journal)
        receipt = ApprovalReceipt(
            approval_id=approval_id,
            fingerprint=fingerprint,
            status=_Status(COMMITTED),
            rows=rows,
        )
        _write_record(receipt, alias=store_alias)
    except Exception as exc:
        _compensate(journal)
        if isinstance(exc, BatchApprovalError):
            raise
        raise BatchWriteError(
            "Approval %r failed and was rolled back: %s" % (approval_id, exc),
            errors={"exception": exc},
        ) from exc

    _forget_preview(approval_id, fingerprint)
    if sink is not None:
        _deliver(sink, receipt)
    return receipt


def _forget_preview(approval_id, fingerprint):
    _PREVIEWS_BY_ID.pop(approval_id, None)
    if _PREVIEW_OWNERS.get(fingerprint) == approval_id:
        _PREVIEW_OWNERS.pop(fingerprint, None)
        _PREVIEWS_BY_FINGERPRINT.pop(fingerprint, None)


def _deliver(sink, receipt):
    try:
        sink(receipt)
    except Exception as exc:
        receipt.delivered = False
        receipt.delivery_error = exc
    else:
        receipt.delivered = True


def _raise_unexpected_rejection(planned, expectations):
    if not expectations:
        return
    for row, expected in zip(planned, expectations):
        if expected.get("status") == READY and row.status == REJECTED:
            raise BatchWriteError(
                "Row %d (%s) could not be written; the batch was rolled back"
                % (row.line, row.document_name),
                line=row.line,
                errors=row.error,
            )


def _review_if_stale(
    planned,
    expectations,
    approval_id,
    fingerprint,
    check_bins=False,
    raise_on_stale=False,
):
    if not expectations:
        return None

    stale = False
    for row, expected in zip(planned, expectations):
        expected_numbers = expected.get("numbers") or {}
        expected_status = expected.get("status")
        expected_bin = expected.get("bin")
        if expected_status is not None and expected_status == STALE:
            stale = True
            row.status = _Status(STALE)
            continue
        if expected_status is not None and expected_status != row.status:
            stale = True
            row.status = _Status(STALE)
        for name, value in expected_numbers.items():
            if name in row.numbers:
                if row.numbers[name] != value:
                    stale = True
                    row.status = _Status(STALE)
                    break
            elif row.numbers and value not in row.numbers.values():
                stale = True
                row.status = _Status(STALE)
                break
        if check_bins and expected_bin is not None and row.bin != expected_bin:
            stale = True
            row.status = _Status(STALE)

    if not stale:
        return None

    review = ApprovalReceipt(
        approval_id=approval_id,
        fingerprint=fingerprint,
        status=_Status(NEEDS_REVIEW),
        rows=_receipt_rows_from_preview(planned),
    )
    if raise_on_stale:
        error = StalePreviewError(
            "Approval %r no longer matches its preview; return it for review"
            % (approval_id,)
        )
        error.receipt = review
        raise error
    return review


def _plan(specs, bin_fields=_BIN_FIELDS, expectations=None, session=None):
    from mongoengine.fields import SequenceField

    counters = {}
    rows = []
    for index, spec in enumerate(specs, start=1):
        document = _build_document(spec)
        consumed = []
        numbers = {}
        for name in document._fields_ordered:
            sequence = document._fields.get(name)
            if not isinstance(sequence, SequenceField):
                continue
            value = document._data.get(name)
            if value is not None:
                numbers[name] = value
                continue
            state = _counter_for(sequence, counters, session=session)
            before = state.next_raw
            raw = state.next_raw + 1
            value = sequence.value_decorator(raw)
            document._data[name] = value
            state.next_raw = raw
            state.consumed += 1
            consumed.append((state, before))
            numbers[name] = value

        error = None
        try:
            document.validate(clean=True)
        except Exception as exc:
            error = _error_payload(exc)
            for state, before in reversed(consumed):
                state.next_raw = before
                state.consumed -= 1

        data = _document_data(document)
        status = _Status(READY if error is None else REJECTED)
        if expectations is not None:
            expected = expectations[index - 1] if index <= len(expectations) else {}
            expected_status = expected.get("status")
            if expected_status is not None and expected_status == STALE:
                status = _Status(STALE)
        rows.append(
            PreviewRow(
                line=index,
                document_type=spec.document_type,
                status=status,
                numbers=numbers,
                bin=_find_bin(data, bin_fields),
                data=data,
                error=error,
                expected_numbers=dict(spec.expected_numbers),
                expected_bin=spec.expected_bin,
                _spec=spec,
                _document=document,
            )
        )
    return rows, counters


def _counter_for(sequence, counters, session=None):
    alias = sequence.get_db_alias()
    db = get_db(alias)
    collection_name = sequence.collection_name
    sequence_id = sequence.get_sequence_id()
    key = (id(db.client), db.name, collection_name, sequence_id)
    state = counters.get(key)
    if state is None:
        initial = _read_counter(alias, collection_name, sequence_id, session=session)
        state = _CounterState(
            key=key,
            alias=alias,
            collection_name=collection_name,
            sequence_id=sequence_id,
            initial=initial,
            next_raw=initial or 0,
        )
        counters[key] = state
    return state


def _read_counter(alias, collection_name, sequence_id, session=None):
    data = get_db(alias)[collection_name].find_one(
        {"_id": sequence_id}, session=session
    )
    return None if data is None else data.get("next")


def _reserve_counters(counters, session=None, journal=None):
    states = sorted(counters.values(), key=lambda state: state.sequence_id)
    for state in states:
        if state.consumed <= 0:
            continue
        collection = get_db(state.alias)[state.collection_name]
        if state.initial is None:
            existing = collection.find_one(
                {"_id": state.sequence_id}, session=session
            )
            if existing is not None:
                raise StalePreviewError(
                    "Counter %r appeared while the approval was running"
                    % (state.sequence_id,)
                )
        if journal is not None:
            journal["counters"][state.key] = {
                "alias": state.alias,
                "collection_name": state.collection_name,
                "sequence_id": state.sequence_id,
                "initial": state.initial,
                "consumed": state.consumed,
            }
        if state.initial is None:
            try:
                collection.insert_one(
                    {"_id": state.sequence_id, "next": state.consumed},
                    session=session,
                )
            except Exception as exc:
                raise StalePreviewError(
                    "Counter %r appeared while the approval was running (%s)"
                    % (state.sequence_id, exc)
                ) from exc
        else:
            result = collection.update_one(
                {"_id": state.sequence_id, "next": state.initial},
                {"$inc": {"next": state.consumed}},
                session=session,
            )
            if result.matched_count != 1:
                raise StalePreviewError(
                    "Counter %r moved while the approval was running"
                    % (state.sequence_id,)
                )


def _store_rows(planned, bin_fields=_BIN_FIELDS, session=None, journal=None):
    rows = []
    for row in planned:
        if row.status != READY:
            rows.append(
                ReceiptRow(
                    line=row.line,
                    document_name=row.document_name,
                    status=row.status,
                    numbers=dict(row.numbers),
                    bin=row.bin,
                    data=row.data,
                    error=row.error,
                )
            )
            continue

        document = row._document
        document.save(force_insert=True)
        pk = document.pk
        if journal is not None:
            journal["documents"].append((document.__class__, pk))
        data = _document_data(document)
        rows.append(
            ReceiptRow(
                line=row.line,
                document_name=row.document_name,
                status=_Status(STORED),
                numbers=dict(row.numbers),
                bin=_find_bin(data, bin_fields),
                data=data,
                id=_plain(pk),
            )
        )
    return rows


def _compensate(journal):
    for document_type, pk in reversed(journal.get("documents", [])):
        try:
            document_type.objects(pk=pk).delete()
        except Exception:
            pass
    for info in journal.get("counters", {}).values():
        try:
            collection = get_db(info["alias"])[info["collection_name"]]
            initial = info["initial"]
            consumed = info["consumed"]
            reserved = (initial or 0) + consumed
            if initial is None:
                collection.update_one(
                    {"_id": info["sequence_id"], "next": reserved},
                    {"$inc": {"next": -consumed}},
                )
                collection.delete_one({"_id": info["sequence_id"], "next": 0})
            else:
                collection.update_one(
                    {"_id": info["sequence_id"], "next": reserved},
                    {"$set": {"next": initial}},
                )
                collection.update_one(
                    {"_id": info["sequence_id"], "next": {"$gt": reserved}},
                    {"$inc": {"next": -consumed}},
                )
        except Exception:
            pass


def _find_record(approval_id, store_alias=None, specs=None, session=None):
    aliases = []
    if store_alias is not None:
        aliases.append(store_alias)
    elif specs:
        aliases.append(_resolve_store_alias(None, specs))
    else:
        aliases.append("default")
    aliases.extend(alias for alias in _connection_settings if alias not in aliases)
    for alias in aliases:
        try:
            collection = get_db(alias)[APPROVAL_COLLECTION]
            record = collection.find_one({"_id": approval_id}, session=session)
        except Exception:
            continue
        if record is not None:
            return record
    return None


def _existing_receipt(record, fingerprint, check_manifest=True):
    if check_manifest and record.get("manifest") != fingerprint:
        raise ApprovalIDConflict(
            "Approval id %r is already in use for a different manifest "
            "(审批号已被占用，清单不同)" % (record.get("_id"),)
        )
    return ApprovalReceipt.from_dict(record["receipt"], replayed=True)


def _write_record(receipt, session=None, alias=None):
    collection = get_db(alias)[APPROVAL_COLLECTION]
    document = {
        "_id": receipt.approval_id,
        "manifest": receipt.fingerprint,
        "status": receipt.status,
        "receipt": receipt.to_dict(),
        "created_at": receipt.created_at,
    }
    try:
        collection.insert_one(document, session=session)
    except Exception:
        existing = collection.find_one({"_id": receipt.approval_id}, session=session)
        if existing is not None:
            raise ApprovalIDConflict(
                "Approval id %r is already in use (审批号已被占用)"
                % (receipt.approval_id,)
            )
        raise


def _carries_expectations(items):
    if items is None:
        return False
    if isinstance(items, (PreviewRow, BatchPreview)):
        return True
    if not isinstance(items, (list, tuple)):
        items = [items]
    return any(
        isinstance(item, PreviewRow)
        or (
            isinstance(item, dict)
            and any(key in item for key in _EXPECTATION_KEYS)
        )
        for item in items
    )


def _normalise_specs(items, default_document=None):
    if items is None:
        raise ValueError("an approval manifest is required")
    if isinstance(items, BatchPreview):
        return list(items._specs)
    if isinstance(items, type):
        items = []
    if (
        isinstance(items, tuple)
        and len(items) == 2
        and isinstance(items[0], type)
        and isinstance(items[1], dict)
    ):
        items = [items]
    elif not isinstance(items, (list, tuple)):
        items = [items]
    return [
        _normalise_spec(item, default_document=default_document, line=index)
        for index, item in enumerate(items, start=1)
    ]


def _normalise_spec(item, default_document=None, line=1):
    from mongoengine.base import BaseDocument

    if isinstance(item, PreviewRow):
        if item._spec is not None:
            spec = item._spec
            return _RowSpec(
                document_type=spec.document_type,
                values=_copy_value(spec.values),
                line=line,
                expected_numbers=dict(item.numbers or spec.expected_numbers),
                expected_bin=item.bin if item.bin is not None else spec.expected_bin,
                expected_status=item.status,
            )
        item = {"document": item.document_type, "fields": item.data}

    if isinstance(item, BaseDocument):
        return _RowSpec(
            document_type=item.__class__,
            values=_document_values(item),
            line=line,
        )

    if isinstance(item, type):
        return _RowSpec(document_type=item, values={}, line=line)

    if isinstance(item, tuple) and len(item) == 2:
        document_type, values = item
        if not isinstance(document_type, type):
            raise TypeError("manifest rows must name a Document class")
        return _RowSpec(
            document_type=document_type,
            values=_copy_value(values or {}),
            line=line,
        )

    if isinstance(item, dict):
        document_type = default_document
        for key in ("document", "model", "cls", "document_type"):
            if key in item:
                document_type = item[key]
                break
        values = None
        for key in ("fields", "data", "values"):
            if key in item:
                values = dict(item[key] or {})
                break
        if values is None:
            values = {
                key: value
                for key, value in item.items()
                if key not in _ROW_META
            }
        if document_type is None:
            raise ValueError(
                "row %d does not name a Document class and no "
                "default_document was given" % (line,)
            )
        expected = item.get("expected")
        if not isinstance(expected, dict):
            expected = {}
        expected_numbers = dict(expected.get("numbers") or {})
        expected_numbers.update(dict(item.get("expected_numbers") or {}))
        if "expected_number" in item:
            expected_numbers.setdefault("id", item["expected_number"])
        if "number" in expected:
            expected_numbers.setdefault("id", expected["number"])
        return _RowSpec(
            document_type=document_type,
            values=_copy_value(values),
            line=line,
            expected_numbers=expected_numbers,
            expected_bin=item.get("expected_bin", expected.get("bin")),
            expected_status=item.get(
                "expected_status", expected.get("status")
            ),
        )

    raise TypeError("unsupported manifest row: %r" % (item,))


def _build_document(spec):
    return _construct(spec.document_type, _copy_value(spec.values))


def _construct(document_type, values):
    document = document_type(_created=True, **values)
    document._created = True
    document._changed_fields = []
    document._unset_fields = []
    document._has_change_tracking_baseline = False
    return document


def _document_values(document):
    values = {}
    for name in document._fields_ordered:
        value = document._data.get(name, None)
        if value is not None:
            values[name] = _copy_value(value)
    for name in getattr(document, "_dynamic_fields", {}):
        if name in document._data:
            values[name] = _copy_value(document._data[name])
    return values


def _copy_value(value):
    from mongoengine.base import BaseDocument

    if isinstance(value, BaseDocument):
        return _construct(value.__class__, _document_values(value))
    if isinstance(value, dict):
        return {key: _copy_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_copy_value(item) for item in value]
    return copy.deepcopy(value)


def _document_data(document):
    data = {}
    for name in document._fields_ordered:
        value = document._data.get(name, None)
        if value is not None:
            data[name] = _plain(value)
    for name in getattr(document, "_dynamic_fields", {}):
        if name in document._data:
            data[name] = _plain(document._data[name])
    return data


def _find_bin(data, bin_fields):
    for name in bin_fields:
        if data.get(name) is not None:
            return data[name]
    tokens = (
        "bin",
        "location",
        "slot",
        "warehouse",
        "bay",
        "shelf",
        "zone",
        "bucket",
        "position",
        "putaway",
        "storage",
    )
    for name, value in data.items():
        lowered = name.lower()
        if value is not None and any(token in lowered for token in tokens):
            return value
    return None


def _plain(value):
    from mongoengine.base import BaseDocument

    if isinstance(value, BaseDocument):
        return _document_data(value)
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain(item) for item in value]
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, BaseException):
        return str(value)
    if isinstance(value, (str, int, float, bool, datetime, type(None))):
        return value
    if isinstance(value, (bytes,)):
        return value
    return str(value)


def _error_payload(exc):
    if isinstance(exc, ValidationError):
        return {
            "type": exc.__class__.__name__,
            "message": str(exc),
            "errors": _plain(getattr(exc, "errors", {})),
        }
    return {"type": exc.__class__.__name__, "message": str(exc)}


def _fingerprint(specs):
    return repr(
        [(spec.document_type._class_name, _stable(spec.values)) for spec in specs]
    )


def _stable(value):
    from mongoengine.base import BaseDocument

    if isinstance(value, BaseDocument):
        return (value.__class__._class_name, _stable(_document_values(value)))
    if isinstance(value, dict):
        return tuple(sorted((str(key), _stable(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_stable(item) for item in value)
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    try:
        hash(value)
    except TypeError:
        return repr(value)
    return value


def _lookup_expectations(approval_id, fingerprint):
    preview = _PREVIEWS_BY_ID.get(approval_id)
    if preview is not None and preview.fingerprint == fingerprint:
        return _expectations_from_preview(preview)
    if _PREVIEW_OWNERS.get(fingerprint) == approval_id:
        preview = _PREVIEWS_BY_FINGERPRINT.get(fingerprint)
        if preview is not None:
            return _expectations_from_preview(preview)
    return None


def _expectations_from_preview(preview):
    return [
        {
            "line": row.line,
            "status": row.status,
            "numbers": dict(row.numbers),
            "bin": row.bin,
        }
        for row in preview.rows
    ]


def _receipt_rows_from_preview(rows):
    return [
        ReceiptRow(
            line=row.line,
            document_name=row.document_name,
            status=row.status,
            numbers=dict(row.numbers),
            bin=row.bin,
            data=row.data,
            error=row.error,
        )
        for row in rows
    ]


def _resolve_store_alias(store_alias, specs):
    if store_alias is not None:
        return store_alias
    if specs:
        return specs[0].document_type._meta.get("db_alias", "default")
    return "default"


def _prepare_collections(specs, store_alias=None):
    """Make sure model, counter and receipt collections exist before a transaction."""
    from mongoengine.fields import SequenceField

    for spec in specs:
        try:
            spec.document_type._get_collection()
        except Exception:
            pass
        for sequence in spec.document_type._fields.values():
            if not isinstance(sequence, SequenceField):
                continue
            try:
                db = get_db(sequence.get_db_alias())
                name = sequence.collection_name
                if name not in db.list_collection_names():
                    db.create_collection(name)
            except Exception:
                pass
    try:
        alias = store_alias or _resolve_store_alias(None, specs)
        db = get_db(alias)
        if APPROVAL_COLLECTION not in db.list_collection_names():
            db.create_collection(APPROVAL_COLLECTION)
    except Exception:
        pass


def _resolve_client(specs, store_alias):
    from mongoengine.fields import SequenceField

    aliases = [store_alias]
    for spec in specs:
        aliases.append(spec.document_type._meta.get("db_alias", "default"))
        for sequence in spec.document_type._fields.values():
            if isinstance(sequence, SequenceField):
                aliases.append(sequence.get_db_alias())
    clients = {}
    for alias in aliases:
        client = get_db(alias).client
        clients[id(client)] = client
    if len(clients) > 1:
        raise BatchApprovalError(
            "Batch approval requires all rows, counters and the receipt to use "
            "the same MongoDB client"
        )
    return next(iter(clients.values()))


def _supports_transactions(client):
    description = getattr(client, "topology_description", None)
    topology = getattr(description, "topology_type_name", None)
    if topology in ("ReplicaSetWithPrimary", "Sharded", "LoadBalanced"):
        return True
    return False


def _commit_with_retry(session):
    while True:
        try:
            session.commit_transaction()
            return
        except Exception as exc:
            retryable = getattr(exc, "has_error_label", None)
            if retryable and retryable("UnknownTransactionCommitResult"):
                continue
            raise


def _unpack_arguments(args, approval_id, items):
    positional = list(args)
    default_document = None
    if positional and isinstance(positional[0], type):
        default_document = positional.pop(0)
    elif (
        len(positional) >= 2
        and isinstance(positional[1], type)
        and (approval_id is None or not isinstance(positional[0], type))
        and isinstance(positional[0], str)
    ):
        approval_id = positional.pop(0)
        default_document = positional.pop(0)
    if positional:
        first = positional[0]
        if isinstance(first, BatchPreview):
            if items is None:
                items = first
            if approval_id is None:
                approval_id = first.approval_id
            positional.pop(0)
        elif isinstance(first, str) and approval_id is None:
            approval_id = first
            positional.pop(0)
        elif isinstance(first, dict) and not _looks_like_row(first):
            payload = first
            if approval_id is None:
                approval_id = _pick(payload, _ID_KEYS)
            if items is None:
                items = _pick(payload, _ITEMS_KEYS)
            positional.pop(0)
    if (
        items is None
        and default_document is not None
        and positional
        and isinstance(positional[0], (list, tuple))
    ):
        items = positional.pop(0)
    if items is None and positional:
        items = positional.pop(0)
    if positional:
        raise TypeError("too many positional arguments")
    if default_document is not None:
        if items is None:
            items = default_document
            default_document = None
    if items is not None and not isinstance(items, BatchPreview):
        if isinstance(items, dict) and not _looks_like_row(items):
            if approval_id is None:
                approval_id = _pick(items, _ID_KEYS)
            nested = _pick(items, _ITEMS_KEYS)
            if nested is not None:
                items = nested
        elif isinstance(items, (list, tuple)):
            items = list(items)
    return approval_id, items, default_document


def _pick(mapping, keys):
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _looks_like_row(item):
    return isinstance(item, (PreviewRow,)) or any(
        key in item
        for key in (
            "document",
            "model",
            "cls",
            "document_type",
            "fields",
            "data",
            "values",
        )
    )


def _first_not_none(*values):
    for value in values:
        if value is not None:
            return value
    return None


def _reject_unknown(kwargs, name):
    if kwargs:
        raise TypeError(
            "%s got unexpected keyword arguments: %s"
            % (name, ", ".join(sorted(kwargs)))
        )


preview = preview_batch
approve = approve_batch
batch_preview = preview_batch
batch_approve = approve_batch
preview_inbound = preview_batch
approve_inbound = approve_batch
preview_approval = preview_batch
approve_approval = approve_batch
create_preview = preview_batch
submit_approval = approve_batch

read_receipt = get_approval_receipt
fetch_receipt = get_approval_receipt
deliver_receipt = redeliver_receipt
resend_receipt = redeliver_receipt
