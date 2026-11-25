"""Deterministic, idempotent delivery of category revisions.

A category correction is turned at the repartition boundary into one leg per
affected category.  All legs for the same source revision share a
``correlation_id`` and have deterministic ``receipt_id`` values.  This lets
independent category shards deduplicate replays and lets a publication gate
wait until every leg for a revision has evidence.

The module is intentionally free of Faust/Kafka types so the ordering and
acknowledgement state machine can be tested without a broker.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Iterable, Literal, Mapping


class RevisionState(str, Enum):
    """Lifecycle state for one source revision."""

    BUFFERED = "buffered"
    DELIVERABLE = "deliverable"
    PARTIAL = "partial"
    READY = "ready"
    PUBLISHED = "published"
    DUPLICATE = "duplicate"


class ChangeSide(str, Enum):
    """Which end of a category move a leg represents."""

    FROM = "from"
    TO = "to"
    ATTRIBUTE = "attribute"


_FIELD_NAMES: dict[str, tuple[str, ...]] = {
    "item_id": ("item_id", "product_id", "goods_id", "sku", "item", "id"),
    "revision": ("revision", "revision_number", "rev", "version", "revision_id"),
    "old_category": (
        "old_category",
        "from_category",
        "previous_category",
        "before_category",
        "old",
        "from",
    ),
    "new_category": (
        "new_category",
        "to_category",
        "current_category",
        "after_category",
        "category",
        "new",
        "to",
    ),
    "quantity": ("quantity", "qty", "count", "units"),
    "attributes": ("attributes", "attrs", "changes", "properties"),
}


class RevisionDeliveryError(ValueError):
    """Base error for invalid revision delivery state."""


class InvalidRevisionError(RevisionDeliveryError):
    """Raised when a source revision is malformed."""


class RevisionGapError(RevisionDeliveryError):
    """Raised when a revision cannot be ordered because a gap remains."""


class WrongShardError(RevisionDeliveryError):
    """Raised when a category leg is applied to the wrong shard."""


def _read_field(source: Any, name: str) -> Any:
    if isinstance(source, Mapping):
        for field_name in _FIELD_NAMES[name]:
            if field_name in source and source[field_name] is not None:
                return source[field_name]
        return None

    for field_name in _FIELD_NAMES[name]:
        if hasattr(source, field_name):
            value = getattr(source, field_name)
            if value is not None:
                return value
    return None


def _normalise_item_id(value: Any) -> str:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise InvalidRevisionError("item_id is required")
    return str(value)


def _normalise_revision(value: Any) -> int:
    if value is None:
        raise InvalidRevisionError("revision is required")
    if isinstance(value, bool):
        raise InvalidRevisionError("revision must be an integer")
    try:
        revision = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidRevisionError("revision must be an integer") from exc
    if revision < 0:
        raise InvalidRevisionError("revision must be non-negative")
    return revision


def _normalise_category(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    return value or None


@dataclass(frozen=True)
class RevisionRef:
    """Identity shared by every leg of one item revision."""

    item_id: str
    revision: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "item_id", _normalise_item_id(self.item_id))
        object.__setattr__(self, "revision", _normalise_revision(self.revision))

    @property
    def correlation_id(self) -> str:
        return f"{self.item_id}@r{self.revision}"

    @property
    def key(self) -> tuple[str, int]:
        return self.item_id, self.revision


@dataclass(frozen=True)
class Correction:
    """One supplier correction.

    ``old_category`` and ``new_category`` may both be supplied.  If only the
    new category is known, the planner's watermark/current category supplies
    the previous category.
    """

    item_id: str
    revision: int
    old_category: str | None = None
    new_category: str | None = None
    quantity: int = 1
    attributes: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        item_id = _normalise_item_id(self.item_id)
        revision = _normalise_revision(self.revision)
        old_category = _normalise_category(self.old_category)
        new_category = _normalise_category(self.new_category)
        if isinstance(self.quantity, bool) or not isinstance(self.quantity, int):
            raise InvalidRevisionError("quantity must be a positive integer")
        if self.quantity <= 0:
            raise InvalidRevisionError("quantity must be a positive integer")
        object.__setattr__(self, "item_id", item_id)
        object.__setattr__(self, "revision", revision)
        object.__setattr__(self, "old_category", old_category)
        object.__setattr__(self, "new_category", new_category)

    @property
    def ref(self) -> RevisionRef:
        return RevisionRef(self.item_id, self.revision)

    @classmethod
    def coerce(cls, event: Any, **overrides: Any) -> "Correction":
        """Coerce a mapping, Faust record, or object into a correction."""

        if isinstance(event, cls) and not overrides:
            return event

        item_id = overrides.pop("item_id", _read_field(event, "item_id"))
        revision = overrides.pop("revision", _read_field(event, "revision"))
        old_category = overrides.pop("old_category", _read_field(event, "old_category"))
        new_category = overrides.pop("new_category", _read_field(event, "new_category"))
        quantity = overrides.pop("quantity", _read_field(event, "quantity") or 1)
        attributes = overrides.pop("attributes", _read_field(event, "attributes"))

        if overrides:
            unexpected = ", ".join(sorted(overrides))
            raise InvalidRevisionError(f"unexpected correction fields: {unexpected}")

        return cls(
            item_id=item_id,
            revision=revision,
            old_category=old_category,
            new_category=new_category,
            quantity=quantity,
            attributes=(
                dict(attributes) if isinstance(attributes, Mapping) else attributes
            ),
        )


@dataclass(frozen=True)
class CategoryChange:
    """One idempotent counter leg routed to one category shard."""

    item_id: str
    revision: int
    category: str
    delta: int
    side: ChangeSide
    correlation_id: str
    receipt_id: str
    idempotency_key: str
    quantity: int = 1
    expected_receipt_ids: tuple[str, ...] = ()

    @property
    def ref(self) -> RevisionRef:
        return RevisionRef(self.item_id, self.revision)

    @property
    def routing_key(self) -> str:
        return self.category

    def to_message(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "revision": self.revision,
            "category": self.category,
            "delta": self.delta,
            "side": self.side.value,
            "correlation_id": self.correlation_id,
            "receipt_id": self.receipt_id,
            "idempotency_key": self.idempotency_key,
            "quantity": self.quantity,
            "expected_receipt_ids": list(self.expected_receipt_ids),
        }


# Descriptive aliases for the same routed unit.
DeliverableChange = CategoryChange
DeliverableDelta = CategoryChange
RevisionDelta = CategoryChange
ShardDelta = CategoryChange


@dataclass(frozen=True)
class CategoryReceipt:
    """Evidence that one category shard accepted a leg."""

    receipt_id: str
    correlation_id: str
    item_id: str
    revision: int
    category: str
    delta: int
    duplicate: bool = False

    @classmethod
    def for_change(
        cls, change: CategoryChange, *, duplicate: bool = False
    ) -> "CategoryReceipt":
        return cls(
            receipt_id=change.receipt_id,
            correlation_id=change.correlation_id,
            item_id=change.item_id,
            revision=change.revision,
            category=change.category,
            delta=change.delta,
            duplicate=duplicate,
        )


@dataclass(frozen=True)
class DeliverableRevision:
    """All deterministic legs composing one source revision."""

    ref: RevisionRef
    changes: tuple[CategoryChange, ...]
    old_category: str | None = None
    new_category: str | None = None
    attributes: Mapping[str, Any] | None = None
    kind: str = "category_change"

    @property
    def correlation_id(self) -> str:
        return self.ref.correlation_id

    @property
    def item_id(self) -> str:
        return self.ref.item_id

    @property
    def revision(self) -> int:
        return self.ref.revision

    @property
    def receipt_ids(self) -> tuple[str, ...]:
        return tuple(change.receipt_id for change in self.changes)

    @property
    def expected_categories(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(change.category for change in self.changes))

    def changes_for(self, category: str) -> tuple[CategoryChange, ...]:
        return tuple(change for change in self.changes if change.category == category)

    def net_by_category(self) -> dict[str, int]:
        totals: dict[str, int] = defaultdict(int)
        for change in self.changes:
            totals[change.category] += change.delta
        return dict(totals)

    def to_message(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "revision": self.revision,
            "correlation_id": self.correlation_id,
            "kind": self.kind,
            "old_category": self.old_category,
            "new_category": self.new_category,
            "changes": [change.to_message() for change in self.changes],
            "attributes": dict(self.attributes or {}),
        }


RevisionEnvelope = DeliverableRevision
Deliverable = DeliverableRevision


@dataclass(frozen=True)
class PublishedRevision:
    """A revision whose every expected leg had receipt evidence."""

    delivery: DeliverableRevision
    receipts: tuple[CategoryReceipt, ...]
    publication_number: int

    @property
    def ref(self) -> RevisionRef:
        return self.delivery.ref

    @property
    def item_id(self) -> str:
        return self.delivery.item_id

    @property
    def revision(self) -> int:
        return self.delivery.revision

    @property
    def correlation_id(self) -> str:
        return self.delivery.correlation_id

    def net_by_category(self) -> dict[str, int]:
        return self.delivery.net_by_category()


@dataclass
class _ItemOrder:
    next_revision: int | None = None
    current_category: str | None = None
    buffered: dict[int, Correction] = field(default_factory=dict)
    released: set[int] = field(default_factory=set)


def _make_change(
    correction: Correction,
    category: str,
    delta: int,
    side: ChangeSide,
) -> CategoryChange:
    ref = correction.ref
    side_value = side.value
    receipt_id = f"{ref.correlation_id}:{side_value}:{category}"
    return CategoryChange(
        item_id=correction.item_id,
        revision=correction.revision,
        category=category,
        delta=delta,
        side=side,
        correlation_id=ref.correlation_id,
        receipt_id=receipt_id,
        idempotency_key=receipt_id,
        quantity=correction.quantity,
    )


def define_deliverable_changes(
    correction: Correction | Mapping[str, Any] | Any,
    *,
    previous_category: str | None = None,
) -> DeliverableRevision:
    """Build the deterministic legs for one already-ordered correction.

    A category move produces an old-category negative leg and a new-category
    positive leg with the same correlation id.  An attribute-only correction
    produces one zero-delta receipt for its current category and no leg for
    any other category shard.
    """

    event = Correction.coerce(correction)
    old_category = event.old_category
    new_category = event.new_category

    if old_category is None:
        old_category = _normalise_category(previous_category)
    if new_category is None:
        new_category = old_category

    changes: list[CategoryChange] = []
    kind = "category_change"

    if old_category is None and new_category is None:
        kind = "attribute"
    elif old_category == new_category:
        kind = "attribute"
        attribute_category = old_category
        assert attribute_category is not None
        changes.append(
            _make_change(
                event,
                attribute_category,
                0,
                ChangeSide.ATTRIBUTE,
            )
        )
    else:
        if old_category is not None:
            changes.append(
                _make_change(
                    event,
                    old_category,
                    -event.quantity,
                    ChangeSide.FROM,
                )
            )
        if new_category is not None:
            changes.append(
                _make_change(
                    event,
                    new_category,
                    event.quantity,
                    ChangeSide.TO,
                )
            )

    expected_receipt_ids = tuple(change.receipt_id for change in changes)
    final_changes = tuple(
        replace(change, expected_receipt_ids=expected_receipt_ids) for change in changes
    )

    return DeliverableRevision(
        ref=event.ref,
        changes=final_changes,
        old_category=old_category,
        new_category=new_category,
        attributes=event.attributes,
        kind=kind,
    )


plan_revision = define_deliverable_changes
build_deliverable_changes = define_deliverable_changes


class RevisionReorderBuffer:
    """Buffer future revisions and release contiguous revisions in order."""

    def __init__(
        self,
        expected_revision: int | Mapping[str, int] | None = None,
        *,
        next_revisions: Mapping[str, int] | None = None,
        last_revisions: Mapping[str, int] | None = None,
        categories: Mapping[str, str] | None = None,
        release_first_observed: bool | Literal["auto"] = "auto",
    ) -> None:
        self._items: dict[str, _ItemOrder] = defaultdict(_ItemOrder)
        self._release_first_observed = release_first_observed

        if isinstance(expected_revision, int):
            expected_watermarks: Mapping[str, int] | None = {"*": expected_revision}
        else:
            expected_watermarks = expected_revision

        watermarks: dict[str, int] = {}
        for source in (expected_watermarks, next_revisions):
            if source:
                watermarks.update(
                    {str(key): int(value) for key, value in source.items()}
                )
        if last_revisions:
            watermarks.update(
                {str(key): int(value) + 1 for key, value in last_revisions.items()}
            )

        self._default_next: int | None = watermarks.pop("*", None)
        for item_id, revision in watermarks.items():
            self._items[item_id].next_revision = revision
        if categories:
            for item_id, category in categories.items():
                normalized_category = _normalise_category(category)
                self._items[str(item_id)].current_category = normalized_category

    def watermark(
        self,
        item_id: str,
        *,
        next_revision: int | None = None,
        last_revision: int | None = None,
        category: str | None = None,
    ) -> None:
        state = self._items[str(item_id)]
        if next_revision is not None:
            state.next_revision = next_revision
        if last_revision is not None:
            state.next_revision = last_revision + 1
        if category is not None:
            state.current_category = _normalise_category(category)

    def pending_revisions(self, item_id: str | None = None) -> list[RevisionRef]:
        refs: list[RevisionRef] = []
        for current_item_id, state in self._items.items():
            if item_id is not None and current_item_id != str(item_id):
                continue
            refs.extend(
                RevisionRef(current_item_id, revision)
                for revision in sorted(state.buffered)
            )
        return refs

    def submit(self, event: Any) -> list[DeliverableRevision]:
        """Accept one event and return any newly contiguous deliveries."""

        correction = Correction.coerce(event)
        state = self._items[correction.item_id]

        if correction.revision in state.released:
            return []

        if state.next_revision is None and self._default_next is not None:
            state.next_revision = self._default_next

        if state.next_revision is None:
            immediate_first = self._release_first_observed is True or (
                self._release_first_observed == "auto"
                and not state.buffered
                and correction.revision <= 1
            )
            if immediate_first:
                state.next_revision = correction.revision
            else:
                state.buffered[correction.revision] = correction
                return self._release_dynamic_prefix(correction.item_id)

        assert state.next_revision is not None

        if correction.revision < state.next_revision:
            return []
        if correction.revision > state.next_revision:
            state.buffered[correction.revision] = correction
            return []

        return self._advance(correction.item_id, correction)

    add = submit
    ingest = submit

    def _release_dynamic_prefix(self, item_id: str) -> list[DeliverableRevision]:
        state = self._items[item_id]
        if not state.buffered:
            return []

        first = min(state.buffered)
        revision = first
        contiguous: list[Correction] = []
        while revision in state.buffered:
            contiguous.append(state.buffered.pop(revision))
            revision += 1

        hold_singleton = self._release_first_observed is not True
        if len(contiguous) == 1 and not state.released and hold_singleton:
            state.buffered[contiguous[0].revision] = contiguous[0]
            return []

        state.next_revision = revision
        deliveries: list[DeliverableRevision] = []
        for correction in contiguous:
            deliveries.append(self._plan_and_release(item_id, correction))
        return deliveries

    def _advance(self, item_id: str, initial: Correction) -> list[DeliverableRevision]:
        state = self._items[item_id]
        deliveries = [self._plan_and_release(item_id, initial)]
        assert state.next_revision is not None
        state.next_revision = initial.revision + 1

        while state.next_revision in state.buffered:
            correction = state.buffered.pop(state.next_revision)
            deliveries.append(self._plan_and_release(item_id, correction))
            state.next_revision = correction.revision + 1

        return deliveries

    def _plan_and_release(
        self, item_id: str, correction: Correction
    ) -> DeliverableRevision:
        state = self._items[item_id]
        previous_category = correction.old_category or state.current_category
        delivery = define_deliverable_changes(
            correction, previous_category=previous_category
        )
        state.current_category = delivery.new_category or state.current_category
        state.released.add(correction.revision)
        state.buffered.pop(correction.revision, None)
        return delivery

    def flush(self, item_id: str | None = None) -> list[DeliverableRevision]:
        """Release buffered revisions in revision order, ignoring gaps.

        Prefer :meth:`submit` with a known watermark.  This is useful for
        batch callers that have explicitly supplied all available revisions.
        """

        deliveries: list[DeliverableRevision] = []
        item_ids = [item_id] if item_id is not None else list(self._items)
        for current_item_id in item_ids:
            state = self._items[current_item_id]
            while state.buffered:
                revision = min(state.buffered)
                correction = state.buffered.pop(revision)
                deliveries.append(self._plan_and_release(current_item_id, correction))
        return deliveries


RevisionBuffer = RevisionReorderBuffer
OutOfOrderRevisionBuffer = RevisionReorderBuffer


def category_net_changes(
    events: Iterable[Any],
    **kwargs: Any,
) -> dict[str, list[int]]:
    """Return category deltas ordered by item/revision after sorting events."""

    history: dict[str, list[int]] = defaultdict(list)
    for delivery in plan_revisions(events, **kwargs):
        for category, delta in delivery.net_by_category().items():
            history[category].append(delta)
    return dict(history)


calculate_net_changes = category_net_changes
compute_net_changes = category_net_changes
net_changes_by_category = category_net_changes


def routed_change_messages(
    events: Iterable[Any], **kwargs: Any
) -> list[dict[str, Any]]:
    """Build category-keyed messages suitable for a Faust repartition topic."""

    messages: list[dict[str, Any]] = []
    for delivery in plan_revisions(events, **kwargs):
        for change in delivery.changes:
            message = change.to_message()
            message["topic"] = "category-revision-changes"
            messages.append(message)
    return messages


repartition_corrections = routed_change_messages


def plan_revisions(
    events: Iterable[Any],
    *,
    categories: Mapping[str, str] | None = None,
    require_contiguous: bool = False,
) -> list[DeliverableRevision]:
    """Sort events by item and revision and build all deliverable revisions.

    With ``require_contiguous=True``, only prefixes with no missing revision are
    returned; future revisions remain buffered in a fresh reorder buffer.
    """

    buffer = RevisionReorderBuffer(categories=categories)
    if require_contiguous:
        result: list[DeliverableRevision] = []
        for event in events:
            result.extend(buffer.submit(event))
        return result

    corrections = [Correction.coerce(event) for event in events]
    deliveries: list[DeliverableRevision] = []
    for correction in sorted(corrections, key=lambda event: event.ref.key):
        deliveries.extend(buffer.submit(correction))
    deliveries.extend(buffer.flush())
    return deliveries


build_deliverable_revisions = plan_revisions


class PublicationGate:
    """Track legs and publish revisions only after all receipts are present."""

    def __init__(self) -> None:
        self._deliveries: dict[tuple[str, int], DeliverableRevision] = {}
        self._receipts: dict[str, CategoryReceipt] = {}
        self._published: set[tuple[str, int]] = set()
        self._next_publication: dict[str, int] = {}
        self.publications: list[PublishedRevision] = []

    def expect(self, delivery: DeliverableRevision) -> DeliverableRevision:
        key = delivery.ref.key
        if key in self._deliveries:
            return self._deliveries[key]
        self._deliveries[key] = delivery
        item_id = delivery.item_id
        if item_id not in self._next_publication:
            self._next_publication[item_id] = delivery.revision
        else:
            self._next_publication[item_id] = min(
                self._next_publication[item_id], delivery.revision
            )
        return delivery

    register = expect
    add_delivery = expect

    def acknowledge(
        self, receipt_or_change: CategoryReceipt | CategoryChange | Mapping[str, Any]
    ) -> CategoryReceipt:
        if isinstance(receipt_or_change, CategoryReceipt):
            receipt = receipt_or_change
        elif isinstance(receipt_or_change, CategoryChange):
            existing = self._receipts.get(receipt_or_change.receipt_id)
            receipt = existing or CategoryReceipt.for_change(receipt_or_change)
        else:
            receipt_id = receipt_or_change["receipt_id"]
            existing = self._receipts.get(receipt_id)
            if existing is not None:
                receipt = existing
            else:
                receipt = CategoryReceipt(
                    receipt_id=receipt_id,
                    correlation_id=receipt_or_change["correlation_id"],
                    item_id=str(receipt_or_change["item_id"]),
                    revision=int(receipt_or_change["revision"]),
                    category=str(receipt_or_change["category"]),
                    delta=int(receipt_or_change["delta"]),
                )

        self._receipts.setdefault(receipt.receipt_id, receipt)
        return self._receipts[receipt.receipt_id]

    ack = acknowledge
    add_receipt = acknowledge

    def receipt(self, receipt_id: str) -> CategoryReceipt | None:
        return self._receipts.get(receipt_id)

    def delivery(
        self, ref: RevisionRef | tuple[str, int]
    ) -> DeliverableRevision | None:
        key = ref.key if isinstance(ref, RevisionRef) else ref
        return self._deliveries.get(key)

    def is_complete(self, ref: RevisionRef | tuple[str, int]) -> bool:
        delivery = self.delivery(ref)
        if delivery is None:
            return False
        return all(receipt_id in self._receipts for receipt_id in delivery.receipt_ids)

    can_publish = is_complete

    def is_published(self, ref: RevisionRef | tuple[str, int]) -> bool:
        key = ref.key if isinstance(ref, RevisionRef) else ref
        return key in self._published

    def state(self, ref: RevisionRef | tuple[str, int]) -> RevisionState:
        key = ref.key if isinstance(ref, RevisionRef) else ref
        if key in self._published:
            return RevisionState.PUBLISHED
        delivery = self._deliveries.get(key)
        if delivery is None:
            return RevisionState.BUFFERED
        if self.is_complete(key):
            return RevisionState.READY
        if any(receipt_id in self._receipts for receipt_id in delivery.receipt_ids):
            return RevisionState.PARTIAL
        return RevisionState.DELIVERABLE

    def pending_changes(
        self, ref: RevisionRef | tuple[str, int] | None = None
    ) -> list[CategoryChange]:
        deliveries: Iterable[DeliverableRevision] = self._deliveries.values()
        if ref is not None:
            wanted_key = ref.key if isinstance(ref, RevisionRef) else ref
            deliveries = [
                delivery for delivery in deliveries if delivery.ref.key == wanted_key
            ]

        pending: list[CategoryChange] = []
        for delivery in deliveries:
            if delivery.ref.key in self._published:
                continue
            for change in delivery.changes:
                if change.receipt_id not in self._receipts:
                    pending.append(change)
        return pending

    def pending_receipt_ids(
        self, ref: RevisionRef | tuple[str, int] | None = None
    ) -> list[str]:
        return [change.receipt_id for change in self.pending_changes(ref)]

    def evidence(
        self, ref: RevisionRef | tuple[str, int]
    ) -> tuple[CategoryReceipt, ...]:
        delivery = self.delivery(ref)
        if delivery is None:
            return ()
        return tuple(
            self._receipts[receipt_id]
            for receipt_id in delivery.receipt_ids
            if receipt_id in self._receipts
        )

    def drain_publications(self) -> list[PublishedRevision]:
        """Publish complete revisions in contiguous revision order per item."""

        newly_published: list[PublishedRevision] = []
        items = {delivery.item_id for delivery in self._deliveries.values()}
        for item_id in sorted(items):
            next_revision = self._next_publication.get(item_id)
            while next_revision is not None:
                key = (item_id, next_revision)
                delivery = self._deliveries.get(key)
                if delivery is None or not self.is_complete(key):
                    break
                if key not in self._published:
                    publication = PublishedRevision(
                        delivery=delivery,
                        receipts=self.evidence(key),
                        publication_number=len(self.publications) + 1,
                    )
                    self.publications.append(publication)
                    newly_published.append(publication)
                    self._published.add(key)
                next_revision += 1
                self._next_publication[item_id] = next_revision
        return newly_published

    def replay_pending(self) -> list[CategoryChange]:
        """Return legs that still lack receipts.

        Already-acked legs, such as the B leg in the motivating scenario, are
        absent and therefore cannot be sent again by a replay loop.
        """

        return self.pending_changes()


CompletionTracker = PublicationGate
ReceiptTracker = PublicationGate
PublicationCoordinator = PublicationGate


class CategoryCountShard:
    """In-memory representation of one category-keyed Faust table/shard."""

    def __init__(self, category: str, initial_count: int = 0) -> None:
        self.category = _normalise_category(category) or ""
        if not self.category:
            raise InvalidRevisionError("shard category is required")
        self.count = initial_count
        self.receipts: dict[str, CategoryReceipt] = {}
        self.applied_changes: list[CategoryChange] = []

    def apply(self, change: CategoryChange | Mapping[str, Any]) -> CategoryReceipt:
        if not isinstance(change, CategoryChange):
            change_data = dict(change)
            side_value = change_data.get("side", ChangeSide.TO.value)
            change = CategoryChange(
                item_id=str(change_data["item_id"]),
                revision=int(change_data["revision"]),
                category=str(change_data["category"]),
                delta=int(change_data["delta"]),
                side=ChangeSide(side_value),
                correlation_id=str(change_data["correlation_id"]),
                receipt_id=str(change_data["receipt_id"]),
                idempotency_key=str(
                    change_data.get("idempotency_key", change_data["receipt_id"])
                ),
                quantity=int(change_data.get("quantity", 1)),
                expected_receipt_ids=tuple(change_data.get("expected_receipt_ids", ())),
            )

        if change.category != self.category:
            raise WrongShardError(
                f"cannot apply {change.category!r} change to {self.category!r} shard"
            )

        existing = self.receipts.get(change.idempotency_key)
        if existing is not None:
            return replace(existing, duplicate=True)

        self.count += change.delta
        receipt = CategoryReceipt.for_change(change)
        self.receipts[change.idempotency_key] = receipt
        self.applied_changes.append(change)
        return receipt

    def acknowledge(self, change: CategoryChange) -> CategoryReceipt:
        return self.apply(change)


IdempotentCategoryShard = CategoryCountShard
CountShard = CategoryCountShard
CategoryShard = CategoryCountShard


@dataclass(frozen=True)
class SubmitResult:
    deliveries: tuple[DeliverableRevision, ...]
    changes: tuple[CategoryChange, ...]
    buffered_revisions: tuple[RevisionRef, ...]

    @property
    def delivery(self) -> DeliverableRevision | None:
        return self.deliveries[0] if self.deliveries else None


class RevisionDeliveryService:
    """Convenience facade combining ordering, shards, and publication gating."""

    def __init__(
        self,
        *,
        initial_counts: Mapping[str, int] | None = None,
        expected_revision: int | Mapping[str, int] | None = None,
        next_revisions: Mapping[str, int] | None = None,
        last_revisions: Mapping[str, int] | None = None,
        categories: Mapping[str, str] | None = None,
        release_first_observed: bool | Literal["auto"] = "auto",
    ) -> None:
        self.buffer = RevisionReorderBuffer(
            expected_revision=expected_revision,
            next_revisions=next_revisions,
            last_revisions=last_revisions,
            categories=categories,
            release_first_observed=release_first_observed,
        )
        self.gate = PublicationGate()
        self.shards: dict[str, CategoryCountShard] = {}
        self.published_counts: dict[str, int] = dict(initial_counts or {})
        for category, count in (initial_counts or {}).items():
            self.shards[category] = CategoryCountShard(category, count)

    def shard(self, category: str) -> CategoryCountShard:
        category = _normalise_category(category) or ""
        if category not in self.shards:
            self.shards[category] = CategoryCountShard(category)
            self.published_counts.setdefault(category, 0)
        return self.shards[category]

    def submit(self, event: Any) -> SubmitResult:
        deliveries = tuple(self.buffer.submit(event))
        changes: list[CategoryChange] = []
        for delivery in deliveries:
            self.gate.expect(delivery)
            changes.extend(delivery.changes)
        return SubmitResult(
            deliveries=deliveries,
            changes=tuple(changes),
            buffered_revisions=tuple(self.buffer.pending_revisions()),
        )

    def route(self, change: CategoryChange) -> tuple[str, str, dict[str, Any]]:
        """Return Faust topic, key, and JSON-serializable message."""

        return (
            "category-revision-changes",
            change.routing_key,
            change.to_message(),
        )

    def dispatch_locally(
        self, ref: RevisionRef | tuple[str, int] | None = None
    ) -> list[CategoryReceipt]:
        """Simulate delivering pending legs to their category shards."""

        receipts: list[CategoryReceipt] = []
        for change in self.gate.pending_changes(ref):
            receipt = self.shard(change.category).apply(change)
            self.gate.acknowledge(receipt)
            receipts.append(receipt)
        self._publish_ready()
        return receipts

    def apply(self, change: CategoryChange | Mapping[str, Any]) -> CategoryReceipt:
        if isinstance(change, CategoryChange):
            category = change.category
        else:
            category = change["category"]
        receipt = self.shard(category).apply(change)
        self.gate.acknowledge(receipt)
        self._publish_ready()
        return receipt

    def acknowledge(self, receipt: CategoryReceipt | Mapping[str, Any]) -> None:
        self.gate.acknowledge(receipt)
        self._publish_ready()

    def replay_pending(self) -> list[CategoryReceipt]:
        return self.dispatch_locally()

    def _publish_ready(self) -> None:
        for publication in self.gate.drain_publications():
            for category, delta in publication.net_by_category().items():
                self.published_counts[category] = (
                    self.published_counts.get(category, 0) + delta
                )

    @property
    def counts(self) -> dict[str, int]:
        return {
            category: shard.count for category, shard in sorted(self.shards.items())
        }

    @property
    def publications(self) -> list[PublishedRevision]:
        return self.gate.publications

    def net_changes(
        self, category: str | None = None
    ) -> dict[str, list[int]] | list[int]:
        history: dict[str, list[int]] = defaultdict(list)
        for publication in self.gate.publications:
            for current_category, delta in publication.net_by_category().items():
                history[current_category].append(delta)
        if category is not None:
            return history.get(category, [])
        return dict(history)


RevisionCoordinator = RevisionDeliveryService
ExactlyOnceRevisionRouter = RevisionDeliveryService


def simulate_category_revision_scenario() -> RevisionDeliveryService:
    """Run the motivating 13/12 scenario and return the populated service."""

    service = RevisionDeliveryService(
        initial_counts={"A": 1},
        last_revisions={"sku-1": 11},
    )

    late_result = service.submit(
        {
            "item_id": "sku-1",
            "revision": 13,
            "old_category": "B",
            "new_category": "A",
        }
    )
    assert late_result.deliveries == ()

    filled_result = service.submit(
        {
            "item_id": "sku-1",
            "revision": 12,
            "old_category": "A",
            "new_category": "B",
        }
    )
    assert [delivery.revision for delivery in filled_result.deliveries] == [12, 13]

    b_leg = next(
        change
        for change in filled_result.changes
        if change.revision == 12 and change.category == "B"
    )
    service.apply(b_leg)

    replay_receipts = service.replay_pending()
    assert {receipt.category for receipt in replay_receipts} == {"A", "B"}

    duplicate_b = service.shard("B").apply(b_leg)
    assert duplicate_b.duplicate is True
    service.gate.acknowledge(duplicate_b)
    service._publish_ready()

    return service


__all__ = [
    "CategoryChange",
    "CategoryCountShard",
    "CategoryReceipt",
    "CategoryShard",
    "ChangeSide",
    "CompletionTracker",
    "Correction",
    "Deliverable",
    "DeliverableChange",
    "DeliverableDelta",
    "DeliverableRevision",
    "ExactlyOnceRevisionRouter",
    "InvalidRevisionError",
    "IdempotentCategoryShard",
    "OutOfOrderRevisionBuffer",
    "PublicationGate",
    "PublicationCoordinator",
    "PublishedRevision",
    "RevisionBuffer",
    "RevisionReorderBuffer",
    "RevisionCoordinator",
    "RevisionDeliveryError",
    "RevisionDeliveryService",
    "RevisionEnvelope",
    "RevisionGapError",
    "RevisionRef",
    "RevisionState",
    "ShardDelta",
    "SubmitResult",
    "WrongShardError",
    "build_deliverable_changes",
    "build_deliverable_revisions",
    "calculate_net_changes",
    "category_net_changes",
    "compute_net_changes",
    "define_deliverable_changes",
    "net_changes_by_category",
    "plan_revision",
    "plan_revisions",
    "repartition_corrections",
    "routed_change_messages",
    "simulate_category_revision_scenario",
]
