"""
Versioned, region-keyed inventory dashboard over streaming order changes.

Order changes arrive as pandas micro-batches (each batch is a ``DataFrame`` of
row-level changes).  A whole batch is applied in row order and only committed as
a single, contiguous version when every row is valid.  Invalid batches are
rejected wholesale: no version and no notification is produced.

Committed versions are retained for a small number of recent successful
batches (two by default); batches that do not affect a dashboard's regions
still occupy a retention slot.  Dashboard pages subscribe for a set of regions,
reconnect after disconnects with the versions they already hold, and confirm
each delivered message.  When retention evicts a change a page still needs and
has not already received, a replacement message based on the page's last
confirmed footing is emitted immediately, without waiting for the outstanding
acknowledgement.

Plain append-only ``groupby`` usage on the underlying streaming dataframe is
unaffected; this module only consumes the same micro-batches on the side.
"""

from collections import deque
from itertools import count

import pandas as pd

from .core import Stream

#: Row operations
UPSERT = "upsert"
CANCEL = "cancel"

#: Message entry kinds
SNAPSHOT = "snapshot"
DELTA = "delta"

_OP_VALUES = (UPSERT, CANCEL)


class InvalidBatch(ValueError):
    """Raised when a micro-batch contains a row that cannot be applied."""


class RegionState:
    """Point-in-time state of one region.

    Attributes
    ----------
    registered : bool
        Whether the region is currently on the books.  A region whose last
        order is cancelled is marked removed; it stays removed until a new
        order is registered again.
    orders : dict
        Mapping ``order_id -> fee`` for the orders currently held.  Fees may be
        ``None`` when the order has no usable fee.
    """

    __slots__ = ("registered", "orders")

    def __init__(self, registered=True, orders=None):
        self.registered = registered
        self.orders = {} if orders is None else dict(orders)

    def copy(self):
        return RegionState(self.registered, self.orders)

    @property
    def count(self):
        """Number of orders on the books (kept even when all fees are empty)."""
        return len(self.orders)

    @property
    def min_fee(self):
        """Lowest non-empty fee, or ``None`` if no order carries a fee."""
        fees = [fee for fee in self.orders.values() if fee is not None]
        return min(fees) if fees else None

    def published(self):
        """The externally published triple for this state."""
        return self.count, self.min_fee, self.registered

    def snapshot(self):
        """Serializable complete state of the region."""
        return {
            "registered": self.registered,
            "count": self.count,
            "min_fee": self.min_fee,
            "orders": dict(self.orders),
        }

    def __eq__(self, other):
        return (isinstance(other, RegionState)
                and self.registered == other.registered
                and self.orders == other.orders)

    def __repr__(self):
        return "RegionState(registered=%r, orders=%r)" % (
            self.registered, self.orders)


class Version:
    """One committed, contiguous batch version."""

    __slots__ = ("version", "regions", "changes")

    def __init__(self, version, regions, changes):
        self.version = version
        # Full state of every region after this version.
        self.regions = regions
        # region -> (count, min_fee, registered)
        self.changes = changes

    @property
    def touched(self):
        """Regions whose published content changed in this version."""
        return frozenset(self.changes)


class InventoryEngine:
    """Apply order micro-batches into retained, versioned region states.

    Parameters
    ----------
    retention : int, optional
        Number of recent successful batches kept around.  Batches that did not
        affect the regions a dashboard currently watches still occupy a slot.
    id_column, region_column, fee_column, op_column : str
        DataFrame column names carrying the order id, current region, fee and
        row operation.
    """

    def __init__(self, retention=2, start_version=0, id_column="order_id",
                 region_column="region", fee_column="fee", op_column="op"):
        self.retention = retention
        self.id_column = id_column
        self.region_column = region_column
        self.fee_column = fee_column
        self.op_column = op_column
        # region -> RegionState reflecting the latest committed version
        self._state = {}
        # deque of Version, newest last
        self._versions = deque()
        # region -> highest evicted version that changed it
        self._evicted = {}
        self._version_counter = count(start_version + 1)
        # Stream of committed Version objects
        self.version_stream = Stream()

    @property
    def current_version(self):
        """Version number of the latest successful batch (``0`` before any)."""
        return self._versions[-1].version if self._versions else 0

    @property
    def oldest_retained_version(self):
        return self._versions[0].version if self._versions else None

    def region_state(self, region):
        """Full current region state, or ``None`` if the region never existed."""
        state = self._state.get(region)
        return None if state is None else state.copy()

    def submit(self, batch):
        """Apply one pandas micro-batch.

        Returns the committed :class:`Version`.  Raises :class:`InvalidBatch`
        and leaves all state untouched when any row is invalid; the invalid
        batch produces neither a version nor a notification.
        """
        columns = list(batch.columns)
        positions = {name: columns.index(name) for name in (
            self.id_column, self.region_column, self.fee_column, self.op_column)}

        states = {region: state.copy()
                  for region, state in self._state.items()}
        touched = set()

        try:
            for row in batch.itertuples(index=False, name=None):
                self._apply_row(row, positions, states, touched)
        except InvalidBatch:
            # Whole batch rejected: nothing was mutated outside the copy.
            raise

        changes = self._collect_changes(states, touched)

        number = next(self._version_counter)
        regions = {region: state.copy() for region, state in states.items()}
        version = Version(number, regions, changes)
        self._versions.append(version)
        while len(self._versions) > self.retention:
            dropped = self._versions.popleft()
            for region in dropped.changes:
                self._evicted[region] = dropped.version
        self._state = regions
        self.version_stream.emit(version)
        return version

    def _collect_changes(self, states, touched):
        changes = {}
        for region in touched:
            new_state = states[region]
            old_state = self._state.get(region)
            if old_state is None or old_state.published() != new_state.published():
                changes[region] = new_state.published()
        return changes

    def _apply_row(self, row, positions, states, touched):
        order_id = row[positions[self.id_column]]
        if pd.isna(order_id):
            raise InvalidBatch("row is missing an order id")
        order_id = _scalar(order_id)

        op = row[positions[self.op_column]]
        if op not in _OP_VALUES:
            raise InvalidBatch(
                "unknown operation %r for order %r" % (op, order_id))

        region = row[positions[self.region_column]]
        if pd.isna(region):
            raise InvalidBatch("order %r is missing a region" % order_id)
        region = _scalar(region)

        if op == CANCEL:
            self._apply_cancel(order_id, region, states, touched)
            return

        fee = row[positions[self.fee_column]]
        fee = None if pd.isna(fee) else _scalar(fee)
        self._apply_upsert(order_id, region, fee, states, touched)

    def _apply_upsert(self, order_id, region, fee, states, touched):
        target = states.get(region)
        if target is None:
            target = RegionState()
            states[region] = target

        previous_region = self._find_region(states, order_id)
        if previous_region is not None and previous_region != region:
            previous = states[previous_region]
            del previous.orders[order_id]
            if not previous.orders:
                # The last order left the region: it is removed from the books.
                previous.registered = False
            touched.add(previous_region)

        target.orders[order_id] = fee
        target.registered = True
        touched.add(region)

    def _apply_cancel(self, order_id, region, states, touched):
        target = states.get(region)
        if target is None or order_id not in target.orders:
            raise InvalidBatch(
                "cannot cancel unknown order %r in region %r" % (order_id, region))
        del target.orders[order_id]
        if not target.orders:
            # Last standing order cancelled: mark the region removed while
            # keeping its on-the-books count (zero), rather than dropping it.
            target.registered = False
        touched.add(region)

    @staticmethod
    def _find_region(states, order_id):
        for region, state in states.items():
            if order_id in state.orders:
                return region
        return None

    def has_evicted_gap(self, region, base_version):
        """Whether a change the page still needs has left retention."""
        return self._evicted.get(region, 0) > base_version


class Message:
    """One deliverable, acknowledgement-scoped unit of dashboard content.

    Two messages carrying the same version still have distinct ``message_id``
    values, so each delivery must be acknowledged separately.

    Attributes
    ----------
    version : int
        Committed version through which every region of the message is current.
    entries : dict
        ``region -> ("snapshot", payload)`` or ``("delta", [(version,
        (count, min_fee, registered)), ...])``.  An empty delta list means the
        region already has everything and keeps its current display.
    connection_id : object
        Identifies the page connection the message was built for; receipts
        from another connection are rejected.
    """

    __slots__ = ("view", "version", "entries", "connection_id", "message_id")
    _id_counter = count(1)

    def __init__(self, view, version, entries, connection_id):
        self.view = view
        self.version = version
        self.entries = entries
        self.connection_id = connection_id
        self.message_id = next(Message._id_counter)

    def __repr__(self):
        return "Message(id=%d, version=%d, regions=%r)" % (
            self.message_id, self.version, sorted(self.entries))


class DashboardView:
    """One dashboard page subscribed to a set of regions.

    A view is handed :class:`Message` objects and acknowledges them through
    :meth:`acknowledge`.  A reconnect (new page, or a page re-rendering after a
    disconnect) is expressed by creating a fresh view carrying the regions it
    now displays and the version each of them already holds; views that leave
    the subscription stop receiving messages and their regions drop off the
    screen.
    """

    def __init__(self, engine, regions, held_versions=None, connection_id=None):
        self.engine = engine
        self.connection_id = connection_id
        self.regions = set(regions)
        # region -> version whose content the page has installed; the entry
        # version is advanced only when a message is acknowledged.
        self.held = {region: version
                     for region, version in dict(held_versions or {}).items()
                     if region in self.regions}
        self.pending = None
        self.messages = []
        # False once the page is superseded by another connection; its receipts
        # can then never release a message of the live connection.
        self.active = True

    # -- message construction ------------------------------------------------

    def build_message(self, target_version=None):
        """Build one message cutting every subscribed region to one version."""
        if target_version is None:
            target_version = self.engine.current_version
        entries = {region: self._build_entry(region, target_version)
                   for region in self.regions}
        return Message(self, target_version, entries, self.connection_id)

    def _build_entry(self, region, target_version):
        base = self._effective_base(region)
        if base is None or self.engine.has_evicted_gap(region, base):
            # No footing at all, or a needed change has left retention:
            # hand over the full state, explicitly marked if it no longer exists.
            return SNAPSHOT, self._snapshot(region)

        deltas = []
        for item in self.engine._versions:
            if base < item.version <= target_version and region in item.changes:
                deltas.append((item.version, item.changes[region]))
        # An empty delta list means the region is already current: keep as-is.
        return DELTA, deltas

    def _effective_base(self, region):
        """Version the page would hold once its pending message is confirmed."""
        held = self.held.get(region)
        if self.pending is not None and (held is None
                                         or self.pending.version > held):
            return self.pending.version
        return held

    def _snapshot(self, region):
        state = self.engine.region_state(region)
        return state.snapshot() if state is not None else {
            "registered": False,
            "count": 0,
            "min_fee": None,
            "orders": {},
        }

    # -- delivery and acknowledgement ----------------------------------------

    def deliver(self, message=None):
        if message is None:
            message = self.build_message()
        self.pending = message
        self.messages.append(message)
        return message

    def acknowledge(self, message):
        """Confirm a delivered message.

        Stale receipts (an inactive connection, another connection's message,
        or a message replaced before it was confirmed) are ignored and can
        never release the current message.  Returns ``True`` only when the
        pending message itself is confirmed.
        """
        if not isinstance(message, Message):
            raise TypeError("can only acknowledge Message objects")
        if not self.active or self.pending is None:
            return False
        if message.message_id != self.pending.message_id:
            return False
        if message.connection_id != self.connection_id:
            return False
        for region in self.regions:
            self.held[region] = message.version
        self.pending = None
        return True

    def needs_replacement(self, target_version):
        """Whether any subscribed region has an evicted, not-yet-delivered gap."""
        return any(self.engine.has_evicted_gap(region, self._effective_base(region))
                   for region in self.regions)


class InventoryDashboard:
    """Coordinates versioned inventory delivery across dashboard pages.

    Parameters
    ----------
    engine : InventoryEngine, optional
        Batch engine to attach to.  A fresh engine with ``retention`` slots is
        created when omitted.
    retention : int, optional
        Retention size used when creating a default engine.
    """

    def __init__(self, engine=None, retention=2):
        self.engine = engine or InventoryEngine(retention=retention)
        self.views = set()
        self.engine.version_stream.sink(self._on_version)

    # -- stream wiring --------------------------------------------------------

    def attach(self, stream):
        """Feed every pandas micro-batch emitted by ``stream`` into the engine."""
        stream.sink(self.submit)
        return stream

    # -- batch passthrough ----------------------------------------------------

    def submit(self, batch):
        """Submit a micro-batch; returns the committed version or raises."""
        return self.engine.submit(batch)

    # -- view lifecycle -------------------------------------------------------

    def connect(self, regions, held_versions=None, connection_id=None):
        """Register a freshly (re-)connected dashboard page.

        Parameters
        ----------
        regions : iterable
            Regions this page now displays.  Regions absent from the list leave
            the subscription and are removed from the screen.
        held_versions : dict, optional
            For each region, the committed version the page already has
            installed, potentially assembled on an earlier page.  Regions
            without an entry, or whose needed changes have been evicted,
            receive full state.
        connection_id : object, optional
            Token identifying the connection; replacing a connection makes all
            of its old receipts invalid.
        """
        view = DashboardView(self.engine, regions, held_versions, connection_id)
        self.views.add(view)
        return view

    def disconnect(self, view):
        """Remove a page from the subscription; its pending receipts die too."""
        view.active = False
        self.views.discard(view)

    def greet(self, view):
        """Deliver the single connection message cutting the page to one version."""
        return view.deliver()

    def acknowledge(self, view, message):
        """Acknowledge a delivered message and hand over anything still due.

        Returns ``True`` when the receipt actually confirmed the page's pending
        message.  Stale receipts return ``False`` and release nothing.  On a
        real acknowledgement the next pending version, if any, is delivered
        immediately.
        """
        confirmed = view.acknowledge(message)
        if confirmed and view.active and self._is_behind(view):
            view.deliver(view.build_message())
        return confirmed

    def _is_behind(self, view):
        current = self.engine.current_version
        for region in view.regions:
            base = view.held.get(region, 0)
            if base >= current:
                continue
            if self.engine.has_evicted_gap(region, base):
                return True
            if any(region in item.changes
                   for item in self.engine._versions
                   if base < item.version <= current):
                return True
        return False

    # -- version-driven delivery ---------------------------------------------

    def _on_version(self, version):
        for view in list(self.views):
            if not view.active:
                continue
            if view.pending is not None:
                # One unacknowledged message at a time.  It may wait for its
                # receipt unless retention evicts a change it cannot cover.
                if view.needs_replacement(version.version):
                    view.pending = None
                    view.deliver(view.build_message(version.version))
                continue
            affected = [region for region in view.regions
                        if region in version.changes]
            if affected:
                # All regions touched by this batch for this page go together.
                view.deliver(view.build_message(version.version))


def _scalar(value):
    """Unwrap numpy/pandas scalars while leaving plain Python values intact."""
    return value.item() if hasattr(value, "item") else value
