"""Batch loader that hands inbound loads to the Red Bird SQL repositories.

The loader accepts a mixed batch of customers, addresses and orders:

* groups whose circular references are complete (in the batch and/or already
  in the warehouse) are inserted together in a single transaction -- the
  deferrable foreign keys are only checked at commit, when both sides exist;
* groups missing one side are shelved in the ``pending_items`` table with the
  id of the batch that shelved them, and are retried automatically on every
  following batch;
* rows already in the warehouse (e.g. left over from a failed run) are
  skipped, so a resent batch never produces a second copy;
* orders only need their (immediate) foreign keys satisfied and are never
  blocked by an unrelated incomplete group.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from sqlalchemy import create_engine, event, inspect

from redbird.repos import SQLRepo

from .models import (
    Address,
    AddressRow,
    Base,
    Customer,
    CustomerRow,
    Order,
    OrderRow,
    PendingRow,
)

ENTITY_MODELS = {"customer": Customer, "address": Address, "order": Order}


@dataclass
class PendingEntry:
    entity_type: str
    entity_id: str
    missing: str
    shelved_batch: str
    status: str = "pending"
    resolved_batch: Optional[str] = None


@dataclass
class BatchReport:
    batch_id: str
    landed: Dict[str, List[str]] = field(default_factory=dict)
    skipped_existing: Dict[str, List[str]] = field(default_factory=dict)
    shelved: List[PendingEntry] = field(default_factory=list)
    resolved: List[PendingEntry] = field(default_factory=list)


class BatchLoader:
    """Loads batches into the warehouse through Red Bird SQLRepos."""

    def __init__(self, engine=None, conn_string: str = "sqlite://"):
        if engine is None:
            engine = create_engine(conn_string)
        if engine.dialect.name == "sqlite":
            self._enforce_foreign_keys(engine)
        Base.metadata.create_all(engine)
        self.engine = engine
        self.session = SQLRepo.create_scoped_session(engine)
        self.repos = {
            "customer": SQLRepo(
                model=Customer, model_orm=CustomerRow, session=self.session,
                id_field="id", autocommit=False,
            ),
            "address": SQLRepo(
                model=Address, model_orm=AddressRow, session=self.session,
                id_field="id", autocommit=False,
            ),
            "order": SQLRepo(
                model=Order, model_orm=OrderRow, session=self.session,
                id_field="id", autocommit=False,
            ),
        }

    @staticmethod
    def _enforce_foreign_keys(engine):
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def load_batch(
        self,
        batch_id: str,
        customers: Iterable = (),
        addresses: Iterable = (),
        orders: Iterable = (),
    ) -> BatchReport:
        report = BatchReport(batch_id=batch_id)

        # Candidates: still-pending items from earlier batches retried first,
        # then this batch's payload (deduplicated by id, batch payload wins).
        candidates = {"customer": {}, "address": {}, "order": {}}
        pending_rows = self._pending_rows()
        for row in pending_rows:
            model = ENTITY_MODELS[row.entity_type]
            candidates[row.entity_type][row.entity_id] = model(**row.payload)
        for entity_type, items in (
            ("customer", customers), ("address", addresses), ("order", orders)
        ):
            repo = self.repos[entity_type]
            for item in items:
                item = repo.to_item(item)
                candidates[entity_type][item.id] = item

        # Rows already in the warehouse are skipped: no second copy is
        # produced for items an earlier run managed to land.
        known = {
            entity_type: {repo.get_field_value(item, "id") for item in repo}
            for entity_type, repo in self.repos.items()
        }
        for entity_type, items in candidates.items():
            skipped = sorted(uid for uid in items if uid in known[entity_type])
            if skipped:
                report.skipped_existing[entity_type] = skipped
            for uid in skipped:
                del items[uid]
                self._resolve_pending(entity_type, uid, batch_id, report)

        landed_customers, landed_addresses, available = self._plan(candidates, known)
        landed_orders = {
            uid: order
            for uid, order in candidates["order"].items()
            if order.customer_id in available["customer"]
            and order.address_id in available["address"]
        }
        landed = {
            "customer": landed_customers,
            "address": landed_addresses,
            "order": landed_orders,
        }

        try:
            # Parents go in before orders so the orders' immediate foreign
            # keys are satisfied at statement time; the circular customer/
            # address keys are deferred and checked at commit.
            for entity_type in ("customer", "address", "order"):
                repo = self.repos[entity_type]
                for item in landed[entity_type].values():
                    repo.insert(item)
                report.landed[entity_type] = sorted(landed[entity_type])

            for entity_type, items in candidates.items():
                for uid, item in items.items():
                    if uid in landed[entity_type]:
                        self._resolve_pending(entity_type, uid, batch_id, report)
                    else:
                        entry = self._shelve(entity_type, item, batch_id)
                        report.shelved.append(entry)

            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        return report

    def pending(self) -> List[PendingEntry]:
        "Items currently shelved, waiting for their missing counterpart."
        return [self._to_entry(row) for row in self._pending_rows()]

    def explain(self, entity_type: str, entity_id: str) -> Optional[PendingEntry]:
        """Lineage of a shelved item.

        Tells which batch shelved the item and, once it has landed, which
        batch approved it.
        """
        row = self.session.get(PendingRow, (entity_type, entity_id))
        return self._to_entry(row) if row is not None else None

    def close(self):
        self.session.remove()

    # ------------------------------------------------------------------
    # planning
    # ------------------------------------------------------------------
    @staticmethod
    def _plan(candidates, known):
        """Fixpoint of what can land given the warehouse and this batch.

        A new customer lands when its default address is in the warehouse or
        is one of its own addresses in the batch (they land together in one
        transaction). A new address lands when its customer is in the
        warehouse or lands. Whatever never reaches the fixpoint stays pending.
        """
        new_customers = candidates["customer"]
        new_addresses = candidates["address"]

        addresses_by_customer = defaultdict(set)
        for address in new_addresses.values():
            addresses_by_customer[address.customer_id].add(address.id)

        landed_customers = set(known["customer"])
        landed_addresses = set(known["address"])
        changed = True
        while changed:
            changed = False
            for uid, customer in new_customers.items():
                if uid in landed_customers:
                    continue
                own_addresses = addresses_by_customer.get(uid, set())
                if (
                    customer.default_address_id in landed_addresses
                    or customer.default_address_id in own_addresses
                ):
                    landed_customers.add(uid)
                    changed = True
            for uid, address in new_addresses.items():
                if uid in landed_addresses:
                    continue
                if address.customer_id in landed_customers:
                    landed_addresses.add(uid)
                    changed = True

        landed_customers = {
            uid: c for uid, c in new_customers.items() if uid in landed_customers
        }
        landed_addresses = {
            uid: a for uid, a in new_addresses.items() if uid in landed_addresses
        }
        available = {
            "customer": known["customer"] | set(landed_customers),
            "address": known["address"] | set(landed_addresses),
        }
        return landed_customers, landed_addresses, available

    # ------------------------------------------------------------------
    # pending bookkeeping (same transaction as the payload)
    # ------------------------------------------------------------------
    def _pending_rows(self):
        return (
            self.session.query(PendingRow)
            .filter_by(status="pending")
            .all()
        )

    @staticmethod
    def _to_entry(row) -> PendingEntry:
        return PendingEntry(
            entity_type=row.entity_type,
            entity_id=row.entity_id,
            missing=row.missing,
            status=row.status,
            shelved_batch=row.shelved_batch,
            resolved_batch=row.resolved_batch,
        )

    def _missing_of(self, entity_type, item) -> str:
        if entity_type == "customer":
            return f"address:{item.default_address_id}"
        if entity_type == "address":
            return f"customer:{item.customer_id}"
        return f"customer:{item.customer_id},address:{item.address_id}"

    def _shelve(self, entity_type, item, batch_id) -> PendingEntry:
        missing = self._missing_of(entity_type, item)
        row = self.session.get(PendingRow, (entity_type, item.id))
        if row is None:
            row = PendingRow(
                entity_type=entity_type,
                entity_id=item.id,
                shelved_batch=batch_id,
                status="pending",
            )
            self.session.add(row)
        # Refresh the payload/missing info but never the shelving batch: the
        # item keeps a single pending record and its original lineage.
        row.payload = item.dict()
        row.missing = missing
        return self._to_entry(row)

    def _resolve_pending(self, entity_type, entity_id, batch_id, report):
        row = self.session.get(PendingRow, (entity_type, entity_id))
        if row is not None and row.status == "pending":
            row.status = "resolved"
            row.resolved_batch = batch_id
            report.resolved.append(self._to_entry(row))
