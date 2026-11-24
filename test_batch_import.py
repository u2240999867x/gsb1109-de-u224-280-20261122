"""Scenario tests for the batch loader.

Warehouse already holds an old customer (C0/A0) plus rows left over from
yesterday's failed run (C1/A1 landed before the failure). Today's batch
resends those, adds a complete new pair (C2/A2), an incomplete group (C3
whose default address A3 is missing, plus its other address A3B), and an
order for the old customer. The next batch supplies A3 and the shelved
group lands with full lineage.
"""

import pytest

from batch_import import Address, BatchLoader, Customer, Order


@pytest.fixture()
def loader():
    loader = BatchLoader(conn_string="sqlite://")
    # Old customer already in the warehouse.
    loader.load_batch(
        "B0",
        customers=[Customer(id="C0", name="Old Corp", default_address_id="A0")],
        addresses=[Address(id="A0", customer_id="C0", street="1 Main St", city="Oldtown")],
    )
    # Yesterday's failed run: this group landed before the failure.
    loader.load_batch(
        "B1",
        customers=[Customer(id="C1", name="Retry Ltd", default_address_id="A1")],
        addresses=[Address(id="A1", customer_id="C1", street="2 Retry Rd", city="Oldtown")],
    )
    yield loader
    loader.close()


def test_incomplete_group_shelved_rest_lands(loader):
    report = loader.load_batch(
        "B2",
        customers=[
            # Resent after yesterday's failure: must not be duplicated.
            Customer(id="C1", name="Retry Ltd", default_address_id="A1"),
            # Complete circular pair, lands together.
            Customer(id="C2", name="New Co", default_address_id="A2"),
            # Missing its default address A3: shelved.
            Customer(id="C3", name="Half Co", default_address_id="A3"),
        ],
        addresses=[
            Address(id="A1", customer_id="C1", street="2 Retry Rd", city="Oldtown"),
            Address(id="A2", customer_id="C2", street="3 New Ave", city="Newtown"),
            # Belongs to shelved C3: shelved with its group.
            Address(id="A3B", customer_id="C3", street="4 Half Way", city="Newtown"),
        ],
        orders=[
            # Old customer, address already in the warehouse: not blocked
            # by the incomplete C3 group.
            Order(id="O1", customer_id="C0", address_id="A0"),
        ],
    )

    assert report.landed == {
        "customer": ["C2"],
        "address": ["A2"],
        "order": ["O1"],
    }
    assert report.skipped_existing == {"customer": ["C1"], "address": ["A1"]}
    assert {(e.entity_type, e.entity_id, e.missing) for e in report.shelved} == {
        ("customer", "C3", "address:A3"),
        ("address", "A3B", "customer:C3"),
    }

    # Warehouse state: the incomplete group left no partial rows behind.
    assert {c.id for c in loader.repos["customer"]} == {"C0", "C1", "C2"}
    assert {a.id for a in loader.repos["address"]} == {"A0", "A1", "A2"}
    assert {o.id for o in loader.repos["order"]} == {"O1"}

    # Shelved items know which batch set them aside.
    assert loader.explain("customer", "C3").shelved_batch == "B2"
    assert loader.explain("customer", "C3").resolved_batch is None


def test_next_batch_completes_shelved_group_with_lineage(loader):
    loader.load_batch(
        "B2",
        customers=[Customer(id="C3", name="Half Co", default_address_id="A3")],
        addresses=[Address(id="A3B", customer_id="C3", street="4 Half Way", city="Newtown")],
    )

    report = loader.load_batch(
        "B3",
        addresses=[Address(id="A3", customer_id="C3", street="5 Half Way", city="Newtown")],
    )

    # The shelved customer was retried from the pending table and landed
    # together with the address that completed it.
    assert report.landed["customer"] == ["C3"]
    assert sorted(report.landed["address"]) == ["A3", "A3B"]
    assert {(e.entity_type, e.entity_id) for e in report.resolved} == {
        ("customer", "C3"),
        ("address", "A3B"),
    }

    # Lineage: shelved by B2, approved by B3.
    lineage = loader.explain("customer", "C3")
    assert lineage.shelved_batch == "B2"
    assert lineage.resolved_batch == "B3"
    assert lineage.status == "resolved"

    # Exactly one copy of the customer, nothing left pending.
    assert [c.id for c in loader.repos["customer"]].count("C3") == 1
    assert loader.pending() == []


def test_resent_batch_is_idempotent(loader):
    first = loader.load_batch(
        "B2",
        customers=[Customer(id="C2", name="New Co", default_address_id="A2")],
        addresses=[Address(id="A2", customer_id="C2", street="3 New Ave", city="Newtown")],
    )
    assert first.landed["customer"] == ["C2"]

    # Same payload sent again (e.g. after a crash before the ack arrived).
    second = loader.load_batch(
        "B2-retry",
        customers=[Customer(id="C2", name="New Co", default_address_id="A2")],
        addresses=[Address(id="A2", customer_id="C2", street="3 New Ave", city="Newtown")],
    )
    assert second.landed == {"customer": [], "address": [], "order": []}
    assert second.skipped_existing == {"customer": ["C2"], "address": ["A2"]}
    assert [c.id for c in loader.repos["customer"]].count("C2") == 1


def test_order_with_missing_side_is_shelved_without_blocking(loader):
    report = loader.load_batch(
        "B2",
        customers=[Customer(id="C2", name="New Co", default_address_id="A2")],
        addresses=[Address(id="A2", customer_id="C2", street="3 New Ave", city="Newtown")],
        orders=[
            Order(id="O1", customer_id="C2", address_id="A2"),
            Order(id="O2", customer_id="C9", address_id="A9"),
        ],
    )
    assert report.landed["order"] == ["O1"]
    assert [(e.entity_id, e.missing) for e in report.shelved] == [
        ("O2", "customer:C9,address:A9")
    ]
    assert {o.id for o in loader.repos["order"]} == {"O1"}


def test_deferrable_foreign_keys_are_enforced_at_commit(loader):
    # A batch whose circular pair is complete lands in one transaction...
    loader.load_batch(
        "B2",
        customers=[Customer(id="C2", name="New Co", default_address_id="A2")],
        addresses=[Address(id="A2", customer_id="C2", street="3 New Ave", city="Newtown")],
    )
    # ...and the database really holds both sides consistent.
    customer = loader.repos["customer"]["C2"]
    address = loader.repos["address"]["A2"]
    assert customer.default_address_id == address.id
    assert address.customer_id == customer.id
