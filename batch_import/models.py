"""Warehouse schema for the batch importer.

customers.default_address_id and addresses.customer_id form a circular
reference. Both foreign keys are declared DEFERRABLE INITIALLY DEFERRED so a
complete customer/address pair can be inserted in any order inside one
transaction and is only validated at commit time.

orders carry ordinary immediate foreign keys: the referenced customer and
address must already exist (in the warehouse or earlier in the same
transaction) at the moment the order row is inserted.
"""

from pydantic import BaseModel
from sqlalchemy import JSON, Column, ForeignKey, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class CustomerRow(Base):
    __tablename__ = "customers"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    default_address_id = Column(
        String,
        ForeignKey("addresses.id", deferrable=True, initially="DEFERRED"),
        nullable=False,
    )


class AddressRow(Base):
    __tablename__ = "addresses"

    id = Column(String, primary_key=True)
    customer_id = Column(
        String,
        ForeignKey("customers.id", deferrable=True, initially="DEFERRED"),
        nullable=False,
    )
    street = Column(String, nullable=False)
    city = Column(String, nullable=False)


class OrderRow(Base):
    __tablename__ = "orders"

    id = Column(String, primary_key=True)
    customer_id = Column(String, ForeignKey("customers.id"), nullable=False)
    address_id = Column(String, ForeignKey("addresses.id"), nullable=False)


class PendingRow(Base):
    """Bookkeeping for shelved items.

    Shelved payloads are kept here instead of the target tables so a
    half-complete group never lands partially. ``shelved_batch`` records the
    batch that first set the item aside and ``resolved_batch`` the batch that
    finally approved it, giving the lineage of every shelved item.
    """

    __tablename__ = "pending_items"

    entity_type = Column(String, primary_key=True)
    entity_id = Column(String, primary_key=True)
    payload = Column(JSON, nullable=False)
    missing = Column(String, nullable=False)
    status = Column(String, nullable=False, default="pending")
    shelved_batch = Column(String, nullable=False)
    resolved_batch = Column(String, nullable=True)


class Customer(BaseModel):
    id: str
    name: str
    default_address_id: str


class Address(BaseModel):
    id: str
    customer_id: str
    street: str
    city: str


class Order(BaseModel):
    id: str
    customer_id: str
    address_id: str
