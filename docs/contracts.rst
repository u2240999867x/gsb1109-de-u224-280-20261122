Field Contracts
===============

.. currentmodule:: msgspec.contracts

.. warning::

   This module is experimental. While we don't expect any breaking changes, we
   also don't promise not to break things between releases while this interface
   stabilizes.

Different consumers often need different columns from the same message type.
A field contract is an executable `msgspec.Struct` generated from an existing
msgspec type, with the columns a particular channel is allowed to see. Because
it is a real msgspec type, the same contract can be used to decode, validate,
and generate a JSON Schema for the projected rows.

.. code-block:: python

    >>> import msgspec
    >>> from typing import Annotated
    >>> from msgspec import contracts
    >>>
    >>> class Waybill(msgspec.Struct, tag="waybill", tag_field="kind"):
    ...     waybill_no: Annotated[str, msgspec.Meta(min_length=1)]
    ...     container_no: str
    ...     customer_address: str
    ...     amount: float = 0.0
    >>>
    >>> analytics_policy = contracts.AccessPolicy(
    ...     exclude={"customer_address"},
    ...     primary_keys={"waybill_no"},
    ... )
    >>> supplier_policy = contracts.AccessPolicy(
    ...     exclude={"amount"},
    ...     primary_keys={"waybill_no"},
    ... )
    >>>
    >>> analytics = contracts.contract(
    ...     Waybill, name="analytics", policy=analytics_policy
    ... )
    >>> supplier = contracts.contract(
    ...     Waybill, name="supplier", policy=supplier_policy
    ... )

Each call builds an independent type. Removing a column for one channel never
changes another channel's contract or the original type.

.. code-block:: python

    >>> row = Waybill("W-1", "C-9", "12 Secret Street", 7.5)
    >>> analytics.export(row)
    {'kind': 'waybill', 'waybill_no': 'W-1', 'container_no': 'C-9', 'amount': 7.5}
    >>> supplier.export(row)
    {'kind': 'waybill', 'waybill_no': 'W-1', 'container_no': 'C-9',
     'customer_address': '12 Secret Street'}

The generated type is executable. It enforces the visible constraints and
rejects columns that are not part of the channel:

.. code-block:: python

    >>> analytics.decode(b'{"kind":"waybill","waybill_no":"W-1",'
    ...                  b'"container_no":"C-9","amount":7.5}')
    analytics_Waybill(waybill_no='W-1', container_no='C-9', amount=7.5)
    >>> analytics.decode(b'{"kind":"waybill","waybill_no":"W-1",'
    ...                  b'"container_no":"C-9","amount":7.5,'
    ...                  b'"customer_address":"leak"}')
    Traceback (most recent call last):
      ...
    msgspec.ValidationError: Object contains unknown field `customer_address`
    >>> analytics.schema["$defs"]["analytics_Waybill"]["additionalProperties"]
    False

Policies
--------

`AccessPolicy` supports default ``include``/``exclude`` lists and per-type or
per-tag rules:

.. code-block:: python

    >>> policy = contracts.AccessPolicy(
    ...     exclude={"customer_address"},
    ...     primary_keys={"waybill_no", "claim_no"},
    ...     by_type={
    ...         Waybill: {"include": {"waybill_no", "container_no", "kind"}},
    ...     },
    ... )

Matching is by class, class name, qualified name, or ``module.qualname``. A
type rule takes precedence over a tag rule. `FieldRule` objects may be used
instead of mappings.

Primary keys are retained at every level, even if an include list would
otherwise hide them. Besides ``primary_keys``, a key may be marked inline:

.. code-block:: python

    >>> class Row(msgspec.Struct):
    ...     key: Annotated[str, msgspec.Meta(extra={"primary_key": True})]
    ...     value: str

Array-like structs are supported; the tag remains the first array element and
fields keep the order of the generated contract.

Tagged types and hidden discriminators
--------------------------------------

For a tagged struct, msgspec stores the discriminator name and tag in the type
metadata. A contract that hides the discriminator but keeps the tag would
require a column the channel forbids, so it could never accept a normal row.
Instead of emitting such a contract, `contract` raises `ContractConflict`:

.. code-block:: python

    >>> contracts.contract(
    ...     Waybill,
    ...     name="broken",
    ...     policy=contracts.AccessPolicy(include={"waybill_no", "container_no"}),
    ... )
    Traceback (most recent call last):
      ...
    msgspec.contracts.ContractConflict: cannot build field contract for channel 'broken':
    - $: Waybill is tagged with 'waybill' via discriminator field 'kind', but that
      field is not exposed to channel 'broken'; ...

Each `Conflict` records the channel, path, offending struct, discriminator
field, and tag. Use `check_contract` to collect conflicts without building:

.. code-block:: python

    >>> contracts.check_contract(
    ...     Waybill,
    ...     name="broken",
    ...     policy=contracts.AccessPolicy(include={"waybill_no", "container_no"}),
    ... )[0].kind
    'hidden-discriminator'

When the discriminator is exposed, tagged unions work normally, including
unions nested inside arrays. Each array element is projected with the contract
for the tag it actually carries:

.. code-block:: python

    >>> class Claim(msgspec.Struct, tag="claim", tag_field="kind"):
    ...     claim_no: str
    ...     waybill: Waybill
    >>>
    >>> mixed = contracts.contract(
    ...     list[Waybill | Claim], name="mixed", policy=analytics_policy
    ... )
    >>> mixed.export([row, Claim("K-1", row)])[1]["waybill"]["container_no"]
    'C-9'

Unknown tags raise `ValueError` instead of silently selecting a branch.

Multiple channels at once
-------------------------

`build_channels` builds a named contract per policy, `build_contracts` builds
one contract per source, and `build_contract`/`field_contract` are aliases for
`contract`:

.. code-block:: python

    >>> channels = contracts.build_channels(
    ...     Waybill,
    ...     {"analytics": analytics_policy, "supplier": supplier_policy},
    ... )
    >>> sorted(channels)
    ['analytics', 'supplier']

`FieldContract.describe` returns a JSON-compatible description of the
executable type, and `FieldContract.columns` returns the encoded column names
it can emit (including columns nested under arrays or unions).

Types supported
---------------

Contracts are generated from `msgspec.inspect` type information. Structs are
the contract root and the nested object types: primitives, constraints, enums,
literals, collections, unions, nested structs, ``array_like`` structs, and
recursive structs are all supported. ``Annotated`` constraints and ``Meta``
metadata are preserved, and constructor keywords such as ``kw_only``,
``rename`` and ``omit_defaults`` are carried over.

TypedDict, dataclass and NamedTuple fields are not converted into contract
structs; passing one as the root raises `TypeError` with the offending type
name. Convert it to a `msgspec.Struct` first. Custom types are carried through
as their original class so a caller-provided encoder/decoder hook still applies.
