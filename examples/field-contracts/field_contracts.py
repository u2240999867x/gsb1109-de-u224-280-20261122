"""Generate per-channel field contracts from one shared msgspec type.

Run with ``python field_contracts.py``.
"""

import decimal
from typing import Annotated

import msgspec
from msgspec import contracts


class Waybill(msgspec.Struct, tag="waybill", tag_field="kind"):
    waybill_no: Annotated[str, msgspec.Meta(min_length=1)]
    container_no: str
    customer_address: str
    amount: decimal.Decimal = decimal.Decimal("0")


class Claim(msgspec.Struct, tag="claim", tag_field="kind"):
    claim_no: str
    waybill: Waybill
    description: str | None = None


ANALYTICS = contracts.AccessPolicy(
    exclude={"customer_address"},
    primary_keys={"waybill_no", "claim_no"},
)
SUPPLIER = contracts.AccessPolicy(
    exclude={"amount"},
    primary_keys={"waybill_no", "claim_no"},
)


def main():
    channels = contracts.build_channels(
        list[Waybill | Claim],
        {"analytics": ANALYTICS, "supplier": SUPPLIER},
    )
    rows = [
        Waybill("W-1", "C-1", "12 Secret St", decimal.Decimal("10.00")),
        Claim("K-1", Waybill("W-2", "C-2", "9 Hidden Rd", decimal.Decimal("4.25"))),
    ]

    for name, channel in channels.items():
        payload = channel.encode(rows)
        # The contract is executable: decode the payload back through it.
        channel.decode(payload)
        print(f"{name}: {payload.decode()}")
        print(f"  columns: {', '.join(channel.columns())}")

    # Hiding a discriminator would produce a contract no normal row can
    # satisfy. This is reported instead of emitted.
    try:
        contracts.contract(
            Waybill,
            name="broken",
            policy=contracts.AccessPolicy(include={"waybill_no", "container_no"}),
        )
    except contracts.ContractConflict as exc:
        for conflict in exc.conflicts:
            print(f"conflict [{conflict.kind}] at {conflict.path}: {conflict.message}")


if __name__ == "__main__":
    main()
