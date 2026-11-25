from __future__ import annotations

import json

from inspection.tickets import VersionedTicketStore


UPDATES = [
    {"sequence": 1, "business_version": 1, "alarm_id": "A", "start": 0, "end": 4, "first_reported_at": 9},
    {"sequence": 1, "business_version": 1, "alarm_id": "B", "start": 8, "end": 12, "first_reported_at": 3},
    {"sequence": 1, "business_version": 1, "alarm_id": "C", "start": 10, "end": 14, "first_reported_at": 6},
    {"sequence": 2, "business_version": 2, "alarm_id": "X", "start": 3, "end": 9, "first_reported_at": 4},
    {"sequence": 3, "business_version": 3, "alarm_id": "X", "operation": "revoke"},
    {"sequence": 4, "business_version": 4, "alarm_id": "B", "operation": "revoke"},
]

CORRECTION = {
    "sequence": 5,
    "business_version": 2,
    "alarm_id": "X",
    "operation": "correct",
    "start": 3,
    "end": 8,
}


def main() -> None:
    store = VersionedTicketStore()
    store.ingest(UPDATES)
    store.publish_daily_report(business_version=2, cutoff=4, report_date="original")

    original = store.get_daily_report(2, 4, "B")
    print("published v2 report at cutoff 4:")
    print(json.dumps(original.to_dict(), ensure_ascii=False, indent=2))

    store.ingest(CORRECTION)

    print("v2 after late correction:")
    for ticket in store.tickets_at(2, 5):
        print(json.dumps(ticket.to_dict(), ensure_ascii=False))

    print("reconciliation changes from cutoff 4 to 5:")
    for change in store.reconcile(4, 5):
        print(json.dumps(change.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
