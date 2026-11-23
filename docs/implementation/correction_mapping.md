# Correction Mapping

Some sources re-emit the same business object whenever it changes. Every
version carries the same business identity and a monotonic **revision
number**, but the later versions may differ in ways a plain append-only map
cannot undo:

- the amount's attribution day may change;
- the record may be filtered out by a rule, even though an earlier version
  was retained;
- corrections may arrive out of order or be replayed after readers have
  already consumed the aggregates.

The `singer_sdk.correction` module provides a reusable, stateful
{py:class}`~singer_sdk.correction.CorrectionMapper` that turns each incoming
revision into the **differential events** needed to keep two downstream
tables correct:

- a **detail table** holding one row per business identity;
- a **daily-total table** holding the running amount per attribution day.

## Core rules

1. Revisions are ordered by the source revision number, never by arrival
   time. A revision lower than or equal to the applied revision is stale or
   duplicate and produces no events.
2. When a new retained revision arrives, the previous detail row is deleted
   before the replacement row is upserted.
3. The daily table only receives signed deltas: the previous revision's
   amount is subtracted from its day and the new amount is added to the new
   day. Same-day changes collapse into a single net delta.
4. A filtered revision still replaces the tracked revision. Its effects are
   revoked, but no replacement row or amount is emitted.
5. Replaying an old revision after a newer one was applied never restores
   amounts that readers may already have consumed.

## Example

A payment counted on October 1st is corrected on October 2nd, and the new
version is filtered out by a rule:

```python
from singer_sdk.correction import CorrectionMapper, CorrectionPolicy

policy = CorrectionPolicy(
    business_id_properties="txn_id",
    revision_property="revision",
    day_property="posting_date",
    amount_property="amount",
    retain=lambda record: not record.get("filtered", False),
)
mapper = CorrectionMapper(policy)

# Revision 1: 90 attributed to 2026-10-01.
mapper.map_record(
    "payments",
    {
        "txn_id": "txn-1",
        "revision": 1,
        "posting_date": "2026-10-01",
        "amount": 90,
    },
)
# -> detail UPSERT (txn-1, 90)
# -> daily delta +90 on 2026-10-01, total 90

# Revision 2: moved to 2026-10-02, but filtered out.
events = mapper.map_record(
    "payments",
    {
        "txn_id": "txn-1",
        "revision": 2,
        "posting_date": "2026-10-02",
        "amount": 90,
        "filtered": True,
    },
)
# -> detail DELETE of the old row (no replacement is inserted)
# -> daily delta -90 on 2026-10-01, total 0
# -> nothing is added for 2026-10-02
```

The October 1st total is back to zero, the detail row is removed, and
October 2nd receives nothing. If revision 1 is delivered again afterwards,
it is ignored because revision 2 is already the applied baseline.

## Emitting Singer messages

{py:meth}`~singer_sdk.correction.CorrectionMapper.to_record_messages`
converts the events into Singer `RECORD` messages. Detail deletions reuse the
previous payload and set the `_sdc_deleted_at` marker; daily records carry
the signed delta and the resulting running total.

```python
for message in mapper.to_record_messages(events):
    writer.write_message(message)
```

The output stream names are derived from the source stream name and can be
customized with the `detail_stream_suffix` and `daily_stream_suffix`
arguments.

## Out-of-order and replayed revisions

The mapper is safe when corrections arrive before or after the original
version:

- an early newer revision is applied normally, and the late older revision is
  dropped;
- a redelivered (equal) revision is idempotent and emits nothing;
- by default stale revisions are logged and dropped; pass
  `on_stale_revision="raise"` to raise
  {py:class}`~singer_sdk.exceptions.StaleRevisionError` instead.

## Persisting mapper state

The mapper's knowledge of applied revisions and daily totals can be
serialized with {py:meth}`~singer_sdk.correction.CorrectionMapper.snapshot`
and restored with {py:meth}`~singer_sdk.correction.CorrectionMapper.restore`,
for example inside a Singer `STATE` message. A restored mapper keeps
rejecting stale revisions and computes deltas against the restored totals.

## Reference

```{eval-rst}
.. autoclass:: singer_sdk.correction.CorrectionPolicy
   :members:
.. autoclass:: singer_sdk.correction.CorrectionMapper
   :members:
.. autoclass:: singer_sdk.correction.CorrectionEvent
   :members:
.. autoclass:: singer_sdk.correction.RecordVersion
   :members:
```
