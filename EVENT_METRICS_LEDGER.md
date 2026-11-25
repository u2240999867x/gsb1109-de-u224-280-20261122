# Event metrics ledger

`event_metrics_ledger.py` implements deterministic one-minute dashboard counts for
late and corrected CloudEvents-style records.

## Guarantees

- Identity is `(source, id)`; the same `id` from two `source` values counts twice.
- SDK/backfill default `time` is not used as occurrence time when the import
  manifest marks it non-explicit. Such events enter the pending ledger.
- A minute seals only after every configured partition watermark has passed it.
  Pending events are excluded from the sealed count.
- Authorized corrections resolve pending events or move counted events between
  minutes. Sealed windows retain their original count and expose net changes.
- Correction delivery is idempotent by `(correction_id, version)`; lower versions
  after a newer head are ignored.
- `export_state()` is JSON-safe. `from_state()` restores a checkpoint, and
  replaying the same records with any positive batch size returns identical state.

## Run the scenario

```bash
.venv/bin/python event_metrics_scenario.py --check-batches
```

The command writes `event_metrics_report.json`. The reference scenario seals the
shared minute at count `2` with one pending event excluded, then records one
pending cleanup and net changes of `0` for the shared minute and `+1` for the
previous minute.

## API

```python
from event_metrics_ledger import EventMetricsLedger

ledger = EventMetricsLedger(expected_partitions=["p0", "p1"])
ledger.ingest(event_or_manifest_record)
ledger.advance_watermark("p0", "2026-11-26T12:01:00Z")
ledger.apply_correction(correction)

ledger.minute_counts
ledger.sealed_windows
ledger.net_changes
ledger.pending_cleanups
ledger.export_state()
```

Compatible entry points include `solution.py`, `metrics_ledger.py`,
`event_metrics.py`, and `cloudevents.event_metrics_ledger`.
