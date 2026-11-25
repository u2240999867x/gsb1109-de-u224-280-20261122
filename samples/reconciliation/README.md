# Offline reconciliation example

Run from the repository root:

```bash
python offline_reconcile.py \
  samples/reconciliation/two_station_batch.json

python offline_reconcile.py \
  samples/reconciliation/two_station_batch.json \
  --correction samples/reconciliation/count_correction.json
```

The first report contains:

- `DWH-W1`: unique chain `EVT-A1 -> MID-B1 -> OUT-C1 -> DWH-W1`; observation
  `OBS-S2-RESEND-7` and resend group `RG-213-7` are attached to that chain.
- `DWH-W2`: two retained candidate sources, `ORIG-7A` and `ORIG-7B`; both obey
  every station count, one-to-one, candidate-range, and resend constraint. The
  engine enumerates all solutions and never selects by ordering.
- After adding the correction file, issue `COUNT_CONTRADICTION` points to
  station `ST-1`, observation `OBS-CORRECTION-COUNT-1`, and
  `logs/corrections.jsonl:64`; the two warehouse results remain readable.

JSON output is available with `--format json`. The Python API is
`reconcile.reconcile(batch, corrections=None)`.
