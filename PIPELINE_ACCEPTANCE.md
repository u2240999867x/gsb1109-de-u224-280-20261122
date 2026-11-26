# Pipeline acceptance

This optional layer adds job-scoped acknowledgements and segment-scoped stop
permits to an `sh._piped=True` unpack → parse → preview pipeline.

## Run

```sh
.venv/bin/python -m pipeline_acceptance.demo
.venv/bin/pytest -q tests/pipeline_acceptance_test.py
```

## Guarantees

- `PreviewConfirmation` carries `job_id`, `count`, `digest`, and short `summary`.
- `StopPermit` is bound to one job, one segment, and one generated `pipe_id`.
- Each adjacent pair gets a dedicated `AF_UNIX` control socket. Only the two
  corresponding child processes inherit its endpoints.
- A permit for `parse->preview` cannot authorize `unpack->parse`.
- Ordinary logs remain on stderr and are copied into
  `SegmentDiagnostics.stderr`.
- `PipelineAcceptance.finalize()` waits for attached commands and produces an
  order-independent `AcceptanceReport`.
- Preview delivery requires a valid preview acknowledgement plus both stop
  permits.
- `mode="full"` ignores preview permits and requires `kind="full"` terminal
  acknowledgement for every record.

## Scenario matrix

| Scenario | Exit codes | Permits | Conclusion |
| --- | --- | --- | --- |
| Authorized preview | all `0` | both segments valid | `delivered` |
| Parser closes input early | all `0` | missing `unpack->parse` | `missing_permit` |
| Bad third record | preview `0`, parser `23` | upstream permit valid | `pending` with parser diagnostics |
| Full read | all `0` | terminal full acknowledgement | `delivered` |

The report retains the two preview records in the exceptional cases so an
operator can inspect the exact content that was displayed.
