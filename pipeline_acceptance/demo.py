from __future__ import annotations

import json
from typing import Any

from .runner import PipelineRunner


def _summary(report: Any) -> dict[str, Any]:
    data = report.to_dict()
    return {
        "job_id": data["job_id"],
        "mode": data["mode"],
        "status": data["status"],
        "delivered": data["delivered"],
        "reasons": data["reasons"],
        "missing_permits": data["missing_permits"],
        "retained_record_count": len(data["records"]),
        "record_ids": [record.get("id") for record in data["records"][:5]],
        "preview_confirmation": data["preview_ack"],
        "full_confirmation": data["full_ack"],
        "segments": [
            {
                "segment": segment["segment"],
                "exit_code": segment["exit_code"],
                "broken_pipe": segment["broken_pipe"],
                "permit_valid": segment["permit_valid"],
                "stderr": segment["stderr"],
            }
            for segment in data["segments"]
        ],
    }


def main() -> int:
    with PipelineRunner() as runner:
        first, second = runner.twin_previews()
        bad = runner.run("bad-record", job_id="demo-bad-record")
        full = runner.run("authorized", job_id="demo-full", mode="full")

    output = {
        "first_preview": _summary(first),
        "second_preview_missing_permit": _summary(second),
        "bad_record": _summary(bad),
        "full_mode": _summary(full),
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
