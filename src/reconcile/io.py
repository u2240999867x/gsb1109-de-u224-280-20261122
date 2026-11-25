from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_batch(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    text = file_path.read_text(encoding="utf-8")
    if file_path.suffix.lower() == ".jsonl":
        records = [json.loads(line) for line in text.splitlines() if line.strip()]
        return {"observations": records}
    data = json.loads(text)
    if isinstance(data, list):
        return {"observations": data}
    if not isinstance(data, dict):
        raise ValueError("reconciliation input must be a JSON object or array")
    return data
