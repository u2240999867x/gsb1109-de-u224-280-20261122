#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent / "src"
if SRC.exists():
    sys.path.insert(0, str(SRC))

from reconcile.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
