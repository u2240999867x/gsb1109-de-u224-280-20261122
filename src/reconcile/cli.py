from __future__ import annotations

import argparse
from pathlib import Path

from .engine import reconcile
from .io import load_batch
from .report import render_json, render_markdown


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reconcile",
        description="Offline event-chain reconciliation entry point."
    )
    parser.add_argument("batch", nargs="?", help="Path to batch JSON/JSONL file")
    parser.add_argument(
        "--correction",
        action="append",
        default=[],
        help="Optional correction JSON file; may be repeated.",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="Report format.",
    )
    parser.add_argument("--output", "-o", help="Write report to this file.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.batch:
        build_parser().print_help()
        return 2

    batch = load_batch(args.batch)
    corrections = []
    for correction_path in args.correction:
        correction = load_batch(correction_path)
        if "observations" in correction and len(correction) == 1:
            corrections.extend(correction["observations"])
        else:
            corrections.append(correction)

    report = reconcile(batch, corrections)
    rendered = render_json(report) if args.format == "json" else render_markdown(report)
    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
