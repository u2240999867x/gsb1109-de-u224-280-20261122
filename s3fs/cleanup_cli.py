"""Command-line entry point for replay-safe version cleanup."""

import argparse
import json
import sys

from s3fs import S3FileSystem
from s3fs.cleanup import _read_json


def _load(value):
    if value is None:
        return None
    if value == "-":
        return _read_json(sys.stdin.read())
    return _read_json(value)


def _dump(value):
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="s3fs-cleanup",
        description=(
            "Remove exact S3 object versions without leaving a replay batch "
            "without a registered readable copy."
        ),
    )
    parser.add_argument(
        "--ledger",
        required=True,
        help="Replay ledger as a JSON/NDJSON file, inline JSON, or - for stdin",
    )
    parser.add_argument(
        "--work-orders",
        required=True,
        help="Work orders as a JSON/NDJSON file, inline JSON, or - for stdin",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Only report status; do not remove any version",
    )
    parser.add_argument("--endpoint-url", default=None, help="Custom S3 endpoint URL")
    parser.add_argument("--key", default=None, help="Access key id")
    parser.add_argument("--secret", default=None, help="Secret access key")
    parser.add_argument("--token", default=None, help="Session token")
    parser.add_argument("--anon", action="store_true", help="Anonymous access")
    parser.add_argument(
        "--merge-ledger",
        action="store_true",
        help="Merge the supplied batches into previously registered ones",
    )
    args = parser.parse_args(argv)

    ledger = _load(args.ledger)
    work_orders = _load(args.work_orders)
    client_kwargs = {}
    if args.endpoint_url:
        client_kwargs["endpoint_url"] = args.endpoint_url
    fs = S3FileSystem(
        key=args.key,
        secret=args.secret,
        token=args.token,
        anon=args.anon,
        client_kwargs=client_kwargs,
    )
    if args.status:
        result = fs.cleanup_status(ledger=ledger, work_orders=work_orders)
    else:
        result = fs.cleanup(ledger, work_orders, merge_ledger=args.merge_ledger)
    _dump(list(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
