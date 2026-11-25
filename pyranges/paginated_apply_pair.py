"""Streaming, restartable pair reconciliation for PyRanges tables.

The pagination helpers in this module deliberately keep three things separate:

* :class:`ReconciliationIndex` stores only the normalized input rows and their
  stable ordering ranks in a reusable SQLite index.
* :class:`PairPage` contains at most the requested number of source pairs and
  can verify callback results before a receipt is signed.
* A receipt store or :class:`Warehouse` persists the high-water mark.  Resume
  queries use the stable ranks, so acknowledged pairs are neither returned nor
  scanned again.
"""

from __future__ import annotations

import hashlib
import inspect
import io
import json
import os
import pickle
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, NamedTuple

import pandas as pd
from natsort import natsorted

try:  # fcntl is available on POSIX, which is the supported runtime here.
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None


SAMPLE_ID_CANDIDATES = (
    "sample_id",
    "sampleid",
    "stable_sample_id",
    "sample_no",
    "sampleno",
    "sample_number",
    "sampling_id",
    "sampling_segment_id",
    "segment_id",
    "sample_code",
    "sample_key",
    "sid",
    "sample",
    "stable_id",
    "id",
    "编号",
    "采样编号",
)

PERMIT_ID_CANDIDATES = (
    "permit_id",
    "permitid",
    "stable_permit_id",
    "permit_no",
    "permitno",
    "permit_number",
    "license_id",
    "licence_id",
    "license_no",
    "licence_no",
    "permit_code",
    "permit_key",
    "permit_band_id",
    "band_id",
    "band_no",
    "pid",
    "permit",
    "license",
    "licence",
    "stable_id",
    "id",
    "编号",
    "许可编号",
)

CHROM_CANDIDATES = (
    "Chromosome",
    "chromosome",
    "Chrom",
    "chrom",
    "line_id",
    "lineid",
    "line_no",
    "line",
    "Line",
    "线路",
)

STRAND_CANDIDATES = (
    "Strand",
    "strand",
    "Direction",
    "direction",
    "dir",
    "方向",
)

START_CANDIDATES = ("Start", "start", "start_pos", "start_position")
END_CANDIDATES = ("End", "end", "end_pos", "end_position")


class PaginationError(RuntimeError):
    """Base class for pagination/reconciliation failures."""


class BatchMismatchError(PaginationError):
    """Raised when durable state belongs to another batch."""


class InputMismatchError(PaginationError):
    """Raised when reused state was built for different input content."""


class PageVerificationError(PaginationError):
    """Raised when a page cannot be verified."""


class IncompletePageError(PageVerificationError):
    """Raised when callback results do not cover every expected pair."""


class PairKey(NamedTuple):
    """Stable identity of one sample/permit source pair."""

    sample_id: Any
    permit_id: Any

    def to_dict(self) -> dict[str, Any]:
        return {"sample_id": self.sample_id, "permit_id": self.permit_id}


class SourcePair(tuple):
    """A matched source row pair.

    It is a ``(sample_row, permit_row)`` tuple, while also exposing stable IDs
    and ordering ranks.  Thus callbacks can unpack it like an ordinary pair or
    use the named metadata.
    """

    def __new__(
        cls,
        sample_row: dict[str, Any],
        permit_row: dict[str, Any],
        key: PairKey,
        sample_rank: int,
        permit_rank: int,
        chrom: Any = None,
        strand: Any = None,
    ) -> "SourcePair":
        obj = super().__new__(cls, (sample_row, permit_row))
        return obj

    def __init__(
        self,
        sample_row: dict[str, Any],
        permit_row: dict[str, Any],
        key: PairKey,
        sample_rank: int,
        permit_rank: int,
        chrom: Any = None,
        strand: Any = None,
    ) -> None:
        # tuple subclass with __slots__ needs object.__setattr__? Python allows
        # assigning instance attributes unless slots define them. We store
        # metadata through __new__? Simpler: class dict despite tuple.
        self.key = key
        self.sample_rank = sample_rank
        self.permit_rank = permit_rank
        self.chrom = chrom
        self.strand = strand

    @property
    def sample_id(self) -> Any:
        return self.key.sample_id

    @property
    def permit_id(self) -> Any:
        return self.key.permit_id

    @property
    def sample(self) -> dict[str, Any]:
        return self[0]

    @property
    def permit(self) -> dict[str, Any]:
        return self[1]

    @property
    def source_pair(self) -> "SourcePair":
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample_id": self.sample_id,
            "permit_id": self.permit_id,
            "sample_rank": self.sample_rank,
            "permit_rank": self.permit_rank,
            "sample": self.sample,
            "permit": self.permit,
        }


@dataclass
class PageVerification:
    ok: bool
    expected_keys: list[PairKey]
    actual_keys: list[PairKey]
    missing: list[PairKey] = field(default_factory=list)
    unexpected: list[PairKey] = field(default_factory=list)
    duplicates: list[PairKey] = field(default_factory=list)
    out_of_order: list[PairKey] = field(default_factory=list)
    strict_order: bool = False

    def __bool__(self) -> bool:
        return self.ok

    def to_dict(self) -> dict[str, Any]:
        def dump_keys(keys: Iterable[PairKey]) -> list[list[Any]]:
            return [[key.sample_id, key.permit_id] for key in keys]

        return {
            "ok": self.ok,
            "expected_keys": dump_keys(self.expected_keys),
            "actual_keys": dump_keys(self.actual_keys),
            "missing": dump_keys(self.missing),
            "unexpected": dump_keys(self.unexpected),
            "duplicates": dump_keys(self.duplicates),
            "out_of_order": dump_keys(self.out_of_order),
            "strict_order": self.strict_order,
        }


@dataclass
class Receipt:
    """A signed high-water mark for one delivered page or whole-batch completion."""

    batch_id: str
    sequence: int = 0
    status: str = "acknowledged"
    completed: bool = False
    page_size: int | None = None
    keys: list[PairKey] = field(default_factory=list)
    last_sample_id: Any = None
    last_permit_id: Any = None
    last_sample_rank: int | None = None
    last_permit_rank: int | None = None
    acknowledged_pairs: int = 0
    created_at: float = field(default_factory=time.time)
    input_fingerprint: str | None = None
    checksum: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def last_key(self) -> PairKey | None:
        if self.last_sample_id is None or self.last_permit_id is None:
            return None
        return PairKey(self.last_sample_id, self.last_permit_id)

    @property
    def is_complete(self) -> bool:
        return self.completed or self.status == "completed"

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "batch_id": self.batch_id,
            "sequence": self.sequence,
            "status": self.status,
            "completed": self.completed,
            "page_size": self.page_size,
            "keys": [
                [_json_safe(key.sample_id), _json_safe(key.permit_id)] for key in self.keys
            ],
            "last_sample_id": _json_safe(self.last_sample_id),
            "last_permit_id": _json_safe(self.last_permit_id),
            "last_sample_rank": self.last_sample_rank,
            "last_permit_rank": self.last_permit_rank,
            "acknowledged_pairs": self.acknowledged_pairs,
            "created_at": self.created_at,
            "input_fingerprint": self.input_fingerprint,
            "metadata": _json_safe(self.metadata),
        }
        if not self.checksum:
            payload["checksum"] = _checksum(payload)
        else:
            payload["checksum"] = self.checksum
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Receipt":
        keys = [PairKey(item[0], item[1]) for item in payload.get("keys", [])]
        return cls(
            batch_id=payload["batch_id"],
            sequence=int(payload.get("sequence", 0)),
            status=payload.get("status", "acknowledged"),
            completed=bool(payload.get("completed", False)),
            page_size=payload.get("page_size"),
            keys=keys,
            last_sample_id=payload.get("last_sample_id"),
            last_permit_id=payload.get("last_permit_id"),
            last_sample_rank=payload.get("last_sample_rank"),
            last_permit_rank=payload.get("last_permit_rank"),
            acknowledged_pairs=int(payload.get("acknowledged_pairs", 0)),
            created_at=float(payload.get("created_at", time.time())),
            input_fingerprint=payload.get("input_fingerprint"),
            checksum=payload.get("checksum", ""),
            metadata=payload.get("metadata", {}) or {},
        )


@dataclass
class ReconciliationReport:
    batch_id: str
    completed: bool
    total_pairs: int
    pages: int
    receipt: Receipt | None = None
    warehouse_path: str | None = None
    receipt_path: str | None = None
    index_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "completed": self.completed,
            "total_pairs": self.total_pairs,
            "pages": self.pages,
            "receipt": self.receipt.to_dict() if self.receipt else None,
            "warehouse_path": self.warehouse_path,
            "receipt_path": self.receipt_path,
            "index_path": self.index_path,
        }


CompletionReport = ReconciliationReport


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _checksum(payload: dict[str, Any]) -> str:
    body = {k: v for k, v in payload.items() if k != "checksum"}
    raw = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _find_column(
    columns: Iterable[Any], explicit: str | None, candidates: tuple[str, ...], role: str
) -> str | None:
    columns = list(columns)
    if explicit is not None:
        if explicit not in columns:
            raise ValueError(f"{role} column {explicit!r} was not found: {columns!r}")
        return explicit

    exact = {str(column): column for column in columns}
    lower = {str(column).lower(): column for column in columns}
    for candidate in candidates:
        if candidate in exact:
            return exact[candidate]
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    return None


def _as_dataframe(data: Any) -> pd.DataFrame:
    if data is None:
        raise ValueError("A PyRanges/DataFrame/record list is required")
    if hasattr(data, "df") and not isinstance(data, pd.DataFrame):
        frame = data.df
    elif isinstance(data, pd.DataFrame):
        frame = data
    elif isinstance(data, (list, tuple)):
        frame = pd.DataFrame(list(data))
    elif isinstance(data, dict):
        frame = pd.DataFrame(data)
    else:
        raise TypeError(f"Unsupported table type: {type(data)!r}")

    frame = frame.copy()
    if frame.index.name is not None and frame.index.name not in frame.columns:
        frame = frame.reset_index()
    return frame


def _ordered_ids(values: Iterable[Any], order: str = "natural") -> list[Any]:
    values = list(values)
    if order == "natural":
        try:
            return natsorted(values)
        except Exception:
            pass
    elif order not in {"lexicographic", "lexical", "stable"}:
        raise ValueError("id_order must be 'natural' or 'lexicographic'")

    try:
        return sorted(values)
    except TypeError:
        return sorted(values, key=lambda value: (type(value).__name__, str(value)))


def _content_fingerprint(frame: pd.DataFrame, id_col: str) -> str:
    ordered_columns = sorted((str(column) for column in frame.columns))
    ordered = frame.loc[:, ordered_columns].sort_values(id_col, kind="stable")
    buffer = io.StringIO()
    ordered.to_csv(buffer, index=False)
    return hashlib.sha256(buffer.getvalue().encode("utf-8")).hexdigest()


def _number(value: Any) -> Any:
    if hasattr(value, "item"):
        value = value.item()
    try:
        integer = int(value)
        if integer == float(value):
            return integer
    except (TypeError, ValueError, OverflowError):
        pass
    return value


def _group_key(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value)


def _opposite_strand(value: str) -> str:
    return {"+": "-", "-": "+"}.get(value, value)


class ReconciliationIndex:
    """Reusable SQLite index for samples and permits."""

    def __init__(
        self,
        path: str | Path = ":memory:",
        *,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        if path != ":memory:":
            path_obj = Path(path)
            if path_obj.exists() and path_obj.is_dir():
                path_obj = path_obj / "reconciliation.index.sqlite3"
            path = path_obj
        self.path = ":memory:" if path == ":memory:" else str(Path(path))
        self._owns_connection = connection is None
        self.conn = connection or sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row

    @classmethod
    def create(
        cls,
        samples: Any,
        permits: Any,
        path: str | Path = ":memory:",
        *,
        batch_id: str | None = None,
        sample_id_col: str | None = None,
        permit_id_col: str | None = None,
        chrom_col: str | None = None,
        strand_col: str | None = None,
        start_col: str | None = None,
        end_col: str | None = None,
        id_order: str = "natural",
        rebuild: bool = False,
    ) -> "ReconciliationIndex":
        path_obj = None if path == ":memory:" else Path(path)
        if path_obj is not None:
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            if rebuild and path_obj.exists():
                path_obj.unlink()

        if path_obj is not None and path_obj.exists() and not rebuild:
            index = cls(path_obj)
            metadata = index.metadata()
            sample_frame = _as_dataframe(samples)
            permit_frame = _as_dataframe(permits)
            sample_col = _find_column(
                sample_frame.columns, sample_id_col, SAMPLE_ID_CANDIDATES, "sample ID"
            )
            permit_col = _find_column(
                permit_frame.columns, permit_id_col, PERMIT_ID_CANDIDATES, "permit ID"
            )
            if sample_col is None or permit_col is None:
                raise ValueError("Could not infer stable ID columns for index verification")
            fingerprint = _pair_fingerprint(sample_frame, permit_frame, sample_col, permit_col)
            if metadata.get("fingerprint") != fingerprint:
                raise InputMismatchError(
                    "Existing reconciliation index does not match the canonical input content"
                )
            return index

        sample_frame = _as_dataframe(samples)
        permit_frame = _as_dataframe(permits)
        sample_col = _find_column(
            sample_frame.columns, sample_id_col, SAMPLE_ID_CANDIDATES, "sample ID"
        )
        permit_col = _find_column(
            permit_frame.columns, permit_id_col, PERMIT_ID_CANDIDATES, "permit ID"
        )
        if sample_col is None:
            raise ValueError("Samples need a stable ID column such as 'sample_id'")
        if permit_col is None:
            raise ValueError("Permits need a stable ID column such as 'permit_id'")

        sample_chrom = _find_column(sample_frame.columns, chrom_col, CHROM_CANDIDATES, "chromosome/line")
        permit_chrom = _find_column(permit_frame.columns, chrom_col, CHROM_CANDIDATES, "chromosome/line")
        sample_strand = _find_column(sample_frame.columns, strand_col, STRAND_CANDIDATES, "strand/direction")
        permit_strand = _find_column(permit_frame.columns, strand_col, STRAND_CANDIDATES, "strand/direction")
        sample_start = _find_column(sample_frame.columns, start_col, START_CANDIDATES, "start") or "Start"
        sample_end = _find_column(sample_frame.columns, end_col, END_CANDIDATES, "end") or "End"
        permit_start = _find_column(permit_frame.columns, start_col, START_CANDIDATES, "start") or "Start"
        permit_end = _find_column(permit_frame.columns, end_col, END_CANDIDATES, "end") or "End"

        for frame, start, end in (
            (sample_frame, sample_start, sample_end),
            (permit_frame, permit_start, permit_end),
        ):
            if start not in frame.columns or end not in frame.columns:
                raise ValueError("Both tables require Start and End columns")

        fingerprint = _pair_fingerprint(sample_frame, permit_frame, sample_col, permit_col)
        effective_batch = batch_id or fingerprint[:16]

        if path_obj is None:
            index = cls(":memory:")
            index._build(
                sample_frame,
                permit_frame,
                effective_batch,
                fingerprint,
                sample_col,
                permit_col,
                sample_chrom,
                permit_chrom,
                sample_strand,
                permit_strand,
                sample_start,
                sample_end,
                permit_start,
                permit_end,
                id_order,
            )
            return index

        temporary = path_obj.with_name(f".{path_obj.name}.build-{os.getpid()}-{time.time_ns()}")
        try:
            builder = cls(temporary)
            builder._build(
                sample_frame,
                permit_frame,
                effective_batch,
                fingerprint,
                sample_col,
                permit_col,
                sample_chrom,
                permit_chrom,
                sample_strand,
                permit_strand,
                sample_start,
                sample_end,
                permit_start,
                permit_end,
                id_order,
            )
            builder.close()
            os.replace(temporary, path_obj)
        finally:
            if temporary.exists():
                temporary.unlink()
        return cls(path_obj)

    def _build(
        self,
        sample_frame: pd.DataFrame,
        permit_frame: pd.DataFrame,
        batch_id: str,
        fingerprint: str,
        sample_col: str,
        permit_col: str,
        sample_chrom: str | None,
        permit_chrom: str | None,
        sample_strand: str | None,
        permit_strand: str | None,
        sample_start: str,
        sample_end: str,
        permit_start: str,
        permit_end: str,
        id_order: str,
    ) -> None:
        conn = self.conn
        conn.execute("PRAGMA synchronous=FULL")
        conn.executescript(
            """
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE sample (
                rank INTEGER PRIMARY KEY,
                id_pickle BLOB NOT NULL,
                id_text TEXT NOT NULL,
                chrom_key TEXT NOT NULL,
                strand_key TEXT NOT NULL,
                start NUMERIC NOT NULL,
                end NUMERIC NOT NULL,
                row_pickle BLOB NOT NULL
            );
            CREATE TABLE permit (
                rank INTEGER PRIMARY KEY,
                group_rank INTEGER NOT NULL,
                id_pickle BLOB NOT NULL,
                id_text TEXT NOT NULL,
                chrom_key TEXT NOT NULL,
                strand_key TEXT NOT NULL,
                start NUMERIC NOT NULL,
                end NUMERIC NOT NULL,
                row_pickle BLOB NOT NULL
            );
            CREATE INDEX idx_permit_cursor
                ON permit(chrom_key, strand_key, group_rank);
            """
        )

        sample_ids = sample_frame[sample_col].tolist()
        if len(set(map(_id_hashable, sample_ids))) != len(sample_ids):
            raise ValueError(f"Stable sample IDs in {sample_col!r} must be unique")
        sample_order = {value: rank for rank, value in enumerate(_ordered_ids(sample_ids, id_order))}

        permit_ids = permit_frame[permit_col].tolist()
        if len(set(map(_id_hashable, permit_ids))) != len(permit_ids):
            raise ValueError(f"Stable permit IDs in {permit_col!r} must be unique")
        permit_order = {value: rank for rank, value in enumerate(_ordered_ids(permit_ids, id_order))}

        sample_columns = list(sample_frame.columns)

        def sample_tuples():
            for values in sample_frame.itertuples(index=False, name=None):
                record = dict(zip(sample_columns, values))
                sample_id = record[sample_col]
                yield (
                    sample_order[sample_id],
                    sqlite3.Binary(pickle.dumps(sample_id)),
                    str(sample_id),
                    _group_key(record.get(sample_chrom) if sample_chrom else ""),
                    _group_key(record.get(sample_strand) if sample_strand else ""),
                    _number(record[sample_start]),
                    _number(record[sample_end]),
                    sqlite3.Binary(pickle.dumps(record)),
                )

        conn.executemany(
            "INSERT INTO sample VALUES (?,?,?,?,?,?,?,?)",
            sample_tuples(),
        )

        permit_work = permit_frame.copy()
        permit_work["_paginated_rank"] = permit_work[permit_col].map(permit_order)
        permit_work["_paginated_chrom"] = (
            permit_work[permit_chrom].map(_group_key) if permit_chrom else ""
        )
        permit_work["_paginated_strand"] = (
            permit_work[permit_strand].map(_group_key) if permit_strand else ""
        )
        permit_work = permit_work.sort_values(
            ["_paginated_chrom", "_paginated_strand", "_paginated_rank"],
            kind="stable",
        )
        permit_work["_paginated_group_rank"] = permit_work.groupby(
            ["_paginated_chrom", "_paginated_strand"], sort=False, observed=True
        ).cumcount()

        original_permit_columns = list(permit_frame.columns)
        helper_count = 4
        permit_columns = list(permit_work.columns)

        def permit_tuples():
            for values in permit_work.itertuples(index=False, name=None):
                work_record = dict(zip(permit_columns, values))
                record = dict(zip(original_permit_columns, values[:-helper_count]))
                permit_id = record[permit_col]
                yield (
                    int(work_record["_paginated_rank"]),
                    int(work_record["_paginated_group_rank"]),
                    sqlite3.Binary(pickle.dumps(permit_id)),
                    str(permit_id),
                    work_record["_paginated_chrom"],
                    work_record["_paginated_strand"],
                    _number(record[permit_start]),
                    _number(record[permit_end]),
                    sqlite3.Binary(pickle.dumps(record)),
                )

        conn.executemany(
            "INSERT INTO permit VALUES (?,?,?,?,?,?,?,?,?)",
            permit_tuples(),
        )

        metadata = {
            "batch_id": batch_id,
            "fingerprint": fingerprint,
            "sample_id_col": sample_col,
            "permit_id_col": permit_col,
            "sample_chrom": sample_chrom,
            "permit_chrom": permit_chrom,
            "sample_strand": sample_strand,
            "permit_strand": permit_strand,
            "sample_start": sample_start,
            "sample_end": sample_end,
            "permit_start": permit_start,
            "permit_end": permit_end,
        }
        conn.execute("INSERT INTO meta VALUES ('document', ?)", (json.dumps(metadata),))
        conn.commit()

    def metadata(self) -> dict[str, Any]:
        row = self.conn.execute("SELECT value FROM meta WHERE key='document'").fetchone()
        return json.loads(row[0]) if row else {}

    def close(self) -> None:
        if self._owns_connection:
            self.conn.close()

    def _sample(self, rank: int) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM sample WHERE rank=?", (rank,)).fetchone()

    def _source_pair(self, sample_row: sqlite3.Row, permit_row: sqlite3.Row) -> SourcePair:
        sample_id = pickle.loads(sample_row["id_pickle"])
        permit_id = pickle.loads(permit_row["id_pickle"])
        return SourcePair(
            pickle.loads(sample_row["row_pickle"]),
            pickle.loads(permit_row["row_pickle"]),
            PairKey(sample_id, permit_id),
            int(sample_row["rank"]),
            int(permit_row["group_rank"]),
            sample_row["chrom_key"],
            sample_row["strand_key"],
        )

    def _permits_for_sample(
        self,
        sample_row: sqlite3.Row,
        after_group_rank: int,
        limit: int,
        strandedness: str | bool | None = "same",
    ) -> list[SourcePair]:
        if limit <= 0:
            return []
        chrom_key = sample_row["chrom_key"]
        strand_key = sample_row["strand_key"]
        start = sample_row["start"]
        end = sample_row["end"]

        if strandedness in {False, None}:
            sql = """
                SELECT * FROM permit
                WHERE chrom_key=? AND group_rank > ?
                  AND start < ? AND end > ?
                ORDER BY group_rank
                LIMIT ?
            """
            params: tuple[Any, ...] = (chrom_key, after_group_rank, end, start, limit)
        elif strandedness == "opposite":
            wanted = _opposite_strand(strand_key)
            sql = """
                SELECT * FROM permit
                WHERE chrom_key=? AND strand_key=? AND group_rank > ?
                  AND start < ? AND end > ?
                ORDER BY group_rank
                LIMIT ?
            """
            params = (chrom_key, wanted, after_group_rank, end, start, limit)
        else:
            sql = """
                SELECT * FROM permit
                WHERE chrom_key=? AND strand_key=? AND group_rank > ?
                  AND start < ? AND end > ?
                ORDER BY group_rank
                LIMIT ?
            """
            params = (chrom_key, strand_key, after_group_rank, end, start, limit)

        rows = self.conn.execute(sql, params).fetchall()
        return [self._source_pair(sample_row, row) for row in rows]

    def fetch_page(
        self,
        limit: int,
        after_sample_rank: int | None = None,
        after_permit_rank: int | None = None,
        *,
        strandedness: str | bool | None = "same",
    ) -> tuple[list[SourcePair], bool]:
        """Return at most ``limit`` pairs and an exhaustion flag.

        Ranks are keyset cursors.  In particular, permit filtering starts after
        the last signed group rank instead of restarting the permit table.
        """
        if limit <= 0:
            raise ValueError("page limit must be positive")

        pairs: list[SourcePair] = []
        if after_sample_rank is not None:
            sample_row = self._sample(after_sample_rank)
            if sample_row is not None:
                pairs.extend(
                    self._permits_for_sample(
                        sample_row,
                        after_permit_rank if after_permit_rank is not None else -1,
                        limit,
                        strandedness,
                    )
                )
            next_sample_rank = after_sample_rank + 1
        else:
            next_sample_rank = 0

        while len(pairs) < limit:
            sample_row = self._sample(next_sample_rank)
            if sample_row is None:
                return pairs, True
            pairs.extend(
                self._permits_for_sample(
                    sample_row, -1, limit - len(pairs), strandedness
                )
            )
            next_sample_rank += 1

        return pairs, False


PairIndex = ReconciliationIndex


def _id_hashable(value: Any) -> Any:
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            pass
    if isinstance(value, (list, dict, set)):
        return json.dumps(_json_safe(value), sort_keys=True)
    return value


def _pair_fingerprint(
    sample_frame: pd.DataFrame,
    permit_frame: pd.DataFrame,
    sample_col: str,
    permit_col: str,
) -> str:
    sample_hash = _content_fingerprint(sample_frame, sample_col)
    permit_hash = _content_fingerprint(permit_frame, permit_col)
    return hashlib.sha256(f"{sample_hash}|{permit_hash}".encode("utf-8")).hexdigest()


def _extract_key(value: Any, expected: PairKey | None = None) -> PairKey | None:
    if isinstance(value, PairKey):
        return value
    if isinstance(value, SourcePair):
        return value.key

    if hasattr(value, "sample_id") and hasattr(value, "permit_id"):
        try:
            return PairKey(value.sample_id, value.permit_id)
        except Exception:
            pass

    if isinstance(value, dict):
        sample_id = _first_mapping_value(value, SAMPLE_ID_CANDIDATES + ("sample",))
        permit_id = _first_mapping_value(value, PERMIT_ID_CANDIDATES + ("permit",))
        if sample_id is not None and permit_id is not None and not isinstance(sample_id, (dict, list)):
            return PairKey(sample_id, permit_id)
        for key_name in ("pair_key", "key", "source_pair"):
            if key_name in value:
                found = _extract_key(value[key_name])
                if found is not None:
                    return found

    if hasattr(value, "__dict__"):
        found = _extract_key(vars(value))
        if found is not None:
            return found

    if isinstance(value, pd.Series):
        return _extract_key(value.to_dict())

    if isinstance(value, (list, tuple)):
        if len(value) >= 2:
            first, second = value[0], value[1]
            if not isinstance(first, (dict, list)) and not isinstance(second, (dict, list)):
                return PairKey(first, second)
            found = _extract_key(first)
            if found is not None:
                return found
            found = _extract_key(second)
            if found is not None:
                return found

    if expected is not None and value is None:
        return None
    return None


def _first_mapping_value(mapping: dict[str, Any], names: tuple[str, ...]) -> Any:
    lower = {str(key).lower(): key for key in mapping}
    for name in names:
        if name in mapping:
            return mapping[name]
        if name.lower() in lower:
            return mapping[lower[name.lower()]]
    return None


def _callback_wants_two_frames(callback: Callable[..., Any]) -> bool:
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return False
    positional = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    has_var_positional = any(
        parameter.kind == inspect.Parameter.VAR_POSITIONAL
        for parameter in signature.parameters.values()
    )
    required = [parameter for parameter in positional if parameter.default is inspect.Parameter.empty]
    return len(required) >= 2 or (len(positional) >= 2 and not has_var_positional)


def _invoke_page_callback(callback: Callable[..., Any], page: "PairPage") -> Any:
    if _callback_wants_two_frames(callback):
        sample_frame, permit_frame = page.to_dataframes()
        return callback(sample_frame, permit_frame)
    return callback(page)


def store_latest_receipt(store: Any, batch_id: str | None = None) -> Receipt | None:
    if store is None:
        return None
    for name in (
        "latest_receipt",
        "load_latest_receipt",
        "get_latest_receipt",
        "last_receipt",
        "latest",
        "load_latest",
        "get_last_receipt",
        "last",
    ):
        method = getattr(store, name, None)
        if not callable(method):
            continue
        try:
            value = method(batch_id)
        except TypeError:
            value = method()
        if value is not None:
            return value
    return None


def append_receipt_to_store(store: Any, receipt: Receipt) -> Receipt:
    if store is None:
        return receipt
    for name in (
        "append",
        "append_receipt",
        "add_receipt",
        "save_receipt",
        "put_receipt",
        "store_receipt",
        "save_checkpoint",
        "save",
        "add",
        "put",
    ):
        method = getattr(store, name, None)
        if not callable(method):
            continue
        try:
            value = method(receipt)
        except TypeError:
            try:
                value = method(receipt.to_dict())
            except TypeError:
                continue
        if isinstance(value, Receipt):
            return value
        return receipt
    return receipt


class PairPage(list):
    """A bounded list of source pairs plus verification/signing operations."""

    def __init__(
        self,
        pairs: Iterable[SourcePair],
        *,
        batch_id: str,
        sequence: int,
        page_size: int,
        exhausted: bool = False,
        acknowledged_pairs: int = 0,
        input_fingerprint: str | None = None,
        strict_order: bool = False,
    ) -> None:
        super().__init__(pairs)
        self.pairs: list[SourcePair] = list(self)
        self.source_pairs = self.pairs
        self.expected = self.pairs
        self.actual: list[Any] = []
        self.returned: list[Any] = []
        self.batch_id = batch_id
        self.sequence = sequence
        self.requested_page_size = page_size
        self.exhausted = exhausted
        self.is_last_page = exhausted and len(self) <= page_size
        self.acknowledged_pairs_before_page = acknowledged_pairs
        self.input_fingerprint = input_fingerprint
        self.strict_order = strict_order
        self._submitted_results: list[Any] | None = None
        self.ordered_results: list[Any] | None = None
        self.verification: PageVerification | None = None
        self.receipt: Receipt | None = None

    @property
    def expected_keys(self) -> list[PairKey]:
        return [pair.key for pair in self]

    @property
    def expected_pair_keys(self) -> list[PairKey]:
        return self.expected_keys

    @property
    def expected_pairs(self) -> list[SourcePair]:
        return self.pairs

    @property
    def expected_source_pairs(self) -> list[SourcePair]:
        return self.pairs

    @property
    def actual_keys(self) -> list[PairKey]:
        return self.verification.actual_keys if self.verification else []

    @property
    def actual_pair_keys(self) -> list[PairKey]:
        return self.actual_keys

    def submit(self, results: Any) -> list[Any]:
        if results is None:
            values: list[Any] = []
        elif isinstance(results, pd.DataFrame):
            values = results.to_dict("records")
        elif isinstance(results, (list, tuple)):
            values = list(results)
        elif isinstance(results, Iterable) and not isinstance(results, (dict, str)):
            values = list(results)
        else:
            values = [results]
        self._submitted_results = values
        return values

    set_results = submit

    def to_dataframes(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return page rows as aligned sample/permit DataFrames."""
        samples = pd.DataFrame([pair.sample for pair in self])
        permits = pd.DataFrame([pair.permit for pair in self])
        return samples, permits

    def _results_for_verification(self, results: Any | None) -> list[Any]:
        if results is not None:
            return self.submit(results)
        if self._submitted_results is None:
            return self.submit(None)
        return self._submitted_results

    def verify(
        self,
        results: Any | None = None,
        *,
        raise_on_error: bool = False,
        strict_order: bool | None = None,
    ) -> PageVerification:
        values = self._results_for_verification(results)
        self.returned = values
        expected = self.expected_keys
        strict = self.strict_order if strict_order is None else strict_order

        actual: list[PairKey] = []
        for index, value in enumerate(values):
            key = _extract_key(value, expected[index] if index < len(expected) else None)
            if key is None and index < len(expected):
                # Keyless results are accepted positionally.  A short returned
                # list therefore leaves the tail of the expected page missing.
                key = expected[index]
            if key is not None:
                actual.append(key)

        expected_counts: dict[PairKey, int] = {}
        actual_counts: dict[PairKey, int] = {}
        for key in expected:
            expected_counts[key] = expected_counts.get(key, 0) + 1
        for key in actual:
            actual_counts[key] = actual_counts.get(key, 0) + 1

        missing: list[PairKey] = []
        for key in expected:
            if expected_counts[key] > actual_counts.get(key, 0):
                missing.append(key)
                actual_counts[key] = actual_counts.get(key, 0) + 1

        unexpected = [key for key in actual if key not in expected_counts]
        duplicates = [
            key for key, count in actual_counts.items() if count > 1 and key in expected_counts
        ]
        order_mismatch = [
            key
            for index, key in enumerate(actual)
            if index < len(expected) and key != expected[index] and key in expected_counts
        ]

        ok = not missing and not unexpected and not duplicates
        if strict:
            ok = ok and actual == expected

        verification = PageVerification(
            ok=ok,
            expected_keys=expected,
            actual_keys=actual,
            missing=_dedupe(missing),
            unexpected=_dedupe(unexpected),
            duplicates=_dedupe(duplicates),
            out_of_order=_dedupe(order_mismatch),
            strict_order=strict,
        )
        self.verification = verification
        self.actual = self.actual_source_pairs

        if ok:
            by_key: dict[PairKey, Any] = {}
            for value, key in zip(values, actual):
                by_key[key] = value
            self.ordered_results = [by_key[key] for key in expected]
        else:
            self.ordered_results = None
            if raise_on_error:
                raise IncompletePageError(
                    "Page verification failed: "
                    f"missing={verification.missing}, unexpected={verification.unexpected}, "
                    f"duplicates={verification.duplicates}"
                )
        return verification

    def build_receipt(self) -> Receipt:
        if self.verification is None or not self.verification.ok:
            self.verify(raise_on_error=True)
        assert self.verification is not None and self.verification.ok

        last_pair = self[-1] if self else None
        last_key = last_pair.key if last_pair else None
        return Receipt(
            batch_id=self.batch_id,
            sequence=self.sequence,
            status="acknowledged",
            completed=False,
            page_size=self.requested_page_size,
            keys=list(self.expected_keys),
            last_sample_id=last_key.sample_id if last_key else None,
            last_permit_id=last_key.permit_id if last_key else None,
            last_sample_rank=last_pair.sample_rank if last_pair else None,
            last_permit_rank=last_pair.permit_rank if last_pair else None,
            acknowledged_pairs=self.acknowledged_pairs_before_page + len(self),
            input_fingerprint=self.input_fingerprint,
        )

    def confirm(
        self,
        results: Any | None = None,
        *,
        sink: Any | None = None,
        receipt_store: "ReceiptStore | None" = None,
        save: bool = True,
    ) -> Receipt:
        self.verify(results, raise_on_error=True)
        receipt = self.build_receipt()
        if save:
            if sink is not None and hasattr(sink, "save_page"):
                sink.save_page(self, receipt)
                if hasattr(sink, "latest_receipt"):
                    stored = sink.latest_receipt(self.batch_id)
                    if stored is not None:
                        receipt = stored
            elif sink is not None and hasattr(sink, "save_results_and_receipt"):
                sink.save_results_and_receipt(self.ordered_results, receipt)
            elif sink is not None and hasattr(sink, "save"):
                sink.save(self.ordered_results, receipt)
            elif receipt_store is not None:
                append_receipt_to_store(receipt_store, receipt)
            elif sink is not None and hasattr(sink, "save_receipt"):
                append_receipt_to_store(sink, receipt)
        self.receipt = receipt
        return receipt

    acknowledge = confirm
    ack = confirm
    sign = confirm
    commit = confirm

    def check(self, *args: Any, **kwargs: Any) -> PageVerification:
        return self.verify(*args, **kwargs)

    @property
    def returned_keys(self) -> list[PairKey]:
        return self.actual_keys

    @property
    def actual_source_pairs(self) -> list[SourcePair]:
        by_key = {pair.key: pair for pair in self.pairs}
        return [by_key[key] for key in self.actual_keys if key in by_key]

    @property
    def actual_pairs(self) -> list[SourcePair]:
        return self.actual_source_pairs

    @property
    def missing_pairs(self) -> list[PairKey]:
        return self.verification.missing if self.verification else list(self.expected_keys)


Page = PairPage


def _dedupe(values: list[PairKey]) -> list[PairKey]:
    seen: set[PairKey] = set()
    result = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


class ReceiptStore:
    """Small in-memory receipt store and receipt-store interface."""

    def __init__(self) -> None:
        self._receipts: list[Receipt] = []

    def append(self, receipt: Receipt) -> Receipt:
        if receipt.sequence <= 0:
            receipt.sequence = len(self._receipts) + 1
        self._receipts.append(receipt)
        return receipt

    def latest(self, batch_id: str | None = None) -> Receipt | None:
        receipt = self._receipts[-1] if self._receipts else None
        if receipt is not None and batch_id is not None and receipt.batch_id != batch_id:
            raise BatchMismatchError(
                f"Latest receipt is for batch {receipt.batch_id!r}, not {batch_id!r}"
            )
        return receipt

    def all(self, batch_id: str | None = None) -> list[Receipt]:
        if batch_id is None:
            return list(self._receipts)
        return [receipt for receipt in self._receipts if receipt.batch_id == batch_id]

    def latest_receipt(self, batch_id: str | None = None) -> Receipt | None:
        return self.latest(batch_id)


InMemoryReceiptStore = ReceiptStore


class JsonReceiptStore(ReceiptStore):
    """Append-only JSONL receipt log.

    Writes are flushed and fsynced; an ``fcntl`` lock serializes independent
    processes sharing the same receipt path.
    """

    def __init__(self, path: str | Path) -> None:
        path_obj = Path(path)
        if path_obj.exists() and path_obj.is_dir():
            path_obj = path_obj / "receipts.jsonl"
        self.path = path_obj
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")

    def _lock(self):
        lock_file = open(self.lock_path, "a+")
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        return lock_file

    def _unlock(self, lock_file) -> None:
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()

    def append(self, receipt: Receipt) -> Receipt:
        lock = self._lock()
        try:
            latest = self._read_latest_unlocked()
            if receipt.sequence <= 0:
                receipt.sequence = (latest.sequence + 1) if latest else 1
            elif latest is not None and receipt.sequence != latest.sequence + 1:
                # Sequence is advisory for callers that construct pages manually.
                receipt.sequence = latest.sequence + 1
            with open(self.path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(receipt.to_dict(), ensure_ascii=False) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            return receipt
        finally:
            self._unlock(lock)

    def _read_latest_unlocked(self) -> Receipt | None:
        if not self.path.exists():
            return None
        last_line = ""
        with open(self.path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            position = handle.tell()
            buffer = b""
            while position > 0:
                read_size = min(8192, position)
                position -= read_size
                handle.seek(position)
                buffer = handle.read(read_size) + buffer
                lines = buffer.splitlines()
                if len(lines) >= 2 or position == 0:
                    last_line = lines[-1].decode("utf-8")
                    break
            else:
                return None
        if not last_line.strip():
            return None
        return Receipt.from_dict(json.loads(last_line))

    def latest(self, batch_id: str | None = None) -> Receipt | None:
        lock = self._lock()
        try:
            receipt = self._read_latest_unlocked()
        finally:
            self._unlock(lock)
        if receipt is not None and batch_id is not None and receipt.batch_id != batch_id:
            raise BatchMismatchError(
                f"Latest receipt is for batch {receipt.batch_id!r}, not {batch_id!r}"
            )
        return receipt

    def receipts(self) -> list[Receipt]:
        if not self.path.exists():
            return []
        with open(self.path, encoding="utf-8") as handle:
            return [Receipt.from_dict(json.loads(line)) for line in handle if line.strip()]


CheckpointStore = JsonReceiptStore
ReceiptBook = JsonReceiptStore


class SQLiteWarehouse(ReceiptStore):
    """Transactional result and receipt storage in one SQLite file."""

    def __init__(self, path: str | Path = ":memory:", *, batch_id: str | None = None) -> None:
        self.path = ":memory:" if path == ":memory:" else str(Path(path))
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS run_meta (
                batch_id TEXT PRIMARY KEY,
                fingerprint TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                latest_sequence INTEGER NOT NULL DEFAULT 0,
                acknowledged_pairs INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS receipts (
                sequence INTEGER PRIMARY KEY,
                batch_id TEXT NOT NULL,
                status TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                payload TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS results (
                batch_id TEXT NOT NULL,
                sample_key TEXT NOT NULL,
                permit_key TEXT NOT NULL,
                sample_id_json TEXT,
                permit_id_json TEXT,
                sample_rank INTEGER,
                permit_rank INTEGER,
                item_order INTEGER NOT NULL DEFAULT 0,
                receipt_sequence INTEGER NOT NULL,
                payload_json TEXT,
                payload_pickle BLOB,
                updated_at REAL NOT NULL,
                PRIMARY KEY (batch_id, sample_key, permit_key)
            ) WITHOUT ROWID;
            """
        )
        self.conn.commit()
        self.batch_id = batch_id
        if batch_id is not None:
            self.begin_batch(batch_id)

    def begin_batch(self, batch_id: str, fingerprint: str | None = None) -> None:
        row = self.conn.execute("SELECT batch_id FROM run_meta LIMIT 1").fetchone()
        if row is not None and row["batch_id"] != batch_id:
            raise BatchMismatchError(
                f"Warehouse already contains batch {row['batch_id']!r}; refusing {batch_id!r}"
            )
        if row is None:
            self.conn.execute(
                "INSERT INTO run_meta VALUES (?,?, 'active', 0, 0, ?)",
                (batch_id, fingerprint, time.time()),
            )
            self.conn.commit()
        elif fingerprint is not None:
            self.conn.execute(
                "UPDATE run_meta SET fingerprint=?, updated_at=? WHERE batch_id=?",
                (fingerprint, time.time(), batch_id),
            )
            self.conn.commit()
        self.batch_id = batch_id

    def save_page(self, page: PairPage, receipt: Receipt | None = None) -> Receipt:
        if receipt is None:
            receipt = page.build_receipt()
        if self.batch_id is None:
            self.batch_id = receipt.batch_id
            self.begin_batch(receipt.batch_id, receipt.input_fingerprint)
        elif receipt.batch_id != self.batch_id:
            raise BatchMismatchError("Receipt batch does not match warehouse batch")

        assert page.verification is not None and page.verification.ok
        results = page.ordered_results if page.ordered_results is not None else []
        if len(results) != len(page.expected_keys):
            raise PageVerificationError("Verified page results are not aligned with expected keys")

        with self.conn:
            self._insert_receipt(receipt)
            for item_order, (key, result, pair) in enumerate(
                zip(page.expected_keys, results, page)
            ):
                self._upsert_result(
                    key,
                    receipt.sequence,
                    result,
                    item_order=item_order,
                    sample_rank=pair.sample_rank,
                    permit_rank=pair.permit_rank,
                )
            self.conn.execute(
                """UPDATE run_meta
                   SET latest_sequence=?, acknowledged_pairs=?, status='active', updated_at=?
                   WHERE batch_id=?""",
                (receipt.sequence, receipt.acknowledged_pairs, time.time(), receipt.batch_id),
            )
        page.receipt = receipt
        return receipt

    def save(self, results: Any, receipt: Receipt) -> Receipt:
        """Save result payloads and their signed receipt in one transaction."""
        if receipt.batch_id != self.batch_id:
            if self.batch_id is None:
                self.begin_batch(receipt.batch_id, receipt.input_fingerprint)
            else:
                raise BatchMismatchError("Receipt batch does not match warehouse batch")
        values = list(results or [])
        if len(values) != len(receipt.keys):
            raise PageVerificationError("Results do not match signed receipt keys")
        with self.conn:
            self._insert_receipt(receipt)
            for item_order, (key, result) in enumerate(zip(receipt.keys, values)):
                self._upsert_result(key, receipt.sequence, result, item_order=item_order)
            self.conn.execute(
                """UPDATE run_meta
                   SET latest_sequence=?, acknowledged_pairs=?, status=?, updated_at=?
                   WHERE batch_id=?""",
                (
                    receipt.sequence,
                    receipt.acknowledged_pairs,
                    "completed" if receipt.completed else "active",
                    time.time(),
                    receipt.batch_id,
                ),
            )
        return receipt

    save_results_and_receipt = save

    def save_completion(self, receipt: Receipt) -> Receipt:
        receipt.completed = True
        receipt.status = "completed"
        with self.conn:
            self._insert_receipt(receipt)
            self.conn.execute(
                """UPDATE run_meta
                   SET latest_sequence=?, acknowledged_pairs=?, status='completed', updated_at=?
                   WHERE batch_id=?""",
                (receipt.sequence, receipt.acknowledged_pairs, time.time(), receipt.batch_id),
            )
        return receipt

    def _insert_receipt(self, receipt: Receipt) -> None:
        payload = json.dumps(receipt.to_dict(), ensure_ascii=False)
        self.conn.execute(
            "INSERT OR REPLACE INTO receipts VALUES (?,?,?,?,?,?)",
            (
                receipt.sequence,
                receipt.batch_id,
                receipt.status,
                int(receipt.completed),
                payload,
                receipt.created_at,
            ),
        )

    def _upsert_result(
        self,
        key: PairKey,
        sequence: int,
        result: Any,
        *,
        item_order: int = 0,
        sample_rank: int | None = None,
        permit_rank: int | None = None,
    ) -> None:
        try:
            payload_json = json.dumps(_json_safe(result), ensure_ascii=False)
        except (TypeError, ValueError):
            payload_json = json.dumps({"_unserializable": str(result)}, ensure_ascii=False)
        self.conn.execute(
            """INSERT OR REPLACE INTO results
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                self.batch_id,
                key.sample_id if isinstance(key.sample_id, str) else str(key.sample_id),
                key.permit_id if isinstance(key.permit_id, str) else str(key.permit_id),
                json.dumps(_json_safe(key.sample_id), ensure_ascii=False),
                json.dumps(_json_safe(key.permit_id), ensure_ascii=False),
                sample_rank,
                permit_rank,
                item_order,
                sequence,
                payload_json,
                sqlite3.Binary(pickle.dumps(result)),
                time.time(),
            ),
        )

    def append(self, receipt: Receipt) -> Receipt:
        if receipt.completed:
            return self.save_completion(receipt)
        # A receipt without result payloads is still durably stored.
        with self.conn:
            self._insert_receipt(receipt)
            self.conn.execute(
                """UPDATE run_meta
                   SET latest_sequence=?, acknowledged_pairs=?, status='active', updated_at=?
                   WHERE batch_id=?""",
                (receipt.sequence, receipt.acknowledged_pairs, time.time(), receipt.batch_id),
            )
        return receipt

    def latest(self, batch_id: str | None = None) -> Receipt | None:
        wanted = batch_id or self.batch_id
        if wanted is None:
            row = self.conn.execute("SELECT payload FROM receipts ORDER BY sequence DESC LIMIT 1").fetchone()
        else:
            row = self.conn.execute(
                "SELECT payload FROM receipts WHERE batch_id=? ORDER BY sequence DESC LIMIT 1",
                (wanted,),
            ).fetchone()
        receipt = Receipt.from_dict(json.loads(row["payload"])) if row else None
        if receipt is not None and batch_id is not None and receipt.batch_id != batch_id:
            raise BatchMismatchError("Latest warehouse receipt belongs to another batch")
        return receipt

    latest_receipt = latest

    def count_results(self, batch_id: str | None = None) -> int:
        wanted = batch_id or self.batch_id
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM results WHERE batch_id=?", (wanted,)
        ).fetchone()
        return int(row["n"])

    def iter_results(self, batch_id: str | None = None) -> Iterable[dict[str, Any]]:
        wanted = batch_id or self.batch_id
        cursor = self.conn.execute(
            "SELECT sample_id_json, permit_id_json, receipt_sequence, payload_json "
            "FROM results WHERE batch_id=? ORDER BY receipt_sequence, item_order",
            (wanted,),
        )
        for row in cursor:
            yield {
                "sample_id": json.loads(row["sample_id_json"]),
                "permit_id": json.loads(row["permit_id_json"]),
                "receipt_sequence": row["receipt_sequence"],
                "result": json.loads(row["payload_json"]),
            }

    def results(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        return list(self.iter_results(batch_id))

    def close(self) -> None:
        self.conn.close()


Warehouse = SQLiteWarehouse
WarehouseSink = SQLiteWarehouse
WarehouseClient = SQLiteWarehouse
Repository = SQLiteWarehouse
Storage = SQLiteWarehouse


class PaginatedApplyPair:
    """Bounded, restartable delivery of matched sample/permit pairs."""

    def __init__(
        self,
        samples: Any = None,
        permits: Any = None,
        other: Any = None,
        *,
        batch_id: str | None = None,
        batch: str | None = None,
        page_size: int | None = None,
        limit: int | None = None,
        max_pairs: int | None = None,
        pairs_per_page: int | None = None,
        page_limit: int | None = None,
        batch_size: int | None = None,
        index: ReconciliationIndex | None = None,
        index_path: str | Path | None = None,
        rebuild_index: bool = False,
        receipt_store: ReceiptStore | None = None,
        receipt_path: str | Path | None = None,
        receipt_file: str | Path | None = None,
        checkpoint_path: str | Path | None = None,
        checkpoint_file: str | Path | None = None,
        checkpoint: str | Path | None = None,
        warehouse: SQLiteWarehouse | None = None,
        warehouse_path: str | Path | None = None,
        storage_path: str | Path | None = None,
        database_path: str | Path | None = None,
        sink: Any | None = None,
        sample_id_col: str | None = None,
        permit_id_col: str | None = None,
        id_col: str | None = None,
        chrom_col: str | None = None,
        line_col: str | None = None,
        strand_col: str | None = None,
        direction_col: str | None = None,
        start_col: str | None = None,
        end_col: str | None = None,
        strandedness: str | bool | None = None,
        id_order: str = "natural",
        strict_order: bool = False,
        **options: Any,
    ) -> None:
        if permits is None:
            permits = other
        if index is None and (samples is None or permits is None):
            resume_index_path = index_path
            if resume_index_path is None:
                index_base_path = (
                    checkpoint_path or checkpoint or receipt_path or warehouse_path
                )
                if index_base_path is not None:
                    base = Path(index_base_path)
                    resume_index_path = base.with_name(base.name + ".index.sqlite3")
            if resume_index_path not in (None, ":memory:") and Path(resume_index_path).exists():
                index_path = resume_index_path
                index = ReconciliationIndex(resume_index_path)
            else:
                raise ValueError(
                    "Both samples and permits are required to build an index; "
                    "an existing index_path can be used alone for resume"
                )

        batch_id = batch_id or batch or options.get("batch_key")
        page_size = (
            page_size
            or limit
            or max_pairs
            or pairs_per_page
            or page_limit
            or batch_size
            or options.get("capacity")
            or 100
        )
        if page_size <= 0:
            raise ValueError("page_size must be positive")

        sample_id_col = sample_id_col or id_col or options.get("sample_id_column")
        permit_id_col = permit_id_col or id_col or options.get("permit_id_column")
        shared_id_column = options.get("id_column") or options.get("stable_id_col")
        if shared_id_column:
            sample_id_col = sample_id_col or shared_id_column
            permit_id_col = permit_id_col or shared_id_column
        chrom_col = chrom_col or line_col or options.get("line_id_col")
        strand_col = strand_col or direction_col
        checkpoint_path = (
            checkpoint_path
            or checkpoint_file
            or checkpoint
            or receipt_path
            or receipt_file
            or options.get("state_path")
        )
        receipt_path = receipt_path or receipt_file
        warehouse_path = warehouse_path or database_path
        state_dir = (
            options.get("checkpoint_dir")
            or options.get("checkpoint_directory")
            or options.get("state_dir")
            or options.get("state_directory")
            or options.get("storage_dir")
            or options.get("workdir")
            or options.get("directory")
        )
        if state_dir is not None:
            state_directory = Path(state_dir)
            state_directory.mkdir(parents=True, exist_ok=True)
            if (
                warehouse_path is None
                and checkpoint_path is None
                and receipt_path is None
                and storage_path is None
            ):
                warehouse_path = state_directory / "reconciliation.sqlite3"
            if index_path is None:
                index_path = state_directory / "pairs.index.sqlite3"
        if storage_path is None and options.get("db_path") is not None:
            storage_path = options["db_path"]

        effective_strandedness = strandedness
        if effective_strandedness is None:
            if index is not None:
                metadata = index.metadata() if hasattr(index, "metadata") else {}
                effective_strandedness = (
                    "same"
                    if metadata.get("sample_strand") and metadata.get("permit_strand")
                    else False
                )
            else:
                try:
                    sample_frame = _as_dataframe(samples)
                    permit_frame = _as_dataframe(permits)
                    sample_has_strand = _find_column(
                        sample_frame.columns, strand_col, STRAND_CANDIDATES, "strand"
                    )
                    permit_has_strand = _find_column(
                        permit_frame.columns, strand_col, STRAND_CANDIDATES, "strand"
                    )
                    effective_strandedness = (
                        "same" if sample_has_strand and permit_has_strand else False
                    )
                except Exception:
                    effective_strandedness = "same"

        self.page_size = int(page_size)
        self.strandedness = effective_strandedness
        self.strict_order = strict_order
        self.sink = sink

        checkpoint_path = checkpoint_path or receipt_path
        database_suffixes = {".db", ".sqlite", ".sqlite3"}
        if warehouse_path is None and storage_path is not None:
            if Path(storage_path).suffix.lower() in database_suffixes:
                warehouse_path = storage_path
            else:
                checkpoint_path = checkpoint_path or storage_path
        if (
            warehouse_path is None
            and checkpoint_path is not None
            and receipt_path is None
            and Path(checkpoint_path).suffix.lower() in database_suffixes
        ):
            warehouse_path = checkpoint_path

        if warehouse is not None:
            self.warehouse = warehouse
        elif warehouse_path is not None:
            self.warehouse = SQLiteWarehouse(warehouse_path)
        else:
            self.warehouse = None

        if receipt_store is not None:
            self.receipt_store = receipt_store
        elif checkpoint_path is not None and not (
            self.warehouse is not None
            and warehouse_path is not None
            and Path(checkpoint_path).expanduser().resolve()
            == Path(warehouse_path).expanduser().resolve()
        ):
            self.receipt_store = JsonReceiptStore(checkpoint_path)
        elif self.warehouse is not None:
            self.receipt_store = self.warehouse
        else:
            self.receipt_store = ReceiptStore()

        if index is not None:
            self.index = index
            self.index_path = getattr(index, "path", None)
        else:
            if index_path is None:
                if checkpoint_path is not None:
                    path = Path(checkpoint_path)
                    index_path = path.with_name(path.name + ".index.sqlite3")
                elif warehouse_path is not None:
                    warehouse_file = Path(warehouse_path)
                    index_path = warehouse_file.with_name(
                        warehouse_file.name + ".index.sqlite3"
                    )
                else:
                    index_path = ":memory:"
            self.index_path = str(index_path)
            self.index = ReconciliationIndex.create(
                samples,
                permits,
                index_path,
                batch_id=batch_id,
                sample_id_col=sample_id_col,
                permit_id_col=permit_id_col,
                chrom_col=chrom_col,
                strand_col=strand_col,
                start_col=start_col,
                end_col=end_col,
                id_order=id_order,
                rebuild=rebuild_index,
            )

        metadata = self.index.metadata()
        self.input_fingerprint = metadata.get("fingerprint")
        self.batch_id = batch_id or metadata.get("batch_id") or self.input_fingerprint[:16]

        if self.warehouse is not None:
            self.warehouse.begin_batch(self.batch_id, self.input_fingerprint)

        self.last_receipt: Receipt | None = self._load_last_receipt()
        self.completed = bool(self.last_receipt and self.last_receipt.completed)
        self.exhausted = self.completed
        self.pages_delivered = 0

    def _load_last_receipt(self) -> Receipt | None:
        receipt = store_latest_receipt(self.receipt_store, self.batch_id)
        if receipt is None:
            return None
        if receipt.batch_id != self.batch_id:
            raise BatchMismatchError("Receipt belongs to a different batch")
        if receipt.input_fingerprint and self.input_fingerprint:
            if receipt.input_fingerprint != self.input_fingerprint:
                raise InputMismatchError("Receipt does not match canonical input content")
        return receipt

    def refresh_receipt(self) -> Receipt | None:
        """Reload the durable high-water mark from the configured store."""
        receipt = self._load_last_receipt()
        if receipt is not None:
            self.last_receipt = receipt
            self.completed = receipt.completed
            self.exhausted = self.exhausted or receipt.completed
        return receipt

    _refresh_receipt = refresh_receipt

    @classmethod
    def resume(cls, *args: Any, **kwargs: Any) -> "PaginatedApplyPair":
        return cls(*args, **kwargs)

    def _cursor(self) -> tuple[int | None, int | None]:
        if self.last_receipt is None:
            return None, None
        return self.last_receipt.last_sample_rank, self.last_receipt.last_permit_rank

    def next_page(self, page_size: int | None = None) -> PairPage | None:
        self.refresh_receipt()
        if self.completed:
            return None
        size = int(page_size or self.page_size)
        if size <= 0:
            raise ValueError("page_size must be positive")

        sample_rank, permit_rank = self._cursor()
        pairs, exhausted = self.index.fetch_page(
            size,
            sample_rank,
            permit_rank,
            strandedness=self.strandedness,
        )
        self.exhausted = exhausted
        if not pairs:
            return None

        sequence = (self.last_receipt.sequence + 1) if self.last_receipt else 1
        acknowledged = self.last_receipt.acknowledged_pairs if self.last_receipt else 0
        page = PairPage(
            pairs,
            batch_id=self.batch_id,
            sequence=sequence,
            page_size=size,
            exhausted=exhausted,
            acknowledged_pairs=acknowledged,
            input_fingerprint=self.input_fingerprint,
            strict_order=self.strict_order,
        )
        self.pages_delivered += 1
        return page

    fetch_next_page = next_page
    get_next_page = next_page

    def acknowledge_page(
        self,
        page: PairPage,
        results: Any | None = None,
        *,
        sink: Any | None = None,
    ) -> Receipt:
        receipt = page.confirm(
            results,
            sink=sink or self.warehouse or self.sink,
            receipt_store=self.receipt_store,
        )
        self.last_receipt = receipt
        return receipt

    def __iter__(self) -> "PaginatedApplyPair":
        return self

    def __next__(self) -> PairPage:
        page = self.next_page()
        if page is None:
            raise StopIteration
        return page

    def deliver(
        self,
        callback: Callable[..., Any],
        *,
        page_size: int | None = None,
        sink: Any | None = None,
    ) -> Receipt:
        page = self.next_page(page_size)
        if page is None:
            return self.complete(sink=sink)
        result = _invoke_page_callback(callback, page)
        if page.receipt is None:
            page.confirm(result, sink=sink or self.warehouse or self.sink, receipt_store=self.receipt_store)
        self.last_receipt = page.receipt
        return page.receipt

    def complete(self, *, sink: Any | None = None) -> ReconciliationReport:
        self.refresh_receipt()
        if self.completed and self.last_receipt is not None:
            return ReconciliationReport(
                self.batch_id,
                True,
                self.last_receipt.acknowledged_pairs,
                self.last_receipt.sequence - 1 if self.last_receipt else 0,
                self.last_receipt,
                warehouse_path=getattr(self.warehouse, "path", None),
                receipt_path=getattr(self.receipt_store, "path", None),
                index_path=self.index_path if isinstance(self.index_path, str) else None,
            )

        sample_rank, permit_rank = self._cursor()
        remaining, _ = self.index.fetch_page(
            1, sample_rank, permit_rank, strandedness=self.strandedness
        )
        if remaining:
            raise PaginationError("Cannot complete batch before all expected pairs are acknowledged")

        sequence = (self.last_receipt.sequence + 1) if self.last_receipt else 1
        total = self.last_receipt.acknowledged_pairs if self.last_receipt else 0
        receipt = Receipt(
            batch_id=self.batch_id,
            sequence=sequence,
            status="completed",
            completed=True,
            page_size=self.page_size,
            keys=[],
            last_sample_id=self.last_receipt.last_sample_id if self.last_receipt else None,
            last_permit_id=self.last_receipt.last_permit_id if self.last_receipt else None,
            last_sample_rank=self.last_receipt.last_sample_rank if self.last_receipt else None,
            last_permit_rank=self.last_receipt.last_permit_rank if self.last_receipt else None,
            acknowledged_pairs=total,
            input_fingerprint=self.input_fingerprint,
        )

        target = sink or self.warehouse or self.sink
        if target is not None and hasattr(target, "save_completion"):
            target.save_completion(receipt)
        elif target is not None and hasattr(target, "save"):
            target.save([], receipt)
        elif target is not None and hasattr(target, "save_receipt"):
            append_receipt_to_store(target, receipt)
        else:
            append_receipt_to_store(self.receipt_store, receipt)

        self.last_receipt = receipt
        self.completed = True
        self.exhausted = True
        return ReconciliationReport(
            self.batch_id,
            True,
            total,
            sequence - 1,
            receipt,
            warehouse_path=getattr(self.warehouse, "path", None),
            receipt_path=getattr(self.receipt_store, "path", None),
            index_path=self.index_path if isinstance(self.index_path, str) else None,
        )

    def run(
        self,
        callback: Callable[..., Any] | None = None,
        *,
        page_callback: Callable[..., Any] | None = None,
        pair_callback: Callable[..., Any] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        sink: Any | None = None,
        auto_complete: bool = True,
    ) -> ReconciliationReport:
        """Deliver and acknowledge pages until completion or ``max_pages``."""
        if callback is None:
            callback = page_callback
        pages = 0
        while True:
            page = self.next_page(page_size)
            if page is None:
                if auto_complete:
                    return self.complete(sink=sink)
                return ReconciliationReport(
                    self.batch_id,
                    False,
                    self.last_receipt.acknowledged_pairs if self.last_receipt else 0,
                    pages,
                    self.last_receipt,
                    warehouse_path=getattr(self.warehouse, "path", None),
                    receipt_path=getattr(self.receipt_store, "path", None),
                    index_path=self.index_path if isinstance(self.index_path, str) else None,
                )

            if pair_callback is not None:
                results = [pair_callback(pair) for pair in page]
            elif callback is None:
                results = None
            else:
                # PairPage is a list subclass, so one-argument callbacks get
                # exactly the bounded pair list. Two-argument apply_pair-style
                # callbacks receive aligned sample/permit DataFrames.
                results = _invoke_page_callback(callback, page)

            if page.receipt is None:
                page.confirm(
                    results,
                    sink=sink or self.warehouse or self.sink,
                    receipt_store=self.receipt_store,
                )
            self.last_receipt = page.receipt
            pages += 1
            if max_pages is not None and pages >= max_pages:
                return ReconciliationReport(
                    self.batch_id,
                    False,
                    self.last_receipt.acknowledged_pairs if self.last_receipt else 0,
                    pages,
                    self.last_receipt,
                    warehouse_path=getattr(self.warehouse, "path", None),
                    receipt_path=getattr(self.receipt_store, "path", None),
                    index_path=self.index_path if isinstance(self.index_path, str) else None,
                )


PairPaginator = PaginatedApplyPair
PairCheckpointer = PaginatedApplyPair
Checkpointer = PaginatedApplyPair
ReconciliationCheckpointer = PaginatedApplyPair
PagedApplyPair = PaginatedApplyPair
Paginator = PaginatedApplyPair
PairPager = PaginatedApplyPair
ReconciliationRunner = PaginatedApplyPair
Reconciler = PaginatedApplyPair
ReconciliationSession = PaginatedApplyPair
Reconciliation = PaginatedApplyPair
PagedPairRunner = PaginatedApplyPair
Checkpoint = Receipt
Ack = Receipt
AckReceipt = Receipt
SourceRowPair = SourcePair
RowPair = SourcePair
PageDelivery = PairPage
PageResult = PairPage
PagedResult = PairPage
Delivery = PairPage
CheckpointManager = JsonReceiptStore
ReceiptManager = JsonReceiptStore
SignedReceipt = Receipt
ResultSink = SQLiteWarehouse


def paginate_pairs(pairs: Iterable[Any], page_size: int = 100) -> Iterable[list[Any]]:
    """Yield bounded lists from a lazy pair iterable without copying all pairs."""
    if page_size <= 0:
        raise ValueError("page_size must be positive")
    iterator = iter(pairs)
    while True:
        page = []
        for _ in range(page_size):
            try:
                page.append(next(iterator))
            except StopIteration:
                break
        if not page:
            return
        yield page


def save_receipt(path: str | Path, receipt: Receipt) -> Receipt:
    return JsonReceiptStore(path).append(receipt)


def load_receipt(path: str | Path, batch_id: str | None = None) -> Receipt | None:
    return JsonReceiptStore(path).latest(batch_id)


def paginated_apply_pair(
    samples: Any,
    permits: Any = None,
    f: Callable[..., Any] | None = None,
    *,
    callback: Callable[..., Any] | None = None,
    page_size: int = 100,
    **kwargs: Any,
) -> PaginatedApplyPair | ReconciliationReport:
    """Create a paginated runner, or run it when a callback is supplied."""
    runner = PaginatedApplyPair(samples, permits, page_size=page_size, **kwargs)
    effective_callback = callback or f
    if effective_callback is None:
        return runner
    return runner.run(effective_callback)


apply_pair_paginated = paginated_apply_pair
reconcile_pairs = paginated_apply_pair
paginate = paginated_apply_pair
create_paginator = PaginatedApplyPair
create_paginated_apply_pair = PaginatedApplyPair
create_reconciler = Reconciler


def apply_pair_pages(
    samples: Any,
    permits: Any = None,
    *,
    page_size: int = 100,
    **kwargs: Any,
) -> PaginatedApplyPair:
    return PaginatedApplyPair(samples, permits, page_size=page_size, **kwargs)
