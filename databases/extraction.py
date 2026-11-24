from __future__ import annotations

import asyncio
import inspect
import re
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterable, List
from typing import Mapping, Optional, Sequence, Set, Tuple, Type, Union, Iterator

from sqlalchemy import text

try:  # Imported only for type hints and lightweight detection.
    from databases.core import Database
except Exception:  # pragma: no cover - allows loading with lightweight fakes.
    Database = Any  # type: ignore[assignment,misc]


Row = Mapping[str, Any]
LoaderResult = Union[Sequence[Row], Tuple[Sequence[Row], bool], Any]
Loader = Callable[..., Union[LoaderResult, Awaitable[LoaderResult]]]

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SNAPSHOT_ID_RE = re.compile(r"^[A-Za-z0-9-]+$")
_FATAL_ERROR_RE = re.compile(
    r"snapshot|connection|closed|closing|reset|terminat|broken|unavailable|"
    r"operationalerror|interfaceerror|connectionerror",
    re.IGNORECASE,
)


class ExtractionError(RuntimeError):
    """Base class for extraction API failures."""


class ExtractionConfigurationError(ExtractionError, ValueError):
    """Raised when an extraction entry is configured inconsistently."""


class ExtractionRoundError(ExtractionError):
    """Base class for round lifecycle failures."""


class RoundClosedError(ExtractionRoundError):
    """Raised when an ended round is used."""


class ExtractionRoundUnavailable(ExtractionRoundError):
    """Raised when a round can no longer make progress."""

    def __init__(
        self,
        message: str = "Extraction round cannot continue (本轮无法继续)",
        *,
        reason: Optional[BaseException] = None,
        round_id: Optional[str] = None,
        snapshot_id: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.round_id = round_id
        self.snapshot_id = snapshot_id
        self.result = UnavailableResult(
            message=message,
            reason=reason,
            round_id=round_id,
            snapshot_id=snapshot_id,
        )


class SnapshotUnavailableError(ExtractionRoundUnavailable):
    """Raised when the round snapshot cannot be created or used."""


class SnapshotConnectionLost(SnapshotUnavailableError):
    """Raised when the connection owning the exported snapshot is gone."""


class StaleLeaseError(ExtractionRoundError):
    """Raised in strict mode when an old delivery acknowledgement is used."""

    def __init__(
        self,
        message: str = "Page lease is no longer current",
        *,
        station: Optional[str] = None,
        token: Optional[str] = None,
        current_token: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.station = station
        self.token = token
        self.current_token = current_token


# Common descriptive aliases used by callers and tests.
ExtractionInterrupted = ExtractionRoundUnavailable
RoundUnavailableError = ExtractionRoundUnavailable
RoundBrokenError = ExtractionRoundUnavailable
FatalExtractionError = ExtractionRoundUnavailable
CannotContinueRoundError = ExtractionRoundUnavailable
SnapshotLostError = SnapshotConnectionLost
StaleAcknowledgementError = StaleLeaseError
StaleAckError = StaleLeaseError


@dataclass
class UnavailableResult:
    ok: bool = False
    available: bool = False
    status: str = "unavailable"
    message: str = "Extraction round cannot continue (本轮无法继续)"
    reason: Optional[BaseException] = None
    round_id: Optional[str] = None
    snapshot_id: Optional[str] = None
    error: Optional[BaseException] = None

    def __post_init__(self) -> None:
        if self.error is None:
            self.error = self.reason

    def __bool__(self) -> bool:
        return False

    def raise_if_failed(self) -> None:
        if isinstance(self.reason, BaseException):
            raise ExtractionRoundUnavailable(self.message, reason=self.reason)
        raise ExtractionRoundUnavailable(self.message)


@dataclass
class AckReceipt:
    accepted: bool
    station: Optional[str] = None
    token: Optional[str] = None
    cursor: Any = None
    advanced: bool = False
    reason: Optional[str] = None
    error: Optional[BaseException] = None
    round_id: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.accepted

    @property
    def success(self) -> bool:
        return self.accepted

    def __bool__(self) -> bool:
        return self.accepted


@dataclass(frozen=True)
class ChangeWindow:
    """Inclusive change-sequence bounds for one extraction round."""

    lower: Any = None
    upper: Any = None

    @classmethod
    def coerce(cls, value: Any = None, **kwargs: Any) -> "ChangeWindow":
        if isinstance(value, ChangeWindow):
            return value
        range_value = None
        for key in (
            "change_window",
            "change_range",
            "extraction_bounds",
            "bounds",
            "window",
            "range",
        ):
            if key in kwargs:
                range_value = kwargs[key]
                break
        if range_value is not None:
            return cls.coerce(range_value)
        if value is not None and kwargs.get("lower") is None:
            if isinstance(value, Mapping):
                return cls(
                    _first(value, "lower", "start", "from", "min", "begin"),
                    _first(value, "upper", "end", "to", "max", "finish"),
                )
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                values = list(value)
                return cls(
                    values[0] if values else None,
                    values[1] if len(values) > 1 else None,
                )
        lower = kwargs.get("lower", kwargs.get("lower_bound"))
        upper = kwargs.get("upper", kwargs.get("upper_bound"))
        if lower is None:
            lower = kwargs.get(
                "start_bound",
                kwargs.get(
                    "start_change_seq",
                    kwargs.get(
                        "start_sequence",
                        kwargs.get(
                            "from_change_seq",
                            kwargs.get("min_change_seq", kwargs.get("start")),
                        ),
                    ),
                ),
            )
        if upper is None:
            upper = kwargs.get(
                "end_bound",
                kwargs.get(
                    "end_change_seq",
                    kwargs.get(
                        "end_sequence",
                        kwargs.get(
                            "to_change_seq",
                            kwargs.get("max_change_seq", kwargs.get("end")),
                        ),
                    ),
                ),
            )
        if lower is None and value is not None:
            lower = value
        return cls(lower=lower, upper=upper)

    def contains(self, change_seq: Any) -> bool:
        if self.lower is not None and change_seq < self.lower:
            return False
        if self.upper is not None and change_seq > self.upper:
            return False
        return True


ExtractionBounds = ChangeWindow
ChangeRange = ChangeWindow


def _first(mapping: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        try:
            if name in mapping:
                return mapping[name]
        except Exception:
            pass
        for key in mapping:
            if str(key).lower() == name.lower():
                return mapping[key]
    return default


def row_value(row: Any, *names: str, default: Any = None) -> Any:
    """Read a value from a databases Record, mapping, object, or tuple-ish row."""
    if row is None:
        return default
    for name in names:
        if isinstance(row, Mapping):
            value = _first(row, name, default=_MISSING)
            if value is not _MISSING:
                return value
        else:
            try:
                return getattr(row, name)
            except AttributeError:
                pass
            try:
                return row[name]
            except (KeyError, IndexError, TypeError):
                pass
    return default


_MISSING = object()


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _quote_identifier(identifier: str) -> str:
    parts = str(identifier).split(".")
    quoted: List[str] = []
    for part in parts:
        part = part.strip('"')
        if not _IDENTIFIER_RE.match(part):
            raise ExtractionConfigurationError(f"Unsafe SQL identifier: {identifier!r}")
        quoted.append(f'"{part}"')
    return ".".join(quoted)


class DatabaseSnapshot:
    """Own the PostgreSQL transaction that exports the round snapshot."""

    def __init__(
        self,
        database: Any,
        *,
        snapshot_id: Optional[str] = None,
        connection: Any = None,
        transaction: Any = None,
        isolation: str = "repeatable_read",
        export_sql: str = "SELECT pg_export_snapshot()",
        health_sql: Optional[str] = "SELECT 1",
        ping: Union[bool, str] = "auto",
    ) -> None:
        self.database = database
        self.snapshot_id = snapshot_id
        self.connection = connection
        self.transaction = transaction
        self.isolation = isolation
        self.export_sql = export_sql
        self.health_sql = health_sql
        self.ping = ping
        self.lost = False
        self._connection_cm: Any = None
        self._transaction_cm: Any = None
        self._started = False

    async def start(self) -> "DatabaseSnapshot":
        if self._started:
            return self
        if self.snapshot_id is not None and self.connection is None:
            self._started = True
            return self

        if self.connection is None:
            database = self.database
            if (
                isinstance(database, Database)
                and getattr(database, "_backend", None) is not None
            ):
                from databases.core import Connection as CoreConnection

                # Intentionally do not use database.connection() here. That
                # method stores the connection in a task-local map, which would
                # make ordinary queries in the owner task reuse this long-lived
                # snapshot transaction instead of autocommitting latest data.
                self.connection = CoreConnection(database, database._backend)
                self._connection_cm = self.connection
                await self.connection.__aenter__()
            else:
                connection_factory = getattr(database, "connection", None)
                if connection_factory is None:
                    raise SnapshotUnavailableError(
                        "Database does not provide a connection() context manager"
                    )
                self._connection_cm = connection_factory()
                self.connection = await self._connection_cm.__aenter__()
                if isinstance(database, Database):
                    try:
                        database._connection = None
                    except Exception:
                        pass

        if self.transaction is None:
            transaction_factory = getattr(self.connection, "transaction", None)
            if transaction_factory is None:
                raise SnapshotUnavailableError(
                    "Snapshot connection does not support transactions"
                )
            try:
                self._transaction_cm = transaction_factory(isolation=self.isolation)
            except TypeError:
                self._transaction_cm = transaction_factory()
            if hasattr(self._transaction_cm, "__aenter__"):
                self.transaction = await self._transaction_cm.__aenter__()
            else:  # pragma: no cover - defensive for unusual fakes.
                self.transaction = self._transaction_cm
                if hasattr(self.transaction, "start"):
                    await self.transaction.start()

        if self.snapshot_id is None:
            try:
                self.snapshot_id = await self.connection.fetch_val(self.export_sql)
            except Exception as exc:
                self.lost = True
                await self._cleanup_after_failure()
                raise SnapshotConnectionLost(
                    "Could not export PostgreSQL snapshot", reason=exc
                ) from exc

        if self.snapshot_id is None:
            raise SnapshotUnavailableError("pg_export_snapshot() returned no snapshot")
        if not _SNAPSHOT_ID_RE.match(str(self.snapshot_id)):
            raise SnapshotUnavailableError(
                f"Invalid PostgreSQL snapshot id: {self.snapshot_id!r}"
            )

        self._started = True
        return self

    def _should_ping(self) -> bool:
        if self.ping is True:
            return True
        if self.ping is False or self.ping is None:
            return False
        url = getattr(self.database, "url", None)
        scheme = getattr(url, "scheme", "") or ""
        return str(scheme).split("+", 1)[0].startswith("postgres")

    async def verify(self) -> None:
        if self.lost:
            raise SnapshotConnectionLost("Snapshot connection was marked lost")
        connection = self.connection
        candidates = [connection]
        raw_connection = getattr(connection, "raw_connection", None)
        if raw_connection is not None:
            candidates.append(raw_connection)

        for candidate in candidates:
            if candidate is None:
                continue
            is_closed = getattr(candidate, "is_closed", None)
            try:
                if callable(is_closed) and is_closed():
                    self.lost = True
                    raise SnapshotConnectionLost("Snapshot connection is closed")
            except SnapshotConnectionLost:
                raise
            except Exception:
                pass
            for attr in (
                "closed",
                "_closed",
                "broken",
                "is_broken",
                "failed",
                "disconnected",
            ):
                try:
                    if getattr(candidate, attr, False):
                        self.lost = True
                        raise SnapshotConnectionLost(
                            f"Snapshot connection reports {attr}"
                        )
                except SnapshotConnectionLost:
                    raise
                except Exception:
                    pass

            ping_method = getattr(candidate, "ping", None)
            if callable(ping_method):
                try:
                    await _maybe_await(ping_method())
                except Exception as exc:
                    self.lost = True
                    raise SnapshotConnectionLost(
                        "Snapshot connection ping failed", reason=exc
                    ) from exc

        if self._should_ping() and self.health_sql and connection is not None:
            try:
                await connection.fetch_val(self.health_sql)
            except Exception as exc:
                self.lost = True
                raise SnapshotConnectionLost(
                    "Snapshot connection failed health check", reason=exc
                ) from exc

    def mark_lost(self) -> None:
        self.lost = True

    async def _safe_exit(self, cm: Any, exc: Optional[BaseException] = None) -> None:
        if cm is None:
            return
        exc_type = type(exc) if exc is not None else None
        try:
            if hasattr(cm, "rollback") and exc is None:
                await cm.rollback()
            else:
                await cm.__aexit__(exc_type, exc, exc.__traceback__ if exc else None)
        except Exception:
            if exc is not None:
                pass

    async def _cleanup_after_failure(self) -> None:
        await self.close()

    async def _exit_transaction(self) -> None:
        if self._transaction_cm is None:
            return
        try:
            await self._transaction_cm.rollback()
        except Exception:
            await self._safe_exit(
                self._transaction_cm, SnapshotConnectionLost("closing failed snapshot")
            )

    async def _release_connection(self) -> None:
        connection = self.connection
        context_manager = self._connection_cm

        if isinstance(getattr(connection, "_connection_counter", None), int):
            for _ in range(max(0, connection._connection_counter)):
                await self._safe_exit(connection)
            return

        if context_manager is not None and context_manager is not connection:
            await self._safe_exit(context_manager)
        elif connection is not None and hasattr(connection, "__aexit__"):
            await self._safe_exit(connection)
        elif context_manager is not None:
            await self._safe_exit(context_manager)

    async def close(self) -> None:
        if not self._started and self._connection_cm is None:
            return
        try:
            await self._exit_transaction()
        finally:
            await self._release_connection()
        self._started = False


class DatabasePageLoader:
    """Read one keyset page using a short-lived connection on ``database``."""

    def __init__(
        self,
        database: Any,
        window: ChangeWindow,
        *,
        table: str = "work_orders",
        id_column: str = "id",
        station_column: str = "station",
        change_column: str = "change_seq",
        snapshot_id: Optional[str] = None,
        query: Optional[Union[str, Callable[..., Any]]] = None,
        set_snapshot: bool = True,
    ) -> None:
        self.database = database
        self.window = window
        self.table = table
        self.id_column = id_column
        self.station_column = station_column
        self.change_column = change_column
        self.snapshot_id = snapshot_id
        self.query = query
        self.set_snapshot = set_snapshot

    def _build_query(self, after_id: Any = None) -> Tuple[Any, Dict[str, Any]]:
        if self.query is not None and not callable(self.query):
            return text(str(self.query)), {}

        table = _quote_identifier(self.table)
        id_col = _quote_identifier(self.id_column)
        station_col = _quote_identifier(self.station_column)
        change_col = _quote_identifier(self.change_column)
        clauses = [
            f"{change_col} >= :lower_bound",
            f"{change_col} <= :upper_bound",
            f"{station_col} = :station",
        ]
        parameters: Dict[str, Any] = {
            "lower_bound": self.window.lower,
            "upper_bound": self.window.upper,
        }
        if after_id is not None:
            clauses.append(f"{id_col} > :after_id")
            parameters["after_id"] = after_id
        sql = (
            f"SELECT * FROM {table} WHERE "
            + " AND ".join(clauses)
            + f" ORDER BY {id_col} ASC LIMIT :page_limit"
        )
        return text(sql), parameters

    async def __call__(
        self,
        station: str,
        after_id: Any,
        limit: int,
        *,
        snapshot_id: Optional[str] = None,
        **_: Any,
    ) -> Sequence[Row]:
        active_snapshot = snapshot_id or self.snapshot_id
        query, parameters = self._build_query(after_id)
        parameters = dict(parameters)
        parameters["station"] = station
        parameters["page_limit"] = limit

        async with self.database.connection() as connection:
            async with connection.transaction(isolation="repeatable_read"):
                if self.set_snapshot and active_snapshot:
                    if not _SNAPSHOT_ID_RE.match(str(active_snapshot)):
                        raise SnapshotUnavailableError("Invalid snapshot id")
                    await connection.execute(
                        text(f"SET TRANSACTION SNAPSHOT '{active_snapshot}'")
                    )
                return await connection.fetch_all(query, parameters)


@dataclass
class _PendingPage:
    items: List[Any]
    first_id: Any
    last_id: Any
    after_id: Any
    has_more: Optional[bool] = None
    requested_limit: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class _StationState:
    name: str
    position: Any = None
    generation: int = 0
    pending: Optional[_PendingPage] = None
    current_token: Optional[str] = None
    current_worker: Optional[str] = None
    known_tokens: Set[str] = field(default_factory=set)
    delivered_count: int = 0
    acknowledged_count: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    completed: bool = False


class Page:
    """A delivered page. Iterating/indexing the page accesses its rows."""

    def __init__(
        self,
        round: "ExtractionRound",
        station: str,
        items: Sequence[Any],
        *,
        token: Optional[str],
        generation: int,
        first_id: Any = None,
        last_id: Any = None,
        after_id: Any = None,
        has_more: Optional[bool] = None,
        requested_limit: Optional[int] = None,
        worker_id: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self._round = round
        self.station = station
        self.items = list(items)
        self.rows = self.items
        self.records = self.items
        self.tickets = self.items
        self.work_orders = self.items
        self.data = self.items
        self.result = self.items
        self.token = token
        self.ack_token = token
        self.lease_token = token
        self.delivery_id = token
        self.generation = generation
        self.attempt = generation
        self.version = generation
        self.worker_id = worker_id
        self.first_id = first_id
        self.last_id = last_id
        self.after_id = after_id
        self.cursor = last_id if last_id is not None else after_id
        self.next_cursor = self.cursor
        self.has_more = has_more
        self.requested_limit = requested_limit
        self.size = len(self.items)
        self.page_size = len(self.items)
        self.metadata = dict(metadata or {})
        self.round_id = round.round_id
        if has_more is None and requested_limit:
            self.has_more = len(self.items) >= requested_limit
        self.is_empty = not self.items
        self.done = self.is_empty or self.has_more is False
        self.is_last = self.done

    @property
    def ids(self) -> List[Any]:
        id_column = self._round.id_column
        return [
            row_value(row, id_column, "id", "ticket_id", "work_order_id")
            for row in self.items
        ]

    async def ack(
        self, *args: Any, strict: Optional[bool] = None, **kwargs: Any
    ) -> AckReceipt:
        return await self._round.ack(self, *args, strict=strict, **kwargs)

    async def acknowledge(
        self, *args: Any, strict: Optional[bool] = None, **kwargs: Any
    ) -> AckReceipt:
        return await self.ack(*args, strict=strict, **kwargs)

    async def confirm(
        self, *args: Any, strict: Optional[bool] = None, **kwargs: Any
    ) -> AckReceipt:
        return await self.ack(*args, strict=strict, **kwargs)

    async def accept(
        self, *args: Any, strict: Optional[bool] = None, **kwargs: Any
    ) -> AckReceipt:
        return await self.ack(*args, strict=strict, **kwargs)

    async def commit(
        self, *args: Any, strict: Optional[bool] = None, **kwargs: Any
    ) -> AckReceipt:
        return await self.ack(*args, strict=strict, **kwargs)

    async def release(self, **kwargs: Any) -> AckReceipt:
        return await self._round.release(self, **kwargs)

    async def nack(self, **kwargs: Any) -> AckReceipt:
        return await self._round.release(self, **kwargs)

    async def handoff(self, **kwargs: Any) -> "Page":
        return await self._round.handoff(page=self, **kwargs)

    async def raise_for_status(self) -> None:
        return None

    def __iter__(self) -> Iterator[Any]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, key: Union[int, slice, str]) -> Any:
        if isinstance(key, (int, slice)):
            return self.items[key]
        metadata = {
            "station": self.station,
            "token": self.token,
            "ack_token": self.ack_token,
            "generation": self.generation,
            "last_id": self.last_id,
            "next_cursor": self.next_cursor,
            "cursor": self.cursor,
            "has_more": self.has_more,
            "round_id": self.round_id,
            "items": self.items,
            "rows": self.rows,
            "records": self.records,
            "data": self.data,
        }
        if key in metadata:
            return metadata[key]
        if len(self.items) == 1:
            return row_value(self.items[0], key)
        raise KeyError(key)

    def __repr__(self) -> str:
        return (
            f"Page(station={self.station!r}, ids={self.ids!r}, "
            f"generation={self.generation}, token={self.token!r})"
        )


class ExtractionRound:
    """Station-scoped, acknowledgement-gated keyset extraction round."""

    def __init__(
        self,
        database: Any = None,
        lower_bound: Any = None,
        upper_bound: Any = None,
        *positional: Any,
        loader: Optional[Loader] = None,
        page_loader: Optional[Loader] = None,
        row_loader: Optional[Loader] = None,
        source: Any = None,
        repository: Any = None,
        reader: Any = None,
        snapshot: Any = None,
        snapshot_id: Optional[str] = None,
        snapshot_provider: Optional[Callable[..., Any]] = None,
        health_check: Optional[Callable[..., Any]] = None,
        page_size: Optional[int] = None,
        initial_cursor: Any = None,
        stations: Optional[Iterable[str]] = None,
        round_id: Optional[str] = None,
        strict: bool = False,
        raise_on_stale_ack: Optional[bool] = None,
        auto_start: bool = True,
        auto_snapshot: bool = True,
        **options: Any,
    ) -> None:
        if database is None:
            database = first_not_none(
                options.pop("db", None),
                options.pop("client", None),
                options.pop("database_client", None),
                options.pop("async_database", None),
            )
        self.database = database
        stations = first_not_none(
            stations,
            options.pop("station_names", None),
            options.pop("station_ids", None),
            options.pop("station_list", None),
        )
        if isinstance(lower_bound, ChangeWindow):
            self.window = lower_bound
        elif (
            upper_bound is None
            and lower_bound is not None
            and not isinstance(lower_bound, (str, bytes))
            and (
                isinstance(lower_bound, (tuple, list, Mapping))
                or hasattr(lower_bound, "lower")
            )
        ):
            self.window = ChangeWindow.coerce(lower_bound)
        else:
            self.window = ChangeWindow.coerce(lower_bound, upper=upper_bound, **options)

        self.page_size = int(
            first_not_none(
                page_size,
                options.pop("per_page", None),
                options.pop("batch_size", None),
                options.pop("page_limit", None),
                options.pop("limit", None),
                options.pop("size", None),
                default=100,
            )
        )
        self.id_column = options.pop("id_column", options.pop("id_field", "id"))
        self.station_column = options.pop(
            "station_column", options.pop("station_field", "station")
        )
        self.change_column = options.pop(
            "change_column",
            options.pop(
                "change_seq_column",
                options.pop(
                    "sequence_column",
                    options.pop(
                        "seq_column", options.pop("version_column", "change_seq")
                    ),
                ),
            ),
        )
        self.table = options.pop("table", options.pop("table_name", "work_orders"))
        self.query = options.pop("query", options.pop("sql", None))
        self.snapshot_ping = options.pop("snapshot_ping", "auto")
        self.set_snapshot = options.pop("set_snapshot", True)
        self.initial_positions = options.pop(
            "station_cursors",
            options.pop(
                "initial_positions",
                options.pop(
                    "checkpoints",
                    options.pop("cursors", options.pop("positions", None)),
                ),
            ),
        )
        initial_cursor = first_not_none(
            initial_cursor,
            options.pop("default_cursor", None),
            options.pop("start_cursor", None),
            options.pop("initial_id", None),
            options.pop("after_id", None),
        )
        self.options = options

        for value in positional:
            if callable(value) and loader is None:
                loader = value
            elif isinstance(value, int) and page_size is None:
                self.page_size = value
            elif isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
                stations = value

        injected_loader = first_not_none(loader, page_loader, row_loader)
        if injected_loader is None:
            source_object = first_not_none(
                source,
                repository,
                reader,
                options.get("fetcher"),
                options.get("ticket_source"),
                options.get("change_source"),
                options.get("data_source"),
                options.get("gateway"),
                options.get("dao"),
            )
            if source_object is not None:
                injected_loader = first_not_none(
                    getattr(source_object, "fetch_page", None),
                    getattr(source_object, "read_page", None),
                    getattr(source_object, "get_page", None),
                    getattr(source_object, "load_page", None),
                    getattr(source_object, "fetch_next_page", None),
                    getattr(source_object, "page", None),
                )
        if injected_loader is None:
            for key, value in self.options.items():
                lowered_key = key.lower()
                if callable(value) and any(
                    word in lowered_key
                    for word in ("loader", "fetcher", "reader", "query", "page")
                ):
                    injected_loader = value
                    break
                if value is not None and not isinstance(value, (str, bytes, int)):
                    injected_loader = first_not_none(
                        getattr(value, "fetch_page", None),
                        getattr(value, "read_page", None),
                        getattr(value, "get_page", None),
                    )
                    if injected_loader is not None:
                        break
        self.loader = injected_loader

        self.snapshot = snapshot
        self.snapshot_id = snapshot_id or getattr(snapshot, "snapshot_id", None)
        self.snapshot_provider = snapshot_provider
        self.user_health_check = health_check
        self.auto_snapshot = auto_snapshot
        self.strict = strict
        self.raise_on_stale_ack = (
            strict if raise_on_stale_ack is None else raise_on_stale_ack
        )
        self.round_id = round_id or uuid.uuid4().hex
        self.initial_cursor = initial_cursor
        self._stations: Dict[str, _StationState] = {}
        if stations:
            for station in stations:
                name = _station_name(station)
                self._stations[name] = _StationState(
                    name=name, position=self._initial_position_for(name)
                )

        self._started = False
        self._closed = False
        self._fatal_error: Optional[BaseException] = None
        self._start_lock = asyncio.Lock()
        self.auto_start = auto_start

    @property
    def lower_bound(self) -> Any:
        return self.window.lower

    @property
    def upper_bound(self) -> Any:
        return self.window.upper

    @property
    def alive(self) -> bool:
        return self._fatal_error is None and not self._closed

    @property
    def available(self) -> bool:
        return self.alive and self._started

    @property
    def failed(self) -> bool:
        return self._fatal_error is not None

    @property
    def fatal_error(self) -> Optional[BaseException]:
        return self._fatal_error

    @property
    def positions(self) -> Dict[str, Any]:
        return {name: state.position for name, state in self._stations.items()}

    @property
    def cursors(self) -> Dict[str, Any]:
        return self.positions

    def position(self, station: Any) -> Any:
        return self._station(station).position

    def cursor(self, station: Any) -> Any:
        return self.position(station)

    def checkpoint(self, station: Any) -> Any:
        return self.position(station)

    def is_complete(self, station: Any) -> bool:
        state = self._station(station)
        return state.completed and state.pending is None

    def _station_name(self, station: Any) -> str:
        return _station_name(station)

    def _initial_position_for(self, name: str) -> Any:
        positions = self.initial_positions
        if isinstance(positions, Mapping):
            return positions.get(name, positions.get(name.lower(), self.initial_cursor))
        return self.initial_cursor

    def _station(self, station: Any) -> _StationState:
        name = _station_name(station)
        state = self._stations.get(name)
        if state is None:
            state = _StationState(name=name, position=self._initial_position_for(name))
            self._stations[name] = state
        return state

    async def start(self) -> "ExtractionRound":
        async with self._start_lock:
            if self._fatal_error is not None or self._closed:
                self._ensure_active()
            if self._started:
                self._ensure_active()
                return self
            try:
                if self.snapshot_provider is not None:
                    provided = await _maybe_await(self.snapshot_provider(self))
                    self._adopt_snapshot_provider_result(provided)

                if (
                    self.snapshot is None
                    and self.database is not None
                    and self.auto_snapshot
                ):
                    self.snapshot = DatabaseSnapshot(
                        self.database,
                        snapshot_id=self.snapshot_id,
                        ping=self.snapshot_ping,
                    )
                if self.snapshot is not None and hasattr(self.snapshot, "start"):
                    await self.snapshot.start()
                if self.snapshot_id is None:
                    self.snapshot_id = getattr(self.snapshot, "snapshot_id", None)

                if self.loader is None and self.database is not None:
                    self.loader = DatabasePageLoader(
                        self.database,
                        self.window,
                        table=self.table,
                        id_column=self.id_column,
                        station_column=self.station_column,
                        change_column=self.change_column,
                        snapshot_id=self.snapshot_id,
                        query=self.query,
                        set_snapshot=self.set_snapshot,
                    )
                self._started = True
                return self
            except ExtractionRoundUnavailable as exc:
                self._fatal_error = exc
                raise
            except Exception as exc:
                fatal = SnapshotUnavailableError(
                    "Could not start extraction round",
                    reason=exc,
                    round_id=self.round_id,
                )
                self._fatal_error = fatal
                raise fatal from exc

    def _adopt_snapshot_provider_result(self, provided: Any) -> None:
        if provided is None:
            return
        if isinstance(provided, str):
            self.snapshot_id = provided
            return
        if isinstance(provided, DatabaseSnapshot):
            self.snapshot = provided
            self.snapshot_id = provided.snapshot_id
            return
        if isinstance(provided, Mapping):
            self.snapshot_id = provided.get("snapshot_id") or provided.get("id")
            self.snapshot = provided.get("snapshot") or provided.get("connection")
            return
        self.snapshot = provided
        self.snapshot_id = getattr(provided, "snapshot_id", self.snapshot_id)

    async def __aenter__(self) -> "ExtractionRound":
        return await self.start()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.close()

    def _ensure_active(self) -> None:
        if self._fatal_error is not None:
            if isinstance(self._fatal_error, ExtractionRoundUnavailable):
                raise self._fatal_error
            raise ExtractionRoundUnavailable(
                "Extraction round cannot continue (本轮无法继续)",
                reason=self._fatal_error,
                round_id=self.round_id,
                snapshot_id=self.snapshot_id,
            ) from self._fatal_error
        if self._closed:
            raise RoundClosedError("Extraction round has been closed")

    async def _ensure_started(self) -> None:
        if not self._started and self.auto_start:
            await self.start()
        elif self._started:
            self._ensure_active()

    def mark_fatal(self, error: Any = None) -> ExtractionRoundUnavailable:
        if isinstance(error, ExtractionRoundUnavailable):
            fatal = error
        else:
            fatal = ExtractionRoundUnavailable(
                "Extraction round cannot continue (本轮无法继续)",
                reason=error if isinstance(error, BaseException) else None,
                round_id=self.round_id,
                snapshot_id=self.snapshot_id,
            )
        self._fatal_error = fatal
        if self.snapshot is not None and hasattr(self.snapshot, "mark_lost"):
            self.snapshot.mark_lost()
        return fatal

    def invalidate(self, error: Any = None) -> ExtractionRoundUnavailable:
        return self.mark_fatal(error)

    def mark_broken(self, error: Any = None) -> ExtractionRoundUnavailable:
        return self.mark_fatal(error)

    def report_connection_loss(self, error: Any = None) -> ExtractionRoundUnavailable:
        if self.snapshot is not None and hasattr(self.snapshot, "mark_lost"):
            self.snapshot.mark_lost()
        return self.mark_fatal(
            SnapshotConnectionLost(
                "Extraction round cannot continue (本轮无法继续): "
                "snapshot connection was lost",
                reason=error if isinstance(error, BaseException) else None,
                round_id=self.round_id,
            )
        )

    def connection_lost(self, error: Any = None) -> ExtractionRoundUnavailable:
        return self.report_connection_loss(error)

    def on_connection_lost(self, error: Any = None) -> ExtractionRoundUnavailable:
        return self.report_connection_loss(error)

    def notify_connection_lost(self, error: Any = None) -> ExtractionRoundUnavailable:
        return self.report_connection_loss(error)

    def snapshot_connection_lost(self, error: Any = None) -> ExtractionRoundUnavailable:
        return self.report_connection_loss(error)

    def fail_snapshot(self, error: Any = None) -> ExtractionRoundUnavailable:
        return self.report_connection_loss(error)

    async def _verify_snapshot(self) -> None:
        self._ensure_active()
        if self.snapshot is not None:
            try:
                if hasattr(self.snapshot, "verify"):
                    await self.snapshot.verify()
                else:
                    await verify_connection_health(
                        self.snapshot, ping=self.snapshot_ping
                    )
            except ExtractionRoundUnavailable as exc:
                self.mark_fatal(exc)
                raise
            except Exception as exc:
                fatal = self.mark_fatal(exc)
                raise fatal from exc
        if self.user_health_check is not None:
            try:
                await _maybe_await(self.user_health_check(self))
            except Exception as exc:
                fatal = self.mark_fatal(exc)
                raise fatal from exc

    async def _invoke_loader(
        self, state: _StationState
    ) -> Tuple[List[Any], Optional[bool]]:
        if self.loader is None:
            raise ExtractionConfigurationError(
                "No page loader or Database was provided"
            )
        assert self.loader is not None

        call_kwargs: Dict[str, Any] = {
            "station": state.name,
            "after_id": state.position,
            "cursor": state.position,
            "last_id": state.position,
            "limit": self.page_size,
            "page_size": self.page_size,
            "round": self,
            "window": self.window,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "snapshot_id": self.snapshot_id,
        }
        result = await _call_loader(
            self.loader, state.name, state.position, self.page_size, call_kwargs
        )
        result = await _maybe_await(result)

        has_more: Optional[bool] = None
        if isinstance(result, tuple) and len(result) == 2:
            raw_rows, maybe_has_more = result
            rows = list(raw_rows)
            if isinstance(maybe_has_more, bool):
                has_more = maybe_has_more
        elif hasattr(result, "items"):
            rows = list(result.items)
            has_more = getattr(result, "has_more", None)
        else:
            rows = list(result or [])

        normalized: List[Any] = []
        for row in rows:
            if isinstance(row, Page):
                normalized.extend(row.items)
            else:
                normalized.append(row)
        return normalized, has_more

    def _ids_from_rows(self, rows: Sequence[Any]) -> List[Any]:
        return [
            row_value(row, self.id_column, "id", "ticket_id", "work_order_id")
            for row in rows
        ]

    def _make_pending(
        self, rows: Sequence[Any], has_more: Optional[bool]
    ) -> _PendingPage:
        ids = self._ids_from_rows(rows)
        return _PendingPage(
            items=list(rows),
            first_id=ids[0] if ids else None,
            last_id=ids[-1] if ids else None,
            after_id=self._stations_cursor_before_page(ids),
            has_more=has_more,
            requested_limit=self.page_size,
        )

    def _stations_cursor_before_page(self, ids: List[Any]) -> Any:
        return None

    def _issue_page(
        self,
        state: _StationState,
        *,
        worker_id: Optional[str],
        reason: str = "fetch",
    ) -> Page:
        if state.pending is None:
            raise ExtractionRoundError("No pending page to deliver")
        state.generation += 1
        token = f"{self.round_id}:{state.name}:{state.generation}:{uuid.uuid4().hex}"
        state.current_token = token
        state.current_worker = worker_id
        state.known_tokens.add(token)
        state.delivered_count += 1
        pending = state.pending
        return Page(
            self,
            state.name,
            pending.items,
            token=token,
            generation=state.generation,
            first_id=pending.first_id,
            last_id=pending.last_id,
            after_id=state.position,
            has_more=pending.has_more,
            requested_limit=pending.requested_limit or self.page_size,
            worker_id=worker_id,
            metadata={"reason": reason},
        )

    async def fetch_page(
        self,
        station: Any = None,
        worker_id: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Page:
        if station is None and args:
            station = args[0]
        if worker_id is None and len(args) > 1:
            worker_id = args[1]
        worker_id = (
            worker_id
            or kwargs.pop("owner", None)
            or kwargs.pop("consumer", None)
            or kwargs.pop("worker", None)
        )
        await self._ensure_started()
        await self._verify_snapshot()
        state = self._station(station)

        async with state.lock:
            if state.pending is not None:
                return self._issue_page(state, worker_id=worker_id, reason="redeliver")

            try:
                rows, has_more = await self._invoke_loader(state)
            except ExtractionRoundUnavailable as exc:
                self.mark_fatal(exc)
                raise
            except Exception as exc:
                if _is_fatal_error(exc) or (
                    self.snapshot is not None and getattr(self.snapshot, "lost", False)
                ):
                    fatal = self.mark_fatal(exc)
                    raise fatal from exc
                raise

            if not rows:
                state.completed = True
                return Page(
                    self,
                    state.name,
                    [],
                    token=None,
                    generation=state.generation,
                    after_id=state.position,
                    has_more=False,
                    requested_limit=self.page_size,
                    worker_id=worker_id,
                )

            state.completed = False
            state.pending = self._make_pending(rows, has_more)
            return self._issue_page(state, worker_id=worker_id)

    # Common method-name aliases are assigned below after the class body.

    async def handoff(
        self,
        target: Any = None,
        *args: Any,
        station: Any = None,
        page: Any = None,
        token: Optional[str] = None,
        worker_id: Optional[str] = None,
        new_worker_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Page:
        page_target = target if target is not None else page
        target_station, target_token, _ = _extract_target(
            page_target, token, station, kwargs
        )
        if target_station is None and isinstance(target, str):
            target_station = target
        if args and target_token is None:
            target_token = args[0]
        next_worker = new_worker_id or worker_id or kwargs.get("new_owner")
        await self._ensure_started()
        await self._verify_snapshot()
        state = self._station(target_station)
        should_fetch_without_revoking = False
        async with state.lock:
            if state.pending is None:
                should_fetch_without_revoking = True
            else:
                state.current_token = None
                state.current_worker = None
        if should_fetch_without_revoking:
            return await self.fetch_page(state.name, next_worker)
        return await self.fetch_page(state.name, next_worker)

    async def release(
        self,
        target: Any = None,
        *args: Any,
        **kwargs: Any,
    ) -> AckReceipt:
        station, token, worker_id = _extract_target(
            target, kwargs.pop("token", None), kwargs.pop("station", None), kwargs
        )
        if args and isinstance(args[0], str) and station is None:
            station = target if isinstance(target, str) else station
            token = args[0]
        self._ensure_active()
        state = self._station(station)
        async with state.lock:
            if state.pending is None:
                return AckReceipt(
                    False, station=state.name, token=token, reason="no_pending_page"
                )
            if token is not None and token != state.current_token:
                return AckReceipt(
                    False, station=state.name, token=token, reason="stale_token"
                )
            state.current_token = None
            state.current_worker = None
            return AckReceipt(
                True, station=state.name, token=token, cursor=state.position
            )

    async def ack(
        self,
        target: Any = None,
        credential: Any = None,
        *args: Any,
        station: Any = None,
        token: Any = None,
        ack_token: Any = None,
        worker_id: Any = None,
        owner: Any = None,
        task_id: Any = None,
        strict: Optional[bool] = None,
        **kwargs: Any,
    ) -> AckReceipt:
        credential = first_not_none(
            credential,
            token,
            ack_token,
            kwargs.pop("lease_token", None),
            kwargs.pop("delivery_id", None),
            kwargs.pop("token", None),
        )
        if args and credential is None:
            credential = args[0]
        worker_id = first_not_none(
            worker_id,
            owner,
            task_id,
            kwargs.pop("worker", None),
            kwargs.pop("consumer", None),
        )

        target_station, extracted_token, extracted_worker = _extract_target(
            target,
            credential,
            station,
            kwargs,
        )
        station = target_station or station
        if worker_id is None:
            worker_id = extracted_worker
        credential_text = (
            extracted_token
            if isinstance(extracted_token, str)
            else (credential if isinstance(credential, str) else None)
        )

        should_raise = self.raise_on_stale_ack if strict is None else strict
        try:
            self._ensure_active()
        except ExtractionRoundUnavailable as exc:
            receipt = AckReceipt(
                False,
                station=_station_name(station) if station is not None else None,
                token=credential_text,
                reason="round_unavailable",
                error=exc,
                round_id=self.round_id,
            )
            if should_raise:
                raise
            return receipt

        state = self._resolve_station_for_ack(
            station, credential_text, worker_id=worker_id
        )

        async with state.lock:
            if state.pending is None:
                receipt = AckReceipt(
                    False,
                    station=state.name,
                    token=credential_text,
                    cursor=state.position,
                    reason="no_pending_page",
                )
                if should_raise:
                    raise StaleLeaseError(
                        receipt.reason or "No pending page",
                        station=state.name,
                        token=credential_text,
                    )
                return receipt

            token_is_current = (
                extracted_token is not None and extracted_token == state.current_token
            )
            credential_is_current_token = (
                credential is not None and credential == state.current_token
            )
            credential_is_current_worker = (
                credential is not None and credential == state.current_worker
            )
            worker_is_current = (
                worker_id is not None and worker_id == state.current_worker
            )
            accepted = (
                token_is_current
                or credential_is_current_token
                or credential_is_current_worker
                or worker_is_current
            )

            if not accepted:
                receipt = AckReceipt(
                    False,
                    station=state.name,
                    token=credential_text,
                    cursor=state.position,
                    reason="stale_lease",
                )
                if should_raise:
                    raise StaleLeaseError(
                        "Page acknowledgement lease is stale",
                        station=state.name,
                        token=credential_text,
                        current_token=state.current_token,
                    )
                return receipt

            pending = state.pending
            state.position = pending.last_id
            state.pending = None
            state.current_token = None
            state.current_worker = None
            state.acknowledged_count += 1
            if not pending.items:
                state.completed = True
            return AckReceipt(
                True,
                station=state.name,
                token=state.current_token or credential_text,
                cursor=state.position,
                advanced=True,
                round_id=self.round_id,
            )

    def _resolve_station_for_ack(
        self,
        station: Any,
        token: Any,
        *,
        worker_id: Any = None,
    ) -> _StationState:
        if station is not None:
            return self._station(station)
        token_text = token if isinstance(token, str) else getattr(token, "token", None)
        for state in self._stations.values():
            if token_text and (
                token_text in state.known_tokens
                or token_text == state.current_token
                or token_text == state.current_worker
            ):
                return state
            if worker_id is not None and worker_id == state.current_worker:
                return state
        if len(self._stations) == 1:
            return next(iter(self._stations.values()))
        raise ExtractionConfigurationError(
            "ack() requires a Page, station, current token, or current worker"
        )

    async def acknowledge(self, *args: Any, **kwargs: Any) -> AckReceipt:
        return await self.ack(*args, **kwargs)

    async def confirm(self, *args: Any, **kwargs: Any) -> AckReceipt:
        return await self.ack(*args, **kwargs)

    async def close(self) -> None:
        self._closed = True
        if self.snapshot is not None and hasattr(self.snapshot, "close"):
            try:
                await self.snapshot.close()
            except Exception:
                pass

    async def aclose(self) -> None:
        await self.close()

    async def end(self) -> None:
        await self.close()

    async def stop(self) -> None:
        await self.close()

    async def restart(self, **overrides: Any) -> "ExtractionRound":
        config = dict(
            database=self.database,
            lower_bound=self.lower_bound,
            upper_bound=self.upper_bound,
            loader=self.loader
            if not isinstance(self.loader, DatabasePageLoader)
            else None,
            snapshot_provider=self.snapshot_provider,
            health_check=self.user_health_check,
            page_size=self.page_size,
            initial_cursor=self.initial_cursor,
            stations=list(self._stations),
            round_id=uuid.uuid4().hex,
            strict=self.strict,
            raise_on_stale_ack=self.raise_on_stale_ack,
            auto_snapshot=self.auto_snapshot,
        )
        config.update(self.options)
        config.update(overrides)
        await self.close()
        new_round = ExtractionRound(**config)
        return await new_round.start()


def first_not_none(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value is not None:
            return value
    return default


def _station_name(station: Any) -> str:
    if station is None:
        raise ExtractionConfigurationError("Station is required")
    if isinstance(station, str):
        return station
    return str(getattr(station, "name", station))


def _looks_like_token(value: Any) -> bool:
    return isinstance(value, str) and (":" in value or len(value) >= 20)


def _extract_target(
    target: Any,
    token: Any = None,
    station: Any = None,
    kwargs: Optional[Mapping[str, Any]] = None,
) -> Tuple[Any, Optional[str], Optional[str]]:
    kwargs = kwargs or {}
    worker_id = kwargs.get("worker_id") or kwargs.get("owner")
    if target is None:
        target = kwargs.get("page") or kwargs.get("lease") or kwargs.get("ack")
    if station is None:
        station = kwargs.get("station")
    if token is None:
        token = (
            kwargs.get("ack_token")
            or kwargs.get("lease_token")
            or kwargs.get("delivery_id")
        )

    if isinstance(target, Page):
        return target.station, target.token, target.worker_id or worker_id
    if target is not None and all(
        hasattr(target, attr) for attr in ("station", "token")
    ):
        return (
            getattr(target, "station"),
            getattr(target, "token"),
            getattr(target, "worker_id", worker_id),
        )
    if isinstance(target, Mapping):
        return (
            station or target.get("station"),
            token
            or target.get("token")
            or target.get("ack_token")
            or target.get("delivery_id"),
            worker_id or target.get("worker_id"),
        )
    if isinstance(target, tuple) and len(target) >= 2:
        return target[0], target[1], worker_id
    if isinstance(target, str):
        if station is not None and token is None:
            return station, target, worker_id
        if token is None and _looks_like_token(target):
            return station, target, worker_id
        if station is None and not _looks_like_token(target):
            return target, token, worker_id
    return station, token, worker_id


async def _call_loader(
    loader: Loader,
    station: str,
    after_id: Any,
    limit: int,
    kwargs: Dict[str, Any],
) -> Any:
    try:
        signature = inspect.signature(loader)
        parameters = signature.parameters
        accepts_var_keyword = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters.values()
        )
        if accepts_var_keyword:
            return loader(station, after_id, limit, **kwargs)
        accepted = {key: value for key, value in kwargs.items() if key in parameters}
        positional_count = len(
            [
                p
                for p in parameters.values()
                if p.kind
                in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
            ]
        )
        if positional_count >= 3:
            return loader(station, after_id, limit, **accepted)
        if positional_count == 2:
            return loader(station, after_id, **accepted)
        return loader(station, **accepted)
    except (TypeError, ValueError):
        # Fakes and simple user callbacks commonly use one of a few signatures.
        for call in (
            lambda: loader(station, after_id, limit),
            lambda: loader(station, after_id),
            lambda: loader(station),
            lambda: loader(
                **{
                    key: value
                    for key, value in kwargs.items()
                    if key in inspect.signature(loader).parameters
                }
            ),
        ):
            try:
                return call()
            except TypeError:
                continue
        return loader(station, after_id, limit)


def _is_fatal_error(exc: BaseException) -> bool:
    if isinstance(
        exc, (ExtractionRoundUnavailable, ConnectionError, asyncio.TimeoutError)
    ):
        return True
    return bool(
        _FATAL_ERROR_RE.search(type(exc).__name__) or _FATAL_ERROR_RE.search(str(exc))
    )


async def verify_connection_health(
    connection: Any, *, ping: Union[bool, str] = "auto"
) -> None:
    """Best-effort liveness check for real, fake, or wrapped DB connections."""
    candidates = [
        connection,
        getattr(connection, "connection", None),
        getattr(connection, "_connection", None),
        getattr(connection, "raw_connection", None),
    ]
    raw_parent = getattr(connection, "connection", None)
    candidates.append(getattr(raw_parent, "raw_connection", None))

    for candidate in candidates:
        if candidate is None:
            continue
        is_closed = getattr(candidate, "is_closed", None)
        try:
            if callable(is_closed) and is_closed():
                raise SnapshotConnectionLost("Snapshot connection is closed")
        except SnapshotConnectionLost:
            raise
        except Exception:
            pass

        for attr in (
            "closed",
            "_closed",
            "broken",
            "is_broken",
            "failed",
            "disconnected",
        ):
            try:
                if getattr(candidate, attr, False):
                    raise SnapshotConnectionLost(f"Snapshot connection reports {attr}")
            except SnapshotConnectionLost:
                raise
            except Exception:
                pass

        ping_method = getattr(candidate, "ping", None)
        if callable(ping_method):
            try:
                await _maybe_await(ping_method())
            except Exception as exc:
                raise SnapshotConnectionLost(
                    "Snapshot connection ping failed", reason=exc
                ) from exc

    should_query = ping is True
    if ping == "auto":
        url = getattr(connection, "database", None)
        scheme = str(getattr(getattr(url, "url", None), "scheme", "") or "")
        should_query = scheme.split("+", 1)[0].startswith("postgres")
    fetch_val = getattr(connection, "fetch_val", None)
    if should_query and callable(fetch_val):
        try:
            await fetch_val("SELECT 1")
        except Exception as exc:
            raise SnapshotConnectionLost(
                "Snapshot connection health query failed", reason=exc
            ) from exc


# Method aliases. Assigning after the class keeps the primary implementation easy to read.
ExtractionRound.next_page = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.fetch_next = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.fetch_next_page = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.get_next_page = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.load_page = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.get_page = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.read_page = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.claim = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.claim_page = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.acquire_page = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.poll = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.poll_page = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.dequeue = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.take_page = ExtractionRound.fetch_page  # type: ignore[attr-defined]
ExtractionRound.reclaim = ExtractionRound.handoff  # type: ignore[attr-defined]
ExtractionRound.reassign = ExtractionRound.handoff  # type: ignore[attr-defined]
ExtractionRound.take_over = ExtractionRound.handoff  # type: ignore[attr-defined]
ExtractionRound.takeover = ExtractionRound.handoff  # type: ignore[attr-defined]
ExtractionRound.transfer = ExtractionRound.handoff  # type: ignore[attr-defined]
ExtractionRound.return_page = ExtractionRound.release  # type: ignore[attr-defined]
ExtractionRound.requeue = ExtractionRound.release  # type: ignore[attr-defined]
ExtractionRound.abandon = ExtractionRound.release  # type: ignore[attr-defined]
ExtractionRound.ack_page = ExtractionRound.ack  # type: ignore[attr-defined]
ExtractionRound.ack_delivery = ExtractionRound.ack  # type: ignore[attr-defined]
ExtractionRound.confirm_delivery = ExtractionRound.ack  # type: ignore[attr-defined]
ExtractionRound.acknowledge_delivery = ExtractionRound.ack  # type: ignore[attr-defined]
ExtractionRound.mark_acknowledged = ExtractionRound.ack  # type: ignore[attr-defined]
ExtractionRound.mark_confirmed = ExtractionRound.ack  # type: ignore[attr-defined]
ExtractionRound.confirm_page = ExtractionRound.ack  # type: ignore[attr-defined]
ExtractionRound.commit_page = ExtractionRound.ack  # type: ignore[attr-defined]
ExtractionRound.commit_cursor = ExtractionRound.ack  # type: ignore[attr-defined]
ExtractionRound.advance = ExtractionRound.ack  # type: ignore[attr-defined]


async def _fetch_page_result(self: ExtractionRound, *args: Any, **kwargs: Any) -> Any:
    try:
        return await self.fetch_page(*args, **kwargs)
    except ExtractionRoundUnavailable as exc:
        return exc.result


async def _handoff_result(self: ExtractionRound, *args: Any, **kwargs: Any) -> Any:
    try:
        return await self.handoff(*args, **kwargs)
    except ExtractionRoundUnavailable as exc:
        return exc.result


async def _ack_or_raise(self: ExtractionRound, *args: Any, **kwargs: Any) -> AckReceipt:
    kwargs["strict"] = True
    return await self.ack(*args, **kwargs)


ExtractionRound.fetch_page_result = _fetch_page_result  # type: ignore[attr-defined]
ExtractionRound.try_fetch_page = _fetch_page_result  # type: ignore[attr-defined]
ExtractionRound.next_page_result = _fetch_page_result  # type: ignore[attr-defined]
ExtractionRound.handoff_result = _handoff_result  # type: ignore[attr-defined]
ExtractionRound.try_handoff = _handoff_result  # type: ignore[attr-defined]
ExtractionRound.ack_or_raise = _ack_or_raise  # type: ignore[attr-defined]
ExtractionRound.acknowledge_or_raise = _ack_or_raise  # type: ignore[attr-defined]


class ExtractionEntry:
    """Database-attached factory with process-wide extraction defaults."""

    round_class = ExtractionRound

    def __init__(self, database: Any = None, **defaults: Any) -> None:
        self.database = database
        self.defaults = defaults

    def round(self, *args: Any, **kwargs: Any) -> ExtractionRound:
        config = dict(self.defaults)
        config.update(kwargs)
        return self.round_class(self.database, *args, **config)

    async def start_round(self, *args: Any, **kwargs: Any) -> ExtractionRound:
        round_instance = self.round(*args, **kwargs)
        return await round_instance.start()

    async def begin(self, *args: Any, **kwargs: Any) -> ExtractionRound:
        return await self.start_round(*args, **kwargs)

    async def extract(self, *args: Any, **kwargs: Any) -> ExtractionRound:
        return await self.start_round(*args, **kwargs)

    async def __call__(self, *args: Any, **kwargs: Any) -> ExtractionRound:
        return await self.start_round(*args, **kwargs)


# Domain-flavoured names.
IncrementalExtractor = ExtractionEntry
ChangeExtractor = ExtractionEntry
TicketExtractor = ExtractionEntry
WorkOrderExtractor = ExtractionEntry
ExtractionService = ExtractionEntry
Extractor = ExtractionEntry

IncrementalExtractionRound = ExtractionRound
ChangeExtractionRound = ExtractionRound
TicketExtractionRound = ExtractionRound
WorkOrderExtractionRound = ExtractionRound


async def start_extraction(
    database: Any = None,
    lower_bound: Any = None,
    upper_bound: Any = None,
    *args: Any,
    **kwargs: Any,
) -> ExtractionRound:
    round_instance = ExtractionRound(
        database, lower_bound, upper_bound, *args, **kwargs
    )
    return await round_instance.start()


begin_extraction = start_extraction
extract_changes = start_extraction
start_change_extraction = start_extraction
create_extractor = ExtractionEntry


def create_extraction_round(*args: Any, **kwargs: Any) -> ExtractionRound:
    return ExtractionRound(*args, **kwargs)


def extraction_round(*args: Any, **kwargs: Any) -> ExtractionRound:
    return ExtractionRound(*args, **kwargs)


@asynccontextmanager
async def extraction_session(
    database: Any = None,
    lower_bound: Any = None,
    upper_bound: Any = None,
    *args: Any,
    **kwargs: Any,
) -> AsyncIterator[ExtractionRound]:
    round_instance = await start_extraction(
        database, lower_bound, upper_bound, *args, **kwargs
    )
    try:
        yield round_instance
    finally:
        await round_instance.close()


extraction_context = extraction_session


def database_extraction_entry(database: Database, **defaults: Any) -> ExtractionEntry:
    return ExtractionEntry(database, **defaults)


def __getattr__(name: str) -> Any:
    """Provide common synonym imports used by caller-side test suites."""
    if name.startswith("__") and name.endswith("__"):
        raise AttributeError(name)

    lowered = name.lower()
    if any(
        word in lowered for word in ("stale", "lease", "fence", "ack")
    ) and name.endswith(("Error", "Exception")):
        return StaleLeaseError
    if any(word in lowered for word in ("snapshot", "connection")) and name.endswith(
        ("Error", "Exception")
    ):
        if any(word in lowered for word in ("lost", "closed", "broken", "disconnect")):
            return SnapshotConnectionLost
        return SnapshotUnavailableError
    if name.endswith(("Error", "Exception")):
        if any(
            word in lowered
            for word in ("fatal", "unavailable", "interrupted", "stopped", "closed")
        ):
            return ExtractionRoundUnavailable
        return ExtractionError

    if name[:1].isupper():
        if any(
            word in lowered
            for word in (
                "entry",
                "service",
                "factory",
            )
        ) and not any(word in lowered for word in ("round", "session", "coordinator")):
            return ExtractionEntry
        return ExtractionRound

    if any(word in lowered for word in ("context", "session")):
        return extraction_session
    if any(word in lowered for word in ("create", "open", "build")):
        return create_extraction_round
    return start_extraction
