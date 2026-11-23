"""Stateful correction mapping for revised source records.

When a source re-emits a record with the same business identity but a newer
revision, an append-only mapper cannot retract the effects of the earlier
version. This module tracks, per business identity, the highest source
revision already applied and emits differential events for a detail stream
and a daily-total stream:

* an old detail row is explicitly deleted before a replacement is inserted;
* daily aggregates receive signed deltas, never blind re-additions;
* records rejected by the retention filter still replace the tracked
  version, so their earlier effects are revoked without emitting new ones;
* stale or replayed revisions are ignored, so amounts already consumed by
  readers can never come back.

Revision ordering is decided solely by the source's revision number, never by
arrival time: a correction may arrive early or late, and replaying an old
revision after a newer one has been applied produces no events.
"""

from __future__ import annotations

__lazy_modules__ = {
    "copy",
    "dataclasses",
    "datetime",
    "decimal",
    "enum",
    "singer_sdk.exceptions",
    "singer_sdk.singerlib.encoding.simple",
    "singer_sdk.singerlib.json",
}

import copy
import dataclasses
import datetime
import decimal
import enum
import logging
import sys
import typing as t

from singer_sdk.exceptions import CorrectionMappingError, StaleRevisionError

if sys.version_info >= (3, 11):
    from typing import Self  # noqa: ICN003
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from singer_sdk.singerlib.encoding.simple import RecordMessage

__all__ = [
    "CorrectionEvent",
    "CorrectionMapper",
    "CorrectionOp",
    "CorrectionPolicy",
    "CorrectionTarget",
    "RecordVersion",
]

logger = logging.getLogger(__name__)

_ZERO = decimal.Decimal(0)
_SNAPSHOT_FORMAT_VERSION = 1

#: A normalized, JSON-safe numeric amount.
Amount: t.TypeAlias = decimal.Decimal

#: A business identity value: a JSON scalar or a tuple of JSON scalars.
BusinessIdValue: t.TypeAlias = t.Any

#: A business identity, scoped by source stream name.
BusinessKey: t.TypeAlias = tuple[str, BusinessIdValue]


class CorrectionOp(str, enum.Enum):
    """Kind of correction emitted for a detail row."""

    #: Insert or replace the detail row with the current record.
    UPSERT = "upsert"

    #: Remove the previously emitted detail row.
    DELETE = "delete"


class CorrectionTarget(str, enum.Enum):
    """Downstream table a correction event applies to."""

    #: The row-level detail table.
    DETAIL = "detail"

    #: The per-day aggregate table.
    DAILY = "daily"


@dataclasses.dataclass(frozen=True, slots=True)
class RecordVersion:
    """One tracked revision of a source record.

    Filtered revisions are tracked too: they carry no downstream effects, but
    they establish the revision baseline so that older revisions cannot be
    replayed afterwards.

    Attributes:
        revision: Source revision number.
        record: Deep-copied source record payload.
        day: Attribution day, or None if absent.
        amount: Normalized amount, or None if absent.
        retained: Whether the retention rules kept this revision.
    """

    revision: int
    record: dict[str, t.Any]
    day: str | None
    amount: decimal.Decimal | None
    retained: bool


@dataclasses.dataclass(frozen=True, slots=True)
class CorrectionEvent:
    """A differential effect of applying a new revision.

    Detail events carry an :class:`CorrectionOp` and the affected record.
    Daily events carry a signed ``amount_delta`` and the running
    ``daily_total`` for the day after the delta is applied.

    Attributes:
        target: Downstream table the event applies to.
        op: Detail operation (``UPSERT`` or ``DELETE``); None for daily.
        stream: Source stream name.
        business_id: Business identity of the revised record.
        revision: Source revision that produced this event.
        record: Detail record payload, or None for daily events.
        day: Attribution day for daily events, else None.
        amount_delta: Signed daily amount change, else None.
        daily_total: Running daily total after applying the delta, else None.
    """

    target: CorrectionTarget
    stream: str
    business_id: BusinessIdValue
    revision: int
    op: CorrectionOp | None = None
    record: dict[str, t.Any] | None = None
    day: str | None = None
    amount_delta: decimal.Decimal | None = None
    daily_total: decimal.Decimal | None = None

    @classmethod
    def detail(
        cls,
        *,
        op: CorrectionOp,
        stream: str,
        business_id: BusinessIdValue,
        revision: int,
        record: dict[str, t.Any],
    ) -> Self:
        """Build a detail-table correction event.

        Args:
            op: Whether the detail row is inserted or removed.
            stream: Source stream name.
            business_id: Business identity of the record.
            revision: Source revision that produced the event.
            record: The record payload to upsert or delete.

        Returns:
            A detail correction event.
        """
        return cls(
            target=CorrectionTarget.DETAIL,
            op=op,
            stream=stream,
            business_id=business_id,
            revision=revision,
            record=record,
        )

    @classmethod
    def daily(
        cls,
        *,
        stream: str,
        business_id: BusinessIdValue,
        revision: int,
        day: str,
        amount_delta: decimal.Decimal,
        daily_total: decimal.Decimal,
    ) -> Self:
        """Build a daily-total correction event.

        Args:
            stream: Source stream name.
            business_id: Business identity of the revised record.
            revision: Source revision that produced the event.
            day: Attribution day of the delta.
            amount_delta: Signed amount change for the day.
            daily_total: Running daily total after applying the delta.

        Returns:
            A daily correction event.
        """
        return cls(
            target=CorrectionTarget.DAILY,
            op=None,
            stream=stream,
            business_id=business_id,
            revision=revision,
            day=day,
            amount_delta=amount_delta,
            daily_total=daily_total,
        )


class CorrectionPolicy:
    """Extract correction coordinates from a source record.

    The default implementation reads plain record properties and applies an
    optional retention predicate. Subclass and override the hooks when the
    coordinates are computed rather than stored.

    Args:
        business_id_properties: Property name, or ordered property names for
            a composite identity, that uniquely identifies the business
            object across revisions.
        revision_property: Property carrying the monotonic source revision.
        day_property: Property carrying the amount attribution day.
        amount_property: Property carrying the amount.
        retain: Optional predicate returning False when the record must be
            filtered out. Defaults to retaining every record.
    """

    def __init__(
        self,
        *,
        business_id_properties: str | Sequence[str],
        revision_property: str,
        day_property: str,
        amount_property: str,
        retain: Callable[[dict[str, t.Any]], bool] | None = None,
    ) -> None:
        """Initialize the policy."""
        self._business_id_properties: tuple[str, ...] = (
            (business_id_properties,)
            if isinstance(business_id_properties, str)
            else tuple(business_id_properties)
        )
        self._revision_property = revision_property
        self._day_property = day_property
        self._amount_property = amount_property
        self._retain = retain

    @property
    def day_property(self) -> str:
        """Name of the record property holding the attribution day."""
        return self._day_property

    def get_business_id(
        self,
        record: dict[str, t.Any],
        stream: str,
    ) -> BusinessIdValue:
        """Return the stable business identity of a record.

        Args:
            record: Individual source record.
            stream: Source stream name.

        Returns:
            The property value, or a tuple of values for composite keys.

        Raises:
            CorrectionMappingError: If any identity property is missing.
        """
        missing = [name for name in self._business_id_properties if name not in record]
        if missing:
            msg = (
                f"Record from stream '{stream}' is missing business identity "
                f"propert{'y' if len(missing) == 1 else 'ies'}: "
                f"{', '.join(missing)}."
            )
            raise CorrectionMappingError(msg)

        if len(self._business_id_properties) == 1:
            return record[self._business_id_properties[0]]

        return tuple(record[name] for name in self._business_id_properties)

    def get_revision(self, record: dict[str, t.Any], stream: str) -> int:
        """Return the source revision of a record.

        Args:
            record: Individual source record.
            stream: Source stream name.

        Returns:
            The monotonic revision number.

        Raises:
            CorrectionMappingError: If the revision is missing or not an int.
        """
        value = record.get(self._revision_property)
        if isinstance(value, bool) or not isinstance(value, int):
            msg = (
                f"Record from stream '{stream}' must carry an integer revision in "
                f"property '{self._revision_property}', got: {value!r}."
            )
            raise CorrectionMappingError(msg)

        return value

    def get_day(self, record: dict[str, t.Any], stream: str) -> str | None:
        """Return the attribution day of a record.

        Args:
            record: Individual source record.
            stream: Source stream name.

        Returns:
            The day as a string, or None when the record carries no day.
        """
        _ = stream
        value = record.get(self._day_property)
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            return t.cast("str", value.isoformat())
        return str(value)

    def get_amount(self, record: dict[str, t.Any], stream: str) -> Amount | None:
        """Return the amount of a record, normalized to a Decimal.

        Args:
            record: Individual source record.
            stream: Source stream name.

        Returns:
            The amount as a Decimal, or None when absent.

        Raises:
            CorrectionMappingError: If the value is not numeric.
        """
        _ = stream
        value = record.get(self._amount_property)
        if value is None:
            return None
        if isinstance(value, decimal.Decimal):
            return value
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            msg = (
                f"Record from stream '{stream}' carries a non-numeric amount in "
                f"property '{self._amount_property}': {value!r}."
            )
            raise CorrectionMappingError(msg)

        try:
            return decimal.Decimal(str(value))
        except decimal.InvalidOperation as exc:
            msg = (
                f"Record from stream '{stream}' carries an invalid amount in "
                f"property '{self._amount_property}': {value!r}."
            )
            raise CorrectionMappingError(msg) from exc

    def should_retain(self, record: dict[str, t.Any], stream: str) -> bool:
        """Return whether a record survives the retention rules.

        Args:
            record: Individual source record.
            stream: Source stream name.

        Returns:
            True to keep the record, False to filter it out.
        """
        _ = stream
        if self._retain is None:
            return True
        return bool(self._retain(record))


class CorrectionMapper:
    """Reusable stateful mapper for revised records.

    The mapper keeps, per business identity, the highest revision already
    applied together with its coordinates (day, amount, retention flag) and
    maintains the running daily totals. Every call to :meth:`map_record`
    returns the differential :class:`CorrectionEvent` instances needed to
    bring both the detail and the daily tables from the previous revision to
    the new one.

    Args:
        policy: Policy extracting identity, revision, day, amount and
            retention from source records.
        detail_stream_suffix: Suffix appended to the source stream name to
            address the detail table. An empty string keeps the source name.
        daily_stream_suffix: Suffix appended to the source stream name to
            address the daily-total table.
        deleted_at_property: Record property set to mark a detail deletion.
        output_day_property: Name of the day property in daily records.
            Defaults to the policy's day property.
        amount_property: Name of the running-total property in daily records.
        delta_property: Name of the signed-delta property in daily records.
        on_stale_revision: ``"ignore"`` silently drops stale revisions
            (default); ``"raise"`` raises :class:`StaleRevisionError`.
    """

    def __init__(
        self,
        policy: CorrectionPolicy,
        *,
        detail_stream_suffix: str = "",
        daily_stream_suffix: str = "_daily",
        deleted_at_property: str = "_sdc_deleted_at",
        output_day_property: str | None = None,
        amount_property: str = "amount",
        delta_property: str = "amount_delta",
        on_stale_revision: t.Literal["ignore", "raise"] = "ignore",
    ) -> None:
        """Initialize the correction mapper."""
        if on_stale_revision not in {"ignore", "raise"}:
            msg = (
                "on_stale_revision must be 'ignore' or 'raise', got: "
                f"{on_stale_revision!r}."
            )
            raise CorrectionMappingError(msg)

        self.policy = policy
        self._detail_stream_suffix = detail_stream_suffix
        self._daily_stream_suffix = daily_stream_suffix
        self._deleted_at_property = deleted_at_property
        self._output_day_property = output_day_property or policy.day_property
        self._amount_property = amount_property
        self._delta_property = delta_property
        self._on_stale_revision = on_stale_revision

        self._versions: dict[BusinessKey, RecordVersion] = {}
        self._daily: dict[BusinessKey, Amount] = {}
        self._stale_revision_count = 0

    @property
    def stale_revision_count(self) -> int:
        """Number of stale or duplicate revisions dropped since init."""
        return self._stale_revision_count

    @property
    def daily_totals(self) -> dict[BusinessKey, Amount]:
        """Copy of the running daily totals keyed by ``(stream, day)``."""
        return dict(self._daily)

    def applied_revision(self, stream: str, business_id: BusinessIdValue) -> int | None:
        """Return the highest applied revision for a business identity.

        Args:
            stream: Source stream name.
            business_id: Business identity to look up.

        Returns:
            The applied revision, or None if the identity is unknown.
        """
        version = self._versions.get((stream, business_id))
        return None if version is None else version.revision

    def detail_stream_name(self, stream: str) -> str:
        """Return the detail-table stream name for a source stream.

        Args:
            stream: Source stream name.

        Returns:
            The detail stream name.
        """
        return f"{stream}{self._detail_stream_suffix}"

    def daily_stream_name(self, stream: str) -> str:
        """Return the daily-table stream name for a source stream.

        Args:
            stream: Source stream name.

        Returns:
            The daily stream name.
        """
        return f"{stream}{self._daily_stream_suffix}"

    def map_record_message(
        self,
        message_dict: dict[str, t.Any],
    ) -> list[CorrectionEvent]:
        """Map a Singer RECORD message dictionary to correction events.

        Args:
            message_dict: A RECORD message JSON dictionary.

        Returns:
            The differential correction events, possibly empty.
        """
        return self.map_record(message_dict["stream"], message_dict["record"])

    def map_record(
        self,
        stream: str,
        record: dict[str, t.Any],
    ) -> list[CorrectionEvent]:
        """Apply one source record and return the differential events.

        Revisions lower than the already-applied revision are ignored,
        regardless of arrival order, and equal revisions are idempotent.

        Args:
            stream: Source stream name.
            record: Individual source record carrying a revision.

        Returns:
            The correction events required to move detail and daily tables
            from the previous revision to this one.

        Raises:
            StaleRevisionError: If a stale revision arrives and the mapper is
                configured with ``on_stale_revision="raise"``.
            CorrectionMappingError: If the record lacks required coordinates.
        """
        business_id = self.policy.get_business_id(record, stream)
        revision = self.policy.get_revision(record, stream)
        key: BusinessKey = (stream, business_id)

        old = self._versions.get(key)
        if old is not None and revision <= old.revision:
            return self._handle_stale_revision(
                stream=stream,
                business_id=business_id,
                incoming=revision,
                applied=old.revision,
            )

        retained = self.policy.should_retain(record, stream)
        day = self.policy.get_day(record, stream)
        amount = self.policy.get_amount(record, stream)
        new_version = RecordVersion(
            revision=revision,
            record=copy.deepcopy(record),
            day=day,
            amount=amount,
            retained=retained,
        )

        events = self._detail_events(
            key=key,
            business_id=business_id,
            old=old,
            new=new_version,
        )
        events.extend(
            self._daily_events(
                key=key,
                business_id=business_id,
                old=old,
                new=new_version,
            ),
        )

        self._versions[key] = new_version
        return events

    def to_record_messages(
        self,
        events: Iterable[CorrectionEvent],
    ) -> t.Generator[RecordMessage, None, None]:
        """Convert correction events into Singer RECORD messages.

        Detail deletions emit the old payload flagged with the configured
        deletion marker property; daily records carry the signed delta and
        the resulting running total.

        Args:
            events: Events produced by :meth:`map_record`.

        Yields:
            Record messages addressed to the detail or daily streams.
        """
        from singer_sdk.singerlib.encoding.simple import RecordMessage  # noqa: PLC0415

        for event in events:
            if event.target is CorrectionTarget.DETAIL:
                yield RecordMessage(
                    stream=self.detail_stream_name(event.stream),
                    record=self._detail_record(event),
                )
            else:
                yield RecordMessage(
                    stream=self.daily_stream_name(event.stream),
                    record={
                        self._output_day_property: event.day,
                        self._amount_property: event.daily_total,
                        self._delta_property: event.amount_delta,
                    },
                )

    def snapshot(self) -> dict[str, t.Any]:
        """Serialize the mapper state for persistence.

        The result is JSON-safe and can be passed back to :meth:`restore` on
        a new mapper instance, for example through a Singer STATE message.

        Returns:
            A JSON-safe snapshot dictionary.
        """
        from singer_sdk.singerlib.json import (  # noqa: PLC0415
            deserialize_json,
            serialize_json,
        )

        entries = [
            {
                "stream": stream,
                "business_id": business_id,
                "revision": version.revision,
                "record": version.record,
                "day": version.day,
                "amount": version.amount,
                "retained": version.retained,
            }
            for (stream, business_id), version in self._versions.items()
        ]
        daily = [
            {"stream": stream, "day": day, "total": total}
            for (stream, day), total in self._daily.items()
        ]
        payload = {
            "format_version": _SNAPSHOT_FORMAT_VERSION,
            "entries": entries,
            "daily": daily,
        }
        return deserialize_json(serialize_json(payload))

    def restore(self, state: dict[str, t.Any]) -> None:
        """Restore mapper state previously produced by :meth:`snapshot`.

        Args:
            state: A snapshot dictionary.

        Raises:
            CorrectionMappingError: If the snapshot format is unsupported.
        """
        from singer_sdk.singerlib.json import (  # noqa: PLC0415
            deserialize_json,
            serialize_json,
        )

        payload = deserialize_json(serialize_json(state))
        if payload.get("format_version") != _SNAPSHOT_FORMAT_VERSION:
            msg = (
                "Unsupported correction snapshot format version: "
                f"{payload.get('format_version')!r}."
            )
            raise CorrectionMappingError(msg)

        self._versions.clear()
        self._daily.clear()
        self._stale_revision_count = 0

        for entry in payload["entries"]:
            business_id = entry["business_id"]
            if isinstance(business_id, list):
                business_id = tuple(business_id)
            key: BusinessKey = (entry["stream"], business_id)
            version = RecordVersion(
                revision=entry["revision"],
                record=entry["record"],
                day=entry["day"],
                amount=entry["amount"],
                retained=entry["retained"],
            )
            self._versions[key] = version

        for row in payload["daily"]:
            self._daily[(row["stream"], row["day"])] = row["total"]

    def _handle_stale_revision(
        self,
        *,
        stream: str,
        business_id: BusinessIdValue,
        incoming: int,
        applied: int,
    ) -> list[CorrectionEvent]:
        self._stale_revision_count += 1
        msg = (
            f"Dropping stale revision {incoming} for stream '{stream}' business id "
            f"{business_id!r}; revision {applied} is already applied."
        )
        if incoming == applied:
            logger.debug("Ignoring duplicate revision: %s", msg)
        elif self._on_stale_revision == "raise":
            raise StaleRevisionError(
                msg,
                stream=stream,
                business_id=business_id,
                incoming_revision=incoming,
                applied_revision=applied,
            )
        else:
            logger.info("Ignoring stale revision: %s", msg)

        return []

    def _detail_events(
        self,
        *,
        key: BusinessKey,
        business_id: BusinessIdValue,
        old: RecordVersion | None,
        new: RecordVersion,
    ) -> list[CorrectionEvent]:
        events: list[CorrectionEvent] = []
        if old is not None and old.retained:
            events.append(
                CorrectionEvent.detail(
                    op=CorrectionOp.DELETE,
                    stream=key[0],
                    business_id=business_id,
                    revision=new.revision,
                    record=old.record,
                ),
            )
        if new.retained:
            events.append(
                CorrectionEvent.detail(
                    op=CorrectionOp.UPSERT,
                    stream=key[0],
                    business_id=business_id,
                    revision=new.revision,
                    record=new.record,
                ),
            )
        return events

    def _daily_events(
        self,
        *,
        key: BusinessKey,
        business_id: BusinessIdValue,
        old: RecordVersion | None,
        new: RecordVersion,
    ) -> list[CorrectionEvent]:
        deltas: dict[str, Amount] = {}

        if (
            old is not None
            and old.retained
            and old.day is not None
            and old.amount is not None
        ):
            deltas[old.day] = deltas.get(old.day, _ZERO) - old.amount

        if new.retained and new.day is not None and new.amount is not None:
            deltas[new.day] = deltas.get(new.day, _ZERO) + new.amount

        events: list[CorrectionEvent] = []
        for day, delta in deltas.items():
            if delta == _ZERO:
                continue

            total = self._daily.get((key[0], day), _ZERO) + delta
            self._daily[(key[0], day)] = total
            events.append(
                CorrectionEvent.daily(
                    stream=key[0],
                    business_id=business_id,
                    revision=new.revision,
                    day=day,
                    amount_delta=delta,
                    daily_total=total,
                ),
            )
        return events

    def _detail_record(self, event: CorrectionEvent) -> dict[str, t.Any]:
        if event.record is None:
            msg = "Detail correction event is missing its record payload."
            raise CorrectionMappingError(msg)

        record = copy.deepcopy(event.record)
        if event.op is CorrectionOp.DELETE:
            record[self._deleted_at_property] = datetime.datetime.now(
                tz=datetime.timezone.utc,
            ).isoformat()
        return record
