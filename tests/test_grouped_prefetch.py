from datetime import datetime, timezone

import pytest
import pytest_asyncio

from tortoise import fields
from tortoise.context import tortoise_test_context
from tortoise.exceptions import OperationalError
from tortoise.functions import Max
from tortoise.models import Model
from tortoise.query_utils import GroupedPrefetch, Prefetch


class Station(Model):
    code = fields.CharField(max_length=16, pk=True)
    name = fields.CharField(max_length=64)

    class Meta:
        table = "station"


class WorkOrder(Model):
    number = fields.CharField(max_length=16, pk=True)

    class Meta:
        table = "work_order"


class Return(Model):
    id = fields.IntField(pk=True)
    station: fields.ForeignKeyRelation[Station] = fields.ForeignKeyField(
        "models.Station", related_name="returns"
    )
    work_order: fields.ForeignKeyRelation[WorkOrder] = fields.ForeignKeyField(
        "models.WorkOrder", related_name="returns"
    )
    event_time = fields.DatetimeField()
    status = fields.CharField(max_length=32, default="pending_confirm")

    class Meta:
        table = "return_order"


class Alarm(Model):
    id = fields.IntField(pk=True)
    station: fields.ForeignKeyRelation[Station] = fields.ForeignKeyField(
        "models.Station", related_name="alarms"
    )
    work_order: fields.ForeignKeyRelation[WorkOrder] = fields.ForeignKeyField(
        "models.WorkOrder", related_name="alarms"
    )
    status = fields.CharField(max_length=32, default="pending_review")

    class Meta:
        table = "alarm"


class Evidence(Model):
    id = fields.IntField(pk=True)
    alarm: fields.ForeignKeyRelation[Alarm] = fields.ForeignKeyField(
        "models.Alarm", related_name="evidences"
    )
    label = fields.CharField(max_length=16)
    signed_at = fields.DatetimeField(null=True)
    uploaded_at = fields.DatetimeField()

    class Meta:
        table = "evidence"


def _dt(minute: int) -> datetime:
    return datetime(2026, 11, 26, 10, minute, tzinfo=timezone.utc)


@pytest_asyncio.fixture
async def station_db():
    async with tortoise_test_context(modules=[__name__]) as ctx:
        yield ctx


async def _seed() -> None:
    east = await Station.create(code="east", name="East station")
    west = await Station.create(code="west", name="West station")
    for number in ("E50", "E49", "E48", "E99"):
        await WorkOrder.create(number=number)

    alarm_e50_east = await Alarm.create(station=east, work_order_id="E50")
    alarm_e50_west = await Alarm.create(station=west, work_order_id="E50")
    alarm_e49_east = await Alarm.create(station=east, work_order_id="E49")
    alarm_e49_west = await Alarm.create(station=west, work_order_id="E49")
    await Alarm.create(station=east, work_order_id="E48")
    await Alarm.create(station=east, work_order_id="E99")

    evidence_rows = [
        # E50 east: A (10:08), B (10:07) signed; C uploaded 10:12 still unsigned
        (alarm_e50_east.pk, "A", _dt(8), _dt(8)),
        (alarm_e50_east.pk, "B", _dt(7), _dt(7)),
        (alarm_e50_east.pk, "C", None, _dt(12)),
        # E50 west: D (10:04) signed; E unsigned
        (alarm_e50_west.pk, "D", _dt(4), _dt(4)),
        (alarm_e50_west.pk, "E", None, _dt(13)),
        # E49 one signed photo per station
        (alarm_e49_east.pk, "F", _dt(5), _dt(5)),
        (alarm_e49_west.pk, "G", _dt(10), _dt(10)),
    ]
    # E48 signed east; E99 uploaded but never signed
    alarm_e48 = await Alarm.get(station_id="east", work_order_id="E48")
    alarm_e99 = await Alarm.get(station_id="east", work_order_id="E99")
    evidence_rows.extend(
        [
            (alarm_e48.pk, "H", _dt(5), _dt(5)),
            (alarm_e99.pk, "I", None, _dt(15)),
        ]
    )
    for alarm_id, label, signed_at, uploaded_at in evidence_rows:
        await Evidence.create(
            alarm_id=alarm_id,
            label=label,
            signed_at=signed_at,
            uploaded_at=uploaded_at,
        )

    # Pending-confirm return R9 is dispatched only to the east station.
    await Return.create(station=east, work_order_id="E99", event_time=_dt(20))


def _alarm_queryset(station_code: str):
    return _alarm_queryset_any_station().filter(station_id=station_code)


def _alarm_queryset_any_station():
    return (
        Alarm.annotate(latest_signed=Max("evidences__signed_at"))
        .filter(evidences__signed_at__isnull=False)
        .order_by("-latest_signed", "-work_order_id")
        .prefetch_related(Prefetch("evidences", queryset=Evidence.all().order_by("id")))
    )


def _returns_queryset(station_code: str):
    return Return.filter(station_id=station_code, status="pending_confirm").order_by(
        "-event_time"
    )


def _station_prefetches(station_code: str):
    return [
        GroupedPrefetch(
            relation="alarms",
            queryset=_alarm_queryset(station_code),
            to_attr="alarm_page",
            limit=2,
        ),
        GroupedPrefetch(
            relation="returns",
            queryset=_returns_queryset(station_code),
            to_attr="return_page",
            limit=1,
        ),
    ]


class QueryCounter:
    def __init__(self, client):
        self.client = client
        self.count = 0
        self._original = client.execute_query

    async def __call__(self, query, values=None):
        self.count += 1
        if values is None:
            return await self._original(query)
        return await self._original(query, values)

    def __enter__(self):
        self.client.execute_query = self
        return self

    def __exit__(self, *exc):
        self.client.execute_query = self._original


@pytest.mark.asyncio
async def test_station_homepage_groups_alarms_and_returns(station_db):
    await _seed()
    client = Station._meta.db

    with QueryCounter(client) as counter:
        east = await Station.filter(code="east").prefetch_related(
            *_station_prefetches("east")
        )
    east = east[0]

    assert [alarm.work_order_id for alarm in east.alarm_page] == ["E50", "E49"]
    assert [alarm.latest_signed for alarm in east.alarm_page] == [_dt(8), _dt(5)]
    assert [ret.work_order_id for ret in east.return_page] == ["E99"]

    with QueryCounter(client) as counter:
        west = await Station.filter(code="west").prefetch_related(
            *_station_prefetches("west")
        )
    west = west[0]

    assert [alarm.work_order_id for alarm in west.alarm_page] == ["E49", "E50"]
    assert [alarm.latest_signed for alarm in west.alarm_page] == [_dt(10), _dt(4)]
    assert west.return_page == []

    # One station select, one grouped alarm query, one grouped return query and one nested
    # evidence query (the west return window is empty, so it still costs no extra query).
    assert counter.count == 4


@pytest.mark.asyncio
async def test_expansion_shows_only_this_station_evidence(station_db):
    await _seed()
    east = (
        await Station.filter(code="east")
        .prefetch_related(*_station_prefetches("east"))
    )[0]
    west = (
        await Station.filter(code="west")
        .prefetch_related(*_station_prefetches("west"))
    )[0]

    east_e50 = next(alarm for alarm in east.alarm_page if alarm.work_order_id == "E50")
    west_e50 = next(alarm for alarm in west.alarm_page if alarm.work_order_id == "E50")

    assert [evidence.label for evidence in east_e50.evidences] == ["A", "B", "C"]
    assert [evidence.label for evidence in west_e50.evidences] == ["D", "E"]

    # Repeated expansion reads the prefetched objects and does not hit the database again.
    assert [evidence.label for evidence in east_e50.evidences] == ["A", "B", "C"]
    assert [evidence.label for evidence in west_e50.evidences] == ["D", "E"]

    east_e49 = next(alarm for alarm in east.alarm_page if alarm.work_order_id == "E49")
    west_e49 = next(alarm for alarm in west.alarm_page if alarm.work_order_id == "E49")
    assert [evidence.label for evidence in east_e49.evidences] == ["F"]
    assert east_e49.latest_signed == _dt(5)
    assert [evidence.label for evidence in west_e49.evidences] == ["G"]
    assert west_e49.latest_signed == _dt(10)


@pytest.mark.asyncio
async def test_keyset_cursor_continues_without_offset(station_db):
    await _seed()
    first = (
        await Station.filter(code="east")
        .prefetch_related(*_station_prefetches("east"))
    )[0]
    lead = first.alarm_page[0]

    # Continue east using only the signing time and work-order number shown on the homepage.
    # The cursor is scoped to the east station, so one combined query can still serve west's
    # unaffected homepage and both stations' return windows.
    cursor = (lead.latest_signed, lead.work_order_id)
    prefetches = [
        GroupedPrefetch(
            relation="alarms",
            queryset=_alarm_queryset_any_station(),
            to_attr="alarm_page",
            limit=2,
            cursor=cursor,
            cursor_parent="east",
        ),
        GroupedPrefetch(
            relation="returns",
            queryset=Return.filter(status="pending_confirm").order_by("-event_time"),
            to_attr="return_page",
            limit=1,
        ),
    ]
    client = Station._meta.db
    with QueryCounter(client) as counter:
        stations = await Station.all().order_by("code").prefetch_related(*prefetches)
    by_code = {station.code: station for station in stations}

    # Both stations' alarm and return windows are produced from a fixed number of queries:
    # the station select, one grouped alarm query, one grouped return query and one nested
    # evidence query, regardless of how many stations are loaded.
    assert counter.count == 4

    assert [alarm.work_order_id for alarm in by_code["east"].alarm_page] == ["E49", "E48"]
    assert [alarm.latest_signed for alarm in by_code["east"].alarm_page] == [_dt(5), _dt(5)]
    assert [ret.work_order_id for ret in by_code["east"].return_page] == ["E99"]

    # West keeps its unaffected first window even though the east cursor is present.
    assert [alarm.work_order_id for alarm in by_code["west"].alarm_page] == ["E49", "E50"]
    assert [alarm.latest_signed for alarm in by_code["west"].alarm_page] == [_dt(10), _dt(4)]
    assert by_code["west"].return_page == []


@pytest.mark.asyncio
async def test_per_parent_limit_with_plain_relation(station_db):
    await _seed()
    # Two alarms on the east station share E50's work order; grouping caps each station at two.
    extra = await Alarm.create(station_id="east", work_order_id="E48")
    await Evidence.create(alarm=extra, label="J", signed_at=_dt(30), uploaded_at=_dt(30))

    stations = await Station.all().prefetch_related(
        GroupedPrefetch(
            relation="alarms",
            queryset=Alarm.filter(station_id__isnull=False).order_by("-id"),
            to_attr="alarm_limited",
            limit=2,
        )
    )
    for station in stations:
        assert len(station.alarm_limited) <= 2


@pytest.mark.asyncio
async def test_grouped_prefetch_coexists_with_plain_prefetch(station_db):
    await _seed()
    # A normal Prefetch (full relation with its own filter/order) and a GroupedPrefetch
    # (limited window into a different relation) run side by side in the same prefetch_related.
    stations = await Station.filter(code="east").prefetch_related(
        Prefetch(
            "returns",
            queryset=Return.filter(status="pending_confirm").order_by("-event_time"),
            to_attr="all_returns",
        ),
        GroupedPrefetch(
            relation="alarms",
            queryset=_alarm_queryset("east"),
            to_attr="alarm_page",
            limit=1,
        ),
    )
    east = stations[0]

    assert [ret.work_order_id for ret in east.all_returns] == ["E99"]
    assert [alarm.work_order_id for alarm in east.alarm_page] == ["E50"]
    # Nested evidence prefetch still runs only against the single selected alarm.
    selected = east.alarm_page[0]
    assert sorted(evidence.label for evidence in selected.evidences) == ["A", "B", "C"]


@pytest.mark.asyncio
async def test_grouped_prefetch_requires_ordering(station_db):
    await _seed()
    with pytest.raises(OperationalError):
        await Station.all().prefetch_related(
            GroupedPrefetch(
                relation="alarms",
                queryset=Alarm.all(),
                to_attr="alarm_page",
                limit=2,
            )
        )
