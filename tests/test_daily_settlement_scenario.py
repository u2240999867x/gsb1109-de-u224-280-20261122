from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from apscheduler.daily_settlement import (
    DailySettlementService,
    Manifest,
    PersistentSettlementStore,
)


D = date(2026, 11, 22)
D1 = date(2026, 11, 23)


def price_manifest(day: date, *, revision: int = 1, include_missing: bool = False):
    partitions = {
        "p1": {"status": "ready", "watermark": 10},
        "p2": {"status": "ready", "watermark": 10},
    }
    if include_missing:
        partitions["p3"] = {"status": "ready", "watermark": 10}
    return Manifest(
        source="price",
        business_date=day,
        version="v1",
        revision=revision,
        required_partitions=["p1", "p2", "p3"],
        partitions=partitions,
        watermark=10,
    )


def inventory_v1(day: date):
    return Manifest(
        source="inventory",
        business_date=day,
        version="v1",
        revision=1,
        required_partitions=["i1", "i2"],
        partitions={
            "i1": {"status": "ready", "watermark": 10},
            "i2": {"status": "ready", "watermark": 10},
        },
        watermark=10,
    )


def inventory_v2(day: date, *, revision: int = 1, complete: bool = False):
    i2 = (
        {"status": "ready", "watermark": 10}
        if complete
        else {"status": "pending", "watermark": 4, "required_watermark": 10}
    )
    return Manifest(
        source="inventory",
        business_date=day,
        version="v2",
        revision=revision,
        required_partitions=["i1", "i2"],
        partitions={
            "i1": {"status": "ready", "watermark": 10},
            "i2": i2,
        },
        watermark=10,
    )


def test_full_daily_settlement_race_persistence_and_audit(tmp_path):
    db_path = tmp_path / "settlement.db"
    scheduler_one = DailySettlementService(db_path, scheduler_id="scheduler-1")
    scheduler_one.register_due(D, scheduled_at="2026-11-22T02:00:00")

    scheduler_one.submit_manifest(inventory_v1(D))
    scheduler_one.submit_manifest(price_manifest(D, revision=1))

    readiness = scheduler_one.evaluate(D)
    assert readiness.ready is False
    assert readiness.pending_partitions["price"] == ["p3"]

    next_day_service = DailySettlementService(db_path, scheduler_id="next-day")
    next_day_service.register_due(D1)
    next_day_service.submit_manifest(price_manifest(D1, include_missing=True))
    next_day_service.submit_manifest(inventory_v1(D1))
    next_day_claim = next_day_service.claim(D1)
    assert next_day_claim.status == "claimed"
    assert next_day_service.evaluate(D).pending_partitions["price"] == ["p3"]

    scheduler_one.submit_manifest(price_manifest(D, revision=2, include_missing=True))
    assert scheduler_one.is_ready(D)

    paused_plan = scheduler_one.prepare_claim(D)
    assert paused_plan is not None
    assert paused_plan.manifest_versions["price"]["revision"] == 2
    assert paused_plan.manifest_versions["inventory"]["version"] == "v1"

    scheduler_one.submit_manifest(inventory_v2(D, revision=1, complete=False))
    assert scheduler_one.is_ready(D) is False
    assert scheduler_one.evaluate(D).pending_partitions["inventory"] == ["i2"]

    scheduler_two = DailySettlementService(
        PersistentSettlementStore(db_path), scheduler_id="scheduler-2"
    )
    recovered = scheduler_two.recover()
    assert recovered["deliveries"]

    old_notification = {
        "notification_id": "old-price-notification",
        "business_date": D.isoformat(),
        "source": "price",
        "version": "v1",
        "revision": 1,
    }

    stale_result = scheduler_one.claim(paused_plan, strict=False)
    assert stale_result.status == "stale"
    assert scheduler_one.used_manifest_versions(D) == []

    redelivered_while_v2_incomplete = scheduler_two.handle_notification(old_notification)
    assert redelivered_while_v2_incomplete.status == "waiting"
    assert scheduler_two.used_manifest_versions(D) == []

    scheduler_two.submit_manifest(inventory_v2(D, revision=2, complete=True))
    assert scheduler_two.is_ready(D)

    barrier = threading.Barrier(2)
    restored_scheduler_one = DailySettlementService(db_path, scheduler_id="scheduler-1-restored")

    def claim(service):
        return service.claim(D, before_claim=barrier.wait)

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(claim, [restored_scheduler_one, scheduler_two]))

    won = [outcome for outcome in outcomes if outcome.status == "claimed"]
    lost = [outcome for outcome in outcomes if outcome.status == "already_claimed"]
    assert len(won) == 1
    assert len(lost) == 1
    assert sum(1 for outcome in outcomes if outcome.won) == 1

    versions = scheduler_two.used_manifest_versions(D)
    assert len(versions) == 1
    used = versions[0]
    assert used.manifest_versions["price"] == {
        "manifest_id": "price:2026-11-22:v1:r2",
        "version": "v1",
        "revision": 2,
        "checksum": used.manifest_versions["price"]["checksum"],
    }
    assert used.manifest_versions["inventory"]["version"] == "v2"
    assert used.manifest_versions["inventory"]["revision"] == 2
    assert used.params["price_revision"] == 2
    assert used.params["inventory_version"] == "v2"
    assert used.params["manifest_versions"]["price"]["revision"] == 2

    restarted = DailySettlementService(db_path, scheduler_id="restarted")
    redelivered = restarted.handle_notification(old_notification)
    assert redelivered.status == "already_claimed"
    assert redelivered.duplicate is True
    assert len(restarted.used_manifest_versions(D)) == 1

    evidence = restarted.explain(D)
    assert evidence["date_scope"] == "exact_business_date_only"
    assert evidence["selected_versions"]["inventory"]["version"] == "v2"
    assert evidence["selected_versions"]["price"]["revision"] == 2
    assert "p3" in evidence["source_reports"]["price"]["satisfied_partitions"]


def test_legal_empty_partition_completes_but_unknown_partition_waits():
    service = DailySettlementService()
    day = "2026-11-24"
    service.register_due(day)
    service.submit_manifest(
        Manifest(
            source="price",
            business_date=day,
            version="v1",
            required_partitions=["p1", "p2", "p3"],
            partitions={"p1": "ready", "p2": "ready"},
            empty_partitions=["p3"],
        )
    )
    service.submit_manifest(
        Manifest(
            source="inventory",
            business_date=day,
            version="v9",
            required_partitions=["i1", "i2", "i3"],
            partitions={"i1": "ready", "i2": "ready"},
        )
    )

    report = service.evaluate(day)
    assert report.ready is False
    assert report.pending_partitions["inventory"] == ["i3"]

    service.submit_manifest(
        Manifest(
            source="inventory",
            business_date=day,
            version="v9",
            revision=2,
            required_partitions=["i1", "i2", "i3"],
            partitions={"i1": "ready", "i2": "ready"},
            empty_partitions=["i3"],
        )
    )
    assert service.is_ready(day)
    result = service.claim(day)
    assert result.status == "claimed"
    evidence = service.explain(day)
    assert "i3" in evidence["source_reports"]["inventory"]["empty_partitions"]


def test_correction_reopens_date_and_appends_version_audit():
    service = DailySettlementService()
    day = date(2026, 11, 25)
    service.register_due(day)
    service.submit_manifest("price", day, version=1, partitions={"p": "ready"}, required_partitions=["p"])
    service.submit_manifest("inventory", day, version=1, partitions={"i": "ready"}, required_partitions=["i"])

    first = service.claim(day)
    assert first.status == "claimed"

    reopened = service.reopen(day)
    assert reopened.status == "waiting"
    service.submit_manifest(
        "price",
        day,
        version=1,
        revision=2,
        partitions={"p": "ready", "p2": "ready"},
        required_partitions=["p", "p2"],
    )

    second = service.claim(day)
    assert second.status == "claimed"
    versions = service.used_manifest_versions(day)
    assert [item.params["price_revision"] for item in versions] == [1, 2]


def test_apscheduler_daily_schedule_is_registered():
    from apscheduler import Scheduler
    from apscheduler.datastores.memory import MemoryDataStore

    service = DailySettlementService()
    scheduler = Scheduler(data_store=MemoryDataStore())
    schedule_id = service.install_schedule(scheduler, at="03:15")
    schedule = scheduler.get_schedule(schedule_id)
    assert schedule.id == schedule_id
    assert str(schedule.trigger._fields[6]) == "15"
    assert str(schedule.trigger._fields[5]) == "3"
