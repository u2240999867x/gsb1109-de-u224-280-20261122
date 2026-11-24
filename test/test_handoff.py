import itertools

import pytest

import dataset
from dataset.handoff import (
    Handoff,
    HandoffError,
    UnresolvedReferenceError,
    import_handoff,
    try_import_handoff,
)


def site(number, reference, vendor="A", **extra):
    return {
        "type": "site",
        "vendor": vendor,
        "legacy_id": number,
        "references": reference,
        **extra,
    }


def collector(number, reference, vendor="A", **extra):
    return {
        "type": "collector",
        "vendor": vendor,
        "legacy_id": number,
        "references": reference,
        **extra,
    }


def ref(vendor, type_, number):
    return {"vendor": vendor, "type": type_, "legacy_id": number}


def ref_asset(vendor, type_, permanent_asset_number):
    return {
        "vendor": vendor,
        "type": type_,
        "permanent_asset_number": permanent_asset_number,
    }


A_SITE = site(7, {"collector_id": ref("A", "collector", 7)})
A_COLLECTOR = collector(
    7,
    {"site_id": ref("A", "site", 7)},
    permanent_asset_number="A-100",
    label="new collector",
)
B_SITE = site(7, {"collector_id": ref_asset("B", "collector", "B-200")}, vendor="B")
B_COLLECTOR = collector(
    None, {}, vendor="B", permanent_asset_number="B-200", label="existing collector"
)

BATCH = [A_SITE, A_COLLECTOR, B_SITE, B_COLLECTOR]


def seed_b(db):
    db["collectors"].insert(
        {
            "vendor": "B",
            "type": "collector",
            "legacy_id": "99",
            "permanent_asset_number": "B-200",
        }
    )
    db["collectors"].insert(
        {
            "vendor": "B",
            "type": "collector",
            "legacy_id": "98",
            "permanent_asset_number": "B-201",
        }
    )
    db.executable.commit()
    return db["collectors"].find_one(permanent_asset_number="B-200")["id"]


@pytest.mark.parametrize("order", list(itertools.permutations(range(4))))
def test_any_order_resolves_both_tables(db, order):
    existing_b = seed_b(db)
    result = Handoff(db).load(BATCH[index] for index in order)

    sites = db["sites"]
    collectors = db["collectors"]
    a_site = sites.find_one(vendor="A", legacy_id="7")
    b_site = sites.find_one(vendor="B", legacy_id="7")
    a_collector = collectors.find_one(vendor="A", permanent_asset_number="A-100")
    b_collector = collectors.find_one(vendor="B", permanent_asset_number="B-200")

    assert a_site["collector_id"] == a_collector["id"]
    assert a_collector["site_id"] == a_site["id"]
    assert b_site["collector_id"] == b_collector["id"] == existing_b
    assert b_collector["site_id"] == b_site["id"]
    assert len(sites) == 2
    assert len(collectors) == 3
    assert result.id_for("site", "A", 7) == a_site["id"]
    assert result.id_for("collector", "B", "B-200") == existing_b


def test_existing_asset_keeps_old_primary_key_and_number(db):
    existing_b = seed_b(db)
    Handoff(db).load(BATCH)

    row = db["collectors"].find_one(permanent_asset_number="B-200")
    assert row["id"] == existing_b
    assert row["legacy_id"] == "99"
    assert db["collectors"].count(vendor="B") == 2


def test_failure_returns_original_record_and_reference(db):
    seed_b(db)
    bad = [
        site(5, {"collector_id": ref("C", "collector", 5)}, vendor="C"),
        collector(6, {}, vendor="C", label="orphan"),
    ]

    with pytest.raises(UnresolvedReferenceError) as info:
        Handoff(db).load(bad)

    references = info.value.references
    assert [item.table for item in references] == ["sites", "collectors"]
    site_ref = references[0]
    assert site_ref.record == bad[0]
    assert (site_ref.vendor, site_ref.type, site_ref.number) == (
        "C",
        "collector",
        5,
    )
    assert site_ref.to_dict()["record"] == bad[0]


def test_failed_batch_writes_nothing_and_keeps_prior_commits(db):
    existing_b = seed_b(db)
    before_collectors = [
        {
            key: row[key]
            for key in ("id", "vendor", "legacy_id", "permanent_asset_number")
        }
        for row in db["collectors"]
    ]
    before_max_id = max(row["id"] for row in before_collectors)

    bad = [
        site(5, {"collector_id": ref("C", "collector", 5)}, vendor="C"),
        collector(6, {}, vendor="C", label="orphan"),
        site(7, {"collector_id": ref("A", "collector", 7)}, vendor="D"),
    ]
    with pytest.raises(UnresolvedReferenceError):
        Handoff(db).load(bad)

    assert "sites" not in db or len(db["sites"]) == 0
    after_collectors = [
        {
            key: row[key]
            for key in ("id", "vendor", "legacy_id", "permanent_asset_number")
        }
        for row in db["collectors"]
    ]
    assert after_collectors == before_collectors
    assert db["collectors"].count() == len(before_collectors)
    assert max(row["id"] for row in after_collectors) == before_max_id
    assert db["collectors"].find_one(id=existing_b)["legacy_id"] == "99"


def test_reads_stream_once(db):
    seed_b(db)

    class Once:
        def __init__(self, items):
            self.items = list(items)
            self.iterated = 0

        def __iter__(self):
            self.iterated += 1
            assert self.iterated == 1
            return iter(self.items)

    Handoff(db).load(Once(BATCH))
    assert len(db["sites"]) == 2


def test_deferred_foreign_keys_enforce_at_commit(db):
    # Inserting a row with a dangling foreign key must fail when the batch
    # transaction commits, and nothing from the batch survives.
    Handoff(db).create_tables()
    db.executable.commit()
    tables = [
        row["name"]
        for row in db.query("SELECT name FROM sqlite_master WHERE type = 'table'")
    ]
    assert "sites" in tables and "collectors" in tables

    with pytest.raises(UnresolvedReferenceError):
        Handoff(db).load([site(1, {"collector_id": ref("Z", "collector", 1)})])
    assert len(db["sites"]) == 0
    assert len(db["collectors"]) == 0


def test_references_to_existing_rows(db):
    db["sites"].insert({"vendor": "E", "type": "site", "legacy_id": "4"})
    db["collectors"].insert(
        {
            "vendor": "E",
            "type": "collector",
            "legacy_id": "9",
            "permanent_asset_number": "E-9",
        }
    )
    db.executable.commit()
    site_id = db["sites"].find_one(vendor="E", legacy_id="4")["id"]
    collector_id = db["collectors"].find_one(vendor="E", legacy_id="9")["id"]

    import_handoff(
        db,
        [
            collector(
                9,
                {"site_id": ref("E", "site", 4)},
                vendor="E",
                permanent_asset_number="E-9",
            ),
            site(4, {"collector_id": ref("E", "collector", 9)}, vendor="E"),
        ],
    )
    assert db["sites"].find_one(id=site_id)["collector_id"] == collector_id
    assert db["collectors"].find_one(id=collector_id)["site_id"] == site_id


def test_shorthand_and_chinese_aliases(db):
    result = Handoff(db).load(
        [
            {"type": "站点", "vendor": "F", "legacy_id": 1, "collector": 2},
            {"type": "采集器", "vendor": "F", "legacy_id": 2, "site": 1},
        ]
    )
    assert result["F/site/1"] == result["F/collector/2"]
    row = db["sites"].find_one(vendor="F", legacy_id="1")
    collector_row = db["collectors"].find_one(vendor="F", legacy_id="2")
    assert row["collector_id"] == collector_row["id"]


def test_missing_reference_is_reported(db):
    with pytest.raises(UnresolvedReferenceError) as info:
        Handoff(db).load([site(3, {}, vendor="G")])
    assert info.value.references[0].number == 3
    assert len(db["sites"]) == 0


def test_handoff_is_exported(db):
    assert dataset.Handoff is Handoff
    assert dataset.import_handoff is import_handoff
    assert callable(db.handoff().load)


def test_non_sqlite_placeholder(db):
    # The loader rejects unknown record types before touching the database.
    with pytest.raises(HandoffError):
        Handoff(db).load([{"type": "unknown", "vendor": "A", "legacy_id": 1}])


def test_created_schema_has_deferred_circular_foreign_keys(db):
    Handoff(db).create_tables()
    db.executable.commit()
    statements = {
        row["name"]: row["sql"]
        for row in db.query("SELECT name, sql FROM sqlite_master WHERE type = 'table'")
    }
    assert "DEFERRABLE INITIALLY DEFERRED" in statements["sites"]
    assert "DEFERRABLE INITIALLY DEFERRED" in statements["collectors"]
    assert "REFERENCES collectors" in statements["sites"]
    assert "REFERENCES sites" in statements["collectors"]


def test_explicit_dangling_reference_does_not_fall_back(db):
    seed_b(db)
    with pytest.raises(UnresolvedReferenceError):
        Handoff(db).load(
            [
                site(7, {"collector_id": ref("A", "collector", 999)}, vendor="D"),
                collector(7, {}, vendor="D"),
            ]
        )
    assert len(db["sites"]) == 0
    assert len(db["collectors"]) == 2


def test_asset_only_record_uses_existing_asset_row(db):
    existing = seed_b(db)
    result = Handoff(db).load([B_SITE, B_COLLECTOR])
    assert result["B/collector/B-200"] == existing
    assert db["collectors"].find_one(id=existing)["site_id"] == result["B/site/7"]
    assert len(db["collectors"]) == 2

    # A stand-alone asset-only refresh reuses the row without inventing a
    # same-number site link.
    refreshed = Handoff(db).load([B_COLLECTOR])
    assert refreshed.ok
    assert db["collectors"].find_one(id=existing)["site_id"] == result["B/site/7"]

    # A new asset-only record cannot be placed without its counter-reference.
    with pytest.raises(UnresolvedReferenceError):
        Handoff(db).load(
            [{"type": "collector", "vendor": "Z", "permanent_asset_number": "Z-1"}]
        )
    assert db["collectors"].count(vendor="Z") == 0


def test_written_rows_always_have_a_resolved_target(db):
    seed_b(db)
    result = Handoff(db).load(BATCH)
    written_sites = set(result.inserted["site"])
    written_collectors = set(result.inserted["collector"]) | set(
        result.updated["collector"]
    )
    for row in db["sites"]:
        if row["id"] in written_sites:
            assert db["collectors"].find_one(id=row["collector_id"]) is not None
    for row in db["collectors"]:
        if row["id"] in written_collectors:
            assert db["sites"].find_one(id=row["site_id"]) is not None


def test_try_import_handoff_returns_failures(db):
    seed_b(db)
    result = try_import_handoff(
        db, [site(5, {"collector_id": ref("C", "collector", 5)}, vendor="C")]
    )
    assert result.ok is False
    assert result.as_dict() == {}
    assert len(result.errors) == 1
    assert result.errors[0].record == site(
        5, {"collector_id": ref("C", "collector", 5)}, vendor="C"
    )
    assert len(db["sites"]) == 0
