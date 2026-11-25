"""End-to-end tests for publish directories and merges.

The tests use an in-memory object store exposing the subset of the S3FileSystem
interface that the publication layer relies on: version-aware ``find``/``info``,
in-bucket ``cp_file`` with ``?versionId=`` sources, ``cat_file``/``pipe_file``,
``rm_file`` and ``object_version_info``.  That keeps the tests fast and avoids
needing a server socket, while exercising exactly the version-pinning behaviour
the publication layer depends on.
"""

from __future__ import annotations

import itertools
from datetime import datetime, timedelta, timezone

import pytest

from s3fs.publication import (
    KEEP,
    REPLACE,
    MergeConflictError,
    PublicationLayoutError,
    PublicationNotFound,
    PublicationStore,
    begin_merge,
    merge_directories,
    publication_for,
    publish_directory,
)


class FakeS3:
    """A small versioned in-bucket object store."""

    def __init__(self):
        self.objects: dict[tuple[str, str], list[dict]] = {}
        self._versions = itertools.count(1)
        self.version_aware = True
        self.clock = None

    # -- helpers ---------------------------------------------------------

    def split_path(self, path):
        path = path[5:] if path.startswith("s3://") else path
        path = path.lstrip("/")
        bucket, _, key = path.partition("/")
        key, _, version = key.partition("?versionId=")
        return bucket, key, version or None

    def _bump(self):
        now = datetime.now(timezone.utc)
        if self.clock is not None and now <= self.clock:
            now = self.clock + timedelta(microseconds=1)
        self.clock = now
        return now

    def versions(self, path):
        bucket, key, _ = self.split_path(path)
        return self.objects.get((bucket, key), [])

    def _latest(self, path):
        bucket, key, version = self.split_path(path)
        versions = self.objects.get((bucket, key), [])
        if version:
            for item in versions:
                if item["VersionId"] == version:
                    return item
            raise FileNotFoundError(path)
        if not versions:
            raise FileNotFoundError(path)
        item = versions[-1]
        if item["DeleteMarker"]:
            raise FileNotFoundError(path)
        return item

    # -- s3fs-compatible surface ----------------------------------------

    def pipe_file(self, path, data, **kwargs):
        bucket, key, _ = self.split_path(path)
        item = {
            "VersionId": f"v{next(self._versions)}",
            "Body": bytes(data),
            "LastModified": self._bump(),
            "DeleteMarker": False,
            "ETag": f'"{hash(bytes(data)) & 0xFFFFFFFF:08x}"',
        }
        self.objects.setdefault((bucket, key), []).append(item)
        return item

    def cat_file(self, path, start=None, end=None):
        data = self._latest(path)["Body"]
        return data[slice(start, end)]

    def get_file(self, path, local_path):
        with open(local_path, "wb") as fh:
            fh.write(self.cat_file(path))
        return local_path

    def open(self, path, mode="rb"):
        import io

        if "r" not in mode:
            raise ValueError(mode)
        return io.BytesIO(self.cat_file(path))

    def info(self, path, refresh=False, version_id=None):
        if version_id:
            path = f"{path}?versionId={version_id}"
        item = self._latest(path)
        bucket, key, _ = self.split_path(path)
        return {
            "name": f"{bucket}/{key}",
            "Key": f"{bucket}/{key}",
            "size": len(item["Body"]),
            "ETag": item["ETag"],
            "VersionId": item["VersionId"],
            "LastModified": item["LastModified"],
            "type": "file",
        }

    def exists(self, path):
        try:
            self.info(path)
            return True
        except FileNotFoundError:
            return False

    def find(self, path, detail=False, refresh=False, **kwargs):
        root = path.rstrip("/")
        prefix = root + "/"
        out = {}
        for (bucket, key) in self.objects:
            full = f"{bucket}/{key}"
            if not full.startswith(prefix):
                continue
            try:
                item = self._latest(full)
            except FileNotFoundError:
                continue
            out[full] = {
                "name": full,
                "Key": full,
                "size": len(item["Body"]),
                "ETag": item["ETag"],
                "VersionId": item["VersionId"],
                "LastModified": item["LastModified"],
                "type": "file",
            }
        if not out:
            raise FileNotFoundError(path)
        return out if detail else sorted(out)

    def rm_file(self, path, **kwargs):
        bucket, key, version = self.split_path(path)
        versions = self.objects.get((bucket, key), [])
        if version:
            self.objects[(bucket, key)] = [
                item for item in versions if item["VersionId"] != version
            ]
        else:
            versions.append(
                {
                    "VersionId": f"v{next(self._versions)}",
                    "Body": None,
                    "LastModified": self._bump(),
                    "DeleteMarker": True,
                    "ETag": None,
                }
            )

    def cp_file(self, path1, path2, **kwargs):
        source = self._latest(path1)
        self.pipe_file(path2, source["Body"])

    def object_version_info(self, path):
        bucket, key, _ = self.split_path(path)
        return [
            {
                "Key": key,
                "VersionId": item["VersionId"],
                "IsLatest": index == len(self.objects[(bucket, key)]) - 1,
                "LastModified": item["LastModified"],
                "Size": len(item["Body"]) if item["Body"] is not None else 0,
            }
            for index, item in enumerate(self.objects.get((bucket, key), []))
        ]


BUCKET = "publish"


def write(fs, path, data):
    return fs.pipe_file(f"{BUCKET}/{path}", data)["VersionId"]


@pytest.fixture
def s3():
    return FakeS3()


@pytest.fixture
def store(s3):
    return PublicationStore(s3)


def test_publication_pins_paths_and_versions(s3, store):
    sales = write(s3, "daily/sales.csv", b"sales v1")
    write(s3, "daily/stock.csv", b"stock v1")
    publish = publish_directory(s3, f"{BUCKET}/daily")

    # overwrite and remove live objects: the generation must not change
    write(s3, "daily/sales.csv", b"sales v2")
    s3.rm_file(f"{BUCKET}/daily/stock.csv")

    assert store.current(f"{BUCKET}/daily").generation == publish.generation
    assert publish.cat_file("sales.csv") == b"sales v1"
    assert publish.cat_file(f"{BUCKET}/daily/stock.csv") == b"stock v1"
    assert publish.entry("sales.csv").version_id == sales
    assert publish.ls() == [f"{BUCKET}/daily/sales.csv", f"{BUCKET}/daily/stock.csv"]
    assert publish.find() == sorted(publish.ls())


def test_publication_tree_navigation(s3):
    for path, data in {
        "daily/a.csv": b"a",
        "daily/sub/b.csv": b"b",
        "daily/sub/deeper/c.csv": b"c",
    }.items():
        write(s3, path, data)
    publication = publish_directory(s3, f"{BUCKET}/daily")

    assert publication.ls() == [
        f"{BUCKET}/daily/a.csv",
        f"{BUCKET}/daily/sub/",
    ]
    assert publication.ls("sub") == [
        f"{BUCKET}/daily/sub/b.csv",
        f"{BUCKET}/daily/sub/deeper/",
    ]
    assert publication.isdir("sub") and not publication.isfile("sub")
    assert publication.find("sub") == [
        f"{BUCKET}/daily/sub/b.csv",
        f"{BUCKET}/daily/sub/deeper/c.csv",
    ]
    with pytest.raises(PublicationNotFound):
        publication.entry("missing.csv")


def test_first_entry_copies_work_order_version(s3):
    write(s3, "old/sales.csv", b"old sales v1")
    work_order_version = s3.info(f"{BUCKET}/old/sales.csv")["VersionId"]
    write(s3, "old/sales.csv", b"old sales v2")
    # ``old/sales.csv`` was never catalogued, so it is a first-time entry even
    # though a later write exists.  The work order version wins.
    publish_directory(s3, f"{BUCKET}/daily")

    draft = begin_merge(
        s3, f"{BUCKET}/old", f"{BUCKET}/daily", versions={"sales.csv": work_order_version}
    )
    result = draft.commit()
    assert result.cat_file("sales.csv") == b"old sales v1"


def test_recatalogued_file_keeps_original_version_when_moved(s3):
    write(s3, "old/sales.csv", b"catalogued")
    original = s3.info(f"{BUCKET}/old/sales.csv")["VersionId"]
    publish_directory(s3, f"{BUCKET}/old")
    # A later correction changes the live object after it was catalogued.
    write(s3, "old/sales.csv", b"corrected but not catalogued")

    publish_directory(s3, f"{BUCKET}/daily")
    draft = begin_merge(s3, f"{BUCKET}/old", f"{BUCKET}/daily")
    assert draft.check() == []
    result = draft.commit()
    assert result.entries["sales.csv"].version_id == original
    # Moving a catalogued file reuses its object version; nothing is copied.
    assert s3.versions(f"{BUCKET}/daily/sales.csv") == []
    assert result.cat_file("sales.csv") == b"catalogued"


def test_conflict_decisions_keep_and_replace(s3):
    write(s3, "daily/sales.csv", b"daily sales")
    write(s3, "daily/stock.csv", b"daily stock")
    write(s3, "daily/tax.csv", b"daily tax")
    publish_directory(s3, f"{BUCKET}/daily")
    daily_versions = {
        name: s3.info(f"{BUCKET}/daily/{name}")["VersionId"]
        for name in ("sales.csv", "stock.csv", "tax.csv")
    }

    write(s3, "old/sales.csv", b"old sales")
    write(s3, "old/stock.csv", b"old stock")
    write(s3, "old/tax.csv", b"old tax")
    publish_directory(s3, f"{BUCKET}/old")

    draft = begin_merge(s3, f"{BUCKET}/old", f"{BUCKET}/daily")
    conflicts = draft.conflicts()
    assert {issue.source_path for issue in conflicts} == {
        "sales.csv",
        "stock.csv",
        "tax.csv",
    }

    draft.keep("sales.csv", "review.csv")
    draft.replace("stock.csv")
    draft.replace("tax.csv")
    assert draft.check() == []

    result = draft.commit()
    assert result.cat_file("sales.csv") == b"daily sales"
    assert result.cat_file("review.csv") == b"old sales"
    assert result.cat_file("stock.csv") == b"old stock"
    assert result.cat_file("tax.csv") == b"old tax"
    # untouched target file keeps its original object version
    assert result.entries["sales.csv"].version_id == daily_versions["sales.csv"]


def test_late_source_arrivals_join_the_merge(s3):
    write(s3, "daily/sales.csv", b"daily sales")
    publish_directory(s3, f"{BUCKET}/daily")
    write(s3, "old/sales.csv", b"old sales")
    publish_directory(s3, f"{BUCKET}/old")

    draft = begin_merge(s3, f"{BUCKET}/old", f"{BUCKET}/daily")
    draft.keep("sales.csv", "sales-review.csv")

    # arrivals after confirmation but before commit
    write(s3, "old/notes.csv/audit.json", b'{"ok": true}')
    write(s3, "old/returns.csv", b"returns")

    result = draft.commit()
    assert result.cat_file("notes.csv/audit.json") == b'{"ok": true}'
    assert result.cat_file("returns.csv") == b"returns"
    assert result.cat_file("sales-review.csv") == b"old sales"


def test_alternative_destination_collides_with_late_arrival(s3):
    write(s3, "daily/sales.csv", b"daily sales")
    write(s3, "daily/stock.csv", b"daily stock")
    write(s3, "daily/tax.csv", b"daily tax")
    publish_directory(s3, f"{BUCKET}/daily")
    write(s3, "old/sales.csv", b"old sales")
    write(s3, "old/stock.csv", b"old stock")
    write(s3, "old/tax.csv", b"old tax")
    publish_directory(s3, f"{BUCKET}/old")

    draft = begin_merge(s3, f"{BUCKET}/old", f"{BUCKET}/daily")
    draft.keep("sales.csv", "sales-review.csv")
    draft.replace("stock.csv")
    draft.replace("tax.csv")
    write(s3, "old/sales-review.csv/audit.json", b"audit")

    with pytest.raises(MergeConflictError) as excinfo:
        draft.commit()
    issues = excinfo.value.issues
    assert any(
        issue.kind == "path-type"
        and "sales.csv" in issue.source_path
        and "sales-review.csv/audit.json" in str(issue.counterpart)
        for issue in issues
    )


def test_stale_confirmation_after_target_replaced_and_restored(s3):
    write(s3, "daily/stock.csv", b"stock original")
    original = s3.info(f"{BUCKET}/daily/stock.csv")["VersionId"]
    publish_directory(s3, f"{BUCKET}/daily")
    write(s3, "old/stock.csv", b"old stock")
    publish_directory(s3, f"{BUCKET}/old")

    draft = begin_merge(s3, f"{BUCKET}/old", f"{BUCKET}/daily")
    draft.replace("stock.csv")

    # Another work order replaces the object and then restores the same version.
    write(s3, "daily/stock.csv", b"stock newer")
    s3.rm_file(f"{BUCKET}/daily/stock.csv")
    s3.pipe_file(f"{BUCKET}/daily/stock.csv", b"stock original")
    restored = s3.info(f"{BUCKET}/daily/stock.csv")["VersionId"]
    assert restored != original
    # The other work order publishes its generation, as any release does.
    publish_directory(s3, f"{BUCKET}/daily")

    with pytest.raises(MergeConflictError) as excinfo:
        draft.commit()
    assert "confirm it again" in str(excinfo.value)

    # Re-confirming against the restored object clears the conflict.
    draft.replace("stock.csv")
    assert draft.check() == []
    result = draft.commit()
    assert result.cat_file("stock.csv") == b"old stock"


def test_tax_confirmation_survives_other_changes(s3):
    write(s3, "daily/sales.csv", b"daily sales")
    write(s3, "daily/stock.csv", b"daily stock")
    write(s3, "daily/tax.csv", b"daily tax")
    publish_directory(s3, f"{BUCKET}/daily")
    write(s3, "old/sales.csv", b"old sales")
    write(s3, "old/stock.csv", b"old stock")
    write(s3, "old/tax.csv", b"old tax")
    publish_directory(s3, f"{BUCKET}/old")

    draft = begin_merge(s3, f"{BUCKET}/old", f"{BUCKET}/daily")
    draft.keep("sales.csv", "review.csv")
    draft.replace("stock.csv")
    draft.replace("tax.csv")

    # Stock changes and is restored; weather arrives; sales destination collides.
    write(s3, "daily/stock.csv", b"stock new")
    write(s3, "daily/weather.csv", b"weather")
    write(s3, "old/review.csv/audit.json", b"audit")

    with pytest.raises(MergeConflictError) as excinfo:
        draft.commit()
    issues = excinfo.value.issues
    kinds = {issue.kind for issue in issues}
    assert "stale" in kinds
    assert "path-type" in kinds
    # The tax confirmation is not reported as stale.
    assert all(issue.source_path != "tax.csv" for issue in issues)


def test_full_scenario_end_to_end(s3):
    # initial target tree
    write(s3, "daily/sales.csv", b"daily sales")
    write(s3, "daily/stock.csv", b"daily stock")
    write(s3, "daily/tax.csv", b"daily tax")
    before = publish_directory(s3, f"{BUCKET}/daily")
    daily_sales_version = before.entries["sales.csv"].version_id

    write(s3, "old/sales.csv", b"old sales")
    write(s3, "old/stock.csv", b"old stock")
    write(s3, "old/tax.csv", b"old tax")
    publish_directory(s3, f"{BUCKET}/old")

    draft = begin_merge(s3, f"{BUCKET}/old", f"{BUCKET}/daily")
    assert {issue.source_path for issue in draft.conflicts()} == {
        "sales.csv",
        "stock.csv",
        "tax.csv",
    }
    draft.keep("sales.csv", "review.csv")
    draft.replace("stock.csv")
    draft.replace("tax.csv")
    confirmed_stock = draft.decisions["stock.csv"].target_version

    # 1. late arrivals in the source
    write(s3, "old/review.csv/audit.json", b'{"audit": 1}')
    write(s3, "old/returns.csv", b"returns")

    # 2. another work order replaces stock, then restores the confirmed version
    write(s3, "daily/stock.csv", b"stock v2")
    s3.pipe_file(f"{BUCKET}/daily/stock.csv", b"daily stock")
    restored_stock = s3.info(f"{BUCKET}/daily/stock.csv")["VersionId"]
    assert restored_stock != confirmed_stock

    # 3. weather is published to the target
    write(s3, "daily/weather.csv", b"weather")
    publish_directory(s3, f"{BUCKET}/daily")

    with pytest.raises(MergeConflictError) as excinfo:
        draft.commit()
    issues = excinfo.value.issues
    assert any(issue.source_path == "stock.csv" and issue.kind == "stale" for issue in issues)
    assert any(
        issue.kind == "path-type"
        and "sales.csv" in issue.source_path
        and "review.csv/audit.json" in str(issue.counterpart)
        for issue in issues
    )
    assert all(issue.source_path != "tax.csv" for issue in issues)

    # Until resolved, readers keep the latest complete tree.
    current = publication_for(s3, f"{BUCKET}/daily")
    assert current.generation != draft.target_generation
    assert current.cat_file("stock.csv") == b"daily stock"
    assert current.cat_file("weather.csv") == b"weather"
    assert not current.exists("review.csv")
    # Tax has not been replaced early.
    assert current.cat_file("tax.csv") == b"daily tax"

    # 4. re-confirm stock and move sales to another free path
    draft.replace("stock.csv")
    draft.keep("sales.csv", "sales-review.csv")

    result = draft.commit()
    assert result.cat_file("sales.csv") == b"daily sales"
    assert result.entries["sales.csv"].version_id == daily_sales_version
    assert result.cat_file("weather.csv") == b"weather"
    assert result.cat_file("review.csv/audit.json") == b'{"audit": 1}'
    assert result.cat_file("returns.csv") == b"returns"
    assert result.cat_file("stock.csv") == b"old stock"
    assert result.cat_file("tax.csv") == b"old tax"
    assert result.cat_file("sales-review.csv") == b"old sales"

    # source entries disappeared from the live source tree, except publication
    # metadata for the source generations
    live = s3.find(f"{BUCKET}/old")
    assert f"{BUCKET}/old/.s3fs-publication/current.json" in live
    assert not any(
        path.endswith(".csv") or path.endswith(".json") and "audit" in path
        for path in live
    )
    source = publication_for(s3, f"{BUCKET}/old")
    assert source.paths() == []

    # a reader on the pre-merge generation still reads its own paths/versions
    assert before.cat_file("stock.csv") == b"daily stock"
    assert before.cat_file("sales.csv") == b"daily sales"
    assert before.entry("stock.csv").version_id != result.entries["stock.csv"].version_id


def test_commit_is_atomic_on_conflict(s3):
    write(s3, "daily/sales.csv", b"daily sales")
    publish_directory(s3, f"{BUCKET}/daily")
    write(s3, "old/sales.csv", b"old sales")
    write(s3, "old/returns.csv", b"returns")
    publish_directory(s3, f"{BUCKET}/old")

    draft = begin_merge(s3, f"{BUCKET}/old", f"{BUCKET}/daily")
    draft.keep("sales.csv", "review.csv")
    # An unconfirmed arrival lands exactly on the target path of another file.
    write(s3, "old/returns.csv", b"returns v2")
    write(s3, "daily/returns.csv", b"unexpected")
    publish_directory(s3, f"{BUCKET}/daily")

    before = publication_for(s3, f"{BUCKET}/daily")
    with pytest.raises(MergeConflictError):
        draft.commit()
    after = publication_for(s3, f"{BUCKET}/daily")
    assert before.generation == after.generation
    assert after.cat_file("returns.csv") == b"unexpected"


def test_keep_cannot_reuse_conflicting_path(s3):
    write(s3, "daily/sales.csv", b"daily sales")
    publish_directory(s3, f"{BUCKET}/daily")
    write(s3, "old/sales.csv", b"old sales")
    publish_directory(s3, f"{BUCKET}/old")

    draft = begin_merge(s3, f"{BUCKET}/old", f"{BUCKET}/daily")
    with pytest.raises(ValueError):
        draft.keep("sales.csv", "sales.csv")


def test_layout_error_is_reported_for_published_tree(s3):
    write(s3, "daily/a.csv", b"a")
    write(s3, "daily/a.csv/inner.csv", b"inner")
    with pytest.raises(PublicationLayoutError):
        publish_directory(s3, f"{BUCKET}/daily")


def test_merge_directories_convenience(s3):
    write(s3, "daily/sales.csv", b"daily sales")
    publish_directory(s3, f"{BUCKET}/daily")
    write(s3, "old/sales.csv", b"old sales")
    write(s3, "old/extra.csv", b"extra")
    publish_directory(s3, f"{BUCKET}/old")

    result = merge_directories(
        s3,
        f"{BUCKET}/old",
        f"{BUCKET}/daily",
        resolutions={"sales.csv": "review.csv"},
    )
    assert result.cat_file("sales.csv") == b"daily sales"
    assert result.cat_file("review.csv") == b"old sales"
    assert result.cat_file("extra.csv") == b"extra"


def test_draft_round_trip(s3):
    write(s3, "daily/sales.csv", b"daily sales")
    publish_directory(s3, f"{BUCKET}/daily")
    write(s3, "old/sales.csv", b"old sales")
    publish_directory(s3, f"{BUCKET}/old")

    draft = begin_merge(s3, f"{BUCKET}/old", f"{BUCKET}/daily")
    draft.replace("sales.csv")
    restored = type(draft).from_dict(draft.merger, draft.to_dict())
    assert restored.decisions["sales.csv"].action == REPLACE
    result = restored.commit()
    assert result.cat_file("sales.csv") == b"old sales"


def test_generation_pointer_is_separate_from_entries(s3, store):
    write(s3, "daily/a.csv", b"a")
    first = publish_directory(s3, f"{BUCKET}/daily")
    write(s3, "daily/b.csv", b"b")
    second = publish_directory(s3, f"{BUCKET}/daily")

    assert first.generation != second.generation
    assert store.current(f"{BUCKET}/daily").generation == second.generation
    assert first.paths() == ["a.csv"]
    assert second.paths() == ["a.csv", "b.csv"]
    assert first.cat_file("a.csv") == b"a"
