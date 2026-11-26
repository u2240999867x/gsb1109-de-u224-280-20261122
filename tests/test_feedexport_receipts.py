from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import scrapy
from scrapy.exceptions import NotConfigured
from scrapy.extensions.feedexport import (
    FeedExporter,
    FeedItemReceipt,
    FeedSlot,
    FileFeedStorage,
    _FeedReceipts,
)
from scrapy.utils.asyncio import sleep
from scrapy.utils.misc import build_from_crawler
from scrapy.utils.test import get_crawler
from tests.utils.decorators import coroutine_test


def split_logistics(item: dict[str, Any]):
    yield {"id": item["id"], "row": 1}
    yield {"id": item["id"], "row": 2}


def split_finance(item: dict[str, Any]):
    yield {"id": item["id"], "row": 1}


class LogisticsFilter:
    def __init__(self, feed_options: dict[str, Any]):
        pass

    def accepts(self, item: dict[str, Any]) -> bool:
        return True


class FinanceFilter:
    """Finance accepts only the entries that carry a ``finance`` flag."""

    def __init__(self, feed_options: dict[str, Any]):
        pass

    def accepts(self, item: dict[str, Any]) -> bool:
        return bool(item.get("finance"))


def receipt_spider(items: list[Any]) -> type[scrapy.Spider]:
    class ReceiptSpider(scrapy.Spider):
        name = "receipts"

        async def start(self):
            for item in items:
                yield item

    return ReceiptSpider


def read_jsonlines(directory: Path) -> dict[str, list[dict[str, Any]]]:
    return {
        path.name: [
            json.loads(line) for line in path.read_text().splitlines() if line
        ]
        for path in sorted(directory.iterdir())
        if path.is_file() and not path.name.startswith(".")
    }


class FailingFinanceStorage(FileFeedStorage):
    """Local storage that fails its first commit of the finance feed."""

    failed = False
    commits: list[str] = []

    def _commit(self, file):
        type(self).commits.append(self.path)
        if "finance" in Path(self.path).parent.name and not type(self).failed:
            type(self).failed = True
            file.close()
            if self._temp_path is not None:
                Path(self._temp_path).unlink(missing_ok=True)
                self._temp_path = None
            raise OSError("disk full")
        super()._commit(file)


class FailAllFinanceStorage(FileFeedStorage):
    """Storage that fails to commit any finance feed."""

    def _commit(self, file):
        if Path(self.path).parent.name == "finance":
            file.close()
            if self._temp_path is not None:
                Path(self._temp_path).unlink(missing_ok=True)
                self._temp_path = None
            raise OSError("disk full")
        super()._commit(file)


class DelayedFileStorage(FileFeedStorage):
    """A local storage whose store() completes only after a delay, so that a
    later feed can store before an earlier one."""

    order: list[str] = []

    async def store(self, file):
        feed = Path(self.path).parent.name
        # Finance finishes first, by failing; logistics finishes later, by
        # succeeding.
        await sleep(0.01 if feed == "finance" else 0.05)
        if feed == "finance":
            file.close()
            if self._temp_path is not None:
                Path(self._temp_path).unlink(missing_ok=True)
                self._temp_path = None
            type(self).order.append("finance-failed")
            raise OSError("disk full")
        super().store(file)
        type(self).order.append("logistics")


def _feed_settings(
    tmp_path: Path, *, batch_item_count: int = 3, finance_first: bool = False
) -> dict[str, Any]:
    logistics = {
        "format": "jsonlines",
        "item_processor": split_logistics,
        "item_filter": LogisticsFilter,
        "batch_item_count": batch_item_count,
    }
    finance = {
        "format": "jsonlines",
        "item_processor": split_finance,
        "item_filter": FinanceFilter,
        "batch_item_count": 100,
    }
    logistics_uri = str(tmp_path / "logistics" / "%(batch_id)d.jsonl")
    finance_uri = str(tmp_path / "finance" / "%(batch_id)d.jsonl")
    feeds = (
        {finance_uri: finance, logistics_uri: logistics}
        if finance_first
        else {logistics_uri: logistics, finance_uri: finance}
    )
    return {
        "JOBDIR": str(tmp_path / "job"),
        "LOG_LEVEL": "ERROR",
        "FEEDS": feeds,
    }


@coroutine_test
async def test_receipts_resume_only_missing_entries(tmp_path: Path) -> None:
    """X reaches logistics twice and finance once; when the finance store
    fails, a replay under the same JOBDIR delivers only the missing finance
    entry and leaves the logistics output untouched."""
    items = [
        {"id": "X", "finance": True},
        {"id": "Y"},
    ]
    settings = _feed_settings(tmp_path)
    logistics = tmp_path / "logistics"
    finance = tmp_path / "finance"

    FailingFinanceStorage.failed = False
    settings["FEED_STORAGES"] = {
        "": FailingFinanceStorage,
        "file": FailingFinanceStorage,
    }
    crawler = get_crawler(receipt_spider(items), settings)
    await crawler.crawl_async()

    assert read_jsonlines(logistics) == {
        "1.jsonl": [
            {"id": "X", "row": 1},
            {"id": "X", "row": 2},
            {"id": "Y", "row": 1},
        ],
        "2.jsonl": [{"id": "Y", "row": 2}],
    }
    assert not (finance / "1.jsonl").exists()

    exporter = next(
        extension
        for extension in crawler.extensions.middlewares
        if isinstance(extension, FeedExporter)
    )
    receipts = exporter.delivery_receipts
    assert {(r.feed.split("/")[-2], r.item_key, r.index) for r in receipts} == {
        ("logistics", "X", 0),
        ("logistics", "X", 1),
        ("logistics", "Y", 0),
        ("logistics", "Y", 1),
    }

    settings["FEED_STORAGES"] = {"file": FileFeedStorage}
    crawler = get_crawler(receipt_spider(items), settings)
    await crawler.crawl_async()

    assert read_jsonlines(logistics) == {
        "1.jsonl": [
            {"id": "X", "row": 1},
            {"id": "X", "row": 2},
            {"id": "Y", "row": 1},
        ],
        "2.jsonl": [{"id": "Y", "row": 2}],
    }
    # Finance retries X into a new batch, because batch 1 was attempted and
    # failed, and never receives Y.
    assert read_jsonlines(finance) == {"2.jsonl": [{"id": "X", "row": 1}]}


@coroutine_test
async def test_receipts_batch_size_change_keeps_delivered_entries(
    tmp_path: Path,
) -> None:
    """Changing the batch size on replay does not resend delivered entries or
    change the files that already hold them."""
    items = [
        {"id": "X", "finance": True},
        {"id": "Y"},
        {"id": "Z", "finance": True},
    ]
    settings = _feed_settings(tmp_path, batch_item_count=3)
    logistics = tmp_path / "logistics"

    crawler = get_crawler(receipt_spider(items), settings)
    await crawler.crawl_async()
    assert read_jsonlines(logistics) == {
        "1.jsonl": [
            {"id": "X", "row": 1},
            {"id": "X", "row": 2},
            {"id": "Y", "row": 1},
        ],
        "2.jsonl": [
            {"id": "Y", "row": 2},
            {"id": "Z", "row": 1},
            {"id": "Z", "row": 2},
        ],
    }

    settings["FEEDS"][str(tmp_path / "logistics" / "%(batch_id)d.jsonl")][
        "batch_item_count"
    ] = 1
    new_items = [
        {"id": "W", "finance": True},
    ]
    crawler = get_crawler(receipt_spider(items + new_items), settings)
    await crawler.crawl_async()

    contents = read_jsonlines(logistics)
    assert contents == {
        "1.jsonl": [
            {"id": "X", "row": 1},
            {"id": "X", "row": 2},
            {"id": "Y", "row": 1},
        ],
        "2.jsonl": [
            {"id": "Y", "row": 2},
            {"id": "Z", "row": 1},
            {"id": "Z", "row": 2},
        ],
        "3.jsonl": [
            {"id": "W", "row": 1},
        ],
        "4.jsonl": [
            {"id": "W", "row": 2},
        ],
    }


@coroutine_test
async def test_receipts_require_jobdir(tmp_path: Path) -> None:
    settings = _feed_settings(tmp_path)
    settings.pop("JOBDIR")
    for feed_options in settings["FEEDS"].values():
        feed_options["delivery_receipts"] = True
    crawler = get_crawler(receipt_spider([]), settings)
    with pytest.raises(NotConfigured, match="JOBDIR"):
        build_from_crawler(FeedExporter, crawler)


def test_receipt_journal_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "feed_receipts.json"
    journal = _FeedReceipts(path)
    journal.add(
        [
            FeedItemReceipt(
                feed="feed",
                item_key="X",
                entry_key=_FeedReceipts.entry_key("feed", "X", 0),
                uri="feed-1.jsonl",
                batch_id=1,
                index=0,
            )
        ],
        batch=("feed", 1),
    )
    journal.mark_attempted("feed", 2)
    journal.add(
        [
            FeedItemReceipt(
                feed="feed",
                item_key="Y",
                entry_key=_FeedReceipts.entry_key("feed", "Y", 0),
                uri="feed-3.jsonl",
                batch_id=3,
                index=0,
            )
        ],
        batch=("feed", 3),
    )

    loaded = _FeedReceipts(path)
    assert loaded.all() == [
        FeedItemReceipt(
            feed="feed",
            item_key="X",
            entry_key=_FeedReceipts.entry_key("feed", "X", 0),
            uri="feed-1.jsonl",
            batch_id=1,
            index=0,
        ),
        FeedItemReceipt(
            feed="feed",
            item_key="Y",
            entry_key=_FeedReceipts.entry_key("feed", "Y", 0),
            uri="feed-3.jsonl",
            batch_id=3,
            index=0,
        ),
    ]
    assert loaded.next_batch_id("feed") == 4


@coroutine_test
async def test_slot_does_not_expose_failed_receipts(tmp_path: Path) -> None:
    settings = {
        "JOBDIR": str(tmp_path / "job"),
        "FEED_STORAGES": {
            "": FailingFinanceStorage,
            "file": FailingFinanceStorage,
        },
        "FEEDS": {
            str(tmp_path / "finance" / "%(batch_id)d.jsonl"): {
                "format": "jsonlines",
                "delivery_receipts": True,
            }
        },
        "FEED_STORE_EMPTY": False,
    }
    FailingFinanceStorage.failed = False
    crawler = get_crawler(receipt_spider([{"id": "X"}]), settings)
    closed: list[FeedSlot] = []

    def collect(slot: FeedSlot) -> None:
        closed.append(slot)

    crawler.signals.connect(collect, signal=scrapy.signals.feed_slot_closed, weak=False)
    await crawler.crawl_async()
    assert closed
    assert not any(slot.delivered for slot in closed)
    assert not any(slot.delivery_receipts for slot in closed)


@coroutine_test
async def test_success_receipt_completes_after_failed_feed(tmp_path: Path) -> None:
    """A successful feed can complete its store after a failed feed; its
    receipt is still recorded, and the failed feed gets none."""
    items = [{"id": "X", "finance": True}, {"id": "Y"}]
    settings = _feed_settings(tmp_path, finance_first=True)
    DelayedFileStorage.order = []
    settings["FEED_STORAGES"] = {
        "": DelayedFileStorage,
        "file": DelayedFileStorage,
    }
    crawler = get_crawler(receipt_spider(items), settings)
    await crawler.crawl_async()
    exporter = next(
        extension
        for extension in crawler.extensions.middlewares
        if isinstance(extension, FeedExporter)
    )
    receipts = exporter.delivery_receipts
    assert {receipt.item_key for receipt in receipts} == {"X", "Y"}
    assert all("finance" not in receipt.uri for receipt in receipts)
    assert DelayedFileStorage.order[:2] == ["finance-failed", "logistics"]
    assert read_jsonlines(tmp_path / "logistics") == {
        "1.jsonl": [
            {"id": "X", "row": 1},
            {"id": "X", "row": 2},
            {"id": "Y", "row": 1},
        ],
        "2.jsonl": [{"id": "Y", "row": 2}],
    }


def test_transactional_storage_keeps_target_untouched_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "items.jsonl"
    path.write_bytes(b"old\n")
    storage = FileFeedStorage(
        str(path),
        feed_options={"mode": "append", "delivery_receipts": True},
    )
    file = storage.open(scrapy.Spider("test"))
    file.write(b"new\n")

    def replace(self, target):
        raise OSError("disk full")

    monkeypatch.setattr(Path, "replace", replace)
    with pytest.raises(OSError, match="disk full"):
        storage.store(file)
    assert path.read_bytes() == b"old\n"
    assert not list(tmp_path.glob(".items.jsonl.*"))


def test_transactional_storage_appends_on_success(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    path.write_bytes(b"old\n")
    storage = FileFeedStorage(
        str(path),
        feed_options={"mode": "append", "delivery_receipts": True},
    )
    file = storage.open(scrapy.Spider("test"))
    file.write(b"new\n")
    storage.store(file)
    assert path.read_bytes() == b"old\nnew\n"
    assert not list(tmp_path.glob(".items.jsonl.*"))


def test_transactional_storage_create_conflict(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    path.write_bytes(b"old\n")
    storage = FileFeedStorage(
        str(path),
        feed_options={"mode": "create", "delivery_receipts": True},
    )
    file = storage.open(scrapy.Spider("test"))
    file.write(b"new\n")
    with pytest.raises(FileExistsError):
        storage.store(file)
    assert path.read_bytes() == b"old\n"
    assert not list(tmp_path.glob(".items.jsonl.*"))
