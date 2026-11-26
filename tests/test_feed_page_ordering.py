from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import scrapy
from scrapy.extensions.feedexport import FeedExporter
from scrapy.utils._pageorder import BoundedPageDelivery, PageDeliveryStatus
from scrapy.utils.misc import build_from_crawler
from scrapy.utils.test import get_crawler
from tests.utils.decorators import coroutine_test

if TYPE_CHECKING:
    from pathlib import Path

    from scrapy import Spider


class _PageResponse:
    def __init__(self, page_index: int) -> None:
        self.meta = {"page_index": page_index}
        self.url = f"file:///example/page-{page_index}.html"


class _DiscardMarkerFilter:
    def __init__(self, feed_options: dict[str, Any] | None) -> None:
        self.feed_options = feed_options

    def accepts(self, item: Any) -> bool:
        return not item.get("discarded")


@coroutine_test
async def test_bounded_page_delivery_blocks_pages_outside_window() -> None:
    delivery: BoundedPageDelivery[str] = BoundedPageDelivery(capacity=2)
    await delivery.wait_for_space(1)
    delivery.open_page(1)
    delivery.add_item(1, "first-page", 0)

    await delivery.wait_for_space(2)
    delivery.open_page(2)
    delivery.add_item(2, "second-page", 0)
    delivery.complete_page(2)

    third_page = asyncio.create_task(delivery.submit_page(3, ["third-page"]))
    await asyncio.sleep(0)
    assert not third_page.done()
    assert len(delivery) == 2
    assert delivery.max_pending_seen == 2

    delivery.complete_page(1)
    await delivery.flush()
    await third_page

    assert [page_index for page_index, _ in delivery.delivered] == [1, 2, 3]
    assert len(delivery) == 0


@coroutine_test
async def test_feed_exporter_preserves_page_order_and_states(
    tmp_path: Path,
) -> None:
    feed_path = tmp_path / "page-feed.jsonl"
    crawler = get_crawler(
        settings_dict={
            "FEEDS": {
                feed_path: {
                    "format": "jsonlines",
                    "item_filter": _DiscardMarkerFilter,
                    "page_ordered": True,
                    "max_pending_pages": 5,
                }
            }
        }
    )
    exporter = build_from_crawler(FeedExporter, crawler)
    spider: Spider = scrapy.Spider.from_crawler(crawler, "page-order-spider")
    exporter.open_spider(spider)

    first_item_started = asyncio.Event()
    release_first_item = asyncio.Event()

    async def export_first_item() -> None:
        first_item_started.set()
        await release_first_item.wait()
        await exporter.item_scraped(
            {"id": "page-1-row-1"},
            spider,
            _PageResponse(1),
            output_index=0,
        )

    first_item = asyncio.create_task(export_first_item())
    await first_item_started.wait()
    await asyncio.sleep(0)

    await exporter.item_scraped(
        {"id": "page-1-row-2"}, spider, _PageResponse(1), output_index=1
    )

    await exporter.item_scraped(
        {"id": "page-2-row-1"}, spider, _PageResponse(2), output_index=0
    )
    await exporter._on_response_processed(_PageResponse(2), spider)

    await exporter.item_scraped(
        {"id": "page-3-filtered", "discarded": True},
        spider,
        _PageResponse(3),
        output_index=0,
    )
    await exporter._on_response_processed(_PageResponse(3), spider)

    await exporter.item_scraped(
        {"id": "page-4-row-1"}, spider, _PageResponse(4), output_index=0
    )
    exporter._on_spider_error(RuntimeError("parse failed"), _PageResponse(4), spider)
    await exporter._on_response_processed(_PageResponse(4), spider)

    await exporter.item_scraped(
        {"id": "page-5-row-1"}, spider, _PageResponse(5), output_index=0
    )
    await exporter._on_response_processed(_PageResponse(5), spider)

    assert not feed_path.exists()

    release_first_item.set()
    await first_item
    await exporter._on_response_processed(_PageResponse(1), spider)
    await exporter.close_spider(spider)

    rows = [json.loads(line)["id"] for line in feed_path.read_bytes().splitlines()]
    assert rows == [
        "page-1-row-1",
        "page-1-row-2",
        "page-2-row-1",
        "page-5-row-1",
    ]

    queue = next(iter(exporter._page_queues.values()))
    assert queue.states == {
        1: PageDeliveryStatus.SUCCESS,
        2: PageDeliveryStatus.SUCCESS,
        3: PageDeliveryStatus.EMPTY,
        4: PageDeliveryStatus.FAILED,
        5: PageDeliveryStatus.SUCCESS,
    }
    assert isinstance(queue.failures[4], RuntimeError)
    assert crawler.stats.get_value("feedexport/page_count/empty") == 1
    assert crawler.stats.get_value("feedexport/page_count/failed") == 1
