from __future__ import annotations

import asyncio
from collections import Counter
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlparse

import pytest

from scrapy.core.downloader.handlers.base import BaseDownloadHandler
from scrapy.entrymode import EntryCollection, EntryModeSpider
from scrapy.http import TextResponse
from scrapy.spiders import Spider
from scrapy.utils.test import get_crawler
from tests.utils.decorators import coroutine_test

if TYPE_CHECKING:
    from pathlib import Path


class SiteState:
    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.referers: list[bytes | None] = []
        self.both_at_x = asyncio.Event()
        self.retry_queued = asyncio.Event()


class FakeSiteHandler(BaseDownloadHandler):
    state: ClassVar[SiteState]

    async def download_request(self, request):
        path = urlparse(request.url).path
        self.state.counts[path] += 1
        self.state.referers.append(request.headers.get("Referer"))
        status = 200
        body = b""
        headers: dict[str, str] = {}

        if path == "/c":
            status, headers["Location"] = 302, "/y"
        elif path == "/a":
            status, headers["Location"] = 302, "/x"
        elif path == "/b":
            status, headers["Location"] = 302, "/relay"
        elif path in {"/relay", "/d"}:
            status, headers["Location"] = 302, "/x"
        elif path == "/x":
            hit = self.state.counts[path]
            if hit == 1:
                await asyncio.wait_for(self.state.both_at_x.wait(), timeout=5)
                status, body = 503, b"busy"
            elif hit == 2:
                status, headers["Location"] = 302, "/y"
            else:
                status, headers["Location"] = 302, "/z"
        elif path == "/y":
            body = "<title>早间通告</title><p>morning</p>".encode()
        elif path == "/z":
            body = "<title>更正通告</title><p>correction</p>".encode()
        elif path == "/paper":
            body = "<title>同址约定</title>".encode()
        else:
            status, body = 404, b"missing"

        return TextResponse(
            request.url,
            status=status,
            headers=headers,
            body=body,
            request=request,
        )


class ScenarioSpider(EntryModeSpider, Spider):
    name = "entry-scenario"
    parse_calls: ClassVar[Counter[str]] = Counter()

    def __init__(self, specs: list[dict[str, Any]], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.specs = specs

    def start_requests(self):
        return self.entry_start_requests(self.specs)

    def parse_entry(self, response):
        title = (response.xpath("//title/text()").get() or "").strip()
        parser = response.meta.get("entry_parser", "default")
        self.parse_calls[parser] += 1
        data: dict[str, Any] = {"title": title, "url": response.url}
        if parser == "length":
            data["title_length"] = len(title)
        else:
            data["normalized_title"] = title.casefold()
        self.save_entry_article(response, data)
        yield {"entry_id": response.meta["entry_id"], **data}


def settings(jobdir: Path) -> dict[str, Any]:
    return {
        "JOBDIR": str(jobdir),
        "ENTRY_SHARING_ENABLED": True,
        "HTTP_CACHE_ENABLED": False,
        "REFERER_ENABLED": False,
        "REDIRECT_MAX_TIMES": 2,
        "RETRY_TIMES": 1,
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0,
        "LOG_LEVEL": "ERROR",
        "TELNETCONSOLE_ENABLED": False,
        "DOWNLOAD_HANDLERS": {"http": FakeSiteHandler},
    }


def specs_for(issue: str, base: str, *entry_ids: str) -> list[dict[str, Any]]:
    return [
        {
            "issue": issue,
            "entry_id": entry_id,
            "url": f"{base}/{entry_id.lower()}",
            "parser": "notice",
            "max_redirects": 2,
        }
        for entry_id in entry_ids
    ]


@pytest.mark.parametrize(("first", "second"), [("A", "B"), ("B", "A")])
@coroutine_test
async def test_full_restart_scenario(  # noqa: PLR0915
    tmp_path: Path, first: str, second: str
) -> None:
    state = SiteState()
    FakeSiteHandler.state = state
    ScenarioSpider.parse_calls.clear()
    jobdir = tmp_path / "job"
    issue = "issue-1"
    base = "http://bulletin.example"
    disk = EntryCollection(jobdir)

    crawler_c = get_crawler(ScenarioSpider, settings(jobdir))
    await crawler_c.crawl_async(specs=specs_for(issue, base, "C"))
    record_c = disk.get(issue, "C")
    assert record_c["status"] == "completed"
    assert record_c["result"]["title"] == "早间通告"
    assert state.counts["/y"] == 1

    ab_specs = specs_for(issue, base, first, second)
    crawler_ab = get_crawler(ScenarioSpider, settings(jobdir))
    collection = EntryCollection.for_crawler(crawler_ab)

    def pause_after_retry(attempt, request) -> None:
        assert crawler_ab.engine is not None
        crawler_ab.engine.pause()
        state.retry_queued.set()

    collection.before_retry_return = pause_after_retry
    ab_task = asyncio.create_task(crawler_ab.crawl_async(specs=ab_specs))

    for _ in range(500):
        attempts = collection.state["issues"].get(issue, {}).get("attempts", {})
        if any(
            set(item.get("waiters", [])) >= {"A", "B"} for item in attempts.values()
        ):
            state.both_at_x.set()
            break
        await asyncio.sleep(0.01)
    assert state.both_at_x.is_set()

    await asyncio.wait_for(state.retry_queued.wait(), timeout=5)
    for _ in range(100):
        assert crawler_ab.engine is not None
        if (
            crawler_ab.engine._slot is not None
            and len(crawler_ab.engine._slot.scheduler) >= 1
        ):
            break
        await asyncio.sleep(0.01)
    scheduler_size = len(crawler_ab.engine._slot.scheduler)
    assert scheduler_size == 1
    await crawler_ab.stop_async()
    await ab_task

    assert state.counts["/x"] == 1

    crawler_restart = get_crawler(ScenarioSpider, settings(jobdir))
    await crawler_restart.crawl_async(specs=ab_specs)

    record_a = disk.get(issue, "A")
    record_b = disk.get(issue, "B")
    assert record_a["status"] == "completed"
    assert record_a["resubmissions"] == 1
    assert record_a["result"]["title"] == "早间通告"
    assert record_a["article_ref"]["entry_id"] == "C"
    assert record_b["status"] == "failed"
    assert record_b["error"]["type"] == "redirect_limit"
    assert record_b["error"]["location"].endswith("/y")
    assert [urlparse(url).path for url in record_a["visited_urls"]] == [
        "/a",
        "/x",
        "/y",
    ]
    assert [urlparse(url).path for url in record_a["visited_urls"][:2]] == [
        "/a",
        "/x",
    ]
    assert [urlparse(url).path for url in record_b["visited_urls"]] == [
        "/b",
        "/relay",
        "/x",
    ]
    for record in (record_a, record_b):
        assert [
            event["status"] for event in record["responses"] if event.get("shared")
        ] == [503]
        assert len(record["retries"]) == 1
        assert record["retries"][0]["reason"].startswith("503")
        assert record["retries"][0]["attempt_number"] == 2
    assert record_b["redirects"][-1]["location"] == "/y"
    assert state.counts["/y"] == 1
    assert state.counts["/x"] == 2
    assert ScenarioSpider.parse_calls["notice"] == 1

    crawler_d = get_crawler(ScenarioSpider, settings(jobdir))
    await crawler_d.crawl_async(specs=specs_for(issue, base, "D"))
    record_d = disk.get(issue, "D")
    assert record_d["status"] == "completed"
    assert record_d["result"]["title"] == "更正通告"
    assert disk.get(issue, "A")["status"] == "completed"
    assert disk.get(issue, "B")["status"] == "failed"
    assert disk.get(issue, "C")["status"] == "completed"
    assert state.counts["/z"] == 1

    convention_specs = [
        {
            "issue": issue,
            "entry_id": "E",
            "url": f"{base}/paper",
            "parser": "text",
            "max_redirects": 2,
        },
        {
            "issue": issue,
            "entry_id": "F",
            "url": f"{base}/paper",
            "parser": "length",
            "max_redirects": 2,
        },
    ]
    crawler_conventions = get_crawler(ScenarioSpider, settings(jobdir))
    await crawler_conventions.crawl_async(specs=convention_specs)
    record_e = disk.get(issue, "E")
    record_f = disk.get(issue, "F")
    assert record_e["status"] == record_f["status"] == "completed"
    assert record_e["result"]["normalized_title"] == "同址约定"
    assert record_f["result"]["title_length"] == 4
    assert (
        record_e["article_ref"]["article_key"] != record_f["article_ref"]["article_key"]
    )
    assert state.counts["/paper"] == 2
    assert all(referer is None for referer in state.referers)
