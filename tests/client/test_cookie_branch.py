"""正式抓取与预览分支共享同一 AsyncClient 的 Cookie 隔离与提交场景。

时间由 ``ControllableClock`` 驱动，响应完全本地生成，覆盖：

1. 根路径页位自然到期 + 预览收到服务端清除指令：清除单独入账，提交成功。
2. 根路径页位被正式会话续期后再自然到期：与预览的清除指令冲突，整笔回滚。
3. 对照组：同一条预览链去掉清除指令，自然到期不产生变更或冲突，正常提交。
"""

from __future__ import annotations

import pytest

import httpx
from cookie_branch import (
    BRANCH_EXTENSION_KEY,
    BranchingAsyncClientFactory,
    ControllableClock,
    CookieBranch,
)

BASE_URL = "http://example.org"


class DirectoryHandler:
    """模拟供应商目录服务，按路径与分支标记返回本地响应。"""

    def __init__(
        self,
        clock: ControllableClock,
        *,
        include_root_clear: bool,
        renew_root: bool,
    ) -> None:
        self.clock = clock
        self.include_root_clear = include_root_clear
        self.renew_root = renew_root

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        is_preview = request.extensions.get(BRANCH_EXTENSION_KEY) is not None

        if path == "/":
            headers = [
                ("set-cookie", "layout=compact; Path=/; Max-Age=100000"),
            ]
            # 第二轮：正式抓取在第 8 秒收到同值根路径页位，有效期续到第 15 秒。
            if self.renew_root and int(self.clock.now) == 8:
                headers.append(("set-cookie", "page=7; Path=/; Max-Age=7"))
            return httpx.Response(200, headers=headers)

        if path == "/region":
            if is_preview:
                # 第 11 秒：预览入口先跳转，不在本跳改动 Cookie。
                return httpx.Response(
                    307, headers={"location": f"{BASE_URL}/region/preview"}
                )
            return httpx.Response(200)

        if path == "/region/preview":
            # 第 12 秒：跳转响应把分区页位改为 9，并按需清除根路径页位。
            set_cookies = [
                ("set-cookie", "page=9; Path=/region; Max-Age=1000"),
            ]
            if self.include_root_clear:
                set_cookies.append(
                    ("set-cookie", "page=; Path=/; Max-Age=0"),
                )
            set_cookies.append(("location", f"{BASE_URL}/region/final"))
            return httpx.Response(307, headers=set_cookies)

        if path == "/region/final":
            return httpx.Response(200)

        raise AssertionError(f"unexpected path: {path}")  # pragma: no cover


def build_factory(
    clock: ControllableClock,
    *,
    include_root_clear: bool,
    renew_root: bool,
) -> BranchingAsyncClientFactory:
    handler = DirectoryHandler(
        clock,
        include_root_clear=include_root_clear,
        renew_root=renew_root,
    )
    return BranchingAsyncClientFactory(handler, clock)


def seed_initial_state(factory: BranchingAsyncClientFactory) -> None:
    # 第 0 秒：根路径 page=7 第 10 秒到期；/region 的 page=2 长期有效。
    factory.seed(
        [
            "page=7; Path=/; Max-Age=10",
            "page=2; Path=/region; Max-Age=100000",
        ],
        f"{BASE_URL}/",
    )


async def send(
    client: httpx.AsyncClient,
    path: str,
    *,
    branch: CookieBranch | None = None,
) -> httpx.Response:
    extensions = {}
    if branch is not None:
        extensions[BRANCH_EXTENSION_KEY] = branch
    request = httpx.Request("GET", f"{BASE_URL}{path}", extensions=extensions)
    return await client.send(request, follow_redirects=False)


def cookie_headers(
    factory: BranchingAsyncClientFactory,
    path: str,
    *,
    preview: bool,
) -> list[str | None]:
    return [
        request.headers.get("cookie")
        for is_preview, request in factory.transport.requests
        if is_preview == preview and request.url.path == path
    ]


def cookie_pairs(header: str | None) -> list[tuple[str, str]]:
    """解析 Cookie 头为有序 (名称, 值) 列表，允许同名不同 Path 的 Cookie。"""
    if header is None:
        return []
    return [
        tuple(part.strip().split("=", 1))  # type: ignore[return-value]
        for part in header.split(";")
    ]


def page_pairs(headers: list[str | None]) -> list[list[tuple[str, str]]]:
    return [
        [pair for pair in cookie_pairs(header) if pair[0] == "page"]
        for header in headers
    ]


def has_cookie(header: str | None, name: str, value: str) -> bool:
    return (name, value) in cookie_pairs(header)


async def run_preview_chain(
    client: httpx.AsyncClient,
    factory: BranchingAsyncClientFactory,
    clock: ControllableClock,
    branch: CookieBranch,
    *,
    root_page_live_at_entry: bool,
) -> None:
    # 第 11 秒：预览访问 /region。分区页位始终是 page=2；根路径页位是否
    # 同行取决于它在当前时钟下是否仍有效。
    clock.set(11)
    first = await send(client, "/region", branch=branch)
    assert first.status_code == 307
    entry_pages = page_pairs(cookie_headers(factory, "/region", preview=True))[-1]
    assert ("page", "2") in entry_pages
    if root_page_live_at_entry:
        assert ("page", "7") in entry_pages
    else:
        # 根路径页位已自然到期，请求中只有该分区的页位。
        assert entry_pages == [("page", "2")]

    # 第 12 秒：跟随跳转，响应把分区页位改为 9（并按需清除根路径页位）。
    clock.set(12)
    second = await client.send(first.next_request, follow_redirects=False)
    assert second.status_code == 307

    # 两边请求交错：正式请求仍用主会话的分区页位 2。
    formal_region = await send(client, "/region")
    assert formal_region.status_code == 200
    formal_pages = page_pairs(cookie_headers(factory, "/region", preview=False))[-1]
    assert ("page", "2") in formal_pages
    assert ("page", "9") not in formal_pages

    # 预览继续跟跳时应带分区页位 9，且不再带根路径页位。
    final = await client.send(second.next_request, follow_redirects=False)
    assert final.status_code == 200
    final_pages = page_pairs(cookie_headers(factory, "/region/final", preview=True))[-1]
    assert final_pages == [("page", "9")]


@pytest.mark.anyio
async def test_round_one_natural_expiry_and_server_clear_commit() -> None:
    clock = ControllableClock()
    factory = build_factory(clock, include_root_clear=True, renew_root=False)
    seed_initial_state(factory)

    async with factory.client() as client:
        # 第 5 秒：正式抓取根路径，收到无关的 layout=compact。
        clock.set(5)
        response = await send(client, "/")
        assert response.status_code == 200
        assert page_pairs(cookie_headers(factory, "/", preview=False))[-1] == [
            ("page", "7")
        ]

        branch = factory.branch()
        await run_preview_chain(
            client, factory, clock, branch, root_page_live_at_entry=False
        )

        # 提交前再发正式请求：根路径页位第 10 秒已自然到期，不再发送。
        clock.set(19)
        await send(client, "/")
        root_headers = cookie_headers(factory, "/", preview=False)
        assert not any(pair[0] == "page" for pair in cookie_pairs(root_headers[-1]))
        assert has_cookie(root_headers[-1], "layout", "compact")
        await send(client, "/region")
        assert ("page", "2") in page_pairs(
            cookie_headers(factory, "/region", preview=False)
        )[-1]

        # 第 20 秒提交预览变更。
        clock.set(20)
        report = branch.commit(factory.main_jar)

    assert report.applied is True
    assert [cookie.path for cookie in report.server_cleared] == ["/"]
    assert report.server_cleared[0].name == "page"
    assert report.server_cleared[0].value == ""
    assert [cookie.path for cookie in report.natural_expirations] == ["/"]
    assert report.natural_expirations[0].value == "7"
    assert [(cookie.path, cookie.value) for cookie in report.applied_cookies] == [
        ("/region", "9")
    ]
    assert report.conflicts == []

    # 提交后正式抓取使用分区页位 9，并保留 layout=compact。
    async with factory.client() as client:
        clock.set(21)
        await send(client, "/region")
        region_header = cookie_headers(factory, "/region", preview=False)[-1]
        assert "page=9" in region_header
        assert "layout=compact" in region_header
        await send(client, "/")
        assert not page_pairs(cookie_headers(factory, "/", preview=False))[-1]


@pytest.mark.anyio
async def test_round_two_renewal_conflicts_with_server_clear() -> None:
    clock = ControllableClock()
    factory = build_factory(clock, include_root_clear=True, renew_root=True)
    seed_initial_state(factory)

    async with factory.client() as client:
        # 第 8 秒：正式抓取收到同值根路径页位，有效期续到第 15 秒。
        clock.set(8)
        await send(client, "/")
        assert page_pairs(cookie_headers(factory, "/", preview=False))[-1] == [
            ("page", "7")
        ]

        # 续期后、到期前仍会发送根路径页位。
        clock.set(14)
        await send(client, "/")
        assert ("page", "7") in page_pairs(cookie_headers(factory, "/", preview=False))[
            -1
        ]

        branch = factory.branch()
        await run_preview_chain(
            client, factory, clock, branch, root_page_live_at_entry=True
        )

        # 第 16 秒：续期后的根路径页位也已自然到期，提交时两边都没有这条 Cookie。
        clock.set(16)
        await send(client, "/")
        root_headers = cookie_headers(factory, "/", preview=False)
        assert not page_pairs(root_headers)[-1]
        assert has_cookie(root_headers[-1], "layout", "compact")

        clock.set(20)
        report = branch.commit(factory.main_jar)

    assert report.applied is False
    assert len(report.conflicts) == 1
    conflict = report.conflicts[0]
    assert conflict.name == "page"
    assert conflict.path == "/"
    assert conflict.server_cleared is True
    assert conflict.current_cookie is not None
    assert conflict.current_cookie.value == "7"
    assert conflict.current_cookie.expires == 15
    # 整笔不生效：分区页位仍是 2，layout 保留。
    assert [cookie.path for cookie in report.server_cleared] == ["/"]

    async with factory.client() as client:
        clock.set(21)
        await send(client, "/region")
        region_header = cookie_headers(factory, "/region", preview=False)[-1]
        assert "page=2" in region_header
        assert "layout=compact" in region_header


@pytest.mark.anyio
async def test_round_three_without_clear_instruction_is_clean_commit() -> None:
    clock = ControllableClock()
    factory = build_factory(clock, include_root_clear=False, renew_root=False)
    seed_initial_state(factory)

    async with factory.client() as client:
        clock.set(5)
        await send(client, "/")

        branch = factory.branch()
        await run_preview_chain(
            client, factory, clock, branch, root_page_live_at_entry=False
        )

        clock.set(19)
        await send(client, "/")
        assert not page_pairs(cookie_headers(factory, "/", preview=False))[-1]

        clock.set(20)
        report = branch.commit(factory.main_jar)

    assert report.applied is True
    assert report.conflicts == []
    # 没有清除指令：根路径只记自然到期，不产生服务端清除或冲突。
    assert report.server_cleared == []
    assert [(cookie.path, cookie.value) for cookie in report.natural_expirations] == [
        ("/", "7")
    ]
    assert [(cookie.path, cookie.value) for cookie in report.applied_cookies] == [
        ("/region", "9")
    ]

    async with factory.client() as client:
        clock.set(21)
        await send(client, "/region")
        region_header = cookie_headers(factory, "/region", preview=False)[-1]
        assert "page=9" in region_header
        assert "layout=compact" in region_header
