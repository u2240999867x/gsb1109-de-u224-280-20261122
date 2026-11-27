"""Cookie 分支会话：在同一个 ``httpx.AsyncClient`` 上隔离正式抓取与预览。

预览请求通过 ``extensions={"cookie_branch": branch}`` 标记，传输层据此把
请求期间的 Cookie 读写导向分支 CookieJar，正式会话的 CookieJar 不受影响。
预览确认后调用 :meth:`CookieBranch.commit`，按 Cookie 槽位（域/路径/名称）
比对正式会话在分支创建后的更新，区分自然到期与服务端清除指令，再决定整笔
提交或报告冲突。

标准库 :class:`http.cookiejar.CookieJar` 的两点行为在这里被显式改造：

* ``add_cookie_header`` 结束时会物理删除已到期 Cookie。分支会话需要把
  “没发清除指令的自然到期”和“服务端要求清除”分开核对，因此覆盖
  :meth:`ClockedCookieJar.clear_expired_cookies` 为空操作，到期项继续保留在
  内部字典里，发送侧仍由 ``DefaultCookiePolicy.return_ok_expires`` 过滤，不会
  被发回服务端。
* ``Set-Cookie: name=; Max-Age=0`` 在解析阶段就被物理删除且不留下任何痕迹。
  提取响应前先识别这类清除指令，提取后把它作为 tombstone（空值、立即过期的
  Cookie）放回同一槽位，使清除指令可核对、可参与冲突检测。

时钟通过临时替换 ``http.cookiejar`` 模块级 ``time`` 绑定实现。替换只发生在
``set_cookie_header``/``extract_cookies`` 这两段同步代码内，其间没有 await
挂起点，因此同一事件循环里交错的正式请求与预览请求不会互相串时；该实现不
适用于跨线程共享同一进程内 CookieJar 的场景。
"""

from __future__ import annotations

import copy
from http import cookiejar as _cookiejar_module
from http.cookiejar import Cookie, CookieJar
from types import ModuleType
from typing import Any, Callable, Iterable

import httpx

# 传输层与测试代码约定的请求扩展键。
BRANCH_EXTENSION_KEY = "cookie_branch"

# 直接引用 http.cookiejar 的模块对象，替换其 time 绑定即影响全部 CookieJar。
# 标注为 Any：time 绑定与若干解析函数不在公开类型存根中。
cookiejar_module: Any = _cookiejar_module


class ControllableClock:
    """可被测试驱动的单调时钟，单位为秒（浮点）。"""

    def __init__(self, now: float = 0.0) -> None:
        self.now = float(now)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def set(self, now: float) -> None:
        self.now = float(now)


def cookiejar_time_module(clock: Callable[[], float]) -> ModuleType:
    """构造一个只替换 ``time`` 的假 time 模块，其余属性透传真模块。"""
    import time as real_time

    class _FakeTime(ModuleType):
        def __getattr__(self, name: str) -> Any:
            return getattr(real_time, name)

    fake = _FakeTime(real_time.__name__)
    fake.time = clock  # type: ignore[attr-defined]
    return fake


class ClockedCookieJar(CookieJar):
    """到期判定使用外部时钟而非系统时间，且保留到期项与清除 tombstone。"""

    def __init__(
        self,
        clock: Callable[[], float],
        policy: Any = None,
    ) -> None:
        super().__init__(policy=policy)
        self._clock = clock
        self._cookiejar_time = cookiejar_time_module(clock)

    @property
    def policy(self) -> Any:
        return self._policy  # type: ignore[attr-defined]

    def _with_clock(self, operation: Callable[[], None]) -> None:
        original_time = cookiejar_module.time
        cookiejar_module.time = self._cookiejar_time
        try:
            operation()
        finally:
            cookiejar_module.time = original_time

    def extract_cookies(self, response: Any, request: Any) -> None:
        # httpx 传入的是 _CookieCompatResponse/_CookieCompatRequest 包装对象。
        raw_response = getattr(response, "response", response)
        raw_request = getattr(request, "request", request)
        clears = parse_clear_directives(raw_response, raw_request, self._clock())

        def run() -> None:
            super(ClockedCookieJar, self).extract_cookies(response, request)
            # 标准库解析到 Max-Age=0 / 过去 Expires 时直接物理删除，
            # 这里把清除指令作为 tombstone 放回同一槽位。
            for cookie in clears:
                self.set_cookie(cookie)

        self._with_clock(run)

    def add_cookie_header(self, request: Any) -> None:
        self._with_clock(lambda: CookieJar.add_cookie_header(self, request))

    def clear_expired_cookies(self) -> None:
        # 保留自然到期项，供提交阶段核对；发送过滤不受影响。
        return None

    def iter_all(self) -> Iterable[Cookie]:
        """迭代所有 Cookie，包括到期项与清除 tombstone。"""
        for paths in self._cookies.values():  # type: ignore[attr-defined]
            for cookies in paths.values():
                yield from cookies.values()


def parse_clear_directives(
    response: httpx.Response,
    request: httpx.Request,
    now: float,
) -> list[Cookie]:
    """从响应的 Set-Cookie 头中解析服务端清除指令（空值且立即/过去过期）。"""
    headers = response.headers.get_list("set-cookie")
    directives: list[Cookie] = []
    for header in headers:
        directives.extend(cookies_from_set_cookie(header, request, now))
    return [cookie for cookie in directives if is_server_clear(cookie)]


def cookies_from_set_cookie(
    header: str,
    request: httpx.Request,
    now: float,
) -> list[Cookie]:
    """复用标准库的 Netscape 头解析，得到该 Set-Cookie 对应的 Cookie。"""
    # 这些解析函数存在于运行时但没有公开类型存根，cookiejar_module 标注为 Any。
    http2time = cookiejar_module.http2time
    parse_ns_headers = cookiejar_module.parse_ns_headers
    strip_quotes = cookiejar_module.strip_quotes

    parsed = parse_ns_headers([header])
    if not parsed:
        return []
    pairs = parsed[0]
    name, value = pairs[0]

    max_age_set = False
    expires: int | None = None
    domain: str | None = None
    path: str | None = None
    secure = False
    rest: dict[str, str] = {}
    for key, raw in pairs[1:]:
        if key == "max-age":
            max_age_set = True
            try:
                expires = int(now) + int(raw)
            except (TypeError, ValueError):
                return []
        elif key == "expires" and not max_age_set:
            expires = http2time(strip_quotes(raw)) if raw is not None else None
        elif key == "domain":
            domain = raw
        elif key == "path":
            path = raw
        elif key == "secure":
            secure = True
        else:
            rest[key] = "" if raw is None else raw

    domain_specified = domain is not None
    path_specified = path is not None
    if domain is None:
        domain = request.url.host
    if not path:
        path = request.url.path

    return [
        Cookie(
            version=0,
            name=name,
            value=value,
            port=None,
            port_specified=False,
            domain=domain,
            domain_specified=domain_specified,
            domain_initial_dot=domain.startswith("."),
            path=path,
            path_specified=path_specified,
            secure=secure,
            expires=expires,
            discard=expires is None,
            comment=None,
            comment_url=None,
            rest=rest,
            rfc2109=False,
        )
    ]


class CookieConflict:
    """单个 Cookie 槽位上的提交冲突说明。"""

    def __init__(
        self,
        cookie: Cookie,
        reason: str,
        *,
        server_cleared: bool = False,
        base_cookie: Cookie | None = None,
        current_cookie: Cookie | None = None,
    ) -> None:
        self.cookie = cookie
        self.reason = reason
        self.server_cleared = server_cleared
        self.base_cookie = base_cookie
        self.current_cookie = current_cookie

    @property
    def name(self) -> str:
        return self.cookie.name

    @property
    def path(self) -> str:
        return self.cookie.path

    @property
    def domain(self) -> str:
        return self.cookie.domain

    def __repr__(self) -> str:
        return (
            f"CookieConflict(name={self.cookie.name!r}, path={self.cookie.path!r}, "
            f"reason={self.reason!r})"
        )


class CommitReport:
    """一次预览提交的核对结果。"""

    def __init__(
        self,
        *,
        applied: bool,
        applied_cookies: list[Cookie] | None = None,
        server_cleared: list[Cookie] | None = None,
        natural_expirations: list[Cookie] | None = None,
        conflicts: list[CookieConflict] | None = None,
    ) -> None:
        self.applied = applied
        self.applied_cookies = applied_cookies or []
        self.server_cleared = server_cleared or []
        self.natural_expirations = natural_expirations or []
        self.conflicts = conflicts or []

    def __repr__(self) -> str:
        return (
            f"CommitReport(applied={self.applied}, "
            f"applied={len(self.applied_cookies)}, "
            f"server_cleared={len(self.server_cleared)}, "
            f"natural_expirations={len(self.natural_expirations)}, "
            f"conflicts={len(self.conflicts)})"
        )


class CookieBranch:
    """一次预览会话的私有 Cookie 分支。"""

    def __init__(self, clock: Callable[[], float], base_jar: ClockedCookieJar) -> None:
        self._clock = clock
        self.jar = ClockedCookieJar(clock, policy=base_jar.policy)
        # 分叉时刻的完整快照（含到期项），作为后续比对的基线。
        for cookie in base_jar.iter_all():
            self.jar.set_cookie(copy.deepcopy(cookie))
        self._baseline = {
            slot_key(cookie): copy.deepcopy(cookie) for cookie in self.jar.iter_all()
        }
        self._closed = False

    @property
    def cookies(self) -> httpx.Cookies:
        return httpx.Cookies(self.jar)

    def commit(self, target: ClockedCookieJar) -> CommitReport:
        """把分支上的 Cookie 变更提交到正式会话。

        任一分支变更与正式会话的后续更新冲突，则整笔不生效。
        自然到期（没有清除指令）只做核对记录，不产生变更也不产生冲突；
        服务端清除指令（空值且立即过期）单独列示，并参与冲突检测。
        """
        if self._closed:
            raise RuntimeError("Cookie branch has already been committed or discarded.")

        current = {slot_key(cookie): cookie for cookie in target.iter_all()}
        branch = {slot_key(cookie): cookie for cookie in self.jar.iter_all()}
        now = int(self._clock())

        applied: list[Cookie] = []
        server_cleared: list[Cookie] = []
        natural_expirations: list[Cookie] = []
        conflicts: list[CookieConflict] = []

        for key, base_cookie in self._baseline.items():
            preview_cookie = branch.get(key)
            live_cookie = current.get(key)
            base_expired = base_cookie.is_expired(now)

            if preview_cookie is not None and is_server_clear(preview_cookie):
                # 预览收到明确的清除指令，与“没发指令的自然到期”分开核对。
                server_cleared.append(preview_cookie)
                if base_expired:
                    natural_expirations.append(base_cookie)
                # 冲突判据：清除指令生效的时刻，正式会话仍持有同槽位且
                # 有效期更晚（或为会话型）的 Cookie。第一轮根路径页位在清除
                # 前已自然到期（expires 更早），不冲突；第二轮被续到清除时刻
                # 之后，即使提交时也已到期，仍要报告冲突。
                clear_at = preview_cookie.expires or 0
                if live_cookie is not None and not is_server_clear(live_cookie):
                    outlives_clear = (
                        live_cookie.expires is None or live_cookie.expires > clear_at
                    )
                    if outlives_clear:
                        conflicts.append(
                            CookieConflict(
                                preview_cookie,
                                reason=(
                                    "server clear conflicts with a main-session "
                                    "cookie that outlives the clear directive"
                                ),
                                server_cleared=True,
                                base_cookie=base_cookie,
                                current_cookie=live_cookie,
                            )
                        )
                continue

            if base_expired:
                # 基线在分叉时已到期且预览未发清除指令：纯自然到期。
                natural_expirations.append(base_cookie)
                continue

            if preview_cookie is None:
                natural_expirations.append(base_cookie)
                continue

            if cookies_equal(preview_cookie, base_cookie):
                continue

            # 预览更新了该槽位；正式会话同时也更新过即冲突。
            if live_cookie is not None and not cookies_equal(live_cookie, base_cookie):
                conflicts.append(
                    CookieConflict(
                        preview_cookie,
                        reason=(
                            "preview update conflicts with a later main-session "
                            "update of the same cookie"
                        ),
                        base_cookie=base_cookie,
                        current_cookie=live_cookie,
                    )
                )
            else:
                applied.append(preview_cookie)

        # 预览期间新出现的槽位（基线中没有）。
        for key, preview_cookie in branch.items():
            if key in self._baseline or is_server_clear(preview_cookie):
                continue
            live_cookie = current.get(key)
            if live_cookie is not None and not is_server_clear(live_cookie):
                conflicts.append(
                    CookieConflict(
                        preview_cookie,
                        reason=(
                            "new preview cookie conflicts with a main-session cookie "
                            "in the same slot"
                        ),
                        current_cookie=live_cookie,
                    )
                )
            else:
                applied.append(preview_cookie)

        if conflicts:
            self._closed = True
            return CommitReport(
                applied=False,
                server_cleared=server_cleared,
                natural_expirations=natural_expirations,
                conflicts=conflicts,
            )

        for cookie in applied:
            target.set_cookie(copy.deepcopy(cookie))
        for cookie in server_cleared:
            target.clear(cookie.domain, cookie.path, cookie.name)

        self._closed = True
        return CommitReport(
            applied=True,
            applied_cookies=applied,
            server_cleared=server_cleared,
            natural_expirations=natural_expirations,
        )

    def discard(self) -> None:
        self._closed = True


def slot_key(cookie: Cookie) -> tuple[str, str, str]:
    return cookie.domain, cookie.path, cookie.name


def cookies_equal(left: Cookie, right: Cookie) -> bool:
    return (
        left.name == right.name
        and left.value == right.value
        and left.domain == right.domain
        and left.path == right.path
        and left.expires == right.expires
        and left.secure == right.secure
    )


def is_server_clear(cookie: Cookie) -> bool:
    """空值且带过期时间（Max-Age=0 / 过去 Expires）属于服务端清除指令。"""
    return cookie.value == "" and cookie.expires is not None


class BranchingMockTransport(httpx.AsyncBaseTransport):
    """按请求扩展把 Cookie 读写分流的 MockTransport。

    正式请求使用主 CookieJar；带 ``cookie_branch`` 扩展的预览请求使用分支
    CookieJar。处理器与 :class:`httpx.MockTransport` 一样接收 Request 返回
    Response，可在同一份本地响应脚本上驱动两条交错的会话。
    """

    def __init__(
        self,
        handler: Callable[[httpx.Request], httpx.Response],
        main_jar: ClockedCookieJar,
    ) -> None:
        self._handler = handler
        self._main_jar = main_jar
        # 每条记录为 (是否预览请求, Request)，供测试核对两边实际发送的页位。
        self.requests: list[tuple[bool, httpx.Request]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        branch: CookieBranch | None = request.extensions.get(BRANCH_EXTENSION_KEY)
        jar = branch.jar if branch is not None else self._main_jar
        is_preview = branch is not None
        # 重定向请求会带着上一跳的 Cookie 头，先移除再由对应 CookieJar 重注入。
        request.headers.pop("cookie", None)
        httpx.Cookies(jar).set_cookie_header(request)
        self.requests.append((is_preview, request))
        response = self._handler(request)
        response.request = request
        httpx.Cookies(jar).extract_cookies(response)
        return response


class BranchingAsyncClientFactory:
    """封装共享传输层与主 CookieJar 的创建入口。"""

    def __init__(
        self,
        handler: Callable[[httpx.Request], httpx.Response],
        clock: ControllableClock,
    ) -> None:
        self.clock = clock
        self.main_jar = ClockedCookieJar(clock)
        self.transport = BranchingMockTransport(handler, self.main_jar)

    def client(self, **kwargs: Any) -> httpx.AsyncClient:
        kwargs.setdefault("transport", self.transport)
        kwargs.setdefault("follow_redirects", True)
        return httpx.AsyncClient(**kwargs)

    def branch(self) -> CookieBranch:
        return CookieBranch(self.clock, self.main_jar)

    def seed(self, set_cookies: Iterable[str], url: str) -> None:
        """通过本地 Set-Cookie 响应向主会话写入初始 Cookie。"""
        headers = [("set-cookie", value) for value in set_cookies]
        response = httpx.Response(
            200,
            headers=headers,
            request=httpx.Request("GET", url),
        )
        httpx.Cookies(self.main_jar).extract_cookies(response)
