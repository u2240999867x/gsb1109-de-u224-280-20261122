from __future__ import annotations

from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from enum import StrEnum
from inspect import isawaitable
from typing import Any, Generic, TypeVar

from twisted.internet.defer import Deferred

from scrapy.utils.defer import maybe_deferred_to_future

_T = TypeVar("_T")


class PageDeliveryStatus(StrEnum):
    LOADING = "loading"
    SUCCESS = "success"
    EMPTY = "empty"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass
class PageReport(Generic[_T]):
    index: int
    status: PageDeliveryStatus
    item_count: int = 0
    error: Any | None = None


@dataclass
class _PageState(Generic[_T]):
    groups: dict[int, list[_T]] = field(default_factory=dict)
    settled: bool = False
    failed: bool = False
    error: Any | None = None
    next_position: int = 0

    @property
    def item_count(self) -> int:
        return sum(len(group) for group in self.groups.values())


class BoundedPageDelivery(Generic[_T]):
    """Buffer completed page results and release them in page order.

    *capacity* is the number of page slots in the contiguous window, including
    the next page to deliver. Producers that get more than *capacity* pages
    ahead wait until an earlier page is settled and released.
    """

    def __init__(
        self,
        capacity: Any = 2,
        *,
        start_index: int = 1,
        writer: Any | None = None,
        max_pending_pages: int | None = None,
        max_buffered_pages: int | None = None,
        budget: int | None = None,
        buffer_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        budget_value = next(
            (
                value
                for value in (
                    max_pending_pages,
                    max_buffered_pages,
                    budget,
                    buffer_size,
                    kwargs.get("max_pages"),
                    kwargs.get("window_size"),
                    kwargs.get("page_budget"),
                )
                if value is not None
            ),
            None,
        )
        resolved_capacity: int
        if budget_value is not None:
            resolved_capacity = int(budget_value)
        elif callable(capacity):
            writer = capacity
            resolved_capacity = 2
        elif isinstance(capacity, int):
            resolved_capacity = capacity
        else:
            resolved_capacity = 2
        if resolved_capacity < 1:
            msg = "capacity must be at least 1"
            raise ValueError(msg)
        self.capacity: int = resolved_capacity
        self.start_index = start_index
        self.next_index = start_index
        self.writer = writer
        self.pages: dict[int, _PageState[_T]] = {}
        self.reports: dict[int, PageReport[_T]] = {}
        self.delivered: list[tuple[int, list[list[_T]]]] = []
        self.failures: dict[int, Any] = {}
        self.max_pending_seen = 0
        self._waiters: deque[Deferred[None]] = deque()

    @property
    def max_pending_pages(self) -> int:
        return self.capacity

    @property
    def pending_count(self) -> int:
        return len(self.pages)

    @property
    def buffered_pages(self) -> int:
        return len(self.pages)

    def __len__(self) -> int:
        return len(self.pages)

    def status(self, index: int) -> PageDeliveryStatus:
        report = self.reports.get(index)
        if report is not None:
            return report.status
        if index in self.pages:
            return PageDeliveryStatus.LOADING
        return PageDeliveryStatus.UNKNOWN

    @property
    def states(self) -> dict[int, PageDeliveryStatus]:
        result = {index: report.status for index, report in self.reports.items()}
        result.update(
            {
                index: PageDeliveryStatus.LOADING
                for index in self.pages
                if index not in self.reports
            }
        )
        return result

    def has_space(self, index: int) -> bool:
        if index < self.next_index or index in self.pages:
            return True
        return index - self.next_index < self.capacity

    async def wait_for_space(self, index: int) -> None:
        while not self.has_space(index):
            waiter: Deferred[None] = Deferred()
            self._waiters.append(waiter)
            try:
                await maybe_deferred_to_future(waiter)
            except BaseException:
                if not waiter.called:
                    with suppress(ValueError):
                        self._waiters.remove(waiter)
                raise

    def open_page(self, index: int) -> _PageState[_T]:
        state = self.pages.get(index)
        if state is None:
            state = _PageState()
            self.pages[index] = state
            self.max_pending_seen = max(self.max_pending_seen, len(self.pages))
        return state

    def add_outputs(self, index: int, position: int | None, outputs: list[_T]) -> None:
        if self.reports.get(index) is not None:
            return
        state = self.open_page(index)
        if state.settled:
            return
        if position is None:
            position = state.next_position
        state.next_position = max(state.next_position, position + 1)
        state.groups[position] = outputs

    def add_item(self, index: int, item: _T, position: int | None = None) -> None:
        self.add_outputs(index, position, [item])

    def complete_page(self, index: int) -> None:
        state = self.pages.get(index)
        if state is None:
            state = self.open_page(index)
        if not state.failed:
            state.settled = True

    def fail_page(self, index: int, error: Any | None = None) -> None:
        state = self.pages.get(index)
        if state is None:
            state = self.open_page(index)
        state.failed = True
        state.settled = True
        state.error = error

    def complete_all(self) -> None:
        for state in self.pages.values():
            if not state.failed:
                state.settled = True

    def is_failed(self, index: int) -> bool:
        report = self.reports.get(index)
        if report is not None:
            return report.status is PageDeliveryStatus.FAILED
        state = self.pages.get(index)
        return state is not None and state.failed

    async def flush(self, writer: Any | None = None) -> list[PageReport[_T]]:
        write = writer or self.writer
        flushed_reports: list[PageReport[_T]] = []
        while True:
            state = self.pages.get(self.next_index)
            if state is None or not state.settled:
                break

            index = self.next_index
            if state.failed:
                error = state.error
                self.failures[index] = error
                self.reports[index] = PageReport(
                    index=index,
                    status=PageDeliveryStatus.FAILED,
                    item_count=state.item_count,
                    error=error,
                )
            else:
                groups = [state.groups[position] for position in sorted(state.groups)]
                item_count = sum(len(group) for group in groups)
                exported_count: int | None = item_count
                if write is not None:
                    result: Any = write(index, groups)
                    if isawaitable(result):
                        result = await result
                    if isinstance(result, int):
                        exported_count = result
                else:
                    self.delivered.append((index, groups))
                self.reports[index] = PageReport(
                    index=index,
                    status=(
                        PageDeliveryStatus.SUCCESS
                        if exported_count
                        else PageDeliveryStatus.EMPTY
                    ),
                    item_count=exported_count or 0,
                )

            flushed_reports.append(self.reports[index])
            del self.pages[index]
            self.next_index += 1
            self._notify_waiters()
        return flushed_reports

    async def submit_page(
        self,
        index: int,
        items: list[_T] | None = None,
        *,
        outputs: list[list[_T]] | None = None,
        error: Any | None = None,
    ) -> None:
        await self.wait_for_space(index)
        self.open_page(index)
        if error is not None:
            self.fail_page(index, error)
        else:
            if outputs is not None:
                for position, group in enumerate(outputs):
                    self.add_outputs(index, position, group)
            elif items is not None:
                for position, item in enumerate(items):
                    self.add_outputs(index, position, [item])
            self.complete_page(index)
        await self.flush()

    def _notify_waiters(self) -> None:
        waiters = list(self._waiters)
        self._waiters.clear()
        for waiter in waiters:
            if not waiter.called:
                waiter.callback(None)


PageOrderBuffer = BoundedPageDelivery
BoundedPageOrderBuffer = BoundedPageDelivery
OrderedPageDelivery = BoundedPageDelivery
