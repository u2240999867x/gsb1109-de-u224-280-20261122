# SPDX-License-Identifier: MIT

"""
Tests for dependency-aware cached properties on attrs slots classes.

Mirrors the warehouse scenario: a scheduler thread updates stock fields
while queries compute unit summaries via cached properties.  Successful
assignments invalidate exactly the dependent caches; stale in-flight
computations must never reinstall expired cache entries.
"""

import threading

import pytest

from attrs import define, field, validators

from .versioned_cache import dependent_cached_property, versioned


class Recorder:
    """Tracks cache computations and controls the artificial pause."""

    def __init__(self):
        self.calls = []
        self.pause = None  # None or (entered, release) threading.Events


def make_stock_classes(recorder):
    """
    Build the two-level warehouse hierarchy:

    - ``StockRecord.gross`` depends only on the converted ``count``.
    - ``NetStockRecord.net`` depends on ``gross`` and ``deduction``.
    - ``NetStockRecord.source`` is an unrelated cache.
    """

    @versioned
    @define
    class StockRecord:
        count: int = field(converter=int, validator=validators.ge(0))

        @dependent_cached_property(depends_on=("count",))
        def gross(self):
            recorder.calls.append("gross")
            value = self.count * 10
            pause = recorder.pause
            if pause is not None:
                entered, release = pause
                entered.set()
                release.wait(timeout=10)
            return value

    @versioned
    @define
    class NetStockRecord(StockRecord):
        deduction: int = field(default=0)

        @dependent_cached_property(depends_on=("gross", "deduction"))
        def net(self):
            recorder.calls.append("net")
            return self.gross - self.deduction

        @dependent_cached_property
        def source(self):
            recorder.calls.append("source")
            return "warehouse-7"

        def __getattr__(self, name):
            if name.startswith("virtual_"):
                return f"virtual:{name.removeprefix('virtual_')}"
            raise AttributeError(name)

    return StockRecord, NetStockRecord


@pytest.fixture(name="recorder")
def _recorder():
    return Recorder()


@pytest.fixture(name="net_stock")
def _net_stock(recorder):
    _, net_stock_record = make_stock_classes(recorder)
    return net_stock_record


def test_stale_inflight_computation_cannot_reinstall_cache(
    recorder, net_stock
):
    """
    The full warehouse race: a suspended gross computation holding an old
    value must not overwrite the cache after a newer count is committed.
    """
    stock = net_stock(count="5", deduction=3)  # converter turns "5" into 5

    # First reads populate net (via gross) and the unrelated source cache.
    assert stock.net == 47
    assert stock.source == "warehouse-7"
    assert recorder.calls == ["net", "gross", "source"]

    # A valid count write invalidates gross and net, but not source.
    stock.count = 10

    # Start a gross computation that pauses after computing its (soon to
    # be old) value but before the descriptor can fill the cache.
    entered = threading.Event()
    release = threading.Event()
    recorder.pause = (entered, release)
    stalled = []
    worker = threading.Thread(
        target=lambda: stalled.append(stock.gross), daemon=True
    )
    worker.start()
    assert entered.wait(timeout=10)

    # The scheduler thread commits a newer count while the old
    # computation is still suspended.
    stock.count = 20
    release.set()
    worker.join(timeout=10)
    recorder.pause = None

    # The suspended reader keeps its own result, ...
    assert stalled == [100]

    # ... but it must not have been cached: fresh reads see count == 20.
    assert stock.gross == 200
    # net recomputes against the same-version, valid gross cache.
    assert stock.net == 197
    gross_calls = recorder.calls.count("gross")
    assert stock.gross == 200
    assert recorder.calls.count("gross") == gross_calls

    # A rejected count assignment keeps every still-valid cache.
    calls_before = list(recorder.calls)
    with pytest.raises(ValueError):
        stock.count = -1
    assert stock.count == 20
    assert stock.gross == 200
    assert stock.net == 197
    assert stock.source == "warehouse-7"
    assert recorder.calls == calls_before

    # Changing only deduction keeps gross and source cached.
    stock.deduction = 8
    assert stock.gross == 200
    assert recorder.calls.count("gross") == gross_calls
    assert stock.net == 192
    assert recorder.calls.count("source") == 1


def test_rejected_assignment_neither_bumps_nor_recomputes(recorder, net_stock):
    """Failed validation leaves versions and caches untouched."""
    stock = net_stock(count=5, deduction=3)
    assert stock.net == 47
    assert stock.source == "warehouse-7"
    calls_before = list(recorder.calls)

    with pytest.raises(ValueError):
        stock.count = -1
    with pytest.raises(ValueError):
        stock.count = "not-a-number"

    assert stock.count == 5
    assert stock.gross == 50
    assert stock.net == 47
    assert stock.source == "warehouse-7"
    assert recorder.calls == calls_before


def test_slots_layout_and_getattr_still_work(recorder, net_stock):
    """Slots stay dict-free and __getattr__ handles non-cached names."""
    stock = net_stock(count=5, deduction=3)

    assert not hasattr(stock, "__dict__")
    for klass in type(stock).__mro__:
        assert "__dict__" not in vars(klass).get("__slots__", ())

    # Non-cached attribute access is unaffected.
    assert stock.count == 5
    assert stock.deduction == 3
    stock.count = "7"  # converter still applies on assignment
    assert stock.count == 7

    # The subclass __getattr__ still answers non-cached names.
    assert stock.virtual_label == "virtual:label"
    with pytest.raises(AttributeError):
        stock.unknown_name
    # Slots still reject brand-new attributes.
    with pytest.raises(AttributeError):
        stock.brand_new = 1


def test_instances_are_independent(recorder, net_stock):
    """Caches and generations are per instance."""
    first = net_stock(count=5, deduction=3)
    second = net_stock(count=2, deduction=1)

    assert first.net == 47
    assert second.net == 19

    second.count = 9
    assert second.net == 89
    # The other instance's caches are untouched by the write.
    assert first.net == 47
    assert first.gross == 50
