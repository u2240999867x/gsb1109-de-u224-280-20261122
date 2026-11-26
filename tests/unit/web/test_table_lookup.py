"""Point lookup endpoint: zero, tombstone, and takeover responses."""

from unittest.mock import Mock

import pytest

from faust.exceptions import SameNode
from faust.types import TP
from faust.web.apps.tables import TableKeyDetail


@pytest.fixture()
def table(app):
    return app.Table("balances", default=int)


def changelog_tp(table, partition):
    return TP(table.changelog_topic.get_topic_name(), partition)


def view(app, table):
    return TableKeyDetail(app, app.web)


async def get(app, table, key):
    app.router.route_req = Mock(side_effect=SameNode())
    return await view(app, table).get(Mock(), table.name, key)


@pytest.mark.asyncio
async def test_zero_balance_is_200_with_position(app, table):
    table.begin_recovery(1, {changelog_tp(table, 0)}, {changelog_tp(table, 0)})
    table.complete_recovery(1, {changelog_tp(table, 0)})
    _apply(table, 0, "K", 0, 42)
    response = await get(app, table, "K")
    assert response.status == 200
    assert response.body == b"0"
    assert response.headers["X-Faust-Table-Partition"] == "0"
    assert response.headers["X-Faust-Table-Offset"] == "42"


@pytest.mark.asyncio
async def test_deleted_account_is_404_with_position(app, table):
    table.begin_recovery(1, {changelog_tp(table, 0)}, {changelog_tp(table, 0)})
    table.complete_recovery(1, {changelog_tp(table, 0)})
    _apply(table, 0, "K", 7, 40)
    _apply(table, 0, "K", None, 41)
    response = await get(app, table, "K")
    assert response.status == 404
    assert response.headers["X-Faust-Table-Partition"] == "0"
    assert response.headers["X-Faust-Table-Offset"] == "41"


@pytest.mark.asyncio
async def test_partition_being_taken_over_is_503_retry(app, table):
    table.begin_recovery(1, {changelog_tp(table, 0)}, {changelog_tp(table, 0)})
    table.complete_recovery(1, {changelog_tp(table, 0)})
    _apply(table, 0, "K", 7, 40)
    table.begin_recovery(2, set(), set())
    response = await get(app, table, "K")
    assert response.status == 503
    assert b"Try again later" in response.body


def _apply(table, partition, key, value, offset):
    from tests.helpers import new_event

    table.apply_changelog_batch(
        [
            new_event(
                table.app,
                key,
                value,
                topic=table.changelog_topic.get_topic_name(),
                partition=partition,
                offset=offset,
            )
        ]
    )
