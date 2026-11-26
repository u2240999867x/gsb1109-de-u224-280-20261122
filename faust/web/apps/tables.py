"""HTTP endpoint showing partition routing destinations."""

from typing import Any, Mapping, cast

from faust import web
from faust.app.router import (
    LOOKUP_OFFSET_HEADER,
    LOOKUP_PARTITION_HEADER,
    SameNode,
)
from faust.exceptions import TablePartitionNotReady
from faust.models import Record
from faust.types import K, TableT, V
from faust.types.tables import TableLookup
from faust.web.exceptions import ServiceUnavailable

__all__ = [
    "TableView",
    "TableList",
    "TableDetail",
    "TableKeyDetail",
    "blueprint",
]

blueprint = web.Blueprint("tables")


class TableInfo(Record, serializer="json", namespace="@TableInfo"):
    name: str
    help: str


class TableView(web.View):
    """Base class for table related views."""

    def table_json(self, table: TableT, **kwargs: Any) -> Mapping:
        """Return table info as JSON serializable object."""
        return TableInfo(table.name, table.help).asdict()

    def get_table_or_404(self, name: str) -> TableT:
        """Find table by name, or raise NotFound if not found."""
        try:
            return cast(TableT, self.app.tables[name])
        except KeyError:
            raise self.NotFound("unknown table", name=name)

    def get_table_value_or_404(self, table: TableT, key: K) -> V:
        """Get value from table by key, or raise NotFound if not found."""
        try:
            value = table[key]
        except KeyError:
            raise self.NotFound("key not found", key=key, table=table.name)
        if value is None:
            # A tombstoned key reads as ``None``; a stored ``0`` is a real
            # value and must be returned as such.
            raise self.NotFound("key not found", key=key, table=table.name)
        return value

    def lookup_headers(self, lookup: TableLookup) -> Mapping[str, str]:
        """Return HTTP headers describing the position of ``lookup``."""
        return {
            LOOKUP_PARTITION_HEADER: str(lookup.partition),
            LOOKUP_OFFSET_HEADER: str(lookup.offset),
        }

    def lookup_response(self, table: TableT, key: K) -> web.Response:
        """Return the response for a fresh point lookup.

        The response -- including a 404 for a deleted key -- carries the
        changelog partition and applied offset it was read at, so the page
        can match the result with the position it was served from.  A key
        whose partition is being taken over gets a retryable 503 instead.
        """
        try:
            lookup = table.lookup(key)
        except TablePartitionNotReady as exc:
            return self.not_ready(exc)
        headers = self.lookup_headers(lookup)
        if lookup.value is None:
            return self.error(
                self.NotFound.code,
                "key not found",
                headers=headers,
                key=key,
                table=table.name,
            )
        return self.json(lookup.value, headers=headers)

    def not_ready(self, exc: TablePartitionNotReady) -> web.Response:
        """Return a retryable response for a partition being taken over."""
        return self.error(
            ServiceUnavailable.code,
            ServiceUnavailable.detail,
            table=exc.table or "",
            partition=exc.partition,
        )


@blueprint.route("/", name="list")
class TableList(TableView):
    """
    ---
    description: List available table names.
    tags:
    - Faust
    produces:
    - application/json
    """

    async def get(self, request: web.Request) -> web.Response:
        """Return JSON response with a list of available table names."""
        return self.json([self.table_json(table) for table in self.app.tables.values()])


@blueprint.route("/{name}/", name="detail")
class TableDetail(TableView):
    """
    ---
    description: Get details for table by name.
    tags:
    - Faust
    parameters:
    - in: path
      name: name
      type: string
      required: true
    produces:
    - application/json
    """

    async def get(self, request: web.Request, name: str) -> web.Response:
        """Return JSON response with table information."""
        table = self.get_table_or_404(name)
        return self.json(self.table_json(table))


@blueprint.route("/{name}/{key}/", name="key-detail")
class TableKeyDetail(TableView):
    """
    ---
    description: List information about key.
    tags:
    - Faust
    parameters:
    - in: path
      name: name
      type: string
      required: true
    - in: path
      name: key
      type: string
      required: true
    produces:
    - application/json
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    async def get(self, request: web.Request, name: str, key: str) -> web.Response:
        """Look up value in table by key."""
        router = self.app.router
        try:
            return await router.route_req(name, key, self.web, request)
        except SameNode:
            table = self.get_table_or_404(name)
            return self.lookup_response(table, key)
