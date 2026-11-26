"""Route messages to Faust nodes by partitioning."""

from typing import Any, Mapping, MutableMapping, Optional, Tuple

from yarl import URL

from faust.exceptions import SameNode
from faust.types.app import AppT
from faust.types.assignor import PartitionAssignorT
from faust.types.core import K
from faust.types.router import HostToPartitionMap, RouterT
from faust.types.tables import CollectionT
from faust.types.topics import TopicT
from faust.types.web import Request, Response, Web
from faust.web.exceptions import ServiceUnavailable

__all__ = [
    "LOOKUP_OFFSET_HEADER",
    "LOOKUP_PARTITION_HEADER",
    "Router",
]

#: Response header carrying the changelog partition a lookup was made on.
LOOKUP_PARTITION_HEADER = "X-Faust-Table-Partition"
#: Response header carrying the applied changelog offset of a lookup.
LOOKUP_OFFSET_HEADER = "X-Faust-Table-Offset"


def _table_name_from_path(path: Any) -> Optional[str]:
    # ``/table/{name}/{key}/`` is the built-in table endpoint.  Used only to
    # fill in position headers when an answer does not carry them itself.
    parts = [part for part in str(path).split("/") if part]
    if len(parts) >= 3 and parts[0] == "table":
        return parts[1]
    return None


class Router(RouterT):
    """Router for ``app.router``."""

    _assignor: PartitionAssignorT

    def __init__(self, app: AppT) -> None:
        self.app = app
        self._assignor = self.app.assignor

    def key_store(self, table_name: str, key: K) -> URL:
        """Return the URL of web server that hosts key in table."""
        table = self._get_table(table_name)
        topic = self._get_table_topic(table)
        k = self._get_serialized_key(table, key)
        return self._assignor.key_store(topic, k)

    def external_topic_key_store(self, topic: TopicT, key: K) -> URL:
        """Return the URL of web server that processes the key in a topics."""
        topic_name = topic.get_topic_name()
        k = topic.prepare_key(key, None)[0]
        # XXX PartitionAssignorT does not declare external_key_store(); only the
        # concrete faust.assignor.partition_assignor.PartitionAssignor defines
        # it.  Settings.PartitionAssignor lets users plug in their own assignor,
        # and any subclass of PartitionAssignorT that does not happen to define
        # this method will raise AttributeError right here.
        return self._assignor.external_key_store(  # type: ignore[attr-defined]
            topic_name, k
        )

    def table_metadata(self, table_name: str) -> HostToPartitionMap:
        """Return metadata stored for table in the partition assignor."""
        table = self._get_table(table_name)
        topic = self._get_table_topic(table)
        return self._assignor.table_metadata(topic)

    def tables_metadata(self) -> HostToPartitionMap:
        """Return metadata stored for all tables in the partition assignor."""
        return self._assignor.tables_metadata()

    def external_topics_metadata(self) -> HostToPartitionMap:
        """Return metadata stored for all external topics in the partition assignor."""
        # XXX PartitionAssignorT does not declare external_topics_metadata();
        # only the concrete faust.assignor.partition_assignor.PartitionAssignor
        # defines it.  Settings.PartitionAssignor lets users plug in their own
        # assignor, and any subclass of PartitionAssignorT that does not happen
        # to define this method will raise AttributeError right here.
        return self._assignor.external_topics_metadata()  # type: ignore[attr-defined]

    @classmethod
    def _get_table_topic(cls, table: CollectionT) -> str:
        return table.changelog_topic.get_topic_name()

    @classmethod
    def _get_serialized_key(cls, table: CollectionT, key: K) -> bytes:
        return table.changelog_topic.prepare_key(key, None)[0]

    def _get_table(self, name: str) -> CollectionT:
        return self.app.tables[name]

    async def _route_req(
        self,
        dest_url: URL,
        web: Web,
        request: Request,
        *,
        table_name: Optional[str] = None,
        key: Optional[K] = None,
    ) -> Response:
        app = self.app
        dest_ident = (host, port) = self._urlident(dest_url)
        if dest_ident == self._urlident(app.conf.canonical_url):
            raise SameNode()
        routed_url = request.url.with_host(host).with_port(int(port))
        async with app.http_client.request(
            method=request.method, headers=request.headers, url=routed_url
        ) as response:
            dest_table = table_name
            if dest_table is None and key is not None:
                dest_table = _table_name_from_path(request.url.path)
            headers = self._position_headers(
                dest_table, key, response.headers if response.headers else {}
            )
            if not headers:
                return web.text(
                    await response.text(),
                    content_type=response.content_type,
                    status=response.status,
                )
            return web.text(
                await response.text(),
                content_type=response.content_type,
                status=response.status,
                headers=headers,
            )

    def _position_headers(
        self,
        table_name: Optional[str],
        key: Optional[K],
        response_headers: Mapping[str, str],
    ) -> MutableMapping[str, str]:
        """Return position headers for a forwarded response.

        The destination includes the changelog position its answer was read
        at; when it does not (an answer from a worker that predates the
        headers), derive the partition from the table metadata the
        answering node is registered with, so the caller can still match
        the response to a partition.
        """
        partition = response_headers.get(LOOKUP_PARTITION_HEADER)
        headers: MutableMapping[str, str] = {}
        if not isinstance(partition, str):
            partition = None
        if partition is None and table_name is not None and key is not None:
            partition = self._partition_for_key(table_name, key)
        if partition is not None:
            headers[LOOKUP_PARTITION_HEADER] = partition
        offset = response_headers.get(LOOKUP_OFFSET_HEADER)
        if isinstance(offset, str):
            headers[LOOKUP_OFFSET_HEADER] = offset
        return headers

    def _partition_for_key(self, table_name: str, key: K) -> Optional[str]:
        try:
            table = self._get_table(table_name)
            return str(table.changelog_partition_for_key(key))
        except (KeyError, AttributeError, TypeError):
            return None

    async def route_req(
        self, table_name: str, key: K, web: Web, request: Request
    ) -> Response:
        """Route request to worker having key in table.

        Arguments:
            table_name: Name of the table.
            key: The key that we want.
            web: The currently sued web driver,
            request: The web request currently being served.
        """
        app = self.app
        try:
            dest_url: URL = app.router.key_store(table_name, key)
        except KeyError:
            raise ServiceUnavailable()

        return await self._route_req(
            dest_url, web, request, table_name=table_name, key=key
        )

    async def route_topic_req(
        self, topic: TopicT, key: K, web: Web, request: Request
    ) -> Response:
        """Route request to a worker that processes the partition with the given key.

        Arguments:
            topic: the topic to route request for.
            key: The key that we want.
            web: The currently sued web driver,
            request: The web request currently being served.
        """
        app = self.app
        try:
            dest_url: URL = app.router.external_topic_key_store(topic, key)
        except KeyError:
            raise ServiceUnavailable()

        return await self._route_req(dest_url, web, request)

    def _urlident(self, url: URL) -> Tuple[str, int]:
        return (
            (url.host if url.scheme else url.path) or "",
            int(url.port or 80),
        )
