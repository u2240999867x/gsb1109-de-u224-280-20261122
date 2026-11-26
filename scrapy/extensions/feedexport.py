"""
Feed Exports extension

See documentation in docs/topics/feed-exports.rst
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import threading
import warnings
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PureWindowsPath
from tempfile import NamedTemporaryFile
from typing import IO, TYPE_CHECKING, Any, Protocol, Self, TypeAlias, cast
from urllib.parse import unquote, urlparse
from uuid import uuid4

from itemadapter import ItemAdapter
from twisted.internet.defer import Deferred, DeferredList, DeferredLock
from w3lib.url import file_uri_to_path
from zope.interface import Interface

from scrapy import Spider, signals
from scrapy.exceptions import NotConfigured, ScrapyDeprecationWarning
from scrapy.extensions.postprocessing import PostProcessingManager
from scrapy.utils._ftp import ftp_store_file
from scrapy.utils.asyncio import is_asyncio_available, run_in_thread
from scrapy.utils.boto import _get_max_pool_connections, is_aioboto3_available
from scrapy.utils.conf import _job_dir, feed_complete_default_values_from_settings
from scrapy.utils.defer import (
    _maybe_coroutine,
    deferred_from_coro,
    ensure_awaitable,
    maybe_deferred_to_future,
)
from scrapy.utils.misc import build_from_crawler, load_object
from scrapy.utils.python import without_none_values

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from _typeshed import OpenBinaryMode

    from scrapy.crawler import Crawler
    from scrapy.exporters import BaseItemExporter
    from scrapy.settings import BaseSettings, Settings


logger = logging.getLogger(__name__)


# Printf-style placeholders (e.g. %(time)s) used to build feed URIs. Any other
# percent character in a URI (e.g. percent-encoding such as %20 or %23) must be
# treated as a literal rather than as the start of a placeholder.
_FEED_URI_PLACEHOLDER_RE = re.compile(
    r"%\([^)]+\)[-+ #0]*(?:\d+|\*)?(?:\.(?:\d+|\*))?[diouxXeEfFgGcrsa]"
)


def apply_uri_params(uri_template: str, uri_params: dict[str, Any]) -> str:
    """Return *uri_template* with its ``%(...)s`` placeholders replaced using
    *uri_params*, leaving any other percent character untouched.

    This allows feed URIs to contain percent-encoded characters (e.g. ``%20``
    in a path with spaces or ``%23`` in FTP credentials) without them being
    misinterpreted as printf-style formatting directives.
    """
    parts: list[str] = []
    last = 0
    for match in _FEED_URI_PLACEHOLDER_RE.finditer(uri_template):
        parts.append(uri_template[last : match.start()].replace("%", "%%"))
        parts.append(match.group(0))
        last = match.end()
    parts.append(uri_template[last:].replace("%", "%%"))
    return "".join(parts) % uri_params


UriParamsCallableT: TypeAlias = Callable[
    [dict[str, Any], Spider], dict[str, Any] | None
]

_ItemProcessor: TypeAlias = Callable[[Any], Iterable[Any]]
_ItemReceiptKey: TypeAlias = Callable[[Any], Any]


@dataclass(frozen=True)
class FeedItemReceipt:
    """A persisted confirmation that one exported entry of a feed reached its
    storage.

    *item_key* identifies the scraped item that the exported entry comes from,
    and *entry_key* identifies that entry among the entries that the item
    produces for the feed, e.g. the two rows that an item processor can
    generate for one scraped item.
    """

    feed: str
    item_key: str
    entry_key: str
    uri: str
    batch_id: int
    index: int


_FEED_RECEIPTS_VERSION = 1
_FEED_RECEIPTS_FILENAME = "feed_receipts.json"


_RECEIPT_KEY_FIELDS = ("id", "_id", "source_id", "record_id")


def _item_receipt_key(
    item: Any,
    receipt_key: Callable[[Any], Any] | None = None,
) -> str:
    """Return the key that identifies the source record of *item*.

    A *receipt_key* callable takes precedence. Otherwise the first populated
    field among :data:`_RECEIPT_KEY_FIELDS` is used, so that a replayed record
    maps to the same key as in the run that first exported it. Items without
    any of those fields fall back to a hash of their contents.
    """
    if receipt_key is not None:
        value = receipt_key(item)
        return value if isinstance(value, str) else str(value)
    try:
        adapter = ItemAdapter(item)
        for field in _RECEIPT_KEY_FIELDS:
            try:
                value = adapter[field]
            except KeyError:
                continue
            return value if isinstance(value, str) else str(value)
        data = json.dumps(
            adapter.asdict(), sort_keys=True, default=repr, ensure_ascii=False
        ).encode()
    except (TypeError, ValueError):
        data = repr(item).encode()
    return hashlib.sha256(data).hexdigest()


class _FeedReceipts:
    """Persist which entries of each feed reached storage.

    A receipt is written only after :meth:`FeedSlot.store` succeeds, one per
    exported entry, so that a later run can skip the entries that were already
    delivered and retry the ones that were not, even when one feed succeeded
    while another failed.
    """

    def __init__(self, path: Path):
        self.path: Path = path
        self._receipts: dict[str, dict[str, FeedItemReceipt]] = {}
        self._batches: dict[str, set[int]] = {}
        self._attempted: dict[str, set[int]] = {}
        self._lock = threading.Lock()
        self._load()

    def _load(self) -> None:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except (OSError, ValueError):
            logger.warning(
                f"Ignoring unreadable feed receipts file: {self.path}",
                exc_info=True,
            )
            return
        if not isinstance(data, dict) or data.get("version") != _FEED_RECEIPTS_VERSION:
            return
        for feed, batches in data.get("batches", {}).items():
            if isinstance(batches, list):
                self._batches[feed] = {
                    int(batch_id)
                    for batch_id in batches
                    if isinstance(batch_id, int)
                }
        for feed, batches in data.get("attempted_batches", {}).items():
            if isinstance(batches, list):
                self._attempted[feed] = {
                    int(batch_id)
                    for batch_id in batches
                    if isinstance(batch_id, int)
                }
        for feed, entries in data.get("feeds", {}).items():
            if not isinstance(entries, dict):
                continue
            feed_receipts: dict[str, FeedItemReceipt] = {}
            for receipt in entries.values():
                if not isinstance(receipt, dict):
                    continue
                try:
                    value = FeedItemReceipt(
                        feed=receipt["feed"],
                        item_key=receipt["item_key"],
                        entry_key=receipt["entry_key"],
                        uri=receipt["uri"],
                        batch_id=int(receipt["batch_id"]),
                        index=int(receipt["index"]),
                    )
                except (KeyError, TypeError, ValueError):
                    continue
                feed_receipts[value.entry_key] = value
            self._receipts[feed] = feed_receipts

    def _save(self) -> None:
        data = {
            "version": _FEED_RECEIPTS_VERSION,
            "batches": {
                feed: sorted(batches)
                for feed, batches in self._batches.items()
            },
            "attempted_batches": {
                feed: sorted(batches)
                for feed, batches in self._attempted.items()
            },
            "feeds": {
                feed: {
                    entry_key: {
                        "feed": receipt.feed,
                        "item_key": receipt.item_key,
                        "entry_key": receipt.entry_key,
                        "uri": receipt.uri,
                        "batch_id": receipt.batch_id,
                        "index": receipt.index,
                    }
                    for entry_key, receipt in entries.items()
                }
                for feed, entries in self._receipts.items()
            },
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_name(f"{self.path.name}.{uuid4().hex}.tmp")
        with tmp_path.open("w", encoding="utf-8") as file:
            file.write(json.dumps(data, indent=2, sort_keys=True))
            file.flush()
            os.fsync(file.fileno())
        tmp_path.replace(self.path)

    @staticmethod
    def entry_key(uri_template: str, item_key: str, index: int) -> str:
        return f"{uri_template}\0{item_key}\0{index}"

    def delivered(self, feed: str, entry_key: str) -> bool:
        with self._lock:
            return entry_key in self._receipts.get(feed, {})

    def add(
        self, receipts: Iterable[FeedItemReceipt], batch: tuple[str, int] | None = None
    ) -> None:
        with self._lock:
            changed = False
            pending = list(receipts)
            for receipt in pending:
                entries = self._receipts.setdefault(receipt.feed, {})
                if receipt.entry_key in entries:
                    continue
                entries[receipt.entry_key] = receipt
                changed = True
            if batch is not None:
                feed, batch_id = batch
                batches = self._batches.setdefault(feed, set())
                if batch_id not in batches:
                    batches.add(batch_id)
                    changed = True
            if changed:
                self._save()

    def all(self) -> list[FeedItemReceipt]:
        with self._lock:
            return sorted(
                (
                    receipt
                    for entries in self._receipts.values()
                    for receipt in entries.values()
                ),
                key=lambda receipt: (
                    receipt.feed,
                    receipt.item_key,
                    receipt.entry_key,
                ),
            )

    def mark_attempted(self, feed: str, batch_id: int) -> None:
        """Record that *batch_id* of *feed* is about to be stored.

        The batch is not a receipt. It is kept apart so that a retry does not
        reuse the URI of a batch that may have left a partial file behind.
        """
        with self._lock:
            batches = self._attempted.setdefault(feed, set())
            if batch_id in batches:
                return
            batches.add(batch_id)
            self._save()

    def next_batch_id(self, feed: str) -> int:
        """Return the batch ID to use for new entries of *feed*."""
        with self._lock:
            used = self._attempted.get(feed, set()) | self._batches.get(feed, set())
            used.update(
                receipt.batch_id
                for receipt in self._receipts.get(feed, {}).values()
            )
            return max(used, default=0) + 1

_FEED_MODES: frozenset[str] = frozenset({"append", "create", "overwrite"})


def _get_mode(storage: Any, feed_options: dict[str, Any] | None, legacy: str) -> str:
    """Return the mode of *storage*, based on the *mode* feed option, or on the
    deprecated *overwrite* feed option, or, if neither is set, on *legacy*,
    which must be the mode that the storage used before the *mode* feed option
    existed."""
    feed_options = feed_options or {}
    mode = feed_options.get("mode")
    if mode is None:
        overwrite = feed_options.get("overwrite")
        mode = legacy if overwrite is None else "overwrite" if overwrite else "append"
    _check_mode(mode, storage)
    return mode


def _check_mode(mode: str, storage: Any, uri: str | None = None) -> None:
    if mode not in _FEED_MODES:
        raise ValueError(
            f"Invalid feed mode: {mode!r}. Supported modes: "
            f"{', '.join(sorted(_FEED_MODES))}."
        )
    supported: frozenset[str] | None = getattr(storage, "supported_modes", None)
    if supported is not None and mode not in supported:
        suffix = f" (feed URI: {uri})" if uri else ""
        raise ValueError(
            f"{type(storage).__name__} does not support the {mode!r} feed "
            f"mode{suffix}. Supported modes: {', '.join(sorted(supported))}."
        )


class ItemFilter:
    """
    This will be used by FeedExporter to decide if an item should be allowed
    to be exported to a particular feed.

    :param feed_options: feed specific options passed from FeedExporter
    :type feed_options: dict
    """

    feed_options: dict[str, Any] | None
    item_classes: tuple[type, ...]

    def __init__(self, feed_options: dict[str, Any] | None) -> None:
        self.feed_options = feed_options
        if feed_options is not None:
            self.item_classes = tuple(
                load_object(item_class)
                for item_class in feed_options.get("item_classes") or ()
            )
        else:
            self.item_classes = ()

    def accepts(self, item: Any) -> bool:
        """
        Return ``True`` if `item` should be exported or ``False`` otherwise.

        :param item: scraped item which user wants to check if is acceptable
        :type item: :ref:`Scrapy items <topics-items>`
        :return: `True` if accepted, `False` otherwise
        :rtype: bool
        """
        if self.item_classes:
            return isinstance(item, self.item_classes)
        return True  # accept all items by default


class _IFeedStorage(Interface):  # type: ignore[misc]  # pragma: no cover
    # pylint: disable=no-self-argument

    def __init__(uri, *, feed_options=None): ...  # type: ignore[no-untyped-def]  # pylint: disable=super-init-not-called

    def open(spider): ...  # type: ignore[no-untyped-def]

    def store(file): ...  # type: ignore[no-untyped-def]


class FeedStorageProtocol(Protocol):
    """Protocol that all Feed Storages must follow.

    :param uri: Feed URI.

    :param feed_options: :ref:`Feed options <feed-options>` of the feed.

    A feed storage may define a ``from_crawler(cls, crawler, uri, *,
    feed_options=None)`` class method instead of ``__init__``, to also get the
    :class:`~scrapy.crawler.Crawler` object.
    """

    def open(self, spider: Spider) -> IO[bytes] | Coroutine[Any, Any, IO[bytes]]:
        """Open the storage for *spider* and return the file-like object that
        item exporters write into.

        .. versionchanged:: 2.20.0
           This method may now be a coroutine function (``async def``).
        """

    def store(
        self, file: IO[bytes]
    ) -> Coroutine[Any, Any, None] | Deferred[None] | None:
        """Store *file*, the file-like object returned by :meth:`open`, and
        close it.

        .. versionchanged:: 2.20.0
           This method may now be a coroutine function (``async def``).
        """


class BlockingFeedStorage(ABC):
    """Base class for feed storages that store feeds using blocking code."""

    def open(self, spider: Spider) -> IO[bytes]:
        path = spider.crawler.settings["FEED_TEMPDIR"]
        if path and not Path(path).is_dir():
            raise OSError("Not a Directory: " + str(path))

        return NamedTemporaryFile(prefix="feed-", dir=path)

    def store(self, file: IO[bytes]) -> Deferred[None]:
        return deferred_from_coro(run_in_thread(self._store_in_thread, file))

    @abstractmethod
    def _store_in_thread(self, file: IO[bytes]) -> None:
        """Store *file* and close it.

        This method runs in a separate thread, so it may use blocking code.
        """
        raise NotImplementedError


class StdoutFeedStorage:
    """:ref:`Standard output <topics-feed-storage-stdout>` storage backend."""

    # The mode is irrelevant here: writing to a stream cannot destroy data.
    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(
        self,
        uri: str,
        _stdout: IO[bytes] | None = None,
        *,
        feed_options: dict[str, Any] | None = None,
    ):
        if not _stdout:
            _stdout = sys.stdout.buffer
        self._stdout: IO[bytes] = _stdout

    def open(self, spider: Spider) -> IO[bytes]:
        return self._stdout

    def store(self, file: IO[bytes]) -> Deferred[None] | None:
        pass


_WRITE_MODES: dict[str, OpenBinaryMode] = {
    "append": "ab",
    "create": "xb",
    "overwrite": "wb",
}


class FileFeedStorage:
    """:ref:`Local filesystem <topics-feed-storage-fs>` storage backend.

    *uri* may be a ``file://`` URI or a plain path. Missing parent directories
    are created when the feed is opened.
    """

    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(self, uri: str, *, feed_options: dict[str, Any] | None = None):
        self.path: str = file_uri_to_path(uri) if uri.startswith("file:") else uri
        self.write_mode: OpenBinaryMode = _WRITE_MODES[
            _get_mode(self, feed_options, "append")
        ]
        self._transactional: bool = bool(
            (feed_options or {}).get("delivery_receipts")
        )
        self._temp_path: str | None = None

    def open(self, spider: Spider) -> IO[bytes]:
        dirname = Path(self.path).parent
        if dirname and not dirname.exists():
            dirname.mkdir(parents=True)
        if self._transactional:
            # Write next to the target, so that committing is a rename within
            # one filesystem, and a failed store cannot leave a partial target
            # that looks like delivered output.
            temp = NamedTemporaryFile(
                prefix=f".{Path(self.path).name}.", dir=dirname or ".", delete=False
            )
            self._temp_path = temp.name
            return temp
        return Path(self.path).open(self.write_mode)

    def store(self, file: IO[bytes]) -> Deferred[None] | None:
        if self._transactional:
            self._commit(file)
            return None
        file.close()
        return None

    def _commit(self, file: IO[bytes]) -> None:
        assert self._temp_path is not None
        try:
            file.flush()
            os.fsync(file.fileno())
            file.close()
            if self.write_mode == "ab":
                # Append into a fresh file that starts with the current target,
                # so that a failed append cannot leave the target half-written.
                combined = NamedTemporaryFile(
                    prefix=f".{Path(self.path).name}.",
                    dir=Path(self.path).parent,
                    delete=False,
                )
                try:
                    with combined, Path(self._temp_path).open("rb") as temp:
                        if Path(self.path).exists():
                            with Path(self.path).open("rb") as target:
                                shutil.copyfileobj(target, combined)
                        shutil.copyfileobj(temp, combined)
                        combined.flush()
                        os.fsync(combined.fileno())
                    Path(combined.name).replace(self.path)
                except BaseException:
                    with contextlib.suppress(OSError):
                        Path(combined.name).unlink()
                    raise
                Path(self._temp_path).unlink()
            else:
                if self.write_mode == "xb" and Path(self.path).exists():
                    raise FileExistsError(str(self.path))
                Path(self._temp_path).replace(self.path)
            self._temp_path = None
        except BaseException:
            with contextlib.suppress(OSError):
                file.close()
            if self._temp_path is not None:
                with contextlib.suppress(OSError):
                    Path(self._temp_path).unlink()
                self._temp_path = None
            raise


class S3FeedStorage(BlockingFeedStorage):
    """:ref:`Amazon S3 <topics-feed-storage-s3>` storage backend."""

    supported_modes: frozenset[str] = frozenset({"create", "overwrite"})

    def __init__(
        self,
        uri: str,
        access_key: str | None = None,
        secret_key: str | None = None,
        acl: str | None = None,
        endpoint_url: str | None = None,
        *,
        feed_options: dict[str, Any] | None = None,
        session_token: str | None = None,
        region_name: str | None = None,
        max_pool_connections: int | None = None,
        upload_timeout: float | None = None,
    ):
        try:
            import boto3.session  # noqa: PLC0415
        except ImportError:
            raise NotConfigured("missing boto3 library") from None
        from botocore.config import Config  # noqa: PLC0415

        u = urlparse(uri)
        assert u.hostname
        self.bucketname: str = u.hostname
        self.access_key: str | None = u.username or access_key
        self.secret_key: str | None = u.password or secret_key
        self.session_token: str | None = session_token
        self.keyname: str = u.path[1:]  # remove first "/"
        self.acl: str | None = acl
        self.endpoint_url: str | None = endpoint_url
        self.region_name: str | None = region_name
        self.max_pool_connections: int | None = max_pool_connections
        self.upload_timeout: float | None = upload_timeout

        config_kwargs: dict[str, Any] = {}
        if max_pool_connections is not None:
            config_kwargs["max_pool_connections"] = max_pool_connections
        if upload_timeout is not None:
            config_kwargs["read_timeout"] = upload_timeout

        self._client_kwargs: dict[str, Any] = {
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_session_token": self.session_token,
            "endpoint_url": self.endpoint_url,
            "region_name": self.region_name,
            "config": Config(**config_kwargs),
        }

        # Synchronous boto3 client, used when asyncio support or aioboto3 is not
        # available (its calls are then run in a thread).
        boto3_session = boto3.session.Session()
        self.s3_client = boto3_session.client("s3", **self._client_kwargs)

        self._mode: str = _get_mode(self, feed_options, "overwrite")

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        uri: str,
        *,
        feed_options: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            uri,
            access_key=crawler.settings.getsecret("AWS_ACCESS_KEY_ID"),
            secret_key=crawler.settings.getsecret("AWS_SECRET_ACCESS_KEY"),
            session_token=crawler.settings.getsecret("AWS_SESSION_TOKEN"),
            acl=crawler.settings["FEED_STORAGE_S3_ACL"] or None,
            endpoint_url=crawler.settings["AWS_ENDPOINT_URL"] or None,
            region_name=crawler.settings["AWS_REGION_NAME"] or None,
            max_pool_connections=_get_max_pool_connections(crawler.settings),
            upload_timeout=crawler.settings.getfloat("UPLOAD_TIMEOUT") or None,
            feed_options=feed_options,
        )

    def store(self, file: IO[bytes]) -> Deferred[None]:
        if is_asyncio_available() and is_aioboto3_available():
            return deferred_from_coro(self._store_async(file))
        return super().store(file)

    async def _store_async(self, file: IO[bytes]) -> None:
        import aioboto3  # noqa: PLC0415

        file.seek(0)
        extra_args: dict[str, Any] = {"ACL": self.acl} if self.acl else {}
        session = aioboto3.Session()
        try:
            async with session.client("s3", **self._client_kwargs) as client:
                if self._mode == "create":
                    with self._raise_if_exists():
                        await client.put_object(
                            Bucket=self.bucketname,
                            Key=self.keyname,
                            Body=file,
                            IfNoneMatch="*",
                            **extra_args,
                        )
                else:
                    await client.upload_fileobj(
                        Fileobj=file,
                        Bucket=self.bucketname,
                        Key=self.keyname,
                        ExtraArgs=extra_args,
                    )
        finally:
            file.close()

    @contextlib.contextmanager
    def _raise_if_exists(self) -> Iterator[None]:
        from botocore.exceptions import ClientError  # noqa: PLC0415

        try:
            yield
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "PreconditionFailed":
                raise FileExistsError(
                    f"s3://{self.bucketname}/{self.keyname} already exists"
                ) from error
            raise

    def _store_in_thread(self, file: IO[bytes]) -> None:
        file.seek(0)
        extra_args: dict[str, Any] = {"ACL": self.acl} if self.acl else {}
        try:
            if self._mode == "create":
                # upload_fileobj() does not allow IfNoneMatch
                # (https://github.com/boto/boto3/issues/4366).
                with self._raise_if_exists():
                    self.s3_client.put_object(
                        Bucket=self.bucketname,
                        Key=self.keyname,
                        Body=file,
                        IfNoneMatch="*",
                        **extra_args,
                    )
            elif extra_args:
                self.s3_client.upload_fileobj(
                    Bucket=self.bucketname,
                    Key=self.keyname,
                    Fileobj=file,
                    ExtraArgs=extra_args,
                )
            else:
                self.s3_client.upload_fileobj(
                    Bucket=self.bucketname,
                    Key=self.keyname,
                    Fileobj=file,
                )
        finally:
            file.close()


class GCSFeedStorage(BlockingFeedStorage):
    """:ref:`GCS <topics-feed-storage-gcs>` storage backend."""

    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(
        self,
        uri: str,
        project_id: str | None,
        acl: str | None,
        *,
        feed_options: dict[str, Any] | None = None,
        timeout: float | None = None,
    ):
        self.project_id: str | None = project_id
        self.acl: str | None = acl
        self.timeout: float | None = timeout
        u = urlparse(uri)
        assert u.hostname
        self.bucket_name: str = u.hostname
        self.blob_name: str = u.path[1:]  # remove first "/"

        self._mode: str = _get_mode(self, feed_options, "overwrite")

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        uri: str,
        *,
        feed_options: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            uri,
            crawler.settings["GCS_PROJECT_ID"],
            crawler.settings["FEED_STORAGE_GCS_ACL"] or None,
            feed_options=feed_options,
            timeout=crawler.settings.getfloat("UPLOAD_TIMEOUT") or None,
        )

    def _store_in_thread(self, file: IO[bytes]) -> None:
        from google.api_core.exceptions import PreconditionFailed  # noqa: PLC0415
        from google.cloud.storage import Client  # noqa: PLC0415

        file.seek(0)
        try:
            client = Client(project=self.project_id)
            bucket = client.bucket(self.bucket_name)
            kwargs: dict[str, Any] = (
                {} if self.timeout is None else {"timeout": self.timeout}
            )
            blob = (
                bucket.get_blob(self.blob_name, **kwargs)
                if self._mode == "append"
                else None
            )
            if blob is None:
                upload_kwargs = (
                    {**kwargs, "if_generation_match": 0}
                    if self._mode == "create"
                    else kwargs
                )
                try:
                    bucket.blob(self.blob_name).upload_from_file(
                        file, predefined_acl=self.acl, **upload_kwargs
                    )
                except PreconditionFailed as error:
                    raise FileExistsError(
                        f"gs://{self.bucket_name}/{self.blob_name} already exists"
                    ) from error
                return
            # Appending uploads the new data as a separate object and composes
            # it with the existing one, leaving the data already stored
            # untouched. Composition resolves its sources by name, so it always
            # appends to the latest version of the blob.
            part = bucket.blob(f"{self.blob_name}.{uuid4().hex}.part")
            part.upload_from_file(file, predefined_acl=self.acl, **kwargs)
            try:
                blob.compose([blob, part], client=client, **kwargs)
            finally:
                part.delete(client=client, **kwargs)
            if self.acl:
                blob.acl.save_predefined(self.acl, client=client, **kwargs)
        finally:
            file.close()


class FTPFeedStorage(BlockingFeedStorage):
    """:ref:`FTP <feed-storage-ftp>` storage backend, which also handles
    :ref:`FTPS <feed-storage-ftps>` when *uri* uses the ``ftps`` scheme."""

    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(
        self,
        uri: str,
        use_active_mode: bool = False,
        *,
        feed_options: dict[str, Any] | None = None,
        timeout: float | None = None,
    ):
        u = urlparse(uri)
        if not u.hostname:
            raise ValueError(f"Got a storage URI without a hostname: {uri}")
        self.host: str = u.hostname
        self.port: int = int(u.port or "21")
        self.username: str = u.username or ""
        self.password: str = unquote(u.password or "")
        self.path: str = u.path
        self.tls: bool = u.scheme == "ftps"
        self.use_active_mode: bool = use_active_mode
        self.timeout: float | None = timeout
        self._mode: str = _get_mode(self, feed_options, "overwrite")

    @property
    def overwrite(self) -> bool:
        warnings.warn(
            "FTPFeedStorage.overwrite is deprecated, use the mode feed option instead.",
            ScrapyDeprecationWarning,
            stacklevel=2,
        )
        return self._mode != "append"

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        uri: str,
        *,
        feed_options: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            uri,
            use_active_mode=crawler.settings.getbool("FEED_STORAGE_FTP_ACTIVE"),
            feed_options=feed_options,
            timeout=crawler.settings.getfloat("UPLOAD_TIMEOUT") or None,
        )

    def _store_in_thread(self, file: IO[bytes]) -> None:
        ftp_store_file(
            path=self.path,
            file=file,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            use_active_mode=self.use_active_mode,
            mode=self._mode,
            tls=self.tls,
            timeout=self.timeout,
        )


class FeedSlot:
    def __init__(
        self,
        storage: FeedStorageProtocol,
        uri: str,
        format: str,  # noqa: A002
        store_empty: bool,
        batch_id: int,
        uri_template: str,
        filter: ItemFilter,  # noqa: A002
        feed_options: dict[str, Any],
        spider: Spider,
        exporters: dict[str, type[BaseItemExporter]],
        settings: BaseSettings,
        crawler: Crawler,
    ):
        self.file: IO[bytes] | None = None
        self.exporter: BaseItemExporter | None = None
        self.storage: FeedStorageProtocol = storage
        # feed params
        self.batch_id: int = batch_id
        self.format: str = format
        self.store_empty: bool = store_empty
        self.uri_template: str = uri_template
        self.uri: str = uri
        self.filter: ItemFilter = filter
        # exporter params
        self.feed_options: dict[str, Any] = feed_options
        self.spider: Spider = spider
        self.exporters: dict[str, type[BaseItemExporter]] = exporters
        self.settings: BaseSettings = settings
        self.crawler: Crawler = crawler
        # flags
        self.itemcount: int = 0
        self._skipped: bool = False
        self._exporting: bool = False
        self._fileloaded: bool = False
        # Entries exported in this slot, and the receipts of the entries that
        # reached storage. A failed store leaves both empty.
        self._pending_receipts: list[FeedItemReceipt] = []
        self._delivered_receipts: list[FeedItemReceipt] = []
        self._pending_count: int = 0
        self._delivered: bool = False

    @property
    def delivery_receipts(self) -> list[FeedItemReceipt]:
        """The receipts of the entries of this slot that reached storage."""
        return list(self._delivered_receipts)

    @property
    def delivered(self) -> bool:
        """Whether this slot reached storage."""
        return self._delivered

    def add_receipt(self, item_key: str, index: int) -> None:
        self._pending_receipts.append(
            FeedItemReceipt(
                feed=self.uri_template,
                item_key=item_key,
                entry_key=_FeedReceipts.entry_key(self.uri_template, item_key, index),
                uri=self.uri,
                batch_id=self.batch_id,
                index=index,
            )
        )

    async def start_exporting(self) -> None:
        if not self._fileloaded:
            self.file = await _maybe_coroutine(self.storage.open, self.spider)
            if "postprocessing" in self.feed_options:
                self.file = cast(
                    "IO[bytes]",
                    PostProcessingManager(
                        self.feed_options["postprocessing"],
                        self.file,
                        self.feed_options,
                    ),
                )
            self.exporter = self._get_exporter(
                file=self.file,
                format_=self.feed_options["format"],
                fields_to_export=self.feed_options["fields"],
                encoding=self.feed_options["encoding"],
                indent=self.feed_options["indent"],
                **self.feed_options["item_export_kwargs"],
            )
            self._fileloaded = True

        if not self._exporting:
            assert self.exporter
            await _maybe_coroutine(self.exporter.start_exporting)
            self._exporting = True

    def _get_exporter(
        self, file: IO[bytes], format_: str, *args: Any, **kwargs: Any
    ) -> BaseItemExporter:
        return build_from_crawler(
            self.exporters[format_], self.crawler, file, *args, **kwargs
        )

    async def finish_exporting(self) -> None:
        if self._exporting:  # pragma: no branch
            assert self.exporter
            await _maybe_coroutine(self.exporter.finish_exporting)
            self._exporting = False


class FeedExporter:
    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        exporter = cls(crawler)
        crawler.signals.connect(exporter.open_spider, signals.spider_opened)
        crawler.signals.connect(exporter._on_state_loaded, signals.spider_state_loaded)
        crawler.signals.connect(exporter._on_engine_started, signals.engine_started)
        crawler.signals.connect(exporter.close_spider, signals.spider_closed)
        crawler.signals.connect(exporter._on_state_saving, signals.spider_state_saving)
        crawler.signals.connect(exporter.item_scraped, signals.item_scraped)
        return exporter

    def __init__(self, crawler: Crawler):
        self.crawler: Crawler = crawler
        self.settings: Settings = crawler.settings
        self.feeds: dict[str, dict[str, Any]] = {}
        self.slots: list[FeedSlot] = []
        self.filters: dict[str, ItemFilter] = {}
        self.processors: dict[str, _ItemProcessor | None] = {}
        self._pending_close_tasks: list[asyncio.Task[None] | Deferred[None]] = []
        self._receipts: _FeedReceipts | None = None
        self._receipt_keys: dict[str, _ItemReceiptKey | None] = {}
        # Item export may await user-defined code (e.g. an async def
        # export_item() method), while item_scraped signals are sent
        # concurrently (see the CONCURRENT_ITEMS setting). This lock prevents
        # overlapping item export calls, which item exporters, being stateful,
        # cannot handle.
        self._export_lock = DeferredLock()

        if not self.settings["FEEDS"] and not self.settings["FEED_URI"]:
            raise NotConfigured

        # Begin: Backward compatibility for FEED_URI and FEED_FORMAT settings
        if self.settings["FEED_URI"]:
            warnings.warn(
                "The `FEED_URI` and `FEED_FORMAT` settings have been deprecated in favor of "
                "the `FEEDS` setting. Please see the `FEEDS` setting docs for more details",
                category=ScrapyDeprecationWarning,
                stacklevel=2,
            )
            uri = self.settings["FEED_URI"]
            # handle pathlib.Path objects
            uri = str(uri.absolute()) if isinstance(uri, Path) else str(uri)
            feed_options = {"format": self.settings["FEED_FORMAT"]}
            self.feeds[uri] = feed_complete_default_values_from_settings(
                feed_options, self.settings, uri
            )
            self.filters[uri] = self._load_filter(feed_options)
            self.processors[uri] = self._load_processor(feed_options)
            self._receipt_keys[uri] = self._load_receipt_key(feed_options)
        # End: Backward compatibility for FEED_URI and FEED_FORMAT settings

        # 'FEEDS' setting takes precedence over 'FEED_URI'
        for settings_uri, feed_options in self.settings.getdict("FEEDS").items():
            # handle pathlib.Path objects
            uri = (
                str(settings_uri.absolute())
                if isinstance(settings_uri, Path)
                else str(settings_uri)
            )
            self.feeds[uri] = feed_complete_default_values_from_settings(
                feed_options, self.settings, uri
            )
            self.filters[uri] = self._load_filter(feed_options)
            self.processors[uri] = self._load_processor(feed_options)
            self._receipt_keys[uri] = self._load_receipt_key(feed_options)

        self._init_receipts()

        self.storages: dict[str, type[FeedStorageProtocol]] = self._load_components(
            "FEED_STORAGES"
        )
        self.exporters: dict[str, type[BaseItemExporter]] = self._load_components(
            "FEED_EXPORTERS"
        )
        if any(
            feed_options.get("mode") is None for feed_options in self.feeds.values()
        ):
            warnings.warn(
                "The default value of the FEED_MODE setting will change from "
                "None to 'create' in a future Scrapy version, i.e. Scrapy will "
                "stop writing feeds whose target already exists. Explicitly "
                "set FEED_MODE, or the mode feed option of every feed, to "
                "silence this warning.",
                category=ScrapyDeprecationWarning,
                stacklevel=2,
            )

        for uri, feed_options in self.feeds.items():
            if not self._storage_supported(uri, feed_options):
                raise NotConfigured
            if not self._settings_are_valid():
                raise NotConfigured
            if not self._exporter_supported(feed_options["format"]):
                raise NotConfigured

    def open_spider(self, spider: Spider) -> None:
        self._slots_initialized = False
        for uri, feed_options in self.feeds.items():
            uri_params = self._get_uri_params(spider, feed_options["uri_params"])
            try:
                resolved_uri = apply_uri_params(uri, uri_params)
            except KeyError as exc:
                logger.error(
                    f"Feed {uri!r} could not be opened: it contains a "
                    f"placeholder for {exc}, which is not a spider "
                    f"attribute and was not provided by the uri_params "
                    f"function."
                )
                continue
            self.slots.append(
                self._start_new_batch(
                    batch_id=1,
                    uri=resolved_uri,
                    feed_options=feed_options,
                    spider=spider,
                    uri_template=uri,
                )
            )

    async def _on_state_loaded(self, state: dict[str, Any]) -> None:
        """Update initial batch-1 slots with the correct resumed batch IDs.

        Called via the spider_state_loaded signal after SpiderState has
        populated spider.state. Safe to mutate slots here because FeedSlot
        opens its file lazily; no I/O has occurred yet.
        """
        feed_batch_ids: dict[str, int] = state.get("feed_batch_ids", {})
        spider = self.crawler.spider
        assert spider is not None
        for slot in self.slots:
            batch_id = feed_batch_ids.get(slot.uri_template, 0)
            if slot.feed_options["delivery_receipts"] and self._receipts is not None:
                # A receipt journal records the batches that were stored and
                # the ones that were merely attempted. Reusing an attempted
                # batch would append to, or overwrite, a file that a failed
                # store may have left behind.
                saved_id = self._receipts.next_batch_id(slot.uri_template)
                if saved_id > 1:
                    batch_id = saved_id
            if batch_id in {0, 1}:
                continue
            uri_params = self._get_uri_params(
                spider, self.feeds[slot.uri_template]["uri_params"]
            )
            uri_params["batch_id"] = batch_id
            uri = apply_uri_params(slot.uri_template, uri_params)
            slot.batch_id = batch_id
            slot.uri = uri
            slot.storage = self._get_storage(uri, self.feeds[slot.uri_template])
        self._slots_initialized = True
        await self.crawler.signals.send_catch_log_async(
            signals.feed_slots_initialized, slots=self.slots
        )

    async def _on_engine_started(self) -> None:
        if not self._slots_initialized:
            self._slots_initialized = True
            await self.crawler.signals.send_catch_log_async(
                signals.feed_slots_initialized, slots=self.slots
            )

    def _on_state_saving(self, state: dict[str, Any]) -> None:
        """Persist the current batch ID for each feed into spider.state.

        Called via the spider_state_saving signal before SpiderState writes
        spider.state to disk, so the next run can resume from the right batch.
        """
        feed_batch_ids = state.setdefault("feed_batch_ids", {})
        for slot in self.slots:
            if slot.feed_options["delivery_receipts"]:
                # The receipt journal, not spider.state, is the source of
                # truth for receipt-enabled feeds: spider.state is not
                # written during the crawl.
                if self._receipts is not None:
                    feed_batch_ids[slot.uri_template] = (
                        self._receipts.next_batch_id(slot.uri_template)
                    )
            else:
                feed_batch_ids[slot.uri_template] = slot.batch_id

    def _skip(self, slot: FeedSlot) -> None:
        """Report that the target of *slot* already exists, and mark *slot* so
        that nothing is written to it."""
        slot._skipped = True
        logger.error(
            f"Not writing {slot.uri} because it already exists and the feed "
            "mode is 'create'; its items are lost. To write it instead, remove "
            "the target, or set the mode feed option or the FEED_MODE setting "
            "to 'overwrite' or 'append' (-O implies 'overwrite').",
            extra={"spider": slot.spider},
        )
        assert self.crawler.stats
        self.crawler.stats.inc_value(
            f"feedexport/conflicts/{type(slot.storage).__name__}"
        )

    async def close_spider(self, spider: Spider) -> None:
        for slot in self.slots:
            self._schedule_slot_close(slot, spider)

        if self._pending_close_tasks:  # pragma: no branch
            if is_asyncio_available():
                await asyncio.wait(
                    cast("list[asyncio.Task[None]]", list(self._pending_close_tasks))
                )
            else:
                await DeferredList(
                    cast("list[Deferred[None]]", list(self._pending_close_tasks))
                )

        # Send FEED_EXPORTER_CLOSED signal
        await self.crawler.signals.send_catch_log_async(signals.feed_exporter_closed)

    def _schedule_slot_close(
        self, slot: FeedSlot, spider: Spider
    ) -> asyncio.Task[None] | Deferred[None]:
        """Start closing the slot without waiting for it to finish, keeping
        track of the pending work so that it can be awaited in
        :meth:`close_spider` if it hasn't finished by then."""
        aw: asyncio.Task[None] | Deferred[None]
        coro = self._close_slot(slot, spider)
        if is_asyncio_available():
            aw = asyncio.create_task(coro)
            self._pending_close_tasks.append(aw)
            aw.add_done_callback(self._pending_close_tasks.remove)
        else:
            aw = deferred_from_coro(coro)
            self._pending_close_tasks.append(aw)
            aw.addBoth(self._untrack_pending_close_task, aw)
        return aw

    def _untrack_pending_close_task(self, result: Any, aw: Deferred[None]) -> Any:
        self._pending_close_tasks.remove(aw)
        return result

    @staticmethod
    def _get_file(slot_: FeedSlot) -> IO[bytes]:
        assert slot_.file
        if isinstance(slot_.file, PostProcessingManager):
            file = slot_.file.file
            slot_.file.close()
            return file
        return slot_.file

    @staticmethod
    def _reset_file(slot: FeedSlot) -> None:
        """Forget the file of a slot whose storage failed, so that the slot
        cannot be reused as if nothing had happened."""
        if isinstance(slot.file, PostProcessingManager):
            file = slot.file.file
        else:
            file = slot.file
        with contextlib.suppress(Exception):
            assert file is not None
            file.close()
        slot.file = None
        slot.exporter = None
        slot._fileloaded = False
        slot._exporting = False
        slot._pending_receipts.clear()
        slot._pending_count = 0

    async def _finish_slot(self, slot: FeedSlot) -> None:
        try:
            await slot.finish_exporting()
        except Exception:
            if self._receipts is not None and slot.feed_options["delivery_receipts"]:
                self._receipts.mark_attempted(slot.uri_template, slot.batch_id)
            raise

    async def _close_slot(self, slot: FeedSlot, spider: Spider) -> None:
        if slot._skipped:
            return

        if slot.itemcount:
            # Normal case
            await self._finish_slot(slot)
        elif slot.store_empty and slot.batch_id == 1:
            # Need to store the empty file
            await slot.start_exporting()
            await self._finish_slot(slot)
        else:
            # In this case, the file is not stored, so no processing is
            # required.
            return

        logmsg = f"{slot.format} feed ({slot.itemcount} items) in: {slot.uri}"
        slot_type = type(slot.storage).__name__
        self.crawler.stats.inc_value(
            f"feedexport/item_count/{slot_type}", slot.itemcount
        )
        if (
            slot.feed_options["delivery_receipts"]
            and slot.itemcount
            and not slot._pending_count
        ):
            # Every entry of this slot was already delivered by an earlier
            # run, so writing a file now would duplicate them.
            await self.crawler.signals.send_catch_log_async(
                signals.feed_slot_closed, slot=slot
            )
            return
        if self._receipts is not None and slot.feed_options["delivery_receipts"]:
            self._receipts.mark_attempted(slot.uri_template, slot.batch_id)
        try:
            await ensure_awaitable(slot.storage.store(self._get_file(slot)))
        except FileExistsError:
            self._skip(slot)
            self.crawler.stats.inc_value(f"feedexport/failed_count/{slot_type}")
            self._reset_file(slot)
        except Exception:
            logger.exception(
                "Error storing %s",
                logmsg,
                extra={"spider": spider},
            )
            self.crawler.stats.inc_value(f"feedexport/failed_count/{slot_type}")
            self._reset_file(slot)
            if self._receipts is not None and slot.feed_options["delivery_receipts"]:
                # The partial file may still be on disk. Move the retry to a
                # different batch instead of appending to or overwriting it.
                self._receipts.mark_attempted(slot.uri_template, slot.batch_id)
        else:
            logger.info("Stored %s", logmsg, extra={"spider": spider})
            self.crawler.stats.inc_value(f"feedexport/success_count/{slot_type}")
            if self._receipts is not None and slot.feed_options["delivery_receipts"]:
                slot._delivered = True
                slot._delivered_receipts = list(slot._pending_receipts)
                self._receipts.add(
                    slot._delivered_receipts,
                    batch=(slot.uri_template, slot.batch_id),
                )

        await self.crawler.signals.send_catch_log_async(
            signals.feed_slot_closed, slot=slot
        )

    @property
    def delivery_receipts(self) -> list[FeedItemReceipt]:
        """The entries that have reached storage, across all feeds.

        Only feeds with the ``delivery_receipts`` feed option are tracked.
        """
        if self._receipts is None:
            return []
        return self._receipts.all()

    def _start_new_batch(
        self,
        batch_id: int,
        uri: str,
        feed_options: dict[str, Any],
        spider: Spider,
        uri_template: str,
    ) -> FeedSlot:
        """
        Redirect the output data stream to a new file.
        Execute multiple times if FEED_EXPORT_BATCH_ITEM_COUNT setting or FEEDS.batch_item_count is specified
        :param batch_id: sequence number of current batch
        :param uri: uri of the new batch to start
        :param feed_options: dict with parameters of feed
        :param spider: user spider
        :param uri_template: template of uri which contains %(batch_time)s or %(batch_id)d to create new uri
        """
        storage = self._get_storage(uri, feed_options)
        return FeedSlot(
            storage=storage,
            uri=uri,
            format=feed_options["format"],
            store_empty=feed_options["store_empty"],
            batch_id=batch_id,
            uri_template=uri_template,
            filter=self.filters[uri_template],
            feed_options=feed_options,
            spider=spider,
            exporters=self.exporters,
            settings=self.settings,
            crawler=self.crawler,
        )

    async def item_scraped(self, item: Any, spider: Spider) -> None:
        if not self._export_lock.locked:
            # Acquiring a free lock fires the returned Deferred right away
            self._export_lock.acquire()
        else:
            await maybe_deferred_to_future(self._export_lock.acquire())
        try:
            await self._export_item(item, spider)
        finally:
            self._export_lock.release()

    def _needs_rotation(self, slot: FeedSlot) -> bool:
        batch_count = self.feeds[slot.uri_template]["batch_item_count"]
        if not batch_count:
            return False
        if slot.feed_options["delivery_receipts"]:
            # Only the entries written and not skipped in this run count.
            return bool(slot._pending_count >= batch_count)
        return bool(slot.itemcount >= batch_count)

    async def _export_entry(
        self,
        slot: FeedSlot,
        item_key: str | None,
        index: int,
        exported_item: Any,
    ) -> None:
        if item_key is not None:
            assert self._receipts is not None
            entry_key = _FeedReceipts.entry_key(slot.uri_template, item_key, index)
            if self._receipts.delivered(slot.uri_template, entry_key):
                return
        if not slot._skipped:
            try:
                await slot.start_exporting()
            except FileExistsError:
                self._skip(slot)
        if not slot._skipped:
            assert slot.exporter
            try:
                await _maybe_coroutine(slot.exporter.export_item, exported_item)
            except Exception as e:
                e.add_note(f"Item: {exported_item!r}")
                raise
            if item_key is not None:
                slot.add_receipt(item_key, index)
                slot._pending_count += 1

    async def _export_item(self, item: Any, spider: Spider) -> None:
        item_key = None
        slots = []
        for slot in self.slots:
            if not slot.filter.accepts(item):
                slots.append(
                    slot
                )  # if slot doesn't accept item, continue with next slot
                continue

            processor = self.processors[slot.uri_template]
            exported_items = list(processor(item)) if processor else [item]
            tracked = slot.feed_options["delivery_receipts"]
            if tracked and item_key is None:
                item_key = _item_receipt_key(
                    item, self._receipt_keys[slot.uri_template]
                )
            for index, exported_item in enumerate(exported_items):
                await self._export_entry(
                    slot, item_key if tracked else None, index, exported_item
                )
                # Skipped items are counted, so that the following files of
                # this feed still cover the same items as those of any other
                # feed.
                slot.itemcount += 1
                if self._needs_rotation(slot):
                    uri_params = self._get_uri_params(
                        spider, self.feeds[slot.uri_template]["uri_params"], slot
                    )
                    self._schedule_slot_close(slot, spider)
                    slot = self._start_new_batch(  # noqa: PLW2901
                        batch_id=slot.batch_id + 1,
                        uri=apply_uri_params(slot.uri_template, uri_params),
                        feed_options=self.feeds[slot.uri_template],
                        spider=spider,
                        uri_template=slot.uri_template,
                    )
            slots.append(slot)
        self.slots = slots

    def _load_components(self, setting_prefix: str) -> dict[str, Any]:
        conf = without_none_values(
            cast("dict[str, str]", self.settings.getwithbase(setting_prefix))
        )
        d = {}
        for k, v in conf.items():
            with contextlib.suppress(NotConfigured):
                d[k] = load_object(v)
        return d

    def _exporter_supported(self, format_: str) -> bool:
        if format_ in self.exporters:
            return True
        if format_:
            logger.error("Unknown feed format: %(format)s", {"format": format_})
        else:
            logger.error(
                "Feed format not set and it could not be inferred from the "
                "feed URI; set it explicitly with the 'format' key"
            )
        return False

    def _settings_are_valid(self) -> bool:
        """
        If FEED_EXPORT_BATCH_ITEM_COUNT setting or FEEDS.batch_item_count is specified uri has to contain
        %(batch_time)s or %(batch_id)d to distinguish different files of partial output
        """
        for uri_template, values in self.feeds.items():
            if values["batch_item_count"] and not re.search(
                r"%\(batch_time\)s|%\(batch_id\)", uri_template
            ):
                logger.error(
                    "%%(batch_time)s or %%(batch_id)d must be in the feed URI (%s) if FEED_EXPORT_BATCH_ITEM_COUNT "
                    "setting or FEEDS.batch_item_count is specified and greater than 0. For more info see: "
                    "https://docs.scrapy.org/en/latest/topics/feed-exports.html#feed-export-batch-item-count",
                    uri_template,
                )
                return False
        return True

    def _storage_supported(self, uri: str, feed_options: dict[str, Any]) -> bool:
        scheme = urlparse(uri).scheme
        if scheme in self.storages or PureWindowsPath(uri).drive:
            try:
                storage = self._get_storage(uri, feed_options)
                self._check_storage_mode(uri, storage, feed_options)
                return True
            except NotConfigured as e:
                logger.error(
                    "Disabled feed storage scheme: %(scheme)s. Reason: %(reason)s",
                    {"scheme": scheme, "reason": str(e)},
                )
        else:
            logger.error("Unknown feed storage scheme: %(scheme)s", {"scheme": scheme})
        return False

    @staticmethod
    def _check_storage_mode(
        uri: str, storage: FeedStorageProtocol, feed_options: dict[str, Any]
    ) -> None:
        mode = feed_options.get("mode")
        if mode is None:
            # Every storage keeps its own historical mode.
            return
        if getattr(storage, "supported_modes", None) is None:
            logger.warning(
                f"{type(storage).__name__} does not declare which feed modes it "
                f"supports, so the mode feed option of the {uri} feed ({mode!r}) "
                "may be ignored."
            )
            return
        _check_mode(mode, storage, uri)

    def _get_storage(
        self, uri: str, feed_options: dict[str, Any]
    ) -> FeedStorageProtocol:
        """Build a storage object for the specified *uri* with the specified
        *feed_options*."""
        cls = self.storages.get(urlparse(uri).scheme, self.storages["file"])
        return build_from_crawler(cls, self.crawler, uri, feed_options=feed_options)

    def _get_uri_params(
        self,
        spider: Spider,
        uri_params_function: str | UriParamsCallableT | None,
        slot: FeedSlot | None = None,
    ) -> dict[str, Any]:
        params = {k: getattr(spider, k) for k in dir(spider)}
        utc_now = datetime.now(tz=UTC)
        params["time"] = utc_now.replace(microsecond=0).isoformat().replace(":", "-")
        params["batch_time"] = utc_now.isoformat().replace(":", "-")
        params["batch_id"] = slot.batch_id + 1 if slot is not None else 1
        uripar_function: UriParamsCallableT = (
            load_object(uri_params_function)
            if uri_params_function
            else lambda params, _: params
        )
        new_params = uripar_function(params, spider)
        return new_params if new_params is not None else params

    def _load_filter(self, feed_options: dict[str, Any]) -> ItemFilter:
        # load the item filter if declared else load the default filter class
        item_filter_class: type[ItemFilter] = load_object(
            feed_options.get("item_filter", ItemFilter)
        )
        return item_filter_class(feed_options)

    def _load_processor(self, feed_options: dict[str, Any]) -> _ItemProcessor | None:
        item_processor = feed_options.get("item_processor")
        return load_object(item_processor) if item_processor else None

    def _load_receipt_key(
        self, feed_options: dict[str, Any]
    ) -> _ItemReceiptKey | None:
        receipt_key = feed_options.get("delivery_receipt_key")
        return load_object(receipt_key) if receipt_key else None

    def _init_receipts(self) -> None:
        jobdir = _job_dir(self.settings)
        enabled_by_setting = self.settings.getbool(
            "FEED_EXPORT_DELIVERY_RECEIPTS"
        )
        for feed_options in self.feeds.values():
            enabled = feed_options.get("delivery_receipts")
            if enabled is None:
                # A feed that reshapes or filters items needs per-entry
                # tracking to resume without duplicates or gaps. Plain batch
                # feeds keep the legacy batch-ID rotation.
                enabled = enabled_by_setting or (
                    bool(jobdir)
                    and bool(
                        feed_options.get("item_processor")
                        or feed_options.get("item_filter")
                        or feed_options.get("item_classes")
                    )
                )
            feed_options["delivery_receipts"] = bool(enabled)
        if not any(
            feed_options["delivery_receipts"]
            for feed_options in self.feeds.values()
        ):
            return
        if jobdir is None:
            raise NotConfigured("Feed delivery receipts need a JOBDIR setting")
        self._receipts = _FeedReceipts(Path(jobdir, _FEED_RECEIPTS_FILENAME))


def __getattr__(name: str) -> Any:  # pragma: no cover
    if name == "IFeedStorage":
        warnings.warn(
            "scrapy.extensions.feedexport.IFeedStorage is deprecated.",
            ScrapyDeprecationWarning,
            stacklevel=2,
        )
        return _IFeedStorage
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
