"""Per-record deserialization failure reporting.

When a consumer is created with ``consumer_record_error_handler`` set, a
failure of the key or value deserializer on a single record no longer kills
the whole fetched batch. Good records keep flowing through ``getone`` and
``getmany`` unchanged, while the offending record is exposed as a
:class:`RecordDecodeFailure` that the consumer may retry or skip explicitly.
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from aiokafka.structs import ConsumerRecord, TopicPartition


class FailureResolver(Protocol):
    """Wired by the fetcher: ``True`` retries, ``False`` skips the record."""

    def __call__(self, *, retry: bool) -> Awaitable[Any]: ...


#: The failure happened while running the key deserializer.
KEY = "key"
#: The failure happened while running the value deserializer.
VALUE = "value"


class RecordDecodeFailure:
    """Report of a record whose key or value could not be deserialized.

    Instances are produced instead of raising out of ``getone``/``getmany``
    when the consumer was created with ``consumer_record_error_handler``.
    A report is single-use: exactly one of :meth:`retry` and :meth:`skip`
    must be awaited before consumption of that partition can continue.

    The report always refers to one specific record of one specific fetch
    response, so retry/skip never re-fetch data from the broker and records
    that were already delivered are never decoded again.
    """

    __slots__ = (
        "_resume",
        "exception",
        "failed_part",
        "offset",
        "raw_key",
        "raw_value",
        "topic_partition",
    )

    def __init__(
        self,
        topic_partition: TopicPartition,
        offset: int,
        failed_part: str,
        raw_key: bytes | None,
        raw_value: bytes | None,
        exception: BaseException,
        resume: FailureResolver,
    ):
        self.topic_partition = topic_partition
        self.offset = offset
        self.failed_part = failed_part
        self.raw_key = raw_key
        self.raw_value = raw_value
        self.exception = exception
        self._resume = resume

    @property
    def partition(self) -> int:
        return self.topic_partition.partition

    @property
    def topic(self) -> str:
        return self.topic_partition.topic

    async def retry(self) -> ConsumerRecord[Any, Any]:
        """Retry decoding this record using the currently configured deserializers.

        Only the part that originally failed (key or value) is decoded again;
        the other part and any raw bytes are reused from the original fetch.
        Returns the fully decoded
        :class:`~aiokafka.structs.ConsumerRecord`, or raises the underlying
        error if the deserializer still fails. A failed retry keeps the
        report valid, so it may be retried again later (for example after a
        dictionary reload).
        """
        return await self._resume(retry=True)

    async def skip(self) -> None:
        """Drop this record and advance the partition past its offset."""
        await self._resume(retry=False)

    def __repr__(self) -> str:
        return (
            f"<RecordDecodeFailure {self.failed_part} "
            f"{self.topic_partition}@{self.offset} cause={self.exception!r}>"
        )
