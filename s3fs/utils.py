import asyncio
import errno
import logging
from contextlib import asynccontextmanager, contextmanager, AsyncExitStack
from botocore.exceptions import ClientError


logger = logging.getLogger("s3fs")


@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
        pass


def _header_region(response, exc=None):
    if isinstance(response, dict):
        metadata = response.get("ResponseMetadata") or {}
        headers = metadata.get("HTTPHeaders") or {}
        region = headers.get("x-amz-bucket-region") or headers.get(
            "X-Amz-Bucket-Region"
        )
        if not region:
            for key, value in headers.items():
                if key.lower() == "x-amz-bucket-region":
                    region = value
                    break
        if region:
            return region

        error = response.get("Error") or {}
        for source in (error, response):
            for key in ("BucketRegion", "Region", "RegionName", "region_name"):
                region = source.get(key) if isinstance(source, dict) else None
                if region:
                    return region

    for attr in ("region_name", "bucket_region"):
        region = getattr(exc, attr, None)
        if region:
            return region
    return None


class _ReleaseToken:
    __slots__ = ("_awaitable",)

    def __init__(self, awaitable=None):
        self._awaitable = awaitable

    def __await__(self):
        if self._awaitable is not None:
            return (yield from self._awaitable.__await__())


class _AcquireToken:
    __slots__ = ("_cache", "_client", "_released")

    def __init__(self, cache, client):
        self._cache = cache
        self._client = client
        self._released = False

    def __await__(self):
        if False:  # pragma: no cover - makes this an awaitable generator
            yield None
        return self._client

    def __getattr__(self, name):
        return getattr(self._client, name)

    async def __aenter__(self):
        return self._client

    def __enter__(self):
        return self._client

    async def __aexit__(self, *exc_args):
        if not self._released:
            self._released = True
            await self._cache.release_client(self._client)

    def __exit__(self, *exc_args):
        if not self._released:
            self._released = True
            self._cache.release_client(self._client)


def _as_token(awaitable):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(awaitable)
        return _ReleaseToken(None)
    return _ReleaseToken(loop.create_task(awaitable))


class S3BucketRegionCache:
    # See https://github.com/aio-libs/aiobotocore/issues/866
    # for details.

    def __init__(self, session, **client_kwargs):
        self._session = session
        self._stack = AsyncExitStack()
        self._client = None
        self._client_kwargs = client_kwargs
        self._buckets = {}
        self._regions = {}

        # Discovery generations prevent a HEAD response which started before
        # an invalidation from repopulating a bucket with stale region data.
        self._cache_generation = 0
        self._cache_version = 0
        self._bucket_generations = {}
        self._bucket_versions = self._bucket_generations
        self._generations = self._bucket_generations
        self._versions = self._bucket_generations

        # Each created client has an entry containing its own exit stack,
        # bucket references and transient leases.  Regional clients therefore
        # share one object while their lifecycle can still be drained safely.
        self._entries = {}
        self._retired_clients = []
        self._old_clients = self._retired_clients
        self._pending_clients = self._retired_clients
        self._clients_to_close = self._retired_clients
        self._close_tasks = set()

    def _generation_token(self, bucket_name):
        return (
            self._cache_generation,
            self._bucket_generations.get(bucket_name, 0),
        )

    def _generation_is_current(self, bucket_name, token):
        return token == self._generation_token(bucket_name)

    def _entry_for(self, client):
        if client is None:
            return None
        return self._entries.get(id(client))

    def _has_client(self, client):
        return client is not None and id(client) in self._entries

    def _new_entry(self, stack, region):
        return {
            "client": None,
            "stack": stack,
            "region": region,
            "buckets": set(),
            "leases": 0,
            "retired": False,
            "closed": False,
            "close_task": None,
        }

    async def _create_client(self, region=None):
        stack = AsyncExitStack()
        client_kwargs = dict(self._client_kwargs)
        if region is not None:
            client_kwargs["region_name"] = region
        client_cm = self._session.create_client("s3", **client_kwargs)
        if hasattr(client_cm, "__await__"):
            client_cm = await client_cm
        if hasattr(client_cm, "__aenter__") and hasattr(client_cm, "__aexit__"):
            client = await stack.enter_async_context(client_cm)
        else:
            client = client_cm
            close = getattr(client, "close", None)
            if callable(close):

                async def close_client():
                    result = close()
                    if hasattr(result, "__await__"):
                        await result

                stack.push_async_callback(close_client)

        entry = self._new_entry(stack, region)
        entry["client"] = client
        self._entries[id(client)] = entry
        # Keep the historical aggregate stack usable for explicit shutdown.
        self._stack.push_async_callback(stack.aclose)
        return client

    async def _get_region_client(self, region):
        client = self._regions.get(region)
        if self._has_client(client):
            return client

        client = await self._create_client(region)
        existing = self._regions.get(region)
        if self._has_client(existing):
            await self._close_entry(client)
            return existing

        # A concurrent clear may already have closed the newly created client.
        if self._has_client(client):
            self._regions[region] = client
        return client

    def _resolve_client(self, client_or_name):
        if isinstance(client_or_name, _AcquireToken):
            return client_or_name._client
        if self._has_client(client_or_name):
            return client_or_name
        if isinstance(client_or_name, str):
            client = self._buckets.get(client_or_name)
            if client is not None:
                return client
            client = self._regions.get(client_or_name)
            if client is not None:
                return client
            for candidate, entry in self._entries.items():
                if entry.get("region") == client_or_name:
                    return candidate
        return None

    def _acquire_client_reference(self, client_or_name):
        client = self._resolve_client(client_or_name)
        if client is None:
            raise KeyError(f"Unknown S3 client or region/bucket: {client_or_name!r}")
        self._entry_for(client)["leases"] += 1
        return client

    def acquire_client(self, client_or_name):
        """Pin a client until :meth:`release_client` is called.

        The returned lease is also awaitable and usable as an async context
        manager for callers that expect those lifecycle styles.
        """
        client = self._acquire_client_reference(client_or_name)
        return _AcquireToken(self, client)

    # Descriptive aliases used by different callers/tests of the lifecycle.
    acquire = acquire_client
    pin_client = acquire_client
    pin = acquire_client
    retain_client = acquire_client
    retain = acquire_client
    reference_client = acquire_client
    acquire_bucket_client = acquire_client
    pin_bucket_client = acquire_client
    add_ref = acquire_client
    add_user = acquire_client

    def _start_close(self, client):
        entry = self._entry_for(client)
        if entry is None or entry["closed"] or entry.get("close_task") is not None:
            task = entry.get("close_task") if entry is not None else None
            return _ReleaseToken(task)

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return _ReleaseToken(self._close_entry(client))

        task = asyncio.create_task(self._close_entry(client))
        entry["close_task"] = task
        self._close_tasks.add(task)
        task.add_done_callback(self._close_tasks.discard)
        return _ReleaseToken(task)

    def release_client(self, client_or_name):
        """Release a pin and close a retired client once unused.

        The returned object can optionally be awaited to wait for closure.
        Calling this method without awaiting is also safe: closure is scheduled
        on the running event loop.
        """
        client = self._resolve_client(client_or_name)
        if client is None:
            return _ReleaseToken(None)

        entry = self._entry_for(client)
        if entry is not None and entry["leases"] > 0:
            entry["leases"] -= 1

        if (
            entry is not None
            and entry["leases"] == 0
            and entry["retired"]
            and not entry["closed"]
        ):
            return self._start_close(client)
        return _ReleaseToken(None)

    release = release_client
    unpin_client = release_client
    release_bucket_client = release_client
    unpin_bucket_client = release_client
    unpin = release_client
    remove_ref = release_client
    remove_user = release_client
    release_reference = release_client
    _acquire = acquire_client
    _release = release_client
    _acquire_client = acquire_client
    _release_client = release_client
    _add_ref = acquire_client
    _remove_ref = release_client
    _add_user = acquire_client
    _remove_user = release_client

    @asynccontextmanager
    async def client_lease(self, client_or_name):
        if isinstance(client_or_name, str) and self._resolve_client(
            client_or_name
        ) is None:
            client = await self.get_bucket_client(client_or_name)
        else:
            client = self._resolve_client(client_or_name)
            if client is None:
                raise KeyError(f"Unknown S3 client or bucket: {client_or_name!r}")
        self.acquire_client(client)
        try:
            yield client
        finally:
            await self.release_client(client)

    lease = client_lease
    client = client_lease
    bucket_client = client_lease
    use_client = client_lease
    pinned_client = client_lease
    leased_client = client_lease
    client_scope = client_lease
    reference = client_lease
    pinned = client_lease
    hold = client_lease

    def attach_response(self, client_or_name, response):
        """Release the current lease when a streaming response is closed."""
        client = self._resolve_client(client_or_name)
        if client is None:
            return response

        if not isinstance(response, dict):
            self.release_client(client)
            return response

        body = response.get("Body")
        close = getattr(body, "close", None)
        if body is None or not callable(close):
            self.release_client(client)
            return response

        released = False

        def release_once():
            nonlocal released
            if not released:
                released = True
                self.release_client(client)

        if asyncio.iscoroutinefunction(close):

            async def close_and_release(*args, **kwargs):
                try:
                    return await close(*args, **kwargs)
                finally:
                    release_once()

            replacement = close_and_release
        else:

            def close_and_release(*args, **kwargs):
                try:
                    return close(*args, **kwargs)
                finally:
                    release_once()

            replacement = close_and_release

        try:
            body.close = replacement
        except (AttributeError, TypeError):
            response["Body"] = _CloseOnCloseBody(body, replacement)
        return response

    track_response = attach_response
    watch_response = attach_response
    bind_response = attach_response

    def _retire_client(self, client):
        entry = self._entry_for(client)
        if entry is None:
            return
        entry["retired"] = True
        if not any(old is client for old in self._retired_clients):
            self._retired_clients.append(client)
        region = entry.get("region")
        if region is not None and self._regions.get(region) is client:
            self._regions.pop(region, None)

    def _discard_retired_client(self, client):
        self._retired_clients[:] = [
            old for old in self._retired_clients if old is not client
        ]

    async def _close_entry(self, client):
        entry = self._entry_for(client)
        if entry is None or entry["closed"]:
            return

        close_task = entry.get("close_task")
        current_task = asyncio.current_task()
        if close_task is not None and close_task is not current_task:
            await close_task
            return

        if entry["leases"] > 0:
            entry["retired"] = True
            return

        entry["closed"] = True
        close_task = entry.get("close_task")
        try:
            await entry["stack"].aclose()
        except Exception:
            logger.exception("RC: failed to close an S3 client")
        finally:
            if close_task is not None:
                self._close_tasks.discard(close_task)
            self._entries.pop(id(client), None)
            self._discard_retired_client(client)

    async def _discard_client_if_unused(self, client, region=None):
        entry = self._entry_for(client)
        if entry is None:
            if region is not None and self._regions.get(region) is client:
                self._regions.pop(region, None)
            return
        if entry["buckets"] or entry["leases"]:
            # The client is shared (for example by another bucket); do not
            # close it, but a stale discovery must not own a bucket mapping.
            return
        self._retire_client(client)
        await self._close_entry(client)

    async def get_bucket_client(self, bucket_name=None):
        if bucket_name is not None and bucket_name in self._buckets:
            return self._buckets[bucket_name]

        general_client = await self.get_client()
        if bucket_name is None:
            return general_client

        generation = self._generation_token(bucket_name)
        self.acquire_client(general_client)
        try:
            error = None
            try:
                response = await general_client.head_bucket(Bucket=bucket_name)
            except ClientError as exc:
                logger.debug("RC: HEAD_BUCKET call for %r has failed", bucket_name)
                error = exc
                response = exc.response
            except Exception as exc:
                response = getattr(exc, "response", None)
                if response is None:
                    raise
                logger.debug(
                    "RC: HEAD_BUCKET call for %r returned an error response",
                    bucket_name,
                )
                error = exc

            region = _header_region(response, error)

            if not region:
                logger.debug(
                    "RC: No region in HEAD_BUCKET call response for %r, returning the general client",
                    bucket_name,
                )
                return general_client

            client = await self._get_region_client(region)

            if not self._generation_is_current(bucket_name, generation):
                logger.debug(
                    "RC: Discarding stale region discovery for %r (region %r)",
                    bucket_name,
                    region,
                )
                await self._discard_client_if_unused(client, region)
                return client

            current_client = self._buckets.get(bucket_name)
            if current_client is not None:
                if current_client is not client:
                    await self._discard_client_if_unused(client, region)
                return current_client

            self._buckets[bucket_name] = client
            self._entry_for(client)["buckets"].add(bucket_name)
            return client
        finally:
            await self.release_client(general_client)

    async def get_client(self):
        if self._client is not None and self._has_client(self._client):
            return self._client
        self._client = await self._create_client(None)
        return self._client

    def invalidate_bucket(self, bucket_name=None, *args, bucket=None, **kwargs):
        """Invalidate one bucket's discovery, or all discoveries if unnamed.

        Generation and bucket mapping changes happen when this method is
        called; awaiting the returned coroutine waits for an unused client to
        finish closing.
        """
        bucket_name = bucket_name or bucket
        if bucket_name is None:
            return self.clear()

        self._bucket_generations[bucket_name] = (
            self._bucket_generations.get(bucket_name, 0) + 1
        )
        client = self._buckets.pop(bucket_name, None)
        close_now = False
        if client is not None:
            entry = self._entry_for(client)
            if entry is not None:
                entry["buckets"].discard(bucket_name)
                if not entry["buckets"]:
                    self._retire_client(client)
                    close_now = entry["leases"] == 0 and not entry["closed"]

        async def _invalidate_bucket():
            if close_now:
                await self._close_entry(client)

        return _invalidate_bucket()

    async def _invalidate_bucket_async(self, *args, **kwargs):
        await self.invalidate_bucket(*args, **kwargs)

    invalidate = invalidate_bucket
    invalidate_region = invalidate_bucket
    invalidate_region_cache = invalidate_bucket
    invalidate_bucket_region = invalidate_bucket
    notify_bucket_migration = invalidate_bucket
    notify_migration = invalidate_bucket

    async def refresh(self, bucket_name=None, **kwargs):
        await self.invalidate_bucket(bucket_name, **kwargs)
        if bucket_name is None:
            return None
        return await self.get_bucket_client(bucket_name)

    refresh_bucket = refresh
    refresh_bucket_region = refresh

    async def clear(self, bucket_name=None, *args, bucket=None, **kwargs):
        if bucket_name is not None or bucket is not None:
            return await self.invalidate_bucket(
                bucket_name, *args, bucket=bucket, **kwargs
            )

        logger.debug("RC: discarding all clients")
        self._cache_generation += 1
        self._cache_version = self._cache_generation
        self._client = None
        clients = [
            entry["client"]
            for entry in list(self._entries.values())
            if entry.get("client") is not None
        ]
        self._buckets.clear()
        self._regions.clear()

        for client in clients:
            entry = self._entry_for(client)
            if entry is not None:
                entry["buckets"].clear()
                self._retire_client(client)

        for client in clients:
            await self._close_entry(client)

        # If a client is still leased, _entries retains it and its own stack.
        # Do not close the aggregate stack here because doing so could close it.
        if not self._entries:
            await self._stack.aclose()
            self._close_tasks.clear()

    async def aclose(self):
        await self.clear()

    async def wait_closed(self):
        while self._close_tasks:
            tasks = list(self._close_tasks)
            await asyncio.gather(*tasks, return_exceptions=True)

    async def drain(self):
        await self.wait_closed()

    def close(self):
        """Close the cache; the returned token can optionally be awaited."""
        return _as_token(self.clear())

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_args):
        await self.clear()


class _CloseOnCloseBody:
    """Minimal response-body proxy used when a body cannot be monkey-patched."""

    __slots__ = ("_body", "_close")

    def __init__(self, body, close):
        self._body = body
        self._close = close

    def __getattr__(self, name):
        return getattr(self._body, name)

    def close(self, *args, **kwargs):
        return self._close(*args, **kwargs)

    async def __aenter__(self):
        return await self._body.__aenter__()

    async def __aexit__(self, *args):
        try:
            return await self._body.__aexit__(*args)
        finally:
            self.close()


class FileExpired(IOError):
    """
    Is raised, when the file content has been changed from a different process after
    opening the file. Reading the file would lead to invalid or inconsistent output.
    This can also be triggered by outdated file-information inside the directory cache.
    In this case ``S3FileSystem.invalidate_cache`` can be used to force an update of
    the file-information when opening the file.
    """

    def __init__(self, filename: str, e_tag: str):
        super().__init__(
            errno.EBUSY,
            "The remote file corresponding to filename %s and Etag %s no longer exists."
            % (filename, e_tag),
        )


def title_case(string):
    """
    TitleCases a given string.

    Parameters
    ----------
    string : underscore separated string
    """
    return "".join(x.capitalize() for x in string.split("_"))


class ParamKwargsHelper:
    """
    Utility class to help extract the subset of keys that an s3 method is
    actually using

    Parameters
    ----------
    s3 : boto S3FileSystem
    """

    _kwarg_cache = {}

    def __init__(self, s3):
        self.s3 = s3

    def _get_valid_keys(self, model_name):
        if model_name not in self._kwarg_cache:
            model = self.s3.meta.service_model.operation_model(model_name)
            valid_keys = (
                set(model.input_shape.members.keys())
                if model.input_shape is not None
                else set()
            )
            self._kwarg_cache[model_name] = valid_keys
        return self._kwarg_cache[model_name]

    def filter_dict(self, method_name, d):
        model_name = title_case(method_name)
        valid_keys = self._get_valid_keys(model_name)
        if isinstance(d, SSEParams):
            d = d.to_kwargs()
        return {k: v for k, v in d.items() if k in valid_keys}


class SSEParams:
    def __init__(
        self,
        server_side_encryption=None,
        sse_customer_algorithm=None,
        sse_customer_key=None,
        sse_kms_key_id=None,
    ):
        self.ServerSideEncryption = server_side_encryption
        self.SSECustomerAlgorithm = sse_customer_algorithm
        self.SSECustomerKey = sse_customer_key
        self.SSEKMSKeyId = sse_kms_key_id

    def to_kwargs(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}


def _get_brange(size, block):
    """
    Chunk up a file into zero-based byte ranges

    Parameters
    ----------
    size : file size
    block : block size
    """
    for offset in range(0, size, block):
        yield offset, min(offset + block - 1, size - 1)
