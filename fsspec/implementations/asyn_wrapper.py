import asyncio
import contextvars
import functools
import inspect
import threading

import fsspec
from fsspec.asyn import AsyncFileSystem, running_async

from .chained import ChainedFileSystem

_ABORTED = object()


async def _run_sync_in_thread(func, *args, **kwargs):
    """Run a synchronous callable in the loop's default executor.

    Cancellation is only honoured once the synchronous call can no longer be
    using anything the caller guards on the coroutine (semaphore permits,
    destination paths): if the call has not started yet it is prevented from
    starting at all, and if it is already running in a worker thread the
    cancellation is held back - however often it is repeated - until the
    thread has returned.
    """
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    lock = threading.Lock()
    started = threading.Event()
    aborted = threading.Event()

    def call():
        with lock:
            if aborted.is_set():
                return _ABORTED
            started.set()
        return ctx.run(func, *args, **kwargs)

    future = loop.run_in_executor(None, call)
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        with lock:
            if started.is_set():
                queued = False
            else:
                # The call never runs the synchronous function: either the
                # executor drops it, or ``call`` sees ``aborted`` first.
                aborted.set()
                queued = True
        if queued:
            future.cancel()
            raise
        # The synchronous call is running in a worker thread and cannot be
        # interrupted. Wait for it to finish, absorbing further
        # cancellations, so nothing it may still be using is released
        # early. Its result or exception is discarded: this call was
        # cancelled, and that is what the caller must observe.
        while not future.done():
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        raise


def async_wrapper(func, obj=None, semaphore=None):
    """
    Wraps a synchronous function to make it awaitable.

    Parameters
    ----------
    func : callable
        The synchronous function to wrap.
    obj : object, optional
        The instance to bind the function to, if applicable.
    semaphore : asyncio.Semaphore, optional
        A semaphore to limit concurrent calls.

    Returns
    -------
    coroutine
        An awaitable version of the function.
    """

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if semaphore:
            async with semaphore:
                return await _run_sync_in_thread(func, *args, **kwargs)
        return await _run_sync_in_thread(func, *args, **kwargs)

    return wrapper


class AsyncFileSystemWrapper(AsyncFileSystem, ChainedFileSystem):
    """
    A wrapper class to convert a synchronous filesystem into an asynchronous one.

    This class takes an existing synchronous filesystem implementation and wraps all
    its methods to provide an asynchronous interface.

    Parameters
    ----------
    sync_fs : AbstractFileSystem
        The synchronous filesystem instance to wrap.
    """

    protocol = "asyncwrapper", "async_wrapper"
    cachable = False

    def __init__(
        self,
        fs=None,
        asynchronous=None,
        target_protocol=None,
        target_options=None,
        semaphore=None,
        max_concurrent_tasks=None,
        **kwargs,
    ):
        if asynchronous is None:
            asynchronous = running_async()
        super().__init__(asynchronous=asynchronous, **kwargs)
        if fs is not None:
            self.sync_fs = fs
        else:
            self.sync_fs = fsspec.filesystem(target_protocol, **target_options)
        self.protocol = self.sync_fs.protocol
        # paths must be normalized the way the wrapped filesystem does it
        self.root_marker = self.sync_fs.root_marker
        self._strip_protocol = self.sync_fs._strip_protocol
        self._parent = self.sync_fs._parent
        self.semaphore = semaphore
        self._wrap_all_sync_methods()

    def _open(self, *args, **kwargs):
        return self.sync_fs._open(*args, **kwargs)

    @property
    def fsid(self):
        return f"async_{self.sync_fs.fsid}"

    def _wrap_all_sync_methods(self):
        """
        Wrap all synchronous methods of the underlying filesystem with asynchronous versions.
        """
        excluded_methods = {"open"}
        for method_name in dir(self.sync_fs):
            if method_name.startswith("_") or method_name in excluded_methods:
                continue

            attr = inspect.getattr_static(self.sync_fs, method_name)
            if isinstance(attr, property):
                continue

            method = getattr(self.sync_fs, method_name)
            # generators such as ``walk`` cannot become coroutines: the async API
            # expects async generators, which AsyncFileSystem already builds on
            # top of the wrapped methods (``_walk`` uses ``_ls``)
            if (
                callable(method)
                and not inspect.iscoroutinefunction(method)
                and not inspect.isgeneratorfunction(method)
            ):
                async_method = async_wrapper(method, obj=self, semaphore=self.semaphore)
                setattr(self, f"_{method_name}", async_method)

    @classmethod
    def wrap_class(cls, sync_fs_class):
        """
        Create a new class that can be used to instantiate an AsyncFileSystemWrapper
        with lazy instantiation of the underlying synchronous filesystem.

        Parameters
        ----------
        sync_fs_class : type
            The class of the synchronous filesystem to wrap.

        Returns
        -------
        type
            A new class that wraps the provided synchronous filesystem class.
        """

        class GeneratedAsyncFileSystemWrapper(cls):
            def __init__(self, *args, **kwargs):
                sync_fs = sync_fs_class(*args, **kwargs)
                super().__init__(sync_fs)

        GeneratedAsyncFileSystemWrapper.__name__ = (
            f"Async{sync_fs_class.__name__}Wrapper"
        )
        return GeneratedAsyncFileSystemWrapper
