"""Local, position based checkpointing and deterministic replay for streamz.

The building blocks in this module let an operator save a snapshot of a small
local pipeline and rebuild it in another process, continuing from explicit
per-source positions:

* :class:`PositionedSource` numbers every emitted element with its own
  monotonically increasing position and can replay a local collection from an
  arbitrary position.
* :class:`checkpoint_transform` is an asynchronous, order preserving map whose
  work on individual positions can be gated, so in-flight elements can be held
  back deterministically.
* :class:`Checkpoint` is the serialisable snapshot container; it remembers the
  per-source cut positions, the state of every named node (e.g. unpaired
  ``zip`` buffers and confirmed ``accumulate`` state) and the inputs that were
  already received but lie after the cut.
* :class:`CheckpointManager` discovers named nodes reachable from the
  positioned sources, saves consistent snapshots and validates a rebuilt graph
  before restoring any state into it.

Streams that do not use these helpers are completely untouched: every hook on
the base :class:`~streamz.core.Stream` is opt-in and defaults to "no state".
"""
import asyncio
from collections import deque
import importlib
import inspect
import logging
import pickle
import time

from tornado.ioloop import IOLoop

from .core import Stream
from .sources import Source

logger = logging.getLogger(__name__)


class CheckpointError(RuntimeError):
    """Raised when a checkpoint cannot be taken or restored consistently."""


def _qualified_name(cls):
    return "{}.{}".format(cls.__module__, cls.__qualname__)


def _resolve_class(qualified_name):
    module_name, _, class_name = qualified_name.rpartition(".")
    if not module_name:
        raise CheckpointError(
            "Cannot resolve operator class {!r}".format(qualified_name)
        )
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise CheckpointError(
            "Cannot import operator module {!r}: {}".format(module_name, exc)
        ) from exc
    try:
        obj = module
        for part in class_name.split("."):
            obj = getattr(obj, part)
    except AttributeError as exc:
        raise CheckpointError(
            "Operator class {!r} no longer exists: {}".format(qualified_name, exc)
        ) from exc
    return obj


async def _wait_until(predicate, timeout, message):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise CheckpointError(message)
        await asyncio.sleep(0.001)


def _walk_downstream(sources):
    """Return all nodes reachable from ``sources`` in deterministic order."""
    ordered = []
    seen = set()
    queue = deque(sources)
    while queue:
        node = queue.popleft()
        if id(node) in seen:
            continue
        seen.add(id(node))
        ordered.append(node)
        for downstream in node.downstreams:
            if id(downstream) not in seen:
                queue.append(downstream)
    return ordered


class PositionedSource(Source):
    """A source that replays a local collection from explicit positions.

    Every element is assigned the position it occupies in ``values`` (starting
    at zero).  Positions are stable across processes, which makes them suitable
    as checkpoint cut points.

    Parameters
    ----------
    values: sequence
        The local collection that is replayed.
    stream_name: str
        Unique name of the source, used as its checkpoint identifier.
    poll_interval: float
        Seconds to sleep when every available value has been emitted.
    """

    def __init__(self, values=(), stream_name=None, poll_interval=0.01,
                 asynchronous=None, loop=None, **kwargs):
        self._values = list(values)
        self._poll_interval = poll_interval
        self._next_position = 0
        self._offset = 0
        self._emitted_count = 0
        self._last_emitted = -1
        self._gates = {}
        self._drained = asyncio.Event()
        self._drained.set()
        if asynchronous and loop is None:
            loop = IOLoop.current()
        super().__init__(
            start=False, stream_name=stream_name,
            asynchronous=asynchronous, loop=loop, **kwargs
        )

    @property
    def emitted_count(self):
        return self._emitted_count

    @property
    def next_position(self):
        return self._next_position

    def emitted_through(self, position):
        """True when every element up to and including ``position`` is out."""
        return self._last_emitted >= position

    def extend(self, values):
        """Append more values that may be emitted in a later stage."""
        self._values.extend(values)
        self._drained.clear()

    def barrier(self, position):
        """Block the source on ``position`` until :meth:`release` is called."""
        if position in self._gates:
            raise CheckpointError(
                "Barrier already registered for position {}".format(position)
            )
        event = asyncio.Event()
        self._gates[position] = event
        return event

    def release(self, position):
        """Release a barrier previously placed at ``position``."""
        self._gates.pop(position).set()

    def replay(self, from_position):
        """Start (or restart) emission at ``from_position``."""
        if from_position < 0:
            raise CheckpointError("Replay positions must be non-negative")
        if from_position > len(self._values):
            raise CheckpointError(
                "Replay position {} is beyond the {} available values".format(
                    from_position, len(self._values)
                )
            )
        self._offset = from_position
        self._next_position = from_position
        self._emitted_count = 0
        self._last_emitted = from_position - 1
        self._drained = asyncio.Event()
        self.start()

    async def wait_drained(self):
        """Wait until every currently available value has been emitted."""
        await self._drained.wait()

    async def run(self):
        while not self.stopped:
            if self._next_position >= len(self._values):
                self._drained.set()
                await asyncio.sleep(self._poll_interval)
                continue

            position = self._next_position
            value = self._values[position]
            self._next_position += 1

            gate = self._gates.get(position)
            if gate is not None:
                await gate.wait()
            await asyncio.gather(*self._emit(value))
            self._emitted_count += 1
            self._last_emitted = position
            if self._next_position >= len(self._values):
                self._drained.set()

    def _checkpoint_state(self):
        return {
            "offset": self._offset,
            "next_position": self._next_position,
            "emitted": self._emitted_count,
            "length": len(self._values),
        }

    def _restore_checkpoint_state(self, state):
        self._offset = state["offset"]


def _identity(x):
    return x


@Stream.register_api()
class checkpoint_transform(Stream):
    """Asynchronous, order preserving transform with controllable gates.

    Inputs are numbered per route in arrival order.  A position can be
    :meth:`gate`d so its transformation hangs until :meth:`release` is called,
    while later inputs are still accepted.  Outputs are always emitted in
    arrival order.  :meth:`hold_from` additionally freezes every position at or
    beyond a boundary, so a checkpoint cut cannot swallow inputs that belong
    after the cut; frozen inputs are released again with :meth:`release_hold`.

    Parameters
    ----------
    upstream: Stream
        The positioned source (or another upstream) of this route.
    func: callable, optional
        The transformation.  May be a normal or async callable; defaults to the
        identity function.
    """

    def __init__(self, upstream, func=None, *args, stream_name=None, **kwargs):
        self.func = func if func is not None else _identity
        self.args = args
        self.kwargs = kwargs
        self._next_position = 0
        self._emitted_count = 0
        self._inflight = deque()
        self._gates = {}
        self._hold_position = None
        self._held_inputs = deque()
        self._held_results = deque()
        self._work_queue = asyncio.Queue()
        self._worker_task = None
        Stream.__init__(
            self, upstream, stream_name=stream_name, ensure_io_loop=True
        )

    @property
    def inflight(self):
        """Positions and values accepted but not yet emitted, in order."""
        return deque(self._inflight)

    def gate(self, position):
        """Hang the transformation of ``position`` until :meth:`release`."""
        if position in self._gates:
            raise CheckpointError(
                "Gate already registered for position {}".format(position)
            )
        event = asyncio.Event()
        self._gates[position] = event
        return event

    def release(self, position):
        self._gates.pop(position).set()

    def hold_from(self, position):
        """Freeze every input at or beyond ``position`` until release.

        Already completed tasks are parked before emission and inputs that have
        only been accepted are parked before transformation, so the boundary
        can be armed while earlier positions are still in flight.
        """
        if self._hold_position is not None and self._hold_position != position:
            raise CheckpointError(
                "Hold boundary already armed at {}".format(self._hold_position)
            )
        self._hold_position = position

    def release_hold(self):
        """Release a boundary installed with :meth:`hold_from`."""
        if self._hold_position is None:
            return
        self._hold_position = None

        while self._held_results:
            position, result, metadata = self._held_results.popleft()
            front_position, _ = self._inflight[0]
            if front_position != position:  # pragma: no cover - ordering guard
                raise CheckpointError(
                    "Held result {} is out of order (front is {})".format(
                        position, front_position
                    )
                )
            self._inflight.popleft()
            self._emitted_count += 1
            self._emit(result, metadata=metadata)

        while self._held_inputs:
            position, x, metadata = self._held_inputs.popleft()
            self._launch(position, x, metadata)

    def emitted_through(self, position):
        return self._emitted_count > position

    def inflight_positions(self):
        return [position for position, _ in self._inflight]

    def inflight_values_after(self, position):
        return [value for pos, value in self._inflight if pos > position]

    def update(self, x, who=None, metadata=None):
        position = self._next_position
        self._next_position += 1
        self._inflight.append((position, x))

        if self._hold_position is not None and position >= self._hold_position:
            self._held_inputs.append((position, x, metadata))
            return []

        self._launch(position, x, metadata)
        return []

    def _launch(self, position, x, metadata):
        gate = self._gates.get(position)
        task = self.loop.asyncio_loop.create_task(
            self._transform(position, x, metadata, gate)
        )
        task.add_done_callback(self._log_task_error)
        self._work_queue.put_nowait(task)
        self._ensure_worker()

    @staticmethod
    def _log_task_error(task):
        if not task.cancelled() and task.exception() is not None:
            logger.exception(
                "checkpoint_transform task failed", exc_info=task.exception()
            )

    def _ensure_worker(self):
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = self.loop.asyncio_loop.create_task(
                self._worker()
            )

    async def _transform(self, position, x, metadata, gate):
        if gate is not None:
            await gate.wait()
        result = self.func(x, *self.args, **self.kwargs)
        if inspect.isawaitable(result):
            result = await result
        return position, result, metadata

    async def _worker(self):
        while True:
            task = await self._work_queue.get()
            position, result, metadata = await task

            if self._hold_position is not None and position >= self._hold_position:
                self._held_results.append((position, result, metadata))
                continue

            front_position, _ = self._inflight[0]
            if front_position != position:  # pragma: no cover - ordering guard
                raise CheckpointError(
                    "Out of order completion: expected position {}, got {}".format(
                        front_position, position
                    )
                )
            self._inflight.popleft()
            self._emitted_count += 1
            emitted = self._emit(result, metadata=metadata)
            if emitted:
                await asyncio.gather(*emitted)

    def _checkpoint_state(self):
        return {
            "next_position": self._next_position,
            "emitted": self._emitted_count,
            "inflight": [value for _, value in self._inflight],
        }

    def _restore_checkpoint_state(self, state):
        # Inputs after the cut are replayed from the positioned source, so the
        # transform starts empty. The manager rebases its position counters on
        # the route replay offset before the source restarts.
        self._next_position = 0
        self._emitted_count = 0
        self._inflight.clear()
        self._held_inputs.clear()
        self._held_results.clear()
        self._hold_position = None


class Checkpoint:
    """Serialised snapshot of a positioned, named pipeline.

    Attributes
    ----------
    positions: dict
        Mapping ``source name -> cut position``.  The element at the cut
        position is included in the snapshot; replay resumes at ``cut + 1``.
    nodes: dict
        Mapping ``node name -> node state`` produced by each node's
        ``_checkpoint_state`` hook.
    pending: dict
        Mapping ``source name -> values`` that had already been received (e.g.
        queued in a transform) but lie after the cut and therefore must be
        replayed.
    topology: list
        Structural signature of the graph, used to detect rebuilt pipelines
        whose connections changed.
    """

    def __init__(self, positions=None, nodes=None, pending=None, topology=None):
        self.positions = dict(positions or {})
        self.nodes = dict(nodes or {})
        self.pending = {name: list(values) for name, values in (pending or {}).items()}
        self.topology = topology or []

    def to_dict(self):
        return {
            "positions": dict(self.positions),
            "nodes": dict(self.nodes),
            "pending": {name: list(values) for name, values in self.pending.items()},
            "topology": list(self.topology),
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            positions=data.get("positions"),
            nodes=data.get("nodes"),
            pending=data.get("pending"),
            topology=data.get("topology"),
        )

    def save(self, path):
        with open(path, "wb") as handle:
            pickle.dump(self.to_dict(), handle)

    @classmethod
    def load(cls, path):
        with open(path, "rb") as handle:
            return cls.from_dict(pickle.load(handle))


class CheckpointManager:
    """Binds positioned sources to the named nodes reachable downstream.

    Parameters
    ----------
    sources: dict
        Mapping ``source name -> PositionedSource``.
    nodes: dict
        Mapping ``node name -> Stream`` for every named reachable node.
    transforms: dict
        Mapping ``source name -> [checkpoint_transform, ...]`` holding the
        transforms that exclusively belong to that route.
    topology: list
        Structural signature of the reachable graph.
    """

    def __init__(self, sources, nodes, transforms, topology):
        self.sources = dict(sources)
        self.nodes = dict(nodes)
        self.transforms = dict(transforms)
        self.topology = topology

    @classmethod
    def from_sources(cls, sources):
        if not isinstance(sources, dict):
            raise CheckpointError(
                "sources must be a mapping of name -> PositionedSource"
            )
        for name, source in sources.items():
            if not isinstance(source, PositionedSource):
                raise CheckpointError(
                    "Source {!r} must be a PositionedSource".format(name)
                )
            if source.name != name:
                raise CheckpointError(
                    "Source is named {!r} but registered as {!r}".format(
                        source.name, name
                    )
                )

        ordered_sources = [sources[name] for name in sorted(sources)]
        ordered_nodes = _walk_downstream(ordered_sources)

        named_nodes = {}
        for node in ordered_nodes:
            name = getattr(node, "name", None)
            if name is None:
                continue
            if name in named_nodes and named_nodes[name] is not node:
                raise CheckpointError(
                    "Duplicate node name {!r}; checkpoint names must be unique".format(
                        name
                    )
                )
            named_nodes[name] = node

        source_reach = {
            name: {id(node) for node in _walk_downstream([source])}
            for name, source in sources.items()
        }
        transforms = {name: [] for name in sources}
        for node in ordered_nodes:
            if not isinstance(node, checkpoint_transform):
                continue
            owners = [
                name for name, reachable in source_reach.items()
                if id(node) in reachable
            ]
            if len(owners) != 1:
                raise CheckpointError(
                    "Transform {!r} is reachable from multiple sources {} and "
                    "cannot be assigned a route".format(node.name, owners)
                )
            transforms[owners[0]].append(node)

        topology = cls._topology_signature(ordered_nodes)
        return cls(sources, named_nodes, transforms, topology)

    @staticmethod
    def _node_keys(ordered_nodes):
        keys = {}
        anon_counts = {}
        for node in ordered_nodes:
            name = getattr(node, "name", None)
            if name is not None:
                keys[id(node)] = name
            else:
                class_name = type(node).__qualname__
                index = anon_counts.get(class_name, 0)
                anon_counts[class_name] = index + 1
                keys[id(node)] = "anon:{}#{}".format(class_name, index)
        return keys

    @classmethod
    def _topology_signature(cls, ordered_nodes):
        keys = cls._node_keys(ordered_nodes)
        signature = []
        for node in ordered_nodes:
            upstream_keys = sorted(keys[id(upstream)] for upstream in node.upstreams)
            signature.append((
                keys[id(node)],
                _qualified_name(type(node)),
                tuple(upstream_keys),
            ))
        return sorted(signature, key=lambda item: item[0])

    async def save_checkpoint(self, positions, path=None, timeout=5,
                              release_held=True):
        """Take a consistent snapshot at the per-source cut positions.

        Each route freezes every position strictly after its cut, waits until
        the cut position (including any gated in-flight input) has fully
        traversed the route transforms, and only then snapshots node state.
        The coroutine therefore cannot complete past an earlier in-flight input
        and cannot swallow inputs received after the cut.

        Parameters
        ----------
        positions: dict
            Mapping ``source name -> cut position``.
        path: str, optional
            When given, the checkpoint is pickled to this path.
        timeout: float
            Seconds to wait for each route to reach its cut.
        release_held: bool
            Release the freeze boundaries again after the snapshot so the live
            pipeline continues.  Pass ``False`` to model process teardown.
        """
        positions = dict(positions)
        missing = set(self.sources).difference(positions)
        if missing:
            raise CheckpointError(
                "Missing cut positions for sources: {}".format(sorted(missing))
            )
        unknown = set(positions).difference(self.sources)
        if unknown:
            raise CheckpointError(
                "Unknown source positions: {}".format(sorted(unknown))
            )

        for name, cutoff in positions.items():
            for transform in self.transforms[name]:
                transform.hold_from(cutoff + 1)

        try:
            for name, cutoff in positions.items():
                source = self.sources[name]
                await _wait_until(
                    lambda source=source, cutoff=cutoff:
                        source.emitted_through(cutoff),
                    timeout,
                    "Source {!r} did not reach cut position {}".format(name, cutoff),
                )
                for transform in self.transforms[name]:
                    await _wait_until(
                        lambda transform=transform, cutoff=cutoff:
                            transform.emitted_through(cutoff),
                        timeout,
                        "Transform {!r} did not process cut position {}".format(
                            transform.name, cutoff
                        ),
                    )

            node_states = {
                name: node._checkpoint_state()
                for name, node in self.nodes.items()
            }
            pending = {}
            for name, cutoff in positions.items():
                values = []
                for transform in self.transforms[name]:
                    values.extend(transform.inflight_values_after(cutoff))
                pending[name] = values

            checkpoint = Checkpoint(
                positions=positions,
                nodes=node_states,
                pending=pending,
                topology=self.topology,
            )
            if path is not None:
                checkpoint.save(path)
        finally:
            if release_held:
                for name in positions:
                    for transform in self.transforms[name]:
                        transform.release_hold()

        return checkpoint

    @classmethod
    def restore(cls, checkpoint, sources):
        """Restore ``checkpoint`` into a freshly built, equivalent pipeline.

        The whole graph is validated first (names, operator classes and
        connections); no node state is mutated and no source is started until
        every check has passed.
        """
        if isinstance(checkpoint, str):
            checkpoint = Checkpoint.load(checkpoint)

        manager = cls.from_sources(sources)

        cls._validate_topology(checkpoint, manager)
        cls._validate_nodes(checkpoint, manager)
        cls._validate_sources(checkpoint, manager)

        for name, state in checkpoint.nodes.items():
            manager.nodes[name]._restore_checkpoint_state(state)

        # Rebase each route's transforms onto the same position space as the
        # source that replays into them: every position below the offset is
        # already considered processed.
        for name, source in manager.sources.items():
            replay_from = checkpoint.positions[name] + 1
            for transform in manager.transforms[name]:
                transform._next_position = replay_from
                transform._emitted_count = replay_from
            source.replay(replay_from)

        return manager

    @classmethod
    def _validate_topology(cls, checkpoint, manager):
        if checkpoint.topology != manager.topology:
            raise CheckpointError(
                "Checkpoint topology does not match the rebuilt pipeline. "
                "Saved signature: {}; current signature: {}".format(
                    checkpoint.topology, manager.topology
                )
            )

    @classmethod
    def _validate_nodes(cls, checkpoint, manager):
        saved_names = set(checkpoint.nodes)
        current_names = set(manager.nodes)
        if saved_names != current_names:
            raise CheckpointError(
                "Named nodes differ. Saved: {}; rebuilt: {}".format(
                    sorted(saved_names), sorted(current_names)
                )
            )
        for name in saved_names:
            node = manager.nodes[name]
            expected_class = _resolve_class(cls._class_name_for(checkpoint, name))
            if not isinstance(node, expected_class):
                raise CheckpointError(
                    "Named operator {!r} was replaced: checkpoint expects {} "
                    "but the rebuilt pipeline contains {}".format(
                        name,
                        expected_class.__qualname__,
                        type(node).__qualname__,
                    )
                )

    @staticmethod
    def _class_name_for(checkpoint, node_name):
        for name, class_name, _ in checkpoint.topology:
            if name == node_name:
                return class_name
        raise CheckpointError(
            "Node {!r} is missing from the checkpoint topology".format(node_name)
        )

    @classmethod
    def _validate_sources(cls, checkpoint, manager):
        if set(checkpoint.positions) != set(manager.sources):
            raise CheckpointError(
                "Positioned sources differ. Saved: {}; rebuilt: {}".format(
                    sorted(checkpoint.positions), sorted(manager.sources)
                )
            )
        for name, source in manager.sources.items():
            cutoff = checkpoint.positions[name]
            if cutoff >= len(source._values):
                raise CheckpointError(
                    "Source {!r} cannot resume after position {} with only {} "
                    "values".format(name, cutoff, len(source._values))
                )
