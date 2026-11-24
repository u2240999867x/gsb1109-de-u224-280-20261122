Checkpointing and Replay
========================

Streamz can snapshot a small local pipeline and rebuild it later (even in
another process), continuing from explicit per-source positions. This is useful
for two local collection files that are replayed in order, transformed
separately, reconciled with ``zip`` and reduced with ``accumulate``.

The feature is opt-in: streams built without the checkpoint helpers keep their
original behaviour, because the base ``Stream`` state hooks are no-ops.

Building a checkpointable pipeline
----------------------------------

Every route starts at a :class:`~streamz.PositionedSource`, which numbers its
elements from zero and can replay them from any position. Per-route work goes
through :class:`~streamz.checkpoint_transform`, an asynchronous, order
preserving map whose individual positions can be gated. The reconciliation and
running state are ordinary named ``zip`` and ``accumulate`` nodes.

.. code-block:: python

   from streamz import Stream
   from streamz import PositionedSource, checkpoint_transform, CheckpointManager

   left = PositionedSource(["L1", "L2"], stream_name="left", asynchronous=True)
   right = PositionedSource(["R1", "R2", "R3"], stream_name="right",
                            asynchronous=True)
   left_t = checkpoint_transform(left, lambda x: x + "*", stream_name="left-t")
   right_t = checkpoint_transform(right, lambda x: x + "*", stream_name="right-t")
   zipped = Stream.zip(left_t, right_t, stream_name="zip")
   total = zipped.accumulate(
       lambda state, pair: state + pair[0] - pair[1],
       start=0, stream_name="acc",
   )

   manager = CheckpointManager.from_sources({"left": left, "right": right})

Every node that owns state must have a unique ``stream_name``. The manager
discovers those names, the node classes and the connections, and stores the
resulting topology signature inside each checkpoint.

Taking a checkpoint
-------------------

A checkpoint is requested with one cut position per source. The element at the
cut position is included in the snapshot, and replay later resumes at
``cut + 1``.

.. code-block:: python

   checkpoint = await manager.save_checkpoint({"left": 0, "right": 1})
   checkpoint.save("pipeline.checkpoint")

While the snapshot is being taken, each route freezes positions strictly after
its cut, then waits until the cut position has fully traversed the route
transforms. A cut therefore cannot overtake an earlier in-flight input of the
same route, and inputs already received but lying after the cut are parked in
the checkpoint as ``pending`` inputs instead of being swallowed.

Gates model slow or hanging transformations deterministically:

.. code-block:: python

   left_t.gate(0)          # position 0 (L1) hangs inside the transform
   # ... L2 can still be accepted behind it ...
   save = asyncio.create_task(
       manager.save_checkpoint({"left": 0, "right": 1})
   )
   # save does not complete while L1 is still in flight
   left_t.release(0)
   checkpoint = await save

The checkpoint contains:

* the per-source cut positions,
* the state of every named node, including unpaired ``zip`` buffers and the
  confirmed ``accumulate`` state,
* the accepted-but-after-cut pending inputs of each route,
* the topology signature used to validate rebuilt pipelines.

Restoring in a new process
--------------------------

Rebuild the same graph with the same node names and connections, then restore
the checkpoint before starting the sources:

.. code-block:: python

   from streamz import Checkpoint

   checkpoint = Checkpoint.load("pipeline.checkpoint")
   manager = CheckpointManager.restore(
       checkpoint, {"left": new_left, "right": new_right}
   )

The whole graph is validated before any state is touched or any source starts.
A missing or renamed node, a replaced named operator (a different node class
under the same name), or changed connections raise
:class:`~streamz.CheckpointError`. After validation, node state is restored and
each positioned source replays from its own ``cut + 1`` position. Already
confirmed outputs are not emitted again; inputs that were unpaired at snapshot
time stay buffered and pair with the replayed inputs in their original order.
