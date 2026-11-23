# Parent-Child Streams

The Tap SDK supports parent-child streams, by which one stream type can be declared to
be a parent to another stream, and the child stream will automatically receive `context`
from a parent record each time the child stream is invoked.

## If you do want to utilize parent-child streams

1. Set [`parent_stream_type`](singer_sdk.Stream.parent_stream_type) in the child-stream's class to the class of the parent.
1. Implement one of the below methods to pass context from the parent to the child:
   1. Override [`get_child_context`](singer_sdk.Stream.get_child_context) to return a new
      child context object based on records and any existing context from the parent stream.
   1. If you need to sync more than one child stream per parent record, you can override
      [`generate_child_contexts`](singer_sdk.Stream.generate_child_contexts) to yield as many
      contexts as you need.
1. If the parent stream's replication key won't get updated when child items are changed,
   indicate this by adding [`ignore_parent_replication_key = True`](singer_sdk.Stream.ignore_parent_replication_key)
   in the child stream class declaration.
1. If the number of _parent_ items is very large (thousands or tens of thousands), you can
   optionally set [`state_partitioning_keys`](singer_sdk.Stream.state_partitioning_keys) on the child stream to specify a subset of context keys to use
   in state bookmarks. (When not set, the number of bookmarks will be equal to the number
   of parent items.) If you do not wish to store any state bookmarks for the child stream, set [`state_partitioning_keys`](singer_sdk.Stream.state_partitioning_keys) to `[]`.

## Example parent-child implementation

Here is an abbreviated example from the Gitlab sample (also in this repo) which uses the
above techniques.
In this example, EpicIssuesStream is a child of EpicsStream.

```py
class GitlabStream(RESTStream):
    # Base stream definition with auth and pagination logic
    # This logic works for other base classes as well, including Stream, GraphQLStream, etc.


class EpicsStream(GitlabStream):

    name = "epics"

    # ...

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "group_id": record["group_id"],
            "epic_id": record["id"],
            "epic_iid": record["iid"],
        }


class EpicIssuesStream(GitlabStream):
    # Note that this class inherits from the GitlabStream base class, and not from
    # the EpicsStream class.

    name = "epic_issues"

    # EpicIssues streams should be invoked once per parent epic:
    parent_stream_type = EpicsStream

    # Assume epics don't have `updated_at` incremented when issues are changed:
    ignore_parent_replication_key = True

    # Path is auto-populated using parent context keys:
    path = "/groups/{group_id}/epics/{epic_iid}/issues"

    # ...
```

### How Path Parameter Replacement Works

In the example above, the `path` contains placeholders like `{group_id}` and `{epic_iid}`. These are **not** instance properties—they're context variables that get automatically replaced.

The SDK's [`get_url()`](singer_sdk.RESTStream.get_url) method extracts these values from each context dictionary yielded by the parent's [`generate_child_contexts()`](singer_sdk.Stream.generate_child_contexts). For each parent record, the child stream receives one or more context dictionaries like:

```python
{
    "group_id": 123,
    "epic_id": 456,
    "epic_iid": 789,
}
```

The SDK then substitutes these into the path, producing URLs like:

```
/groups/123/epics/789/issues
```

```{note}
All the keys in the `context` dictionary are added to the child's record, but they will be automatically removed if they are not present in the child's schema. If you wish to preserve these keys in the child's record, you must add them to the child's schema.
```

## Additional Parent-Child References

- [Singer State in SDK Taps](./implementation/state.md)

## Resuming interrupted child stream contexts

A child stream context is normally re-synced whenever its parent stream yields the
corresponding parent record again. However, when the parent stream is incremental, its
bookmark may move past a parent record whose child context was only partially synced -
for instance, if the run was interrupted while the child stream was still syncing that
context. Because the parent stream will not yield that record again, the child stream
would never finish syncing the context.

To prevent this, the SDK tracks each child stream context as _unfinished_ when its sync
starts, and removes the marker once the context has been fully synced. Unfinished
contexts are stored in the child stream's state under an `unfinished_contexts` key,
keyed by their [state partition context](./implementation/state.md#partitioned-state):

```json
{
  "bookmarks": {
    "bills": {
      "partitions": [
        {
          "context": { "merchant_id": 7 },
          "replication_key": "updated_at",
          "replication_key_value": "2026-01-01T01:00:00Z"
        }
      ],
      "unfinished_contexts": [{ "merchant_id": 7 }]
    }
  }
}
```

At the end of each parent stream sync, any unfinished child contexts found in state are
synced again, resuming from the context's own bookmark. Contexts that were fully synced
are not fetched again, and newly appearing parent records are still synced as usual in
the same run.

Because unfinished contexts are keyed by their state partition context, you should set
[`state_partitioning_keys`](singer_sdk.Stream.state_partitioning_keys) on the child
stream to the _stable_ identifiers of the parent record (e.g. `["merchant_id"]`). This
ensures the child context is still matched to its bookmark when mutable values, such as
the parent record's display name, change between runs.

Existing state files that only contain per-context `partitions` bookmarks keep working
unchanged: the child stream resumes each context from its stored bookmark, and no
unfinished contexts are resumed until an interruption is recorded by a run.
