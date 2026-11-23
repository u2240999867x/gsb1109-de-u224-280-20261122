---
title: Tree Subscription
---

# 🛰️ Subscription

Incremental path subscriptions over wildcard rules.

A subscription resolves wildcard paths against a tree, remembers the page identities each rule produced, and
reports the pages that are genuinely new or genuinely gone. Results are matched by a stable page identity
(for example, `page_id`) instead of by path or node name, so moving a directory does not look like an exit plus an
entry.

- Rules are anchored at a node so they keep working when the anchor itself is moved.
- Rules can exclude subtrees (for example, a whole province-level directory).
- Batches of directory changes are committed atomically; intermediate paths never trigger events.
- Rules unaffected by a batch carry their previous result forward.

```python
from bigtree import Node, PathSubscription

root = Node("catalog")
regions = Node("regions", parent=root)
north = Node("north", parent=regions)
east = Node("east", parent=regions)
outside = Node("outside", parent=root)

for region, page_ids in (
    (north, ("N-1", "N-2")),
    (east, ("E-1", "E-2")),
    (outside, ("O-1", "O-2")),
):
    pages = Node("pages", parent=region)
    for page_id in page_ids:
        Node(f"page-{page_id}", parent=pages, page_id=page_id)

subscription = PathSubscription(root)
subscription.subscribe("siblings", "../*/pages/*", anchor=north)
subscription.snapshot()["siblings"].current
# ('N-1', 'N-2', 'E-1', 'E-2', 'O-1', 'O-2')

# Move the anchor to another layer and migrate `outside` in as its new sibling, as one batch.
archive = Node("archive", parent=root)
with subscription.batch() as batch:
    batch.mark_affected("siblings")
    north.parent = archive
    outside.parent = archive

change = batch.result["siblings"]
change.added
# ('O-1', 'O-2')
change.exited
# ('E-1', 'E-2')
```

Use `PathSubscription.commit()` for a single snapshot, or `PathSubscription.batch()` for a batch of directory
changes. Each result exposes `added`, `exited`, `current`, and `retained` per rule.

-----

::: bigtree.tree.subscription
