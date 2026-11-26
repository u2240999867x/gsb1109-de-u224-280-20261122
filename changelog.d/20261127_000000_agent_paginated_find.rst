Added
-----

*   Add resumable, paginated JSONPath queries via ``PagedFind``.

    ``PagedFind`` returns matches a page at a time, in the same order and with
    the same multiplicity as a complete ``find()`` call, and each match keeps
    its real parent context. It supports ordinary fields, array selection
    (indices, slices, and wildcards), and descendant traversal. A JSON
    receipt can be persisted and reopened in another process together with the
    original document and the same expression to continue the query without
    re-traversing already finished subtrees. Reopening a changed document or
    using a different expression raises a specific error.
