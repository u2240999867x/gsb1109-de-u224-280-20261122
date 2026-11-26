Added
-----

*   Add review sessions that keep sorted-list hits stable across edits.

    ``jsonpath_ng.ext.review`` introduces ``ReviewSession`` and ``ReviewTicket``.
    A session gives every occurrence in a JSON document a stable identity, so a
    hit captured through an existing expression (including the ``[/field]``
    sort extension) can be replaced or deleted later even when rows are
    removed, reordered, or changed in the meantime. Each hit still reports the
    rank and document position it had when captured, and ``refresh()`` reports
    whether the same occurrence still exists. A ticket whose hit went stale is
    rejected as a whole instead of writing to the wrong row. Plain queries and
    direct edits keep their existing behavior.
