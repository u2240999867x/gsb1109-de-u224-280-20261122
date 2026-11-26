Added
-----

*   Add opt-in object graph queries and corrections.

    ``GraphQuery`` and ``find_graph()`` traverse Python object graphs rather
    than plain JSON trees: every path to a value is reported, and a reference
    that loops back to a container on the current walk is reported as a cycle
    with its link location and the path it returns to, without being expanded.
    ``correct_graph()`` produces corrected copies of selected mounts, keeps
    each mount's corrections independent, preserves unchanged shared objects,
    and rejects a whole commit when paths within one mount request conflicting
    values. Standard ``.find()`` tree semantics and ordering are unchanged.
