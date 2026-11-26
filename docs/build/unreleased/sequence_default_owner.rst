.. change::
    :tags: bug, autogenerate, postgresql

    Fixed autogenerate's handling of PostgreSQL sequence-backed column
    defaults.  A column whose default takes its values from a sequence that
    is owned by a *different* column, such as a primary key that still
    points at another schema's sequence, was previously treated as an
    ordinary ``SERIAL`` column and its default was silently omitted from
    comparison, so that the change was never detected.  The ``SERIAL``
    ownership check now verifies the owning table and schema in addition to
    the column name, and sequence defaults are compared by the identity of
    the sequence recorded in the PostgreSQL catalog rather than by the
    rendered ``nextval()`` text, which is dependent on ``search_path``.
    Reflected defaults are now schema-qualified according to the sequence
    they actually reference, so that a migration generated under one
    ``search_path`` connects to the same sequence when run under another.
    The ``nextval()`` expression itself is never evaluated during
    comparison.
