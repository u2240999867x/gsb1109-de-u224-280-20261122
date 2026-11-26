.. module:: jsonschema.batch

Streaming Batch Validation
==========================

Sometimes a collection of JSON records arrives as a *single-pass* stream
-- for example records decoded one at a time by a collector -- while a
contract describes the whole batch as one JSON array. Materializing the
entire batch as a list just to validate it may be undesirable or
impossible.

The `jsonschema.batch` module validates such a stream directly: each
record is consumed once, and the acceptance or rejection verdict matches
what validating the same records as one ordinary list would produce.

.. code-block:: python

    from jsonschema import Draft202012Validator

    schema = {
        "prefixItems": [{"const": "header"}],
        "contains": {"type": "object"},
        "minContains": 0,
        "unevaluatedItems": False,
        "uniqueItems": True,
    }
    validator = Draft202012Validator(schema)

    records = iter(["header", {"a": 1}, {"a": 2}])  # any single-pass iterable
    if validator.is_valid_batch(records):
        print("batch accepted")

The same functionality is available as `iter_errors` and `is_valid` in
`jsonschema.batch`, or as the ``iter_batch_errors`` and
``is_valid_batch`` methods on any validator instance.

Supported keywords
------------------

At the batch (array) level, the following keywords are evaluated
incrementally: ``prefixItems``, ``items``, ``contains`` (with
``minContains`` / ``maxContains``), ``minItems`` / ``maxItems``,
``allOf``, ``anyOf``, ``oneOf``, ``unevaluatedItems: false``,
``uniqueItems`` and ``type``. Subschemas applied to individual records
are validated with the full semantics of the validator in use. Any other
array-applicable keyword at the batch level raises
`UnsupportedBatchKeyword`.

Errors about individual records carry the record's zero-based index in
``error.path``. Errors for ``unevaluatedItems`` and ``uniqueItems``
report the original zero-based indices of the uncovered or duplicated
records.

Memory usage
------------

Working memory is bounded independently of the batch size. Coverage of
array indices is tracked with bit sets, and ``uniqueItems`` bookkeeping
is spilled to hash-sharded files under a local staging directory, which
the caller may provide via the ``spool_dir`` argument (a temporary
subdirectory is created inside it and removed afterwards). As a design
point, a batch of one million records of at most 1 KiB each is validated
within a 64 MiB working memory budget, measured from the first record
read and excluding the interpreter, the schema, and the record currently
held by the caller.

Incomplete input
----------------

If the record stream raises an exception partway through, an error
reporting that the batch was not fully read is produced, and no final
verdicts (for example for ``unevaluatedItems`` or ``uniqueItems``) are
emitted -- a truncated prefix is never mistaken for a complete batch.
