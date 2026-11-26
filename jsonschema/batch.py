"""
Streaming validation of record batches against array-shaped schemas.

The functions here validate a *single-pass* iterable of decoded JSON
records as if the records were the elements of one JSON array, without
ever holding the whole batch in memory. The acceptance / rejection
verdict matches what validating the same records as one ordinary list
against the same schema would produce.

Only a subset of array-level keywords is supported at the batch level:
``prefixItems``, ``items``, ``contains`` (with ``minContains`` /
``maxContains``), ``minItems`` / ``maxItems``, ``allOf``, ``anyOf``,
``oneOf``, ``unevaluatedItems: false``, ``uniqueItems`` and ``type``.
Subschemas applied to individual records (e.g. under ``prefixItems``,
``items`` or ``contains``) are validated with the full semantics of the
validator the batch is run under. Any other array-applicable keyword at
the batch level raises `UnsupportedBatchKeyword`.

Working memory is bounded independently of the batch size: coverage of
array indices is tracked with bit sets, and ``uniqueItems`` bookkeeping
is spilled to hash-sharded files under a local staging directory (see
the ``spool_dir`` argument). As a design point, a batch of one million
records of at most 1 KiB each is validated within a 64 MiB working
memory budget, measured from the first record read and excluding the
interpreter, the schema, and the record currently held by the caller.

If the record stream raises partway through, an error reporting the
incomplete input is produced and no final verdicts (e.g. for
``unevaluatedItems`` or ``uniqueItems``) are emitted, so a truncated
prefix is never mistaken for a complete batch.
"""

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING
import json
import tempfile

from jsonschema._utils import _UNSUPPORTED, _uniq_key, equal
from jsonschema.exceptions import ValidationError

if TYPE_CHECKING:
    import os

__all__ = ["UnsupportedBatchKeyword", "is_valid", "iter_errors"]


class UnsupportedBatchKeyword(ValueError):
    """
    The batch schema uses a keyword that streaming validation cannot evaluate.
    """


#: Number of errors kept per ``anyOf`` / ``oneOf`` branch for reporting
#: as context. Verdicts are unaffected by the cap; it only bounds memory.
_CONTEXT_ERROR_LIMIT = 100

#: Number of hash shards used to spill ``uniqueItems`` bookkeeping.
_UNIQUENESS_SHARDS = 64

#: Keywords which would change the verdict for an array instance but
#: cannot be evaluated on a single-pass stream.
_UNSUPPORTED_BATCH_KEYWORDS = frozenset(
    {
        "$ref",
        "$dynamicRef",
        "const",
        "else",
        "enum",
        "if",
        "not",
        "then",
    },
)


def iter_errors(
    validator,
    records: Iterable,
    *,
    spool_dir: "str | os.PathLike | None" = None,
) -> Iterator[ValidationError]:
    """
    Lazily yield validation errors for a batch of records.

    Errors for individual records carry their zero-based index in
    ``error.path``. Errors about uncovered items (``unevaluatedItems``)
    and duplicated items (``uniqueItems``) report the original
    zero-based indices of the offending records.

    Arguments:

        validator:

            a validator instance (e.g. ``Draft202012Validator(schema)``)
            whose schema describes the batch as one JSON array

        records:

            a single-pass iterable of decoded JSON records, one per
            element of the logical array

        spool_dir:

            a local directory used to spill ``uniqueItems`` bookkeeping
            to disk. A temporary subdirectory is created inside it (and
            removed again afterwards). If not provided, the system
            temporary directory is used.

    """
    run = _BatchRun(validator, spool_dir=spool_dir)
    return run.execute(records)


def is_valid(
    validator,
    records: Iterable,
    *,
    spool_dir: "str | os.PathLike | None" = None,
) -> bool:
    """
    Check whether a batch of records is valid under the given schema.

    Consumes the record stream fully; see `iter_errors` for details.
    """
    error = next(iter_errors(validator, records, spool_dir=spool_dir), None)
    return error is None


class _Node:
    """
    One schema object of the batch schema, mirrored as a streaming evaluator.
    """

    def __init__(self, schema, segments):
        self.schema = schema
        self.segments = segments

        self.const_valid = None  # set for boolean schemas
        self.prefix_items = ()
        self.has_items = False
        self.items = None
        self.contains = None
        self.contains_validator = None
        self.min_contains = 1
        self.max_contains = None
        self.min_items = None
        self.max_items = None
        self.unique = False
        self.unevaluated_false = False
        self.type_failed = None
        self.all_of = []
        self.any_of = []
        self.one_of = []

        # Runtime state.
        self.failed = False  # an own error was produced while feeding
        self.context = []  # buffered errors when under anyOf / oneOf
        self.context_overflow = 0
        self.contains_count = 0
        self.contains_mask = bytearray()
        self.contains_max_reported = False
        self.items_false_reported = False
        self.dedup = None
        self.valid = False
        self.coverage = 0

    def add_context(self, error):
        self.failed = True
        if len(self.context) < _CONTEXT_ERROR_LIMIT:
            self.context.append(error)
        else:
            self.context_overflow += 1


def _build(schema, validator, segments, unique_nodes):
    node = _Node(schema, segments)

    if schema is True or schema is False:
        node.const_valid = schema
        return node

    unsupported = _UNSUPPORTED_BATCH_KEYWORDS.intersection(schema)
    if unsupported:
        names = ", ".join(sorted(unsupported))
        raise UnsupportedBatchKeyword(
            f"cannot validate a single-pass record stream against "
            f"schemas containing {names}",
        )

    unevaluated = schema.get("unevaluatedItems")
    if unevaluated is not None and unevaluated is not False:
        raise UnsupportedBatchKeyword(
            "only `unevaluatedItems: false` is supported "
            "for single-pass batch validation",
        )
    node.unevaluated_false = unevaluated is False

    node.prefix_items = schema.get("prefixItems", ())

    if "items" in schema:
        items = schema["items"]
        if not isinstance(items, (bool, dict)):
            raise UnsupportedBatchKeyword(
                "only schema-valued `items` is supported "
                "for single-pass batch validation",
            )
        node.has_items = True
        node.items = items

    if "contains" in schema:
        node.contains = schema["contains"]
        node.contains_validator = validator.evolve(schema=node.contains)
        node.min_contains = schema.get("minContains", 1)
        node.max_contains = schema.get("maxContains")

    node.min_items = schema.get("minItems")
    node.max_items = schema.get("maxItems")

    node.unique = bool(schema.get("uniqueItems"))
    if node.unique:
        unique_nodes.append(node)

    if "type" in schema:
        declared = schema["type"]
        types = [declared] if isinstance(declared, str) else list(declared)
        if not any(validator.is_type([], each) for each in types):
            node.type_failed = declared

    for keyword, attribute in [
        ("allOf", "all_of"),
        ("anyOf", "any_of"),
        ("oneOf", "one_of"),
    ]:
        children = [
            _build(each, validator, [*segments, keyword, index], unique_nodes)
            for index, each in enumerate(schema.get(keyword, ()))
        ]
        setattr(node, attribute, children)

    return node


def _prepend_schema_path(error, segments):
    for segment in reversed(segments):
        error.schema_path.appendleft(segment)
    return error


def _set_bit(mask: bytearray, index: int):
    byte = index >> 3
    if byte >= len(mask):
        mask.extend(b"\x00" * (byte + 1 - len(mask)))
    mask[byte] |= 1 << (index & 7)


class _Deduper:
    """
    Spill-based uniqueness bookkeeping for one ``uniqueItems`` keyword.

    Each record is reduced to a structural digest compatible with
    `jsonschema._utils.equal` and appended, together with its index and
    a canonical serialization, to one of a fixed number of hash-sharded
    spill files. At the end of the stream each shard fits comfortably in
    memory and is checked for duplicate groups independently.
    """

    def __init__(self, directory):
        self._handles = [
            # the handles stay open until `close`, after the stream ends
            (directory / f"unique-items-{shard}.jsonl").open("w+b")
            for shard in range(_UNIQUENESS_SHARDS)
        ]

    def add(self, index, record):
        key = _uniq_key(record)
        digest = -1 if key is _UNSUPPORTED else hash(key)
        data = json.dumps(record, sort_keys=True, separators=(",", ":"))
        line = f"{digest}\t{index}\t{data}\n".encode("ascii")
        self._handles[digest % _UNIQUENESS_SHARDS].write(line)

    def duplicate_groups(self):
        """
        Return the sorted index lists of all groups of duplicate records.
        """
        groups = []
        for handle in self._handles:
            handle.flush()
            handle.seek(0)
            by_digest = {}
            for line in handle:
                digest, index, data = line.split(b"\t", 2)
                by_digest.setdefault(int(digest), []).append(
                    (int(index), data),
                )
            for entries in by_digest.values():
                if len(entries) == 1:
                    continue
                clusters = []
                for index, data in entries:
                    candidate = json.loads(data)
                    for representative, indices in clusters:
                        if equal(representative, candidate):
                            indices.append(index)
                            break
                    else:
                        clusters.append((candidate, [index]))
                for _, indices in clusters:
                    if len(indices) > 1:
                        groups.append(sorted(indices))
        return groups

    def close(self):
        for handle in self._handles:
            handle.close()


class _BatchRun:
    def __init__(self, validator, spool_dir):
        self.validator = validator
        self.spool_dir = spool_dir
        self._unique_nodes = []
        self.root = _build(validator.schema, validator, [], self._unique_nodes)
        self._tempdir = None

    def execute(self, records) -> Iterator[ValidationError]:
        try:
            if self.root.const_valid is not None:
                yield from self._execute_const(records)
                return

            self._open_spool()

            index = 0
            iterator = iter(records)
            while True:
                try:
                    record = next(iterator)
                except StopIteration:
                    break
                except Exception as error:  # noqa: BLE001 -- any stream failure
                    yield _incomplete_input_error(error)
                    return
                yield from self._feed_node(self.root, index, record)
                index += 1

            yield from self._finalize_node(self.root, index)
        finally:
            self._close_spool()

    def _execute_const(self, records) -> Iterator[ValidationError]:
        if not self.root.const_valid:
            yield ValidationError("False schema does not allow the batch")
            return
        try:
            for _ in records:
                pass
        except Exception as error:  # noqa: BLE001 -- any stream failure
            yield _incomplete_input_error(error)

    def _open_spool(self):
        if not self._unique_nodes:
            return
        if self.spool_dir is not None:
            Path(self.spool_dir).mkdir(parents=True, exist_ok=True)
        self._tempdir = tempfile.TemporaryDirectory(
            prefix="jsonschema-batch-",
            dir=self.spool_dir,
        )
        for node in self._unique_nodes:
            node.dedup = _Deduper(Path(self._tempdir.name))

    def _close_spool(self):
        for node in self._unique_nodes:
            if node.dedup is not None:
                node.dedup.close()
        if self._tempdir is not None:
            self._tempdir.cleanup()

    def _feed_node(self, node, index, record) -> Iterator[ValidationError]:
        prefix = len(node.prefix_items)
        if index < prefix:
            errors = self.validator.descend(
                record,
                node.prefix_items[index],
                path=index,
                schema_path=index,
            )
            for error in errors:
                node.failed = True
                yield _prepend_schema_path(
                    error,
                    [*node.segments, "prefixItems"],
                )
        elif node.has_items:
            if node.items is False:
                if not node.items_false_reported:
                    node.items_false_reported = True
                    node.failed = True
                    yield ValidationError(
                        f"Expected at most {prefix} items but found "
                        f"an extra item at index {index}",
                        validator="items",
                        validator_value=False,
                        schema_path=[*node.segments, "items"],
                    )
            else:
                errors = self.validator.descend(record, node.items, path=index)
                for error in errors:
                    node.failed = True
                    yield _prepend_schema_path(
                        error,
                        [*node.segments, "items"],
                    )

        if node.contains is not None and node.contains_validator.is_valid(
            record,
        ):
            node.contains_count += 1
            _set_bit(node.contains_mask, index)
            if (
                node.max_contains is not None
                and node.contains_count > node.max_contains
                and not node.contains_max_reported
            ):
                node.contains_max_reported = True
                node.failed = True
                yield ValidationError(
                    "Too many items match the given schema "
                    f"(expected at most {node.max_contains})",
                    validator="maxContains",
                    validator_value=node.max_contains,
                    schema_path=[*node.segments, "contains"],
                )

        if node.unique:
            node.dedup.add(index, record)

        for child in node.all_of:
            yield from self._feed_node(child, index, record)
        for child in (*node.any_of, *node.one_of):
            for error in self._feed_node(child, index, record):
                child.add_context(error)

    def _finalize_node(self, node, count) -> Iterator[ValidationError]:
        if node.const_valid is not None:
            node.valid = node.const_valid
            node.coverage = 0
            if not node.valid:
                yield ValidationError(
                    "False schema does not allow the batch",
                    schema_path=node.segments,
                )
            return

        for child in node.all_of:
            yield from self._finalize_node(child, count)
        for child in (*node.any_of, *node.one_of):
            for error in self._finalize_node(child, count):
                child.add_context(error)

        own_ok = not node.failed

        if node.type_failed is not None:
            own_ok = False
            yield ValidationError(
                f"the batch is not of type {node.type_failed!r}",
                validator="type",
                validator_value=node.type_failed,
                schema_path=[*node.segments, "type"],
            )

        if (
            node.contains is not None
            and node.contains_count < node.min_contains
        ):
            own_ok = False
            if not node.contains_count:
                yield ValidationError(
                    "the batch does not contain items "
                    "matching the given schema",
                    validator="contains",
                    validator_value=node.contains,
                    schema_path=[*node.segments, "contains"],
                )
            else:
                yield ValidationError(
                    "Too few items match the given schema (expected at least "
                    f"{node.min_contains} but only "
                    f"{node.contains_count} matched)",
                    validator="minContains",
                    validator_value=node.min_contains,
                    schema_path=[*node.segments, "contains"],
                )

        if node.max_items is not None and count > node.max_items:
            own_ok = False
            yield ValidationError(
                f"the batch is too long ({count} items, expected at most "
                f"{node.max_items})",
                validator="maxItems",
                validator_value=node.max_items,
                schema_path=[*node.segments, "maxItems"],
            )
        if node.min_items is not None and count < node.min_items:
            own_ok = False
            yield ValidationError(
                f"the batch is too short ({count} items, expected at least "
                f"{node.min_items})",
                validator="minItems",
                validator_value=node.min_items,
                schema_path=[*node.segments, "minItems"],
            )

        if node.unique:
            for indices in node.dedup.duplicate_groups():
                own_ok = False
                joined = ", ".join(str(each) for each in indices)
                yield ValidationError(
                    f"the batch has non-unique elements (items at "
                    f"indexes {joined} are duplicates)",
                    validator="uniqueItems",
                    validator_value=True,
                    schema_path=[*node.segments, "uniqueItems"],
                )

        if node.any_of and not any(child.valid for child in node.any_of):
            own_ok = False
            yield ValidationError(
                "the batch is not valid under any of the given schemas",
                validator="anyOf",
                validator_value=node.schema["anyOf"],
                context=self._combined_context(node.any_of),
                schema_path=[*node.segments, "anyOf"],
            )

        if node.one_of:
            matching = [child for child in node.one_of if child.valid]
            if not matching:
                own_ok = False
                yield ValidationError(
                    "the batch is not valid under any of the given schemas",
                    validator="oneOf",
                    validator_value=node.schema["oneOf"],
                    context=self._combined_context(node.one_of),
                    schema_path=[*node.segments, "oneOf"],
                )
            elif len(matching) > 1:
                own_ok = False
                reprs = ", ".join(repr(each.schema) for each in matching)
                yield ValidationError(
                    f"the batch is valid under each of {reprs}",
                    validator="oneOf",
                    validator_value=node.schema["oneOf"],
                    schema_path=[*node.segments, "oneOf"],
                )

        if node.has_items:
            coverage = (1 << count) - 1
        else:
            coverage = (1 << len(node.prefix_items)) - 1
            if node.contains_mask:
                coverage |= int.from_bytes(node.contains_mask, "little")
        for child in (*node.all_of, *node.any_of, *node.one_of):
            if child.valid:
                coverage |= child.coverage
        node.coverage = coverage

        if node.unevaluated_false:
            uncovered = ((1 << count) - 1) & ~coverage
            if uncovered:
                own_ok = False
                data = uncovered.to_bytes((count + 7) // 8, "little")
                for byte_index, byte in enumerate(data):
                    if not byte:
                        continue
                    for bit in range(8):
                        if byte >> bit & 1:
                            index = byte_index * 8 + bit
                            yield ValidationError(
                                "Unevaluated items are not allowed "
                                f"(item at index {index} was unexpected)",
                                validator="unevaluatedItems",
                                validator_value=False,
                                path=[index],
                                schema_path=(
                                    [*node.segments, "unevaluatedItems"]
                                ),
                            )

        node.valid = own_ok and all(child.valid for child in node.all_of)

    @staticmethod
    def _combined_context(children):
        context = []
        for child in children:
            context.extend(child.context)
        return context


def _incomplete_input_error(error):
    return ValidationError(
        f"input stream failed before the batch was fully read: {error}",
        cause=error,
    )
