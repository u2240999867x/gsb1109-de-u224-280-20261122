"""Revision-aware corrections for :class:`CachedReferenceField`.

Cached references are denormalized on purpose, which makes two writes race:

* a source document save propagates only the fields that changed, while
* :meth:`CachedReferenceField.sync_all` compensates every cached field from a
  snapshot of the current source documents.

If a compensation is paused after reading its snapshot, or a correction is
replayed from an older receipt, it must not overwrite a newer value. This
module therefore tracks a revision per cached database path in a sidecar
collection, leaving the visible cached document in its historical shape
(``{"_id": ..., "amount": ...}``).

Corrections carry a caller-supplied import number. Receipts are persisted in
the same sidecar collection before any cached owner is touched, so a process
that commits a source write and then disappears still leaves a durable,
retrievable receipt. Retrying the same import number returns the original
receipt without allocating a new revision or re-applying the source change.
"""

from pymongo import ReturnDocument

META_SUFFIX = "__cached_reference_meta"

_INDEXED_COLLECTIONS = set()


class Receipt(dict):
    """A dict with attribute access and friendly aliases."""

    _ALIASES = {
        "import_id": "import_number",
        "import_no": "import_number",
        "batch_id": "import_number",
        "correction_id": "import_number",
        "set_fields": "set",
        "fields_set": "set",
        "unset_fields": "unset",
        "fields_unset": "unset",
        "deleted_fields": "unset",
        "deleted": "unset",
        "document_id": "document",
        "source_id": "document",
        "provenance": "source",
    }

    def __getitem__(self, key):
        return super().__getitem__(self._ALIASES.get(key, key))

    def get(self, key, default=None):
        return super().get(self._ALIASES.get(key, key), default)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as error:
            raise AttributeError(item) from error

    @property
    def path_revisions(self):
        return self["revision"]

    def __repr__(self):
        return f"Receipt({dict(self)!r})"


def _field_key(field):
    return f"{field.owner_document.__name__}.{field.name}"


def _meta_collection(field):
    owner_document = field.owner_document
    db = owner_document._get_db()
    collection = db[owner_document._get_collection_name() + META_SUFFIX]
    key = (db.name, collection.name)
    if key not in _INDEXED_COLLECTIONS:
        try:
            collection.create_index([("import_number", 1), ("field_key", 1)])
            collection.create_index([("owner_pk", 1)])
        except Exception:
            pass
        _INDEXED_COLLECTIONS.add(key)
    return collection


def _str(value):
    return str(value)


def _counter_id(field, source_pk):
    return f"counter|{_field_key(field)}|{_str(source_pk)}"


def _source_revision_id(field, source_pk, path):
    return f"source|{_field_key(field)}|{_str(source_pk)}|{path}"


def _owner_revision_id(field, source_pk, owner_pk, path):
    return f"owner|{_field_key(field)}|{_str(source_pk)}|{_str(owner_pk)}|{path}"


def _receipt_id(field, source_pk, import_number):
    return f"receipt|{_field_key(field)}|{_str(source_pk)}|{import_number}"


def collect_db_paths(field):
    """Map cached Python field names to their database paths."""
    paths = {}
    for cached in field.fields:
        try:
            lookup = field.document_type._lookup_field(cached)
        except Exception:
            lookup = None

        if lookup and all(hasattr(part, "db_field") for part in lookup):
            paths[cached] = ".".join(part.db_field for part in lookup)
        else:
            paths[cached] = cached
    return paths


def cached_db_paths(field):
    return list(collect_db_paths(field).values())


def _matches(path, other):
    return (
        path == other
        or path.startswith(other + ".")
        or other.startswith(path + ".")
    )


def affected_paths(field, changed_paths, unset_paths):
    """Return the cached database paths touched by a change set."""
    changes = set(changed_paths) | set(unset_paths)
    return {
        db_path
        for db_path in collect_db_paths(field).values()
        if any(_matches(db_path, path) for path in changes)
    }


def value_at(son, path):
    """Read a database path from a raw SON, reporting presence separately."""
    value = son
    for part in path.split("."):
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None, False
    return value, True


def raw_statuses(field, source_pk, paths=None):
    """Read cached statuses from the raw source document.

    The raw database document is the only reliable way to tell a stored
    ``None`` from a field that is absent. That distinction is visible to
    callers inspecting owners afterwards.
    """
    raw = field.document_type._get_collection().find_one({"_id": source_pk})
    if raw is None:
        return {}

    statuses = {}
    for path in paths if paths is not None else cached_db_paths(field):
        value, present = value_at(raw, path)
        statuses[path] = (value, not present)
    return statuses


def current_revision(field, source_pk):
    doc = _meta_collection(field).find_one(
        {"_id": _counter_id(field, source_pk)}, {"value": 1}
    )
    return (doc or {}).get("value", 0)


def _next_revision(field, source_pk):
    collection = _meta_collection(field)
    counter = collection.find_one_and_update(
        {"_id": _counter_id(field, source_pk)},
        {"$inc": {"value": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return counter["value"]


def source_revision(field, source_pk, path):
    doc = _meta_collection(field).find_one(
        {"_id": _source_revision_id(field, source_pk, path)},
        {"revision": 1},
    )
    return (doc or {}).get("revision", 0)


def _set_source_revision(field, source_pk, path, revision, missing):
    _meta_collection(field).update_one(
        {"_id": _source_revision_id(field, source_pk, path)},
        {
            "$set": {
                "revision": revision,
                "path": path,
                "missing": bool(missing),
                "source_pk": source_pk,
            }
        },
        upsert=True,
    )


def owner_revision(field, source_pk, owner_pk, path):
    doc = _meta_collection(field).find_one(
        {"_id": _owner_revision_id(field, source_pk, owner_pk, path)},
        {"revision": 1},
    )
    # -1 means "never synchronized", which is older than any revision.
    return (doc or {}).get("revision", -1)


def _set_owner_revision(field, source_pk, owner_pk, path, revision):
    _meta_collection(field).update_one(
        {"_id": _owner_revision_id(field, source_pk, owner_pk, path)},
        {
            "$set": {
                "revision": revision,
                "path": path,
                "source_pk": source_pk,
                "owner_pk": owner_pk,
            }
        },
        upsert=True,
    )


def _owner_query(field, owner_pk, source_pk):
    return {"_id": owner_pk, f"{field.name}._id": source_pk}


def write_path(field, owner_pk, source_pk, path, value, missing, revision):
    """Write one cached path if no newer value already won.

    The filter includes the still-referenced source ``_id``; an owner that was
    re-pointed at another document is therefore left untouched.
    """
    if owner_revision(field, source_pk, owner_pk, path) >= revision:
        return 0

    db_path = f"{field.name}.{path}"
    raw_update = {"$unset": {db_path: 1}} if missing else {"$set": {db_path: value}}

    # Route through QuerySet.update so callers can observe/pause compensations
    # and so write concerns/read concerns defined on the queryset apply.
    updated = field.owner_document.objects(
        pk=owner_pk, __raw__={f"{field.name}._id": source_pk}
    ).update(__raw__=raw_update)
    if updated:
        _set_owner_revision(field, source_pk, owner_pk, path, revision)
        return updated
    return 0


def owner_ids(field, source_pk):
    """Return the primary keys of owners still referencing ``source_pk``."""
    collection = field.owner_document._get_collection()
    cursor = collection.find({f"{field.name}._id": source_pk}, {"_id": 1})
    return [document["_id"] for document in cursor]


def write_statuses(field, source_pk, statuses, revisions):
    """Write statuses to every owner still referencing ``source_pk``."""
    total = 0
    for owner_pk in owner_ids(field, source_pk):
        for path, (value, missing) in statuses.items():
            total += write_path(
                field,
                owner_pk,
                source_pk,
                path,
                value,
                missing,
                revisions.get(path, 0),
            )
    return total


def _receipt_payload(doc):
    hidden = {"_id", "field_key", "source_pk"}
    return {key: value for key, value in doc.items() if key not in hidden}


def _receipt_query(field, import_number, source=None):
    query = {"field_key": _field_key(field), "import_number": import_number}
    if source is not None:
        query["source_pk"] = _str(getattr(source, "pk", source))
    return query


def get_receipt(field, import_number, source=None):
    """Retrieve the durable receipt for an import number."""
    doc = _meta_collection(field).find_one(
        _receipt_query(field, import_number, source),
        sort=[("revision", -1)],
    )
    return None if doc is None else Receipt(_receipt_payload(doc))


def get_any_receipt(document, import_number):
    """Find a receipt for any cached reference field of ``document``."""
    for field in getattr(document.__class__, "_cached_reference_fields", []):
        receipt = get_receipt(field, import_number, document.pk)
        if receipt is not None:
            return field, receipt
    return None, None


def _store_receipt(field, document, import_number, source, statuses, revision):
    collection = _meta_collection(field)
    receipt_id = _receipt_id(field, document.pk, import_number)
    existing = collection.find_one({"_id": receipt_id})
    if existing is not None:
        return Receipt(_receipt_payload(existing))

    set_fields = {}
    unset_fields = {}
    for path, (value, missing) in statuses.items():
        if missing:
            unset_fields[path] = True
        else:
            set_fields[path] = value

    receipt = {
        "_id": receipt_id,
        "field_key": _field_key(field),
        "source_pk": _str(document.pk),
        "import_number": import_number,
        "source": source if source is not None else document.pk,
        "document": document.pk,
        "collection": field.document_type._get_collection_name(),
        "revision": revision,
        "set": set_fields,
        "unset": unset_fields,
        "values": {path: value for path, (value, _) in statuses.items()},
        "missing": [path for path, (_, missing) in statuses.items() if missing],
    }

    try:
        collection.insert_one(receipt)
        stored = receipt
    except Exception:
        existing = collection.find_one({"_id": receipt_id})
        if existing is None:
            raise
        stored = existing

    return Receipt(_receipt_payload(stored))


def record_source_save(field, document, import_number=None, source=None, sync=None):
    """Propagate a source save and optionally persist a correction receipt.

    This is called from the field's ``post_save`` listener, i.e. only after
    the source write has actually been committed. A save rejected by
    validation never reaches this point, so it cannot produce a correction
    that later jobs could synchronize.

    A save carrying an import number is recorded but *not* propagated: the
    cleaning flow keeps the correction in the receipt until the caller
    explicitly replays it. Saves without an import number keep the historical
    auto-sync behavior.
    """
    changed_paths, unset_paths = document._get_updated_fields()

    if import_number is not None:
        existing = get_receipt(field, import_number, document.pk)
        if existing is not None:
            # Retrying a correction must return the original receipt and must
            # not allocate a revision or replay the change onto the source.
            return existing

    affected = affected_paths(field, changed_paths, unset_paths)

    if not affected:
        # Nothing cached changed (or the save was a no-op). A correction must
        # report actual changes, so no receipt is produced for this field.
        return None

    statuses = _deferred_statuses(field, document, affected, unset_paths)

    revision = _next_revision(field, document.pk)
    for path, (_, missing) in statuses.items():
        _set_source_revision(field, document.pk, path, revision, missing)

    receipt = None
    if import_number is not None:
        receipt = _store_receipt(
            field, document, import_number, source, statuses, revision
        )

    if import_number is not None:
        should_sync = bool(sync)
    else:
        should_sync = field.auto_sync if sync is None else sync

    if should_sync:
        write_statuses(
            field,
            document.pk,
            statuses,
            {path: revision for path in statuses},
        )

    return receipt


def _deferred_statuses(field, document, paths, unset_paths):
    """Statuses for a save's cached paths, preserving explicit nulls.

    The in-memory delta knows whether ``None`` was assigned or a field was
    deleted. The raw document can additionally tell an actually deleted field
    from one that has been stored as ``null`` by MongoEngine.
    """
    raw_statuses_map = raw_statuses(field, document.pk, paths)
    statuses = {}
    for path in paths:
        deleted = any(_matches(path, unset) for unset in unset_paths)
        if deleted:
            statuses[path] = (None, True)
            continue

        value, missing = raw_statuses_map.get(path, (None, True))
        if missing:
            # If the in-memory value is None we treat it as an explicit null;
            # a real deletion was already handled above.
            cached_name = _cached_name_for(field, path)
            if cached_name is not None and document._data.get(cached_name) is None:
                statuses[path] = (None, False)
            else:
                statuses[path] = (None, True)
        else:
            statuses[path] = (value, False)
    return statuses


def _cached_name_for(field, db_path):
    for name, path in collect_db_paths(field).items():
        if path == db_path:
            return name
    return None


def apply_receipt(field, receipt, documents=None):
    """Apply a previously issued receipt to cached owner documents.

    Writes are guarded by the receipt revision, so an owner that already
    holds a newer value keeps it, and an owner re-pointed at another source
    is skipped by the reference ``_id`` filter.
    """
    if not isinstance(receipt, Receipt):
        receipt = Receipt(receipt)

    source_pk = receipt["document"]
    revision = receipt["revision"]

    statuses = {}
    for path, value in (receipt.get("set") or {}).items():
        statuses[path] = (value, False)
    for path in receipt.get("unset") or {}:
        statuses[path] = (None, True)

    if not statuses:
        return 0

    if documents is not None:
        total = 0
        for document in documents:
            owner_pk = getattr(document, "pk", document)
            for path, (value, missing) in statuses.items():
                total += write_path(
                    field, owner_pk, source_pk, path, value, missing, revision
                )
        return total

    return write_statuses(field, source_pk, statuses, {p: revision for p in statuses})


def sync_all(field):
    """Compensate every cached owner from a consistent source snapshot.

    All source values and revisions are collected before the first owner
    write. A correction committed while this method is paused has a higher
    revision and therefore wins over the stale snapshot.
    """
    snapshots = []
    for source in field.document_type.objects:
        statuses = raw_statuses(field, source.pk)
        revisions = {
            path: source_revision(field, source.pk, path) for path in statuses
        }
        snapshots.append((source.pk, statuses, revisions))

    total = 0
    for source_pk, statuses, revisions in snapshots:
        total += write_statuses(field, source_pk, statuses, revisions)
    return total
