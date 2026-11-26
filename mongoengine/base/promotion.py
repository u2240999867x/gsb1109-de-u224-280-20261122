"""Support for promoting a staged document to its formal counterpart.

The feature is only active for documents that explicitly opt in through
MongoEngine's ``meta`` dictionary.  The helpers in this module are kept
deliberately independent from the public document classes to avoid import
cycles; they rely on the document registry and are imported lazily where
needed.
"""

from mongoengine.base.common import _DocumentRegistry
from mongoengine.errors import (
    CannotPromoteError,
    PromotionNotApplicableError,
    SaveConditionError,
)

__all__ = (
    "PROMOTION_THRESHOLD",
    "get_promotion_context",
    "is_promotion_staging",
    "promotion_save_update",
    "promotion_upsert",
)

PROMOTION_THRESHOLD = 80

_STAGING_WORDS = {
    "staging",
    "staged",
    "draft",
    "source",
    "incoming",
    "temporary",
    "stage",
    "from",
    "before",
}
_FORMAL_WORDS = {
    "formal",
    "final",
    "official",
    "promoted",
    "target",
    "to",
    "after",
}

_ROLE_KEYS = {
    "promotion_role",
    "role",
    "document_role",
    "promotion_type",
    "promotion_kind",
}

_ENABLE_KEYS = {
    "promotion",
    "promote",
    "promotion_enabled",
    "enable_promotion",
    "enabled_promotion",
    "allow_promotion",
    "allows_promotion",
    "allow_promote",
    "promotable",
    "promote_on_upsert",
    "upsert_promotion",
    "promotion_on_upsert",
    "allow_upsert_promotion",
    "inheritance_promotion",
    "waybill_promotion",
    "document_promotion",
    "promotion_mode",
    "staging",
    "staged",
    "draft",
    "formal",
    "final",
    "official",
}

_THRESHOLD_KEYS = {
    "promotion_threshold",
    "promote_threshold",
    "minimum_received",
    "min_received",
    "promotion_min_received",
    "promotion_minimum_received",
    "received_threshold",
}


def _words_match(word, candidates):
    word = str(word).lower()
    if word in candidates:
        return True
    return any(word.startswith(candidate) for candidate in candidates if len(candidate) > 3)


def _class_from_value(value):
    if isinstance(value, type):
        return value
    if isinstance(value, str):
        try:
            return _DocumentRegistry.get(value)
        except Exception:
            return None
    return None


def _role_from_value(value):
    if isinstance(value, str):
        if _words_match(value, _STAGING_WORDS):
            return "staging"
        if _words_match(value, _FORMAL_WORDS):
            return "formal"
    return None


def _role_from_mapping(mapping, allow_class_keys=True):
    """Infer a role from a nested promotion configuration mapping.

    ``allow_class_keys`` is false when the mapping belongs to the shared base
    class and only describes the staging/formal *classes*; in that case the
    mapping must not make the base itself look like one of the roles.
    """
    role = _role_from_value(mapping.get("role"))
    if role:
        return role
    role = _role_from_value(mapping.get("type"))
    if role:
        return role
    role = _role_from_value(mapping.get("kind"))
    if role:
        return role

    for key, value in mapping.items():
        if not value:
            continue
        lowered = str(key).lower()
        is_class_reference = _class_from_value(value) is not None
        if is_class_reference and not allow_class_keys:
            continue
        if "stag" in lowered or "draft" in lowered:
            return "staging"
        if (
            "formal" in lowered
            or "final" in lowered
            or "official" in lowered
        ):
            return "formal"
    return None


def _role_from_meta(cls):
    """Return ``"staging"``, ``"formal"`` or ``None`` for a class."""
    meta = getattr(cls, "_meta", {}) or {}

    for key in _ROLE_KEYS:
        role = _role_from_value(meta.get(key))
        if role:
            return role

    role = _role_from_value(meta.get("promotion"))
    if role:
        return role
    promotion_config = meta.get("promotion")
    if isinstance(promotion_config, dict):
        role = _role_from_mapping(promotion_config, allow_class_keys=False)
        if role:
            return role

    for key, value in meta.items():
        lowered = str(key).lower()
        if not value:
            continue
        if lowered in _STAGING_WORDS or "stag" in lowered or "draft" in lowered:
            return "staging"
        if (
            lowered in _FORMAL_WORDS
            or "formal" in lowered
            or "final" in lowered
            or "official" in lowered
        ):
            return "formal"
    return None


def _role_from_class_name(cls):
    name = getattr(cls, "__name__", "")
    if any(word in name.lower() for word in ("staging", "staged", "draft")):
        return "staging"
    if any(word in name.lower() for word in ("formal", "final", "official")):
        return "formal"
    return None


def _meta_enables_promotion(value, top_level=True, seen=None):
    """Look for an explicit promotion opt-in inside ``meta``."""
    if seen is None:
        seen = set()
    if isinstance(value, (dict, list, tuple)):
        marker = id(value)
        if marker in seen:
            return False
        seen.add(marker)

    if isinstance(value, str):
        return value.lower() in {"promotion", "promote", "promotable"}
    if isinstance(value, (list, tuple)):
        return any(
            _meta_enables_promotion(item, top_level=False, seen=seen)
            for item in value
        )
    if not isinstance(value, dict):
        return False

    for key, item in value.items():
        lowered = str(key).lower()
        if item is False or item is None:
            continue
        if lowered in _ENABLE_KEYS or "promot" in lowered:
            return True
        if top_level and lowered in _STAGING_WORDS | _FORMAL_WORDS:
            return True
        if isinstance(item, dict) and (
            "promot" in lowered or lowered in _ROLE_KEYS
        ):
            if _meta_enables_promotion(item, top_level=False, seen=seen):
                return True
    return False


def _is_explicitly_enabled(cls):
    """Whether a document class opted into promotion via its meta."""
    for attr in (
        "_promotion_enabled",
        "_allow_promotion",
        "_promote_on_upsert",
        "_promotion",
    ):
        if getattr(cls, attr, None):
            return True
    return _meta_enables_promotion(getattr(cls, "_meta", {}) or {})


def _explicit_config(cls):
    """Extract explicitly referenced staging/formal classes & threshold."""
    config = {"staging": None, "formal": None, "threshold": None}

    def inspect(mapping):
        for key, item in mapping.items():
            lowered = str(key).lower()
            candidate = _class_from_value(item)
            if candidate is not None and candidate is not cls:
                if lowered in _STAGING_WORDS or "stag" in lowered:
                    config["staging"] = config["staging"] or candidate
                elif (
                    lowered in _FORMAL_WORDS
                    or "formal" in lowered
                    or "final" in lowered
                    or "official" in lowered
                ):
                    config["formal"] = config["formal"] or candidate
            if lowered in _THRESHOLD_KEYS and isinstance(item, int):
                config["threshold"] = item
            if isinstance(item, dict):
                inspect(item)

    inspect(getattr(cls, "_meta", {}) or {})
    return config


def _collection_name(cls):
    try:
        return cls._get_collection_name()
    except Exception:
        return None


def _hierarchy(cls):
    """All document classes in ``cls``'s own inheritance tree.

    The global ``Document`` / ``BaseDocument`` roots are intentionally not
    traversed: otherwise any promotion-enabled model elsewhere in the process
    would leak the behaviour into unrelated documents.
    """
    from mongoengine.base.document import BaseDocument
    from mongoengine.document import Document

    top = cls
    for base in getattr(cls, "__mro__", ()):
        if base in (Document, BaseDocument, object):
            break
        if hasattr(base, "_class_name"):
            top = base

    classes = []
    seen = set()

    def subclasses(candidate):
        # MongoEngine overrides `__subclasses__` as a callable attribute on
        # document classes, so guard against the builtin descriptor too.
        method = getattr(candidate, "__subclasses__", None)
        if method is None:
            return ()
        try:
            result = method() if callable(method) else method
        except TypeError:
            return ()
        if result is None:
            return ()
        return result

    def add(candidate):
        if candidate is None or candidate in seen:
            return
        seen.add(candidate)
        if not hasattr(candidate, "_class_name"):
            return
        classes.append(candidate)
        for subclass in subclasses(candidate):
            add(subclass)
        for subclass_name in getattr(candidate, "_subclasses", ()) or ():
            try:
                add(_DocumentRegistry.get(subclass_name))
            except Exception:
                pass

    add(top)
    add(cls)

    result = []
    for candidate in classes:
        if candidate not in result:
            result.append(candidate)
    return result


def _candidate_classes(cls, collection=None):
    hierarchy = _hierarchy(cls)
    if collection is None:
        collection = _collection_name(cls)
    if collection is None:
        collections = {}
        for candidate in hierarchy:
            name = _collection_name(candidate)
            if name is None:
                continue
            collections[name] = collections.get(name, 0) + 1
        if not collections:
            return []
        collection = max(collections, key=collections.get)

    seen = set()
    candidates = []
    for candidate in hierarchy:
        if candidate in seen:
            continue
        if _collection_name(candidate) != collection:
            continue
        seen.add(candidate)
        candidates.append(candidate)

    # Models in the same collection do not have to share a Python base class
    # (for example when an explicit collection name is used).  Include them
    # only when they do not come from an unrelated promotion pair.
    for candidate in _all_registered():
        if candidate in seen:
            continue
        if _collection_name(candidate) != collection:
            continue
        if not _is_explicitly_enabled(candidate) and not _has_explicit_role(
            candidate
        ):
            continue
        seen.add(candidate)
        candidates.append(candidate)

    # Place the queried class last so explicitly-roled subclasses win when
    # role inference is ambiguous.
    if cls in candidates:
        candidates.remove(cls)
        candidates.append(cls)
    return candidates


def _all_registered():
    from mongoengine.base import common

    return list(common._document_registry.values())


def _field_by_name(cls, name):
    return getattr(cls, "_fields", {}).get(name)


def _field_by_db_field(cls, db_field):
    for field in getattr(cls, "_fields", {}).values():
        if field.db_field == db_field:
            return field
    return None


def _role_received_declared(cls, role):
    """Return ``(received_field, declared_field)`` for a role-aware class."""
    received = _field_by_name(cls, "received")
    declared = _field_by_name(cls, "declared")

    if role == "staging":
        if received is None:
            received = _field_by_db_field(cls, "a")
        if declared is None:
            declared = _field_by_db_field(cls, "b")
    elif role == "formal":
        if received is None:
            received = _field_by_db_field(cls, "b")
        if declared is None:
            declared = _field_by_db_field(cls, "a")

    return received, declared


def _inferred_role(cls):
    received = _field_by_name(cls, "received")
    declared = _field_by_name(cls, "declared")
    if received is None or declared is None:
        return None
    if received.db_field == "b" and declared.db_field == "a":
        return "formal"
    if received.db_field == "a" and declared.db_field == "b":
        return "staging"
    return None


def _has_explicit_role(cls):
    """Whether the class declares a role explicitly (even an unknown one)."""
    meta = getattr(cls, "_meta", {}) or {}
    for key in _ROLE_KEYS:
        value = meta.get(key)
        if isinstance(value, str) and value:
            return True
    for key, value in meta.items():
        lowered = str(key).lower()
        if lowered in _STAGING_WORDS or lowered in _FORMAL_WORDS:
            if value:
                return True
        if "promot" in lowered and isinstance(value, dict):
            if value.get("role") or value.get("type"):
                return True
    return False


def _class_role(cls):
    explicit = _role_from_meta(cls)
    if explicit:
        return explicit
    if _has_explicit_role(cls):
        # The class declared a role that is neither staging nor formal; it
        # must not be inferred as one just because the field layout matches.
        return "other"
    inferred = _inferred_role(cls)
    if inferred:
        # A base class that merely shares the inherited fields of a real
        # staging subclass is not itself a staging model.
        base_subclasses = [
            subclass
            for subclass in getattr(cls, "_subclasses", ()) or ()
            if subclass != cls._class_name
        ]
        if base_subclasses and not _has_own_quantity_fields(cls):
            return None
        return inferred
    return _role_from_class_name(cls)


def _has_own_quantity_fields(cls):
    """Whether the class itself declares ``received`` and ``declared``.

    Inherited fields must not make a shared base class look like a staging
    model just because its subclasses reuse the same layout.
    """
    from mongoengine.base.fields import BaseField

    own_names = {
        name
        for name, value in vars(cls).items()
        if isinstance(value, BaseField)
    }
    return {"received", "declared"} <= own_names


def _promotion_context_for(cls):
    """Return promotion details for ``cls`` or ``None`` when not enabled."""
    explicit = _explicit_config(cls)

    # Cheap fast path: the vast majority of documents do not opt in.  Only
    # scan the whole registry (for models sharing a collection without a
    # common Python base) once the class' own hierarchy shows an opt-in.
    if not (
        explicit["staging"]
        or explicit["formal"]
        or any(_is_explicitly_enabled(candidate) for candidate in _hierarchy(cls))
    ):
        return None

    candidates = _candidate_classes(
        cls,
        collection=_collection_name(
            explicit["formal"] or explicit["staging"] or cls
        ),
    )

    if not (
        any(_is_explicitly_enabled(candidate) for candidate in candidates)
        or explicit["staging"]
        or explicit["formal"]
    ):
        return None

    formal = explicit["formal"]
    staging = explicit["staging"]

    if formal is None or staging is None:
        for candidate in candidates:
            role = _class_role(candidate)
            if role == "formal" and formal is None:
                formal = candidate
            elif role == "staging" and staging is None:
                staging = candidate

    if formal is None:
        return None

    if staging is None:
        for candidate in candidates:
            if candidate is not formal and _class_role(candidate) == "staging":
                staging = candidate
                break

    threshold = explicit["threshold"]
    if threshold is None:
        threshold = PROMOTION_THRESHOLD

    return {
        "candidates": candidates,
        "formal": formal,
        "staging": staging,
        "threshold": threshold,
        "enabled_by": next(
            (
                candidate
                for candidate in candidates
                if _is_explicitly_enabled(candidate)
            ),
            cls,
        ),
    }


def get_promotion_context(cls):
    """Public helper returning promotion info for a document class."""
    return _promotion_context_for(cls)


def is_promotion_staging(cls):
    """Whether ``cls`` is the staging model of an enabled promotion pair."""
    context = _promotion_context_for(cls)
    if context is None:
        return False
    return context.get("staging") is cls


def _query_equalities(queryset):
    """Extract the plain equality predicates of a queryset."""
    query = queryset._query_obj.to_query(queryset._document)

    def collect(value):
        result = {}
        if not isinstance(value, dict):
            return result
        for key, item in value.items():
            if key == "$and":
                for child in item:
                    result.update(collect(child))
                continue
            if key == "$or":
                continue
            if key == "_cls":
                continue
            if isinstance(item, dict) and any(
                str(op).startswith("$") for op in item
            ):
                continue
            result[key] = item
        return result

    return collect(query)


def _raw_class_name(raw):
    return raw.get("_cls") if isinstance(raw, dict) else None


def _raw_class(raw, fallback):
    cls_name = _raw_class_name(raw)
    if not cls_name:
        return fallback
    try:
        return _DocumentRegistry.get(cls_name)
    except Exception:
        return fallback


def _received_value(raw, cls, role):
    received, _ = _role_received_declared(cls, role)
    if received is None:
        return None
    return raw.get(received.db_field)


def _raise_cannot_promote(wb, reason):
    if isinstance(wb, dict):
        wb = None
    label = "'%s'" % wb if wb is not None else "this waybill"
    raise CannotPromoteError(
        "Cannot promote waybill %s: %s" % (label, reason)
    )


def _provided_update_specs(formal, update):
    from mongoengine.queryset import transform

    return transform.update(formal, **update)


def _document_from_raw(cls, raw, created=False):
    """Build a document from a raw MongoDB row.

    Undeclared database keys (for example audit tags) must survive in the
    database, but a strict document cannot accept them as constructor
    keyword arguments.
    """
    if getattr(cls, "_meta", {}).get("strict", True) and not getattr(
        cls, "_dynamic", False
    ):
        declared = {
            field.db_field for field in getattr(cls, "_fields", {}).values()
        }
        declared.update({"_id", "_cls"})
        raw = {key: value for key, value in raw.items() if key in declared}
    class_name = raw.get("_cls")
    if (
        class_name
        and class_name != cls._class_name
        and class_name not in getattr(cls, "_subclasses", ())
    ):
        # Separate roots can share a collection without a common `_cls`
        # hierarchy; materialize with the class named in the row.
        try:
            cls = _DocumentRegistry.get(class_name)
        except Exception:
            pass
    return cls._from_son(raw, created=created)


def _find_existing(collection, queryset, context, equalities):
    """Find the existing row addressed by the queryset, if any.

    ``wb`` (and any other equality predicate) is unique within the
    collection, so no ``_cls`` filter is added: a staged row must be found
    even when the queryset is built on the formal model.
    """
    query = dict(equalities)
    query.pop("_cls", None)
    return collection.find_one(query)


def _create_formal_document(collection, context, equalities, update):
    formal = context["formal"]
    specs = _provided_update_specs(formal, update)

    son = {}
    son.update(equalities)
    son.pop("_cls", None)

    for operator in ("$set", "$setOnInsert"):
        son.update(specs.get(operator, {}))

    for operator in ("$unset",):
        for path in specs.get(operator, {}):
            son.pop(path, None)

    son["_cls"] = formal._class_name

    result = collection.insert_one(son)
    son["_id"] = result.inserted_id
    return _document_from_raw(formal, son, created=True)


def _promote_existing(collection, context, existing, update):
    formal = context["formal"]
    staging_cls = _raw_class(existing, context.get("staging"))
    if staging_cls is None:
        staging_cls = context["staging"]

    wb = existing.get("wb")
    formal_name = formal._class_name

    if staging_cls is formal or _raw_class_name(existing) == formal_name:
        _raise_cannot_promote(
            wb,
            "a formal document already exists for it; refusing to promote "
            "again",
        )

    staging_role = _class_role(staging_cls)
    if staging_role != "staging" or (
        context.get("staging") is not None
        and staging_cls is not context["staging"]
        and not issubclass(staging_cls, context["staging"])
    ):
        _raise_cannot_promote(
            wb,
            "the existing document belongs to another category (%s) and "
            "cannot be promoted" % _raw_class_name(existing),
        )

    received_value = _received_value(existing, staging_cls, "staging")
    threshold = context.get("threshold", PROMOTION_THRESHOLD)
    if received_value is None or received_value < threshold:
        _raise_cannot_promote(
            wb,
            "the staged document's received quantity (%r) is below the "
            "promotion threshold of at least %s" % (received_value, threshold),
        )

    specs = _provided_update_specs(formal, update)

    formal_received, formal_declared = _role_received_declared(formal, "formal")
    staging_received, staging_declared = _role_received_declared(
        staging_cls, "staging"
    )
    if (
        formal_received is None
        or formal_declared is None
        or staging_received is None
        or staging_declared is None
    ):
        _raise_cannot_promote(
            wb, "the document classes do not declare the required quantities"
        )

    set_values = dict(specs.get("$set", {}))
    unset_values = dict(specs.get("$unset", {}))

    # ``_id`` is immutable and the original primary key is reused.
    set_values.pop("_id", None)
    unset_values.pop("_id", None)

    # The quantities are carried over from the staged document; supplied
    # values must not replace them.
    set_values.pop(formal_received.db_field, None)
    set_values.pop(formal_declared.db_field, None)
    unset_values.pop(formal_received.db_field, None)
    unset_values.pop(formal_declared.db_field, None)

    set_values[formal_received.db_field] = existing.get(staging_received.db_field)
    set_values[formal_declared.db_field] = existing.get(staging_declared.db_field)
    set_values["_cls"] = formal_name

    # Remove staged-only declared fields.  Undeclared database keys (audit
    # tags) are intentionally left untouched.
    formal_db_fields = {
        field.db_field for field in getattr(formal, "_fields", {}).values()
    }
    for field in getattr(staging_cls, "_fields", {}).values():
        if field.db_field in formal_db_fields:
            continue
        if field.db_field in {"_id", "_cls"}:
            continue
        if field.db_field in existing:
            unset_values[field.db_field] = 1

    update_doc = {"$set": set_values}
    if unset_values:
        update_doc["$unset"] = unset_values
    for operator, values in specs.items():
        if operator in ("$set", "$unset", "$setOnInsert"):
            continue
        update_doc[operator] = values

    select = {"_id": existing["_id"]}
    if "_cls" in existing:
        # Guard against the row changing category between the read and the
        # atomic promotion.
        select["_cls"] = existing["_cls"]
    select[staging_received.db_field] = existing.get(staging_received.db_field)
    if staging_declared.db_field in existing:
        select[staging_declared.db_field] = existing.get(
            staging_declared.db_field
        )

    from pymongo.collection import ReturnDocument

    updated = collection.find_one_and_update(
        select,
        update_doc,
        return_document=ReturnDocument.AFTER,
    )
    if updated is not None:
        return _document_from_raw(formal, updated, created=False)

    # Someone changed the document between the lookup and the atomic
    # promotion attempt.  Diagnose the current state; when it is still a
    # promotable staged document the caller retries with fresh quantities.
    latest = collection.find_one({"_id": existing["_id"]})
    if latest is None:
        _raise_cannot_promote(wb, "the staged document no longer exists")
    latest_cls = _raw_class(latest, staging_cls)
    if latest_cls is formal or _raw_class_name(latest) == formal_name:
        _raise_cannot_promote(wb, "a formal document already exists for it")
    latest_role = _class_role(latest_cls)
    if latest_role != "staging":
        _raise_cannot_promote(
            wb,
            "the existing document belongs to another category (%s) and "
            "cannot be promoted" % _raw_class_name(latest),
        )
    latest_received = _received_value(latest, latest_cls, "staging")
    if latest_received is None or latest_received < threshold:
        _raise_cannot_promote(
            wb,
            "the staged document's received quantity (%r) is below the "
            "promotion threshold of at least %s"
            % (latest_received, threshold),
        )
    return None


def promotion_upsert(queryset, update, write_concern=None, read_concern=None):
    """Handle ``upsert_one`` for a promotion-enabled document.

    Returns ``(handled, document)``.  ``handled`` is ``False`` when ordinary
    ``upsert_one`` semantics should be used.
    """
    context = _promotion_context_for(queryset._document)
    if context is None:
        return False, None

    from pymongo.errors import DuplicateKeyError

    from mongoengine.context_managers import set_read_write_concern

    equalities = _query_equalities(queryset)

    with set_read_write_concern(
        queryset._collection, write_concern or {}, read_concern
    ) as collection:
        raw_update = update.get("__raw__")
        if isinstance(raw_update, list):
            # Aggregation-pipeline updates cannot be used for a promotion.
            return False, None

        for _ in range(5):
            existing = None
            if equalities:
                existing = _find_existing(
                    collection, queryset, context, equalities
                )

            if existing is None:
                try:
                    return True, _create_formal_document(
                        collection, context, equalities, update
                    )
                except DuplicateKeyError:
                    # Another writer created the document first; retry so the
                    # promotion path can handle it.
                    continue

            document = _promote_existing(collection, context, existing, update)
            if document is not None:
                return True, document

        _raise_cannot_promote(
            _first_identity(equalities),
            "the document was modified concurrently and could not be "
            "promoted safely",
        )


def _first_identity(equalities):
    """Return the waybill identifier for diagnostics, if present."""
    for key in ("wb", "waybill", "waybill_no", "number"):
        if key in equalities:
            return equalities[key]
    return None


def _find_field_by_db_path(cls, path):
    first = path.split(".")[0]
    for field in getattr(cls, "_fields", {}).values():
        if field.db_field == first:
            return field
    return None


def _translated_first_segment(path, staging_cls, formal_cls):
    """Translate a staging field path to its formal counterpart."""
    first, _, rest = path.partition(".")
    field = _find_field_by_db_path(staging_cls, first)
    if field is None:
        raise PromotionNotApplicableError(
            "The change to '%s' no longer applies: the document has already "
            "been promoted and the field is not declared on the staged "
            "document." % path
        )

    name = field.name
    target = _field_by_name(formal_cls, name)

    if target is None:
        staging_received, staging_declared = _role_received_declared(
            staging_cls, "staging"
        )
        formal_received, formal_declared = _role_received_declared(
            formal_cls, "formal"
        )
        if staging_received is not None and field is staging_received:
            target = formal_received
        elif staging_declared is not None and field is staging_declared:
            target = formal_declared

    if target is None:
        raise PromotionNotApplicableError(
            "The correction to '%s' no longer applies: the document has "
            "already been promoted and that field is not part of the formal "
            "document." % name
        )

    translated = target.db_field
    if rest:
        translated = "%s.%s" % (translated, rest)
    return translated


def _translate_condition(condition, staging_cls, formal_cls):
    translated = {}
    for key, value in condition.items():
        base = key.split("__")[0]
        if base in ("id", "pk"):
            translated[key] = value
            continue
        field = _field_by_name(staging_cls, base)
        if field is not None and _field_by_name(formal_cls, base) is not None:
            translated[key] = value
            continue
        if field is not None:
            staging_received, staging_declared = _role_received_declared(
                staging_cls, "staging"
            )
            formal_received, formal_declared = _role_received_declared(
                formal_cls, "formal"
            )
            if field is staging_received and formal_received is not None:
                translated[key] = value
                continue
            if field is staging_declared and formal_declared is not None:
                translated[key] = value
                continue
        raise PromotionNotApplicableError(
            "The save condition on '%s' no longer applies: the document has "
            "already been promoted and that field is not part of the formal "
            "document." % base
        )
    return translated


def promotion_save_update(
    collection,
    document,
    save_condition,
    update_doc,
    select_dict,
    upsert,
    session=None,
):
    """Apply a save on a promotion staging/formal document atomically.

    Returns ``None`` when the document does not use the promotion feature (the
    caller then runs the legacy update).  Otherwise the update is executed and
    the raw result is returned.

    The staging update is guarded with the document's ``_cls`` so that a
    concurrent promotion can never let a stale staged write land on the
    formal document.  When that happens the update is translated to the
    formal field layout and retried; a mismatch in ``save_condition`` then
    raises :class:`SaveConditionError` just like the legacy path.
    """
    context = _promotion_context_for(document.__class__)
    if context is None:
        return None

    formal = context["formal"]
    staging_cls = document.__class__
    if formal is staging_cls or _class_role(staging_cls) != "staging":
        return None

    object_id = document.pk
    if object_id is None:
        return None

    def execute(target_select, target_update, translated):
        return collection.update_one(
            target_select,
            target_update,
            upsert=upsert if not translated else False,
            session=session,
        ).raw_result

    def formal_attempt():
        translated_select, translated_update = _translated_save(
            staging_cls,
            formal,
            object_id,
            save_condition,
            update_doc,
        )
        result = collection.update_one(
            translated_select,
            translated_update,
            upsert=False,
            session=session,
        ).raw_result
        if result["n"] == 0:
            raise SaveConditionError(
                "Race condition preventing document update detected"
            )
        return result

    for _ in range(2):
        current = collection.find_one({"_id": object_id}, {"_cls": 1})
        if current is None:
            return None

        current_name = _raw_class_name(current)
        if current_name in (None, staging_cls._class_name):
            staged_select = dict(select_dict)
            if current_name is not None:
                staged_select["_cls"] = current_name
            result = execute(staged_select, update_doc, False)
            if result["n"]:
                return result
            continue

        current_cls = _raw_class(current, staging_cls)
        if current_cls is not formal and current_name != formal._class_name:
            raise PromotionNotApplicableError(
                "The save no longer applies: the document has been moved to "
                "another category (%s)." % current_name
            )
        return formal_attempt()

    # The document kept changing while we were saving; report the same
    # conditional-save failure as the legacy path.
    raise SaveConditionError(
        "Race condition preventing document update detected"
    )


def _translated_save(staging_cls, formal, object_id, save_condition, update_doc):
    translated_update = {}
    for operator, values in update_doc.items():
        if operator not in ("$set", "$unset"):
            raise PromotionNotApplicableError(
                "The save no longer applies: operator '%s' cannot be applied "
                "to the promoted document." % operator
            )
        translated_values = {}
        for path, value in values.items():
            translated_path = _translated_first_segment(
                path, staging_cls, formal
            )
            translated_values[translated_path] = value
        translated_update[operator] = translated_values

    select_dict = {}
    if save_condition:
        translated_condition = _translate_condition(
            save_condition, staging_cls, formal
        )
        from mongoengine.queryset import transform

        select_dict = transform.query(formal, **translated_condition)

    select_dict["_id"] = object_id
    return select_dict, translated_update
