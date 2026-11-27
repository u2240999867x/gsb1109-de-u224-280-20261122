from bson import DBRef

from mongoengine.base import (
    BaseDict,
    BaseList,
    EmbeddedDocumentList,
    TopLevelDocumentMetaclass,
    _DocumentRegistry,
)
from mongoengine.base.datastructures import LazyReference, UnresolvedReference
from mongoengine.connection import _get_db_for_database, _get_session, get_db
from mongoengine.document import Document, EmbeddedDocument
from mongoengine.errors import NotRegistered
from mongoengine.fields import (
    GenericLazyReferenceField,
    LazyReferenceField,
    ReferenceField,
)
from mongoengine.queryset import QuerySet

MISSING_REFERENCE = "missing"
TYPE_MISMATCH_REFERENCE = "type_mismatch"
UNRESOLVED_DATABASE_REFERENCE = "database_unresolved"

# Keyword aliases accepted by select_related to specify the region database
# (used for this read) and the default database (used for legacy references
# which do not carry a database of their own).
DATABASE_KEYS = (
    "database",
    "db",
    "db_alias",
    "region",
    "region_db",
    "region_database",
    "region_alias",
    "read_db",
    "read_database",
    "read_alias",
)
DEFAULT_DATABASE_KEYS = (
    "default_database",
    "default_db",
    "default_db_alias",
    "default_alias",
    "legacy_db",
    "legacy_database",
    "legacy_alias",
)


def resolve_select_related_databases(
    database=None, default_database=None, **kwargs
):
    """Resolve the many accepted keyword spellings to ``(database, default)``."""
    explicit = database is not None or default_database is not None
    explicit = explicit or any(kwargs.get(key) is not None for key in DATABASE_KEYS)
    explicit = explicit or any(
        kwargs.get(key) is not None for key in DEFAULT_DATABASE_KEYS
    )

    region = database
    for key in DATABASE_KEYS:
        if region is None and kwargs.get(key) is not None:
            region = kwargs[key]

    legacy = default_database
    for key in DEFAULT_DATABASE_KEYS:
        if legacy is None and kwargs.get(key) is not None:
            legacy = kwargs[key]

    if legacy is None:
        legacy = region

    return region, legacy, explicit


class DeReference:
    def __call__(
        self,
        items,
        max_depth=1,
        instance=None,
        name=None,
        database=None,
        default_database=None,
        **kwargs,
    ):
        """
        Cheaply dereferences the items to a set depth.
        Also handles the conversion of complex data types.

        :param items: The iterable (dict, list, queryset) to be dereferenced.
        :param max_depth: The maximum depth to recurse to
        :param instance: The owning instance used for tracking changes by
            :class:`~mongoengine.base.ComplexBaseField`
        :param name: The name of the field, used for tracking changes by
            :class:`~mongoengine.base.ComplexBaseField`
        :param database: The region database used for this read.  References
            that carry their own database are read from it; references without
            one are resolved against this database.
        :param default_database: Alias for ``database``, describing the
            database used for legacy references that do not carry one.
        """
        if items is None or isinstance(items, str):
            return items

        # cheapest way to convert a queryset to a list
        # list(queryset) uses a count() query to determine length
        if isinstance(items, QuerySet):
            items = [i for i in items]

        self.max_depth = max_depth
        # The default database is used for legacy references without a
        # database of their own.  ``default_database`` is the explicit name
        # for it; ``database`` doubles as the region database when only one
        # value is supplied.
        self.default_database = (
            default_database if default_database is not None else database
        )
        self.explicit_database = kwargs.get("explicit", None)
        if self.explicit_database is None:
            self.explicit_database = (
                database is not None or default_database is not None
            )

        # When a field lazily dereferences its value (e.g. a ListField being
        # accessed after select_related), use the owning document's database
        # context when that document itself was fetched from an explicit
        # database.  Ordinary documents keep using the model's own alias.
        if self.default_database is None and instance is not None:
            if getattr(instance, "_db_context_for_nested", False):
                context = getattr(instance, "_db_context", None)
                if context:
                    self.default_database = context.get("database") or context.get(
                        "alias"
                    )

        self.reference_map = {}
        self.reference_types = {}
        self.database_info = {}
        self.reference_levels = {}
        self._attached_documents = set()
        self.unresolvable_groups = set()

        doc_type = None
        expected_type = None

        if instance and isinstance(
            instance, (Document, EmbeddedDocument, TopLevelDocumentMetaclass)
        ):
            doc_type = instance._fields.get(name)
            while hasattr(doc_type, "field"):
                doc_type = doc_type.field

            if isinstance(doc_type, ReferenceField):
                field = doc_type
                doc_type = doc_type.document_type
                is_list = not hasattr(items, "items")

                if is_list and all(i.__class__ == doc_type for i in items):
                    return items
                elif not is_list and all(
                    i.__class__ == doc_type for i in items.values()
                ):
                    return items
                elif not field.dbref:
                    # We must turn the ObjectIds into DBRefs

                    # Recursively dig into the sub items of a list/dict
                    # to turn the ObjectIds into DBRefs
                    def _get_items_from_list(items):
                        new_items = []
                        for v in items:
                            value = v
                            if isinstance(v, dict):
                                value = _get_items_from_dict(v)
                            elif isinstance(v, list):
                                value = _get_items_from_list(v)
                            elif not isinstance(v, (DBRef, Document)):
                                value = field.to_python(v)
                            new_items.append(value)
                        return new_items

                    def _get_items_from_dict(items):
                        new_items = {}
                        for k, v in items.items():
                            value = v
                            if isinstance(v, list):
                                value = _get_items_from_list(v)
                            elif isinstance(v, dict):
                                value = _get_items_from_dict(v)
                            elif not isinstance(v, (DBRef, Document)):
                                value = field.to_python(v)
                            new_items[k] = value
                        return new_items

                    if not hasattr(items, "items"):
                        items = _get_items_from_list(items)
                    else:
                        items = _get_items_from_dict(items)

                expected_type = doc_type

        self._find_references(
            items, 0, expected_type=expected_type, database=self.default_database
        )
        self.object_map = self._fetch_objects()
        return self._attach_objects(
            items,
            0,
            instance,
            name,
            expected_type=expected_type,
            database=self.default_database,
            use_instance_context=False,
        )

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------
    def _model_database(self, document_type=None):
        """Resolve the database configured on the model itself."""
        alias = None
        if document_type is not None:
            alias = getattr(document_type, "_meta", {}).get("db_alias")

        if alias is None:
            try:
                db = get_db()
            except Exception:
                return None, None
            return db.name, "default"

        db, resolved_alias = _get_db_for_database(alias)
        if db is None:
            return alias, alias
        return db.name, resolved_alias

    def _database_for_reference(self, database, document_type=None):
        """Return ``(name, db, alias)`` for a reference's database."""
        if database is not None:
            db, alias = _get_db_for_database(database)
            if db is not None:
                return db.name, db, alias
            # A DBRef database name may not be resolvable through a registered
            # alias.  Try the model's connection before giving up.
            name, model_alias = self._model_database(document_type)
            model_db, resolved_alias = _get_db_for_database(name)
            if model_db is not None and model_db.name == database:
                return name, model_db, model_alias or resolved_alias
            return database, None, None

        name, alias = self._model_database(document_type)
        db, resolved_alias = _get_db_for_database(name)
        if db is None:
            return name, None, alias
        return name, db, resolved_alias

    @staticmethod
    def _field_document_type(field):
        inner = getattr(field, "field", None)
        if inner is not None:
            field = inner
        return getattr(field, "document_type", None)

    @staticmethod
    def _class_from_collection(collection):
        try:
            return _DocumentRegistry.get(
                "".join(x.capitalize() for x in collection.split("_"))
            )
        except NotRegistered:
            return None

    # ------------------------------------------------------------------
    # Finding references
    # ------------------------------------------------------------------
    def _find_references(
        self,
        items,
        depth=0,
        expected_type=None,
        database=None,
        level=1,
    ):
        """
        Recursively finds all db references to be dereferenced

        :param items: The iterable (dict, list, queryset)
        :param depth: The current depth of recursion
        :param expected_type: The document class the reference is expected to
            resolve to, when it is known.
        :param database: The database used for references without a database of
            their own.
        :param level: Eager dereferencing level (used to bound how far nested
            references are followed).
        """
        if not items or depth >= self.max_depth:
            return

        # Determine the iterator to use
        if isinstance(items, dict):
            iterator = items.values()
        else:
            iterator = items

        # Recursively find dbreferences
        depth += 1
        for item in iterator:
            if isinstance(item, (UnresolvedReference, LazyReference)):
                # LazyReference inherits DBRef but must not be dereferenced
                # here; UnresolvedReference is already resolved (as failed).
                continue
            elif isinstance(item, (Document, EmbeddedDocument)):
                item_database = database
                if getattr(item, "_db_context_for_nested", False):
                    context = getattr(item, "_db_context", None) or {}
                    item_database = context.get("database") or database

                for field_name, field in item._fields.items():
                    v = item._data.get(field_name, None)
                    if isinstance(v, (LazyReference, UnresolvedReference)):
                        continue
                    elif isinstance(
                        field, (LazyReferenceField, GenericLazyReferenceField)
                    ):
                        # Lazy references must remain lazy even if an eager
                        # reference to the same address was already fetched.
                        continue
                    elif isinstance(v, DBRef):
                        ref_type = expected_type
                        if hasattr(v, "cls"):
                            try:
                                ref_type = _DocumentRegistry.get(v.cls)
                            except NotRegistered:
                                ref_type = expected_type
                        field_type = self._field_document_type(field) or ref_type
                        self._add_reference(
                            v,
                            field_type,
                            item_database,
                            source=item,
                            level=level,
                        )
                    elif isinstance(v, dict) and "_ref" in v:
                        doc_cls = _DocumentRegistry.get(v["_cls"])
                        self._add_reference(
                            v["_ref"],
                            doc_cls,
                            item_database,
                            source=item,
                            level=level,
                        )
                    elif isinstance(v, (dict, list, tuple)) and depth <= self.max_depth:
                        inner_type = self._field_document_type(field)
                        self._find_references(
                            v,
                            depth,
                            expected_type=inner_type,
                            database=item_database,
                            level=level,
                        )
            elif isinstance(item, DBRef):
                self._add_reference(item, expected_type, database, level=level)
            elif isinstance(item, dict) and "_ref" in item:
                doc_cls = _DocumentRegistry.get(item["_cls"])
                self._add_reference(
                    item["_ref"], doc_cls, database, level=level
                )
            elif isinstance(item, (dict, list, tuple)) and depth - 1 <= self.max_depth:
                self._find_references(
                    item,
                    depth - 1,
                    expected_type=expected_type,
                    database=database,
                    level=level,
                )

    def _add_reference(
        self, dbref, document_type, database, source=None, level=1
    ):
        if isinstance(dbref, UnresolvedReference):
            return

        # A reference is "explicit" when either the DBRef itself pins a
        # database, or the caller explicitly chose one for this read.  Only
        # those should force the fetched document's nested references to use
        # the same database; otherwise the related model's own connection
        # configuration keeps applying (historical behavior).
        explicit_database = dbref.database is not None or database is not None
        reference_database = dbref.database or database
        db_name, db, alias = self._database_for_reference(
            reference_database, document_type
        )

        collection = dbref.collection
        group = (db_name, collection)
        self.reference_map.setdefault(group, set()).add(dbref.id)
        previous = self.database_info.get(group)
        if previous is None:
            self.database_info[group] = (db, alias, explicit_database)
        else:
            # If any reference in the batch was reached through an explicit
            # database (either its own DBRef or the report's database), the
            # fetched documents must propagate that database to their nested
            # references.
            self.database_info[group] = (
                db,
                alias,
                previous[2] or explicit_database,
            )

        previous_level = self.reference_levels.get(group)
        if previous_level is None or level < previous_level:
            self.reference_levels[group] = level

        if document_type is not None:
            self.reference_types.setdefault(group, set()).add(document_type)

    # ------------------------------------------------------------------
    # Fetching references
    # ------------------------------------------------------------------
    @staticmethod
    def _most_general_type(types):
        types = [t for t in types if t is not None]
        if not types:
            return None
        for candidate in types:
            try:
                if all(issubclass(t, candidate) for t in types):
                    return candidate
            except TypeError:
                continue
        return types[0]

    def _fetch_objects(self):
        """Fetch all references and convert to their document objects.

        References are batched by ``(database, collection)``; duplicate ids
        are only fetched once.  Documents fetched at a depth lower than
        ``max_depth`` are scanned for further eager references so those are
        batched as well instead of triggering a query per access.
        """
        object_map = {}
        attempted = set()

        while True:
            work = []
            for (db_name, collection), dbrefs in self.reference_map.items():
                new_ids = [
                    dbref
                    for dbref in dbrefs
                    if (db_name, collection, dbref) not in attempted
                ]
                if new_ids:
                    work.append(((db_name, collection), new_ids))

            if not work:
                break

            for (db_name, collection), new_ids in work:
                db, alias, explicit_database = self.database_info.get(
                    (db_name, collection), (None, None, False)
                )
                attempted.update(
                    (db_name, collection, dbref) for dbref in new_ids
                )
                db = self.database_info.get((db_name, collection), (None,))[0]
                if db is None:
                    # The database could not be resolved.  Every reference in
                    # the batch is materialized as an unresolved reference
                    # carrying its database, collection, id and reason.
                    self.unresolvable_groups.add((db_name, collection))
                    continue

                expected_type = self._most_general_type(
                    self.reference_types.get((db_name, collection), ())
                )

                fetched = []
                references = db[collection].find(
                    {"_id": {"$in": new_ids}}, session=_get_session()
                )
                for ref in references:
                    doc_cls = self._document_class_for_son(
                        ref, collection, expected_type
                    )
                    if doc_cls is None:
                        continue
                    doc = doc_cls._from_son(ref)
                    doc._db_context = {"database": db.name, "alias": alias}
                    if explicit_database:
                        doc._db_context_for_nested = True
                    object_map[(db_name, collection, doc.id)] = doc
                    fetched.append(doc)

                level = self.reference_levels.get((db_name, collection), 1)
                if fetched and level < self.max_depth:
                    # Only explicit reads propagate the region database to
                    # documents without a database of their own; ordinary
                    # reads keep using each model's configured connection.
                    nested_database = db.name if explicit_database else None
                    self._find_references(
                        fetched,
                        0,
                        expected_type=None,
                        database=nested_database,
                        level=level + 1,
                    )

        return object_map

    def _document_class_for_son(self, son, collection, expected_type):
        if "_cls" in son:
            return _DocumentRegistry.get(son["_cls"])

        if expected_type is not None:
            return expected_type

        return self._class_from_collection(collection)

    # ------------------------------------------------------------------
    # Attaching fetched documents
    # ------------------------------------------------------------------
    @staticmethod
    def _is_type_compatible(document, expected_type):
        if expected_type is None:
            return True
        try:
            return isinstance(document, expected_type)
        except TypeError:
            return True

    def _unresolved(self, dbref, database, reason, expected_type=None):
        return UnresolvedReference(
            dbref.collection,
            dbref.id,
            database,
            reason=reason,
            expected_type=expected_type,
        )

    def _resolve_reference(self, dbref, expected_type, database):
        if isinstance(dbref, UnresolvedReference):
            return dbref

        reference_database = dbref.database or database
        db_name, db, _alias = self._database_for_reference(
            reference_database, expected_type
        )
        document = self.object_map.get(
            (db_name, dbref.collection, dbref.id)
        )

        explicit = self.explicit_database or dbref.database is not None

        if (db_name, dbref.collection) in self.unresolvable_groups:
            return self._unresolved(
                dbref, db_name, UNRESOLVED_DATABASE_REFERENCE, expected_type
            )

        if document is None:
            if not explicit:
                # Preserve historical behavior: leave the DBRef in place and
                # let a later access raise if it is truly dangling.
                return dbref
            return self._unresolved(
                dbref, db_name, MISSING_REFERENCE, expected_type
            )

        if not self._is_type_compatible(document, expected_type):
            if not explicit:
                # Historical select_related did not type-check references.
                return document
            return self._unresolved(
                dbref, db_name, TYPE_MISMATCH_REFERENCE, expected_type
            )

        return document

    def _prepare_lazy_reference(self, value, database, document_type=None):
        if isinstance(value, UnresolvedReference):
            return value

        if isinstance(value, LazyReference):
            if value.database is None and database is not None:
                return LazyReference(
                    value.document_type,
                    value.pk,
                    cached_doc=value._cached_doc,
                    passthrough=value.passthrough,
                    database=database,
                )
            return value

        if isinstance(value, DBRef):
            reference_database = value.database or database
            if document_type is None:
                document_type = getattr(value, "document_type", None)
            if document_type is None:
                document_type = self._class_from_collection(value.collection)
            return LazyReference(
                document_type,
                value.id,
                passthrough=False,
                database=reference_database,
            )

        return value

    def _resolve_generic_dict(self, value, database, expected_type=None):
        if isinstance(value, UnresolvedReference):
            return value
        if not (isinstance(value, dict) and "_ref" in value):
            return value

        dbref = value["_ref"]
        doc_cls = _DocumentRegistry.get(value["_cls"])
        resolved = self._resolve_reference(
            dbref, doc_cls or expected_type, database
        )
        if isinstance(resolved, DBRef) and not isinstance(
            resolved, (Document, UnresolvedReference)
        ):
            # Legacy references that could not be eagerly resolved must keep
            # their generic-reference representation so a later access can
            # dereference (or raise) as usual.
            return value
        return resolved

    def _attach_objects(
        self,
        items,
        depth=0,
        instance=None,
        name=None,
        expected_type=None,
        database=None,
        use_instance_context=False,
    ):
        """
        Recursively finds all db references to be dereferenced

        :param items: The iterable (dict, list, queryset)
        :param depth: The current depth of recursion
        :param instance: The owning instance used for tracking changes by
            :class:`~mongoengine.base.ComplexBaseField`
        :param name: The name of the field, used for tracking changes by
            :class:`~mongoengine.base.ComplexBaseField`
        :param use_instance_context: Whether a document's own database context
            should override the inherited database (used for documents fetched
            from another database).
        """
        if not items:
            if isinstance(items, (BaseDict, BaseList)):
                return items

            if instance:
                if isinstance(items, dict):
                    return BaseDict(items, instance, name)
                else:
                    return BaseList(items, instance, name)

        if isinstance(items, UnresolvedReference):
            return items

        if isinstance(items, LazyReference):
            return self._prepare_lazy_reference(items, database)

        if isinstance(items, dict):
            if "_ref" in items:
                return self._resolve_generic_dict(items, database, expected_type)
            elif "_cls" in items:
                doc = _DocumentRegistry.get(items["_cls"])._from_son(items)
                _cls = doc._data.pop("_cls", None)
                del items["_cls"]
                doc._data = self._attach_objects(
                    doc._data,
                    depth,
                    doc,
                    None,
                    expected_type=expected_type,
                    database=database,
                    use_instance_context=use_instance_context,
                )
                if _cls is not None:
                    doc._data["_cls"] = _cls
                return doc

        if not hasattr(items, "items"):
            is_list = True
            list_type = BaseList
            if isinstance(items, EmbeddedDocumentList):
                list_type = EmbeddedDocumentList
            as_tuple = isinstance(items, tuple)
            iterator = enumerate(items)
            data = []
        else:
            is_list = False
            iterator = items.items()
            data = {}

        depth += 1
        for k, v in iterator:
            if is_list:
                data.append(v)
            else:
                data[k] = v

            if isinstance(v, UnresolvedReference):
                continue
            elif isinstance(v, LazyReference):
                data[k] = self._prepare_lazy_reference(v, database)
            elif isinstance(v, DBRef):
                v = self._resolve_reference(v, expected_type, database)
                data[k] = v

            if isinstance(v, (Document, EmbeddedDocument)) and not isinstance(
                v, UnresolvedReference
            ):
                self._attach_document_fields(
                    v,
                    depth,
                    database,
                    instance,
                    name,
                    k,
                    use_instance_context=use_instance_context,
                )
            elif (
                isinstance(v, (dict, list, tuple))
                and not isinstance(data[k], (Document, EmbeddedDocument))
                and depth <= self.max_depth
            ):
                item_name = f"{name}.{k}" if name else name
                data[k] = self._attach_objects(
                    v,
                    depth - 1,
                    instance=instance,
                    name=item_name,
                    expected_type=expected_type,
                    database=database,
                    use_instance_context=use_instance_context,
                )

        if instance and name:
            if is_list:
                wrapped = tuple(data) if as_tuple else list_type(data, instance, name)
            else:
                wrapped = BaseDict(data, instance, name)
            if hasattr(wrapped, "_dereferenced"):
                wrapped._dereferenced = True
            return wrapped
        depth += 1
        return data

    def _attach_document_fields(
        self,
        document,
        depth,
        database,
        instance,
        name,
        k,
        use_instance_context=False,
    ):
        """Resolve the direct reference fields of a fetched document.

        Fetched documents are resolved from the batched object map, so this
        only needs to attach the already-fetched instances.  A guard prevents
        infinite recursion when references form a cycle.
        """
        marker = id(document)
        if marker in self._attached_documents:
            return document
        self._attached_documents.add(marker)

        node_database = database
        if isinstance(document, Document):
            context = getattr(document, "_db_context", None) or {}
            if context.get("database") and (
                use_instance_context
                or getattr(document, "_db_context_for_nested", False)
            ):
                node_database = context["database"]

        for field_name, field in document._fields.items():
            field_value = document._data.get(field_name, None)
            if isinstance(field_value, UnresolvedReference):
                continue
            elif isinstance(field_value, LazyReference) or isinstance(
                field, (LazyReferenceField, GenericLazyReferenceField)
            ):
                document._data[field_name] = self._prepare_lazy_reference(
                    field_value,
                    node_database,
                    document_type=self._field_document_type(field),
                )
            elif isinstance(field_value, DBRef):
                field_type = self._field_document_type(field)
                resolved = self._resolve_reference(
                    field_value, field_type, node_database
                )
                document._data[field_name] = resolved
                if isinstance(resolved, (Document, EmbeddedDocument)) and not (
                    isinstance(resolved, UnresolvedReference)
                ):
                    self._attach_document_fields(
                        resolved,
                        depth + 1,
                        node_database,
                        instance,
                        name,
                        k,
                        use_instance_context=True,
                    )
            elif isinstance(field_value, dict) and "_ref" in field_value:
                document._data[field_name] = self._resolve_generic_dict(
                    field_value, node_database
                )
            elif (
                isinstance(field_value, (dict, list, tuple))
                and depth <= self.max_depth
            ):
                inner_type = self._field_document_type(field)
                item_name = f"{name}.{k}.{field_name}"
                document._data[field_name] = self._attach_objects(
                    field_value,
                    depth,
                    instance=instance,
                    name=item_name,
                    expected_type=inner_type,
                    database=node_database,
                    use_instance_context=True,
                )
        return document
