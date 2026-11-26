import weakref

from bson import DBRef

from mongoengine.common import _import_class
from mongoengine.errors import DoesNotExist, MultipleObjectsReturned

__all__ = (
    "BaseDict",
    "StrictDict",
    "BaseList",
    "EmbeddedDocumentList",
    "LazyReference",
)


def _make_owner_ref(owner):
    """Return a weak reference to ``owner`` when possible."""
    if isinstance(owner, weakref.ProxyTypes):
        return owner
    try:
        return weakref.ref(owner)
    except TypeError:
        return owner


def _owner_links(holder, create=False):
    try:
        links = getattr(holder, "_owner_links", None)
    except ReferenceError:
        return None
    if links is None and create:
        links = {}
        try:
            holder._owner_links = links
        except (AttributeError, ReferenceError):
            return None
    return links


def _add_owner(child, owner):
    """Record that ``child`` is currently contained by ``owner``."""
    if child is None or owner is None:
        return

    links = _owner_links(child, create=True)
    if links is not None:
        links[id(owner)] = _make_owner_ref(owner)


def _remove_owner(child, owner):
    links = _owner_links(child)
    if links is not None and owner is not None:
        links.pop(id(owner), None)


def _iter_owners(child):
    """Yield the live objects that currently contain ``child``."""
    try:
        links = _owner_links(child)
    except ReferenceError:
        return
    if not links:
        return

    dead = []
    for owner_id, owner_ref in list(links.items()):
        try:
            owner = (
                owner_ref()
                if isinstance(owner_ref, weakref.ReferenceType)
                else owner_ref
            )
        except ReferenceError:
            owner = None

        if owner is None:
            dead.append(owner_id)
        else:
            yield owner

    for owner_id in dead:
        links.pop(owner_id, None)


def _child_positions(owner, child):
    """Return every list index or dict key where ``owner`` contains ``child``."""
    positions = []
    if isinstance(owner, BaseList):
        for index, value in enumerate(list.__iter__(owner)):
            if value is child:
                positions.append(index)
    elif isinstance(owner, BaseDict):
        for key, value in dict.items(owner):
            if value is child:
                positions.append(key)
    return positions


def _locate_identities(value, child, path, ancestors, matches):
    """Collect every path from ``value`` down to ``child``."""
    if value is child:
        matches.append(path)
        return

    object_id = id(value)
    if object_id in ancestors:
        return
    ancestors.add(object_id)
    try:
        if isinstance(value, BaseList):
            for index, item in enumerate(list.__iter__(value)):
                _locate_identities(item, child, [*path, str(index)], ancestors, matches)
        elif isinstance(value, BaseDict):
            for key, item in dict.items(value):
                _locate_identities(item, child, [*path, str(key)], ancestors, matches)
        elif not getattr(value, "_is_document", False):
            data = getattr(value, "_data", None)
            if data is not None:
                db_field_map = getattr(value, "_db_field_map", {})
                for field_name, item in dict.items(data):
                    path_part = db_field_map.get(field_name, field_name)
                    _locate_identities(
                        item, child, [*path, path_part], ancestors, matches
                    )
    finally:
        ancestors.discard(object_id)


def _paths_within(owner, child):
    """Return every dotted path from ``owner``'s fields down to ``child``."""
    data = getattr(owner, "_data", None)
    if data is None:
        return []

    matches = []
    for field_name, value in dict.items(data):
        _locate_identities(value, child, [field_name], {id(owner)}, matches)
    return [".".join(match) for match in matches]


def _first_field_name(owner, child):
    """Return the document field that directly holds ``child``."""
    data = getattr(owner, "_data", None)
    if data is None:
        return None

    for field_name, value in dict.items(data):
        if value is child:
            return field_name
    return None


def _resolve_relative_key(child, key):
    """Translate a path relative to ``child`` into database field names."""
    if key is None:
        return None

    resolved = []
    current = child
    for part in str(key).split("."):
        data = getattr(current, "_data", None)
        if data is not None:
            db_field_map = getattr(current, "_db_field_map", {})
            resolved.append(db_field_map.get(part, part))
            current = data.get(part)
        elif isinstance(current, (BaseList, list)) and part.isdigit():
            resolved.append(part)
            index = int(part)
            current = current[index] if index < len(current) else None
        elif isinstance(current, (BaseDict, dict)) and part in current:
            resolved.append(part)
            current = dict.__getitem__(current, part)
        else:
            resolved.append(part)
            current = None

    return ".".join(resolved)


def _owner_path(owner, child, key):
    """Resolve absolute paths for a change to ``child`` inside ``owner``."""
    relative_key = _resolve_relative_key(child, key)
    paths = _paths_within(owner, child)
    if not paths:
        return None
    return [_join_path(path, relative_key) for path in paths]


def _join_path(base, key):
    base = "" if base is None else str(base)
    key = "" if key is None else str(key)
    if not base:
        return key
    if not key:
        return base
    return f"{base}.{key}"


def _record_change(owner, path, unset):
    """Record a path on ``owner`` without triggering another propagation."""
    try:
        note_name = "_note_unset_path" if unset else "_note_changed_path"
        note = getattr(owner, note_name, None)
        if note is not None:
            note(path)
            return

        method_name = "_mark_as_unset" if unset else "_mark_as_changed"
        method = getattr(owner, method_name, None)
        if method is not None:
            method(path)
    except ReferenceError:
        pass


def _propagate_change(holder, key=None, unset=False):
    """Propagate a change to every document currently containing ``holder``."""
    _propagate_change_from(holder, key, unset, set())


def _propagate_change_from(holder, key, unset, seen):
    branch = (id(holder), key)
    if branch in seen:
        return
    seen.add(branch)

    EmbeddedDocument = _import_class("EmbeddedDocument")
    owners = list(_iter_owners(holder))
    if not owners:
        # Containers created without an owning document keep the historical
        # single-instance behaviour. Containers that are (or were) attached to
        # a document must not write through a stale ``_instance``.
        if isinstance(holder, (BaseDict, BaseList)) and _owner_links(holder) is None:
            _legacy_mark(holder, key, unset)
        return

    for owner in owners:
        if isinstance(owner, (BaseList, BaseDict)):
            positions = _child_positions(owner, holder)
            if not positions:
                # The value has been removed from this container.
                _remove_owner(holder, owner)
                continue
            for position in positions:
                _propagate_change_from(owner, _join_path(position, key), unset, seen)
            continue

        paths = _owner_path(owner, holder, key)
        if paths is None:
            field_name = _first_field_name(owner, holder)
            if field_name is None:
                _remove_owner(holder, owner)
                continue
            paths = [_join_path(field_name, key)]

        for path in paths:
            _record_change(owner, path, unset)
        if isinstance(owner, EmbeddedDocument):
            # Every document currently containing this embedded document keeps
            # its own absolute path too, so one document saving doesn't clear
            # the tracking of another document that shares the same value.
            for path in paths:
                _propagate_change_from(owner, path, unset, seen)


def _legacy_mark(holder, key, unset):
    """Notify the single instance historically stored on a container."""
    try:
        instance = holder._instance
    except (AttributeError, ReferenceError):
        return

    if instance is None:
        return

    if key is None:
        path = holder._name
    else:
        path = _join_path(holder._name, key)
    _record_change(instance, path, unset)


def _bind_value(container, value):
    """Register ``value`` as currently contained by ``container``."""
    EmbeddedDocument = _import_class("EmbeddedDocument")
    if isinstance(value, (BaseDict, BaseList, EmbeddedDocument)):
        _add_owner(value, container)


def _bind_values(container, values):
    for value in values:
        _bind_value(container, value)


def _wrap_value(value):
    """Convert plain nested containers to their tracked equivalents."""
    if isinstance(value, dict) and not isinstance(value, BaseDict):
        return BaseDict(value, None, None)
    if isinstance(value, list) and not isinstance(value, BaseList):
        return BaseList(value, None, None)
    return value


class BaseDict(dict):
    """A special dict so we can watch any changes."""

    _dereferenced = False
    _instance = None
    _name = None

    def __init__(self, dict_items, instance, name):
        BaseDocument = _import_class("BaseDocument")

        if isinstance(instance, BaseDocument):
            self._instance = weakref.proxy(instance)
            _add_owner(self, instance)
        self._name = name
        super().__init__(dict_items)
        _bind_values(self, dict.values(self))

    def get(self, key, default=None):
        # get does not use __getitem__ by default so we must override it as well
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __getitem__(self, key):
        value = super().__getitem__(key)

        EmbeddedDocument = _import_class("EmbeddedDocument")
        if isinstance(value, EmbeddedDocument):
            if value._instance is None:
                value._instance = self._instance
            _bind_value(self, value)
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            value = BaseDict(value, None, f"{self._name}.{key}")
            super().__setitem__(key, value)
            value._instance = self._instance
            _bind_value(self, value)
        elif isinstance(value, list) and not isinstance(value, BaseList):
            value = BaseList(value, None, f"{self._name}.{key}")
            super().__setitem__(key, value)
            value._instance = self._instance
            _bind_value(self, value)
        elif isinstance(value, (BaseDict, BaseList)):
            _bind_value(self, value)
        return value

    def __setitem__(self, key, value):
        if key not in self or self[key] != value:
            self._mark_as_changed(key)

        value = _wrap_value(value)
        result = super().__setitem__(key, value)
        _bind_value(self, value)
        return result

    def __delitem__(self, key):
        result = super().__delitem__(key)
        self._mark_as_unset(key)
        return result

    def pop(self, *args, **kwargs):
        result = super().pop(*args, **kwargs)
        self._mark_as_changed()
        return result

    def clear(self):
        result = super().clear()
        self._mark_as_changed()
        return result

    def update(self, *args, **kwargs):
        result = super().update(*args, **kwargs)
        _bind_values(self, dict.values(self))
        self._mark_as_changed()
        return result

    def popitem(self):
        result = super().popitem()
        self._mark_as_changed()
        return result

    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            value = _wrap_value(default)
            result = super().__setitem__(key, value)
            _bind_value(self, value)
            self._mark_as_changed()
            return result

    def __getstate__(self):
        self.instance = None
        self._dereferenced = False
        return self

    def __setstate__(self, state):
        self = state
        return self

    def _mark_as_changed(self, key=None):
        _propagate_change(self, key)

    def _mark_as_unset(self, key):
        _propagate_change(self, key, unset=True)


class BaseList(list):
    """A special list so we can watch any changes."""

    _dereferenced = False
    _instance = None
    _name = None

    def __init__(self, list_items, instance, name):
        BaseDocument = _import_class("BaseDocument")

        if isinstance(instance, BaseDocument):
            if isinstance(instance, weakref.ProxyTypes):
                self._instance = instance
            else:
                self._instance = weakref.proxy(instance)
            _add_owner(self, instance)

        self._name = name
        super().__init__(list_items)
        _bind_values(self, list.__iter__(self))

    def __getitem__(self, key):
        # change index to positive value because MongoDB does not support negative one
        if isinstance(key, int) and key < 0:
            key = len(self) + key
        value = super().__getitem__(key)

        if isinstance(key, slice):
            # When receiving a slice operator, we don't convert the structure and bind
            # to parent's instance. This is buggy for now but would require more work to be handled properly
            return value

        EmbeddedDocument = _import_class("EmbeddedDocument")
        if isinstance(value, EmbeddedDocument):
            if value._instance is None:
                value._instance = self._instance
            _bind_value(self, value)
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            # Replace dict by BaseDict
            value = BaseDict(value, None, f"{self._name}.{key}")
            super().__setitem__(key, value)
            value._instance = self._instance
            _bind_value(self, value)
        elif isinstance(value, list) and not isinstance(value, BaseList):
            # Replace list by BaseList
            value = BaseList(value, None, f"{self._name}.{key}")
            super().__setitem__(key, value)
            value._instance = self._instance
            _bind_value(self, value)
        elif isinstance(value, (BaseDict, BaseList)):
            _bind_value(self, value)
        return value

    def __iter__(self):
        yield from super().__iter__()

    def __getstate__(self):
        self.instance = None
        self._dereferenced = False
        return self

    def __setstate__(self, state):
        self = state
        return self

    def __setitem__(self, key, value):
        changed_key = key
        if isinstance(key, slice):
            # In case of slice, we don't bother to identify the exact elements
            # being updated; instead, we simply mark the whole list as changed.
            changed_key = None
            values = [_wrap_value(v) for v in value]
            result = super().__setitem__(key, values)
            _bind_values(self, values)
        else:
            value = _wrap_value(value)
            result = super().__setitem__(key, value)
            _bind_value(self, value)
        if isinstance(changed_key, int) and changed_key < 0:
            changed_key %= len(self)
        self._mark_as_changed(changed_key)
        return result

    def append(self, value):
        value = _wrap_value(value)
        result = super().append(value)
        _bind_value(self, value)
        self._mark_as_changed()
        return result

    def extend(self, values):
        values = [_wrap_value(v) for v in values]
        result = super().extend(values)
        _bind_values(self, values)
        self._mark_as_changed()
        return result

    def insert(self, index, value):
        value = _wrap_value(value)
        result = super().insert(index, value)
        _bind_value(self, value)
        self._mark_as_changed()
        return result

    def __iadd__(self, values):
        values = [_wrap_value(v) for v in values]
        result = super().__iadd__(values)
        _bind_values(self, values)
        self._mark_as_changed()
        return result

    def __imul__(self, count):
        result = super().__imul__(count)
        _bind_values(self, list.__iter__(self))
        self._mark_as_changed()
        return result

    def pop(self, *args, **kwargs):
        result = super().pop(*args, **kwargs)
        self._mark_as_changed()
        return result

    def remove(self, value):
        result = super().remove(value)
        self._mark_as_changed()
        return result

    def reverse(self):
        result = super().reverse()
        self._mark_as_changed()
        return result

    def sort(self, *args, **kwargs):
        result = super().sort(*args, **kwargs)
        self._mark_as_changed()
        return result

    def clear(self):
        result = super().clear()
        self._mark_as_changed()
        return result

    def __delitem__(self, key):
        result = super().__delitem__(key)
        self._mark_as_changed()
        return result

    def _mark_as_changed(self, key=None):
        _propagate_change(self, key)

    def _mark_as_unset(self, key):
        _propagate_change(self, key, unset=True)


class EmbeddedDocumentList(BaseList):
    @classmethod
    def __match_all(cls, embedded_doc, kwargs):
        """Return True if a given embedded doc matches all the filter
        kwargs. If it doesn't return False.
        """
        for key, expected_value in kwargs.items():
            doc_val = getattr(embedded_doc, key)
            if doc_val != expected_value and str(doc_val) != expected_value:
                return False
        return True

    @classmethod
    def __only_matches(cls, embedded_docs, kwargs):
        """Return embedded docs that match the filter kwargs."""
        if not kwargs:
            return embedded_docs
        return [doc for doc in embedded_docs if cls.__match_all(doc, kwargs)]

    def filter(self, **kwargs):
        """
        Filters the list by only including embedded documents with the
        given keyword arguments.

        This method only supports simple comparison (e.g. .filter(name='John Doe'))
        and does not support operators like __gte, __lte, __icontains like queryset.filter does

        :param kwargs: The keyword arguments corresponding to the fields to
         filter on. *Multiple arguments are treated as if they are ANDed
         together.*
        :return: A new ``EmbeddedDocumentList`` containing the matching
         embedded documents.

        Raises ``AttributeError`` if a given keyword is not a valid field for
        the embedded document class.
        """
        values = self.__only_matches(self, kwargs)
        return EmbeddedDocumentList(values, self._instance, self._name)

    def exclude(self, **kwargs):
        """
        Filters the list by excluding embedded documents with the given
        keyword arguments.

        :param kwargs: The keyword arguments corresponding to the fields to
         exclude on. *Multiple arguments are treated as if they are ANDed
         together.*
        :return: A new ``EmbeddedDocumentList`` containing the non-matching
         embedded documents.

        Raises ``AttributeError`` if a given keyword is not a valid field for
        the embedded document class.
        """
        exclude = self.__only_matches(self, kwargs)
        values = [item for item in self if item not in exclude]
        return EmbeddedDocumentList(values, self._instance, self._name)

    def count(self):
        """
        The number of embedded documents in the list.

        :return: The length of the list, equivalent to the result of ``len()``.
        """
        return len(self)

    def get(self, **kwargs):
        """
        Retrieves an embedded document determined by the given keyword
        arguments.

        :param kwargs: The keyword arguments corresponding to the fields to
         search on. *Multiple arguments are treated as if they are ANDed
         together.*
        :return: The embedded document matched by the given keyword arguments.

        Raises ``DoesNotExist`` if the arguments used to query an embedded
        document returns no results. ``MultipleObjectsReturned`` if more
        than one result is returned.
        """
        values = self.__only_matches(self, kwargs)
        if len(values) == 0:
            raise DoesNotExist("%s matching query does not exist." % self._name)
        elif len(values) > 1:
            raise MultipleObjectsReturned(
                "%d items returned, instead of 1" % len(values)
            )

        return values[0]

    def first(self):
        """Return the first embedded document in the list, or ``None``
        if empty.
        """
        if len(self) > 0:
            return self[0]

    def create(self, **values):
        """
        Creates a new instance of the EmbeddedDocument and appends it to this EmbeddedDocumentList.

        .. note::
            the instance of the EmbeddedDocument is not automatically saved to the database.
            You still need to call .save() on the parent Document.

        :param values: A dictionary of values for the embedded document.
        :return: The new embedded document instance.
        """
        name = self._name
        EmbeddedClass = self._instance._fields[name].field.document_type_obj
        self._instance[self._name].append(EmbeddedClass(**values))

        return self._instance[self._name][-1]

    def save(self, *args, **kwargs):
        """
        Saves the ancestor document.

        :param args: Arguments passed up to the ancestor Document's save
         method.
        :param kwargs: Keyword arguments passed up to the ancestor Document's
         save method.
        """
        self._instance.save(*args, **kwargs)

    def delete(self):
        """
        Deletes the embedded documents from the database.

        .. note::
            The embedded document changes are not automatically saved
            to the database after calling this method.

        :return: The number of entries deleted.
        """
        values = list(self)
        for item in values:
            self._instance[self._name].remove(item)

        return len(values)

    def update(self, **update):
        """
        Updates the embedded documents with the given replacement values. This
        function does not support mongoDB update operators such as ``inc__``.

        .. note::
            The embedded document changes are not automatically saved
            to the database after calling this method.

        :param update: A dictionary of update values to apply to each
         embedded document.
        :return: The number of entries updated.
        """
        if len(update) == 0:
            return 0
        values = list(self)
        for item in values:
            for k, v in update.items():
                setattr(item, k, v)

        return len(values)


class StrictDict:
    __slots__ = ()
    _special_fields = {"get", "pop", "iteritems", "items", "keys", "create"}
    _classes = {}

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getitem__(self, key):
        key = "_reserved_" + key if key in self._special_fields else key
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        key = "_reserved_" + key if key in self._special_fields else key
        return setattr(self, key, value)

    def __contains__(self, key):
        return hasattr(self, key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key, default=None):
        v = self.get(key, default)
        try:
            delattr(self, key)
        except AttributeError:
            pass
        return v

    def iteritems(self):
        for key in self:
            yield key, self[key]

    def items(self):
        return [(k, self[k]) for k in iter(self)]

    def iterkeys(self):
        return iter(self)

    def keys(self):
        return list(iter(self))

    def __iter__(self):
        return (key for key in self.__slots__ if hasattr(self, key))

    def __len__(self):
        return len(list(self.items()))

    def __eq__(self, other):
        return list(self.items()) == list(other.items())

    def __ne__(self, other):
        return not (self == other)

    @classmethod
    def create(cls, allowed_keys):
        allowed_keys_tuple = tuple(
            ("_reserved_" + k if k in cls._special_fields else k) for k in allowed_keys
        )
        allowed_keys = frozenset(allowed_keys_tuple)
        if allowed_keys not in cls._classes:

            class SpecificStrictDict(cls):
                __slots__ = allowed_keys_tuple

                def __repr__(self):
                    return "{%s}" % ", ".join(
                        f'"{k!s}": {v!r}' for k, v in self.items()
                    )

            cls._classes[allowed_keys] = SpecificStrictDict
        return cls._classes[allowed_keys]


class LazyReference(DBRef):
    __slots__ = ("_cached_doc", "passthrough", "document_type")

    def fetch(self, force=False):
        if not self._cached_doc or force:
            self._cached_doc = self.document_type.objects.get(pk=self.pk)
            if not self._cached_doc:
                raise DoesNotExist("Trying to dereference unknown document %s" % (self))
        return self._cached_doc

    @property
    def pk(self):
        return self.id

    def __init__(self, document_type, pk, cached_doc=None, passthrough=False):
        self.document_type = document_type
        self._cached_doc = cached_doc
        self.passthrough = passthrough
        super().__init__(self.document_type._get_collection_name(), pk)

    def __getitem__(self, name):
        if not self.passthrough:
            raise KeyError()
        document = self.fetch()
        return document[name]

    def __getattr__(self, name):
        if not object.__getattribute__(self, "passthrough"):
            raise AttributeError()
        document = self.fetch()
        try:
            return document[name]
        except KeyError:
            raise AttributeError()

    def __repr__(self):
        return f"<LazyReference({self.document_type}, {self.pk!r})>"
