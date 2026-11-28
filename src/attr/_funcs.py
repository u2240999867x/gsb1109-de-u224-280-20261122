# SPDX-License-Identifier: MIT


from ._compat import _get_annotations, get_generic_base
from ._make import _OBJ_SETATTR, NOTHING, _make_attr_tuple_class, fields
from .exceptions import AttrsAttributeNotFoundError


_ATOMIC_TYPES = frozenset(
    {
        type(None),
        bool,
        int,
        float,
        str,
        complex,
        bytes,
        type(...),
        type,
        range,
        property,
    }
)


def asdict(
    inst,
    recurse=True,
    filter=None,
    dict_factory=dict,
    retain_collection_types=False,
    value_serializer=None,
):
    """
    Return the *attrs* attribute values of *inst* as a dict.

    Optionally recurse into other *attrs*-decorated classes.

    Args:
        inst: Instance of an *attrs*-decorated class.

        recurse (bool): Recurse into classes that are also *attrs*-decorated.

        filter (~typing.Callable):
            A callable whose return code determines whether an attribute or
            element is included (`True`) or dropped (`False`).  Is called with
            the `attrs.Attribute` as the first argument and the value as the
            second argument.

        dict_factory (~typing.Callable):
            A callable to produce dictionaries from.  For example, to produce
            ordered dictionaries instead of normal Python dictionaries, pass in
            ``collections.OrderedDict``.

        retain_collection_types (bool):
            Do not convert to `list` when encountering an attribute whose type
            is `tuple` or `set`.  Only meaningful if *recurse* is `True`.

        value_serializer (typing.Callable | None):
            A hook that is called for every attribute or dict key/value.  It
            receives the current instance, field and value and must return the
            (updated) value.  The hook is run *after* the optional *filter* has
            been applied.

    Returns:
        Return type of *dict_factory*.

    Raises:
        attrs.exceptions.NotAnAttrsClassError:
            If *cls* is not an *attrs* class.

    ..  versionadded:: 16.0.0 *dict_factory*
    ..  versionadded:: 16.1.0 *retain_collection_types*
    ..  versionadded:: 20.3.0 *value_serializer*
    ..  versionadded:: 21.3.0
        If a dict has a collection for a key, it is serialized as a tuple.
    """
    attrs = fields(inst.__class__)
    rv = dict_factory()
    for a in attrs:
        v = getattr(inst, a.name)
        if filter is not None and not filter(a, v):
            continue

        if value_serializer is not None:
            v = value_serializer(inst, a, v)

        if recurse is True:
            value_type = type(v)
            if value_type in _ATOMIC_TYPES:
                rv[a.name] = v
            elif has(value_type):
                rv[a.name] = asdict(
                    v,
                    recurse=True,
                    filter=filter,
                    dict_factory=dict_factory,
                    retain_collection_types=retain_collection_types,
                    value_serializer=value_serializer,
                )
            elif issubclass(value_type, (tuple, list, set, frozenset)):
                cf = value_type if retain_collection_types is True else list
                items = [
                    _asdict_anything(
                        i,
                        is_key=False,
                        filter=filter,
                        dict_factory=dict_factory,
                        retain_collection_types=retain_collection_types,
                        value_serializer=value_serializer,
                    )
                    for i in v
                ]
                try:
                    rv[a.name] = cf(items)
                except TypeError:
                    if not issubclass(cf, tuple):
                        raise
                    # Workaround for TypeError: cf.__new__() missing 1 required
                    # positional argument (which appears, for a namedturle)
                    rv[a.name] = cf(*items)
            elif issubclass(value_type, dict):
                df = dict_factory
                rv[a.name] = df(
                    (
                        _asdict_anything(
                            kk,
                            is_key=True,
                            filter=filter,
                            dict_factory=df,
                            retain_collection_types=retain_collection_types,
                            value_serializer=value_serializer,
                        ),
                        _asdict_anything(
                            vv,
                            is_key=False,
                            filter=filter,
                            dict_factory=df,
                            retain_collection_types=retain_collection_types,
                            value_serializer=value_serializer,
                        ),
                    )
                    for kk, vv in v.items()
                )
            else:
                rv[a.name] = v
        else:
            rv[a.name] = v
    return rv


def _asdict_anything(
    val,
    is_key,
    filter,
    dict_factory,
    retain_collection_types,
    value_serializer,
):
    """
    ``asdict`` only works on attrs instances, this works on anything.
    """
    val_type = type(val)
    if val_type in _ATOMIC_TYPES:
        rv = val
        if value_serializer is not None:
            rv = value_serializer(None, None, rv)
    elif getattr(val_type, "__attrs_attrs__", None) is not None:
        # Attrs class.
        rv = asdict(
            val,
            recurse=True,
            filter=filter,
            dict_factory=dict_factory,
            retain_collection_types=retain_collection_types,
            value_serializer=value_serializer,
        )
    elif issubclass(val_type, (tuple, list, set, frozenset)):
        if retain_collection_types is True:
            cf = val.__class__
        elif is_key:
            cf = tuple
        else:
            cf = list

        rv = cf(
            [
                _asdict_anything(
                    i,
                    is_key=False,
                    filter=filter,
                    dict_factory=dict_factory,
                    retain_collection_types=retain_collection_types,
                    value_serializer=value_serializer,
                )
                for i in val
            ]
        )
    elif issubclass(val_type, dict):
        df = dict_factory
        rv = df(
            (
                _asdict_anything(
                    kk,
                    is_key=True,
                    filter=filter,
                    dict_factory=df,
                    retain_collection_types=retain_collection_types,
                    value_serializer=value_serializer,
                ),
                _asdict_anything(
                    vv,
                    is_key=False,
                    filter=filter,
                    dict_factory=df,
                    retain_collection_types=retain_collection_types,
                    value_serializer=value_serializer,
                ),
            )
            for kk, vv in val.items()
        )
    else:
        rv = val
        if value_serializer is not None:
            rv = value_serializer(None, None, rv)

    return rv


def astuple(
    inst,
    recurse=True,
    filter=None,
    tuple_factory=tuple,
    retain_collection_types=False,
):
    """
    Return the *attrs* attribute values of *inst* as a tuple.

    Optionally recurse into other *attrs*-decorated classes.

    Args:
        inst: Instance of an *attrs*-decorated class.

        recurse (bool):
            Recurse into classes that are also *attrs*-decorated.

        filter (~typing.Callable):
            A callable whose return code determines whether an attribute or
            element is included (`True`) or dropped (`False`).  Is called with
            the `attrs.Attribute` as the first argument and the value as the
            second argument.

        tuple_factory (~typing.Callable):
            A callable to produce tuples from. For example, to produce lists
            instead of tuples.

        retain_collection_types (bool):
            Do not convert to `list` or `dict` when encountering an attribute
            which type is `tuple`, `dict` or `set`. Only meaningful if
            *recurse* is `True`.

    Returns:
        Return type of *tuple_factory*

    Raises:
        attrs.exceptions.NotAnAttrsClassError:
            If *cls* is not an *attrs* class.

    ..  versionadded:: 16.2.0
    """
    attrs = fields(inst.__class__)
    rv = []
    retain = retain_collection_types  # Very long. :/
    for a in attrs:
        v = getattr(inst, a.name)
        if filter is not None and not filter(a, v):
            continue
        value_type = type(v)
        if recurse is True:
            if value_type in _ATOMIC_TYPES:
                rv.append(v)
            elif has(value_type):
                rv.append(
                    astuple(
                        v,
                        recurse=True,
                        filter=filter,
                        tuple_factory=tuple_factory,
                        retain_collection_types=retain,
                    )
                )
            elif issubclass(value_type, (tuple, list, set, frozenset)):
                cf = v.__class__ if retain is True else list
                items = [
                    (
                        astuple(
                            j,
                            recurse=True,
                            filter=filter,
                            tuple_factory=tuple_factory,
                            retain_collection_types=retain,
                        )
                        if type(j) not in _ATOMIC_TYPES and has(j.__class__)
                        else j
                    )
                    for j in v
                ]
                try:
                    rv.append(cf(items))
                except TypeError:
                    if not issubclass(cf, tuple):
                        raise
                    # Workaround for TypeError: cf.__new__() missing 1 required
                    # positional argument (which appears, for a namedturle)
                    rv.append(cf(*items))
            elif issubclass(value_type, dict):
                df = value_type if retain is True else dict
                rv.append(
                    df(
                        (
                            (
                                astuple(
                                    kk,
                                    tuple_factory=tuple_factory,
                                    retain_collection_types=retain,
                                )
                                if has(kk.__class__)
                                else kk
                            ),
                            (
                                astuple(
                                    vv,
                                    tuple_factory=tuple_factory,
                                    retain_collection_types=retain,
                                )
                                if has(vv.__class__)
                                else vv
                            ),
                        )
                        for kk, vv in v.items()
                    )
                )
            else:
                rv.append(v)
        else:
            rv.append(v)

    return rv if tuple_factory is list else tuple_factory(rv)


def has(cls):
    """
    Check whether *cls* is a class with *attrs* attributes.

    Args:
        cls (type): Class to introspect.

    Raises:
        TypeError: If *cls* is not a class.

    Returns:
        bool:
    """
    attrs = getattr(cls, "__attrs_attrs__", None)
    if attrs is not None:
        return True

    # No attrs, maybe it's a specialized generic (A[str])?
    generic_base = get_generic_base(cls)
    if generic_base is not None:
        generic_attrs = getattr(generic_base, "__attrs_attrs__", None)
        if generic_attrs is not None:
            # Stick it on here for speed next time.
            cls.__attrs_attrs__ = generic_attrs
        return generic_attrs is not None
    return False


def assoc(inst, **changes):
    """
    Copy *inst* and apply *changes*.

    This is different from `evolve` that applies the changes to the arguments
    that create the new instance.

    `evolve`'s behavior is preferable, but there are `edge cases`_ where it
    doesn't work. Therefore `assoc` is deprecated, but will not be removed.

    .. _`edge cases`: https://github.com/python-attrs/attrs/issues/251

    Args:
        inst: Instance of a class with *attrs* attributes.

        changes: Keyword changes in the new copy.

    Returns:
        A copy of inst with *changes* incorporated.

    Raises:
        attrs.exceptions.AttrsAttributeNotFoundError:
            If *attr_name* couldn't be found on *cls*.

        attrs.exceptions.NotAnAttrsClassError:
            If *cls* is not an *attrs* class.

    ..  deprecated:: 17.1.0
        Use `attrs.evolve` instead if you can. This function will not be
        removed du to the slightly different approach compared to
        `attrs.evolve`, though.
    """
    import copy

    new = copy.copy(inst)
    attrs = fields(inst.__class__)
    for k, v in changes.items():
        a = getattr(attrs, k, NOTHING)
        if a is NOTHING:
            msg = f"{k} is not an attrs attribute on {new.__class__}."
            raise AttrsAttributeNotFoundError(msg)
        _OBJ_SETATTR(new, k, v)
    return new


def _declaring_class(cls, field):
    """
    Return the class that actually declared *field* on *cls*, together with the
    raw annotation it declared it with (or ``None`` if it wasn't annotated).

    This is the class whose namespace must be used to resolve the annotation:
    an inherited field keeps the annotation -- and therefore the names -- of
    the class that actually declared it, not the class it is inherited into.
    A field that was (re)defined locally on *cls* belongs to *cls*, even if a
    base class happens to have an annotation of the same name.
    """
    if not field.inherited:
        return cls, _get_annotations(cls).get(field.name)

    # Walk the MRO for the class that defined the field itself (as opposed to
    # one that only inherited a copy of it).
    for base in cls.__mro__[1:]:
        own = base.__dict__.get("__attrs_attrs__")
        if own is None:
            continue
        for candidate in own:
            if candidate.name == field.name and not candidate.inherited:
                return base, _get_annotations(base).get(field.name)

    return cls, _get_annotations(cls).get(field.name)


def _is_class_namespace_map(namespaces):
    """
    Whether *namespaces* maps declaring classes to their own namespaces,
    instead of being a single namespace.

    A regular namespace like ``globals()`` is keyed by names (strings), so a
    dict whose keys are all classes and whose values are all dicts is
    unambiguously a per-declaring-class mapping.
    """
    return (
        isinstance(namespaces, dict)
        and bool(namespaces)
        and all(isinstance(key, type) for key in namespaces)
        and all(isinstance(value, dict) for value in namespaces.values())
    )


def _namespace_for(namespaces, declaring, is_map):
    """
    Pick the resolution environment that applies to *declaring*.

    *namespaces* is either a plain namespace used for every class, or a mapping
    of declaring class -> namespace.  A declaring class that's missing from
    such a mapping is resolved without any extras from it.
    """
    if not is_map:
        return namespaces

    return namespaces.get(declaring)


def _resolve_type(declaring, name, raw, globalns, localns, include_extras):
    """
    Resolve a single raw annotation *raw* declared on *declaring*.

    A caller-provided *globalns* replaces the declaring class's module
    namespace (just like it does for `typing.get_type_hints`); when it's not
    given, the module namespace is used.
    """
    import sys
    import typing

    if globalns is not None:
        resolved_globalns = dict(globalns)
    else:
        module = sys.modules.get(getattr(declaring, "__module__", None))
        resolved_globalns = dict(vars(module)) if module is not None else {}

    # Make forward references between the involved classes resolvable, but
    # never override names the caller provided explicitly.
    for base in declaring.__mro__:
        resolved_globalns.setdefault(base.__name__, base)

    # Resolve the annotation in a throwaway class so that exactly one
    # annotation is evaluated against the declaring class's environment,
    # without inheriting or mutating anything else.
    shim = type(
        f"_resolve_{name}",
        (),
        {
            "__annotations__": {name: raw},
            "__module__": getattr(declaring, "__module__", None),
        },
    )

    hints = typing.get_type_hints(
        shim,
        globalns=resolved_globalns,
        localns=localns,
        include_extras=include_extras,
    )

    return hints[name]


def _resolve_field_view(
    cls, globalns=None, localns=None, attribs=None, include_extras=True
):
    """
    Build an independent, resolved view of *cls*'s fields.

    Every field is resolved against the namespace of the class that actually
    declared it -- not against *cls* -- so that inherited fields keep the
    meaning that their name had at their declaration site.
    """
    import copy

    fields_for = list(fields(cls) if attribs is None else attribs)

    owners = [_declaring_class(cls, field) for field in fields_for]

    globalns_is_map = _is_class_namespace_map(globalns)
    localns_is_map = _is_class_namespace_map(localns)

    resolved = []
    for field, (declaring, raw) in zip(fields_for, owners, strict=True):
        # Work on a copy so that neither the class's real ``Attribute`` nor any
        # previously returned view is affected -- even for fields we don't have
        # to resolve.
        new_field = copy.copy(field)

        if declaring is not None and raw is not None:
            new_type = _resolve_type(
                declaring,
                field.name,
                raw,
                _namespace_for(globalns, declaring, globalns_is_map),
                _namespace_for(localns, declaring, localns_is_map),
                include_extras,
            )
            _OBJ_SETATTR(new_field, "type", new_type)

        resolved.append(new_field)

    AttrsClass = _make_attr_tuple_class(
        cls.__name__, [f.name for f in resolved]
    )
    return AttrsClass(resolved)


def resolve_types(
    cls,
    globalns=None,
    localns=None,
    attribs=None,
    include_extras=True,
    in_place=True,
):
    """
    Resolve any strings and forward annotations in type annotations.

    This is only required if you need concrete types in :class:`Attribute`'s
    *type* field. In other words, you don't need to resolve your types if you
    only use them for static type checking.

    With no arguments, names will be looked up in the module in which the class
    was created. If this is not what you want, for example, if the name only
    exists inside a method, you may pass *globalns* or *localns* to specify
    other dictionaries in which to look up these names. See the docs of
    `typing.get_type_hints` for more details.

    By default, the types are resolved in place. Pass ``in_place=False`` to get
    an independent field view instead; see below.

    Args:
        cls (type): Class to resolve.

        globalns (dict | None): Dictionary containing global variables.

        localns (dict | None): Dictionary containing local variables.

        attribs (list | None):
            List of attribs for the given class. This is necessary when calling
            from inside a ``field_transformer`` since *cls* is not an *attrs*
            class yet.

        include_extras (bool):
            Resolve more accurately by passing ``include_extras=True`` to
            `typing.get_type_hints`.

        in_place (bool):
            If ``True`` (the default), resolve the types of the class's fields
            in place and return *cls*. If ``False``, leave *cls* and its fields
            untouched and return an independent, resolved field view instead.
            Like `attrs.fields`, the view allows accessing its fields by name.
            Each field in a view is resolved against the namespace of the class
            that actually declared it, so inherited fields keep their original
            meaning even if a name is rebound elsewhere.

            *globalns* and *localns* may also be mappings of declaring class to
            namespace, in which case each declaring class is resolved using its
            own entry. A declaring class that's missing from such a mapping is
            resolved without extra local names.

    Raises:
        TypeError: If *cls* is not a class.

        attrs.exceptions.NotAnAttrsClassError:
            If *cls* is not an *attrs* class and you didn't pass any attribs.

        NameError: If types cannot be resolved because of missing variables.

    Returns:
        *cls* if *in_place* is ``True`` so you can use this function also as a
        class decorator. Please note that you have to apply it **after**
        `attrs.define`. That means the decorator has to come in the line
        **before** `attrs.define`.

        A tuple of resolved `attrs.Attribute` instances if *in_place* is
        ``False``.

    ..  versionadded:: 20.1.0
    ..  versionadded:: 21.1.0 *attribs*
    ..  versionadded:: 23.1.0 *include_extras*
    ..  versionadded:: 26.2.0 *in_place*
    """
    if not in_place:
        return _resolve_field_view(
            cls,
            globalns=globalns,
            localns=localns,
            attribs=attribs,
            include_extras=include_extras,
        )

    # Since calling get_type_hints is expensive we cache whether we've
    # done it already.
    if getattr(cls, "__attrs_types_resolved__", None) != cls:
        import typing

        kwargs = {
            "globalns": globalns,
            "localns": localns,
            "include_extras": include_extras,
        }

        hints = typing.get_type_hints(cls, **kwargs)
        for field in fields(cls) if attribs is None else attribs:
            if field.name in hints:
                # Since fields have been frozen we must work around it.
                _OBJ_SETATTR(field, "type", hints[field.name])
        # We store the class we resolved so that subclasses know they haven't
        # been resolved.
        cls.__attrs_types_resolved__ = cls

    # Return the class so you can use it as a decorator too.
    return cls
