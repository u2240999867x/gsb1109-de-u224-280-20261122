# SPDX-License-Identifier: MIT

"""
Dependency-aware cached properties for attrs slots classes.

``dependent_cached_property`` works like :func:`functools.cached_property`
but declares the attrs fields (or other cached properties) it depends on.
``versioned`` is a class decorator to be applied after ``@define``: it wraps
the attrs-generated ``__setattr__`` so that every *successful* assignment
invalidates exactly the cached properties that transitively depend on the
assigned field.

Invalidation is generation-based: a computation snapshots the generation of
its property before running and only stores its result if the generation is
unchanged afterwards.  A stale in-flight computation can therefore never
reinstall an expired cache entry, and rejected assignments (converter or
validator failures) neither bump generations nor drop caches.

Per-instance state lives in a weakly-held registry keyed by ``id(instance)``,
so the slot layout of the attrs classes is left untouched.
"""

import functools
import threading
import weakref


__all__ = ["dependent_cached_property", "versioned"]


class _InstanceCacheState:
    """Per-instance cache entries and invalidation generations."""

    __slots__ = ("entries", "generations", "lock")

    def __init__(self):
        self.lock = threading.RLock()
        self.generations = {}  # property name -> invalidation generation
        self.entries = {}  # property name -> (value, generation)


_states = {}
_states_lock = threading.Lock()


def _drop_state(key):
    with _states_lock:
        _states.pop(key, None)


def _state_for(instance):
    key = id(instance)
    with _states_lock:
        state = _states.get(key)
        if state is None:
            state = _InstanceCacheState()
            _states[key] = state
            weakref.finalize(instance, _drop_state, key)
    return state


class dependent_cached_property:
    """
    A cached property whose cache is invalidated when one of its declared
    dependencies is successfully assigned a new value.

    Dependencies name attrs fields or other ``dependent_cached_property``
    attributes; invalidation propagates transitively.
    """

    def __init__(self, func=None, *, depends_on=()):
        self.func = func
        self.depends_on = tuple(depends_on)
        self.name = None
        self.__doc__ = getattr(func, "__doc__", None)

    def __call__(self, func):
        # Used as @dependent_cached_property(depends_on=(...)).
        return type(self)(func, depends_on=self.depends_on)

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        state = _state_for(instance)
        name = self.name
        with state.lock:
            entry = state.entries.get(name)
            if entry is not None and entry[1] == state.generations.get(
                name, 0
            ):
                return entry[0]
            generation = state.generations.get(name, 0)
        # Compute outside the lock so nested cached properties and slow
        # computations don't block writers; the generation check below
        # decides whether the result may be cached.
        value = self.func(instance)
        with state.lock:
            if state.generations.get(name, 0) == generation:
                state.entries[name] = (value, generation)
        return value


def versioned(cls):
    """
    Class decorator for attrs classes using ``dependent_cached_property``.

    Wraps the attrs-generated ``__setattr__`` so that a successful field
    assignment invalidates all cached properties that transitively depend on
    that field.  Failed assignments (converter/validator errors) raise before
    any invalidation happens.
    """
    props = {}
    for klass in cls.__mro__:
        for name, member in vars(klass).items():
            if isinstance(member, dependent_cached_property):
                props.setdefault(name, member)

    def transitive_fields(prop_name, seen):
        for dep in props[prop_name].depends_on:
            if dep in props:
                if dep not in seen:
                    seen.add(dep)
                    yield from transitive_fields(dep, seen)
            else:
                yield dep

    invalidation_map = {}
    for prop_name in props:
        for field_name in transitive_fields(prop_name, {prop_name}):
            invalidation_map.setdefault(field_name, set()).add(prop_name)

    original_setattr = cls.__setattr__

    @functools.wraps(original_setattr)
    def __setattr__(self, name, value):
        # Runs attrs' converters/validators; raises on rejection, in which
        # case no generation is bumped and no cache is dropped.
        original_setattr(self, name, value)
        affected = invalidation_map.get(name)
        if affected:
            state = _state_for(self)
            with state.lock:
                for prop_name in affected:
                    state.generations[prop_name] = (
                        state.generations.get(prop_name, 0) + 1
                    )
                    state.entries.pop(prop_name, None)

    cls.__setattr__ = __setattr__
    return cls
