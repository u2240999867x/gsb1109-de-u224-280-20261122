# SPDX-License-Identifier: MIT

"""
Commonly used hooks for on_setattr.
"""

from . import _config
from .exceptions import FrozenAttributeError


# Sentinel for "resume with the originally deferred value".
_MISSING = object()


class DeferredAssignment(Exception):
    """
    Signals that an ``on_setattr`` hook has taken over an assignment and will
    finish it later through a cooperative assignment.

    Raise it from an ``on_setattr`` hook (or from any layer that receives the
    assignment through ``super().__setattr__``) to keep the current value
    untouched.  The nearest class that opted into cooperative assignments
    catches it, arms the :meth:`resume` callback, and returns from the
    original assignment without changing anything.  Classes that did not opt
    in keep their regular behavior, so the exception propagates like any
    other error raised by a hook.

    Args:
        deferred_value:
            The value that was about to be assigned when the assignment was
            deferred.  It is what :meth:`resume` uses unless an explicitly
            confirmed value is passed instead.

    .. versionadded:: 26.2.0
    """

    def __init__(self, deferred_value):
        self.deferred_value = deferred_value
        self._continuation = None
        super().__init__(
            f"assignment of {deferred_value!r} deferred; call "
            "DeferredAssignment.resume() to continue it."
        )

    def arm(self, continuation):
        """
        Attach the callable that continues the assignment below the layer
        that deferred it.  Called by *attrs*-generated cooperative
        ``__setattr__`` methods; user code does not need to call it.
        """
        self._continuation = continuation

    def resume(self, confirmed_value=_MISSING):
        """
        Resume the deferred assignment.

        Args:
            confirmed_value:
                Value to continue with.  If omitted, the original
                *deferred_value* is reused.
        """
        if self._continuation is None:
            msg = "Deferred assignment has not been armed by an attrs class."
            raise RuntimeError(msg)

        if confirmed_value is _MISSING:
            return self._continuation()

        return self._continuation(confirmed_value)


def pipe(*setters):
    """
    Run all *setters* and return the return value of the last one.

    .. warning::
       Generators are not allowed in ``pipe()``.

    .. versionadded:: 20.1.0
    """

    def wrapped_pipe(instance, attrib, new_value):
        rv = new_value

        for setter in setters:
            rv = setter(instance, attrib, rv)

        return rv

    return wrapped_pipe


def frozen(_, __, ___):
    """
    Prevent an attribute to be modified.

    .. versionadded:: 20.1.0
    """
    raise FrozenAttributeError


def validate(instance, attrib, new_value):
    """
    Run *attrib*'s validator on *new_value* if it has one.

    .. versionadded:: 20.1.0
    """
    if _config._run_validators is False:
        return new_value

    v = attrib.validator
    if not v:
        return new_value

    v(instance, attrib, new_value)

    return new_value


def convert(instance, attrib, new_value):
    """
    Run *attrib*'s converter -- if it has one -- on *new_value* and return the
    result.

    .. versionadded:: 20.1.0
    """
    c = attrib.converter
    if c:
        # This can be removed once we drop 3.8 and use attrs.Converter instead.
        from ._make import Converter

        if not isinstance(c, Converter):
            return c(new_value)

        return c(new_value, instance, attrib)

    return new_value


# Sentinel for disabling class-wide *on_setattr* hooks for certain attributes.
# Sphinx's autodata stopped working, so the docstring is inlined in the API
# docs.
NO_OP = object()
