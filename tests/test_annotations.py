# SPDX-License-Identifier: MIT

"""
Tests for PEP-526 type annotations.
"""

import sys
import types
import typing

import pytest

import attr
import attrs

from attr._make import _is_class_var
from attr.exceptions import UnannotatedAttributeError


def assert_init_annotations(cls, **annotations):
    """
    Assert cls.__init__ has the correct annotations.
    """
    __tracebackhide__ = True

    annotations["return"] = type(None)

    assert annotations == typing.get_type_hints(cls.__init__)


class TestAnnotations:
    """
    Tests for types derived from variable annotations (PEP-526).
    """

    def test_basic_annotations(self):
        """
        Sets the `Attribute.type` attr from basic type annotations.
        """

        @attr.resolve_types
        @attr.s
        class C:
            x: int = attr.ib()
            y = attr.ib(type=str)
            z = attr.ib()

        assert int is attr.fields(C).x.type
        assert str is attr.fields(C).y.type
        assert None is attr.fields(C).z.type
        assert_init_annotations(C, x=int, y=str)

    def test_catches_basic_type_conflict(self):
        """
        Raises ValueError if type is specified both ways.
        """
        with pytest.raises(ValueError) as e:

            @attr.s
            class C:
                x: int = attr.ib(type=int)

        assert (
            "Type annotation and type argument cannot both be present for 'x'.",
        ) == e.value.args

    def test_typing_annotations(self):
        """
        Sets the `Attribute.type` attr from typing annotations.
        """

        @attr.resolve_types
        @attr.s
        class C:
            x: typing.List[int] = attr.ib()
            y = attr.ib(type=typing.Optional[str])

        assert typing.List[int] is attr.fields(C).x.type
        assert typing.Optional[str] is attr.fields(C).y.type
        assert_init_annotations(C, x=typing.List[int], y=typing.Optional[str])

    def test_only_attrs_annotations_collected(self):
        """
        Annotations that aren't set to an attr.ib are ignored.
        """

        @attr.resolve_types
        @attr.s
        class C:
            x: typing.List[int] = attr.ib()
            y: int

        assert 1 == len(attr.fields(C))
        assert_init_annotations(C, x=typing.List[int])

    @pytest.mark.skipif(
        sys.version_info[:2] < (3, 11),
        reason="Incompatible behavior on older Pythons",
    )
    def test_auto_attribs(self, slots):
        """
        If *auto_attribs* is True, bare annotations are collected too.
        Defaults work and class variables are ignored.
        """

        @attr.s(auto_attribs=True, slots=slots)
        class C:
            cls_var: typing.ClassVar[int] = 23
            a: int
            x: typing.List[int] = attrs.Factory(list)
            y: int = 2
            z: int = attr.ib(default=3)
            foo: typing.Any = None

        i = C(42)
        assert "C(a=42, x=[], y=2, z=3, foo=None)" == repr(i)

        attr_names = {a.name for a in C.__attrs_attrs__}
        assert "a" in attr_names  # just double check that the set works
        assert "cls_var" not in attr_names

        attr.resolve_types(C)

        assert int is attr.fields(C).a.type

        assert attr.Factory(list) == attr.fields(C).x.default
        assert typing.List[int] is attr.fields(C).x.type

        assert int is attr.fields(C).y.type
        assert 2 == attr.fields(C).y.default

        assert int is attr.fields(C).z.type

        assert typing.Any == attr.fields(C).foo.type

        # Class body is clean.
        if slots is False:
            with pytest.raises(AttributeError):
                C.y

            assert 2 == i.y
        else:
            assert isinstance(C.y, types.MemberDescriptorType)

            i.y = 23
            assert 23 == i.y

        assert_init_annotations(
            C,
            a=int,
            x=typing.List[int],
            y=int,
            z=int,
            foo=typing.Any,
        )

    def test_auto_attribs_unannotated(self, slots):
        """
        Unannotated `attr.ib`s raise an error.
        """
        with pytest.raises(UnannotatedAttributeError) as e:

            @attr.s(slots=slots, auto_attribs=True)
            class C:
                v = attr.ib()
                x: int
                y = attr.ib()
                z: str

        assert (
            "The following `attr.ib`s lack a type annotation: v, y.",
        ) == e.value.args

    def test_auto_attribs_subclassing(self, slots):
        """
        Attributes from base classes are inherited, it doesn't matter if the
        subclass has annotations or not.

        Ref #291
        """

        @attr.resolve_types
        @attr.s(slots=slots, auto_attribs=True)
        class A:
            a: int = 1

        @attr.resolve_types
        @attr.s(slots=slots, auto_attribs=True)
        class B(A):
            b: int = 2

        @attr.resolve_types
        @attr.s(slots=slots, auto_attribs=True)
        class C(A):
            pass

        assert "B(a=1, b=2)" == repr(B())
        assert "C(a=1)" == repr(C())
        assert_init_annotations(A, a=int)
        assert_init_annotations(B, a=int, b=int)
        assert_init_annotations(C, a=int)

    def test_converter_annotations(self):
        """
        An unannotated attribute with an annotated converter gets its
        annotation from the converter.
        """

        def int2str(x: int) -> str:
            return str(x)

        @attr.s
        class A:
            a = attr.ib(converter=int2str)

        assert_init_annotations(A, a=int)

        def int2str_(x: int, y: str = ""):
            return str(x)

        @attr.s
        class A:
            a = attr.ib(converter=int2str_)

        assert_init_annotations(A, a=int)

    def test_converter_attrib_annotations(self):
        """
        If a converter is provided, an explicit type annotation has no
        effect on an attribute's type annotation.
        """

        def int2str(x: int) -> str:
            return str(x)

        @attr.s
        class A:
            a: str = attr.ib(converter=int2str)
            b = attr.ib(converter=int2str, type=str)

        assert_init_annotations(A, a=int, b=int)

    def test_non_introspectable_converter(self):
        """
        A non-introspectable converter doesn't cause a crash.
        """

        @attr.s
        class A:
            a = attr.ib(converter=print)

    def test_nullary_converter(self):
        """
        A converter with no arguments doesn't cause a crash.
        """

        def noop():
            pass

        @attr.s
        class A:
            a = attr.ib(converter=noop)

        assert A.__init__.__annotations__ == {"return": None}

    def test_pipe(self):
        """
        pipe() uses the input annotation of its first argument and the
        output annotation of its last argument.
        """

        def int2str(x: int) -> str:
            return str(x)

        def strlen(y: str) -> int:
            return len(y)

        def identity(z):
            return z

        assert attr.converters.pipe(int2str).__annotations__ == {
            "val": int,
            "return": str,
        }
        assert attr.converters.pipe(int2str, strlen).__annotations__ == {
            "val": int,
            "return": int,
        }
        assert attr.converters.pipe(identity, strlen).__annotations__ == {
            "return": int
        }
        assert attr.converters.pipe(int2str, identity).__annotations__ == {
            "val": int
        }

        def int2str_(x: int, y: int = 0) -> str:
            return str(x)

        assert attr.converters.pipe(int2str_).__annotations__ == {
            "val": int,
            "return": str,
        }

    def test_pipe_empty(self):
        """
        pipe() with no converters is annotated like the identity.
        """

        p = attr.converters.pipe()

        assert "val" in p.__annotations__

        t = p.__annotations__["val"]

        assert isinstance(t, typing.TypeVar)
        assert p.__annotations__ == {"val": t, "return": t}

    def test_pipe_non_introspectable(self):
        """
        pipe() doesn't crash when passed a non-introspectable converter.
        """

        assert attr.converters.pipe(print).__annotations__ == {}

    def test_pipe_nullary(self):
        """
        pipe() doesn't crash when passed a nullary converter.
        """

        def noop():
            pass

        assert attr.converters.pipe(noop).__annotations__ == {}

    def test_optional(self):
        """
        optional() uses the annotations of the converter it wraps.
        """

        def int2str(x: int) -> str:
            return str(x)

        def int_identity(x: int):
            return x

        def strify(x) -> str:
            return str(x)

        def identity(x):
            return x

        assert attr.converters.optional(int2str).__annotations__ == {
            "val": typing.Optional[int],
            "return": typing.Optional[str],
        }
        assert attr.converters.optional(int_identity).__annotations__ == {
            "val": typing.Optional[int]
        }
        assert attr.converters.optional(strify).__annotations__ == {
            "return": typing.Optional[str]
        }
        assert attr.converters.optional(identity).__annotations__ == {}

        def int2str_(x: int, y: int = 0) -> str:
            return str(x)

        assert attr.converters.optional(int2str_).__annotations__ == {
            "val": typing.Optional[int],
            "return": typing.Optional[str],
        }

    def test_optional_non_introspectable(self):
        """
        optional() doesn't crash when passed a non-introspectable
        converter.
        """

        assert attr.converters.optional(print).__annotations__ == {}

    def test_optional_nullary(self):
        """
        optional() doesn't crash when passed a nullary converter.
        """

        def noop():
            pass

        assert attr.converters.optional(noop).__annotations__ == {}

    @pytest.mark.skipif(
        sys.version_info[:2] < (3, 11),
        reason="Incompatible behavior on older Pythons",
    )
    def test_annotations_strings(self, slots):
        """
        String annotations are passed into __init__ as is.

        The strings keep changing between releases.
        """
        import typing as t

        from typing import ClassVar

        @attr.s(auto_attribs=True, slots=slots)
        class C:
            cls_var1: "typing.ClassVar[int]" = 23
            cls_var2: "ClassVar[int]" = 23
            cls_var3: "t.ClassVar[int]" = 23
            a: "int"
            x: "typing.List[int]" = attrs.Factory(list)
            y: "int" = 2
            z: "int" = attr.ib(default=3)
            foo: "typing.Any" = None

        attr.resolve_types(C, locals(), globals())

        assert_init_annotations(
            C,
            a=int,
            x=typing.List[int],
            y=int,
            z=int,
            foo=typing.Any,
        )

    def test_typing_extensions_classvar(self, slots):
        """
        If ClassVar is coming from typing_extensions, it is recognized too.
        """

        @attr.s(auto_attribs=True, slots=slots)
        class C:
            cls_var: "typing_extensions.ClassVar" = 23  # noqa: F821

        assert_init_annotations(C)

    @pytest.mark.skipif(
        sys.version_info[:2] < (3, 14),
        reason="Python 3.14 changed annotation evaluation behavior.",
    )
    def test_missing_classvar_import(self, slots):
        """
        Unimported ClassVars are recognized as ForwardRefs.
        """

        @attr.s(auto_attribs=True, slots=slots)
        class C:
            cls_var: ClassVar[str]  # noqa: F821
            value: int = 1

        assert "cls_var" not in attr.fields_dict(C)
        assert 1 == C().value

    def test_keyword_only_auto_attribs(self):
        """
        `kw_only` propagates to attributes defined via `auto_attribs`.
        """

        @attr.s(auto_attribs=True, kw_only=True)
        class C:
            x: int
            y: int

        with pytest.raises(TypeError):
            C(0, 1)

        with pytest.raises(TypeError):
            C(x=0)

        c = C(x=0, y=1)

        assert c.x == 0
        assert c.y == 1

    def test_base_class_variable(self):
        """
        Base class' class variables can be overridden with an attribute
        without resorting to using an explicit `attr.ib()`.
        """

        class Base:
            x: int = 42

        @attr.s(auto_attribs=True)
        class C(Base):
            x: int

        assert 1 == C(1).x

    def test_removes_none_too(self):
        """
        Regression test for #523: make sure defaults that are set to None are
        removed too.
        """

        @attr.s(auto_attribs=True)
        class C:
            x: int = 42
            y: typing.Any = None

        with pytest.raises(AttributeError):
            C.x

        with pytest.raises(AttributeError):
            C.y

    def test_non_comparable_defaults(self):
        """
        Regression test for #585: objects that are not directly comparable
        (for example numpy arrays) would cause a crash when used as
        default values of an attrs auto-attrib class.
        """

        class NonComparable:
            def __eq__(self, other):
                raise ValueError

        @attr.s(auto_attribs=True)
        class C:
            x: typing.Any = NonComparable()  # noqa: RUF009

    def test_basic_resolve(self):
        """
        Resolve the `Attribute.type` attr from basic type annotations.
        Unannotated types are ignored.
        """

        @attr.s
        class C:
            x: "int" = attr.ib()
            y = attr.ib(type=str)
            z = attr.ib()

        attr.resolve_types(C)

        assert int is attr.fields(C).x.type
        assert str is attr.fields(C).y.type
        assert None is attr.fields(C).z.type

    @pytest.mark.skipif(
        sys.version_info[:2] < (3, 9),
        reason="Incompatible behavior on older Pythons",
    )
    def test_extra_resolve(self):
        """
        `get_type_hints` returns extra type hints.
        """
        from typing import Annotated

        globals = {"Annotated": Annotated}

        @attr.define
        class C:
            x: 'Annotated[float, "test"]'

        attr.resolve_types(C, globals)

        assert Annotated[float, "test"] is attr.fields(C).x.type

        @attr.define
        class D:
            x: 'Annotated[float, "test"]'

        attr.resolve_types(D, globals, include_extras=False)

        assert float is attr.fields(D).x.type

    def test_resolve_types_auto_attrib(self, slots):
        """
        Types can be resolved even when strings are involved.
        """

        @attr.s(slots=slots, auto_attribs=True)
        class A:
            a: typing.List[int]
            b: typing.List["int"]
            c: "typing.List[int]"

        # Note: I don't have to pass globals and locals here because
        # int is a builtin and will be available in any scope.
        attr.resolve_types(A)

        assert typing.List[int] == attr.fields(A).a.type
        assert typing.List[int] == attr.fields(A).b.type
        assert typing.List[int] == attr.fields(A).c.type

    def test_resolve_types_decorator(self, slots):
        """
        Types can be resolved using it as a decorator.
        """

        @attr.resolve_types
        @attr.s(slots=slots, auto_attribs=True)
        class A:
            a: typing.List[int]
            b: typing.List["int"]
            c: "typing.List[int]"

        assert typing.List[int] == attr.fields(A).a.type
        assert typing.List[int] == attr.fields(A).b.type
        assert typing.List[int] == attr.fields(A).c.type

    def test_self_reference(self, slots):
        """
        References to self class using quotes can be resolved.
        """

        @attr.s(slots=slots, auto_attribs=True)
        class A:
            a: "A"
            b: typing.Optional["A"]  # will resolve below -- noqa: F821

        attr.resolve_types(A, globals(), locals())

        assert A == attr.fields(A).a.type
        assert typing.Optional[A] == attr.fields(A).b.type

    def test_forward_reference(self, slots):
        """
        Forward references can be resolved.
        """

        @attr.s(slots=slots, auto_attribs=True)
        class A:
            a: typing.List["B"]  # will resolve below -- noqa: F821

        @attr.s(slots=slots, auto_attribs=True)
        class B:
            a: A

        attr.resolve_types(A, globals(), locals())
        attr.resolve_types(B, globals(), locals())

        assert typing.List[B] == attr.fields(A).a.type
        assert A == attr.fields(B).a.type

        assert typing.List[B] == attr.fields(A).a.type
        assert A == attr.fields(B).a.type

    def test_init_type_hints(self):
        """
        Forward references in __init__ can be automatically resolved.
        """

        @attr.s
        class C:
            x = attr.ib(type="typing.List[int]")

        assert_init_annotations(C, x=typing.List[int])

    def test_init_type_hints_fake_module(self):
        """
        If you somehow set the __module__ to something that doesn't exist
        you'll lose __init__ resolution.
        """

        class C:
            x = attr.ib(type="typing.List[int]")

        C.__module__ = "totally fake"
        C = attr.s(C)

        with pytest.raises(NameError):
            typing.get_type_hints(C.__init__)

    def test_inheritance(self):
        """
        Subclasses can be resolved after the parent is resolved.
        """

        @attr.define()
        class A:
            n: "int"

        @attr.define()
        class B(A):
            pass

        attr.resolve_types(A)
        attr.resolve_types(B)

        assert int is attr.fields(A).n.type
        assert int is attr.fields(B).n.type

    def test_resolve_twice(self):
        """
        You can call resolve_types as many times as you like.
        This test is here mostly for coverage.
        """

        @attr.define()
        class A:
            n: "int"

        attr.resolve_types(A)

        assert int is attr.fields(A).n.type

        attr.resolve_types(A)

        assert int is attr.fields(A).n.type


class TestResolveTypesView:
    """
    Tests for the independent field view returned by
    ``resolve_types(..., in_place=False)``.
    """

    def test_view_is_a_field_tuple(self):
        """
        The view behaves like ``attrs.fields()``: a tuple that also provides
        name accessors.
        """

        @attr.define
        class C:
            x: "int"

        view = attr.resolve_types(C, in_place=False)

        assert isinstance(view, tuple)
        assert int is view.x.type
        assert int is view[0].type

    def test_view_does_not_mutate_class(self):
        """
        Building a view neither touches the class's real fields nor is affected
        by a later resolution.
        """

        @attr.define
        class C:
            x: "int"

        view = attr.resolve_types(C, in_place=False)

        assert "int" == attr.fields(C).x.type
        assert int is view.x.type

        attr.resolve_types(C)

        assert int is attr.fields(C).x.type
        assert int is view.x.type

    def test_include_extras(self):
        """
        ``include_extras`` controls whether ``Annotated`` metadata is kept for
        every view individually.
        """
        from typing import Annotated

        globs = {"Annotated": Annotated}

        @attr.define
        class C:
            x: 'Annotated[float, "unit"]'

        without = attr.resolve_types(
            C, globs, include_extras=False, in_place=False
        )
        with_extras = attr.resolve_types(
            C, globs, include_extras=True, in_place=False
        )

        assert float is without.x.type
        assert Annotated[float, "unit"] == with_extras.x.type
        assert 'Annotated[float, "unit"]' == attr.fields(C).x.type

    def test_resolves_declaring_class_namespace(self):
        """
        Each field is resolved against the namespace of the class that declared
        it, not the class it's accessed on.
        """

        @attr.define
        class Base:
            x: '"Amount"'  # noqa: F821

        @attr.define
        class Sub(Base):
            pass

        class Decimal:
            pass

        class Count:
            pass

        view = attr.resolve_types(
            Sub,
            globalns={Base: {"Amount": Decimal}, Sub: {"Amount": Count}},
            in_place=False,
        )

        assert Decimal is view.x.type

    def test_winning_declaration_in_diamond(self):
        """
        When two paths inherit a field and one explicitly redeclares it, the
        view uses the declaration that actually wins the MRO *and* the
        namespace of the winning declaring class.
        """

        @attr.define
        class Base:
            x: '"Amount"'  # noqa: F821

        @attr.define
        class Left(Base):
            pass

        @attr.define
        class Right(Base):
            x: '"Amount"'  # noqa: F821

        @attr.define
        class Diamond(Left, Right):
            pass

        class Decimal:
            pass

        class Count:
            pass

        view = attr.resolve_types(
            Diamond,
            globalns={Base: {"Amount": Decimal}, Right: {"Amount": Count}},
            in_place=False,
        )

        # Right wins the MRO for "x".
        assert Count is view.x.type

    def test_forward_references_between_fields(self):
        """
        Forward references between fields of the same inheritance graph resolve
        in a view.
        """

        @attr.define
        class A:
            b: '"B"'

        @attr.define
        class B:
            a: "A"

        view = attr.resolve_types(A, globals(), locals(), in_place=False)

        assert B is view.b.type

    def test_failure_leaves_everything_intact(self):
        """
        A view that fails to resolve can't damage the class's fields or an
        earlier view, and a later view still works.
        """

        @attr.define
        class C:
            x: '"Amount"'  # noqa: F821

        class Decimal:
            pass

        good = attr.resolve_types(
            C, globalns={"Amount": Decimal}, in_place=False
        )
        assert Decimal is good.x.type

        with pytest.raises(NameError):
            attr.resolve_types(C, globalns={}, in_place=False)

        assert '"Amount"' == attr.fields(C).x.type
        assert Decimal is good.x.type

        again = attr.resolve_types(
            C, globalns={"Amount": Decimal}, in_place=False
        )
        assert Decimal is again.x.type

    def test_missing_namespace_entry_raises(self):
        """
        In a mapping of declaring class -> namespace, a declaring class that's
        missing resolves without the other classes' names.
        """

        @attr.define
        class Base:
            x: '"Amount"'  # noqa: F821

        @attr.define
        class Sub(Base):
            pass

        class Decimal:
            pass

        with pytest.raises(NameError):
            attr.resolve_types(
                Sub, globalns={Base: {"Other": Decimal}}, in_place=False
            )

    def test_in_field_transformer(self):
        """
        The view is usable from a ``field_transformer`` where *cls* isn't an
        attrs class yet.
        """
        seen = {}

        def hook(cls, attribs):
            view = attr.resolve_types(
                cls, attribs=attribs, include_extras=False, in_place=False
            )
            seen["types"] = {f.name: f.type for f in view}
            return attribs

        @attr.define(field_transformer=hook)
        class C:
            x: "int"
            y: "str"

        assert {"x": int, "y": str} == seen["types"]

    def test_two_views_diamond_with_annotated_containers(self):
        """
        End-to-end: one view drops ``Annotated`` metadata while another keeps
        it, each resolving every field in the namespace of its declaring class.

        A field travels a diamond; one path explicitly replaces its declaration
        under the same name, so the winning declaration and the namespace of
        the class that declared it both matter.
        """
        from typing import Annotated, Dict, List

        @attr.define
        class Base:
            # "Amount" is a decimal quantity in the parent plugin's namespace.
            both: 'Annotated[Dict[str, "Amount"], "unit"]'  # noqa: F821

        @attr.define
        class Left(Base):
            pass

        @attr.define
        class Right(Base):
            # This path replaces the declaration but keeps the same name;
            # in the sub-module namespace "Amount" is an integer count.
            both: 'Annotated[List["Amount"], "count"]'  # noqa: F821

        @attr.define
        class Diamond(Left, Right):
            pass

        class DecimalAmount:
            pass

        class IntCount:
            pass

        parent_ns = {
            "Annotated": Annotated,
            "Dict": Dict,
            "Amount": DecimalAmount,
        }
        sub_ns = {"Annotated": Annotated, "List": List, "Amount": IntCount}

        no_extras = attr.resolve_types(
            Diamond,
            globalns={Base: parent_ns, Left: sub_ns, Right: sub_ns},
            localns={Base: parent_ns, Left: sub_ns, Right: sub_ns},
            include_extras=False,
            in_place=False,
        )

        # The sub-module's ``Right`` wins the MRO and replaces the field, so it
        # is the declaring class and "Amount" is the integer count.
        assert List[IntCount] == no_extras.both.type

        extras = attr.resolve_types(
            Diamond,
            globalns={Base: parent_ns, Left: sub_ns, Right: sub_ns},
            localns={Base: parent_ns, Left: sub_ns, Right: sub_ns},
            include_extras=True,
            in_place=False,
        )

        assert Annotated[List[IntCount], "count"] == extras.both.type

        # The class's own field is untouched and both views are independent.
        assert (
            'Annotated[List["Amount"], "count"]'
            == attr.fields(Diamond).both.type
        )
        assert List[IntCount] == no_extras.both.type

        # A failing view doesn't damage the class or the earlier views.
        with pytest.raises(NameError):
            attr.resolve_types(
                Diamond,
                globalns={Right: {"Annotated": Annotated, "List": List}},
                include_extras=True,
                in_place=False,
            )

        assert (
            'Annotated[List["Amount"], "count"]'
            == attr.fields(Diamond).both.type
        )
        assert List[IntCount] == no_extras.both.type
        assert Annotated[List[IntCount], "count"] == extras.both.type

        # A later view still resolves as usual.
        again = attr.resolve_types(
            Diamond,
            globalns={Base: parent_ns, Left: sub_ns, Right: sub_ns},
            include_extras=False,
            in_place=False,
        )
        assert List[IntCount] == again.both.type


@pytest.mark.parametrize(
    "annot",
    [
        typing.ClassVar,
        "typing.ClassVar",
        "'typing.ClassVar[dict]'",
        "t.ClassVar[int]",
        typing.ForwardRef("ClassVar[str]"),
    ],
)
def test_is_class_var(annot):
    """
    ClassVars are detected, even if they're a string or quoted.
    """
    assert _is_class_var(annot)
