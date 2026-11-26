from __future__ import annotations as _annotations

import asyncio
import functools
import inspect
from collections.abc import AsyncGenerator as AsyncGeneratorABC
from collections.abc import Awaitable, Callable
from collections.abc import Generator as GeneratorABC
from functools import partial
from typing import Any, get_args, get_origin

import pydantic_core

from ..config import ConfigDict
from ..plugin._schema_validator import create_schema_validator
from ._config import ConfigWrapper, prepare_config
from ._generate_schema import GenerateSchema, ValidateCallSupportedTypes
from ._namespace_utils import MappingNamespace, NsResolver, ns_for_function
from ._typing_extra import eval_type, is_annotated, signature_no_eval


def extract_function_name(func: ValidateCallSupportedTypes) -> str:
    """Extract the name of a `ValidateCallSupportedTypes` object."""
    return f'partial({func.func.__name__})' if isinstance(func, functools.partial) else func.__name__


def extract_function_qualname(func: ValidateCallSupportedTypes) -> str:
    """Extract the qualname of a `ValidateCallSupportedTypes` object."""
    return f'partial({func.func.__qualname__})' if isinstance(func, functools.partial) else func.__qualname__


def update_wrapper_attributes(wrapped: ValidateCallSupportedTypes, wrapper: Callable[..., Any]):
    """Update the `wrapper` function with the attributes of the `wrapped` function. Return the updated function."""
    if inspect.iscoroutinefunction(wrapped):

        @functools.wraps(wrapped)
        async def wrapper_function(*args, **kwargs):  # type: ignore
            return await wrapper(*args, **kwargs)
    else:

        @functools.wraps(wrapped)
        def wrapper_function(*args, **kwargs):
            return wrapper(*args, **kwargs)

    # We need to manually update this because `partial` object has no `__name__` and `__qualname__`.
    wrapper_function.__name__ = extract_function_name(wrapped)
    wrapper_function.__qualname__ = extract_function_qualname(wrapped)
    wrapper_function.raw_function = wrapped  # type: ignore

    return wrapper_function


def _prefix_validation_error(
    error: pydantic_core.ValidationError, loc_prefix: tuple[Any, ...], title: str
) -> pydantic_core.ValidationError:
    """Rebuild a validation error with `loc_prefix` prepended to every error location."""
    line_errors = []
    for line_error in error.errors(include_url=False):
        line_error = dict(line_error)
        loc = line_error.pop('loc', ())
        line_errors.append({**line_error, 'loc': (*loc_prefix, *loc)})
    return pydantic_core.ValidationError.from_exception_data(title, line_errors)


class _ValidatedGenerator(GeneratorABC[Any, Any, Any]):
    """Generator proxy that validates yielded, sent, and returned values."""

    __slots__ = (
        '_generator',
        '_yield_validator',
        '_send_validator',
        '_return_validator',
        '_error_title',
        '_yield_count',
        '_closed',
    )

    def __init__(
        self,
        generator: GeneratorABC[Any, Any, Any],
        *,
        yield_validator: Any,
        send_validator: Any,
        return_validator: Any,
        error_title: str,
    ) -> None:
        self._generator = generator
        self._yield_validator = yield_validator
        self._send_validator = send_validator
        self._return_validator = return_validator
        self._error_title = error_title
        self._yield_count = 0
        self._closed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._generator, name)

    def __repr__(self) -> str:
        return repr(self._generator)

    def _validate_send(self, value: Any) -> Any:
        try:
            return self._send_validator.validate_python(value)
        except pydantic_core.ValidationError as error:
            send_index = max(self._yield_count - 1, 0)
            raise _prefix_validation_error(error, (send_index, 'send'), self._error_title) from error

    def _validate_return(self, value: Any) -> Any:
        try:
            return self._return_validator.validate_python(value)
        except pydantic_core.ValidationError as error:
            raise _prefix_validation_error(error, ('return',), self._error_title) from error

    def _validate_yield(self, value: Any) -> Any:
        yield_index = self._yield_count
        try:
            validated = self._yield_validator.validate_python(value)
        except pydantic_core.ValidationError as error:
            raise _prefix_validation_error(error, (yield_index,), self._error_title) from error
        self._yield_count += 1
        return validated

    def _close(self) -> None:
        if not self._closed:
            self._closed = True
            self._generator.close()

    def __next__(self) -> Any:
        return self.send(None)

    def send(self, value: Any) -> Any:
        if self._closed:
            raise StopIteration
        sent_value = None if value is None else self._validate_send(value)
        try:
            yielded_value = self._generator.send(sent_value)
        except StopIteration as exc:
            self._closed = True
            return_value = self._validate_return(exc.value)
            raise StopIteration(return_value) from exc
        except BaseException:
            self._closed = True
            raise

        try:
            return self._validate_yield(yielded_value)
        except pydantic_core.ValidationError:
            self._close()
            raise

    def throw(self, typ: Any, val: Any = None, tb: Any = None) -> Any:
        try:
            if tb is not None:
                yielded_value = self._generator.throw(typ, val, tb)
            elif val is not None:
                yielded_value = self._generator.throw(typ, val)
            else:
                yielded_value = self._generator.throw(typ)
        except StopIteration as exc:
            self._closed = True
            return_value = self._validate_return(exc.value)
            raise StopIteration(return_value) from exc
        except BaseException:
            self._closed = True
            raise

        try:
            return self._validate_yield(yielded_value)
        except pydantic_core.ValidationError:
            self._close()
            raise

    def close(self) -> None:
        self._close()


class _ValidatedAsyncGenerator(AsyncGeneratorABC[Any, Any]):
    """Async generator proxy with serialized operations and shared close coordination."""

    __slots__ = (
        '_generator',
        '_yield_validator',
        '_send_validator',
        '_error_title',
        '_yield_count',
        '_closed',
        '_lock',
    )

    def __init__(
        self,
        generator: AsyncGeneratorABC[Any, Any],
        *,
        yield_validator: Any,
        send_validator: Any,
        error_title: str,
    ) -> None:
        self._generator = generator
        self._yield_validator = yield_validator
        self._send_validator = send_validator
        self._error_title = error_title
        self._yield_count = 0
        self._closed = False
        self._lock = asyncio.Lock()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._generator, name)

    def __repr__(self) -> str:
        return repr(self._generator)

    def _validate_send(self, value: Any) -> Any:
        try:
            return self._send_validator.validate_python(value)
        except pydantic_core.ValidationError as error:
            send_index = max(self._yield_count - 1, 0)
            raise _prefix_validation_error(error, (send_index, 'send'), self._error_title) from error

    def _validate_yield(self, value: Any) -> Any:
        yield_index = self._yield_count
        try:
            validated = self._yield_validator.validate_python(value)
        except pydantic_core.ValidationError as error:
            raise _prefix_validation_error(error, (yield_index,), self._error_title) from error
        self._yield_count += 1
        return validated

    async def _close(self) -> None:
        if not self._closed:
            self._closed = True
            await self._generator.aclose()

    def __aiter__(self) -> _ValidatedAsyncGenerator:
        return self

    async def asend(self, value: Any) -> Any:
        async with self._lock:
            if self._closed:
                raise StopAsyncIteration
            sent_value = None if value is None else self._validate_send(value)
            try:
                yielded_value = await self._generator.asend(sent_value)
            except StopAsyncIteration:
                self._closed = True
                raise
            except BaseException:
                self._closed = True
                raise

            try:
                return self._validate_yield(yielded_value)
            except pydantic_core.ValidationError:
                await self._close()
                raise

    async def athrow(self, typ: Any, val: Any = None, tb: Any = None) -> Any:
        async with self._lock:
            try:
                if tb is not None:
                    yielded_value = await self._generator.athrow(typ, val, tb)
                elif val is not None:
                    yielded_value = await self._generator.athrow(typ, val)
                else:
                    yielded_value = await self._generator.athrow(typ)
            except StopAsyncIteration:
                self._closed = True
                raise
            except BaseException:
                self._closed = True
                raise

            try:
                return self._validate_yield(yielded_value)
            except pydantic_core.ValidationError:
                await self._close()
                raise

    async def aclose(self) -> None:
        async with self._lock:
            await self._close()


class ValidateCallWrapper:
    """This is a wrapper around a function that validates the arguments passed to it, and optionally the return value."""

    __slots__ = (
        'function',
        'validate_return',
        'schema_type',
        'module',
        'qualname',
        'ns_resolver',
        'config_wrapper',
        '__pydantic_complete__',
        '__pydantic_validator__',
        '__return_pydantic_validator__',
    )

    def __init__(
        self,
        function: ValidateCallSupportedTypes,
        config: ConfigDict | None,
        validate_return: bool,
        parent_namespace: MappingNamespace | None,
    ) -> None:
        self.function = function
        config_dict = prepare_config(config)
        self.validate_return = validate_return or config_dict.get('validate_return', False)
        if self.validate_return:
            # Return validation is handled below, including lazy generator round-trip validation.
            config_dict['validate_return'] = False
        if isinstance(function, partial):
            self.schema_type = function.func
            self.module = function.func.__module__
        else:
            self.schema_type = function
            self.module = function.__module__
        self.qualname = extract_function_qualname(function)

        self.ns_resolver = NsResolver(
            namespaces_tuple=ns_for_function(self.schema_type, parent_namespace=parent_namespace)
        )
        self.config_wrapper = ConfigWrapper(config_dict)
        if not self.config_wrapper.defer_build:
            self._create_validators()
        else:
            self.__pydantic_complete__ = False

    def _create_validators(self) -> None:
        gen_schema = GenerateSchema(self.config_wrapper, self.ns_resolver)
        schema = gen_schema.clean_schema(gen_schema.generate_schema(self.function))
        core_config = self.config_wrapper.core_config(title=self.qualname)

        self.__pydantic_validator__ = create_schema_validator(
            schema,
            self.schema_type,
            self.module,
            self.qualname,
            'validate_call',
            core_config,
            self.config_wrapper.plugin_settings,
        )
        if self.validate_return:
            signature = signature_no_eval(self.function)
            return_type = signature.return_annotation if signature.return_annotation is not signature.empty else Any
            generator_info = self._get_generator_validation_info(return_type)

            gen_schema = GenerateSchema(self.config_wrapper, self.ns_resolver)

            def build_validator(annotation: Any) -> Any:
                component_schema_generator = GenerateSchema(self.config_wrapper, self.ns_resolver)
                schema = component_schema_generator.clean_schema(component_schema_generator.generate_schema(annotation))
                return create_schema_validator(
                    schema,
                    self.schema_type,
                    self.module,
                    self.qualname,
                    'validate_call',
                    core_config,
                    self.config_wrapper.plugin_settings,
                )

            if generator_info is not None:
                is_async_generator, yield_type, send_type, return_value_type = generator_info
                yield_validator = build_validator(yield_type)
                send_validator = build_validator(send_type)

                if is_async_generator:

                    def generator_return_wrapper(generator: Any) -> _ValidatedAsyncGenerator:
                        return _ValidatedAsyncGenerator(
                            generator,
                            yield_validator=yield_validator,
                            send_validator=send_validator,
                            error_title=self.qualname,
                        )

                    self.__return_pydantic_validator__ = generator_return_wrapper
                else:
                    return_validator = build_validator(return_value_type)

                    def generator_return_wrapper(generator: Any) -> _ValidatedGenerator:
                        return _ValidatedGenerator(
                            generator,
                            yield_validator=yield_validator,
                            send_validator=send_validator,
                            return_validator=return_validator,
                            error_title=self.qualname,
                        )

                    self.__return_pydantic_validator__ = generator_return_wrapper
            else:
                schema = gen_schema.clean_schema(gen_schema.generate_schema(return_type))
                validator = create_schema_validator(
                    schema,
                    self.schema_type,
                    self.module,
                    self.qualname,
                    'validate_call',
                    core_config,
                    self.config_wrapper.plugin_settings,
                )
                if inspect.iscoroutinefunction(self.function):

                    async def return_val_wrapper(aw: Awaitable[Any]) -> None:
                        return validator.validate_python(await aw)

                    self.__return_pydantic_validator__ = return_val_wrapper
                else:
                    self.__return_pydantic_validator__ = validator.validate_python
        else:
            self.__return_pydantic_validator__ = None

        self.__pydantic_complete__ = True

    def _get_generator_validation_info(self, return_type: Any) -> tuple[bool, Any, Any, Any] | None:
        """Return `(is_async, yield_type, send_type, return_type)` for a generator annotation."""
        if return_type is inspect.Signature.empty:
            return None

        try:
            globalns, localns = self.ns_resolver.types_namespace
            resolved_return_type = eval_type(
                return_type,
                globalns,
                localns,
                getattr(self.schema_type, '__type_params__', None),
            )
        except Exception:
            resolved_return_type = return_type

        if is_annotated(resolved_return_type):
            resolved_return_type = get_args(resolved_return_type)[0]

        origin = get_origin(resolved_return_type) or resolved_return_type
        args = get_args(resolved_return_type)

        if origin is GeneratorABC:
            return (
                False,
                args[0] if len(args) >= 1 else Any,
                args[1] if len(args) >= 2 else Any,
                args[2] if len(args) >= 3 else Any,
            )

        if origin is AsyncGeneratorABC:
            return (
                True,
                args[0] if len(args) >= 1 else Any,
                args[1] if len(args) >= 2 else Any,
                Any,
            )

        return None

    def __call__(self, /, *args: Any, **kwargs: Any) -> Any:
        if not self.__pydantic_complete__:
            self._create_validators()

        res = self.__pydantic_validator__.validate_python(pydantic_core.ArgsKwargs(args, kwargs))
        if self.__return_pydantic_validator__:
            return self.__return_pydantic_validator__(res)
        else:
            return res
