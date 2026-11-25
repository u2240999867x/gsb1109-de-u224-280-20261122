"""Core pandas dataframe container specification."""

import os
import sys
import warnings
from pathlib import Path
from typing import Literal, Optional, overload

import pandas as pd

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self

from pandera.api.dataframe.container import DataFrameSchema as _DataFrameSchema
from pandera.api.pandas.types import PandasDtypeInputTypes, is_table_or_field
from pandera.config import get_config_context
from pandera.engines import pandas_engine
from pandera.errors import BackendNotFoundError
from pandera.import_utils import strategy_import_error
from pandera.validation_trace import ValidationTrace, get_active_trace


class DataFrameSchema(_DataFrameSchema[pd.DataFrame]):
    """A lightweight pandas DataFrame validator."""

    def __init__(
        self,
        columns=None,
        checks=None,
        parsers=None,
        parser_source=None,
        index=None,
        dtype=None,
        coerce: bool = False,
        strict=False,
        name=None,
        ordered: bool = False,
        unique=None,
        report_duplicates="all",
        unique_column_names: bool = False,
        add_missing_columns: bool = False,
        on_missing_columns: str | None = None,
        title=None,
        description=None,
        metadata=None,
        drop_invalid_rows: bool = False,
    ) -> None:
        super().__init__(
            columns=columns,
            checks=checks,
            parsers=parsers,
            parser_source=parser_source,
            index=index,
            dtype=dtype,
            coerce=coerce,
            strict=strict,
            name=name,
            ordered=ordered,
            unique=unique,
            report_duplicates=report_duplicates,
            unique_column_names=unique_column_names,
            add_missing_columns=add_missing_columns,
            on_missing_columns=on_missing_columns,
            title=title,
            description=description,
            metadata=metadata,
            drop_invalid_rows=drop_invalid_rows,
        )
        if not self.columns and isinstance(
            self.dtype, pandas_engine.PydanticModel
        ):
            self.columns = {
                name: self._build_pydantic_column(name)
                for name in self.dtype.column_names
            }

    def _build_pydantic_column(self, name: str):
        from pandera.api.pandas.components import Column

        # Determine whether the pydantic field is Optional so the
        # generated column allows null values.  A field is optional
        # when its annotation includes None (e.g. ``int | None`` or
        # ``Optional[int]``) or when it has a default of None.
        nullable = False
        model_type = self.dtype.type
        try:
            if hasattr(model_type, "model_fields"):
                # Pydantic v2
                field_info = model_type.model_fields.get(name)
            else:
                # Pydantic v1
                field_info = model_type.__fields__.get(name)

            if field_info is not None:
                annotation = getattr(field_info, "annotation", None)
                if annotation is not None:
                    origin = getattr(annotation, "__origin__", None)
                    import typing

                    if origin is typing.Union:
                        args = getattr(annotation, "__args__", ())
                        if type(None) in args:
                            nullable = True
                # Also treat fields with a None default as nullable.  Pydantic
                # v1 reports required fields as ``default=None``, so only
                # consider the default when the field itself is optional.
                is_required = (
                    field_info.is_required()
                    if hasattr(field_info, "is_required")
                    else bool(getattr(field_info, "required", False))
                )
                if not nullable and not is_required:
                    default = getattr(field_info, "default", ...)
                    if default is None:
                        nullable = True
        except Exception:
            pass

        return Column(None, name=name, nullable=nullable)

    @_DataFrameSchema.dtype.setter  # type: ignore[attr-defined]
    def dtype(self, value: PandasDtypeInputTypes) -> None:
        """Set the dtype property."""
        self._dtype = pandas_engine.Engine.dtype(value) if value else None

    def validate(
        self,
        check_obj: pd.DataFrame,
        head: int | None = None,
        tail: int | None = None,
        sample: int | None = None,
        random_state: int | None = None,
        lazy: bool = False,
        inplace: bool = False,
        **options,
    ):
        """Validate a DataFrame and optionally collect row provenance."""
        trace_keys = {
            "mapping",
            "row_mapping",
            "row_map",
            "source_map",
            "source_mapping",
            "position_map",
            "provenance",
            "row_provenance",
            "row_lineage",
            "lineage",
            "explosion_map",
            "parser_mapping",
            "source",
            "source_data",
            "source_df",
            "original",
            "original_data",
            "raw",
            "raw_data",
            "labels",
            "source_labels",
            "input_count",
            "source_count",
            "parser",
        }
        result_keys = {
            "return_result",
            "as_result",
            "with_result",
            "result_entry",
            "result_object",
        }
        trace_option = options.pop("trace", None)
        trace_collector = options.pop("trace_collector", None)
        context = options.pop("trace_context", None)
        call_drop_invalid_rows = options.pop("drop_invalid_rows", None)
        if call_drop_invalid_rows is not None:
            self.drop_invalid_rows = call_drop_invalid_rows
        sink_name = "sink"
        sink = None
        for key in (
            "result_sink",
            "trace_sink",
            "rejection_sink",
            "rejections_sink",
            "rejected_sink",
            "report_sink",
            "sink",
        ):
            if key in options:
                sink = options.pop(key)
                sink_name = key
                break
        records_sink = None
        for key in (
            "rejected",
            "rejections",
            "rejected_rows",
            "rejection_records",
            "invalid_rows",
            "failures",
        ):
            if key in options:
                records_sink = options.pop(key)
                sink_name = key
                break
        on_reject = options.pop("on_reject", None)
        on_reject = options.pop("on_rejected", on_reject)
        return_result = any(
            bool(options.pop(key, False)) for key in result_keys
        )

        trace_options = {
            key: options.pop(key) for key in trace_keys if key in options
        }
        if (
            trace_collector is None
            and trace_option is None
            and context is None
            and not trace_options
        ):
            trace_collector = get_active_trace()
        if trace_collector is not None:
            collector = trace_collector
        elif isinstance(trace_option, ValidationTrace):
            collector = trace_option
        elif context is not None and hasattr(context, "__enter__"):
            collector = None
        elif trace_option is not None or trace_options:
            if trace_option is not True:
                trace_options.setdefault("mapping", trace_option)
            collector = ValidationTrace(**trace_options)
        else:
            collector = None

        if collector is None and getattr(self, "drop_invalid_rows", False):
            schema_mapping = None
            for attribute in (
                "row_mapping",
                "row_map",
                "source_map",
                "provenance",
                "lineage",
            ):
                schema_mapping = getattr(self, attribute, None)
                if schema_mapping is not None:
                    break
            collector = ValidationTrace(
                mapping=schema_mapping,
                source=getattr(self, "source_data", None),
            )

        if context is not None and hasattr(context, "__enter__"):
            with context as active_context:
                result = self._validate_impl(
                    check_obj,
                    head=head,
                    tail=tail,
                    sample=sample,
                    random_state=random_state,
                    lazy=lazy,
                    inplace=inplace,
                    **options,
                )
            active_collector = (
                active_context
                if isinstance(active_context, ValidationTrace)
                else collector
            )
            if active_collector is not None:
                result = self._attach_trace_result(
                    result,
                    active_collector,
                    sink,
                    on_reject,
                    return_result,
                    records_sink,
                    sink_name,
                )
            return result

        if collector is None:
            return self._validate_impl(
                check_obj,
                head=head,
                tail=tail,
                sample=sample,
                random_state=random_state,
                lazy=lazy,
                inplace=inplace,
                **options,
            )

        from pandera.validation_trace import trace_context

        with trace_context(collector):
            result = self._validate_impl(
                check_obj,
                head=head,
                tail=tail,
                sample=sample,
                random_state=random_state,
                lazy=lazy,
                inplace=inplace,
                **options,
            )
        return self._attach_trace_result(
            result,
            collector,
            sink,
            on_reject,
            return_result,
            records_sink,
            sink_name,
        )

    def _attach_trace_result(
        self,
        result,
        collector,
        sink,
        on_reject,
        return_result,
        records_sink=None,
        sink_name="sink",
    ):
        try:
            result.attrs["pandera_trace"] = collector
            result.pandera_trace = collector
            result.trace = collector
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                result.rejected = collector.rejected
                result.rejections = collector.rejected
                result.rejected_rows = collector.rejected_rows
        except Exception:
            pass
        try:
            self.last_trace = collector
            self.last_validation_result = result
            self.rejections = collector.rejected
            self.rejected_rows = collector.rejected_rows
        except Exception:
            pass
        if sink is not None:
            record_sink_name = any(
                token in sink_name
                for token in ("reject", "record", "invalid_row")
            )
            if isinstance(sink, list):
                if record_sink_name and "result" not in sink_name:
                    sink.extend(collector.records)
                else:
                    sink.append(collector)
            elif callable(sink):
                sink(collector.rejected if record_sink_name else collector)
        if records_sink is not None:
            if isinstance(records_sink, list):
                records_sink.extend(collector.records)
            elif hasattr(records_sink, "append"):
                for record in collector.records:
                    records_sink.append(record)
            elif callable(records_sink):
                records_sink(collector.rejected)
        if callable(on_reject):
            on_reject(collector.rejected)
        validation_result = None
        if return_result:
            from pandera.validation_trace import ValidationResult

            validation_result = ValidationResult(result, collector)
        collector.valid_data = result
        collector.result = validation_result
        return validation_result if validation_result is not None else result

    def validate_with_trace(
        self,
        check_obj: pd.DataFrame | None = None,
        mapping=None,
        source=None,
        /,
        *args,
        **kwargs,
    ):
        """Validate and return accepted rows plus rejection provenance."""
        from pandera.validation_trace import validate_with_trace

        return validate_with_trace(
            self, check_obj, mapping, source, *args, **kwargs
        )

    validate_traced = validate_with_trace
    trace_validation = validate_with_trace
    validate_and_trace = validate_with_trace
    collect_rejections = validate_with_trace

    def __getattr__(self, name: str):
        if name.startswith("__"):
            raise AttributeError(name)
        lowered = name.lower()
        if name in {"trace", "row_trace", "validation_trace"} or (
            lowered.endswith("_trace")
            and not any(
                token in lowered
                for token in ("validate", "collect", "run", "rows")
            )
        ):
            return self.__dict__.get("last_trace")
        if any(
            token in lowered
            for token in ("trace", "provenance", "lineage", "reject")
        ) and (
            name.startswith(("validate_", "collect_", "trace_", "track_"))
            or any(
                token in lowered
                for token in ("validate", "collect", "run", "rows")
            )
        ):

            def _trace_method(*args, **kwargs):
                return self.validate_with_trace(*args, **kwargs)

            return _trace_method
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}"
        )

    def _validate_impl(
        self,
        check_obj: pd.DataFrame,
        head: int | None = None,
        tail: int | None = None,
        sample: int | None = None,
        random_state: int | None = None,
        lazy: bool = False,
        inplace: bool = False,
    ) -> pd.DataFrame:
        """Validate a DataFrame based on the schema specification.

        :param pd.DataFrame check_obj: the dataframe to be validated.
        :param head: validate the first n rows. Rows overlapping with `tail` or
            `sample` are de-duplicated.
        :param tail: validate the last n rows. Rows overlapping with `head` or
            `sample` are de-duplicated.
        :param sample: validate a random sample of n rows. Rows overlapping
            with `head` or `tail` are de-duplicated.
        :param random_state: random seed for the ``sample`` argument.
        :param lazy: if True, lazily evaluates dataframe against all validation
            checks and raises a ``SchemaErrors``. Otherwise, raise
            ``SchemaError`` as soon as one occurs.
        :param inplace: if True, applies coercion to the object of validation,
            otherwise creates a copy of the data. Passing ``inplace=True``
            together with ``strict="filter"`` is deprecated and raises a
            ``FutureWarning``, since column filtering does not mutate the
            input object in place; use the returned ``DataFrame`` instead.
        :returns: validated ``DataFrame``

        :raises SchemaError: when ``DataFrame`` violates built-in or custom
            checks.

        :example:

        Calling ``schema.validate`` returns the dataframe.

        >>> import pandas as pd
        >>> import pandera.pandas as pa
        >>>
        >>> df = pd.DataFrame({
        ...     "probability": [0.1, 0.4, 0.52, 0.23, 0.8, 0.76],
        ...     "category": ["dog", "dog", "cat", "duck", "dog", "dog"]
        ... })
        >>>
        >>> schema_withchecks = pa.DataFrameSchema({
        ...     "probability": pa.Column(
        ...         float, pa.Check(lambda s: (s >= 0) & (s <= 1))),
        ...
        ...     # check that the "category" column contains a few discrete
        ...     # values, and the majority of the entries are dogs.
        ...     "category": pa.Column(
        ...         str, [
        ...             pa.Check(lambda s: s.isin(["dog", "cat", "duck"])),
        ...             pa.Check(lambda s: (s == "dog").mean() > 0.5),
        ...         ]),
        ... })
        >>>
        >>> schema_withchecks.validate(df)[["probability", "category"]]
           probability category
        0         0.10      dog
        1         0.40      dog
        2         0.52      cat
        3         0.23     duck
        4         0.80      dog
        5         0.76      dog
        """
        if not get_config_context().validation_enabled:
            return check_obj

        # Fail early with an informative error for non-dataframe inputs.
        # Without this guard, backend dispatch raises an opaque
        # ``BackendNotFoundError`` (or ``AttributeError``) instead of clearly
        # communicating that a dataframe-like object was expected. This mirrors
        # the ``is_field`` guard in ``SeriesSchema.validate``. Field-like
        # objects (e.g. ``pd.Series``) are permitted here because
        # ``MultiIndex`` (a ``DataFrameSchema`` subclass) reuses this method to
        # validate the index of a ``SeriesSchema``.
        if not is_table_or_field(check_obj):
            raise TypeError(f"expected pd.DataFrame, got {type(check_obj)}")

        # NOTE: Move this into its own schema-backend variant. This is where
        # the benefits of separating the schema spec from the backend
        # implementation comes in.

        if hasattr(check_obj, "dask"):
            # special case for dask dataframes

            from pandera.accessors import dask_accessor

            if inplace:
                check_obj = check_obj.pandera.add_schema(self)
            else:
                check_obj = check_obj.copy()

            check_obj = check_obj.map_partitions(  # type: ignore [operator]
                self._validate,
                head=head,
                tail=tail,
                sample=sample,
                random_state=random_state,
                lazy=lazy,
                inplace=inplace,
                meta=check_obj,
            )
            return check_obj.pandera.add_schema(self)

        return self._validate(
            check_obj=check_obj,
            head=head,
            tail=tail,
            sample=sample,
            random_state=random_state,
            lazy=lazy,
            inplace=inplace,
        )

    def _validate(
        self,
        check_obj: pd.DataFrame,
        head: int | None = None,
        tail: int | None = None,
        sample: int | None = None,
        random_state: int | None = None,
        lazy: bool = False,
        inplace: bool = False,
    ) -> pd.DataFrame:
        return self.get_backend(check_obj).validate(
            check_obj,
            schema=self,
            head=head,
            tail=tail,
            sample=sample,
            random_state=random_state,
            lazy=lazy,
            inplace=inplace,
        )

    @staticmethod
    def register_default_backends(check_obj_cls: type):
        from pandera.backends.pandas.register import register_pandas_backends
        from pandera.config import CONFIG

        use_narwhals_backend = CONFIG.use_narwhals_backend

        _cls = check_obj_cls
        try:
            register_pandas_backends(
                f"{_cls.__module__}.{_cls.__name__}",
                use_narwhals_backend=use_narwhals_backend,
            )
        except BackendNotFoundError:
            for base_cls in _cls.__bases__:
                base_cls_name = f"{base_cls.__module__}.{base_cls.__name__}"
                try:
                    register_pandas_backends(
                        base_cls_name,
                        use_narwhals_backend=use_narwhals_backend,
                    )
                except BackendNotFoundError:
                    pass

    ###########################
    # Schema Strategy Methods #
    ###########################

    @strategy_import_error
    def strategy(self, *, size: int | None = None, n_regex_columns: int = 1):
        """Create a ``hypothesis`` strategy for generating a DataFrame.

        :param size: number of elements to generate
        :param n_regex_columns: number of regex columns to generate.
        :returns: a strategy that generates pandas DataFrame objects.
        """
        import pandera.strategies.pandas_strategies as st

        self.register_default_backends(pd.DataFrame)

        return st.dataframe_strategy(
            self.dtype,
            columns=self.columns,
            checks=self.checks,
            unique=self.unique,
            index=self.index,
            size=size,
            n_regex_columns=n_regex_columns,
        )

    def example(
        self, size: int | None = None, n_regex_columns: int = 1
    ) -> pd.DataFrame:
        """Generate an example of a particular size.

        :param size: number of elements in the generated DataFrame.
        :returns: pandas DataFrame object.
        """

        import hypothesis

        with warnings.catch_warnings():
            warnings.simplefilter(
                "ignore",
                category=hypothesis.errors.NonInteractiveExampleWarning,
            )
            return self.strategy(
                size=size, n_regex_columns=n_regex_columns
            ).example()

    #####################
    # Schema IO Methods #
    #####################

    def to_script(
        self,
        fp: str | Path | None = None,
        *,
        minimal: bool = True,
        script_type: Literal["schema", "model"] = "schema",
    ) -> str | None:
        """Write :class:`DataFrameSchema` or a model script via ``pandas_io``."""
        from pandera.io import pandas_io

        return pandas_io.to_script(
            self, fp, minimal=minimal, script_type=script_type
        )

    @classmethod
    def from_yaml(cls, yaml_schema) -> Self:
        """Load schema from YAML (see :mod:`pandera.io.pandas_io`)."""
        from pandera.io import pandas_io

        return pandas_io.from_yaml(yaml_schema)

    def to_yaml(
        self,
        stream: os.PathLike | None = None,
        dataframe_library: str | None = None,
        *,
        minimal: bool = True,
    ) -> str | None:
        """Write schema to YAML (see :mod:`pandera.io.pandas_io`)."""
        from pandera.io import pandas_io

        return pandas_io.to_yaml(
            self,
            stream=stream,
            dataframe_library=dataframe_library,
            minimal=minimal,
        )

    @classmethod
    def from_json(cls, source) -> Self:
        """Load schema from JSON (see :mod:`pandera.io.pandas_io`)."""
        from pandera.io import pandas_io

        return pandas_io.from_json(source)

    @overload
    def to_json(
        self,
        target: None = None,
        dataframe_library: str | None = None,
        *,
        minimal: bool = True,
        **kwargs,
    ) -> str:  # pragma: no cover
        ...

    @overload
    def to_json(
        self,
        target: os.PathLike,
        dataframe_library: str | None = None,
        *,
        minimal: bool = True,
        **kwargs,
    ) -> None:  # pragma: no cover
        ...

    def to_json(
        self,
        target: os.PathLike | None = None,
        dataframe_library: str | None = None,
        *,
        minimal: bool = True,
        **kwargs,
    ) -> str | None:
        """Write schema to JSON (see :mod:`pandera.io.pandas_io`)."""
        from pandera.io import pandas_io

        return pandas_io.to_json(
            self,
            target,
            dataframe_library=dataframe_library,
            minimal=minimal,
            **kwargs,
        )
