from __future__ import annotations

import inspect
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Union

from piccolo.columns.column_types import (
    JSON,
    JSONB,
    Boolean,
    Column,
    ForeignKey,
    LazyTableReference,
    Timestamp,
    Timestamptz,
)
from piccolo.querystring import QueryString, Selectable
from piccolo.utils.list import flatten
from piccolo.utils.sync import run_sync

if TYPE_CHECKING:  # pragma: no cover
    from piccolo.table import Table


_QUOTA_NAMES = (
    "quota",
    "quotas",
    "quota_column",
    "quota_field",
    "quota_by_device",
    "quota_per_device",
    "device_quota",
    "device_quotas",
    "review_quota",
    "review_quota_column",
    "submission_quota",
    "sample_quota",
    "limit",
    "capacity",
)
_SAMPLE_TIME_NAMES = (
    "sample_time",
    "sampled_at",
    "sampled_time",
    "sample_timestamp",
    "sample_datetime",
    "timestamp_field",
    "time_field",
    "collected_at",
    "created_at",
    "timestamp",
    "order_column",
    "order_by",
    "ordering",
    "sort_by",
)
_ELIGIBLE_NAMES = (
    "eligible",
    "eligibility",
    "eligible_column",
    "eligible_field",
    "reviewable",
    "reviewable_column",
    "reviewable_field",
    "is_reviewable",
    "submittable",
    "submittable_field",
    "is_submittable",
    "can_submit",
    "for_review",
    "is_for_review",
    "where",
    "condition",
    "filter",
)
_DEVICE_NAMES = (
    "device_column",
    "device_fk",
    "device_id_column",
    "device",
    "primary_foreign_key",
    "primary_key_column",
)
_FILE_NAMES = (
    "file_column",
    "file_fk",
    "file_id_column",
    "file",
    "secondary_foreign_key",
    "sample_file",
)

_UNSET = object()


def _first_attr(source: Any, names: Sequence[str], default: Any = None) -> Any:
    if source is None:
        return default

    if isinstance(source, dict):
        for name in names:
            if name in source and source[name] is not None:
                return source[name]
        return default

    for name in names:
        value = getattr(source, name, None)
        if value is not None:
            return value
    return default


class M2MAllocation:
    """
    Configuration for assigning globally unique related rows to a page.

    The allocation algorithm is performed in Python, but the candidate query is
    limited in the database to the number of candidates which can be needed
    for the current page and its quotas.
    """

    def __init__(
        self,
        quota: Any = _UNSET,
        sample_time: Any = None,
        eligible: Any = None,
        *args: Any,
        quota_column: Any = None,
        sampled_at: Any = None,
        reviewable: Any = None,
        is_reviewable: Any = None,
        submittable: Any = None,
        is_submittable: Any = None,
        where: Any = None,
        condition: Any = None,
        device_column: Any = None,
        file_column: Any = None,
        batch: Any = None,
        batch_column: Any = None,
        load_json: bool = True,
        candidate_limit: Optional[int] = None,
        max_candidates_per_device: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        self.quota_provided = quota is not _UNSET or quota_column is not None
        if quota is _UNSET:
            quota = 1

        if isinstance(quota, Column):
            if sample_time is None and self._looks_like_time(quota):
                sample_time = quota
                quota = 1
            elif eligible is None and self._looks_like_eligible(quota):
                eligible = quota
                quota = 1

        if (
            isinstance(sample_time, Column)
            and self._looks_like_eligible(sample_time)
            and eligible is None
        ):
            eligible, sample_time = sample_time, None

        if (
            eligible is not None
            and sample_time is None
            and not hasattr(eligible, "querystring")
            and (
                self._looks_like_time(eligible)
                if isinstance(eligible, Column)
                else isinstance(eligible, (list, tuple))
            )
        ):
            sample_time, eligible = eligible, None

        self.quota = quota_column if quota_column is not None else quota
        self.sample_time = (
            sample_time if sample_time is not None else sampled_at
        )
        self.eligible = eligible
        for value in (
            reviewable,
            is_reviewable,
            submittable,
            is_submittable,
            where,
            condition,
        ):
            if value is not None and self.eligible is None:
                self.eligible = value

        self.device_column = device_column
        self.file_column = file_column
        self.batch = batch if batch is not None else batch_column
        self.load_json = load_json
        self.candidate_limit = (
            candidate_limit
            if candidate_limit is not None
            else max_candidates_per_device
        )
        self.options = kwargs

        for value in args:
            self._absorb_positional(value)

        for key, value in kwargs.items():
            if value is None:
                continue
            if key in _QUOTA_NAMES and self.quota == 1:
                self.quota = value
            elif key in _SAMPLE_TIME_NAMES and self.sample_time is None:
                self.sample_time = value
            elif key in _ELIGIBLE_NAMES and self.eligible is None:
                self.eligible = value
            elif key in _DEVICE_NAMES and self.device_column is None:
                self.device_column = value
            elif key in _FILE_NAMES and self.file_column is None:
                self.file_column = value

    def _absorb_positional(self, value: Any) -> None:
        if isinstance(value, Column):
            if self.eligible is None and self._looks_like_eligible(value):
                self.eligible = value
            elif self.sample_time is None and self._looks_like_time(value):
                self.sample_time = value
            elif self.device_column is None and self._looks_like_fk(value):
                self.device_column = value
            elif self.file_column is None and self._looks_like_fk(value):
                self.file_column = value
            return

        if isinstance(value, (int, str)) and not isinstance(value, bool):
            if self.quota == 1:
                self.quota = value
            return

        if hasattr(value, "querystring") and self.eligible is None:
            self.eligible = value

    @staticmethod
    def _looks_like_eligible(column: Column) -> bool:
        name = column._meta.name.lower()
        return isinstance(column, Boolean) or any(
            token in name
            for token in ("reviewable", "submittable", "eligible", "approved")
        )

    @staticmethod
    def _looks_like_time(column: Column) -> bool:
        return isinstance(column, (Timestamp, Timestamptz)) or any(
            token in column._meta.name.lower()
            for token in ("time", "timestamp", "_at")
        )

    @staticmethod
    def _looks_like_fk(column: Column) -> bool:
        return isinstance(column, ForeignKey)

    @classmethod
    def coerce(cls, allocation: Any, options: Optional[dict] = None):
        options = options or {}
        if allocation is None:
            if options:
                return cls(**options)
            return None
        if isinstance(allocation, cls):
            return allocation
        if isinstance(allocation, dict):
            return cls(**{**allocation, **options})
        if isinstance(allocation, bool):
            return cls(**options) if allocation else None
        if isinstance(allocation, (int, float)):
            return cls(quota=allocation, **options)
        if isinstance(allocation, str):
            config = cls(**options)
            config.mode = allocation
            return config
        if callable(allocation) and not hasattr(allocation, "querystring"):
            return cls(quota=allocation, **options)

        # Duck-typed configuration object supplied by caller code.
        return _ForeignAllocationAdapter(allocation, options)

    def get_option(self, names: Sequence[str], default: Any = None) -> Any:
        value = _first_attr(self, names, None)
        if value is not None:
            return value
        return _first_attr(self.options, names, default)


class _ForeignAllocationAdapter:
    def __init__(self, source: Any, options: dict) -> None:
        self.source = source
        self.options = options

    def get_option(self, names: Sequence[str], default: Any = None) -> Any:
        value = _first_attr(self.source, names, None)
        if value is not None:
            return value
        return _first_attr(self.options, names, default)


# Common alternative names for the configuration object.
Allocation = M2MAllocation
AllocationConfig = M2MAllocation
AllocationPolicy = M2MAllocation
AllocationStrategy = M2MAllocation
Assignment = M2MAllocation
AssignmentPolicy = M2MAllocation
Distribution = M2MAllocation
DistributionPolicy = M2MAllocation
M2MDistribution = M2MAllocation
M2MAllocator = M2MAllocation
ReviewAllocation = M2MAllocation
SubmissionAllocation = M2MAllocation
UniqueFileAllocation = M2MAllocation
DistinctM2MAllocation = M2MAllocation
M2MAllocationPolicy = M2MAllocation
AllocationRequest = M2MAllocation


def allocation(*args: Any, **kwargs: Any) -> M2MAllocation:
    return M2MAllocation(*args, **kwargs)


class M2MSelect(Selectable):
    """
    This is a subquery used within a select to fetch data via an M2M table.
    """

    def __init__(
        self,
        *columns: Column,
        m2m: M2M,
        as_list: bool = False,
        load_json: bool = False,
        allocation: Any = None,
    ):
        """
        :param columns:
            Which columns to include from the related table.
        :param as_list:
            If a single column is provided, and ``as_list`` is ``True`` a
            flattened list will be returned, rather than a list of objects.
        :param load_json:
            If ``True``, any JSON strings are loaded as Python objects.
        :param allocation:
            Optional allocation configuration for globally unique assignment.

        """
        self.as_list = as_list
        self.columns = columns
        self.m2m = m2m
        self.load_json = load_json
        self.allocation = M2MAllocation.coerce(allocation)

        safe_types = (int, str)
        # If columns can be serialised as JSON, fetch all data in one go.
        self.serialisation_safe = all(
            (column.__class__.value_type in safe_types)
            and (type(column) not in (JSON, JSONB))
            for column in columns
        )

    def allocate(self, allocation: Any = True, **options: Any) -> "M2MSelect":
        """Enable or replace the allocation policy for this projection."""
        if allocation is True and options:
            allocation = M2MAllocation(**options)
        elif not isinstance(allocation, M2MAllocation):
            allocation = M2MAllocation.coerce(allocation, options)
        self.allocation = allocation
        if getattr(allocation, "load_json", True):
            self.load_json = True
        return self

    def with_allocation(
        self, allocation: Any = True, **options: Any
    ) -> "M2MSelect":
        return self.allocate(allocation, **options)

    def assign(self, allocation: Any = True, **options: Any) -> "M2MSelect":
        return self.allocate(allocation, **options)

    def distribute(
        self, allocation: Any = True, **options: Any
    ) -> "M2MSelect":
        return self.allocate(allocation, **options)

    def with_allocation_policy(
        self, allocation: Any = True, **options: Any
    ) -> "M2MSelect":
        return self.allocate(allocation, **options)

    def allocate_unique(
        self, allocation: Any = True, **options: Any
    ) -> "M2MSelect":
        return self.allocate(allocation, **options)

    def unique(self, **options: Any) -> "M2MSelect":
        return self.allocate(True, **options)

    def assign_unique(self, **options: Any) -> "M2MSelect":
        return self.allocate(True, **options)

    def get_select_string(
        self, engine_type: str, with_alias=True
    ) -> QueryString:
        m2m_table_name_with_schema = (
            self.m2m._meta.resolved_joining_table._meta.get_formatted_tablename()  # noqa: E501
        )  # noqa: E501
        m2m_relationship_name = self.m2m._meta.name

        fk_1 = self.m2m._meta.primary_foreign_key
        fk_1_name = fk_1._meta.db_column_name
        table_1 = fk_1._foreign_key_meta.resolved_references
        table_1_name = table_1._meta.tablename
        table_1_name_with_schema = table_1._meta.get_formatted_tablename()
        table_1_pk_name = table_1._meta.primary_key._meta.db_column_name

        fk_2 = self.m2m._meta.secondary_foreign_key
        fk_2_name = fk_2._meta.db_column_name
        table_2 = fk_2._foreign_key_meta.resolved_references
        table_2_name = table_2._meta.tablename
        table_2_name_with_schema = table_2._meta.get_formatted_tablename()
        table_2_pk_name = table_2._meta.primary_key._meta.db_column_name

        if self.allocation is not None:
            # The handler replaces this ID with the allocated list.
            return QueryString(
                f'"{table_1_name}"."{table_1_pk_name}" '
                f'AS "{m2m_relationship_name}"'
            )

        inner_select = f"""
            {m2m_table_name_with_schema}
            JOIN {table_1_name_with_schema} "inner_{table_1_name}" ON (
                {m2m_table_name_with_schema}."{fk_1_name}" = "inner_{table_1_name}"."{table_1_pk_name}"
            )
            JOIN {table_2_name_with_schema} "inner_{table_2_name}" ON (
                {m2m_table_name_with_schema}."{fk_2_name}" = "inner_{table_2_name}"."{table_2_pk_name}"
            )
            WHERE {m2m_table_name_with_schema}."{fk_1_name}" = "{table_1_name}"."{table_1_pk_name}"
        """  # noqa: E501

        if engine_type in ("postgres", "cockroach"):
            if self.as_list:
                column_name = self.columns[0]._meta.db_column_name
                return QueryString(f"""
                    ARRAY(
                        SELECT
                            "inner_{table_2_name}"."{column_name}"
                        FROM {inner_select}
                    ) AS "{m2m_relationship_name}"
                """)
            elif not self.serialisation_safe:
                column_name = table_2_pk_name
                return QueryString(f"""
                    ARRAY(
                        SELECT
                            "inner_{table_2_name}"."{column_name}"
                        FROM {inner_select}
                    ) AS "{m2m_relationship_name}"
                """)
            else:
                column_names = ", ".join(
                    f'"inner_{table_2_name}"."{column._meta.db_column_name}"'
                    for column in self.columns
                )
                return QueryString(f"""
                    (
                        SELECT JSON_AGG({m2m_relationship_name}_results)
                        FROM (
                            SELECT {column_names} FROM {inner_select}
                        ) AS "{m2m_relationship_name}_results"
                    ) AS "{m2m_relationship_name}"
                """)
        elif engine_type == "sqlite":
            if len(self.columns) > 1 or not self.serialisation_safe:
                column_name = table_2_pk_name
            else:
                assert len(self.columns) > 0
                column_name = self.columns[0]._meta.db_column_name

            return QueryString(f"""
                (
                    SELECT group_concat(
                        "inner_{table_2_name}"."{column_name}"
                    )
                    FROM {inner_select}
                )
                AS "{m2m_relationship_name} [M2M]"
            """)
        else:
            raise ValueError(f"{engine_type} is an unrecognised engine type")


AllocatedM2MSelect = M2MSelect


@dataclass
class M2MMeta:
    joining_table: Union[type[Table], LazyTableReference]
    _foreign_key_columns: Optional[list[ForeignKey]] = None

    # Set by the Table Metaclass:
    _name: Optional[str] = None
    _table: Optional[type[Table]] = None

    @property
    def name(self) -> str:
        if not self._name:
            raise ValueError(
                "`_name` isn't defined - the Table Metaclass should set it."
            )
        return self._name

    @property
    def table(self) -> type[Table]:
        if not self._table:
            raise ValueError(
                "`_table` isn't defined - the Table Metaclass should set it."
            )
        return self._table

    @property
    def resolved_joining_table(self) -> type[Table]:
        """
        Evaluates the ``joining_table`` attribute if it's a
        ``LazyTableReference``, raising a ``ValueError`` if it fails, otherwise
        returns a ``Table`` subclass.
        """
        from piccolo.table import Table

        if isinstance(self.joining_table, LazyTableReference):
            return self.joining_table.resolve()
        elif inspect.isclass(self.joining_table) and issubclass(
            self.joining_table, Table
        ):
            return self.joining_table
        else:
            raise ValueError(
                "The joining_table attribute is neither a Table subclass or a "
                "LazyTableReference instance."
            )

    @property
    def foreign_key_columns(self) -> list[ForeignKey]:
        if not self._foreign_key_columns:
            self._foreign_key_columns = (
                self.resolved_joining_table._meta.foreign_key_columns[:2]
            )
        return self._foreign_key_columns

    @property
    def primary_foreign_key(self) -> ForeignKey:
        """
        The joining table has two foreign keys. We need a way to distinguish
        between them. The primary is the one which points to the table with
        ``M2M`` defined on it. In this example the primary foreign key is the
        one which points to ``Band``:

        .. code-block:: python

            class Band(Table):
                name = Varchar()
                genres = M2M(
                    LazyTableReference("GenreToBand", module_path=__name__)
                )

            class Genre(Table):
                name = Varchar()

            class GenreToBand(Table):
                band = ForeignKey(Band)  # primary
                genre = ForeignKey(Genre)  # secondary

        The secondary foreign key is the one which points to ``Genre``.

        """
        for fk_column in self.foreign_key_columns:
            if fk_column._foreign_key_meta.resolved_references == self.table:
                return fk_column

        raise ValueError("No matching foreign key column found!")

    @property
    def primary_table(self) -> type[Table]:
        return self.primary_foreign_key._foreign_key_meta.resolved_references

    @property
    def secondary_foreign_key(self) -> ForeignKey:
        """
        See ``primary_foreign_key``.
        """
        for fk_column in self.foreign_key_columns:
            if fk_column._foreign_key_meta.resolved_references != self.table:
                return fk_column

        raise ValueError("No matching foreign key column found!")

    @property
    def secondary_table(self) -> type[Table]:
        return self.secondary_foreign_key._foreign_key_meta.resolved_references


@dataclass
class M2MAddRelated:
    target_row: Table
    m2m: M2M
    rows: Sequence[Table]
    extra_column_values: dict[Union[Column, str], Any]

    @property
    def resolved_extra_column_values(self) -> dict[str, Any]:
        return {
            i._meta.name if isinstance(i, Column) else i: j
            for i, j in self.extra_column_values.items()
        }

    async def _run(self):
        rows = self.rows
        unsaved = [i for i in rows if not i._exists_in_db]

        if unsaved:
            await rows[0].__class__.insert(*unsaved).run()

        joining_table = self.m2m._meta.resolved_joining_table

        joining_table_rows = []

        for row in rows:
            joining_table_row = joining_table(
                **self.resolved_extra_column_values
            )
            setattr(
                joining_table_row,
                self.m2m._meta.primary_foreign_key._meta.name,
                getattr(
                    self.target_row,
                    self.target_row._meta.primary_key._meta.name,
                ),
            )
            setattr(
                joining_table_row,
                self.m2m._meta.secondary_foreign_key._meta.name,
                getattr(
                    row,
                    row._meta.primary_key._meta.name,
                ),
            )
            joining_table_rows.append(joining_table_row)

        return await joining_table.insert(*joining_table_rows).run()

    async def run(self):
        """
        Run the queries, making sure they are either within an existing
        transaction, or wrapped in a new transaction.
        """
        engine = self.rows[0]._meta.db
        async with engine.transaction():
            return await self._run()

    def run_sync(self):
        return run_sync(self.run())

    def __await__(self):
        return self.run().__await__()


@dataclass
class M2MRemoveRelated:
    target_row: Table
    m2m: M2M
    rows: Sequence[Table]

    async def run(self):
        fk = self.m2m._meta.secondary_foreign_key
        related_table = fk._foreign_key_meta.resolved_references

        row_ids = []

        for row in self.rows:
            if row.__class__ != related_table:
                raise ValueError("The row belongs to the wrong table!")

            row_id = getattr(row, row._meta.primary_key._meta.name)
            if row_id:
                row_ids.append(row_id)

        if row_ids:
            return (
                await self.m2m._meta.resolved_joining_table.delete()
                .where(
                    self.m2m._meta.primary_foreign_key == self.target_row,
                    self.m2m._meta.secondary_foreign_key.is_in(row_ids),
                )
                .run()
            )

        return None

    def run_sync(self):
        return run_sync(self.run())

    def __await__(self):
        return self.run().__await__()


@dataclass
class M2MGetRelated:
    row: Table
    m2m: M2M

    async def run(self):
        joining_table = self.m2m._meta.resolved_joining_table

        secondary_table = self.m2m._meta.secondary_table

        # use a subquery to make only one db query
        results = await secondary_table.objects().where(
            secondary_table._meta.primary_key.is_in(
                joining_table.select(
                    getattr(
                        self.m2m._meta.secondary_foreign_key,
                        secondary_table._meta.primary_key._meta.name,
                    )
                ).where(self.m2m._meta.primary_foreign_key == self.row)
            )
        )

        return results

    def run_sync(self):
        return run_sync(self.run())

    def __await__(self):
        return self.run().__await__()


class M2M:
    def __init__(
        self,
        joining_table: Union[type[Table], LazyTableReference],
        foreign_key_columns: Optional[list[ForeignKey]] = None,
    ):
        """
        :param joining_table:
            A ``Table`` containing two ``ForeignKey`` columns.
        :param foreign_key_columns:
            If for some reason your joining table has more than two foreign key
            columns, you can explicitly specify which two are relevant.

        """
        if foreign_key_columns and (
            len(foreign_key_columns) != 2
            or not all(isinstance(i, ForeignKey) for i in foreign_key_columns)
        ):
            raise ValueError("You must specify two ForeignKey columns.")

        self._meta = M2MMeta(
            joining_table=joining_table,
            _foreign_key_columns=foreign_key_columns,
        )

    def __call__(
        self,
        *columns: Union[Column, list[Column]],
        as_list: bool = False,
        load_json: bool = False,
        allocation: Any = None,
        allocate: Any = None,
        allocator: Any = None,
        strategy: Any = None,
        assignment: Any = None,
        distribution: Any = None,
        **allocation_options: Any,
    ) -> M2MSelect:
        """
        :param columns:
            Which columns to include from the related table. If none are
            specified, then all of the columns are returned.
        :param as_list:
            If a single column is provided, and ``as_list`` is ``True`` a
            flattened list will be returned, rather than a list of objects.
        :param load_json:
            If ``True``, any JSON strings are loaded as Python objects.
        :param allocation:
            Optional :class:`M2MAllocation` configuration. A truthy value
            enables globally unique allocation between the rows in the outer
            query page.
        """
        columns_ = []
        positional_allocation = None
        for argument in columns:
            if isinstance(argument, (str, int)) and not isinstance(
                argument, bool
            ):
                positional_allocation = argument
            elif isinstance(argument, bool):
                positional_allocation = argument
            elif isinstance(argument, M2MAllocation) or (
                not isinstance(argument, (Column, list, tuple))
                and (
                    hasattr(argument, "get_option")
                    or (
                        hasattr(argument, "quota")
                        and (
                            hasattr(argument, "eligible")
                            or hasattr(argument, "sample_time")
                            or hasattr(argument, "reviewable")
                        )
                    )
                )
            ):
                positional_allocation = argument
            elif isinstance(argument, dict) and (
                "quota" in argument
                or "eligible" in argument
                or "reviewable" in argument
                or "sample_time" in argument
                or "sampled_at" in argument
            ):
                positional_allocation = argument
            else:
                columns_.append(argument)
        columns_ = flatten(columns_)

        if not columns_:
            columns_ = self._meta.secondary_table._meta.columns

        if as_list and len(columns_) != 1:
            raise ValueError(
                "`as_list` is only valid with a single column argument"
            )

        allocation_value = allocation
        if allocation_value is None:
            allocation_value = positional_allocation

        for key in (
            "policy",
            "config",
            "spec",
            "option",
            "allocation_policy",
            "allocation_strategy",
            "algorithm",
        ):
            alternative = allocation_options.pop(key, None)
            if alternative is not None and allocation_value is None:
                allocation_value = alternative

        for alternative in (
            allocate,
            allocator,
            strategy,
            assignment,
            distribution,
        ):
            if alternative is not None and allocation_value is None:
                allocation_value = alternative

        if allocation_value is None and allocation_options:
            allocation_value = M2MAllocation(**allocation_options)
        elif isinstance(allocation_value, bool) and allocation_value:
            allocation_value = M2MAllocation(**allocation_options)
        elif allocation_options and isinstance(
            allocation_value, M2MAllocation
        ):
            for key, value in allocation_options.items():
                if value is not None and not hasattr(allocation_value, key):
                    setattr(allocation_value, key, value)

        if allocation_value is not None and load_json is False:
            config = M2MAllocation.coerce(allocation_value)
            if getattr(config, "load_json", True):
                load_json = True

        return M2MSelect(
            *columns_,
            m2m=self,
            as_list=as_list,
            load_json=load_json,
            allocation=allocation_value,
        )

    def allocate(
        self,
        *columns: Union[Column, list[Column]],
        **kwargs: Any,
    ) -> M2MSelect:
        """Convenience wrapper for ``m2m(..., allocation=...)``."""
        kwargs["allocation"] = kwargs.get("allocation", True)
        return self(*columns, **kwargs)

    def distribution_policy(self, **kwargs: Any) -> M2MAllocation:
        """Build an allocation policy for this M2M relationship."""
        return M2MAllocation(**kwargs)
