from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pypika_tortoise import Order, Table
from pypika_tortoise.analytics import RowNumber
from pypika_tortoise.terms import Tuple

from tortoise.exceptions import OperationalError
from tortoise.fields.relational import BackwardFKRelation

if TYPE_CHECKING:  # pragma: nocoverage
    from tortoise.backends.base.client import BaseDBAsyncClient
    from tortoise.models import Model
    from tortoise.query_utils import GroupedPrefetch
    from tortoise.queryset import QuerySet

RANK_COLUMN = "_grouped_rank"
WRAPPER_ALIAS = "_grouped"
RANK_ALIAS = "_grouped_rank_q"
BASE_ALIAS = "_grouped_base"


async def run_grouped_prefetch(
    db: BaseDBAsyncClient,
    parent_model: type[Model],
    instance_list: list[Model],
    prefetch: GroupedPrefetch,
) -> None:
    """
    Execute one :class:`~tortoise.query_utils.GroupedPrefetch` and attach ``to_attr`` lists.

    A single SQL statement ranks the candidate related rows inside each parent group with a
    window function and keeps the first ``limit`` ranks, so the query count does not grow with
    the number of parents or selected rows.
    """
    relation_field = parent_model._meta.fields_map.get(prefetch.relation)
    if not isinstance(relation_field, BackwardFKRelation):
        raise OperationalError(
            f"relation {prefetch.relation} on {parent_model.__name__} cannot be "
            "grouped-prefetched; only reverse foreign-key relations are supported"
        )
    related_model = relation_field.related_model

    parent_pk_field = parent_model._meta.pk
    parent_ids = {
        parent_pk_field.to_db_value(instance.pk, instance)
        for instance in instance_list
        if instance.pk is not None
    }
    for instance in instance_list:
        setattr(instance, prefetch.to_attr, [])
    if not parent_ids:
        return

    inner_qs = _prepare_inner_queryset(prefetch.queryset, db)
    base_table = related_model._meta.basetable
    group_column_name = relation_field.relation_source_field
    inner_qs.query = inner_qs.query.where(base_table[group_column_name].isin(list(parent_ids)))

    order_terms = list(inner_qs.query._orderbys)
    inner_qs.query._orderbys = []
    if not order_terms:
        raise OperationalError(
            "GroupedPrefetch requires an explicit order_by on the queryset to define the "
            "ranking of the limited window"
        )
    order_columns = _order_column_names(related_model, inner_qs, order_terms)

    middle_alias = BASE_ALIAS
    middle_table = Table(middle_alias)
    middle_query = db.query_class.from_(inner_qs.query.as_(middle_alias)).select(middle_table.star)
    if prefetch.cursor is not None:
        cursor_criterion = _build_cursor_criterion(
            inner_qs, related_model, middle_table, order_columns, prefetch.cursor
        )
        if prefetch.cursor_parent is not None:
            # Only the paginated parent drops the rows before its cursor; every other parent
            # keeps its full window in the same query.
            parent_instance = next(
                (instance for instance in instance_list if instance.pk == prefetch.cursor_parent),
                None,
            )
            if parent_instance is None:
                raise OperationalError(
                    "GroupedPrefetch cursor_parent must match the primary key of one of the "
                    "prefetched parent instances"
                )
            cursor_parent_db_value = parent_pk_field.to_db_value(
                prefetch.cursor_parent, parent_instance
            )
            cursor_criterion = cursor_criterion | (
                middle_table[group_column_name] != cursor_parent_db_value
            )
        middle_query = middle_query.where(cursor_criterion)

    # The window and its rank alias cannot be filtered at the same SELECT level, so ranking
    # lives in its own subquery and the ``rn <= limit`` predicate wraps it once more.
    rank_table = Table(RANK_ALIAS)
    row_number = RowNumber().over(rank_table[group_column_name])
    for column_name, order in order_columns:
        row_number = row_number.orderby(rank_table[column_name], order=order)
    ranked_query = (
        db.query_class.from_(middle_query.as_(RANK_ALIAS))
        .select(rank_table.star, row_number.as_(RANK_COLUMN))
    )

    wrapper_table = Table(WRAPPER_ALIAS)
    outer_query = (
        db.query_class.from_(ranked_query.as_(WRAPPER_ALIAS))
        .select(wrapper_table.star)
        .where(wrapper_table[RANK_COLUMN] <= prefetch.limit)
    )

    sql, values = outer_query.get_parameterized_sql()
    _, raw_rows = await db.execute_query(sql, values)

    annotation_fields = _annotation_output_fields(inner_qs)
    grouped: dict[Any, list[Model]] = {}
    ranks: dict[Any, int] = {}
    for row in raw_rows:
        obj = related_model._init_from_db(**row)
        for annotation_name, output_field in annotation_fields.items():
            if annotation_name in row.keys():
                raw_value = row[annotation_name]
                if output_field is not None and raw_value is not None:
                    raw_value = output_field.to_python_value(raw_value)
                setattr(obj, annotation_name, raw_value)
        group_value = parent_pk_field.to_python_value(row[group_column_name])
        grouped.setdefault(group_value, []).append(obj)
        ranks[obj.pk] = row[RANK_COLUMN]

    hydrated: list[Model] = [obj for objects in grouped.values() for obj in objects]
    if hydrated and (inner_qs._prefetch_map or inner_qs._prefetch_queries):
        executor = db.executor_class(
            model=related_model,
            db=db,
            prefetch_map=inner_qs._prefetch_map,
            prefetch_queries=inner_qs._prefetch_queries,
        )
        await executor._execute_prefetch_queries(hydrated)

    for instance in instance_list:
        objects = grouped.get(instance.pk, [])
        objects.sort(key=lambda candidate: ranks[candidate.pk])
        setattr(instance, prefetch.to_attr, objects)


def _prepare_inner_queryset(queryset: QuerySet, db: BaseDBAsyncClient) -> QuerySet:
    inner_qs = queryset._clone()
    inner_qs._db = db
    inner_qs._limit = None
    inner_qs._offset = None
    inner_qs._choose_db_if_not_chosen()
    inner_qs._make_query()
    return inner_qs


def _order_column_names(
    related_model: type[Model], queryset: QuerySet, order_terms: list
) -> list[tuple[str, Any]]:
    selectable_columns = {
        *related_model._meta.fields_db_projection.values(),
        *queryset._annotations.keys(),
    }
    columns = []
    for term, order in order_terms:
        column_name = getattr(term, "name", None)
        if column_name not in selectable_columns:
            raise OperationalError(
                "GroupedPrefetch can only order by selected model fields or annotations; "
                f"{column_name!r} is not selectable"
            )
        columns.append((column_name, order))
    return columns


def _annotation_output_fields(queryset: QuerySet) -> dict[str, Any]:
    return {
        name: getattr(annotation, "field_object", None)
        for name, annotation in queryset._annotations.items()
        if getattr(annotation, "field_object", None) is not None
    }


def _build_cursor_criterion(
    queryset: QuerySet,
    related_model: type[Model],
    wrapper_table: Table,
    order_columns: list[tuple[str, Any]],
    cursor: tuple[Any, ...],
) -> Any:
    if len(cursor) != len(order_columns):
        raise OperationalError(
            "GroupedPrefetch cursor must provide one value per ordering field "
            f"({len(order_columns)} expected, got {len(cursor)})"
        )

    directions = {order for _column_name, order in order_columns}
    if len(directions) != 1:
        raise OperationalError(
            "GroupedPrefetch cursor requires all ordering fields to share the same direction"
        )

    output_fields = _annotation_output_fields(queryset)
    converted_values = []
    for (column_name, _order), value in zip(order_columns, cursor):
        field_object = output_fields.get(column_name)
        if field_object is None:
            field_object = related_model._meta.fields_map.get(
                related_model._meta.fields_db_projection_reverse.get(column_name, column_name)
            )
        if field_object is not None and value is not None:
            value = field_object.to_db_value(value, None)
        converted_values.append(value)

    left = Tuple(*[wrapper_table[column_name] for column_name, _order in order_columns])
    right = Tuple(*converted_values)
    if directions.pop() == Order.asc:
        return left > right
    return left < right

