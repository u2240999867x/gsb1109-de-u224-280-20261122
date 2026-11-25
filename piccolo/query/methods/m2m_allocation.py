from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

from piccolo.columns.base import Column
from piccolo.columns.m2m import (
    M2MAllocation,
    _DEVICE_NAMES,
    _ELIGIBLE_NAMES,
    _FILE_NAMES,
    _QUOTA_NAMES,
    _SAMPLE_TIME_NAMES,
)

if TYPE_CHECKING:  # pragma: no cover
    from piccolo.columns.m2m import M2MSelect
    from piccolo.query.methods.select import Select


_MAPPING_ALIAS = "__piccolo_allocation_mapping_key"


@dataclass
class _Edge:
    to: int
    reverse: int
    capacity: int
    cost: int


def _key(value: Any) -> str:
    if value is None:
        return "none"
    if isinstance(value, UUID):
        return f"uuid:{value}"
    if isinstance(value, str):
        try:
            return f"uuid:{UUID(value)}"
        except ValueError:
            return f"scalar:{value}"
    if isinstance(value, (int, bytes)):
        return f"scalar:{value}"
    return (
        f"object:{type(value).__module__}." f"{type(value).__name__}:{value}"
    )


def _config_value(
    allocation: Any, names: tuple[str, ...], default: Any = None
) -> Any:
    getter = getattr(allocation, "get_option", None)
    if callable(getter):
        return getter(names, default)

    if isinstance(allocation, dict):
        for name in names:
            if name in allocation and allocation[name] is not None:
                return allocation[name]
        return default

    for name in names:
        value = getattr(allocation, name, None)
        if value is not None:
            return value
    return default


def _resolve_column(
    value: Any, table: type[Any], names: tuple[str, ...] = ()
) -> Optional[Column]:
    if isinstance(value, Column):
        return value

    if isinstance(value, str):
        try:
            return table._meta.get_column_by_name(value)
        except ValueError:
            return None

    for name in names:
        column = getattr(table, name, None)
        if isinstance(column, Column):
            return column

    for name in names:
        try:
            return table._meta.get_column_by_name(name)
        except ValueError:
            continue

    return None


def _root_table(column: Column) -> Optional[type[Any]]:
    call_chain = getattr(column._meta, "call_chain", [])
    if call_chain:
        first = call_chain[0]
        return getattr(first._meta, "table", None)
    return column._meta.table


def _projection_key(column: Column) -> str:
    alias = getattr(column, "_alias", None)
    if alias:
        return alias
    return column._meta.name


def _add_edge(
    graph: list[list[_Edge]],
    source: int,
    target: int,
    capacity: int,
    cost: int,
) -> _Edge:
    forward = _Edge(
        to=target,
        reverse=len(graph[target]),
        capacity=capacity,
        cost=cost,
    )
    backward = _Edge(
        to=source,
        reverse=len(graph[source]),
        capacity=0,
        cost=-cost,
    )
    graph[source].append(forward)
    graph[target].append(backward)
    return forward


def _min_cost_allocation(
    device_keys: list[str],
    quotas: dict[str, int],
    candidates: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    """
    Allocate globally unique files.

    The min-cost flow costs encode, in order: maximum covered devices, maximum
    filled quota slots, then lexicographic device/sample ordering.
    """

    slots: list[tuple[str, int]] = []
    for device_key in device_keys:
        quota = max(0, int(quotas.get(device_key, 0) or 0))
        for slot_index in range(quota):
            slots.append((device_key, slot_index))

    slot_count = len(slots)
    result: dict[str, list[dict[str, Any]]] = {
        device_key: [] for device_key in device_keys
    }

    if slot_count == 0:
        return result

    max_rank = 0
    for device_key in device_keys:
        for index, candidate in enumerate(candidates.get(device_key, []), 1):
            rank = int(candidate.get("rank") or index)
            candidate["rank"] = rank
            max_rank = max(max_rank, rank)

    rank_base = max(slot_count, max_rank, 1) + 2
    empty_rank = rank_base - 1
    lex_bound = rank_base ** (slot_count + 1)
    fill_weight = lex_bound + 1
    coverage_weight = slot_count * fill_weight + lex_bound + 1

    file_keys = sorted(
        {
            candidate["file_key"]
            for device_candidates in candidates.values()
            for candidate in device_candidates
        }
    )
    file_node = {
        file_key: 1 + slot_count + index
        for index, file_key in enumerate(file_keys)
    }

    source = 0
    first_file_node = 1 + slot_count
    sink = first_file_node + len(file_keys)
    node_count = sink + 1
    graph: list[list[_Edge]] = [[] for _ in range(node_count)]

    slot_edges: list[tuple[int, _Edge, dict[str, Any]]] = []
    empty_edges: list[tuple[int, _Edge]] = []

    for slot_index, (device_key, position) in enumerate(slots):
        slot_node = 1 + slot_index
        _add_edge(graph, source, slot_node, 1, 0)

        priority_weight = rank_base ** (slot_count - slot_index)
        empty_cost = empty_rank * priority_weight + fill_weight
        if position == 0:
            empty_cost += coverage_weight
        empty_edge = _add_edge(graph, slot_node, sink, 1, empty_cost)
        empty_edges.append((slot_index, empty_edge))

        for candidate in candidates.get(device_key, []):
            if candidate["file_key"] not in file_node:
                continue
            edge = _add_edge(
                graph,
                slot_node,
                file_node[candidate["file_key"]],
                1,
                int(candidate["rank"]) * priority_weight,
            )
            slot_edges.append((slot_index, edge, candidate))

    for node in file_node.values():
        _add_edge(graph, node, sink, 1, 0)

    flow = 0
    while flow < slot_count:
        distances = [None] * node_count
        previous_vertex = [-1] * node_count
        previous_edge = [-1] * node_count
        distances[source] = 0
        queue = deque([source])
        in_queue = [False] * node_count
        in_queue[source] = True

        while queue:
            vertex = queue.popleft()
            in_queue[vertex] = False
            current_distance = distances[vertex]
            if current_distance is None:
                continue

            for edge_index, edge in enumerate(graph[vertex]):
                if edge.capacity <= 0:
                    continue
                new_distance = current_distance + edge.cost
                target_distance = distances[edge.to]
                if target_distance is None or new_distance < target_distance:
                    distances[edge.to] = new_distance
                    previous_vertex[edge.to] = vertex
                    previous_edge[edge.to] = edge_index
                    if not in_queue[edge.to]:
                        queue.append(edge.to)
                        in_queue[edge.to] = True

        if distances[sink] is None:
            break

        vertex = sink
        while vertex != source:
            previous = previous_vertex[vertex]
            edge_index = previous_edge[vertex]
            edge = graph[previous][edge_index]
            edge.capacity -= 1
            graph[vertex][edge.reverse].capacity += 1
            vertex = previous

        flow += 1

    assigned_by_slot: dict[int, dict[str, Any]] = {}
    for slot_index, edge, candidate in slot_edges:
        if edge.capacity == 0:
            assigned_by_slot[slot_index] = candidate

    for slot_index, candidate in assigned_by_slot.items():
        device_key, _ = slots[slot_index]
        result[device_key].append(candidate)

    for device_key in device_keys:
        result[device_key].sort(
            key=lambda candidate: (
                int(candidate.get("rank") or 0),
                _key(candidate.get("sample_id")),
            )
        )

    return result


def allocate_candidates(
    device_order: list[Any],
    quotas: Any,
    candidates: list[dict[str, Any]],
) -> dict[Any, list[dict[str, Any]]]:
    """
    Pure allocation helper for callers with bounded candidates.
    """

    device_keys = [_key(device_id) for device_id in device_order]
    quota_map: dict[str, int] = {}
    for index, device_id in enumerate(device_order):
        if isinstance(quotas, dict):
            value = quotas.get(device_id)
            if value is None:
                value = quotas.get(device_keys[index], 1)
        elif isinstance(quotas, (list, tuple)):
            value = quotas[index] if index < len(quotas) else 1
        else:
            value = quotas
        quota_map[device_keys[index]] = max(0, int(value or 0))

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: dict[str, set[str]] = defaultdict(set)
    for candidate in candidates:
        device_id = candidate.get("device_id")
        file_id = candidate.get("file_id")
        device_key = _key(device_id)
        file_key = _key(file_id)
        if file_key in seen[device_key]:
            continue
        seen[device_key].add(file_key)
        grouped[device_key].append(
            {
                **candidate,
                "device_key": device_key,
                "file_key": file_key,
            }
        )

    for device_key in device_keys:
        grouped[device_key].sort(
            key=lambda item: (
                item.get("rank") is None,
                item.get("rank") if item.get("rank") is not None else 0,
                item.get("sample_time") is None,
                item.get("sample_time"),
                _key(item.get("sample_id")),
            )
        )

    keyed_result = _min_cost_allocation(device_keys, quota_map, grouped)
    return {
        device_order[index]: keyed_result[device_keys[index]]
        for index in range(len(device_order))
    }


async def _resolve_quotas(
    query: Select,
    device_ids: list[Any],
    allocation: Any,
    device_table: type[Any],
    device_pk: Column,
    response: list[dict[str, Any]],
    m2m_name: str,
) -> list[int]:
    quota_value = getattr(allocation, "quota", None)
    if quota_value is None:
        quota_value = _config_value(allocation, _QUOTA_NAMES, 1)

    quota_provided = getattr(allocation, "quota_provided", None)
    if quota_provided is None:
        quota_provided = quota_value is not None

    if isinstance(quota_value, dict):
        result = []
        for device_id in device_ids:
            value = quota_value.get(device_id)
            if value is None:
                value = quota_value.get(_key(device_id))
            if value is None:
                for key, candidate in quota_value.items():
                    if str(key) == str(device_id):
                        value = candidate
                        break
            result.append(max(0, int(value or 0)))
        return result

    if isinstance(quota_value, (list, tuple)):
        return [
            (
                max(0, int(quota_value[index] or 0))
                if index < len(quota_value)
                else 1
            )
            for index in range(len(device_ids))
        ]

    if callable(quota_value) and not isinstance(quota_value, Column):
        quotas = []
        for index, device_id in enumerate(device_ids):
            row = response[index] if index < len(response) else {}
            try:
                value = quota_value(row)
            except TypeError:
                value = quota_value(device_id)
            quotas.append(max(0, int(value or 0)))
        return quotas

    quota_column = _resolve_column(
        quota_value,
        device_table,
        (
            "review_quota",
            "submission_quota",
            "sample_quota",
            "quota",
        ),
    )

    if quota_column is None and not quota_provided:
        quota_column = _resolve_column(
            None,
            device_table,
            (
                "review_quota",
                "submission_quota",
                "sample_quota",
                "quota",
            ),
        )

    if quota_column is None:
        try:
            return [max(0, int(quota_value or 0))] * len(device_ids)
        except (TypeError, ValueError):
            return [1] * len(device_ids)

    quota_alias = "_piccolo_allocation_quota"
    rows = (
        await device_table.select(
            device_pk.as_alias(_MAPPING_ALIAS),
            quota_column.as_alias(quota_alias),
        )
        .where(device_pk.is_in(device_ids))
        .run()
    )
    quota_by_id = {
        _key(row[_MAPPING_ALIAS]): row.get(quota_alias) for row in rows
    }
    return [
        max(0, int(quota_by_id.get(_key(device_id), 1) or 0))
        for device_id in device_ids
    ]


def _condition_fragment(
    allocation: Any,
    join_table: type[Any],
) -> tuple[str, list[Any]]:
    eligible = getattr(allocation, "eligible", None)
    if eligible is None:
        eligible = _config_value(allocation, _ELIGIBLE_NAMES, None)

    eligible_value = _config_value(
        allocation,
        ("eligible_value", "reviewable_value", "value"),
        True,
    )

    if eligible is None or eligible is True:
        column = _resolve_column(
            None,
            join_table,
            (
                "is_reviewable",
                "reviewable",
                "is_submittable",
                "submittable",
                "can_submit",
                "eligible",
                "is_eligible",
            ),
        )
        if column is None:
            return "", []
        return f'AND "{column._meta.db_column_name}" = {{}}', [eligible_value]

    if eligible is False:
        return "", []

    if isinstance(eligible, Column):
        return f'AND "{eligible._meta.db_column_name}" = {{}}', [
            eligible_value
        ]

    querystring = getattr(eligible, "querystring", eligible)
    if hasattr(querystring, "template"):
        return "AND {}", [querystring]

    if isinstance(eligible, str):
        column = _resolve_column(eligible, join_table)
        if column is not None:
            return f'AND "{column._meta.db_column_name}" = {{}}', [
                eligible_value
            ]
        return f"AND ({eligible})", []

    return "", []


def _time_fragment(
    allocation: Any, join_table: type[Any]
) -> tuple[str, list[Any]]:
    time_value = getattr(allocation, "sample_time", None)
    if time_value is None:
        time_value = _config_value(allocation, _SAMPLE_TIME_NAMES, None)

    if time_value is None:
        column = _resolve_column(
            None,
            join_table,
            ("sampled_at", "sample_time", "collected_at", "created_at"),
        )
        if column is None:
            return "NULL", []
        return f'"{column._meta.db_column_name}"', []

    if isinstance(time_value, Column):
        return f'"{time_value._meta.db_column_name}"', []

    if isinstance(time_value, (list, tuple)):
        for order_column in time_value:
            if isinstance(order_column, Column):
                return (
                    f'"{order_column._meta.db_column_name}"',
                    [],
                )
            querystring = getattr(order_column, "querystring", None)
            if querystring is not None:
                return "{}", [querystring]
        return "NULL", []

    querystring = getattr(time_value, "querystring", None)
    if querystring is not None:
        return "{}", [querystring]

    column = _resolve_column(time_value, join_table)
    if column is not None:
        return f'"{column._meta.db_column_name}"', []

    if isinstance(time_value, str):
        return f"({time_value})", []

    return "NULL", []


async def _fetch_candidates(
    query: Select,
    m2m_select: M2MSelect,
    device_ids: list[Any],
    candidate_limit: int,
) -> list[dict[str, Any]]:
    allocation = m2m_select.allocation
    m2m = m2m_select.m2m
    join_table = m2m._meta.resolved_joining_table
    join_pk = join_table._meta.primary_key
    device_fk = (
        _resolve_column(
            _config_value(allocation, _DEVICE_NAMES, None),
            join_table,
            (),
        )
        or m2m._meta.primary_foreign_key
    )
    file_fk = (
        _resolve_column(
            _config_value(allocation, _FILE_NAMES, None),
            join_table,
            (),
        )
        or m2m._meta.secondary_foreign_key
    )

    condition_sql, condition_args = _condition_fragment(allocation, join_table)
    time_sql, time_args = _time_fragment(allocation, join_table)

    id_placeholders = ", ".join("{}" for _ in device_ids)
    sql = f"""
        WITH eligible AS (
            SELECT
                "{join_pk._meta.db_column_name}"
                    AS _alloc_sample_id,
                "{device_fk._meta.db_column_name}"
                    AS _alloc_device_id,
                "{file_fk._meta.db_column_name}"
                    AS _alloc_file_id,
                {time_sql} AS _alloc_sample_time
            FROM {join_table._meta.get_formatted_tablename()}
            WHERE "{device_fk._meta.db_column_name}"
                IN ({id_placeholders})
            {condition_sql}
        ),
        first_file AS (
            SELECT
                eligible.*,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        _alloc_device_id,
                        _alloc_file_id
                    ORDER BY
                        _alloc_sample_time ASC,
                        _alloc_sample_id ASC
                ) AS _alloc_file_rank
            FROM eligible
        ),
        first_files AS (
            SELECT *
            FROM first_file
            WHERE _alloc_file_rank = 1
        ),
        ranked AS (
            SELECT
                first_files.*,
                ROW_NUMBER() OVER (
                    PARTITION BY _alloc_device_id
                    ORDER BY
                        _alloc_sample_time ASC,
                        _alloc_sample_id ASC
                ) AS _alloc_rank
            FROM first_files
        )
        SELECT
            _alloc_sample_id AS sample_id,
            _alloc_device_id AS device_id,
            _alloc_file_id AS file_id,
            _alloc_sample_time AS sample_time,
            _alloc_rank AS rank
        FROM ranked
        WHERE _alloc_rank <= {{}}
        ORDER BY _alloc_device_id, _alloc_rank
    """

    args: list[Any] = [
        *device_ids,
        *time_args,
        *condition_args,
        candidate_limit,
    ]
    rows = await query.table.raw(sql, *args).run()
    return rows


async def _fetch_projection_values(
    m2m_select: M2MSelect,
    sample_ids: list[Any],
    file_ids: list[Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    m2m = m2m_select.m2m
    join_table = m2m._meta.resolved_joining_table
    file_table = m2m._meta.secondary_table
    join_pk = join_table._meta.primary_key
    file_pk = file_table._meta.primary_key

    join_columns: list[Column] = []
    file_columns: list[Column] = []
    for column in m2m_select.columns:
        if not isinstance(column, Column):
            continue
        root_table = _root_table(column)
        if root_table is join_table:
            join_columns.append(column)
        elif root_table is file_table:
            file_columns.append(column)
        else:
            # Related columns are safest when selected from their root table.
            if column._meta.call_chain:
                root = root_table or join_table
                join_columns.append(column)
            else:
                file_columns.append(column)

    load_json = bool(
        getattr(m2m_select.allocation, "load_json", True)
        or m2m_select.load_json
    )

    join_map: dict[str, dict[str, Any]] = {}
    if join_columns and sample_ids:
        rows = (
            await join_table.select(
                *join_columns,
                join_pk.as_alias(_MAPPING_ALIAS),
            )
            .where(join_pk.is_in(sample_ids))
            .output(load_json=load_json)
            .run()
        )
        join_map = {
            _key(row[_MAPPING_ALIAS]): {
                key: value
                for key, value in row.items()
                if key != _MAPPING_ALIAS
            }
            for row in rows
        }

    file_map: dict[str, dict[str, Any]] = {}
    if file_columns and file_ids:
        rows = (
            await file_table.select(
                *file_columns,
                file_pk.as_alias(_MAPPING_ALIAS),
            )
            .where(file_pk.is_in(file_ids))
            .output(load_json=load_json)
            .run()
        )
        file_map = {
            _key(row[_MAPPING_ALIAS]): {
                key: value
                for key, value in row.items()
                if key != _MAPPING_ALIAS
            }
            for row in rows
        }

    return join_map, file_map


async def allocate_m2m_response(
    query: Select,
    response: list[dict[str, Any]],
    m2m_select: M2MSelect,
) -> list[dict[str, Any]]:
    m2m_name = m2m_select.m2m._meta.name
    device_ids: list[Any] = []
    for row in response:
        value = row.get(m2m_name)
        if isinstance(value, (list, tuple)):
            value = value[0] if value else None
        device_ids.append(value)

    valid_positions = [
        index for index, value in enumerate(device_ids) if value is not None
    ]
    ordered_device_ids = [device_ids[index] for index in valid_positions]

    if not ordered_device_ids:
        for row in response:
            row[m2m_name] = []
        return response

    device_table = m2m_select.m2m._meta.primary_table
    device_pk = device_table._meta.primary_key
    quotas = await _resolve_quotas(
        query,
        ordered_device_ids,
        m2m_select.allocation,
        device_table,
        device_pk,
        response,
        m2m_name,
    )
    quota_by_device = {
        _key(device_id): quota
        for device_id, quota in zip(ordered_device_ids, quotas)
    }
    total_quota = sum(quotas)

    if total_quota <= 0:
        for row in response:
            row[m2m_name] = []
        return response

    config_limit = getattr(m2m_select.allocation, "candidate_limit", None)
    candidate_limit = max(1, int(config_limit or total_quota))

    candidate_rows = await _fetch_candidates(
        query,
        m2m_select,
        ordered_device_ids,
        candidate_limit,
    )

    candidates_by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidate_rows:
        device_key = _key(row["device_id"])
        file_key = _key(row["file_id"])
        candidates_by_key[device_key].append(
            {
                "sample_id": row["sample_id"],
                "device_id": row["device_id"],
                "file_id": row["file_id"],
                "sample_time": row.get("sample_time"),
                "rank": row.get("rank"),
                "device_key": device_key,
                "file_key": file_key,
            }
        )

    device_keys = [_key(value) for value in ordered_device_ids]
    assigned_by_key = _min_cost_allocation(
        device_keys, quota_by_device, candidates_by_key
    )

    selected: list[dict[str, Any]] = []
    for device_key in device_keys:
        for candidate in assigned_by_key.get(device_key, []):
            selected.append(candidate)

    sample_ids = [candidate["sample_id"] for candidate in selected]
    file_ids = [candidate["file_id"] for candidate in selected]
    join_map, file_map = await _fetch_projection_values(
        m2m_select, sample_ids, file_ids
    )

    values_by_device: dict[str, list[Any]] = {
        device_key: [] for device_key in device_keys
    }
    for device_key in device_keys:
        for candidate in assigned_by_key.get(device_key, []):
            item: dict[str, Any] = {}
            item.update(join_map.get(_key(candidate["sample_id"]), {}))
            item.update(file_map.get(_key(candidate["file_id"]), {}))

            if m2m_select.as_list:
                column = m2m_select.columns[0]
                item = item.get(_projection_key(column))  # type: ignore
            values_by_device[device_key].append(item)

    for index, row in enumerate(response):
        device_id = device_ids[index]
        row[m2m_name] = (
            values_by_device.get(_key(device_id), [])
            if device_id is not None
            else []
        )

    return response
