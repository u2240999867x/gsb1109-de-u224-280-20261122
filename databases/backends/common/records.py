import re
import typing
from collections.abc import (
    ItemsView as ABCItemsView,
    Mapping as MappingABC,
    ValuesView as ABCValuesView,
)

from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.engine.row import Row as SQLRow
from sqlalchemy.exc import (
    AmbiguousColumnError,
    MultipleResultsFound,
    NoSuchColumnError,
)
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import TypeEngine

from databases.interfaces import Record as RecordInterface

DIALECT_EXCLUDE = {"postgresql"}

_AS_ALIAS_PATTERN = re.compile(
    r"\bAS\s+(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|"
    r"([A-Za-z_][A-Za-z0-9_$]*))",
    re.IGNORECASE,
)


class AmbiguousColumnKeyError(
    AmbiguousColumnError, MultipleResultsFound, KeyError
):
    pass


def _raise_ambiguous(
    key: typing.Any, indexes: typing.Sequence[int]
) -> typing.NoReturn:
    raise AmbiguousColumnKeyError(
        f"Ambiguous column {key!r}; matches more than one result "
        f"column at indexes {list(indexes)}"
    )


def _raise_missing(key: typing.Any) -> typing.NoReturn:
    raise NoSuchColumnError(
        f"Could not locate column {key!r}; column not found in result row"
    )


def _result_column_parts(entry: typing.Any) -> typing.Tuple[typing.Any, ...]:
    return tuple(entry)


def _entry_aliases(entry: typing.Any) -> typing.List[str]:
    parts = _result_column_parts(entry)
    objects = parts[2] if len(parts) > 2 else ()
    aliases: typing.List[str] = []

    for obj in objects:
        expression = getattr(obj, "text", None)
        if expression is None and isinstance(obj, str):
            expression = obj
        if not isinstance(expression, str):
            continue

        for match in _AS_ALIAS_PATTERN.finditer(expression):
            alias = next((group for group in match.groups() if group is not None), None)
            if alias is not None:
                aliases.append(alias)

    return aliases


def _entry_text(entry: typing.Any) -> str:
    parts = _result_column_parts(entry)
    objects = parts[2] if len(parts) > 2 else ()
    fragments: typing.List[str] = []

    for obj in objects:
        expression = getattr(obj, "text", None)
        if expression is None and isinstance(obj, str):
            expression = obj
        if isinstance(expression, str):
            fragments.append(expression)

    return "\n".join(fragments)


def _name_appears_in_text(name: str, expression: str) -> bool:
    pattern = rf"(?<![A-Za-z0-9_$]){re.escape(name)}(?![A-Za-z0-9_$])"
    return re.search(pattern, expression, re.IGNORECASE) is not None


def _find_actual_name(
    names: typing.Sequence[str], name: str, start: int
) -> typing.Optional[int]:
    for index in range(start, len(names)):
        if names[index] == name:
            return index
    return None


def _align_result_columns(
    result_columns: typing.Sequence[typing.Any],
    actual_names: typing.Sequence[typing.Optional[str]],
) -> typing.List[typing.Tuple[int, ...]]:
    if not actual_names:
        return [(index,) for index in range(len(result_columns))]

    if len(result_columns) == len(actual_names):
        return [(index,) for index in range(len(result_columns))]

    positions: typing.List[typing.Tuple[int, ...]] = []
    current = 0

    for entry in result_columns:
        parts = _result_column_parts(entry)
        key_name = parts[0] if len(parts) > 0 else None
        rendered_name = parts[1] if len(parts) > 1 else None
        singleton_name = rendered_name if rendered_name is not None else key_name

        if singleton_name is not None:
            if current >= len(actual_names):
                return [(index,) for index in range(len(result_columns))]
            positions.append((current,))
            current += 1
            continue

        expression = _entry_text(entry)
        anchor_names = set(_entry_aliases(entry))
        for name in actual_names[current:]:
            if name is not None and _name_appears_in_text(name, expression):
                anchor_names.add(name)

        end: typing.Optional[int] = None
        for alias in anchor_names:
            alias_index = _find_actual_name(actual_names, alias, current)
            if alias_index is not None:
                end = alias_index if end is None else max(end, alias_index)

        if end is None or end < current:
            end = min(current, len(actual_names) - 1)

        if end < 0:
            return [(index,) for index in range(len(result_columns))]

        positions.append(tuple(range(current, end + 1)))
        current = end + 1

    if current != len(actual_names):
        return [(index,) for index in range(len(result_columns))]

    for entry, entry_positions in zip(result_columns, positions):
        parts = _result_column_parts(entry)
        rendered_name = parts[1] if len(parts) > 1 else None
        if rendered_name is None or len(entry_positions) != 1:
            continue
        index = entry_positions[0]
        if index >= len(actual_names) or actual_names[index] != rendered_name:
            return [(index,) for index in range(len(result_columns))]

    return positions


def _projection_items(projection: typing.Any) -> typing.List[typing.Any]:
    if projection is None:
        return []

    inner_columns = getattr(projection, "inner_columns", None)
    if inner_columns is not None:
        projection = inner_columns

    try:
        return list(projection)
    except TypeError:
        return []


def _object_roots(key: typing.Any) -> typing.Iterator[typing.Any]:
    seen: typing.Set[int] = set()
    current = key

    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current

        clause_element = getattr(current, "__clause_element__", None)
        if callable(clause_element):
            try:
                clause = clause_element()
            except Exception:  # pragma: no cover - defensive SQLAlchemy hook
                clause = None
            if clause is not None and id(clause) not in seen:
                current = clause
                continue

        deannotate = getattr(current, "_deannotate", None)
        if callable(deannotate):
            try:
                deannotated = deannotate()
            except Exception:  # pragma: no cover - defensive SQLAlchemy hook
                deannotated = None
            if deannotated is not None and deannotated is not current:
                current = deannotated
                continue

        break


class ColumnResolver:
    __slots__ = (
        "names",
        "types",
        "name_indexes",
        "object_indexes",
        "_object_refs",
    )

    def __init__(
        self,
        result_columns: typing.Sequence[typing.Any] = (),
        actual_names: typing.Optional[typing.Sequence[typing.Optional[str]]] = None,
        projection: typing.Any = None,
    ) -> None:
        result_columns = tuple(result_columns or ())

        if actual_names is None:
            names = tuple(
                (entry[1] if len(entry) > 1 and entry[1] is not None else entry[0])
                for entry in result_columns
            )
        else:
            names = tuple(actual_names)

        self.names = names
        self.types: typing.List[TypeEngine] = [NullType() for _ in names]
        self.name_indexes: typing.Dict[str, typing.List[int]] = {}
        self.object_indexes: typing.Dict[int, typing.List[int]] = {}
        self._object_refs: typing.List[typing.Any] = []

        for index, name in enumerate(names):
            if name is not None:
                self.name_indexes.setdefault(str(name), []).append(index)

        positions = _align_result_columns(result_columns, names)
        projection_items = _projection_items(projection)
        entry_object_ids: typing.List[typing.List[int]] = []

        for entry_index, entry in enumerate(result_columns):
            parts = _result_column_parts(entry)
            objects = parts[2] if len(parts) > 2 else ()
            datatype = parts[3] if len(parts) > 3 else NullType()
            entry_positions = (
                positions[entry_index]
                if entry_index < len(positions)
                else ()
            )

            for position in entry_positions:
                if 0 <= position < len(self.types):
                    self.types[position] = datatype

            object_ids: typing.List[int] = []
            candidates = list(objects)
            if entry_index < len(projection_items):
                candidates.append(projection_items[entry_index])

            for candidate in candidates:
                if not isinstance(candidate, ColumnElement):
                    continue
                for root in _object_roots(candidate):
                    object_id = self._register_object(root)
                    if object_id not in object_ids:
                        object_ids.append(object_id)
                    for position in entry_positions:
                        self._add_object_index(object_id, position)

            entry_object_ids.append(object_ids)

        for entry_index, entry in enumerate(result_columns):
            if entry_object_ids[entry_index]:
                continue
            positions_for_entry = (
                positions[entry_index]
                if entry_index < len(positions)
                else ()
            )
            if len(positions_for_entry) != 1:
                continue

            parts = _result_column_parts(entry)
            entry_name = parts[1] if len(parts) > 1 else parts[0]
            datatype = parts[3] if len(parts) > 3 else None
            position = positions_for_entry[0]

            for previous_index in range(entry_index):
                previous_parts = _result_column_parts(result_columns[previous_index])
                previous_name = (
                    previous_parts[1]
                    if len(previous_parts) > 1
                    else previous_parts[0]
                )
                previous_type = previous_parts[3] if len(previous_parts) > 3 else None
                if previous_name != entry_name or previous_type is not datatype:
                    continue
                for object_id in entry_object_ids[previous_index]:
                    self._add_object_index(object_id, position)
                    entry_object_ids[entry_index].append(object_id)
                break

    def _register_object(self, obj: typing.Any) -> int:
        object_id = id(obj)
        if object_id not in self.object_indexes:
            self.object_indexes[object_id] = []
            self._object_refs.append(obj)
        return object_id

    def _add_object_index(self, object_id: int, index: int) -> None:
        indexes = self.object_indexes.setdefault(object_id, [])
        if index not in indexes:
            indexes.append(index)

    def resolve_index(self, key: int) -> typing.Tuple[int, TypeEngine]:
        length = len(self.names) or len(self.types)
        if key < 0:
            key += length
        if key < 0 or key >= length:
            raise IndexError(f"Result column index {key} is out of range")
        return key, self.types[key]

    def resolve_name(self, key: str) -> typing.Tuple[int, TypeEngine]:
        indexes = self.name_indexes.get(key)
        if not indexes:
            _raise_missing(key)
        if len(indexes) > 1:
            _raise_ambiguous(key, indexes)
        index = indexes[0]
        return index, self.types[index]

    def resolve_object(self, key: typing.Any) -> typing.Tuple[int, TypeEngine]:
        indexes: typing.List[int] = []
        for root in _object_roots(key):
            object_indexes = self.object_indexes.get(id(root))
            if object_indexes:
                indexes.extend(object_indexes)

        indexes = list(dict.fromkeys(indexes))
        if not indexes:
            _raise_missing(key)
        if len(indexes) > 1:
            _raise_ambiguous(key, indexes)

        index = indexes[0]
        return index, self.types[index]

    def processors(
        self, dialect: Dialect
    ) -> typing.List[typing.Optional[typing.Callable]]:
        return [
            None
            if datatype is None
            else datatype._cached_result_processor(dialect, None)
            for datatype in self.types
        ]


class ColumnMaps(tuple):
    resolver: ColumnResolver

    def __new__(cls, resolver: ColumnResolver) -> "ColumnMaps":
        column_map: typing.Dict[typing.Any, typing.Tuple[int, TypeEngine]] = {}
        column_map_int: typing.Dict[int, typing.Tuple[int, TypeEngine]] = {}
        column_map_full: typing.Dict[typing.Any, typing.Tuple[int, TypeEngine]] = {}

        for name, indexes in resolver.name_indexes.items():
            index = indexes[-1]
            column_map[name] = (index, resolver.types[index])

        for index, datatype in enumerate(resolver.types):
            column_map_int[index] = (index, datatype)

        for object_id, indexes in resolver.object_indexes.items():
            index = indexes[-1]
            obj = next(
                (
                    item
                    for item in resolver._object_refs
                    if id(item) == object_id
                ),
                None,
            )
            if obj is not None:
                column_map_full[obj] = (index, resolver.types[index])
                column_map_full[str(obj)] = (index, resolver.types[index])

        instance = super().__new__(
            cls, (column_map, column_map_int, column_map_full)
        )
        instance.resolver = resolver
        return instance


class _RecordValuesView(ABCValuesView):
    def __iter__(self) -> typing.Iterator[typing.Any]:
        record = self._mapping._record
        for index in range(len(record)):
            yield record[index]

    def __len__(self) -> int:
        return len(self._mapping._record)

    def __contains__(self, value: object) -> bool:
        return any(item == value for item in self)


class _RecordItemsView(ABCItemsView):
    def __iter__(self) -> typing.Iterator[typing.Tuple[typing.Any, typing.Any]]:
        record = self._mapping._record
        for index in range(len(record)):
            yield record._key_at_index(index), record[index]

    def __len__(self) -> int:
        return len(self._mapping._record)


class RecordMapping(MappingABC):
    __slots__ = ("_record",)

    def __init__(self, record: "Record") -> None:
        self._record = record

    def __getitem__(self, key: typing.Any) -> typing.Any:
        return self._record[key]

    def __iter__(self) -> typing.Iterator[typing.Any]:
        for index in range(len(self._record)):
            yield self._record._key_at_index(index)

    def __len__(self) -> int:
        return len(self._record)

    def get(self, key: typing.Any, default: typing.Any = None) -> typing.Any:
        try:
            return self[key]
        except AmbiguousColumnError:
            raise
        except KeyError:
            return default

    def values(self) -> ABCValuesView:
        return _RecordValuesView(self)

    def items(self) -> ABCItemsView:
        return _RecordItemsView(self)


class Record(RecordInterface):
    __slots__ = (
        "_row",
        "_result_columns",
        "_dialect",
        "_column_map",
        "_column_map_int",
        "_column_map_full",
        "_resolver",
    )

    def __init__(
        self,
        row: typing.Any,
        result_columns: tuple,
        dialect: Dialect,
        column_maps: typing.Tuple[
            typing.Mapping[typing.Any, typing.Tuple[int, TypeEngine]],
            typing.Mapping[int, typing.Tuple[int, TypeEngine]],
            typing.Mapping[str, typing.Tuple[int, TypeEngine]],
        ],
        projection: typing.Any = None,
    ) -> None:
        self._row = row
        self._result_columns = result_columns
        self._dialect = dialect
        self._column_map, self._column_map_int, self._column_map_full = column_maps

        actual_names = _row_column_names(row)
        passed_resolver = getattr(column_maps, "resolver", None)
        if (
            projection is None
            and passed_resolver is not None
            and (not actual_names or tuple(passed_resolver.names) == actual_names)
        ):
            self._resolver = passed_resolver
        else:
            self._resolver = ColumnResolver(
                result_columns=result_columns,
                actual_names=actual_names,
                projection=projection,
            )

    @property
    def _mapping(self) -> typing.Mapping:
        return RecordMapping(self)

    def keys(self) -> typing.KeysView:
        return self._mapping.keys()

    def values(self) -> typing.ValuesView:
        return self._mapping.values()

    def _key_at_index(self, index: int) -> typing.Any:
        names = self._resolver.names
        if 0 <= index < len(names) and names[index] is not None:
            return names[index]
        return index

    def __getitem__(self, key: typing.Any) -> typing.Any:
        if isinstance(key, slice):
            return self._row[key]

        if isinstance(key, int):
            idx, datatype = self._resolver.resolve_index(key)
        elif isinstance(key, ColumnElement):
            idx, datatype = self._resolver.resolve_object(key)
        elif hasattr(key, "__clause_element__"):
            idx, datatype = self._resolver.resolve_object(key)
        else:
            idx, datatype = self._resolver.resolve_name(key)

        raw = self._row[idx]
        processor = datatype._cached_result_processor(self._dialect, None)

        if (
            self._dialect.name in DIALECT_EXCLUDE
            and processor is not None
            and isinstance(raw, (int, str, float))
            and not isinstance(raw, bool)
        ):
            return processor(raw)

        return raw

    def __iter__(self) -> typing.Iterator:
        return iter(self._row.keys())

    def __len__(self) -> int:
        return len(self._row)

    def __getattr__(self, name: str) -> typing.Any:
        try:
            return self.__getitem__(name)
        except AmbiguousColumnError:
            raise
        except KeyError as e:
            raise AttributeError(e.args[0]) from e


def _row_column_names(row: typing.Any) -> typing.Tuple[typing.Optional[str], ...]:
    keys = getattr(row, "keys", None)
    if not callable(keys):
        return ()

    try:
        raw_keys = list(keys())
    except TypeError:
        return ()

    return tuple(str(key) if key is not None else None for key in raw_keys)


class Row(SQLRow):
    def __getitem__(self, key: typing.Any) -> typing.Any:
        """
        An instance of a Row in SQLAlchemy allows the access
        to the Row._fields as tuple and the Row._mapping for
        the values.
        """
        if isinstance(key, int):
            return super().__getitem__(key)

        key_map = self._key_to_index
        if key not in key_map:
            raise NoSuchColumnError(f"Could not locate column {key!r} in result row")

        mapped = key_map[key]
        idx = mapped[0] if isinstance(mapped, tuple) else mapped
        if idx is None:
            indexes = [
                entry[0]
                for entry in key_map.values()
                if isinstance(entry, tuple) and entry[0] is not None
            ]
            _raise_ambiguous(key, indexes)

        return super().__getitem__(idx)

    def keys(self):
        return self._mapping.keys()

    def values(self):
        return self._mapping.values()


def create_column_maps(
    result_columns: typing.Any,
    actual_names: typing.Optional[typing.Sequence[typing.Optional[str]]] = None,
    projection: typing.Any = None,
) -> ColumnMaps:
    """
    Generate column -> datatype mappings from the column definitions.

    These mappings are used throughout PostgresConnection methods
    to initialize Record-s. The underlying DB driver does not do type
    conversion for us so we have wrap the returned asyncpg.Record-s.

    :return: Three mappings from different ways to address a column, \
                wrapped in a tuple that also exposes a ColumnResolver.
    """
    resolver = ColumnResolver(result_columns, actual_names, projection)
    return ColumnMaps(resolver)
