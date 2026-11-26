"""Tablib - SQL Export Support."""

__lazy_modules__ = {
    "datetime",
    "decimal",
    "tablib.exceptions",
}

import datetime
import decimal

from ..exceptions import TablibWorkOrderError, UnsupportedFormat

WORK_ORDER_HEADERS = ('工单号', '前单号', '站点', '原读数', '新读数')
DEFAULT_STATION_TABLE = '测站 A'
DEFAULT_RECEIPT_TABLE = '工单回执'
DEFAULT_START_MARKER = '起点'
_FAIL_TABLE = 'tablib_wo_fail'
_FAIL_TRIGGER = 'tablib_wo_fail_raise'
_SAVEPOINT = 'tablib_wo'


class SQLFormat:
    """Export Dataset rows as SQL statements."""
    title = 'sql'
    extensions = ('sql',)

    @staticmethod
    def _render_literal(value):
        """Render a Python value as an SQL literal."""
        if value is None:
            return 'NULL'
        if isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        if isinstance(value, (int, decimal.Decimal)):
            return str(value)
        if isinstance(value, float):
            # Represent finite floats; non-finite as NULL
            try:
                if value == value and value not in (float('inf'), -float('inf')):
                    return repr(value)
            except Exception:
                pass
            return 'NULL'
        if isinstance(value, datetime.datetime):
            # ANSI SQL timestamp literal
            return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
        if isinstance(value, datetime.date):
            # ANSI SQL date literal
            return f"DATE '{value.isoformat()}'"
        # Fallback for strings and others
        text = str(value).replace("'", "''")
        return f"'{text}'"

    @staticmethod
    def _quote_identifier(name):
        """Quote an SQL identifier, preserving embedded spaces and dots."""
        return '"' + str(name).replace('"', '""') + '"'

    # ----------------
    # Regular INSERTs
    # ----------------

    @classmethod
    def _export_inserts(cls, dataset, table, columns, commit):
        tbl = table or getattr(dataset, 'title', None) or 'export_table'
        tbl_ident = str(tbl)
        columns_headers = (','.join(
                columns if columns is not None else
                dataset.headers if dataset.headers is not None else []
            )
        )
        columns_headers = f' ({columns_headers})' if columns_headers else ''
        statements = []
        for row in dataset._data:
            values = ', '.join(cls._render_literal(v) for v in row)
            statements.append(
                f'INSERT INTO {tbl_ident}{columns_headers} VALUES ({values});'
            )
        return '\n'.join(statements) + '\n' + ('COMMIT;\n' if commit else '')

    # ---------------------------
    # SQLite work order (工单) export
    # ---------------------------

    @staticmethod
    def _collect_work_orders(dataset):
        """Group dataset rows into work orders, validating shape and content."""
        if dataset.headers is not None and tuple(dataset.headers) != WORK_ORDER_HEADERS:
            raise TablibWorkOrderError(
                f"SQLite 工单导出需要列 {list(WORK_ORDER_HEADERS)}，当前列为 {list(dataset.headers)}"
            )
        if dataset.width != 5:
            raise TablibWorkOrderError(
                f"SQLite 工单导出每行需要 5 列（工单号,前单号,站点,原读数,新读数），当前为 {dataset.width} 列"
            )
        if dataset.height == 0:
            raise TablibWorkOrderError('SQLite 工单导出需要至少一张工单')

        orders = []
        index = {}
        for row_no, row in enumerate(dataset._data, start=1):
            order_no, prev_no, station, old_value, new_value = list(row)
            if not isinstance(order_no, str) or not order_no:
                raise TablibWorkOrderError(f'第 {row_no} 行工单号必须是非空字符串')
            if not isinstance(prev_no, str) or not prev_no:
                raise TablibWorkOrderError(f'第 {row_no} 行前单号必须是非空字符串')
            if not isinstance(station, str) or not station:
                raise TablibWorkOrderError(f'第 {row_no} 行站点必须是非空字符串')

            if order_no not in index:
                index[order_no] = len(orders)
                orders.append({'no': order_no, 'prev': prev_no, 'lines': []})
            order = orders[index[order_no]]
            if order['prev'] != prev_no:
                raise TablibWorkOrderError(
                    f"工单 {order_no} 的前单号不一致：{order['prev']!r} 与 {prev_no!r}"
                )
            if any(line['station'] == station for line in order['lines']):
                raise TablibWorkOrderError(f'工单 {order_no} 中站点 {station} 出现了多次')
            order['lines'].append({
                'station': station,
                'old': old_value,
                'new': new_value,
            })
        return orders

    @classmethod
    def _work_order_content(cls, lines):
        """Canonical, rerun-stable description stored in the receipt's 内容 column."""
        parts = []
        for line in lines:
            parts.append(
                f"{line['station']}={cls._render_literal(line['old'])}"
                f"->{cls._render_literal(line['new'])}"
            )
        return ';'.join(parts)

    @classmethod
    def _fail_insert(cls, message, condition):
        """A guarded insert that raises via the temp fail trigger when condition holds."""
        return (
            f"INSERT INTO temp.{_FAIL_TABLE}(消息) "
            f"SELECT {cls._render_literal(message)} WHERE {condition};"
        )

    @classmethod
    def _export_work_orders(cls, dataset, station_table, receipt_table, start):
        if not isinstance(start, str) or not start:
            raise TablibWorkOrderError('起点标记必须是非空字符串')
        orders = cls._collect_work_orders(dataset)
        station_ident = cls._quote_identifier(station_table)
        receipt_ident = cls._quote_identifier(receipt_table)

        lines = [
            '-- tablib SQLite 工单导出；执行器在首个 SQL 错误处停止，连接无需预先打开事务。',
            f'CREATE TEMP TABLE IF NOT EXISTS temp.{_FAIL_TABLE} (消息 TEXT NOT NULL);',
            f'CREATE TEMP TRIGGER IF NOT EXISTS {_FAIL_TRIGGER}',
            f'BEFORE INSERT ON temp.{_FAIL_TABLE}',
            'BEGIN',
            '    SELECT RAISE(ROLLBACK, NEW.消息);',
            'END;',
            f'SAVEPOINT {_SAVEPOINT};',
        ]

        for order in orders:
            order_no = order['no']
            prev_no = order['prev']
            order_lines = order['lines']
            no_lit = cls._render_literal(order_no)
            prev_lit = cls._render_literal(prev_no)
            content_lit = cls._render_literal(cls._work_order_content(order_lines))

            exists = f"EXISTS(SELECT 1 FROM {receipt_ident} WHERE 工单号={no_lit})"
            not_exists = f"NOT {exists}"
            # The live tail: receipt no other receipt points at as its predecessor.
            tail = (
                "COALESCE((SELECT 工单号 FROM "
                f"{receipt_ident} r WHERE NOT EXISTS("
                f"SELECT 1 FROM {receipt_ident} q WHERE q.前单号 = r.工单号) LIMIT 1), "
                f"{cls._render_literal(start)})"
            )

            # Already booked: the receipt's predecessor and content must match the file.
            lines.append(cls._fail_insert(
                f'工单 {order_no} 的回执前单号或更正内容与文件不符，拒绝执行',
                f"{exists} AND ("
                f"(SELECT 前单号 FROM {receipt_ident} WHERE 工单号={no_lit}) IS NOT {prev_lit} "
                f"OR (SELECT 内容 FROM {receipt_ident} WHERE 工单号={no_lit}) IS NOT {content_lit})",
            ))
            # Not yet booked: it must continue from the last receipt on site.
            lines.append(cls._fail_insert(
                f'工单 {order_no} 断签：必须从现场最后一张回执继续签发，前单号应为 {prev_no}',
                f"{not_exists} AND {tail} IS NOT {prev_lit}",
            ))

            for line in order_lines:
                station = line['station']
                old_value = line['old']
                new_value = line['new']
                station_lit = cls._render_literal(station)
                old_lit = cls._render_literal(old_value)
                new_lit = cls._render_literal(new_value)
                reading = f"(SELECT 读数 FROM {station_ident} WHERE 站点={station_lit})"
                row_exists = f"EXISTS(SELECT 1 FROM {station_ident} WHERE 站点={station_lit})"

                # Verify the original reading at execution time.
                lines.append(cls._fail_insert(
                    f'工单 {order_no} 站点 {station} 不存在',
                    f"{not_exists} AND NOT {row_exists}",
                ))
                lines.append(cls._fail_insert(
                    f'工单 {order_no} 站点 {station} 原值核对失约',
                    f"{not_exists} AND {row_exists} AND {reading} IS NOT {old_lit}",
                ))

                if old_value == new_value:
                    # Patrol confirmation: the row exists and matches, never update it.
                    lines.append(cls._fail_insert(
                        f'工单 {order_no} 站点 {station} 巡查核对失约',
                        f"{not_exists} AND {row_exists} AND {reading} IS NOT {new_lit}",
                    ))
                else:
                    # Corrective update, then re-check after site triggers have run.
                    lines.append(
                        f"UPDATE {station_ident} SET 读数={new_lit} "
                        f"WHERE 站点={station_lit} AND 读数 IS {old_lit} AND {not_exists};"
                    )
                    lines.append(cls._fail_insert(
                        f'工单 {order_no} 站点 {station} 新值核对失约',
                        f"{not_exists} AND {row_exists} AND {reading} IS NOT {new_lit}",
                    ))

            # Book the receipt only after every line of this order has applied and checked.
            lines.append(
                f"INSERT INTO {receipt_ident} (工单号,前单号,内容) "
                f"SELECT {no_lit},{prev_lit},{content_lit} WHERE {not_exists};"
            )

        lines.append(f'RELEASE SAVEPOINT {_SAVEPOINT};')
        return '\n'.join(lines) + '\n'

    # ----------------
    # Format interface
    # ----------------

    @classmethod
    def export_set(cls, dataset, table=None, columns=None, commit=False,
                   work_order=False, station_table=DEFAULT_STATION_TABLE,
                   receipt_table=DEFAULT_RECEIPT_TABLE,
                   start=DEFAULT_START_MARKER):
        """
        Return SQL statements for Dataset rows.

        By default each row becomes an SQL INSERT statement. Pass ``work_order=True``
        to instead generate an idempotent SQLite work-order script that applies the
        rows (工单号,前单号,站点,原读数,新读数) against a station table and records
        receipts, rolling back the whole file on the first failed check.

        :param table: optional table name for INSERT export; defaults to the
            dataset title or 'data'
        :param work_order: emit the SQLite work-order script instead of INSERTs
        :param station_table: station table name (unique key 站点, reading column 读数)
        :param receipt_table: receipt table name (工单号 primary key, 前单号, 内容)
        :param start: predecessor marker used by the first work order
        """
        if work_order:
            return cls._export_work_orders(dataset, station_table, receipt_table, start)
        return cls._export_inserts(dataset, table, columns, commit)

    @classmethod
    def import_set(cls, dataset, in_stream, **kwargs):
        """Importing SQL is not supported."""
        raise UnsupportedFormat('SQL import is not supported.')

    @classmethod
    def detect(cls, stream):
        """Always return False: no autodetect for SQL."""
        return False
