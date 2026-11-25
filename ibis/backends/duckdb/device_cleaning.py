from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ibis import util

if TYPE_CHECKING:
    from collections.abc import Sequence

    import ibis.expr.types as ir


_DEVICE_KEYS = (
    "device_id",
    "deviceId",
    "device_number",
    "device_no",
    "device_sn",
    "device_code",
    "device_name",
    "device",
    "name",
    "id",
)
_REVISION_KEYS = (
    "revision",
    "revision_number",
    "revisionNo",
    "revision_id",
    "rev_number",
    "rev",
    "version",
)
_OPERATION_KEYS = (
    "op",
    "operation",
    "action",
    "change_type",
    "event_type",
    "record_type",
    "kind",
    "type",
)
_PERIOD_KEYS = (
    "reporting_period",
    "report_period",
    "reporting_interval",
    "report_interval",
    "report_interval_seconds",
    "reporting_interval_seconds",
    "upload_interval",
    "upload_period",
    "period_seconds",
    "period_sec",
    "period_s",
    "frequency_seconds",
    "frequency",
    "interval_seconds",
    "interval",
    "period",
)


def _json_string_expr(source: str, keys: Sequence[str]) -> str:
    return (
        "coalesce("
        + ", ".join(f"json_extract_string({source}, '$.{key}')" for key in keys)
        + ")"
    )


def _json_type_expr(source: str, keys: Sequence[str]) -> str:
    return (
        "coalesce("
        + ", ".join(f"json_type(json_extract({source}, '$.{key}'))" for key in keys)
        + ")"
    )


def _path_array(paths: str | Any | Sequence[str | Any]) -> str:
    path_list = util.promote_list(paths)
    literals = ["'" + str(path).replace("'", "''") + "'" for path in path_list]
    return "[" + ", ".join(literals) + "]"


def _quote_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _cte_sql(path_array: str, revision_filter: str) -> str:
    device_expr = _json_string_expr("raw_json", _DEVICE_KEYS)
    revision_text_expr = _json_string_expr("raw_json", _REVISION_KEYS)
    operation_expr = _json_string_expr("raw_json", _OPERATION_KEYS)
    period_text_expr = _json_string_expr("raw_json", _PERIOD_KEYS)
    period_kind_expr = _json_type_expr("raw_json", _PERIOD_KEYS)

    return f"""
WITH RECURSIVE raw_records AS (
    SELECT
        filename AS file_path,
        json AS raw_json,
        md5(json(json)::VARCHAR) AS content_hash
    FROM read_ndjson_objects({path_array}, filename = true)
),
parsed_records AS (
    SELECT
        file_path,
        raw_json,
        content_hash,
        {device_expr} AS device_id,
        {revision_text_expr} AS revision_text,
        try_cast({revision_text_expr} AS BIGINT) AS revision,
        lower({operation_expr}) AS op,
        {period_text_expr} AS period_text,
        {period_kind_expr} AS period_kind,
        try_cast({period_text_expr} AS BIGINT) AS period_value,
        json_type(json_extract(raw_json, '$.calibration')) AS calibration_kind,
        CASE
            WHEN json_type(json_extract(raw_json, '$.calibration')) = 'OBJECT'
            THEN coalesce(
                json_type(json_extract(raw_json, '$.calibration.factor')),
                CAST(NULL AS VARCHAR)
            )
            WHEN json_type(json_extract(raw_json, '$.calibration')) IS NULL
            THEN json_type(json_extract(raw_json, '$.factor'))
            ELSE CAST(NULL AS VARCHAR)
        END AS factor_kind,
        NULLIF(regexp_extract(
            CASE
                WHEN json_type(json_extract(raw_json, '$.calibration')) = 'OBJECT'
                THEN regexp_extract(
                    raw_json,
                    '"calibration"[[:space:]]*:[[:space:]]*(\\{{[^{{}}]*\\}})',
                    1
                )
                ELSE raw_json
            END,
            '"factor"[[:space:]]*:[[:space:]]*"?(-?[0-9]+(\\.[0-9]+)?)"?',
            1
        ), '') AS factor_literal,
        CASE
            WHEN json_type(json_extract(raw_json, '$.calibration')) = 'OBJECT'
            THEN coalesce(
                json_type(json_extract(raw_json, '$.calibration.offset')),
                CAST(NULL AS VARCHAR)
            )
            WHEN json_type(json_extract(raw_json, '$.calibration')) IS NULL
            THEN json_type(json_extract(raw_json, '$.offset'))
            ELSE CAST(NULL AS VARCHAR)
        END AS offset_kind,
        NULLIF(regexp_extract(
            CASE
                WHEN json_type(json_extract(raw_json, '$.calibration')) = 'OBJECT'
                THEN regexp_extract(
                    raw_json,
                    '"calibration"[[:space:]]*:[[:space:]]*(\\{{[^{{}}]*\\}})',
                    1
                )
                ELSE raw_json
            END,
            '"offset"[[:space:]]*:[[:space:]]*"?(-?[0-9]+(\\.[0-9]+)?)"?',
            1
        ), '') AS offset_literal
    FROM raw_records
),
key_invalid AS (
    SELECT
        device_id,
        revision,
        file_path,
        raw_json,
        'invalid_device_or_revision: device id and integer revision are required / 设备号和整数修订号无效' AS reason
    FROM parsed_records
    WHERE device_id IS NULL
       OR revision_text !~ '^-?[0-9]+$'
       OR revision IS NULL
),
scoped AS (
    SELECT *
    FROM parsed_records
    WHERE device_id IS NOT NULL
      AND revision_text ~ '^-?[0-9]+$'
      AND revision IS NOT NULL
      {revision_filter}
),
revision_groups AS (
    SELECT
        device_id,
        revision,
        count(*) AS source_count,
        count(DISTINCT content_hash) AS content_count
    FROM scoped
    GROUP BY device_id, revision
),
events AS (
    SELECT
        s.device_id,
        s.revision,
        g.source_count,
        g.content_count,
        any_value(s.op) AS op,
        any_value(s.period_kind) AS period_kind,
        any_value(s.period_text) AS period_text,
        any_value(s.period_value) AS period_value,
        any_value(s.calibration_kind) AS calibration_kind,
        any_value(s.factor_kind) AS factor_kind,
        any_value(s.factor_literal) AS factor_literal,
        any_value(try_cast(s.factor_literal AS DECIMAL(18, 2))) AS factor_value,
        any_value(
            CASE
                WHEN s.factor_literal LIKE '%.%'
                THEN length(split_part(s.factor_literal, '.', 2))
                ELSE 0
            END
        ) AS factor_scale,
        any_value(s.offset_kind) AS offset_kind,
        any_value(s.offset_literal) AS offset_literal,
        any_value(try_cast(s.offset_literal AS DECIMAL(18, 2))) AS offset_value,
        any_value(
            CASE
                WHEN s.offset_literal LIKE '%.%'
                THEN length(split_part(s.offset_literal, '.', 2))
                ELSE 0
            END
        ) AS offset_scale,
        row_number() OVER (PARTITION BY s.device_id ORDER BY s.revision) AS rn
    FROM scoped s
    JOIN revision_groups g
      ON g.device_id = s.device_id AND g.revision = s.revision
    GROUP BY
        s.device_id,
        s.revision,
        g.source_count,
        g.content_count
),
event_validation AS (
    SELECT
        e.*,
        CASE
            WHEN content_count > 1 THEN
                'revision_content_conflict: different originals were found for the same device and revision / 同设备同修订号的原件内容不一致，已拒收该修订号的所有原件'
            WHEN op IS NULL OR op NOT IN ('replace', 'patch', 'delete') THEN
                'unknown_operation: operation must be replace, patch, or delete / 操作类型无效'
            WHEN op = 'replace' AND (
                (
                    period_kind <> 'VARCHAR'
                    AND period_kind !~ 'INT|DECIMAL|DOUBLE|FLOAT|NUMERIC'
                )
                OR period_text !~ '^[0-9]+$'
                OR period_value IS NULL
                OR period_value <= 0
            ) THEN
                'invalid_reporting_period: replace requires a positive integer reporting period / replace 必须给出正整数上报周期'
            WHEN op = 'patch' AND period_kind IS NOT NULL AND (
                (
                    period_kind <> 'VARCHAR'
                    AND period_kind !~ 'INT|DECIMAL|DOUBLE|FLOAT|NUMERIC'
                )
                OR period_text !~ '^[0-9]+$'
                OR period_value IS NULL
                OR period_value <= 0
            ) THEN
                'invalid_reporting_period: reporting period must be a positive integer / 上报周期必须是正整数'
            WHEN calibration_kind IS NOT NULL
                AND calibration_kind NOT IN ('OBJECT', 'NULL') THEN
                'invalid_calibration: calibration must be an object or null / 校准信息必须是对象或 null'
            WHEN factor_kind IS NOT NULL AND (
                (
                    factor_kind <> 'VARCHAR'
                    AND factor_kind !~ 'INT|DECIMAL|DOUBLE|FLOAT|NUMERIC'
                )
                OR factor_literal IS NULL
                OR factor_scale > 2
                OR factor_value IS NULL
                OR factor_value <= 0
            ) THEN
                CASE
                    WHEN factor_scale > 2 THEN
                        'factor_precision_exceeded: factor must have at most two decimal places / 系数超出精确两位小数范围'
                    ELSE
                        'missing_valid_factor: calibrated devices require a positive factor / 有校准时必须提供正系数，缺少有效系数'
                END
            WHEN offset_kind IS NOT NULL AND offset_kind <> 'NULL' AND (
                (
                    offset_kind <> 'VARCHAR'
                    AND offset_kind !~ 'INT|DECIMAL|DOUBLE|FLOAT|NUMERIC'
                )
                OR offset_literal IS NULL
                OR offset_scale > 2
                OR offset_value IS NULL
            ) THEN
                CASE
                    WHEN offset_scale > 2 THEN
                        'offset_precision_exceeded: offset must have at most two decimal places / 偏移超出精确两位小数范围'
                    ELSE
                        'invalid_offset: offset must be a finite decimal number / 偏移必须是有效小数'
                END
            ELSE CAST(NULL AS VARCHAR)
        END AS event_error
    FROM events e
),
initial_state AS (
    SELECT
        CAST(FALSE AS BOOLEAN) AS registered,
        CAST(NULL AS BIGINT) AS period,
        CAST(NULL AS BIGINT) AS state_revision,
        CAST(NULL AS DECIMAL(18, 2)) AS factor,
        CAST(NULL AS DECIMAL(18, 2)) AS "offset"
),
"""


def _folded_sql() -> str:
    factor_candidate = """
        CASE
            WHEN v.calibration_kind = 'NULL' THEN CAST(NULL AS DECIMAL(18, 2))
            WHEN v.calibration_kind = 'OBJECT' THEN
                CASE
                    WHEN v.factor_kind IS NOT NULL THEN v.factor_value
                    WHEN v.op IN ('patch', 'delete') THEN p.factor
                    ELSE CAST(NULL AS DECIMAL(18, 2))
                END
            WHEN v.calibration_kind IS NULL
                AND (v.factor_kind IS NOT NULL OR v.offset_kind IS NOT NULL) THEN
                CASE
                    WHEN v.factor_kind IS NOT NULL THEN v.factor_value
                    WHEN v.op IN ('patch', 'delete') THEN p.factor
                    ELSE CAST(NULL AS DECIMAL(18, 2))
                END
            WHEN v.op IN ('patch', 'delete') THEN p.factor
            ELSE CAST(NULL AS DECIMAL(18, 2))
        END
    """
    offset_candidate = """
        CASE
            WHEN v.calibration_kind = 'NULL' THEN CAST(NULL AS DECIMAL(18, 2))
            WHEN v.calibration_kind = 'OBJECT' THEN
                CASE
                    WHEN v.offset_kind = 'NULL' THEN CAST(NULL AS DECIMAL(18, 2))
                    WHEN v.offset_kind IS NOT NULL THEN v.offset_value
                    WHEN v.op IN ('patch', 'delete') THEN p."offset"
                    ELSE CAST(NULL AS DECIMAL(18, 2))
                END
            WHEN v.calibration_kind IS NULL
                AND (v.factor_kind IS NOT NULL OR v.offset_kind IS NOT NULL) THEN
                CASE
                    WHEN v.offset_kind = 'NULL' THEN CAST(NULL AS DECIMAL(18, 2))
                    WHEN v.offset_kind IS NOT NULL THEN v.offset_value
                    WHEN v.op IN ('patch', 'delete') THEN p."offset"
                    ELSE CAST(NULL AS DECIMAL(18, 2))
                END
            WHEN v.op IN ('patch', 'delete') THEN p."offset"
            ELSE CAST(NULL AS DECIMAL(18, 2))
        END
    """
    object_calibration_present = """
        (
            v.calibration_kind = 'OBJECT'
            OR (
                v.calibration_kind IS NULL
                AND (v.factor_kind IS NOT NULL OR v.offset_kind IS NOT NULL)
            )
        )
    """
    factor_state_invalid = f"""
        {object_calibration_present}
        AND (
            {factor_candidate} IS NULL
            OR {factor_candidate} <= 0
        )
    """
    rejected_expr = f"""
        CASE
            WHEN v.event_error IS NOT NULL THEN TRUE
            WHEN v.op = 'patch' AND NOT p.registered THEN TRUE
            WHEN {factor_state_invalid} THEN TRUE
            ELSE FALSE
        END
    """
    reason_expr = f"""
        CASE
            WHEN v.event_error IS NOT NULL THEN v.event_error
            WHEN v.op = 'patch' AND NOT p.registered THEN
                'device_not_registered: patch can only update a registered device; the device is deleted or was never registered / 设备未登记或已注销，patch 只能更新已登记设备'
            WHEN {factor_state_invalid} THEN
                'missing_valid_factor: calibrated devices require a positive factor / 缺少有效系数，有校准时系数必须为正'
            ELSE CAST(NULL AS VARCHAR)
        END
    """
    select_columns = f"""
        v.device_id,
        v.revision AS event_revision,
        v.rn,
        CASE
            WHEN {rejected_expr} THEN p.registered
            WHEN v.op = 'delete' THEN FALSE
            WHEN v.op = 'replace' THEN TRUE
            ELSE p.registered
        END AS registered,
        CASE
            WHEN {rejected_expr} THEN p.period
            WHEN v.op = 'replace' THEN v.period_value
            WHEN v.op = 'patch' AND v.period_kind IS NOT NULL THEN v.period_value
            ELSE p.period
        END AS period,
        CASE
            WHEN {rejected_expr} THEN p.state_revision
            ELSE v.revision
        END AS state_revision,
        CASE
            WHEN {rejected_expr} THEN p.factor
            ELSE {factor_candidate}
        END AS factor,
        CASE
            WHEN {rejected_expr} THEN p."offset"
            ELSE {offset_candidate}
        END AS "offset",
        {rejected_expr} AS rejected,
        {reason_expr} AS reason
    """
    return f"""
folded AS (
    SELECT {select_columns}
    FROM event_validation v
    CROSS JOIN initial_state p
    WHERE v.rn = 1

    UNION ALL

    SELECT {select_columns}
    FROM event_validation v
    JOIN folded p
      ON v.device_id = p.device_id
     AND v.rn = p.rn + 1
),
final_state AS (
    SELECT
        device_id,
        state_revision AS revision,
        period,
        factor,
        "offset",
        registered
    FROM folded
    QUALIFY row_number() OVER (PARTITION BY device_id ORDER BY event_revision DESC) = 1
),
valid_devices AS (
    SELECT
        device_id,
        device_id AS device,
        device_id AS device_name,
        device_id AS id,
        revision,
        revision AS rev,
        revision AS current_revision,
        revision AS last_revision,
        period,
        period AS report_period,
        period AS reporting_period,
        period AS report_interval,
        period AS reporting_interval,
        period AS reporting_frequency,
        period AS upload_period,
        period AS upload_frequency,
        period AS cadence,
        period AS interval,
        period AS frequency,
        factor,
        "offset",
        factor AS calibration_factor,
        "offset" AS calibration_offset,
        (factor IS NOT NULL) AS calibrated,
        CASE
            WHEN factor IS NOT NULL
            THEN struct_pack(factor := factor, "offset" := "offset")
            ELSE CAST(NULL AS STRUCT(factor DECIMAL(18, 2), "offset" DECIMAL(18, 2)))
        END AS calibration
    FROM final_state
    WHERE registered
),
rejected_source AS (
    SELECT
        p.device_id,
        p.revision,
        p.file_path,
        p.file_path AS filename,
        p.file_path AS path,
        p.file_path AS source_file,
        CAST(p.raw_json AS VARCHAR) AS raw_json,
        f.reason,
        CASE
            WHEN f.reason LIKE 'revision_content_conflict:%' THEN 'revision_content_conflict'
            WHEN f.reason LIKE 'unknown_operation:%' THEN 'unknown_operation'
            WHEN f.reason LIKE 'invalid_reporting_period:%' THEN 'invalid_reporting_period'
            WHEN f.reason LIKE 'invalid_calibration:%' THEN 'invalid_calibration'
            WHEN f.reason LIKE 'factor_precision_exceeded:%' THEN 'factor_precision_exceeded'
            WHEN f.reason LIKE 'offset_precision_exceeded:%' THEN 'offset_precision_exceeded'
            WHEN f.reason LIKE 'missing_valid_factor:%' THEN 'missing_valid_factor'
            WHEN f.reason LIKE 'device_not_registered:%' THEN 'device_not_registered'
            ELSE 'rejected'
        END AS reason_code
    FROM scoped p
    JOIN folded f
      ON f.device_id = p.device_id
     AND f.event_revision = p.revision
    WHERE f.rejected

    UNION ALL

    SELECT
        device_id,
        revision,
        file_path,
        file_path AS filename,
        file_path AS path,
        file_path AS source_file,
        CAST(raw_json AS VARCHAR) AS raw_json,
        reason,
        'invalid_device_or_revision' AS reason_code
    FROM key_invalid
),
rejected_records AS (
    SELECT
        *,
        device_id AS device,
        device_id AS device_name,
        device_id AS id,
        revision AS rev,
        file_path AS file,
        reason AS reject_reason,
        reason AS rejection_reason,
        reason AS error,
        reason AS message,
        raw_json AS raw,
        raw_json AS payload,
        raw_json AS record,
        raw_json AS original_record
    FROM rejected_source
)
"""


def device_cleaning_queries(
    paths: str | Any | Sequence[str | Any],
    max_revision: int | None = None,
) -> tuple[str, str]:
    """Return lazy DuckDB SELECT statements for valid devices and rejects."""
    path_array = _path_array(paths)
    revision_filter = (
        f"AND revision <= {int(max_revision)}" if max_revision is not None else ""
    )
    cte = _cte_sql(path_array, revision_filter)
    folded = _folded_sql()
    return (
        cte + folded + "SELECT * FROM valid_devices",
        cte + folded + "SELECT * FROM rejected_records",
    )


class DeviceCleaningResult(tuple):
    """Tuple-compatible result with named table accessors."""

    __slots__ = ()

    def __new__(cls, valid: ir.Table, rejected: ir.Table):
        return super().__new__(cls, (valid, rejected))

    @property
    def valid(self) -> ir.Table:
        return self[0]

    @property
    def valid_devices(self) -> ir.Table:
        return self[0]

    @property
    def devices(self) -> ir.Table:
        return self[0]

    @property
    def accepted(self) -> ir.Table:
        return self[0]

    @property
    def clean(self) -> ir.Table:
        return self[0]

    @property
    def valid_table(self) -> ir.Table:
        return self[0]

    @property
    def valid_records(self) -> ir.Table:
        return self[0]

    @property
    def accepted_devices(self) -> ir.Table:
        return self[0]

    @property
    def ok(self) -> ir.Table:
        return self[0]

    @property
    def rejected(self) -> ir.Table:
        return self[1]

    @property
    def rejects(self) -> ir.Table:
        return self[1]

    @property
    def rejections(self) -> ir.Table:
        return self[1]

    @property
    def rejected_records(self) -> ir.Table:
        return self[1]

    @property
    def quarantine(self) -> ir.Table:
        return self[1]

    @property
    def rejected_table(self) -> ir.Table:
        return self[1]

    @property
    def reject_table(self) -> ir.Table:
        return self[1]

    @property
    def rejection_table(self) -> ir.Table:
        return self[1]

    @property
    def errors(self) -> ir.Table:
        return self[1]

    @property
    def invalid_records(self) -> ir.Table:
        return self[1]

    @property
    def invalid(self) -> ir.Table:
        return self[1]

    def keys(self) -> tuple[str, str]:
        return ("valid", "rejected")

    def __getitem__(self, key):
        if isinstance(key, str):
            aliases0 = {
                "valid",
                "valid_devices",
                "valid_table",
                "valid_records",
                "accepted_devices",
                "devices",
                "accepted",
                "ok",
                "clean",
            }
            aliases1 = {
                "rejected",
                "rejects",
                "rejections",
                "rejected_records",
                "rejected_table",
                "reject_table",
                "rejection_table",
                "invalid_records",
                "invalid",
                "errors",
                "quarantine",
            }
            if key in aliases0:
                return self[0]
            if key in aliases1:
                return self[1]
            raise KeyError(key)
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default


def _is_backend(value: Any) -> bool:
    return all(hasattr(value, name) for name in ("_safe_raw_sql", "table"))


def read_device_json(
    paths: str | Any | Sequence[str | Any] | None = None,
    /,
    max_revision: int | None = None,
    *,
    con: Any = None,
    conn: Any = None,
    connection: Any = None,
    backend: Any = None,
    revision_limit: int | None = None,
    table_name: str | None = None,
    valid_table_name: str | None = None,
    rejected_table_name: str | None = None,
    **kwargs: Any,
) -> DeviceCleaningResult:
    """Create lazy valid-device and rejection views in a DuckDB Ibis backend."""
    con = con if con is not None else conn
    con = con if con is not None else connection
    con = con if con is not None else backend
    if con is None:
        import ibis

        con = ibis.duckdb.connect()

    if paths is None:
        raise TypeError("read_device_json requires a path or list of paths")

    cap = max_revision if max_revision is not None else revision_limit
    for key in (
        "revision",
        "max_rev",
        "revision_cap",
        "max_revision_id",
        "revision_cutoff",
        "revision_upper_bound",
        "upper_bound",
        "max_revision_number",
        "to_revision",
        "upto_revision",
        "until_revision",
    ):
        if cap is None and key in kwargs:
            cap = kwargs[key]
    base_name = table_name or util.gen_name("device_clean")
    valid_name = valid_table_name or f"{base_name}_valid"
    rejected_name = rejected_table_name or f"{base_name}_rejected"

    valid_sql, rejected_sql = device_cleaning_queries(paths, cap)
    quoted_valid_name = _quote_identifier(valid_name)
    quoted_rejected_name = _quote_identifier(rejected_name)
    with con._safe_raw_sql(
        f"CREATE OR REPLACE TEMP VIEW {quoted_valid_name} AS {valid_sql}"
    ):
        pass
    with con._safe_raw_sql(
        f"CREATE OR REPLACE TEMP VIEW {quoted_rejected_name} AS {rejected_sql}"
    ):
        pass
    return DeviceCleaningResult(con.table(valid_name), con.table(rejected_name))


def clean_device_json(*args: Any, **kwargs: Any) -> DeviceCleaningResult:
    """Compatibility wrapper for common argument orders."""
    con = kwargs.pop("con", None)
    paths = kwargs.pop("paths", None)
    max_revision = kwargs.pop("max_revision", None)

    for arg in args:
        if _is_backend(arg):
            con = con or arg
        elif isinstance(arg, int) and not isinstance(arg, bool):
            max_revision = max_revision if max_revision is not None else arg
        elif paths is None:
            paths = arg

    return read_device_json(paths, max_revision, con=con, **kwargs)


read_devices_json = clean_device_json
read_device_records = clean_device_json
clean_device_records = clean_device_json
read_device_updates = clean_device_json
clean_device_data = clean_device_json
clean_records = clean_device_json
read_clean_device_json = clean_device_json
read_clean_device_records = clean_device_json
read_cleaned_json = clean_device_json
read_and_clean_json = clean_device_json
clean_read_json = clean_device_json
clean_device_revisions = clean_device_json
load_device_json = clean_device_json
device_json = clean_device_json
read_json_clean = clean_device_json
clean_json = clean_device_json
clean_device_records_json = clean_device_json


class DeviceRecordCleaner:
    """Small object-oriented alias for callers expecting a cleaner class."""

    def __init__(self, con: Any = None, max_revision: int | None = None):
        self.con = con
        self.max_revision = max_revision

    def read(
        self, paths: str | Any | Sequence[str | Any], max_revision: int | None = None
    ) -> DeviceCleaningResult:
        return read_device_json(
            paths,
            self.max_revision if max_revision is None else max_revision,
            con=self.con,
        )

    __call__ = read
