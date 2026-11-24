from ._core import (
    DEFERRED,
    DeferredType,
    PrecheckResult,
    JSONDecoder as Decoder,
    JSONEncoder as Encoder,
    json_decode as decode,
    json_encode as encode,
    json_format as format,
    json_precheck as precheck,
    json_precheck_lines as precheck_lines,
)
from ._json_schema import schema, schema_components
