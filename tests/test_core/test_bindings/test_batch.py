#  Copyright 2018-Present The CloudEvents Authors
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import base64
import json
from io import BytesIO
from typing import Any

import pytest

from cloudevents.core.bindings.batch import (
    AmbiguousRouteError,
    BatchReadError,
    BatchResponseReader,
    BatchRouteRegistry,
    HTTPBatchResponse,
    WAREHOUSE_DATACONTENTTYPE,
    WAREHOUSE_MEDIA_TYPE,
    read_http_batch,
    read_http_response,
)
from cloudevents.core.bindings.http import HTTPMessage, from_http_event

STRUCTURED_HEADERS = {"Content-Type": WAREHOUSE_MEDIA_TYPE}


class OneShotStream:
    """Byte stream that refuses to be read more than once."""

    def __init__(self, data: bytes) -> None:
        self._buffer = BytesIO(data)
        self.read_count = 0
        self.max_read = 0

    def read(self, size: int = -1) -> bytes:
        if self.read_count and self._buffer.closed:
            raise AssertionError("response stream was read more than once")
        self.read_count += 1
        self.max_read = max(self.max_read, size if size and size > 0 else 0)
        chunk = self._buffer.read(size)
        if not chunk:
            self._buffer.close()
        return chunk


def warehouse_decoder(payload: bytes) -> str:
    return json.loads(payload.decode("utf-8"))["bin"]


def warehouse_decoder_v2(payload: bytes) -> str:
    return "W-" + json.loads(payload.decode("utf-8"))["bin"]


def make_warehouse_envelope() -> bytes:
    # data first; identifying attributes only appear at the envelope tail
    return (
        b'{"data":"'
        + base64.b64encode(b'{"bin":"007"}')
        + b'","id":"evt-1","source":"/wms","type":"com.wms.movement",'
        b'"specversion":"0.3","datacontenttype":"'
        + WAREHOUSE_DATACONTENTTYPE.encode("utf-8")
        + b'","datacontentencoding":"base64"}'
    )


def make_v1_envelope() -> bytes:
    return json.dumps(
        {
            "id": "evt-2",
            "source": "/wms",
            "type": "com.wms.movement",
            "specversion": "1.0",
            "datacontenttype": WAREHOUSE_DATACONTENTTYPE,
            "data": "007",
        }
    ).encode("utf-8")


def make_registry(
    decoder: Any = warehouse_decoder,
) -> tuple[BatchRouteRegistry, str, str]:
    registry = BatchRouteRegistry()
    registry.register_generic("generic-route")
    registry.register_warehouse("warehouse-route", decoder)
    return registry, "generic-route", "warehouse-route"


def test_warehouse_route_decodes_base64_payload_across_read_boundaries() -> None:
    registry, _, warehouse_name = make_registry()
    stream = OneShotStream(make_warehouse_envelope())

    result = read_http_response(STRUCTURED_HEADERS, stream, registry, chunk_size=7)

    assert result.success
    assert result.payload_route == warehouse_name
    assert result.route_version == 1
    assert result.data == b'{"bin":"007"}'
    assert result.business_data == "007"
    assert result.attributes["specversion"] == "0.3"
    assert result.attributes["datacontenttype"] == WAREHOUSE_DATACONTENTTYPE
    assert "data" not in result.attributes
    assert stream.read_count > 1  # fields really crossed read boundaries


def test_v1_envelope_uses_generic_route_and_keeps_string_data() -> None:
    registry, generic_name, warehouse_name = make_registry()
    stream = OneShotStream(make_v1_envelope())

    result = read_http_response(STRUCTURED_HEADERS, stream, registry, chunk_size=5)

    assert result.success
    assert result.payload_route == generic_name
    assert result.route_version == 1
    assert result.data == "007"
    assert result.business_data is None
    assert result.attributes["specversion"] == "1.0"
    matched_names = {match.route_name for match in result.matches}
    assert generic_name in matched_names
    assert warehouse_name not in matched_names


def test_warehouse_route_outranks_generic_route() -> None:
    registry, generic_name, warehouse_name = make_registry()
    result = read_http_response(
        STRUCTURED_HEADERS, OneShotStream(make_warehouse_envelope()), registry
    )

    assert result.success
    assert result.payload_route == warehouse_name
    matched_names = {match.route_name for match in result.matches}
    assert matched_names == {generic_name, warehouse_name}
    warehouse_match = next(
        match for match in result.matches if match.route_name == warehouse_name
    )
    assert not warehouse_match.generic
    assert any("specversion" in reason for reason in warehouse_match.reasons)
    assert any("datacontenttype" in reason for reason in warehouse_match.reasons)
    assert any("base64" in reason for reason in warehouse_match.reasons)


def test_generic_route_still_matches_when_payload_representation_differs() -> None:
    registry, generic_name, warehouse_name = make_registry()
    envelope = json.dumps(
        {
            "id": "evt-3",
            "source": "/wms",
            "type": "com.wms.movement",
            "specversion": "0.3",
            "datacontenttype": WAREHOUSE_DATACONTENTTYPE,
            "data": {"bin": "007"},
        }
    ).encode("utf-8")

    result = read_http_response(
        STRUCTURED_HEADERS, OneShotStream(envelope), registry
    )

    assert result.success
    assert result.payload_route == generic_name
    assert result.data == {"bin": "007"}
    assert {match.route_name for match in result.matches} == {generic_name}
    assert warehouse_name not in {match.route_name for match in result.matches}


def test_equally_specific_routes_are_ambiguous_and_not_executed() -> None:
    registry = BatchRouteRegistry()
    registry.register_generic("generic-route")

    calls: list[bytes] = []

    def first_decoder(payload: bytes) -> str:
        calls.append(payload)
        return warehouse_decoder(payload)

    def second_decoder(payload: bytes) -> str:
        calls.append(payload)
        return warehouse_decoder(payload)

    registry.register_warehouse("warehouse-a", first_decoder)
    registry.register_warehouse("warehouse-b", second_decoder)

    result = read_http_response(
        STRUCTURED_HEADERS, OneShotStream(make_warehouse_envelope()), registry
    )

    assert not result.success
    assert result.business_data is None
    assert calls == []
    assert isinstance(result.error, str)
    matched_names = {match.route_name for match in result.matches}
    assert matched_names == {"warehouse-a", "warehouse-b"}
    for match in result.matches:
        assert match.reasons


def test_residual_ce_headers_ignored_in_structured_mode() -> None:
    registry, _, _ = make_registry()
    headers = {
        "Content-Type": WAREHOUSE_MEDIA_TYPE,
        "ce-type": "gateway.residual.type",
        "ce-id": "gateway-id",
    }

    result = read_http_response(
        headers, OneShotStream(make_v1_envelope()), registry
    )

    assert result.success
    assert result.envelope == "structured"
    assert result.attributes["type"] == "com.wms.movement"
    assert result.attributes["id"] == "evt-2"
    assert "gateway.residual.type" not in result.attributes.values()


def test_binary_mode_reads_attributes_from_ce_headers() -> None:
    registry, _, _ = make_registry()
    headers = {
        "ce-id": "bin-1",
        "ce-source": "/scanner",
        "ce-type": "com.wms.scan",
        "ce-specversion": "1.0",
        "Content-Type": "application/json",
    }
    result = read_http_response(
        headers, OneShotStream(b'{"bin":"007"}'), registry
    )

    assert result.success
    assert result.envelope == "binary"
    assert result.attributes["id"] == "bin-1"
    assert result.attributes["specversion"] == "1.0"
    assert result.attributes["datacontenttype"] == "application/json"
    assert result.data == {"bin": "007"}


def test_binary_content_type_selects_text_payload() -> None:
    registry, _, _ = make_registry()
    headers = {
        "ce-id": "bin-2",
        "ce-source": "/scanner",
        "ce-type": "com.wms.scan",
        "ce-specversion": "1.0",
        "Content-Type": "text/plain",
    }
    result = read_http_response(
        headers, OneShotStream(b"hello bin"), registry
    )

    assert result.success
    assert result.data == "hello bin"


def test_truncated_response_fails_but_batch_remains_readable() -> None:
    registry, _, _ = make_registry()
    full = make_warehouse_envelope()
    truncated = full[: len(full) - 12]  # cut off the legal envelope tail
    responses = [
        (STRUCTURED_HEADERS, OneShotStream(truncated)),
        (STRUCTURED_HEADERS, OneShotStream(make_v1_envelope())),
        (STRUCTURED_HEADERS, OneShotStream(make_warehouse_envelope())),
    ]

    results = read_http_batch(responses, registry, chunk_size=11)

    assert len(results) == 3
    assert not results[0].success
    assert results[0].envelope == "structured"
    assert results[0].business_data is None
    assert results[1].success and results[1].data == "007"
    assert results[2].success and results[2].business_data == "007"


def test_each_stream_is_read_exactly_once_per_batch() -> None:
    registry, _, _ = make_registry()
    streams = [
        OneShotStream(make_warehouse_envelope()),
        OneShotStream(make_v1_envelope()),
    ]
    responses = [
        HTTPBatchResponse(STRUCTURED_HEADERS, streams[0]),
        HTTPBatchResponse(STRUCTURED_HEADERS, streams[1]),
    ]

    reader = BatchResponseReader(responses, registry, chunk_size=13)
    results = reader.read_responses()

    assert all(result.success for result in results)
    for stream in streams:
        assert stream.read_count >= 1
    with pytest.raises(BatchReadError):
        reader.read_responses()


def test_large_payload_keeps_working_memory_bounded() -> None:
    registry, _, warehouse_name = make_registry()
    raw_payload = b'{"bin":"' + b"x" * (200 * 1024) + b'","seq":7}'
    envelope = (
        b'{"data":"'
        + base64.b64encode(raw_payload)
        + b'","id":"big","source":"/wms","type":"com.wms.movement",'
        b'"specversion":"0.3","datacontenttype":"'
        + WAREHOUSE_DATACONTENTTYPE.encode("utf-8")
        + b'","datacontentencoding":"base64"}'
    )
    stream = OneShotStream(envelope)

    result = read_http_response(
        STRUCTURED_HEADERS, stream, registry, chunk_size=64
    )

    assert result.success
    assert result.payload_route == warehouse_name
    assert result.data == raw_payload
    assert result.business_data == "x" * (200 * 1024)
    assert stream.max_read <= 64


def test_business_decoder_not_called_before_route_is_known() -> None:
    calls: list[bytes] = []

    def counting_decoder(payload: bytes) -> str:
        calls.append(payload)
        return warehouse_decoder(payload)

    registry = BatchRouteRegistry()
    registry.register_generic("generic-route")
    registry.register_warehouse("warehouse-route", counting_decoder)

    generic_result = read_http_response(
        STRUCTURED_HEADERS, OneShotStream(make_v1_envelope()), registry
    )
    assert generic_result.success
    assert calls == []

    ambiguous_registry = BatchRouteRegistry()
    ambiguous_registry.register_generic("generic-route")
    ambiguous_registry.register_warehouse("warehouse-route", counting_decoder)
    ambiguous_registry.register_warehouse(
        "warehouse-copy", counting_decoder
    )
    ambiguous_result = read_http_response(
        STRUCTURED_HEADERS,
        OneShotStream(make_warehouse_envelope()),
        ambiguous_registry,
    )
    assert not ambiguous_result.success
    assert calls == []


def test_batch_snapshot_pins_route_version_across_mid_batch_swap() -> None:
    registry = BatchRouteRegistry()
    registry.register_generic("generic-route")
    registry.register_warehouse("warehouse-route", warehouse_decoder)

    first_batch_streams = [
        OneShotStream(make_warehouse_envelope()),
        OneShotStream(make_warehouse_envelope()),
        OneShotStream(make_v1_envelope()),
    ]
    reader = BatchResponseReader(
        [
            (STRUCTURED_HEADERS, first_batch_streams[0]),
            (STRUCTURED_HEADERS, first_batch_streams[1]),
            (STRUCTURED_HEADERS, first_batch_streams[2]),
        ],
        registry,
        chunk_size=9,
    )

    # Operator replaces the route while the first body is still in flight.
    new_registration = registry.register_warehouse(
        "warehouse-route", warehouse_decoder_v2
    )
    assert new_registration.version == 2
    assert registry.registration_version("warehouse-route") == 2

    first_results = reader.read_responses()
    assert all(result.success for result in first_results)
    assert first_results[0].business_data == "007"
    assert first_results[1].business_data == "007"
    assert first_results[0].route_version == 1
    assert first_results[1].route_version == 1
    assert first_results[2].data == "007"

    # The old registration version remains discoverable via the snapshot.
    assert reader.registry.registration_version("warehouse-route") == 1

    next_batch = read_http_batch(
        [
            (STRUCTURED_HEADERS, OneShotStream(make_warehouse_envelope())),
            (STRUCTURED_HEADERS, OneShotStream(make_v1_envelope())),
        ],
        registry,
    )
    assert next_batch[0].business_data == "W-007"
    assert next_batch[0].route_version == 2
    assert next_batch[1].data == "007"


def test_generic_route_handles_v1_data_base64_and_scalar_values() -> None:
    registry, generic_name, _ = make_registry()
    envelope = json.dumps(
        {
            "id": "evt-b64",
            "source": "/wms",
            "type": "com.wms.movement",
            "specversion": "1.0",
            "datacontenttype": "application/octet-stream",
            "data_base64": base64.b64encode(b"raw-bytes").decode("utf-8"),
        }
    ).encode("utf-8")
    result = read_http_response(
        STRUCTURED_HEADERS, OneShotStream(envelope), registry, chunk_size=3
    )
    assert result.success
    assert result.payload_route == generic_name
    assert result.data == b"raw-bytes"

    for value in (42, 3.14, True, False, None):
        envelope = json.dumps(
            {
                "id": "evt-scalar",
                "source": "/wms",
                "type": "com.wms.movement",
                "specversion": "1.0",
                "data": value,
            }
        ).encode("utf-8")
        result = read_http_response(
            STRUCTURED_HEADERS, OneShotStream(envelope), registry, chunk_size=2
        )
        assert result.success
        assert result.data is None if value is None else result.data == value


def test_unicode_payload_survives_arbitrary_read_boundaries() -> None:
    registry, _, _ = make_registry()
    envelope = json.dumps(
        {
            "id": "evt-unicode",
            "source": "/wms",
            "type": "com.wms.movement",
            "specversion": "1.0",
            "data": "café 中",
        }
    ).encode("utf-8")
    for chunk_size in range(1, len(envelope) + 1):
        result = read_http_response(
            STRUCTURED_HEADERS,
            OneShotStream(envelope),
            registry,
            chunk_size=chunk_size,
        )
        assert result.success and result.data == "café 中"


def test_invalid_base64_fails_only_that_response() -> None:
    registry, _, _ = make_registry()
    envelope = (
        b'{"data":"@@@notbase64@@@","id":"evt-bad","source":"/wms",'
        b'"type":"com.wms.movement","specversion":"0.3","datacontenttype":"'
        + WAREHOUSE_DATACONTENTTYPE.encode("utf-8")
        + b'","datacontentencoding":"base64"}'
    )
    result = read_http_response(
        STRUCTURED_HEADERS, OneShotStream(envelope), registry, chunk_size=5
    )
    assert not result.success
    assert "base64" in result.error  # type: ignore[operator]
    assert result.business_data is None


def test_failed_item_publishes_neither_attributes_nor_payload() -> None:
    registry, _, _ = make_registry()
    full = make_warehouse_envelope()
    truncated = full[: len(full) - 12]

    truncated_result = read_http_response(
        STRUCTURED_HEADERS, OneShotStream(truncated), registry
    )
    assert not truncated_result.success
    assert truncated_result.attributes == {}
    assert truncated_result.data is None
    assert truncated_result.business_data is None

    ambiguous_registry = BatchRouteRegistry()
    ambiguous_registry.register_generic("generic-route")
    ambiguous_registry.register_warehouse("warehouse-a", warehouse_decoder)
    ambiguous_registry.register_warehouse("warehouse-b", warehouse_decoder_v2)
    ambiguous_result = read_http_response(
        STRUCTURED_HEADERS,
        OneShotStream(make_warehouse_envelope()),
        ambiguous_registry,
    )
    assert not ambiguous_result.success
    assert ambiguous_result.attributes == {}
    assert ambiguous_result.data is None
    assert ambiguous_result.business_data is None


def test_media_type_parameters_match_independent_of_order() -> None:
    registry, _, warehouse_name = make_registry()
    headers = {
        "Content-Type": "application/cloudevents+json; charset=utf-8; profile=warehouse"
    }
    result = read_http_response(
        headers, OneShotStream(make_warehouse_envelope()), registry, chunk_size=8
    )
    assert result.success
    assert result.payload_route == warehouse_name


def test_ambiguous_route_error_carries_both_names_and_reasons() -> None:
    registry = BatchRouteRegistry()
    registry.register_warehouse("warehouse-a", warehouse_decoder)
    registry.register_warehouse("warehouse-b", warehouse_decoder_v2)

    result = read_http_response(
        STRUCTURED_HEADERS, OneShotStream(make_warehouse_envelope()), registry
    )
    assert not result.success
    with pytest.raises(AmbiguousRouteError) as exc_info:
        raise AmbiguousRouteError(list(result.matches))
    assert "warehouse-a" in str(exc_info.value)
    assert "warehouse-b" in str(exc_info.value)


def test_legacy_byte_body_entry_still_works() -> None:
    message = HTTPMessage(
        headers={
            "ce-id": "legacy-1",
            "ce-source": "/legacy",
            "ce-type": "com.example.legacy",
            "ce-specversion": "1.0",
            "Content-Type": "application/json",
        },
        body=b'{"bin":"007"}',
    )
    event = from_http_event(message)
    assert event.get_id() == "legacy-1"
    assert event.get_data() == {"bin": "007"}
