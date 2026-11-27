import json

import pytest

from pydantic import BaseModel, OmitReceipt, OnErrorOmit, TypeAdapter, ValidationError, model_validator


class Probe(BaseModel):
    readings: list[OnErrorOmit[int]]
    closed: bool

    @model_validator(mode='after')
    def _must_be_closed(self) -> 'Probe':
        if not self.closed:
            raise ValueError('probe not closed')
        return self


class Station(BaseModel):
    probes: dict[OnErrorOmit[int], OnErrorOmit[Probe]]
    signed_off: bool = True

    @model_validator(mode='after')
    def _sign_off(self) -> 'Station':
        if not self.signed_off:
            raise ValueError('station not signed off')
        return self


class Batch(BaseModel):
    stations: list[OnErrorOmit[Station]]
    confirmed: bool


def batch_data() -> dict:
    return {
        'stations': [
            {
                'probes': {
                    # all of '01', '1' and '+1' normalize to the integer key 1
                    '01': {'readings': ['bad-old', '7'], 'closed': True},
                    '1': {'readings': ['7', 'bad-live'], 'closed': True},
                    '+1': {'readings': ['bad-later', '7'], 'closed': False},
                    'spare': {'readings': ['bad-spare', '7'], 'closed': True},
                }
            },
            {
                'signed_off': False,
                'probes': {'2': {'readings': ['bad-station', '7'], 'closed': True}},
            },
        ],
        'confirmed': True,
    }


@pytest.fixture
def adapter() -> TypeAdapter[Batch]:
    return TypeAdapter(Batch)


def check_batch_receipt(receipt: OmitReceipt) -> None:
    assert receipt.succeeded

    station_0, station_1 = receipt.records
    assert (station_0.locator, station_0.status) == (0, 'kept')
    assert (station_1.locator, station_1.status) == (1, 'omitted')
    assert [e['type'] for e in station_1.errors] == ['value_error']

    # the kept probe is traced back to the raw key '1'
    assert set(station_0.entries) == {'01', '1', '+1', 'spare'}
    kept = station_0.entries['1']
    assert kept.status == 'kept'
    assert kept.validated_key == 1
    assert kept.value_status == 'validated'

    # '01' normalized to the same key and was overwritten by '1'
    overwritten = station_0.entries['01']
    assert overwritten.status == 'overwritten'
    assert overwritten.validated_key == 1

    # its bad reading is still queryable under the overwritten probe
    assert receipt.find(0, '01', 0).status == 'omitted'
    assert receipt.find(0, '01', 0).errors[0]['input'] == 'bad-old'

    # '+1' was omitted because of the `closed` check, without knocking out the kept '1'
    omitted = station_0.entries['+1']
    assert omitted.status == 'omitted'
    assert omitted.value_status == 'omitted'
    assert [e['type'] for e in omitted.errors] == ['value_error']
    assert receipt.find(0, '+1', 0).status == 'omitted'

    # 'spare' only reports the bad key; its value was never validated
    spare = station_0.entries['spare']
    assert spare.status == 'invalid_key'
    assert spare.value_status == 'unvalidated'
    assert spare.validated_key is None
    assert spare.children == ()
    assert [e['type'] for e in spare.errors] == ['int_parsing']

    # the bad reading of the unsigned station is found by its original coordinates
    lost = receipt.find(1, '2', 0)
    assert lost.status == 'omitted'
    assert lost.path == (1, '2', 0)
    assert lost.errors[0]['input'] == 'bad-station'
    assert not lost.recoverable

    # only the bad reading of the kept probe can be re-collected individually
    assert receipt.recoverable == (receipt.find(0, '1', 1),)
    assert receipt.find(0, '1', 1).errors[0]['input'] == 'bad-live'

    assert {record.path for record in receipt.omissions} == {
        (0, '01', 0),
        (0, '1', 1),
        (0, '+1'),
        (0, '+1', 0),
        (0, 'spare'),
        (1,),
        (1, '2', 0),
    }


def test_omit_receipt_validate_python(adapter: TypeAdapter[Batch]) -> None:
    receipt = OmitReceipt()
    batch = adapter.validate_python(batch_data(), omit_receipt=receipt)

    # the validated result is unchanged: station 0 keeps integer key 1 with readings [7]
    assert len(batch.stations) == 1
    assert set(batch.stations[0].probes) == {1}
    assert batch.stations[0].probes[1].readings == [7]

    check_batch_receipt(receipt)


def test_omit_receipt_validate_json(adapter: TypeAdapter[Batch]) -> None:
    receipt = OmitReceipt()
    batch = adapter.validate_json(json.dumps(batch_data()), omit_receipt=receipt)

    assert len(batch.stations) == 1
    assert batch.stations[0].probes[1].readings == [7]

    check_batch_receipt(receipt)


def test_omit_receipt_validation_error(adapter: TypeAdapter[Batch]) -> None:
    receipt = OmitReceipt()
    data = {**batch_data(), 'confirmed': 'not-a-bool'}

    with pytest.raises(ValidationError) as exc_info:
        adapter.validate_python(data, omit_receipt=receipt)

    # the original validation error is reported unchanged
    assert [e['loc'] for e in exc_info.value.errors(include_url=False)] == [('confirmed',)]

    # the omissions and overwrites that already happened can still be reconciled...
    assert not receipt.succeeded
    station_0 = receipt.records[0]
    assert station_0.entries['1'].status == 'kept'
    assert station_0.entries['01'].status == 'overwritten'
    assert station_0.entries['+1'].status == 'omitted'
    assert station_0.entries['spare'].status == 'invalid_key'
    assert receipt.find(1, '2', 0).status == 'omitted'

    # ...but nothing can be re-collected for a batch that failed as a whole
    assert receipt.recoverable == ()


def test_omit_receipt_does_not_change_validator_calls() -> None:
    counts = {'closed': 0, 'sign_off': 0}

    class CountedProbe(BaseModel):
        readings: list[OnErrorOmit[int]]
        closed: bool

        @model_validator(mode='after')
        def _must_be_closed(self) -> 'CountedProbe':
            counts['closed'] += 1
            if not self.closed:
                raise ValueError('probe not closed')
            return self

    class CountedStation(BaseModel):
        probes: dict[OnErrorOmit[int], OnErrorOmit[CountedProbe]]
        signed_off: bool = True

        @model_validator(mode='after')
        def _sign_off(self) -> 'CountedStation':
            counts['sign_off'] += 1
            if not self.signed_off:
                raise ValueError('station not signed off')
            return self

    class CountedBatch(BaseModel):
        stations: list[OnErrorOmit[CountedStation]]
        confirmed: bool

    counted_adapter = TypeAdapter(CountedBatch)

    counted_adapter.validate_python(batch_data())
    without_receipt = dict(counts)

    counts['closed'] = counts['sign_off'] = 0
    counted_adapter.validate_python(batch_data(), omit_receipt=OmitReceipt())
    with_receipt = dict(counts)

    assert with_receipt == without_receipt
    assert with_receipt == {'closed': 4, 'sign_off': 2}


def test_omit_receipt_simple_list() -> None:
    adapter = TypeAdapter(list[OnErrorOmit[int]])
    receipt = OmitReceipt()

    assert adapter.validate_python(['1', 'a', '3'], omit_receipt=receipt) == [1, 3]
    assert receipt.succeeded
    assert [(r.locator, r.status) for r in receipt.records] == [(0, 'kept'), (1, 'omitted'), (2, 'kept')]
    assert [(r.locator, r.status) for r in receipt.omissions] == [(1, 'omitted')]
    assert receipt.recoverable == receipt.omissions
    assert receipt.find(1).errors[0]['type'] == 'int_parsing'


def test_omit_receipt_reused_across_calls() -> None:
    adapter = TypeAdapter(list[OnErrorOmit[int]])
    receipt = OmitReceipt()

    adapter.validate_python(['a'], omit_receipt=receipt)
    assert [(r.locator, r.status) for r in receipt.records] == [(0, 'omitted')]

    adapter.validate_python(['1'], omit_receipt=receipt)
    assert [(r.locator, r.status) for r in receipt.records] == [(0, 'kept')]
    assert receipt.omissions == ()


def test_omit_receipt_not_requested() -> None:
    adapter = TypeAdapter(list[OnErrorOmit[int]])
    assert adapter.validate_python(['1', 'a', '3']) == [1, 3]
    assert adapter.validate_json('["1", "a", "3"]') == [1, 3]


def test_omit_receipt_variadic_tuple() -> None:
    adapter = TypeAdapter(tuple[OnErrorOmit[int], ...])
    receipt = OmitReceipt()

    assert adapter.validate_python(['1', 'a', '3'], omit_receipt=receipt) == (1, 3)
    assert [(r.locator, r.status) for r in receipt.omissions] == [(1, 'omitted')]


def test_omit_receipt_omittable_keys_only() -> None:
    adapter = TypeAdapter(dict[OnErrorOmit[int], str])
    receipt = OmitReceipt()

    assert adapter.validate_python({'1': 'a', 'bad': 'b'}, omit_receipt=receipt) == {1: 'a'}
    assert [(r.locator, r.status, r.value_status) for r in receipt.records] == [
        ('1', 'kept', 'validated'),
        ('bad', 'invalid_key', 'unvalidated'),
    ]


def test_omit_receipt_omittable_values_only() -> None:
    adapter = TypeAdapter(dict[str, OnErrorOmit[int]])
    receipt = OmitReceipt()

    assert adapter.validate_python({'a': '1', 'b': 'x'}, omit_receipt=receipt) == {'a': 1}
    # without omittable keys, entries are located by their position in the input
    assert [(r.locator, r.status) for r in receipt.omissions] == [(1, 'omitted')]
