import pandas as pd
import pytest

from streamz.dataframe.provenance import (
    Attach, ProvenanceDataFrame, ProvenanceError, ProvenanceRecord, RowRef,
    align, provenance_of)


def make_batch(insurance, fees, waybill='W1'):
    """ Two waybill rows sharing Pandas index 7 and the waybill number.

    The first row is cancelled but still carries boxes and fees.  The
    second row holds three boxes (all with display code 'X'): the first is
    not shipped, the second is shipped without fees and the third is
    shipped with ``fees`` in their original order.
    """
    return pd.DataFrame({
        'waybill': [waybill, waybill],
        'batch': ['business', 'business'],
        'status': ['cancelled', 'active'],
        'insurance': [0, insurance],
        'boxes': [
            [{'code': 'X', 'shipped': True, 'fees': [5]}],
            [
                {'code': 'X', 'shipped': False, 'fees': []},
                {'code': 'X', 'shipped': True, 'fees': []},
                {'code': 'X', 'shipped': True, 'fees': list(fees)},
            ],
        ],
    }, index=[7, 7])


def make_empty_batch():
    """ A batch whose boxes are all filtered out (none shipped) """
    return pd.DataFrame({
        'waybill': ['W4'],
        'batch': ['business'],
        'status': ['active'],
        'insurance': [45],
        'boxes': [[{'code': 'X', 'shipped': False, 'fees': []}]],
    }, index=[7])


def unpack_boxes(df):
    return df.assign(code=df['boxes'].str['code'],
                     shipped=df['boxes'].str['shipped'])


def unpack_fees(df):
    return df.assign(fee=df['boxes'].str['fees'])


def reverse(df):
    return df.iloc[::-1]


def reverse_correspondence(record, x, result):
    return record.take(range(len(x) - 1, -1, -1))


def build_pipeline():
    """ Boxes, fees and insurance processed as three independent branches """
    example = make_batch(90, [7, 3]).iloc[:0]
    box_src = ProvenanceDataFrame(example=example)
    fee_src = ProvenanceDataFrame(example=example)
    ins_src = ProvenanceDataFrame(example=example)

    # Box branch: drop cancelled waybills, expand boxes, keep shipped ones,
    # reverse the order and reset the index.  The reversal changes row
    # correspondence, so it is declared explicitly.
    boxes = box_src.query("status == 'active'").explode('boxes')
    boxes = boxes.map_partitions(unpack_boxes)
    boxes = boxes.query("shipped")
    boxes = boxes[['waybill', 'batch', 'code', 'shipped']]
    boxes = boxes.map_partitions(reverse, provenance=reverse_correspondence)
    boxes = boxes.reset_index()

    # Fee branch: independently expand the same original box list, then
    # split the fees.  ``fee_values`` is the point where externally
    # sharded fees with explicit correspondence can be injected.
    fees = fee_src.explode('boxes')
    fees = fees.map_partitions(unpack_fees)
    fees = fees.explode('fee').query("fee == fee")
    fee_values = fees[['fee']]

    # Insurance branch: one row per waybill, used as-is.
    combined = align(boxes, attach=[
        Attach(fee_values, column='fee', into='fees', kind='collect'),
        Attach(ins_src, column='insurance', into='insurance', kind='broadcast'),
    ])
    return box_src, fee_src, ins_src, fee_values, combined


def test_row_ref_navigation():
    ref = RowRef('A', (1, 2, 0))
    assert ref.parent() == RowRef('A', (1, 2))
    assert ref.parent().parent() == RowRef('A', (1,))
    assert RowRef('A', (1,)).child(2) == RowRef('A', (1, 2))
    with pytest.raises(ValueError):
        RowRef('A', ()).parent()


def test_align_by_original_item():
    box_src, fee_src, ins_src, fee_values, combined = build_pipeline()
    results = combined.stream.sink_to_list()
    provenance = provenance_of(combined).sink_to_list()
    errors = combined.errors.sink_to_list()

    A = make_batch(90, [7, 3])
    B = make_batch(120, [11, 5])
    D = make_empty_batch()

    box_src.emit(A, batch='A')
    # Fee branch delivers batch A in shards: the third box's fee 3 first,
    # the cancelled waybill's fees in between, fee 7 in a later shard.
    fee_values.emit(pd.DataFrame({'fee': [3]}), batch='A', final=False,
                    rows=[RowRef('A', (1, 2, 1))])
    fee_src.emit(A.iloc[:1], batch='A', final=False, rows=[RowRef('A', (0,))])
    # Batch B looks identical on display values but is a different batch;
    # all of its branches complete first.
    box_src.emit(B, batch='B')
    fee_src.emit(B, batch='B')
    ins_src.emit(B, batch='B')
    assert len(results) == 1  # B is complete and delivered
    # A batch whose boxes are all filtered out still completes (empty).
    box_src.emit(D, batch='D')
    fee_src.emit(D, batch='D')
    ins_src.emit(D, batch='D')
    assert len(results) == 2
    # A's second fee shard arrives; the insurance is still missing, so the
    # waiting batch must not be delivered (not even as an empty batch).
    fee_values.emit(pd.DataFrame({'fee': [7]}), batch='A', final=True,
                    rows=[RowRef('A', (1, 2, 0))])
    assert len(results) == 2
    ins_src.emit(A, batch='A')
    assert len(results) == 3
    # Exactly once: late shards of an already delivered batch are ignored.
    fee_values.emit(pd.DataFrame({'fee': [42]}), batch='A', final=True,
                    rows=[RowRef('A', (1, 2, 0))])
    assert len(results) == 3
    assert errors == []

    b_df, d_df, a_df = results

    # Batch A: boxes in reversed order (third then second), fees in their
    # original order, insurance of their own waybill on both.
    assert list(a_df['code']) == ['X', 'X']
    assert list(a_df['fees']) == [[7, 3], []]
    assert list(a_df['insurance']) == [90, 90]
    # Batch B: identical display values, but results belong to its own batch.
    assert list(b_df['fees']) == [[11, 5], []]
    assert list(b_df['insurance']) == [120, 120]
    # The fully filtered batch is delivered once, completed and empty.
    assert len(d_df) == 0
    assert list(d_df.columns) == list(a_df.columns)

    # Inspectors can trace every row back to the original batch, the
    # original waybill and box, and each fee's original position.
    a_prov, b_prov = provenance[2], provenance[0]
    assert a_prov.batch == 'A'
    assert [row['origin'] for row in a_prov.rows] == [
        RowRef('A', (1, 2)), RowRef('A', (1, 1))]
    assert a_prov.rows[0]['fees'] == [RowRef('A', (1, 2, 0)),
                                      RowRef('A', (1, 2, 1))]
    assert a_prov.rows[1]['fees'] == []
    assert [row['insurance'] for row in a_prov.rows] == [
        RowRef('A', (1,)), RowRef('A', (1,))]
    assert b_prov.batch == 'B'
    assert [row['origin'] for row in b_prov.rows] == [
        RowRef('B', (1, 2)), RowRef('B', (1, 1))]
    assert b_prov.rows[0]['fees'] == [RowRef('B', (1, 2, 0)),
                                      RowRef('B', (1, 2, 1))]


def test_invalid_fee_reports_batch_and_position():
    box_src, fee_src, ins_src, fee_values, combined = build_pipeline()
    results = combined.stream.sink_to_list()
    errors = combined.errors.sink_to_list()

    C = make_batch(60, [1])
    box_src.emit(C, batch='C')
    fee_src.emit(C, batch='C', final=False)
    ins_src.emit(C, batch='C')
    # The fee shards are not finished yet: nothing may be delivered.
    assert results == []
    # A fee arrives for box position 5, which does not exist in the
    # original waybill (it has three boxes), even though the display box
    # code 'X' matches the existing boxes.
    fee_values.emit(pd.DataFrame({'fee': [99]}), batch='C', final=True,
                    rows=[RowRef('C', (1, 5, 0))])
    assert len(errors) == 1
    error = errors[0]
    assert isinstance(error, ProvenanceError)
    assert error.batch == 'C'
    assert error.ref == RowRef('C', (1, 5, 0))
    # The batch delivers no partial results...
    assert results == []
    # ...and later batches are still ingested normally.
    E = make_batch(30, [2])
    box_src.emit(E, batch='E')
    fee_src.emit(E, batch='E')
    ins_src.emit(E, batch='E')
    assert len(results) == 1
    assert list(results[0]['fees']) == [[2], []]
    assert list(results[0]['insurance']) == [30, 30]


def test_provenance_survives_query_projection_reset_index():
    box_src, fee_src, ins_src, fee_values, combined = build_pipeline()
    downstream = (combined
                  .query("insurance >= 100")
                  [['waybill', 'code', 'fees', 'insurance']]
                  .reset_index())
    results = downstream.stream.sink_to_list()
    provenance = provenance_of(downstream).sink_to_list()

    A = make_batch(90, [7, 3])
    B = make_batch(120, [11, 5])
    for batch_df, batch in [(A, 'A'), (B, 'B')]:
        box_src.emit(batch_df, batch=batch)
        fee_src.emit(batch_df, batch=batch)
        ins_src.emit(batch_df, batch=batch)

    assert len(results) == 2
    # Batch A (insurance 90) is filtered out by the query.
    assert len(results[0]) == 0
    # Batch B survives with the projected columns only.
    assert list(results[1].columns) == ['index', 'waybill', 'code', 'fees',
                                        'insurance']
    assert list(results[1]['fees']) == [[11, 5], []]
    assert list(results[1]['insurance']) == [120, 120]
    # Provenance followed the surviving rows through all three operations.
    rows = provenance[1].rows
    assert [row['origin'] for row in rows] == [RowRef('B', (1, 2)),
                                               RowRef('B', (1, 1))]
    assert rows[0]['fees'] == [RowRef('B', (1, 2, 0)), RowRef('B', (1, 2, 1))]
    assert rows[0]['insurance'] == RowRef('B', (1,))


def test_independent_explodes_recognize_same_box():
    example = make_batch(90, [7, 3]).iloc[:0]
    first_src = ProvenanceDataFrame(example=example)
    second_src = ProvenanceDataFrame(example=example)
    first = first_src.explode('boxes')
    second = second_src.explode('boxes')
    first_prov = provenance_of(first).sink_to_list()
    second_prov = provenance_of(second).sink_to_list()

    df = make_batch(90, [7, 3])
    first_src.emit(df, batch='B')
    second_src.emit(df, batch='B')

    # Exploding the same original box list independently attributes the
    # same positions, so both branches agree on which box is which.
    assert first_prov[0].rows == second_prov[0].rows
    assert first_prov[0].rows[-1] == RowRef('B', (1, 2))


def test_example_and_dtypes_do_not_emit_results():
    _, _, _, _, combined = build_pipeline()
    results = combined.stream.sink_to_list()

    columns = list(combined.columns)
    assert columns == ['index', 'waybill', 'batch', 'code', 'shipped',
                       'fees', 'insurance']
    assert combined.dtypes['fees'] == object
    assert len(combined.example) == 0
    # Reading example columns and dtypes produced no business results.
    assert results == []


def test_plain_index_and_same_named_columns_unaffected():
    example = make_batch(90, [7, 3]).iloc[:0]
    src = ProvenanceDataFrame(example=example)
    # A business column that happens to share the name of a provenance
    # concept stays an ordinary, readable and writable column.
    assert 'batch' in src.columns
    series = src['batch']
    assert series.example.dtype == src.example['batch'].dtype
    src['extra'] = 1
    assert 'extra' in src.columns

    results = src.stream.sink_to_list()
    df = make_batch(90, [7, 3])
    df['extra'] = 1
    src.emit(df, batch='A')
    # The duplicated Pandas index 7 flows through untouched.
    assert list(results[0].index) == [7, 7]
    assert list(results[0]['batch']) == ['business', 'business']


def test_record_transformations_do_not_mutate():
    record = ProvenanceRecord('A', [RowRef('A', (0,)), RowRef('A', (1,))],
                              final=False)
    taken = record.take([1])
    assert taken.rows == [RowRef('A', (1,))]
    assert taken.final is False
    assert len(record.rows) == 2  # original record untouched
