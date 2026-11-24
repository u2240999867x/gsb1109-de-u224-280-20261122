from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array import from_array
from dask.array.reshape import (
    _smooth_chunks,
    contract_tuple,
    expand_tuple,
    reshape_blockwise,
    reshape_rechunk,
)
from dask.array.utils import assert_eq


@pytest.mark.parametrize(
    "inshape,outshape,prechunks,inchunks,outchunks",
    [
        ((4,), (4,), ((2, 2),), ((2, 2),), ((2, 2),)),
        ((4,), (2, 2), ((2, 2),), ((2, 2),), ((1, 1), (2,))),
        ((4,), (4, 1), ((2, 2),), ((2, 2),), ((2, 2), (1,))),
        ((4,), (1, 4), ((2, 2),), ((2, 2),), ((1,), (2, 2))),
        ((1, 4), (4,), ((1,), (2, 2)), ((1,), (2, 2)), ((2, 2),)),
        ((4, 1), (4,), ((2, 2), (1,)), ((2, 2), (1,)), ((2, 2),)),
        (
            (4, 1, 4),
            (4, 4),
            ((2, 2), (1,), (2, 2)),
            ((2, 2), (1,), (2, 2)),
            ((2, 2), (2, 2)),
        ),
        ((4, 4), (4, 1, 4), ((2, 2), (2, 2)), ((2, 2), (2, 2)), ((2, 2), (1,), (2, 2))),
        ((2, 2), (4,), ((2,), (2,)), ((2,), (2,)), ((4,),)),
        ((2, 2), (4,), ((1, 1), (2,)), ((1, 1), (2,)), ((2, 2),)),
        ((2, 2), (4,), ((2,), (1, 1)), ((1, 1), (2,)), ((2, 2),)),
        (
            (64,),
            (4, 4, 4),
            ((8, 8, 8, 8, 8, 8, 8, 8),),
            ((8, 8, 8, 8, 8, 8, 8, 8),),
            ((1, 1, 1, 1), (2, 2), (4,)),
        ),
        ((64,), (4, 4, 4), ((32, 32),), ((32, 32),), ((2, 2), (4,), (4,))),
        ((64,), (4, 4, 4), ((16, 48),), ((16, 48),), ((1, 3), (4,), (4,))),
        ((64,), (4, 4, 4), ((20, 44),), ((16, 16, 32),), ((1, 1, 2), (4,), (4,))),
        (
            (64, 4),
            (8, 8, 4),
            ((16, 16, 16, 16), (2, 2)),
            ((16, 16, 16, 16), (2, 2)),
            ((2, 2, 2, 2), (8,), (2, 2)),
        ),
    ],
)
def test_reshape_rechunk(inshape, outshape, prechunks, inchunks, outchunks):
    result_in, result_out, _, _ = reshape_rechunk(inshape, outshape, prechunks)
    assert result_in == inchunks
    assert result_out == outchunks
    assert np.prod(list(map(len, result_in))) == np.prod(list(map(len, result_out)))


def test_expand_tuple():
    assert expand_tuple((2, 4), 2) == (1, 1, 2, 2)
    assert expand_tuple((2, 4), 3) == (1, 1, 1, 1, 2)
    assert expand_tuple((3, 4), 2) == (1, 2, 2, 2)
    assert expand_tuple((7, 4), 3) == (2, 2, 3, 1, 1, 2)


def test_contract_tuple():
    assert contract_tuple((1, 1, 2, 3, 1), 2) == (2, 2, 2, 2)
    assert contract_tuple((1, 1, 2, 5, 1), 2) == (2, 2, 4, 2)
    assert contract_tuple((2, 4), 2) == (2, 4)
    assert contract_tuple((2, 4), 3) == (6,)


def test_reshape_unknown_sizes():
    a = np.random.random((10, 6, 6))
    A = da.from_array(a, chunks=(5, 2, 3))

    a2 = a.reshape((60, -1))
    A2 = A.reshape((60, -1))

    assert A2.shape == (60, 6)
    assert_eq(A2, a2)

    with pytest.raises(ValueError):
        a.reshape((60, -1, -1))
    with pytest.raises(ValueError):
        A.reshape((60, -1, -1))


@pytest.mark.parametrize(
    "inshape, inchunks, outshape, outchunks",
    [
        # (2, 3, 4) -> (6, 4)
        ((2, 3, 4), ((1, 1), (1, 2), (2, 2)), (6, 4), ((1, 2, 1, 2), (2, 2))),
        # (1, 2, 3, 4) -> (12, 4)
        ((1, 2, 3, 4), ((1,), (1, 1), (1, 2), (2, 2)), (6, 4), ((1, 2, 1, 2), (2, 2))),
        # (2, 2, 3, 4) -> (12, 4)
        (
            (2, 2, 3, 4),
            ((1, 1), (1, 1), (1, 2), (2, 2)),
            (12, 4),
            ((1, 2, 1, 2, 1, 2, 1, 2), (2, 2)),
        ),
        # (2, 2, 3, 4) -> (4, 3, 4)
        (
            (2, 2, 3, 4),
            ((1, 1), (1, 1), (1, 2), (2, 2)),
            (4, 3, 4),
            ((1, 1, 1, 1), (1, 2), (2, 2)),
        ),
        # (2, 2, 3, 4) -> (4, 3, 4)
        ((2, 2, 3, 4), ((1, 1), (2,), (1, 2), (4,)), (4, 3, 4), ((2, 2), (1, 2), (4,))),
        # (2, 3, 4) -> (24,).
        ((2, 3, 4), ((1, 1), (1, 1, 1), (2, 2)), (24,), ((2,) * 12,)),
        # (2, 3, 4) -> (2, 12)
        ((2, 3, 4), ((1, 1), (1, 1, 1), (4,)), (2, 12), ((1, 1), (4,) * 3)),
    ],
)
def test_reshape_all_chunked_no_merge(inshape, inchunks, outshape, outchunks):
    # https://github.com/dask/dask/issues/5544#issuecomment-712280433
    # When the early axes are completely chunked then we are just moving blocks
    # and can avoid any rechunking. The result inchunks are the same as the
    # input chunks.
    base = np.arange(np.prod(inshape)).reshape(inshape)
    a = da.from_array(base, chunks=inchunks)

    # test directly
    inchunks2, outchunks2, _, _ = reshape_rechunk(a.shape, outshape, inchunks)
    assert inchunks2 == inchunks
    assert outchunks2 == outchunks

    # and via reshape
    result = a.reshape(outshape)
    assert result.chunks == outchunks
    assert_eq(result, base.reshape(outshape))


@pytest.mark.parametrize(
    "inshape, inchunks, expected_inchunks, outshape, outchunks",
    [
        # (2, 3, 4) -> (24,). This does merge, since the second dim isn't fully chunked!
        (
            (2, 3, 4),
            ((1, 1), (1, 2), (2, 2)),
            (
                (1, 1),
                (
                    1,
                    1,
                    1,
                ),
                (4,),
            ),
            (24,),
            ((4, 4, 4, 4, 4, 4),),
        ),
    ],
)
def test_reshape_all_not_chunked_merge(
    inshape, inchunks, expected_inchunks, outshape, outchunks
):
    base = np.arange(np.prod(inshape)).reshape(inshape)
    a = da.from_array(base, chunks=inchunks)

    # test directly
    inchunks2, outchunks2, _, _ = reshape_rechunk(a.shape, outshape, inchunks)
    assert inchunks2 == expected_inchunks
    assert outchunks2 == outchunks

    # and via reshape
    result = a.reshape(outshape)
    assert result.chunks == outchunks
    assert_eq(result, base.reshape(outshape))


@pytest.mark.parametrize(
    "inshape, inchunks, outshape, outchunks",
    [
        # (2, 3, 4) -> (6, 4)
        ((2, 3, 4), ((2,), (1, 2), (2, 2)), (6, 4), ((1, 2, 1, 2), (2, 2))),
        # (1, 2, 3, 4) -> (12, 4)
        ((1, 2, 3, 4), ((1,), (2,), (1, 2), (2, 2)), (6, 4), ((1, 2, 1, 2), (2, 2))),
        # (2, 2, 3, 4) -> (12, 4) (3 cases)
        (
            (2, 2, 3, 4),
            ((1, 1), (2,), (1, 2), (2, 2)),
            (12, 4),
            ((1, 2, 1, 2, 1, 2, 1, 2), (2, 2)),
        ),
        (
            (2, 2, 3, 4),
            ((2,), (1, 1), (1, 2), (2, 2)),
            (12, 4),
            ((1, 2, 1, 2, 1, 2, 1, 2), (2, 2)),
        ),
        (
            (2, 2, 3, 4),
            ((2,), (2,), (1, 2), (2, 2)),
            (12, 4),
            ((1, 2, 1, 2, 1, 2, 1, 2), (2, 2)),
        ),
        # (2, 2, 3, 4) -> (4, 3, 4)
        # TODO: I'm confused about the behavior in this case.
        # (
        #     (2, 2, 3, 4),
        #     ((2,), (2,), (1, 2), (2, 2)),
        #     (4, 3, 4),
        #     ((1, 1, 1, 1), (1, 2), (2, 2)),
        # ),
        # (2, 2, 3, 4) -> (4, 3, 4)
        ((2, 2, 3, 4), ((2,), (2,), (1, 2), (4,)), (4, 3, 4), ((2, 2), (1, 2), (4,))),
    ],
)
def test_reshape_merge_chunks(inshape, inchunks, outshape, outchunks):
    # https://github.com/dask/dask/issues/5544#issuecomment-712280433
    # When the early axes are completely chunked then we are just moving blocks
    # and can avoid any rechunking. The outchunks will always be ...
    base = np.arange(np.prod(inshape)).reshape(inshape)
    a = da.from_array(base, chunks=inchunks)

    # and via reshape
    result = a.reshape(outshape, merge_chunks=False)
    assert result.chunks == outchunks
    assert_eq(result, base.reshape(outshape))

    assert result.chunks != a.reshape(outshape).chunks


def test_smooth_chunks():
    ii = 2
    ileft = 1
    result_inchunks = [None, (1, 1, 1, 1, 1, 1, 1, 1, 1, 1), (20,)]
    result_in = _smooth_chunks(ileft, ii, 2, result_inchunks)
    expected_in = [None, (1, 1, 1, 1, 1, 1, 1, 1, 1, 1), (2, 2, 2, 2, 2, 2, 2, 2, 2, 2)]
    assert result_in == expected_in


def test_smooth_chunks_not_divisible():
    ii = 2
    ileft = 1
    result_inchunks = [None, (1,) * 36, (9,)]
    result_in = _smooth_chunks(ileft, ii, 6, result_inchunks)
    expected_in = [None, (1,) * 36, (4, 5)]
    assert result_in == expected_in


def test_smooth_chunks_first_dimension():
    ii = 1
    ileft = 0
    result_inchunks = [
        (1, 1, 3, 1, 1, 3),
        (6,),
        (
            3,
            3,
        ),
    ]
    result_in = _smooth_chunks(ileft, ii, 12, result_inchunks)
    expected_in = [(1, 1, 1, 2, 1, 1, 1, 2), (6,), (3, 3)]
    assert result_in == expected_in
    result_in = _smooth_chunks(ileft, ii, 6, result_inchunks)
    expected_in = [(1, 1, 1, 1, 1, 1, 1, 1, 1, 1), (6,), (3, 3)]
    assert result_in == expected_in


def test_smooth_chunks_large_dimension():
    # Ensure that we properly smooth 2 dimensions
    ii = 3
    ileft = 1
    result_inchunks = [None, (1, 1), (2,), (400,)]
    result_in = _smooth_chunks(ileft, ii, 4, result_inchunks)
    expected_in = [None, (1, 1), (1, 1), (4,) * 100]
    assert result_in == expected_in


def test_reshape_not_squashing_chunks():
    arr = np.arange(0, 30 * 10 * 20).reshape(30, 10, 20)
    darr = da.from_array(arr, chunks=(30, 2, 1))

    result = darr.reshape(30, 200)

    assert result.chunks == ((30,), (2,) * 100)
    assert np.prod(result.shape) == np.prod(darr.shape)
    assert_eq(result, arr.reshape(30, 200))


@pytest.mark.parametrize(
    "reshaper, chunks",
    [
        ((2, 2, 100), ((1, 1), (1, 1), (20,) * 5)),
        ((2, 200), ((1, 1), (20,) * 10)),
        ((10, 10, 4), ((1,) * 10, (5, 5), (4,))),
        ((2, 25, 8), ((1,) * 2, (2,) * 5 + (3,) * 5, (8,))),
    ],
)
def test_reshape_to_higher_dimension(reshaper, chunks):
    arr = np.arange(0, 4_000).reshape(10, 400)
    darr = da.from_array(arr, chunks=(5, (20,) * 19 + (19, 1)))

    result = darr.reshape(10, *reshaper)

    assert result.chunks == ((5, 5),) + chunks
    assert np.prod(result.shape) == np.prod(darr.shape)
    assert_eq(result, arr.reshape(10, *reshaper))


def test_reshape_lower_dimension():
    arr = da.ones((20, 20, 5), chunks=(10, 10, 5))
    result = arr.reshape(400, 5)
    assert result.chunks == ((100,) * 4, (5,))


def test_reshape_split_out_chunks():
    # too large to assert the actual result
    arr = da.ones(shape=(2, 1000, 100, 4800), chunks=(2, 1000, 100, 83))
    result = arr.reshape(2, 1000, 100, 4, 1200)
    assert result.chunks == (
        (2,),
        (1000,),
        (100,),
        (1, 1, 1, 1),
        (80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80),
    )


def test_reshape_blockwise():
    x = np.arange(0, 54).reshape(6, 3, 3)
    arr = from_array(x, chunks=(3, 2, (2, 1)))
    assert arr.chunks == ((3, 3), (2, 1), (2, 1))
    result = reshape_blockwise(arr, (18, 3))
    assert result.chunks == ((6, 3, 6, 3), (2, 1))
    expected = [
        np.concatenate(
            [x[:3, :2, :2].reshape(6, 2), x[:3, :2, 2:].reshape(6, 1)], axis=1
        ),
        np.concatenate(
            [x[:3, 2:, :2].reshape(3, 2), x[:3, 2:, 2:].reshape(3, 1)], axis=1
        ),
        np.concatenate(
            [x[3:, :2, :2].reshape(6, 2), x[3:, :2, 2:].reshape(6, 1)], axis=1
        ),
        np.concatenate(
            [x[3:, 2:, :2].reshape(3, 2), x[3:, 2:, 2:].reshape(3, 1)], axis=1
        ),
    ]
    expected = np.concatenate(expected, axis=0)
    assert_eq(result, expected)
    result2 = reshape_blockwise(result, (6, 3, 3), chunks=arr.chunks)
    assert_eq(arr, result2)
    assert result2.chunks == arr.chunks

    result = reshape_blockwise(arr, (6, 9))
    assert result.chunks == ((3, 3), (4, 2, 2, 1))
    expected = [
        np.concatenate(
            [
                x[:3, :2, :2].reshape(3, 4),
                x[:3, :2, 2:].reshape(3, 2),
                x[:3, 2:, :2].reshape(3, 2),
                x[:3, 2:, 2:].reshape(3, 1),
            ],
            axis=1,
        ),
        np.concatenate(
            [
                x[3:, :2, :2].reshape(3, 4),
                x[3:, :2, 2:].reshape(3, 2),
                x[3:, 2:, :2].reshape(3, 2),
                x[3:, 2:, 2:].reshape(3, 1),
            ],
            axis=1,
        ),
    ]

    # chunks of first half of the array, i.e. arr[:3, :, :]:
    # [[[0, 1], [3, 4]], [[9, 10], [12, 13]], [[18, 19], [21, 22]]]
    # [[[2], [5]], [[11], [14]], [[20], [23]]]
    # [[[6, 7], [15, 16]], [[24, 25]]]
    # [[[8], [17]], [[26]]]
    # The inner dimensions are squashed, i.e.
    # [[ 0  1  3  4  2  5  6  7  8]
    #  [ 9 10 12 13 11 14 15 16 17]
    #  [18 19 21 22 20 23 24 25 26]]
    expected = np.concatenate(expected, axis=0)
    assert_eq(result, expected)
    result2 = reshape_blockwise(result, (6, 3, 3), chunks=arr.chunks)
    assert_eq(arr, result2)
    assert result2.chunks == arr.chunks

    with pytest.raises(
        ValueError, match="Chunk sizes do not match for the following chunks"
    ):
        reshape_blockwise(result, (6, 3, 3), chunks=((3, 4), (3, 1), (3, 1)))


def test_reshape_blockwise_raises_for_expansion():
    x = np.arange(0, 54).reshape(6, 3, 3)
    arr = from_array(x, chunks=(3, 2, (2, 1)))
    msg = "Need to specify chunks if expanding dimensions."
    with pytest.raises(TypeError, match=msg):
        reshape_blockwise(arr, (2, 3, 3, 3))

    with pytest.raises(
        NotImplementedError,
        match="reshape_blockwise not implemented for expanding dimensions",
    ):
        reshape_blockwise(arr, (3, 2, 9))


def test_reshape_blockwise_dimension_reduction_and_chunks():
    x = np.arange(0, 54).reshape(6, 3, 3)
    arr = from_array(x, chunks=(3, 2, (2, 1)))
    with pytest.raises(ValueError, match="Setting chunks is not allowed when"):
        reshape_blockwise(arr, (18, 3), arr.chunks)


def test_reshape_blockwise_identity():
    x = np.arange(0, 54).reshape(6, 3, 3)
    arr = from_array(x, chunks=(3, 2, (2, 1)))
    result = reshape_blockwise(arr, (6, 3, 3))
    assert len(arr.dask) == len(result.dask)
    assert_eq(result, x)

    result = reshape_blockwise(arr, (6, 3, 3), chunks=arr.chunks)
    assert len(arr.dask) == len(result.dask)
    assert_eq(result, x)


def test_argwhere_reshaping_not_concatenating_chunks():
    # GH#10080
    arr = da.random.random((500, 500, 500), chunks=(100, 100, 100)) < 0
    result = da.argwhere(arr)
    assert result.chunks == ((np.nan,) * 125, (1, 1, 1))


def test_blockwise_reshape_sanity_checks():
    x = np.arange(0, 20).reshape(10, 2)
    arr = from_array(x, chunks=(5, 1))
    with pytest.raises(ValueError, match="total size of new array must be unchanged"):
        reshape_blockwise(arr, (10, 6))

    arr = from_array(x, chunks=(5, 1))
    arr = arr[arr[:, 0] > 0]
    with pytest.raises(ValueError, match="Array chunk size or shape is unknown"):
        reshape_blockwise(arr, (5, 2, 2))


def _unknown_1d_array(parts, dtype=np.int64):
    """Build a 1-D array with one unknown-length block per ``parts`` entry."""
    from dask import delayed

    def _materialize(values):
        return np.asarray(values, dtype=dtype)

    blocks = [
        da.from_delayed(
            delayed(_materialize)(part),
            shape=(np.nan,),
            dtype=dtype,
        )
        for part in parts
    ]
    return da.concatenate(blocks, axis=0)


def test_reshape_lazy_frames_basic():
    parts = [[0, 1, 2], [], [3], [], [4, 5, 6, 7, 8, 9, 10, 11]]
    arr = _unknown_1d_array(parts)

    framed = arr.reshape((-1, 4))
    assert np.isnan(framed.shape[0]) and framed.shape[1] == 4
    assert len(framed.chunks[0]) == 5
    assert all(np.isnan(c) for c in framed.chunks[0])
    assert framed.chunks[1] == (4,)
    # one output block per input block, selectable before execution
    assert framed.npartitions == arr.npartitions

    expected = np.arange(12).reshape(-1, 4)
    assert_eq(framed, expected)

    block_shapes = [framed.blocks[i, 0].compute().shape for i in range(5)]
    assert block_shapes == [(0, 4), (0, 4), (1, 4), (0, 4), (2, 4)]
    assert_eq(framed.blocks[2, 0], np.array([[0, 1, 2, 3]]))
    assert_eq(
        framed.blocks[4, 0],
        np.array([[4, 5, 6, 7], [8, 9, 10, 11]]),
    )


def test_reshape_lazy_frames_width_six():
    parts = [[0, 1, 2], [], [3], [], [4, 5, 6, 7, 8, 9, 10, 11]]
    arr = _unknown_1d_array(parts)

    framed = arr.reshape((-1, 6))
    assert np.isnan(framed.shape[0]) and framed.shape[1] == 6
    # both frames cross multiple files and finish in the last file
    block_shapes = [framed.blocks[i, 0].compute().shape for i in range(5)]
    assert block_shapes == [(0, 6), (0, 6), (0, 6), (0, 6), (2, 6)]
    assert_eq(framed, np.arange(12).reshape(-1, 6))


def test_reshape_lazy_frames_row_sums():
    parts = [[0, 1, 2], [], [3], [], [4, 5, 6, 7, 8, 9, 10, 11]]
    arr = _unknown_1d_array(parts)

    sums4 = arr.reshape((-1, 4)).sum(axis=1)
    sums6 = arr.reshape((-1, 6)).sum(axis=1)
    assert_eq(sums4, np.array([6, 22, 38]))
    assert_eq(sums6, np.array([15, 51]))


@pytest.mark.parametrize(
    "lengths,width",
    [
        ([3, 0, 1, 0, 8], 4),
        ([7, 0, 3, 11, 2, 0, 7], 5),
        ([1, 1, 1, 1], 2),
        ([6], 3),
        ([0, 0, 6, 0], 3),
        ([2, 2, 2, 2, 2], 1),
    ],
)
def test_reshape_lazy_frames_matches_numpy(lengths, width):
    total = sum(lengths)
    assert total % width == 0
    values = np.arange(total, dtype=np.int64)
    parts = []
    start = 0
    for length in lengths:
        parts.append(values[start : start + length])
        start += length

    framed = _unknown_1d_array(parts).reshape((-1, width))
    assert_eq(framed, values.reshape(-1, width))

    # frame attribution: frames belong to the block holding their last record
    expected_counts = []
    cursor = 0
    for length in lengths:
        end = cursor + length
        expected_counts.append(end // width - cursor // width)
        cursor = end
    block_counts = [
        framed.blocks[i, 0].compute().shape[0] for i in range(len(lengths))
    ]
    assert block_counts == expected_counts


def test_reshape_lazy_frames_empty_batch():
    arr = _unknown_1d_array([[], [], []])
    framed = arr.reshape((-1, 4))
    assert np.isnan(framed.shape[0]) and framed.shape[1] == 4
    result = framed.compute()
    assert result.shape == (0, 4)
    assert framed.blocks[1, 0].compute().shape == (0, 4)
    assert_eq(framed.sum(axis=1), np.empty((0,), dtype=np.int64))


def test_reshape_lazy_frames_incomplete_batch_raises():
    # 5 records cannot be framed with width 4
    arr = _unknown_1d_array([[0, 1, 2], [], [3, 4]])
    framed = arr.reshape((-1, 4))

    # construction must not raise or run any cleaning
    with pytest.raises(ValueError, match="not divisible"):
        framed.compute()
    # selecting a single block cannot drop the validation
    with pytest.raises(ValueError, match="not divisible"):
        framed.blocks[0, 0].compute()
    with pytest.raises(ValueError, match="not divisible"):
        framed.sum(axis=1).compute()


def test_reshape_lazy_frames_invalid_width():
    arr = _unknown_1d_array([[1, 2]])
    with pytest.raises(ValueError, match="positive integer"):
        arr.reshape((-1, 0))
    # two unknown dimensions are rejected by the generic reshape rule
    with pytest.raises(ValueError, match="one unknown dimension"):
        arr.reshape((-1, -1))


def test_reshape_lazy_frames_known_length_unchanged():
    # arrays with known lengths keep using the ordinary reshape
    arr = from_array(np.arange(12), chunks=4)
    result = arr.reshape((-1, 4))
    assert result.name.startswith("reshape-")
    assert_eq(result, np.arange(12).reshape(3, 4))


def test_reshape_lazy_frames_log_depth_and_bounded_state():
    import math

    npartitions = 64
    arr = _unknown_1d_array([[i] for i in range(npartitions)])
    framed = arr.reshape((-1, 2))
    layer = framed.dask.layers[framed.name]

    def _depth(graph):
        depths = {}
        remaining = set(graph)
        while remaining:
            for key in list(remaining):
                deps = getattr(graph[key], "dependencies", None) or ()
                if all(dep not in graph or dep in depths for dep in deps):
                    depths[key] = 1 + max(
                        (depths[dep] for dep in deps if dep in depths), default=0
                    )
                    remaining.discard(key)
        return max(depths.values())

    # the reshape layer's depth grows logarithmically with the file count
    assert _depth(dict(layer)) <= math.ceil(math.log2(npartitions)) + 3
    assert_eq(framed, np.arange(npartitions).reshape(-1, 2))

