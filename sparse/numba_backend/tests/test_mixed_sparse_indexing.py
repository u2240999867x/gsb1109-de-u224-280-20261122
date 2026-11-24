import sparse
from sparse.numba_backend._utils import assert_eq

import pytest

import numpy as np


def _sparse_index(dense_index, fill_value, format):
    return sparse.COO.from_numpy(np.asarray(dense_index), fill_value=fill_value).asformat(format)


@pytest.mark.parametrize("format", ["coo", "gcxs"])
@pytest.mark.parametrize("index_format", ["coo", "gcxs", "dok"])
@pytest.mark.parametrize("mask_fill", [False, True])
@pytest.mark.parametrize("cat_fill", [0, 1])
@pytest.mark.parametrize("fill_value", [0, 2, np.nan])
def test_mixed_mask_slice_category(format, index_format, mask_fill, cat_fill, fill_value):
    rng = np.random.default_rng(42)
    dense = rng.integers(-5, 5, size=(6, 4, 3)).astype(float)
    x = sparse.COO.from_numpy(dense, fill_value=fill_value).asformat(format)
    dense_mask = np.array([True, False, True, True, False, True])
    dense_cat = np.array([0, 2, 1, 0])
    mask = _sparse_index(dense_mask, mask_fill, index_format)
    cat = _sparse_index(dense_cat, cat_fill, index_format)

    result = x[mask, ::-1, cat]
    assert isinstance(result, type(x))
    assert_eq(result, dense[dense_mask, ::-1, dense_cat])
    # The operands are left unchanged.
    assert_eq(x, dense)
    assert_eq(mask, dense_mask)
    assert_eq(cat, dense_cat)


def test_mixed_store_time_category_example():
    # Five stores, store 1 excluded; category defaults to 0 with exceptions
    # at selected positions 1 and 3. Each kept store pairs with its category
    # and yields its full reversed timeline.
    dense = np.arange(5 * 3 * 2, dtype=float).reshape(5, 3, 2)
    x = sparse.COO.from_numpy(dense)
    mask = sparse.COO([np.array([1])], [False], shape=(5,), fill_value=True)
    cat = sparse.COO([np.array([1, 3])], [1, 1], shape=(4,), fill_value=0)

    result = x[mask, ::-1, cat]
    expected = np.stack([dense[0, ::-1, 0], dense[2, ::-1, 1], dense[3, ::-1, 0], dense[4, ::-1, 1]])
    assert_eq(result, expected)


@pytest.mark.parametrize("format", ["coo", "gcxs"])
@pytest.mark.parametrize("mask_fill", [False, True])
def test_mixed_contiguous_advanced(format, mask_fill):
    dense = np.arange(4 * 5 * 3, dtype=float).reshape(4, 5, 3)
    x = sparse.COO.from_numpy(dense).asformat(format)
    dense_mask = np.array([True, False, True, True, False])
    dense_cat = np.array([2, 0, 1])
    mask = _sparse_index(dense_mask, mask_fill, "coo")
    cat = _sparse_index(dense_cat, 0, "coo")

    assert_eq(x[:, mask, cat], dense[:, dense_mask, dense_cat])


@pytest.mark.parametrize("format", ["coo", "gcxs"])
def test_mixed_newaxis_and_ellipsis(format):
    dense = np.arange(5 * 4 * 3, dtype=float).reshape(5, 4, 3)
    x = sparse.COO.from_numpy(dense).asformat(format)
    dense_mask = np.array([True, False, True, True, False])
    dense_cat = np.array([2, 0, 1])
    mask = _sparse_index(dense_mask, True, "coo")
    cat = _sparse_index(dense_cat, 0, "coo")

    assert_eq(x[mask, None, ::-1, cat], dense[dense_mask, None, ::-1, dense_cat])
    assert_eq(x[mask, ..., cat], dense[dense_mask, ..., dense_cat])


@pytest.mark.parametrize("format", ["coo", "gcxs"])
def test_mixed_integers_and_steps(format):
    dense = np.arange(2 * 6 * 5 * 3, dtype=float).reshape(2, 6, 5, 3)
    x = sparse.COO.from_numpy(dense).asformat(format)
    dense_mask = np.array([True, False, True, True, False, True])
    dense_cat = np.array([2, 0, 1, 2])
    mask = _sparse_index(dense_mask, True, "coo")
    cat = _sparse_index(dense_cat, 0, "coo")

    assert_eq(x[1, mask, ::-2, cat], dense[1, dense_mask, ::-2, dense_cat])
    assert_eq(x[-1, mask, 3, cat], dense[-1, dense_mask, 3, dense_cat])


@pytest.mark.parametrize("format", ["coo", "gcxs"])
def test_mixed_multidim_mask(format):
    dense = np.arange(2 * 3 * 4 * 2, dtype=float).reshape(2, 3, 4, 2)
    x = sparse.COO.from_numpy(dense).asformat(format)
    dense_mask = np.array([[True, False, True], [False, True, True]])
    dense_cat = np.array([0, 1, 0, 1])
    mask = _sparse_index(dense_mask, True, "coo")
    cat = _sparse_index(dense_cat, 0, "coo")

    assert_eq(x[mask, ::-1, cat], dense[dense_mask, ::-1, dense_cat])


@pytest.mark.parametrize("format", ["coo", "gcxs"])
def test_mixed_dense_index_combinations(format):
    dense = np.arange(5 * 4 * 3, dtype=float).reshape(5, 4, 3)
    x = sparse.COO.from_numpy(dense).asformat(format)
    dense_mask = np.array([True, False, True, True, False])
    dense_cat = np.array([2, 0, 1])
    mask = _sparse_index(dense_mask, True, "coo")
    cat = _sparse_index(dense_cat, 0, "coo")

    # Sparse mask with a dense category array.
    assert_eq(x[mask, ::-1, dense_cat], dense[dense_mask, ::-1, dense_cat])
    # Sparse mask with a size-one dense category array.
    assert_eq(x[mask, ::-1, np.array([2])], dense[dense_mask, ::-1, np.array([2])])
    # Dense boolean mask with a sparse category array.
    assert_eq(x[dense_mask, ::-1, cat], dense[dense_mask, ::-1, dense_cat])
    # Sparse mask alone with basic indices.
    assert_eq(x[mask, ::-1], dense[dense_mask, ::-1])


def test_mixed_dok_array():
    dense = np.arange(5 * 4 * 3, dtype=float).reshape(5, 4, 3)
    x = sparse.COO.from_numpy(dense).asformat("dok")
    dense_mask = np.array([True, False, True, True, False])
    dense_cat = np.array([2, 0, 1])
    mask = _sparse_index(dense_mask, True, "coo")
    cat = _sparse_index(dense_cat, 0, "coo")

    result = x[mask, ::-1, cat]
    assert isinstance(result, sparse.DOK)
    assert_eq(result, dense[dense_mask, ::-1, dense_cat])


@pytest.mark.parametrize("mask_fill", [False, True])
def test_mixed_empty_selection(mask_fill):
    dense = np.arange(5 * 4 * 3, dtype=float).reshape(5, 4, 3)
    x = sparse.COO.from_numpy(dense)
    dense_mask = np.zeros(5, dtype=bool)
    dense_cat = np.array([], dtype=np.intp)
    mask = _sparse_index(dense_mask, mask_fill, "coo")
    cat = _sparse_index(dense_cat, 0, "coo")

    assert_eq(x[mask, ::-1, cat], dense[dense_mask, ::-1, dense_cat])


@pytest.mark.parametrize("format", ["coo", "gcxs"])
def test_mixed_large_shape_no_densify(format, monkeypatch):
    # The store and category axes are huge, but only a few exceptions are
    # registered; the selection must not expand either index.
    n_stores = 10**12
    x = sparse.COO(
        [
            [2, n_stores - 2, n_stores - 2, n_stores - 1, n_stores - 1],
            [0, 0, 0, 2, 1],
            [0, 0, 1, 1, 0],
        ],
        [3.0, 11.0, 13.0, 5.0, 7.0],
        shape=(n_stores, 3, 2),
    )
    mask = sparse.COO([np.array([1, 2])], [False, False], shape=(n_stores,), fill_value=True)
    cat = sparse.COO([np.array([n_stores - 4])], [1], shape=(n_stores - 2,), fill_value=0)
    if format == "gcxs":
        x = x.asformat("gcxs", compressed_axes=(1,))

    def no_dense(*args, **kwargs):
        pytest.fail("Mixed sparse indexing must not densify its operands")

    with monkeypatch.context() as patch:
        patch.setattr(sparse.COO, "todense", no_dense)
        patch.setattr(sparse.GCXS, "todense", no_dense)
        result = x[mask, ::-1, cat].asformat("coo")

    # Store 2 is excluded. Store n_stores - 2 has selected rank n_stores - 4,
    # which is the registered category exception (1). Every other kept store
    # selects category 0.
    assert result.shape == (n_stores - 2, 3)
    np.testing.assert_array_equal(result.coords, [[n_stores - 4, n_stores - 3], [2, 1]])
    np.testing.assert_array_equal(result.data, [13.0, 7.0])


def test_mixed_category_length_mismatch():
    x = sparse.COO.from_numpy(np.arange(60, dtype=float).reshape(5, 4, 3))
    mask = _sparse_index([True, False, True, True, False], True, "coo")
    cat = _sparse_index([0, 1], 0, "coo")
    with pytest.raises(IndexError, match="shape mismatch"):
        x[mask, ::-1, cat]


def test_mixed_mask_shape_mismatch():
    x = sparse.COO.from_numpy(np.arange(60, dtype=float).reshape(5, 4, 3))
    mask = _sparse_index([True, False, True], True, "coo")
    cat = _sparse_index([0, 1, 0], 0, "coo")
    with pytest.raises(IndexError, match="boolean index"):
        x[mask, ::-1, cat]


def test_mixed_sparse_integer_index_without_mask():
    x = sparse.COO.from_numpy(np.arange(60, dtype=float).reshape(5, 4, 3))
    cat = _sparse_index([0, 1, 0], 0, "coo")
    with pytest.raises(NotImplementedError, match="boolean mask"):
        x[:, ::-1, cat]


def test_mixed_multiple_masks_unsupported():
    x = sparse.COO.from_numpy(np.arange(60, dtype=float).reshape(5, 4, 3))
    mask = _sparse_index([True, False, True, True, False], True, "coo")
    other = _sparse_index([True, True, False], False, "coo")
    with pytest.raises(NotImplementedError, match="Only one sparse boolean mask"):
        x[mask, ::-1, other]


def test_mixed_category_out_of_bounds():
    x = sparse.COO.from_numpy(np.arange(60, dtype=float).reshape(5, 4, 3))
    mask = _sparse_index([True, False, True, True, False], True, "coo")
    cat = sparse.COO([np.array([0])], [5], shape=(3,), fill_value=0)
    with pytest.raises(IndexError, match="out of bounds"):
        x[mask, ::-1, cat]


def test_mixed_float_sparse_index():
    x = sparse.COO.from_numpy(np.arange(60, dtype=float).reshape(5, 4, 3))
    mask = _sparse_index([True, False, True, True, False], True, "coo")
    cat = sparse.COO.from_numpy(np.array([0.5, 1.0, 0.0]))
    with pytest.raises(IndexError, match="valid indices"):
        x[mask, ::-1, cat]
