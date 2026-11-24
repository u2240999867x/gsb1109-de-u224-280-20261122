import builtins
import warnings
from collections.abc import Iterable
from functools import reduce, wraps
from itertools import chain
from operator import index, mul

import numba
from numba import literal_unroll

import numpy as np

from ._coo import COO, as_coo, expand_dims
from ._sparse_array import SparseArray
from ._utils import (
    _zero_of_dtype,
    check_zero_fill_value,
    equivalent,
    normalize_axis,
)

_EINSUM_SYMBOLS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
_EINSUM_SYMBOLS_SET = set(_EINSUM_SYMBOLS)


def _is_scipy_sparse_obj(x):
    """
    Tests if the supplied argument is a SciPy sparse object.
    """
    return bool(hasattr(x, "__module__") and x.__module__.startswith("scipy.sparse"))


def _check_device(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        device = kwargs.get("device")
        if device not in {"cpu", None}:
            raise ValueError("Device must be `'cpu'` or `None`.")
        return func(*args, **kwargs)

    return wrapped


def _is_sparse(x):
    """
    Tests if the supplied argument is a SciPy sparse object, or one from this library.
    """
    return isinstance(x, SparseArray) or _is_scipy_sparse_obj(x)


@numba.njit
def nan_check(*args):
    """
    Check for the NaN values in Numpy Arrays

    Parameters
    ----------
    Union[Numpy Array, Integer, Float]

    Returns
    -------
    Boolean Whether Numpy Array Contains NaN

    """
    for i in literal_unroll(args):
        ia = np.asarray(i)
        if ia.size != 0 and np.isnan(np.min(ia)):
            return True
    return False


def check_class_nan(test):
    """
    Check NaN for Sparse Arrays

    Parameters
    ----------
    test : Union[sparse.COO, sparse.GCXS, scipy.sparse.spmatrix, Numpy Ndarrays]

    Returns
    -------
    Boolean Whether Sparse Array Contains NaN

    """
    from ._compressed import GCXS
    from ._coo import COO

    if isinstance(test, GCXS | COO):
        return nan_check(test.fill_value, test.data)
    if _is_scipy_sparse_obj(test):
        return nan_check(test.data)
    return nan_check(test)


def tensordot(a, b, axes=2, *, return_type=None):
    """
    Perform the equivalent of [`numpy.tensordot`][].

    Parameters
    ----------
    a, b : Union[SparseArray, np.ndarray, scipy.sparse.spmatrix]
        The arrays to perform the `tensordot` operation on.
    axes : tuple[Union[int, tuple[int], Union[int, tuple[int]], optional
        The axes to match when performing the sum.
    return_type : {None, COO, np.ndarray}, optional
        Type of returned array.

    Returns
    -------
    Union[SparseArray, numpy.ndarray]
        The result of the operation.

    Raises
    ------
    ValueError
        If all arguments don't have zero fill-values.

    See Also
    --------
    - [`numpy.tensordot`][] : NumPy equivalent function
    """
    from ._compressed import GCXS

    # Much of this is stolen from numpy/core/numeric.py::tensordot
    # Please see license at https://github.com/numpy/numpy/blob/main/LICENSE.txt
    check_zero_fill_value(a, b)

    if _is_scipy_sparse_obj(a):
        a = GCXS.from_scipy_sparse(a)
    if _is_scipy_sparse_obj(b):
        b = GCXS.from_scipy_sparse(b)

    try:
        iter(axes)
    except TypeError:
        axes_a = list(range(-axes, 0))
        axes_b = list(range(axes))
    else:
        axes_a, axes_b = axes
    try:
        na = len(axes_a)
        axes_a = list(axes_a)
    except TypeError:
        axes_a = [axes_a]
        na = 1
    try:
        nb = len(axes_b)
        axes_b = list(axes_b)
    except TypeError:
        axes_b = [axes_b]
        nb = 1

    # a, b = asarray(a), asarray(b)  # <--- modified
    as_ = a.shape
    nda = a.ndim
    bs = b.shape
    ndb = b.ndim
    equal = True
    if nda == 0 or ndb == 0:
        if axes_a == [] and axes_b == []:
            if nda == 0 and isinstance(a, SparseArray):
                a = a.todense()
            if ndb == 0 and isinstance(b, SparseArray):
                b = b.todense()
            return a * b
        pos = int(nda != 0)
        raise ValueError(f"Input {pos} operand does not have enough dimensions")
    if na != nb:
        equal = False
    else:
        for k in range(na):
            if as_[axes_a[k]] != bs[axes_b[k]]:
                equal = False
                break
            if axes_a[k] < 0:
                axes_a[k] += nda
            if axes_b[k] < 0:
                axes_b[k] += ndb
    if not equal:
        raise ValueError("shape-mismatch for sum")

    # Move the axes to sum over to the end of "a"
    # and to the front of "b"
    notin = [k for k in range(nda) if k not in axes_a]
    newaxes_a = notin + axes_a
    N2 = 1
    for axis in axes_a:
        N2 *= as_[axis]
    newshape_a = (-1, N2)
    olda = [as_[axis] for axis in notin]

    notin = [k for k in range(ndb) if k not in axes_b]
    newaxes_b = axes_b + notin
    N2 = 1
    for axis in axes_b:
        N2 *= bs[axis]
    newshape_b = (N2, -1)
    oldb = [bs[axis] for axis in notin]

    if builtins.any(dim == 0 for dim in chain(newshape_a, newshape_b)):
        from sparse import COO

        dt = np.result_type(a.dtype, b.dtype)
        res = COO(
            np.empty((len(olda) + len(oldb), 0), dtype=np.uintp), data=np.empty(0, dtype=dt), shape=tuple(olda + oldb)
        )
        if isinstance(a, np.ndarray) or isinstance(b, np.ndarray):
            res = res.todense()

        return res

    at = a.transpose(newaxes_a).reshape(newshape_a)
    bt = b.transpose(newaxes_b).reshape(newshape_b)
    res = _dot(at, bt, return_type)
    return res.reshape(olda + oldb)


def matmul(a, b):
    """Perform the equivalent of [`numpy.matmul`][] on two arrays.

    Parameters
    ----------
    a, b : Union[SparseArray, np.ndarray, scipy.sparse.spmatrix]
        The arrays to perform the `matmul` operation on.

    Returns
    -------
    Union[SparseArray, numpy.ndarray]
        The result of the operation.

    Raises
    ------
    ValueError
        If all arguments don't have zero fill-values, or the shape of the two arrays is not broadcastable.

    See Also
    --------
    - [`numpy.matmul`][] : NumPy equivalent function.
    - `COO.__matmul__`: Equivalent function for COO objects.
    """
    check_zero_fill_value(a, b)
    if not hasattr(a, "ndim") or not hasattr(b, "ndim"):
        raise TypeError(f"Cannot perform dot product on types {type(a)}, {type(b)}")

    if check_class_nan(a) or check_class_nan(b):
        warnings.warn("Nan will not be propagated in matrix multiplication", RuntimeWarning, stacklevel=1)

    # When b is 2-d, it is equivalent to dot
    if b.ndim <= 2:
        return dot(a, b)

    # when a is 2-d, we need to transpose result after dot
    if a.ndim <= 2:
        res = dot(a, b)
        axes = list(range(res.ndim))
        axes.insert(-1, axes.pop(0))
        return res.transpose(axes)

    # If a can be squeeze to a vector, use dot will be faster
    if a.ndim <= b.ndim and np.prod(a.shape[:-1]) == 1:
        res = dot(a.reshape(-1), b)
        shape = list(res.shape)
        shape.insert(-1, 1)
        return res.reshape(shape)

    # If b can be squeeze to a matrix, use dot will be faster
    if b.ndim <= a.ndim and np.prod(b.shape[:-2]) == 1:
        return dot(a, b.reshape(b.shape[-2:]))

    if a.ndim < b.ndim:
        a = a[(None,) * (b.ndim - a.ndim)]
    if a.ndim > b.ndim:
        b = b[(None,) * (a.ndim - b.ndim)]
    for i, j in zip(a.shape[:-2], b.shape[:-2], strict=True):
        if i != 1 and j != 1 and i != j:
            raise ValueError("shapes of a and b are not broadcastable")

    def _matmul_recurser(a, b):
        if a.ndim == 2:
            return dot(a, b)
        res = []
        for i in range(builtins.max(a.shape[0], b.shape[0])):
            a_i = a[0] if a.shape[0] == 1 else a[i]
            b_i = b[0] if b.shape[0] == 1 else b[i]
            res.append(_matmul_recurser(a_i, b_i))
        mask = [isinstance(x, SparseArray) for x in res]
        if builtins.all(mask):
            return stack(res)

        res = [x.todense() if isinstance(x, SparseArray) else x for x in res]
        return np.stack(res)

    return _matmul_recurser(a, b)


def dot(a, b):
    """
    Perform the equivalent of [`numpy.dot`][] on two arrays.

    Parameters
    ----------
    a, b : Union[SparseArray, np.ndarray, scipy.sparse.spmatrix]
        The arrays to perform the `dot` operation on.

    Returns
    -------
    Union[SparseArray, numpy.ndarray]
        The result of the operation.

    Raises
    ------
    ValueError
        If all arguments don't have zero fill-values.

    See Also
    --------
    - [`numpy.dot`][] : NumPy equivalent function.
    - [`sparse.COO.dot`][] : Equivalent function for COO objects.
    """
    check_zero_fill_value(a, b)
    if not hasattr(a, "ndim") or not hasattr(b, "ndim"):
        raise TypeError(f"Cannot perform dot product on types {type(a)}, {type(b)}")

    if a.ndim == 1 and b.ndim == 1:
        if isinstance(a, SparseArray):
            a = as_coo(a)
        if isinstance(b, SparseArray):
            b = as_coo(b)
        # Sum in the product dtype like NumPy, e.g. ``bool`` stays ``bool``.
        return (a * b).sum(dtype=_dot_dtype(a.dtype, b.dtype))

    a_axis = -1
    b_axis = -2

    if b.ndim == 1:
        b_axis = -1
    return tensordot(a, b, axes=(a_axis, b_axis))


def _dot(a, b, return_type=None):
    from ._compressed import GCXS
    from ._coo import COO
    from ._sparse_array import SparseArray

    out_shape = (a.shape[0], b.shape[1])
    if builtins.all(isinstance(arr, SparseArray) for arr in [a, b]) and builtins.any(
        isinstance(arr, GCXS) for arr in [a, b]
    ):
        a = a.asformat("gcxs")
        b = b.asformat("gcxs", compressed_axes=a.compressed_axes)

    if isinstance(a, GCXS) and isinstance(b, GCXS):
        if a.nbytes > b.nbytes:
            b = b.change_compressed_axes(a.compressed_axes)
        else:
            a = a.change_compressed_axes(b.compressed_axes)

        if a.compressed_axes == (0,):  # csr @ csr
            compressed_axes = (0,)
            data, indices, indptr = _dot_csr_csr_type(a.dtype, b.dtype)(
                out_shape, a.data, b.data, a.indices, b.indices, a.indptr, b.indptr
            )
        elif a.compressed_axes == (1,):  # csc @ csc
            # a @ b = (b.T @ a.T).T
            compressed_axes = (1,)
            data, indices, indptr = _dot_csr_csr_type(b.dtype, a.dtype)(
                out_shape[::-1],
                b.data,
                a.data,
                b.indices,
                a.indices,
                b.indptr,
                a.indptr,
            )
        out = GCXS(
            (data, indices, indptr),
            shape=out_shape,
            compressed_axes=compressed_axes,
            prune=True,
        )
        if return_type == np.ndarray:
            return out.todense()
        if return_type == COO:
            return out.tocoo()
        return out

    if isinstance(a, GCXS) and isinstance(b, np.ndarray):
        if a.compressed_axes == (0,):  # csr @ ndarray
            if return_type is None or return_type == np.ndarray:
                return _dot_csr_ndarray_type(a.dtype, b.dtype)(out_shape, a.data, a.indices, a.indptr, b)
            data, indices, indptr = _dot_csr_ndarray_type_sparse(a.dtype, b.dtype)(
                out_shape, a.data, a.indices, a.indptr, b
            )
            out = GCXS(
                (data, indices, indptr),
                shape=out_shape,
                compressed_axes=(0,),
                prune=True,
            )
            if return_type == COO:
                return out.tocoo()
            return out
        if return_type is None or return_type == np.ndarray:  # csc @ ndarray
            return _dot_csc_ndarray_type(a.dtype, b.dtype)(a.shape, b.shape, a.data, a.indices, a.indptr, b)
        data, indices, indptr = _dot_csc_ndarray_type_sparse(a.dtype, b.dtype)(
            a.shape, b.shape, a.data, a.indices, a.indptr, b
        )
        compressed_axes = (1,)
        out = GCXS(
            (data, indices, indptr),
            shape=out_shape,
            compressed_axes=compressed_axes,
            prune=True,
        )
        if return_type == COO:
            return out.tocoo()
        return out

    if isinstance(a, np.ndarray) and isinstance(b, GCXS):
        at = a.view(type=np.ndarray).T
        bt = b.T  # constant-time transpose
        if b.compressed_axes == (0,):
            if return_type is None or return_type == np.ndarray:
                out = _dot_csc_ndarray_type(bt.dtype, at.dtype)(bt.shape, at.shape, bt.data, bt.indices, bt.indptr, at)
                return out.T
            data, indices, indptr = _dot_csc_ndarray_type_sparse(bt.dtype, at.dtype)(
                bt.shape, at.shape, bt.data, b.indices, b.indptr, at
            )
            out = GCXS(
                (data, indices, indptr),
                shape=out_shape,
                compressed_axes=(0,),
                prune=True,
            )
            if return_type == COO:
                return out.tocoo()
            return out

        # compressed_axes == (1,)
        if return_type is None or return_type == np.ndarray:
            out = _dot_csr_ndarray_type(bt.dtype, at.dtype)(out_shape[::-1], bt.data, bt.indices, bt.indptr, at)
            return out.T
        data, indices, indptr = _dot_csr_ndarray_type_sparse(bt.dtype, at.dtype)(
            out_shape[::-1], bt.data, bt.indices, bt.indptr, at
        )
        out = GCXS((data, indices, indptr), shape=out_shape, compressed_axes=(1,), prune=True)
        if return_type == COO:
            return out.tocoo()
        return out

    if isinstance(a, COO) and isinstance(b, COO):
        # convert to csr
        a_indptr = np.empty(a.shape[0] + 1, dtype=np.intp)
        a_indptr[0] = 0
        np.cumsum(np.bincount(a.coords[0], minlength=a.shape[0]), out=a_indptr[1:])

        b_indptr = np.empty(b.shape[0] + 1, dtype=np.intp)
        b_indptr[0] = 0
        np.cumsum(np.bincount(b.coords[0], minlength=b.shape[0]), out=b_indptr[1:])
        coords, data = _dot_coo_coo_type(a.dtype, b.dtype)(
            out_shape, a.coords, b.coords, a.data, b.data, a_indptr, b_indptr
        )
        out = COO(
            coords,
            data,
            shape=out_shape,
            has_duplicates=False,
            sorted=False,
            prune=True,
        )

        if return_type == np.ndarray:
            return out.todense()
        if return_type == GCXS:
            return out.asformat("gcxs")
        return out

    if isinstance(a, COO) and isinstance(b, np.ndarray):
        b = b.view(type=np.ndarray).T

        if return_type is None or return_type == np.ndarray:
            return _dot_coo_ndarray_type(a.dtype, b.dtype)(a.coords, a.data, b, out_shape)
        coords, data = _dot_coo_ndarray_type_sparse(a.dtype, b.dtype)(a.coords, a.data, b, out_shape)
        out = COO(coords, data, shape=out_shape, has_duplicates=False, sorted=True)
        if return_type == GCXS:
            return out.asformat("gcxs")
        return out

    if isinstance(a, np.ndarray) and isinstance(b, COO):
        a = a.view(type=np.ndarray)

        if return_type is None or return_type == np.ndarray:
            return _dot_ndarray_coo_type(a.dtype, b.dtype)(a, b.coords, b.data, out_shape)
        b = b.T
        coords, data = _dot_ndarray_coo_type_sparse(a.dtype, b.dtype)(a, b.coords, b.data, out_shape)
        out = COO(coords, data, shape=out_shape, has_duplicates=False, sorted=True, prune=True)
        if return_type == GCXS:
            return out.asformat("gcxs")
        return out

    if isinstance(a, np.ndarray) and isinstance(b, np.ndarray):
        return np.dot(a, b)

    raise TypeError("Unsupported types.")


def _memoize_dtype(f):
    """
    Memoizes a function taking in NumPy dtypes.

    Parameters
    ----------
    f : Callable

    Returns
    -------
    wrapped : Callable

    Examples
    --------
    >>> def func(dt1):
    ...     return object()
    >>> func = _memoize_dtype(func)
    >>> func(np.dtype("i8")) is func(np.dtype("int64"))
    True
    >>> func(np.dtype("i8")) is func(np.dtype("i4"))
    False
    """
    cache = {}

    @wraps(f)
    def wrapped(*args):
        key = tuple(arg.name for arg in args)
        if key in cache:
            return cache[key]

        result = f(*args)
        cache[key] = result
        return result

    return wrapped


@numba.jit(nopython=True, nogil=True)
def _csr_csr_count_nnz(out_shape, a_indices, b_indices, a_indptr, b_indptr):  # pragma: no cover
    """
    A function for computing the number of nonzero values in the resulting
    array from multiplying an array with compressed rows with an array
    with compressed rows: (a @ b).nnz.

    Parameters
    ----------
    out_shape : tuple
        The shape of the output array.
    a_indices, a_indptr : np.ndarray
        The indices and index pointer array of ``a``.
    b_data, b_indices, b_indptr : np.ndarray
        The indices and index pointer array of ``b``.
    """
    n_row, n_col = out_shape
    nnz = 0
    mask = np.full(n_col, -1)
    for i in range(n_row):
        row_nnz = 0
        for j in a_indices[a_indptr[i] : a_indptr[i + 1]]:
            for k in b_indices[b_indptr[j] : b_indptr[j + 1]]:
                if mask[k] != i:
                    mask[k] = i
                    row_nnz += 1
        nnz += row_nnz
    return nnz


@numba.jit(nopython=True, nogil=True)
def _csr_ndarray_count_nnz(out_shape, indptr, a_indices, a_indptr, b):  # pragma: no cover
    """
    A function for computing the number of nonzero values in the resulting
    array from multiplying an array with compressed rows with a dense
    numpy array: (a @ b).nnz.

    Parameters
    ----------
    out_shape : tuple
        The shape of the output array.
    indptr : ndarray
        The empty index pointer array for the output.
    a_indices, a_indptr : np.ndarray
        The indices and index pointer array of ``a``.
    b : np.ndarray
        The second input array ``b``.
    """
    nnz = 0
    for i in range(out_shape[0]):
        cur_row = a_indices[a_indptr[i] : a_indptr[i + 1]]
        for j in range(out_shape[1]):
            for k in cur_row:
                if b[k, j] != 0:
                    nnz += 1
                    break
        indptr[i + 1] = nnz
    return nnz


@numba.jit(nopython=True, nogil=True)
def _csc_ndarray_count_nnz(a_shape, b_shape, indptr, a_indices, a_indptr, b):  # pragma: no cover
    """
    A function for computing the number of nonzero values in the resulting
    array from multiplying an array with compressed columns with a dense
    numpy array: (a @ b).nnz.

    Parameters
    ----------
    a_shape, b_shape : tuple
        The shapes of the input arrays.
    indptr : ndarray
        The empty index pointer array for the output.
    a_indices, a_indptr : np.ndarray
        The indices and index pointer array of ``a``.
    b : np.ndarray
        The second input array ``b``.
    """
    nnz = 0
    mask = np.full(a_shape[0], -1)
    for i in range(b_shape[1]):
        col_nnz = 0
        for j in range(b_shape[0]):
            for k in a_indices[a_indptr[j] : a_indptr[j + 1]]:
                if b[j, i] != 0 and mask[k] != i:
                    mask[k] = i
                    col_nnz += 1
        nnz += col_nnz
        indptr[i + 1] = nnz
    return nnz


def _dot_dtype(dt1, dt2):
    return (np.zeros((), dtype=dt1) * np.zeros((), dtype=dt2)).dtype


@_memoize_dtype
def _dot_csr_csr_type(dt1, dt2):
    dtr = _dot_dtype(dt1, dt2)

    @numba.jit(
        nopython=True,
        nogil=True,
        locals={"data_curr": numba.np.numpy_support.from_dtype(dtr)},
    )
    def _dot_csr_csr(out_shape, a_data, b_data, a_indices, b_indices, a_indptr, b_indptr):  # pragma: no cover
        """
        Utility function taking in two ``GCXS`` objects and calculating
        their dot product: a @ b for a and b with compressed rows.

        Parameters
        ----------
        out_shape : tuple
            The shape of the output array.
        a_data, a_indices, a_indptr : np.ndarray
            The data, indices, and index pointer arrays of ``a``.
        b_data, b_indices, b_indptr : np.ndarray
            The data, indices, and index pointer arrays of ``b``.
        """

        # much of this is borrowed from:
        # https://github.com/scipy/scipy/blob/main/scipy/sparse/sparsetools/csr.h

        # calculate nnz before multiplying so we can use static arrays
        nnz = _csr_csr_count_nnz(out_shape, a_indices, b_indices, a_indptr, b_indptr)
        n_row, n_col = out_shape
        indptr = np.empty(n_row + 1, dtype=np.intp)
        indptr[0] = 0
        indices = np.empty(nnz, dtype=np.intp)
        data = np.empty(nnz, dtype=dtr)
        next_ = np.full(n_col, -1)
        sums = np.zeros(n_col, dtype=dtr)
        nnz = 0

        for i in range(n_row):
            head = -2
            length = 0
            next_[:] = -1
            for j, av in zip(  # noqa: B905
                a_indices[a_indptr[i] : a_indptr[i + 1]],
                a_data[a_indptr[i] : a_indptr[i + 1]],
            ):
                for k, bv in zip(  # noqa: B905
                    b_indices[b_indptr[j] : b_indptr[j + 1]],
                    b_data[b_indptr[j] : b_indptr[j + 1]],
                ):
                    sums[k] += av * bv
                    if next_[k] == -1:
                        next_[k] = head
                        head = k
                        length += 1

            for _ in range(length):
                if next_[head] != -1:
                    indices[nnz] = head
                    data[nnz] = sums[head]
                    nnz += 1

                temp = head
                head = next_[head]

                next_[temp] = -1
                sums[temp] = 0

            indptr[i + 1] = nnz

        if n_col > 0 and len(indices) == (n_col * n_row):
            for i in range(len(indices) // n_col):
                j = n_col * i
                k = n_col * (1 + i)
                data[j:k] = data[j:k][::-1]
                indices[j:k] = indices[j:k][::-1]
        return data, indices, indptr

    return _dot_csr_csr


@_memoize_dtype
def _dot_csr_ndarray_type(dt1, dt2):
    dtr = _dot_dtype(dt1, dt2)

    @numba.jit(
        nopython=True,
        nogil=True,
        locals={"data_curr": numba.np.numpy_support.from_dtype(dtr)},
    )
    def _dot_csr_ndarray(out_shape, a_data, a_indices, a_indptr, b):  # pragma: no cover
        """
        Utility function taking in one `GCXS` and one ``ndarray`` and
        calculating their dot product: a @ b for a with compressed rows.
        Returns a dense result.

        Parameters
        ----------
        a_data, a_indices, a_indptr : np.ndarray
            The data, indices, and index pointers of ``a``.
        b : np.ndarray
            The second input array ``b``.
        out_shape : Tuple[int]
            The shape of the output array.
        """
        b = np.ascontiguousarray(b)  # ensure memory aligned
        out = np.zeros(out_shape, dtype=dtr)
        for i in range(out_shape[0]):
            val = out[i]
            for k in range(a_indptr[i], a_indptr[i + 1]):
                ind = a_indices[k]
                v = a_data[k]
                for j in range(out_shape[1]):
                    val[j] += v * b[ind, j]
        return out

    return _dot_csr_ndarray


@_memoize_dtype
def _dot_csr_ndarray_type_sparse(dt1, dt2):
    dtr = _dot_dtype(dt1, dt2)

    @numba.jit(
        nopython=True,
        nogil=True,
        locals={"data_curr": numba.np.numpy_support.from_dtype(dtr)},
    )
    def _dot_csr_ndarray_sparse(out_shape, a_data, a_indices, a_indptr, b):  # pragma: no cover
        """
        Utility function taking in one `GCXS` and one ``ndarray`` and
        calculating their dot product: a @ b for a with compressed rows.
        Returns a sparse result.

        Parameters
        ----------
        a_data, a_indices, a_indptr : np.ndarray
            The data, indices, and index pointers of ``a``.
        b : np.ndarray
            The second input array ``b``.
        out_shape : Tuple[int]
            The shape of the output array.
        """
        indptr = np.empty(out_shape[0] + 1, dtype=np.intp)
        indptr[0] = 0
        nnz = _csr_ndarray_count_nnz(out_shape, indptr, a_indices, a_indptr, b)
        indices = np.empty(nnz, dtype=np.intp)
        data = np.empty(nnz, dtype=dtr)
        current = 0
        for i in range(out_shape[0]):
            for j in range(out_shape[1]):
                val = 0
                nonzero = False
                for k in range(a_indptr[i], a_indptr[i + 1]):
                    ind = a_indices[k]
                    v = a_data[k]
                    val += v * b[ind, j]
                    if b[ind, j] != 0:
                        nonzero = True
                if nonzero:
                    data[current] = val
                    indices[current] = j
                    current += 1
        return data, indices, indptr

    return _dot_csr_ndarray_sparse


@_memoize_dtype
def _dot_csc_ndarray_type_sparse(dt1, dt2):
    dtr = _dot_dtype(dt1, dt2)

    @numba.jit(
        nopython=True,
        nogil=True,
        locals={"data_curr": numba.np.numpy_support.from_dtype(dtr)},
    )
    def _dot_csc_ndarray_sparse(a_shape, b_shape, a_data, a_indices, a_indptr, b):  # pragma: no cover
        """
        Utility function taking in one `GCXS` and one ``ndarray`` and
        calculating their dot product: a @ b for a with compressed columns.
        Returns a sparse result.

        Parameters
        ----------
        a_data, a_indices, a_indptr : np.ndarray
            The data, indices, and index pointers of ``a``.
        b : np.ndarray
            The second input array ``b``.
        a_shape, b_shape : Tuple[int]
            The shapes of the input arrays.
        """
        indptr = np.empty(b_shape[1] + 1, dtype=np.intp)
        nnz = _csc_ndarray_count_nnz(a_shape, b_shape, indptr, a_indices, a_indptr, b)
        indices = np.empty(nnz, dtype=np.intp)
        data = np.empty(nnz, dtype=dtr)
        sums = np.zeros(a_shape[0])
        mask = np.full(a_shape[0], -1)
        nnz = 0
        indptr[0] = 0
        for i in range(b_shape[1]):
            head = -2
            length = 0
            for j in range(b_shape[0]):
                u = b[j, i]
                if u != 0:
                    for k in range(a_indptr[j], a_indptr[j + 1]):
                        ind = a_indices[k]
                        v = a_data[k]
                        sums[ind] += u * v
                        if mask[ind] == -1:
                            mask[ind] = head
                            head = ind
                            length += 1
            for _ in range(length):
                if sums[head] != 0:
                    indices[nnz] = head
                    data[nnz] = sums[head]
                    nnz += 1

                temp = head
                head = mask[head]

                mask[temp] = -1
                sums[temp] = 0
        return data, indices, indptr

    return _dot_csc_ndarray_sparse


@_memoize_dtype
def _dot_csc_ndarray_type(dt1, dt2):
    dtr = _dot_dtype(dt1, dt2)

    @numba.jit(
        nopython=True,
        nogil=True,
        locals={"data_curr": numba.np.numpy_support.from_dtype(dtr)},
    )
    def _dot_csc_ndarray(a_shape, b_shape, a_data, a_indices, a_indptr, b):  # pragma: no cover
        """
        Utility function taking in one `GCXS` and one ``ndarray`` and
        calculating their dot product: a @ b for a with compressed columns.
        Returns a dense result.

        Parameters
        ----------
        a_data, a_indices, a_indptr : np.ndarray
            The data, indices, and index pointers of ``a``.
        b : np.ndarray
            The second input array ``b``.
        a_shape, b_shape : Tuple[int]
            The shapes of the input arrays.
        """
        b = np.ascontiguousarray(b)  # ensure memory aligned
        out = np.zeros((a_shape[0], b_shape[1]), dtype=dtr)
        for i in range(b_shape[0]):
            for k in range(a_indptr[i], a_indptr[i + 1]):
                ind = a_indices[k]
                v = a_data[k]
                val = out[ind]
                for j in range(b_shape[1]):
                    val[j] += v * b[i, j]
        return out

    return _dot_csc_ndarray


@_memoize_dtype
def _dot_coo_coo_type(dt1, dt2):
    dtr = _dot_dtype(dt1, dt2)

    @numba.jit(
        nopython=True,
        nogil=True,
        locals={"data_curr": numba.np.numpy_support.from_dtype(dtr)},
    )
    def _dot_coo_coo(out_shape, a_coords, b_coords, a_data, b_data, a_indptr, b_indptr):  # pragma: no cover
        """
        Utility function taking in two ``COO`` objects and calculating
        their dot product: a @ b.

        Parameters
        ----------
        a_shape, b_shape : tuple
            The shapes of the input arrays.
        a_data, a_coords : np.ndarray
            The data and coordinates of ``a``.
        b_data, b_coords : np.ndarray
            The data and coordinates of ``b``.
        """

        # much of this is borrowed from:
        # https://github.com/scipy/scipy/blob/main/scipy/sparse/sparsetools/csr.h

        n_row, n_col = out_shape
        # calculate nnz before multiplying so we can use static arrays
        nnz = _csr_csr_count_nnz(out_shape, a_coords[1], b_coords[1], a_indptr, b_indptr)
        coords = np.empty((2, nnz), dtype=np.intp)
        data = np.empty(nnz, dtype=dtr)
        next_ = np.full(n_col, -1)
        sums = np.zeros(n_col, dtype=dtr)
        nnz = 0

        for i in range(n_row):
            head = -2
            length = 0
            next_[:] = -1
            for j, av in zip(  # noqa: B905
                a_coords[1, a_indptr[i] : a_indptr[i + 1]],
                a_data[a_indptr[i] : a_indptr[i + 1]],
            ):
                for k, bv in zip(  # noqa: B905
                    b_coords[1, b_indptr[j] : b_indptr[j + 1]],
                    b_data[b_indptr[j] : b_indptr[j + 1]],
                ):
                    sums[k] += av * bv
                    if next_[k] == -1:
                        next_[k] = head
                        head = k
                        length += 1

            for _ in range(length):
                if next_[head] != -1:
                    coords[0, nnz] = i
                    coords[1, nnz] = head
                    data[nnz] = sums[head]
                    nnz += 1

                temp = head
                head = next_[head]

                next_[temp] = -1
                sums[temp] = 0

        return coords, data

    return _dot_coo_coo


@_memoize_dtype
def _dot_coo_ndarray_type(dt1, dt2):
    dtr = _dot_dtype(dt1, dt2)

    @numba.jit(nopython=True, nogil=True)
    def _dot_coo_ndarray(coords1, data1, array2, out_shape):  # pragma: no cover
        """
        Utility function taking in one `COO` and one ``ndarray`` and
        calculating a "sense" of their dot product. Acually computes
        ``s1 @ x2.T``.

        Parameters
        ----------
        data1, coords1 : np.ndarray
            The data and coordinates of ``s1``.
        array2 : np.ndarray
            The second input array ``x2``.
        out_shape : Tuple[int]
            The output shape.
        """
        out = np.zeros(out_shape, dtype=dtr)

        if array2.flags.f_contiguous and out_shape[1] >= 4:
            # array2 is transposed; traverse contiguous output columns while
            # preserving the accumulation order within each output element.
            for didx1 in range(len(data1)):
                row = coords1[0, didx1]
                col = coords1[1, didx1]
                value = data1[didx1]
                for oidx2 in range(out_shape[1]):
                    out[row, oidx2] += value * array2[oidx2, col]
            return out

        didx1 = 0

        while didx1 < len(data1):
            oidx1 = coords1[0, didx1]
            didx1_curr = didx1

            for oidx2 in range(out_shape[1]):
                didx1 = didx1_curr
                while didx1 < len(data1) and coords1[0, didx1] == oidx1:
                    out[oidx1, oidx2] += data1[didx1] * array2[oidx2, coords1[1, didx1]]
                    didx1 += 1

        return out

    return _dot_coo_ndarray


@_memoize_dtype
def _dot_coo_ndarray_type_sparse(dt1, dt2):
    dtr = _dot_dtype(dt1, dt2)

    @numba.jit(
        nopython=True,
        nogil=True,
        locals={"data_curr": numba.np.numpy_support.from_dtype(dtr)},
    )
    def _dot_coo_ndarray(coords1, data1, array2, out_shape):  # pragma: no cover
        """
        Utility function taking in one `COO` and one ``ndarray`` and
        calculating a "sense" of their dot product. Acually computes
        ``s1 @ x2.T``.

        Parameters
        ----------
        data1, coords1 : np.ndarray
            The data and coordinates of ``s1``.
        array2 : np.ndarray
            The second input array ``x2``.
        out_shape : Tuple[int]
            The output shape.
        """

        out_data = []
        out_coords = []

        # coords1.shape = (2, len(data1))
        # coords1[0, :] = rows, sorted
        # coords1[1, :] = columns

        didx1 = 0
        while didx1 < len(data1):
            current_row = coords1[0, didx1]

            cur_didx1 = didx1
            oidx2 = 0
            while oidx2 < out_shape[1]:
                cur_didx1 = didx1
                data_curr = 0
                while cur_didx1 < len(data1) and coords1[0, cur_didx1] == current_row:
                    data_curr += data1[cur_didx1] * array2[oidx2, coords1[1, cur_didx1]]
                    cur_didx1 += 1
                if data_curr != 0:
                    out_data.append(data_curr)
                    out_coords.append((current_row, oidx2))
                oidx2 += 1
            didx1 = cur_didx1

        if len(out_data) == 0:
            return np.empty((2, 0), dtype=np.intp), np.empty((0,), dtype=dtr)

        return np.array(out_coords).T, np.array(out_data)

    return _dot_coo_ndarray


@_memoize_dtype
def _dot_ndarray_coo_type(dt1, dt2):
    dtr = _dot_dtype(dt1, dt2)

    @numba.jit(nopython=True, nogil=True)
    def _dot_ndarray_coo(array1, coords2, data2, out_shape):  # pragma: no cover
        """
        Utility function taking in two one ``ndarray`` and one ``COO`` and
        calculating a "sense" of their dot product. Acually computes ``x1 @ s2.T``.

        Parameters
        ----------
        array1 : np.ndarray
            The input array ``x1``.
        data2, coords2 : np.ndarray
            The data and coordinates of ``s2``.
        out_shape : Tuple[int]
            The output shape.
        """
        out = np.zeros(out_shape, dtype=dtr)

        for oidx1 in range(out_shape[0]):
            for didx2 in range(len(data2)):
                oidx2 = coords2[1, didx2]
                out[oidx1, oidx2] += array1[oidx1, coords2[0, didx2]] * data2[didx2]

        return out

    return _dot_ndarray_coo


@_memoize_dtype
def _dot_ndarray_coo_type_sparse(dt1, dt2):
    dtr = _dot_dtype(dt1, dt2)

    @numba.jit(
        nopython=True,
        nogil=True,
        locals={"data_curr": numba.np.numpy_support.from_dtype(dtr)},
    )
    def _dot_ndarray_coo(array1, coords2, data2, out_shape):  # pragma: no cover
        """
        Utility function taking in two one ``ndarray`` and one ``COO`` and
        calculating a "sense" of their dot product. Acually computes ``x1 @ s2.T``.

        Parameters
        ----------
        array1 : np.ndarray
            The input array ``x1``.
        data2, coords2 : np.ndarray
            The data and coordinates of ``s2``.
        out_shape : Tuple[int]
            The output shape.
        """
        out_data = []
        out_coords = []

        # coords2.shape = (2, len(data2))
        # coords2[0, :] = columns, sorted
        # coords2[1, :] = rows

        for oidx1 in range(out_shape[0]):
            data_curr = 0
            current_col = 0
            for didx2 in range(len(data2)):
                if coords2[0, didx2] != current_col:
                    if data_curr != 0:
                        out_data.append(data_curr)
                        out_coords.append([oidx1, current_col])
                        data_curr = 0
                    current_col = coords2[0, didx2]

                data_curr += array1[oidx1, coords2[1, didx2]] * data2[didx2]

            if data_curr != 0:
                out_data.append(data_curr)
                out_coords.append([oidx1, current_col])

        if len(out_data) == 0:
            return np.empty((2, 0), dtype=np.intp), np.empty((0,), dtype=dtr)

        return np.array(out_coords).T, np.array(out_data)

    return _dot_ndarray_coo


# Copied from : https://github.com/numpy/numpy/blob/59fec4619403762a5d785ad83fcbde5a230416fc/numpy/core/einsumfunc.py#L523
# under BSD-3-Clause license : https://github.com/numpy/numpy/blob/v1.24.0/LICENSE.txt
def _parse_einsum_input(operands):
    """
    A copy of the numpy parse_einsum_input that
    does not cast the operands to numpy array.

    Returns
    -------
    input_strings : str
        Parsed input strings
    output_string : str
        Parsed output string
    operands : list of array_like
        The operands to use in the numpy contraction
    Examples
    --------
    The operand list is simplified to reduce printing:
    >>> rng = np.random.default_rng(42)
    >>> a = rng.random((4, 4))
    >>> b = rng.random((4, 4, 4))
    >>> _parse_einsum_input(("...a,...a->...", a, b))  # doctest: +SKIP
    ('za,xza', 'xz', [a, b])
    >>> _parse_einsum_input((a, [Ellipsis, 0], b, [Ellipsis, 0]))  # doctest: +SKIP
    ('za,xza', 'xz', [a, b])
    """

    if len(operands) == 0:
        raise ValueError("No input operands")

    if isinstance(operands[0], str):
        subscripts = operands[0].replace(" ", "")
        operands = operands[1:]

        # Ensure all characters are valid
        for s in subscripts:
            if s in ".,->":
                continue
            if not s.isalpha():
                raise ValueError(f"Character {s} is not a valid symbol.")

    else:
        tmp_operands = list(operands)
        operand_list = []
        subscript_list = []
        for _ in range(len(operands) // 2):
            operand_list.append(tmp_operands.pop(0))
            subscript_list.append(tmp_operands.pop(0))

        output_list = tmp_operands[-1] if len(tmp_operands) else None
        operands = operand_list
        subscripts = ""
        last = len(subscript_list) - 1
        for num, sub in enumerate(subscript_list):
            for s in sub:
                if s is Ellipsis:
                    subscripts += "..."
                else:
                    try:
                        s = index(s)
                    except TypeError as e:
                        raise TypeError("For this input type lists must contain either int or Ellipsis") from e
                    subscripts += _EINSUM_SYMBOLS[s]
            if num != last:
                subscripts += ","

        if output_list is not None:
            subscripts += "->"
            for s in output_list:
                if s is Ellipsis:
                    subscripts += "..."
                else:
                    try:
                        s = index(s)
                    except TypeError as e:
                        raise TypeError("For this input type lists must contain either int or Ellipsis") from e
                    subscripts += _EINSUM_SYMBOLS[s]
    # Check for proper "->"
    if ("-" in subscripts) or (">" in subscripts):
        invalid = (subscripts.count("-") > 1) or (subscripts.count(">") > 1)
        if invalid or (subscripts.count("->") != 1):
            raise ValueError("Subscripts can only contain one '->'.")

    # Parse ellipses
    if "." in subscripts:
        used = subscripts.replace(".", "").replace(",", "").replace("->", "")
        unused = list(_EINSUM_SYMBOLS_SET - set(used))
        ellipse_inds = "".join(unused)
        longest = 0

        if "->" in subscripts:
            input_tmp, output_sub = subscripts.split("->")
            split_subscripts = input_tmp.split(",")
            out_sub = True
        else:
            split_subscripts = subscripts.split(",")
            out_sub = False

        for num, sub in enumerate(split_subscripts):
            if "." in sub:
                if (sub.count(".") != 3) or (sub.count("...") != 1):
                    raise ValueError("Invalid Ellipses.")

                # Take into account numerical values
                if operands[num].shape == ():
                    ellipse_count = 0
                else:
                    ellipse_count = builtins.max(operands[num].ndim, 1)
                    ellipse_count -= len(sub) - 3

                if ellipse_count > longest:
                    longest = ellipse_count

                if ellipse_count < 0:
                    raise ValueError("Ellipses lengths do not match.")
                if ellipse_count == 0:
                    split_subscripts[num] = sub.replace("...", "")
                else:
                    rep_inds = ellipse_inds[-ellipse_count:]
                    split_subscripts[num] = sub.replace("...", rep_inds)

        subscripts = ",".join(split_subscripts)
        out_ellipse = "" if longest == 0 else ellipse_inds[-longest:]

        if out_sub:
            subscripts += "->" + output_sub.replace("...", out_ellipse)
        else:
            # Special care for outputless ellipses
            output_subscript = ""
            tmp_subscripts = subscripts.replace(",", "")
            for s in sorted(set(tmp_subscripts)):
                if not s.isalpha():
                    raise ValueError(f"Character {s} is not a valid symbol.")
                if tmp_subscripts.count(s) == 1:
                    output_subscript += s
            normal_inds = "".join(sorted(set(output_subscript) - set(out_ellipse)))

            subscripts += "->" + out_ellipse + normal_inds

    # Build output string if does not exist
    if "->" in subscripts:
        input_subscripts, output_subscript = subscripts.split("->")
    else:
        input_subscripts = subscripts
        # Build output subscripts
        tmp_subscripts = subscripts.replace(",", "")
        output_subscript = ""
        for s in sorted(set(tmp_subscripts)):
            if not s.isalpha():
                raise ValueError(f"Character {s} is not a valid symbol.")
            if tmp_subscripts.count(s) == 1:
                output_subscript += s

    # Make sure output subscripts are in the input
    for char in output_subscript:
        if char not in input_subscripts:
            raise ValueError(f"Output character {char} did not appear in the input")

    # Make sure number operands is equivalent to the number of terms
    if len(input_subscripts.split(",")) != len(operands):
        raise ValueError("Number of einsum subscripts must be equal to the number of operands.")

    return (input_subscripts, output_subscript, operands)


def _einsum_single(lhs, rhs, operand):
    """Perform a single term einsum, i.e. any combination of transposes, sums
    and traces of dimensions.

    Parameters
    ----------
    lhs : str
        The indices of the input array.
    rhs : str
        The indices of the output array.
    operand : SparseArray
        The array to perform the einsum on.

    Returns
    -------
    output : SparseArray
    """
    from ._coo import COO

    if lhs == rhs:
        if not rhs:
            # full contraction — return 0-D array per the Array API standard
            return operand.sum()
        return operand

    if not isinstance(operand, SparseArray):
        # just use numpy for dense input
        return np.einsum(f"{lhs}->{rhs}", operand)

    # else require COO for operations, but check if should convert back
    to_output_format = getattr(operand, "from_coo", lambda x: x)
    operand = as_coo(operand)

    # check if repeated / 'trace' indices mean we are only taking a subset
    where = {}
    for i, ix in enumerate(lhs):
        where.setdefault(ix, []).append(i)

    selector = None
    for locs in where.values():
        loc0, *rlocs = locs
        if rlocs:
            # repeated index
            if len({operand.shape[loc] for loc in locs}) > 1:
                raise ValueError("Repeated indices must have the same dimension.")

            # only select data where all indices match
            subselector = (operand.coords[loc0] == operand.coords[rlocs]).all(axis=0)
            if selector is None:
                selector = subselector
            else:
                selector &= subselector

    # indices that are removed (i.e. not in the output / `perm`)
    # are handled by `has_duplicates=True` below
    perm = [lhs.index(ix) for ix in rhs]
    new_shape = tuple(operand.shape[i] for i in perm)

    # select the new COO data
    if selector is not None:
        new_coords = operand.coords[:, selector][perm]
        new_data = operand.data[selector]
    else:
        new_coords = operand.coords[perm]
        new_data = operand.data

    if not rhs:
        # full contraction — return 0-D COO array per the Array API standard
        data = np.asarray(new_data.sum())
        return COO.from_numpy(data)

    return to_output_format(COO(new_coords, new_data, shape=new_shape, has_duplicates=True))


class EinsumBudgetError(ValueError, MemoryError):
    """Raised when no contraction order fits the temporary-element budget.

    The exception also derives from [`MemoryError`][] because it reports an
    avoidable out-of-memory condition: the contraction is refused before large
    intermediates are allocated.
    """


EinsumContractionError = EinsumBudgetError
EinsumMemoryError = EinsumBudgetError
ContractionBudgetError = EinsumBudgetError

_TEMP_BUDGET_KWARGS = (
    "temp_elements",
    "max_temp_elements",
    "temporary_elements",
    "max_temporary_elements",
    "temp_budget",
    "temporary_budget",
    "max_intermediate_elements",
    "max_intermediate_entries",
    "max_temp_entries",
    "max_temp_size",
    "temp_size",
    "element_budget",
    "element_limit",
    "max_elements",
    "max_stored_elements",
    "memory_limit",
    "budget",
)


def _pop_temp_element_budget(kwargs):
    """Pop and validate the temporary-element budget from ``kwargs``."""
    found = []
    for name in _TEMP_BUDGET_KWARGS:
        if name in kwargs:
            value = kwargs.pop(name)
            if value is not None:
                found.append((name, value))

    if not found:
        return None

    budgets = set()
    for name, value in found:
        if isinstance(value, bool):
            raise TypeError(f"`{name}` must be a non-negative integer, not {value!r}.")
        try:
            budget = index(value)
        except TypeError as exc:
            raise TypeError(f"`{name}` must be a non-negative integer, not {value!r}.") from exc
        if budget < 0:
            raise ValueError(f"`{name}` must be a non-negative integer, not {value!r}.")
        budgets.add(budget)

    if len(budgets) > 1:
        raise ValueError(f"Conflicting temporary-element budgets: {sorted(budgets)}.")

    return budgets.pop()


def _structured_keys(coords):
    """Return a 1-D sortable view of the columns of ``coords``.

    ``coords`` has shape ``(ndim, nnz)``.  Structured arrays keep the
    lexicographic ordering of coordinate tuples and compare exactly, without
    any floating point promotion of integer indices.
    """
    ndim, nnz = coords.shape
    if ndim == 0:
        return np.zeros(nnz, dtype=np.dtype([]))
    if ndim == 1:
        return np.ascontiguousarray(coords[0])
    dtype = np.dtype([(f"f{i}", coords.dtype) for i in range(ndim)])
    keys = np.empty(nnz, dtype=dtype)
    for i in range(ndim):
        keys[f"f{i}"] = coords[i]
    return keys


def _key_groups(coords):
    """Group equal coordinate tuples.

    Returns ``(keys, order, first, counts)`` where ``order`` sorts the stored
    entries by key, and ``first``/``counts`` delimit each key group in
    ``order``.
    """
    keys = _structured_keys(coords)
    order = np.argsort(keys, kind="stable")
    sorted_keys = keys[order]
    unique, first, counts = np.unique(sorted_keys, return_index=True, return_counts=True)
    return unique, order, first, counts


def _shared_join_count(xlabels, xarray, ylabels, yarray):
    """Count matching stored-entry pairs over shared indices.

    This is an upper bound on the number of stored elements in the pairwise
    contraction; it is computed from the actual index patterns, so two inputs
    with identical table sizes but different sparsity still receive different
    estimates.
    """
    shared = [ix for ix in xlabels if ix in ylabels]
    if not shared:
        return int(xarray.nnz) * int(yarray.nnz)

    xcoords = xarray.coords[[xlabels.index(ix) for ix in shared]]
    ycoords = yarray.coords[[ylabels.index(ix) for ix in shared]]
    xkeys, _, _, xcounts = _key_groups(xcoords)
    ykeys, _, _, ycounts = _key_groups(ycoords)
    if len(xkeys) == 0 or len(ykeys) == 0:
        return 0

    _, xpos, ypos = np.intersect1d(xkeys, ykeys, assume_unique=True, return_indices=True)
    if len(xpos) == 0:
        return 0
    return int(np.sum(xcounts[xpos] * ycounts[ypos], dtype=np.int64))


def _pair_result_labels(xlabels, ylabels, keep):
    labels = [ix for ix in xlabels if ix in keep]
    labels.extend(ix for ix in ylabels if ix in keep and ix not in xlabels)
    return "".join(labels)


def _pair_keep_labels(rhs, factors, skip):
    keep = set(rhs)
    for position, (labels, _) in enumerate(factors):
        if position not in skip:
            keep.update(labels)
    return keep


def _explicit_scalar(array):
    """Represent a 0-D COO with an explicit stored entry."""
    if array.ndim != 0 or array.nnz != 0:
        return array
    return COO(
        np.empty((0, 1), dtype=np.intp),
        np.asarray(array.fill_value).reshape(1),
        shape=(),
        has_duplicates=False,
        sorted=True,
    )


def _empty_pair_result(labels, sizes, dtype):
    shape = tuple(sizes[ix] for ix in labels)
    if not labels:
        return COO.from_numpy(np.asarray(0, dtype=dtype))
    coords = np.empty((len(labels), 0), dtype=np.intp)
    return COO(coords, np.empty(0, dtype=dtype), shape=shape, has_duplicates=False, sorted=True)


class _PairOverBudget(Exception):
    """Internal signal that a pairwise result exceeded the temp budget."""

    def __init__(self, required):
        super().__init__(required)
        self.required = int(required)


def _merge_blocks(acc_coords, acc_data, new_coords, new_data, budget=None):
    """Add one block of coordinates/data into the accumulator."""
    if new_coords.shape[1] == 0:
        return acc_coords, acc_data
    unique_rows, inverse = np.unique(new_coords.T, axis=0, return_inverse=True)
    new_coords = unique_rows.T
    if len(inverse) == new_coords.shape[1]:
        new_data = new_data[np.argsort(inverse, kind="stable")]
    else:
        summed = np.zeros(new_coords.shape[1], dtype=new_data.dtype)
        np.add.at(summed, inverse, new_data)
        new_data = summed

    if acc_coords.shape[1] == 0:
        if budget is not None and new_coords.shape[1] > budget:
            raise _PairOverBudget(new_coords.shape[1])
        return new_coords, new_data

    acc_keys = _structured_keys(acc_coords)
    new_keys = _structured_keys(new_coords)
    positions = np.searchsorted(acc_keys, new_keys)
    found = positions < len(acc_keys)
    found[found] = acc_keys[positions[found]] == new_keys[found]

    if budget is not None:
        missing_count = int(new_coords.shape[1] - np.count_nonzero(found))
        union_size = acc_coords.shape[1] + missing_count
        if union_size > budget:
            raise _PairOverBudget(union_size)

    acc_data[positions[found]] += new_data[found]

    missing = ~found
    if builtins.any(missing):
        merged_coords = np.concatenate([acc_coords, new_coords[:, missing]], axis=1)
        merged_data = np.concatenate([acc_data, new_data[missing]])
        order = np.argsort(_structured_keys(merged_coords), kind="stable")
        return merged_coords[:, order], merged_data[order]
    return acc_coords, acc_data


def _pair_entries(xlabels, xarray, ylabels, yarray, shared_groups, start, stop):
    """Entry positions for a contiguous range of matching stored-entry pairs."""
    if shared_groups is None:
        positions = np.arange(start, stop, dtype=np.intp)
        return positions // yarray.nnz, positions % yarray.nnz

    xorder, yorder, xpos, ypos, xfirst, yfirst, xcounts, ycounts, cumulative = shared_groups
    entries = np.arange(start, stop, dtype=np.intp)
    group = np.searchsorted(cumulative, entries, side="right")
    base = cumulative[group] - xcounts[group] * ycounts[group]
    local = entries - base
    matched_y = ycounts[group]
    xlocal = local // matched_y
    ylocal = local % matched_y
    return xorder[xfirst[xpos][group] + xlocal], yorder[yfirst[ypos][group] + ylocal]


def _contract_pair(xlabels, xarray, ylabels, yarray, keep, result_labels, sizes, chunk_elements=None, budget=None):
    """Contract two canonical COO factors in one sparse join.

    Only stored entries that agree on all shared indices are combined.  There
    is no dense bounding-box intermediate, so a large outer product is never
    materialised just to be reduced afterwards.  Matching pairs are processed
    in chunks bounded by ``chunk_elements`` and merged incrementally, so even
    a long join keeps its scratch storage within the budget.  When ``budget``
    is given, the accumulated result is checked by exact union size before each
    block is merged and ``_PairOverBudget`` is raised as soon as it cannot fit.
    """
    x_result_labels = [ix for ix in xlabels if ix in keep]
    y_result_labels = [ix for ix in ylabels if ix in keep and ix not in xlabels]
    x_result_pos = [xlabels.index(ix) for ix in x_result_labels]
    y_result_pos = [ylabels.index(ix) for ix in y_result_labels]
    dtype = _dot_dtype(xarray.dtype, yarray.dtype)

    shared = [ix for ix in xlabels if ix in ylabels]
    if shared:
        xcoords = xarray.coords[[xlabels.index(ix) for ix in shared]]
        ycoords = yarray.coords[[ylabels.index(ix) for ix in shared]]
        xkeys, xorder, xfirst, xcounts = _key_groups(xcoords)
        ykeys, yorder, yfirst, ycounts = _key_groups(ycoords)
        if len(xkeys) == 0 or len(ykeys) == 0:
            return _empty_pair_result(result_labels, sizes, dtype)

        _, xpos, ypos = np.intersect1d(xkeys, ykeys, assume_unique=True, return_indices=True)
        if len(xpos) == 0:
            return _empty_pair_result(result_labels, sizes, dtype)

        matched_xcounts = xcounts[xpos]
        matched_ycounts = ycounts[ypos]
        pairs_per_key = matched_xcounts * matched_ycounts
        total = int(pairs_per_key.sum())
        if total == 0:
            return _empty_pair_result(result_labels, sizes, dtype)
        shared_groups = (
            xorder,
            yorder,
            xpos,
            ypos,
            xfirst,
            yfirst,
            matched_xcounts,
            matched_ycounts,
            np.cumsum(pairs_per_key),
        )
    else:
        total = int(xarray.nnz) * int(yarray.nnz)
        if total == 0:
            return _empty_pair_result(result_labels, sizes, dtype)
        shared_groups = None

    if chunk_elements is None:
        chunk_elements = total
    chunk_elements = builtins.max(1, int(chunk_elements))

    if not result_labels:
        value = np.zeros((), dtype=dtype)
        for start in range(0, total, chunk_elements):
            xentries, yentries = _pair_entries(
                xlabels, xarray, ylabels, yarray, shared_groups, start, builtins.min(start + chunk_elements, total)
            )
            value = value + np.asarray(xarray.data[xentries] * yarray.data[yentries]).sum()
        return COO(
            np.empty((0, 1), dtype=np.intp),
            value.reshape(1),
            shape=(),
            has_duplicates=False,
            sorted=True,
            prune=True,
        )

    acc_coords = np.empty((len(result_labels), 0), dtype=np.intp)
    acc_data = np.empty(0, dtype=dtype)
    for start in range(0, total, chunk_elements):
        stop = builtins.min(start + chunk_elements, total)
        xentries, yentries = _pair_entries(xlabels, xarray, ylabels, yarray, shared_groups, start, stop)
        pair_data = xarray.data[xentries] * yarray.data[yentries]
        coord_parts = [xarray.coords[pos, xentries] for pos in x_result_pos]
        coord_parts.extend(yarray.coords[pos, yentries] for pos in y_result_pos)
        coords = np.stack(coord_parts, axis=0)
        acc_coords, acc_data = _merge_blocks(acc_coords, acc_data, coords, pair_data, budget=budget)

    shape = tuple(sizes[ix] for ix in result_labels)
    return COO(
        acc_coords,
        acc_data,
        shape=shape,
        has_duplicates=False,
        sorted=True,
        prune=True,
    )


def _pair_estimate(xlabels, xarray, ylabels, yarray, result_labels, sizes):
    """Upper-bound the stored elements produced by a pairwise contraction.

    The result can have at most one entry per matching stored-entry pair and
    at most one entry per point of its output shape.  Chunked reduction keeps
    the working arrays proportional to the smaller of the two.
    """
    if xarray.nnz == 0 or yarray.nnz == 0:
        return 0
    pairs = _shared_join_count(xlabels, xarray, ylabels, yarray)
    shape_size = 1
    for ix in result_labels:
        shape_size *= sizes[ix]
    return builtins.min(pairs, shape_size)


def _pair_equation(xlabels, ylabels, result_labels):
    return f"{''.join(xlabels)},{''.join(ylabels)}->{''.join(result_labels)}"


def _prepare_einsum_factors(terms, rhs, operands):
    """Reduce each operand to canonical distinct labels and COO storage."""
    term_counts = {}
    sizes = {}
    for term, operand in zip(terms, operands, strict=True):
        for ix, dim in zip(term, operand.shape, strict=False):
            if dim != sizes.setdefault(ix, dim):
                raise ValueError(f"Inconsistent shape for index '{ix}'.")
            term_counts[ix] = term_counts.get(ix, 0) + 1

    factors = []
    for term, operand in zip(terms, operands, strict=True):
        target = ""
        for ix in term:
            keep = term_counts[ix] > 1 or ix in rhs
            if keep and ix not in target:
                target += ix
        if target != term:
            # Trace repeated indices and sum leaf-private axes immediately.
            operand = _einsum_single(term, target, operand)
        factors.append((target, _explicit_scalar(as_coo(operand))))
    return factors, sizes


def _pair_candidates(factors, rhs, sizes):
    """Yield possible pairwise contractions, best (smallest) first."""
    candidates = []
    for i in range(len(factors)):
        xlabels, xarray = factors[i]
        for j in range(i + 1, len(factors)):
            ylabels, yarray = factors[j]
            keep = _pair_keep_labels(rhs, factors, {i, j})
            result_labels = _pair_result_labels(xlabels, ylabels, keep)
            estimate = _pair_estimate(xlabels, xarray, ylabels, yarray, result_labels, sizes)
            candidates.append((estimate, i, j, keep, result_labels))
    candidates.sort(key=lambda item: item[0])
    return candidates


def _merge_pair(factors, i, j, keep, result_labels, sizes, budget=None, chunk_elements=None):
    xlabels, xarray = factors[i]
    ylabels, yarray = factors[j]
    if chunk_elements is None and budget is not None:
        chunk_elements = builtins.max(1, budget)
    result = _contract_pair(
        xlabels,
        xarray,
        ylabels,
        yarray,
        keep,
        result_labels,
        sizes,
        chunk_elements=chunk_elements,
        budget=budget,
    )
    remaining = [factor for k, factor in enumerate(factors) if k not in (i, j)]
    remaining.append((result_labels, result))
    return remaining


def _greedy_einsum_factors(factors, rhs, sizes):
    """Contract the cheapest pair first, without a memory budget."""
    factors = list(factors)
    while len(factors) > 1:
        _, i, j, keep, result_labels = _pair_candidates(factors, rhs, sizes)[0]
        factors = _merge_pair(factors, i, j, keep, result_labels, sizes)
    return factors[0]


def _budgeted_einsum_factors(factors, rhs, sizes, budget, equation):
    """Find and execute a pairwise contraction order within ``budget``.

    Each candidate's stored-element count is computed from the actual sparse
    index patterns before any array is built.  Branches that cannot fit are
    abandoned, so the failure path never allocates the oversized
    intermediate.
    """
    dead_ends = []
    nodes = 0
    max_nodes = 100_000

    def search(current, depth):
        nonlocal nodes
        nodes += 1
        if nodes > max_nodes:
            raise EinsumBudgetError(
                f"Cannot decide whether einsum '{equation}' fits within a temporary-element budget of {budget}; "
                "the contraction-order search limit was reached."
            )
        if len(current) == 1:
            return current[0]

        candidates = _pair_candidates(current, rhs, sizes)
        if not candidates:
            return current[0]

        progressed = False
        for _estimate, i, j, keep, result_labels in candidates:
            pair = _pair_equation(current[i][0], current[j][0], result_labels)
            # The budget covers temporary intermediates only; the result of the
            # last contraction is the output and is allowed to be larger.
            final = len(current) == 2
            try:
                remaining = _merge_pair(
                    current,
                    i,
                    j,
                    keep,
                    result_labels,
                    sizes,
                    None if final else budget,
                )
            except _PairOverBudget as exc:
                dead_ends.append((-depth, exc.required, pair))
                continue
            progressed = True
            solved = search(remaining, depth + 1)
            if solved is not None:
                return solved
        if not progressed:
            dead_ends.append((-depth, None, None))
        return None

    solved = search(list(factors), 0)
    if solved is not None:
        return solved

    if dead_ends:
        sized = [(depth, estimate, pair) for depth, estimate, pair in dead_ends if estimate is not None]
        if sized:
            _, estimate, pair = builtins.min(sized, key=lambda item: (item[0], item[1]))
            raise EinsumBudgetError(
                f"Cannot complete einsum '{equation}' within a temporary-element budget of {budget} stored elements. "
                f"The contraction '{pair}' requires at least {estimate} stored elements"
                f" ({estimate - budget} over budget); no budget-feasible contraction order exists."
            )

    raise EinsumBudgetError(
        f"Cannot complete einsum '{equation}' within a temporary-element budget of {budget} stored elements; "
        "no budget-feasible contraction order exists."
    )


def _einsum_output_format(operands):
    from ._compressed import GCXS
    from ._dok import DOK

    sparse_operands = [operand for operand in operands if isinstance(operand, SparseArray)]
    if sparse_operands and builtins.all(isinstance(operand, DOK) for operand in sparse_operands):
        return DOK, {}
    if sparse_operands and builtins.all(isinstance(operand, GCXS) for operand in sparse_operands):
        if len({operand.compressed_axes for operand in sparse_operands}) == 1:
            return GCXS, {"compressed_axes": sparse_operands[0].compressed_axes}
        return GCXS, {}
    return COO, {}


def einsum(*operands, **kwargs):
    """
    Perform the equivalent of [`numpy.einsum`][].

    Parameters
    ----------
    subscripts : str
        Specifies the subscripts for summation as comma separated list of
        subscript labels. An implicit (classical Einstein summation)
        calculation is performed unless the explicit indicator '->' is
        included as well as subscript labels of the precise output form.
    operands : sequence of SparseArray
        These are the arrays for the operation.
    dtype : data-type, optional
        If provided, forces the calculation to use the data type specified.
        Default is `None`.
    temp_elements : int, optional
        Maximum number of stored elements allowed in any temporary
        intermediate produced while contracting several operands.  The
        aliases ``max_temp_elements``, ``temporary_elements``,
        ``max_intermediate_elements``, ``element_budget``, ``max_elements``,
        ``memory_limit`` and ``budget`` are accepted as well.  If no
        contraction order fits, [`EinsumBudgetError`][] is raised before the
        oversized intermediate is allocated.
    optimize : bool or {"greedy", "auto", "optimal"}, optional
        If true, plan a pairwise contraction order with the smallest
        estimated intermediate instead of building one outer product over all
        operands.  No budget is enforced in this mode.
    **kwargs : dict, optional
        Any additional arguments to pass to the function.

    Returns
    -------
    output : SparseArray
        The calculation based on the Einstein summation convention.

    Raises
    ------
    EinsumBudgetError
        If a temporary-element budget was supplied and no pairwise
        contraction order stays within it.  The exception names the
        contraction that cannot be completed and is also a [`MemoryError`][].
    """

    lhs, rhs, operands = _parse_einsum_input(operands)  # Parse input

    check_zero_fill_value(*operands)

    if "dtype" in kwargs and kwargs["dtype"] is not None:
        operands = [o.astype(kwargs["dtype"]) for o in operands]

    temp_elements = _pop_temp_element_budget(kwargs)
    optimize = kwargs.pop("optimize", False)
    if optimize is None:
        optimize = False
    if isinstance(optimize, str):
        if optimize not in {"greedy", "auto", "optimal"}:
            raise ValueError(f"Invalid einsum optimize strategy: {optimize!r}.")
        optimize = True
    if optimize not in (True, False):
        raise TypeError(f"`optimize` must be a bool or strategy name, not {optimize!r}.")

    if len(operands) == 1:
        return _einsum_single(lhs, rhs, operands[0])

    pairwise = temp_elements is not None or optimize

    if pairwise and builtins.any(isinstance(operand, SparseArray) for operand in operands):
        # Contract pairwise, inspecting the actual sparse index patterns.  The
        # old path below forms one product over all shared indices before any
        # reduction, which can materialise an enormous `ijkl` intermediate even
        # though each pairwise contraction is small.
        terms = lhs.split(",")
        factors, sizes = _prepare_einsum_factors(terms, rhs, operands)
        if temp_elements is not None:
            out_labels, result = _budgeted_einsum_factors(
                factors,
                rhs,
                sizes,
                temp_elements,
                lhs + "->" + rhs,
            )
        else:
            out_labels, result = _greedy_einsum_factors(factors, rhs, sizes)
        out_term = "".join(out_labels)
        if set(out_term) != set(rhs) or len(out_term) != len(rhs):
            raise ValueError(f"Inconsistent shape for index '{out_term}'.")
        if out_term != rhs:
            result = _einsum_single(out_term, rhs, result)
        out_type, out_kwargs = _einsum_output_format(operands)
        return result.asformat(out_type, **out_kwargs)

    # if multiple arrays: align, broadcast multiply and then use single einsum
    # for example:
    #     "aab,cbd->dac"
    # we first perform single term reductions and align:
    #     aab -> ab..
    #     cbd -> .bcd
    # (where dots represent broadcastable size 1 dimensions), then multiply all
    # to form the 'minimal outer product' and do a final single term einsum:
    #     abcd -> dac

    # get ordered union of indices from all terms, indicies that only appear
    # on a single term will be removed in the 'preparation' step below
    terms = lhs.split(",")
    total = {}
    sizes = {}
    for t, term in enumerate(terms):
        shape = operands[t].shape
        for ix, d in zip(term, shape, strict=False):
            if d != sizes.setdefault(ix, d):
                raise ValueError(f"Inconsistent shape for index '{ix}'.")
            total.setdefault(ix, set()).add(t)
    for ix in rhs:
        total[ix].add(-1)
    aligned_term = "".join(ix for ix, apps in total.items() if len(apps) > 1)

    # NB: if every index appears exactly twice,
    # we could identify and dispatch to tensordot here?

    parrays = []
    for term, array in zip(terms, operands, strict=True):
        # calc the target indices for this term
        pterm = "".join(ix for ix in aligned_term if ix in term)
        if pterm != term:
            # perform necessary transpose and reductions
            array = _einsum_single(term, pterm, array)
        # calc broadcastable shape
        shape = tuple(array.shape[pterm.index(ix)] if ix in pterm else 1 for ix in aligned_term)
        parrays.append(array.reshape(shape) if array.shape != shape else array)

    aligned_array = reduce(mul, parrays)

    return _einsum_single(aligned_term, rhs, aligned_array)


def stack(arrays, axis=0, compressed_axes=None):
    """
    Stack the input arrays along the given dimension.

    Parameters
    ----------
    arrays : Iterable[SparseArray]
        The input arrays to stack.
    axis : int, optional
        The axis along which to stack the input arrays.
    compressed_axes : iterable, optional
        The axes to compress if returning a GCXS array.

    Returns
    -------
    SparseArray
        The output stacked array.

    Raises
    ------
    ValueError
        If all elements of `arrays` don't have the same fill-value.

    See Also
    --------
    [`numpy.stack`][]: NumPy equivalent function
    """
    from ._compressed import GCXS

    if not builtins.all(isinstance(arr, GCXS) for arr in arrays):
        from ._coo import stack as coo_stack

        return coo_stack(arrays, axis)

    from ._compressed import stack as gcxs_stack

    return gcxs_stack(arrays, axis, compressed_axes)


def concatenate(arrays, axis=0, compressed_axes=None):
    """
    Concatenate the input arrays along the given dimension.

    Parameters
    ----------
    arrays : Iterable[SparseArray]
        The input arrays to concatenate.
    axis : int, optional
        The axis along which to concatenate the input arrays. The default is zero.
    compressed_axes : iterable, optional
        The axes to compress if returning a GCXS array.

    Returns
    -------
    SparseArray
        The output concatenated array.

    Raises
    ------
    ValueError
        If all elements of `arrays` don't have the same fill-value.

    See Also
    --------
    [`numpy.concatenate`][] : NumPy equivalent function
    """
    from ._compressed import GCXS

    if not builtins.all(isinstance(arr, GCXS) for arr in arrays):
        from ._coo import concatenate as coo_concat

        return coo_concat(arrays, axis)

    from ._compressed import concatenate as gcxs_concat

    return gcxs_concat(arrays, axis, compressed_axes)


concat = concatenate


@_check_device
def eye(N, M=None, k=0, dtype=float, format="coo", *, device=None, **kwargs):
    """Return a 2-D array in the specified format with ones on the diagonal and zeros elsewhere.

    Parameters
    ----------
    N : int
        Number of rows in the output.
    M : int, optional
        Number of columns in the output. If None, defaults to `N`.
    k : int, optional
        Index of the diagonal: 0 (the default) refers to the main diagonal,
        a positive value refers to an upper diagonal, and a negative value
        to a lower diagonal.
    dtype : data-type, optional
        Data-type of the returned array.
    format : str, optional
        A format string.

    Returns
    -------
    I : SparseArray of shape (N, M)
        An array where all elements are equal to zero, except for the `k`-th
        diagonal, whose values are equal to one.

    Examples
    --------
    >>> eye(2, dtype=int).todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[1, 0],
           [0, 1]])
    >>> eye(3, k=1).todense()  # doctest: +SKIP
    array([[0., 1., 0.],
           [0., 0., 1.],
           [0., 0., 0.]])
    """
    from ._coo import COO

    if M is None:
        M = N

    N = int(N)
    M = int(M)
    k = int(k)

    data_length = builtins.min(N, M)
    if k > 0:
        data_length = builtins.max(builtins.min(data_length, M - k), 0)
    elif k < 0:
        data_length = builtins.max(builtins.min(data_length, N + k), 0)

    if data_length == 0:
        return zeros((N, M), dtype=dtype, format=format, device=device)

    if k > 0:
        n_coords = np.arange(data_length, dtype=np.intp)
        m_coords = n_coords + k
    elif k < 0:
        m_coords = np.arange(data_length, dtype=np.intp)
        n_coords = m_coords - k
    else:
        n_coords = m_coords = np.arange(data_length, dtype=np.intp)

    coords = np.stack([n_coords, m_coords])
    data = np.array(1, dtype=dtype)

    return COO(coords, data=data, shape=(N, M), has_duplicates=False, sorted=True).asformat(format, **kwargs)


@_check_device
def full(shape, fill_value, dtype=None, format="coo", order="C", *, device=None, **kwargs):
    """Return a SparseArray of given shape and type, filled with `fill_value`.

    Parameters
    ----------
    shape : int or tuple of ints
        Shape of the new array, e.g., ``(2, 3)`` or ``2``.
    fill_value : scalar
        Fill value.
    dtype : data-type, optional
        The desired data-type for the array. The default, `None`, means
        `np.array(fill_value).dtype`.
    format : str, optional
        A format string.
    compressed_axes : iterable, optional
        The axes to compress if returning a GCXS array.
    order : {'C', None}
        Values except these are not currently supported and raise a
        NotImplementedError.

    Returns
    -------
    out : SparseArray
        Array of `fill_value` with the given shape and dtype.

    Examples
    --------
    >>> full(5, 9).todense()  # doctest: +NORMALIZE_WHITESPACE
    array([9, 9, 9, 9, 9])

    >>> full((2, 2), 9, dtype=float).todense()  # doctest: +SKIP
    array([[9., 9.],
           [9., 9.]])
    """
    from sparse import COO

    if dtype is None:
        dtype = np.array(fill_value).dtype
    if not isinstance(shape, tuple):
        shape = (shape,)
    if order not in {"C", None}:
        raise NotImplementedError("Currently, only 'C' and None are supported.")
    data = np.empty(0, dtype=dtype)
    coords = np.empty((len(shape), 0), dtype=np.intp)
    return COO(
        coords,
        data=data,
        shape=shape,
        fill_value=fill_value,
        has_duplicates=False,
        sorted=True,
    ).asformat(format, **kwargs)


@_check_device
def full_like(a, fill_value, dtype=None, shape=None, format=None, *, device=None, **kwargs):
    """Return a full array with the same shape and type as a given array.

    Parameters
    ----------
    a : array_like
        The shape and data-type of the result will match those of `a`.
    dtype : data-type, optional
        Overrides the data type of the result.
    format : str, optional
        A format string.
    compressed_axes : iterable, optional
        The axes to compress if returning a GCXS array.

    Returns
    -------
    out : SparseArray
        Array of `fill_value` with the same shape and type as `a`.

    Examples
    --------
    >>> x = np.ones((2, 3), dtype="i8")
    >>> full_like(x, 9.0).todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[9, 9, 9],
           [9, 9, 9]])
    """
    if format is None and not isinstance(a, np.ndarray):
        format = type(a).__name__.lower()
    elif format is None:
        format = "coo"

    compressed_axes = kwargs.pop("compressed_axes", None)
    if hasattr(a, "compressed_axes") and compressed_axes is None:
        compressed_axes = a.compressed_axes
    return full(
        a.shape if shape is None else shape,
        fill_value,
        dtype=(a.dtype if dtype is None else dtype),
        format=format,
        **kwargs,
    )


def zeros(shape, dtype=float, format="coo", *, device=None, **kwargs):
    """Return a SparseArray of given shape and type, filled with zeros.

    Parameters
    ----------
    shape : int or tuple of ints
        Shape of the new array, e.g., ``(2, 3)`` or ``2``.
    dtype : data-type, optional
        The desired data-type for the array, e.g., `numpy.int8`.  Default is
        `numpy.float64`.
    format : str, optional
        A format string.
    compressed_axes : iterable, optional
        The axes to compress if returning a GCXS array.

    Returns
    -------
    out : SparseArray
        Array of zeros with the given shape and dtype.

    Examples
    --------
    >>> zeros(5).todense()  # doctest: +SKIP
    array([0., 0., 0., 0., 0.])

    >>> zeros((2, 2), dtype=int).todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[0, 0],
           [0, 0]])
    """
    return full(shape, fill_value=0, dtype=np.dtype(dtype), format=format, device=device, **kwargs)


def zeros_like(a, dtype=None, shape=None, format=None, *, device=None, **kwargs):
    """Return a SparseArray of zeros with the same shape and type as ``a``.

    Parameters
    ----------
    a : array_like
        The shape and data-type of the result will match those of `a`.
    dtype : data-type, optional
        Overrides the data type of the result.
    format : str, optional
        A format string.
    compressed_axes : iterable, optional
        The axes to compress if returning a GCXS array.

    Returns
    -------
    out : SparseArray
        Array of zeros with the same shape and type as `a`.

    Examples
    --------
    >>> x = np.ones((2, 3), dtype="i8")
    >>> zeros_like(x).todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[0, 0, 0],
           [0, 0, 0]])
    """
    return full_like(a, fill_value=0, dtype=dtype, shape=shape, format=format, device=device, **kwargs)


def ones(shape, dtype=float, format="coo", *, device=None, **kwargs):
    """Return a SparseArray of given shape and type, filled with ones.

    Parameters
    ----------
    shape : int or tuple of ints
        Shape of the new array, e.g., ``(2, 3)`` or ``2``.
    dtype : data-type, optional
        The desired data-type for the array, e.g., `numpy.int8`.  Default is
        `numpy.float64`.
    format : str, optional
        A format string.
    compressed_axes : iterable, optional
        The axes to compress if returning a GCXS array.

    Returns
    -------
    out : SparseArray
        Array of ones with the given shape and dtype.

    Examples
    --------
    >>> ones(5).todense()  # doctest: +SKIP
    array([1., 1., 1., 1., 1.])

    >>> ones((2, 2), dtype=int).todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[1, 1],
           [1, 1]])
    """
    return full(shape, fill_value=1, dtype=np.dtype(dtype), format=format, device=device, **kwargs)


def ones_like(a, dtype=None, shape=None, format=None, *, device=None, **kwargs):
    """Return a SparseArray of ones with the same shape and type as ``a``.

    Parameters
    ----------
    a : array_like
        The shape and data-type of the result will match those of `a`.
    dtype : data-type, optional
        Overrides the data type of the result.
    format : str, optional
        A format string.
    compressed_axes : iterable, optional
        The axes to compress if returning a GCXS array.

    Returns
    -------
    out : SparseArray
        Array of ones with the same shape and type as `a`.

    Examples
    --------
    >>> x = np.ones((2, 3), dtype="i8")
    >>> ones_like(x).todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[1, 1, 1],
           [1, 1, 1]])
    """
    return full_like(a, fill_value=1, dtype=dtype, shape=shape, format=format, device=device, **kwargs)


def empty(shape, dtype=float, format="coo", *, device=None, **kwargs):
    return full(shape, fill_value=0, dtype=np.dtype(dtype), format=format, device=device, **kwargs)


empty.__doc__ = zeros.__doc__


def empty_like(a, dtype=None, shape=None, format=None, *, device=None, **kwargs):
    return full_like(a, fill_value=0, dtype=dtype, shape=shape, format=format, device=device, **kwargs)


empty_like.__doc__ = zeros_like.__doc__


def can_cast(from_: SparseArray, to: np.dtype, /, *, casting: str = "safe") -> bool:
    """Determines if one data type can be cast to another data type

    Parameters
    ----------
    from_ : dtype or SparseArray
        Source array or dtype.
    to : dtype
        Destination dtype.
    casting: str
        Casting kind

    Returns
    -------
    out : bool
        Whether or not a cast is possible.

    Examples
    --------
    >>> x = sparse.ones((2, 3), dtype=sparse.int8)
    >>> sparse.can_cast(x, sparse.float64)
    True

    See Also
    --------
    - [`numpy.can_cast`][] : NumPy equivalent function
    """
    from_ = np.dtype(from_)

    return np.can_cast(from_, to, casting=casting)


def outer(a, b, out=None):
    """
    Return outer product of two sparse arrays.

    Parameters
    ----------
    a, b : sparse.SparseArray
        The input arrays.
    out : sparse.SparseArray
        The output array.

    Examples
    --------
    >>> import numpy as np
    >>> import sparse
    >>> a = sparse.COO(np.arange(4))
    >>> o = sparse.outer(a, a)
    >>> o.todense()
    array([[0, 0, 0, 0],
           [0, 1, 2, 3],
           [0, 2, 4, 6],
           [0, 3, 6, 9]])
    """
    from ._coo import COO
    from ._sparse_array import SparseArray

    if isinstance(a, SparseArray):
        a = COO(a)
    if isinstance(b, SparseArray):
        b = COO(b)
    return np.multiply.outer(a.flatten(), b.flatten(), out=out)


def asnumpy(a, dtype=None, order=None):
    """Returns a dense numpy array from an arbitrary source array.

    Parameters
    ----------
    a: array_like
        Arbitrary object that can be converted to [`numpy.ndarray`][].
    order: ({'C', 'F', 'A'})
        The desired memory layout of the output
        array. When ``order`` is 'A', it uses 'F' if ``a`` is
        fortran-contiguous and 'C' otherwise.

    Returns
    -------
    numpy.ndarray: Converted array on the host memory.
    """
    from ._sparse_array import SparseArray

    if isinstance(a, SparseArray):
        a = a.todense()
    return np.asarray(a, dtype=dtype, order=order)


# this code was taken from numpy.moveaxis
# (cf. numpy/core/numeric.py, lines 1340-1409, v1.18.4)
# https://github.com/numpy/numpy/blob/v1.18.4/numpy/core/numeric.py#L1340-L1409
def moveaxis(a, source, destination):
    """
    Move axes of an array to new positions.

    Other axes remain in their original order.

    Parameters
    ----------
    a : SparseArray
        The array whose axes should be reordered.
    source : int or List[int]
        Original positions of the axes to move. These must be unique.
    destination : int or List[int]
        Destination positions for each of the original axes. These must also be unique.

    Returns
    -------
    SparseArray
        Array with moved axes.

    Examples
    --------
    >>> import numpy as np
    >>> import sparse
    >>> x = sparse.COO.from_numpy(np.ones((2, 3, 4, 5)))
    >>> sparse.moveaxis(x, (0, 1), (2, 3))
    <COO: shape=(4, 5, 2, 3), dtype=float64, nnz=120, fill_value=0.0>
    """

    if not isinstance(source, Iterable):
        source = (source,)
    if not isinstance(destination, Iterable):
        destination = (destination,)

    source = normalize_axis(source, a.ndim)
    destination = normalize_axis(destination, a.ndim)

    if len(source) != len(destination):
        raise ValueError("`source` and `destination` arguments must have the same number of elements")

    order = [n for n in range(a.ndim) if n not in source]

    for dest, src in sorted(zip(destination, source, strict=True)):
        order.insert(dest, src)

    return a.transpose(order)


def pad(array, pad_width, mode="constant", **kwargs):
    """
    Performs the equivalent of [`sparse.SparseArray`][]. Note that
    this function returns a new array instead of a view.

    Parameters
    ----------
    array : SparseArray
        Sparse array which is to be padded.

    pad_width : {sequence, array_like, int}
        Number of values padded to the edges of each axis. ((before_1, after_1), … (before_N, after_N)) unique pad
        widths for each axis. ((before, after),) yields same before and after pad for each axis. (pad,) or int is a
        shortcut for before = after = pad width for all axes.

    mode : str
        Pads to a constant value which is fill value. Currently only constant mode is implemented

    constant_values : int
        The values to set the padded values for each axis. Default is 0. This must be same as fill value.

    Returns
    -------
    SparseArray
        The padded sparse array.

    Raises
    ------
    NotImplementedError
        If mode != 'constant' or there are unknown arguments.

    ValueError
        If constant_values != self.fill_value

    See Also
    --------
    [`numpy.pad`][] : NumPy equivalent function

    """
    if not isinstance(array, SparseArray):
        raise NotImplementedError("Input array is not compatible.")

    if mode.lower() != "constant":
        raise NotImplementedError(f"Mode '{mode}' is not yet supported.")

    if not equivalent(kwargs.pop("constant_values", _zero_of_dtype(array.dtype)), array.fill_value):
        raise ValueError("constant_values can only be equal to fill value.")

    if kwargs:
        raise NotImplementedError("Additional Unknown arguments present.")

    from ._coo import COO

    array = array.asformat("coo")

    pad_width = np.broadcast_to(pad_width, (len(array.shape), 2))
    new_coords = array.coords + pad_width[:, 0:1]
    new_shape = tuple([array.shape[i] + pad_width[i, 0] + pad_width[i, 1] for i in range(len(array.shape))])
    new_data = array.data
    return COO(new_coords, new_data, new_shape, fill_value=array.fill_value)


def format_to_string(format):
    if isinstance(format, type):
        if not issubclass(format, SparseArray):
            raise ValueError(f"invalid format: {format}")
        format = format.__name__.lower()

    if isinstance(format, str):
        return format

    raise ValueError(f"invalid format: {format}")


@_check_device
def asarray(obj, /, *, dtype=None, format=None, copy=False, device=None):
    """
    Convert the input to a sparse array.

    Parameters
    ----------
    obj : array_like
        Object to be converted to an array.
    dtype : dtype, optional
        Output array data type.
    format : str, optional
        Output array sparse format.
        Default: existing format if the input is a `SparseArray`,
        else COO.
    device : str, optional
        Device on which to place the created array.
    copy : bool, optional
        Boolean indicating whether or not to copy the input.

    Returns
    -------
    out : Union[SparseArray, numpy.ndarray]
        Sparse or 0-D array containing the data from `obj`.

    Examples
    --------
    >>> x = np.eye(8, dtype="i8")
    >>> sparse.asarray(x, format="coo")
    <COO: shape=(8, 8), dtype=int64, nnz=8, fill_value=0>
    """

    if format not in {None, "coo", "dok", "gcxs", "csc", "csr"}:
        raise ValueError(f"{format} format not supported.")

    from ._compressed import CSC, CSR, GCXS
    from ._coo import COO
    from ._dok import DOK

    format_dict = {"coo": COO, "dok": DOK, "gcxs": GCXS, "csc": CSC, "csr": CSR}

    if isinstance(obj, SparseArray):
        if copy:
            obj = obj.copy()
        if format is not None:
            obj = obj.asformat(format)
        if dtype is not None and obj.dtype != dtype:
            obj = obj.astype(dtype)
        return obj

    format = "coo" if format is None else format

    if _is_scipy_sparse_obj(obj):
        sparse_obj = format_dict[format].from_scipy_sparse(obj)
        if dtype is None:
            dtype = sparse_obj.dtype
        return sparse_obj.astype(dtype=dtype, copy=copy)

    if np.isscalar(obj) or isinstance(obj, np.ndarray | Iterable):
        sparse_obj = format_dict[format].from_numpy(np.asarray(obj))
        if dtype is None:
            dtype = sparse_obj.dtype
        return sparse_obj.astype(dtype=dtype, copy=copy)

    raise ValueError(f"{type(obj)} not supported.")


def _support_numpy(func):
    """
    In case a NumPy array is passed to `sparse` namespace function
    we want to flag it and dispatch to NumPy.
    """

    @wraps(func)
    def wrapper_func(*args, **kwargs):
        x = args[0]
        if isinstance(x, np.ndarray | np.number):
            warnings.warn(
                f"Sparse {func.__name__} received dense NumPy array instead "
                "of sparse array. Dispatching to NumPy function.",
                RuntimeWarning,
                stacklevel=2,
            )
            return getattr(np, func.__name__)(*args, **kwargs)

        return func(*args, **kwargs)

    return wrapper_func


def all(x, /, *, axis=None, keepdims=False):
    """
    Tests whether all input array elements evaluate to ``True`` along a specified axis.

    Parameters
    ----------
    x: array
        input array.
    axis: Optional[Union[int, Tuple[int, ...]]]
        axis or axes along which to perform a logical AND reduction. By default, a logical AND
        reduction is performed over the entire array.
        If a tuple of integers, logical AND reductions are performed over multiple axes.
        A valid ``axis`` is an integer on the interval ``[-N, N)``, where ``N`` is the rank
        (number of dimensions) of ``x``. If an ``axis`` is specified as a negative integer,
        the function determines the axis along which to perform a reduction by counting backward
        from the last dimension (where ``-1`` refers to the last dimension). If provided an invalid
        ``axis``, the function raiseS an exception. Default: ``None``.
    keepdims: bool
        If ``True``, the reduced axes (dimensions) are included in the result as singleton dimensions,
        and, accordingly, the result is compatible with the input array.
        Otherwise, if ``False``, the reduced axes (dimensions) are not included in the result.
        Default: ``False``.

    Returns
    -------
    out: array
        if a logical AND reduction was performed over the entire array, the returned array is a
        zero-dimensional array containing the test result; otherwise, the returned array is a
        non-zero-dimensional array containing the test results.
        The returned array has a data type of ``bool``.

    Special Cases
    -------------

       - Positive infinity, negative infinity, and NaN  evaluate to ``True``.

       - If ``x`` has a complex floating-point data type, elements having a non-zero component
        (real or imaginary) evaluate to ``True``.

       - If ``x`` is an empty array or the size of the axis (dimension) along which to evaluate elements
         is zero, the test result is ``True``.

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, 0]]))
    >>> o = sparse.all(a, axis=1)
    >>> o.todense()  # doctest: +NORMALIZE_WHITESPACE
    array([False, False])
    """
    return x.all(axis=axis, keepdims=keepdims)


def any(x, /, *, axis=None, keepdims=False):
    """
    Tests whether any input array element evaluates to ``True`` along a specified axis.

    Parameters
    ----------
    x: array
        input array.
    axis: Optional[Union[int, Tuple[int, ...]]]
        axis or axes along which to perform a logical OR reduction.
        By default, a logical OR reduction is performed over the entire array.
        If a tuple of integers, logical OR reductions are performed over multiple axes.
        A valid ``axis`` must be an integer on the interval ``[-N, N)``, where ``N`` is the rank (number of
        dimensions) of ``x``. If an ``axis`` is specified as a negative integer, the function determines
        the axis along which to perform a reduction by counting backward from the last dimension (where
        ``-1`` refers to the last dimension).
        If provided an invalid ``axis``, the function raises an exception.
        Default: ``None``.
    keepdims: bool
        If ``True``, the reduced axes (dimensions) are included in the result as singleton dimensions,
        and, accordingly, the result must is compatible with the input array. Otherwise, if ``False``,
        the reduced axes (dimensions) is not included in the result.
        Default: ``False``.

    Returns
    -------
    out: array
        if a logical OR reduction was performed over the entire array, the returned array is a
        zero-dimensional array containing the test result.
        Otherwise, the returned array is a non-zero-dimensional array containing the test results.
        The returned array is of type ``bool``.

    Special Cases
    -------------

       - Positive infinity, negative infinity, and NaN  evaluate to ``True``.

       - If ``x`` has a complex floating-point data type, elements having a non-zero component
        (real or imaginary) evaluate to ``True``.

       - If ``x`` is an empty array or the size of the axis (dimension) along which to evaluate elements
         is zero, the test result is ``False``.

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, 0]]))
    >>> o = sparse.any(a, axis=1)
    >>> o.todense()  # doctest: +NORMALIZE_WHITESPACE
    array([ True, True])
    """
    return x.any(axis=axis, keepdims=keepdims)


def permute_dims(x, /, axes=None):
    """
    Permutes the axes (dimensions) of an array ``x``.

    Parameters
    ----------
    x: array
        input array.
    axes: Tuple[int, ...]
        tuple containing a permutation of ``(0, 1, ..., N-1)`` where ``N`` is the number of axes (dimensions)
        of ``x``.

    Returns
    -------
    out: array
        an array containing the axes permutation. The returned array must have the same data type as ``x``.

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, 0]]))
    >>> o = sparse.permute_dims(a, axes=(1, 0))
    >>> o.todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[0, 2],
           [1, 0]])
    """

    return x.transpose(axes=axes)


def max(x, /, *, axis=None, keepdims=False):
    """
    Calculates the maximum value of the input array ``x``.

    Parameters
    ----------
    x: array
        input array of a real-valued data type.
    axis: Optional[Union[int, Tuple[int, ...]]]
        axis or axes along which maximum values are computed.
        By default, the maximum value are computed over the entire array.
        If a tuple of integers, maximum values are computed over multiple axes. Default: ``None``.
    keepdims: bool
        If ``True``, the reduced axes (dimensions) are included in the result as singleton dimensions.
        Accordingly, the result is compatible with the input array.
        Otherwise, if ``False``, the reduced axes (dimensions) must not be included in the result. Default: ``False``.

    Returns
    -------
    out: array
        if the maximum value was computed over the entire array, a zero-dimensional array containing the maximum value.
        Otherwise, a non-zero-dimensional array containing the maximum values.
        The returned array has the same data type as ``x``.

    Special Cases
    -------------
    For floating-point operands, if ``x_i`` is ``NaN``, the maximum value is ``NaN`` (i.e., ``NaN`` values propagate).

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, 0]]))
    >>> o = sparse.max(a, axis=1)
    >>> o.todense()
    array([1, 2])
    """
    return x.max(axis=axis, keepdims=keepdims)


def mean(x, /, *, axis=None, keepdims=False, dtype=None):
    """
    Calculates the arithmetic mean of the input array ``x``.

    Parameters
    ----------
    x: array
        input array of  a real-valued floating-point data type.
    axis: Optional[Union[int, Tuple[int, ...]]]
        axis or axes along which arithmetic means must be computed.
        By default, the mean is computed over the entire array.
        If a tuple of integers, arithmetic means are computed over multiple axes. Default: ``None``.
    keepdims: bool
        if ``True``, the reduced axes (dimensions) are included in the result as singleton dimensions.
        Accordingly, the result is compatible is the input array.
        Otherwise, if ``False``, the reduced axes (dimensions) are not be included in the result. Default: ``False``.

    Returns
    -------
    out: array
        if the arithmetic mean was computed over the entire array, a zero-dimensional array with the arithmetic mean.
        Otherwise, a non-zero-dimensional array containing the arithmetic means.
        The returned array has the same data type as ``x``.

    Special Cases
    -------------
    Let ``N`` equal the number of elements over which to compute the arithmetic mean.
    If ``N`` is ``0``, the arithmetic mean is ``NaN``.
    If ``x_i`` is ``NaN``, the arithmetic mean is ``NaN`` (i.e., ``NaN`` values propagate).

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, 0]]))
    >>> o = sparse.mean(a, axis=1)
    >>> o.todense()
    array([0.5, 1. ])
    """

    return x.mean(axis=axis, keepdims=keepdims, dtype=dtype)


def min(x, /, *, axis=None, keepdims=False):
    """
    Calculates the minimum value of the input array ``x``.

    Parameters
    ----------
    x: array
        input array. Should have a real-valued data type.
    axis: Optional[Union[int, Tuple[int, ...]]]
        axis or axes along which minimum values are computed.
        By default, the minimum value must be computed over the entire array.
        If a tuple of integers, minimum values must be computed over multiple axes. Default: ``None``.
    keepdims: bool
        If ``True``, the reduced axes (dimensions) are included in the result as singleton dimensions.
        Accordingly, the result must be compatible with the input array.
        Otherwise, if ``False``, the reduced axes (dimensions) are not be included in the result. Default: ``False``.

    Returns
    -------
    out: array
        if the minimum value was computed over the entire array, a zero-dimensional array containing the minimum value.
        Otherwise, a non-zero-dimensional array containing the minimum values.
        The returned array must have the same data type as ``x``.

    Special Cases
    -------------
    For floating-point operands, if ``x_i`` is ``NaN``, the minimum value is ``NaN`` (i.e., ``NaN`` values propagate).

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, -1], [-2, 0]]))
    >>> o = sparse.min(a, axis=1)
    >>> o.todense()
    array([-1, -2])
    """
    return x.min(axis=axis, keepdims=keepdims)


def prod(x, /, *, axis=None, dtype=None, keepdims=False):
    """
    Calculates the product of input array ``x`` elements.

    Parameters
    ----------
    x: array
        input array of a numeric data type.
    axis: Optional[Union[int, Tuple[int, ...]]]
        axis or axes along which products is computed.
        By default, the product are computed over the entire array.
        If a tuple of integers, products are computed over multiple axes. Default: ``None``.

    dtype: Optional[dtype]
        data type of the returned array.
        If ``None``, the returned array has the same data type as ``x``, unless ``x`` has an integer
        data type supporting a smaller range of values than the default integer data type
        (e.g., ``x`` has an ``int16`` or ``uint32`` data type and the default integer data type is ``int64``).
        In those latter cases:

        -   if ``x`` has a signed integer data type (e.g., ``int16``), the returned array has the
            default integer data type.
        -   if ``x`` has an unsigned integer data type (e.g., ``uint16``), the returned array has an
            unsigned integer data type having the same number of bits as the default integer data type
            (e.g., if the default integer data type is ``int32``, the returned array must have a ``uint32`` data type).

        If the data type (either specified or resolved) differs from the data type of ``x``, the input array is
        cast to the specified data type before computing the sum (rationale: the ``dtype`` keyword argument is
        intended to help prevent overflows). Default: ``None``.

    keepdims: bool
        if ``True``, the reduced axes (dimensions) are included in the result as singleton dimensions.
        Accordingly, the result are compatible with the input array.
        Otherwise, if ``False``, the reduced axes (dimensions)  are not included in the result.
        Default: ``False``.

    Returns
    -------
    out: array
        if the product was computed over the entire array, a zero-dimensional array containing the product.
        Otherwise, a non-zero-dimensional array containing the products.
        The returned array has a data type as described by the ``dtype`` parameter above.

    Notes
    -----

    Special Cases
    -------------
    Let ``N`` equal the number of elements over which to compute the product.

    -   If ``N`` is ``0``, the product is `1` (i.e., the empty product).

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 2], [-1, 1]]))
    >>> o = sparse.prod(a, axis=1)
    >>> o.todense()
    array([ 0, -1])
    """
    return x.prod(axis=axis, keepdims=keepdims, dtype=dtype)


def std(x, /, *, axis=None, correction=0.0, keepdims=False):
    """
    Calculates the standard deviation of the input array ``x``.

    Parameters
    ----------
    x: array
        input array of a real-valued floating-point data type.
    axis: Optional[Union[int, Tuple[int, ...]]]
        axis or axes along which standard deviations are computed.
        By default, the standard deviation is computed over the entire array.
        If a tuple of integers, standard deviations are computed over multiple axes.
        Default: ``None``.
    correction: Union[int, float]
        degrees of freedom adjustment.
        Setting this parameter to a value other than ``0`` has the effect of adjusting the divisor
        during the calculation of the standard deviation according to ``N-c`` where ``N`` corresponds
        to the total number of elements over which the standard deviation is computed
        and ``c`` corresponds to the provided degrees of freedom adjustment. When computing the
        standard deviation of a population, setting this parameter to ``0`` is the standard choice
        (i.e., the provided array contains data constituting an entire population). When computing
        the corrected sample standard deviation, setting this parameter to ``1`` is the standard
        choice (i.e., the provided array contains data sampled from a larger population; this is
        commonly referred to as Bessel's correction). Default: ``0``.
    keepdims: bool
        if ``True``, the reduced axes (dimensions) are included in the result as singleton
        dimensions, and, accordingly, the result must be compatible with the input array.
        Otherwise, if ``False``, the reduced axes (dimensions) must not
        be included in the result. Default: ``False``.

    Returns
    -------
    out: array
        if the standard deviation was computed over the entire array, a zero-dimensional array containing
        the standard deviation; otherwise, a non-zero-dimensional array containing the standard deviations.
        The returned array has the same data type as ``x``.

    Special Cases
    -------------
    Let ``N`` equal the number of elements over which to compute the standard deviation.

    -   If ``N - correction`` is less than or equal to ``0``, the standard deviation is ``NaN``.
    -   If ``x_i`` is ``NaN``, the standard deviation is ``NaN`` (i.e., ``NaN`` values propagate).

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 2], [-1, 1]]))
    >>> o = sparse.std(a, axis=1)
    >>> o.todense()
    array([1., 1.])
    """
    return x.std(axis=axis, ddof=correction, keepdims=keepdims)


def sum(x, /, *, axis=None, dtype=None, keepdims=False):
    """
    Calculates the sum of the input array ``x``.

    Parameters
    ----------
    x: array
        input array of a numeric data type.
    axis: Optional[Union[int, Tuple[int, ...]]]
        axis or axes along which sums are computed.
        By default, the sum is computed over the entire array.
        If a tuple of integers, sums must are computed over multiple axes. Default: ``None``.

    dtype: Optional[dtype]
        data type of the returned array.
        If ``None``, the returned array has the same data type as ``x``, unless ``x`` has an integer data type
        supporting a smaller range of values than the default integer data type (e.g., ``x`` has an ``int16``
        or ``uint32`` data type and the default integer data type is ``int64``). In those latter cases:

        -   if ``x`` has a signed integer data type (e.g., ``int16``), the returned array has the default integer
            data type.
        -   if ``x`` has an unsigned integer data type (e.g., ``uint16``), the returned array has an unsigned integer
            data type having the same number of bits as the default integer data type (e.g., if the default integer
            data type is ``int32``, the returned array must have a ``uint32`` data type).

        If the data type (either specified or resolved) differs from the data type of ``x``, the input array is cast to
        the specified data type before computing the sum.
        Rationale: the ``dtype`` keyword argument is intended to help prevent overflows. Default: ``None``.
    keepdims: bool
        If ``True``, the reduced axes (dimensions) are included in the result as singleton dimensions.
        Accordingly, the result is compatible with the input array.
        Otherwise, if ``False``, the reduced axes (dimensions) are not included in the result. Default: ``False``.

    Returns
    -------
    out: array
        if the sum was computed over the entire array, a zero-dimensional array containing the sum.
        Otherwise, an array containing the sums.
        The returned array has the data type as described by the ``dtype`` parameter above.

    Special Cases
    -------------
    Let ``N`` equal the number of elements over which to compute the sum.

    -   If ``N`` is ``0``, the sum is ``0`` (i.e., the empty sum).

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, 0]]))
    >>> o = sparse.sum(a, axis=1)
    >>> o.todense()
    array([1, 2])
    """
    return x.sum(axis=axis, keepdims=keepdims, dtype=dtype)


def var(x, /, *, axis=None, correction=0.0, keepdims=False):
    """
    Calculates the variance of the input array ``x``.

    Parameters
    ----------
    x: array
        input array of a real-valued floating-point data type.
    axis: Optional[Union[int, Tuple[int, ...]]]
        axis or axes along which variances are computed.
        By default, the variance is computed over the entire array.
        If a tuple of integers, variances are computed over multiple axes. Default: ``None``.
    correction: Union[int, float]
        degrees of freedom adjustment. Setting this parameter to a value other than ``0``
        has the effect of adjusting the divisor during the calculation of the variance according to ``N-c``
        where ``N`` corresponds to the total number of elements over which the variance is computed and ``c``
        corresponds to the provided degrees of freedom adjustment.
        When computing the variance of a population, setting this parameter to ``0`` is the standard choice
        (i.e., the provided array contains data constituting an entire population).
        When computing the unbiased sample variance, setting this parameter to ``1`` is the standard choice
        (i.e., the provided array contains data sampled from a larger population; this is commonly referred
        to as Bessel's correction). Default: ``0``.
    keepdims: bool
        if ``True``, the reduced axes are included in the result as singleton dimensions, and,
        accordingly, the result is compatible with the input array.
        Otherwise, if ``False``, the reduced axes (dimensions) are not included in the result. Default: ``False``.

    Returns
    -------
    out: array
        if the variance was computed over the entire array, a zero-dimensional array containing the variance;
        otherwise, a non-zero-dimensional array containing the variances.
        The returned array must have the same data type as ``x``.

    Special Cases
    -------------
    Let ``N`` equal the number of elements over which to compute the variance.

    -   If ``N - correction`` is less than or equal to ``0``, the variance is ``NaN``.
    -   If ``x_i`` is ``NaN``, the variance is ``NaN`` (i.e., ``NaN`` values propagate).

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 2], [-1, 1]]))
    >>> o = sparse.var(a, axis=1)
    >>> o.todense()
    array([1., 1.])
    """
    return x.var(axis=axis, ddof=correction, keepdims=keepdims)


def abs(x, /):
    """
    Calculates the absolute value for each element ``x_i`` of the input array ``x``.

    For real-valued input arrays, the element-wise result has the same magnitude as the respective
    element in ``x`` but has positive sign.

    For complex floating-point operands, the complex absolute value is known as the norm, modulus, or
    magnitude and, for a complex number :math:`z = a + bj` is computed as

    $$
    operatorname{abs}(z) = sqrt{a^2 + b^2}
    $$

    Parameters
    ----------
    x: array
        input array of a numeric data type.

    Returns
    -------
    out: array
        an array containing the absolute value of each element in ``x``.
        If ``x`` has a real-valued data type, the returned array has the same data type as ``x``.
        If ``x`` has a complex floating-point data type, the returned array has a real-valued
        floating-point data type whose precision matches the precision of ``x``
        (e.g., if ``x`` is ``complex128``, then the returned array must has a ``float64`` data type).

    Special Cases
    -------------
    For real-valued floating-point operands,

    - If ``x_i`` is ``NaN``, the result is ``NaN``.
    - If ``x_i`` is ``-0``, the result is ``+0``.
    - If ``x_i`` is ``-infinity``, the result is ``+infinity``.

    For complex floating-point operands, let ``a = real(x_i)``, ``b = imag(x_i)``, and

    - If ``a`` is either ``+infinity`` or ``-infinity`` and ``b`` is any value (including ``NaN``),
    the result is ``+infinity``.
    - If ``a`` is any value (including ``NaN``) and ``b`` is either ``+infinity`` or ``-infinity``,
    the result is ``+infinity``.
    - If ``a`` is either ``+0`` or ``-0``, the result is equal to ``abs(b)``.
    - If ``b`` is either ``+0`` or ``-0``, the result is equal to ``abs(a)``.
    - If ``a`` is ``NaN`` and ``b`` is a finite number, the result is ``NaN``.
    - If ``a`` is a finite number and ``b`` is ``NaN``, the result is ``NaN``.
    - If ``a`` is ``NaN`` and ``b`` is ``NaN``, the result is ``NaN``.

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, -1], [-2, 0]]))
    >>> o = sparse.abs(a)
    >>> o.todense()
    array([[0, 1],
           [2, 0]])
    """

    return x.__abs__()


def reshape(x, /, shape, *, copy=None):
    """
    Reshapes an array without changing its data.

    Parameters
    ----------
    x: array
        input array to reshape.
    shape: Tuple[int, ...]
        a new shape compatible with the original shape. One shape dimension is allowed to be ``-1``.
        When a shape dimension is ``-1``, the corresponding output array shape dimension must be inferred
        from the length of the array and the remaining dimensions.
    copy: Optional[bool]
        whether or not to copy the input array.
        If ``True``, the function always copies.
        If ``False``, the function must never copies.
        If ``None``, the function avoids copying, if possible.
        Default: ``None``.

    Returns
    -------
    out: array
        an output array having the same data type and elements as ``x``.

    Raises
    ------
    ValueError
        If ``copy=False`` and a copy would be necessary, a ``ValueError``
        will be raised.

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, 0]]))
    >>> o = sparse.reshape(a, shape=(1, 4))
    >>> o.todense()
    array([[0, 1, 2, 0]])
    """
    return x.reshape(shape=shape)


@_check_device
def astype(x, dtype, /, *, copy=True, device=None):
    """
    Copies an array to a specified data type irrespective of type-promotion rules.

    Parameters
    ----------
    x: array
        array to cast.
    dtype: dtype
        desired data type.
    copy: bool
        specifies whether to copy an array when the specified ``dtype`` matches the data type
        of the input array ``x``. If ``True``, a newly allocated array is always returned.
        If ``False`` and the specified ``dtype`` matches the data type of the input array,
        the input array is returned; otherwise, a newly allocated array is returned.
        Default: ``True``.

    Notes
    -----

       - When casting a boolean input array to a real-valued data type, a value of ``True`` is cast
         to a real-valued number equal to ``1``, and a value of ``False`` must cast to a real-valued
         number equal to ``0``.

       - When casting a boolean input array to a complex floating-point data type, a value of ``True``
       is cast to a complex number equal to ``1 + 0j``, and a value of ``False`` is cast to a complex
       number equal to ``0 + 0j``.

       - When casting a real-valued input array to ``bool``, a value of ``0`` is cast to ``False``,
       and a non-zero value is cast to ``True``.

       - When casting a complex floating-point array to ``bool``, a value of ``0 + 0j`` is cast
       to ``False``, and all other values are cast to ``True``.

    Returns
    -------
    out: array
        an array having the specified data type. The returned array has the same shape as ``x``.

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, 0]]))
    >>> o = sparse.astype(a, "float32")
    >>> o.todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[0., 1.],
           [2., 0.]], dtype=float32)
    """
    return x.astype(dtype, copy=copy)


@_support_numpy
def squeeze(x, /, axis=None):
    """Remove singleton dimensions from array.

    Parameters
    ----------
    x : SparseArray
        Input array.
    axis : int or tuple[int, ...], optional
        The singleton axes to remove. By default all singleton axes are removed.

    Returns
    -------
    output : SparseArray
        Array with singleton dimensions removed.
    """
    return x.squeeze(axis=axis)


@_support_numpy
def broadcast_to(x, /, shape):
    """
    Broadcasts an array to a specified shape.

    Parameters
    ----------
    x: array
        array to broadcast.
    shape: Tuple[int, ...]
        Target shape. It must have at least as many dimensions as ``x``
        and be broadcast-compatible with it.
        If the array is incompatible with the specified shape, the function raises an exception.

    Returns
    -------
    out: array
        an array having a specified shape and having the same data type as ``x``.

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, 0]]))
    >>> o = sparse.broadcast_to(a, shape=(1, 2, 2))
    >>> o.todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[[0, 1],
            [2, 0]]])
    """
    return x.broadcast_to(shape)


def broadcast_arrays(*arrays):
    """
    Broadcasts one or more arrays against one another.

    Parameters
    ----------
    arrays: array
        an arbitrary number of to-be broadcasted arrays.

    Returns
    -------
    out: List[array]
        a list of broadcasted arrays. Each array has the same shape.
        Each array has the same dtype as its corresponding input array.

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1]]))
    >>> b = sparse.COO.from_numpy(np.array([[0], [2]]))
    >>> oa, ob = sparse.broadcast_arrays(a, b)
    >>> oa.todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[0, 1],
            [0, 1]])
    >>> ob.todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[0, 0],
            [2, 2]])
    """

    shape = np.broadcast_shapes(*[a.shape for a in arrays])
    arrays_out = []
    for a in arrays:
        if isinstance(a, np.generic | np.ndarray):
            a = COO.from_numpy(a)
        arrays_out.append(a.broadcast_to(shape))
    return tuple(arrays_out)


def equal(x1, x2, /):
    """
    Computes the truth value of ``x1_i == x2_i`` for each element ``x1_i`` of the input array ``x1``
    with the respective element ``x2_i`` of the input array ``x2``.

    Parameters
    ----------
    x1: array
        first input array. May have any data type.
    x2: array
        second input array. Must be compatible with ``x1``. May have any data type.

    Returns
    -------
    out: array
        an array containing the element-wise results. The returned array is of  data type of ``bool``.

    Special Cases
    -------------

    For real-valued floating-point operands,

    - If ``x1_i`` is ``NaN`` or ``x2_i`` is ``NaN``, the result is ``False``.
    - If ``x1_i`` is ``+infinity`` and ``x2_i`` is ``+infinity``, the result is ``True``.
    - If ``x1_i`` is ``-infinity`` and ``x2_i`` is ``-infinity``, the result is ``True``.
    - If ``x1_i`` is ``-0`` and ``x2_i`` is either ``+0`` or ``-0``, the result is ``True``.
    - If ``x1_i`` is ``+0`` and ``x2_i`` is either ``+0`` or ``-0``, the result is ``True``.
    - If ``x1_i`` is a finite number, ``x2_i`` is a finite number, and ``x1_i`` equals ``x2_i``, the result is ``True``.
    - In the remaining cases, the result is ``False``.

    For complex floating-point operands, let ``a = real(x1_i)``, ``b = imag(x1_i)``, ``c = real(x2_i)``,
    ``d = imag(x2_i)``, and

    - If ``a``, ``b``, ``c``, or ``d`` is ``NaN``, the result is ``False``.
    - In the remaining cases, the result is the logical AND of the equality comparison between the real values ``a``
        and ``c`` (real components) and between the real values ``b`` and ``d`` (imaginary components), as described
        above for real-valued  floating-point operands (i.e., ``a == c AND b == d``).

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, 0]]))
    >>> b = sparse.COO.from_numpy(np.array([[0, 1], [1, 0]]))
    >>> o = sparse.equal(a, b)  # doctest: +SKIP
    >>> o.todense()  # doctest: +SKIP
    array([[ True,  True],
           [ False,  True]])
    """
    return x1 == x2


@_support_numpy
def round(x, /, decimals=0, out=None):
    return x.round(decimals=decimals, out=out)


@_support_numpy
def isinf(x, /):
    """
    Tests each element ``x_i`` of the input array ``x`` to determine if equal to positive or negative infinity.

    Parameters
    ----------
    x: array
        input array of a numeric data type.

    Returns
    -------
    out: array
        an array containing test results. The returned array has a data type of ``bool``.

    Special Cases
    -------------

    For real-valued floating-point operands,

    - If ``x_i`` is either ``+infinity`` or ``-infinity``, the result is ``True``.
    - In the remaining cases, the result is ``False``.

    For complex floating-point operands, let ``a = real(x_i)``, ``b = imag(x_i)``, and

    - If ``a`` is either ``+infinity`` or ``-infinity`` and ``b`` is any value (including ``NaN``), the result
        is ``True``.
    - If ``a`` is either a finite number or ``NaN`` and ``b`` is either ``+infinity`` or ``-infinity``, the result
        is ``True``.
    - In the remaining cases, the result is ``False``.

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, np.inf]]))
    >>> o = sparse.isinf(a)  # doctest: +SKIP
    >>> o.todense()  # doctest: +SKIP
    array([[False, False],
           [False,  True]])
    """
    return x.isinf()


@_support_numpy
def isnan(x, /):
    """
    Tests each element ``x_i`` of the input array ``x`` to determine whether the element is ``NaN``.

    Parameters
    ----------
    x: array
        input array with a numeric data type.

    Returns
    -------
    out: array
        an array containing test results. The returned array has data type ``bool``.

    Notes
    -----

    For real-valued floating-point operands,

    - If ``x_i`` is ``NaN``, the result is ``True``.
    - In the remaining cases, the result is ``False``.

    For complex floating-point operands, let ``a = real(x_i)``, ``b = imag(x_i)``, and

    - If ``a`` or ``b`` is ``NaN``, the result is ``True``.
    - In the remaining cases, the result is ``False``.

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, np.nan]]))
    >>> o = sparse.isnan(a)
    >>> o.todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[False, False],
           [False,  True]])
    """

    return x.isnan()


def nonzero(x, /):
    """
    Returns the indices of the array elements which are non-zero.

    If ``x`` has a complex floating-point data type, non-zero elements are those elements having at least
    one component (real or imaginary) which is non-zero.

    If ``x`` has a boolean data type, non-zero elements are those elements which are equal to ``True``.

    Parameters
    ----------
    x: array
        input array having a positive rank.
        If ``x`` is zero-dimensional, the function raises an exception.

    Returns
    -------
    out: Tuple[array, ...]
        a tuple of ``k`` arrays, one for each dimension of ``x`` and each of size ``n`` (where ``n`` is
        the total number of non-zero elements), containing the indices of the non-zero elements in that
        dimension. The indices must are returned in row-major, C-style order.

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0, 1], [2, 0]]))
    >>> o = sparse.nonzero(a)
    >>> o
    (array([0, 1]), array([1, 0]))
    """
    return x.nonzero()


def imag(x, /):
    """
    Returns the imaginary component of a complex number for each element ``x_i`` of the input array ``x``.

    Parameters
    ----------
    x: array
        input array of a complex floating-point data type.

    Returns
    -------
    out: array
        an array containing the element-wise results.
        The returned array has a floating-point data type with the same floating-point precision as ``x``
        (e.g., if ``x`` is ``complex64``, the returned array has the floating-point data type ``float32``).

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0 + 1j, 2 + 0j], [0 + 0j, 3 + 1j]]))
    >>> o = sparse.imag(a)
    >>> o.todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[1., 0.],
           [0., 1.]])
    """
    return x.imag


def real(x, /):
    """
    Returns the real component of a complex number for each element ``x_i`` of the input array ``x``.

    Parameters
    ----------
    x: array
        input array of a complex floating-point data type.

    Returns
    -------
    out: array
        an array containing the element-wise results.
        The returned array has a floating-point data type with the same floating-point precision as ``x``
        (e.g., if ``x`` is ``complex64``, the returned array has the floating-point data type ``float32``).

    Examples
    --------
    >>> a = sparse.COO.from_numpy(np.array([[0 + 1j, 2 + 0j], [0 + 0j, 3 + 1j]]))
    >>> o = sparse.real(a)
    >>> o.todense()  # doctest: +NORMALIZE_WHITESPACE
    array([[0., 2.],
           [0., 3.]])
    """
    return x.real


def vecdot(x1, x2, /, *, axis=-1):
    """
    Computes the (vector) dot product of two arrays.

    Parameters
    ----------
    x1, x2 : array_like
        Input sparse arrays
    axis : int
        The axis to reduce over.

    Returns
    -------
    out : Union[SparseArray, numpy.ndarray]
        Sparse or 0-D array containing dot product.
    """
    ndmin = builtins.min((x1.ndim, x2.ndim))
    if not (-ndmin <= axis < ndmin) or x1.shape[axis] != x2.shape[axis]:
        raise ValueError("Shapes must match along `axis`.")

    if np.issubdtype(x1.dtype, np.complexfloating):
        x1 = np.conjugate(x1)

    return np.sum(x1 * x2, axis=axis, dtype=np.result_type(x1, x2))


def repeat(a, repeats, axis: int | None = None):
    """
    Repeat each element of an array after themselves

    Parameters
    ----------
    a : SparseArray
        Input sparse arrays
    repeats : int
        The number of repetitions for each element.
        (Uneven repeats are not yet Implemented.)
    axis : int, optional
        The axis along which to repeat values. Returns a flattened sparse array if not specified.

    Returns
    -------
    out : SparseArray
        A sparse array which has the same shape as a, except along the given axis.
    """
    if not isinstance(a, SparseArray):
        raise TypeError("`a` must be a SparseArray.")

    if not isinstance(repeats, int):
        raise ValueError("`repeats` must be an integer, uneven repeats are not yet Implemented.")
    axes = list(range(a.ndim))
    new_shape = list(a.shape)
    axis_is_none = False
    if axis is None:
        a = a.reshape(-1)
        axis = 0
        axis_is_none = True
    if axis < 0:
        axis = a.ndim + axis
    axes[a.ndim - 1], axes[axis] = axes[axis], axes[a.ndim - 1]
    new_shape[axis] *= repeats
    a = expand_dims(a, axis=axis + 1)
    shape_to_broadcast = a.shape[: axis + 1] + (a.shape[axis + 1] * repeats,) + a.shape[axis + 2 :]
    a = broadcast_to(a, shape_to_broadcast)
    if not axis_is_none:
        return a.reshape(new_shape)
    return a.reshape(new_shape).flatten()


def tile(a, reps):
    """
    Constructs an array by tiling an input array.

    Parameters
    ----------
    a : SparseArray
        Input sparse arrays.
    reps : int or tuple[int, ...]
        The number of repetitions for each dimension.
        If an integer, the same number of repetitions is applied to all dimensions.

    Returns
    -------
    out : SparseArray
        A tiled output array.
    """
    if not isinstance(a, SparseArray):
        a = as_coo(a)

    if isinstance(reps, int):
        reps = (reps,)
    reps = tuple(reps)

    if a.ndim == 0:
        a = a.reshape((1,))

    if len(reps) < a.ndim:
        reps = (1,) * (a.ndim - len(reps)) + reps
    elif len(reps) > a.ndim:
        a = a.reshape((1,) * (len(reps) - a.ndim) + a.shape)

    shape = a.shape
    ndim = len(reps)
    a = a.reshape(tuple(np.column_stack(([1] * ndim, shape)).reshape(-1)))
    a = a.broadcast_to(tuple(np.column_stack((reps, shape)).reshape(-1)))
    return a.reshape(tuple(np.multiply(reps, shape)))


def unstack(x, axis=0):
    """
    Splits an array into a sequence of arrays along the given axis.

    Parameters
    ----------
    x : SparseArray
        Input sparse arrays.
    axis : int
        Axis along which the array will be split

    Returns
    -------
    out : Tuple[SparseArray,...]
        Tuple of slices along the given dimension. All the arrays have the same shape.
    """
    ndim = x.ndim

    if not (-ndim <= axis < ndim):
        raise ValueError(f"axis must be in range [-{ndim}, {ndim}), got {axis}")

    if not isinstance(x, SparseArray):
        raise TypeError("`a` must be a SparseArray.")

    if axis < 0:
        axis = ndim + axis
    new_order = (axis,) + tuple(i for i in range(ndim) if i != axis)
    x = x.transpose(new_order)
    return (*x,)


def diff(x, axis=-1, n=1, prepend=None, append=None):
    """
    Calculates the n-th discrete difference along the given axis.

    Parameters
    ----------
    x : SparseArray
        Input sparse arrays.
    n : int
        The number of times values are differenced. Default: 1.
    axis : int
        The axis along which the difference is taken. Default: -1.

    Returns
    -------
    out : SparseArray
        An array containing the n-th discrete difference along the given axis.
    """
    if not isinstance(x, SparseArray):
        raise TypeError("`x` must be a SparseArray.")

    if axis < 0:
        axis = x.ndim + axis
    if prepend is not None:
        x = concatenate([prepend, x], axis=axis)
    if append is not None:
        x = concatenate([x, append], axis=axis)
    result = x
    for _ in range(n):
        result = result[(slice(None),) * axis + (slice(1, None),)] - result[(slice(None),) * axis + (slice(None, -1),)]
    return result


def interp(x, xp, fp, left=None, right=None, period=None):
    """
    An implementation of ``numpy.interp`` for sparse arrays.

    Thanks to the function dispatch of numpy, this enables interpolation on sparse arrays
    using the numpy universal function. This function effectively wraps ``np.interp`` by
    calling it on the array data and the fill value. See the numpy documentation for
    details on the parameters.

    Parameters
    ----------
    x : SparseArray
        The x-coordinates at which to evaluate the interpolated values.
    xp : 1-D sequence or SparseArray
        The x-coordinates of the data points.
    fp : 1-D sequence or SparseArray
        The y-coordinates of the data points, same length as ``xp``.
    left : float or complex, optional
        Value to return for ``x < xp[0]``, default is ``fp[0]``.
    right : float or complex, optional
        Value to return for ``x > xp[-1]``, default is ``fp[-1]``.
    period : None or float, optional
        A period for the x-coordinates.

    Returns
    -------
    out : SparseArray
        The interpolated values, same shape as x.

    See Also
    --------
    https://numpy.org/doc/stable/reference/generated/numpy.interp.html

    Examples
    --------
    When interpolating a sparse array, its data and the fill value are interpolated. The
    returned array is pruned. Therefore, the fill value and the number of nonzero
    elements might change.

    >>> import numpy as np
    >>> xp = [1, 2, 3]
    >>> fp = [3, 2, 0]
    >>> y = np.interp(sparse.COO.from_numpy(np.array([0, 1, 1.5, 2.72, 3.14])), xp, fp)
    >>> y.todense()
    array([3.  , 3.  , 2.5 , 0.56, 0.  ])
    >>> y.fill_value
    np.float64(3.0)
    >>> y.nnz
    3
    """
    from ._compressed import GCXS
    from ._coo import COO
    from ._dok import DOK

    # Densify sparse interpolants
    if isinstance(xp, SparseArray):
        xp = xp.todense()
    if isinstance(fp, SparseArray):
        fp = fp.todense()

    def interp_func(xx):
        return np.interp(xx, xp, fp, left=left, right=right, period=period)

    # Shortcut for dense arrays
    if not isinstance(x, SparseArray):
        return interp_func(x)

    # Define output type
    out_kwargs = {}
    out_type = COO
    if isinstance(x, GCXS):
        out_type = GCXS
        out_kwargs["compressed_axes"] = x.compressed_axes
    elif isinstance(x, DOK):
        out_type = DOK

    # Perform interpolation on sparse object
    arr = as_coo(x)
    data = interp_func(arr.data)
    fill_value = interp_func(arr.fill_value)
    return COO(data=data, coords=arr.coords, shape=arr.shape, fill_value=fill_value, prune=True).asformat(
        out_type, **out_kwargs
    )
