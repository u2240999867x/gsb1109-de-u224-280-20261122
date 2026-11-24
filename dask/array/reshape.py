from __future__ import annotations

import math
from functools import reduce
from itertools import product
from operator import index, mul

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array.core import Array
from dask.array.utils import meta_from_array
from dask.base import tokenize
from dask.core import flatten, quote
from dask.highlevelgraph import HighLevelGraph
from dask.utils import M

_not_implemented_message = """
Dask's reshape only supports operations that merge or split existing dimensions
evenly. For example:

>>> x = da.ones((6, 5, 4), chunks=(3, 2, 2))
>>> x.reshape((3, 2, 5, 4))  # supported, splits 6 into 3 & 2
>>> x.reshape((30, 4))       # supported, merges 6 & 5 into 30
>>> x.reshape((4, 5, 6))     # unsupported, existing dimensions split unevenly

To work around this you may call reshape in multiple passes, or (if your data
is small enough) call ``compute`` first and handle reshaping in ``numpy``
directly.
"""


def reshape_rechunk(inshape, outshape, inchunks, disallow_dimension_expansion=False):
    assert all(isinstance(c, tuple) for c in inchunks)
    ii = len(inshape) - 1
    oi = len(outshape) - 1
    result_inchunks = [None for i in range(len(inshape))]
    result_outchunks = [None for i in range(len(outshape))]
    mapper_in, one_dimensions = {}, []

    while ii >= 0 or oi >= 0:
        if inshape[ii] == outshape[oi]:
            result_inchunks[ii] = inchunks[ii]
            result_outchunks[oi] = inchunks[ii]
            mapper_in[ii] = oi
            ii -= 1
            oi -= 1
            continue
        din = inshape[ii]
        dout = outshape[oi]
        if din == 1:
            result_inchunks[ii] = (1,)
            ii -= 1
        elif dout == 1:
            result_outchunks[oi] = (1,)
            one_dimensions.append(oi)
            oi -= 1
        elif din < dout:  # (4, 4, 4) -> (64,)
            ileft = ii - 1
            mapper_in[ii] = oi
            while (
                ileft >= 0 and reduce(mul, inshape[ileft : ii + 1]) < dout
            ):  # 4 < 64, 4*4 < 64, 4*4*4 == 64
                mapper_in[ileft] = oi
                ileft -= 1

            mapper_in[ileft] = oi
            if reduce(mul, inshape[ileft : ii + 1]) != dout:
                raise NotImplementedError(_not_implemented_message)
            # Special case to avoid intermediate rechunking:
            # When all the lower axis are completely chunked (chunksize=1) then
            # we're simply moving around blocks.
            if all(len(inchunks[i]) == inshape[i] for i in range(ii)):
                for i in range(ii + 1):
                    result_inchunks[i] = inchunks[i]
                result_outchunks[oi] = inchunks[ii] * math.prod(
                    map(len, inchunks[ileft:ii])
                )
            else:
                for i in range(ileft + 1, ii + 1):  # need single-shape dimensions
                    result_inchunks[i] = (inshape[i],)  # chunks[i] = (4,)

                chunk_reduction = reduce(mul, map(len, inchunks[ileft + 1 : ii + 1]))
                result_inchunks[ileft] = expand_tuple(inchunks[ileft], chunk_reduction)

                max_in_chunk = _cal_max_chunk_size(inchunks, ileft, ii)
                result_inchunks = _smooth_chunks(
                    ileft, ii, max_in_chunk, result_inchunks
                )
                # Build cross product of result_inchunks[ileft:ii+1]
                result_outchunks[oi] = _calc_lower_dimension_chunks(
                    result_inchunks, ileft, ii
                )

            oi -= 1
            ii = ileft - 1
        elif din > dout:  # (64,) -> (4, 4, 4)
            if disallow_dimension_expansion:
                raise NotImplementedError(
                    "reshape_blockwise not implemented for expanding dimensions without passing chunk hints."
                )
            oleft = oi - 1
            while oleft >= 0 and reduce(mul, outshape[oleft : oi + 1]) < din:
                oleft -= 1
            if reduce(mul, outshape[oleft : oi + 1]) != din:
                raise NotImplementedError(_not_implemented_message)
            # TODO: don't coalesce shapes unnecessarily
            cs = reduce(mul, outshape[oleft + 1 : oi + 1])

            result_inchunks[ii] = contract_tuple(inchunks[ii], cs)  # (16, 16, 16, 16)

            for i in range(oleft + 1, oi + 1):
                result_outchunks[i] = (outshape[i],)

            result_outchunks[oleft] = tuple(c // cs for c in result_inchunks[ii])

            max_in_chunk = _cal_max_chunk_size(inchunks, ii, ii)
            result_outchunks = _smooth_chunks(oleft, oi, max_in_chunk, result_outchunks)
            # Build cross product of result_outchunks[oleft:oi+1]
            result_inchunks[ii] = _calc_lower_dimension_chunks(
                result_outchunks, oleft, oi
            )
            oi = oleft - 1
            ii -= 1

    return tuple(result_inchunks), tuple(result_outchunks), mapper_in, one_dimensions


def _calc_lower_dimension_chunks(chunks, start, stop):
    # We need the lower dimension chunks to match what the higher dimension chunks
    # can be combined to, i.e. multiply the different dimensions
    return tuple(
        map(
            lambda x: reduce(mul, x),
            product(*chunks[start : stop + 1]),
        )
    )


def _smooth_chunks(ileft, ii, max_in_chunk, result_inchunks):
    # The previous step squashed the whole dimension into a single
    # chunk for ileft + 1 (and potentially combined too many elements
    # into a single chunk for ileft as well). We split up the single
    # chunk into multiple chunks to match the max_in_chunk to keep
    # chunksizes consistent:
    # ((1, 1), (200)) -> ((1, 1), (20, ) * 10) for max_in_chunk = 20
    # It's important to ensure that all dimensions before the dimension
    # we adjust have all-1 chunks to respect C contiguous arrays
    # during the reshaping
    # Example:
    # Assume arr = da.from_array(np.arange(0, 12).reshape(4, 3), chunks=(2, 3))
    # Reshaping to arr.reshape(-1, ) will return
    # [ 0  1  2  3  4  5  6  7  8  9 10 11]
    # The first dimension of the reshaped axis are the chunks with length 2
    # Assume we split the second dimension into (2, 1), i.e. setting the chunks to
    # ((2, 2), (2, 1)) and the output chunks to ((4, 2, 4, 2), )
    # In this case, the individual chunks do not hold a contiguous sequence.
    # For example, the first chunk is [[0, 1], [3, 4]].
    # Then, the result will be different because we first reshape the individual,
    # non-contiguous chunks before concatenating them:
    # [ 0  1  3  4  2  5  6  7  9 10  8 11]
    # This is equivalent to
    # arr = np.arange(0, 12).reshape(4, 3)
    # np.concatenate(list(map(lambda x: x.reshape(-1), [arr[:2, :2], arr[:2, 2:], arr[2:, :2], arr[2:, 2:]])))

    ileft_orig = ileft
    max_result_in_chunk = _cal_max_chunk_size(result_inchunks, ileft, ii)
    if max_in_chunk == max_result_in_chunk:
        # reshaping doesn't mess up
        return result_inchunks

    while all(x == 1 for x in result_inchunks[ileft]):
        # Find the first dimension where we can split chunks
        ileft += 1

    if ileft < ii + 1:
        factor = math.ceil(max_result_in_chunk / max_in_chunk)
        result_in_chunk = result_inchunks[ileft]

        if len(result_in_chunk) == 1:
            # This is a trivial case, when we arrive here is the chunk we are
            # splitting the same length as the whole dimension and all previous
            # chunks that are reshaped into the same dimension are all-one.
            # So we can split this dimension.
            elem = result_in_chunk[0]
            factor = min(factor, elem)
            ceil_elem = math.ceil(elem / factor)
            new_inchunk = [ceil_elem] * factor
            for i in range(ceil_elem * factor - elem):
                new_inchunk[i] -= 1
            result_inchunks[ileft] = tuple(new_inchunk)

            if all(x == 1 for x in new_inchunk) and ileft < ii:
                # might have to do another round
                return _smooth_chunks(ileft_orig, ii, max_in_chunk, result_inchunks)
        else:
            # We are now in the more complicated case. The first dimension in the set
            # of dimensions to squash has non-ones and our max chunk is bigger than
            # what we want. We need to split the non-ones into multiple chunks along
            # this axis.
            other_max_chunk = max_result_in_chunk // max(result_inchunks[ileft])
            result_in = []

            for elem_in in result_in_chunk:
                if elem_in * other_max_chunk <= max_in_chunk:
                    result_in.append(elem_in)
                    continue

                factor = math.ceil(elem_in * other_max_chunk / max_in_chunk)
                ceil_elem = math.ceil(elem_in / factor)
                new_in_chunk = [ceil_elem] * math.ceil(factor)
                for i in range(ceil_elem * factor - elem_in):
                    new_in_chunk[i] -= 1
                result_in.extend(new_in_chunk)

            result_inchunks[ileft] = tuple(result_in)
    return result_inchunks


def _cal_max_chunk_size(chunks, start, stop):
    return int(
        reduce(
            mul,
            [max(chunks[axis]) for axis in range(start, stop + 1)],
        )
    )


def expand_tuple(chunks, factor):
    """

    >>> expand_tuple((2, 4), 2)
    (1, 1, 2, 2)

    >>> expand_tuple((2, 4), 3)
    (1, 1, 1, 1, 2)

    >>> expand_tuple((3, 4), 2)
    (1, 2, 2, 2)

    >>> expand_tuple((7, 4), 3)
    (2, 2, 3, 1, 1, 2)
    """
    if factor == 1:
        return chunks

    out = []
    for c in chunks:
        x = c
        part = max(x / factor, 1)
        while x >= 2 * part:
            out.append(int(part))
            x -= int(part)
        if x:
            out.append(x)
    assert sum(chunks) == sum(out)
    return tuple(out)


def contract_tuple(chunks, factor):
    """Return simple chunks tuple such that factor divides all elements

    Examples
    --------

    >>> contract_tuple((2, 2, 8, 4), 4)
    (4, 8, 4)
    """
    assert sum(chunks) % factor == 0

    out = []
    residual = 0
    for chunk in chunks:
        chunk += residual
        div = chunk // factor
        residual = chunk % factor
        good = factor * div
        if good:
            out.append(good)
    return tuple(out)


def reshape(x, shape, merge_chunks=True, limit=None):
    """Reshape array to new shape

    Parameters
    ----------
    shape : int or tuple of ints
        The new shape should be compatible with the original shape. If
        an integer, then the result will be a 1-D array of that length.
        One shape dimension can be -1. In this case, the value is
        inferred from the length of the array and remaining dimensions.
    merge_chunks : bool, default True
        Whether to merge chunks using the logic in :meth:`dask.array.rechunk`
        when communication is necessary given the input array chunking and
        the output shape. With ``merge_chunks==False``, the input array will
        be rechunked to a chunksize of 1, which can create very many tasks.
    limit: int (optional)
        The maximum block size to target in bytes. If no limit is provided,
        it defaults to using the ``array.chunk-size`` Dask config value.

    Notes
    -----
    This is a parallelized version of the ``np.reshape`` function with the
    following limitations:

    1.  It assumes that the array is stored in `row-major order`_
    2.  It only allows for reshapings that collapse or merge dimensions like
        ``(1, 2, 3, 4) -> (1, 6, 4)`` or ``(64,) -> (4, 4, 4)``

    .. _`row-major order`: https://en.wikipedia.org/wiki/Row-_and_column-major_order

    When communication is necessary this algorithm depends on the logic within
    rechunk.  It endeavors to keep chunk sizes roughly the same when possible.

    A 1-D array with an unknown chunk length (for example one produced by
    :meth:`dask.dataframe.DataFrame.to_dask_array`) can be lazily framed into
    ``(-1, width)``.  The result keeps one block per input block: a frame is
    attributed to the block containing its last record, blocks that complete no
    frame stay empty, and the row counts only become known at execution time.
    If the total number of records is not divisible by ``width`` an error is
    raised when the result is computed.  See
    :func:`dask.array.reshape_lazy_frames` for details.

    See :ref:`array-chunks.reshaping` for a discussion the tradeoffs of
    ``merge_chunks``.

    See Also
    --------
    dask.array.rechunk
    dask.array.reshape_lazy_frames
    numpy.reshape
    """
    # Sanitize inputs, look for -1 in shape
    from dask.array.slicing import sanitize_index

    shape = tuple(sanitize_index(s) for s in shape)
    known_sizes = [s for s in shape if s != -1]
    if len(known_sizes) < len(shape):
        if len(shape) - len(known_sizes) > 1:
            raise ValueError("can only specify one unknown dimension")
        # Fastpath for x.reshape(-1) on 1D arrays, allows unknown shape in x
        # for this case only.
        if len(shape) == 1 and x.ndim == 1:
            return x
        # Lazy framing of an unknown-length 1-D array into (-1, width):
        # the number of rows stays unknown until execution, but every output
        # block can already be mapped to the input block (file) it belongs to.
        if (
            len(shape) == 2
            and x.ndim == 1
            and shape[0] == -1
            and not math.isnan(shape[1])
            and math.isnan(x.shape[0])
        ):
            return reshape_lazy_frames(x, index(shape[1]))
        missing_size = sanitize_index(x.size / reduce(mul, known_sizes, 1))
        shape = tuple(missing_size if s == -1 else s for s in shape)

    _sanity_checks(x, shape)

    if x.shape == shape:
        return x

    meta = meta_from_array(x, len(shape))

    name = "reshape-" + tokenize(x, shape)

    if x.npartitions == 1:
        key = next(flatten(x.__dask_keys__()))
        new_key = (name,) + (0,) * len(shape)
        dsk = {new_key: Task(new_key, M.reshape, TaskRef(key), shape)}
        chunks = tuple((d,) for d in shape)
        graph = HighLevelGraph.from_collections(name, dsk, dependencies=[x])
        return Array(graph, name, chunks, meta=meta)

    # Logic or how to rechunk
    din = len(x.shape)
    dout = len(shape)
    if not merge_chunks and din > dout:
        x = x.rechunk(dict.fromkeys(range(din - dout), 1))

    inchunks, outchunks, _, _ = reshape_rechunk(x.shape, shape, x.chunks)
    x2 = x.rechunk(inchunks)

    # Construct graph
    in_keys = list(product([x2.name], *[range(len(c)) for c in inchunks]))
    out_keys = list(product([name], *[range(len(c)) for c in outchunks]))
    shapes = list(product(*outchunks))
    dsk = {
        a: Task(a, M.reshape, TaskRef(b), shape)
        for a, b, shape in zip(out_keys, in_keys, shapes)
    }

    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[x2])
    return Array(graph, name, outchunks, meta=meta)


def _sanity_checks(x, shape):
    if np.isnan(sum(x.shape)):
        raise ValueError(
            f"Array chunk size or shape is unknown. shape: {x.shape}\n\n"
            "Possible solution with x.compute_chunk_sizes()"
        )

    if reduce(mul, shape, 1) != x.size:
        raise ValueError("total size of new array must be unchanged")


def reshape_blockwise(
    x: Array,
    shape: int | tuple[int, ...],
    chunks: tuple[tuple[int, ...], ...] | None = None,
) -> Array:
    """Blockwise-reshape into a new shape.

    The regular reshape operation in Dask preserves C-ordering in the array
    which requires a rechunking for most reshaping operations, making the
    computation relatively expensive.

    Blockwise-reshape reshapes every block into the new shape and concatenates
    the results. This is a trivial blockwise computation but will return the
    result in a different order than NumPy. This is a good solution for
    subsequent operations that don't rely on the order.

    Parameters
    ----------
    x: Array
        The input array to reshape.
    shape : int or tuple of ints
        The new shape should be compatible with the original shape. If
        an integer, then the result will be a 1-D array of that length.
        One shape dimension can be -1. In this case, the value is
        inferred from the length of the array and remaining dimensions.
    chunks: tuple of ints, default None
        The chunk sizes for every chunk in the output array. Dask will expand
        the chunks per dimension into the cross product of chunks for every
        chunk in the array.

        An error is raised if chunks is given and the number of dimensions
        decreases.

        .. note::
            This information is required if the number of dimensions is increased.
            Dask cannot infer the output chunks in this case. The keyword is ignored
            if the number of dimensions is reduced.

    Notes
    -----
    This is a parallelized version of the ``np.reshape`` function with the
    following limitations:

    1.  It does not return elements in the same order as NumPy would
    2.  It only allows for reshapings that collapse like
        ``(1, 2, 3, 4) -> (1, 6, 4)``

    Examples
    --------
    >>> import dask.array as da
    >>> import numpy as np
    >>> x = da.from_array(np.arange(0, 27).reshape(3, 3, 3), chunks=(3, 2, (2, 1)))
    >>> result = reshape_blockwise(x, (3, 9))
    >>> result.chunks
    ((3,), (4, 2, 2, 1))

    The resulting chunks are calculated automatically to match the new shape.

    >>> result.compute()
    array([[ 0,  1,  3,  4,  2,  5,  6,  7,  8],
           [ 9, 10, 12, 13, 11, 14, 15, 16, 17],
           [18, 19, 21, 22, 20, 23, 24, 25, 26]])

    >>> result = reshape_blockwise(result, (3, 3, 3), chunks=x.chunks)
    >>> result.chunks
    ((3,), (2, 1), (2, 1))

    The resulting chunks are taken from the input. Chaining the reshape operation
    together like this reverts the previous reshaping operation that reduces the
    number of dimensions.

    >>> result.compute()
    array([[[ 0,  1,  2],
            [ 3,  4,  5],
            [ 6,  7,  8]],
    <BLANKLINE>
           [[ 9, 10, 11],
            [12, 13, 14],
            [15, 16, 17]],
    <BLANKLINE>
           [[18, 19, 20],
            [21, 22, 23],
            [24, 25, 26]]])
    """
    if shape in [-1, (-1,)]:
        shape = (reduce(mul, x.shape),)

    if not isinstance(shape, tuple):
        shape = (shape,)

    _sanity_checks(x, shape)

    if len(shape) == x.ndim and shape == x.shape:
        return Array(x.dask, x.name, x.chunks, meta=x)

    outname = "reshape-blockwise-" + tokenize(x, shape)
    chunk_tuples = list(product(*(range(len(c)) for i, c in enumerate(x.chunks))))

    if len(shape) > x.ndim:
        if chunks is None:
            raise TypeError("Need to specify chunks if expanding dimensions.")
        out_shapes = list(product(*(c for c in chunks)))
        out_chunk_tuples = list(product(*(range(len(c)) for c in chunks)))
        in_shapes = list(product(*(c for c in x.chunks)))
        non_matching_chunks = [
            (i, in_c, out_c)
            for i, (in_c, out_c) in enumerate(zip(in_shapes, out_shapes))
            if math.prod(in_c) != math.prod(out_c)
        ]
        if non_matching_chunks:
            raise ValueError(
                f"Chunk sizes do not match for the following chunks: "
                f"{[x[0] for x in non_matching_chunks[:5]]}. \n"
                f"The corresponding chunksizes are: {[x[1:] for x in non_matching_chunks[:5]]}. "
                f"(restricted to first 5 entries)."
            )

        dsk = {
            (outname,)
            + tuple(chunk_out): Task(
                (outname,) + tuple(chunk_out),
                _reshape_blockwise,
                TaskRef((x.name,) + tuple(chunk_in)),
                shape,
            )
            for chunk_in, chunk_out, shape in zip(
                chunk_tuples, out_chunk_tuples, out_shapes
            )
        }

        graph = HighLevelGraph.from_collections(outname, dsk, dependencies=[x])  # type: ignore[arg-type]
        return Array(graph, outname, chunks, meta=x._meta)

    if chunks is not None:
        raise ValueError(
            "Setting chunks is not allowed when reducing the number of dimensions."
        )

    _, _, mapper_in, one_dimensions = reshape_rechunk(
        x.shape, shape, x.chunks, disallow_dimension_expansion=True
    )

    # Convert input chunks to output chunks
    out_shapes = [
        _convert_to_shape(c, mapper_in, one_dimensions)
        for c in list(product(*(c for c in x.chunks)))
    ]
    # Calculate the number of chunks for each dimension in the output
    nr_out_chunks = _convert_to_shape(
        tuple(map(len, x.chunks)), mapper_in, one_dimensions
    )
    # Create output chunk tuples for the graph
    out_chunk_tuples = list(product(*(range(c) for c in nr_out_chunks)))

    dsk = {
        (outname,)
        + tuple(chunk_out): Task(
            (outname,) + tuple(chunk_out),
            _reshape_blockwise,
            TaskRef((x.name,) + tuple(chunk_in)),
            shape,
        )
        for chunk_in, chunk_out, shape in zip(
            chunk_tuples, out_chunk_tuples, out_shapes
        )
    }

    # Calculating the output chunks is a bit tricky. We have all the output chunk
    # tuples, but now we have to create the source of the cross product. We can
    # iterate over the tuples to extract all elements along a single dimension.
    output_chunks = []
    ctr = 1
    for i, nr_chunks_dim in enumerate(reversed(nr_out_chunks)):
        dimension_chunks = []
        for elem in range(nr_chunks_dim):
            dimension_chunks.append(out_shapes[elem * ctr][len(nr_out_chunks) - i - 1])
        output_chunks.append(tuple(dimension_chunks))
        ctr *= nr_chunks_dim

    graph = HighLevelGraph.from_collections(outname, dsk, dependencies=[x])  # type: ignore[arg-type]
    return Array(graph, outname, tuple(reversed(output_chunks)), meta=x._meta)


def _reshape_blockwise(arr, shape):
    return arr.reshape(shape)


def _convert_to_shape(
    shape: tuple[int, ...], mapper_in: dict[int, int], one_dims: list[int]
):
    # Used to map the input dimensions onto output dimensions. The mapper tracks
    # which input dimensions are mapped onto what output dimensions.
    # i.e. {0: 0, 1:0, 2: 1} means that dimensions 0 and 1 are mapped into 0 and 2 into 1
    output_shape: list[list[int]] = [[]] * (
        len(set(mapper_in.values())) + len(one_dims)
    )
    output_shape = list(map(lambda x: x.copy(), output_shape))
    for i in one_dims:
        output_shape[i] = [1]

    for k, v in mapper_in.items():
        output_shape[v].append(shape[k])

    return tuple(map(lambda x: reduce(mul, x), output_shape))


def _lazy_frame_local_state(values, width):
    """Per-file bounded state: ``(record count, last up to width-1 values)``.

    The raw tail is truncated without any frame alignment so that states of
    adjacent ranges can be combined associatively.
    """
    values = np.asarray(values).reshape(-1)
    tail = values[-(width - 1) :] if width > 1 else values[:0]
    return int(values.size), tail


def _lazy_frame_combine_state(left, right, width):
    """Associative combine of two bounded ``(count, tail)`` states.

    The merged tail keeps at most ``width - 1`` raw values, so no task ever
    forwards the full record history.
    """
    left_count, left_tail = left
    right_count, right_tail = right
    count = left_count + right_count
    if width == 1:
        tail = np.empty((0,), dtype=left_tail.dtype)
    else:
        tail = np.concatenate([np.asarray(left_tail), np.asarray(right_tail)])[
            -(width - 1) :
        ]
    return int(count), tail


def _lazy_frame_validate(prefix_state, width):
    """Fail at execution time if the batch leaves an incomplete final frame."""
    total_count = prefix_state[0]
    if total_count % width:
        raise ValueError(
            f"cannot reshape array into shape (-1, {width}): the total number "
            f"of records ({total_count}) is not divisible by the frame width "
            f"{width}"
        )
    return True


def _lazy_frame_block(prefix_state, values, _validated, width):
    """Build one file's output block from its data and the prefix raw tail.

    ``prefix_state`` is ``(count, tail)`` for all earlier files.  The absolute
    frame boundary determines how many trailing prefix values belong to the
    frame that may finish inside this file; every returned frame contains at
    least one record of this file, so its last record lives here.
    """
    prefix_count, prefix_tail = prefix_state
    values = np.asarray(values).reshape(-1)
    carry = prefix_count % width
    aligned_tail = np.asarray(prefix_tail)[-carry:] if carry else prefix_tail[:0]
    joined = np.concatenate([aligned_tail, values])
    n_full = joined.size // width
    return joined[: n_full * width].reshape(n_full, width)


def reshape_lazy_frames(x: Array, width: int) -> Array:
    """Reshape an unknown-length 1-D array into ``(-1, width)`` frames.

    The input is typically produced by ``DataFrame.to_dask_array()`` where each
    input block corresponds to one file and only its metadata is known.  The
    result keeps one output block per input block: a completed frame is
    attributed to the block (file) containing its last record, and blocks that
    complete no frame remain present as empty blocks.  The block-to-file
    mapping and the number of blocks are known before execution even though the
    row count of every block is not.

    Pending records move through a balanced parallel-prefix tree whose depth is
    ``O(log(number of files))``.  A per-file task only ever sees its own
    records plus the bounded external tail (fewer than ``width`` values) needed
    to finish frames crossing file boundaries; the full history is never
    forwarded.  If the total number of records is not divisible by ``width`` an
    error is raised at execution time, including when only a subset of blocks
    is requested, because every output block depends on a shared validation
    task.
    """
    try:
        width = index(width)
    except TypeError:
        raise TypeError("frame width must be a positive integer") from None
    if width <= 0:
        raise ValueError("frame width must be a positive integer")
    if x.ndim != 1:
        raise ValueError("lazy framing is only supported for 1-D arrays")

    npartitions = x.npartitions
    meta = meta_from_array(x, 2)
    token = tokenize(x, width)
    local_name = "lazy-frame-local-" + token
    prefix_name = "lazy-frame-prefix-" + token
    validate_name = "lazy-frame-validate-" + token
    out_name = "reshape-lazy-" + token

    dsk: dict = {}

    # Per-file bounded states: (record count, raw tail of at most width - 1).
    local_keys: list[tuple] = []
    for i in range(npartitions):
        key = (local_name, i)
        dsk[key] = Task(
            key,
            _lazy_frame_local_state,
            TaskRef((x.name, i)),
            quote(width),
        )
        local_keys.append(key)

    # Inclusive parallel prefix (Hillis-Steele scan) of the bounded states.
    # Level d combines states that are 2**d blocks apart, so the dependency
    # depth is ceil(log2(npartitions)) and every task only touches two states
    # whose tails are each smaller than width.
    inclusive_keys = _lazy_frame_prefix_graph(dsk, local_keys, prefix_name, width)

    # The exclusive prefix of block i is the inclusive prefix of block i - 1.
    exclusive_keys = [None, *inclusive_keys[:-1]]

    # A single shared validation task; every output block depends on it so an
    # incomplete batch cannot be hidden by selecting a subset of blocks.
    validate_key = (validate_name, 0)
    dsk[validate_key] = Task(
        validate_key,
        _lazy_frame_validate,
        TaskRef(inclusive_keys[-1]),
        quote(width),
    )

    # One output block per input block (file).  The validation task is a
    # dependency that contributes no data.
    empty_state = quote((0, np.empty((0,), dtype=x.dtype)))
    for i in range(npartitions):
        key = (out_name, i, 0)
        prefix_ref = (
            TaskRef(exclusive_keys[i])
            if exclusive_keys[i] is not None
            else empty_state
        )
        dsk[key] = Task(
            key,
            _lazy_frame_block,
            prefix_ref,
            TaskRef((x.name, i)),
            TaskRef(validate_key),
            quote(width),
        )

    out_chunks = (tuple(np.nan for _ in range(npartitions)), (width,))
    graph = HighLevelGraph.from_collections(out_name, dsk, dependencies=[x])
    return Array(graph, out_name, out_chunks, meta=meta)


def _lazy_frame_prefix_graph(dsk, local_keys, prefix_name, width):
    """Build an inclusive parallel-prefix tree of bounded states.

    Uses the Hillis-Steele scan: at level ``d`` every state is combined with the
    state ``2**d`` positions to its left.  The returned keys hold the inclusive
    prefix states in the original block order, and the dependency depth is
    ``ceil(log2(len(local_keys)))``.
    """
    n = len(local_keys)
    if n == 1:
        return list(local_keys)

    current = list(local_keys)
    step = 1
    level = 0
    while step < n:
        updated = list(current)
        for i in range(step, n):
            level += 1
            key = (prefix_name, level)
            dsk[key] = Task(
                key,
                _lazy_frame_combine_state,
                TaskRef(current[i - step]),
                TaskRef(current[i]),
                quote(width),
            )
            updated[i] = key
        current = updated
        step *= 2
    return current
