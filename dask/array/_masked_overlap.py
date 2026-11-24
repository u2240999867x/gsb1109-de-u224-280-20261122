from __future__ import annotations

import inspect
from itertools import product

import numpy as np
from tlz import merge

from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer

# Names are versioned so that two arrays built from identical inputs share all
# intermediate keys.
_TOKEN_PREFIX = "map-overlap-mask-v1"

# Keyword names accepted for the valid-centre mask handed to ``func``.
_CENTER_NAMES = (
    "valid_center",
    "valid_centers",
    "valid_center_mask",
    "center",
    "centers",
    "center_valid",
    "center_mask",
    "valid_mask",
    "mask",
    "valid",
    "water_mask",
)

_COORD_NAMES = (
    "block_info",
    "block_id",
    "global_coords",
    "coordinates",
    "coords",
)


def _parse_mask_depth(depth):
    """Return ``(left, right)`` column depths for the masked overlap."""
    if depth is None:
        return 0, 0
    # ``map_overlap`` users often pass a one-element list of per-array depths.
    if isinstance(depth, list) and len(depth) == 1:
        depth = depth[0]
    if isinstance(depth, (int | np.integer)):
        d = int(depth)
        return d, d
    if isinstance(depth, tuple):
        if len(depth) == 2 and all(
            isinstance(d, (int | np.integer)) for d in depth
        ):
            return int(depth[0]), int(depth[1])
        raise ValueError(
            "masked map_overlap expects depth=(left, right) for the column axis"
        )
    if isinstance(depth, dict):
        d = depth.get(1, 0)
        if isinstance(d, tuple):
            if len(d) != 2:
                raise ValueError("Column depth must be an int or a (left, right) pair")
            return int(d[0]), int(d[1])
        d = int(d)
        return d, d
    raise TypeError("depth must be an int, a (left, right) tuple, or a dict")


def _as_int_array(values):
    result = np.asarray(values, dtype=np.int64)
    if result.ndim == 0:
        result = result.reshape(1)
    return result


# ---------------------------------------------------------------------------
# Bank summaries: the first (inclusive) and last (exclusive) wet column of
# every row.  Each mask block contributes a tiny summary and the summaries are
# merged in a balanced tree, so the graph stays logarithmic in the number of
# mask blocks per row.
# ---------------------------------------------------------------------------


def _bank_leaf(block, row_start, col_start):
    block = np.asarray(block, dtype=bool)
    nrows = block.shape[0]
    starts = np.full(nrows, -1, dtype=np.int64)
    ends = np.full(nrows, -1, dtype=np.int64)
    if block.shape[1]:
        has = block.any(axis=1)
        if has.any():
            first = block.argmax(axis=1)
            last = block.shape[1] - 1 - block[:, ::-1].argmax(axis=1)
            starts[has] = first[has] + col_start
            ends[has] = last[has] + col_start + 1
    return starts, ends


def _bank_combine(left, right):
    starts_l, ends_l = left
    starts_r, ends_r = right
    starts = np.where(starts_l >= 0, starts_l, starts_r)
    ends = np.where(ends_r >= 0, ends_r, ends_l)
    return starts.astype(np.int64, copy=False), ends.astype(np.int64, copy=False)


def _bank_gather(pairs, row_offsets, nrows):
    starts = np.full(nrows, -1, dtype=np.int64)
    ends = np.full(nrows, -1, dtype=np.int64)
    for (s, e), offset in zip(pairs, row_offsets):
        s = _as_int_array(s)
        e = _as_int_array(e)
        starts[offset : offset + len(s)] = s
        ends[offset : offset + len(e)] = e
    return starts, ends


# ---------------------------------------------------------------------------
# Per-row bank data.  A periodic window can only leave the water at a bank, so
# each row only needs the first ``right`` and last ``left`` cells of its water
# interval.  Every x block contributes at most one small segment per side and
# the segments are merged once per row and shared by all output blocks.
# ---------------------------------------------------------------------------


def _segment_merge(a, b):
    segments = sorted(list(a) + list(b), key=lambda item: item[0])
    if not segments:
        return []
    out = [segments[0]]
    for start, values in segments[1:]:
        prev_start, prev_values = out[-1]
        prev_end = prev_start + len(prev_values)
        if start <= prev_end:
            if start < prev_end:
                values = values[prev_end - start :]
            if len(values):
                out[-1] = (prev_start, np.concatenate([prev_values, values]))
        else:
            out.append((start, values))
    return out


def _edge_leaf(
    block, row, local_row, col_start, col_end, bank, left, right, wrap
):
    starts, ends = bank
    start = int(_as_int_array(starts)[row])
    end = int(_as_int_array(ends)[row])
    if start < 0 or end <= start:
        return []

    # A block that starts before the water (or ends after it) can place an
    # extended column arbitrarily far past the bank.  Widen both slices by
    # twice the largest block width so every wrap target is available; when
    # the water is shorter than this the two slices simply cover the run.
    intervals = [
        (start, min(end, start + right + wrap)),
        (max(start, end - left - wrap), end),
    ]
    clipped = []
    for lo, hi in intervals:
        lo = max(lo, col_start)
        hi = min(hi, col_end)
        if lo < hi:
            clipped.append((lo, hi))
    clipped.sort()

    # A water interval shorter than the window makes the two intervals
    # overlap.  Union them so a source cell is extracted only once.
    merged = []
    for lo, hi in clipped:
        if merged and lo <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))

    row_values = np.asarray(block)[local_row]
    return [[lo, row_values[lo - col_start : hi - col_start]] for lo, hi in merged]


def _edge_combine(left, right):
    return _segment_merge(left, right)


def _edge_finish(segments, bank, row):
    starts, ends = bank
    start = int(_as_int_array(starts)[row])
    end = int(_as_int_array(ends)[row])
    if start < 0 or end <= start:
        return start, end, []
    return start, end, _segment_merge(segments, [])


def _interval_intersect(a, b):
    out = []
    for alo, ahi in a:
        for blo, bhi in b:
            lo = max(alo, blo)
            hi = min(ahi, bhi)
            if lo < hi:
                out.append((lo, hi))
    return sorted(out)


def _interval_subtract(intervals, cuts):
    out = []
    for lo, hi in intervals:
        pieces = [(lo, hi)]
        for clo, chi in cuts:
            next_pieces = []
            for plo, phi in pieces:
                if chi <= plo or clo >= phi:
                    next_pieces.append((plo, phi))
                    continue
                if plo < clo:
                    next_pieces.append((plo, clo))
                if chi < phi:
                    next_pieces.append((chi, phi))
            pieces = next_pieces
        out.extend(pieces)
    return sorted(out)


def _edge_select(edge_value, spans, needed=None):
    """Keep only edge fragments that are not local and are actually needed."""
    start, end, segments = edge_value
    if not segments:
        return start, end, []
    spans = sorted((int(lo), int(hi)) for lo, hi in spans)
    if needed is not None:
        needed = sorted((int(lo), int(hi)) for lo, hi in needed if lo < hi)
        if not needed:
            return start, end, []
    out = []
    for lo, values in segments:
        hi = lo + len(values)
        intervals = (
            _interval_intersect([(lo, hi)], needed)
            if needed is not None
            else [(lo, hi)]
        )
        for begin, stop in _interval_subtract(intervals, spans):
            out.append([begin, values[begin - lo : stop - lo]])
    return start, end, out


# ---------------------------------------------------------------------------
# Final block computation
# ---------------------------------------------------------------------------


def _call_func(func, block, valid_center, block_info, block_id, kwargs):
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        sig = None

    injected = {}
    extra_positional = []
    if sig is not None:
        params = sig.parameters
        var_kw = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())
        for name in _CENTER_NAMES:
            if name in params:
                injected[name] = valid_center
        for name in _COORD_NAMES:
            if name in params:
                if name == "block_info":
                    injected[name] = block_info
                elif name == "block_id":
                    injected[name] = block_id
                else:
                    injected[name] = block_info[0]["array-location"]
        if var_kw:
            injected.setdefault("block_info", block_info)
            injected.setdefault("valid_center", valid_center)

        positional = [
            p
            for p in params.values()
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        if len(positional) >= 2:
            second = positional[1]
            if (
                second.name not in _CENTER_NAMES
                and second.name not in _COORD_NAMES
                and second.default is inspect.Parameter.empty
            ):
                extra_positional = [valid_center]

    # Explicit user keywords win.
    injected.update(kwargs)
    return func(block, *extra_positional, **injected)


def _slice_cols(block, start, stop):
    block = np.asarray(block)
    if block.ndim == 1:
        return block[start:stop]
    return block[:, start:stop]


def _masked_overlap_block(
    func,
    local_blocks,
    edge_keys,
    bank,
    row_bi,
    col_bi,
    row_start,
    row_end,
    col_start,
    col_end,
    left,
    right,
    shape,
    numblocks,
    dtype,
    user_kwargs,
):
    local_spans = [(lo, hi) for _, lo, hi in local_blocks]
    local_blocks = [np.asarray(block) for block, _, _ in local_blocks]
    nrows = row_end - row_start
    ncols = col_end - col_start
    ext_width = ncols + left + right

    ext_dtype = np.dtype(dtype)
    if not np.issubdtype(ext_dtype, np.floating):
        ext_dtype = np.dtype(np.float64)
    ext = np.full((nrows, ext_width), np.nan, dtype=ext_dtype)
    valid_center = np.zeros((nrows, ext_width), dtype=bool)

    starts, ends = bank
    starts = _as_int_array(starts)
    ends = _as_int_array(ends)
    edges_by_row = {}
    for row, value in zip(range(row_start, row_end), edge_keys):
        start = int(starts[row])
        end = int(ends[row])
        if start < 0 or end <= start:
            edges_by_row[row] = (start, end, [])
            continue
        first = max(start, col_start)
        last = min(end, col_end)
        if first >= last:
            edges_by_row[row] = (start, end, [])
            continue
        # Source cells reached by wrapping columns of this block's extension.
        water_len = end - start
        reached = set()
        for column in range(col_start - left, col_end + right):
            if not (start <= column < end):
                reached.add(start + ((column - start) % water_len))
        if reached:
            needed = []
            for source in sorted(reached):
                needed.append((source, source + 1))
        else:
            needed = []
        edges_by_row[row] = _edge_select(value, local_spans, needed)

    def lookup(row, column):
        for (lo, hi), block in zip(local_spans, local_blocks):
            if lo <= column < hi:
                if block.ndim == 1:
                    return block[column - lo]
                return block[row - row_start, column - lo]
        _, _, segments = edges_by_row[row]
        for seg_start, values in segments:
            if seg_start <= column < seg_start + len(values):
                return values[column - seg_start]
        raise ValueError(
            "Internal masked overlap error: no source value for column "
            f"{column} in row {row}"
        )

    center_valid = np.zeros(nrows, dtype=bool)
    for i, row in enumerate(range(row_start, row_end)):
        start = int(starts[row])
        end = int(ends[row])
        if start < 0 or end <= start:
            continue
        if max(start, col_start) >= min(end, col_end):
            continue

        # Any valid centre makes the *entire* extension periodic for this row.
        center_valid[i] = True
        water_len = end - start
        for j, column in enumerate(range(col_start - left, col_end + right)):
            if start <= column < end:
                source = column
            else:
                source = start + ((column - start) % water_len)
            ext[i, j] = lookup(row, source)
        for j, column in enumerate(range(col_start, col_end), start=left):
            if start <= column < end:
                valid_center[i, j] = True

    block_info = {
        0: {
            "shape": shape,
            "num-chunks": numblocks,
            "chunk-location": (row_bi, col_bi),
            "array-location": [(row_start, row_end), (col_start, col_end)],
        },
        None: {
            "shape": shape,
            "num-chunks": numblocks,
            "chunk-location": (row_bi, col_bi),
            "array-location": [(row_start, row_end), (col_start, col_end)],
            "chunk-shape": (nrows, ncols),
            "dtype": dtype,
        },
    }
    result = _call_func(
        func, ext, valid_center, block_info, (row_bi, col_bi), user_kwargs
    )
    result = np.asarray(result)
    if result.shape == (nrows, ncols):
        # The callable already returned a centre-sized block.
        out = np.array(result, copy=True)
    elif result.shape == (nrows, ext_width):
        out = np.array(result[:, left : left + ncols], copy=True)
    elif result.shape == (nrows,):
        out = np.array(np.broadcast_to(result[:, None], (nrows, ncols)), copy=True)
    elif result.shape == (nrows, 1):
        out = np.array(np.broadcast_to(result, (nrows, ncols)), copy=True)
    else:
        raise ValueError(
            "The masked map_overlap function must return an array with the "
            f"shape of its extended block {ext.shape} or its center "
            f"{(nrows, ncols)}, got {result.shape}"
        )
    if not np.issubdtype(out.dtype, np.floating):
        out = out.astype(np.float64)
    invalid = ~center_valid[:, None] | ~valid_center[:, left : left + ncols]
    out[invalid] = np.nan
    return out


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------


def _balanced_row_roots(bank_name, bank_dsk, ri, keys):
    level = 0
    while len(keys) > 1:
        next_level = []
        for j in range(0, len(keys), 2):
            if j + 1 == len(keys):
                next_level.append(keys[j])
                continue
            key = (bank_name, "combine", ri, level, j // 2)
            bank_dsk[key] = (_bank_combine, keys[j], keys[j + 1])
            next_level.append(key)
        keys = next_level
        level += 1
    return keys[0]


def _build_bank_layer(x, mask, bank_name, layers, deps, mask_layers):
    bank_dsk = {}
    mask_keys = mask.__dask_keys__()
    row_starts = np.cumsum((0,) + tuple(mask.chunks[0]))
    col_starts = np.cumsum((0,) + tuple(mask.chunks[1]))

    row_roots = []
    for ri, row_chunk in enumerate(mask.chunks[0]):
        leaves = []
        for ci, _ in enumerate(mask.chunks[1]):
            key = (bank_name, "leaf", ri, ci)
            bank_dsk[key] = (
                _bank_leaf,
                mask_keys[ri][ci],
                int(row_starts[ri]),
                int(col_starts[ci]),
            )
            leaves.append(key)
        if not leaves:
            root = (bank_name, "empty", ri)
            bank_dsk[root] = (np.full, (len(row_chunk),), -1)
        else:
            root = _balanced_row_roots(bank_name, bank_dsk, ri, leaves)
        row_roots.append(root)

    if len(row_roots) == 1:
        bank_key = row_roots[0]
    else:
        bank_key = (bank_name, "gather")
        bank_dsk[bank_key] = (
            _bank_gather,
            list(row_roots),
            [int(r) for r in row_starts[:-1]],
            x.shape[0],
        )

    layers[bank_name] = MaterializedLayer(bank_dsk)
    deps[bank_name] = set(mask_layers)
    return bank_key


def _build_edge_layer(x, bank_key, edge_name, left, right, layers, deps, x_layers):
    edge_dsk = {}
    x_keys = x.__dask_keys__()
    row_starts = np.cumsum((0,) + tuple(x.chunks[0]))
    col_starts = np.cumsum((0,) + tuple(x.chunks[1]))
    wrap = 2 * max(x.chunks[1]) if x.chunks[1] else 0

    roots = {}
    for row in range(x.shape[0]):
        row_bi = int(np.searchsorted(row_starts, row, side="right") - 1)
        local_row = row - int(row_starts[row_bi])
        leaves = []
        for col_bi, (col_chunk, col_start) in enumerate(
            zip(x.chunks[1], col_starts[:-1])
        ):
            key = (edge_name, "leaf", row, col_bi)
            edge_dsk[key] = (
                _edge_leaf,
                x_keys[row_bi][col_bi],
                row,
                local_row,
                int(col_start),
                int(col_start + col_chunk),
                bank_key,
                left,
                right,
                wrap,
            )
            leaves.append(key)

        level = 0
        while len(leaves) > 1:
            next_level = []
            for j in range(0, len(leaves), 2):
                if j + 1 == len(leaves):
                    next_level.append(leaves[j])
                    continue
                key = (edge_name, "combine", row, level, j // 2)
                edge_dsk[key] = (_edge_combine, leaves[j], leaves[j + 1])
                next_level.append(key)
            leaves = next_level
            level += 1

        root = leaves[0]
        finish_key = (edge_name, "finish", row)
        edge_dsk[finish_key] = (_edge_finish, root, bank_key, row)
        roots[row] = finish_key

    layers[edge_name] = MaterializedLayer(edge_dsk)
    deps[edge_name] = set(x_layers) | {bank_key[0]}
    return roots


def _layer_names(array):
    try:
        return set(array.__dask_layers__())
    except (AttributeError, ValueError):
        from dask.highlevelgraph import _get_some_layer_name

        return {_get_some_layer_name(array)}


def _as_high_level_graph(array):
    graph = array.__dask_graph__()
    if isinstance(graph, HighLevelGraph):
        return graph
    from dask.highlevelgraph import _get_some_layer_name

    name = _get_some_layer_name(array)
    return HighLevelGraph({name: graph}, {name: set()})


def masked_map_overlap(func, x, mask, depth, *, dtype=None, **kwargs):
    """Build a mask-aware :func:`dask.array.map_overlap` result."""
    from dask.array.core import Array, asarray
    from dask.array.utils import meta_from_array
    from dask.base import is_dask_collection
    # Accept ``left=``/``right=`` (or ``look_left=``/``look_right=``) as an
    # alternative to a ``(left, right)`` depth.
    if any(k in kwargs for k in ("left", "right", "look_left", "look_right")):
        if depth is not None:
            raise TypeError("Specify either depth or explicit left/right depths")
        if "left" in kwargs:
            left = kwargs.pop("left")
        else:
            left = kwargs.pop("look_left", 0)
        if "right" in kwargs:
            right = kwargs.pop("right")
        else:
            right = kwargs.pop("look_right", 0)
        depth = (left, right)

    if not is_dask_collection(mask):
        mask = asarray(mask)
    if x.ndim != 2:
        raise ValueError("masked map_overlap is only implemented for 2-D arrays")
    if mask.ndim != 2 or mask.shape != x.shape:
        raise ValueError("mask must be a 2-D dask array with the same shape as x")

    left, right = _parse_mask_depth(depth)
    if left < 0 or right < 0:
        raise ValueError("depth must be non-negative")
    if dtype is None:
        dtype = x.dtype
    dtype = np.dtype(dtype)

    token = tokenize(func, x, mask, left, right, dtype, kwargs)
    bank_name = f"{_TOKEN_PREFIX}-bank-{token}"
    edge_name = f"{_TOKEN_PREFIX}-edges-{token}"
    out_name = f"{_TOKEN_PREFIX}-{token}"

    x_graph = _as_high_level_graph(x)
    mask_graph = _as_high_level_graph(mask)
    layers = merge(x_graph.layers, mask_graph.layers)
    deps = merge(x_graph.dependencies, mask_graph.dependencies)
    x_layers = _layer_names(x)
    mask_layers = _layer_names(mask)

    bank_key = _build_bank_layer(x, mask, bank_name, layers, deps, mask_layers)
    roots = _build_edge_layer(
        x, bank_key, edge_name, left, right, layers, deps, x_layers
    )

    out_dsk = {}
    slice_dsk = {}
    slice_name = f"{_TOKEN_PREFIX}-slice-{x.name}"
    x_keys = x.__dask_keys__()
    row_starts = np.cumsum((0,) + tuple(x.chunks[0]))
    col_starts = np.cumsum((0,) + tuple(x.chunks[1]))
    for row_bi, col_bi in product(range(x.numblocks[0]), range(x.numblocks[1])):
        row_start = int(row_starts[row_bi])
        row_end = int(row_starts[row_bi + 1])
        col_start = int(col_starts[col_bi])
        col_end = int(col_starts[col_bi + 1])
        lo = col_start - left
        hi = col_end + right

        local = []
        for other_bi, (other_chunk, other_start) in enumerate(
            zip(x.chunks[1], col_starts[:-1])
        ):
            other_start = int(other_start)
            other_end = other_start + other_chunk
            if other_start < hi and other_end > lo:
                # Only the columns this target block can read directly; the
                # bank slices cover the periodic wrap-around columns.
                frag_start = max(other_start, lo)
                frag_stop = min(other_end, hi)
                rel_start = frag_start - other_start
                rel_stop = rel_start + (frag_stop - frag_start)
                if rel_start == 0 and rel_stop == other_chunk:
                    frag_key = x_keys[row_bi][other_bi]
                else:
                    frag_key = (
                        slice_name,
                        row_bi,
                        other_bi,
                        rel_start,
                        rel_stop,
                    )
                    slice_dsk[frag_key] = (
                        _slice_cols,
                        x_keys[row_bi][other_bi],
                        rel_start,
                        rel_stop,
                    )
                local.append([frag_key, frag_start, frag_stop])

        edge_keys = [roots[row] for row in range(row_start, row_end)]
        key = (out_name, row_bi, col_bi)
        out_dsk[key] = (
            _masked_overlap_block,
            func,
            local,
            edge_keys,
            bank_key,
            row_bi,
            col_bi,
            row_start,
            row_end,
            col_start,
            col_end,
            left,
            right,
            x.shape,
            x.numblocks,
            dtype,
            kwargs,
        )

    if slice_dsk:
        layers[slice_name] = MaterializedLayer(slice_dsk)
        deps[slice_name] = set(x_layers)

    layers[out_name] = MaterializedLayer(out_dsk)
    deps[out_name] = set(x_layers) | {
        bank_key[0],
        edge_name,
    }
    if slice_dsk:
        deps[out_name].add(slice_name)

    graph = HighLevelGraph(layers, deps)
    meta = meta_from_array(x._meta, ndim=2, dtype=dtype)
    result = Array(graph, out_name, x.chunks, meta=meta)

    if not isinstance(x, Array):
        # The classic Array above is a transport for the graph; return a
        # collection of the same flavour as the input (e.g. array-expr).
        from dask.core import flatten

        try:
            from dask.array._array_expr._collection import from_graph

            return from_graph(
                graph,
                meta,
                x.chunks,
                list(flatten(result.__dask_keys__())),
                out_name,
            )
        except ImportError:  # pragma: no cover - classic backend only
            pass
    return result
