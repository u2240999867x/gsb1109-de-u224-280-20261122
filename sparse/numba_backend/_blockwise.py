from __future__ import annotations

from numbers import Integral

import numpy as np

from ._slicing import normalize_index
from ._sparse_array import SparseArray
from ._utils import normalize_axis


def _ceil_div(a, b):
    """Ceiling division for integers with a positive divisor."""
    return -(-a // b)


class BlockwiseArray(SparseArray):
    """
    A lazy concatenation of sparse blocks along a single axis, where every
    block keeps its own fill-value (background).

    Only the stored value exceptions of each block and the block boundaries
    along the concatenation axis are kept in memory. Transposing and slicing
    are performed block-wise and never materialize the implicit background
    cells, so blocks with huge all-background extents stay cheap.

    Parameters
    ----------
    blocks : Iterable[SparseArray]
        The blocks to concatenate. All blocks must have the same number of
        dimensions and agree on every axis except the concatenation axis.
    axis : int, optional
        The axis along which the blocks are concatenated. The default is zero.

    Attributes
    ----------
    blocks : list[SparseArray]
        The concatenated blocks.
    axis : int
        The concatenation axis.
    offsets : numpy.ndarray
        The cumulative block offsets along the concatenation axis, starting
        at zero and ending at ``shape[axis]``.

    See Also
    --------
    [`sparse.concatenate`][] : The function producing these arrays for
    mismatched fill-values.
    """

    def __init__(self, blocks, axis=0):
        blocks = list(blocks)
        if len(blocks) == 0:
            raise ValueError("At least one block is required.")

        ndim = blocks[0].ndim
        axis = normalize_axis(axis, ndim)

        shape = list(blocks[0].shape)
        for block in blocks:
            if block.ndim != ndim:
                raise ValueError("All blocks must have the same number of dimensions.")
            for ax in range(ndim):
                if ax != axis and block.shape[ax] != shape[ax]:
                    raise ValueError("Block shapes must agree on all axes except the concatenation axis.")

        shape[axis] = sum(block.shape[axis] for block in blocks)

        self.blocks = blocks
        self.axis = axis
        offsets = np.empty(len(blocks) + 1, dtype=np.intp)
        offsets[0] = 0
        np.cumsum([block.shape[axis] for block in blocks], out=offsets[1:])
        self.offsets = offsets
        self._dtype = np.result_type(*(block.dtype for block in blocks))

        super().__init__(shape=tuple(shape), fill_value=blocks[0].fill_value)

    @property
    def dtype(self):
        return self._dtype

    @property
    def format(self):
        return "blockwise"

    @property
    def nnz(self):
        """
        The number of stored value exceptions, summed over all blocks.

        Returns
        -------
        int
            The number of stored values in this array.
        """
        return sum(block.nnz for block in self.blocks)

    @property
    def boundaries(self):
        """
        The block boundaries along the concatenation axis.

        Returns
        -------
        tuple[int]
            The offsets at which every block after the first starts.
        """
        return tuple(int(o) for o in self.offsets[1:-1])

    @property
    def nbytes(self):
        return sum(getattr(block, "nbytes", 0) for block in self.blocks)

    def __len__(self):
        return self.shape[0]

    def __str__(self):
        summary = (
            f"<BlockwiseArray: shape={self.shape!s}, dtype={self.dtype!s}, "
            f"nnz={self.nnz:d}, blocks={len(self.blocks):d}, axis={self.axis:d}>"
        )
        return self._str_impl(summary)

    __repr__ = __str__

    def transpose(self, axes=None):
        """
        Returns a new array with the order of the axes switched. Only the
        stored blocks are transposed; background cells are never materialized.

        Parameters
        ----------
        axes : Iterable[int], optional
            The new order of the axes compared to the previous one. Reverses
            the axes by default.

        Returns
        -------
        BlockwiseArray
            The array with the axes in the desired order.

        See Also
        --------
        [`numpy.ndarray.transpose`][] : NumPy equivalent function
        """
        if axes is None:
            axes = tuple(reversed(range(self.ndim)))

        axes = normalize_axis(axes, self.ndim)

        if len(np.unique(axes)) < len(axes):
            raise ValueError("repeated axis in transpose")

        if len(axes) != self.ndim:
            raise ValueError("axes don't match array")

        axes = tuple(axes)

        if axes == tuple(range(self.ndim)):
            return self

        return BlockwiseArray([block.transpose(axes) for block in self.blocks], axis=axes.index(self.axis))

    @property
    def T(self):
        """
        Returns a new array which has the order of the axes reversed.

        Returns
        -------
        BlockwiseArray
            The new array with the axes in the desired order.
        """
        return self.transpose()

    def swapaxes(self, axis1, axis2):
        """
        Returns a new array with the two given axes swapped.

        Parameters
        ----------
        axis1, axis2 : int
            The axes to swap.

        Returns
        -------
        BlockwiseArray
            The new array with the axes swapped.
        """
        axis1 = normalize_axis(axis1, self.ndim)
        axis2 = normalize_axis(axis2, self.ndim)
        axes = list(range(self.ndim))
        axes[axis1], axes[axis2] = axes[axis2], axes[axis1]
        return self.transpose(tuple(axes))

    def flatten(self, order="C"):
        """
        Returns a new sparse array that is a flattened version of this array.

        Parameters
        ----------
        order : {"C"}, optional
            Only row-major ("C") order is supported.

        Returns
        -------
        SparseArray
            The flattened output array.
        """
        if order not in {"C", None}:
            raise NotImplementedError("Only C-order flatten is supported for BlockwiseArray.")

        if self.axis == 0:
            # C-order flattening of a leading-axis concatenation is the
            # concatenation of the flattened blocks.
            return BlockwiseArray([block.flatten() for block in self.blocks], axis=0)

        return self.asformat("coo").flatten()

    def todense(self):
        """
        Convert this array to a dense [`numpy.ndarray`][]. Note that this may
        take a large amount of memory and time.

        Returns
        -------
        numpy.ndarray
            The converted dense array.
        """
        out = np.empty(self.shape, dtype=self.dtype)
        sl = [slice(None)] * self.ndim
        for block, lo, hi in zip(self.blocks, self.offsets[:-1], self.offsets[1:], strict=True):
            sl[self.axis] = slice(int(lo), int(hi))
            out[tuple(sl)] = block.todense()
        return out

    def asformat(self, format, **kwargs):
        """
        Convert this sparse array to a given format. Converting to a
        single-fill-value format materializes the background of every block
        whose fill-value differs from the output fill-value.

        Parameters
        ----------
        format : str
            A format string.

        Returns
        -------
        out : SparseArray
            The converted array.
        """
        from ._coo import COO

        if format == "coo":
            return COO.from_numpy(self.todense(), fill_value=self.fill_value)

        return self.asformat("coo").asformat(format, **kwargs)

    def get_nodes(self):
        nodes = {
            "format": self.format,
            "axis": np.asarray(self.axis),
            "nblocks": np.asarray(len(self.blocks)),
        }
        for i, block in enumerate(self.blocks):
            for key, value in block.get_nodes().items():
                nodes[f"block_{i}_{key}"] = value
        return nodes

    @classmethod
    def from_nodes(cls, nodes):
        from ._coo import COO

        formats = {"coo": COO, "blockwise": cls}
        axis = int(np.asarray(nodes["axis"]))
        nblocks = int(np.asarray(nodes["nblocks"]))
        keys = nodes.files if hasattr(nodes, "files") else nodes.keys()
        blocks = []
        for i in range(nblocks):
            prefix = f"block_{i}_"
            block_nodes = {key[len(prefix) :]: nodes[key] for key in keys if key.startswith(prefix)}
            block_format = str(np.asarray(block_nodes.pop("format")))
            blocks.append(formats[block_format].from_nodes(block_nodes))
        return cls(blocks, axis=axis)

    def isnan(self):
        """
        Element-wise NaN check, performed block-wise.

        Returns
        -------
        BlockwiseArray
            The output boolean array.
        """
        return BlockwiseArray([block.isnan() for block in self.blocks], axis=self.axis)

    def isinf(self):
        """
        Element-wise infinity check, performed block-wise.

        Returns
        -------
        BlockwiseArray
            The output boolean array.
        """
        return BlockwiseArray([block.isinf() for block in self.blocks], axis=self.axis)

    def _block_index(self, index, concat_entry):
        """Replace the concatenation-axis entry of a normalized index tuple."""
        block_index = []
        ax = 0
        for ind in index:
            if ind is None:
                block_index.append(None)
                continue
            block_index.append(concat_entry if ax == self.axis else ind)
            ax += 1
        return tuple(block_index)

    def __getitem__(self, index):
        from ._coo import COO

        if not isinstance(index, tuple):
            index = (index,)

        index = normalize_index(index, self.shape)

        if any(isinstance(ind, np.ndarray) for ind in index):
            raise IndexError("BlockwiseArray does not support advanced indexing.")

        axis_entries = [ind for ind in index if ind is not None]

        # Identity selection: no new axes and every consumed axis is a full slice.
        if len(axis_entries) == len(index) and all(
            isinstance(ind, slice) and ind == slice(0, dim, 1)
            for ind, dim in zip(axis_entries, self.shape, strict=True)
        ):
            return self

        concat_entry = axis_entries[self.axis]

        if isinstance(concat_entry, Integral):
            block_idx = int(np.searchsorted(self.offsets, concat_entry, side="right")) - 1
            local_entry = concat_entry - int(self.offsets[block_idx])
            return self.blocks[block_idx][self._block_index(index, local_entry)]

        # A slice selects an arithmetic progression along the concatenation
        # axis; intersecting it with a block extent yields another arithmetic
        # progression, so every block is sliced with its own local slice.
        start, stop, step = concat_entry.start, concat_entry.stop, concat_entry.step
        n = len(range(start, stop, step))
        extents = list(zip(self.blocks, self.offsets[:-1], self.offsets[1:], strict=True))
        if step < 0:
            # A reversed progression visits the blocks in reverse order.
            extents = extents[::-1]
        new_blocks = []
        for block, lo, hi in extents:
            lo, hi = int(lo), int(hi)
            if step > 0:
                k_lo = min(max(_ceil_div(lo - start, step), 0), n)
                k_hi = min(max(_ceil_div(hi - start, step), 0), n)
            else:
                k_lo = min(max(_ceil_div(start - hi + 1, -step), 0), n)
                k_hi = min(max(_ceil_div(start - lo + 1, -step), 0), n)
            count = k_hi - k_lo
            if count <= 0:
                continue
            first = start + k_lo * step - lo
            local_entry = slice(first, first + count * step, step)
            new_blocks.append(block[self._block_index(index, local_entry)])

        if not new_blocks:
            shape = tuple(
                1 if ind is None else len(range(ind.start, ind.stop, ind.step))
                for ind in index
                if not isinstance(ind, Integral)
            )
            return COO(
                np.empty((len(shape), 0), dtype=np.intp),
                np.empty(0, dtype=self.dtype),
                shape=shape,
                has_duplicates=False,
                sorted=True,
                fill_value=self.fill_value,
            )

        if len(new_blocks) == 1:
            return new_blocks[0]

        new_axis = 0
        ax = 0
        for ind in index:
            if ind is None:
                new_axis += 1
                continue
            if ax == self.axis:
                break
            if isinstance(ind, slice):
                new_axis += 1
            ax += 1

        return BlockwiseArray(new_blocks, axis=new_axis)
