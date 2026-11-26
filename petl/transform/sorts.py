from __future__ import absolute_import, print_function, division


import os
import heapq
from tempfile import NamedTemporaryFile
import itertools
import logging
from collections import namedtuple
import operator
from petl.compat import pickle, next, text_type


import petl.config as config
from petl.comparison import comparable_itemgetter
from petl.util.base import Table, asindices


logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


def sort(table, key=None, reverse=False, buffersize=None, tempdir=None,
         cache=True):
    """
    Sort the table. Field names or indices (from zero) can be used to specify
    the key. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['C', 2],
        ...           ['A', 9],
        ...           ['A', 6],
        ...           ['F', 1],
        ...           ['D', 10]]
        >>> table2 = etl.sort(table1, 'foo')
        >>> table2
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'A' |   9 |
        +-----+-----+
        | 'A' |   6 |
        +-----+-----+
        | 'C' |   2 |
        +-----+-----+
        | 'D' |  10 |
        +-----+-----+
        | 'F' |   1 |
        +-----+-----+

        >>> # sorting by compound key is supported
        ... table3 = etl.sort(table1, key=['foo', 'bar'])
        >>> table3
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'A' |   6 |
        +-----+-----+
        | 'A' |   9 |
        +-----+-----+
        | 'C' |   2 |
        +-----+-----+
        | 'D' |  10 |
        +-----+-----+
        | 'F' |   1 |
        +-----+-----+

        >>> # if no key is specified, the default is a lexical sort
        ... table4 = etl.sort(table1)
        >>> table4
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'A' |   6 |
        +-----+-----+
        | 'A' |   9 |
        +-----+-----+
        | 'C' |   2 |
        +-----+-----+
        | 'D' |  10 |
        +-----+-----+
        | 'F' |   1 |
        +-----+-----+

        >>> # sorting is stable, so fields can be sorted in different
        ... # directions by chaining sorts, starting with the last field
        ... table5 = etl.sort(table1, 'bar').sort('foo', reverse=True)
        >>> table5
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'F' |   1 |
        +-----+-----+
        | 'D' |  10 |
        +-----+-----+
        | 'C' |   2 |
        +-----+-----+
        | 'A' |   6 |
        +-----+-----+
        | 'A' |   9 |
        +-----+-----+

    The `buffersize` argument should be an `int` or `None`.

    If the number of rows in the table is less than `buffersize`, the table
    will be sorted in memory. Otherwise, the table is sorted in chunks of
    no more than `buffersize` rows, each chunk is written to a temporary file,
    and then a merge sort is performed on the temporary files.

    If `buffersize` is `None`, the value of
    `petl.config.sort_buffersize` will be used. By default this is
    set to 100000 rows, but can be changed, e.g.::

        >>> import petl.config
        >>> petl.config.sort_buffersize = 500000

    If `petl.config.sort_buffersize` is set to `None`, this forces
    all sorting to be done entirely in memory.

    By default the results of the sort will be cached, and so a second pass over
    the sorted table will yield rows from the cache and will not repeat the
    sort operation. To turn off caching, set the `cache` argument to `False`.

    A cached sort view is bound to the current input when a reader first
    requests the header; requesting only the header does not read any data
    rows. All readers which bind to the same input share one source read and
    sort, regardless of which reader first asks for data. Call
    ``clearcache()`` after pointing the source at a different input, so that
    new readers bind to the new input. Readers which have already started
    keep reading the input and cached rows they bound to. Temporary sort files
    stay available until the last reader bound to their input has finished or
    closed.

    """

    return SortView(table, key=key, reverse=reverse, buffersize=buffersize,
                    tempdir=tempdir, cache=cache)


Table.sort = sort


def _iterchunk(fn):
    # reopen so iterators from file cache are independent
    debug('iterchunk, opening %s' % fn)
    with open(fn, 'rb') as f:
        try:
            while True:
                yield pickle.load(f)
        except EOFError:
            pass
    debug('end of iterchunk, closed %s' % fn)


class _Keyed(namedtuple('Keyed', ['key', 'obj'])):
    #  Override default behavior of namedtuple comparisons, only keys need to be compared for heapmerge
    def __eq__(self, other):
        return self.key == other.key

    def __lt__(self, other):
        return self.key < other.key

    def __le__(self, other):
        return self.key <= other.key

    def __ne__(self, other):
        return self.key != other.key

    def __gt__(self, other):
        return self.key > other.key

    def __ge__(self, other):
        return self.key >= other.key


def _heapqmergesorted(key=None, *iterables):
    """Return a single iterator over the given iterables, sorted by the
    given `key` function, assuming the input iterables are already sorted by
    the same function. (I.e., the merge part of a general merge sort.) Uses
    :func:`heapq.merge` for the underlying implementation."""

    if key is None:
        keyed_iterables = iterables
        for element in heapq.merge(*keyed_iterables):
            yield element
    else:
        keyed_iterables = [(_Keyed(key(obj), obj) for obj in iterable)
                           for iterable in iterables]
        for element in heapq.merge(*keyed_iterables):
            yield element.obj


def _shortlistmergesorted(key=None, reverse=False, *iterables):
    """Return a single iterator over the given iterables, sorted by the
    given `key` function, assuming the input iterables are already sorted by
    the same function. (I.e., the merge part of a general merge sort.) Uses
    :func:`min` (or :func:`max` if ``reverse=True``) for the underlying
    implementation."""

    if reverse:
        op = max
    else:
        op = min
    if key is not None:
        opkwargs = {'key': key}
    else:
        opkwargs = dict()
    # populate initial shortlist
    # (remember some iterables might be empty)
    iterators = list()
    shortlist = list()
    for iterable in iterables:
        it = iter(iterable)
        try:
            first = next(it)
            iterators.append(it)
            shortlist.append(first)
        except StopIteration:
            pass
    # do the mergesort
    while iterators:
        nxt = op(shortlist, **opkwargs)
        yield nxt
        nextidx = shortlist.index(nxt)
        try:
            shortlist[nextidx] = next(iterators[nextidx])
        except StopIteration:
            del shortlist[nextidx]
            del iterators[nextidx]


def _mergesorted(key=None, reverse=False, *iterables):
    # N.B., I've used heapq for normal merge sort and shortlist merge sort for
    # reverse merge sort because I've assumed that heapq.merge is faster and
    # so is preferable but it doesn't support reverse sorting so the shortlist
    # merge sort has to be used for reverse sorting. Some casual profiling
    # suggests there isn't much between the two in terms of speed, but might be
    # worth profiling more carefully

    if reverse:
        return _shortlistmergesorted(key, True, *iterables)
    else:
        return _heapqmergesorted(key, *iterables)


class _SortCache(object):
    """State shared by all readers of one batch of a :class:`SortView`.

    A cache is bound to a particular source iterator (and therefore to a
    particular batch) when its header is read. Data is sorted lazily on the
    first request for a data row, but the same sorted result is then shared by
    every reader of the batch.
    """

    def __init__(self, hdr, getkey, source_it, no_output=False):
        self.hdr = hdr
        self.getkey = getkey
        self.source_it = source_it
        self.no_output = no_output
        self.rows = None
        self.files = None
        self.error = None
        self.complete = False

    def store_mem(self, rows):
        self.rows = rows
        self.source_it = None
        self.complete = True

    def store_files(self, files):
        self.files = files
        self.source_it = None
        self.complete = True

    def store_error(self, error):
        self.source_it = None
        self.error = error

    def iterrows(self, reverse):
        if self.error is not None:
            raise self.error
        if self.rows is not None:
            for row in self.rows:
                yield tuple(row)
        else:
            # Keep a local reference to the files for the lifetime of this
            # iterator, so they are not deleted while they are being read.
            filecache = self.files
            filenames = list(map(operator.attrgetter('name'), filecache))
            debug('iterate from file cache: %r', filenames)
            chunkiters = [_iterchunk(fn) for fn in filenames]
            rows = _mergesorted(self.getkey, reverse, *chunkiters)
            try:
                for row in rows:
                    yield tuple(row)
            finally:
                # N.B., need to ensure that any open files are closed
                # **before** temporary files are deleted, as deletion will
                # fail on Windows if a file is still in use.
                del chunkiters
                del rows
                del filecache


class SortView(Table):
    def __init__(self, source, key=None, reverse=False, buffersize=None,
                 tempdir=None, cache=True):
        self.source = source
        self.key = key
        self.reverse = reverse
        if buffersize is None:
            self.buffersize = config.sort_buffersize
        else:
            self.buffersize = buffersize
        self.tempdir = tempdir
        self._cache = cache
        self._batchcache = None
        self._hdrcache = None
        self._memcache = None
        self._filecache = None
        self._getkey = None

    def clearcache(self):
        debug('clear cache')
        # Readers which are already iterating keep their own reference to the
        # cache they bound to, so clearing here only detaches the view from
        # the current batch.
        self._batchcache = None
        self._hdrcache = None
        self._memcache = None
        self._filecache = None
        self._getkey = None

    def __iter__(self):
        if self._cache:
            return self._itercached()
        else:
            return self._iteruncached()

    def _itercached(self):
        # Bind to a batch when the header is first requested. From this point
        # on this iterator only ever sees this cache, even if the view is
        # pointed at another batch and clearcache() is called.
        cache = self._batchcache
        if cache is None:
            cache = self._newcache()
            if cache.no_output:
                return
        yield tuple(cache.hdr)
        try:
            if cache.error is not None:
                raise cache.error
            if cache.complete:
                for row in cache.iterrows(self.reverse):
                    yield row
            elif cache.source_it is None:
                # Another reader already failed to build this batch.
                raise RuntimeError('sort cache is not available')
            else:
                for row in self._iternocache(cache.source_it, cache.getkey,
                                             self.reverse, cache):
                    yield row
        except Exception as e:
            if not cache.complete:
                # Do not retain a partially built batch; readers which are
                # already bound to it see the same failure, while a later pass
                # through the view can start again.
                cache.store_error(e)
                if self._batchcache is cache:
                    self.clearcache()
            raise

    def _newcache(self):
        debug('create cache for current batch')
        it = iter(self.source)
        try:
            hdr = next(it)
        except StopIteration:
            if self.key is None:
                # nothing to do on a table without headers
                return _SortCache(None, None, None, no_output=True)
            hdr = []

        if self.key is not None:
            # convert field selection into field indices
            indices = asindices(hdr, self.key)
        else:
            indices = range(len(hdr))
        # now use field indices to construct a _getkey function
        getkey = comparable_itemgetter(*indices)

        cache = _SortCache(hdr, getkey, it)
        self._batchcache = cache
        self._hdrcache = hdr
        self._memcache = None
        self._filecache = None
        self._getkey = getkey
        return cache

    def _cache_updated(self, cache):
        # The compatibility attributes on the view always describe the
        # current batch. A retired batch may finish sorting while the view is
        # serving a newer batch, and must not overwrite them.
        if self._batchcache is cache:
            self._hdrcache = cache.hdr
            self._getkey = cache.getkey
            self._memcache = cache.rows
            self._filecache = cache.files

    def _iteruncached(self):
        debug('iterate without cache')
        it = iter(self.source)

        try:
            hdr = next(it)
        except StopIteration:
            if self.key is None:
                return  # nothing to do on a table without headers
            hdr = []
        yield tuple(hdr)

        if self.key is not None:
            # convert field selection into field indices
            indices = asindices(hdr, self.key)
        else:
            indices = range(len(hdr))
        # now use field indices to construct a _getkey function
        getkey = comparable_itemgetter(*indices)

        for row in self._iternocache(it, getkey, self.reverse):
            yield row

    def _iternocache(self, it, getkey, reverse, cache=None):
        debug('sorting rows')
        # TODO support native comparison

        # initialise the first chunk
        rows = list(itertools.islice(it, 0, self.buffersize))
        rows.sort(key=getkey, reverse=reverse)

        # have we exhausted the source iterator?
        if self.buffersize is None or len(rows) < self.buffersize:
            # yes, table fits within sort buffer

            if cache is not None:
                debug('caching mem')
                cache.store_mem(rows)
                self._cache_updated(cache)

            for row in rows:
                yield tuple(row)

        else:
            # no, table is too big, need to sort in chunks

            chunkfiles = []

            try:
                while rows:

                    # dump the chunk
                    with NamedTemporaryFile(dir=self.tempdir, delete=False,
                                            mode='wb') as f:
                        # N.B., we **don't** want the file to be deleted on
                        # close, but we **do** want the file to be deleted
                        # when all references to the wrapper are gone, or
                        # when the program exits.
                        wrapper = _NamedTempFileDeleteOnGC(f.name)
                        debug('created temporary chunk file %s' % f.name)
                        for row in rows:
                            pickle.dump(row, f, protocol=-1)
                        f.flush()
                        chunkfiles.append(wrapper)

                    # grab the next chunk
                    rows = list(itertools.islice(it, 0, self.buffersize))
                    rows.sort(key=getkey, reverse=reverse)
            except Exception:
                # Release any chunks written before the failure.
                chunkfiles = None
                wrapper = None
                raise

            if cache is not None:
                debug('caching files')
                cache.store_files(chunkfiles)
                self._cache_updated(cache)

            chunkiters = [_iterchunk(f.name) for f in chunkfiles]
            rows = _mergesorted(getkey, reverse, *chunkiters)
            try:
                for row in rows:
                    yield tuple(row)
            finally:
                del chunkiters
                del rows


class _NamedTempFileDeleteOnGC(object):

    def __init__(self, name):
        self.name = name

    def delete(self, unlink=os.unlink, log=logger.debug):
        name = self.name
        try:
            log('deleting %s' % name)
            unlink(name)
        except Exception as e:
            log('exception deleting %s: %s' % (name, e))
            raise
        else:
            log('deleted %s' % name)

    def __del__(self):
        self.delete()

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


def mergesort(*tables, **kwargs):
    """
    Combine multiple input tables into one sorted output table. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['A', 9],
        ...           ['C', 2],
        ...           ['D', 10],
        ...           ['A', 6],
        ...           ['F', 1]]
        >>> table2 = [['foo', 'bar'],
        ...           ['B', 3],
        ...           ['D', 10],
        ...           ['A', 10],
        ...           ['F', 4]]
        >>> table3 = etl.mergesort(table1, table2, key='foo')
        >>> table3.lookall()
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'A' |   9 |
        +-----+-----+
        | 'A' |   6 |
        +-----+-----+
        | 'A' |  10 |
        +-----+-----+
        | 'B' |   3 |
        +-----+-----+
        | 'C' |   2 |
        +-----+-----+
        | 'D' |  10 |
        +-----+-----+
        | 'D' |  10 |
        +-----+-----+
        | 'F' |   1 |
        +-----+-----+
        | 'F' |   4 |
        +-----+-----+

    If the input tables are already sorted by the given key, give
    ``presorted=True`` as a keyword argument.

    This function is equivalent to concatenating the input tables using
    :func:`cat` then sorting, however this function will typically be more
    efficient, especially if the input tables are presorted.

    Keyword arguments:

    key : string or tuple of strings, optional
        Field name or tuple of fields to sort by (defaults to `None` lexical
        sort)
    reverse : bool, optional
        `True` if sort in reverse (descending) order (defaults to `False`)
    presorted : bool, optional
        `True` if inputs are already sorted by the given key (defaults to
        `False`)
    missing : object
        Value to fill with when input tables have different fields (defaults to
        `None`)
    header : sequence of strings, optional
        Specify a fixed header for the output table
    buffersize : int, optional
        Limit the number of rows in memory per input table when inputs are not
        presorted

    """

    return MergeSortView(tables, **kwargs)


Table.mergesort = mergesort


class MergeSortView(Table):
    def __init__(self, tables, key=None, reverse=False, presorted=False,
                 missing=None, header=None, buffersize=None, tempdir=None,
                 cache=True):
        self.key = key
        if presorted:
            self.tables = tables
        else:
            self.tables = [sort(t, key=key, reverse=reverse,
                                buffersize=buffersize, tempdir=tempdir,
                                cache=cache)
                           for t in tables]
        self.missing = missing
        self._header = header
        self.reverse = reverse

    def __iter__(self):
        return itermergesort(self.tables, self.key, self._header, self.missing,
                             self.reverse)


def itermergesort(sources, key, header, missing, reverse):
    # first need to standardise headers of all input tables
    # borrow this from itercat - TODO remove code smells

    its = [iter(t) for t in sources]
    src_hdrs = []
    for it in its:
        try:
            src_hdrs.append(next(it))
        except StopIteration:
            src_hdrs.append([])

    if header is None:
        # determine output fields by gathering all fields found in the sources
        outhdr = list()
        for hdr in src_hdrs:
            for f in list(map(text_type, hdr)):
                if f not in outhdr:
                    # add any new fields as we find them
                    outhdr.append(f)
    else:
        # predetermined output fields
        outhdr = header
    yield tuple(outhdr)

    def _standardisedata(it, hdr, ofs):
        flds = list(map(text_type, hdr))
        # now construct and yield the data rows
        for _row in it:
            try:
                # should be quickest to do this way
                yield tuple(_row[flds.index(fo)] if fo in flds else missing
                            for fo in ofs)
            except IndexError:
                # handle short rows
                outrow = [missing] * len(ofs)
                for i, fi in enumerate(flds):
                    try:
                        outrow[ofs.index(fi)] = _row[i]
                    except IndexError:
                        pass  # be relaxed about short rows
                yield tuple(outrow)

    # wrap all iterators to standardise fields
    sits = [_standardisedata(it, hdr, outhdr)
            for hdr, it in zip(src_hdrs, its)]

    # now determine key function
    getkey = None
    if key is not None:
        # convert field selection into field indices
        indices = asindices(outhdr, key)
        # now use field indices to construct a _getkey function
        # N.B., this will probably raise an exception on short rows
        getkey = comparable_itemgetter(*indices)

    # OK, do the merge sort
    for row in _shortlistmergesorted(getkey, reverse, *sits):
        yield row


def issorted(table, key=None, reverse=False, strict=False):
    """
    Return True if the table is ordered (i.e., sorted) by the given key. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['a', 1, True],
        ...           ['b', 3, True],
        ...           ['b', 2]]
        >>> etl.issorted(table1, key='foo')
        True
        >>> etl.issorted(table1, key='bar')
        False
        >>> etl.issorted(table1, key='foo', strict=True)
        False
        >>> etl.issorted(table1, key='foo', reverse=True)
        False

    When no key is given, compare all fields in the header using the same
    mixed-type ordering as :func:`sort`. Missing cells compare as ``None``.

    """

    # determine the operator to use when comparing rows
    if reverse and strict:
        op = operator.lt
    elif reverse and not strict:
        op = operator.le
    elif strict:
        op = operator.gt
    else:
        op = operator.ge

    it = iter(table)
    try:
        flds = [text_type(f) for f in next(it)]
    except StopIteration:
        flds = []
    if key is not None:
        getkey = comparable_itemgetter(*asindices(flds, key))
    elif flds:
        getkey = comparable_itemgetter(*range(len(flds)))
    else:
        getkey = lambda row: row  # no fields to select in a zero-column table
    try:
        prev = next(it)
    except StopIteration:
        return True  # no data rows
    prevkey = getkey(prev)
    for curr in it:
        currkey = getkey(curr)
        if not op(currkey, prevkey):
            return False
        prevkey = currkey
    return True


Table.issorted = issorted
