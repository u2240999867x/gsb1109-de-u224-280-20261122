# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

# standard library dependencies
import copy
import io
import json
import inspect
import threading
from itertools import chain, islice
from json.encoder import JSONEncoder
from os import unlink
from tempfile import NamedTemporaryFile

from petl.compat import pickle
from petl.errors import ArgumentError, ResumeError
from petl.io.sources import read_source_from_arg, write_source_from_arg
# internal dependencies
from petl.util.base import data, Table, dicts as _dicts


def fromjson(source, *args, **kwargs):
    """
    Extract data from a JSON file. The file must contain a JSON array as
    the top level object, and each member of the array will be treated as a
    row of data. E.g.::

        >>> import petl as etl
        >>> data = '''
        ... [{"foo": "a", "bar": 1},
        ... {"foo": "b", "bar": 2},
        ... {"foo": "c", "bar": 2}]
        ... '''
        >>> with open('example.file1.json', 'w') as f:
        ...     f.write(data)
        ...
        74
        >>> table1 = etl.fromjson('example.file1.json', header=['foo', 'bar'])
        >>> table1
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+
        | 'c' |   2 |
        +-----+-----+

    Setting argument `lines` to `True` will enable to
    infer the document as a JSON lines document. For more details about JSON lines
    please visit https://jsonlines.org/.

    An empty JSON lines file produces a table with no data rows. If no
    `header` is supplied, the table has an empty header tuple.

    An explicit `header` iterable is read once on first iteration and reused
    for every row and subsequent pass over the table.

    Additional arguments are passed to :func:`json.load`, or to
    :func:`json.loads` for each JSON line. For example, ``parse_float=Decimal``
    preserves decimal precision in both formats. Decoder options also apply
    when discovering the header from the first JSON line. The decoded first
    record is reused as the first data row.

        >>> import petl as etl
        >>> data_with_jlines = '''{"name": "Gilbert", "wins": [["straight", "7S"], ["one pair", "10H"]]}
        ... {"name": "Alexa", "wins": [["two pair", "4S"], ["two pair", "9S"]]}
        ... {"name": "May", "wins": []}
        ... {"name": "Deloise", "wins": [["three of a kind", "5S"]]}'''
        ...
        >>> with open('example.file2.json', 'w') as f:
        ...     f.write(data_with_jlines)
        ...
        223
        >>> table2 = etl.fromjson('example.file2.json', lines=True)
        >>> table2
        +-----------+-------------------------------------------+
        | name      | wins                                      |
        +===========+===========================================+
        | 'Gilbert' | [['straight', '7S'], ['one pair', '10H']] |
        +-----------+-------------------------------------------+
        | 'Alexa'   | [['two pair', '4S'], ['two pair', '9S']]  |
        +-----------+-------------------------------------------+
        | 'May'     | []                                        |
        +-----------+-------------------------------------------+
        | 'Deloise' | [['three of a kind', '5S']]               |
        +-----------+-------------------------------------------+

    If your JSON file does not fit this structure, you will need to parse it
    via :func:`json.load` and select the array to treat as the data, see also
    :func:`petl.io.json.fromdicts`.

    .. versionchanged:: 1.1.0

    If no `header` is specified, fields will be discovered by sampling keys
    from the first `sample` objects in `source`. The header will be
    constructed from keys in the order discovered. Note that this
    ordering may not be stable, and therefore it may be advisable to specify
    an explicit `header` or to use another function like
    :func:`petl.transform.headers.sortheader` on the resulting table to
    guarantee stability.

    With `sample=1`, only the first object's keys are used. An empty input
    array produces an empty header and no data rows.

    """

    source = read_source_from_arg(source)
    return JsonView(source, *args, **kwargs)


class JsonView(Table):
    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.missing = kwargs.pop('missing', None)
        self._header = kwargs.pop('header', None)
        self.sample = kwargs.pop('sample', 1000)
        self.lines = kwargs.pop('lines', False)
        self.args = args
        self.kwargs = kwargs

    def __iter__(self):
        if self._header is not None:
            self._header = tuple(self._header)
        with self.source.open('rb') as f:
            # wrap buffer for text IO
            f = io.TextIOWrapper(f, encoding='utf-8', newline='',
                                 write_through=True)
            try:
                if self.lines:
                    for row in iterjlines(f, self._header, self.missing,
                                         self.args, self.kwargs):
                        yield row
                else:
                    dicts = json.load(f, *self.args, **self.kwargs)
                    for row in iterdicts(dicts, self._header, self.sample,
                                         self.missing):
                        yield row
            finally:
                f.detach()


def fromdicts(dicts, header=None, sample=1000, missing=None, resumable=False,
              sequence=None):
    """
    View a sequence of Python :class:`dict` as a table. E.g.::

        >>> import petl as etl
        >>> dicts = [{"foo": "a", "bar": 1},
        ...          {"foo": "b", "bar": 2},
        ...          {"foo": "c", "bar": 2}]
        >>> table1 = etl.fromdicts(dicts, header=['foo', 'bar'])
        >>> table1
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+
        | 'c' |   2 |
        +-----+-----+

    An explicit `header` iterable is read once on first iteration and reused
    for every row and subsequent pass over the table.

    Argument `dicts` can also be a generator, the output of generator
    is iterated and cached using a temporary file to support further
    transforms and multiple passes of the table:

        >>> import petl as etl
        >>> dicts = ({"foo": chr(ord("a")+i), "bar":i+1} for i in range(3))
        >>> table1 = etl.fromdicts(dicts, header=['foo', 'bar'])
        >>> table1
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+
        | 'c' |   3 |
        +-----+-----+

    If `header` is not specified, `sample` items from `dicts` will be
    inspected to discovery dictionary keys. Note that the order in which
    dictionary keys are discovered may not be stable,

    A sample size of one discovers the first dictionary's keys. Empty
    inputs produce an empty header and no data rows, including with
    `sample=1`.

    See also :func:`petl.io.json.fromjson`.

    .. versionchanged:: 1.1.0

    If no `header` is specified, fields will be discovered by sampling keys
    from the first `sample` dictionaries in `dicts`. The header will be
    constructed from keys in the order discovered. Note that this
    ordering may not be stable, and therefore it may be advisable to specify
    an explicit `header` or to use another function like
    :func:`petl.transform.headers.sortheader` on the resulting table to
    guarantee stability.

    .. versionchanged:: 1.7.5

    Full support of generators passed as `dicts` has been added, leveraging
    `itertools.tee`.

    .. versionchanged:: 1.7.11

    Generator support has been modified to use temporary file cache
    instead of `itertools.tee` due to high memory usage.

    .. versionchanged:: 1.8.0

    With ``resumable=True`` the input is cached on disk and the table
    supports several independent readers and explicit resumption after a
    broken source stream; the `sequence` argument names the field holding
    the record sequence, defaulting to the first header field. See
    :func:`petl.io.json.fromresumable`.

    """
    if resumable:
        return fromresumable(dicts, header=header, sample=sample,
                             missing=missing, sequence=sequence)
    view = DictsGeneratorView if inspect.isgenerator(dicts) else DictsView
    return view(dicts, header=header, sample=sample, missing=missing)


def fromresumable(dicts, header, sample=1000, missing=None, sequence=None):
    """
    View a stream of dictionaries that may be delivered in several attempts
    as a resumable, multi-reader table. Each record carries a monotonically
    increasing integer sequence number in the field named by `sequence`,
    which defaults to the first field of `header`. E.g.::

        >>> import petl as etl
        >>> table = etl.fromresumable(
        ...     [{'seq': 1, 'value': 'a'}, {'seq': 2, 'value': 'b'}],
        ...     header=['seq', 'value'])
        >>> list(table)
        [('seq', 'value'), (1, 'a'), (2, 'b')]

    Unlike :func:`petl.io.json.fromdicts` with a generator, the source is
    read at most once. Records received from the source are written to a
    temporary file owned by the table, and every reader created by calling
    the table keeps its own position in that cache. A reader only pulls from
    the source when it has caught up with the newest cached record, so
    memory usage does not grow with the distance between readers', and the
    declared header values are captured when the record is received rather
    than read later from the source.

    Calling the table returns a new independent reader::

        >>> audit = table()

    Readers and the table share the same header. Iterating a reader yields
    the header first, then the records the reader has not seen yet. Calling
    :meth:`~petl.io.json.ResumableView.header` returns the header without
    pulling any data from the source.

    The source may raise any exception to signal a broken stream, e.g. a
    gateway disconnection. The exception is stored at the sequence of the
    last record received and is re-raised by every reader that reaches that
    point. A new reader created after the break stops at the break, so it
    also sees the exception unless the stream is resumed.

    After a break, the collector calls
    :meth:`~petl.io.json.ResumableView.resume` with the sequence number it
    will resend from and a new iterable of dictionaries. The new stream must
    contain, in order, every record from that sequence up to the last record
    received before the break. Records are verified against the table cache
    by sequence number and by every field named in the header. Resent
    records are not appended to the table and no new record is read:
    verification stops at the last record received before the break.

    If a resent record is missing, or differs, or the new stream breaks
    during verification, the new stream is closed, and the raised
    :class:`petl.errors.ResumeError` carries the offending sequence number.
    The previously received records and the original break remain readable,
    and the collector may call `resume()` again. After a successful
    resumption, readers created earlier still see the original break, while
    new readers and new records continue in sequence.

    Calling :meth:`~petl.io.json.ResumableView.close` stops accepting new
    readers and new streams, but readers that already exist may still read
    to the end. The last reader to leave releases the table's source and
    temporary file.

    """
    return ResumableView(dicts, header=header, sample=sample,
                         missing=missing, sequence=sequence)


class _ResumableReader(Table):
    """An independent branch over a :class:`ResumableView` cache."""

    def __init__(self, view):
        self._view = view
        self._position = 0
        self._epoch = view._epoch
        self._started = False
        self._finished = False
        self._error = None
        self._released = False

    def header(self):
        return self._view.header()

    def __iter__(self):
        return self

    def __next__(self):
        if self._error is not None:
            raise self._error
        if self._finished:
            raise StopIteration
        if not self._started:
            self._started = True
            return self._view.header()
        row, self._position, error = self._view._next(
            self._position, self._epoch)
        if error is not None:
            self._error = error
            self._finish()
            raise error
        if row is None:
            self._finish()
            raise StopIteration
        return row

    next = __next__

    def __len__(self):
        raise TypeError('length of a resumable branch is not known')

    def close(self):
        self._finish()

    def _finish(self):
        self._finished = True
        if not self._released:
            self._released = True
            self._view._release()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    def __del__(self):
        try:
            self._finish()
        except Exception:
            pass


class ResumableView(Table):

    def __init__(self, dicts, header, sample=1000, missing=None,
                 sequence=None):
        if header is None:
            raise ArgumentError('resumable input requires an explicit header')
        self._header = tuple(header)
        self._sequence_field = sequence
        if self._sequence_field is None and self._header:
            self._sequence_field = self._header[0]
            self._sequence_default = True
        else:
            self._sequence_default = False
        self.sample = sample
        self.missing = missing
        self._source = iter(dicts)
        self._source_owner = dicts
        self._source_done = False
        self._first_sequence = None
        self._last_sequence = None
        self._file = None
        self._cached = 0
        self._breaks = []
        self._epoch = 0
        self._closed = False
        self._finished = False
        self._readers = 0
        self._lock = threading.RLock()

    def __iter__(self):
        return self()

    def __call__(self):
        with self._lock:
            if self._closed:
                raise ValueError('resumable table is closed')
            self._readers += 1
            return _ResumableReader(self)

    open = __call__
    reader = __call__
    branch = __call__

    def header(self):
        return self._header

    def resume(self, start, dicts):
        with self._lock:
            if self._closed:
                raise ValueError('resumable table is closed')
            if isinstance(start, bool) or not isinstance(start, int):
                raise ArgumentError('resume start must be an integer')
            if not self._breaks:
                raise ResumeError(start, 'no stream break to resume')
            break_ = None
            for candidate in self._breaks:
                if candidate.resolved_epoch is None:
                    break_ = candidate
            if break_ is None:
                raise ResumeError(start, 'no unresolved stream break to '
                                         'resume')
            received = self._last_sequence
            if received is None:
                raise ResumeError(start, 'no records were received before the '
                                         'break')
            if break_.after is not None and start > break_.after:
                raise ResumeError(start, 'start is after the last record '
                                         'received before the break, %r'
                                         % (break_.after,))
            if start < self._first_sequence or start > received:
                raise ResumeError(start, 'start is outside the received '
                                         'range %r..%r'
                                         % (self._first_sequence, received))
            it = iter(dicts)
            cached = self._cached_from(start)
            expected = start
            try:
                for record in it:
                    sequence, row = self._record_parts(record)
                    if isinstance(sequence, bool) or \
                            not isinstance(sequence, int) or \
                            sequence != expected:
                        raise ResumeError(expected, 'expected sequence %r, '
                                                   'found %r'
                                                   % (expected, sequence))
                    try:
                        cached_row = next(cached)
                    except StopIteration:
                        raise ResumeError(expected, 'record was not received '
                                                   'before the break')
                    if cached_row != row:
                        raise ResumeError(expected, 'record differs from the '
                                                   'one already received')
                    if expected == received:
                        self._source = it
                        self._source_owner = dicts
                        self._source_done = False
                        self._epoch += 1
                        break_.resolved_epoch = self._epoch
                        for pending in self._breaks:
                            if pending.resolved_epoch is None:
                                pending.resolved_epoch = self._epoch
                        return self
                    expected += 1
            except ResumeError:
                _close_iterable(dicts)
                _close_iterable(it)
                raise
            except Exception as error:
                _close_iterable(dicts)
                _close_iterable(it)
                raise ResumeError(expected, 'stream broke while verifying: '
                                           '%s' % (error,), error=error)
            _close_iterable(dicts)
            _close_iterable(it)
            raise ResumeError(expected, 'resend stream ended before the last '
                                       'received record %r' % (break_.after,))

    resend = resume
    handoff = resume
    reconnect = resume
    deliver = resume

    def close(self):
        with self._lock:
            self._closed = True
            if self._readers == 0:
                self._finish()

    @property
    def closed(self):
        return self._closed

    def _next(self, position, epoch):
        with self._lock:
            error = self._error_for(position, epoch)
            if error is not None:
                return None, position, error
            if position < self._cached:
                self._file.seek(position)
                sequence, row = pickle.load(self._file)
                return row, self._file.tell(), None
            if self._source_done:
                return None, position, None
            return self._pull(position)

    def _pull(self, position):
        try:
            record = next(self._source)
        except StopIteration:
            self._source_done = True
            return None, position, None
        except Exception as error:
            self._break(self._last_sequence, error)
            return None, position, error
        try:
            sequence, row = self._record_parts(record)
            if isinstance(sequence, bool) or not isinstance(sequence, int):
                raise ResumeError(self._last_sequence,
                                  'record has no integer sequence number')
            if self._last_sequence is None:
                self._first_sequence = sequence
            elif sequence != self._last_sequence + 1:
                raise ResumeError(sequence,
                                  'expected consecutive sequence %r, found %r'
                                  % (self._last_sequence + 1, sequence))
        except ResumeError as error:
            self._break(self._last_sequence, error)
            return None, position, error
        self._ensure_file()
        self._file.seek(self._cached)
        pickle.dump((sequence, row), self._file, protocol=-1)
        self._cached = position = self._file.tell()
        self._last_sequence = sequence
        return row, position, None

    def _record_parts(self, record):
        if isinstance(record, dict):
            return self._sequence_of(record), self._row(record)
        if not isinstance(record, (tuple, list)) or len(record) != 2:
            raise ResumeError(self._last_sequence,
                              'record is not a dictionary or sequence pair')
        sequence, payload = record
        if isinstance(payload, dict):
            value = payload.get(self._sequence_field, sequence) \
                if self._sequence_field is not None else sequence
            if isinstance(value, bool) or not isinstance(value, int) or \
                    value != sequence:
                raise ResumeError(sequence, 'sequence field %r contradicts '
                                            'the record sequence'
                                            % (self._sequence_field,))
            return sequence, self._row(payload, sequence)
        if isinstance(payload, (tuple, list)) and \
                len(payload) == len(self._header):
            row = tuple(copy.deepcopy(value) for value in payload)
            if self._sequence_field is not None and \
                    self._sequence_field in self._header:
                value = row[self._header.index(self._sequence_field)]
                if isinstance(value, bool) or not isinstance(value, int) or \
                        value != sequence:
                    raise ResumeError(sequence, 'sequence field %r contradicts '
                                                'the record sequence'
                                                % (self._sequence_field,))
            return sequence, row
        raise ResumeError(self._last_sequence,
                          'record is not a dictionary or row pair')

    def _sequence_of(self, record):
        if self._sequence_field is not None:
            value = record.get(self._sequence_field, self.missing)
            if isinstance(value, int) and not isinstance(value, bool):
                return value
            if self._sequence_default:
                for name in ('seq', 'sequence'):
                    if name in record:
                        return record[name]
            return value
        for name in ('seq', 'sequence'):
            if name in record:
                return record[name]
        return self.missing

    def _row(self, record, sequence=None):
        row = []
        for field in self._header:
            if sequence is not None and field == self._sequence_field:
                row.append(copy.deepcopy(sequence))
            else:
                row.append(copy.deepcopy(record.get(field, self.missing)))
        return tuple(row)

    def _break(self, after, error):
        if not isinstance(error, ResumeError):
            for name in ('sequence', 'position', 'index', 'seq'):
                try:
                    setattr(error, name, after)
                except Exception:
                    pass
        self._breaks.append(_Break(after, self._cached, error))
        self._source_done = True
        _close_iterable(self._source_owner)
        _close_iterable(self._source)

    def _error_for(self, position, epoch):
        for break_ in self._breaks:
            if position < break_.offset:
                continue
            if break_.resolved_epoch is not None and \
                    epoch >= break_.resolved_epoch:
                continue
            return break_.error
        return None

    def _cached_from(self, sequence):
        self._ensure_file()
        self._file.seek(0)
        try:
            while True:
                entry = pickle.load(self._file)
                if entry[0] >= sequence:
                    break
        except EOFError:
            raise ResumeError(sequence, 'record was not received before the '
                                       'break')
        while True:
            yield entry[1]
            try:
                entry = pickle.load(self._file)
            except EOFError:
                return

    def _ensure_file(self):
        if self._file is None:
            self._file = NamedTemporaryFile(delete=False, mode='wb+',
                                            buffering=0)

    def _release(self):
        with self._lock:
            self._readers -= 1
            if self._readers == 0 and self._closed:
                self._finish()

    def _finish(self):
        with self._lock:
            if self._readers > 0 or self._finished:
                return
            self._finished = True
            _close_iterable(self._source_owner)
            _close_iterable(self._source)
            self._source_done = True
            if self._file is not None:
                self._file.close()
                unlink(self._file.name)
                self._file = None

    @property
    def nreaders(self):
        return self._readers

    @property
    def finished(self):
        return self._finished

    @property
    def first_sequence(self):
        return self._first_sequence

    @property
    def last_sequence(self):
        return self._last_sequence

    received_sequence = last_sequence

    @property
    def break_sequence(self):
        return self._breaks[-1].after if self._breaks else None

    @property
    def break_error(self):
        return self._breaks[-1].error if self._breaks else None

    @property
    def tempfile(self):
        return self._file.name if self._file is not None else None

    def __del__(self):
        if getattr(self, '_finished', True):
            return
        try:
            _close_iterable(getattr(self, '_source_owner', ()))
        except Exception:
            pass
        try:
            _close_iterable(getattr(self, '_source', ()))
        except Exception:
            pass
        if getattr(self, '_file', None) is not None:
            try:
                self._file.close()
                unlink(self._file.name)
            except Exception:
                pass
            self._file = None


class _Break(object):

    def __init__(self, after, offset, error):
        self.after = after
        self.offset = offset
        self.error = error
        self.resolved_epoch = None


def _close_iterable(it):
    close = getattr(it, 'close', None)
    if close is not None:
        try:
            close()
        except Exception:
            pass


class _DictsAttr(object):
    """Reads as the input data, calls as the inherited Table.dicts() method.

    Only exists to keep `.dicts` working both ways in the v1.x series; v2 can
    drop it along with the property below.
    """

    def __init__(self, view):
        self._view = view

    def __call__(self, *sliceargs, **kwargs):
        return _dicts(self._view, *sliceargs, **kwargs)

    def __iter__(self):
        return iter(self._view._dicts)

    def __next__(self):
        return next(self._view._dicts)

    next = __next__

    def __len__(self):
        return len(self._view._dicts)

    def __getitem__(self, item):
        return self._view._dicts[item]

    def __eq__(self, other):
        return self._view._dicts == other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._view._dicts)

    def __repr__(self):
        return repr(self._view._dicts)

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        return getattr(self._view._dicts, name)


class DictsView(Table):

    def __init__(self, dicts, header=None, sample=1000, missing=None):
        self._dicts = dicts
        self._header = header
        self.sample = sample
        self.missing = missing

    def __iter__(self):
        if self._header is not None:
            self._header = tuple(self._header)
        for row in iterdicts(self._dicts, self._header, self.sample, self.missing):  # noqa: UP028 - Python 2 compatibility
            yield row

    @property
    def dicts(self):
        # `.dicts` was the raw input data, which shadowed Table.dicts() (#643).
        # A property on the subclass wins over the inherited method, so both
        # uses keep working until v2.
        return _DictsAttr(self)

    @dicts.setter
    def dicts(self, value):
        self._dicts = value


class DictsGeneratorView(DictsView):

    def __init__(self, dicts, header=None, sample=1000, missing=None):
        super(DictsGeneratorView, self).__init__(dicts, header, sample, missing)
        self._filecache = None
        self._cached = 0

    def __iter__(self):
        if self._header is None:
            self._determine_header()
        else:
            self._header = tuple(self._header)
        yield self._header

        if not self._filecache:
            self._filecache = NamedTemporaryFile(delete=False, mode='wb+', buffering=0)

        position = 0
        it = iter(self._dicts)
        while True:
            if position < self._cached:
                self._filecache.seek(position)
                row = pickle.load(self._filecache)
                position = self._filecache.tell()
                yield row
                continue
            try:
                o = next(it)
            except StopIteration:
                break
            row = tuple(o.get(f, self.missing) for f in self._header)
            self._filecache.seek(self._cached)
            pickle.dump(row, self._filecache, protocol=-1)
            self._cached = position = self._filecache.tell()
            yield row

    def _determine_header(self):
        it = iter(self._dicts)
        header = list()
        peek = list(islice(it, self.sample))
        it = chain(peek, it)
        self._dicts = it
        for o in peek:
            if hasattr(o, 'keys'):
                header += [k for k in o.keys() if k not in header]
        self._header = tuple(header)
        return it

    def __del__(self):
        if self._filecache:
            self._filecache.close()
            unlink(self._filecache.name)


def iterjlines(f, header, missing, args=(), kwargs=None):
    if kwargs is None:
        kwargs = {}
    it = iter(f)
    first = ()

    if header is None:
        header = list()
        try:
            peek = next(it)
        except StopIteration:
            yield tuple()
            return
        json_obj = json.loads(peek, *args, **kwargs)
        first = (json_obj,)
        if hasattr(json_obj, 'keys'):
            header += [k for k in json_obj.keys() if k not in header]
    yield tuple(header)

    for json_obj in chain(first, (json.loads(o, *args, **kwargs) for o in it)):
        yield tuple(json_obj[f] if f in json_obj else missing for f in header)


def iterdicts(dicts, header, sample, missing):
    it = iter(dicts)

    # determine header row
    if header is None:
        # discover fields
        header = list()
        peek = list(islice(it, sample))
        it = chain(peek, it)
        for o in peek:
            if hasattr(o, 'keys'):
                header += [k for k in o.keys() if k not in header]
    yield tuple(header)

    # generate data rows
    for o in it:
        yield tuple(o.get(f, missing) for f in header)


def tojson(table, source=None, prefix=None, suffix=None, *args, **kwargs):
    """
    Write a table in JSON format, with rows output as JSON objects. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> etl.tojson(table1, 'example.file3.json', sort_keys=True)
        >>> # check what it did
        ... print(open('example.file3.json').read())
        [{"bar": 1, "foo": "a"}, {"bar": 2, "foo": "b"}, {"bar": 2, "foo": "c"}]



    Setting argument `lines` to `True` will enable to
    infer the writing format as a JSON lines . For more details about JSON lines
    please visit https://jsonlines.org/.

        >>> import petl as etl
        >>> table1 = [['name', 'wins'],
        ...           ['Gilbert', [['straight', '7S'], ['one pair', '10H']]],
        ...           ['Alexa', [['two pair', '4S'], ['two pair', '9S']]],
        ...           ['May', []],
        ...           ['Deloise',[['three of a kind', '5S']]]]
        >>> etl.tojson(table1, 'example.file3.jsonl', lines = True, sort_keys=True)
        >>> # check what it did
        ... print(open('example.file3.jsonl').read())
        {"name": "Gilbert", "wins": [["straight", "7S"], ["one pair", "10H"]]}
        {"name": "Alexa", "wins": [["two pair", "4S"], ["two pair", "9S"]]}
        {"name": "May", "wins": []}
        {"name": "Deloise", "wins": [["three of a kind", "5S"]]}

    Note that this is currently not streaming, all data is loaded into memory
    before being written to the file.

    """

    obj = [row for row in _dicts(table)]
    _writejson(source, obj, prefix, suffix, *args, **kwargs)


Table.tojson = tojson


def tojsonarrays(table, source=None, prefix=None, suffix=None,
                 output_header=False, *args, **kwargs):
    """
    Write a table in JSON format, with rows output as JSON arrays. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> etl.tojsonarrays(table1, 'example.file4.json')
        >>> # check what it did
        ... print(open('example.file4.json').read())
        [["a", 1], ["b", 2], ["c", 2]]

    Note that this is currently not streaming, all data is loaded into memory
    before being written to the file.

    """

    if output_header:
        obj = [row for row in table]
    else:
        obj = [row for row in data(table)]
    _writejson(source, obj, prefix, suffix, *args, **kwargs)


Table.tojsonarrays = tojsonarrays


def _writejson(source, obj, prefix, suffix, *args, **kwargs):
    lines = kwargs.pop('lines', False)
    encoder = JSONEncoder(*args, **kwargs)
    source = write_source_from_arg(source)
    with source.open('wb') as f:
        # wrap buffer for text IO
        f = io.TextIOWrapper(f, encoding='utf-8', newline='',
                             write_through=True)
        try:
            _writeobj(encoder, obj, f, prefix, suffix, lines=lines)
        finally:
            f.detach()


def _writeobj(encoder, obj, f, prefix, suffix, lines=False):
    if prefix is not None:
        f.write(prefix)
    if lines:
        for rec in obj:
            for chunk in encoder.iterencode(rec):
                f.write(chunk)
            f.write('\n')
    else:
        for chunk in encoder.iterencode(obj):
            f.write(chunk)
    if suffix is not None:
        f.write(suffix)
