from __future__ import absolute_import, print_function, division


import gc
import gzip
import os

from petl.test.helpers import ieq
from petl.transform.sorts import sort


A = [('id', 'amount'), ('A2', 12), ('A1', 11), ('A3', 13)]
B = [('amount', 'id'), (23, 'B3'), (21, 'B1'), (22, 'B2')]
C = [('id', 'amount'), ('C3', 33), ('C1', 31), ('C2', 32)]

SORTED_A = [('id', 'amount'), ('A1', 11), ('A2', 12), ('A3', 13)]
SORTED_B = [('amount', 'id'), (21, 'B1'), (22, 'B2'), (23, 'B3')]
SORTED_C = [('id', 'amount'), ('C1', 31), ('C2', 32), ('C3', 33)]


class _BatchSource(object):
    """An upstream that holds one batch at a time and is read sequentially."""

    def __init__(self, batch):
        self.batch = batch
        self.opens = []

    def __iter__(self):
        # A stream that is already open keeps reading the batch it started
        # on, even when the caller points the source at a new batch.
        batch = self.batch
        self.opens.append(batch)

        def gen():
            for row in batch:
                yield row

        return gen()


def _get_names(filecache):
    return [wrapper.name for wrapper in filecache]


def _readgzip(filename):
    with gzip.open(filename, 'rt', encoding='utf-8', newline='') as f:
        return f.read()


def test_sort_batch_header_binding(tmpdir):
    source = _BatchSource(A)
    view = sort(source, key='id', buffersize=2, tempdir=str(tmpdir))

    ieq(SORTED_A, view)

    # Only a header is needed to bind a reader to the current batch. The
    # archive stops here and must not be disturbed by later batch switches.
    archive = iter(view.teecsv(str(tmpdir.join('a.csv.gz')), encoding='utf-8'))
    assert next(archive) == ('id', 'amount')

    source.batch = B
    view.clearcache()
    b1 = iter(view)
    b2 = iter(view)
    assert next(b1) == ('amount', 'id')
    assert next(b2) == ('amount', 'id')

    source.batch = C
    view.clearcache()
    c_file = str(tmpdir.join('c.csv.gz'))
    ieq(SORTED_C, view.teecsv(c_file, encoding='utf-8'))

    b1.close()
    assert list(b2) == [(21, 'B1'), (22, 'B2'), (23, 'B3')]
    assert list(archive) == [('A1', 11), ('A2', 12), ('A3', 13)]

    assert _readgzip(str(tmpdir.join('a.csv.gz'))) == (
        'id,amount\r\nA1,11\r\nA2,12\r\nA3,13\r\n'
    )
    assert _readgzip(c_file) == (
        'id,amount\r\nC1,31\r\nC2,32\r\nC3,33\r\n'
    )

    # The current batch stays cached; reading again opens no new stream.
    opens = len(source.opens)
    ieq(SORTED_C, view)
    assert len(source.opens) == opens


def test_sort_batch_header_only_reader_does_not_consume_source(tmpdir):
    source = _BatchSource(A)
    view = sort(source, key='id', buffersize=2, tempdir=str(tmpdir))

    first = iter(view)
    assert next(first) == ('id', 'amount')

    # A second reader starts after the header was taken. It must still be
    # able to populate the batch cache itself and read every row.
    second = iter(view)
    assert next(second) == ('id', 'amount')
    rows = list(second)
    assert rows == [('A1', 11), ('A2', 12), ('A3', 13)]

    # The first reader can then share the completed cache.
    assert list(first) == rows


def test_sort_batch_header_only_reader_exits(tmpdir):
    source = _BatchSource(A)
    view = sort(source, key='id', buffersize=2, tempdir=str(tmpdir))

    first = iter(view)
    assert next(first) == ('id', 'amount')
    first.close()

    second = iter(view)
    assert next(second) == ('id', 'amount')
    assert list(second) == [('A1', 11), ('A2', 12), ('A3', 13)]
    assert len(source.opens) == 1


def test_sort_batch_open_reader_keeps_old_batch(tmpdir):
    source = _BatchSource(A)
    view = sort(source, key='id', buffersize=2, tempdir=str(tmpdir))

    # Bind an A reader to its batch without asking for any data rows yet.
    a = iter(view)
    assert next(a) == ('id', 'amount')

    source.batch = B
    view.clearcache()
    b = iter(view)
    assert next(b) == ('amount', 'id')
    assert list(b) == [(21, 'B1'), (22, 'B2'), (23, 'B3')]

    # The already open reader still reads A from the stream it captured.
    assert list(a) == [('A1', 11), ('A2', 12), ('A3', 13)]
    assert source.opens == [A, B]


def test_sort_batch_shared_single_source_read(tmpdir):
    source = _BatchSource(A)
    view = sort(source, key='id', buffersize=2, tempdir=str(tmpdir))

    first = iter(view)
    second = iter(view)
    assert next(first) == next(second) == ('id', 'amount')
    assert next(first) == ('A1', 11)
    assert next(second) == ('A1', 11)
    assert list(first) == [('A2', 12), ('A3', 13)]
    assert list(second) == [('A2', 12), ('A3', 13)]
    assert len(source.opens) == 1


def test_sort_batch_cleanup_after_last_reader(tmpdir):
    source = _BatchSource(A)
    view = sort(source, key='id', buffersize=2, tempdir=str(tmpdir))

    ieq(SORTED_A, view)
    a_files = _get_names(view._filecache)
    assert len(a_files) == 2
    assert all(os.path.exists(fn) for fn in a_files)

    # A reader that only takes the header still holds the batch alive.
    a = iter(view)
    assert next(a) == ('id', 'amount')

    source.batch = B
    view.clearcache()
    b = iter(view)
    assert next(b) == ('amount', 'id')

    # The retired batch keeps its temporary files while an iterator that
    # bound to it is still open.
    gc.collect()
    assert all(os.path.exists(fn) for fn in a_files)

    a.close()
    del a
    gc.collect()
    assert not any(os.path.exists(fn) for fn in a_files)

    # The current batch remains readable from its cache.
    assert list(view) == SORTED_B


def test_sort_batch_uncached_reads_each_time():
    source = _BatchSource(A)
    view = sort(source, key='id', buffersize=2, cache=False)

    ieq(SORTED_A, view)
    ieq(SORTED_A, view)
    assert len(source.opens) == 2

    source.batch = B
    view.clearcache()
    ieq(SORTED_B, view)
    assert len(source.opens) == 3
