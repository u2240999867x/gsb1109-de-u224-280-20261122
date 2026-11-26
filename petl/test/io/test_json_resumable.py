# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import os

import pytest

from petl import fromdicts, fromresumable
from petl.errors import ResumeError

HEADER = ['seq', 'value']


def _records(*rows):
    return [{'seq': seq, 'value': value} for seq, value in rows]


class _Stream(object):

    def __init__(self, records, fail_after=None, error=None):
        self._it = iter(records)
        self._fail_after = fail_after
        self._error = error
        self._n = 0
        self.closed = False
        self.pulled = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._fail_after is not None and self._n >= self._fail_after:
            raise self._error
        try:
            record = next(self._it)
        except StopIteration:
            raise
        self._n += 1
        self.pulled += 1
        return record

    next = __next__

    def close(self):
        self.closed = True


def _drain(reader, limit=None):
    rows = []
    for row in reader:
        if limit is not None and len(rows) >= limit:
            break
        rows.append(tuple(row))
    return rows


def _expect_error(reader):
    rows = []
    error = None
    try:
        for row in reader:
            rows.append(tuple(row))
    except Exception as exc:
        error = exc
    return rows, error


def _read_rows(reader, n):
    rows = []
    for _ in range(n):
        try:
            rows.append(tuple(next(reader)))
        except StopIteration:
            break
    return rows


def test_fromdicts_resumable_flag():
    stream = _Stream(_records((1, 'a'), (2, 'b')))
    table = fromdicts(stream, header=HEADER, resumable=True)
    assert table.header() == ('seq', 'value')
    assert list(table()) == [('seq', 'value'), (1, 'a'), (2, 'b')]


def test_header_does_not_pull_source():
    stream = _Stream(_records((1, 'a'), (2, 'b')))
    table = fromresumable(stream, header=HEADER)
    assert table.header() == ('seq', 'value')
    assert stream.pulled == 0
    assert table().header() == ('seq', 'value')
    assert stream.pulled == 0
    assert list(table()) == [('seq', 'value'), (1, 'a'), (2, 'b')]
    assert stream.pulled == 2


def test_sequence_field_outside_header():
    table = fromresumable([{'seq': 1, 'a': 'x'}, {'seq': 2, 'a': 'y'}],
                          header=['a'], sequence='seq')
    assert table.header() == ('a',)
    assert list(table()) == [('a',), ('x',), ('y',)]


def test_source_cached_against_mutation():
    record = {'seq': 1, 'value': ['a']}
    table = fromresumable([record], header=HEADER)
    assert list(table()) == [('seq', 'value'), (1, ['a'])]
    record['value'].append('b')
    assert list(table()) == [('seq', 'value'), (1, ['a'])]


def test_independent_readers_and_lazy_source():
    stream = _Stream(_records((1, 'a'), (2, 'b'), (3, 'c')))
    table = fromresumable(stream, header=HEADER)
    first = table()
    assert next(first) == ('seq', 'value')
    assert next(first) == (1, 'a')
    assert stream.pulled == 1
    second = table()
    assert next(second) == ('seq', 'value')
    assert next(second) == (1, 'a')
    assert stream.pulled == 1
    assert next(second) == (2, 'b')
    assert stream.pulled == 2
    assert next(first) == (2, 'b')
    assert stream.pulled == 2


def test_break_visible_to_readers_behind_only():
    stream = _Stream(_records((1, 'a'), (2, 'b'), (3, 'c')),
                     fail_after=2, error=RuntimeError('gateway down'))
    table = fromresumable(stream, header=HEADER)
    ahead = table()
    rows_ahead = _read_rows(ahead, 3)
    assert rows_ahead == [('seq', 'value'), (1, 'a'), (2, 'b')]
    behind = table()
    rows, error = _expect_error(behind)
    assert rows == [('seq', 'value'), (1, 'a'), (2, 'b')]
    assert isinstance(error, RuntimeError)
    assert str(error) == 'gateway down'


def test_resume_rejects_changed_record():
    stream = _Stream(_records((1, 'a'), (2, 'b'), (3, 'c')),
                     fail_after=3, error=RuntimeError('gateway down'))
    table = fromresumable(stream, header=HEADER)
    reader = table()
    rows, error = _expect_error(reader)
    assert rows == [('seq', 'value'), (1, 'a'), (2, 'b'), (3, 'c')]
    assert str(error) == 'gateway down'
    resend = _Stream(_records((2, 'b'), (3, 'X')))
    with pytest.raises(ResumeError) as info:
        table.resume(2, resend)
    assert info.value.sequence == 3
    assert resend.closed
    table.close()


def test_resume_requires_full_resend():
    table = fromresumable(_Stream(_records((1, 'a'), (2, 'b')),
                                 fail_after=2,
                                 error=RuntimeError('gateway down')),
                           header=HEADER)
    reader = table()
    _expect_error(reader)
    with pytest.raises(ResumeError) as info:
        table.resume(2, _Stream([]))
    assert info.value.sequence == 2
    table.close()


def test_resume_gap_is_reported():
    table = fromresumable(_Stream(_records((1, 'a'), (2, 'b'), (3, 'c')),
                                 fail_after=3,
                                 error=RuntimeError('gateway down')),
                           header=HEADER)
    reader = table()
    assert _read_rows(reader, 4) == [('seq', 'value'), (1, 'a'), (2, 'b'),
                                     (3, 'c')]
    rows, error = _expect_error(reader)
    assert str(error) == 'gateway down'
    with pytest.raises(ResumeError) as info:
        table.resume(2, _Stream(_records((2, 'b'), (4, 'd'))))
    assert info.value.sequence == 3
    table.close()


def test_resume_break_during_verification_keeps_original():
    table = fromresumable(_Stream(_records((1, 'a'), (2, 'b'), (3, 'c')),
                                 fail_after=3,
                                 error=RuntimeError('gateway down')),
                           header=HEADER)
    old = table()
    rows, error = _expect_error(old)
    assert rows == [('seq', 'value'), (1, 'a'), (2, 'b'), (3, 'c')]
    assert str(error) == 'gateway down'
    resend = _Stream(_records((2, 'b'), (3, 'c')),
                     fail_after=1, error=RuntimeError('broke again'))
    with pytest.raises(ResumeError) as info:
        table.resume(2, resend)
    assert info.value.sequence == 3
    assert resend.closed
    rows, error = _expect_error(table())
    assert rows == [('seq', 'value'), (1, 'a'), (2, 'b'), (3, 'c')]
    assert str(error) == 'gateway down'
    table.close()


def test_second_readers_after_break_and_success():
    table = fromresumable(
        _Stream(_records((41, 'a'), (42, 'b'), (43, 'c')),
                fail_after=3, error=RuntimeError('gateway down')),
        header=HEADER)
    ingest = table()
    rows, error = _expect_error(ingest)
    assert rows[-1] == (43, 'c')
    assert str(error) == 'gateway down'
    audit = table()
    assert audit.header() == ('seq', 'value')
    rows, error = _expect_error(audit)
    assert rows == [('seq', 'value'), (41, 'a'), (42, 'b'), (43, 'c')]
    assert str(error) == 'gateway down'

    bad = _Stream(_records((42, 'b'), (43, 'X')))
    with pytest.raises(ResumeError) as info:
        table.resume(42, bad)
    assert info.value.sequence == 43

    review = table()
    assert review.header() == ('seq', 'value')

    good = _Stream(_records((42, 'b'), (43, 'c'), (44, 'd'), (45, 'e')))
    table.resume(42, good)
    assert not good.closed

    rows, error = _expect_error(review)
    assert rows == [('seq', 'value'), (41, 'a'), (42, 'b'), (43, 'c')]
    assert str(error) == 'gateway down'

    fresh = table()
    assert list(fresh) == [('seq', 'value'), (41, 'a'), (42, 'b'), (43, 'c'),
                           (44, 'd'), (45, 'e')]


def test_resend_not_pre_read():
    table = fromresumable(
        _Stream(_records((1, 'a'), (2, 'b')), fail_after=2,
                error=RuntimeError('gateway down')),
        header=HEADER)
    reader = table()
    assert _read_rows(reader, 3) == [('seq', 'value'), (1, 'a'), (2, 'b')]
    rows, error = _expect_error(reader)
    assert str(error) == 'gateway down'
    resend = _Stream(_records((2, 'b'), (3, 'c')))
    table.resume(2, resend)
    assert resend.pulled == 1
    new = table()
    assert next(new) == ('seq', 'value')
    assert next(new) == (1, 'a')
    assert next(new) == (2, 'b')
    assert resend.pulled == 1


def test_close_blocks_new_readers_but_existing_drain():
    table = fromresumable(_records((1, 'a'), (2, 'b')), header=HEADER)
    reader = table()
    assert next(reader) == ('seq', 'value')
    table.close()
    with pytest.raises(ValueError):
        table()
    with pytest.raises(ValueError):
        table.resume(1, _records((1, 'a')))
    assert list(reader) == [(1, 'a'), (2, 'b')]


def test_last_reader_releases_temp_file():
    stream = _Stream(_records((1, 'a'), (2, 'b')))
    table = fromresumable(stream, header=HEADER)
    reader = table()
    assert next(reader) == ('seq', 'value')
    assert next(reader) == (1, 'a')
    path = table.tempfile
    assert path is not None
    assert os.path.exists(path)
    assert not table.finished
    table.close()
    assert not table.finished
    assert list(reader) == [(2, 'b')]
    reader.close()
    assert table.finished
    assert not os.path.exists(path)


def test_record_must_be_consecutive():
    table = fromresumable(_Stream([{'seq': 1, 'value': 'a'},
                                   {'seq': 3, 'value': 'c'}]),
                          header=HEADER)
    rows, error = _expect_error(table())
    assert rows == [('seq', 'value'), (1, 'a')]
    assert isinstance(error, ResumeError)
    assert error.sequence == 3
