from __future__ import absolute_import, division, print_function

from petl import (
    antijoin,
    crossjoin,
    cut,
    hashantijoin,
    hashjoin,
    hashleftjoin,
    hashlookupjoin,
    hashrightjoin,
    join,
    leftjoin,
    lookupjoin,
    outerjoin,
    rightjoin,
    sort,
    unjoin,
)
from petl.test.helpers import ieq


def _test_join_basic(join_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))

    # normal inner join
    table3 = join_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = join_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)
    ieq(expect4, table4)  # check twice

    # multiple rows for each key
    table5 = (('id', 'colour'),
              (1, 'blue'),
              (1, 'red'),
              (2, 'purple'))
    table6 = (('id', 'shape'),
              (1, 'circle'),
              (1, 'square'),
              (2, 'ellipse'))
    table7 = join_impl(table5, table6, key='id')
    expect7 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (1, 'blue', 'square'),
               (1, 'red', 'circle'),
               (1, 'red', 'square'),
               (2, 'purple', 'ellipse'))
    ieq(expect7, table7)


def _test_join_compound_keys(join_impl):

    # compound keys
    table8 = (('id', 'time', 'height'),
              (1, 1, 12.3),
              (1, 2, 34.5),
              (2, 1, 56.7))
    table9 = (('id', 'time', 'weight'),
              (1, 2, 4.5),
              (2, 1, 6.7),
              (2, 2, 8.9))
    table10 = join_impl(table8, table9, key=['id', 'time'])
    expect10 = (('id', 'time', 'height', 'weight'),
                (1, 2, 34.5, 4.5),
                (2, 1, 56.7, 6.7))
    ieq(expect10, table10)

    # natural join on compound key
    table11 = join_impl(table8, table9)
    expect11 = expect10
    ieq(expect11, table11)


def _test_join_string_key(join_impl):

    table1 = (('id', 'colour'),
              ('aa', 'blue'),
              ('bb', 'red'),
              ('cc', 'purple'))
    table2 = (('id', 'shape'),
              ('aa', 'circle'),
              ('cc', 'square'),
              ('dd', 'ellipse'))

    # normal inner join
    table3 = join_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               ('aa', 'blue', 'circle'),
               ('cc', 'purple', 'square'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice


def _test_join_empty(join_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),)
    table3 = join_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),)
    ieq(expect3, table3)

    table1 = (('id', 'colour'),)
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = join_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),)
    ieq(expect3, table3)


def _test_join_novaluefield(join_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))

    expect = (('id', 'colour', 'shape'),
              (1, 'blue', 'circle'),
              (3, 'purple', 'square'))

    actual = join_impl(table1, table2, key='id')
    ieq(expect, actual)
    actual = join_impl(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id', 'shape'), actual)
    actual = join_impl(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id', 'colour'), actual)
    actual = join_impl(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def _test_join_prefix(join_impl):

    table1 = (('id', 'colour'),
              ('aa', 'blue'),
              ('bb', 'red'),
              ('cc', 'purple'))
    table2 = (('id', 'shape'),
              ('aa', 'circle'),
              ('cc', 'square'),
              ('dd', 'ellipse'))

    table3 = join_impl(table1, table2, key='id', lprefix='l_', rprefix='r_')
    expect3 = (('l_id', 'l_colour', 'r_shape'),
               ('aa', 'blue', 'circle'),
               ('cc', 'purple', 'square'))
    ieq(expect3, table3)


def _test_join_lrkey(join_impl):

    table1 = (('id', 'colour'),
              ('aa', 'blue'),
              ('bb', 'red'),
              ('cc', 'purple'))
    table2 = (('identifier', 'shape'),
              ('aa', 'circle'),
              ('cc', 'square'),
              ('dd', 'ellipse'))

    table3 = join_impl(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour', 'shape'),
               ('aa', 'blue', 'circle'),
               ('cc', 'purple', 'square'))
    ieq(expect3, table3)


def _test_join_multiple(join_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (1, 'red', 8),
              (2, 'yellow', 15),
              (2, 'orange', 5),
              (3, 'purple', 4),
              (4, 'chartreuse', 42))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (2, 'square', 'big'),
              (3, 'ellipse', 'small'),
              (3, 'ellipse', 'tiny'),
              (5, 'didodecahedron', 3.14159265))

    actual = join_impl(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (1, 'red', 8, 'circle', 'big'),
              (2, 'yellow', 15, 'square', 'tiny'),
              (2, 'yellow', 15, 'square', 'big'),
              (2, 'orange', 5, 'square', 'tiny'),
              (2, 'orange', 5, 'square', 'big'),
              (3, 'purple', 4, 'ellipse', 'small'),
              (3, 'purple', 4, 'ellipse', 'tiny'))
    ieq(expect, actual)


def _test_join_uneven_rows(join_impl):
    table1 = (('id', 'colour'),
              ('aa',),
              ('bb', 'red'),
              ('cc', 'purple'))
    table2 = (('identifier', 'shape'),
              ('aa', 'circle'),
              ('cc',),
              ('dd', 'ellipse'))

    table3 = join_impl(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour', 'shape'),
               ('aa', None, 'circle'),
               ('cc', 'purple', None))
    ieq(expect3, table3)


def _test_join(join_impl):
    _test_join_basic(join_impl)
    _test_join_compound_keys(join_impl)
    _test_join_string_key(join_impl)
    _test_join_empty(join_impl)
    _test_join_novaluefield(join_impl)
    _test_join_prefix(join_impl)
    _test_join_lrkey(join_impl)
    _test_join_multiple(join_impl)
    _test_join_uneven_rows(join_impl)


def test_join():
    _test_join(join)


def _test_leftjoin_1(leftjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = leftjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (5, 'yellow', None,),
               (7, 'orange', None))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = leftjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_leftjoin_2(leftjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = leftjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (5, 'yellow', None,),
               (7, 'orange', None))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = leftjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_leftjoin_3(leftjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'triangle'))
    table3 = leftjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = leftjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_leftjoin_compound_keys(leftjoin_impl):

    # compound keys
    table5 = (('id', 'time', 'height'),
              (1, 1, 12.3),
              (1, 2, 34.5),
              (2, 1, 56.7))
    table6 = (('id', 'time', 'weight', 'bp'),
              (1, 2, 4.5, 120),
              (2, 1, 6.7, 110),
              (2, 2, 8.9, 100))
    table7 = leftjoin_impl(table5, table6, key=['id', 'time'])
    expect7 = (('id', 'time', 'height', 'weight', 'bp'),
               (1, 1, 12.3, None, None),
               (1, 2, 34.5, 4.5, 120),
               (2, 1, 56.7, 6.7, 110))
    ieq(expect7, table7)


def _test_leftjoin_empty(leftjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),)
    table3 = leftjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', None),
               (2, 'red', None),
               (3, 'purple', None),
               (5, 'yellow', None,),
               (7, 'orange', None))
    ieq(expect3, table3)


def _test_leftjoin_novaluefield(leftjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    expect = (('id', 'colour', 'shape'),
              (1, 'blue', 'circle'),
              (2, 'red', None),
              (3, 'purple', 'square'),
              (5, 'yellow', None,),
              (7, 'orange', None))
    
    actual = leftjoin_impl(table1, table2, key='id')
    ieq(expect, actual)
    actual = leftjoin_impl(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id', 'shape'), actual)
    actual = leftjoin_impl(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id', 'colour'), actual)
    actual = leftjoin_impl(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def _test_leftjoin_multiple(leftjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (1, 'red', 8),
              (2, 'yellow', 15),
              (2, 'orange', 5),
              (3, 'purple', 4),
              (4, 'chartreuse', 42))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (2, 'square', 'big'),
              (3, 'ellipse', 'small'),
              (3, 'ellipse', 'tiny'),
              (5, 'didodecahedron', 3.14159265))

    actual = leftjoin_impl(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (1, 'red', 8, 'circle', 'big'),
              (2, 'yellow', 15, 'square', 'tiny'),
              (2, 'yellow', 15, 'square', 'big'),
              (2, 'orange', 5, 'square', 'tiny'),
              (2, 'orange', 5, 'square', 'big'),
              (3, 'purple', 4, 'ellipse', 'small'),
              (3, 'purple', 4, 'ellipse', 'tiny'),
              (4, 'chartreuse', 42, None, None))
    ieq(expect, actual)


def _test_leftjoin_prefix(leftjoin_impl):
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = leftjoin_impl(table1, table2, key='id', lprefix='l_', rprefix='r_')
    expect3 = (('l_id', 'l_colour', 'r_shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (5, 'yellow', None,),
               (7, 'orange', None))
    ieq(expect3, table3)


def _test_leftjoin_lrkey(leftjoin_impl):
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('identifier', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = leftjoin_impl(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (5, 'yellow', None,),
               (7, 'orange', None))
    ieq(expect3, table3)


def _test_leftjoin(leftjoin_impl):
    _test_leftjoin_1(leftjoin_impl)
    _test_leftjoin_2(leftjoin_impl)
    _test_leftjoin_3(leftjoin_impl)
    _test_leftjoin_compound_keys(leftjoin_impl)
    _test_leftjoin_empty(leftjoin_impl)
    _test_leftjoin_novaluefield(leftjoin_impl)
    _test_leftjoin_multiple(leftjoin_impl)
    _test_leftjoin_prefix(leftjoin_impl)
    _test_leftjoin_lrkey(leftjoin_impl)


def test_leftjoin():
    _test_leftjoin(leftjoin)


def _test_rightjoin_1(rightjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    table3 = rightjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'triangle'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, None, 'pentagon'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = rightjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_rightjoin_2(rightjoin_impl):

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = rightjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = rightjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_rightjoin_3(rightjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (4, 'orange'))
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (5, 'ellipse'),
              (7, 'pentagon'))
    table3 = rightjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'triangle'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (5, None, 'ellipse'),
               (7, None, 'pentagon'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = rightjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_rightjoin_empty(rightjoin_impl):

    table1 = (('id', 'colour'),)
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    table3 = rightjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'triangle'),
               (1, None, 'circle'),
               (3, None, 'square'),
               (4, None, 'ellipse'),
               (5, None, 'pentagon'))
    ieq(expect3, table3)


def _test_rightjoin_novaluefield(rightjoin_impl):
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    expect = (('id', 'colour', 'shape'),
              (0, None, 'triangle'),
              (1, 'blue', 'circle'),
              (3, 'purple', 'square'),
              (4, None, 'ellipse'),
              (5, None, 'pentagon'))

    actual = rightjoin_impl(table1, table2, key='id')
    ieq(expect, actual)
    actual = rightjoin_impl(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id', 'shape'), actual)
    actual = rightjoin_impl(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id', 'colour'), actual)
    actual = rightjoin_impl(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def _test_rightjoin_prefix(rightjoin_impl):
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    table3 = rightjoin_impl(table1, table2, key='id', lprefix='l_',
                            rprefix='r_')
    expect3 = (('l_id', 'l_colour', 'r_shape'),
               (0, None, 'triangle'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, None, 'pentagon'))
    ieq(expect3, table3)


def _test_rightjoin_lrkey(rightjoin_impl):
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('identifier', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    table3 = rightjoin_impl(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'triangle'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, None, 'pentagon'))
    ieq(expect3, table3)


def _test_rightjoin_multiple(rightjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (1, 'red', 8),
              (2, 'yellow', 15),
              (2, 'orange', 5),
              (3, 'purple', 4),
              (4, 'chartreuse', 42))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (2, 'square', 'big'),
              (3, 'ellipse', 'small'),
              (3, 'ellipse', 'tiny'),
              (5, 'didodecahedron', 3.14159265))

    actual = rightjoin_impl(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (1, 'red', 8, 'circle', 'big'),
              (2, 'yellow', 15, 'square', 'tiny'),
              (2, 'yellow', 15, 'square', 'big'),
              (2, 'orange', 5, 'square', 'tiny'),
              (2, 'orange', 5, 'square', 'big'),
              (3, 'purple', 4, 'ellipse', 'small'),
              (3, 'purple', 4, 'ellipse', 'tiny'),
              (5, None, None, 'didodecahedron', 3.14159265))

    # N.B., need to sort because hash and sort implementations will return
    # rows in a different order
    ieq(sort(expect), sort(actual))


def _test_rightjoin(rightjoin_impl):
    _test_rightjoin_1(rightjoin_impl)
    _test_rightjoin_2(rightjoin_impl)
    _test_rightjoin_3(rightjoin_impl)
    _test_rightjoin_empty(rightjoin_impl)
    _test_rightjoin_novaluefield(rightjoin_impl)
    _test_rightjoin_prefix(rightjoin_impl)
    _test_rightjoin_lrkey(rightjoin_impl)
    _test_rightjoin_multiple(rightjoin_impl)


def test_rightjoin():
    _test_rightjoin(rightjoin)


def test_outerjoin():

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, 'black', None),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, 'yellow', None),
               (7, 'white', None))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = outerjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def test_outerjoin_2():

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (0, 'pentagon'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'triangle'))
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'pentagon'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, None, 'triangle'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = outerjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def test_outerjoin_fieldorder():

    table1 = (('colour', 'id'),
              ('blue', 1),
              ('red', 2),
              ('purple', 3))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('colour', 'id', 'shape'),
               ('blue', 1, 'circle'),
               ('red', 2, None),
               ('purple', 3, 'square'),
               (None, 4, 'ellipse'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice


def test_outerjoin_empty():

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),)
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, 'black', None),
               (1, 'blue', None),
               (2, 'red', None),
               (3, 'purple', None),
               (5, 'yellow', None),
               (7, 'white', None))
    ieq(expect3, table3)


def test_outerjoin_novaluefield():
    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    expect = (('id', 'colour', 'shape'),
              (0, 'black', None),
              (1, 'blue', 'circle'),
              (2, 'red', None),
              (3, 'purple', 'square'),
              (4, None, 'ellipse'),
              (5, 'yellow', None),
              (7, 'white', None))
    actual = outerjoin(table1, table2, key='id')
    ieq(expect, actual)
    actual = outerjoin(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id', 'shape'), actual)
    actual = outerjoin(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id', 'colour'), actual)
    actual = outerjoin(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def test_outerjoin_prefix():

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = outerjoin(table1, table2, key='id', lprefix='l_', rprefix='r_')
    expect3 = (('l_id', 'l_colour', 'r_shape'),
               (0, 'black', None),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, 'yellow', None),
               (7, 'white', None))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice


def test_outerjoin_lrkey():

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('identifier', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = outerjoin(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour', 'shape'),
               (0, 'black', None),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, 'yellow', None),
               (7, 'white', None))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice


def test_outerjoin_multiple():

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (1, 'red', 8),
              (2, 'yellow', 15),
              (2, 'orange', 5),
              (3, 'purple', 4),
              (4, 'chartreuse', 42))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (2, 'square', 'big'),
              (3, 'ellipse', 'small'),
              (3, 'ellipse', 'tiny'),
              (5, 'didodecahedron', 3.14159265))

    actual = outerjoin(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (1, 'red', 8, 'circle', 'big'),
              (2, 'yellow', 15, 'square', 'tiny'),
              (2, 'yellow', 15, 'square', 'big'),
              (2, 'orange', 5, 'square', 'tiny'),
              (2, 'orange', 5, 'square', 'big'),
              (3, 'purple', 4, 'ellipse', 'small'),
              (3, 'purple', 4, 'ellipse', 'tiny'),
              (4, 'chartreuse', 42, None, None),
              (5, None, None, 'didodecahedron', 3.14159265))

    ieq(expect, actual)


def test_crossjoin():

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = crossjoin(table1, table2)
    expect3 = (('id', 'colour', 'id', 'shape'),
               (1, 'blue', 1, 'circle'),
               (1, 'blue', 3, 'square'),
               (2, 'red', 1, 'circle'),
               (2, 'red', 3, 'square'))
    ieq(expect3, table3)


def test_crossjoin_empty():

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'))
    table2 = (('id', 'shape'),)
    table3 = crossjoin(table1, table2)
    expect3 = (('id', 'colour', 'id', 'shape'),)
    ieq(expect3, table3)


def test_crossjoin_novaluefield():
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    expect = (('id', 'colour', 'id', 'shape'),
              (1, 'blue', 1, 'circle'),
              (1, 'blue', 3, 'square'),
              (2, 'red', 1, 'circle'),
              (2, 'red', 3, 'square'))
    actual = crossjoin(table1, table2, key='id')
    ieq(expect, actual)
    actual = crossjoin(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 0, 2, 'shape'), actual)
    actual = crossjoin(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 0, 'colour', 2), actual)
    actual = crossjoin(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 0, 2), actual)


def test_crossjoin_prefix():

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = crossjoin(table1, table2, prefix=True)
    expect3 = (('1_id', '1_colour', '2_id', '2_shape'),
               (1, 'blue', 1, 'circle'),
               (1, 'blue', 3, 'square'),
               (2, 'red', 1, 'circle'),
               (2, 'red', 3, 'square'))
    ieq(expect3, table3)

def test_crossjoin_uneven_rows():
    table1 = (('id', 'colour'),
              (1,),
              (2, 'red'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3,))
    table3 = crossjoin(table1, table2, prefix=True)
    expect3 = (('1_id', '1_colour', '2_id', '2_shape'),
               (1, None, 1, 'circle'),
               (1, None, 3, None),
               (2, 'red', 1, 'circle'),
               (2, 'red', 3, None))
    ieq(expect3, table3)

def _test_antijoin_basics(antijoin_impl):

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = antijoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour'),
               (0, 'black'),
               (2, 'red'),
               (4, 'yellow'),
               (5, 'white'))
    ieq(expect3, table3)

    table4 = antijoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_antijoin_empty(antijoin_impl):

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    table2 = (('id', 'shape'),)
    actual = antijoin_impl(table1, table2, key='id')
    expect = table1
    ieq(expect, actual)


def _test_antijoin_novaluefield(antijoin_impl):
    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    expect = (('id', 'colour'),
              (0, 'black'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    actual = antijoin_impl(table1, table2, key='id')
    ieq(expect, actual)
    actual = antijoin_impl(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id'), actual)
    actual = antijoin_impl(table1, cut(table2, 'id'), key='id')
    ieq(expect, actual)
    actual = antijoin_impl(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def _test_antijoin_lrkey(antijoin_impl):

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    table2 = (('identifier', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = antijoin_impl(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour'),
               (0, 'black'),
               (2, 'red'),
               (4, 'yellow'),
               (5, 'white'))
    ieq(expect3, table3)

def _test_antijoin_uneven_rows(antijoin_impl):
    table1 = (('id', 'colour'),
              (0,),
              (1,),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    table2 = (('identifier', 'shape'),
              (1,),
              (3, 'square'))
    table3 = antijoin_impl(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour'),
               (0, None),
               (2, 'red'),
               (4, 'yellow'),
               (5, 'white'))
    ieq(expect3, table3)

def _test_antijoin(antijoin_impl):
    _test_antijoin_basics(antijoin_impl)
    _test_antijoin_empty(antijoin_impl)
    _test_antijoin_novaluefield(antijoin_impl)
    _test_antijoin_lrkey(antijoin_impl)
    _test_antijoin_uneven_rows(antijoin_impl)


def test_antijoin():
    _test_antijoin(antijoin)


def _test_lookupjoin_1(lookupjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (2, 'red', 8),
              (3, 'purple', 4))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (3, 'ellipse', 'small'))

    actual = lookupjoin_impl(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)
    ieq(expect, actual)

    # natural join
    actual = lookupjoin_impl(table1, table2)
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)
    ieq(expect, actual)


def _test_lookupjoin_2(lookupjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (2, 'red', 8),
              (3, 'purple', 4))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (1, 'circle', 'small'),
              (2, 'square', 'tiny'),
              (2, 'square', 'big'),
              (3, 'ellipse', 'small'),
              (3, 'ellipse', 'tiny'))

    actual = lookupjoin_impl(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)
    ieq(expect, actual)


def _test_lookupjoin_prefix(lookupjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (2, 'red', 8),
              (3, 'purple', 4))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (3, 'ellipse', 'small'))

    actual = lookupjoin_impl(table1, table2, key='id', lprefix='l_',
                             rprefix='r_')
    expect = (('l_id', 'l_color', 'l_cost', 'r_shape', 'r_size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)


def _test_lookupjoin_lrkey(lookupjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (2, 'red', 8),
              (3, 'purple', 4))

    table2 = (('identifier', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (3, 'ellipse', 'small'))

    actual = lookupjoin_impl(table1, table2, lkey='id', rkey='identifier')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)


def _test_lookupjoin_novaluefield(lookupjoin_impl):
    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (2, 'red', 8),
              (3, 'purple', 4))
    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (3, 'ellipse', 'small'))
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    actual = lookupjoin_impl(table1, table2, key='id')
    ieq(expect, actual)
    actual = lookupjoin_impl(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id', 'shape', 'size'), actual)
    actual = lookupjoin_impl(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id', 'color', 'cost'), actual)
    actual = lookupjoin_impl(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def _test_lookupjoin_uneven_rows(lookupjoin_impl):
    table1 = (('id', 'color', 'cost'),
              (1, 'blue',),
              (2, 'red', 8),
              (3, 'purple', 4))

    table2 = (('identifier', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square',),
              (3, 'ellipse', 'small'))

    actual = lookupjoin_impl(table1, table2, lkey='id', rkey='identifier')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', None, 'circle', 'big'),
              (2, 'red', 8, 'square', None),
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)


def _test_lookupjoin_empty(lookupjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),)

    # empty right table
    actual = lookupjoin_impl(table1, table2, key='id')
    expect = (('id', 'colour', 'shape'),
              (1, 'blue', None),
              (2, 'red', None),
              (3, 'purple', None))
    ieq(expect, actual)
    ieq(expect, actual)

    # empty left table
    actual = lookupjoin_impl(table2, table1, key='id')
    expect = (('id', 'shape', 'colour'),)
    ieq(expect, actual)

    # both tables empty
    actual = lookupjoin_impl(table2, table2, key='id')
    expect = (('id', 'shape', 'shape'),)
    ieq(expect, actual)

    # empty right table where the first left key is None; None must not be
    # mistaken for an "iterator exhausted" marker and drop the first group
    table3 = (('id', 'colour'),
              (None, 'blue'),
              (2, 'red'))
    actual = lookupjoin_impl(table3, table2, key='id')
    expect = (('id', 'colour', 'shape'),
              (None, 'blue', None),
              (2, 'red', None))
    ieq(expect, actual)
    ieq(expect, actual)

    # ... and where every left key is None
    table4 = (('id', 'colour'),
              (None, 'blue'),
              (None, 'red'))
    actual = lookupjoin_impl(table4, table2, key='id')
    expect = (('id', 'colour', 'shape'),
              (None, 'blue', None),
              (None, 'red', None))
    ieq(expect, actual)


def _test_lookupjoin_uncomparable_keys(lookupjoin_impl):

    # key values that raise TypeError under native Python 3 comparison;
    # petl orders them as None < numbers < everything else
    table1 = (('id', 'colour'),
              (None, 'blue'),
              (1, 'red'),
              ('two', 'purple'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              ('two', 'square'))

    actual = lookupjoin_impl(table1, table2, key='id')
    expect = (('id', 'colour', 'shape'),
              (None, 'blue', None),
              (1, 'red', 'circle'),
              ('two', 'purple', 'square'))
    ieq(expect, actual)
    ieq(expect, actual)

    # no key values in common, so every comparison crosses types
    table3 = (('id', 'shape'),
              ('three', 'ellipse'))
    actual = lookupjoin_impl(table1, table3, key='id')
    expect = (('id', 'colour', 'shape'),
              (None, 'blue', None),
              (1, 'red', None),
              ('two', 'purple', None))
    ieq(expect, actual)


def _test_lookupjoin(lookupjoin_impl):
    _test_lookupjoin_1(lookupjoin_impl)
    _test_lookupjoin_2(lookupjoin_impl)
    _test_lookupjoin_prefix(lookupjoin_impl)
    _test_lookupjoin_lrkey(lookupjoin_impl)
    _test_lookupjoin_novaluefield(lookupjoin_impl)
    _test_lookupjoin_uneven_rows(lookupjoin_impl)
    _test_lookupjoin_empty(lookupjoin_impl)
    _test_lookupjoin_uncomparable_keys(lookupjoin_impl)


def test_lookupjoin():
    _test_lookupjoin(lookupjoin)


def test_hashjoin():
    _test_join(hashjoin)


def test_hashleftjoin():
    _test_leftjoin(hashleftjoin)


def test_hashrightjoin():
    _test_rightjoin(hashrightjoin)


def test_hashantijoin():
    _test_antijoin(hashantijoin)


def test_hashlookupjoin():
    _test_lookupjoin(hashlookupjoin)


def test_unjoin_implicit_key():

    # test the case where the join key needs to be reconstructed
        
    table1 = (('foo', 'bar'),
              (1, 'apple'),
              (2, 'apple'),
              (3, 'orange'))
    
    expect_left = (('foo', 'bar_id'),
                   (1, 1),
                   (2, 1),
                   (3, 2))
    expect_right = (('id', 'bar'),
                    (1, 'apple'),
                    (2, 'orange'))
    
    left, right = unjoin(table1, 'bar')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)

    
def test_unjoin_explicit_key():

    # test the case where the join key is still present
    
    table2 = (('Customer ID', 'First Name', 'Surname', 'Telephone Number'),
              (123, 'Robert', 'Ingram', '555-861-2025'),
              (456, 'Jane', 'Wright', '555-403-1659'),
              (456, 'Jane', 'Wright', '555-776-4100'),
              (789, 'Maria', 'Fernandez', '555-808-9633'))
    
    expect_left = (('Customer ID', 'First Name', 'Surname'),
                   (123, 'Robert', 'Ingram'),
                   (456, 'Jane', 'Wright'),
                   (789, 'Maria', 'Fernandez'))
    expect_right = (('Customer ID', 'Telephone Number'),
                    (123, '555-861-2025'),
                    (456, '555-403-1659'),
                    (456, '555-776-4100'),
                    (789, '555-808-9633'))
    left, right = unjoin(table2, 'Telephone Number', key='Customer ID')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_negative_value_index():
    table = (('item', 'customer_id', 'customer_name'),
             ('Book', 1, 'Alice'), ('Pen', 1, 'Alice'), ('Ruler', 2, 'Bob'))
    left, right = unjoin(table, -1, key='customer_id')
    ieq((('item', 'customer_id'), ('Book', 1), ('Pen', 1), ('Ruler', 2)), left)
    ieq((('customer_id', 'customer_name'), (1, 'Alice'), (2, 'Bob')), right)


def test_unjoin_explicit_key_2():
    
    table3 = (('Employee', 'Skill', 'Current Work Location'),
              ('Jones', 'Typing', '114 Main Street'),
              ('Jones', 'Shorthand', '114 Main Street'),
              ('Jones', 'Whittling', '114 Main Street'),
              ('Bravo', 'Light Cleaning', '73 Industrial Way'),
              ('Ellis', 'Alchemy', '73 Industrial Way'),
              ('Ellis', 'Flying', '73 Industrial Way'),
              ('Harrison', 'Light Cleaning', '73 Industrial Way'))
    # N.B., we do expect rows will get sorted
    expect_left = (('Employee', 'Current Work Location'),
                   ('Bravo', '73 Industrial Way'),
                   ('Ellis', '73 Industrial Way'),
                   ('Harrison', '73 Industrial Way'),
                   ('Jones', '114 Main Street'))
    expect_right = (('Employee', 'Skill'),
                    ('Bravo', 'Light Cleaning'),
                    ('Ellis', 'Alchemy'),
                    ('Ellis', 'Flying'),
                    ('Harrison', 'Light Cleaning'),
                    ('Jones', 'Shorthand'),
                    ('Jones', 'Typing'),
                    ('Jones', 'Whittling'))
    left, right = unjoin(table3, 'Skill', key='Employee')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_explicit_key_3():
    
    table4 = (('Tournament', 'Year', 'Winner', 'Date of Birth'),
              ('Indiana Invitational', 1998, 'Al Fredrickson', '21 July 1975'),
              ('Cleveland Open', 1999, 'Bob Albertson', '28 September 1968'),
              ('Des Moines Masters', 1999, 'Al Fredrickson', '21 July 1975'),
              ('Indiana Invitational', 1999, 'Chip Masterson', '14 March 1977'))
    
    # N.B., we do expect rows will get sorted
    expect_left = (('Tournament', 'Year', 'Winner'),
                   ('Cleveland Open', 1999, 'Bob Albertson'),
                   ('Des Moines Masters', 1999, 'Al Fredrickson'),
                   ('Indiana Invitational', 1998, 'Al Fredrickson'),
                   ('Indiana Invitational', 1999, 'Chip Masterson'))    
    expect_right = (('Winner', 'Date of Birth'),
                    ('Al Fredrickson', '21 July 1975'),
                    ('Bob Albertson', '28 September 1968'),
                    ('Chip Masterson', '14 March 1977'))
    left, right = unjoin(table4, 'Date of Birth', key='Winner')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_explicit_key_4():
    
    table5 = (('Restaurant', 'Pizza Variety', 'Delivery Area'),
              ('A1 Pizza', 'Thick Crust', 'Springfield'),
              ('A1 Pizza', 'Thick Crust', 'Shelbyville'),
              ('A1 Pizza', 'Thick Crust', 'Capital City'),
              ('A1 Pizza', 'Stuffed Crust', 'Springfield'),
              ('A1 Pizza', 'Stuffed Crust', 'Shelbyville'),
              ('A1 Pizza', 'Stuffed Crust', 'Capital City'),
              ('Elite Pizza', 'Thin Crust', 'Capital City'),
              ('Elite Pizza', 'Stuffed Crust', 'Capital City'),
              ("Vincenzo's Pizza", "Thick Crust", "Springfield"),
              ("Vincenzo's Pizza", "Thick Crust", "Shelbyville"),
              ("Vincenzo's Pizza", "Thin Crust", "Springfield"),
              ("Vincenzo's Pizza", "Thin Crust", "Shelbyville"))
    
    # N.B., we do expect rows will get sorted
    expect_left = (('Restaurant', 'Pizza Variety'),
                   ('A1 Pizza', 'Stuffed Crust'),
                   ('A1 Pizza', 'Thick Crust'),
                   ('Elite Pizza', 'Stuffed Crust'),
                   ('Elite Pizza', 'Thin Crust'),
                   ("Vincenzo's Pizza", "Thick Crust"),
                   ("Vincenzo's Pizza", "Thin Crust"))
    expect_right = (('Restaurant', 'Delivery Area'),
                    ('A1 Pizza', 'Capital City'),
                    ('A1 Pizza', 'Shelbyville'),
                    ('A1 Pizza', 'Springfield'),
                    ('Elite Pizza', 'Capital City'),
                    ("Vincenzo's Pizza", "Shelbyville"),
                    ("Vincenzo's Pizza", "Springfield"))
    left, right = unjoin(table5, 'Delivery Area', key='Restaurant')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_explicit_key_5():
    
    table6 = (('ColA', 'ColB', 'ColC'),
              ('A', 1, 'apple'),
              ('B', 1, 'apple'),
              ('C', 2, 'orange'),
              ('D', 3, 'lemon'),
              ('E', 3, 'lemon'))

    # N.B., we do expect rows will get sorted
    expect_left = (('ColA', 'ColB'),
                   ('A', 1),
                   ('B', 1),
                   ('C', 2),
                   ('D', 3),
                   ('E', 3))
    expect_right = (('ColB', 'ColC'),
                    (1, 'apple'),
                    (2, 'orange'),
                    (3, 'lemon'))
    left, right = unjoin(table6, 'ColC', key='ColB')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_joins_negative_key_indices():
    left = [('value', 'id'), ('a', 1)]
    right = [('other', 'key'), ('x', 1)]
    expected = [('value', 'id', 'other'), ('a', 1, 'x')]
    for join_impl in (join, leftjoin, rightjoin, outerjoin, hashjoin,
                      hashleftjoin, hashrightjoin, lookupjoin, hashlookupjoin):
        ieq(expected, join_impl(left, right, lkey=-1, rkey=-1))


# ---------------------------------------------------------------------------
# Device reconciliation ("stale device claim") rules
# ---------------------------------------------------------------------------

from decimal import Decimal

import pytest

from petl.transform.reconcile import ReconcileSpec


# The catalogue is in serial-then-device order so that the short R6 row can
# carry its site and serial while the device and record number are absent.
_NORTH_CATALOG = (
    ('site', 'serial', 'device', 'record'),
    ('north', 'Y', Decimal('9007199254740992.00'), 'R0'),
    ('north', 'Y', Decimal('9007199254740993.00'), 'R1'),
    ('north', 'Y', Decimal('9007199254740993.00'), 'R2'),
    ('north', 'X', 7, 'R3'),
    ('north', 'X', 7, 'R4'),
    ('north', 'X', None, 'R5'),
    ('north', 'X'),  # R6: short row, serial present, device absent
)

_NORTH_READINGS = (
    ('site', 'serial', 'device'),
    ('north', 'Y', 9007199254740992),
    ('north', 'X', 9007199254740993),
    ('north', 'Y', None),
    ('north', 'X', 99),
    ('north', 'X'),  # short row: serial present, device absent
)


def _reconciled(fn, left=_NORTH_READINGS, right=_NORTH_CATALOG, **kwargs):
    return list(fn(left, right, reconcile=True, **kwargs))


def _records(result):
    return [row[-1] for row in list(result)[1:]]


@pytest.mark.parametrize('fn', [join, hashjoin])
def test_reconcile_exact_and_serial_fallback(fn):
    result = _reconciled(fn)
    # int and finite Decimal agree by exact numeric value
    assert result[1] == ('north', 'Y', 9007199254740992, 'Y', 'R0')
    assert [row[-1] for row in result[2:4]] == ['R1', 'R2']
    assert [row[2] for row in result[2:4]] == [9007199254740993] * 2
    # unknown device and short row both fall back; each catalogue row kept
    assert [row[-1] for row in result[4:]] == ['R3', 'R4', 'R3', 'R4']
    # the explicit None reading is unclaimed for an inner join
    assert len(result) == 8


def test_reconcile_exact_match_wins_over_serial():
    catalog = (
        ('site', 'device', 'serial', 'record'),
        ('north', 1, 'S', 'A'),
        ('north', 2, 'S', 'B'),
    )
    readings = (('site', 'serial', 'device'),
                ('north', 'S', 1))
    assert _records(join(readings, catalog, reconcile=True)) == ['A']


def test_reconcile_ambiguous_serial_unclaimed():
    catalog = (
        ('site', 'device', 'serial', 'record'),
        ('north', 1, 'S', 'A'),
        ('north', 2, 'S', 'B'),
    )
    readings = (('site', 'serial', 'device'),
                ('north', 'S'))
    assert _records(join(readings, catalog, reconcile=True)) == []
    left = _reconciled(leftjoin, readings, catalog)
    assert len(left) == 2 and left[1][-1] is None


def test_reconcile_catalog_missing_and_none_not_claimed_by_serial():
    catalog = (
        ('site', 'serial', 'device', 'record'),
        ('north', 'S', None, 'A'),
        ('north', 'S'),  # missing device number
        ('north', 'S', 3, 'B'),
    )
    readings = (('site', 'serial', 'device'),
                ('north', 'S'))
    # only device 3 is an eligible serial claim; None/missing rows are not
    assert _records(join(readings, catalog, reconcile=True)) == ['B']


def test_reconcile_short_y_reading_unclaimed():
    readings = (('site', 'serial', 'device'),
                ('north', 'Y'))
    assert _records(join(readings, _NORTH_CATALOG, reconcile=True)) == []
    left = list(leftjoin(readings, _NORTH_CATALOG, reconcile=True))
    assert left[1:] == [('north', 'Y', None, None, None)]


def test_reconcile_none_device_only_when_none_matching():
    result = _reconciled(hashjoin)
    assert all(row[-1] != 'R5' for row in result)

    result = _reconciled(hashleftjoin, match_none=True)
    explicit_none = [row for row in result[1:] if row[2] is None]
    # the explicit None reading claims R5; the short reading falls back
    assert explicit_none == [('north', 'Y', None, 'X', 'R5'),
                             ('north', 'X', None, 'X', 'R3'),
                             ('north', 'X', None, 'X', 'R4')]


@pytest.mark.parametrize('fn', [join, leftjoin, hashjoin, hashleftjoin])
def test_reconcile_left_and_inner_shape(fn):
    result = _reconciled(fn)
    assert result[0] == ('site', 'serial', 'device', 'serial', 'record')
    inner = fn is join or fn is hashjoin
    if inner:
        assert len(result) == 8
    else:
        # every unclaimed reading is kept exactly once
        assert len(result) == 9
        unclaimed = [row for row in result[1:] if row[-1] is None]
        assert unclaimed == [('north', 'Y', None, None, None)]
    # left device value keeps its original Python type
    assert type(result[1][2]) is int


def test_reconcile_reordered_catalog_and_value_types():
    catalog = (
        ('record', 'serial', 'device', 'site'),
        ('R3', 'X', 7, 'north'),
        ('R4', 'X', 7, 'north'),
        ('R5', 'X', None, 'north'),
    )
    readings = (('site', 'serial', 'device'),
                ('north', 'X'))
    result = list(hashjoin(readings, catalog, reconcile=True))
    assert result[0] == ('site', 'serial', 'device', 'record', 'serial')
    assert [row[3] for row in result[1:]] == ['R3', 'R4']


def test_reconcile_booleans_and_strings_distinct():
    catalog = (
        ('site', 'device', 'serial', 'record'),
        ('north', True, 'S', 'B'),
        ('north', 1, 'S', 'N'),
        ('north', '1', 'S', 'T'),
    )
    readings = (('site', 'serial', 'device'),
                ('north', 'S', True))
    assert _records(join(readings, catalog, reconcile=True)) == ['B']


def test_reconcile_explicit_spec():
    spec = ReconcileSpec(lsite='site', rsite='site', ldevice='device',
                         rdevice='device', lserial='serial',
                         rserial='serial', rrecord='record')
    catalog = (('site', 'device', 'serial', 'record'),
               ('north', 7, 'S', 'A'))
    readings = (('site', 'serial', 'device'),
                ('north', 'S'))
    result = list(join(readings, catalog, reconcile=spec))
    assert result[1][-1] == 'A'


class _BatchCatalog(object):
    def __init__(self, batches):
        self.batches = batches
        self.opens = 0

    def __iter__(self):
        batch = self.batches[min(self.opens, len(self.batches) - 1)]
        self.opens += 1
        return iter(batch)


class _Readings(object):
    def __init__(self, rows):
        self.rows = rows
        self.opens = 0

    def __iter__(self):
        self.opens += 1
        return iter(self.rows)


@pytest.mark.parametrize('fn', [join, leftjoin, hashjoin, hashleftjoin])
@pytest.mark.parametrize('buffersize', [None, 1])
def test_reconcile_two_catalog_batches_uncached(fn, buffersize):
    catalog = _BatchCatalog([
        (
            ('site', 'device', 'serial', 'record'),
            ('north', 7, 'X', 'R3'),
            ('north', 7, 'X', 'R4'),
        ),
        (
            ('device', 'record', 'serial', 'site'),
            (7, 'R7', 'X', 'north'),
        ),
    ])
    readings = _Readings((('site', 'serial', 'device'),
                          ('north', 'X')))

    view = fn(readings, catalog, reconcile=True, cache=False,
              buffersize=buffersize)
    a = iter(view)
    header_a = next(a)
    assert header_a == ('site', 'serial', 'device', 'serial', 'record')

    b = iter(view)
    header_b = next(b)
    assert header_b == ('site', 'serial', 'device', 'record', 'serial')
    rows_b = list(b)
    assert rows_b == [('north', 'X', None, 'R7', 'X')]

    rows_a = list(a)
    assert rows_a == [('north', 'X', None, 'X', 'R3'),
                      ('north', 'X', None, 'X', 'R4')]
    assert catalog.opens == 2
    assert readings.opens == 2


def test_reconcile_legacy_join_unchanged():
    table1 = (('site', 'device'), ('north', 1))
    table2 = (('site', 'device', 'record'), ('north', 1, 'R'))
    # without the reconciliation rules the ordinary join applies
    assert list(join(table1, table2)) == [
        ('site', 'device', 'record'), ('north', 1, 'R')]
    assert list(hashjoin(table1, table2)) == [
        ('site', 'device', 'record'), ('north', 1, 'R')]
