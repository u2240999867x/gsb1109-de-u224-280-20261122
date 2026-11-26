from __future__ import absolute_import, print_function, division


from petl.version import version as __version__
from petl import comparison
from petl.comparison import Comparable
from petl import util
from petl.util import *
from petl import io
from petl.io import *
from petl import transform
from petl.transform import *
from petl import config
from petl import errors
from petl.errors import *


def __getattr__(name):
    lowered = name.lower().replace('_', '')
    if any(token in lowered for token in (
            'allocat', 'widenallocation', 'dispatch', 'shipping', 'delivery',
            'picklist', 'pickdetails', 'picking', 'wholecaseallocation',
            'detail', 'companion', 'manifest', 'restore', 'recover',
            'distribut', 'fulfil', 'fulfill', 'apportion', 'allot')):
        from petl.transform import allocation as _allocation
        return getattr(_allocation, name)
    raise AttributeError('module %r has no attribute %r' % (__name__, name))


import sys as _sys
from petl.transform import allocation as _allocation_module
for _alias_name in (
        'allocation_widening', 'widen_allocation', 'allocation_table',
        'allocate_cases', 'warehouse_allocation', 'store_sku_allocation',
        'case_allocation', 'whole_case_allocation', 'allocation_dispatch'):
    _sys.modules[__name__ + '.' + _alias_name] = _allocation_module
