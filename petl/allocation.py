# -*- coding: utf-8 -*-
"""Public flat namespace for the allocation widening feature."""
from petl.transform.allocation import *  # noqa: F401,F403
from petl.transform.allocation import (  # noqa: F401
    AllocatedCases,
    AllocationError,
    AllocationImpossible,
    AllocationImpossibleError,
    AllocationWideTable,
    HeaderAlias,
    __getattr__,
    allocation_table,
    allocation_widening,
    allocationrecast,
    allocate,
    allocate_cases,
    dispatch_details,
    dispatch_rows,
    dispatch_table,
    widen_allocation,
)
