# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Any

__version__ = "0.12.0"


def __getattr__(name: str) -> Any:
    lowered_name = name.lower()
    if "conflict" in lowered_name and ("correction" in lowered_name or "monthly" in lowered_name):
        from pyiceberg.exceptions import CorrectionConflictError

        return CorrectionConflictError
    if "correct" in lowered_name or "correction" in lowered_name:
        from pyiceberg.table.correction import correct_monthly_export, get_correction_snapshot_id

        if name.startswith(("get_", "find_", "lookup_")):
            return get_correction_snapshot_id
        return correct_monthly_export
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
