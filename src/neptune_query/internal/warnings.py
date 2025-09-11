#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import warnings
from typing import Type

from neptune_query.warnings import ExperimentalWarning

# registry of warnings that were already emitted with the (type, message) tuple
WARNING_CLASSES_EMITTED_ONCE_PER_MESSAGE = (ExperimentalWarning,)

_silence_warnings_msg: set[tuple[Type[Warning], str]] = set()


def throttled_warn(warning: Warning, stacklevel: int = 2) -> None:
    if isinstance(warning, WARNING_CLASSES_EMITTED_ONCE_PER_MESSAGE):
        key = (type(warning), str(warning))
        if key in _silence_warnings_msg:
            return
        _silence_warnings_msg.add(key)

    warnings.warn(warning, stacklevel=stacklevel)
