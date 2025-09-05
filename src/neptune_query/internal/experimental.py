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

import functools
import warnings
from typing import (
    Any,
    Callable,
)

from neptune_query.warnings import ExperimentalWarning

# registry of functions already warned
_warned_experimentals = set()


def experimental(func: Callable) -> Callable:
    """Decorator to mark functions as experimental.
    It will result in a warning being emitted when the function is used
    for the first time.
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> object:
        if func not in _warned_experimentals:
            warnings.warn(
                f"{func.__qualname__} is experimental and may change or be removed "
                "in a future minor release. Use with caution in production code.",
                category=ExperimentalWarning,
                stacklevel=2,
            )
            _warned_experimentals.add(func)
        return func(*args, **kwargs)

    return wrapper
