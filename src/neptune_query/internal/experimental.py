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
from typing import (
    Callable,
    ParamSpec,
    TypeVar,
)

from neptune_query.internal.warnings import throttled_warn
from neptune_query.warnings import ExperimentalWarning

T = ParamSpec("T")
R = TypeVar("R")


def experimental(func: Callable[T, R]) -> Callable[T, R]:
    """Decorator to mark functions as experimental.
    It will result in a warning being emitted when the function is used
    for the first time.
    """

    @functools.wraps(func)
    def wrapper(*args: T.args, **kwargs: T.kwargs) -> R:
        throttled_warn(
            ExperimentalWarning(
                f"`{func.__module__}.{func.__qualname__}` is experimental and might change or be removed "
                "in a future minor release. Use with caution in production code."
            ),
            stacklevel=3,
        )
        return func(*args, **kwargs)

    return wrapper
