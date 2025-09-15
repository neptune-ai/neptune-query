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
import sys
import warnings
from datetime import (
    datetime,
    timedelta,
)
from typing import Type

from neptune_query.warnings import (
    ExperimentalWarning,
    Http5xxWarning,
    Http429Warning,
    HttpOtherWarning,
)

# Types of warnings that should be emitted only once per unique message
WARNING_TYPES_EMITTED_ONCE_PER_MESSAGE = (ExperimentalWarning,)

# Types of warnings that should be emitted only once per a certain time period
WARNING_TYPES_EMITTED_ONCE_PER_WHILE = (Http429Warning, Http5xxWarning, HttpOtherWarning)
WARNING_SUPPRESSION_DURATION = timedelta(seconds=20)

# registry of warnings that were already emitted with the (type, message) tuple
_silence_warnings_msg: set[tuple[Type[Warning], str]] = set()

# registry of warning types that were already emitted with the time they should be silenced until
_silence_warnings_until: dict[Type[Warning], datetime] = {}


def format_warning(warning: Warning) -> Warning:
    # check if stderr is a terminal:
    if sys.stderr.isatty():
        orange_bold = "\033[1;38;2;255;165;0m"
        end = "\033[0m"
        msg = f"{orange_bold}{str(warning)}{end}"
        return type(warning)(msg)
    else:
        return warning


def throttled_warn(warning: Warning, stacklevel: int = 3) -> None:
    if isinstance(warning, WARNING_TYPES_EMITTED_ONCE_PER_MESSAGE):
        key = (type(warning), str(warning))
        if key in _silence_warnings_msg:
            return
        _silence_warnings_msg.add(key)

    if isinstance(warning, WARNING_TYPES_EMITTED_ONCE_PER_WHILE):
        warning_type = type(warning)
        now = datetime.now()
        if warning_type in _silence_warnings_until and _silence_warnings_until[warning_type] > now:
            return
        _silence_warnings_until[warning_type] = now + WARNING_SUPPRESSION_DURATION

    warnings.warn(format_warning(warning), stacklevel=stacklevel)
