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

from datetime import datetime
from unittest.mock import patch

import pytest

from neptune_query.internal.warnings import (
    _silence_warnings_msg,
    _silence_warnings_until,
    throttled_warn,
)
from neptune_query.warnings import (
    ExperimentalWarning,
    Http5xxWarning,
    Http429Warning,
)

TIME_00_01 = datetime(2025, 9, 11, 15, 0, 1)
TIME_00_03 = datetime(2025, 9, 11, 15, 0, 3)
TIME_00_07 = datetime(2025, 9, 11, 15, 0, 7)
TIME_00_10 = datetime(2025, 9, 11, 15, 0, 10)
TIME_00_18 = datetime(2025, 9, 11, 15, 0, 18)
TIME_00_22 = datetime(2025, 9, 11, 15, 0, 22)
TIME_00_25 = datetime(2025, 9, 11, 15, 0, 25)


@pytest.fixture(autouse=True)
def clear_warning_registries():
    # Clear the warning registries before each test
    _silence_warnings_msg.clear()
    _silence_warnings_until.clear()


def emit_warning_at_time(time, warning):
    """Helper function to emit a warning at a specific time by mocking INITIAL_TIME"""
    with patch("neptune_query.internal.warnings.datetime") as mock_datetime:
        mock_datetime.now.return_value = time
        throttled_warn(warning)


@patch("neptune_query.internal.warnings.warnings.warn")
def test_regular_warning(mock_warn):
    """Regular warnings should be emitted every time"""
    warning = Warning("Test warning")
    emit_warning_at_time(TIME_00_01, warning)
    mock_warn.assert_called_once_with(warning, stacklevel=3)

    # Second emission should also go through
    emit_warning_at_time(TIME_00_01, warning)
    assert mock_warn.call_count == 2


@patch("neptune_query.internal.warnings.warnings.warn")
def test_experimental_warning_once_per_message(mock_warn):
    """ExperimentalWarning with the same message should be emitted only once"""
    warning = ExperimentalWarning("Test experimental feature")
    different_warning = ExperimentalWarning("Different message")

    # First emission should go through
    emit_warning_at_time(TIME_00_01, warning)
    mock_warn.assert_called_once_with(warning, stacklevel=3)

    # Second emission with same message should be suppressed
    emit_warning_at_time(TIME_00_03, warning)
    assert mock_warn.call_count == 1

    # Different message should go through
    emit_warning_at_time(TIME_00_03, different_warning)
    assert mock_warn.call_count == 2

    # Emitting the same warnings again should still be suppressed
    emit_warning_at_time(TIME_00_10, warning)
    emit_warning_at_time(TIME_00_10, different_warning)
    assert mock_warn.call_count == 2


@patch("neptune_query.internal.warnings.warnings.warn")
def test_http429_warning_once_per_minute(mock_warn):
    """Http429Warning should be emitted once per minute"""
    warning = Http429Warning("Rate limit exceeded")
    different_warning = Http429Warning("Another rate limit message")
    non_throttled_warning = Warning("Non-throttled warning")

    # First emission should go through
    emit_warning_at_time(TIME_00_01, warning)
    mock_warn.assert_called_once_with(warning, stacklevel=3)

    # Further emissions within a minute should be suppressed
    emit_warning_at_time(TIME_00_03, warning)
    emit_warning_at_time(TIME_00_07, warning)
    emit_warning_at_time(TIME_00_18, warning)
    assert mock_warn.call_count == 1

    # Non-throttled warning should go through
    emit_warning_at_time(TIME_00_07, non_throttled_warning)
    emit_warning_at_time(TIME_00_10, non_throttled_warning)
    assert mock_warn.call_count == 3

    # Different message but same type should also be suppressed
    emit_warning_at_time(TIME_00_18, different_warning)
    assert mock_warn.call_count == 3

    # After a minute it should go through again
    emit_warning_at_time(TIME_00_22, warning)
    assert mock_warn.call_count == 4


@patch("neptune_query.internal.warnings.warnings.warn")
def test_http429_and_http5xx_warnings(mock_warn):
    """Http429Warning and Http5xxWarning should be throttled independently"""
    warning_429 = Http429Warning("Rate limit exceeded")
    warning_5xx = Http5xxWarning("Server error")
    non_throttled_warning = Warning("Non-throttled warning")

    # Emit Http429Warning
    emit_warning_at_time(TIME_00_01, warning_429)
    assert mock_warn.call_count == 1
    mock_warn.assert_called_once_with(warning_429, stacklevel=3)

    # Emit Http5xxWarning 9 seconds later
    emit_warning_at_time(TIME_00_03, warning_5xx)
    assert mock_warn.call_count == 2
    mock_warn.assert_called_with(warning_5xx, stacklevel=3)

    # Further emissions within a minute should be suppressed
    emit_warning_at_time(TIME_00_07, warning_429)
    emit_warning_at_time(TIME_00_10, warning_5xx)
    assert mock_warn.call_count == 2

    # Non-throttled warning should go through
    emit_warning_at_time(TIME_00_10, non_throttled_warning)
    emit_warning_at_time(TIME_00_18, non_throttled_warning)
    assert mock_warn.call_count == 4

    # After a minute both should go through again
    emit_warning_at_time(TIME_00_22, warning_429)
    assert mock_warn.call_count == 5
    mock_warn.assert_called_with(warning_429, stacklevel=3)

    # Time for 5xx warnings is not yet up
    emit_warning_at_time(TIME_00_22, warning_5xx)
    assert mock_warn.call_count == 5

    # After another 10 seconds (total 1m15s), 5xx warning should go through
    emit_warning_at_time(TIME_00_25, warning_5xx)
    assert mock_warn.call_count == 6
    mock_warn.assert_called_with(warning_5xx, stacklevel=3)
