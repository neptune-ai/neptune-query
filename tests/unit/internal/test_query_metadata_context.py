#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
#
from __future__ import annotations

import json
from unittest.mock import Mock

import mock

from neptune_query.internal.composition.concurrency import get_thread_local
from neptune_query.internal.query_metadata_context import (
    QueryMetadata,
    use_query_metadata,
    with_neptune_client_metadata,
)


def test_query_metadata_truncation() -> None:
    # given
    metadata = QueryMetadata(
        api_function="a" * 100,
        client_version="b" * 100,
        nq_query_id="c" * 100,
        user_data=None,
    )

    # then
    assert len(metadata.to_json()) < 200
    assert metadata.api_function == "a" * 32
    assert metadata.client_version == "b" * 24
    assert metadata.nq_query_id == "c" * 8


def test_use_query_metadata() -> None:
    # when
    assert get_thread_local("query_metadata", QueryMetadata) is None
    with use_query_metadata(api_function="test_func"):
        # then
        retrieved_metadata = get_thread_local("query_metadata", QueryMetadata)
        assert retrieved_metadata is not None
        assert retrieved_metadata.api_function == "test_func"

    # and
    assert get_thread_local("query_metadata", QueryMetadata) is None


def test_with_neptune_client_metadata_no_context() -> None:
    # given
    mock_api_call = Mock()

    # when
    decorated_call = with_neptune_client_metadata(mock_api_call)
    decorated_call(arg1="value1")

    # then
    mock_api_call.assert_called_once()
    _, kwargs = mock_api_call.call_args
    assert "x_neptune_client_metadata" not in kwargs


def test_with_neptune_client_metadata_with_context() -> None:
    # given
    mock_api_call = Mock()

    # when
    with (
        mock.patch("neptune_query.internal.query_metadata_context.ADD_QUERY_METADATA", True),
        mock.patch("neptune_query.internal.query_metadata_context.get_client_version", return_value="1.2.3"),
        mock.patch("neptune_query.internal.query_metadata_context.random.choices", return_value="abcd1234"),
        mock.patch(
            "neptune_query.internal.query_metadata_context.env.NEPTUNE_QUERY_METADATA.get",
            return_value='{"magic_number": 42, "names": ["John", "Larry"]}',
        ),
    ):
        decorated_call = with_neptune_client_metadata(mock_api_call)
        with use_query_metadata(api_function="test_api"):
            decorated_call(arg1="value1")

    # then
    mock_api_call.assert_called_once()
    _, kwargs = mock_api_call.call_args
    assert "x_neptune_client_metadata" in kwargs
    expected_json = json.dumps(
        {"fn": "test_api", "v": "1.2.3", "qid": "abcd1234", "ud": {"magic_number": 42, "names": ["John", "Larry"]}}
    )
    assert kwargs["x_neptune_client_metadata"] == expected_json
