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
#
from __future__ import annotations

import json

from neptune_query.internal.query_metadata_context import QueryMetadata


def test_to_json_with_none_user_data() -> None:
    # given
    qm = QueryMetadata(
        api_function="fetch_experiments_table_global",
        client_version="nq/1.2.3",
        nq_query_id="abcd1234",
        user_data=None,
    )

    # when
    payload = json.loads(qm.to_json())

    # then
    assert payload == {
        "fn": "fetch_experiments_table_global",
        "v": "nq/1.2.3",
        "qid": "abcd1234",
        "ud": None,
    }


def test_to_json_with_80_char_plain_string_user_data() -> None:
    # given: 80 chars -> json.dumps will be 82 chars including quotes; allowed is up to 82 chars
    user_data = "x" * 80
    qm = QueryMetadata(
        api_function="fetch_experiments_table_global",
        client_version="nq/1.2.3",
        nq_query_id="abcd1234",
        user_data=user_data,
    )

    # when
    out = qm.to_json()
    payload = json.loads(out)

    # then
    assert len(out) < 200  # sanity check on total length

    assert payload["ud"] == user_data  # kept as a plain string
    assert payload["fn"] == "fetch_experiments_table_global"
    assert payload["v"] == "nq/1.2.3"
    assert payload["qid"] == "abcd1234"


def test_to_json_with_valid_json_string_user_data() -> None:
    # given: JSON-encoded user data should be parsed and embedded as object
    user_obj = {"k": "v"}
    qm = QueryMetadata(
        api_function="fetch_experiments_table_global",
        client_version="nq/1.2.3",
        nq_query_id="abcd1234",
        user_data=json.dumps(user_obj),
    )

    # when
    out = qm.to_json()
    payload = json.loads(out)

    # then
    assert len(out) < 200  # sanity check on total length

    assert payload["ud"] == user_obj  # becomes a dict, not a string
    assert payload["fn"] == "fetch_experiments_table_global"
    assert payload["v"] == "nq/1.2.3"
    assert payload["qid"] == "abcd1234"


def test_to_json_with_81_char_plain_string_user_data_triggers_too_long(caplog) -> None:
    # given: 81 chars -> json.dumps will be 83 chars including quotes; exceeds the threshold
    user_data = "y" * 81

    with caplog.at_level("DEBUG"):
        qm = QueryMetadata(
            api_function="fetch_experiments_table_global",
            client_version="nq/1.2.3",
            nq_query_id="abcd1234",
            user_data=user_data,
        )

    # when
    out = qm.to_json()
    payload = json.loads(out)

    # then
    assert len(out) < 200  # sanity check on total length
    assert payload["ud"] == "NEPTUNE_QUERY_METADATA too long"

    assert "User data in NEPTUNE_QUERY_METADATA env too long (and not JSON-encoded)" in caplog.text


def test_to_json_with_long_string_compacting_to_short_json() -> None:
    # given: JSON-encoded input that's long as a string but short when parsed
    user_data = """
        {
                                  "abc": 123,
                                  "def": "hello"
        }
    """

    assert len(user_data) > 82  # sanity check that the input string is long

    qm = QueryMetadata(
        api_function="fetch_experiments_table_global",
        client_version="nq/1.2.3",
        nq_query_id="abcd1234",
        user_data=user_data,
    )

    # when
    out = qm.to_json()
    payload = json.loads(out)

    # then
    assert len(out) < 200
    assert payload["ud"] == {"abc": 123, "def": "hello"}


def test_to_json_with_long_json_encoded_user_data_triggers_too_long(caplog) -> None:
    # given: JSON-encoded input where dumps length exceeds 82
    long_val = "z" * 83
    user_json_str = json.dumps({"k": long_val})

    with caplog.at_level("DEBUG"):
        qm = QueryMetadata(
            api_function="fetch_experiments_table_global",
            client_version="nq/1.2.3",
            nq_query_id="abcd1234",
            user_data=user_json_str,
        )

    # when
    out = qm.to_json()
    payload = json.loads(out)

    # then
    assert len(out) < 200
    assert payload["ud"] == "NEPTUNE_QUERY_METADATA too long"
    assert "User data in NEPTUNE_QUERY_METADATA env too long" in caplog.text


def test_to_json_with_non_ascii_string_user_data_resulting_in_ascii_output() -> None:
    # given
    user_data = "żółć"
    qm = QueryMetadata(
        api_function="fetch_experiments_table_global",
        client_version="nq/1.2.3",
        nq_query_id="abcd1234",
        user_data=user_data,
    )

    expected_str = (
        '{"fn": "fetch_experiments_table_global", "v": "nq/1.2.3", '
        '"qid": "abcd1234", "ud": "\\u017c\\u00f3\\u0142\\u0107"}'
    )

    # when
    out = qm.to_json()

    # then
    assert out == expected_str
    assert len(out) < 200  # sanity check on total length
