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

from neptune_query.internal.query_metadata_context import _generate_nq_query_id


def test_generate_nq_query_id_length():
    """Test that generated query IDs are always 8 characters long.

    Note: We don't seed the random number generator since it uses system RNG,
    but we want to verify that the code consistently generates 8-character strings
    regardless of the random values we're getting.
    """

    query_ids = []
    for i in range(1000):
        query_ids.append(_generate_nq_query_id())
    assert all(len(query_id) == 8 for query_id in query_ids)
