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
import random
from dataclasses import dataclass
from typing import (
    Iterable,
    Literal,
    Optional,
)

from neptune_api.client import AuthenticatedClient

from .attribute_values import AttributeValue
from .search import ContainerType


@dataclass(frozen=True)
class BucketMetric:
    index: int
    from_x: float
    to_x: float
    first_x: float
    first_y: float
    last_x: float
    last_y: float


def fetch_time_series_buckets(
    client: AuthenticatedClient,
    attribute_values: Iterable[AttributeValue],
    container_type: ContainerType,
    x: Literal["step"],
    lineage_to_the_root: bool,
    include_point_previews: bool,
    limit: Optional[int],
) -> dict[AttributeValue, list[BucketMetric]]:
    if not attribute_values:
        return {}

    result = {}
    for run_attribute_definition in list(attribute_values):
        result[run_attribute_definition] = [
            BucketMetric(
                index=i,
                from_x=20 * i,
                to_x=20 * (i + 1),
                first_x=-random.random(),
                first_y=random.random(),
                last_x=-random.random(),
                last_y=random.random(),
            )
            for i in range(5)
        ]
    return result
