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
from neptune_api.proto.protobuf_v4plus.neptune_pb.api.v1.model.requests_pb2 import (
    AttributesHolderIdentifier,
    ProtoCustomExpression,
    ProtoGetTimeseriesBucketsRequest,
    ProtoLineage,
    ProtoPointFilters,
    ProtoScale,
    ProtoView,
    ProtoXAxis,
    XCustom,
    XEpochMillis,
    XRelativeTime,
    XSteps,
)

from ..identifiers import RunAttributeDefinition
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
    run_attribute_definitions: Iterable[RunAttributeDefinition],
    container_type: ContainerType,
    x: Literal["step"],
    lineage_to_the_root: bool,
    include_point_previews: bool,
    limit: Optional[int],
) -> dict[RunAttributeDefinition, list[BucketMetric]]:
    if not run_attribute_definitions:
        return {}

    run_attribute_definitions = list(run_attribute_definitions)

    lineage = ProtoLineage.FULL if lineage_to_the_root else ProtoLineage.ONLY_OWNED

    protobuf_msg = ProtoGetTimeseriesBucketsRequest(
        expressions=[
            ProtoCustomExpression(
                requestId="abc",
                holder=AttributesHolderIdentifier(
                    entityType=container_type.value,
                    identifier=run_attribute_definition.run_identifier.identifier,
                ),
                customYFormula=run_attribute_definition.attribute_definition.name,
                includePreview=include_point_previews,
                lineage=lineage,
            )
            for run_attribute_definition in run_attribute_definitions
        ],
        view=ProtoView(
            to=1.0,
            pointFilters=ProtoPointFilters(),
            maxBuckets=100,
            xScale=ProtoScale.linear,
            yScale=ProtoScale.linear,
            xAxis=ProtoXAxis(
                # if x=step:
                steps=XSteps(),
                # from neptune_api.proto.protobuf_v4plus.neptune_pb.api.v1.model.requests_pb2 import XEpochMillis
                # epochMillis=XEpochMillis(),
                # from neptune_api.proto.protobuf_v4plus.neptune_pb.api.v1.model.requests_pb2 import XRelativeTime
                # relativeTime=XRelativeTime(),
                # from neptune_api.proto.protobuf_v4plus.neptune_pb.api.v1.model.requests_pb2 import XCustom
                # custom=XCustom(),
            ),
        ),
    )
    print(str(protobuf_msg))
    alksdjfljkasdf()

    result = {}
    for run_attribute_definition in run_attribute_definitions:
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
