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

from dataclasses import dataclass
from typing import (
    Iterable,
    Literal,
    Optional,
)

from neptune_api.api.retrieval import get_timeseries_buckets_proto
from neptune_api.client import AuthenticatedClient
from neptune_api.proto.neptune_pb.api.v1.model.requests_pb2 import (
    LineageEntityType,
    ProtoCustomExpression,
    ProtoGetTimeseriesBucketsRequest,
    ProtoLineage,
    ProtoScale,
    ProtoView,
    ProtoXAxis,
    XSteps,
)
from neptune_api.proto.neptune_pb.api.v1.model.series_values_pb2 import ProtoTimeseriesBucketsDTO

from ..identifiers import RunAttributeDefinition
from ..logger import get_logger
from ..query_metadata_context import with_neptune_client_metadata
from . import retry
from .search import ContainerType
from .util import ProtobufPayload

logger = get_logger()


@dataclass(frozen=True)
class TimeseriesBucket:
    index: int
    from_x: float
    to_x: float
    first_x: Optional[float]
    first_y: Optional[float]
    last_x: Optional[float]
    last_y: Optional[float]

    # statistics:
    y_min: Optional[float]
    y_max: Optional[float]
    finite_point_count: int
    nan_count: int
    positive_inf_count: int
    negative_inf_count: int
    finite_points_sum: Optional[float]


# Build once at module import
ATTRIBUTE_NAME_TO_FORMULA_ESCAPE_MAP = {
    ord("\\"): r"\\",
    ord("$"): r"\$",
    ord("{"): r"\{",
    ord("}"): r"\}",
}


def attribute_name_to_formula(attribute_name: str) -> str:
    return "${" + attribute_name.translate(ATTRIBUTE_NAME_TO_FORMULA_ESCAPE_MAP) + "}"


def int_to_uuid(num: int) -> str:
    # Generate a UUID from an integer in a deterministic way
    # This is a simple implementation and can be replaced with a more robust one if needed
    return f"00000000-0000-0000-0000-{num:012d}"


def fetch_time_series_buckets(
    client: AuthenticatedClient,
    run_attribute_definitions: Iterable[RunAttributeDefinition],
    container_type: ContainerType,
    x: Literal["step"],
    lineage_to_the_root: bool,
    include_point_previews: bool,
    limit: int,
    x_range: Optional[tuple[float, float]],
) -> dict[RunAttributeDefinition, list[TimeseriesBucket]]:
    run_attribute_definitions = list(run_attribute_definitions)

    lineage = ProtoLineage.FULL if lineage_to_the_root else ProtoLineage.ONLY_OWNED
    lineage_entity_type = LineageEntityType.RUN if container_type == ContainerType.RUN else LineageEntityType.EXPERIMENT

    if x == "step":
        xAxis = ProtoXAxis(steps=XSteps())
    # elif x == "relativeTime":
    #     xAxis = ProtoXAxis(relativeTime=XRelativeTime())
    # elif x == "epochMillis":
    #     xAxis = ProtoXAxis(epochMillis=XEpochMillis())
    # elif x == "custom":
    #     xAxis = ProtoXAxis(custom=XCustom())
    else:
        raise ValueError('Unsupported x value. Only "step" is supported')

    expressions = {}
    request_id_to_request_mapping = {}
    for num, run_attribute_definition in enumerate(run_attribute_definitions):
        if num >= 1000:
            raise ValueError("Cannot fetch more than 1000 time series at once")

        request_id = int_to_uuid(num)
        request_id_to_request_mapping[request_id] = run_attribute_definition

        expressions[request_id] = ProtoCustomExpression(
            requestId=request_id,
            runId=str(run_attribute_definition.run_identifier),
            entityType=lineage_entity_type,
            lineage=lineage,
            customYFormula=attribute_name_to_formula(run_attribute_definition.attribute_definition.name),
            includePreview=include_point_previews,
        )

    view = ProtoView(
        maxBuckets=limit,
        xScale=ProtoScale.linear,
        yScale=ProtoScale.linear,
        xAxis=xAxis,
    )
    if x_range is not None:
        x_from, x_to = x_range
        view.to = x_to
        setattr(view, "from", x_from)  # from is a reserved keyword in Python

    request_object = ProtoGetTimeseriesBucketsRequest(
        expressions=expressions.values(),
        view=view,
    )

    logger.debug(f"Calling get_timeseries_buckets_proto with body: {request_object}")

    call_api = retry.handle_errors_default(with_neptune_client_metadata(get_timeseries_buckets_proto.sync_detailed))
    response = call_api(
        client=client,
        body=ProtobufPayload(request_object),
    )

    logger.debug(
        f"get_timeseries_buckets_proto response status: {response.status_code}, "
        f"content length: {len(response.content) if response.content else 'no content'}"
    )

    result_object: ProtoTimeseriesBucketsDTO = ProtoTimeseriesBucketsDTO.FromString(response.content)

    out: dict[RunAttributeDefinition, list[TimeseriesBucket]] = {}

    for entry in result_object.entries:
        request = request_id_to_request_mapping.get(entry.requestId, None)
        if request is None:
            raise RuntimeError(f"Received unknown requestId from the server: {request_id}")

        if request in out:
            raise RuntimeError(f"Received duplicate requestId from the server: {request_id}")

        out[request] = [
            TimeseriesBucket(
                index=bucket.index,
                from_x=bucket.fromX,
                to_x=bucket.toX,
                first_x=bucket.first.x if bucket.HasField("first") else None,
                first_y=bucket.first.y if bucket.HasField("first") else None,
                last_x=bucket.last.x if bucket.HasField("last") else None,
                last_y=bucket.last.y if bucket.HasField("last") else None,
                y_min=bucket.localMin if bucket.HasField("localMin") else None,
                y_max=bucket.localMax if bucket.HasField("localMax") else None,
                finite_point_count=bucket.finitePointCount,
                nan_count=bucket.nanCount,
                positive_inf_count=bucket.positiveInfCount,
                negative_inf_count=bucket.negativeInfCount,
                finite_points_sum=bucket.localSum if bucket.HasField("localSum") else None,
            )
            for bucket in entry.bucket
        ]

    for request in run_attribute_definitions:
        if request not in out:
            raise RuntimeError("Didn't get data for all the requests from the server. " f"Missing request {request_id}")

    return out
