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

import functools as ft
from dataclasses import dataclass
from typing import (
    Any,
    Optional,
    Union,
)

import numpy as np
from neptune_api.api.retrieval import get_multiple_float_series_values_proto
from neptune_api.client import AuthenticatedClient
from neptune_api.models import FloatTimeSeriesValuesRequest
from neptune_api.proto.neptune_pb.api.v1.model.series_values_pb2 import ProtoFloatSeriesValuesResponseDTO

from neptune_query.internal.query_metadata_context import with_neptune_client_metadata

from .. import identifiers
from ..logger import get_logger
from ..retrieval import (
    retry,
    util,
)
from .search import ContainerType

logger = get_logger()

TOTAL_POINT_LIMIT: int = 1_000_000


@dataclass(frozen=True, slots=True)
class MetricDatapoints:
    steps: np.ndarray
    values: np.ndarray
    timestamps: Optional[np.ndarray]
    is_preview: Optional[np.ndarray]
    completion_ratio: Optional[np.ndarray]

    @classmethod
    def allocate(cls, size: int, include_timestamp: bool, include_preview: bool) -> "MetricDatapoints":
        return cls(
            steps=np.empty(size, dtype=np.float64),
            values=np.empty(size, dtype=np.float64),
            timestamps=np.empty(size, dtype=np.float64) if include_timestamp else None,
            is_preview=np.empty(size, dtype=bool) if include_preview else None,
            completion_ratio=np.empty(size, dtype=np.float64) if include_preview else None,
        )

    @classmethod
    def concatenate(cls, metrics_list: list["MetricDatapoints"]) -> "MetricDatapoints":
        return cls(
            steps=np.concatenate([m.steps for m in metrics_list], axis=0),
            values=np.concatenate([m.values for m in metrics_list], axis=0),
            timestamps=np.concatenate([m.timestamps for m in metrics_list], axis=0)
            if metrics_list[0].timestamps is not None
            else None,
            is_preview=np.concatenate([m.is_preview for m in metrics_list], axis=0)
            if metrics_list[0].is_preview is not None
            else None,
            completion_ratio=np.concatenate([m.completion_ratio for m in metrics_list], axis=0)
            if metrics_list[0].completion_ratio is not None
            else None,
        )

    @property
    def length(self) -> int:
        return self.steps.size  # type: ignore # np.ndarray.size is int

    @classmethod
    def length_sum(cls, metrics_list: list["MetricDatapoints"]) -> int:
        return sum(m.length for m in metrics_list)


def fetch_multiple_series_values(
    client: AuthenticatedClient,
    run_attribute_definitions: list[identifiers.RunAttributeDefinition],
    include_inherited: bool,
    container_type: ContainerType,
    include_timestamp: bool,
    include_preview: bool,
    step_range: tuple[Union[float, None], Union[float, None]] = (None, None),
    tail_limit: Optional[int] = None,
) -> dict[identifiers.RunAttributeDefinition, MetricDatapoints]:
    if not run_attribute_definitions:
        return {}

    assert len(run_attribute_definitions) <= TOTAL_POINT_LIMIT, (
        f"The number of requested attributes {len(run_attribute_definitions)} exceeds the maximum limit of "
        f"{TOTAL_POINT_LIMIT}. Please reduce the number of attributes."
    )

    width = len(str(len(run_attribute_definitions) - 1))
    request_id_to_attribute: dict[str, identifiers.RunAttributeDefinition] = {
        f"{i:0{width}d}": attr for i, attr in enumerate(run_attribute_definitions)
    }

    params: dict[str, Any] = {
        "requests": [
            {
                "requestId": request_id,
                "series": {
                    "holder": {
                        "identifier": str(run_attribute.run_identifier),
                        "type": "experiment",
                    },
                    "attribute": run_attribute.attribute_definition.name,
                    "lineage": "FULL" if include_inherited else "NONE",
                    "lineageEntityType": "EXPERIMENT" if container_type == ContainerType.EXPERIMENT else "RUN",
                    "includePreview": include_preview,
                },
            }
            for request_id, run_attribute in request_id_to_attribute.items()
        ],
        "stepRange": {"from": step_range[0], "to": step_range[1]},
        "order": "ascending" if not tail_limit else "descending",
    }

    paged_results: dict[identifiers.RunAttributeDefinition, list[MetricDatapoints]] = {}

    for page_result in util.fetch_pages(
        client=client,
        fetch_page=_fetch_metrics_page,
        process_page=ft.partial(
            _process_metrics_page,
            request_id_to_attribute=request_id_to_attribute,
            include_timestamp=include_timestamp,
            include_preview=include_preview,
            reverse_order=tail_limit is not None,
        ),
        make_new_page_params=ft.partial(
            _make_new_metrics_page_params,
            request_id_to_attribute=request_id_to_attribute,
            tail_limit=tail_limit,
            partial_results=paged_results,
        ),
        params=params,
    ):
        for definition, metric_values in page_result.items:
            paged_results.setdefault(definition, []).append(metric_values)

    results: dict[identifiers.RunAttributeDefinition, MetricDatapoints] = {}
    for definition, paged_metric_values in paged_results.items():
        if len(paged_metric_values) > 1:
            results[definition] = MetricDatapoints.concatenate(paged_metric_values)
        elif len(paged_metric_values) == 1:
            results[definition] = paged_metric_values[0]
        else:
            pass

    return results


def _fetch_metrics_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
) -> ProtoFloatSeriesValuesResponseDTO:
    logger.debug(f"Calling get_multiple_float_series_values_proto with params: {params}")

    body = FloatTimeSeriesValuesRequest.from_dict(params)
    call_api = retry.handle_errors_default(
        with_neptune_client_metadata(get_multiple_float_series_values_proto.sync_detailed)
    )
    response = call_api(client=client, body=body)

    logger.debug(
        f"get_multiple_float_series_values_proto response status: {response.status_code}, "
        f"content length: {len(response.content) if response.content else 'no content'}"
    )
    return ProtoFloatSeriesValuesResponseDTO.FromString(response.content)


def _process_metrics_page(
    data: ProtoFloatSeriesValuesResponseDTO,
    request_id_to_attribute: dict[str, identifiers.RunAttributeDefinition],
    include_timestamp: bool,
    include_preview: bool,
    reverse_order: bool,
) -> util.Page[tuple[identifiers.RunAttributeDefinition, MetricDatapoints]]:
    result = {}
    for series in data.series:
        metric_values = MetricDatapoints.allocate(
            size=len(series.series.values), include_timestamp=include_timestamp, include_preview=include_preview
        )

        for i, point in enumerate(series.series.values):
            idx = metric_values.length - 1 - i if reverse_order else i

            metric_values.steps[idx] = point.step
            metric_values.values[idx] = point.value
            if include_timestamp:
                assert metric_values.timestamps is not None
                metric_values.timestamps[idx] = point.timestamp_millis
            if include_preview:
                assert metric_values.is_preview is not None
                assert metric_values.completion_ratio is not None
                metric_values.is_preview[idx] = point.is_preview
                metric_values.completion_ratio[idx] = point.completion_ratio
        definition = request_id_to_attribute[series.requestId]
        result[definition] = metric_values

    return util.Page(items=list(result.items()))


def _make_new_metrics_page_params(
    params: dict[str, Any],
    data: Optional[ProtoFloatSeriesValuesResponseDTO],
    request_id_to_attribute: dict[str, identifiers.RunAttributeDefinition],
    tail_limit: Optional[int],
    partial_results: dict[identifiers.RunAttributeDefinition, list[MetricDatapoints]],
) -> Optional[dict[str, Any]]:
    if data is None:  # no past data, we are fetching the first page
        for request in params["requests"]:
            if "afterStep" in request:
                del request["afterStep"]
        per_series_points_limit = max(1, TOTAL_POINT_LIMIT // len(params["requests"]))
        if tail_limit is not None:
            per_series_points_limit = min(per_series_points_limit, tail_limit)
        params["perSeriesPointsLimit"] = per_series_points_limit
        return params

    prev_per_series_points_limit = params["perSeriesPointsLimit"]

    new_request_after_steps = {}
    for series in data.series:
        request_id = series.requestId
        value_size = len(series.series.values)
        is_page_full = value_size == prev_per_series_points_limit

        attribute = request_id_to_attribute[request_id]
        need_more_points = (
            MetricDatapoints.length_sum(partial_results[attribute]) < tail_limit if tail_limit is not None else True
        )

        if is_page_full and need_more_points:
            new_request_after_steps[request_id] = series.series.values[-1].step

    if not new_request_after_steps:  # no data left to fetch, return None to stop
        return None

    new_requests = []
    for request in params["requests"]:
        request_id = request["requestId"]
        if request_id in new_request_after_steps:
            after_step = new_request_after_steps[request_id]
            request["afterStep"] = after_step
            new_requests.append(request)
    params["requests"] = new_requests

    per_series_points_limit = max(1, TOTAL_POINT_LIMIT // len(params["requests"]))
    if tail_limit is not None:
        already_fetched = next(
            MetricDatapoints.length_sum(partial_results[request_id_to_attribute[request_id]])
            for request_id in new_request_after_steps.keys()
        )  # assumes the results for all unfinished series have the same length
        per_series_points_limit = min(per_series_points_limit, tail_limit - already_fetched)
    params["perSeriesPointsLimit"] = per_series_points_limit

    return params
