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

from concurrent.futures import Executor
from typing import (
    Generator,
    Literal,
    Optional,
    Union,
)

import pandas as pd
from neptune_api.client import AuthenticatedClient

from .. import identifiers
from ..client import get_client
from ..composition import (
    concurrency,
    type_inference,
    validation,
)
from ..context import (
    Context,
    get_context,
    validate_context,
)
from ..filters import (
    _BaseAttributeFilter,
    _Filter,
)
from ..identifiers import RunAttributeDefinition
from ..output_format import create_metric_buckets_dataframe
from ..retrieval import (
    metric_buckets,
    search,
    util,
)
from ..retrieval.attribute_values import AttributeValue
from ..retrieval.metric_buckets import TimeseriesBucket
from ..retrieval.search import ContainerType
from .attribute_components import fetch_attribute_values_by_filter_split

__all__ = ("fetch_metric_buckets",)


def fetch_metric_buckets(
    *,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[_Filter],
    x: Union[Literal["step"]] = "step",
    y: _BaseAttributeFilter,
    limit: int,
    lineage_to_the_root: bool,
    include_point_previews: bool,
    context: Optional[Context] = None,
    container_type: ContainerType,
) -> pd.DataFrame:
    validation.validate_metrics_x(x)
    validation.validate_bucket_limit(limit)
    restricted_y = validation.restrict_attribute_filter_type(y, type_in={"float_series"})
    limit = limit + 1  # we request one extra bucket because the 1st one is (-inf, 1st point] and we merge it

    valid_context = validate_context(context or get_context())
    client = get_client(context=valid_context)

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        inference_result = type_inference.infer_attribute_types_in_filter(
            client=client,
            project_identifier=project_identifier,
            filter_=filter_,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )
        inferred_filter = inference_result.get_result_or_raise()
        inference_result.emit_warnings()

        metric_buckets_data, sys_id_to_label_mapping = _fetch_metric_buckets(
            filter_=inferred_filter,
            x=x,
            y=restricted_y,
            client=client,
            project_identifier=project_identifier,
            limit=limit,
            lineage_to_the_root=lineage_to_the_root,
            include_point_previews=include_point_previews,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            container_type=container_type,
        )

        df = create_metric_buckets_dataframe(
            buckets_data=metric_buckets_data,
            sys_id_label_mapping=sys_id_to_label_mapping,
            container_column_name="experiment" if container_type == ContainerType.EXPERIMENT else "run",
        )

    return df


def _fetch_metric_buckets(
    filter_: Optional[_Filter],
    x: Literal["step"],
    y: _BaseAttributeFilter,
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    lineage_to_the_root: bool,
    include_point_previews: bool,
    limit: int,
    container_type: ContainerType,
) -> tuple[dict[identifiers.RunAttributeDefinition, list[TimeseriesBucket]], dict[identifiers.SysId, str]]:
    sys_id_label_mapping: dict[identifiers.SysId, str] = {}

    def go_fetch_sys_attrs() -> Generator[list[identifiers.SysId], None, None]:
        for page in search.fetch_sys_id_labels(container_type)(
            client=client,
            project_identifier=project_identifier,
            filter_=filter_,
        ):
            sys_ids = []
            for item in page.items:
                sys_id_label_mapping[item.sys_id] = item.label
                sys_ids.append(item.sys_id)
            yield sys_ids

    output = concurrency.generate_concurrently(
        items=go_fetch_sys_attrs(),
        executor=executor,
        downstream=lambda sys_ids: fetch_attribute_values_by_filter_split(
            client=client,
            project_identifier=project_identifier,
            sys_ids=sys_ids,
            attribute_filter=y,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            downstream=concurrency.return_value,
        ),
    )

    results: Generator[util.Page[AttributeValue], None, None] = concurrency.gather_results(output)

    run_attribute_definitions = []
    for page in results:
        for value in page.items:
            run_attribute_definition = RunAttributeDefinition(
                run_identifier=value.run_identifier, attribute_definition=value.attribute_definition
            )
            run_attribute_definitions.append(run_attribute_definition)

    buckets_data = metric_buckets.fetch_time_series_buckets(
        client=client,
        x=x,
        run_attribute_definitions=run_attribute_definitions,
        lineage_to_the_root=lineage_to_the_root,
        include_point_previews=include_point_previews,
        limit=limit,
        container_type=container_type,
        x_range=None,
    )

    return buckets_data, sys_id_label_mapping
