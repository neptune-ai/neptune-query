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
    Iterable,
    Literal,
    Optional,
    Sequence,
    cast,
)

import pandas as pd

from neptune_query.generated.neptune_api.client import AuthenticatedClient

from .. import client as _client
from .. import identifiers
from ..composition import (
    concurrency,
    type_inference,
    validation,
)
from ..composition.attribute_components import fetch_attribute_definitions_split
from ..context import (
    Context,
    get_context,
    validate_context,
)
from ..filters import (
    _BaseAttributeFilter,
    _Filter,
)
from ..output_format import create_metrics_dataframe
from ..retrieval import (
    search,
    split,
)
from ..retrieval.metrics import (
    FloatPointValue,
    fetch_multiple_series_values,
)
from ..retrieval.search import ContainerType

__all__ = ("fetch_metrics",)

RunLabelIdentifier = identifiers.SysId | identifiers.CustomRunId


def fetch_metrics(
    *,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[_Filter],
    attributes: _BaseAttributeFilter,
    exact_run_ids: Optional[list[str]],
    exact_attribute_names: Optional[list[str]],
    include_time: Optional[Literal["absolute"]],
    step_range: tuple[Optional[float], Optional[float]],
    lineage_to_the_root: bool,
    tail_limit: Optional[int],
    type_suffix_in_column_names: bool,
    include_point_previews: bool,
    context: Optional[Context] = None,
    container_type: ContainerType,
) -> pd.DataFrame:
    validation.validate_step_range(step_range)
    validation.validate_tail_limit(tail_limit)
    validation.validate_include_time(include_time)
    restricted_attributes = validation.restrict_attribute_filter_type(attributes, type_in={"float_series"})

    valid_context = validate_context(context or get_context())
    client = _client.get_client(context=valid_context)

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

        metrics_data, sys_id_to_label_mapping = _fetch_metrics(
            filter_=inferred_filter,
            attributes=restricted_attributes,
            exact_run_ids=exact_run_ids,
            exact_attribute_names=exact_attribute_names,
            client=client,
            project_identifier=project_identifier,
            step_range=step_range,
            lineage_to_the_root=lineage_to_the_root,
            include_point_previews=include_point_previews,
            tail_limit=tail_limit,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            container_type=container_type,
        )

        df = create_metrics_dataframe(
            metrics_data=metrics_data,
            sys_id_label_mapping=sys_id_to_label_mapping,
            index_column_name="experiment" if container_type == ContainerType.EXPERIMENT else "run",
            timestamp_column_name="absolute_time" if include_time == "absolute" else None,
            include_point_previews=include_point_previews,
            type_suffix_in_column_names=type_suffix_in_column_names,
        )

    return df


def _fetch_metrics(
    filter_: Optional[_Filter],
    attributes: _BaseAttributeFilter,
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    step_range: tuple[Optional[float], Optional[float]],
    lineage_to_the_root: bool,
    include_point_previews: bool,
    tail_limit: Optional[int],
    container_type: ContainerType,
    exact_run_ids: Optional[list[str]] = None,
    exact_attribute_names: Optional[list[str]] = None,
) -> tuple[dict[identifiers.RunAttributeDefinition, list[FloatPointValue]], dict[RunLabelIdentifier, str]]:
    """Fetch float-series metric points for matching containers (runs/experiments).

    This function resolves the target containers, determines which attributes to
    query (either explicitly or by filtering to ``float_series`` definitions),
    fetches series values concurrently in splits, and merges chunked results.

    Behavior
    --------
    Default path (experiments and non-fast-path runs):
      1) Search for matching containers and build a ``SysId -> label`` mapping.
      2) Determine run-attribute pairs:
         - If ``exact_attribute_names`` is provided, build attribute definitions
           directly (skips attribute-definition lookup).
         - Otherwise, resolve attribute definitions via
           ``fetch_attribute_definitions_split`` and keep only ``float_series``.
      3) Fetch values via ``fetch_multiple_series_values`` concurrently and merge
         the split outputs.

    Run fast path:
      Activated only when:
        - ``container_type == ContainerType.RUN``, and
        - both ``exact_run_ids`` and ``exact_attribute_names`` are provided.
      In this mode the function skips container search and definition lookup.
      It deduplicates run ids and attribute names, builds the cartesian product
      of ``(run_id, attribute_name)`` into ``RunAttributeDefinition`` objects,
      and stores each provided run id as ``RunIdentifier.custom_run_id``.
      The returned label mapping is ``{CustomRunId(run_id): run_id}``.
    """

    def merge_results(
        results: Generator[dict[identifiers.RunAttributeDefinition, list[FloatPointValue]], None, None],
    ) -> dict[identifiers.RunAttributeDefinition, list[FloatPointValue]]:
        metrics_data: dict[identifiers.RunAttributeDefinition, list[FloatPointValue]] = {}
        for result in results:
            for run_attribute_definition, metric_points in result.items():
                metrics_data.setdefault(run_attribute_definition, []).extend(metric_points)
        return metrics_data

    def fetch_metrics_for_run_attribute_definitions(
        *,
        run_attribute_definitions: Iterable[identifiers.RunAttributeDefinition],
    ) -> concurrency.OUT:
        return concurrency.generate_concurrently(
            items=split.split_series_attributes(items=run_attribute_definitions),
            executor=executor,
            downstream=lambda run_attribute_definitions_split: concurrency.return_value(
                fetch_multiple_series_values(
                    client=client,
                    run_attribute_definitions=run_attribute_definitions_split,
                    include_inherited=lineage_to_the_root,
                    include_preview=include_point_previews,
                    container_type=container_type,
                    step_range=step_range,
                    tail_limit=tail_limit,
                )
            ),
        )

    def _make_run_identifier(
        *,
        run_id: identifiers.SysId | identifiers.CustomRunId,
        identifiers_are_custom_run_ids: bool,
    ) -> identifiers.RunIdentifier:
        if identifiers_are_custom_run_ids:
            return identifiers.RunIdentifier(
                project_identifier=project_identifier,
                custom_run_id=cast(identifiers.CustomRunId, run_id),
            )
        return identifiers.RunIdentifier(
            project_identifier=project_identifier,
            sys_id=cast(identifiers.SysId, run_id),
        )

    def fetch_metrics_for_run_ids(
        *,
        run_ids: Sequence[identifiers.SysId | identifiers.CustomRunId],
        deduplicated_exact_attribute_names: Optional[set[str]],
        identifiers_are_custom_run_ids: bool = False,
    ) -> concurrency.OUT:
        if deduplicated_exact_attribute_names is not None:
            return concurrency.generate_concurrently(
                items=split.split_sys_ids(cast(list[identifiers.SysId], run_ids)),
                executor=executor,
                downstream=lambda run_id_batch: fetch_metrics_for_run_attribute_definitions(
                    run_attribute_definitions=(
                        identifiers.RunAttributeDefinition(
                            run_identifier=_make_run_identifier(
                                run_id=run_id,
                                identifiers_are_custom_run_ids=identifiers_are_custom_run_ids,
                            ),
                            attribute_definition=identifiers.AttributeDefinition(
                                name=attribute_name,
                                type="float_series",
                            ),
                        )
                        for run_id in run_id_batch
                        for attribute_name in deduplicated_exact_attribute_names
                    ),
                ),
            )

        if identifiers_are_custom_run_ids:
            raise ValueError("Custom run ids require exact attribute names")

        return fetch_attribute_definitions_split(
            client=client,
            project_identifier=project_identifier,
            attribute_filter=attributes,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            sys_ids=cast(list[identifiers.SysId], run_ids),
            downstream=lambda sys_ids_split, definitions_page: fetch_metrics_for_run_attribute_definitions(
                run_attribute_definitions=(
                    identifiers.RunAttributeDefinition(
                        run_identifier=identifiers.RunIdentifier(project_identifier, sys_id),
                        attribute_definition=definition,
                    )
                    for sys_id in sys_ids_split
                    for definition in definitions_page.items
                    if definition.type == "float_series"
                ),
            ),
        )

    deduplicated_exact_attribute_names = set(exact_attribute_names) if exact_attribute_names is not None else None
    if container_type == ContainerType.RUN and exact_run_ids is not None and exact_attribute_names is not None:
        deduplicated_run_ids = {identifiers.CustomRunId(run_id) for run_id in exact_run_ids}
        run_label_mapping: dict[RunLabelIdentifier, str] = {run_id: str(run_id) for run_id in deduplicated_run_ids}

        output = fetch_metrics_for_run_ids(
            run_ids=list(run_label_mapping.keys()),
            deduplicated_exact_attribute_names=deduplicated_exact_attribute_names,
            identifiers_are_custom_run_ids=True,
        )
        return merge_results(concurrency.gather_results(output)), run_label_mapping

    sys_id_label_mapping: dict[RunLabelIdentifier, str] = {}

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
        downstream=lambda sys_ids: fetch_metrics_for_run_ids(
            run_ids=sys_ids,
            deduplicated_exact_attribute_names=deduplicated_exact_attribute_names,
        ),
    )
    return merge_results(concurrency.gather_results(output)), sys_id_label_mapping
