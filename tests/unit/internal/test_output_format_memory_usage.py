import time
from datetime import timedelta
from typing import (
    Literal,
    Optional,
)

import pytest
from humanize import (
    naturaldelta,
    naturalsize,
)

from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.output_format import create_metrics_dataframe
from neptune_query.internal.retrieval.metrics import MetricValues
from tests.helpers.memory import MemoryMonitor


def _generate_metrics_dataset(
    *,
    num_experiments: int,
    num_metrics: int,
    num_steps: int,
    include_timestamp: bool,
    include_preview: bool,
) -> tuple[dict[RunAttributeDefinition, MetricValues], dict[SysId, str]]:
    project = ProjectIdentifier("perf/project")
    metrics_data: dict[RunAttributeDefinition, MetricValues] = {}
    label_mapping: dict[SysId, str] = {}

    for experiment_index in range(num_experiments):
        sys_id = SysId(f"sysid{experiment_index}")
        label_mapping[sys_id] = f"exp{experiment_index}"
        run_identifier = RunIdentifier(project, sys_id)

        for metric_index in range(num_metrics):
            attribute = AttributeDefinition(f"metric_{metric_index}", "float_series")
            definition = RunAttributeDefinition(run_identifier, attribute)
            metric_values = MetricValues.allocate(
                size=num_steps, include_timestamp=include_timestamp, include_preview=include_preview
            )
            for step in range(num_steps):
                metric_values.steps[step] = step
                metric_values.values[step] = step
                if include_timestamp:
                    metric_values.timestamps[step] = 1_600_000_000_000 + step * 1_000
                if include_preview:
                    metric_values.is_preview[step] = False
                    metric_values.completion_ratio[step] = 1.0
            metrics_data[definition] = metric_values

    return metrics_data, label_mapping


# This test doesn't test any regressions, but it's very useful when iterating on performance improvements
@pytest.mark.parametrize(
    "timestamp_column_name,include_point_previews",
    [
        (None, False),
        (None, True),
        ("absolute_time", False),
        ("absolute_time", True),
    ],
)
def test_create_metrics_dataframe_timing(
    timestamp_column_name: Optional[Literal["absolute_time"]], include_point_previews: bool
):
    metrics_data, sys_id_label_mapping = _generate_metrics_dataset(
        num_experiments=30,
        num_metrics=60,
        num_steps=400,
        include_timestamp=timestamp_column_name is not None,
        include_preview=include_point_previews,
    )

    start = time.perf_counter_ns()
    _ = create_metrics_dataframe(
        metrics_data=metrics_data,
        sys_id_label_mapping=sys_id_label_mapping,
        type_suffix_in_column_names=True,
        timestamp_column_name=timestamp_column_name,
        include_point_previews=include_point_previews,
        index_column_name="experiment",
    )
    end = time.perf_counter_ns()
    delta = timedelta(microseconds=(end - start) / 1e3)

    print()
    print(f"Timestamp column: {timestamp_column_name}, include_point_previews: {include_point_previews}")
    print("Duration:", naturaldelta(value=delta, minimum_unit="microseconds"))


@pytest.mark.parametrize(
    "timestamp_column_name,include_point_previews,max_allowed_ratio",
    [
        (None, False, 2.5),
        (None, True, 4.5),
        ("absolute_time", False, 3.8),
        ("absolute_time", True, 4.0),
    ],
)
def test_create_metrics_dataframe_timestamp_peak_memory_usage(
    timestamp_column_name: Optional[Literal["absolute_time"]], include_point_previews: bool, max_allowed_ratio: float
):
    metrics_data, sys_id_label_mapping = _generate_metrics_dataset(
        num_experiments=1000,
        num_metrics=1000,
        num_steps=1,
        include_timestamp=timestamp_column_name is not None,
        include_preview=include_point_previews,
    )

    monitor = MemoryMonitor()
    monitor.start()
    try:
        df = create_metrics_dataframe(
            metrics_data=metrics_data,
            sys_id_label_mapping=sys_id_label_mapping,
            type_suffix_in_column_names=True,
            timestamp_column_name=timestamp_column_name,
            include_point_previews=include_point_previews,
            index_column_name="experiment",
        )
    finally:
        peak = monitor.stop()

    dataframe_memory = df.memory_usage(deep=True).sum()
    peak_to_df_ratio = peak / dataframe_memory

    print()
    print(f"Timestamp column: {timestamp_column_name}, include_point_previews: {include_point_previews}")
    print("Dataframe size:", naturalsize(dataframe_memory))
    print("Peak memory:", naturalsize(peak))
    print(f"Peak/DataFrame memory ratio: {peak_to_df_ratio}")
    # assert (
    #     peak_to_df_ratio <= max_allowed_ratio
    # ), f"Peak/DataFrame memory ratio too high: {peak_to_df_ratio:.2f} (peak={peak}, df={dataframe_memory})"
