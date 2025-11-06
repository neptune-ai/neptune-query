import numpy as np
import pytest

from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.output_format import create_metrics_dataframe
from neptune_query.internal.retrieval.metrics import MetricDatapoints


def test_create_metrics_dataframe_large_debug_workload():
    num_runs = 1000000
    num_metrics = 1
    num_datapoints = 1

    project_identifier = ProjectIdentifier("debug/project")
    metrics_data: dict[RunAttributeDefinition, MetricDatapoints] = {}
    sys_id_label_mapping: dict[SysId, str] = {}
    base_steps = np.arange(num_datapoints, dtype=np.float64)

    for run_idx in range(num_runs):
        sys_id = SysId(f"sys{run_idx:04d}")
        sys_id_label_mapping[sys_id] = f"run-{run_idx:04d}"
        run_identifier = RunIdentifier(project_identifier, sys_id)

        for metric_idx in range(num_metrics):
            attribute_definition = AttributeDefinition(f"metric_{metric_idx:02d}", "float_series")
            run_attribute_definition = RunAttributeDefinition(run_identifier, attribute_definition)

            datapoints = MetricDatapoints.allocate(
                size=num_datapoints, include_timestamp=False, include_preview=False
            )
            step_offset = metric_idx + 1
            base_value = run_idx * num_metrics * num_datapoints + metric_idx * num_datapoints

            shifted_steps = base_steps + step_offset
            for idx, base_step in enumerate(base_steps):
                datapoints.append(step=float(shifted_steps[idx]), value=float(base_value + base_step))

            metrics_data[run_attribute_definition] = datapoints.compile()

    dataframe = create_metrics_dataframe(
        metrics_data=metrics_data,
        sys_id_label_mapping=sys_id_label_mapping,
        type_suffix_in_column_names=False,
        include_point_previews=False,
        index_column_name="run",
        timestamp_column_name=None,
    )