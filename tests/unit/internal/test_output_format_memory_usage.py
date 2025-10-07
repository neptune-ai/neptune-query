import time
import tracemalloc
from datetime import timedelta

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
from neptune_query.internal.retrieval.metrics import FloatPointValue


def _generate_metrics_dataset(
    *,
    num_experiments: int,
    num_metrics: int,
    num_steps: int,
) -> tuple[dict[RunAttributeDefinition, list[FloatPointValue]], dict[SysId, str]]:
    project = ProjectIdentifier("perf/project")
    metrics_data: dict[RunAttributeDefinition, list[FloatPointValue]] = {}
    label_mapping: dict[SysId, str] = {}

    for experiment_index in range(num_experiments):
        sys_id = SysId(f"sysid{experiment_index}")
        label_mapping[sys_id] = f"exp{experiment_index}"
        run_identifier = RunIdentifier(project, sys_id)

        for metric_index in range(num_metrics):
            attribute = AttributeDefinition(f"metric_{metric_index}", "float_series")
            definition = RunAttributeDefinition(run_identifier, attribute)
            points: list[FloatPointValue] = []
            for step in range(num_steps):
                points.append(
                    (
                        float(step),
                        float(step),
                        float(metric_index + step),
                        False,
                        1.0,
                    )
                )
            metrics_data[definition] = points

    return metrics_data, label_mapping


def test_create_metrics_dataframe_peak_memory_usage():
    metrics_data, sys_id_label_mapping = _generate_metrics_dataset(
        num_experiments=300,
        num_metrics=60,
        num_steps=400,
    )

    tracemalloc.start()
    try:
        df = create_metrics_dataframe(
            metrics_data=metrics_data,
            sys_id_label_mapping=sys_id_label_mapping,
            type_suffix_in_column_names=True,
            include_point_previews=False,
            index_column_name="experiment",
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    dataframe_memory = df.memory_usage(deep=True).sum()
    peak_to_df_ratio = peak / dataframe_memory

    max_allowed_ratio = 2.5
    print()
    print("Dataframe size:", naturalsize(dataframe_memory))
    print("Peak memory:", naturalsize(peak))
    print(f"Peak/DataFrame memory ratio: {peak_to_df_ratio}")
    assert (
        peak_to_df_ratio <= max_allowed_ratio
    ), f"Peak/DataFrame memory ratio too high: {peak_to_df_ratio:.2f} (peak={peak}, df={dataframe_memory})"


# This test doesn't test any regressions, but it's very useful when iterating on performance improvements
def test_create_metrics_dataframe_timing():
    metrics_data, sys_id_label_mapping = _generate_metrics_dataset(
        num_experiments=30,
        num_metrics=60,
        num_steps=400,
    )

    start = time.perf_counter_ns()
    _ = create_metrics_dataframe(
        metrics_data=metrics_data,
        sys_id_label_mapping=sys_id_label_mapping,
        type_suffix_in_column_names=True,
        include_point_previews=False,
        index_column_name="experiment",
    )
    end = time.perf_counter_ns()
    delta = timedelta(microseconds=(end - start) / 1e3)

    print()
    print("Duration:", naturaldelta(value=delta, minimum_unit="microseconds"))
