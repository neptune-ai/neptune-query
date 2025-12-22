from itertools import chain
from typing import (
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)
from unittest.mock import patch

import pandas as pd
import pytest

from neptune_query import fetch_metrics
from neptune_query.filters import (
    AttributeFilter,
    Filter,
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
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    IngestedRunData,
    ProjectData,
    RunData,
    step_to_timestamp,
)

METRIC_STEPS = 6
EXPERIMENT_NAMES = ["metrics-alpha", "metrics-beta", "metrics-gamma"]
INF_NAN_EXPERIMENT_NAME = "metrics-inf-nan"

INF_SERIES_VALUES = [float("inf"), 1.0, float("-inf"), 3.0, 4.0, float("inf"), 6.0, float("-inf"), 8.0, 9.0]
NAN_SERIES_VALUES = [float("nan"), 1.0, float("nan"), 3.0, 4.0, float("nan"), 6.0, float("nan"), 8.0, 9.0]


def _timestamp_millis(step: float) -> int:
    return int(step_to_timestamp(step).timestamp() * 1000)


def _to_run_attribute_definition(project_identifier: str, run: str, metric_name: str):
    return RunAttributeDefinition(
        RunIdentifier(ProjectIdentifier(project_identifier), SysId(run)),
        AttributeDefinition(metric_name, "float_series"),
    )


def _to_float_point_value(step, value):
    return _timestamp_millis(step), step, value, False, 1.0


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    project_data = ProjectData(
        runs=[
            RunData(
                experiment_name=EXPERIMENT_NAMES[0],
                run_id="metrics-alpha-run",
                configs={"configs/int-value": 1},
                float_series={
                    "train/metrics/value_0": {float(step): float(step) * 0.1 for step in range(METRIC_STEPS)},
                    "train/metrics/value_1": {float(step): float(step) * 0.2 + 0.5 for step in range(METRIC_STEPS)},
                },
            ),
            RunData(
                experiment_name=EXPERIMENT_NAMES[1],
                run_id="metrics-beta-run",
                configs={"configs/int-value": 2},
                float_series={
                    "train/metrics/value_0": {float(step): float(step) * 0.3 for step in range(METRIC_STEPS)},
                    "train/metrics/value_2": {float(step): float(step) * 0.4 + 1.0 for step in range(METRIC_STEPS)},
                },
            ),
            RunData(
                experiment_name=EXPERIMENT_NAMES[2],
                run_id="metrics-gamma-run",
                configs={"configs/int-value": 3},
                float_series={
                    "train/metrics/value_1": {float(step): float(step) * 0.5 + 1.5 for step in range(METRIC_STEPS)},
                    "train/metrics/value_2": {float(step): float(step) * 0.6 + 2.0 for step in range(METRIC_STEPS)},
                },
            ),
            RunData(
                experiment_name=INF_NAN_EXPERIMENT_NAME,
                run_id="metrics-inf-nan-run",
                float_series={
                    "series-containing-inf": {float(step): value for step, value in enumerate(INF_SERIES_VALUES)},
                    "series-containing-nan": {float(step): value for step, value in enumerate(NAN_SERIES_VALUES)},
                },
            ),
        ]
    )

    return ensure_project(project_data)


@pytest.fixture(scope="module")
def lineage_project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    return ensure_project(
        ProjectData(
            runs=[
                RunData(
                    experiment_name="lineage-metrics-root-exp",
                    run_id="lineage-metrics-run-1",
                    float_series={"metrics/m1": {float(step): float(step) * 0.1 for step in range(0, 5)}},
                ),
                RunData(
                    experiment_name="lineage-metrics-child-exp",
                    run_id="lineage-metrics-run-2",
                    fork_point=("lineage-metrics-run-1", 4.0),
                    float_series={"metrics/m1": {float(step): float(step) * 0.2 for step in range(5, 9)}},
                ),
                RunData(
                    experiment_name="lineage-metrics-child-exp",
                    run_id="lineage-metrics-run-3",
                    fork_point=("lineage-metrics-run-2", 8.0),
                    float_series={"metrics/m1": {float(step): float(step) * 0.3 for step in range(9, 12)}},
                ),
            ]
        )
    )


def create_expected_data(
    project: IngestedProjectData,
    runs: list[IngestedRunData],
    type_suffix_in_column_names: bool,
    include_time: Union[Literal["absolute"], None],
    step_range: Tuple[Optional[int], Optional[int]],
    tail_limit: Optional[int],
) -> Tuple[pd.DataFrame, List[str], set[str]]:
    metrics_data: dict[RunAttributeDefinition, list[FloatPointValue]] = {}

    columns = set()
    filtered_experiments = set()

    step_filter = (
        step_range[0] if step_range[0] is not None else -float("inf"),
        step_range[1] if step_range[1] is not None else float("inf"),
    )
    for run in runs:
        for path, series in run.float_series.items():
            filtered = []
            for step in series:
                if step_filter[0] <= step <= step_filter[1]:
                    columns.add(f"{path}:float_series" if type_suffix_in_column_names else path)
                    filtered_experiments.add(run.experiment_name)
                    filtered.append(_to_float_point_value(step, series[step]))
            limited = filtered[-tail_limit:] if tail_limit is not None else filtered

            attribute_run = RunAttributeDefinition(
                RunIdentifier(ProjectIdentifier(project.project_identifier), SysId(run.run_id)),
                AttributeDefinition(path, "float_series"),
            )
            metrics_data.setdefault(attribute_run, []).extend(limited)

    df = create_metrics_dataframe(
        metrics_data=metrics_data,
        sys_id_label_mapping={SysId(run.run_id): run.experiment_name for run in runs},
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=False,
        timestamp_column_name="absolute_time" if include_time == "absolute" else None,
        index_column_name="experiment",
    )

    sorted_columns = list(sorted(columns))
    if include_time == "absolute":
        absolute_columns = [[(c, "value"), (c, "absolute_time")] for c in sorted_columns]
        return df, list(chain.from_iterable(absolute_columns)), filtered_experiments
    else:
        return df, sorted_columns, filtered_experiments


@pytest.mark.parametrize(
    "arg_attributes",
    [
        AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
        ".*/metrics/.*",
        # Alternative should work too, see bug PY-137
        AttributeFilter(name=r".*/metrics/.*", type=["float_series"])
        | AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
    ],
)
@pytest.mark.parametrize(
    "arg_experiments",
    [
        Filter.name(EXPERIMENT_NAMES),
        f"{EXPERIMENT_NAMES[0]}|{EXPERIMENT_NAMES[1]}|{EXPERIMENT_NAMES[2]}",
        f"{EXPERIMENT_NAMES[0]} | {EXPERIMENT_NAMES[1]} | {EXPERIMENT_NAMES[2]}",  # ERS
        EXPERIMENT_NAMES,
    ],
)
@pytest.mark.parametrize(
    "step_range,tail_limit,page_point_limit,type_suffix_in_column_names,include_time",
    [
        (
            (0, 5),
            None,
            50,
            True,
            None,
        ),
        (
            (0, None),
            3,
            1_000_000,
            False,
            "absolute",
        ),
        (
            (None, 5),
            5,
            50,
            True,
            "absolute",
        ),
    ],
)
def test__fetch_metrics_unique__filter_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    page_point_limit,
    type_suffix_in_column_names,
    include_time,
):
    runs = [run for run in project.ingested_runs if run.experiment_name in EXPERIMENT_NAMES]

    with patch("neptune_query.internal.retrieval.metrics.TOTAL_POINT_LIMIT", page_point_limit):
        result = fetch_metrics(
            experiments=arg_experiments,
            attributes=arg_attributes,
            type_suffix_in_column_names=type_suffix_in_column_names,
            step_range=step_range,
            tail_limit=tail_limit,
            include_time=include_time,
            project=project.project_identifier,
        )

    expected, columns, filtered_experiments = create_expected_data(
        project, runs, type_suffix_in_column_names, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_experiments


@pytest.mark.parametrize("step_range", [(0, 5), (0, None), (None, 5), (None, None), (100, 200)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize("page_point_limit", [50, 1_000_000])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,type_suffix_in_column_names,include_time",
    [
        (
            Filter.name(EXPERIMENT_NAMES),
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            True,
            None,
        ),
        (
            f"{EXPERIMENT_NAMES[0]} | {EXPERIMENT_NAMES[1]} | {EXPERIMENT_NAMES[2]}",
            ".*/metrics/.*",
            False,
            "absolute",
        ),
        (
            EXPERIMENT_NAMES,
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"])
            | AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            True,
            "absolute",
        ),
    ],
)
def test__fetch_metrics_unique__step_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range: tuple[float | None, float | None],
    tail_limit,
    page_point_limit,
    type_suffix_in_column_names,
    include_time,
):
    runs = [run for run in project.ingested_runs if run.experiment_name in EXPERIMENT_NAMES]

    with patch("neptune_query.internal.retrieval.metrics.TOTAL_POINT_LIMIT", page_point_limit):
        result = fetch_metrics(
            experiments=arg_experiments,
            attributes=arg_attributes,
            type_suffix_in_column_names=type_suffix_in_column_names,
            step_range=step_range,
            tail_limit=tail_limit,
            include_time=include_time,
            project=project.project_identifier,
        )

    expected, columns, filtered_experiments = create_expected_data(
        project, runs, type_suffix_in_column_names, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_experiments


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_time", [None, "absolute"])  # "relative",
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,step_range,tail_limit,page_point_limit",
    [
        (
            Filter.name(EXPERIMENT_NAMES),
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            (0, 5),
            None,
            50,
        ),
        (
            f"{EXPERIMENT_NAMES[0]} | {EXPERIMENT_NAMES[1]} | {EXPERIMENT_NAMES[2]}",
            ".*/metrics/.*",
            (0, None),
            3,
            1_000_000,
        ),
        (
            EXPERIMENT_NAMES,
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"])
            | AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            (None, 5),
            5,
            50,
        ),
    ],
)
def test__fetch_metrics_unique__output_format_variants(
    project,
    arg_experiments,
    arg_attributes,
    type_suffix_in_column_names,
    include_time,
    step_range,
    tail_limit,
    page_point_limit,
):
    runs = [run for run in project.ingested_runs if run.experiment_name in EXPERIMENT_NAMES]

    with patch("neptune_query.internal.retrieval.metrics.TOTAL_POINT_LIMIT", page_point_limit):
        result = fetch_metrics(
            experiments=arg_experiments,
            attributes=arg_attributes,
            type_suffix_in_column_names=type_suffix_in_column_names,
            step_range=step_range,
            tail_limit=tail_limit,
            include_time=include_time,
            project=project.project_identifier,
        )

    expected, columns, filtered_experiments = create_expected_data(
        project, runs, type_suffix_in_column_names, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_experiments


@pytest.mark.parametrize(
    "lineage_to_the_root,expected_values",
    [
        (
            True,
            [(step, step * 0.1) for step in range(0, 5)]
            + [(step, step * 0.2) for step in range(5, 9)]
            + [(step, step * 0.3) for step in range(9, 12)],
        ),
        (False, [(step, step * 0.2) for step in range(5, 9)] + [(step, step * 0.3) for step in range(9, 12)]),
    ],
)
def test__fetch_metrics__lineage(lineage_project, lineage_to_the_root, expected_values):
    target_run = next(
        run for run in lineage_project.ingested_runs if run.experiment_name == "lineage-metrics-child-exp"
    )

    df = fetch_metrics(
        project=lineage_project.project_identifier,
        experiments=["lineage-metrics-child-exp"],
        attributes=r"metrics/m1",
        lineage_to_the_root=lineage_to_the_root,
    )

    expected = create_metrics_dataframe(
        metrics_data={
            _to_run_attribute_definition(lineage_project.project_identifier, target_run.run_id, "metrics/m1"): [
                _to_float_point_value(step, value) for step, value in expected_values
            ]
        },
        sys_id_label_mapping={SysId(target_run.run_id): target_run.experiment_name},
        type_suffix_in_column_names=False,
        include_point_previews=False,
        timestamp_column_name=None,
        index_column_name="experiment",
    )

    pd.testing.assert_frame_equal(df, expected)


@pytest.mark.parametrize(
    "series_name,expected_values",
    [
        ("series-containing-inf", list(enumerate(INF_SERIES_VALUES))),
        ("series-containing-nan", list(enumerate(NAN_SERIES_VALUES))),
    ],
)
def test__fetch_metrics_nan_inf(project, series_name, expected_values):
    target_run = next(run for run in project.ingested_runs if run.experiment_name == INF_NAN_EXPERIMENT_NAME)

    df = fetch_metrics(
        project=project.project_identifier,
        experiments=[INF_NAN_EXPERIMENT_NAME],
        attributes=[series_name],
    )

    expected = create_metrics_dataframe(
        metrics_data={
            _to_run_attribute_definition(project.project_identifier, target_run.run_id, series_name): [
                _to_float_point_value(step, value) for step, value in expected_values
            ]
        },
        sys_id_label_mapping={SysId(target_run.run_id): target_run.experiment_name},
        type_suffix_in_column_names=False,
        include_point_previews=False,
        timestamp_column_name=None,
        index_column_name="experiment",
    )

    pd.testing.assert_frame_equal(df, expected)
