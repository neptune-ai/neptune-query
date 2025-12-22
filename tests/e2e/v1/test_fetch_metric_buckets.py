import threading
from math import (
    inf,
    nan,
)
from typing import (
    Iterable,
    Literal,
    Optional,
    Union,
)

import pandas as pd
import pytest

from neptune_query import fetch_metric_buckets
from neptune_query.filters import (
    AttributeFilter,
    Filter,
)
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.output_format import create_metric_buckets_dataframe
from neptune_query.internal.retrieval import metric_buckets
from neptune_query.internal.retrieval.metric_buckets import TimeseriesBucket
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    ProjectData,
    RunData,
)
from tests.e2e.metric_buckets import (
    aggregate_metric_buckets,
    calculate_global_range,
    calculate_metric_bucket_ranges,
)

FINITE_EXPERIMENT_NAMES = [
    "metric-buckets-alpha",
    "metric-buckets-beta",
    "metric-buckets-gamma",
]


MISALIGNED_STEPS_SETS = [[0], [0, 1], list(range(10)), [1, 10], [12, 14, 16], [18, 19, 20]]
MISALIGNED_PATHS = [
    f"misaligned-steps/misaligned-steps-float-series-value-{ix}" for ix in range(len(MISALIGNED_STEPS_SETS))
]


@pytest.fixture(scope="module")
def project_finite(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    alpha_series = {
        "metrics/step": {float(i): float(i) for i in range(10)},
        "metrics/float-series-value_0": {float(i): float(i) + 0.1 for i in range(10)},
        "metrics/float-series-value_1": {float(i): float(i) + 1.1 for i in range(10)},
    }
    beta_series = {
        "metrics/step": {float(i): float(i) for i in range(10)},
        "metrics/float-series-value_0": {float(i): float(i) * 1.5 for i in range(10)},
    }
    gamma_series = {
        "metrics/step": {float(i): float(i) for i in range(10)},
        "metrics/float-series-value_0": {float(i): float(i) * 0.5 for i in range(10)},
    }

    misaligned_series = {
        path: {float(step): float(step) ** 2 + 0.123 for step in steps}
        for path, steps in zip(MISALIGNED_PATHS, MISALIGNED_STEPS_SETS)
    }

    return ensure_project(
        ProjectData(
            runs=[
                RunData(
                    experiment_name="metric-buckets-alpha",
                    run_id="metric-buckets-alpha-run",
                    float_series={**alpha_series, **misaligned_series},
                ),
                RunData(
                    experiment_name="metric-buckets-beta",
                    run_id="metric-buckets-beta-run",
                    float_series={**beta_series, **misaligned_series},
                ),
                RunData(
                    experiment_name="metric-buckets-gamma",
                    run_id="metric-buckets-gamma-run",
                    float_series={**gamma_series, **misaligned_series},
                ),
            ]
        )
    )


@pytest.fixture(scope="module")
def project_non_finite(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    return ensure_project(
        ProjectData(
            runs=[
                RunData(
                    experiment_name="metric-buckets-inf-nan",
                    run_id="inf-nan-run",
                    float_series={
                        "series-containing-inf": {0.0: 1.0, 1.0: inf, 2.0: 3.0},
                        "series-containing-nan": {0.0: 1.0, 1.0: nan, 2.0: 3.0},
                        "series-ending-with-inf": {0.0: 1.0, 1.0: 2.0, 2.0: inf},
                        "series-ending-with-nan": {0.0: 1.0, 1.0: 2.0, 2.0: nan},
                    },
                ),
            ]
        )
    )


def _to_run_attribute_definition(project, run, path):
    return RunAttributeDefinition(
        RunIdentifier(project, SysId(run)),
        AttributeDefinition(path, "float_series"),
    )


def _sys_id_label_mapping(experiments: Iterable[str]) -> dict[SysId, str]:
    return {SysId(name): name for name in experiments}  # we just use the id as the label in tests


def create_expected_data_experiments(
    project: IngestedProjectData,
    experiment_names: list[str],
    x: Union[Literal["step"]],  # only option currently
    limit: int,
    include_point_previews: bool,
) -> pd.DataFrame:
    data: dict[str, dict[str, list[tuple[float, float]]]] = {}
    for exp_name in experiment_names:
        run = project.get_run_by_run_id(exp_name + "-run")
        # Take all metric-like float series (including step itself) under ^metrics
        series_pairs = {
            path: sorted(list(values.items()))
            for path, values in run.float_series.items()
            if path.startswith("metrics/")
        }
        data[exp_name] = series_pairs

    return _create_expected_data_metric_buckets_dataframe(
        data=data,
        project_identifier=project.project_identifier,
        x=x,
        limit=limit,
        include_point_previews=include_point_previews,
    )


def _calculate_expected_data_global_range(
    data: dict[str, dict[str, list[tuple[float, float]]]],
) -> tuple[float, float]:
    global_range: Optional[tuple[float, float]] = None
    for experiment_data in data.values():
        for series_data in experiment_data.values():
            series_range = calculate_global_range(series_data, x_range=None)
            if global_range is None:
                global_range = series_range
            else:
                global_range = (
                    min(global_range[0], series_range[0]),
                    max(global_range[1], series_range[1]),
                )
    assert global_range is not None
    return global_range


def _create_expected_data_metric_buckets_dataframe(
    data: dict[str, dict[str, list[tuple[float, float]]]],
    project_identifier: str,
    x: Union[Literal["step"]],  # TODO - only option
    limit: int,
    include_point_previews: bool,  # TODO - add to the test data?
) -> pd.DataFrame:
    global_from, global_to = _calculate_expected_data_global_range(data)
    bucket_ranges = calculate_metric_bucket_ranges(global_from, global_to, limit=limit + 1)

    bucket_data: dict[RunAttributeDefinition, list[TimeseriesBucket]] = {}
    for experiment_name, experiment_data in data.items():
        for path, series in experiment_data.items():
            buckets = aggregate_metric_buckets(series, bucket_ranges)
            attribute_run = _to_run_attribute_definition(project_identifier, experiment_name, path)
            bucket_data.setdefault(attribute_run, []).extend(buckets)

    return create_metric_buckets_dataframe(
        buckets_data=bucket_data,
        sys_id_label_mapping=_sys_id_label_mapping(data.keys()),
        container_column_name="experiment",
    )


@pytest.mark.parametrize(
    "y",
    [
        AttributeFilter(name="^metrics/.*", type=["float_series"]),
        "^metrics/.*",
        AttributeFilter(name="^metrics/.*", type=["float_series"])
        | AttributeFilter(name="^metrics/.*", type=["float_series"]),
    ],
)
@pytest.mark.parametrize(
    "arg_experiments",
    [
        Filter.name(FINITE_EXPERIMENT_NAMES),
        "metric-buckets-alpha|metric-buckets-beta|metric-buckets-gamma",  # regular expressions
        "metric-buckets-alpha | metric-buckets-beta | metric-buckets-gamma",  # ERS
        FINITE_EXPERIMENT_NAMES,
    ],
)
@pytest.mark.parametrize(
    "x,limit,include_point_previews",
    [
        (
            "step",
            2,
            True,
        ),
        (
            "step",
            10,
            False,
        ),
    ],
)
def test__fetch_metric_buckets__experiment_attribute_filter_variants(
    project_finite,
    arg_experiments,
    x,
    y,
    limit,
    include_point_previews,
):
    result_df = fetch_metric_buckets(
        project=project_finite.project_identifier,
        experiments=arg_experiments,
        x=x,
        y=y,
        limit=limit,
        include_point_previews=include_point_previews,
        lineage_to_the_root=True,
    )

    expected_df = create_expected_data_experiments(
        project=project_finite,
        experiment_names=FINITE_EXPERIMENT_NAMES,
        x=x,
        limit=limit,
        include_point_previews=include_point_previews,
    )

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    "limit",
    [1, 2, 3, 10, 20, 1000],
)
@pytest.mark.parametrize(
    "x",
    ["step"],
)
@pytest.mark.parametrize(  # Not sure where to test variants of preview. They are ignored for now anyway...
    "include_point_previews",
    [True, False],
)
@pytest.mark.parametrize(
    "arg_experiments,y",
    [
        (
            Filter.name(FINITE_EXPERIMENT_NAMES),
            AttributeFilter(name="^metrics/.*", type=["float_series"]),
        ),
        (
            "metric-buckets-alpha | metric-buckets-beta | metric-buckets-gamma",  # ERS
            "^metrics/.*",
        ),
        (
            FINITE_EXPERIMENT_NAMES,
            AttributeFilter(name="^metrics/.*", type=["float_series"])
            | AttributeFilter(name="^metrics/.*", type=["float_series"]),
        ),
    ],
)
def test__fetch_metric_buckets__bucketing_x_limit_variants(
    project_finite,
    arg_experiments,
    x,
    y,
    limit,
    include_point_previews,
):
    result_df = fetch_metric_buckets(
        project=project_finite.project_identifier,
        experiments=arg_experiments,
        x=x,
        y=y,
        limit=limit,
        include_point_previews=include_point_previews,
        lineage_to_the_root=True,
    )

    expected_df = create_expected_data_experiments(
        project=project_finite,
        experiment_names=FINITE_EXPERIMENT_NAMES,
        x=x,
        limit=limit,
        include_point_previews=include_point_previews,
    )

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    "arg_experiments",
    [["metric-buckets-alpha"], FINITE_EXPERIMENT_NAMES],
)
@pytest.mark.parametrize(
    "y",
    [[path] for path in MISALIGNED_PATHS] + [MISALIGNED_PATHS],
)
@pytest.mark.parametrize(
    "x",
    ["step"],
)
@pytest.mark.parametrize(
    "limit",
    [2, 3, 10],
)
@pytest.mark.parametrize(
    "include_point_previews",
    [True],
)
def test__fetch_metric_buckets__handles_misaligned_steps_in_metrics(
    arg_experiments,
    y,
    project_finite,
    x,
    limit,
    include_point_previews,
):
    result_df = fetch_metric_buckets(
        project=project_finite.project_identifier,
        experiments=arg_experiments,
        x=x,
        y=y,
        limit=limit,
        include_point_previews=include_point_previews,
        lineage_to_the_root=True,
    )

    expected_data = {}
    for exp_name in arg_experiments:
        run = project_finite.get_run_by_run_id(exp_name + "-run")
        expected_data[exp_name] = {path: sorted(list(run.float_series[path].items())) for path in y}
    expected_df = _create_expected_data_metric_buckets_dataframe(
        data=expected_data,
        project_identifier=project_finite.project_identifier,
        x=x,
        limit=limit,
        include_point_previews=include_point_previews,
    )

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    "attribute_filter, expected_attributes",
    [
        (
            AttributeFilter(name=r"series-.*", type=["float_series"]),
            [
                "series-containing-inf",
                "series-containing-nan",
                "series-ending-with-inf",
                "series-ending-with-nan",
            ],
        ),
        (
            r"series-ending-.*",
            ["series-ending-with-inf", "series-ending-with-nan"],
        ),
    ],
)
def test__fetch_metric_buckets__over_1k_series(
    project_non_finite,
    monkeypatch,
    attribute_filter,
    expected_attributes,
):
    """
    This test verifies that when fetching metric buckets for a run with over 1000 series,
    the function correctly splits the requests into multiple chunks to avoid exceeding the limit.

    It does so by monkeypatching the actual limit to 1 and capturing the calls made to fetch_time_series_buckets.
    """
    original_fetch = metric_buckets.fetch_time_series_buckets
    call_chunks: list[list[RunAttributeDefinition]] = []
    lock = threading.Lock()

    def capture_fetch(*args, **kwargs):
        with lock:
            call_chunks.append(kwargs["run_attribute_definitions"])
        return original_fetch(*args, **kwargs)

    monkeypatch.setattr(metric_buckets, "fetch_time_series_buckets", capture_fetch)

    forced_limit = 1
    monkeypatch.setattr("neptune_query.internal.retrieval.metric_buckets.MAX_SERIES_PER_REQUEST", forced_limit)
    monkeypatch.setattr("neptune_query.internal.composition.fetch_metric_buckets.MAX_SERIES_PER_REQUEST", forced_limit)

    experiment_name = "metric-buckets-inf-nan"

    result_df = fetch_metric_buckets(
        project=project_non_finite.project_identifier,
        experiments=[experiment_name],
        x="step",
        y=attribute_filter,
        limit=5,
        include_point_previews=False,
        lineage_to_the_root=True,
    )

    # Build expected data from ingested project
    run = project_non_finite.ingested_runs[0]
    expected_data = {
        experiment_name: {
            attribute_name: sorted(list(run.float_series[attribute_name].items()))
            for attribute_name in expected_attributes
        }
    }
    expected_df = _create_expected_data_metric_buckets_dataframe(
        data=expected_data,
        project_identifier=project_non_finite.project_identifier,
        x="step",
        limit=5,
        include_point_previews=False,
    )

    pd.testing.assert_frame_equal(result_df, expected_df)

    assert len(call_chunks) > 1
    total_series = sum(len(chunk) for chunk in call_chunks)
    assert total_series > forced_limit
    assert all(len(chunk) <= forced_limit for chunk in call_chunks)


@pytest.mark.parametrize(
    "arg_experiments,y",
    [
        ("metric-buckets-inf-nan", "series-containing-inf"),
        ("metric-buckets-inf-nan", "series-containing-nan"),
        ("metric-buckets-inf-nan", "series-ending-with-inf"),
        ("metric-buckets-inf-nan", "series-ending-with-nan"),
    ],
)
@pytest.mark.parametrize(
    "x",
    ["step"],
)
@pytest.mark.parametrize(
    "limit",
    [2, 3, 10, 20],
)
@pytest.mark.parametrize(
    "include_point_previews",
    [True],
)
def test__fetch_metric_buckets__inf_nan(
    project_non_finite,
    arg_experiments,
    x,
    y,
    limit,
    include_point_previews,
):
    result_df = fetch_metric_buckets(
        project=project_non_finite.project_identifier,
        experiments=arg_experiments,
        x=x,
        y=y,
        limit=limit,
        include_point_previews=include_point_previews,
        lineage_to_the_root=True,
    )

    run = project_non_finite.ingested_runs[0]
    expected_data = {arg_experiments: {y: sorted(list(run.float_series[y].items()))}}
    expected_df = _create_expected_data_metric_buckets_dataframe(
        data=expected_data,
        project_identifier=project_non_finite.project_identifier,
        x=x,
        limit=limit,
        include_point_previews=include_point_previews,
    )

    pd.testing.assert_frame_equal(result_df, expected_df)
