import itertools as it
from typing import (
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pandas as pd
import pytest

from neptune_query import fetch_series
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
from neptune_query.internal.output_format import create_series_dataframe
from neptune_query.internal.retrieval.attribute_types import (
    File,
    Histogram,
)
from neptune_query.internal.retrieval.series import SeriesValue
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    IngestionFile,
    IngestionHistogram,
    ProjectData,
    RunData,
    step_to_timestamp,
)

FILE_SERIES_NUMBER_OF_STEPS = 3
STRING_SERIES_STEPS = 6
HISTOGRAM_SERIES_STEPS = 6
EXPERIMENT_NAMES = ["series-alpha", "series-beta", "series-gamma"]


def _timestamp_millis(step: float) -> int:
    return int(step_to_timestamp(step).timestamp() * 1000)


def _to_series_value(step, value):
    return SeriesValue(
        step=step,
        value=value,
        timestamp_millis=_timestamp_millis(step),
    )


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    series_runs = [
        RunData(
            experiment_name=EXPERIMENT_NAMES[0],
            run_id="series-alpha-run",
            configs={
                "configs/int-value": 1,
            },
            float_series={
                "series/float-series-value_0": {float(step): float(step) + 0.1 for step in range(STRING_SERIES_STEPS)},
                "series/float-series-value_1": {float(step): float(step) + 1.1 for step in range(STRING_SERIES_STEPS)},
            },
            string_series={
                "series/string-series-value_0": {
                    float(step): f"alpha-main-{step}" for step in range(STRING_SERIES_STEPS)
                },
                "series/string-series-value_1": {
                    float(step): f"alpha-alt-{step}" for step in range(STRING_SERIES_STEPS)
                },
            },
            histogram_series={
                "series/histogram-series-value_0": {
                    float(step): IngestionHistogram(
                        bin_edges=[0.0, 1.0, 2.0, 3.0],
                        counts=[step + 1, step + 2, step + 3],
                    )
                    for step in range(HISTOGRAM_SERIES_STEPS)
                },
                "series/histogram-series-value_1": {
                    float(step): IngestionHistogram(
                        bin_edges=[1.0, 2.5, 4.0, 5.5],
                        counts=[step + 2, step + 4, step + 6],
                    )
                    for step in range(HISTOGRAM_SERIES_STEPS)
                },
            },
            file_series={
                "files/file-series-value_0": {
                    float(step): IngestionFile(
                        f"alpha-file-{step}".encode("utf-8"),
                        mime_type="text/plain",
                        destination=f"alpha-run_file-series-value_0_{step}.bin",
                    )
                    for step in range(FILE_SERIES_NUMBER_OF_STEPS)
                },
                "files/file-series-value_1": {
                    float(step): IngestionFile(
                        f"alpha-asset-{step}".encode("utf-8"),
                        mime_type="application/octet-stream",
                        destination=f"alpha-run_file-series-value_1_{step}.bin",
                    )
                    for step in range(FILE_SERIES_NUMBER_OF_STEPS)
                },
            },
        ),
        RunData(
            experiment_name=EXPERIMENT_NAMES[1],
            run_id="series-beta-run",
            configs={
                "configs/int-value": 2,
            },
            float_series={
                "series/float-series-value_0": {float(step): float(step) + 2.5 for step in range(STRING_SERIES_STEPS)},
            },
            string_series={
                "series/string-series-value_0": {
                    float(step): f"beta-main-{step}" for step in range(STRING_SERIES_STEPS)
                },
                "series/string-series-value_1": {
                    float(step): f"beta-alt-{step}" for step in range(STRING_SERIES_STEPS)
                },
            },
            histogram_series={
                "series/histogram-series-value_0": {
                    float(step): IngestionHistogram(
                        bin_edges=[0.5, 1.5, 2.5, 3.5],
                        counts=[step + 5, step + 6, step + 7],
                    )
                    for step in range(HISTOGRAM_SERIES_STEPS)
                },
                "series/histogram-series-value_1": {
                    float(step): IngestionHistogram(
                        bin_edges=[2.0, 4.0, 6.0, 8.0],
                        counts=[step + 3, step + 5, step + 7],
                    )
                    for step in range(HISTOGRAM_SERIES_STEPS)
                },
            },
        ),
        RunData(
            experiment_name=EXPERIMENT_NAMES[2],
            run_id="series-gamma-run",
            configs={
                "configs/int-value": 3,
            },
            float_series={
                "series/float-series-value_0": {float(step): float(step) * 1.5 for step in range(STRING_SERIES_STEPS)},
            },
            string_series={
                "series/string-series-value_0": {
                    float(step): f"gamma-main-{step}" for step in range(STRING_SERIES_STEPS)
                },
                "series/string-series-value_1": {
                    float(step): f"gamma-alt-{step}" for step in range(STRING_SERIES_STEPS)
                },
            },
            histogram_series={
                "series/histogram-series-value_0": {
                    float(step): IngestionHistogram(
                        bin_edges=[1.0, 2.0, 3.0, 4.0],
                        counts=[step + 9, step + 10, step + 11],
                    )
                    for step in range(HISTOGRAM_SERIES_STEPS)
                },
                "series/histogram-series-value_1": {
                    float(step): IngestionHistogram(
                        bin_edges=[1.5, 3.0, 4.5, 6.0],
                        counts=[step + 4, step + 6, step + 8],
                    )
                    for step in range(HISTOGRAM_SERIES_STEPS)
                },
            },
        ),
    ]

    project_data = ProjectData(runs=series_runs)

    return ensure_project(project_data)


@pytest.fixture(scope="module")
def lineage_project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    lineage_runs = [
        RunData(
            experiment_name="lineage-parent-exp",
            run_id="lineage-run-1",
            string_series={
                "string_series/s1": {float(step): f"val_run1_{step}" for step in range(0, 5)},
            },
        ),
        RunData(
            experiment_name="lineage-child-exp",
            run_id="lineage-run-2",
            fork_point=("lineage-run-1", 4.0),
            string_series={
                "string_series/s1": {float(step): f"val_run2_{step}" for step in range(5, 9)},
            },
        ),
        RunData(
            experiment_name="lineage-child-exp",
            run_id="lineage-run-3",
            fork_point=("lineage-run-2", 8.0),
            string_series={
                "string_series/s1": {float(step): f"val_run3_{step}" for step in range(9, 12)},
            },
        ),
    ]

    lineage_project_data = ProjectData(runs=lineage_runs)

    return ensure_project(lineage_project_data)


def create_expected_data_string_series(
    project: IngestedProjectData,
    include_time: Union[Literal["absolute"], None],
    step_range: Tuple[Optional[int], Optional[int]],
    tail_limit: Optional[int],
) -> Tuple[pd.DataFrame, List[str], set[str]]:
    series_data: dict[RunAttributeDefinition, list[SeriesValue]] = {}
    sys_id_label_mapping: dict[SysId, str] = {}

    columns = set()
    filtered_exps = set()

    step_filter = (
        step_range[0] if step_range[0] is not None else -float("inf"),
        step_range[1] if step_range[1] is not None else float("inf"),
    )
    for run in project.ingested_runs:
        sys_id = SysId(run.run_id)
        sys_id_label_mapping[sys_id] = run.experiment_name

        for path, series in run.string_series.items():
            run_attr = RunAttributeDefinition(
                RunIdentifier(ProjectIdentifier(project.project_identifier), sys_id),
                AttributeDefinition(path, type="string_series"),
            )

            filtered = []
            for step in sorted(series):
                if step_filter[0] <= step <= step_filter[1]:
                    columns.add(path)
                    filtered_exps.add(run.experiment_name)
                    filtered.append(SeriesValue(step, series[step], _timestamp_millis(step)))
            limited = filtered[-tail_limit:] if tail_limit is not None else filtered

            series_data.setdefault(run_attr, []).extend(limited)

    df = create_series_dataframe(
        series_data,
        project.project_identifier,
        sys_id_label_mapping,
        index_column_name="experiment",
        timestamp_column_name="absolute_time" if include_time == "absolute" else None,
    )

    sorted_columns = list(sorted(columns))
    if include_time == "absolute":
        absolute_columns = [[(c, "value"), (c, "absolute_time")] for c in sorted_columns]
        return df, list(it.chain.from_iterable(absolute_columns)), filtered_exps
    else:
        return df, sorted_columns, filtered_exps


@pytest.mark.parametrize(
    "arg_attributes",
    [
        AttributeFilter(name="^series/.*", type=["string_series"]),
        "^series/string-series.*",
        AttributeFilter(name="^series/.*", type=["string_series"]) | AttributeFilter(name=".*/int-value"),
    ],
)
@pytest.mark.parametrize(
    "arg_experiments",
    [
        Filter.name(EXPERIMENT_NAMES),
        f"{EXPERIMENT_NAMES[0]}|{EXPERIMENT_NAMES[1]}|{EXPERIMENT_NAMES[2]}",
        f"{EXPERIMENT_NAMES[0]} | {EXPERIMENT_NAMES[1]} | {EXPERIMENT_NAMES[2]}",
        EXPERIMENT_NAMES,
    ],
)
@pytest.mark.parametrize(
    "step_range, tail_limit, type_suffix_in_column_names, include_time",
    [
        ((0.0, 5), None, True, None),
        ((0, None), 3, False, "absolute"),
        ((None, 5), 5, True, None),
        ((None, None), None, False, "absolute"),
    ],
)
def test__fetch_string_series__filter_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_string_series(project, include_time, step_range, tail_limit)

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("step_range", [(0.0, 5), (0, None), (None, 5), (None, None), (100, 200)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,type_suffix_in_column_names,include_time",
    [
        (
            Filter.name(EXPERIMENT_NAMES),
            AttributeFilter(name="^series/.*", type=["string_series"]),
            True,
            None,
        ),
        (
            f"{EXPERIMENT_NAMES[0]} | {EXPERIMENT_NAMES[1]} | {EXPERIMENT_NAMES[2]}",
            "^series/string-series.*",
            False,
            "absolute",
        ),
        (
            EXPERIMENT_NAMES,
            AttributeFilter(name="^series/.*", type=["string_series"]) | AttributeFilter(name=".*/int-value"),
            True,
            None,
        ),
    ],
)
def test__fetch_string_series__step_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range: tuple[float | None, float | None],
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_string_series(project, include_time, step_range, tail_limit)

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_time", [None, "absolute"])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,step_range,tail_limit",
    [
        (
            Filter.name(EXPERIMENT_NAMES),
            AttributeFilter(name="^series/.*", type=["string_series"]),
            (0.0, 5),
            None,
        ),
        (
            f"{EXPERIMENT_NAMES[0]} | {EXPERIMENT_NAMES[1]} | {EXPERIMENT_NAMES[2]}",
            "^series/string-series.*",
            (0, None),
            3,
        ),
        (
            EXPERIMENT_NAMES,
            AttributeFilter(name="^series/.*", type=["string_series"]) | AttributeFilter(name=".*/int-value"),
            (None, 5),
            5,
        ),
    ],
)
def test__fetch_string_series__output_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_string_series(project, include_time, step_range, tail_limit)

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


def _to_nq_histogram(histogram: IngestionHistogram) -> Histogram:
    return Histogram(type="COUNTING", edges=histogram.bin_edges, values=histogram.counts)


def create_expected_data_histogram_series(
    project: IngestedProjectData,
    include_time: Union[Literal["absolute"], None],
    step_range: Tuple[Optional[int], Optional[int]],
    tail_limit: Optional[int],
) -> Tuple[pd.DataFrame, List[str], set[str]]:
    series_data: dict[RunAttributeDefinition, list[SeriesValue]] = {}
    sys_id_label_mapping: dict[SysId, str] = {}

    columns = set()
    filtered_exps = set()

    step_filter = (
        step_range[0] if step_range[0] is not None else -float("inf"),
        step_range[1] if step_range[1] is not None else float("inf"),
    )
    for run in project.ingested_runs:
        sys_id = SysId(run.run_id)
        sys_id_label_mapping[sys_id] = run.experiment_name

        for path, series in run.histogram_series.items():
            run_attr = RunAttributeDefinition(
                RunIdentifier(ProjectIdentifier(project.project_identifier), sys_id),
                AttributeDefinition(path, type="histogram_series"),
            )

            filtered = []
            for step in sorted(series):
                if step_filter[0] <= step <= step_filter[1]:
                    columns.add(path)
                    filtered_exps.add(run.experiment_name)
                    filtered.append(SeriesValue(step, _to_nq_histogram(series[step]), _timestamp_millis(step)))
            limited = filtered[-tail_limit:] if tail_limit is not None else filtered

            series_data.setdefault(run_attr, []).extend(limited)

    df = create_series_dataframe(
        series_data,
        project.project_identifier,
        sys_id_label_mapping,
        index_column_name="experiment",
        timestamp_column_name="absolute_time" if include_time == "absolute" else None,
    )

    sorted_columns = list(sorted(columns))
    if include_time == "absolute":
        absolute_columns = [[(c, "value"), (c, "absolute_time")] for c in sorted_columns]
        return df, list(it.chain.from_iterable(absolute_columns)), filtered_exps
    else:
        return df, sorted_columns, filtered_exps


@pytest.mark.parametrize(
    "arg_attributes",
    [
        AttributeFilter(name="^series/.*", type=["histogram_series"]),
        "^series/histogram-series.*",
        AttributeFilter(name="^series/.*", type=["histogram_series"]) | AttributeFilter(name=".*/int-value"),
    ],
)
@pytest.mark.parametrize(
    "arg_experiments",
    [
        f"{EXPERIMENT_NAMES[0]}|{EXPERIMENT_NAMES[1]}|{EXPERIMENT_NAMES[2]}",
        EXPERIMENT_NAMES,
    ],
)
@pytest.mark.parametrize(
    "step_range, tail_limit, type_suffix_in_column_names, include_time",
    [
        ((0.0, 5), None, True, None),
        ((0, None), 3, False, "absolute"),
        ((None, 5), 5, True, None),
        ((None, None), None, False, "absolute"),
    ],
)
def test__fetch_histogram_series__filter_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_histogram_series(
        project, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("step_range", [(0.0, 5), (0, None), (None, 5), (None, None), (100, 200)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,type_suffix_in_column_names,include_time",
    [
        (
            f"{EXPERIMENT_NAMES[0]} | {EXPERIMENT_NAMES[1]} | {EXPERIMENT_NAMES[2]}",
            "^series/histogram-series.*",
            True,
            None,
        ),
        (
            EXPERIMENT_NAMES,
            AttributeFilter(name="^series/.*", type=["histogram_series"]) | AttributeFilter(name=".*/int-value"),
            False,
            "absolute",
        ),
    ],
)
def test__fetch_histogram_series__step_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range: tuple[float | None, float | None],
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_histogram_series(
        project, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_time", [None, "absolute"])
@pytest.mark.parametrize(
    "arg_experiments, arg_attributes, step_range, tail_limit",
    [
        (
            f"{EXPERIMENT_NAMES[0]}|{EXPERIMENT_NAMES[1]}|{EXPERIMENT_NAMES[2]}",
            "^series/histogram-series.*",
            (0.0, 5),
            None,
        ),
        (
            EXPERIMENT_NAMES,
            AttributeFilter(name="^series/.*", type=["histogram_series"]) | AttributeFilter(name=".*/int-value"),
            (0, None),
            3,
        ),
        (
            f"{EXPERIMENT_NAMES[0]} | {EXPERIMENT_NAMES[1]} | {EXPERIMENT_NAMES[2]}",
            "^series/histogram-series.*",
            (None, 5),
            5,
        ),
    ],
)
def test__fetch_histogram_series__output_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_histogram_series(
        project, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


def create_expected_data_file_series(
    project: IngestedProjectData,
    include_time: Union[Literal["absolute"], None],
    step_range: Tuple[Optional[int], Optional[int]],
    tail_limit: Optional[int],
) -> Tuple[pd.DataFrame, List[str], set[str]]:
    series_data: dict[RunAttributeDefinition, list[SeriesValue]] = {}
    sys_id_label_mapping: dict[SysId, str] = {}
    columns = set()
    filtered_exps = set()

    step_filter = (
        step_range[0] if step_range[0] is not None else -float("inf"),
        step_range[1] if step_range[1] is not None else float("inf"),
    )
    for run in project.ingested_runs:
        sys_id = SysId(run.run_id)
        sys_id_label_mapping[sys_id] = run.experiment_name
        for path, series in run.file_series.items():
            filtered = []
            for step in sorted(series):
                if step_filter[0] <= step <= step_filter[1]:
                    columns.add(path)
                    ingestion_file = series[step]
                    filtered.append(
                        SeriesValue(
                            step=step,
                            value=File(
                                path=ingestion_file.destination,
                                size_bytes=len(ingestion_file.source),
                                mime_type=ingestion_file.mime_type or "application/octet-stream",
                            ),
                            timestamp_millis=_timestamp_millis(step),
                        )
                    )
            limited = filtered[-tail_limit:] if tail_limit is not None else filtered
            if limited:
                filtered_exps.add(run.experiment_name)
                run_attr = RunAttributeDefinition(
                    RunIdentifier(ProjectIdentifier(project.project_identifier), sys_id),
                    AttributeDefinition(path, type="file_series"),
                )
                series_data.setdefault(run_attr, []).extend(limited)

    df = create_series_dataframe(
        series_data,
        project.project_identifier,
        sys_id_label_mapping,
        index_column_name="experiment",
        timestamp_column_name="absolute_time" if include_time == "absolute" else None,
    )

    sorted_columns = list(sorted(columns))
    if include_time == "absolute":
        absolute_columns = [[(c, "value"), (c, "absolute_time")] for c in sorted_columns]
        return df, list(it.chain.from_iterable(absolute_columns)), filtered_exps
    else:
        return df, sorted_columns, filtered_exps


@pytest.mark.parametrize(
    "arg_attributes",
    [
        AttributeFilter(name="^files/.*", type=["file_series"]),
        "^files/file-series.*",
        AttributeFilter(name="^files/.*", type=["file_series"]) | AttributeFilter(name=".*/int-value"),
    ],
)
@pytest.mark.parametrize(
    "arg_experiments",
    [
        f"{EXPERIMENT_NAMES[0]}|{EXPERIMENT_NAMES[1]}",
        EXPERIMENT_NAMES[:2],
    ],
)
@pytest.mark.parametrize(
    "step_range, tail_limit, type_suffix_in_column_names, include_time",
    [
        ((0.0, 5), None, True, None),
        ((0, None), 3, False, "absolute"),
        ((None, 5), 5, True, None),
        ((None, None), None, False, "absolute"),
    ],
)
def test__fetch_file_series__filter_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_file_series(project, include_time, step_range, tail_limit)

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("step_range", [(0.0, 5), (0, None), (None, 5), (None, None), (100, 200)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,type_suffix_in_column_names,include_time",
    [
        (
            f"{EXPERIMENT_NAMES[0]} | {EXPERIMENT_NAMES[1]}",
            "^files/file-series.*",
            True,
            None,
        ),
        (
            EXPERIMENT_NAMES[:2],
            AttributeFilter(name="^files/.*", type=["file_series"]) | AttributeFilter(name=".*/int-value"),
            False,
            "absolute",
        ),
    ],
)
def test__fetch_file_series__step_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range: tuple[float | None, float | None],
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_file_series(project, include_time, step_range, tail_limit)

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_time", [None, "absolute"])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,step_range,tail_limit",
    [
        (
            f"{EXPERIMENT_NAMES[0]}|{EXPERIMENT_NAMES[1]}",
            "^files/file-series.*",
            (0.0, 5),
            None,
        ),
        (
            EXPERIMENT_NAMES[:2],
            AttributeFilter(name="^files/.*", type=["file_series"]) | AttributeFilter(name=".*/int-value"),
            (0, None),
            3,
        ),
        (
            f"{EXPERIMENT_NAMES[0]} | {EXPERIMENT_NAMES[1]}",
            "^files/file-series.*",
            (None, 5),
            5,
        ),
    ],
)
def test__fetch_file_series__output_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_file_series(project, include_time, step_range, tail_limit)

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize(
    "lineage_to_the_root,expected_values",
    [
        (
            True,
            [(step, f"val_run1_{step}") for step in range(0, 5)]
            + [(step, f"val_run2_{step}") for step in range(5, 9)]
            + [(step, f"val_run3_{step}") for step in range(9, 12)],
        ),
        (
            False,
            [(step, f"val_run2_{step}") for step in range(5, 9)]
            + [(step, f"val_run3_{step}") for step in range(9, 12)],
        ),
    ],
)
def test__fetch_series__lineage(lineage_project, lineage_to_the_root, expected_values):
    df = fetch_series(
        project=lineage_project.project_identifier,
        experiments=["lineage-child-exp"],
        attributes=r"string_series/s1",
        lineage_to_the_root=lineage_to_the_root,
    )

    run_attribute_definition = RunAttributeDefinition(
        RunIdentifier(ProjectIdentifier(lineage_project.project_identifier), SysId("lineage-run-3")),
        AttributeDefinition("string_series/s1", "string_series"),
    )

    expected = create_series_dataframe(
        series_data={run_attribute_definition: [_to_series_value(step, value) for step, value in expected_values]},
        project_identifier=lineage_project.project_identifier,
        sys_id_label_mapping={SysId("lineage-run-3"): "lineage-child-exp"},
        index_column_name="experiment",
        timestamp_column_name=None,
    )
    pd.testing.assert_frame_equal(df, expected)
