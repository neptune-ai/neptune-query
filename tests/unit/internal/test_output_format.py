import itertools
import pathlib
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pandas import Interval
from pandas._testing import assert_frame_equal

import neptune_query as npt
from neptune_query.exceptions import ConflictingAttributeTypes
from neptune_query.filters import AttributeFilter
from neptune_query.internal import (
    context,
    identifiers,
)
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
    SysName,
)
from neptune_query.internal.output_format import (
    convert_table_to_dataframe,
    create_files_dataframe,
    create_metric_buckets_dataframe,
    create_metrics_dataframe,
    create_series_dataframe,
)
from neptune_query.internal.retrieval import util
from neptune_query.internal.retrieval.attribute_types import File as IFile
from neptune_query.internal.retrieval.attribute_types import (
    FileSeriesAggregations,
    FloatSeriesAggregations,
)
from neptune_query.internal.retrieval.attribute_types import Histogram as IHistogram
from neptune_query.internal.retrieval.attribute_types import (
    HistogramSeriesAggregations,
    StringSeriesAggregations,
)
from neptune_query.internal.retrieval.attribute_values import AttributeValue
from neptune_query.internal.retrieval.metric_buckets import TimeseriesBucket
from neptune_query.internal.retrieval.metrics import FloatPointValue
from neptune_query.internal.retrieval.search import (
    ContainerType,
    ExperimentSysAttrs,
)
from neptune_query.internal.retrieval.series import SeriesValue
from neptune_query.types import File as OFile
from neptune_query.types import Histogram as OHistogram

EXPERIMENT_IDENTIFIER = identifiers.RunIdentifier(
    identifiers.ProjectIdentifier("project/abc"), identifiers.SysId("XXX-1")
)


def test_convert_experiment_table_to_dataframe_empty():
    # given
    experiment_data = {}

    # when
    dataframe = convert_table_to_dataframe(experiment_data, "my-project", type_suffix_in_column_names=False)

    # then
    assert dataframe.empty


def test_convert_experiment_table_to_dataframe_single_string():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(experiment_data, "my-project", type_suffix_in_column_names=False)

    # then
    assert dataframe.to_dict() == {
        "attr1": {"exp1": 42},
    }


def test_convert_experiment_table_to_dataframe_single_string_with_type_suffix():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(experiment_data, "my-project", type_suffix_in_column_names=True)

    # then
    assert dataframe.to_dict() == {
        "attr1:int": {"exp1": 42},
    }


def test_convert_experiment_table_to_dataframe_single_float_series():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(
                AttributeDefinition("attr1", "float_series"),
                FloatSeriesAggregations(last=42.0, min=0.0, max=100, average=24.0, variance=100.0),
                EXPERIMENT_IDENTIFIER,
            ),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(
        experiment_data,
        "my-project",
        type_suffix_in_column_names=False,
    )

    # then
    assert dataframe.to_dict() == {
        "attr1": {"exp1": 42.0},
    }


def test_convert_experiment_table_to_dataframe_single_string_series():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(
                AttributeDefinition("attr1", "string_series"),
                StringSeriesAggregations(last="last log", last_step=10.0),
                EXPERIMENT_IDENTIFIER,
            ),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(
        experiment_data,
        "my-project",
        type_suffix_in_column_names=False,
    )

    # then
    assert dataframe.to_dict() == {
        "attr1": {"exp1": "last log"},
    }


def test_convert_experiment_table_to_dataframe_single_histogram_series():
    # given
    last_histogram = IHistogram(type="COUNTING", edges=list(range(6)), values=list(range(5)))
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(
                AttributeDefinition("attr1", "histogram_series"),
                HistogramSeriesAggregations(last=last_histogram, last_step=10.0),
                EXPERIMENT_IDENTIFIER,
            ),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(
        experiment_data,
        "my-project",
        type_suffix_in_column_names=False,
    )

    # then
    assert dataframe.to_dict() == {
        "attr1": {"exp1": OHistogram(type="COUNTING", edges=list(range(6)), values=list(range(5)))},
    }


def test_convert_experiment_table_to_dataframe_single_file_series():
    # given
    last_file = IFile(path="path/to/last/file", size_bytes=1024, mime_type="text/plain")
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(
                AttributeDefinition("attr1", "file_series"),
                FileSeriesAggregations(last=last_file, last_step=10.0),
                EXPERIMENT_IDENTIFIER,
            ),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(
        experiment_data,
        "my-project",
        type_suffix_in_column_names=False,
    )

    # then
    assert dataframe.to_dict() == {
        "attr1": {
            "exp1": OFile(
                project_identifier="my-project",
                experiment_name="exp1",
                run_id=None,
                attribute_path="attr1",
                step=10.0,
                path=last_file.path,
                size_bytes=last_file.size_bytes,
                mime_type=last_file.mime_type,
            )
        },
    }


def test_convert_experiment_table_to_dataframe_single_file():
    # given
    file = IFile(path="path/to/file", size_bytes=1024, mime_type="text/plain")
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(
                AttributeDefinition("attr1", "file"),
                file,
                EXPERIMENT_IDENTIFIER,
            ),
        ],
    }

    # when
    dataframe_unflattened = convert_table_to_dataframe(experiment_data, "my-project", type_suffix_in_column_names=False)

    # then
    assert dataframe_unflattened.to_dict() == {
        "attr1": {
            "exp1": OFile(
                project_identifier="my-project",
                experiment_name="exp1",
                run_id=None,
                attribute_path="attr1",
                step=None,
                path=file.path,
                size_bytes=file.size_bytes,
                mime_type=file.mime_type,
            )
        },
    }


def test_convert_experiment_table_to_dataframe_disjoint_names():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
        identifiers.SysName("exp2"): [
            AttributeValue(AttributeDefinition("attr2", "int"), 43, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(experiment_data, "my-project", type_suffix_in_column_names=False)

    # then
    expected_data = pd.DataFrame.from_dict(
        {
            "attr1": {"exp1": 42.0, "exp2": float("nan")},
            "attr2": {"exp1": float("nan"), "exp2": 43.0},
        }
    )
    expected_data.index.name = "experiment"
    expected_data.columns.name = "attribute"
    assert_frame_equal(dataframe, expected_data)


def test_convert_experiment_table_to_dataframe_conflicting_types_with_suffix():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
        identifiers.SysName("exp2"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "float"), 0.43, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(experiment_data, "my-project", type_suffix_in_column_names=True)

    # then
    expected_data = pd.DataFrame.from_dict(
        {
            "attr1/a:b:c:float": {"exp1": float("nan"), "exp2": 0.43},
            "attr1/a:b:c:int": {"exp1": 42.0, "exp2": float("nan")},
        }
    )
    expected_data.index.name = "experiment"
    expected_data.columns.name = "attribute"
    assert_frame_equal(dataframe, expected_data)


def test_convert_experiment_table_to_dataframe_conflicting_types_without_suffix():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
        identifiers.SysName("exp2"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "float"), 0.43, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    with pytest.raises(ConflictingAttributeTypes) as exc_info:
        convert_table_to_dataframe(experiment_data, "my-project", type_suffix_in_column_names=False)

    # then
    assert "attr1/a:b:c" in str(exc_info.value)


def test_convert_experiment_table_to_dataframe_duplicate_column_name_with_type_suffix():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr", "int"), 1, EXPERIMENT_IDENTIFIER),
            AttributeValue(AttributeDefinition("attr", "float"), 2.0, EXPERIMENT_IDENTIFIER),
        ],
    }
    # when
    dataframe = convert_table_to_dataframe(experiment_data, "my-project", type_suffix_in_column_names=True)
    # then
    assert set(dataframe.columns.get_level_values(0)) == {"attr:int", "attr:float"}


def test_convert_experiment_table_to_dataframe_duplicate_column_name_without_type_suffix_raises():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr", "int"), 1, EXPERIMENT_IDENTIFIER),
            AttributeValue(AttributeDefinition("attr", "float"), 2.0, EXPERIMENT_IDENTIFIER),
        ],
    }
    # when / then
    with pytest.raises(ConflictingAttributeTypes):
        convert_table_to_dataframe(experiment_data, "my-project", type_suffix_in_column_names=False)


def test_convert_experiment_table_to_dataframe_index_column_name_custom():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
    }
    # when
    dataframe = convert_table_to_dataframe(
        experiment_data,
        "my-project",
        type_suffix_in_column_names=False,
        index_column_name="custom_index",
    )
    # then
    assert dataframe.index.name == "custom_index"
    assert dataframe.to_dict() == {
        "attr1": {"exp1": 42},
    }


EXPERIMENTS = 5
PATHS = 5
STEPS = 10
BUCKETS = 5


def _generate_float_point_values(
    experiments: int, paths: int, steps: int, preview: bool
) -> dict[RunAttributeDefinition, list[FloatPointValue]]:
    return {
        _generate_run_attribute_definition(experiment, path): [
            _generate_float_point_value(step, preview) for step in range(steps)
        ]
        for experiment in range(experiments)
        for path in range(paths)
    }


def _generate_bucket_metrics(
    experiments: int, paths: int, buckets: int
) -> dict[RunAttributeDefinition, list[TimeseriesBucket]]:
    return {
        _generate_run_attribute_definition(experiment, path): [_generate_bucket_metric(index=i) for i in range(buckets)]
        for experiment in range(experiments)
        for path in range(paths)
    }


def _a_timestamp(seconds_delta) -> datetime:
    return datetime(2023, 1, 1, 0, 0, 0, 0, timezone.utc) + timedelta(seconds=seconds_delta)


def _generate_float_point_value(step: int, preview: bool) -> FloatPointValue:
    return (
        _a_timestamp(seconds_delta=step).timestamp(),
        float(step),
        float(step) * 100,
        preview,
        1.0 - (float(step) / 1000.0),
    )


def _generate_run_attribute_definition(
    experiment: int, path: int, attribute_type="float_series"
) -> RunAttributeDefinition:
    return RunAttributeDefinition(
        RunIdentifier(ProjectIdentifier("foo/bar"), SysId(f"sysid{experiment}")),
        AttributeDefinition(f"path{path}", attribute_type),
    )


def _generate_bucket_metric(index: int) -> TimeseriesBucket:
    if index > 0:
        return TimeseriesBucket(
            index=index,
            from_x=20.0 * index,
            to_x=20.0 * (index + 1),
            first_x=20.0 * index + 2,
            first_y=100.0 * (index - 1) + 90.0,
            last_x=20.0 * (index + 1) - 2,
            last_y=100.0 * index,
            y_min=80.0 * index,
            y_max=110.0 * index,
            finite_point_count=10 + index,
            nan_count=5 - index,
            positive_inf_count=2 * index,
            negative_inf_count=index,
            finite_points_sum=950.0 * index,
        )
    else:
        return TimeseriesBucket(
            index=index,
            from_x=float("-inf"),
            to_x=20.0,
            first_x=20.0,
            first_y=0.0,
            last_x=20.0,
            last_y=0.0,
            y_min=0.0,
            y_max=0.0,
            finite_point_count=1,
            nan_count=0,
            positive_inf_count=0,
            negative_inf_count=0,
            finite_points_sum=0.0,
        )


def _format_path_name(path: str, type_suffix_in_column_names: bool) -> str:
    return f"{path}:float_series" if type_suffix_in_column_names else path


def _make_timestamp(year: int, month: int, day: int) -> float:
    return datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000


@pytest.mark.parametrize("include_preview", [False, True])
def test_create_metrics_dataframe_shape(include_preview):
    float_point_values = _generate_float_point_values(EXPERIMENTS, PATHS, STEPS, include_preview)
    sys_id_label_mapping = {SysId(f"sysid{experiment}"): f"exp{experiment}" for experiment in range(EXPERIMENTS)}

    """Test the creation of a flat DataFrame from float point values."""
    df = create_metrics_dataframe(
        metrics_data=float_point_values,
        sys_id_label_mapping=sys_id_label_mapping,
        include_point_previews=include_preview,
        type_suffix_in_column_names=False,
        index_column_name="experiment",
    )

    # Check if the DataFrame is not empty
    assert not df.empty, "DataFrame should not be empty"

    # Check the shape of the DataFrame
    num_expected_rows = EXPERIMENTS * STEPS
    assert df.shape[0] == num_expected_rows, f"DataFrame should have {num_expected_rows} rows"

    # Check the columns of the DataFrame
    all_paths = {key.attribute_definition.name for key in float_point_values.keys()}
    if not include_preview:
        expected_columns = all_paths
    else:
        expected_columns = set(itertools.product(all_paths, ["value", "is_preview", "preview_completion"]))

    assert set(df.columns) == expected_columns, f"DataFrame should have {len(all_paths)} columns"
    assert (
        df.index.get_level_values(0).nunique() == EXPERIMENTS
    ), f"DataFrame should have {EXPERIMENTS} experiment names"

    # Convert DataFrame to list of tuples
    tuples_list = list(df.to_records(index=False))
    assert (
        len(tuples_list) == num_expected_rows
    ), "The list of tuples should have the same number of rows as the DataFrame"


def test_create_metrics_dataframe_from_exp_with_no_points():
    df = create_metrics_dataframe(
        # This input data produces a "hole" in our categorical mapping of experiment names to integers
        metrics_data={
            _generate_run_attribute_definition(1, 1): [_generate_float_point_value(1, False)],
            _generate_run_attribute_definition(2, 2): [],
            _generate_run_attribute_definition(3, 1): [_generate_float_point_value(2, False)],
        },
        sys_id_label_mapping={
            SysId("sysid1"): "exp1",
            SysId("sysid2"): "exp2",
            SysId("sysid3"): "exp3",
        },
        include_point_previews=False,
        type_suffix_in_column_names=False,
        index_column_name="experiment",
    )

    expected_df = pd.DataFrame(
        data={
            "path1": [
                100.0,
                200.0,
            ],
        },
        index=pd.MultiIndex.from_tuples(
            tuples=[
                ("exp1", 1.0),
                ("exp3", 2.0),
            ],
            names=["experiment", "step"],
        ),
    )
    pd.testing.assert_frame_equal(df, expected_df)


def test_create_metrics_dataframe_from_exp_with_no_points_preview():
    df = create_metrics_dataframe(
        # This input data produces a "hole" in our categorical mapping of experiment names to integers
        metrics_data={
            _generate_run_attribute_definition(1, 1): [_generate_float_point_value(1, True)],
            _generate_run_attribute_definition(2, 2): [],
            _generate_run_attribute_definition(3, 1): [_generate_float_point_value(2, True)],
        },
        sys_id_label_mapping={
            SysId("sysid1"): "exp1",
            SysId("sysid2"): "exp2",
            SysId("sysid3"): "exp3",
        },
        include_point_previews=True,
        type_suffix_in_column_names=False,
        index_column_name="experiment",
    )

    expected_df = pd.DataFrame(
        data={
            ("path1", "is_preview"): [
                True,
                True,
            ],
            ("path1", "preview_completion"): [
                0.999,
                0.998,
            ],
            ("path1", "value"): [100.0, 200.0],
        },
        index=pd.MultiIndex.from_tuples(
            tuples=[
                ("exp1", 1.0),
                ("exp3", 2.0),
            ],
            names=["experiment", "step"],
        ),
    )
    expected_df[("path1", "is_preview")] = expected_df[("path1", "is_preview")].astype("object")
    pd.testing.assert_frame_equal(df, expected_df)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_preview", [True, False])
def test_create_metrics_dataframe_with_absolute_timestamp(type_suffix_in_column_names: bool, include_preview: bool):
    # Given
    data = {
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid1")), AttributeDefinition("path1", "float_series")
        ): [
            (_make_timestamp(2023, 1, 1), 1, 10.0, False, 1.0),
        ],
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid1")), AttributeDefinition("path2", "float_series")
        ): [
            (_make_timestamp(2023, 1, 3), 2, 20.0, False, 1.0),
        ],
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid2")), AttributeDefinition("path1", "float_series")
        ): [
            (_make_timestamp(2023, 1, 2), 1, 30.0, True, 0.5),
        ],
    }
    sys_id_label_mapping = {
        SysId("sysid1"): "exp1",
        SysId("sysid2"): "exp2",
    }

    df = create_metrics_dataframe(
        metrics_data=data,
        sys_id_label_mapping=sys_id_label_mapping,
        timestamp_column_name="absolute_time",
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_preview,
        index_column_name="experiment",
    )

    # Then
    expected = {
        (_format_path_name("path1", type_suffix_in_column_names), "absolute_time"): [
            datetime(2023, 1, 1, tzinfo=timezone.utc),
            np.nan,
            datetime(2023, 1, 2, tzinfo=timezone.utc),
        ],
        (_format_path_name("path1", type_suffix_in_column_names), "value"): [10.0, np.nan, 30.0],
        (_format_path_name("path2", type_suffix_in_column_names), "absolute_time"): [
            np.nan,
            datetime(2023, 1, 3, tzinfo=timezone.utc),
            np.nan,
        ],
        (_format_path_name("path2", type_suffix_in_column_names), "value"): [np.nan, 20.0, np.nan],
    }
    if include_preview:
        expected.update(
            {
                (_format_path_name("path1", type_suffix_in_column_names), "is_preview"): [False, np.nan, True],
                (_format_path_name("path1", type_suffix_in_column_names), "preview_completion"): [1.0, np.nan, 0.5],
                (_format_path_name("path2", type_suffix_in_column_names), "is_preview"): [np.nan, False, np.nan],
                (_format_path_name("path2", type_suffix_in_column_names), "preview_completion"): [np.nan, 1.0, np.nan],
            }
        )

    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples([("exp1", 1.0), ("exp1", 2.0), ("exp2", 1.0)], names=["experiment", "step"]),
    )

    pd.testing.assert_frame_equal(df, expected_df)


def _run_definition(run_id: str, attribute_path: str, attribute_type: str = "string_series") -> RunAttributeDefinition:
    return RunAttributeDefinition(
        RunIdentifier(ProjectIdentifier("foo/bar"), SysId(run_id)), AttributeDefinition(attribute_path, attribute_type)
    )


def test_create_string_series_dataframe_with_absolute_timestamp():
    # Given
    series_data = {
        _run_definition("expid1", "path1"): [SeriesValue(1, "aaa", _make_timestamp(2023, 1, 1))],
        _run_definition("expid1", "path2"): [SeriesValue(2, "bbb", _make_timestamp(2023, 1, 3))],
        _run_definition("expid2", "path1"): [SeriesValue(1, "ccc", _make_timestamp(2023, 1, 2))],
    }
    sys_id_label_mapping = {
        SysId("expid1"): "exp1",
        SysId("expid2"): "exp2",
    }

    df = create_series_dataframe(
        series_data=series_data,
        project_identifier="my-project",
        sys_id_label_mapping=sys_id_label_mapping,
        index_column_name="experiment",
        timestamp_column_name="absolute_time",
    )

    # Then
    expected = {
        ("path1", "absolute_time"): [
            datetime(2023, 1, 1, tzinfo=timezone.utc),
            np.nan,
            datetime(2023, 1, 2, tzinfo=timezone.utc),
        ],
        ("path1", "value"): ["aaa", np.nan, "ccc"],
        ("path2", "absolute_time"): [
            np.nan,
            datetime(2023, 1, 3, tzinfo=timezone.utc),
            np.nan,
        ],
        ("path2", "value"): [np.nan, "bbb", np.nan],
    }
    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples([("exp1", 1.0), ("exp1", 2.0), ("exp2", 1.0)], names=["experiment", "step"]),
    )
    pd.testing.assert_frame_equal(df, expected_df)


def test_create_histogram_dataframe_with_absolute_timestamp():
    # Given
    histograms = [
        IHistogram(type="COUNTING", edges=[1, 2, 3], values=[10, 20]),
        IHistogram(type="COUNTING", edges=[5, 6], values=[100]),
        IHistogram(type="COUNTING", edges=[1, 2, 3], values=[11, 19]),
    ]
    series_data = {
        _run_definition("expid1", "path1", "histogram_series"): [
            SeriesValue(1, histograms[0], _make_timestamp(2023, 1, 1))
        ],
        _run_definition("expid1", "path2", "histogram_series"): [
            SeriesValue(2, histograms[1], _make_timestamp(2023, 1, 3))
        ],
        _run_definition("expid2", "path1", "histogram_series"): [
            SeriesValue(1, histograms[2], _make_timestamp(2023, 1, 2))
        ],
    }
    sys_id_label_mapping = {
        SysId("expid1"): "exp1",
        SysId("expid2"): "exp2",
    }

    df = create_series_dataframe(
        series_data=series_data,
        project_identifier="my-project",
        sys_id_label_mapping=sys_id_label_mapping,
        index_column_name="experiment",
        timestamp_column_name="absolute_time",
    )

    # Then
    expected = {
        ("path1", "absolute_time"): [
            datetime(2023, 1, 1, tzinfo=timezone.utc),
            np.nan,
            datetime(2023, 1, 2, tzinfo=timezone.utc),
        ],
        ("path1", "value"): [
            OHistogram(type=histograms[0].type, edges=histograms[0].edges, values=histograms[0].values),
            np.nan,
            OHistogram(type=histograms[2].type, edges=histograms[2].edges, values=histograms[2].values),
        ],
        ("path2", "absolute_time"): [
            np.nan,
            datetime(2023, 1, 3, tzinfo=timezone.utc),
            np.nan,
        ],
        ("path2", "value"): [
            np.nan,
            OHistogram(type=histograms[1].type, edges=histograms[1].edges, values=histograms[1].values),
            np.nan,
        ],
    }
    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples([("exp1", 1.0), ("exp1", 2.0), ("exp2", 1.0)], names=["experiment", "step"]),
    )
    pd.testing.assert_frame_equal(df, expected_df)


def test_create_file_series_dataframe_with_absolute_timestamp():
    # Given
    files = [
        IFile(path="path/to/file1", size_bytes=1024, mime_type="text/plain"),
        IFile(path="path/to/file2", size_bytes=2048, mime_type="image/png"),
        IFile(path="path/to/file3", size_bytes=512, mime_type="application/json"),
    ]
    series_data = {
        _run_definition("expid1", "path1", "file_series"): [SeriesValue(1, files[0], _make_timestamp(2023, 1, 1))],
        _run_definition("expid1", "path2", "file_series"): [SeriesValue(2, files[1], _make_timestamp(2023, 1, 3))],
        _run_definition("expid2", "path1", "file_series"): [SeriesValue(1, files[2], _make_timestamp(2023, 1, 2))],
    }
    sys_id_label_mapping = {
        SysId("expid1"): "exp1",
        SysId("expid2"): "exp2",
    }

    df = create_series_dataframe(
        series_data=series_data,
        project_identifier="my-project",
        sys_id_label_mapping=sys_id_label_mapping,
        index_column_name="experiment",
        timestamp_column_name="absolute_time",
    )

    # Then
    downloadable_files = [
        OFile(
            experiment_name="exp1",
            run_id=None,
            project_identifier="my-project",
            attribute_path="path1",
            step=1.0,
            path=files[0].path,
            size_bytes=files[0].size_bytes,
            mime_type=files[0].mime_type,
        ),
        OFile(
            experiment_name="exp1",
            run_id=None,
            project_identifier="my-project",
            attribute_path="path2",
            step=2.0,
            path=files[1].path,
            size_bytes=files[1].size_bytes,
            mime_type=files[1].mime_type,
        ),
        OFile(
            experiment_name="exp2",
            run_id=None,
            project_identifier="my-project",
            attribute_path="path1",
            step=1.0,
            path=files[2].path,
            size_bytes=files[2].size_bytes,
            mime_type=files[2].mime_type,
        ),
    ]
    expected = {
        ("path1", "absolute_time"): [
            datetime(2023, 1, 1, tzinfo=timezone.utc),
            np.nan,
            datetime(2023, 1, 2, tzinfo=timezone.utc),
        ],
        ("path1", "value"): [downloadable_files[0], np.nan, downloadable_files[2]],
        ("path2", "absolute_time"): [
            np.nan,
            datetime(2023, 1, 3, tzinfo=timezone.utc),
            np.nan,
        ],
        ("path2", "value"): [np.nan, downloadable_files[1], np.nan],
    }
    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples([("exp1", 1.0), ("exp1", 2.0), ("exp2", 1.0)], names=["experiment", "step"]),
    )
    pd.testing.assert_frame_equal(df, expected_df)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_preview", [True, False])
def test_create_metrics_dataframe_without_timestamp(type_suffix_in_column_names: bool, include_preview: bool):
    # Given
    data = {
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid1")), AttributeDefinition("path1", "float_series")
        ): [
            (_make_timestamp(2023, 1, 1), 1, 10.0, False, 1.0),
        ],
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid1")), AttributeDefinition("path2", "float_series")
        ): [
            (_make_timestamp(2023, 1, 3), 2, 20.0, False, 1.0),
        ],
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid2")), AttributeDefinition("path1", "float_series")
        ): [
            (_make_timestamp(2023, 1, 2), 1, 30.0, True, 0.5),
        ],
    }
    sys_id_label_mapping = {
        SysId("sysid1"): "exp1",
        SysId("sysid2"): "exp2",
    }

    df = create_metrics_dataframe(
        metrics_data=data,
        sys_id_label_mapping=sys_id_label_mapping,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_preview,
        index_column_name="experiment",
    )

    # Then
    if not include_preview:
        # Flat columns
        expected = {
            _format_path_name("path1", type_suffix_in_column_names): [10.0, np.nan, 30.0],
            _format_path_name("path2", type_suffix_in_column_names): [np.nan, 20.0, np.nan],
        }
    else:
        # MultiIndex columns are returned on include_preview=True
        expected = {
            (_format_path_name("path1", type_suffix_in_column_names), "value"): [10.0, np.nan, 30.0],
            (_format_path_name("path2", type_suffix_in_column_names), "value"): [np.nan, 20.0, np.nan],
            (_format_path_name("path1", type_suffix_in_column_names), "is_preview"): [False, np.nan, True],
            (_format_path_name("path1", type_suffix_in_column_names), "preview_completion"): [1.0, np.nan, 0.5],
            (_format_path_name("path2", type_suffix_in_column_names), "is_preview"): [np.nan, False, np.nan],
            (_format_path_name("path2", type_suffix_in_column_names), "preview_completion"): [np.nan, 1.0, np.nan],
        }

    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples([("exp1", 1.0), ("exp1", 2.0), ("exp2", 1.0)], names=["experiment", "step"]),
    )

    pd.testing.assert_frame_equal(df, expected_df)


def test_create_metrics_dataframe_random_order():
    # Given
    data = {
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid1")), AttributeDefinition("path1", "float_series")
        ): [
            (_make_timestamp(2023, 1, 1), 3, 30.0, False, 1.0),
            (_make_timestamp(2023, 1, 1), 2, 20.0, False, 1.0),
            (_make_timestamp(2023, 1, 1), 1, 10.0, False, 1.0),
            (_make_timestamp(2023, 1, 1), 5, 50.0, False, 1.0),
            (_make_timestamp(2023, 1, 1), 4, 40.0, False, 1.0),
        ],
    }
    sys_id_label_mapping = {
        SysId("sysid1"): "exp1",
    }

    df = create_metrics_dataframe(
        metrics_data=data,
        sys_id_label_mapping=sys_id_label_mapping,
        type_suffix_in_column_names=False,
        include_point_previews=False,
        index_column_name="experiment",
    )

    # Then
    expected = {
        "path1": [10.0, 20.0, 30.0, 40.0, 50.0],
    }

    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples(
            [("exp1", 1.0), ("exp1", 2.0), ("exp1", 3.0), ("exp1", 4.0), ("exp1", 5.0)], names=["experiment", "step"]
        ),
    )

    pd.testing.assert_frame_equal(df, expected_df)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_preview", [True, False])
@pytest.mark.parametrize("timestamp_column_name", [None, "absolute"])
def test_create_empty_metrics_dataframe(
    type_suffix_in_column_names: bool, include_preview: bool, timestamp_column_name: str
):
    # When
    df = create_metrics_dataframe(
        metrics_data={},
        sys_id_label_mapping={},
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_preview,
        timestamp_column_name=timestamp_column_name,
        index_column_name="experiment",
    )

    # Then
    expected_df = (
        pd.DataFrame(data={"experiment": [], "step": []})
        .astype(dtype={"experiment": "object", "step": "float64"})
        .set_index(["experiment", "step"])
    )

    # With previews or timestamps, MultiIndex columns are returned
    if include_preview or timestamp_column_name:
        expected_df.columns = pd.MultiIndex.from_product([[], ["value"]], names=[None, None])
        # the comparator seems not to delve into the exact column names on the 2nd level when the 1st level is empty..

    pd.testing.assert_frame_equal(df, expected_df)


@pytest.mark.parametrize("timestamp_column_name", [None, "absolute"])
def test_create_empty_series_dataframe(timestamp_column_name: str):
    # Given empty dataframe

    # When
    df = create_series_dataframe(
        series_data={},
        project_identifier="my-project",
        sys_id_label_mapping={},
        index_column_name="experiment",
        timestamp_column_name=timestamp_column_name,
    )

    # Then
    expected_df = (
        pd.DataFrame(data={"experiment": [], "step": []})
        .astype(dtype={"experiment": "object", "step": "float64"})
        .set_index(["experiment", "step"])
    )

    if timestamp_column_name:
        expected_df.columns = pd.MultiIndex.from_product([[], ["value"]], names=[None, None])

    pd.testing.assert_frame_equal(df, expected_df)


@pytest.mark.parametrize(
    "path", ["value", "step", "experiment", "value", "timestamp", "is_preview", "preview_completion"]
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_preview", [True, False])
@pytest.mark.parametrize("timestamp_column_name", ["absolute_time"])
def test_create_metrics_dataframe_with_reserved_paths_with_multiindex(
    path: str, type_suffix_in_column_names: bool, include_preview: bool, timestamp_column_name: str
):
    # Given
    data = {
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid1")), AttributeDefinition(path, "float_series")
        ): [
            (_make_timestamp(2023, 1, 1), 1, 10.0, False, 1.0),
        ],
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid2")), AttributeDefinition(path, "float_series")
        ): [
            (_make_timestamp(2023, 1, 2), 1, 30.0, True, 0.5),
        ],
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid1")),
            AttributeDefinition("other_path", "float_series"),
        ): [
            (_make_timestamp(2023, 1, 3), 2, 20.0, False, 1.0),
        ],
    }
    sys_id_label_mapping = {
        SysId("sysid1"): "exp1",
        SysId("sysid2"): "exp2",
    }

    df = create_metrics_dataframe(
        metrics_data=data,
        sys_id_label_mapping=sys_id_label_mapping,
        timestamp_column_name=timestamp_column_name,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_preview,
        index_column_name="experiment",
    )

    # Then
    expected = {
        (_format_path_name(path, type_suffix_in_column_names), "absolute_time"): [
            datetime(2023, 1, 1, tzinfo=timezone.utc),
            np.nan,
            datetime(2023, 1, 2, tzinfo=timezone.utc),
        ],
        (_format_path_name(path, type_suffix_in_column_names), "value"): [10.0, np.nan, 30.0],
        (_format_path_name("other_path", type_suffix_in_column_names), "absolute_time"): [
            np.nan,
            datetime(2023, 1, 3, tzinfo=timezone.utc),
            np.nan,
        ],
        (_format_path_name("other_path", type_suffix_in_column_names), "value"): [np.nan, 20.0, np.nan],
    }
    if include_preview:
        expected.update(
            {
                (_format_path_name(path, type_suffix_in_column_names), "is_preview"): [False, np.nan, True],
                (_format_path_name(path, type_suffix_in_column_names), "preview_completion"): [1.0, np.nan, 0.5],
                (_format_path_name("other_path", type_suffix_in_column_names), "is_preview"): [np.nan, False, np.nan],
                (_format_path_name("other_path", type_suffix_in_column_names), "preview_completion"): [
                    np.nan,
                    1.0,
                    np.nan,
                ],
            }
        )

    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples([("exp1", 1.0), ("exp1", 2.0), ("exp2", 1.0)], names=["experiment", "step"]),
    )

    pd.testing.assert_frame_equal(df, expected_df)


@pytest.mark.parametrize(
    "path", ["value", "step", "experiment", "value", "timestamp", "is_preview", "preview_completion"]
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test_create_metrics_dataframe_with_reserved_paths_with_flat_index(path: str, type_suffix_in_column_names: bool):
    # Given
    data = {
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid1")), AttributeDefinition(path, "float_series")
        ): [
            (_make_timestamp(2023, 1, 1), 1, 10.0, False, 1.0),
        ],
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid2")), AttributeDefinition(path, "float_series")
        ): [
            (_make_timestamp(2023, 1, 2), 1, 30.0, True, 0.5),
        ],
        RunAttributeDefinition(
            RunIdentifier(ProjectIdentifier("foo/bar"), SysId("sysid1")),
            AttributeDefinition("other_path", "float_series"),
        ): [
            (_make_timestamp(2023, 1, 3), 2, 20.0, False, 1.0),
        ],
    }
    sys_id_label_mapping = {
        SysId("sysid1"): "exp1",
        SysId("sysid2"): "exp2",
    }

    df = create_metrics_dataframe(
        metrics_data=data,
        sys_id_label_mapping=sys_id_label_mapping,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=False,
        index_column_name="experiment",
    )

    # Then
    expected = {
        _format_path_name(path, type_suffix_in_column_names): [10.0, np.nan, 30.0],
        _format_path_name("other_path", type_suffix_in_column_names): [np.nan, 20.0, np.nan],
    }

    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples([("exp1", 1.0), ("exp1", 2.0), ("exp2", 1.0)], names=["experiment", "step"]),
    )

    pd.testing.assert_frame_equal(df, expected_df)


def test_create_files_dataframe_empty():
    # given
    files_data = {}
    container_type = ContainerType.EXPERIMENT

    # when
    dataframe = create_files_dataframe(file_data=files_data, container_type=container_type)

    # then
    assert dataframe.empty
    assert dataframe.index.names == ["experiment", "step"]
    assert dataframe.columns.names == ["attribute"]


def test_create_files_dataframe():
    # given
    file_data = {
        OFile(
            experiment_name="experiment_1",
            run_id=None,
            project_identifier="foo/bar",
            attribute_path="attr1",
            step=None,
            path="",
            size_bytes=0,
            mime_type="",
        ): pathlib.Path("/path/to/file1"),
        OFile(
            experiment_name="experiment_2",
            run_id=None,
            project_identifier="foo/bar",
            attribute_path="attr2",
            step=None,
            path="",
            size_bytes=0,
            mime_type="",
        ): pathlib.Path("/path/to/file2"),
        OFile(
            experiment_name="experiment_3",
            run_id=None,
            project_identifier="foo/bar",
            attribute_path="series1",
            step=1.0,
            path="",
            size_bytes=0,
            mime_type="",
        ): pathlib.Path("/path/to/file3"),
        OFile(
            experiment_name="experiment_4",
            run_id=None,
            project_identifier="foo/bar",
            attribute_path="attr1",
            step=None,
            path="",
            size_bytes=0,
            mime_type="",
        ): None,
    }
    container_type = ContainerType.EXPERIMENT
    index_column_name = "experiment"

    # when
    dataframe = create_files_dataframe(file_data=file_data, container_type=container_type)

    # then
    expected_data = [
        {index_column_name: "experiment_1", "step": None, "attr1": str(pathlib.Path("/path/to/file1"))},
        {index_column_name: "experiment_2", "step": None, "attr2": str(pathlib.Path("/path/to/file2"))},
        {index_column_name: "experiment_3", "step": 1.0, "series1": str(pathlib.Path("/path/to/file3"))},
        {index_column_name: "experiment_4", "step": None, "attr1": None},
    ]
    expected_df = pd.DataFrame(expected_data).set_index([index_column_name, "step"])
    expected_df.columns.names = ["attribute"]
    assert_frame_equal(dataframe, expected_df)


def test_create_files_dataframe_index_name_attribute_conflict():
    # given
    file_data = {
        OFile(
            experiment_name="experiment_1",
            run_id=None,
            project_identifier="foo/bar",
            attribute_path="experiment",
            step=None,
            path="",
            size_bytes=0,
            mime_type="",
        ): pathlib.Path("/path/to/file1"),
    }
    container_type = ContainerType.EXPERIMENT
    index_column_name = "experiment"

    # when
    dataframe = create_files_dataframe(file_data=file_data, container_type=container_type)

    # then
    expected_data = [
        {"_REPLACE_": "experiment_1", "step": None, "experiment": str(pathlib.Path("/path/to/file1"))},
    ]
    expected_df = pd.DataFrame(expected_data).set_index(["_REPLACE_", "step"])
    expected_df.columns.names = ["attribute"]
    expected_df.index.names = [index_column_name, "step"]
    assert_frame_equal(dataframe, expected_df)


@pytest.mark.parametrize("duplicate_variant", [(2, 1, 1), (1, 2, 1), (1, 1, 2), (2, 2, 2)])
@pytest.mark.parametrize("include_time", [None, "absolute"])
def test_fetch_series_duplicate_values(duplicate_variant, include_time):
    #  given
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    experiments = [ExperimentSysAttrs(sys_id=SysId("sysid0"), sys_name=SysName("irrelevant"))]
    attributes = [AttributeDefinition(name="attribute0", type="irrelevant")]
    run_attribute_definitions = [
        RunAttributeDefinition(
            run_identifier=RunIdentifier(project_identifier=project, sys_id=experiments[0].sys_id),
            attribute_definition=attributes[0],
        )
    ]

    duped_values, duped_attributes, duped_pages = duplicate_variant
    series_values = [
        (
            run_attribute_definitions[0],
            [SeriesValue(step=i, value=f"{i}", timestamp_millis=i) for i in range(100)] * duped_values,
        )
    ] * duped_attributes

    # when
    with (
        patch("neptune_query.internal.composition.fetch_series.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_experiment_sys_attrs") as fetch_experiment_sys_attrs,
        patch(
            "neptune_query.internal.retrieval.attribute_definitions.fetch_attribute_definitions_single_filter"
        ) as fetch_attribute_definitions_single_filter,
        patch("neptune_query.internal.retrieval.series.fetch_series_values") as fetch_series_values,
    ):
        get_client.return_value = None
        fetch_experiment_sys_attrs.return_value = iter([util.Page(experiments)])
        fetch_attribute_definitions_single_filter.side_effect = lambda **kwargs: iter([util.Page(attributes)])
        fetch_series_values.return_value = iter([util.Page(series_values)] * duped_pages)

        df = npt.fetch_series(
            project=project,
            experiments="ignored",
            attributes=AttributeFilter(name="ignored"),
            include_time=include_time,
        )

    # then
    assert df.shape == (100, 1 if not include_time else 2)


@pytest.mark.parametrize("include_time", [None, "absolute"])
def test_fetch_metrics_duplicate_values(include_time):
    #  given
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    experiments = [ExperimentSysAttrs(sys_id=SysId("sysid0"), sys_name=SysName("irrelevant"))]
    attributes = [AttributeDefinition(name="attribute0", type="float_series")]
    run_attribute_definitions = [
        RunAttributeDefinition(
            run_identifier=RunIdentifier(project_identifier=project, sys_id=experiments[0].sys_id),
            attribute_definition=attributes[0],
        )
    ]
    series_values = {
        run_attribute_definitions[0]: [SeriesValue(step=i, value=float(i), timestamp_millis=i) for i in range(100)] * 2
    }

    # when
    with (
        patch("neptune_query.internal.composition.fetch_metrics.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_experiment_sys_attrs") as fetch_experiment_sys_attrs,
        patch(
            "neptune_query.internal.retrieval.attribute_definitions.fetch_attribute_definitions_single_filter"
        ) as fetch_attribute_definitions_single_filter,
        patch(
            "neptune_query.internal.composition.fetch_metrics.fetch_multiple_series_values"
        ) as fetch_multiple_series_values,
    ):
        get_client.return_value = None
        fetch_experiment_sys_attrs.return_value = iter([util.Page(experiments)])
        fetch_attribute_definitions_single_filter.side_effect = lambda **kwargs: iter([util.Page(attributes)])
        fetch_multiple_series_values.return_value = series_values

        df = npt.fetch_metrics(
            project=project,
            experiments="ignored",
            attributes=AttributeFilter(name="ignored"),
            include_time=include_time,
        )

    # then
    assert df.shape == (100, 1 if not include_time else 2)


def test_create_empty_metric_buckets_dataframe():
    # given
    buckets_data = {}
    sys_id_label_mapping = {}

    # when
    df = create_metric_buckets_dataframe(
        buckets_data=buckets_data,
        sys_id_label_mapping=sys_id_label_mapping,
        container_column_name="experiment",
    )

    # Then
    expected_df = pd.DataFrame(data={"bucket": []}).astype(dtype={"bucket": "object"}).set_index("bucket")
    expected_df.columns = pd.MultiIndex.from_product([[], [], ["x", "y"]], names=["experiment", "metric", "bucket"])
    expected_df.index.name = None

    pd.testing.assert_frame_equal(df, expected_df)


def test_create_metric_buckets_dataframe():
    buckets_data = _generate_bucket_metrics(EXPERIMENTS, PATHS, BUCKETS)
    sys_id_label_mapping = {SysId(f"sysid{experiment}"): f"exp{experiment}" for experiment in range(EXPERIMENTS)}

    """Test the creation of a flat DataFrame from float point values."""
    df = create_metric_buckets_dataframe(
        buckets_data=buckets_data,
        sys_id_label_mapping=sys_id_label_mapping,
        container_column_name="experiment",
    )

    # Check if the DataFrame is not empty
    assert not df.empty, "DataFrame should not be empty"

    # Check the shape of the DataFrame
    num_expected_rows = BUCKETS - 1
    assert df.shape[0] == num_expected_rows, f"DataFrame should have {num_expected_rows} rows"

    # Check the columns of the DataFrame
    METRICS = ["x", "y"]
    expected_columns = {
        (sys_id_label_mapping[key.run_identifier.sys_id], key.attribute_definition.name, metric)
        for key in buckets_data.keys()
        for metric in METRICS
    }

    assert set(df.columns) == expected_columns, f"DataFrame should have {expected_columns} columns"
    assert (
        df.columns.get_level_values(0).nunique() == EXPERIMENTS
    ), f"DataFrame should have {EXPERIMENTS} experiment names"
    assert df.columns.get_level_values(1).nunique() == PATHS, f"DataFrame should have {PATHS} paths"
    assert df.columns.get_level_values(2).nunique() == len(METRICS), f"DataFrame should have {METRICS} metrics"


@pytest.mark.parametrize(
    "data,expected_df",
    [
        (
            {
                _generate_run_attribute_definition(experiment=1, path=1): [
                    _generate_bucket_metric(index=0),
                ]
            },
            pd.DataFrame(
                {
                    ("exp1", "path1", "x"): [20.0],
                    ("exp1", "path1", "y"): [0.0],
                },
                index=pd.Index([Interval(20.0, 20.0, closed="both")], dtype="object"),
            ),
        ),
        (
            {
                _generate_run_attribute_definition(experiment=1, path=1): [
                    _generate_bucket_metric(index=0),
                    _generate_bucket_metric(index=2),
                ]
            },
            pd.DataFrame(
                {
                    ("exp1", "path1", "x"): [20.0, 58.0],
                    ("exp1", "path1", "y"): [0.0, 200.0],
                },
                index=pd.Index(
                    [Interval(20.0, 40.0, closed="both"), Interval(40.0, 60.0, closed="right")], dtype="object"
                ),
            ),
        ),
        (
            {
                _generate_run_attribute_definition(experiment=1, path=1): [
                    _generate_bucket_metric(index=0),
                    _generate_bucket_metric(index=3),
                ]
            },
            pd.DataFrame(
                {
                    ("exp1", "path1", "x"): [20.0, 78.0],
                    ("exp1", "path1", "y"): [0.0, 300.0],
                },
                index=pd.Index(
                    [Interval(20.0, 40.0, closed="both"), Interval(60.0, 80.0, closed="right")], dtype="object"
                ),
            ),
        ),
    ],
)
def test_create_metric_buckets_dataframe_parametrized(data, expected_df):
    # Given
    sys_id_label_mapping = {
        SysId("sysid1"): "exp1",
    }
    expected_df.columns.names = ["experiment", "metric", "bucket"]

    # When
    df = create_metric_buckets_dataframe(
        buckets_data=data,
        sys_id_label_mapping=sys_id_label_mapping,
        container_column_name="experiment",
    )

    # Then
    pd.testing.assert_frame_equal(df, expected_df)


def test_create_metric_buckets_dataframe_missing_values():
    # Given
    data = {
        _generate_run_attribute_definition(experiment=1, path=1): [
            _generate_bucket_metric(index=0),
            _generate_bucket_metric(index=1),
            _generate_bucket_metric(index=2),
        ],
        _generate_run_attribute_definition(experiment=1, path=2): [
            _generate_bucket_metric(index=1),
            _generate_bucket_metric(index=2),
        ],
        _generate_run_attribute_definition(experiment=1, path=3): [
            _generate_bucket_metric(index=2),
            _generate_bucket_metric(index=3),
        ],
        _generate_run_attribute_definition(experiment=2, path=1): [
            _generate_bucket_metric(index=0),
            _generate_bucket_metric(index=3),
        ],
    }
    sys_id_label_mapping = {
        SysId("sysid1"): "exp1",
        SysId("sysid2"): "exp2",
    }

    df = create_metric_buckets_dataframe(
        buckets_data=data,
        sys_id_label_mapping=sys_id_label_mapping,
        container_column_name="experiment",
    )

    # Then
    expected = {
        ("exp1", "path1", "x"): [20.0, 58.0, np.nan],
        ("exp1", "path1", "y"): [0.0, 200.0, np.nan],
        ("exp1", "path2", "x"): [22.0, 58.0, np.nan],
        ("exp1", "path2", "y"): [90.0, 200.0, np.nan],
        ("exp1", "path3", "x"): [np.nan, 42.0, 78.0],
        ("exp1", "path3", "y"): [np.nan, 190.0, 300.0],
        ("exp2", "path1", "x"): [20.0, np.nan, 78.0],
        ("exp2", "path1", "y"): [0.0, np.nan, 300.0],
    }

    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.Index(
            [
                Interval(20.0, 40.0, closed="both"),
                Interval(40.0, 60.0, closed="right"),
                Interval(60.0, 80.0, closed="right"),
            ],
            dtype="object",
        ),
    )
    expected_df.columns.names = ["experiment", "metric", "bucket"]

    pd.testing.assert_frame_equal(df, expected_df)


def test_create_metric_buckets_dataframe_sorted():
    # Given
    data = {
        _generate_run_attribute_definition(experiment=1, path=1): [
            _generate_bucket_metric(index=2),
            _generate_bucket_metric(index=0),
            _generate_bucket_metric(index=1),
            _generate_bucket_metric(index=3),
        ],
    }
    sys_id_label_mapping = {
        SysId("sysid1"): "exp1",
    }

    df = create_metric_buckets_dataframe(
        buckets_data=data,
        sys_id_label_mapping=sys_id_label_mapping,
        container_column_name="experiment",
    )

    # Then
    expected = {
        ("exp1", "path1", "x"): [20.0, 58.0, 78.0],
        ("exp1", "path1", "y"): [0.0, 200.0, 300.0],
    }

    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.Index(
            [
                Interval(20.0, 40.0, closed="both"),
                Interval(40.0, 60.0, closed="right"),
                Interval(60.0, 80.0, closed="right"),
            ]
        ),
    )
    expected_df.columns.names = ["experiment", "metric", "bucket"]

    pd.testing.assert_frame_equal(df, expected_df)


def test_create_metric_buckets_dataframe_completely_nan():
    # Given
    def _generate_bucket_metric_nan(index: int) -> TimeseriesBucket:
        return TimeseriesBucket(
            index=index,
            from_x=20.0 * index if index > 0 else float("-inf"),
            to_x=20.0 * (index + 1),
            first_x=float("nan"),
            first_y=float("nan"),
            last_x=float("nan"),
            last_y=float("nan"),
            y_min=float("nan"),
            y_max=float("nan"),
            finite_point_count=0,
            finite_points_sum=0,
            nan_count=1,
            positive_inf_count=0,
            negative_inf_count=0,
        )

    data = {
        _generate_run_attribute_definition(experiment=1, path=1): [
            _generate_bucket_metric(index=0),
            _generate_bucket_metric(index=1),
            _generate_bucket_metric(index=2),
        ],
        _generate_run_attribute_definition(experiment=2, path=2): [
            _generate_bucket_metric_nan(index=0),
            _generate_bucket_metric_nan(index=1),
            _generate_bucket_metric_nan(index=2),
        ],
    }
    sys_id_label_mapping = {
        SysId("sysid1"): "exp1",
        SysId("sysid2"): "exp2",
    }

    df = create_metric_buckets_dataframe(
        buckets_data=data,
        sys_id_label_mapping=sys_id_label_mapping,
        container_column_name="experiment",
    )

    # Then
    expected = {
        ("exp1", "path1", "x"): [20.0, 58.0],
        ("exp1", "path1", "y"): [0.0, 200.0],
        ("exp2", "path2", "x"): [np.nan, np.nan],
        ("exp2", "path2", "y"): [np.nan, np.nan],
    }

    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.Index(
            [
                Interval(20.0, 40.0, closed="both"),
                Interval(40.0, 60.0, closed="right"),
            ]
        ),
    )
    expected_df.columns.names = ["experiment", "metric", "bucket"]

    pd.testing.assert_frame_equal(df, expected_df)
