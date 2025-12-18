from datetime import (
    datetime,
    timezone,
)

import pandas as pd
import pytest

from neptune_query import (
    fetch_experiments_table,
    list_experiments,
)
from neptune_query.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from neptune_query.types import File
from neptune_query.types import Histogram as OHistogram
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    IngestionFile,
    IngestionHistogram,
    ProjectData,
    RunData,
)

# Local dataset definitions for this module
EXPERIMENT_NAMES = ["test_alpha_1", "test_alpha_2", "test_alpha_3"]

FLOAT_SERIES_PATHS = [f"metrics/float-series-value_{j}" for j in range(3)]
STRING_SERIES_PATHS = [f"metrics/string-series-value_{j}" for j in range(2)]
HISTOGRAM_SERIES_PATHS = [f"metrics/histogram-series-value_{j}" for j in range(2)]


@pytest.fixture(scope="module", autouse=True)
def run_with_attributes_autouse():
    # Override autouse ingestion from shared v1 fixtures; this module ingests its own data.
    return None


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    runs: list[RunData] = [
        RunData(
            experiment_name="test_alpha_1",
            configs={
                "int-value": 1,
                "float-value": 1.0,
                "str-value": "hello_1",
                "bool-value": True,
                "datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc),
            },
            string_sets={"string_set-value": [f"string-0-{j}" for j in range(3)]},
            float_series={
                "metrics/step": {float(step): float(step) for step in range(10)},
                "metrics/float-series-value_0": {float(step): step / 1.0 for step in range(10)},
                "metrics/float-series-value_1": {float(step): step / 2.0 for step in range(10)},
            },
            string_series={p: {float(step): f"string-{int(step)}" for step in range(10)} for p in STRING_SERIES_PATHS},
            histogram_series={
                p: {
                    float(step): IngestionHistogram(
                        bin_edges=[n + step for n in range(4)], counts=[n * step for n in range(3)]
                    )
                    for step in range(10)
                }
                for p in HISTOGRAM_SERIES_PATHS
            },
            files={
                "files/file-value.txt": IngestionFile(
                    source=b"Text content",
                    mime_type="text/plain",
                    destination="dst/file/file-value.txt",
                ),
            },
            file_series={
                "files/file-series-value_0": {
                    float(step): IngestionFile(
                        source=f"file-{int(step)}".encode("utf-8"),
                        mime_type="application/octet-stream",
                        destination="dst/file/file-series-value_0",
                    )
                    for step in range(3)
                },
                "files/file-series-value_1": {
                    float(step): IngestionFile(
                        source=f"file-{int(step)}".encode("utf-8"),
                        mime_type="application/octet-stream",
                        destination="dst/file/file-series-value_1",
                    )
                    for step in range(3)
                },
            },
        ),
        RunData(
            experiment_name="test_alpha_2",
            configs={
                "int-value": 2,
                "float-value": 2.0,
                "str-value": "hello_2",
                "bool-value": False,
                "datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc),
            },
            string_sets={"string_set-value": [f"string-1-{j}" for j in range(3)]},
            float_series={
                "metrics/step": {float(step): float(step) for step in range(10)},
                **{p: {float(step): step / 2.0 for step in range(10)} for p in FLOAT_SERIES_PATHS},
            },
            string_series={p: {float(step): f"string-{int(step)}" for step in range(10)} for p in STRING_SERIES_PATHS},
            histogram_series={
                p: {
                    float(step): IngestionHistogram(
                        bin_edges=[n + step for n in range(4)], counts=[n * step for n in range(3)]
                    )
                    for step in range(10)
                }
                for p in HISTOGRAM_SERIES_PATHS
            },
        ),
        RunData(
            experiment_name="test_alpha_3",
            configs={
                "int-value": 3,
                "float-value": 3.0,
                "str-value": "hello_3",
                "bool-value": True,
                "datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc),
            },
            float_series={
                "metrics/step": {float(step): float(step) for step in range(10)},
                **{p: {float(step): step / 3.0 for step in range(10)} for p in FLOAT_SERIES_PATHS},
            },
        ),
    ]

    return ensure_project(ProjectData(runs=runs))


@pytest.fixture(scope="module")
def project_non_finite(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    return ensure_project(
        ProjectData(
            runs=[
                RunData(
                    experiment_name="exp_inf_nan_run",
                    run_id="experiments-inf-nan-run",
                    configs={
                        "inf-float": float("inf"),
                        "nan-float": float("nan"),
                        "neg-inf-float": float("-inf"),
                    },
                    float_series={
                        "series-containing-inf": {
                            float(i): v
                            for i, v in enumerate(
                                [float("inf"), 1, float("-inf"), 3, 4, float("inf"), 6, float("-inf"), 8, 9]
                            )
                        },
                        "series-ending-with-inf": {
                            float(i): v for i, v in enumerate([0, 1, 2, 3, 4, 5, 6, 7, 8, float("inf")])
                        },
                        "series-containing-nan": {
                            float(i): v
                            for i, v in enumerate(
                                [float("nan"), 1, float("nan"), 3, 4, float("nan"), 6, float("nan"), 8, 9]
                            )
                        },
                        "series-ending-with-nan": {
                            float(i): v for i, v in enumerate([0, 1, 2, 3, 4, 5, 6, 7, 8, float("nan")])
                        },
                    },
                ),
            ]
        )
    )


@pytest.mark.parametrize("sort_direction", ["asc", "desc"])
def test__fetch_experiments_table(project, sort_direction):
    df = fetch_experiments_table(
        experiments=EXPERIMENT_NAMES,
        sort_by=Attribute("sys/name", type="string"),
        sort_direction=sort_direction,
        project=project.project_identifier,
    )

    expected = pd.DataFrame(
        {
            "experiment": EXPERIMENT_NAMES if sort_direction == "asc" else EXPERIMENT_NAMES[::-1],
        }
    ).set_index("experiment", drop=True)
    expected.columns.name = "attribute"
    assert len(df) == 3
    pd.testing.assert_frame_equal(df, expected)


def test__fetch_experiments_table_empty_attribute_list(project):
    df = fetch_experiments_table(
        project=project.project_identifier,
        experiments=EXPERIMENT_NAMES,
        attributes=[],
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
    )

    expected = pd.DataFrame({"experiment": EXPERIMENT_NAMES}).set_index("experiment", drop=True)
    expected.columns.name = "attribute"
    assert len(df) == 3
    pd.testing.assert_frame_equal(df, expected)


@pytest.mark.parametrize(
    "arg_experiments",
    [
        # Regular expressions:
        "test_alpha_1|test_alpha_2|test_alpha_3",
        Filter.name("test_alpha_1 | test_alpha_2 | test_alpha_3"),
        # ERS:
        "test_alpha_1 | test_alpha_2 | test_alpha_3",
        Filter.name("test_alpha_1 | test_alpha_2 | test_alpha_3"),
        # Explicit list:
        ["test_alpha_1", "test_alpha_2", "test_alpha_3"],
        # Combined OR filters:
        (Filter.name("test_alpha_1") | Filter.name("test_alpha_2") | Filter.name("test_alpha_3")),
    ],
)
@pytest.mark.parametrize(
    "arg_attributes",
    [
        "int-value|float-value|metrics/step",
        ["int-value", "float-value", "metrics/step"],
        AttributeFilter.any(
            AttributeFilter("int-value", type=["int"]),
            AttributeFilter("float-value", type=["float"]),
            AttributeFilter("metrics/step", type=["float_series"]),
        ),
        AttributeFilter("int-value", type=["int"])
        | AttributeFilter("float-value", type=["float"])
        | AttributeFilter("metrics/step", type=["float_series"]),
    ],
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test__fetch_experiments_table_with_attributes_filter(
    project,
    arg_experiments,
    arg_attributes,
    type_suffix_in_column_names,
):
    df = fetch_experiments_table(
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        experiments=arg_experiments,
        attributes=arg_attributes,
        type_suffix_in_column_names=type_suffix_in_column_names,
        project=project.project_identifier,
    )

    def suffix(name):
        return f":{name}" if type_suffix_in_column_names else ""

    expected = pd.DataFrame(
        {
            "experiment": EXPERIMENT_NAMES,
            f"int-value{suffix('int')}": [1, 2, 3],
            f"float-value{suffix('float')}": [1.0, 2.0, 3.0],
            f"metrics/step{suffix('float_series')}": [9.0, 9.0, 9.0],
        }
    ).set_index("experiment", drop=True)
    expected.columns.name = "attribute"
    assert df.shape == (3, 3)
    pd.testing.assert_frame_equal(df[expected.columns], expected)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter("metrics/step", type=["float_series"])
        | AttributeFilter(FLOAT_SERIES_PATHS[0], type=["float_series"])
        | AttributeFilter(FLOAT_SERIES_PATHS[1], type=["float_series"]),
    ],
)
def test__fetch_experiments_table_with_attributes_filter_for_metrics(project, attr_filter, type_suffix_in_column_names):
    df = fetch_experiments_table(
        project=project.project_identifier,
        experiments=EXPERIMENT_NAMES,
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    suffix = ":float_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": EXPERIMENT_NAMES,
            "metrics/step" + suffix: [9.0, 9.0, 9.0],
            FLOAT_SERIES_PATHS[0] + suffix: [9.0, 4.5, 3.0],
            FLOAT_SERIES_PATHS[1] + suffix: [4.5, 4.5, 3.0],
        }
    ).set_index("experiment", drop=True)
    expected.columns.name = "attribute"
    assert df.shape == (3, 3)
    pd.testing.assert_frame_equal(df[expected.columns], expected)
    assert df[expected.columns].columns.equals(expected.columns)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter("metrics/string-series-value_0", type=["string_series"])
        | AttributeFilter("metrics/string-series-value_1", type=["string_series"])
    ],
)
def test__fetch_experiments_table_with_attributes_filter_for_string_series(
    project, attr_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        project=project.project_identifier,
        experiments=["test_alpha_1", "test_alpha_2"],
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    suffix = ":string_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": ["test_alpha_1", "test_alpha_2"],
            "metrics/string-series-value_0" + suffix: ["string-9", "string-9"],
            "metrics/string-series-value_1" + suffix: ["string-9", "string-9"],
        }
    ).set_index("experiment", drop=True)
    expected.columns.name = "attribute"
    assert df.shape == expected.shape
    pd.testing.assert_frame_equal(df[expected.columns], expected)
    assert df[expected.columns].columns.equals(expected.columns)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter("metrics/histogram-series-value_0", type=["histogram_series"])
        | AttributeFilter("metrics/histogram-series-value_1", type=["histogram_series"])
    ],
)
def test__fetch_experiments_table_with_attributes_filter_for_histogram_series(
    project, attr_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        project=project.project_identifier,
        experiments=Filter.name(["test_alpha_1", "test_alpha_2"]),
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    suffix = ":histogram_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": ["test_alpha_1", "test_alpha_2"],
            "metrics/histogram-series-value_0"
            + suffix: [
                OHistogram(type="COUNTING", edges=[n + 9.0 for n in range(4)], values=[n * 9.0 for n in range(3)]),
                OHistogram(type="COUNTING", edges=[n + 9.0 for n in range(4)], values=[n * 9.0 for n in range(3)]),
            ],
            "metrics/histogram-series-value_1"
            + suffix: [
                OHistogram(type="COUNTING", edges=[n + 9.0 for n in range(4)], values=[n * 9.0 for n in range(3)]),
                OHistogram(type="COUNTING", edges=[n + 9.0 for n in range(4)], values=[n * 9.0 for n in range(3)]),
            ],
        }
    ).set_index("experiment", drop=True)
    expected.columns.name = "attribute"
    assert df.shape == expected.shape
    pd.testing.assert_frame_equal(df[expected.columns], expected)
    assert df[expected.columns].columns.equals(expected.columns)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter("files/file-series-value_0", type=["file_series"])
        | AttributeFilter("files/file-series-value_1", type=["file_series"])
    ],
)
def test__fetch_experiments_table_with_attributes_filter_for_file_series(
    project, attr_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        project=project.project_identifier,
        experiments=Filter.name("test_alpha_1"),
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    suffix = ":file_series" if type_suffix_in_column_names else ""

    expected = pd.DataFrame(
        {
            "experiment": ["test_alpha_1"],
            "files/file-series-value_0"
            + suffix: [
                File(
                    project_identifier=project.project_identifier,
                    experiment_name="test_alpha_1",
                    run_id=None,
                    attribute_path="files/file-series-value_0",
                    step=2.0,
                    path="dst/file/file-series-value_0",
                    size_bytes=6,
                    mime_type="application/octet-stream",
                )
            ],
            "files/file-series-value_1"
            + suffix: [
                File(
                    project_identifier=project.project_identifier,
                    experiment_name="test_alpha_1",
                    run_id=None,
                    attribute_path="files/file-series-value_1",
                    step=2.0,
                    path="dst/file/file-series-value_1",
                    size_bytes=6,
                    mime_type="application/octet-stream",
                )
            ],
        }
    ).set_index("experiment", drop=True)
    expected.columns.name = "attribute"
    assert df.shape == expected.shape
    pd.testing.assert_frame_equal(df[expected.columns], expected)
    assert df[expected.columns].columns.equals(expected.columns)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter(name=f"metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]}"),
        AttributeFilter(name=f"metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]} & !.*value_[5-9].*"),
        f"metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]}",
    ],
)
def test__fetch_experiments_table_with_attributes_regex_filter_for_metrics(
    project, attr_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        project=project.project_identifier,
        experiments=EXPERIMENT_NAMES,
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    suffix = ":float_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": EXPERIMENT_NAMES,
            "metrics/step" + suffix: [9.0, 9.0, 9.0],
            FLOAT_SERIES_PATHS[0] + suffix: [9.0, 4.5, 3.0],
            FLOAT_SERIES_PATHS[1] + suffix: [4.5, 4.5, 3.0],
        }
    ).set_index("experiment", drop=True)
    expected.columns.name = "attribute"
    assert df.shape == (3, 3)
    pd.testing.assert_frame_equal(df[expected.columns], expected)
    assert df[expected.columns].columns.equals(expected.columns)


def test__fetch_experiments_table_nan_inf(project_non_finite):
    df = fetch_experiments_table(
        project=project_non_finite.project_identifier,
        experiments=["exp_inf_nan_run"],
        attributes=[
            "inf-float",
            "nan-float",
            "neg-inf-float",
            "series-containing-inf",
            "series-ending-with-inf",
            "series-containing-nan",
            "series-ending-with-nan",
        ],
    )

    expected = pd.DataFrame(
        {
            "experiment": ["exp_inf_nan_run"],
            "inf-float": [float("inf")],
            "nan-float": [float("nan")],
            "neg-inf-float": [float("-inf")],
            "series-containing-inf": [9.0],
            "series-ending-with-inf": [float("inf")],
            "series-containing-nan": [9.0],
            "series-ending-with-nan": [float("nan")],
        }
    ).set_index("experiment", drop=True)
    expected.columns.name = "attribute"
    assert df.shape == (1, 7)
    pd.testing.assert_frame_equal(df[expected.columns], expected)


@pytest.mark.parametrize(
    "arg_experiments, expected_subset",
    [
        (None, EXPERIMENT_NAMES),
        (".*", EXPERIMENT_NAMES),
        ("", EXPERIMENT_NAMES),
        ("test_alpha", EXPERIMENT_NAMES),
        (Filter.matches(Attribute("sys/name", type="string"), ".*"), EXPERIMENT_NAMES),
    ],
)
def test_list_experiments_with_regex_and_filters_matching_all(project, arg_experiments, expected_subset):
    """We need to check if expected names are a subset of all names returned, as
    the test data could contain other experiments"""
    names = list_experiments(
        project=project.project_identifier,
        experiments=arg_experiments,
    )
    assert set(expected_subset) <= set(names)


@pytest.mark.parametrize(
    "regex, expected",
    [
        ("alpha_1", ["test_alpha_1"]),
        ("alpha_[1,2]", ["test_alpha_1", "test_alpha_2"]),
        ("not-found", []),
        ("experiment_999", []),
    ],
)
def test_list_experiments_with_regex_matching_some(project, regex, expected):
    """This check is more strict than test_list_experiments_with_regex_matching_all, as we are able
    to predict the exact output because of the filtering applied"""
    names = list_experiments(
        project=project.project_identifier,
        experiments=regex,
    )
    assert len(names) == len(expected)
    assert set(names) == set(expected)


@pytest.mark.parametrize(
    "arg_experiments, expected",
    [
        (Filter.eq(Attribute("sys/name", type="string"), ""), []),
        (EXPERIMENT_NAMES, EXPERIMENT_NAMES),
        (
            Filter.matches(Attribute("sys/name", type="string"), "alpha.* & _2"),
            ["test_alpha_2"],
        ),
        (
            Filter.matches(Attribute("sys/name", type="string"), "!alpha_2 & !alpha_3")
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha_[0-9]"),
            ["test_alpha_1"],
        ),
        (Filter.eq(Attribute("str-value", type="string"), "hello_123"), []),
        (
            Filter.eq(Attribute("str-value", type="string"), "hello_1")
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha_[0-9]"),
            ["test_alpha_1"],
        ),
        (
            (
                Filter.eq(Attribute("str-value", type="string"), "hello_1")
                | Filter.eq(Attribute("str-value", type="string"), "hello_2")
            )
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha_[0-9]"),
            ["test_alpha_1", "test_alpha_2"],
        ),
        (
            Filter.ne(Attribute("str-value", type="string"), "hello_1")
            & Filter.eq(Attribute("str-value", type="string"), "hello_2")
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha_[0-9]"),
            ["test_alpha_2"],
        ),
        (Filter.eq(Attribute("int-value", type="int"), 12345), []),
        (
            Filter.eq(Attribute("int-value", type="int"), 2)
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha_[0-9]"),
            ["test_alpha_2"],
        ),
        (
            Filter.eq(Attribute("int-value", type="int"), 2)
            | Filter.eq(Attribute("int-value", type="int"), 3)
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha_[0-9]"),
            ["test_alpha_2", "test_alpha_3"],
        ),
        (Filter.eq(Attribute("float-value", type="float"), 1.2345), []),
        (
            Filter.eq(Attribute("float-value", type="float"), 3.0)
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha_[0-9]"),
            ["test_alpha_3"],
        ),
        (
            Filter.eq(Attribute("bool-value", type="bool"), False)
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha_[0-9]"),
            ["test_alpha_2"],
        ),
        (
            Filter.eq(Attribute("bool-value", type="bool"), True)
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha_[0-9]"),
            ["test_alpha_1", "test_alpha_3"],
        ),
        (Filter.gt(Attribute("datetime-value", type="datetime"), datetime.now()), []),
        (
            Filter.contains_all(Attribute("string_set-value", type="string_set"), "no-such-string"),
            [],
        ),
        (
            Filter.contains_all(Attribute("string_set-value", type="string_set"), ["string-1-0", "string-1-1"])
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha"),
            ["test_alpha_2"],
        ),
        (
            (
                Filter.contains_all(Attribute("string_set-value", type="string_set"), "string-1-0")
                | Filter.contains_all(Attribute("string_set-value", type="string_set"), "string-0-0")
            )
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha"),
            ["test_alpha_1", "test_alpha_2"],
        ),
        (
            Filter.contains_none(
                Attribute("string_set-value", type="string_set"),
                ["string-1-0", "string-2-0", "string-3-0"],
            )
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha_[0-9]"),
            ["test_alpha_1", "test_alpha_3"],
        ),
        (
            Filter.contains_none(
                Attribute("string_set-value", type="string_set"),
                ["string-1-0", "string-2-0", "string-3-0"],
            )
            & Filter.contains_all(Attribute("string_set-value", type="string_set"), "string-0-0")
            & Filter.matches(Attribute("sys/name", type="string"), "test_alpha_[0-9]"),
            ["test_alpha_1"],
        ),
        (
            Filter.eq(Attribute("sys/name", type="string"), "test_alpha_1"),
            ["test_alpha_1"],
        ),
        (
            Filter.exists(Attribute("files/file-value.txt", type="file")),
            ["test_alpha_1"],
        ),
        (
            Filter.exists(Attribute(FLOAT_SERIES_PATHS[0], type="float_series")),
            EXPERIMENT_NAMES,
        ),
        (
            Filter.exists(Attribute(STRING_SERIES_PATHS[0], type="string_series")),
            ["test_alpha_1", "test_alpha_2"],
        ),
        # ( # todo: histogram_series not supported yet in the nql
        #     Filter.exists(Attribute(HISTOGRAM_SERIES_PATHS[0], type="histogram_series")),
        #     EXPERIMENT_NAMES,
        # ),
        # ( # todo: histogram_series not supported yet in the nql
        #     Filter.exists(Attribute(FILE_SERIES_PATHS[0], type="file_series")),
        #     EXPERIMENT_NAMES,
        # ),
    ],
)
def test_list_experiments_with_filter_matching_some(project, arg_experiments, expected):
    names = list_experiments(
        project=project.project_identifier,
        experiments=arg_experiments,
    )
    assert set(names) == set(expected)
    assert len(names) == len(expected)


def test_empty_experiment_list(project):
    """Test the behavior of filtering by experiments=[]

    In alpha, this used to return all experiments, but in v1 it will raises an error
    """

    with pytest.raises(ValueError, match="got an empty list"):
        list_experiments(project=project.project_identifier, experiments=[])
