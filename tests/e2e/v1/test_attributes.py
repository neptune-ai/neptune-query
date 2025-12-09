from datetime import (
    datetime,
    timezone,
)
from typing import Iterable

import pytest

from neptune_query import list_attributes
from neptune_query.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    IngestionHistogram,
    ProjectData,
    RunData,
)

FLOAT_SERIES_PATHS = [f"metrics/float-series-value_{j}" for j in range(5)]
STRING_SERIES_PATHS = [f"metrics/string-series-value_{j}" for j in range(2)]
HISTOGRAM_SERIES_PATHS = [f"metrics/histogram-series-value_{j}" for j in range(3)]

SERIES_PATHS = set(FLOAT_SERIES_PATHS + STRING_SERIES_PATHS + HISTOGRAM_SERIES_PATHS)

EXPERIMENT_NAMES = ["test_alpha_0", "test_alpha_1", "test_alpha_2"]

# Expected attributes for this module’s dataset
ALL_ATTRIBUTE_NAMES = SERIES_PATHS | {
    "bool-value",
    "datetime-value",
    "float-value",
    "int-value",
    "str-value",
    "string_set-value",
    "unique-value-0",
    "unique-value-1",
    "unique-value-2",
    "unique-value-3",
    "unique-value-4",
    "unique-value-5",
    "files/file-value",
    "files/file-value.txt",
    "files/object-does-not-exist",
    "files/file-series-value_0",
    "files/file-series-value_1",
}


def _drop_sys_attr_names(attributes: Iterable[str]) -> list[str]:
    return [attr for attr in attributes if not attr.startswith("sys/")]


@pytest.fixture(scope="module", autouse=True)
def run_with_attributes_autouse():
    # Override autouse ingestion from shared v1 fixtures; this module ingests its own data.
    return None


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    runs: list[RunData] = [
        RunData(
            experiment_name="test_alpha_0",
            configs={
                "int-value": 1,
                "float-value": 1.0,
                "str-value": "hello_1",
                "bool-value": True,
                "datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc),
                "unique-value-0": "unique_0",
                "unique-value-3": "unique_3",
            },
            string_sets={"string_set-value": [f"string-0-{j}" for j in range(3)]},
            float_series={
                "metrics/float-series-value_0": {float(step): step / 1.0 for step in range(10)},
                "metrics/float-series-value_1": {float(step): step / 2.0 for step in range(10)},
                "metrics/float-series-value_2": {float(step): step / 3.0 for step in range(10)},
            },
            string_series={
                p: {float(step): f"string-{int(step)}" for step in range(10)} for p in STRING_SERIES_PATHS
            },
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
                "files/file-value": b"Binary content",
                "files/file-value.txt": b"Text content",
                "files/object-does-not-exist": b"x",
            },
            file_series={
                "files/file-series-value_0": {
                    float(step): f"file-{int(step)}".encode("utf-8") for step in range(3)
                },
                "files/file-series-value_1": {
                    float(step): f"file-{int(step)}".encode("utf-8") for step in range(3)
                },
            }
        ),
        RunData(
            experiment_name="test_alpha_1",
            configs={
                "int-value": 2,
                "float-value": 2.0,
                "str-value": "hello_2",
                "bool-value": False,
                "datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc),
                "unique-value-1": "unique_1",
                "unique-value-4": "unique_4",
            },
            string_sets={"string_set-value": [f"string-1-{j}" for j in range(3)]},
            float_series={
                p: {float(step): step / 2.0 for step in range(10)} for p in FLOAT_SERIES_PATHS
            },
            string_series={
                p: {float(step): f"string-{int(step)}" for step in range(10)} for p in STRING_SERIES_PATHS
            },
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
            experiment_name="test_alpha_2",
            configs={
                "int-value": 3,
                "float-value": 3.0,
                "str-value": "hello_3",
                "bool-value": True,
                "datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc),
                "unique-value-2": "unique_2",
                "unique-value-5": "unique_5",
            },
        ),
    ]

    return ensure_project(ProjectData(runs=runs))


@pytest.mark.parametrize(
    "arg_experiments",
    (
        Filter.name(EXPERIMENT_NAMES),
        EXPERIMENT_NAMES,
        "test_alpha_[0-9]+",
    ),
)
@pytest.mark.parametrize(
    "arg_attributes, expected",
    [
        (".*", ALL_ATTRIBUTE_NAMES),
        ("int-value", {"int-value"}),
        ("metrics/.*", SERIES_PATHS),
        (
            "files/.*",
            {
                "files/file-value",
                "files/file-value.txt",
                "files/object-does-not-exist",
                "files/file-series-value_0",
                "files/file-series-value_1",
            },
        ),
        ("unique-value-[0-9]", {f"unique-value-{i}" for i in range(6)}),
        (AttributeFilter(name=".*"), ALL_ATTRIBUTE_NAMES),
        (
            AttributeFilter(name="metrics/string-series-value_.*", type=["string_series"]),
            set(STRING_SERIES_PATHS),
        ),
        (AttributeFilter(name="!.*"), []),  # ERS NOT
    ],
)
def test_list_attributes_known_in_all_experiments_with_name_filter_excluding_sys(
    arg_experiments, arg_attributes, expected
):
    attributes = _drop_sys_attr_names(
        list_attributes(
            experiments=arg_experiments,
            attributes=arg_attributes,
        )
    )
    assert set(attributes) == set(expected)
    assert len(attributes) == len(expected)


@pytest.mark.parametrize(
    "name_filter",
    (
        None,
        "",
        ".*",
        AttributeFilter(name=".*"),
        AttributeFilter(),
    ),
)
def test_list_attributes_all_names_from_all_experiments_excluding_sys(name_filter):
    attributes = _drop_sys_attr_names(list_attributes(experiments=Filter.name(EXPERIMENT_NAMES), attributes=name_filter))
    assert set(attributes) == set(ALL_ATTRIBUTE_NAMES)
    assert len(attributes) == len(ALL_ATTRIBUTE_NAMES)


@pytest.mark.parametrize(
    "filter_",
    (
        "unknown",
        ".*unknown.*",
        AttributeFilter(name="unknown"),
    ),
)
def test_list_attributes_unknown_name(filter_):
    attributes = list_attributes(attributes=filter_)
    assert not attributes


@pytest.mark.parametrize(
    "arg_experiments, arg_attributes, expected",
    [
        (Filter.name(EXPERIMENT_NAMES), "unique-value-[0-2]", {f"unique-value-{i}" for i in range(3)}),
        (
            "test_alpha_(0|2)",
            "unique-value-.*",
            {"unique-value-0", "unique-value-2", "unique-value-3", "unique-value-5"},
        ),
        (
            Filter.contains_all(Attribute("string_set-value", type="string_set"), "string-0-0"),
            "unique-value-.*",
            {"unique-value-0", "unique-value-3"},
        ),
        (
            Filter.contains_none(Attribute("string_set-value", type="string_set"), ["string-0-0", "string-1-0"]),
            "unique-value-.*",
            {"unique-value-2", "unique-value-5"},
        ),
        (
            EXPERIMENT_NAMES,
            ["int-value", "float-value"],
            {"int-value", "float-value"},
        ),
        (
            Filter.lt(Attribute("int-value", type="int"), 4) & Filter.name(EXPERIMENT_NAMES),
            "unique-value",
            {f"unique-value-{i}" for i in range(6)},
        ),
        (
            Filter.eq(Attribute("bool-value", type="bool"), False) & Filter.name(EXPERIMENT_NAMES),
            "unique-value",
            {"unique-value-1", "unique-value-4"},
        ),
    ],
)
def test_list_attributes_depending_on_values_in_experiments(arg_experiments, arg_attributes, expected):
    attributes = list_attributes(
        experiments=arg_experiments,
        attributes=arg_attributes,
    )
    assert set(attributes) == set(expected)
    assert len(attributes) == len(expected)


@pytest.mark.parametrize(
    "attribute_filter, expected",
    [
        (
            r"sys/(name|id)",
            {"sys/name", "sys/id"},
        ),
        (r"sys/.*id$", {"sys/custom_run_id", "sys/id", "sys/diagnostics/project_uuid", "sys/diagnostics/run_uuid"}),
        (AttributeFilter(name="sys/(name|id)"), {"sys/name", "sys/id"}),
        (AttributeFilter(name="sys/name | sys/id"), {"sys/name", "sys/id"}),  # ERS
    ],
)
def test_list_attributes_sys_attrs(attribute_filter, expected):
    """A separate test for sys attributes, as we ignore them in tests above for simplicity."""

    attributes = list_attributes(attributes=attribute_filter)
    assert set(attributes) == expected
    assert len(attributes) == len(expected)
