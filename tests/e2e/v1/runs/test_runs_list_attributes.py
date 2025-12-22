from datetime import (
    datetime,
    timezone,
)

import pytest

import neptune_query.runs as runs
from neptune_query.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    ProjectData,
    RunData,
)


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    runs_data = [
        RunData(
            experiment_name="experiment_a",
            run_id="experiment_a_run1",
            configs={
                "int-value": 1,
                "float-value": 1.0,
                "str-value": "a1",
                "bool-value": True,
                "datetime-value": datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
            },
        ),
        RunData(
            experiment_name="experiment_b",
            run_id="experiment_b_run1",
            configs={
                "int-value": 3,
                "float-value": 3.0,
                "str-value": "b1",
                "bool-value": True,
                "datetime-value": datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
            },
            float_series={
                "foo0": {1.0: 1.1},
                "unique1/0": {1.0: 1.3},
            },
        ),
        RunData(
            experiment_name="experiment_b",
            run_id="experiment_b_run2",
            configs={
                "int-value": 4,
                "float-value": 4.0,
                "str-value": "b2",
                "bool-value": False,
            },
            float_series={
                "unique2/0": {1.0: 1.7},
            },
        ),
    ]

    return ensure_project(ProjectData(runs=runs_data))


@pytest.mark.parametrize(
    "runs_filter, expected_attributes",
    [
        (
            ".*",
            ["bool-value", "datetime-value", "float-value", "foo0", "int-value", "str-value", "unique1/0", "unique2/0"],
        ),
        (
            None,
            ["bool-value", "datetime-value", "float-value", "foo0", "int-value", "str-value", "unique1/0", "unique2/0"],
        ),
        (
            Filter.name(["experiment_a", "experiment_b"]),
            ["bool-value", "datetime-value", "float-value", "foo0", "int-value", "str-value", "unique1/0", "unique2/0"],
        ),
        (
            ["experiment_a_run1", "experiment_a_run2", "experiment_b_run1", "experiment_b_run2"],
            ["bool-value", "datetime-value", "float-value", "foo0", "int-value", "str-value", "unique1/0", "unique2/0"],
        ),
        (
            "experiment_a.*",
            ["bool-value", "datetime-value", "float-value", "int-value", "str-value"],
        ),
        (
            Filter.name(["experiment_a"]),
            ["bool-value", "datetime-value", "float-value", "int-value", "str-value"],
        ),
        (
            Filter.eq(Attribute(name="bool-value", type="bool"), True),
            ["bool-value", "datetime-value", "float-value", "foo0", "int-value", "str-value", "unique1/0"],
        ),
    ],
)
def test_list_attributes(project: IngestedProjectData, runs_filter, expected_attributes):
    attributes = runs.list_attributes(
        project=project.project_identifier,
        runs=runs_filter,
        attributes=None,
    )

    assert _filter_out_sys(attributes) == expected_attributes


@pytest.mark.parametrize(
    "attribute_filter, expected_attributes",
    [
        # DateTime attributes
        (AttributeFilter(name="datetime-value", type=["datetime"]), ["datetime-value"]),
        # Numeric series
        (AttributeFilter(name="unique1/0", type=["float_series"]), ["unique1/0"]),
        (AttributeFilter(name="foo0", type=["float_series"]), ["foo0"]),
        # Primitive types
        (AttributeFilter(type=["int"]), ["int-value"]),
        (AttributeFilter(type=["float"]), ["float-value"]),
        (AttributeFilter(type=["string"]), ["str-value"]),
        (AttributeFilter(type=["bool"]), ["bool-value"]),
        # Multiple types
        (AttributeFilter(type=["float", "int"]), ["float-value", "int-value"]),
        # Name patterns
        (AttributeFilter(name="unique.*"), ["unique1/0", "unique2/0"]),
        (AttributeFilter(name="foo.*"), ["foo0"]),
        # Combined filters
        (AttributeFilter(name=".*value.*", type=["float"]), ["float-value"]),
        (AttributeFilter(name=".*value.*", type=["int"]), ["int-value"]),
        (
            AttributeFilter(name=".*value.*", type=["float"]) | AttributeFilter(name=".*value.*", type=["int"]),
            ["float-value", "int-value"],
        ),
    ],
)
def test_list_attributes_with_attribute_filter(project: IngestedProjectData, attribute_filter, expected_attributes):
    attributes = runs.list_attributes(
        project=project.project_identifier,
        runs=r"^experiment_b_run1$|^experiment_b_run2$",
        attributes=attribute_filter,
    )

    assert _filter_out_sys(attributes) == expected_attributes


def _filter_out_sys(attributes):
    return [attr for attr in attributes if not attr.startswith("sys/")]
