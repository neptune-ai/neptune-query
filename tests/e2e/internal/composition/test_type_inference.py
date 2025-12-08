from datetime import datetime

import pytest

from neptune_query.exceptions import AttributeTypeInferenceError
from neptune_query.internal.composition.type_inference import (
    infer_attribute_types_in_filter,
    infer_attribute_types_in_sort_by,
)
from neptune_query.internal.filters import (
    _Attribute,
    _Filter,
)
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    ProjectData,
    RunData,
)

DATETIME_VALUE = datetime(2025, 1, 1, 0, 0, 0, 0)


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    return ensure_project(
        ProjectData(
            project_name_base="composition-type-inference-project",
            runs=[
                RunData(
                    experiment_name_base="composition-type-inference-a",
                    run_id_base="composition-type-inference-run-a",
                    configs={
                        "int-value": 10,
                        "float-value": 0.5,
                        "str-value": "hello",
                        "bool-value": True,
                        "datetime-value": DATETIME_VALUE,
                        "conflicting-type-int-str-value": 10,
                        "conflicting-type-int-float-value": 3,
                    },
                    float_series={"float-series-value": {float(step * 0.5): float(step**2) for step in range(10)}},
                ),
                RunData(
                    experiment_name_base="composition-type-inference-b",
                    run_id_base="composition-type-inference-run-b",
                    configs={
                        "int-value": 10,
                        "float-value": 0.5,
                        "str-value": "hello",
                        "bool-value": True,
                        "datetime-value": DATETIME_VALUE,
                        "conflicting-type-int-str-value": "hello",
                        "conflicting-type-int-float-value": 0.3,
                    },
                ),
            ],
        )
    )


def test_infer_attribute_types_in_filter_no_filter(client, executor, project):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_filter(client, project_identifier, None, executor)

    # then
    # no exception is raised
    result.raise_if_incomplete()  # doesn't raise


@pytest.mark.parametrize(
    "filter_before, filter_after",
    [
        (_Filter.eq("int-value", 10), _Filter.eq(_Attribute("int-value", type="int"), 10)),
        (_Filter.eq("float-value", 0.5), _Filter.eq(_Attribute("float-value", type="float"), 0.5)),
        (_Filter.eq("str-value", "hello"), _Filter.eq(_Attribute("str-value", type="string"), "hello")),
        (_Filter.eq("bool-value", True), _Filter.eq(_Attribute("bool-value", type="bool"), True)),
        (
            _Filter.eq("datetime-value", DATETIME_VALUE),
            _Filter.eq(_Attribute("datetime-value", type="datetime"), DATETIME_VALUE),
        ),
        (
            _Filter.eq("float-series-value", float(9**2)),
            _Filter.eq(_Attribute("float-series-value", type="float_series"), float(9**2)),
        ),
    ],
)
def test_infer_attribute_types_in_filter_single(client, executor, project, filter_before, filter_after):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_filter(client, project_identifier, filter_before, executor)

    # then
    assert filter_before != filter_after
    assert result.result == filter_after
    result.raise_if_incomplete()  # doesn't raise


@pytest.mark.parametrize(
    "filter_before, filter_after",
    [
        (
            _Filter.eq("sys/name", "n"),
            _Filter.eq(_Attribute("sys/name", type="string"), "n"),
        ),
        (
            _Filter.eq("sys/id", "id"),
            _Filter.eq(_Attribute("sys/id", type="string"), "id"),
        ),
        (
            _Filter.eq("sys/modification_time", DATETIME_VALUE),
            _Filter.eq(_Attribute("sys/modification_time", type="datetime"), DATETIME_VALUE),
        ),
        (
            _Filter.eq("sys/owner", "owner"),
            _Filter.eq(_Attribute("sys/owner", type="string"), "owner"),
        ),
        (
            _Filter.eq("sys/family", "family"),
            _Filter.eq(_Attribute("sys/family", type="string"), "family"),
        ),
        (
            _Filter.eq("sys/custom_run_id", "custom_run_id"),
            _Filter.eq(_Attribute("sys/custom_run_id", type="string"), "custom_run_id"),
        ),
        (
            _Filter.eq("sys/archived", True),
            _Filter.eq(_Attribute("sys/archived", type="bool"), True),
        ),
        (
            _Filter.eq("sys/creation_time", DATETIME_VALUE),
            _Filter.eq(_Attribute("sys/creation_time", type="datetime"), DATETIME_VALUE),
        ),
        (
            _Filter.eq("sys/experiment/name", "experiment_name"),
            _Filter.eq(_Attribute("sys/experiment/name", type="string"), "experiment_name"),
        ),
        (
            _Filter.eq("sys/experiment/running_time_seconds", 10.0),
            _Filter.eq(_Attribute("sys/experiment/running_time_seconds", type="float"), 10.0),
        ),
    ],
)
def test_infer_attribute_types_in_filter_sys(client, executor, project, filter_before, filter_after):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_filter(
        client,
        project_identifier,
        filter_before,
        executor,
    )

    # then
    assert filter_before != filter_after
    assert result.result == filter_after
    result.raise_if_incomplete()  # doesn't raise
    assert all(attr.success_details == "Inferred as a known system attribute" for attr in result.attributes)


@pytest.mark.parametrize(
    "attribute_before, attribute_after",
    [
        (_Attribute("int-value"), _Attribute("int-value", type="int")),
        (_Attribute("float-value"), _Attribute("float-value", type="float")),
        (_Attribute("str-value"), _Attribute("str-value", type="string")),
        (_Attribute("bool-value"), _Attribute("bool-value", type="bool")),
        (_Attribute("datetime-value"), _Attribute("datetime-value", type="datetime")),
        (_Attribute("float-series-value"), _Attribute("float-series-value", type="float_series")),
    ],
)
def test_infer_attribute_types_in_sort_by_single(client, executor, project, attribute_before, attribute_after):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_sort_by(
        client, project_identifier, sort_by=attribute_before, fetch_attribute_definitions_executor=executor
    )

    # then
    assert attribute_before != attribute_after
    assert result.result == attribute_after
    result.raise_if_incomplete()  # doesn't raise


def test_infer_attribute_types_in_filter_missing(client, executor, project):
    # given
    project_identifier = project.project_identifier
    filter_before = _Filter.eq("does-not-exist", 10)

    #  when
    result = infer_attribute_types_in_filter(
        client, project_identifier, filter_=filter_before, fetch_attribute_definitions_executor=executor
    )

    # then
    result.raise_if_incomplete()  # doesn't raise
    assert result.result.attribute.type == "string"


def test_infer_attribute_types_in_sort_by_missing_attribute(client, executor, project):
    # given
    project_identifier = project.project_identifier
    attribute = _Attribute("does-not-exist")

    #  when
    result = infer_attribute_types_in_sort_by(
        client, project_identifier, sort_by=attribute, fetch_attribute_definitions_executor=executor
    )

    # then
    result.raise_if_incomplete()  # doesn't raise
    assert result.result.type == "string"


def test_infer_attribute_types_in_filter_conflicting_types_int_string(client, executor, project):
    # given
    project_identifier = project.project_identifier
    filter_before = _Filter.eq("conflicting-type-int-str-value", 10)

    #  when
    result = infer_attribute_types_in_filter(
        client, project_identifier, filter_=filter_before, fetch_attribute_definitions_executor=executor
    )

    # then
    with pytest.raises(AttributeTypeInferenceError) as exc:
        result.raise_if_incomplete()
    exc.match(
        "Neptune found the attribute name in multiple runs and experiments across the project with conflicting types"
    )


@pytest.mark.skip(
    reason="Backend inconsistently skips one of the two records (int/float). Merge with the test above when fixed"
)
def test_infer_attribute_types_in_filter_conflicting_types_int_float(client, executor, project):
    # given
    project_identifier = project.project_identifier
    filter_before = _Filter.eq("conflicting-type-int-float-value", 0.5)

    #  when
    result = infer_attribute_types_in_filter(
        client, project_identifier, filter_=filter_before, fetch_attribute_definitions_executor=executor
    )

    # then
    with pytest.raises(AttributeTypeInferenceError) as exc:
        result.raise_if_incomplete()
    exc.match(
        "Neptune found the attribute name in multiple runs and experiments across the project with conflicting types"
    )


def test_infer_attribute_types_in_sort_by_conflicting_types_int_string(client, executor, project):
    # given
    project_identifier = project.project_identifier
    attribute_before = _Attribute("conflicting-type-int-str-value")

    #  when
    result = infer_attribute_types_in_sort_by(
        client, project_identifier, sort_by=attribute_before, fetch_attribute_definitions_executor=executor
    )

    # then
    result.raise_if_incomplete()  # doesn't raise
    assert result.result.type == "string"

    with pytest.warns(
        UserWarning,
        match="found the attribute name in multiple runs and experiments across the project with conflicting types",
    ):
        result.emit_warnings()


def test_infer_attribute_types_in_sort_by_conflicting_types_int_float(client, executor, project):
    # given
    project_identifier = project.project_identifier
    attribute_before = _Attribute("conflicting-type-int-float-value")

    #  when
    result = infer_attribute_types_in_sort_by(
        client, project_identifier, sort_by=attribute_before, fetch_attribute_definitions_executor=executor
    )

    # then
    result.raise_if_incomplete()  # doesn't raise
    assert result.result.type == "string"
    with pytest.warns(
        UserWarning,
        match="found the attribute name in multiple runs and experiments across the project with conflicting types",
    ):
        result.emit_warnings()
