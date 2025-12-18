from datetime import datetime

import pytest

from neptune_query.internal.composition.attributes import (
    fetch_attribute_definitions,
    fetch_attribute_values,
)
from neptune_query.internal.filters import (
    _AttributeFilter,
    _AttributeNameFilter,
    _Filter,
)
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.retrieval import search
from neptune_query.internal.retrieval.attribute_values import AttributeValue
from tests.e2e.conftest import (
    EnsureProjectFunction,
    extract_pages,
)
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    ProjectData,
    RunData,
)


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    return ensure_project(
        ProjectData(
            runs=[
                RunData(
                    experiment_name="composition-attributes-experiment",
                    run_id="composition-attributes-run",
                    configs={
                        "int-value": 10,
                        "float-value": 0.5,
                        "str-value": "hello",
                        "bool-value": True,
                        "datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0),
                        "int_value_a": 123,
                        "int_value_b": 456,
                        "float_value_a": 1.5,
                        "float_value_b": 2.5,
                    },
                    float_series={
                        "float-series-value": {float(step * 0.5): float(step**2) for step in range(10)},
                    },
                    string_sets={"sys/tags": ["string-set-item"]},
                )
            ],
        )
    )


@pytest.fixture(scope="module")
def experiment_identifier(client, project) -> RunIdentifier:
    project_identifier = ProjectIdentifier(project.project_identifier)
    experiment_name = project.ingested_runs[0].experiment_name

    sys_ids: list[SysId] = []
    for page in search.fetch_experiment_sys_ids(
        client=client,
        project_identifier=project_identifier,
        filter_=_Filter.name_eq(experiment_name),
    ):
        sys_ids.extend(page.items)

    if len(sys_ids) != 1:
        raise RuntimeError(f"Expected to fetch exactly one sys_id for {experiment_name}, got {sys_ids}")

    return RunIdentifier(project_identifier, SysId(sys_ids[0]))


def test_fetch_attribute_definitions_filter_or(client, executor, project, experiment_identifier):
    # given
    project_identifier = ProjectIdentifier(project.project_identifier)

    attribute_filter_1 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[r"^.*_value_a$"])],
        type_in=["int"],
    )
    attribute_filter_2 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[r"^.*_value_b$"])],
        type_in=["float"],
    )

    #  when
    attribute_filter = _AttributeFilter.any([attribute_filter_1, attribute_filter_2])
    attributes = extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=executor,
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("int_value_a", "int"),
            AttributeDefinition("float_value_b", "float"),
        ],
    )


@pytest.mark.parametrize(
    "make_attribute_filter",
    [
        lambda a, b, c: _AttributeFilter.any([a, b, c]),
        lambda a, b, c: _AttributeFilter.any([a, _AttributeFilter.any([b, c])]),
    ],
)
def test_fetch_attribute_definitions_filter_triple_or(
    client, executor, project, experiment_identifier, make_attribute_filter
):
    # given
    project_identifier = ProjectIdentifier(project.project_identifier)

    attribute_filter_1 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[r"^.*_value_a$"])],
        type_in=["int"],
    )
    attribute_filter_2 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[r"^.*_value_b$"])],
        type_in=["float"],
    )
    attribute_filter_3 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[r"^.*_value_b$"])],
        type_in=["int"],
    )
    attribute_filter = make_attribute_filter(attribute_filter_1, attribute_filter_2, attribute_filter_3)

    #  when
    attributes = extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=executor,
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("int_value_a", "int"),
            AttributeDefinition("int_value_b", "int"),
            AttributeDefinition("float_value_b", "float"),
        ],
    )


def test_fetch_attribute_definitions_paging_executor(client, executor, project, experiment_identifier):
    # given
    project_identifier = ProjectIdentifier(project.project_identifier)

    #  when
    attribute_filter = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=["sys/.*_time"])],
        type_in=["datetime"],
    )

    attributes = extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            batch_size=1,
            executor=executor,
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime"),
            AttributeDefinition("sys/modification_time", "datetime"),
            AttributeDefinition("sys/ping_time", "datetime"),
        ],
    )


def test_fetch_attribute_definitions_should_deduplicate_items(client, executor, project, experiment_identifier):
    # given
    project_identifier = ProjectIdentifier(project.project_identifier)

    #  when
    attribute_filter_0 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=["sys/.*_time"])],
        type_in=["datetime"],
    )
    attribute_filter = attribute_filter_0
    for i in range(10):
        attribute_filter = _AttributeFilter.any(
            [
                attribute_filter,
                _AttributeFilter(
                    must_match_any=[_AttributeNameFilter(must_match_regexes=["sys/.*_time"])],
                    type_in=["datetime"],
                ),
            ]
        )

    attributes = extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            batch_size=1,
            executor=executor,
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime"),
            AttributeDefinition("sys/modification_time", "datetime"),
            AttributeDefinition("sys/ping_time", "datetime"),
        ],
    )


def assert_items_equal(a: list[AttributeDefinition], b: list[AttributeDefinition]):
    assert sorted(a, key=lambda d: (d.name, d.type)) == sorted(b, key=lambda d: (d.name, d.type))


def test_fetch_attribute_values_filter_or(client, executor, project, experiment_identifier):
    # given
    project_identifier = ProjectIdentifier(project.project_identifier)

    attribute_filter_1 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[r"^.*-value$"])],
        type_in=["int"],
    )
    attribute_filter_2 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[r"^.*-value$"])],
        type_in=["float"],
    )

    #  when
    attribute_filter = _AttributeFilter.any([attribute_filter_1, attribute_filter_2])
    values = extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=executor,
        )
    )

    # then
    assert len(values) == 2
    assert set(values) == {
        AttributeValue(AttributeDefinition("int-value", "int"), 10, experiment_identifier),
        AttributeValue(AttributeDefinition("float-value", "float"), 0.5, experiment_identifier),
    }


def test_fetch_attribute_values_deduplicate(client, executor, project, experiment_identifier):
    # given
    project_identifier = ProjectIdentifier(project.project_identifier)

    attribute_filter_1 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[r"^.*-value$"])],
        type_in=["int", "float"],
    )
    attribute_filter_2 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[r"^.*-value$"])],
        type_in=["float", "string"],
    )
    attribute_filter_3 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[r"^int-value$"])],
    )

    #  when
    attribute_filter = _AttributeFilter.any([attribute_filter_1, attribute_filter_2, attribute_filter_3])
    values = extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=executor,
        )
    )

    # then
    assert len(values) == 3
    assert set(values) == {
        AttributeValue(AttributeDefinition("int-value", "int"), 10, experiment_identifier),
        AttributeValue(AttributeDefinition("float-value", "float"), 0.5, experiment_identifier),
        AttributeValue(AttributeDefinition("str-value", "string"), "hello", experiment_identifier),
    }
