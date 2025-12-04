import math
import re
from datetime import (
    datetime,
    timezone,
)

import pytest

from neptune_query.exceptions import NeptuneProjectInaccessible
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
from neptune_query.internal.retrieval.attribute_definitions import fetch_attribute_definitions_single_filter
from neptune_query.internal.retrieval.attribute_types import (
    FloatSeriesAggregations,
    Histogram,
    HistogramSeriesAggregations,
    StringSeriesAggregations,
)
from neptune_query.internal.retrieval.attribute_values import (
    AttributeValue,
    fetch_attribute_values,
)
from tests.e2e.conftest import (
    EnsureProjectFunction,
    extract_pages,
)
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    IngestionFile,
    IngestionHistogram,
    ProjectData,
    RunData,
)

NUMBER_OF_STEPS = 10
FILE_SERIES_NUMBER_OF_STEPS = 3  # less, since files are heavier to ingest


@pytest.fixture(scope="session")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    run_data = RunData(
        experiment_name_base="test_attributes_experiment",
        run_id_base="test_attributes_run",
        configs={
            "int-value": 0,
            "float-value": 0.0,
            "str-value": "hello_0",
            "bool-value": True,
            "datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc),
        },
        # string sets are logged separately because RunData does not support them directly
        files={"files/file-value.txt": IngestionFile(b"Text content", mime_type="text/plain")},
        float_series={
            "metrics/float-series-value_0": {float(step): float(step**2) for step in range(NUMBER_OF_STEPS)},
            "metrics/float-series-value_1": {float(step): float(step**2 + 1) for step in range(NUMBER_OF_STEPS)},
            "metrics/float-series-value_2": {float(step): float(step**2 + 2) for step in range(NUMBER_OF_STEPS)},
            "metrics/float-series-value_3": {float(step): float(step**2 + 3) for step in range(NUMBER_OF_STEPS)},
            "metrics/float-series-value_4": {float(step): float(step**2 + 4) for step in range(NUMBER_OF_STEPS)},
        },
        string_series={
            "metrics/string-series-value_0": {float(step): f"string-0-{step}" for step in range(NUMBER_OF_STEPS)},
            "metrics/string-series-value_1": {float(step): f"string-1-{step}" for step in range(NUMBER_OF_STEPS)},
        },
        histogram_series={
            name: {float(step): value for step, value in enumerate(values)}
            for name, values in {
                "metrics/histogram-series-value_0": [
                    IngestionHistogram(
                        bin_edges=[float(step + offset) for offset in range(6)],
                        counts=[int(step * offset) for offset in range(5)],
                    )
                    for step in range(NUMBER_OF_STEPS)
                ],
                "metrics/histogram-series-value_1": [
                    IngestionHistogram(
                        bin_edges=[float(step + offset + 1) for offset in range(6)],
                        counts=[int((step + 1) * offset) for offset in range(5)],
                    )
                    for step in range(NUMBER_OF_STEPS)
                ],
                "metrics/histogram-series-value_2": [
                    IngestionHistogram(
                        bin_edges=[float(step + offset + 2) for offset in range(6)],
                        counts=[int((step + 2) * offset) for offset in range(5)],
                    )
                    for step in range(NUMBER_OF_STEPS)
                ],
            }.items()
        },
        file_series={
            name: {float(step): IngestionFile(value) for step, value in enumerate(values)}
            for name, values in {
                "files/file-series-value_0": [
                    f"file-0-{step}".encode("utf-8") for step in range(FILE_SERIES_NUMBER_OF_STEPS)
                ],
                "files/file-series-value_1": [
                    f"file-1-{step}".encode("utf-8") for step in range(FILE_SERIES_NUMBER_OF_STEPS)
                ],
            }.items()
        },
        string_sets={"string_set-value": [f"string-0-{j}" for j in range(5)]},
    )

    ingested_project = ensure_project(
        ProjectData(
            project_name_base="project_attributes",
            runs=[run_data],
        )
    )
    return ingested_project


@pytest.fixture(scope="session")
def project_identifier(project) -> ProjectIdentifier:
    return ProjectIdentifier(project.project_identifier)


@pytest.fixture(scope="session")
def experiment_name(project) -> str:
    return project.ingested_runs[0].experiment_name


@pytest.fixture(scope="session")
def experiment_identifier(client, experiment_name, project_identifier) -> RunIdentifier:
    sys_ids: list[SysId] = []
    for page in search.fetch_experiment_sys_ids(
        client=client,
        project_identifier=project_identifier,
        filter_=_Filter.name_eq(experiment_name),
    ):
        sys_ids.extend(page.items)

    if len(sys_ids) != 1:
        raise RuntimeError(f"Expected exactly one sys_id for experiment {experiment_name}, got {sys_ids}")

    return RunIdentifier(project_identifier, SysId(sys_ids[0]))


def test_fetch_attribute_definitions_project_does_not_exist(client, workspace):
    project_identifier = ProjectIdentifier(f"{workspace}/does-not-exist")

    attribute_filter = _AttributeFilter(name_eq="sys/name", type_in=["string"])
    with pytest.raises(NeptuneProjectInaccessible):
        extract_pages(
            fetch_attribute_definitions_single_filter(
                client,
                [project_identifier],
                attribute_filter=attribute_filter,
                run_identifiers=None,
            )
        )


def test_fetch_attribute_definitions_workspace_does_not_exist(client):
    project_identifier = ProjectIdentifier("this-workspace/does-not-exist")

    attribute_filter = _AttributeFilter(name_eq="sys/name", type_in=["string"])
    with pytest.raises(NeptuneProjectInaccessible):
        extract_pages(
            fetch_attribute_definitions_single_filter(
                client,
                [project_identifier],
                attribute_filter=attribute_filter,
                run_identifiers=None,
            )
        )


def test_fetch_attribute_definitions_single_string(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def test_fetch_attribute_definitions_does_not_exist(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(name_eq="does-not-exist", type_in=["string"])
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert attributes == []


def test_fetch_attribute_definitions_two_strings(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(name_eq=["sys/name", "sys/owner"], type_in=["string"])
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/name", "string"),
            AttributeDefinition("sys/owner", "string"),
        ],
    )


@pytest.mark.parametrize(
    "path, type_in",
    [
        ("metrics/float-series-value_0", "float_series"),
        ("metrics/string-series-value_0", "string_series"),
        ("files/file-series-value_0", "file_series"),
        ("metrics/histogram-series-value_0", "histogram_series"),
    ],
)
def test_fetch_attribute_definitions_single_series(client, project_identifier, experiment_identifier, path, type_in):
    #  when
    attribute_filter = _AttributeFilter(name_eq=path, type_in=[type_in])
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert attributes == [AttributeDefinition(path, type_in)]


def test_fetch_attribute_definitions_all_types(client, project_identifier, experiment_identifier):
    # given
    all_attrs = [
        ("int-value", "int"),
        ("float-value", "float"),
        ("str-value", "string"),
        ("bool-value", "bool"),
        ("datetime-value", "datetime"),
        ("metrics/float-series-value_0", "float_series"),
        ("metrics/string-series-value_0", "string_series"),
        ("files/file-series-value_0", "file_series"),
        ("metrics/histogram-series-value_0", "histogram_series"),
        ("files/file-value.txt", "file"),
        ("sys/tags", "string_set"),
    ]

    #  when
    attribute_filter = _AttributeFilter(name_eq=[name for name, _ in all_attrs])
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
        )
    )

    # then
    expected_definitions = [AttributeDefinition(name, type) for name, type in all_attrs]
    assert_items_equal(attributes, expected_definitions)


def test_fetch_attribute_definitions_no_type_in(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(name_eq="sys/name")
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def test_fetch_attribute_definitions_regex_matches_all(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=["sys/.*_time"])],
        type_in=["datetime"],
    )
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
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


def test_fetch_attribute_definitions_regex_matches_none(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(
        must_match_any=[
            _AttributeNameFilter(
                must_match_regexes=["sys/.*_time"],
                must_not_match_regexes=["modification"],
            ),
        ],
        type_in=["datetime"],
    )
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime"),
            AttributeDefinition("sys/ping_time", "datetime"),
        ],
    )


def test_fetch_attribute_definitions_regex_must_match_any_empty(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(type_in=["datetime"], must_match_any=[])
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert_items_equal(
        attributes,
        [],
    )


def test_fetch_attribute_definitions_regex_must_match_any_single(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(
        type_in=["datetime"],
        must_match_any=[
            _AttributeNameFilter(
                must_match_regexes=["sys/.*_time"],
            ),
            _AttributeNameFilter(),
        ],
    )
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
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


def test_fetch_attribute_definitions_regex_must_match_any_multiple(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(
        type_in=["string", "datetime"],
        must_match_any=[
            _AttributeNameFilter(must_match_regexes=["sys/.*_time"], must_not_match_regexes=["modification"]),
            _AttributeNameFilter(
                must_match_regexes=["sys/name"],
            ),
        ],
    )
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/name", "string"),
            AttributeDefinition("sys/creation_time", "datetime"),
            AttributeDefinition("sys/ping_time", "datetime"),
        ],
    )


def test_fetch_attribute_definitions_multiple_projects(client, project_identifier, experiment_identifier):
    # given
    project_identifier_2 = ProjectIdentifier(f"{project_identifier}-does-not-exist")

    #  when
    attribute_filter = _AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier, project_identifier, project_identifier_2],
            [experiment_identifier],
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def test_fetch_attribute_definitions_paging(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=["sys/.*_time"])],
        type_in=["datetime"],
    )
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            batch_size=1,
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


def test_fetch_attribute_definitions_experiment_identifier_none(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            None,
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def test_fetch_attribute_definitions_experiment_identifier_empty(client, project_identifier, experiment_identifier):
    #  when
    attribute_filter = _AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            [],
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert attributes == []


def test_fetch_attribute_values_single_string(client, project_identifier, experiment_identifier, experiment_name):
    #  when
    values = extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition("sys/name", "string")],
        )
    )

    # then
    assert values == [AttributeValue(AttributeDefinition("sys/name", "string"), experiment_name, experiment_identifier)]


def test_fetch_attribute_values_two_strings(client, project_identifier, experiment_identifier):
    #  when
    values: list[AttributeValue] = extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [
                AttributeDefinition("sys/name", "string"),
                AttributeDefinition("sys/owner", "string"),
            ],
        )
    )

    # then
    assert set(value.attribute_definition for value in values) == {
        AttributeDefinition("sys/name", "string"),
        AttributeDefinition("sys/owner", "string"),
    }


def test_fetch_attribute_values_single_float_series_all_aggregations(
    client, project, project_identifier, experiment_identifier
):
    # given
    path = "metrics/float-series-value_0"
    ingested_run = project.ingested_runs[0]

    #  when
    values = extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition(path, "float_series")],
        )
    )

    # then
    data = [value for _, value in sorted(ingested_run.float_series[path].items())]
    average = sum(data) / len(data)
    aggregates = FloatSeriesAggregations(
        last=data[-1],
        min=min(data),
        max=max(data),
        average=average,
        variance=sum((value - average) ** 2 for value in data) / len(data),
    )
    assert len(values) == 1
    assert values[0].attribute_definition == AttributeDefinition(path, "float_series")
    assert values[0].run_identifier == experiment_identifier
    assert values[0].value.last == aggregates.last
    assert values[0].value.min == aggregates.min
    assert values[0].value.max == aggregates.max
    assert values[0].value.average == aggregates.average
    assert math.isclose(values[0].value.variance, aggregates.variance, rel_tol=1e-6)


def test_fetch_attribute_values_single_string_series_all_aggregations(
    client, project, project_identifier, experiment_identifier
):
    # given
    path = "metrics/string-series-value_0"
    ingested_run = project.ingested_runs[0]

    #  when
    values = extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition(path, "string_series")],
        )
    )

    # then
    data = [value for _, value in sorted(ingested_run.string_series[path].items())]
    aggregates = StringSeriesAggregations(
        last=data[-1],
        last_step=NUMBER_OF_STEPS - 1,
    )
    assert values == [AttributeValue(AttributeDefinition(path, "string_series"), aggregates, experiment_identifier)]


def test_fetch_attribute_values_single_file_series_all_aggregations(client, project_identifier, experiment_identifier):
    # given
    path = "files/file-series-value_0"
    attribute_definition = AttributeDefinition(path, "file_series")

    #  when
    values = extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [attribute_definition],
        )
    )

    # then
    assert len(values) == 1
    value = values[0]
    assert value.attribute_definition == attribute_definition
    aggregate = value.value
    assert aggregate.last_step == FILE_SERIES_NUMBER_OF_STEPS - 1
    last = aggregate.last
    assert re.search(rf".*{path.replace('/', '_')}.*", last.path)
    assert last.size_bytes == 8
    assert last.mime_type == "application/octet-stream"


def test_fetch_attribute_values_single_histogram_series_all_aggregations(
    client, project, project_identifier, experiment_identifier
):
    # given
    path = "metrics/histogram-series-value_0"
    ingested_run = project.ingested_runs[0]

    #  when
    values = extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition(path, "histogram_series")],
        )
    )

    # then
    histograms = ingested_run.histogram_series[path]
    last_step = max(histograms)
    last_value = Histogram(type="COUNTING", edges=histograms[last_step].bin_edges, values=histograms[last_step].counts)
    aggregates = HistogramSeriesAggregations(last=last_value, last_step=last_step)
    assert values == [AttributeValue(AttributeDefinition(path, "histogram_series"), aggregates, experiment_identifier)]


def test_fetch_attribute_values_all_types(client, project, project_identifier, experiment_identifier):
    # given
    configs = project.ingested_runs[0].configs

    all_values = [
        AttributeValue(AttributeDefinition("int-value", "int"), configs["int-value"], experiment_identifier),
        AttributeValue(AttributeDefinition("float-value", "float"), configs["float-value"], experiment_identifier),
        AttributeValue(AttributeDefinition("str-value", "string"), configs["str-value"], experiment_identifier),
        AttributeValue(AttributeDefinition("bool-value", "bool"), configs["bool-value"], experiment_identifier),
        AttributeValue(
            AttributeDefinition("datetime-value", "datetime"),
            configs["datetime-value"],
            experiment_identifier,
        ),
        AttributeValue(
            AttributeDefinition("string_set-value", "string_set"),
            {f"string-0-{j}" for j in range(5)},
            experiment_identifier,
        ),
    ]

    #  when
    values = extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [value.attribute_definition for value in all_values],
        )
    )

    # then
    assert len(values) == len(all_values)
    for expected in all_values:
        value = next(value for value in values if value.attribute_definition == expected.attribute_definition)
        assert value == expected


def test_fetch_attribute_values_file(client, project_identifier, experiment_identifier):
    # given
    attribute_definition = AttributeDefinition("files/file-value.txt", "file")

    #  when
    values = extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [attribute_definition],
        )
    )

    # then
    assert len(values) == 1
    value = values[0]
    assert value.attribute_definition == attribute_definition
    assert re.search(r".*files_file-value_txt.*", value.value.path)
    assert value.value.size_bytes == 12
    assert value.value.mime_type == "text/plain"


def test_fetch_attribute_values_paging(client, project_identifier, experiment_identifier):
    #  when
    values = extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [
                AttributeDefinition("sys/creation_time", "datetime"),
                AttributeDefinition("sys/modification_time", "datetime"),
                AttributeDefinition("sys/ping_time", "datetime"),
            ],
            batch_size=1,
        )
    )

    # then
    assert set(value.attribute_definition for value in values) == {
        AttributeDefinition("sys/creation_time", "datetime"),
        AttributeDefinition("sys/modification_time", "datetime"),
        AttributeDefinition("sys/ping_time", "datetime"),
    }


def assert_items_equal(a: list[AttributeDefinition], b: list[AttributeDefinition]):
    assert sorted(a, key=lambda d: (d.name, d.type)) == sorted(b, key=lambda d: (d.name, d.type))


def test_fetch_attribute_definitions_experiment_large_number_experiment_identifiers(
    client, project_identifier, experiment_identifier
):
    # given
    experiment_identifiers = [experiment_identifier] + _generate_experiment_identifiers(project_identifier, 240 * 1024)

    #  when
    attribute_filter = _AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = extract_pages(
        fetch_attribute_definitions_single_filter(
            client,
            [project_identifier],
            experiment_identifiers,
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def _generate_experiment_identifiers(project_identifier, size_bytes: int):
    per_uuid_size = 50

    identifiers_count = (size_bytes + per_uuid_size) // per_uuid_size

    experiment_identifiers = [RunIdentifier(project_identifier, SysId(f"TEST-{i}")) for i in range(identifiers_count)]

    return experiment_identifiers
