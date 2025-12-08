import typing

import pytest

import neptune_query as npt
from neptune_query.exceptions import (
    NeptuneRetryError,
    NeptuneUnexpectedResponseError,
)
from neptune_query.filters import AttributeFilter
from neptune_query.internal import identifiers
from neptune_query.internal.filters import (
    _AttributeFilter,
    _Filter,
)
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    RunAttributeDefinition,
)
from neptune_query.internal.retrieval import search
from neptune_query.internal.retrieval.attribute_definitions import fetch_attribute_definitions_single_filter
from neptune_query.internal.retrieval.attribute_values import (
    AttributeValue,
    fetch_attribute_values,
)
from neptune_query.internal.retrieval.metrics import fetch_multiple_series_values
from neptune_query.internal.retrieval.search import ContainerType
from neptune_query.internal.retrieval.series import (
    SeriesValue,
    fetch_series_values,
)
from tests.e2e.conftest import extract_pages
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    ProjectData,
    RunData,
    step_to_timestamp,
)

MAX_PATH_LENGTH = 1024
NUM_LONG_ATTRIBUTES = 4000
STEP = 1.0
STEP_TIMESTAMP_MS = int(step_to_timestamp(STEP).timestamp() * 1000)

_LONG_CONFIG_PREFIX = "int-value-"
_LONG_CONFIG_DIGITS = MAX_PATH_LENGTH - len(_LONG_CONFIG_PREFIX)
LONG_PATH_CONFIGS = {f"{_LONG_CONFIG_PREFIX}{k:0{_LONG_CONFIG_DIGITS}d}": k for k in range(NUM_LONG_ATTRIBUTES)}

_LONG_SERIES_PREFIX = "string-series-"
_LONG_SERIES_DIGITS = MAX_PATH_LENGTH - len(_LONG_SERIES_PREFIX)
LONG_PATH_SERIES = {
    f"{_LONG_SERIES_PREFIX}{k:0{_LONG_SERIES_DIGITS}d}": f"string-{k}" for k in range(NUM_LONG_ATTRIBUTES)
}

_LONG_METRICS_PREFIX = "float-series-"
_LONG_METRICS_DIGITS = MAX_PATH_LENGTH - len(_LONG_METRICS_PREFIX)
LONG_PATH_METRICS = {
    f"{_LONG_METRICS_PREFIX}{k:0{_LONG_METRICS_DIGITS}d}": float(k) for k in range(NUM_LONG_ATTRIBUTES)
}


@pytest.fixture(scope="module")
def project(ensure_project) -> IngestedProjectData:
    project_data = ProjectData(
        runs=[
            RunData(
                experiment_name=f"split-experiment-{index}",
                run_id=f"split-run-{index}",
                configs={**LONG_PATH_CONFIGS},
                string_series={path: {STEP: value} for path, value in LONG_PATH_SERIES.items()},
                float_series={path: {STEP: value} for path, value in LONG_PATH_METRICS.items()},
            )
            for index in range(3)
        ],
    )

    return ensure_project(project_data)


@pytest.fixture(scope="module")
def experiment_identifiers(client, project):
    project_identifier = identifiers.ProjectIdentifier(project.project_identifier)
    experiment_names = [run.experiment_name for run in project.ingested_runs]
    identifiers_by_name: list[identifiers.RunIdentifier] = []

    for experiment_name in experiment_names:
        sys_ids: list[identifiers.SysId] = []
        for page in search.fetch_experiment_sys_ids(
            client=client,
            project_identifier=project_identifier,
            filter_=_Filter.name_eq(experiment_name),
        ):
            sys_ids.extend(page.items)

        if len(sys_ids) != 1:
            raise RuntimeError(f"Expected to fetch exactly one sys_id for {experiment_name}, got {sys_ids}")

        identifiers_by_name.append(identifiers.RunIdentifier(project_identifier, identifiers.SysId(sys_ids[0])))

    return identifiers_by_name


@pytest.mark.parametrize(
    "exp_limit,attr_limit,success",
    [
        (1, len(LONG_PATH_CONFIGS), True),  # no known limit, TODO: could we reach the limit if we generate more data?
        (2, len(LONG_PATH_CONFIGS), True),  # attribute definitions may be pretty resilient though
        (3, len(LONG_PATH_CONFIGS), True),
    ],
)
def test_fetch_attribute_definitions_retrieval(client, project, experiment_identifiers, exp_limit, attr_limit, success):
    # given
    exp_identifiers = experiment_identifiers[:exp_limit]
    attribute_paths = list(LONG_PATH_CONFIGS.keys())[:attr_limit]
    project_identifier = identifiers.ProjectIdentifier(project.project_identifier)
    attribute_filter = typing.cast(_AttributeFilter, _attribute_filter("int-value", attr_limit)._to_internal())

    #  when
    result = None
    thrown_e = None
    try:
        result = extract_pages(
            fetch_attribute_definitions_single_filter(client, [project_identifier], exp_identifiers, attribute_filter)
        )
    except NeptuneUnexpectedResponseError as e:
        thrown_e = e

    # then
    if success:
        assert thrown_e is None
        assert set(result) == {AttributeDefinition(path, "int") for path in attribute_paths}
    else:
        assert result is None
        assert thrown_e is not None


@pytest.mark.parametrize(
    "exp_limit,attr_limit",
    [
        (1, len(LONG_PATH_CONFIGS)),
        (2, len(LONG_PATH_CONFIGS)),
        (3, len(LONG_PATH_CONFIGS)),
    ],
)
def test_fetch_attribute_definitions_composition(client, project, experiment_identifiers, exp_limit, attr_limit):
    # given
    exp_names = [run.experiment_name for run in project.ingested_runs][:exp_limit]
    attribute_paths = list(LONG_PATH_CONFIGS.keys())[:attr_limit]

    #  when
    result = npt.list_attributes(
        project=project.project_identifier,
        experiments=exp_names,
        attributes=_attribute_filter("int-value", attr_limit),
    )

    # then
    assert set(result) == set(attribute_paths)


@pytest.mark.parametrize(
    "exp_limit,attr_limit,success",
    [
        (1, len(LONG_PATH_CONFIGS), True),  # no known limit, TODO: could we reach the limit if we generate more data?
        (2, len(LONG_PATH_CONFIGS), True),
        (3, len(LONG_PATH_CONFIGS), True),
    ],
)
def test_fetch_attribute_values_retrieval(client, project, experiment_identifiers, exp_limit, attr_limit, success):
    # given
    exp_identifiers = experiment_identifiers[:exp_limit]
    attribute_data = dict(list(LONG_PATH_CONFIGS.items())[:attr_limit])
    project_identifier = identifiers.ProjectIdentifier(project.project_identifier)
    attribute_definitions = [AttributeDefinition(key, "int") for key in attribute_data]

    #  when
    result = None
    thrown_e = None
    try:
        result = extract_pages(
            fetch_attribute_values(client, project_identifier, exp_identifiers, attribute_definitions)
        )
    except (NeptuneRetryError, NeptuneUnexpectedResponseError) as e:
        thrown_e = e

    # then
    if success:
        assert thrown_e is None
        assert set(result) == {
            AttributeValue(AttributeDefinition(key, "int"), value=value, run_identifier=exp)
            for exp in exp_identifiers
            for key, value in attribute_data.items()
        }
    else:
        assert result is None
        assert thrown_e is not None


@pytest.mark.parametrize(
    "exp_limit,attr_limit",
    [
        (1, len(LONG_PATH_CONFIGS)),
        (2, len(LONG_PATH_CONFIGS)),
        (3, len(LONG_PATH_CONFIGS)),
    ],
)
def test_fetch_attribute_values_composition(client, project, experiment_identifiers, exp_limit, attr_limit):
    # given
    exp_names = [run.experiment_name for run in project.ingested_runs][:exp_limit]
    attribute_paths = list(LONG_PATH_CONFIGS.keys())[:attr_limit]

    #  when
    result = npt.fetch_experiments_table(
        experiments=exp_names,
        attributes=_attribute_filter("int-value", attr_limit),
        sort_direction="asc",
    )

    # then
    assert result.shape == (exp_limit, attr_limit)
    assert result.index.tolist() == exp_names
    assert result.columns.tolist() == attribute_paths


@pytest.mark.parametrize(
    "exp_limit,attr_limit,success",
    [
        (1, 2000, True),
        (1, 2001, False),
        (2, 1000, True),
        (2, 1001, False),
        (3, 666, True),
        (3, 667, False),
    ],
)
def test_fetch_string_series_values_retrieval(client, project, experiment_identifiers, exp_limit, attr_limit, success):
    #  given
    exp_identifiers = experiment_identifiers[:exp_limit]
    attribute_data = dict(list(LONG_PATH_SERIES.items())[:attr_limit])
    attribute_definitions = [
        RunAttributeDefinition(run_identifier=exp, attribute_definition=AttributeDefinition(key, "string_series"))
        for exp in exp_identifiers
        for key in attribute_data
    ]

    # when
    result = None
    thrown_e = None
    try:
        result = fetch_series_values(
            client,
            attribute_definitions,
            include_inherited=True,
            container_type=ContainerType.EXPERIMENT,
            step_range=(None, None),
            tail_limit=None,
        )
    except (NeptuneRetryError, NeptuneUnexpectedResponseError) as e:
        thrown_e = e

    # then
    if success:
        expected_result = {
            RunAttributeDefinition(
                run_identifier=exp, attribute_definition=AttributeDefinition(key, "string_series")
            ): [SeriesValue(STEP, value, STEP_TIMESTAMP_MS)]
            for exp in exp_identifiers
            for key, value in attribute_data.items()
        }

        assert thrown_e is None
        assert result == expected_result
    else:
        assert result is None
        assert thrown_e is not None


@pytest.mark.parametrize(
    "exp_limit,attr_limit",
    [
        (1, len(LONG_PATH_SERIES)),
        (2, len(LONG_PATH_SERIES)),
        (3, len(LONG_PATH_SERIES)),
    ],
)
def test_fetch_string_series_values_composition(client, project, experiment_identifiers, exp_limit, attr_limit):
    #  given
    exp_names = [run.experiment_name for run in project.ingested_runs][:exp_limit]
    attribute_paths = list(LONG_PATH_SERIES.keys())[:attr_limit]

    # when
    result = npt.fetch_series(
        experiments=exp_names,
        attributes=_attribute_filter("string-series", attr_limit),
    )

    # then
    assert result.shape == (exp_limit, attr_limit)
    assert result.index.tolist() == [(exp, 1.0) for exp in exp_names]
    assert result.columns.tolist() == attribute_paths


@pytest.mark.parametrize(
    "exp_limit,attr_limit,success",
    [
        (1, len(LONG_PATH_METRICS), True),  # no known limit, TODO: could we reach the limit if we generate more data?
        (2, len(LONG_PATH_METRICS), True),
        (3, len(LONG_PATH_METRICS), True),
    ],
)
def test_fetch_float_series_values_retrieval(client, project, experiment_identifiers, exp_limit, attr_limit, success):
    #  given
    exp_identifiers = experiment_identifiers[:exp_limit]
    attribute_data = dict(list(LONG_PATH_METRICS.items())[:attr_limit])
    attribute_definitions = [
        RunAttributeDefinition(run_identifier=exp, attribute_definition=AttributeDefinition(key, "float_series"))
        for exp in exp_identifiers
        for key in attribute_data
    ]

    # when
    result = None
    thrown_e = None
    try:
        result = fetch_multiple_series_values(
            client,
            attribute_definitions,
            include_inherited=True,
            container_type=ContainerType.EXPERIMENT,
            include_preview=False,
            step_range=(None, None),
            tail_limit=None,
        )
    except NeptuneRetryError as e:
        thrown_e = e

    # then
    if success:
        expected_values = {
            RunAttributeDefinition(
                run_identifier=exp,
                attribute_definition=AttributeDefinition(key, "float_series"),
            ): [(STEP_TIMESTAMP_MS, STEP, value, False, STEP)]
            for exp in exp_identifiers
            for key, value in attribute_data.items()
        }
        assert thrown_e is None
        assert result == expected_values
    else:
        assert result is None
        assert thrown_e is not None


@pytest.mark.parametrize(
    "exp_limit,attr_limit",
    [
        (1, len(LONG_PATH_METRICS)),
        (2, len(LONG_PATH_METRICS)),
        (3, len(LONG_PATH_METRICS)),
    ],
)
def test_fetch_float_series_values_composition(client, project, experiment_identifiers, exp_limit, attr_limit):
    #  given
    exp_names = [run.experiment_name for run in project.ingested_runs][:exp_limit]
    attribute_paths = list(LONG_PATH_METRICS.keys())[:attr_limit]

    # when
    result = npt.fetch_metrics(
        experiments=exp_names,
        attributes=_attribute_filter("float-series", attr_limit),
    )

    # then
    assert result.shape == (exp_limit, attr_limit)
    assert result.index.tolist() == [(exp, 1.0) for exp in exp_names]
    assert result.columns.tolist() == attribute_paths


def _attribute_filter(name, limit):
    id_regex = "|".join(str(n) for n in range(limit))
    return AttributeFilter(name=f"^{name}-0+0({id_regex})$")
