import pytest

from neptune_query.internal.filters import _Filter
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.retrieval import search
from neptune_query.internal.retrieval.metric_buckets import (
    TimeseriesBucket,
    fetch_time_series_buckets,
)
from neptune_query.internal.retrieval.search import ContainerType
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


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    return ensure_project(
        ProjectData(
            project_name_base="metric-buckets-project",
            runs=[
                RunData(
                    experiment_name_base="metric-buckets-experiment",
                    run_id_base="metric-buckets-run-id",
                    float_series={
                        "metrics/float-series-value_0": {
                            0.0: 0.5,
                            1.0: 1.5,
                            2.0: 2.5,
                            3.0: 3.5,
                        },
                        "metrics/float-series-value_1": {
                            0.0: 10.0,
                            1.0: 11.0,
                            2.0: 12.0,
                            3.0: 13.0,
                        },
                    },
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

    return RunIdentifier(project_identifier=project_identifier, sys_id=SysId(sys_ids[0]))


def test_fetch_time_series_buckets_does_not_exist(client, project, experiment_identifier):
    # given
    run_definition = RunAttributeDefinition(experiment_identifier, AttributeDefinition("does-not-exist", "string"))

    # when
    result = fetch_time_series_buckets(
        client,
        run_attribute_definitions=[run_definition],
        container_type=ContainerType.EXPERIMENT,
        x="step",
        lineage_to_the_root=False,
        include_point_previews=False,
        limit=10,
        x_range=None,
    )

    # then
    assert result == {run_definition: []}


@pytest.mark.parametrize(
    "limit",
    [2, 10, 100],
)
@pytest.mark.parametrize(
    "x_range",
    [None, (1, 2), (-100, 100)],
)
def test_fetch_time_series_buckets_single_series(client, project, experiment_identifier, limit, x_range):
    # given
    run_definition = RunAttributeDefinition(
        experiment_identifier, AttributeDefinition("metrics/float-series-value_0", "float-series")
    )

    # when
    result = fetch_time_series_buckets(
        client,
        run_attribute_definitions=[run_definition],
        container_type=ContainerType.EXPERIMENT,
        x="step",
        lineage_to_the_root=False,
        include_point_previews=False,
        limit=limit,
        x_range=x_range,
    )

    # then
    expected_buckets = _aggregate_metric_buckets(
        series=list(project.ingested_runs[0].float_series["metrics/float-series-value_0"].items()),
        limit=limit,
        x_range=x_range,
    )
    assert result == {run_definition: expected_buckets}


def _aggregate_metric_buckets(
    series: list[tuple[float, float]], limit: int, x_range: tuple[float, float] | None
) -> list[TimeseriesBucket]:
    global_from, global_to = calculate_global_range(series, x_range)
    bucket_ranges = calculate_metric_bucket_ranges(global_from, global_to, limit)
    return aggregate_metric_buckets(series, bucket_ranges)
