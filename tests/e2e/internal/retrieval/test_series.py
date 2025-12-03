import re
from dataclasses import dataclass

import pytest

from neptune_query.internal.filters import (
    _Attribute,
    _Filter,
)
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.retrieval import search
from neptune_query.internal.retrieval.attribute_types import File as NQInternalFile
from neptune_query.internal.retrieval.attribute_types import Histogram as NQInternalHistogram
from neptune_query.internal.retrieval.search import ContainerType
from neptune_query.internal.retrieval.series import (
    RunAttributeDefinition,
    fetch_series_values,
)
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    IngestionFile,
    IngestionHistogram,
    ProjectData,
    RunData,
    step_to_timestamp,
)


@dataclass
class FileMatcher:
    path_pattern: str
    size_bytes: int
    mime_type: str

    def __eq__(self, other: NQInternalFile) -> bool:
        return (
            self.size_bytes == other.size_bytes
            and self.mime_type == other.mime_type
            and re.search(self.path_pattern, other.path) is not None
        )


FILE_MATCHER_0 = FileMatcher(
    path_pattern=".*/file-series_file_series_1.*/.*0000_000.*/.*.txt",
    mime_type="text/plain",
    size_bytes=8,
)

FILE_MATCHER_1 = FileMatcher(
    path_pattern=".*/file-series_file_series_1.*/.*0001_000.*/.*.html",
    mime_type="text/html",
    size_bytes=41,
)

FILE_MATCHER_2 = FileMatcher(
    path_pattern=".*/file-series_file_series_1.*/.*0002_000.*/.*.txt",
    mime_type="text/plain",
    size_bytes=8,
)


@dataclass
class HistogramMatcher:
    edges: list[float]
    values: list[int]

    def __eq__(self, other: NQInternalHistogram) -> bool:
        return other.type == "COUNTING" and self.edges == other.edges and self.values == other.values


@pytest.fixture(scope="session")
def project_1(ensure_project) -> IngestedProjectData:
    return ensure_project(
        ProjectData(
            project_name_base="internal__retrieval__test-series__project_1",
            runs=[
                RunData(
                    experiment_name_base="experiment_1",
                    run_id_base="run_xyz",
                    string_series={
                        "metrics/str_foo_bar_1": {i: f"string-1-{i}" for i in range(10)},
                        "metrics/str_foo_bar_2": {i: f"string-2-{i}" for i in range(5, 15)},
                    },
                    histogram_series={
                        "metrics/histograms_1": {
                            0: IngestionHistogram(bin_edges=[1, 2, 3, 4], counts=[10, 20, 30]),
                            1: IngestionHistogram(bin_edges=[4, 5, 6, 7], counts=[40, 50, 60]),
                            2: IngestionHistogram(bin_edges=[7, 8, 9, 10], counts=[70, 80, 90]),
                            3: IngestionHistogram(bin_edges=[11, 12, 13, 14], counts=[10, 20, 30]),
                            4: IngestionHistogram(bin_edges=[14, 15, 16, 17], counts=[40, 50, 60]),
                            5: IngestionHistogram(bin_edges=[17, 18, 19, 20], counts=[70, 80, 90]),
                        },
                        "metrics/histograms_2": {
                            4: IngestionHistogram(bin_edges=[1, 2, 3, 4], counts=[10, 20, 30]),
                            5: IngestionHistogram(bin_edges=[4, 5, 6, 7], counts=[40, 50, 60]),
                            6: IngestionHistogram(bin_edges=[7, 8, 9, 10], counts=[70, 80, 90]),
                        },
                    },
                    file_series={
                        "file-series/file_series_1": {
                            0: File(b"file-1-0", mime_type="text/plain"),
                            1: File(b"<html><title>Hello Neptune</title></html>", mime_type="text/html"),
                            2: File(b"file-1-2", mime_type="text/plain"),
                        },
                        "file-series/file_series_2": {
                            0: File(b"file-2-0", mime_type="text/plain"),
                            1: File(b"<html><title>Hello Neptune 2</title></html>", mime_type="text/html"),
                            2: File(b"file-2-2", mime_type="text/plain"),
                        },
                        "file-series/file_series_3": {
                            10: IngestionFile(b"file-3-0", mime_type="text/plain"),
                            11: IngestionFile(b"file-3-1", mime_type="text/plain"),
                            12: IngestionFile(b"file-3-2", mime_type="text/plain"),
                        },
                    },
                ),
            ],
        ),
    )


@pytest.fixture(scope="session")
def run_1_id(client, project_1: IngestedProjectData) -> RunIdentifier:
    run_id = project_1.ingested_runs[0].run_id
    sys_ids: list[SysId] = []
    for page in search.fetch_run_sys_ids(
        client=client,
        project_identifier=project_1.project_identifier,
        filter_=_Filter.eq(_Attribute("sys/custom_run_id", type="string"), run_id),
    ):
        for item in page.items:
            sys_ids.append(item)
    if len(sys_ids) == 0:
        raise RuntimeError(f"Expected exactly one sys_id for run_id {run_id}, got 0")
    if len(sys_ids) > 1:
        raise RuntimeError(f"Expected exactly one sys_id for run_id {run_id}, got {sys_ids}")

    sys_id = SysId(sys_ids[0])
    return RunIdentifier(ProjectIdentifier(project_1.project_identifier), sys_id)


@pytest.fixture(scope="session")
def experiment_1_id(client, project_1: IngestedProjectData) -> RunIdentifier:
    experiment_name = project_1.ingested_runs[0].experiment_name
    sys_ids: list[SysId] = []
    for page in search.fetch_experiment_sys_ids(
        client=client,
        project_identifier=project_1.project_identifier,
        filter_=_Filter.eq(_Attribute("sys/name", type="string"), experiment_name),
    ):
        for item in page.items:
            sys_ids.append(item)

    if len(sys_ids) == 0:
        raise RuntimeError(f"Expected exactly one sys_id for experiment {experiment_name}, got 0")
    if len(sys_ids) > 1:
        raise RuntimeError(f"Expected exactly one sys_id for experiment {experiment_name}, got {sys_ids}")

    sys_id = SysId(sys_ids[0])
    return RunIdentifier(ProjectIdentifier(project_1.project_identifier), sys_id)


def test_fetch_series_values_does_not_exist(client, experiment_1_id):
    # given
    run_attribute_definition = RunAttributeDefinition(
        experiment_1_id,
        AttributeDefinition("does-not-exist", "string"),
    )

    # when
    series_by_experiment = fetch_series_values(
        client,
        [run_attribute_definition],
        include_inherited=False,
        container_type=ContainerType.EXPERIMENT,
    )

    series_by_run = fetch_series_values(
        client,
        [run_attribute_definition],
        include_inherited=False,
        container_type=ContainerType.RUN,
    )

    # container_type should only matter when include_inherited=True
    assert series_by_experiment == series_by_run
    series = series_by_experiment

    # then
    assert series == {
        run_attribute_definition: [],
    }


@dataclass
class Scenario:
    id: str
    description: str
    attribute_definition: AttributeDefinition
    expected_values: list[tuple[int, object]]
    step_range: tuple[int | None, int | None] | None = None
    tail_limit: int | None = None

    def __repr__(self):
        return f"Scenario(id={self.id}, description={self.description})"


TEST_SCENARIOS = [
    # String series - no filters
    Scenario(
        id="tc01",
        description="string series, no filters",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        expected_values=[(i, f"string-1-{i}") for i in range(10)],
    ),
    Scenario(
        id="tc02",
        description="string series, explicit none filters",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_2", "string_series"),
        step_range=(None, None),
        expected_values=[(i, f"string-2-{i}") for i in range(5, 15)],
    ),
    # String series - step ranges
    Scenario(
        id="tc03",
        description="string series, step range left-bounded",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(1, None),
        expected_values=[(i, f"string-1-{i}") for i in range(1, 10)],
    ),
    Scenario(
        id="tc04",
        description="string series, step range right-bounded",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(None, 5),
        expected_values=[(i, f"string-1-{i}") for i in range(6)],
    ),
    Scenario(
        id="tc05",
        description="string series, step range both-bounded",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(2, 7),
        expected_values=[(i, f"string-1-{i}") for i in range(2, 8)],
    ),
    Scenario(
        id="tc06",
        description="string series, step range doesn't include any values",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_2", "string_series"),
        step_range=(1, 3),
        expected_values=[],
    ),
    Scenario(
        id="tc07",
        description="string series, step range beyond available",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(11, None),
        expected_values=[],
    ),
    # String series - tail limits
    Scenario(
        id="tc08",
        description="string series, positive tail limit",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        tail_limit=3,
        expected_values=[(7, "string-1-7"), (8, "string-1-8"), (9, "string-1-9")],
    ),
    Scenario(
        id="tc09",
        description="string series, zero tail limit",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        tail_limit=0,
        expected_values=[],
    ),
    Scenario(
        id="tc10",
        description="string series, large tail limit",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        tail_limit=100,
        expected_values=[(i, f"string-1-{i}") for i in range(10)],
    ),
    # String series - combined filters
    Scenario(
        id="tc11",
        description="string series, step range with tail limit",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(2, 7),
        tail_limit=2,
        expected_values=[(6, "string-1-6"), (7, "string-1-7")],
    ),
    Scenario(
        id="tc12",
        description="string series, left-bounded range with tail limit",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(5, None),
        tail_limit=2,
        expected_values=[(8, "string-1-8"), (9, "string-1-9")],
    ),
    Scenario(
        id="tc13",
        description="string series, right-bounded range with tail limit",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(None, 5),
        tail_limit=2,
        expected_values=[(4, "string-1-4"), (5, "string-1-5")],
    ),
    # Histogram series - no filters
    Scenario(
        id="tc14",
        description="histogram series, no filters",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        expected_values=[
            (0, HistogramMatcher(edges=[1.0, 2.0, 3.0, 4.0], values=[10, 20, 30])),
            (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40, 50, 60])),
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
        ],
    ),
    # Histogram series - step ranges
    Scenario(
        id="tc15",
        description="histogram series, step range left-bounded",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(1, None),
        expected_values=[
            (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40, 50, 60])),
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
        ],
    ),
    Scenario(
        id="tc16",
        description="histogram series, step range right-bounded",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(None, 2),
        expected_values=[
            (0, HistogramMatcher(edges=[1.0, 2.0, 3.0, 4.0], values=[10, 20, 30])),
            (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40, 50, 60])),
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
        ],
    ),
    Scenario(
        id="tc17",
        description="histogram series, step range both-bounded",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(2, 7),
        expected_values=[
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
        ],
    ),
    Scenario(
        id="tc18",
        description="histogram series, step range beyond available",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(10, None),
        expected_values=[],
    ),
    # Histogram series - tail limits
    Scenario(
        id="tc19",
        description="histogram series, positive tail limit",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        tail_limit=2,
        expected_values=[
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
        ],
    ),
    Scenario(
        id="tc20",
        description="histogram series, zero tail limit",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        tail_limit=0,
        expected_values=[],
    ),
    Scenario(
        id="tc21",
        description="histogram series, large tail limit",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        tail_limit=100,
        expected_values=[
            (0, HistogramMatcher(edges=[1.0, 2.0, 3.0, 4.0], values=[10, 20, 30])),
            (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40, 50, 60])),
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
        ],
    ),
    # File series - no filters
    Scenario(
        id="tc22",
        description="file series, no filters",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        expected_values=[
            (0, FILE_MATCHER_0),
            (1, FILE_MATCHER_1),
            (2, FILE_MATCHER_2),
        ],
    ),
    # File series - step ranges
    Scenario(
        id="tc23",
        description="file series, step range left-bounded",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        step_range=(1, None),
        expected_values=[
            (1, FILE_MATCHER_1),
            (2, FILE_MATCHER_2),
        ],
    ),
    Scenario(
        id="tc24",
        description="file series, step range right-bounded",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        step_range=(None, 1),
        expected_values=[
            (0, FILE_MATCHER_0),
            (1, FILE_MATCHER_1),
        ],
    ),
    Scenario(
        id="tc25",
        description="file series, step range beyond available",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        step_range=(10, None),
        expected_values=[],
    ),
    # File series - tail limits
    Scenario(
        id="tc26",
        description="file series, positive tail limit",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        tail_limit=2,
        expected_values=[
            (1, FILE_MATCHER_1),
            (2, FILE_MATCHER_2),
        ],
    ),
    Scenario(
        id="tc27",
        description="file series, zero tail limit",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        tail_limit=0,
        expected_values=[],
    ),
    Scenario(
        id="tc28",
        description="file series, large tail limit",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        tail_limit=100,
        expected_values=[
            (0, FILE_MATCHER_0),
            (1, FILE_MATCHER_1),
            (2, FILE_MATCHER_2),
        ],
    ),
]


@pytest.mark.parametrize("scenario", TEST_SCENARIOS, ids=lambda scenario: scenario.id)
def test_fetch_series_values_single_series(client, run_1_id, scenario):
    # both run and experiment have the same sys_id as tested in test_experiment_and_run_have_the_same_sys_id

    # given
    run_attribute_definition = RunAttributeDefinition(run_1_id, scenario.attribute_definition)

    kwargs = {}
    if scenario.step_range:
        kwargs["step_range"] = scenario.step_range
    if scenario.tail_limit is not None:
        kwargs["tail_limit"] = scenario.tail_limit

    # when
    series_by_experiment = list(
        fetch_series_values(
            client,
            [run_attribute_definition],
            include_inherited=False,
            container_type=ContainerType.EXPERIMENT,
            **kwargs,
        ).items()
    )

    series_by_run = list(
        fetch_series_values(
            client,
            [run_attribute_definition],
            include_inherited=False,
            container_type=ContainerType.RUN,
            **kwargs,
        ).items()
    )

    # container_type should only matter when include_inherited=True
    assert series_by_experiment == series_by_run
    series = series_by_experiment

    # then
    assert len(series) == 1
    run_attribute_definition_returned, values = series[0]

    assert run_attribute_definition_returned == run_attribute_definition
    assert len(values) == len(scenario.expected_values)

    for i, (expected_step, expected_value) in enumerate(scenario.expected_values):
        (step, value, timestamp_millis) = values[i]
        assert step == expected_step
        assert value == expected_value
        assert timestamp_millis == step_to_timestamp(step).timestamp() * 1000.0


def test_experiment_and_run_have_the_same_sys_id(run_1_id, experiment_1_id):
    assert run_1_id == experiment_1_id
