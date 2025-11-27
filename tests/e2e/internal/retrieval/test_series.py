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
from tests.e2e.conftest import extract_pages
from tests.e2e.data_ingestion import (
    File,
    Histogram,
    IngestedProjectData,
    ProjectData,
    RunData,
    ensure_project,
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

FILE_MATCHER_2 = FileMatcher(
    path_pattern=".*/file-series_file_series_1.*/.*0002_000.*/.*.txt",
    mime_type="text/plain",
    size_bytes=8,
)

FILE_MATCHER_1 = FileMatcher(
    path_pattern=".*/file-series_file_series_1.*/.*0001_000.*/.*.txt",
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
def project_1(client, api_token, workspace, test_execution_id) -> IngestedProjectData:
    return ensure_project(
        client=client,
        api_token=api_token,
        workspace=workspace,
        unique_key=test_execution_id,
        project_data=ProjectData(
            project_name_base="project_404",
            runs=[
                RunData(
                    run_id_base="run_project_1_alpha",
                    experiment_name_base="experiment_project_1_alpha",
                    fork_point=None,
                    string_series={
                        "metrics/str_foo_bar_1": {i: f"string-1-{i}" for i in range(10)},
                        "metrics/str_foo_bar_2": {i: f"string-2-{i}" for i in range(10)},
                    },
                    histogram_series={
                        "metrics/histograms_1": {
                            0: Histogram(bin_edges=[1, 2, 3, 4], counts=[10, 20, 30]),
                            1: Histogram(bin_edges=[4, 5, 6, 7], counts=[40, 50, 60]),
                            2: Histogram(bin_edges=[7, 8, 9, 10], counts=[70, 80, 90]),
                            3: Histogram(bin_edges=[11, 12, 13, 14], counts=[10, 20, 30]),
                            4: Histogram(bin_edges=[14, 15, 16, 17], counts=[40, 50, 60]),
                            5: Histogram(bin_edges=[17, 18, 19, 20], counts=[70, 80, 90]),
                        },
                    },
                    file_series={
                        "file-series/file_series_1": {
                            0: File(b"file-1-0", mime_type="text/plain"),
                            1: File(b"file-1-1", mime_type="text/plain"),
                            2: File(b"file-1-2", mime_type="text/plain"),
                        },
                        "file-series/file_series_2": {
                            0: File(b"file-2-0", mime_type="text/plain"),
                            1: File(b"file-2-1", mime_type="text/plain"),
                            2: File(b"file-2-2", mime_type="text/plain"),
                        },
                        "file-series/file_series_3": {
                            0: File(b"file-3-0", mime_type="text/plain"),
                            1: File(b"file-3-1", mime_type="text/plain"),
                            2: File(b"file-3-2", mime_type="text/plain"),
                        },
                    },
                ),
            ],
        ),
    )


@pytest.fixture(scope="session")
def run_1_sys_id(client, project_1: IngestedProjectData) -> RunIdentifier:
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
def experiment_1_sys_id(client, project_1: IngestedProjectData) -> RunIdentifier:
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


def test_fetch_series_values_does_not_exist(client, project_1):
    # given
    run_definition = RunAttributeDefinition(
        RunIdentifier(project_1.project_identifier, project_1.ingested_runs[0].experiment_name),
        AttributeDefinition("does-not-exist", "string"),
    )

    # when
    series = extract_pages(
        fetch_series_values(
            client,
            [run_definition],
            include_inherited=False,
            container_type=ContainerType.EXPERIMENT,
        )
    )

    # then
    assert series == []


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


def range_inclusive(from_, to):
    if from_ <= to:
        return range(from_, to + 1)
    else:
        return range(from_, to - 1, -1)


TEST_SCENARIOS = [
    Scenario(
        id="tc01",
        description="Fetch all string series values without filters",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        expected_values=[(i, f"string-1-{i}") for i in range_inclusive(9, 0)],
    ),
    Scenario(
        id="tc02",
        description="Fetch all values from a different string series",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_2", "string_series"),
        expected_values=[(i, f"string-2-{i}") for i in range_inclusive(9, 0)],
    ),
    Scenario(
        id="tc03",
        description="Fetch all values from a string series with explicit step_range=(None, None)",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_2", "string_series"),
        step_range=(None, None),
        expected_values=[(i, f"string-2-{i}") for i in range_inclusive(9, 0)],
    ),
    Scenario(
        id="tc04",
        description="Fetch string series values from step 1 onwards",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(1, None),
        expected_values=[(i, f"string-1-{i}") for i in range_inclusive(9, 1)],
    ),
    Scenario(
        id="tc05",
        description="Fetch string series values up to step 5",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(None, 5),
        expected_values=[(i, f"string-1-{i}") for i in range_inclusive(5, 0)],
    ),
    Scenario(
        id="tc06",
        description="Fetch string series values in step range 2-7",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(2, 7),
        expected_values=[(i, f"string-1-{i}") for i in range_inclusive(7, 2)],
    ),
    Scenario(
        id="tc07",
        description="Fetch string series values up to step 2",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(None, 2),
        expected_values=[(i, f"string-1-{i}") for i in range_inclusive(2, 0)],
    ),
    Scenario(
        id="tc08",
        description="Fetch string series values from step 5 onwards",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(5, None),
        expected_values=[(i, f"string-1-{i}") for i in range_inclusive(9, 5)],
    ),
    Scenario(
        id="tc09",
        description="Fetch string series with negative upper bound returns empty",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(None, -1),
        expected_values=[],
    ),
    Scenario(
        id="tc10",
        description="Fetch string series beyond available steps returns empty",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(11, None),
        expected_values=[],
    ),
    Scenario(
        id="tc11",
        description="Fetch last 3 string series values using tail limit",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        tail_limit=3,
        expected_values=[(9, "string-1-9"), (8, "string-1-8"), (7, "string-1-7")],
    ),
    Scenario(
        id="tc12",
        description="Fetch last 5 string series values using tail limit",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        tail_limit=5,
        expected_values=[
            (9, "string-1-9"),
            (8, "string-1-8"),
            (7, "string-1-7"),
            (6, "string-1-6"),
            (5, "string-1-5"),
        ],
    ),
    Scenario(
        id="tc13",
        description="Fetch string series with tail limit 0 returns empty",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        tail_limit=0,
        expected_values=[],
    ),
    Scenario(
        id="tc14",
        description="Fetch all histogram series values without filters",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        expected_values=[
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
            (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40, 50, 60])),
            (0, HistogramMatcher(edges=[1.0, 2.0, 3.0, 4.0], values=[10, 20, 30])),
        ],
    ),
    Scenario(
        id="tc15",
        description="Fetch histogram series values from step 1 onwards",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(1, None),
        expected_values=[
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
            (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40, 50, 60])),
        ],
    ),
    Scenario(
        id="tc16",
        description="Fetch histogram series values up to step 5",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(None, 5),
        expected_values=[
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
            (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40, 50, 60])),
            (0, HistogramMatcher(edges=[1.0, 2.0, 3.0, 4.0], values=[10, 20, 30])),
        ],
    ),
    Scenario(
        id="tc17",
        description="Fetch histogram series values in step range 2-7",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(2, 7),
        expected_values=[
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
        ],
    ),
    Scenario(
        id="tc18",
        description="Fetch histogram series values up to step 2",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(None, 2),
        expected_values=[
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
            (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40, 50, 60])),
            (0, HistogramMatcher(edges=[1.0, 2.0, 3.0, 4.0], values=[10, 20, 30])),
        ],
    ),
    Scenario(
        id="tc19",
        description="Fetch histogram series values from step 5 onwards",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(5, None),
        expected_values=[
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
        ],
    ),
    Scenario(
        id="tc20",
        description="Fetch last 2 histogram series values using tail limit",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        tail_limit=2,
        expected_values=[
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
        ],
    ),
    Scenario(
        id="tc21",
        description="Fetch histogram series with tail limit 0 returns empty",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        tail_limit=0,
        expected_values=[],
    ),
    Scenario(
        id="tc22",
        description="Fetch file series values from step 1 onwards",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        step_range=(1, None),
        expected_values=[
            (2, FILE_MATCHER_2),
            (1, FILE_MATCHER_1),
        ],
    ),
    Scenario(
        id="tc23",
        description="Fetch file series values up to step 1",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        step_range=(None, 1),
        expected_values=[
            (1, FILE_MATCHER_1),
            (0, FILE_MATCHER_0),
        ],
    ),
    Scenario(
        id="tc24",
        description="Fetch all file series values without filters",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        expected_values=[
            (2, FILE_MATCHER_2),
            (1, FILE_MATCHER_1),
            (0, FILE_MATCHER_0),
        ],
    ),
    Scenario(
        id="tc25",
        description="Fetch last 2 file series values using tail limit",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        tail_limit=2,
        expected_values=[
            (2, FILE_MATCHER_2),
            (1, FILE_MATCHER_1),
        ],
    ),
    Scenario(
        id="tc26",
        description="Fetch file series with tail limit 0 returns empty",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        tail_limit=0,
        expected_values=[],
    ),
    Scenario(
        id="tc27",
        description="Fetch histogram series beyond available steps returns empty",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(10, None),
        expected_values=[],
    ),
    Scenario(
        id="tc28",
        description="Fetch single histogram series value at specific step",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(3, 3),
        expected_values=[
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
        ],
    ),
    Scenario(
        id="tc29",
        description="Fetch file series beyond available steps returns empty",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        step_range=(10, None),
        expected_values=[],
    ),
    Scenario(
        id="tc30",
        description="Fetch single file series value at specific step",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        step_range=(2, 2),
        expected_values=[
            (2, FILE_MATCHER_2),
        ],
    ),
    Scenario(
        id="tc31",
        description="Fetch string series values from step 8 onwards (second series)",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_2", "string_series"),
        step_range=(8, None),
        expected_values=[(9, "string-2-9"), (8, "string-2-8")],
    ),
    Scenario(
        id="tc32",
        description="Fetch string series values up to step 0 (second series)",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_2", "string_series"),
        step_range=(None, 0),
        expected_values=[(0, "string-2-0")],
    ),
    Scenario(
        id="tc33",
        description="Fetch last 1 string series value using tail limit (second series)",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_2", "string_series"),
        tail_limit=1,
        expected_values=[(9, "string-2-9")],
    ),
    Scenario(
        id="tc34",
        description="Fetch last 2 values within step range 2-7",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(2, 7),
        tail_limit=2,
        expected_values=[(7, "string-1-7"), (6, "string-1-6")],
    ),
    Scenario(
        id="tc35",
        description="Fetch last 3 values within step range 1-5",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(1, 5),
        tail_limit=3,
        expected_values=[(5, "string-1-5"), (4, "string-1-4"), (3, "string-1-3")],
    ),
    Scenario(
        id="tc36",
        description="Fetch last 2 values with step range having only lower bound",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(5, None),
        tail_limit=2,
        expected_values=[(9, "string-1-9"), (8, "string-1-8")],
    ),
    Scenario(
        id="tc37",
        description="Fetch last 2 values with step range having only upper bound",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(None, 5),
        tail_limit=2,
        expected_values=[(5, "string-1-5"), (4, "string-1-4")],
    ),
    Scenario(
        id="tc38",
        description="Fetch string series with tail limit larger than available data returns all data",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        tail_limit=100,
        expected_values=[(i, f"string-1-{i}") for i in range_inclusive(9, 0)],
    ),
    Scenario(
        id="tc39",
        description="Fetch histogram series with tail limit larger than available data returns all data",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        tail_limit=100,
        expected_values=[
            (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70, 80, 90])),
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
            (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40, 50, 60])),
            (0, HistogramMatcher(edges=[1.0, 2.0, 3.0, 4.0], values=[10, 20, 30])),
        ],
    ),
    Scenario(
        id="tc40",
        description="Fetch file series with tail limit larger than available data returns all data",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        tail_limit=100,
        expected_values=[
            (2, FILE_MATCHER_2),
            (1, FILE_MATCHER_1),
            (0, FILE_MATCHER_0),
        ],
    ),
    Scenario(
        id="tc41",
        description="Fetch string series with tail limit larger than available data within step range",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_1", "string_series"),
        step_range=(2, 7),
        tail_limit=100,
        expected_values=[(i, f"string-1-{i}") for i in range_inclusive(7, 2)],
    ),
    Scenario(
        id="tc42",
        description="Fetch histogram series with tail limit larger than available data within step range",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(1, 4),
        tail_limit=100,
        expected_values=[
            (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40, 50, 60])),
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
            (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40, 50, 60])),
        ],
    ),
    Scenario(
        id="tc43",
        description="Fetch file series with tail limit larger than available data within step range",
        attribute_definition=AttributeDefinition("file-series/file_series_1", "file_series"),
        step_range=(0, 1),
        tail_limit=100,
        expected_values=[
            (1, FILE_MATCHER_1),
            (0, FILE_MATCHER_0),
        ],
    ),
    Scenario(
        id="tc44",
        description="Fetch string series with tail limit larger than available data from step onwards",
        attribute_definition=AttributeDefinition("metrics/str_foo_bar_2", "string_series"),
        step_range=(7, None),
        tail_limit=100,
        expected_values=[(9, "string-2-9"), (8, "string-2-8"), (7, "string-2-7")],
    ),
    Scenario(
        id="tc45",
        description="Fetch histogram series with tail limit larger than available data up to step",
        attribute_definition=AttributeDefinition("metrics/histograms_1", "histogram_series"),
        step_range=(None, 3),
        tail_limit=100,
        expected_values=[
            (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10, 20, 30])),
            (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70, 80, 90])),
            (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40, 50, 60])),
            (0, HistogramMatcher(edges=[1.0, 2.0, 3.0, 4.0], values=[10, 20, 30])),
        ],
    ),
]


@pytest.mark.parametrize("scenario", TEST_SCENARIOS, ids=lambda scenario: scenario.id)
def test_fetch_series_values_single_series_experiment(client, experiment_1_sys_id, scenario):
    # given
    run_attribute_definition = RunAttributeDefinition(experiment_1_sys_id, scenario.attribute_definition)

    kwargs = {}
    if scenario.step_range:
        kwargs["step_range"] = scenario.step_range
    if scenario.tail_limit is not None:
        kwargs["tail_limit"] = scenario.tail_limit

    # when
    series = extract_pages(
        fetch_series_values(
            client,
            [run_attribute_definition],
            include_inherited=False,
            container_type=ContainerType.EXPERIMENT,
            **kwargs,
        )
    )

    # then
    if not scenario.expected_values:
        assert series == []
    else:
        assert len(series) == 1
        run_attribute_definition_returned, values = series[0]

        # TODO: PY-309 make fetch_series_values return values sorted by step, descending order
        # Then we don't need to sort here
        values = sorted(values, reverse=True)

        assert run_attribute_definition_returned == run_attribute_definition
        assert_series_matches(values, scenario.expected_values)
