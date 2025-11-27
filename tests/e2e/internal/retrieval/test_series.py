import re
from dataclasses import dataclass

import pytest

from neptune_query.internal.identifiers import (
    AttributeDefinition,
    RunIdentifier,
)
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
)
from tests.e2e.internal.conftest import get_sys_id_for_run


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
                    fork_point=None,
                    string_series={
                        "metrics/strfoobar_1": {i: f"string-1-{i}" for i in range(10)},
                        "metrics/strfoobar_2": {i: f"string-2-{i}" for i in range(10)},
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
                    files={
                        "files/file-value": File(b"Binary content"),
                        "files/file-value.txt": File(b"Text content", mime_type="text/plain"),
                        "files/object-does-not-exist": File(
                            "/tmp/object-does-not-exist", mime_type="text/plain", size=1
                        ),
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


@pytest.mark.parametrize(
    "attribute_name, attribute_type, additional_kws, expected_values",
    [
        pytest.param("metrics/strfoobar_1", "string_series", {}, [(i, f"string-1-{i}") for i in range(10)], id="tc01"),
        pytest.param("metrics/strfoobar_2", "string_series", {}, [(i, f"string-2-{i}") for i in range(10)], id="tc02"),
        pytest.param(
            "metrics/strfoobar_1",
            "string_series",
            {"step_range": (None, None)},
            [(i, f"string-1-{i}") for i in range(0, 10)],
            id="tc03",
        ),
        pytest.param(
            "metrics/strfoobar_1",
            "string_series",
            {"step_range": (1, None)},
            [(i, f"string-1-{i}") for i in range(1, 10)],
            id="tc04",
        ),
        pytest.param(
            "metrics/strfoobar_1",
            "string_series",
            {"step_range": (None, 5)},
            [(i, f"string-1-{i}") for i in range(0, 6)],
            id="tc05",
        ),
        pytest.param(
            "metrics/strfoobar_1",
            "string_series",
            {"step_range": (2, 7)},
            [(i, f"string-1-{i}") for i in range(2, 8)],
            id="tc06",
        ),
        pytest.param(
            "metrics/strfoobar_1",
            "string_series",
            {"step_range": (None, 2)},
            [(i, f"string-1-{i}") for i in range(0, 3)],
            id="tc07",
        ),
        pytest.param(
            "metrics/strfoobar_1",
            "string_series",
            {"step_range": (5, None)},
            [(i, f"string-1-{i}") for i in range(5, 10)],
            id="tc08",
        ),
        pytest.param("metrics/strfoobar_1", "string_series", {"step_range": (None, -1)}, [], id="tc09"),
        pytest.param("metrics/strfoobar_1", "string_series", {"step_range": (11, None)}, [], id="tc10"),
        pytest.param(
            "metrics/strfoobar_1",
            "string_series",
            {"tail_limit": 3},
            [(7, "string-1-7"), (8, "string-1-8"), (9, "string-1-9")],
            id="tc11",
        ),
        pytest.param(
            "metrics/strfoobar_1",
            "string_series",
            {"tail_limit": 5},
            [(5, "string-1-5"), (6, "string-1-6"), (7, "string-1-7"), (8, "string-1-8"), (9, "string-1-9")],
            id="tc12",
        ),
        pytest.param("metrics/strfoobar_1", "string_series", {"tail_limit": 0}, [], id="tc13"),
        pytest.param(
            "metrics/histograms_1",
            "histogram_series",
            {},
            [
                (0, HistogramMatcher(edges=[1.0, 2.0, 3.0, 4.0], values=[10.0, 20.0, 30.0])),
                (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40.0, 50.0, 60.0])),
                (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70.0, 80.0, 90.0])),
                (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10.0, 20.0, 30.0])),
                (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40.0, 50.0, 60.0])),
                (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70.0, 80.0, 90.0])),
            ],
            id="tc14",
        ),
        pytest.param(
            "metrics/histograms_1",
            "histogram_series",
            {"step_range": (1, None)},
            [
                (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40.0, 50.0, 60.0])),
                (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70.0, 80.0, 90.0])),
                (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10.0, 20.0, 30.0])),
                (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40.0, 50.0, 60.0])),
                (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70.0, 80.0, 90.0])),
            ],
            id="tc15",
        ),
        pytest.param(
            "metrics/histograms_1",
            "histogram_series",
            {"step_range": (None, 5)},
            [
                (0, HistogramMatcher(edges=[1.0, 2.0, 3.0, 4.0], values=[10.0, 20.0, 30.0])),
                (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40.0, 50.0, 60.0])),
                (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70.0, 80.0, 90.0])),
                (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10.0, 20.0, 30.0])),
                (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40.0, 50.0, 60.0])),
                (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70.0, 80.0, 90.0])),
            ],
            id="tc16",
        ),
        pytest.param(
            "metrics/histograms_1",
            "histogram_series",
            {"step_range": (2, 7)},
            [
                (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70.0, 80.0, 90.0])),
                (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10.0, 20.0, 30.0])),
                (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40.0, 50.0, 60.0])),
                (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70.0, 80.0, 90.0])),
            ],
            id="tc17",
        ),
        pytest.param(
            "metrics/histograms_1",
            "histogram_series",
            {"step_range": (None, 2)},
            [
                (0, HistogramMatcher(edges=[1.0, 2.0, 3.0, 4.0], values=[10.0, 20.0, 30.0])),
                (1, HistogramMatcher(edges=[4.0, 5.0, 6.0, 7.0], values=[40.0, 50.0, 60.0])),
                (2, HistogramMatcher(edges=[7.0, 8.0, 9.0, 10.0], values=[70.0, 80.0, 90.0])),
            ],
            id="tc18",
        ),
        pytest.param(
            "metrics/histograms_1",
            "histogram_series",
            {"step_range": (5, None)},
            [
                (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70.0, 80.0, 90.0])),
            ],
            id="tc19",
        ),
        pytest.param(
            "metrics/histograms_1",
            "histogram_series",
            {"tail_limit": 2},
            [
                (4, HistogramMatcher(edges=[14.0, 15.0, 16.0, 17.0], values=[40.0, 50.0, 60.0])),
                (5, HistogramMatcher(edges=[17.0, 18.0, 19.0, 20.0], values=[70.0, 80.0, 90.0])),
            ],
            id="tc20",
        ),
        pytest.param("metrics/histograms_1", "histogram_series", {"tail_limit": 0}, [], id="tc21"),
        pytest.param(
            "file-series/file_series_1",
            "file_series",
            {"step_range": (1, None)},
            [
                (
                    1,
                    FileMatcher(
                        path_pattern=".*/file-series_file_series_1.*/.*0001_000.*/.*.txt",
                        mime_type="text/plain",
                        size_bytes=8,
                    ),
                ),
                (
                    2,
                    FileMatcher(
                        path_pattern=".*/file-series_file_series_1.*/.*0002_000.*/.*.txt",
                        mime_type="text/plain",
                        size_bytes=8,
                    ),
                ),
            ],
            id="tc22",
        ),
        pytest.param(
            "file-series/file_series_1",
            "file_series",
            {"step_range": (None, 1)},
            [
                (
                    0,
                    FileMatcher(
                        path_pattern=".*/file-series_file_series_1.*/.*0000_000.*/.*.txt",
                        mime_type="text/plain",
                        size_bytes=8,
                    ),
                ),
                (
                    1,
                    FileMatcher(
                        path_pattern=".*/file-series_file_series_1.*/.*0001_000.*/.*.txt",
                        mime_type="text/plain",
                        size_bytes=8,
                    ),
                ),
            ],
            id="tc23",
        ),
        pytest.param(
            "file-series/file_series_1",
            "file_series",
            {},
            [
                (
                    0,
                    FileMatcher(
                        path_pattern=".*/file-series_file_series_1.*/.*0000_000.*/.*.txt",
                        mime_type="text/plain",
                        size_bytes=8,
                    ),
                ),
                (
                    1,
                    FileMatcher(
                        path_pattern=".*/file-series_file_series_1.*/.*0001_000.*/.*.txt",
                        mime_type="text/plain",
                        size_bytes=8,
                    ),
                ),
                (
                    2,
                    FileMatcher(
                        path_pattern=".*/file-series_file_series_1.*/.*0002_000.*/.*.txt",
                        mime_type="text/plain",
                        size_bytes=8,
                    ),
                ),
            ],
            id="tc24",
        ),
        pytest.param(
            "file-series/file_series_1",
            "file_series",
            {"tail_limit": 2},
            [
                (
                    1,
                    FileMatcher(
                        path_pattern=".*/file-series_file_series_1.*/.*0001_000.*/.*.txt",
                        mime_type="text/plain",
                        size_bytes=8,
                    ),
                ),
                (
                    2,
                    FileMatcher(
                        path_pattern=".*/file-series_file_series_1.*/.*0002_000.*/.*.txt",
                        mime_type="text/plain",
                        size_bytes=8,
                    ),
                ),
            ],
            id="tc25",
        ),
        pytest.param("file-series/file_series_1", "file_series", {"tail_limit": 0}, [], id="tc26"),
        # Additional histogram series edge cases
        pytest.param(
            "metrics/histograms_1",
            "histogram_series",
            {"step_range": (10, None)},
            [],
            id="tc27",
        ),
        pytest.param(
            "metrics/histograms_1",
            "histogram_series",
            {"step_range": (3, 3)},
            [
                (3, HistogramMatcher(edges=[11.0, 12.0, 13.0, 14.0], values=[10.0, 20.0, 30.0])),
            ],
            id="tc28",
        ),
        # Additional file series edge cases
        pytest.param(
            "file-series/file_series_1",
            "file_series",
            {"step_range": (10, None)},
            [],
            id="tc29",
        ),
        pytest.param(
            "file-series/file_series_1",
            "file_series",
            {"step_range": (2, 2)},
            [
                (
                    2,
                    FileMatcher(
                        path_pattern=".*/file-series_file_series_1.*/.*0002_000.*/.*.txt",
                        mime_type="text/plain",
                        size_bytes=8,
                    ),
                ),
            ],
            id="tc30",
        ),
        # Additional string series edge cases
        pytest.param(
            "metrics/strfoobar_2",
            "string_series",
            {"step_range": (8, None)},
            [(8, "string-2-8"), (9, "string-2-9")],
            id="tc31",
        ),
        pytest.param(
            "metrics/strfoobar_2",
            "string_series",
            {"step_range": (None, 0)},
            [(0, "string-2-0")],
            id="tc32",
        ),
        pytest.param(
            "metrics/strfoobar_2",
            "string_series",
            {"tail_limit": 1},
            [(9, "string-2-9")],
            id="tc33",
        ),
    ],
)
def test_fetch_series_values_single_series(
    client,
    project_1,
    attribute_name,
    attribute_type,
    additional_kws,
    expected_values,
):
    # given
    sys_id = get_sys_id_for_run(client, project_1.project_identifier, project_1.ingested_runs[0].run_id)
    run_definition = RunAttributeDefinition(
        RunIdentifier(project_1.project_identifier, sys_id),
        AttributeDefinition(attribute_name, attribute_type),
    )

    # when
    series = extract_pages(
        fetch_series_values(
            client,
            [run_definition],
            include_inherited=False,
            container_type=ContainerType.RUN,
            **additional_kws,
        )
    )

    # then
    if not expected_values:
        assert series == []
    else:
        assert len(series) == 1
        assert series[0][0] == run_definition
        _, values = series[0]

        # Fun fact: with tail_limit, the order of returned values is reversed
        values = sorted(values)
        assert len(values) == len(expected_values)

        for i, (expected_step, expected_value) in enumerate(expected_values):
            (step, value, timestamp) = values[i]

            assert step == expected_step
            assert value == expected_value
            # Don't check the timestamp
