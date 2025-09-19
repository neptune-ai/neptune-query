import pytest
from performance.test_helpers import PerfRequestBuilder

from neptune_query import list_experiments


@pytest.mark.timeout(2.5)
def test_list_experiments_latency(http_client):
    expected_experiments_count = 29_000

    perf_request = (
        PerfRequestBuilder()
        .with_latency(min_ms=400, max_ms=500)
        .with_search_leaderboard_entries(
            attributes={"sys/name": "string", "sys/id": "string"}, total_entries=expected_experiments_count
        )
    )

    http_client.set_x_perf_request_header(value=perf_request.build())

    experiments = list_experiments(
        project="workspace/project",
        experiments=None,
    )

    assert len(experiments) == expected_experiments_count


@pytest.mark.timeout(5)
def test_list_experiments_1m(http_client):
    expected_experiments_count = 1_000_000

    perf_request = (
        PerfRequestBuilder()
        .with_latency(min_ms=20, max_ms=30)
        .with_search_leaderboard_entries(
            attributes={"sys/name": "string", "sys/id": "string"}, total_entries=expected_experiments_count
        )
    )

    http_client.set_x_perf_request_header(value=perf_request.build())

    experiments = list_experiments(
        project="workspace/project",
        experiments=None,
    )

    assert len(experiments) == expected_experiments_count
