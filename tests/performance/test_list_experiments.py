from dataclasses import dataclass

import pytest

from neptune_query import list_experiments
from tests.performance.test_helpers import PerfRequestBuilder


@dataclass
class Scenario:
    experiments_count: int
    latency_range_ms: tuple[int, int]

    def to_pytest_param(self, name: str, timeout: float):
        return pytest.param(self, id=name, marks=pytest.mark.timeout(timeout, func_only=True))


@pytest.mark.parametrize(
    "scenario",
    [
        Scenario(
            experiments_count=29_000,
            latency_range_ms=(500, 500),
        ).to_pytest_param(name="29k-experiments-500ms-latency", timeout=2.5),
        Scenario(
            experiments_count=1_000_000,
            latency_range_ms=(30, 30),
        ).to_pytest_param(name="1M-experiments-30ms-latency", timeout=5),
    ],
)
def test_list_experiments(scenario, http_client):
    perf_request = PerfRequestBuilder().with_search_leaderboard_entries(
        attributes={"sys/name": "string", "sys/id": "string"},
        total_entries=scenario.experiments_count,
        latency_range_ms=scenario.latency_range_ms,
    )

    http_client.set_x_perf_request_header(value=perf_request.build())

    experiments = list_experiments(
        project="workspace/project",
        experiments=None,
    )

    assert len(experiments) == scenario.experiments_count
