from dataclasses import dataclass

import pytest
from humanize import metric

from neptune_query import list_experiments
from tests.performance_e2e.conftest import resolve_timeout
from tests.performance_e2e.test_helpers import PerfRequestBuilder


@dataclass
class Scenario:
    id: str
    experiments_count: int
    latency_range_ms: tuple[int, int]

    @property
    def name(self):
        return "; ".join(
            f"{key}={value}"
            for key, value in {
                "id": self.id,
                "exp_count": metric(self.experiments_count, precision=0),
                "latency_range_ms": self.latency_range_ms,
            }.items()
        )

    def to_pytest_param(self, timeout: float):
        return pytest.param(self, id=self.name, marks=pytest.mark.timeout(resolve_timeout(timeout), func_only=True))


@pytest.mark.parametrize(
    "scenario",
    [
        Scenario(
            id="le001",
            experiments_count=29_000,
            latency_range_ms=(500, 500),
        ).to_pytest_param(timeout=1.617),
        Scenario(
            id="le002",
            experiments_count=1_000_000,
            latency_range_ms=(30, 30),
        ).to_pytest_param(timeout=10.103),
    ],
)
def test_list_experiments(scenario, http_client):
    perf_request = PerfRequestBuilder().with_search_leaderboard_entries(
        attributes={"sys/name": "string", "sys/id": "string"},
        total_entries=scenario.experiments_count,
        latency_range_ms=scenario.latency_range_ms,
    )

    http_client.set_x_perf_request_header(value=perf_request.build())
    http_client.set_scenario_name_header(scenario_name=scenario.name)

    experiments = list_experiments(
        project="workspace/project",
        experiments=None,
    )

    assert len(experiments) == scenario.experiments_count
