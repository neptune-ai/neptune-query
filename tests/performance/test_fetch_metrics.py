from dataclasses import dataclass
from typing import (
    Any,
    Union,
)

import pytest

from neptune_query import fetch_metrics
from tests.performance.test_helpers import PerfRequestBuilder


@dataclass
class Scenario:
    experiments_count: int
    attribute_definitions_count: int
    metric_existence_probability: float
    steps_count_range_per_metric: tuple[int, int]

    # Represents exact or approximate expected number of data points
    # It's a sanity check to ensure we fetched the expected amount of data
    expected_points: Union[int, Any]

    def to_pytest_param(self, name: str, timeout: float):
        return pytest.param(self, id=name, marks=pytest.mark.timeout(timeout, func_only=True))


@pytest.mark.parametrize(
    "scenario",
    [
        Scenario(
            experiments_count=100_000,
            attribute_definitions_count=5,
            metric_existence_probability=0.1,
            steps_count_range_per_metric=(1, 1),
            expected_points=pytest.approx(50_000, rel=0.05),  # the result is probabilistic
        ).to_pytest_param(name="sparse-metric-match-exp-pagination-55k-points", timeout=8),
        Scenario(
            experiments_count=100_000,
            attribute_definitions_count=50,
            metric_existence_probability=0.1,
            steps_count_range_per_metric=(1, 1),
            expected_points=pytest.approx(500_000, rel=0.05),  # the result is probabilistic
        ).to_pytest_param(name="sparse-metric-match-exp-pagination-55k-points", timeout=65),
        Scenario(
            experiments_count=1_000,
            attribute_definitions_count=1_000,
            metric_existence_probability=0.01,
            steps_count_range_per_metric=(1, 1),
            expected_points=pytest.approx(10_000, rel=0.05),  # the result is probabilistic
        ).to_pytest_param(name="sparse-metric-match-10k-points", timeout=17),
        Scenario(
            experiments_count=1_000,
            attribute_definitions_count=1_000,
            metric_existence_probability=1.0,
            steps_count_range_per_metric=(1, 1),
            expected_points=1_000_000,
        ).to_pytest_param(name="dense-metric-match-1M-points", timeout=17),
        Scenario(
            experiments_count=1_000,
            attribute_definitions_count=100,
            metric_existence_probability=1.0,
            steps_count_range_per_metric=(1_000, 1_000),
            expected_points=100_000_000,
        ).to_pytest_param(name="dense-metric-match-100M-points", timeout=110),
    ],
)
def test_sparse_attribute_match(scenario, http_client):
    perf_request = (
        PerfRequestBuilder()
        .with_search_leaderboard_entries(
            attributes={"sys/name": "string", "sys/id": "string"},
            total_entries=scenario.experiments_count,
            latency_range_ms=(20, 30),
        )
        .with_query_attribute_definitions(
            attribute_types=["float_series"],
            total_definitions=scenario.attribute_definitions_count,
            latency_range_ms=(20, 30),
        )
        .with_multiple_float_series_values(
            seed=42,
            existence_probability=scenario.metric_existence_probability,
            series_cardinality_policy="uniform",
            series_cardinality_uniform_range=scenario.steps_count_range_per_metric,
            latency_range_ms=(10, 20),
        )
    )

    http_client.set_x_perf_request_header(value=perf_request.build())

    metrics_df = fetch_metrics(
        project="workspace/project",
        experiments=".*",
        attributes=".*",
    )

    non_nan_values = metrics_df.count().sum()
    assert non_nan_values == scenario.expected_points
