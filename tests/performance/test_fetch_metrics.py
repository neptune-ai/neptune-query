from dataclasses import dataclass
from typing import (
    Any,
    Union,
)

import pytest
from humanize import metric

from neptune_query import fetch_metrics
from tests.performance.conftest import resolve_timeout
from tests.performance.test_helpers import PerfRequestBuilder


@dataclass
class Scenario:
    # Total number of experiments matching user's filter
    experiments_count: int
    # Total number of attribute definitions in selected experiments matching user's filter
    attribute_definitions_count: int
    # The chance that a particular (experiment, metric) pair exists
    metric_existence_probability: float
    # A range for the number of steps per metric (uniformly distributed)
    steps_count_range_per_metric: tuple[int, int]

    # Represents exact or approximate expected number of data points / rows / columns
    # It's a sanity check to ensure we fetched what we wanted
    expected_points: Union[int, Any]
    expected_columns: Union[int, Any]
    expected_rows: Union[int, Any]

    def _name(self):
        steps = (self.steps_count_range_per_metric[0] + self.steps_count_range_per_metric[1]) / 2
        points = self.expected_points if isinstance(self.expected_points, int) else self.expected_points.expected
        density = self.metric_existence_probability

        return "; ".join(
            f"{key}={value}"
            for key, value in {
                "exp_count": metric(self.experiments_count, precision=0),
                "attr_count": metric(self.attribute_definitions_count, precision=0),
                "density": f"{density:.0%}" if density >= 0.01 else f"{density:.2%}",
                "avg_steps": metric(steps, precision=0),
                "points": metric(points, precision=0),
            }.items()
        )

    def to_pytest_param(self, timeout: float):
        return pytest.param(self, id=self._name(), marks=pytest.mark.timeout(resolve_timeout(timeout), func_only=True))


@pytest.mark.parametrize(
    "scenario",
    [
        Scenario(
            experiments_count=100_000,
            attribute_definitions_count=5,
            metric_existence_probability=0.1,
            steps_count_range_per_metric=(1, 1),
            expected_points=pytest.approx(50_000, rel=0.05),
            expected_columns=5,
            # a row exists if at least one of the 5 metrics exists for that experiment
            expected_rows=pytest.approx(100_000 * (1 - 0.9**5), rel=0.05),  # 41k
        ).to_pytest_param(timeout=8),
        Scenario(
            experiments_count=100_000,
            attribute_definitions_count=50,
            metric_existence_probability=0.1,
            steps_count_range_per_metric=(1, 1),
            expected_points=pytest.approx(500_000, rel=0.05),
            expected_columns=50,
            # a row exists if at least one of the 50 metrics exists for that experiment
            expected_rows=pytest.approx(100_000 * (1 - 0.9**50), rel=0.05),  # 99k
        ).to_pytest_param(timeout=65),
        Scenario(
            experiments_count=1_000,
            attribute_definitions_count=1_000,
            metric_existence_probability=0.01,
            steps_count_range_per_metric=(1, 1),
            expected_points=pytest.approx(10_000, rel=0.05),
            expected_columns=1_000,
            expected_rows=1_000,
        ).to_pytest_param(timeout=17),
        Scenario(
            experiments_count=1_000,
            attribute_definitions_count=1_000,
            metric_existence_probability=1.0,
            steps_count_range_per_metric=(1, 1),
            expected_points=1_000_000,
            expected_columns=1_000,
            expected_rows=1_000,
        ).to_pytest_param(timeout=17),
        Scenario(
            experiments_count=1_000,
            attribute_definitions_count=100,
            metric_existence_probability=1.0,
            steps_count_range_per_metric=(1_000, 1_000),
            expected_points=100_000_000,
            expected_columns=100,
            expected_rows=1_000_000,
        ).to_pytest_param(timeout=110),
    ],
)
def test_fetch_metrics(scenario, http_client):
    perf_request = (
        PerfRequestBuilder()
        .with_search_leaderboard_entries(
            attributes={"sys/name": "string", "sys/id": "string"},
            total_entries=scenario.experiments_count,
            latency_range_ms=(20, 30),
        )
        .with_query_attribute_definitions(
            seed=42,
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
    assert metrics_df.shape == (scenario.expected_rows, scenario.expected_columns)
