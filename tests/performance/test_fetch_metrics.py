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
    # A range for the number of steps per metric (uniformly distributed);
    # If a single int is provided, all metrics have that many steps
    steps_count_per_metric: Union[int, tuple[int, int]]

    # Represents exact or approximate expected number of data points / rows / columns
    # It's a sanity check to ensure we fetched what we wanted
    expected_points: Union[int, Any]
    expected_columns: Union[int, Any]
    expected_rows: Union[int, Any]

    @property
    def steps_range_per_metric(self):
        return (
            self.steps_count_per_metric
            if isinstance(self.steps_count_per_metric, tuple)
            else (self.steps_count_per_metric, self.steps_count_per_metric)
        )

    @property
    def name(self):
        steps = (self.steps_range_per_metric[0] + self.steps_range_per_metric[1]) / 2
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
        return pytest.param(self, id=self.name, marks=pytest.mark.timeout(resolve_timeout(timeout), func_only=True))


@pytest.mark.parametrize(
    "scenario",
    [
        ##########################
        # 1 experiment, 1 metric #
        ##########################
        ## 1M steps
        Scenario(
            experiments_count=1,
            attribute_definitions_count=1,
            metric_existence_probability=1.0,
            steps_count_per_metric=1_000_000,
            expected_points=1_000_000,
            expected_columns=1,
            expected_rows=1_000_000,
        ).to_pytest_param(timeout=600),
        ## 10M steps
        Scenario(
            experiments_count=1,
            attribute_definitions_count=1,
            metric_existence_probability=1.0,
            steps_count_per_metric=10_000_000,
            expected_points=10_000_000,
            expected_columns=1,
            expected_rows=10_000_000,
        ).to_pytest_param(timeout=600),
        ##############################
        # 1 experiment, 100k metrics #
        ##############################
        ## 1 step
        Scenario(
            experiments_count=1,
            attribute_definitions_count=100_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=1,
            expected_points=100_000,
            expected_columns=100_000,
            expected_rows=1,
        ).to_pytest_param(timeout=600),
        ## 10 steps
        Scenario(
            experiments_count=1,
            attribute_definitions_count=100_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=10,
            expected_points=1_000_000,
            expected_columns=100_000,
            expected_rows=10,
        ).to_pytest_param(timeout=600),
        ## 100 steps
        Scenario(
            experiments_count=1,
            attribute_definitions_count=100_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=100,
            expected_points=10_000_000,
            expected_columns=100_000,
            expected_rows=100,
        ).to_pytest_param(timeout=600),
        ## 1k steps
        Scenario(
            experiments_count=1,
            attribute_definitions_count=100_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=1_000,
            expected_points=100_000_000,
            expected_columns=100_000,
            expected_rows=1_000,
        ).to_pytest_param(timeout=600),
        ############################
        # 1 experiment, 1M metrics #
        ############################
        ## 1 step
        Scenario(
            experiments_count=1,
            attribute_definitions_count=1_000_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=1,
            expected_points=1_000_000,
            expected_columns=1_000_000,
            expected_rows=1,
        ).to_pytest_param(timeout=600),
        ## 10 steps
        Scenario(
            experiments_count=1,
            attribute_definitions_count=1_000_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=10,
            expected_points=10_000_000,
            expected_columns=1_000_000,
            expected_rows=10,
        ).to_pytest_param(timeout=600),
        ## 100 steps
        Scenario(
            experiments_count=1,
            attribute_definitions_count=1_000_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=100,
            expected_points=100_000_000,
            expected_columns=1_000_000,
            expected_rows=100,
        ).to_pytest_param(timeout=600),
        #################################################################
        # 2 experiments, 50k metrics per experiment (75k metrics total) #
        #################################################################
        ## 1 step
        Scenario(
            experiments_count=2,
            attribute_definitions_count=100_000,
            metric_existence_probability=0.5,
            steps_count_per_metric=1,
            expected_points=pytest.approx(100_000, rel=0.05),
            expected_columns=pytest.approx(75_000, rel=0.05),
            expected_rows=2,
        ).to_pytest_param(timeout=600),
        ## 10 steps
        Scenario(
            experiments_count=2,
            attribute_definitions_count=100_000,
            metric_existence_probability=0.5,
            steps_count_per_metric=10,
            expected_points=pytest.approx(1_000_000, rel=0.05),
            expected_columns=pytest.approx(75_000, rel=0.05),
            expected_rows=20,
        ).to_pytest_param(timeout=600),
        ## 100 steps
        Scenario(
            experiments_count=2,
            attribute_definitions_count=100_000,
            metric_existence_probability=0.5,
            steps_count_per_metric=100,
            expected_points=pytest.approx(10_000_000, rel=0.05),
            expected_columns=pytest.approx(75_000, rel=0.05),
            expected_rows=200,
        ).to_pytest_param(timeout=600),
        ## 500 steps
        Scenario(
            experiments_count=2,
            attribute_definitions_count=100_000,
            metric_existence_probability=0.5,
            steps_count_per_metric=500,
            expected_points=pytest.approx(50_000_000, rel=0.05),
            expected_columns=pytest.approx(75_000, rel=0.05),
            expected_rows=1_000,
        ).to_pytest_param(timeout=600),
        ##################################################################
        # 10 experiments, 10k metrics per experiment (65k metrics total) #
        ##################################################################
        ## 1 step
        Scenario(
            experiments_count=10,
            attribute_definitions_count=100_000,
            metric_existence_probability=0.1,
            steps_count_per_metric=1,
            expected_points=pytest.approx(100_000, rel=0.05),
            expected_columns=pytest.approx(65_000, rel=0.05),
            expected_rows=10,
        ).to_pytest_param(timeout=15),
        ## 10 steps
        Scenario(
            experiments_count=10,
            attribute_definitions_count=100_000,
            metric_existence_probability=0.1,
            steps_count_per_metric=10,
            expected_points=pytest.approx(1_000_000, rel=0.05),
            expected_columns=pytest.approx(65_000, rel=0.05),
            expected_rows=100,
        ).to_pytest_param(timeout=15),
        ## 100 steps
        Scenario(
            experiments_count=10,
            attribute_definitions_count=100_000,
            metric_existence_probability=0.1,
            steps_count_per_metric=100,
            expected_points=pytest.approx(10_000_000, rel=0.05),
            expected_columns=pytest.approx(65_000, rel=0.05),
            expected_rows=1_000,
        ).to_pytest_param(timeout=25),
        ## 1k steps
        Scenario(
            experiments_count=10,
            attribute_definitions_count=100_000,
            metric_existence_probability=0.1,
            steps_count_per_metric=1_000,
            expected_points=pytest.approx(100_000_000, rel=0.05),
            expected_columns=pytest.approx(65_000, rel=0.05),
            expected_rows=10_000,
        ).to_pytest_param(timeout=115),
        ####################################################################
        # 10 experiments, 100k metrics per experiment (100k metrics total) #
        ####################################################################
        ## 1 step
        Scenario(
            experiments_count=10,
            attribute_definitions_count=100_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=1,
            expected_points=pytest.approx(1_000_000, rel=0.05),
            expected_columns=pytest.approx(100_000, rel=0.05),
            expected_rows=10,
        ).to_pytest_param(timeout=17),
        ## 10 steps
        Scenario(
            experiments_count=10,
            attribute_definitions_count=100_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=10,
            expected_points=pytest.approx(10_000_000, rel=0.05),
            expected_columns=pytest.approx(100_000, rel=0.05),
            expected_rows=100,
        ).to_pytest_param(timeout=24),
        ## 100 steps
        Scenario(
            experiments_count=10,
            attribute_definitions_count=100_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=100,
            expected_points=pytest.approx(100_000_000, rel=0.05),
            expected_columns=pytest.approx(100_000, rel=0.05),
            expected_rows=1_000,
        ).to_pytest_param(timeout=120),
        # ##################################################################
        # #############     Doesn't work - too slow    #####################
        # # 1k experiments, 1k metrics per experiment (100k metrics total) #
        # ##################################################################
        # ## 1 step
        # Scenario(
        #     experiments_count=1_000,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=0.01,
        #     steps_count_per_metric=1,
        #     expected_points=pytest.approx(1_000_000, rel=0.05),
        #     expected_columns=pytest.approx(100_000, rel=0.05),
        #     expected_rows=1_000,
        # ).to_pytest_param(timeout=600),
        # ## 10 steps
        # Scenario(
        #     experiments_count=1_000,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=0.01,
        #     steps_count_per_metric=10,
        #     expected_points=pytest.approx(10_000_000, rel=0.05),
        #     expected_columns=pytest.approx(100_000, rel=0.05),
        #     expected_rows=10_000,
        # ).to_pytest_param(timeout=600),
        # ## 100 steps
        # Scenario(
        #     experiments_count=10,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=0.01,
        #     steps_count_per_metric=100,
        #     expected_points=pytest.approx(100_000_000, rel=0.05),
        #     expected_columns=pytest.approx(100_000, rel=0.05),
        #     expected_rows=100_000,
        # ).to_pytest_param(timeout=600),
        ################################################################
        # 1k experiments, 1k metrics per experiment (1k metrics total) #
        ################################################################
        ## 1 step
        Scenario(
            experiments_count=1_000,
            attribute_definitions_count=1_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=1,
            expected_points=1_000_000,
            expected_columns=1_000,
            expected_rows=1_000,
        ).to_pytest_param(timeout=17),
        ## 10 steps
        Scenario(
            experiments_count=1_000,
            attribute_definitions_count=1_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=10,
            expected_points=10_000_000,
            expected_columns=1_000,
            expected_rows=10_000,
        ).to_pytest_param(timeout=23),
        ## 100 steps
        Scenario(
            experiments_count=1_000,
            attribute_definitions_count=1_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=100,
            expected_points=100_000_000,
            expected_columns=1_000,
            expected_rows=100_000,
        ).to_pytest_param(timeout=115),
        ##################################################################
        # 1k experiments, 10k metrics per experiment (10k metrics total) #
        ##################################################################
        ## 1 step
        Scenario(
            experiments_count=1_000,
            attribute_definitions_count=10_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=1,
            expected_points=10_000_000,
            expected_columns=10_000,
            expected_rows=1_000,
        ).to_pytest_param(timeout=130),
        ## 10 steps
        Scenario(
            experiments_count=1_000,
            attribute_definitions_count=10_000,
            metric_existence_probability=1.0,
            steps_count_per_metric=10,
            expected_points=100_000_000,
            expected_columns=10_000,
            expected_rows=10_000,
        ).to_pytest_param(timeout=220),
        # ###################################################################
        # #############     Doesn't work - too slow    ######################
        # # 1k experiments, 10k metrics per experiment (100k metrics total) #
        # ###################################################################
        # ## 1 step
        # Scenario(
        #     experiments_count=1_000,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=0.1,
        #     steps_count_per_metric=1,
        #     expected_points=10_000_000,
        #     expected_columns=pytest.approx(100_000, rel=0.05),
        #     expected_rows=1_000,
        # ).to_pytest_param(timeout=600),
        # ## 10 steps
        # Scenario(
        #     experiments_count=1_000,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=0.1,
        #     steps_count_per_metric=10,
        #     expected_points=100_000_000,
        #     expected_columns=pytest.approx(100_000, rel=0.05),
        #     expected_rows=10_000,
        # ).to_pytest_param(timeout=600),
        # ####################################################################
        # #############     Doesn't work - too slow    #######################
        # # 1k experiments, 100k metrics per experiment (100k metrics total) #
        # ####################################################################
        # ## 1 step
        # Scenario(
        #     experiments_count=1_000,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=1.0,
        #     steps_count_per_metric=1,
        #     expected_points=100_000_000,
        #     expected_columns=100_000,
        #     expected_rows=1_000,
        # ).to_pytest_param(timeout=600),
        # ###################################################################
        # #############     Doesn't work - too slow    ######################
        # # 10k experiments, 1k metrics per experiment (100k metrics total) #
        # ###################################################################
        # ## 1 step
        # Scenario(
        #     experiments_count=10_000,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=0.01,
        #     steps_count_per_metric=1,
        #     expected_points=10_000_000,
        #     expected_columns=pytest.approx(100_000, rel=0.05),
        #     expected_rows=10_000,
        # ).to_pytest_param(timeout=600),
        # ## 10 steps
        # Scenario(
        #     experiments_count=10_000,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=0.01,
        #     steps_count_per_metric=10,
        #     expected_points=100_000_000,
        #     expected_columns=pytest.approx(100_000, rel=0.05),
        #     expected_rows=100_000,
        # ).to_pytest_param(timeout=600),
        # ####################################################################
        # # #############     Doesn't work - too slow    #####################
        # # 10k experiments, 10k metrics per experiment (100k metrics total) #
        # ####################################################################
        # ## 1 step
        # Scenario(
        #     experiments_count=10_000,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=0.1,
        #     steps_count_per_metric=1,
        #     expected_points=100_000_000,
        #     expected_columns=pytest.approx(100_000, rel=0.05),
        #     expected_rows=10_000,
        # ).to_pytest_param(timeout=600),
        #################################################################
        # 100k experiments, 10 metrics per experiment (10 metric total) #
        #################################################################
        ## 1 step
        Scenario(
            experiments_count=100_000,
            attribute_definitions_count=10,
            metric_existence_probability=1.0,
            steps_count_per_metric=1,
            expected_points=1_000_000,
            expected_columns=pytest.approx(10, rel=0.05),
            expected_rows=100_000,
        ).to_pytest_param(timeout=18),
        ## 10 steps
        Scenario(
            experiments_count=100_000,
            attribute_definitions_count=10,
            metric_existence_probability=1.0,
            steps_count_per_metric=10,
            expected_points=10_000_000,
            expected_columns=pytest.approx(10, rel=0.05),
            expected_rows=1_000_000,
        ).to_pytest_param(timeout=24),
        ## 100 steps
        Scenario(
            experiments_count=100_000,
            attribute_definitions_count=10,
            metric_existence_probability=1.0,
            steps_count_per_metric=100,
            expected_points=100_000_000,
            expected_columns=pytest.approx(10, rel=0.05),
            expected_rows=10_000_000,
        ).to_pytest_param(timeout=115),
        # #################################################################
        # # #############     Doesn't work - too slow    ##################
        # # 100k experiments, 1 metric per experiment (63k metric total) #
        # #################################################################
        # ## 1 step
        # Scenario(
        #     experiments_count=100_000,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=0.00001,
        #     steps_count_per_metric=1,
        #     expected_points=100_000,
        #     expected_columns=pytest.approx(63_000, rel=0.05),
        #     expected_rows=pytest.approx(74_000, rel=0.05),
        # ).to_pytest_param(timeout=600),
        # ## 10 steps
        # Scenario(
        #     experiments_count=100_000,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=0.00001,
        #     steps_count_per_metric=10,
        #     expected_points=1_000_000,
        #     expected_columns=pytest.approx(63_000, rel=0.05),
        #     expected_rows=pytest.approx(74_000, rel=0.05),
        # ).to_pytest_param(timeout=600),
        # ## 100 steps
        # Scenario(
        #     experiments_count=100_000,
        #     attribute_definitions_count=100_000,
        #     metric_existence_probability=0.00001,
        #     steps_count_per_metric=100,
        #     expected_points=100_000_000,
        #     expected_columns=pytest.approx(63_000, rel=0.05),
        #     expected_rows=pytest.approx(74_000, rel=0.05),
        # ).to_pytest_param(timeout=600),
        ################################################################
        # 1M experiments, 1 metric per experiment (1 metric total) #
        ################################################################
        ## 1 step
        Scenario(
            experiments_count=1_000_000,
            attribute_definitions_count=1,
            metric_existence_probability=1.0,
            steps_count_per_metric=1,
            expected_points=1_000_000,
            expected_columns=1,
            expected_rows=1_000_000,
        ).to_pytest_param(timeout=19),
        ## 10 steps
        Scenario(
            experiments_count=1_000_000,
            attribute_definitions_count=1,
            metric_existence_probability=1.0,
            steps_count_per_metric=10,
            expected_points=10_000_000,
            expected_columns=1,
            expected_rows=10_000_000,
        ).to_pytest_param(timeout=31),
        ## 100 steps
        Scenario(
            experiments_count=1_000_000,
            attribute_definitions_count=1,
            metric_existence_probability=1.0,
            steps_count_per_metric=100,
            expected_points=100_000_000,
            expected_columns=1,
            expected_rows=100_000_000,
        ).to_pytest_param(timeout=200),
        # ################################################################
        # # # #############     Doesn't work - too slow    ###############
        # # 1M experiments, 100 metric per experiment (100 metric total) #
        # ################################################################
        # ## 1 step
        # Scenario(
        #     experiments_count=1_000_000,
        #     attribute_definitions_count=100,
        #     metric_existence_probability=1.0,
        #     steps_count_per_metric=1,
        #     expected_points=100_000_000,
        #     expected_columns=100,
        #     expected_rows=1_000_000,
        # ).to_pytest_param(timeout=600),
    ],
)
def test_fetch_metrics(scenario, http_client, record_property):
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
            series_cardinality_uniform_range=scenario.steps_range_per_metric,
            latency_range_ms=(10, 20),
        )
    )

    http_client.set_x_perf_request_header(value=perf_request.build())
    http_client.set_scenario_name_header(scenario_name=scenario.name)

    metrics_df = fetch_metrics(
        project="workspace/project",
        experiments=".*",
        attributes=".*",
    )

    record_property("dataframe_memory_usage", metrics_df.memory_usage(deep=True).sum())

    non_nan_values = metrics_df.count().sum()
    assert non_nan_values == scenario.expected_points
    assert metrics_df.shape == (scenario.expected_rows, scenario.expected_columns)
