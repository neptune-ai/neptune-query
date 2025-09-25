import json
import os
import warnings
from dataclasses import dataclass
from functools import (
    cache,
    wraps,
)
from typing import Any

import pytest


@cache
def _get_benchmark_data() -> dict[tuple[str, str], dict[str, Any]]:
    benchmark_output_file = os.getenv("BENCHMARK_VALIDATE_FILE")
    if benchmark_output_file is None:
        raise RuntimeError("Environment variable BENCHMARK_VALIDATE_FILE is not set.")

    stats = {}
    with open(benchmark_output_file) as f:
        data = json.load(f)
        for benchmark in data["benchmarks"]:
            name = benchmark["name"].split("[")[0]  # Remove params from the name
            params = json.dumps(benchmark["params"], sort_keys=True)
            stats[name, params] = benchmark["stats"]

    return stats


@dataclass
class PerfomanceCase:
    fn_name: str
    params: dict[str, Any]
    min_p0: float | None
    max_p80: float | None
    max_p100: float | None

    def get_params_for_parametrize(self):
        if len(self.params) == 1:
            return list(self.params.values())[0]
        return tuple(self.params.values())

    def get_params_json(self):
        return json.dumps(self.params, sort_keys=True)


def expected_benchmark(*multiple_cases: dict, **single_case: dict):
    def wrapper(fn):
        perf_cases = []
        param_keys = {}

        all_cases = multiple_cases or [single_case]

        for case in all_cases:
            case_param_keys = {k for k in case.keys() if k not in ("min_p0", "max_p80", "max_p100")}
            if not param_keys:
                param_keys = case_param_keys

            if case_param_keys != param_keys:
                raise ValueError(
                    "All expected_benchmark decorators must have the same parameter keys."
                    f"Expected {param_keys}, got {case_param_keys}"
                )

            perf_cases.append(
                PerfomanceCase(
                    fn_name=fn.__name__,
                    params={k: case[k] for k in param_keys},
                    min_p0=case.get("min_p0"),
                    max_p80=case.get("max_p80"),
                    max_p100=case.get("max_p100"),
                )
            )

        if not os.getenv("BENCHMARK_VALIDATE_FILE"):
            pytest.mark.parametrize(
                ",".join(param_keys),
                [case.get_params_for_parametrize() for case in perf_cases],
            )(fn)
            return fn

        performance_factor = float(os.getenv("BENCHMARK_PERFORMANCE_FACTOR", "1.0"))

        @wraps(fn)
        def validation(*args, **kwargs):
            # Find the matching performance case
            perf_case: PerfomanceCase | None = None
            for case in perf_cases:
                if all(kwargs.get(k) == v for k, v in case.params.items()):
                    perf_case = case
                    break

            assert perf_case is not None, "No matching performance case found for the given parameters."

            # Extract the actual parameters used in this test run
            if perf_case.min_p0 is None or perf_case.max_p80 is None or perf_case.max_p100 is None:
                warnings.warn("Benchmark thresholds not set, skipping validation.", category=UserWarning)
                return

            perf_data = _get_benchmark_data()

            assert perf_case.fn_name, perf_case.get_params_json() in perf_data
            stats = perf_data[perf_case.fn_name, perf_case.get_params_json()]

            times = sorted(stats["data"])
            p0 = times[0]
            p80 = times[int(len(times) * 0.8)]
            p100 = times[-1]

            adjusted_min_p0 = perf_case.min_p0 * performance_factor
            adjusted_max_p80 = perf_case.max_p80 * performance_factor
            adjusted_max_p100 = perf_case.max_p100 * performance_factor

            p0_marker = "✓" if p0 >= adjusted_min_p0 else "✗"
            p80_marker = "✓" if p80 <= adjusted_max_p80 else "✗"
            p100_marker = "✓" if p100 <= adjusted_max_p100 else "✗"

            params_human = ", ".join(f"{k}={v!r}" for k, v in perf_case.params.items())
            detailed_msg = f"""

                Benchmark '{perf_case.fn_name}' with params {params_human} results:

                {p0_marker} 0th percentile:       {p0:.3f} s
                  Unadjusted min_p0:    {perf_case.min_p0:.3f} s
                  Adjusted (*) min_p0:  {adjusted_min_p0:.3f} s

                {p80_marker} 80th percentile:       {p80:.3f} s
                  Unadjusted max_p80:    {perf_case.max_p80:.3f} s
                  Adjusted (*) max_p80:  {adjusted_max_p80:.3f} s

                {p100_marker} 100th percentile:       {p100:.3f} s
                  Unadjusted max_p100:    {perf_case.max_p100:.3f} s
                  Adjusted (*) max_p100:  {adjusted_max_p100:.3f} s

                (*) Use the environment variable "BENCHMARK_PERFORMANCE_FACTOR" to adjust the thresholds.

                BENCHMARK_PERFORMANCE_FACTOR=1.0 (default) is meant to represent GitHub Actions performance.
                Decrease this factor if your local machine is faster than GitHub Actions.

"""

            if performance_factor == 1.0:
                adjusted_min_p0_str = f"{adjusted_min_p0:.3f}"
                adjusted_max_p80_str = f"{adjusted_max_p80:.3f}"
                adjusted_max_p100_str = f"{adjusted_max_p100:.3f}"
            else:
                adjusted_min_p0_str = f"{adjusted_min_p0:.3f} (= {perf_case.min_p0:.3f} * {performance_factor})"
                adjusted_max_p80_str = f"{adjusted_max_p80:.3f} (= {perf_case.max_p80:.3f} * {performance_factor})"
                adjusted_max_p100_str = f"{adjusted_max_p100:.3f} (= {perf_case.max_p100:.3f} * {performance_factor})"

            assert p0 >= adjusted_min_p0, f"p0 {p0:.3f} is less than expected {adjusted_min_p0_str}" + detailed_msg
            assert p80 <= adjusted_max_p80, f"p80 {p80:.3f} is more than expected {adjusted_max_p80_str}" + detailed_msg
            assert p100 <= adjusted_max_p100, (
                f"p100 {p100:.3f} is more than expected {adjusted_max_p100_str}" + detailed_msg
            )

        pytest.mark.parametrize(
            ",".join(param_keys),
            [case.get_params_for_parametrize() for case in perf_cases],
        )(validation)

        return validation

    return wrapper
