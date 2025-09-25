import json
import os
import warnings
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
            params = json.dumps(benchmark["params"])
            stats[name, params] = benchmark["stats"]

    return stats


def expected_benchmark(
    min_p0: float = None,
    max_p80: float = None,
    max_p100: float = None,
    **params: object,
):
    def copy_fn(fn):
        @wraps(fn)
        def new_fn(*args, **kwargs):
            return fn(*args, **kwargs)

        return new_fn

    def wrapper(wrapped_fn):
        # Save the original function in case of multiple decorators
        if not hasattr(wrapped_fn, "_original_fn"):
            wrapped_fn._original_fn = wrapped_fn
            wrapped_fn._collected_params = []
            wrapped_fn._collected_param_keys = list(params.keys())
            wrapped_fn._expected_benchmarks = {}

        # Operate on the original function, not the wrapped one
        fn = wrapped_fn._original_fn

        if list(params.keys()) != fn._collected_param_keys:
            raise ValueError(
                "All expected_benchmark decorators must have the same parameter keys. "
                f"Expected {wrapped_fn._collected_param_keys}, got {list(params.keys())}"
            )

        # Adjust the shape of collected_params for pytest.mark.parametrize
        param_values = list(params.values())
        if len(param_values) == 1:
            param_values = param_values[0]
        fn._collected_params.insert(0, param_values)

        fn_with_params = pytest.mark.parametrize(",".join(fn._collected_param_keys), fn._collected_params)(copy_fn(fn))
        fn_with_params._original_fn = fn
        fn_with_params._expected_benchmarks[json.dumps(params)] = (min_p0, max_p80, max_p100)

        if not os.getenv("BENCHMARK_VALIDATE_FILE"):
            return fn_with_params

        performance_factor = float(os.getenv("BENCHMARK_PERFORMANCE_FACTOR", "1.0"))

        @wraps(fn_with_params)
        def validation(*_args, **kwargs):
            # Extract the actual parameters used in this test run
            my_params = {key: kwargs[key] for key in params.keys()}
            params_str = json.dumps(my_params)

            my_min_p0, my_max_p80, my_max_p100 = fn._expected_benchmarks[params_str]

            if my_min_p0 is None or my_max_p80 is None or my_max_p100 is None:
                warnings.warn("Benchmark thresholds not set, skipping validation.", category=UserWarning)
                return

            perf_data = _get_benchmark_data()

            assert fn.__name__, params_str in perf_data
            stats = perf_data[fn.__name__, params_str]

            times = sorted(stats["data"])
            p0 = times[0]
            p80 = times[int(len(times) * 0.8)]
            p100 = times[-1]

            adjusted_min_p0 = my_min_p0 * performance_factor
            adjusted_max_p80 = my_max_p80 * performance_factor
            adjusted_max_p100 = my_max_p100 * performance_factor

            p0_marker = "✓" if p0 >= adjusted_min_p0 else "✗"
            p80_marker = "✓" if p80 <= adjusted_max_p80 else "✗"
            p100_marker = "✓" if p100 <= adjusted_max_p100 else "✗"

            params_human = ", ".join(f"{k}={v!r}" for k, v in params.items())
            detailed_msg = f"""

                Benchmark '{fn.__name__}' with params {params_human} results:

                {p0_marker} 0th percentile:       {p0:.3f} s
                  Unadjusted min_p0:    {my_min_p0:.3f} s
                  Adjusted (*) min_p0:  {adjusted_min_p0:.3f} s

                {p80_marker} 80th percentile:       {p80:.3f} s
                  Unadjusted max_p80:    {my_max_p80:.3f} s
                  Adjusted (*) max_p80:  {adjusted_max_p80:.3f} s

                {p100_marker} 100th percentile:       {p100:.3f} s
                  Unadjusted max_p100:    {my_max_p100:.3f} s
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
                adjusted_min_p0_str = f"{adjusted_min_p0:.3f} (= {my_min_p0:.3f} * {performance_factor})"
                adjusted_max_p80_str = f"{adjusted_max_p80:.3f} (= {my_max_p80:.3f} * {performance_factor})"
                adjusted_max_p100_str = f"{adjusted_max_p100:.3f} (= {my_max_p100:.3f} * {performance_factor})"

            assert p0 >= adjusted_min_p0, f"p0 {p0:.3f} is less than expected {adjusted_min_p0_str}" + detailed_msg
            assert p80 <= adjusted_max_p80, f"p80 {p80:.3f} is more than expected {adjusted_max_p80_str}" + detailed_msg
            assert p100 <= adjusted_max_p100, (
                f"p100 {p100:.3f} is more than expected {adjusted_max_p100_str}" + detailed_msg
            )

        return validation

    return wrapper
