import json
import os
import warnings
from functools import (
    cache,
    wraps,
)

import pytest


@cache
def _get_benchmark_data() -> dict[tuple[str, str], dict[str, float]]:
    benchmark_output_file = os.getenv("BENCHMARK_VALIDATE_FILE")
    if benchmark_output_file is None:
        raise RuntimeError("Environment variable BENCHMARK_VALIDATE_FILE is not set.")

    stats = {}
    with open(benchmark_output_file) as f:
        data = json.load(f)
        for benchmark in data["benchmarks"]:
            name = benchmark["name"].split("[")[0]  # Remove params from name
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

        # Operate on the original function, not the wrapped one
        fn = wrapped_fn._original_fn

        if list(params.keys()) != fn._collected_param_keys:
            raise ValueError(
                "All parametrize_once decorators must have the same parameter keys. "
                f"Expected {wrapped_fn._collected_param_keys}, got {list(params.keys())}"
            )

        # Adjust the shape of collected_params for pytest.mark.parametrize
        param_values = list(params.values())
        if len(param_values) == 1:
            param_values = param_values[0]
        fn._collected_params.insert(0, param_values)

        fn_with_params = pytest.mark.parametrize(",".join(fn._collected_param_keys), fn._collected_params)(copy_fn(fn))
        fn_with_params._original_fn = fn

        if not os.getenv("BENCHMARK_VALIDATE_FILE"):
            return fn_with_params

        BENCHMARK_PERFORMANCE_FACTOR = float(os.getenv("BENCHMARK_PERFORMANCE_FACTOR", "1.0"))

        @wraps(fn_with_params)
        def validation(*args, **kwargs):
            if min_p0 is None or max_p80 is None or max_p100 is None:
                warnings.warn("Benchmark thresholds not set, skipping validation.", category=UserWarning)
                return

            perf_data = _get_benchmark_data()

            params_str = json.dumps(params)
            assert fn.__name__, params_str in perf_data
            stats = perf_data[fn.__name__, params_str]

            times = sorted(stats["data"])
            p0 = times[0]
            p80 = times[int(len(times) * 0.8)]
            p100 = times[-1]

            adjusted_min_p0 = min_p0 * BENCHMARK_PERFORMANCE_FACTOR
            adjusted_max_p80 = max_p80 * BENCHMARK_PERFORMANCE_FACTOR
            adjusted_max_p100 = max_p100 * BENCHMARK_PERFORMANCE_FACTOR

            detailed_msg = f"""

                Benchmark '{fn.__name__}' with params {params_str} results:

                0th percentile: {p0:.3f} s
                Unadjusted min_p0: {min_p0:.3f} s
                Adjusted (*) min_p0: {adjusted_min_p0:.3f} s

                80th percentile: {p80:.3f} s
                Unadjusted max_p80: {max_p80:.3f} s
                Adjusted (*) max_p80: {adjusted_max_p80:.3f} s

                100th percentile: {p100:.3f} s
                Unadjusted max_p100: {max_p100:.3f} s
                Adjusted (*) max_p100: {adjusted_max_p100:.3f} s

                (*) Use the environment variable "BENCHMARK_PERFORMANCE_FACTOR" to adjust the thresholds.

                BENCHMARK_PERFORMANCE_FACTOR=1.0 (default) is meant to represent GitHub Actions performance.
                Decrease this factor if your local machine is faster than GitHub Actions.

"""

            assert p0 >= adjusted_min_p0, f"p0 {p0:.3f} is less than expected {adjusted_min_p0:.3f}" + detailed_msg
            assert p80 <= adjusted_max_p80, f"p80 {p80:.3f} is more than expected {adjusted_max_p80:.3f}" + detailed_msg
            assert p100 <= adjusted_max_p100, f"max {p100:.3f} is more than expected {adjusted_max_p100:.3f}" + detailed_msg

        return validation

    return wrapper
