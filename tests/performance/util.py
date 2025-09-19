import json
import os
import warnings
from functools import (
    cache,
    wraps,
)

from pytest_benchmark.logger import PytestBenchmarkWarning


@cache
def _get_benchmark_data() -> dict[str, dict[str, float]]:
    warnings.filterwarnings("ignore", category=PytestBenchmarkWarning)

    benchmark_output_file = os.getenv("BENCHMARK_OUTPUT_FILE")
    if benchmark_output_file is None:
        raise RuntimeError("Environment variable BENCHMARK_OUTPUT_FILE is not set.")

    stats = {}
    with open(benchmark_output_file) as f:
        data = json.load(f)
        for benchmark in data["benchmarks"]:
            name = benchmark["name"]
            stats[name] = benchmark["stats"]

    return stats


def expected_benchmark(
    min_p0: float,
    max_p80: float,
    max_p100: float,
):
    def wrapper(fn):
        if os.getenv("BENCHMARK_VALIDATE") != "1":
            return fn

        @wraps(fn)
        def validation(*args, **kwargs):
            perf_data = _get_benchmark_data()

            assert fn.__name__ in perf_data
            stats = perf_data[fn.__name__]

            times = sorted(stats["data"])
            p0 = times[0]
            p80 = times[int(len(times) * 0.8)]
            p100 = times[-1]

            assert p0 >= min_p0, f"p0 {p0} is less than expected {min_p0}"
            assert p80 <= max_p80, f"p80 {p80} is more than expected {max_p80}"
            assert p100 <= max_p100, f"max {p100} is more than expected {max_p100}"

        return validation

    return wrapper
