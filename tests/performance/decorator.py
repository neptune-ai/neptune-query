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
class PerformanceTestCaseSpec:
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

    def get_params_human(self):
        if all(type(value) in [float, int] for value in self.params.values()):
            return ", ".join(f"{key}={value}" for key, value in sorted(self.params.items()))
        return self.get_params_json()


def expected_benchmark(*multiple_cases: dict, **single_case: dict):
    def wrapper(fn):
        specs = []
        param_keys = {}

        all_cases = multiple_cases or [single_case]

        for case in all_cases:
            case_param_keys = {k for k in case.keys() if k not in ("min_p0", "max_p80", "max_p100")}
            if not param_keys:
                param_keys = case_param_keys

            if case_param_keys != param_keys:
                raise ValueError(
                    "All listed cases in expected_benchmark must have the same parameter keys."
                    f"Expected {param_keys}, got {case_param_keys}"
                )

            specs.append(
                PerformanceTestCaseSpec(
                    fn_name=fn.__name__,
                    params={k: case[k] for k in param_keys},
                    min_p0=case.get("min_p0"),
                    max_p80=case.get("max_p80"),
                    max_p100=case.get("max_p100"),
                )
            )

        pytest.mark.parametrize(
            ",".join(param_keys),
            [spec.get_params_for_parametrize() for spec in specs],
        )(fn)
        fn.__expected_benchmark_specs = specs
        return fn

    return wrapper
