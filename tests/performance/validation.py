import importlib
import json
import os
import textwrap
from dataclasses import dataclass
from functools import (
    cache,
    cached_property,
)
from pathlib import Path
from typing import (
    Generator,
    Literal,
)

import pandas as pd
from junit_xml import (
    TestCase,
    TestSuite,
    to_xml_report_file,
)

from .decorator import PerformanceTestCaseSpec


def get_benchmark_spec(benchmark: dict) -> PerformanceTestCaseSpec:
    module_path = Path(benchmark["fullname"].split("::")[0])
    module_name = str(module_path.with_suffix("")).replace("/", ".")
    fn_name = benchmark["name"].split("[")[0]
    params = benchmark["params"]
    module = importlib.import_module(module_name)
    fn = getattr(module, fn_name)
    specs: PerformanceTestCaseSpec = fn.__expected_benchmark_specs
    for spec in specs:
        if spec.params == params:
            return spec
    raise ValueError(f"No matching spec found for benchmark {module_name}.{fn_name} with params {params}")


@cache
def get_performance_factor() -> float:
    return float(os.getenv("BENCHMARK_PERFORMANCE_FACTOR", "1.0"))


@dataclass
class BenchmarkTest:
    module_path: str
    fn_name: str
    params: str

    spec: PerformanceTestCaseSpec
    times: list[float]

    @cached_property
    def __times_sorted(self) -> list[float]:
        return sorted(self.times)

    @property
    def module_name(self) -> str:
        return self.module_path.split(".")[-1]

    @property
    def p0(self) -> float:
        return self.__times_sorted[0]

    @property
    def p80(self) -> float:
        return self.__times_sorted[int(len(self.__times_sorted) * 0.8)]

    @property
    def p100(self) -> float:
        return self.__times_sorted[-1]

    @property
    def p0_adjusted(self) -> float:
        return self.p0 * get_performance_factor()

    @property
    def p80_adjusted(self) -> float:
        return self.p80 * get_performance_factor()

    @property
    def p100_adjusted(self) -> float:
        return self.p100 * get_performance_factor()

    @property
    def p0_result(self) -> Literal["pass", "fail", "skip"]:
        if self.spec.min_p0 is None:
            return "skip"
        return "pass" if self.p0_adjusted > self.spec.min_p0 else "fail"

    @property
    def p80_result(self) -> Literal["pass", "fail", "skip"]:
        if self.spec.max_p80 is None:
            return "skip"
        return "pass" if self.p80_adjusted < self.spec.max_p80 else "fail"

    @property
    def p100_result(self) -> Literal["pass", "fail", "skip"]:
        if self.spec.max_p100 is None:
            return "skip"
        return "pass" if self.p100_adjusted < self.spec.max_p100 else "fail"

    @property
    def num_rounds(self) -> int:
        return len(self.times)

    @cached_property
    def result(self) -> Literal["pass", "fail", "skip"]:
        results = [self.p0_result, self.p80_result, self.p100_result]

        if results.count("fail") > 0:
            return "fail"

        if results.count("skip") == 3:
            return "skip"

        return "pass"


def get_benchmark_tests(benchmark_path: str | Path) -> Generator[BenchmarkTest, None, None]:
    with open(benchmark_path, "r") as f:
        report = json.load(f)

    for benchmark in report["benchmarks"]:
        module_path = Path(benchmark["fullname"].split("::")[0])
        module_path = str(module_path.with_suffix("")).replace("/", ".")
        fn_name = benchmark["name"].split("[")[0]

        spec = get_benchmark_spec(benchmark)

        yield BenchmarkTest(
            module_path=module_path,
            fn_name=fn_name,
            params=spec.get_params_human(),
            spec=spec,
            times=benchmark["stats"]["data"],
        )


def generate_test_suite(benchmark_path: str) -> TestSuite:
    test_cases = []
    for test in get_benchmark_tests(benchmark_path):
        if get_performance_factor() == 1.0:
            times = f"p0={test.p0:.3f}s, p80={test.p80:.3f}s, p100={test.p100:.3f}s"
        else:
            times = (
                f"p0_adj={test.p0_adjusted:.3f}s, "
                f"p80_adj={test.p80_adjusted:.3f}s, "
                f"p100_adj={test.p100_adjusted:.3f}s"
            )

        name = f"{test.fn_name} ({test.spec.get_params_human()}) {times}"

        tc = TestCase(name=name, classname=test.module_path, elapsed_sec=test.p80)

        if test.result == "fail":
            tc.add_failure_info("failed")
        elif test.result == "skip":
            tc.add_skipped_info("not measured")

        test_cases.append(tc)

    return TestSuite(name="BenchmarkResults", test_cases=test_cases)


def generate_junit_report(benchmark_path: Path | str, report_path: Path | str):
    test_suite = generate_test_suite(benchmark_path)
    with open(report_path, "w") as f:
        to_xml_report_file(f, [test_suite], prettyprint=True)


def generate_text_report(benchmark_path: str | Path) -> pd.DataFrame:
    tests = get_benchmark_tests(benchmark_path)

    # format times
    def ft(time: float | None):
        if time is None:
            return " (none)"
        else:
            return f"{time:6.3f}s"

    def marker(status: Literal["pass", "fail", "skip"], show_pass: bool = False):
        if status == "pass" and show_pass:
            return "✅️"
        elif status == "fail":
            return "❌️"
        else:
            return "  "

    data = []
    failed = False
    for test in tests:
        if test.result == "fail":
            failed = True

        spec = test.spec

        data.append(
            {
                "test": textwrap.dedent(
                    f"""
                    {marker(test.result, show_pass=True)} {test.module_name}
                         {test.fn_name}
                           {test.params}"""
                ).strip(),
                "p0": textwrap.dedent(
                    f"""
                    {marker(test.p0_result)} real: {ft(test.p0)}
                       adj*: {ft(test.p0_adjusted)}
                       min:  {ft(spec.min_p0)}"""
                ).strip(),
                "p80": textwrap.dedent(
                    f"""
                    {marker(test.p80_result)} real: {ft(test.p80)}
                       adj*: {ft(test.p80_adjusted)}
                       max:  {ft(spec.max_p80)}"""
                ).strip(),
                "p100": textwrap.dedent(
                    f"""
                    {marker(test.p100_result)} real: {ft(test.p100)}
                       adj*: {ft(test.p100_adjusted)}
                       max:  {ft(spec.max_p100)}"""
                ).strip(),
                "rounds": test.num_rounds,
            }
        )

    df = pd.DataFrame(data)

    out = df.to_markdown(tablefmt="grid", index=False)

    if failed or get_performance_factor() != 1.0:
        out += """

(*) Use the environment variable "BENCHMARK_PERFORMANCE_FACTOR" to adjust the thresholds.

BENCHMARK_PERFORMANCE_FACTOR=1.0 (default) is meant to represent GitHub Actions performance.
Decrease this factor if your local machine is faster than GitHub Actions."""

    return out + "\n\n"
