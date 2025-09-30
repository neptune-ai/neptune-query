import json
import os
import subprocess
import sys
import tempfile
from io import BytesIO
from pathlib import Path

if not os.getenv("BENCHMARK_VALIDATE_FILE"):
    # Create a temp dir for the benchmark results:
    tmp_dir = tempfile.mkdtemp(prefix="neptune-query-benchmark-")
    report_path = Path(tmp_dir) / "benchmark.json"


def pytest_configure(config):
    if not os.getenv("BENCHMARK_VALIDATE_FILE"):
        # Perform at least 15 rounds per test
        # Testing at least for 10 seconds per test
        config.option.benchmark_min_rounds = 15
        config.option.benchmark_max_time = 10.0
        config.option.benchmark_disable_gc = True
        config.option.benchmark_time_unit = "ms"
        config.option.benchmark_sort = "name"
        config.option.benchmark_json = BytesIO()
        config.option.junitxml = "benchmark_measurement.xml"
    else:
        if config.option.xmlpath:
            # For --junitxml = /path/abc.xml, create /path/abc__validation.xml
            path = Path(config.option.xmlpath)
            config.option.xmlpath = str(path.with_stem(path.stem + "__validation"))


def pytest_benchmark_update_json(config, benchmarks, output_json):
    with open(report_path, "w") as f:
        json.dump(output_json, f, indent=2)
    with open("benchmark_results.json", "w") as f:
        json.dump(output_json, f, indent=2)


def pytest_sessionfinish(session, exitstatus):
    try:
        if exitstatus != 0:
            return

        if os.getenv("BENCHMARK_VALIDATE_FILE"):
            return

        if os.getenv("BENCHMARK_NO_VALIDATION") == "1":
            return

        # Rerun the tests in validation mode
        os.environ["BENCHMARK_VALIDATE_FILE"] = str(report_path)
        cp = subprocess.run(
            [sys.executable] + sys.argv + ["-W", "ignore::pytest_benchmark.logger.PytestBenchmarkWarning"]
        )
        session.exitstatus = cp.returncode

    finally:
        try:
            os.unlink(report_path)
        except Exception:
            pass
        try:
            os.rmdir(tmp_dir)
        except Exception:
            pass
