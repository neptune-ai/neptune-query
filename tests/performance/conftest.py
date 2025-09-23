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
        # Test at least 5 rounds, at least for 5 seconds
        config.option.benchmark_min_rounds = 5
        config.option.benchmark_max_time = 5.0
        config.option.benchmark_disable_gc = True
        config.option.benchmark_time_unit = "ms"
        config.option.benchmark_sort = "name"
        config.option.benchmark_json = BytesIO()


def pytest_benchmark_update_json(config, benchmarks, output_json):
    with open(report_path, "w") as f:
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
        cp = subprocess.run([sys.executable] + sys.argv)

    finally:
        try:
            os.unlink(report_path)
        except Exception:
            pass
        try:
            os.rmdir(tmp_dir)
        except Exception:
            pass

    if cp.returncode != 0:
        sys.exit(cp.returncode)
