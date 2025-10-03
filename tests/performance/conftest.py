import json
import os
import tempfile
from io import BytesIO
from pathlib import Path

from .validation import (
    generate_junit_report,
    generate_text_report,
)

# Create a temp dir for the benchmark results:
tmp_dir = tempfile.mkdtemp(prefix="neptune-query-benchmark-")
benchmark_json_path = Path(tmp_dir) / "benchmark.json"


def cleanup():
    try:
        os.unlink(benchmark_json_path)
    except Exception:
        pass
    try:
        os.rmdir(tmp_dir)
    except Exception:
        pass


def pytest_configure(config):
    # Perform at least 15 rounds per test
    # Testing at least for 10 seconds per test
    config.option.benchmark_min_rounds = 15
    config.option.benchmark_max_time = 10.0
    config.option.benchmark_disable_gc = True
    config.option.benchmark_time_unit = "ms"
    config.option.benchmark_sort = "name"
    config.option.benchmark_json = BytesIO()
    config.option.benchmark_quiet = True

    config.option.original_xmlpath = config.option.xmlpath
    config.option.xmlpath = None


def pytest_benchmark_update_json(config, benchmarks, output_json):
    with open(benchmark_json_path, "w") as f:
        json.dump(output_json, f, indent=2)

    with open("benchmark_results.json", "w") as f:
        json.dump(output_json, f, indent=2)


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    try:
        # Print a report to the terminal
        msg = generate_text_report(benchmark_json_path)
        terminalreporter.ensure_newline()
        terminalreporter.write(msg)
        terminalreporter.ensure_newline()

        # And save a nice JUnit XML
        if config.option.original_xmlpath:
            path = Path(config.option.original_xmlpath)
            path.parent.mkdir(parents=True, exist_ok=True)
            generate_junit_report(benchmark_json_path, path)

    finally:
        cleanup()
