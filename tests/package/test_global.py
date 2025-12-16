import os
from datetime import datetime

import pytest

os.environ["NEPTUNE_LOGGER_LEVEL"] = "DEBUG"

import neptune_query.experimental as npte
from neptune_query import filters


def test_filter_none():
    data = npte.fetch_experiments_table_global(
        experiments=None,
        attributes=["sys/id"]
    )
    print(data)
    assert not data.empty

run_filter_cases = [
    filters.Filter.name("log-and-check-small-run"),
    "log-and-check-small-run",
    filters.Filter.name("(scor.*|log-and-check-small-run)"),
    filters.Filter.name("scor.*") | filters.Filter.name("log-and-check-small-run"),
    # filters.Filter.matches("sys/owner", "nobody"),   # 400
    filters.Filter.matches(filters.Attribute("sys/owner", "string"), "michal") & filters.Filter.name("scor.*"),
    # filters.Filter.ge("sys/creation_time", datetime(2025, 12, 10)),   # 400
    filters.Filter.ge(filters.Attribute("sys/creation_time", "datetime"), datetime(2025, 12, 10)),
    # filters.Filter.eq("sys/archived", False) & filters.Filter.name("scor.*"),   # 400
    filters.Filter.eq(filters.Attribute("sys/archived", "bool"), False) & filters.Filter.name("scor.*"),
]

@pytest.mark.parametrize("experiment_filter", run_filter_cases)
def test_experiment_filters(experiment_filter):
    data = npte.fetch_experiments_table_global(
        experiments=experiment_filter,
        attributes=["sys/id"]
    )
    print(data)
    assert not data.empty

@pytest.mark.parametrize("run_filter", run_filter_cases)
def test_runs_run_filters(run_filter):
    data = npte.fetch_runs_table_global(
        runs=run_filter,
        attributes=["sys/id"]
    )
    print(data)
    assert not data.empty

attribute_filter_cases = [
    "^scorck/metrics/[0-3][0-9]$",
    filters.AttributeFilter(name="^scorck/metrics/[0-3][0-9]$"),
    filters.AttributeFilter(name="^scorck/metrics/[0-3][0-9]$", type="float_series"),
    filters.AttributeFilter(type="float"),
    filters.AttributeFilter(type="datetime") | filters.AttributeFilter(type="bool"),
    filters.AttributeFilter(name="time", type="datetime"),
]

@pytest.mark.parametrize("attributes_filter", attribute_filter_cases)
def test_experiments_attribute_filter(attributes_filter):
    data = npte.fetch_experiments_table_global(
        experiments=filters.Filter.name("(scor.*|log-and-check-small-run)"),
        attributes=attributes_filter
    )
    print(data)
    assert not data.empty

@pytest.mark.parametrize("attributes_filter", attribute_filter_cases)
def test_runs_attribute_filter(attributes_filter):
    data = npte.fetch_runs_table_global(
        runs=filters.Filter.name("(scor.*|log-and-check-small-run)"),
        attributes=attributes_filter
    )
    print(data)
    assert not data.empty

@pytest.mark.parametrize("experiment_filter", [None])
@pytest.mark.parametrize("attributes_filter", [["sys/id"]])
@pytest.mark.parametrize("sort_direction", ["asc", "desc"])
@pytest.mark.parametrize("sort_by", [
    filters.Attribute("sys/creation_time", type="datetime"),
    filters.Attribute("sys/name", type="string"),
])
@pytest.mark.parametrize("limit", [1, 5, 100])
def test_experiments_limit(experiment_filter, attributes_filter, sort_direction, sort_by, limit):
    data = npte.fetch_experiments_table_global(
        experiments=experiment_filter,
        attributes=attributes_filter,
        sort_direction=sort_direction,
        sort_by=sort_by,
        limit=limit
    )
    print(data)
    assert not data.empty

