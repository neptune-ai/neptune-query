import pandas as pd

from neptune_query import filters
from neptune_query.experimental import fetch_experiments_table
from tests.e2e.v1.generator import (
    EXP_NAME_INF_NAN_RUN,
    LINEAR_TREE_EXP_NAME,
    MULT_EXPERIMENT_HISTORY_EXP_1,
)

pytest_plugins = ("tests.e2e.v1.conftest",)


def test_experimental_fetch_experiments_table_basic(new_project_id):
    experiment_names = [LINEAR_TREE_EXP_NAME, MULT_EXPERIMENT_HISTORY_EXP_1]

    df = fetch_experiments_table(
        experiments=experiment_names,
        attributes=[],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
        limit=len(experiment_names),
    )

    expected_index = pd.MultiIndex.from_tuples(
        [(new_project_id, name) for name in sorted(experiment_names)],
        names=["project", "experiment"],
    )

    assert list(df.index.names) == ["project", "experiment"]
    assert df.index.equals(expected_index)
    assert df.empty


def test_experimental_fetch_experiments_table_with_sys_id(new_project_id):
    experiment_names = [EXP_NAME_INF_NAN_RUN, LINEAR_TREE_EXP_NAME]
    experiments_filter = filters.Filter.eq(
        filters.Attribute("sys/name", type="string"), experiment_names[0]
    ) | filters.Filter.eq(filters.Attribute("sys/name", type="string"), experiment_names[1])

    df = fetch_experiments_table(
        experiments=experiments_filter,
        attributes=["sys/id"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="desc",
        limit=len(experiment_names),
    )

    expected_order = sorted(experiment_names, reverse=True)
    expected_index = pd.MultiIndex.from_tuples(
        [(new_project_id, name) for name in expected_order],
        names=["project", "experiment"],
    )

    assert df.index.equals(expected_index)
    assert list(df.columns) == ["sys/id"]
    assert df["sys/id"].notna().all()
    assert df["sys/id"].str.len().gt(0).all()
