import pandas as pd
import pytest

import neptune_query.runs as runs
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.output_format import create_metrics_dataframe
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    ProjectData,
    RunData,
    step_to_timestamp,
)

METRICS: dict[str, dict[str, list[tuple[float, float]]]] = {
    "linear_history_root": {
        "foo0": [(float(step), float(step * 0.1)) for step in range(10)],
        "foo1": [(float(step), float(step * 0.2)) for step in range(10)],
        "unique1/0": [(float(step), float(step * 0.3)) for step in range(10)],
    },
    "linear_history_fork1": {
        "foo0": [(float(step), float(step * 0.4)) for step in range(5, 10)],
        "foo1": [(float(step), float(step * 0.5)) for step in range(5, 10)],
        "unique2/0": [(float(step), float(step * 0.6)) for step in range(5, 10)],
    },
    "forked_history_root": {
        "foo0": [(float(step), float(step * 0.1)) for step in range(1, 5)],
        "foo1": [(float(step), float(step * 0.2)) for step in range(1, 5)],
    },
    "forked_history_fork1": {
        "foo0": [(float(step), float(step * 0.4)) for step in range(5, 9)],
        "foo1": [(float(step), float(step * 0.5)) for step in range(5, 9)],
    },
    "forked_history_fork2": {
        "foo0": [(float(step), float(step * 0.7)) for step in range(9, 20)],
        "foo1": [(float(step), float(step * 0.8)) for step in range(9, 20)],
    },
}


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    # root (level: None)
    #   └── fork1 (level: 1, fork_point: 4)
    #         └── fork2 (level: 2, fork_point: 8)
    linear_history_tree = [
        RunData(
            experiment_name="linear-history",
            run_id="linear_history_root",
            float_series={name: dict(series) for name, series in METRICS["linear_history_root"].items()},
        ),
        RunData(
            experiment_name="linear-history",
            run_id="linear_history_fork1",
            fork_point=("linear_history_root", 4.0),
            float_series={name: dict(series) for name, series in METRICS["linear_history_fork1"].items()},
        ),
    ]

    # forked_history_tree:
    # root (level: None)
    #   ├── fork1 (level: 1, fork_point: 4)
    #   └── fork2 (level: 1, fork_point: 8)
    forked_history_tree = [
        RunData(
            experiment_name="forked-history",
            run_id="forked_history_root",
            float_series={name: dict(series) for name, series in METRICS["forked_history_root"].items()},
        ),
        RunData(
            experiment_name="forked-history",
            run_id="forked_history_fork1",
            fork_point=("forked_history_root", 4.0),
            float_series={name: dict(series) for name, series in METRICS["forked_history_fork1"].items()},
        ),
        RunData(
            experiment_name="forked-history",
            run_id="forked_history_fork2",
            fork_point=("forked_history_root", 8.0),
            float_series={name: dict(series) for name, series in METRICS["forked_history_fork2"].items()},
        ),
    ]

    return ensure_project(ProjectData(runs=linear_history_tree + forked_history_tree))


@pytest.mark.parametrize(
    "runs_filter, attributes_filter, expected_metrics, tail_limit, step_range, lineage_to_the_root",
    [
        (
            r"^non_existent_run_name$",
            r"^foo0$",
            {},
            None,
            (None, None),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^non_existent_attribute_name$",
            {},
            None,
            (None, None),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {
                ("linear_history_root", "foo0"): METRICS["linear_history_root"]["foo0"],
            },
            None,
            (None, None),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {("linear_history_root", "foo0"): METRICS["linear_history_root"]["foo0"][-3:]},
            3,
            (None, None),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {("linear_history_root", "foo0"): METRICS["linear_history_root"]["foo0"][0:7]},
            None,
            (0, 6),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {("linear_history_root", "foo0"): METRICS["linear_history_root"]["foo0"][4:7]},
            3,
            (0, 6),
            True,
        ),
        (
            r"^linear_history_(root|fork1)$",
            r"foo.*",
            {
                ("linear_history_root", "foo0"): METRICS["linear_history_root"]["foo0"][2:5],
                ("linear_history_root", "foo1"): METRICS["linear_history_root"]["foo1"][2:5],
                ("linear_history_fork1", "foo0"): METRICS["linear_history_root"]["foo0"][2:5],
                ("linear_history_fork1", "foo1"): METRICS["linear_history_root"]["foo1"][2:5],
            },
            3,
            (0, 4),
            True,
        ),
        (
            r"^linear_history_(root|fork1)$",
            r"foo.*",
            {
                ("linear_history_root", "foo0"): METRICS["linear_history_root"]["foo0"][2:5],
                ("linear_history_root", "foo1"): METRICS["linear_history_root"]["foo1"][2:5],
                ("linear_history_fork1", "foo0"): METRICS["linear_history_root"]["foo0"][2:5],
                ("linear_history_fork1", "foo1"): METRICS["linear_history_root"]["foo1"][2:5],
            },
            3,
            (None, 4),
            True,
        ),
        (
            r"^linear_history_(root|fork1)$",
            r"foo.*",
            {
                ("linear_history_root", "foo0"): METRICS["linear_history_root"]["foo0"][2:5],
                ("linear_history_root", "foo1"): METRICS["linear_history_root"]["foo1"][2:5],
            },
            3,
            (0, 4),
            False,
        ),
        (
            ["linear_history_root", "linear_history_fork1"],
            r"unique.*",
            {
                ("linear_history_root", "unique1/0"): METRICS["linear_history_root"]["unique1/0"],
                ("linear_history_fork1", "unique2/0"): METRICS["linear_history_fork1"]["unique2/0"],
            },
            None,
            (None, None),
            False,
        ),
        (
            r"^linear_history_(root|fork1)$",
            r"unique.*",
            {
                ("linear_history_root", "unique1/0"): METRICS["linear_history_root"]["unique1/0"],
                ("linear_history_fork1", "unique2/0"): METRICS["linear_history_fork1"]["unique2/0"],
            },
            None,
            (None, None),
            False,
        ),
        (
            r"^forked_history_fork1$",
            r"foo.*",
            {
                ("forked_history_fork1", "foo0"): METRICS["forked_history_fork1"]["foo0"][1:4],
                ("forked_history_fork1", "foo1"): METRICS["forked_history_fork1"]["foo1"][1:4],
            },
            3,
            (5, 10),
            False,
        ),
        (
            r"^forked_history_fork1$",
            r"foo.*",
            {
                ("forked_history_fork1", "foo0"): METRICS["forked_history_fork1"]["foo0"][0:4],
                ("forked_history_fork1", "foo1"): METRICS["forked_history_fork1"]["foo1"][0:4],
            },
            10,
            (None, None),
            False,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {("linear_history_root", "foo0"): METRICS["linear_history_root"]["foo0"][5:6]},
            None,
            (5, 5),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {},
            None,
            (100, 200),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {("linear_history_root", "foo0"): METRICS["linear_history_root"]["foo0"]},
            None,
            (0, 20),
            True,
        ),
        # Fetch last point of metrics for runs with forked history, including lineage to the root.
        (
            r"^forked_history_.*$",
            r"foo.*",
            {
                ("forked_history_root", "foo0"): METRICS["forked_history_root"]["foo0"][-1:],
                ("forked_history_root", "foo1"): METRICS["forked_history_root"]["foo1"][-1:],
                ("forked_history_fork1", "foo0"): METRICS["forked_history_fork1"]["foo0"][-1:],
                ("forked_history_fork1", "foo1"): METRICS["forked_history_fork1"]["foo1"][-1:],
                ("forked_history_fork2", "foo0"): METRICS["forked_history_fork2"]["foo0"][-1:],
                ("forked_history_fork2", "foo1"): METRICS["forked_history_fork2"]["foo1"][-1:],
            },
            1,
            (None, None),
            True,
        ),
    ],
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_time", ["absolute", None])
def test_fetch_run_metrics(
    project: IngestedProjectData,
    runs_filter,
    attributes_filter,
    expected_metrics,
    type_suffix_in_column_names: bool,
    include_time: str | None,
    tail_limit: int | None,
    step_range: tuple[float | None, float | None],
    lineage_to_the_root: bool,
):
    df = runs.fetch_metrics(
        project=project.project_identifier,
        runs=runs_filter,
        attributes=attributes_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_time=include_time,
        tail_limit=tail_limit,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
    )

    expected_df = build_expected_dataframe(
        project,
        expected_metrics,
        include_time=include_time,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    pd.testing.assert_frame_equal(df, expected_df, check_dtype=False)


def build_expected_dataframe(
    project: IngestedProjectData,
    expected_metrics: dict[tuple[str, str], list[tuple[float, float]]],
    include_time: str | None,
    type_suffix_in_column_names: bool,
):
    metrics_data = {}
    sys_id_to_run_id = {}
    for (run_id, metric_name), values in expected_metrics.items():
        # the run_ids are changed by the ingestion logic; resolve them here
        sys_id = SysId(f"PROJECT-{hash(run_id)}")  # sys_id doesn't matter here, it's just a unique string
        sys_id_to_run_id[sys_id] = run_id

        run_attribute_definition = RunAttributeDefinition(
            run_identifier=RunIdentifier(
                project_identifier=ProjectIdentifier(project.project_identifier), sys_id=sys_id
            ),
            attribute_definition=AttributeDefinition(name=metric_name, type="float_series"),
        )

        metrics_data[run_attribute_definition] = [
            (int(step_to_timestamp(step).timestamp() * 1000), step, value, False, 1.0) for step, value in values
        ]

    return create_metrics_dataframe(
        metrics_data=metrics_data,
        sys_id_label_mapping=sys_id_to_run_id,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=False,
        timestamp_column_name="absolute_time" if include_time == "absolute" else None,
        index_column_name="run",
    )
