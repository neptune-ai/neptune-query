from __future__ import annotations

from tests.e2e.data_model import (
    ProjectData,
    RunData,
)

project_1_data = ProjectData(
    project_name_base="global_fetch_experiments_table_project_1",
    runs=[
        RunData(
            experiment_name_base="exp_project_1",
            run_id_base="run_project_1",
            configs={
                "config/int": 1,
                "config/string": "project-1",
            },
            float_series={
                "metrics/loss": {0: 0.5, 1: 0.25, 2: 0.125},
                "metrics/accuracy": {0: 0.1, 1: 0.2, 2: 0.3},
            },
        ),
    ],
)

project_2_data = ProjectData(
    project_name_base="global_fetch_experiments_table_project_2",
    runs=[
        RunData(
            experiment_name_base="exp_project_2",
            run_id_base="run_project_2",
            configs={
                "config/int": 2,
                "config/string": "project-2",
            },
            float_series={
                "metrics/loss": {0: 1.0, 1: 0.8, 2: 0.6},
                "metrics/accuracy": {0: 0.4, 1: 0.5, 2: 0.6},
            },
        ),
    ],
)
