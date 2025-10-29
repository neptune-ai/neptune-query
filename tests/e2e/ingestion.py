from __future__ import annotations

import tempfile
from pathlib import Path

import filelock
from neptune_api import AuthenticatedClient
from neptune_scale import Run

from tests.e2e.data_model import (
    IngestedProjectData,
    IngestedRunData,
    ProjectData,
    RunData,
)


def format_experiment_name(base_name: str, execution_id: str) -> str:
    return f"{base_name}__{execution_id}"


def format_run_id(base_name: str, execution_id: str) -> str:
    return f"{base_name}__{execution_id}"


def project_exists(client: AuthenticatedClient, project_identifier: str) -> bool:
    workspace, project_name = project_identifier.split("/")
    args = {
        "method": "get",
        "url": "/api/backend/v1/projects/get",
        "params": {"projectIdentifier": f"{workspace}/{project_name}"},
    }

    try:
        response = client.get_httpx_client().request(**args)
        response.raise_for_status()
        return True
    except Exception:
        return False


def ensure_project(
    *,
    client: AuthenticatedClient,
    api_token: str,
    workspace: str,
    execution_id: str,
    project_data: ProjectData,
) -> IngestedProjectData:
    project_name = f"py-e2e__{execution_id}__{project_data.project_name_base}"
    project_identifier = f"{workspace}/{project_name}"

    if not project_exists(client, project_identifier):
        lock_path = Path(tempfile.gettempdir()) / f"neptune_e2e__{workspace}__{project_name}.lock"
        with filelock.FileLock(str(lock_path), timeout=300):
            if not project_exists(client, project_identifier):
                response = client.get_httpx_client().request(
                    method="post",
                    url="/api/backend/v1/projects",
                    json={"organizationIdentifier": workspace, "name": project_name, "visibility": "priv"},
                )
                response.raise_for_status()

            for run in project_data.runs:
                _ingest_run(
                    api_token=api_token,
                    project_identifier=project_identifier,
                    run_data=run,
                    execution_id=execution_id,
                )

    return IngestedProjectData(
        project_identifier=project_identifier,
        ingested_runs=[
            IngestedRunData(
                project_identifier=project_identifier,
                experiment_name=format_experiment_name(run.experiment_name_base, execution_id),
                run_id=format_run_id(run.run_id_base, execution_id),
                configs=run.configs,
                float_series=run.float_series,
            )
            for run in project_data.runs
        ],
    )


def _ingest_run(*, api_token: str, project_identifier: str, run_data: RunData, execution_id: str) -> None:
    with Run(
        api_token=api_token,
        project=project_identifier,
        run_id=format_run_id(run_data.run_id_base, execution_id),
        experiment_name=format_experiment_name(run_data.experiment_name_base, execution_id),
        source_tracking_config=None,
    ) as run:
        if run_data.configs:
            run.log_configs(run_data.configs)

        all_float_series_steps = set().union(*(series.keys() for series in run_data.float_series.values()))
        for step in sorted(all_float_series_steps):
            run.log_metrics(
                step=step,
                data={attr_name: series[step] for attr_name, series in run_data.float_series.items() if step in series},
            )
