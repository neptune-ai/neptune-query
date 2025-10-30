from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import filelock
import neptune_scale
from neptune_api import AuthenticatedClient


@dataclass(frozen=True)
class RunData:
    """
    Definition of the data to be ingested for a run in tests.
    """

    experiment_name_base: str
    run_id_base: str

    # run_id_base of the parent run and the fork step
    fork_point: Optional[tuple[str, float]]

    configs: dict[str, int | str]
    float_series: dict[str, dict[float, float]]


@dataclass(frozen=True)
class ProjectData:
    """
    Definition of the data to be ingested for a project in tests.
    """

    project_name_base: str
    runs: list[RunData]


@dataclass(frozen=True)
class IngestedRunData:
    """
    Representation of the ingested run data with actual identifiers.
    """

    project_identifier: str
    experiment_name: str
    run_id: str

    configs: dict[str, int | str]
    float_series: dict[str, dict[float, float]]


@dataclass(frozen=True)
class IngestedProjectData:
    """
    Representation of the ingested project data with actual identifiers.
    """

    project_identifier: str
    ingested_runs: list[IngestedRunData]


def ensure_project(
    *,
    client: AuthenticatedClient,
    api_token: str,
    workspace: str,
    unique_key: str,
    project_data: ProjectData,
) -> IngestedProjectData:
    """
    Ensures that a project with the specified data exists in Neptune.
    If the project does not exist, it is created and the data is ingested.
    Uses a file lock to prevent concurrent creation/ingestion of the same project.

    workspace: The Neptune workspace/organization name where the project should be created.
    unique_key: A unique key to append to project and run names/IDs to allow cross-project search tests.
    """
    project_name = _format_project_name(project_data.project_name_base, unique_key)
    project_identifier = f"{workspace}/{project_name}"

    lock_path = Path(tempfile.gettempdir()) / f"neptune_e2e__{workspace}__{project_name}.lock"
    with filelock.FileLock(str(lock_path), timeout=300):
        if not _project_exists(client, project_identifier):
            response = client.get_httpx_client().request(
                method="post",
                url="/api/backend/v1/projects",
                json={"organizationIdentifier": workspace, "name": project_name, "visibility": "priv"},
            )
            response.raise_for_status()

            _ingest_project_data(
                api_token=api_token,
                workspace=workspace,
                project_data=project_data,
                unique_key=unique_key,
            )

    return IngestedProjectData(
        project_identifier=project_identifier,
        ingested_runs=[
            IngestedRunData(
                project_identifier=project_identifier,
                experiment_name=_format_experiment_name(run_data.experiment_name_base, unique_key),
                run_id=_format_run_id(run_data.run_id_base, unique_key),
                configs=run_data.configs,
                float_series=run_data.float_series,
            )
            for run_data in project_data.runs
        ],
    )


def _ingest_project_data(api_token: str, workspace: str, project_data: ProjectData, unique_key: str) -> None:
    project_name = _format_project_name(project_name_base=project_data.project_name_base, unique_key=unique_key)
    project_identifier = f"{workspace}/{project_name}"

    batches = _group_runs_by_execution_order(project_data.runs)
    for batch in batches:
        _ingest_runs(
            runs_data=batch,
            api_token=api_token,
            project_identifier=project_identifier,
            unique_key=unique_key,
        )


def _group_runs_by_execution_order(runs_data: list[RunData]) -> list[list[RunData]]:
    if not runs_data:
        return []

    def parent_id(run: RunData) -> str | None:
        if run.fork_point is not None:
            return run.fork_point[0]

        return None

    remaining: list[RunData] = list(runs_data)
    resolved: set[Optional[str]] = {None}  # None represents a parent of root runs
    batches: list[list[RunData]] = []

    while remaining:
        ready_batch = [run for run in remaining if parent_id(run) in resolved]
        if not ready_batch:
            raise ValueError("Detected cyclic or unresolved fork dependencies among runs to ingest.")

        for run in ready_batch:
            remaining.remove(run)
            resolved.add(run.run_id_base)

        batches.append(ready_batch)

    return batches


def _ingest_runs(runs_data: list[RunData], api_token: str, project_identifier: str, unique_key: str) -> None:
    runs = []
    for run_data in runs_data:
        run = neptune_scale.Run(
            api_token=api_token,
            project=project_identifier,
            experiment_name=_format_experiment_name(run_data.experiment_name_base, unique_key),
            run_id=_format_run_id(run_data.run_id_base, unique_key),
            fork_run_id=_format_run_id(run_data.fork_point[0], unique_key) if run_data.fork_point else None,
            fork_step=run_data.fork_point[1] if run_data.fork_point else None,
            enable_console_log_capture=False,
            source_tracking_config=None,
        )

        if run_data.configs:
            run.log_configs(run_data.configs)

        all_float_series_steps = set().union(*(series.keys() for series in run_data.float_series.values()))
        for step in sorted(all_float_series_steps):
            run.log_metrics(
                step=step,
                data={attr_name: series[step] for attr_name, series in run_data.float_series.items() if step in series},
            )

        runs.append(run)

    for run in runs:
        run.close()


def _project_exists(client: AuthenticatedClient, project_identifier: str) -> bool:
    """
    This is a very simplified check to see if a project exists.
    TODO: It should be improved to verify the data in the project is as expected.
    """

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


def _format_experiment_name(base_name: str, unique_key: str) -> str:
    return f"{base_name}__{unique_key}"


def _format_run_id(base_name: str, unique_key: str) -> str:
    return f"{base_name}__{unique_key}"


def _format_project_name(project_name_base: str, unique_key: str) -> str:
    return f"py-e2e__{unique_key}__{project_name_base}"
