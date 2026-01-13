from __future__ import annotations

import tempfile
from collections import defaultdict
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
    timezone,
)
from pathlib import Path
from time import sleep
from typing import (
    Generator,
    Iterable,
    Mapping,
    Optional,
    TypeVar,
)

import filelock
import neptune_scale
import neptune_scale.types
from neptune_api import AuthenticatedClient

from neptune_query.internal.identifiers import ProjectIdentifier
from neptune_query.internal.retrieval import search

IngestionHistogram = neptune_scale.types.Histogram
IngestionFile = neptune_scale.types.File


SeriesPoint = TypeVar("SeriesPoint", str, float, IngestionHistogram, IngestionFile)


_STEP0_TIMESTAMP = 1_700_000_000.0  # Arbitrary fixed timestamp for ingestion start


def step_to_timestamp(step: float) -> datetime:
    """
    Converts a step number to a fixed timestamp for testing purposes.

    Don't rely on this. The behavior of this function may change in the future.
    """
    return datetime.fromtimestamp(_STEP0_TIMESTAMP + step, tz=timezone.utc)


@dataclass(frozen=True)
class RunData:
    """
    Definition of the data to be ingested for a run in tests.
    """

    experiment_name: str | None = None
    run_id: str | None = None

    # run_id of the parent run and the fork step
    fork_point: tuple[str, float] | None = None

    configs: dict[str, float | int | str] | None = field(default_factory=dict)
    files: dict[str, IngestionFile] | None = field(default_factory=dict)

    float_series: dict[str, dict[float, float]] = field(default_factory=dict)
    string_series: dict[str, dict[float, str]] = field(default_factory=dict)
    histogram_series: dict[str, dict[float, IngestionHistogram]] = field(default_factory=dict)
    file_series: dict[str, dict[float, IngestionFile]] = field(default_factory=dict)

    # string set attributes (logged as tags)
    string_sets: dict[str, list[str]] | None = field(default_factory=dict)


@dataclass(frozen=True)
class ProjectData:
    """
    Definition of the data to be ingested for a project in tests.
    """

    runs: list[RunData]


@dataclass(frozen=True)
class IngestedRunData:
    """
    Representation of the ingested run data with actual identifiers.
    """

    project_identifier: str
    experiment_name: str
    run_id: str

    configs: dict[str, float | int | str]
    float_series: dict[str, dict[float, float]]
    string_series: dict[str, dict[float, str]]
    histogram_series: dict[str, dict[float, IngestionHistogram]]
    file_series: dict[str, dict[float, IngestionFile]] = field(default_factory=dict)


@dataclass(frozen=True)
class IngestedProjectData:
    """
    Representation of the ingested project data with actual identifiers.
    """

    project_identifier: str
    ingested_runs: list[IngestedRunData]

    def get_run_by_run_id(self: IngestedProjectData, run_id: str) -> IngestedRunData:
        for run in self.ingested_runs:
            if run.run_id == run_id:
                return run
        raise ValueError(f"Run not found: {run_id}")


def _wait_for_ingestion(
    client: AuthenticatedClient, project_identifier: ProjectIdentifier, expected_data: ProjectData
) -> None:
    for attempt in range(20):
        found_runs = 0

        for page in search.fetch_run_sys_ids(
            client=client,
            project_identifier=project_identifier,
            filter_=None,
        ):
            found_runs += len(page.items)

        # Extra wait to ensure data is available to query before proceeding
        sleep(2)

        if found_runs == len(expected_data.runs):
            return

    raise RuntimeError(
        f"Timed out waiting for data ingestion, " f"found runs: {found_runs} out of expected: {len(expected_data.runs)}"
    )


def ingest_project(
    *,
    client: AuthenticatedClient,
    api_token: str,
    workspace: str,
    project_name: str,
    project_data: ProjectData,
) -> IngestedProjectData:
    """
    Ensures that a project with the specified data exists in Neptune.
    If the project does not exist, it is created and the data is ingested.
    Uses a file lock to prevent concurrent creation/ingestion of the same project.

    workspace: The Neptune workspace/organization name where the project should be created.
    """
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
                project_identifier=project_identifier,
                project_data=project_data,
            )

            _wait_for_ingestion(client=client, project_identifier=project_identifier, expected_data=project_data)

    return IngestedProjectData(
        project_identifier=project_identifier,
        ingested_runs=[
            IngestedRunData(
                project_identifier=project_identifier,
                experiment_name=run_data.experiment_name,
                run_id=run_data.run_id,
                configs=run_data.configs,
                float_series=run_data.float_series,
                string_series=run_data.string_series,
                histogram_series=run_data.histogram_series,
                file_series=run_data.file_series,
            )
            for run_data in project_data.runs
        ],
    )


def _ingest_project_data(api_token: str, project_identifier, project_data: ProjectData) -> None:
    batches = _group_runs_by_execution_order(project_data.runs)
    for batch in batches:
        _ingest_runs(
            runs_data=batch,
            api_token=api_token,
            project_identifier=project_identifier,
        )


def _ingest_runs(runs_data: list[RunData], api_token: str, project_identifier: str) -> None:
    runs = []
    for run_data in runs_data:
        run = neptune_scale.Run(
            api_token=api_token,
            project=project_identifier,
            experiment_name=run_data.experiment_name,
            run_id=run_data.run_id,
            fork_run_id=run_data.fork_point[0] if run_data.fork_point else None,
            fork_step=run_data.fork_point[1] if run_data.fork_point else None,
            enable_console_log_capture=False,
            source_tracking_config=None,
        )

        if run_data.configs:
            run.log_configs(run_data.configs)

        if run_data.string_sets:
            run.log(tags_add=run_data.string_sets)

        if run_data.files:
            run.assign_files(run_data.files)

        all_steps = _get_all_steps(run_data)
        float_series_by_step = _get_series_by_step(run_data.float_series)
        string_series_by_step = _get_series_by_step(run_data.string_series)
        histogram_series_by_step = _get_series_by_step(run_data.histogram_series)
        file_series_by_step = _get_series_by_step(run_data.file_series)

        for step in all_steps:
            timestamp = step_to_timestamp(step)
            if data := float_series_by_step[step]:
                run.log_metrics(
                    step=step,
                    data=data,
                    timestamp=timestamp,
                )

            run._log(
                step=step,
                timestamp=timestamp,
                string_series=string_series_by_step[step],
                histograms=histogram_series_by_step[step],
                file_series=file_series_by_step[step],
            )

        runs.append(run)

    for run in runs:
        run.close()


def _get_all_steps(run_data: RunData) -> Iterable[float]:
    # Collect all unique steps
    all_steps = set()
    for series in run_data.float_series.values():
        all_steps.update(series.keys())
    for series in run_data.string_series.values():
        all_steps.update(series.keys())
    for series in run_data.histogram_series.values():
        all_steps.update(series.keys())
    for series in run_data.file_series.values():
        all_steps.update(series.keys())

    return sorted(all_steps)


def _get_series_by_step(series: dict[str, dict[float, SeriesPoint]]) -> Mapping[float, dict[str, SeriesPoint]]:
    series_by_step = defaultdict(dict)

    for series_name, series_data in series.items():
        for step, value in series_data.items():
            series_by_step[step][series_name] = value

    return series_by_step


def _group_runs_by_execution_order(runs_data: list[RunData]) -> Generator[list[RunData], None, None]:
    if not runs_data:
        return

    def parent_id(run: RunData) -> str | None:
        if run.fork_point is not None:
            return run.fork_point[0]
        return None

    remaining: list[RunData] = list(runs_data)
    resolved: set[Optional[str]] = {None}  # None represents a parent of root runs

    while remaining:
        ready_batch = [run for run in remaining if parent_id(run) in resolved]
        if not ready_batch:
            raise ValueError("Detected cyclic or unresolved fork dependencies among runs to ingest.")

        for run in ready_batch:
            remaining.remove(run)
            resolved.add(run.run_id)

        yield ready_batch


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
