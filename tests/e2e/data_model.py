from dataclasses import dataclass


@dataclass(frozen=True)
class RunData:
    """
    Definition of the data to be ingested for a run in tests.
    """

    experiment_name_base: str
    run_id_base: str
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
