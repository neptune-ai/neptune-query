import os
import pathlib

import pandas as pd
import pytest

from neptune_query import (
    fetch_experiments_table,
    fetch_series,
)
from neptune_query._internal import resolve_files
from neptune_query.filters import AttributeFilter
from neptune_query.internal.composition.download_files import download_files
from neptune_query.internal.retrieval.search import ContainerType
from neptune_query.types import File
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    IngestionFile,
    ProjectData,
    RunData,
)

FILE_SERIES_PATHS = ["file-series-value_0", "file-series-value_1"]


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    return ensure_project(
        ProjectData(
            project_name_base="composition-download-files-project",
            runs=[
                RunData(
                    experiment_name_base="composition-download-files-experiment",
                    run_id_base="composition-download-files-run",
                    files={
                        "file-value": b"Binary content",
                        "file-value.txt": IngestionFile(b"Text content", mime_type="text/plain"),
                    },
                    file_series={
                        "file-series-value_0": {
                            0.0: IngestionFile(b"file-0-0"),
                            1.0: IngestionFile(b"file-0-1"),
                            2.0: IngestionFile(b"file-0-2"),
                        },
                        "file-series-value_1": {
                            0.0: IngestionFile(b"file-1-0"),
                            1.0: IngestionFile(b"file-1-1"),
                            2.0: IngestionFile(b"file-1-2"),
                        },
                    },
                )
            ],
        )
    )


@pytest.mark.files
def test_download_files_missing(client, project, temp_dir):
    experiment_name = project.ingested_runs[0].experiment_name
    # when
    result_df = download_files(
        files=[
            File(
                project_identifier=project.project_identifier,
                experiment_name=experiment_name,
                run_id=None,
                attribute_path="object-does-not-exist",
                step=None,
                path="object-does-not-exist",
                size_bytes=0,
                mime_type="application/octet-stream",
            ),
        ],
        destination=temp_dir,
        container_type=ContainerType.EXPERIMENT,
        context=None,
    )

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": experiment_name,
                "step": None,
                "object-does-not-exist": None,
            }
        ]
    ).set_index(["experiment", "step"])
    expected_df.columns.names = ["attribute"]
    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.files
def test_download_files_no_permission(client, project, temp_dir):
    experiment_name = project.ingested_runs[0].experiment_name
    os.chmod(temp_dir, 0o000)  # No permissions

    with pytest.raises(PermissionError):
        download_files(
            files=[
                File(
                    project_identifier=project.project_identifier,
                    experiment_name=experiment_name,
                    run_id=None,
                    attribute_path="file-value.txt",
                    step=None,
                    path="not-real-path",
                    size_bytes=len(b"Text content"),
                    mime_type="text/plain",
                ),
            ],
            destination=temp_dir,
            container_type=ContainerType.EXPERIMENT,
            context=None,
        )

    os.chmod(temp_dir, 0o755)  # Reset permissions


@pytest.mark.files
def test_download_files_destination_file_type(client, project, temp_dir):
    experiment_name = project.ingested_runs[0].experiment_name
    destination = temp_dir / "file"
    with open(destination, "wb") as file:
        file.write(b"test")

    with pytest.raises(NotADirectoryError):
        download_files(
            files=[
                File(
                    project_identifier=project.project_identifier,
                    experiment_name=experiment_name,
                    run_id=None,
                    attribute_path="file-value.txt",
                    step=None,
                    path="not-a-real-path",
                    size_bytes=len(b"Text content"),
                    mime_type="text/plain",
                ),
            ],
            destination=destination,
            container_type=ContainerType.EXPERIMENT,
            context=None,
        )


@pytest.mark.files
def test_download_files_single(client, project, temp_dir):
    experiment_name = project.ingested_runs[0].experiment_name
    # when
    files_df = fetch_experiments_table(
        experiments=experiment_name,
        attributes=AttributeFilter(name="file-value.txt", type="file"),
        project=project.project_identifier,
    )
    downloadable_files = [files_df.loc[experiment_name, "file-value.txt"]]
    result_df = download_files(
        files=downloadable_files, destination=temp_dir, container_type=ContainerType.EXPERIMENT, context=None
    )

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": experiment_name,
                "step": None,
                "file-value.txt": str(temp_dir / experiment_name / "file-value_txt.txt"),
            }
        ]
    ).set_index(["experiment", "step"])
    expected_df.columns.names = ["attribute"]
    pd.testing.assert_frame_equal(result_df, expected_df)

    target_path = result_df.loc[(experiment_name, None), "file-value.txt"]
    assert pathlib.Path(target_path).exists()
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Text content"


@pytest.mark.files
def test_download_files_multiple(client, project, temp_dir):
    experiment_name = project.ingested_runs[0].experiment_name
    # when
    files_df = fetch_experiments_table(
        experiments=experiment_name,
        attributes=AttributeFilter(name=["file-value", "file-value.txt"], type="file"),
        project=project.project_identifier,
    )
    downloadable_files = [
        files_df.loc[experiment_name, "file-value"],
        files_df.loc[experiment_name, "file-value.txt"],
    ]
    result_df = download_files(
        files=downloadable_files, destination=temp_dir, container_type=ContainerType.EXPERIMENT, context=None
    )

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": experiment_name,
                "step": None,
                "file-value": str(temp_dir / experiment_name / "file-value.bin"),
                "file-value.txt": str(temp_dir / experiment_name / "file-value_txt.txt"),
            }
        ]
    ).set_index(["experiment", "step"])
    expected_df.columns.names = ["attribute"]
    pd.testing.assert_frame_equal(result_df, expected_df)

    target_path_value = result_df.loc[(experiment_name, None), "file-value"]
    with open(target_path_value, "rb") as file:
        content = file.read()
        assert content == b"Binary content"
    target_path_value_txt = result_df.loc[(experiment_name, None), "file-value.txt"]
    with open(target_path_value_txt, "rb") as file:
        content = file.read()
        assert content == b"Text content"


@pytest.mark.files
def test_download_file_series(client, project, temp_dir):
    experiment_name = project.ingested_runs[0].experiment_name
    # when
    files_df = fetch_series(
        experiments=experiment_name,
        attributes=AttributeFilter(name=FILE_SERIES_PATHS, type="file_series"),
        project=project.project_identifier,
    )
    files = resolve_files(files_df)
    assert len(files) == 6
    result_df = download_files(files=files, destination=temp_dir, container_type=ContainerType.EXPERIMENT, context=None)

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": experiment_name,
                "step": step,
                "file-series-value_0": str(
                    temp_dir / experiment_name / f"file-series-value_0/step_{int(step)}_000000.bin"
                ),
                "file-series-value_1": str(
                    temp_dir / experiment_name / f"file-series-value_1/step_{int(step)}_000000.bin"
                ),
            }
            for step in [0.0, 1.0, 2.0]
        ]
    ).set_index(["experiment", "step"])
    expected_df.columns.names = ["attribute"]
    pd.testing.assert_frame_equal(result_df, expected_df)

    for attribute in (0, 1):
        for step in (0.0, 1.0, 2.0):
            target_path = result_df.loc[(experiment_name, step), f"file-series-value_{attribute}"]
            with open(target_path, "rb") as file:
                content = file.read()
                expected_content = f"file-{attribute}-{int(step)}".encode("utf-8")
                assert content == expected_content
