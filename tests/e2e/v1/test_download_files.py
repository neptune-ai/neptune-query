import pathlib

import pandas as pd
import pytest

from neptune_query import (
    download_files,
    fetch_experiments_table,
    fetch_series,
)
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    IngestionFile,
    ProjectData,
    RunData,
)


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    return ensure_project(
        ProjectData(
            runs=[
                RunData(
                    experiment_name="experiment-with-files",
                    files={"files/file-value.txt": IngestionFile(source=b"Text content", mime_type="text/plain")},
                    file_series={
                        "files/file-series-value_0": {i: f"file-0-{i}".encode("utf-8") for i in range(3)},
                    },
                ),
            ]
        )
    )


@pytest.mark.files
def test__download_files_from_table(project, temp_dir):
    # when
    files = fetch_experiments_table(
        project=project.project_identifier,
        experiments="experiment-with-files",
        attributes="files/",
    )
    assert not files.empty
    result_df = download_files(
        files=files,
        destination=temp_dir,
    )

    # then
    expected_path = (temp_dir / "experiment-with-files" / "files/file-value_txt.txt").resolve()
    expected_df = pd.DataFrame(
        [
            {
                "experiment": "experiment-with-files",
                "step": None,
                "files/file-value.txt": str(expected_path),
            }
        ]
    ).set_index(["experiment", "step"])
    expected_df.columns.names = ["attribute"]
    pd.testing.assert_frame_equal(result_df, expected_df)

    target_path = result_df.loc[("experiment-with-files", None), "files/file-value.txt"]
    assert pathlib.Path(target_path).exists()
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Text content"


@pytest.mark.files
def test__download_files_from_file_series(project, temp_dir):
    # when
    file_series = fetch_series(
        project=project.project_identifier,
        experiments="experiment-with-files",
        attributes="files/file-series-value_0",
    )
    assert not file_series.empty
    result_df = download_files(
        files=file_series,
        destination=temp_dir,
    )

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": "experiment-with-files",
                "step": step,
                "files/file-series-value_0": str(
                    (
                        temp_dir / "experiment-with-files" / f"files/file-series-value_0/step_{int(step)}_000000.bin"
                    ).resolve()
                ),
            }
            for step in [0.0, 1.0, 2.0]
        ]
    ).set_index(["experiment", "step"])
    expected_df.columns.names = ["attribute"]
    pd.testing.assert_frame_equal(result_df, expected_df)

    for step in (0.0, 1.0, 2.0):
        target_path = result_df.loc[("experiment-with-files", step), "files/file-series-value_0"]
        with open(target_path, "rb") as file:
            content = file.read()
            expected_content = f"file-0-{int(step)}".encode("utf-8")
            assert content == expected_content
