#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib
import shutil
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory


def test_docker_permissions():
    msg = 'Error: Cannot run "docker run --rm alpine echo OK". Please ensure that docker is installed and running.'
    try:
        cp = subprocess.run(
            ["docker", "run", "--rm", "alpine", "echo", "OK"],
            capture_output=True,
        )
    except Exception as e:
        raise RuntimeError(f"{e}\n\n{msg}")
    if cp.returncode != 0 or cp.stdout.decode().strip() != "OK":
        raise RuntimeError(f"{cp.stderr.decode().strip()}\n\n{msg}")


def run_in_docker(project_root: Path, dockerfile: str, command: list[str], verbose: bool = False) -> None:
    """
    Run a command inside a Docker container built from the specified Dockerfile
    src/nacc/files directory is mounted read-only into container's directory /nacc/files

    Args:
        project_root: Path to the project root directory to be mounted into the container at /app
        dockerfile: Name of the Dockerfile to use (must be located in the nacc/files/dockerfiles directory)
        command: Command to run inside the Docker container (as a list of strings)
        verbose: If True, Docker build and run commands will output to the console
    """
    api_codegen_files = importlib.resources.files("neptune_api_codegen") / "files"
    dockerfile_path = api_codegen_files / "dockerfiles" / dockerfile

    # Build the Docker image in an empty (temporary) directory
    image_name = f"neptune-api-codegen_{dockerfile.replace('Dockerfile.', '')}"
    with TemporaryDirectory() as tmpdir:
        shutil.copy(str(dockerfile_path), str(Path(tmpdir) / "Dockerfile"))
        subprocess.run(
            ["docker", "build", "-t", image_name, tmpdir],
            check=True,
            capture_output=not verbose,
        )

    volumes = [
        f"-v{project_root}/neptune-api:/neptune-api",
        f"-v{api_codegen_files}:/files:ro",
    ]

    # Run the Docker container with the specified command
    subprocess.run(
        ["docker", "run", "--rm"] + volumes + [image_name] + command,
        check=True,
        capture_output=not verbose,
    )
