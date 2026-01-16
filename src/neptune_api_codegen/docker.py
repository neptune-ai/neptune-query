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

import importlib.resources
import shlex
import shutil
import subprocess
import sys
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


def run_in_docker(work_dir: Path, dockerfile: str, command: list[str], verbose: bool = False) -> None:
    """
    Run a command inside a Docker container built from the specified Dockerfile

    Mounts:
    1. src/neptune_api_codegen/docker/rofiles directory is mounted read-only as container's directory /rofiles
    2. work_dir is mounted as container's directory /work

    Args:
        work_dir: Path to the directory to be mounted into the container at /work
        dockerfile: Name of the Dockerfile to use (must be located in src/neptune_api_codegen/docker)
        command: Command to run inside the Docker container (as a list of strings)
        verbose: If True, Docker build and run commands will output to the console
                 If False, output will be suppressed unless an error occurs
    """
    docker_dir = importlib.resources.files("neptune_api_codegen") / "docker"
    dockerfile_path = docker_dir / dockerfile
    docker_rofiles = docker_dir / "rofiles"

    # Build the Docker image in an empty (temporary) directory
    image_name = f"neptune-api-codegen_{dockerfile.replace('Dockerfile.', '')}"
    with TemporaryDirectory() as tmpdir:
        shutil.copy(str(dockerfile_path), str(Path(tmpdir) / "Dockerfile"))
        verbose_run(
            ["docker", "build", "-t", image_name, tmpdir],
            verbose=verbose,
        )

    volumes = [
        f"-v{work_dir}:/work",
        f"-v{docker_rofiles}:/rofiles:ro",
    ]

    # Run the Docker container with the specified command
    docker_cmd = ["docker", "run", "--rm"] + volumes + [image_name] + command
    return verbose_run(docker_cmd, verbose)


def verbose_run(command: list[str], verbose: bool) -> None:
    docker_cmd_str = " ".join([shlex.quote(part) for part in command])

    if verbose:
        print("\n=== CMD ===", file=sys.stderr)
        print(docker_cmd_str, file=sys.stderr)
        print("\n=== OUT ===", file=sys.stderr)
        try:
            subprocess.run(
                command,
                check=True,
                capture_output=False,
            )
        finally:
            print("\n=== END ===\n", file=sys.stderr)
    else:
        # Be silent on success
        # and verbose on failure
        cp = subprocess.run(
            command,
            check=False,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
        )
        if cp.returncode != 0:
            print("\n=== CMD ===", file=sys.stderr)
            print(docker_cmd_str, file=sys.stderr)
            print("\n=== OUT ===", file=sys.stderr)
            print(cp.stdout.decode().strip(), file=sys.stderr)
            print("\n=== END ===\n", file=sys.stderr)
            raise subprocess.CalledProcessError(
                returncode=cp.returncode,
                cmd=command,
                output=cp.stdout,
            )
