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
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

from neptune_api_codegen.fmt import rel

SWAGGER_FILES = {
    "apps/backend/server/src/main/resources/webapp/api/backend/swagger.json": "swagger/backend.json",
    "apps/ingestion-gateway/swagger/swagger.json": "swagger/ingestion_openapi.json",
    "apps/leaderboard/swagger/leaderboardSwagger.json": "swagger/retrieval.json",
    "apps/storagebridge/swagger/swagger.json": "swagger/storagebridge_openapi.json",
}

PROTO_DIRS = {
    "libs/models/leaderboard-proto/src/main/proto/neptune_pb": "proto/neptune_pb",
    "libs/models/leaderboard-proto/src/main/proto/google_rpc": "proto/google_rpc",
}


def is_neptune_repo(candidate_dir: Path) -> bool:
    find_files = [candidate_dir / Path(file) for file in SWAGGER_FILES.keys()]
    find_dirs = [candidate_dir / Path(dir) for dir in PROTO_DIRS.keys()]

    return all(file.is_file() for file in find_files) and all(dir.is_dir() for dir in find_dirs)


def find_neptune_repo(
    search_start: Path, up_levels: int = 2, down_levels: int = 4, limit_at_level: int = 128
) -> Path | None:
    candidate_dirs = set()

    for up_dir in [search_start] + list(search_start.parents)[:up_levels]:
        for level in range(1, down_levels + 1):
            glob = "*/" * level
            dirs_at_level = list(up_dir.glob(glob))
            if len(dirs_at_level) >= limit_at_level:
                break

            for candidate_dir in dirs_at_level:
                if is_neptune_repo(candidate_dir):
                    candidate_dirs.add(candidate_dir)

            if len(candidate_dirs) == 1:
                return candidate_dirs.pop()

            if len(candidate_dirs) > 1:
                raise RuntimeError(f"Found multiple neptune backend repositories: {candidate_dirs}")

    raise RuntimeError("Could not find neptune backend repository.\nPass the location using --neptune-repo-path.")


def copy_swagger_files(neptune_repo_path: Path, target_dir: Path, verbose: bool = False) -> None:
    # Write git reference from the backend repo (commit hash and date)
    (target_dir / "GIT_REF").write_text(get_commit_info(neptune_repo_path))

    # Copy proto dirs
    for proto_dir_src, proto_dir_dest in PROTO_DIRS.items():
        if verbose:
            print(
                f"  {rel(neptune_repo_path / proto_dir_src):82}  ->  {rel(target_dir / proto_dir_dest)}",
                file=sys.stderr,
            )
        shutil.copytree(neptune_repo_path / proto_dir_src, target_dir / proto_dir_dest)

    # Copy swagger files
    for swagger_file_src, swagger_file_dest in SWAGGER_FILES.items():
        if verbose:
            print(
                f"  {rel(neptune_repo_path / swagger_file_src):82}  ->  {rel(target_dir / swagger_file_dest)}",
                file=sys.stderr,
            )
        dest_path = target_dir / swagger_file_dest
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(neptune_repo_path / swagger_file_src, dest_path)


def get_commit_info(neptune_repo_path: Path) -> str:
    try:
        commit_hash = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=neptune_repo_path,
            text=True,
        ).strip()
        commit_date = subprocess.check_output(
            ["git", "show", "--no-patch", "--date=format-local:%Y-%m-%d %H:%M:%S UTC", "--format=%cd", "HEAD"],
            cwd=neptune_repo_path,
            text=True,
        ).strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error obtaining git commit info from neptune repo at {neptune_repo_path}: {e}")

    return textwrap.dedent(
        f"""\
            Files copied from neptune.git
            Commit hash: {commit_hash}
            Commit date: {commit_date}
        """
    )


def test_git_cmd_working():
    msg = 'Error: Cannot run "git". Please ensure that Git is installed.'
    try:
        subprocess.run(["git", "--version"], check=True, stdout=subprocess.DEVNULL)
    except Exception as e:
        raise RuntimeError(f"{e}\n\n{msg}")
