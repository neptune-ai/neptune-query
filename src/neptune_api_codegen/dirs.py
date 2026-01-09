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
from pathlib import Path

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

EXTRA_FILES = {
    "neptune_api/auth_helpers.py": "neptune_api/auth_helpers.py",
    "neptune_api/client.py": "neptune_api/client.py",
    "neptune_api/credentials.py": "neptune_api/credentials.py",
}


def find_project_root() -> Path:
    current_dir = Path.cwd()
    for candidate in [current_dir] + list(current_dir.parents):
        if (candidate / "pyproject.toml").exists() and (candidate / ".git").exists():
            return candidate
    raise RuntimeError("Could not find the project root (directory containing pyproject.toml and .git).")


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


def copy_swagger_files(neptune_repo_path: Path, target_dir: Path) -> None:
    # Copy proto dirs
    for proto_dir_src, proto_dir_dest in PROTO_DIRS.items():
        shutil.copytree(neptune_repo_path / proto_dir_src, target_dir / proto_dir_dest)

    # Copy swagger files
    for swagger_file_src, swagger_file_dest in SWAGGER_FILES.items():
        dest_path = target_dir / swagger_file_dest
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(neptune_repo_path / swagger_file_src, dest_path)


def copy_generated_files(source_dir: Path, target_dir: Path):
    shutil.copytree(source_dir, target_dir, dirs_exist_ok=True)


def remove_empty_dirs(dir: Path) -> bool:
    """
    Remove empty directories from the dir,
    and then remove the dir itself if it's empty
    """

    if not dir.is_dir():
        return

    for file in dir.iterdir():
        if file.is_dir():
            if not remove_empty_dirs(file):
                return False

    try:
        dir.rmdir()
    except OSError:
        # Directory not empty
        return False

    return True


def remove_work_dir(work_dir: Path) -> None:
    shutil.rmtree(work_dir)
    remove_empty_dirs(work_dir.parent)
