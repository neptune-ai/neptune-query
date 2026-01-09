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

import subprocess
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from pathlib import Path


def check_git_freshness(repo_path: Path, max_age_days: int) -> None:
    if not (repo_path / ".git").is_dir():
        raise RuntimeError(
            f"Error: The repository at {repo_path} is not a git repository. "
            "Cannot check the last commit age. "
            "(Use --last-commit-max-age-days=0 to skip this check.)"
        )
    try:
        # Use the commit timestamp (seconds since epoch, UTC) to avoid tz parsing issues
        result = subprocess.run(
            ["git", "-C", str(repo_path), "log", "-1", "--format=%ct"],
            capture_output=True,
            text=True,
            check=True,
        )
        ts_str = result.stdout.strip()
        if not ts_str:
            raise RuntimeError("git returned an empty commit timestamp")

        last_commit_date = datetime.fromtimestamp(int(ts_str), tz=timezone.utc)
        now = datetime.now(timezone.utc)

        if now - last_commit_date > timedelta(days=max_age_days):
            plural = "s" if max_age_days > 1 else ""
            raise RuntimeError(
                f"The last commit in the repository at {repo_path} is older than {max_age_days} day{plural}.\n"
                "Please update the repository. (Use --last-commit-max-age-days=0 to skip this check.)"
            )
    except RuntimeError as e:
        raise e
    except Exception as e:
        raise RuntimeError(
            f"Error checking git repository: {e}.\n"
            "Cannot check the last commit age. (Use --last-commit-max-age-days=0 to skip this check.)"
        )


def get_last_git_commit_message(repo_path: Path) -> str:
    if not (repo_path / ".git").is_dir():
        raise RuntimeError(f"The repository at {repo_path} is not a git repository.")

    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "log", "-1", "--pretty=full"],
            capture_output=True,
            text=True,
            check=True,
        )
        message = result.stdout.strip()
        if not message:
            raise RuntimeError("git returned an empty commit message")
        return message
    except Exception as e:
        raise RuntimeError(f"Error retrieving last git commit message: {e}")
