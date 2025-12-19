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

import argparse
import sys
from datetime import (
    datetime,
)
from importlib.metadata import (
    PackageNotFoundError,
    version,
)
from pathlib import Path
from random import random

from neptune_api_codegen.dirs import (
    copy_swagger_files,
    find_neptune_repo,
    find_project_root,
    is_neptune_repo,
    copy_generated_files,
)
from neptune_api_codegen.git import check_git_freshness
from neptune_api_codegen.docker import (
    run_in_docker,
    test_docker_permissions,
)

DIST_NAME = "nacc"
PROG_NAME = "nacc"


def _resolve_version() -> str:
    try:
        return version(DIST_NAME)
    except PackageNotFoundError:
        return "unknown"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG_NAME,
        description="Code generation for Neptune API bindings",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_resolve_version()}",
        help="Show the installed version and exit.",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        help="Path to the project root. If not specified, it will be auto-detected.",
        default=None,
    )
    parser.add_argument(
        "--neptune-repo-path",
        type=Path,
        help="Path to the neptune backend repository. If not specified, it will be auto-detected.",
        default=None,
    )
    parser.add_argument(
        "--last-commit-max-age-days",
        type=int,
        help="Maximum age of the last commit to consider the repository up-to-date in days. "
        "Set to 0 to skip checking. The default is 10 days.",
        default=10,
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Move the generated files to the target directory (if unset, the code is left in generated_python)",
        default=False,
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output for Docker commands.",
        default=False,
    )
    return parser


def stderr_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RuntimeError as e:
            if sys.stderr.isatty():
                msg_start = "\n\033[91m"
                msg_end = "\033[0m"
            else:
                msg_start = "\nERROR: "
                msg_end = ""

            print(f"{msg_start}{e}{msg_end}", file=sys.stderr)
            sys.exit(1)

    return wrapper


def rel(path: Path) -> str:
    cwd_parents = [Path.cwd()] + list(Path.cwd().parents)
    for level_up, prefix in [(0, "./"), (1, "../"), (2, "../../")]:
        try:
            return prefix + str(path.relative_to(cwd_parents[level_up]))
        except ValueError:
            continue
    # Fall back to showing the absolute path
    return str(path)


@stderr_errors
def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    test_docker_permissions()
    print("Testing if we can call Docker", file=sys.stderr)

    # Project root
    if args.project_root is None:
        args.project_root = find_project_root()

    # Neptune repo path
    if args.neptune_repo_path is None:
        args.neptune_repo_path = find_neptune_repo(args.project_root)
    elif not is_neptune_repo(args.neptune_repo_path):
        raise RuntimeError(
            f"The specified neptune repo path {args.neptune_repo_path} is not a valid neptune backend repository."
        )
    print(f"Using neptune backend repository at:              {rel(args.neptune_repo_path)}", file=sys.stderr)

    # Git freshness check
    if args.last_commit_max_age_days > 0:
        check_git_freshness(args.neptune_repo_path, args.last_commit_max_age_days)

    # Target and backup dirs
    rand_bit = int(random() * 1e4)
    neptune_api_dir = args.project_root / "neptune-api"
    backup_dir = neptune_api_dir / "backup" / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{rand_bit:04d}"
    apispec_dir = neptune_api_dir / "apispec"
    genenerated_py_dir = neptune_api_dir / "generated_python"
    target_dir = args.project_root / "src" / "neptune_query" / "internal" / "generated" / "neptune_api"
    target_backup_dir = backup_dir / "src" / "neptune_query" / "internal" / "generated" / "neptune_api"

    try:
        # Back up existing contents of apispec
        if apispec_dir.exists():
            print(f"Backing up existing apispec directory to:         {rel(backup_dir / 'apispec')}", file=sys.stderr)
            backup_dir.mkdir(parents=True, exist_ok=True)
            apispec_dir.rename(backup_dir / "apispec")

        # Back up existing contents of codegen/src
        if genenerated_py_dir.exists():
            print(
                f"Backing up existing apispec directory to:         {rel(backup_dir / 'generated_python')}", file=sys.stderr
            )
            (backup_dir / "generated_python").mkdir(parents=True, exist_ok=True)
            genenerated_py_dir.rename(backup_dir / "generated_python")

        # Re-create target directories
        apispec_dir.mkdir(parents=True, exist_ok=True)
        genenerated_py_dir.mkdir(parents=True, exist_ok=True)

        print(f"Copying proto directories and swagger files to:   {rel(apispec_dir)}", file=sys.stderr)
        copy_swagger_files(
            neptune_repo_path=args.neptune_repo_path,
            target_dir=apispec_dir,
        )

        print(f"Generating OpenAPI clients in:                    {rel(genenerated_py_dir)}", file=sys.stderr)
        run_in_docker(
            project_root=args.project_root,
            dockerfile="Dockerfile.generate-openapi",
            command=["/files/bin/generate-openapi-clients.sh", "/neptune-api/generated_python"],
            verbose=args.verbose,
        )

        output_dir_full = genenerated_py_dir / "neptune_api" / "proto"
        print(
            f"Generating protobuf code in:                      {rel(output_dir_full)}",
            file=sys.stderr,
        )
        run_in_docker(
            project_root=args.project_root,
            dockerfile="Dockerfile.generate-protobuf",
            command=["/files/bin/generate-protobuf.sh", "/neptune-api/generated_python/neptune_api/proto"],
            verbose=args.verbose,
        )

        print("Reformatting generated code with ruff", file=sys.stderr)
        run_in_docker(
            project_root=args.project_root,
            dockerfile="Dockerfile.ruff",
            command=["ruff", "format", "/neptune-api/generated_python/"],
            verbose=args.verbose,
        )

        if not args.update:
            print(
                '\nGeneration complete. The generated files are in ./neptune-api/generated_python.\n'
                "To move them to the target directories, re-run this command with --update flag.",
                file=sys.stderr,
            )
            return

    finally:
        print("Fixing file permissions in ./neptune-api", file=sys.stderr)
        run_in_docker(
            project_root=args.project_root,
            dockerfile="Dockerfile.chown",
            command=["/files/bin/chown.py", "/neptune-api", "/files"],
            verbose=args.verbose,
        )

    # Back up existing contents of target_dir
    if target_dir.exists():
        print(f"Backing up previously generated files to          {rel(target_backup_dir)}", file=sys.stderr)
        target_backup_dir.parent.mkdir(parents=True, exist_ok=True)
        target_dir.rename(target_backup_dir)

    print(f"Copying generated files to:                        {rel(target_dir)}", file=sys.stderr)
    copy_generated_files(genenerated_py_dir / "neptune_api", target_dir)


if __name__ == "__main__":
    main()
