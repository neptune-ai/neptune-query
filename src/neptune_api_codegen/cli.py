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
import shutil
from datetime import datetime
from pathlib import Path
from random import random

from neptune_api_codegen.docker import (
    run_in_docker,
    test_docker_permissions,
)
from neptune_api_codegen.fmt import (
    bold,
    print_err,
    rel,
    stderr_errors,
)
from neptune_api_codegen.neptune_repo import (
    copy_swagger_files,
    find_neptune_repo,
    is_neptune_repo,
    test_git_cmd_working,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="neptune_api_codegen",
        description="Code generation for Neptune API bindings",
    )
    parser.add_argument(
        "--neptune-repo-path",
        type=Path,
        help="Path to the neptune backend repository. If not specified, it will be auto-detected.",
        default=None,
    )
    parser.add_argument(
        "--no-update-api-spec",
        action="store_true",
        help="Do not fetch new API spec files (swagger, proto) from Neptune repo.",
        default=False,
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Move the generated files to the target directory (if unset, the code is left in generated_python)",
        default=False,
    )
    parser.add_argument(
        "--keep-tmpdir",
        action="store_true",
        help="Keep the temporary working directory with generated files (for inspection).",
        default=False,
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output for Docker commands.",
        default=False,
    )
    return parser


def fix_dir_permissions(dirs: list[Path], *, ignore_errors: bool = False, verbose: bool = False) -> None:
    for dir in dirs:
        if not dir.exists():
            continue

        command = "/rofiles/bin/chown.py /work /rofiles"
        if ignore_errors:
            command += " || true"

        try:
            run_in_docker(
                work_dir=dir,
                dockerfile="Dockerfile.chown",
                command=["bash", "-c", command],
                verbose=verbose,
            )
        except Exception as e:
            if not ignore_errors:
                raise e
            if verbose:
                print_err(f"Warning: Could not fix permissions in {rel(dir)}: {e}")


def find_project_root() -> Path:
    current_dir = Path.cwd()
    for candidate in [current_dir] + list(current_dir.parents):
        if (candidate / "pyproject.toml").exists() and (candidate / ".git").exists():
            return candidate
    raise RuntimeError("Could not find the project root (directory containing pyproject.toml and .git).")


def copy_generated_files(source_dir: Path, target_dir: Path):
    shutil.copytree(source_dir, target_dir, dirs_exist_ok=True)


def _remove_empty_dirs(dir: Path) -> bool:
    """
    Remove empty directories from the dir,
    and then remove the dir itself if it's empty
    """

    for file in dir.iterdir():
        if file.is_dir():
            if not _remove_empty_dirs(file):
                return False

    try:
        dir.rmdir()
    except OSError:
        # Directory is not empty
        return False

    return True


def remove_work_dir(work_dir: Path) -> None:
    shutil.rmtree(work_dir)
    _remove_empty_dirs(work_dir.parent)


@stderr_errors
def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    test_docker_permissions()
    test_git_cmd_working()

    # Project root
    project_root = find_project_root()

    update_api_spec = not args.no_update_api_spec

    if update_api_spec:
        # Neptune repo path
        if args.neptune_repo_path is None:
            args.neptune_repo_path = find_neptune_repo(project_root)
        elif not is_neptune_repo(args.neptune_repo_path):
            raise RuntimeError(
                f"The specified neptune repo path {args.neptune_repo_path} is not a valid neptune backend repository."
            )
        print_err(f"Using neptune backend repository at:              {rel(args.neptune_repo_path)}")

    # Target and backup dirs
    rand_bit = int(random() * 1e4)
    tmp_dir = project_root / "tmp"
    work_dir = tmp_dir / f"neptune-api__{datetime.now().strftime('%Y%m%d_%H%M%S')}_{rand_bit:04d}"
    apispec_dir = work_dir / "api_spec"
    generated_py_dir = work_dir / "generated_python"
    target_apispec = project_root / "src" / "neptune_query" / "generated" / "neptune_api_spec"
    target_py_dir = project_root / "src" / "neptune_query" / "generated" / "neptune_api"

    dirs_to_chown_afterwards = [
        tmp_dir,
        work_dir,
        target_apispec,
        target_py_dir,
    ]

    final_message = None

    try:
        print_err(f"Setting up directories in:                        {rel(tmp_dir)}")
        tmp_dir.mkdir(parents=True, exist_ok=True)
        work_dir.mkdir()
        apispec_dir.mkdir()
        generated_py_dir.mkdir()

        if update_api_spec:
            print_err(f"Copying proto directories and swagger files to:   {rel(apispec_dir)}")
            copy_swagger_files(
                neptune_repo_path=args.neptune_repo_path,
                target_dir=apispec_dir,
                verbose=args.verbose,
            )
        else:
            print_err(f"Using proto directories and swagger files from:   {rel(target_apispec)}")
            shutil.copytree(target_apispec, apispec_dir, dirs_exist_ok=True)

        print_err(f"Generating OpenAPI clients in:                    {rel(generated_py_dir)}")
        run_in_docker(
            work_dir=work_dir,
            dockerfile="Dockerfile.generate-openapi",
            command=["/rofiles/bin/generate-openapi-clients.sh", "/work/generated_python"],
            verbose=args.verbose,
        )

        output_dir_full = generated_py_dir / "neptune_api" / "proto"
        print_err(f"Generating protobuf code in:                      {rel(output_dir_full)}")
        run_in_docker(
            work_dir=work_dir,
            dockerfile="Dockerfile.generate-protobuf",
            command=["/rofiles/bin/generate-protobuf.sh", "/work/generated_python/neptune_api/proto"],
            verbose=args.verbose,
        )

        if not args.update and not args.keep_tmpdir:
            final_message = (
                bold("\nDry-run successful.\n"),
                "We successfully generated the files in a temporary directory that will be now removed.\n",
                "To apply the change to the target directories, re-run this command with --update flag.",
                "To inspect the generated files, re-run this command with --keep-tmpdir flag.\n",
            )
            remove_work_dir(work_dir)

        if not args.update and args.keep_tmpdir:
            final_message = (
                bold("\nDry-run successful.\n"),
                "We successfully generated the files in a temporary directory that will be kept for inspection:\n",
                f"  {rel(work_dir)}\n",
                "To apply the change to the target directories, re-run this command with --update flag.\n",
            )

        if args.update:
            if target_apispec.exists():
                print_err(f"Deleting files from:                              {rel(target_apispec)}")
                shutil.rmtree(target_apispec)

            print_err(f"Copying generated spec files to:                  {rel(target_apispec)}")
            copy_generated_files(apispec_dir, target_apispec)

            if target_py_dir.exists():
                print_err(f"Deleting files from:                              {rel(target_py_dir)}")
                shutil.rmtree(target_py_dir)

            print_err(f"Copying generated Python files to:                {rel(target_py_dir)}")
            copy_generated_files(generated_py_dir / "neptune_api", target_py_dir)

            final_message = (
                bold("\nUpdate complete.\n"),
                "The generated files have been copied to the target directories:\n",
                f" - API spec:   {rel(target_apispec)}",
                f" - Python:     {rel(target_py_dir)}\n",
                "Please, run pre-commit now to format the generated code:\n",
                "    pre-commit run --all-files\n",
            )

        if not args.keep_tmpdir:
            try:
                remove_work_dir(work_dir)
                tmp_dir.rmdir()
            except OSError:  # Directory is not empty
                pass

    # Having run things in Docker means we can be left with root-owned files.
    # We attempt to fix the permissions in all relevant directories.

    except Exception as e:
        # Don't add verbosity here - the original exception is more important
        fix_dir_permissions(dirs_to_chown_afterwards, ignore_errors=True, verbose=False)
        raise e

    else:
        print_err("Fixing file permissions...")
        fix_dir_permissions(dirs_to_chown_afterwards, verbose=args.verbose)

    print_err(*final_message)


if __name__ == "__main__":
    main()
