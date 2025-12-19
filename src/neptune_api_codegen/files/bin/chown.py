#!/usr/bin/env python3

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

# This script fixes file permissions in the mounted volume to ensure that files are owned by the host user

import os
from pathlib import Path
import sys


paths_updated = set()


def ensure_file_ownership(path: Path, uid: int, gid: int) -> None:
    try:
        path_stat = path.lstat()  # Use lstat to avoid following symlinks
        if path_stat.st_uid != uid or path_stat.st_gid != gid:
            # Skip logging if parent or grandparent was updated by us (to reduce verbosity)
            updated_parent = path.parent in paths_updated
            updated_grandparent = path.parent.parent in paths_updated
            if not updated_parent and not updated_grandparent:
                print(f"Fixing ownership of {path} from {path_stat.st_uid}:{path_stat.st_gid} to {uid}:{gid}", file=sys.stderr)
            paths_updated.add(path)
            os.chown(path, uid, gid)
    except Exception as e:
        print(f"Error processing {path}: {e}", file=sys.stderr)


def fix_file_ownership_in_dir(path: Path, ref_path: Path) -> None:
    """
    Fix file ownership in the specified directory to match the ownership of ref_path.
    """

    if not ref_path.exists():
        raise FileNotFoundError(f"Error: {ref_path} does not exist.")

    stat_info = ref_path.stat()
    uid = stat_info.st_uid
    gid = stat_info.st_gid

    if uid <= 0 and gid <= 0:
        # We're probably on Windows or the whole thing is owned by root, so do nothing
        print("UID and GID are non-positive; skipping ownership fix.", file=sys.stderr)
        return

    if not path.exists():
        print(f"Path {path} does not exist; skipping ownership fix.", file=sys.stderr)
        return

    # Now verify that all files in path are owned by this uid/gid, and if not, change them
    ensure_file_ownership(path, uid, gid)
    for root, dirs, files in os.walk(path):
        for name in dirs + files:
            ensure_file_ownership(Path(root) / name, uid, gid)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: chown.py <target_directory> <reference_path>", file=sys.stderr)
        sys.exit(1)

    target_directory = Path(sys.argv[1])
    reference_path = Path(sys.argv[2])

    try:
        fix_file_ownership_in_dir(target_directory, reference_path)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
