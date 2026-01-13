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

import subprocess
import sys
from pathlib import Path

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: chown.py <target_directory> <reference_path>", file=sys.stderr)
        sys.exit(1)

    target_directory = Path(sys.argv[1])
    reference_path = Path(sys.argv[2])

    if not target_directory.exists():
        raise FileNotFoundError(f"Error: {target_directory} does not exist.")

    if not reference_path.exists():
        raise FileNotFoundError(f"Error: {reference_path} does not exist.")

    stat_info = reference_path.stat()
    uid = stat_info.st_uid
    gid = stat_info.st_gid

    if uid <= 0 and gid <= 0:
        # We're probably on Windows and/or the whole thing is owned by root, so do nothing
        print("UID and GID are non-positive; skipping ownership fix.", file=sys.stderr)
        sys.exit(0)

    try:
        subprocess.run(["chown", "-R", f"{uid}:{gid}", target_directory], check=True)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
