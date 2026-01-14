import sys
from pathlib import Path
from subprocess import SubprocessError


def print_err(*lines):
    print(*lines, sep="\n", file=sys.stderr)


def bold(text: str) -> str:
    if sys.stderr.isatty():
        return f"\033[1m{text}\033[0m"
    else:
        return text


def stderr_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (RuntimeError, SubprocessError) as e:
            if sys.stderr.isatty():
                msg_start = "\n\033[91m"
                msg_end = "\033[0m\n"
            else:
                msg_start = "\nERROR: "
                msg_end = ""

            print_err(f"{msg_start}{e}{msg_end}")
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
