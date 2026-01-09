import sys
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
