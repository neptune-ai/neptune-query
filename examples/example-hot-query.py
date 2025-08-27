import sys
import time
from neptune_query import runs

def timed(func):
    def wrapper():
        start_time = time.time()
        try:
            func()
        finally:
            end_time = time.time()
            print(f"Time taken: {end_time - start_time:.2f} seconds")

    return wrapper


@timed
def example_common():
    df = runs.fetch_runs_table(
        runs="^hot-1..$",
        attributes="^hot/common/.*$"
    )
    print(df.shape)

@timed
def example_unique():
    df = runs.fetch_runs_table(
        runs="^hot-1..$",
        attributes="hot/unique/.*"
    )
    print(df.shape)


@timed
def example_unique_separate():
    import concurrent.futures

    def fetch_run(i):
        df = runs.fetch_runs_table(
            runs=f"^hot-{i}$",
            attributes="hot/unique/.*"
        )
        print(df.shape)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(fetch_run, range(100, 200))

EXAMPLES = {
    "common": example_common, 
    "unique": example_unique,
    "unique_separate": example_unique_separate,
}


def main():
    arg = sys.argv[1]
    if arg in EXAMPLES:
        print(f"Running {arg}")
        EXAMPLES[arg]()
    else:
        raise ValueError(f"Invalid argument {arg}")


if __name__ == "__main__":
    main()

