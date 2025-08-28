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
    df = runs.fetch_metrics(
        runs="^burn-.*$",
        attributes="^burn/metrics/[123]0.*$"
    )
    print(df.shape)


@timed
def example_common_concurrent():
    import concurrent.futures

    def fetch_run(i):
        df = runs.fetch_metrics(
            runs=f"^burn-{i}$",
            attributes="^burn/metrics/[123]0.*$"
        )
        print(df.shape)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(fetch_run, range(100))

EXAMPLES = {
    "common": example_common, 
    "common_concurrent": example_common_concurrent, 
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

