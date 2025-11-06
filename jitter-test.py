import timeit
from statistics import mean
from typing import Callable

import numpy as np

try:
    from numba import njit
except ImportError as exc:
    raise SystemExit(
        "Numba is required for this benchmark. Install it with `pip install numba` before running the script."
    ) from exc


NUM_LISTS = 4
LIST_LENGTH = 100000
REPEAT = 5
NUMBER = 1000

SENTINEL = np.int64(np.iinfo(np.int64).max)


def make_sorted_arrays(num_lists: int, list_length: int) -> list[np.ndarray]:
    rng = np.random.default_rng(seed=0)
    return [
        np.sort(rng.integers(0, 10_000, size=list_length, dtype=np.int64))
        for _ in range(num_lists)
    ]


@njit(cache=True)
def _merge_numba_impl(data: np.ndarray) -> np.ndarray:
    num_lists, list_len = data.shape
    total = num_lists * list_len
    indices = np.zeros(num_lists, dtype=np.int64)
    merged = np.empty(total, dtype=np.int64)

    out_idx = 0
    last_val = SENTINEL

    while True:
        best_val = SENTINEL
        best_list = -1
        for list_idx in range(num_lists):
            pos = indices[list_idx]
            if pos < list_len:
                value = data[list_idx, pos]
                if value < best_val:
                    best_val = value
                    best_list = list_idx

        if best_list == -1:
            break

        if best_val != last_val:
            merged[out_idx] = best_val
            out_idx += 1
            last_val = best_val

        for list_idx in range(num_lists):
            pos = indices[list_idx]
            if pos < list_len:
                value = data[list_idx, pos]
                if value == best_val:
                    pos += 1
                    while pos < list_len and data[list_idx, pos] == best_val:
                        pos += 1
                    indices[list_idx] = pos

    return merged[:out_idx]


def merge_numba(data: list[np.ndarray], precomputed: np.ndarray | None = None) -> np.ndarray:
    stacked = precomputed if precomputed is not None else np.vstack(data)
    return _merge_numba_impl(stacked)


def merge_numpy(data: list[np.ndarray]) -> np.ndarray:
    return np.unique(np.concatenate(data))


def time_function(action: Callable[[], np.ndarray]) -> float:
    timer = timeit.Timer(action)
    runs = timer.repeat(repeat=REPEAT, number=NUMBER)
    return mean(runs) / NUMBER


if __name__ == "__main__":
    dataset = make_sorted_arrays(NUM_LISTS, LIST_LENGTH)

    stacked_dataset = np.vstack(dataset)
    numpy_result = merge_numpy(dataset)
    numba_result = merge_numba(dataset, stacked_dataset)
    assert np.array_equal(numpy_result, numba_result)

    # Ensure Numba compilation happens before timing.
    merge_numba(dataset, stacked_dataset)

    benchmarks = {
        "NumPy sort": lambda: merge_numpy(dataset),
        "Numba merge": lambda: merge_numba(dataset, stacked_dataset),
    }

    for label, action in benchmarks.items():
        per_call = time_function(action)
        print(f"{label}: {per_call * 1_000_000:.2f} microseconds per merge")
