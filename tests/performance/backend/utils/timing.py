import time
from typing import Optional


class Timer:
    def __init__(self):
        self._total_time_ms: Optional[float] = None

    @property
    def time_ms(self):
        assert self._total_time_ms is not None
        return self._total_time_ms

    def __enter__(self):
        self._enter_time = time.perf_counter_ns()
        return self

    def __exit__(self, *exc_args):
        self._total_time_ms = (time.perf_counter_ns() - self._enter_time) / 1_000_000
