import dataclasses
import os
import sys
import threading
import time
from collections.abc import Mapping

import numpy as np
import psutil


class MemoryMonitor:
    def __init__(self, interval: float = 0.1):
        self.interval = interval
        self._thread = None
        self._running = False
        self._peak = 0
        self._process = psutil.Process(os.getpid())

    def _run(self):
        while self._running:
            try:
                mem = self._process.memory_info().rss  # w bajtach
                if mem > self._peak:
                    self._peak = mem
                time.sleep(self.interval)
            except psutil.NoSuchProcess:
                break

    def start(self):
        if self._running:
            raise RuntimeError("Monitor już działa.")
        self._peak = 0
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> int:
        if not self._running:
            raise RuntimeError("Monitor nie jest uruchomiony.")
        self._running = False
        if self._thread:
            self._thread.join(timeout=self.interval * 2)
        return self._peak


def get_size(obj, seen=None) -> int:
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)

    if isinstance(obj, np.ndarray):
        return size + obj.nbytes

    if dataclasses.is_dataclass(obj):
        for field in dataclasses.fields(obj):
            value = getattr(obj, field.name)
            size += get_size(value, seen)
        return size

    if isinstance(obj, Mapping):
        for k, v in obj.items():
            size += get_size(k, seen)
            size += get_size(v, seen)
        return size

    if isinstance(obj, (list, tuple, set, frozenset)):
        for item in obj:
            size += get_size(item, seen)
        return size

    if hasattr(obj, "__dict__"):
        size += get_size(vars(obj), seen)

    return size
