import os
import threading
import time

import psutil


class MemoryMonitor:
    def __init__(self, interval: float = 0.5):
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
