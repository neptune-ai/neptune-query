from tests.performance.backend.perf_request import (
    LatencyConfig,
    PerfRequestConfig,
    SearchLeaderboardEntriesConfig,
)


class PerfRequestBuilder:
    """Helper for building X-Perf-Request headers in test cases."""

    def __init__(self):
        self._config = PerfRequestConfig()

    def with_latency(self, min_ms: float = 0, max_ms: float = 0) -> "PerfRequestBuilder":
        self._config.latency = LatencyConfig(min_ms=min_ms, max_ms=max_ms)
        return self

    def with_search_leaderboard_entries(self, attributes: dict[str, str], total_entries: int) -> "PerfRequestBuilder":
        config = SearchLeaderboardEntriesConfig(requested_attributes=attributes, total_entries_count=total_entries)
        self._config.add_endpoint_config("/api/leaderboard/v1/proto/leaderboard/entries/search/", "POST", config)
        return self

    def build(self) -> str:
        return self._config.to_json()
