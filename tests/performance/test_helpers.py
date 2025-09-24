from typing import Optional

from tests.performance.backend.perf_request import (
    FloatTimeSeriesValuesConfig,
    LatencyConfig,
    PerfRequestConfig,
    QueryAttributeDefinitionsConfig,
    SearchLeaderboardEntriesConfig,
)


class PerfRequestBuilder:
    """Helper for building X-Perf-Request headers in test cases."""

    def __init__(self):
        self._config = PerfRequestConfig()

    def with_search_leaderboard_entries(
        self,
        attributes: dict[str, str],
        total_entries: int,
        latency_range_ms: Optional[tuple[float, float]] = None,
    ) -> "PerfRequestBuilder":
        """Configure the search_leaderboard_entries endpoint.

        Args:
            attributes: Dict mapping attribute names to their types
            total_entries: Total number of entries to return across pagination
            latency_range_ms: Optional min/max latency in ms for this endpoint
        """
        latency = LatencyConfig(min_ms=latency_range_ms[0], max_ms=latency_range_ms[1]) if latency_range_ms else None
        self._config.add_endpoint_config(
            path="/api/leaderboard/v1/proto/leaderboard/entries/search/",
            method="POST",
            config=SearchLeaderboardEntriesConfig(
                latency=latency, requested_attributes=attributes, total_entries_count=total_entries
            ),
        )
        return self

    def with_query_attribute_definitions(
        self,
        seed: int,
        attribute_types: list[str],
        total_definitions: int,
        latency_range_ms: Optional[tuple[float, float]] = None,
    ) -> "PerfRequestBuilder":
        """Configure the query_attribute_definitions endpoint.

        Args:
            attribute_types: List of attribute types to include in the response
            total_definitions: Total number of attribute definitions to return across pagination
            latency_range_ms: Optional min/max latency in ms for this endpoint
        """
        latency = LatencyConfig(min_ms=latency_range_ms[0], max_ms=latency_range_ms[1]) if latency_range_ms else None
        self._config.add_endpoint_config(
            path="/api/leaderboard/v1/leaderboard/attributes/definitions/query",
            method="POST",
            config=QueryAttributeDefinitionsConfig(
                latency=latency, seed=seed, attribute_types=attribute_types, total_definitions_count=total_definitions
            ),
        )
        return self

    def with_multiple_float_series_values(
        self,
        seed: int,
        existence_probability: float,
        series_cardinality_policy: str,
        series_cardinality_uniform_range: tuple[int, int] = None,
        series_cardinality_buckets: list[tuple[float, float]] = None,
        latency_range_ms: Optional[tuple[float, float]] = None,
    ) -> "PerfRequestBuilder":
        """Configure the get_multiple_float_series_values endpoint.

        Args:
            existence_probability: Probability that a requested series exists
            seed: Seed for deterministic point generation
            series_cardinality_policy: Policy for determining series cardinality ("uniform" or "bucketed")
            series_cardinality_uniform_range: For "uniform" policy: (min_points, max_points)
            series_cardinality_buckets: For "bucketed" policy: list of (probability, num_points) tuples
            latency_range_ms: Optional min/max latency in ms for this endpoint

        Returns:
            Self for chaining
        """

        latency = LatencyConfig(min_ms=latency_range_ms[0], max_ms=latency_range_ms[1]) if latency_range_ms else None

        self._config.add_endpoint_config(
            path="/api/leaderboard/v1/proto/attributes/series/float",
            method="POST",
            config=FloatTimeSeriesValuesConfig(
                latency=latency,
                seed=seed,
                existence_probability=existence_probability,
                series_cardinality_policy=series_cardinality_policy,
                series_cardinality_uniform_range=series_cardinality_uniform_range,
                series_cardinality_buckets=series_cardinality_buckets,
            ),
        )
        return self

    def build(self) -> str:
        return self._config.to_json()
