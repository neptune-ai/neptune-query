"""
Metrics utilities for performance monitoring.
"""
from dataclasses import dataclass


@dataclass
class RequestMetrics:
    """Collection of metrics for request processing performance monitoring."""

    parse_time_ms: float = 0.0  # Time taken to parse the request
    generation_time_ms: float = 0.0  # Time taken to generate the response data
    latency_added_ms: float = 0.0  # Artificial latency added to simulate processing / network delays
    total_time_ms: float = 0.0  # Total time including parsing, data generation and artificial latency
    returned_payload_size_bytes: int = 0  # Size of the response payload in bytes
