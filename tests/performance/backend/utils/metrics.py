"""
Metrics utilities for performance monitoring.
"""
from dataclasses import dataclass


@dataclass
class RequestMetrics:
    """Collection of metrics for request processing performance monitoring."""

    parse_time_ms: float = 0.0
    generation_time_ms: float = 0.0
    total_time_ms: float = 0.0
    returned_payload_size_bytes: int = 0
    latency_added_ms: float = 0.0
