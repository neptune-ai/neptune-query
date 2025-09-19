"""
Common schema and utilities for X-Perf-Request headers used in performance testing.
This module provides shared functionality for both test cases and the server implementation.
"""
import json
from dataclasses import (
    asdict,
    dataclass,
    field,
)
from typing import (
    ClassVar,
    Dict,
    Optional,
)


@dataclass
class LatencyConfig:
    """Configuration for simulated latency."""

    min_ms: float = 0.0
    max_ms: float = 0.0


@dataclass
class EndpointConfig:
    """Base class for endpoint-specific configuration."""

    pass


@dataclass
class SearchLeaderboardEntriesConfig(EndpointConfig):
    """Configuration for the search_leaderboard_entries endpoint."""

    # name -> type, e.g. {"accuracy": "float", "status": "string"}
    requested_attributes: Dict[str, str] = field(default_factory=dict)
    total_entries_count: int = 0


@dataclass
class PerfRequestConfig:
    """Schema for the X-Perf-Request header."""

    # Registry of endpoint paths to their configuration classes
    ENDPOINT_CONFIG_CLASSES: ClassVar[Dict[str, dict[str, type]]] = {
        "/api/leaderboard/v1/proto/leaderboard/entries/search/": {"POST": SearchLeaderboardEntriesConfig}
    }

    # Generic configurations
    latency: Optional[LatencyConfig] = field(default_factory=LatencyConfig)

    # Endpoint-specific configurations: path -> method -> config
    endpoints_configuration: Dict[str, Dict[str, EndpointConfig]] = field(default_factory=dict)

    def add_endpoint_config(self, path: str, method: str, config: EndpointConfig) -> None:
        """Add configuration for a specific endpoint.

        Args:
            path: API endpoint path
            method: HTTP method (GET, POST, etc.)
            config: Configuration object for the endpoint
        """
        if path not in self.endpoints_configuration:
            self.endpoints_configuration[path] = {}
        self.endpoints_configuration[path][method] = config

    def get_endpoint_config(self, path: str, method: str) -> Optional[EndpointConfig]:
        """Get configuration for a specific endpoint.

        Args:
            path: API endpoint path
            method: HTTP method (GET, POST, etc.)

        Returns:
            Configuration object for the endpoint or None if not found
        """
        return self.endpoints_configuration.get(path, {}).get(method)

    @classmethod
    def _serialize_dataclass(cls, obj):
        """Helper to serialize dataclasses to dictionaries."""
        if hasattr(obj, "__dataclass_fields__"):
            result = {}
            for k, v in asdict(obj).items():
                result[k] = cls._serialize_dataclass(v)
            return result
        elif isinstance(obj, dict):
            return {k: cls._serialize_dataclass(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [cls._serialize_dataclass(i) for i in obj]
        else:
            return obj

    def to_json(self) -> str:
        """Serialize the configuration to JSON format.

        Returns:
            JSON string representation of the configuration
        """
        return json.dumps(self._serialize_dataclass(self))

    @classmethod
    def from_json(cls, json_str: str) -> "PerfRequestConfig":
        """Deserialize a configuration from JSON.

        Args:
            json_str: JSON string to parse

        Returns:
            PerfRequestConfig object
        """
        data = json.loads(json_str)

        # Create a new config object
        config = cls()

        # Parse generic configurations
        if "latency" in data:
            config.latency = LatencyConfig(**data["latency"])

        # Parse endpoint-specific configurations
        if "endpoints_configuration" in data:
            endpoints_data = data["endpoints_configuration"]
            for path, methods in endpoints_data.items():
                for method, endpoint_config in methods.items():
                    # Look up the correct config class for this endpoint
                    config_class = cls.ENDPOINT_CONFIG_CLASSES.get(path, {}).get(method)
                    if config_class:
                        # Create and add the config object
                        endpoint_obj = config_class(**endpoint_config)
                        config.add_endpoint_config(path, method, endpoint_obj)

        return config
