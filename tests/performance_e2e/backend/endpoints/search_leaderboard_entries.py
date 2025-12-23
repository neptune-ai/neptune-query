"""
Endpoint for handling leaderboard entries search requests.
"""
import json
import random
import time
from typing import Final

from fastapi import (
    APIRouter,
    Request,
    Response,
)
from neptune_api.models.search_leaderboard_entries_params_dto import SearchLeaderboardEntriesParamsDTO
from neptune_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import ProtoLeaderboardEntriesSearchResultDTO

# Import attribute types from neptune_query
from neptune_query.internal.retrieval.attribute_types import ALL_TYPES
from tests.performance_e2e.backend.middleware.read_perf_config_middleware import PERF_REQUEST_CONFIG_ATTRIBUTE_NAME
from tests.performance_e2e.backend.perf_request import SearchLeaderboardEntriesConfig
from tests.performance_e2e.backend.utils.exceptions import MalformedRequestError
from tests.performance_e2e.backend.utils.logging import setup_logger
from tests.performance_e2e.backend.utils.metrics import RequestMetrics
from tests.performance_e2e.backend.utils.random_utils import (
    MAX_NUMERIC_VALUE,
    MIN_NUMERIC_VALUE,
    MIN_VARIANCE,
    random_string,
)
from tests.performance_e2e.backend.utils.timing import Timer

# Path without /api prefix since we're mounting under /api in the main app
SEARCH_LEADERBOARD_ENTRIES_ENDPOINT_PATH: Final[str] = "/leaderboard/v1/proto/leaderboard/entries/search/"
# Path used for configuration matching (with /api prefix for backward compatibility)
SEARCH_LEADERBOARD_ENTRIES_CONFIG_PATH: Final[str] = "/api" + SEARCH_LEADERBOARD_ENTRIES_ENDPOINT_PATH

logger = setup_logger("search_leaderboard_entries")

router = APIRouter()


def _build_page_result(endpoint_config: SearchLeaderboardEntriesConfig, limit: int, offset: int) -> bytes:
    """Build a protobuf page according to pagination parameters.

    Args:
        endpoint_config: Configuration for the endpoint
        limit: Maximum number of entries to return
        offset: Starting position in the virtual result set

    Returns:
        Serialized protobuf response with generated entries

    Raises:
        ValueError: If an unsupported attribute type is requested
    """
    start = offset
    end = min(offset + limit, endpoint_config.total_entries_count)
    actual_entry_count = max(0, end - start)

    logger.debug(f"Building page result: limit={limit}, offset={offset}, actual_entries={actual_entry_count}")

    if start >= endpoint_config.total_entries_count or not endpoint_config.requested_attributes:
        logger.debug(
            f"Returning empty result: start={start} >= total={endpoint_config.total_entries_count} " f"or no attributes"
        )
        return ProtoLeaderboardEntriesSearchResultDTO().SerializeToString()

    result = ProtoLeaderboardEntriesSearchResultDTO()

    # Pre-process attribute types for faster lookup
    processed_attrs = {
        name: attr_type.lower() if attr_type.lower() in ALL_TYPES else None
        for name, attr_type in endpoint_config.requested_attributes.items()
    }

    if invalid_attrs := {name: attr_type for name, attr_type in processed_attrs.items() if attr_type is None}:
        logger.error(f"Found invalid attribute types: {invalid_attrs}")
        raise NotImplementedError(f"Found invalid attribute types: {invalid_attrs}")

    entries_generated = 0
    for idx in range(start, end):
        entry = result.entries.add()
        entries_generated += 1

        for name, attr_type in processed_attrs.items():
            proto_attr = entry.attributes.add()
            proto_attr.name = name

            if attr_type == "string":
                proto_attr.string_properties.value = random_string()
            elif attr_type == "float":
                proto_attr.float_properties.value = random.uniform(MIN_NUMERIC_VALUE, MAX_NUMERIC_VALUE)
            elif attr_type == "int":
                proto_attr.int_properties.value = random.randint(int(MIN_NUMERIC_VALUE), int(MAX_NUMERIC_VALUE))
            elif attr_type == "bool":
                proto_attr.bool_properties.value = random.random() < 0.5
            elif attr_type == "float_series":
                min_val, max_val = sorted(random.uniform(MIN_NUMERIC_VALUE, MAX_NUMERIC_VALUE) for _ in (1, 2))

                proto_attr.float_series_properties.min = min_val
                proto_attr.float_series_properties.max = max_val
                proto_attr.float_series_properties.last = random.uniform(min_val, max_val)
                proto_attr.float_series_properties.average = random.uniform(min_val, max_val)
                proto_attr.float_series_properties.variance = random.uniform(MIN_VARIANCE, MAX_NUMERIC_VALUE)
            else:
                logger.error(f"Unsupported attribute type: {endpoint_config.requested_attributes.get(name)}")
                raise NotImplementedError(
                    f"Unsupported attribute type: {endpoint_config.requested_attributes.get(name)}"
                )

    if entries_generated > 0:
        logger.debug(f"Generated {entries_generated} entries with {len(processed_attrs)} attributes each")

    serialized_result = result.SerializeToString()
    logger.debug(f"Serialized result size: {len(serialized_result)} bytes")

    return serialized_result


@router.post(SEARCH_LEADERBOARD_ENTRIES_ENDPOINT_PATH)
async def search_leaderboard_entries(request: Request) -> Response:
    """Handle requests for leaderboard entries search."""
    metrics: RequestMetrics = request.state.metrics

    try:
        # Parse request body
        with Timer() as parsing_timer:
            raw_body = await request.json()
            parsed_request = SearchLeaderboardEntriesParamsDTO.from_dict(raw_body)

            # Get the configuration from middleware
            perf_config = getattr(request.state, PERF_REQUEST_CONFIG_ATTRIBUTE_NAME, None)
            if not perf_config:
                logger.error("No performance_e2e configuration found")
                raise MalformedRequestError("Missing or invalid X-Perf-Request header")

            # Get endpoint-specific configuration using the config path (with /api prefix)
            endpoint_config = perf_config.get_endpoint_config(SEARCH_LEADERBOARD_ENTRIES_CONFIG_PATH, "POST")
            if not endpoint_config or not isinstance(endpoint_config, SearchLeaderboardEntriesConfig):
                logger.warning("Missing configuration for this endpoint")
                raise MalformedRequestError("Missing configuration for this endpoint")

        metrics.parse_time_ms = parsing_timer.time_ms

        # Extract pagination parameters
        limit = int(parsed_request.pagination.limit)
        offset = int(parsed_request.pagination.offset)

        # Log request details
        attr_keys = list(endpoint_config.requested_attributes.keys())
        logger.info(
            f"Processing: limit={limit} offset={offset} attrs={attr_keys} "
            f"total_entries={endpoint_config.total_entries_count}"
        )

        # Generate and return protobuf response
        with Timer() as data_generation_timer:
            payload = _build_page_result(endpoint_config, limit, offset)

        metrics.generation_time_ms = data_generation_timer.time_ms
        metrics.returned_payload_size_bytes = len(payload)

        return Response(
            content=payload,
            media_type="application/octet-stream",
            status_code=200,
        )

    except MalformedRequestError as exc:
        logger.error(f"Invalid request configuration: {str(exc)}")
        return Response(
            status_code=400,
            content=json.dumps({"error": str(exc), "timestamp": time.time()}),
            media_type="application/json",
        )
    except Exception as exc:
        logger.exception(f"Unhandled exception during request processing: {exc}")
        return Response(
            status_code=500,
            content=json.dumps({"error": "Internal server error", "timestamp": time.time()}),
            media_type="application/json",
        )
