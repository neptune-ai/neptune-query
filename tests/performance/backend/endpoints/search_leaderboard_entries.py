"""
Endpoint for handling leaderboard entries search requests.
"""
import json
import random
import time
from typing import (
    Final,
    Optional,
)

from fastapi import (
    APIRouter,
    Request,
    Response,
)
from neptune_api.models.search_leaderboard_entries_params_dto import SearchLeaderboardEntriesParamsDTO
from neptune_api.proto.protobuf_v4plus.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
    ProtoLeaderboardEntriesSearchResultDTO,
)
from performance.backend.utils.logging import setup_logger
from performance.backend.utils.metrics import RequestMetrics

# Import attribute types from neptune_query
from neptune_query.internal.retrieval.attribute_types import (
    ALL_TYPES,
    ATTRIBUTE_LITERAL,
    map_attribute_type_backend_to_python,
)
from tests.performance.backend.perf_request import SearchLeaderboardEntriesConfig
from tests.performance.backend.utils.exceptions import MalformedRequestError
from tests.performance.backend.utils.random_utils import (
    MAX_NUMERIC_VALUE,
    MIN_NUMERIC_VALUE,
    MIN_VARIANCE,
    random_string,
)

# Path without /api prefix since we're mounting under /api in the main app
SEARCH_LEADERBOARD_ENTRIES_ENDPOINT_PATH: Final[str] = "/leaderboard/v1/proto/leaderboard/entries/search/"
# Path used for configuration matching (with /api prefix for backward compatibility)
SEARCH_LEADERBOARD_ENTRIES_CONFIG_PATH: Final[str] = "/api" + SEARCH_LEADERBOARD_ENTRIES_ENDPOINT_PATH

logger = setup_logger("search_leaderboard_entries")

router = APIRouter()


def _parse_attribute_type(attr_type: str) -> Optional[ATTRIBUTE_LITERAL]:
    """Convert string attribute type to standard attribute type.

    Args:
        attr_type: String representation of attribute type

    Returns:
        Corresponding attribute type from ATTRIBUTE_LITERAL or None if not recognized
    """
    # Handle python-style attribute types
    lowercase_type = attr_type.lower()

    # Check if it's already a valid attribute type
    if lowercase_type in ALL_TYPES:
        return lowercase_type  # type: ignore

    # Try to map backend type to python type
    backend_mapped = map_attribute_type_backend_to_python(lowercase_type)
    if backend_mapped in ALL_TYPES:
        return backend_mapped  # type: ignore

    return None


def _build_page_result(
    endpoint_config: SearchLeaderboardEntriesConfig, limit: int, offset: int, request_id: str
) -> bytes:
    """Build a protobuf page according to pagination parameters.

    Args:
        endpoint_config: Configuration for the endpoint
        limit: Maximum number of entries to return
        offset: Starting position in the virtual result set
        request_id: Unique identifier for the request for correlation in logs

    Returns:
        Serialized protobuf response with generated entries

    Raises:
        NotImplementedError: If an unsupported attribute type is requested
    """
    start = offset
    end = min(offset + limit, endpoint_config.total_entries_count)
    actual_entry_count = max(0, end - start)

    logger.debug(
        f"[{request_id}] Building page result: limit={limit}, offset={offset}, actual_entries={actual_entry_count}"
    )

    if start >= endpoint_config.total_entries_count or not endpoint_config.requested_attributes:
        logger.debug(
            f"[{request_id}] Returning empty result: start={start} >= total={endpoint_config.total_entries_count} "
            f"or no attributes"
        )
        return ProtoLeaderboardEntriesSearchResultDTO().SerializeToString()

    result = ProtoLeaderboardEntriesSearchResultDTO()

    # Pre-process attribute types for faster lookup
    processed_attrs = {
        name: _parse_attribute_type(attr_type) for name, attr_type in endpoint_config.requested_attributes.items()
    }
    invalid_attrs = [name for name, attr_type in processed_attrs.items() if attr_type is None]

    if invalid_attrs:
        logger.warning(f"[{request_id}] Found invalid attribute types: {invalid_attrs}")

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
                min_val = random.uniform(MIN_NUMERIC_VALUE, MAX_NUMERIC_VALUE)
                max_val = random.uniform(min_val, MAX_NUMERIC_VALUE)

                proto_attr.float_series_properties.min = min_val
                proto_attr.float_series_properties.max = max_val
                proto_attr.float_series_properties.last = random.uniform(min_val, max_val)
                proto_attr.float_series_properties.average = random.uniform(min_val, max_val)
                proto_attr.float_series_properties.variance = random.uniform(MIN_VARIANCE, MAX_NUMERIC_VALUE)
            else:
                logger.error(
                    f"[{request_id}] Unsupported attribute type: {endpoint_config.requested_attributes.get(name)}"
                )
                raise NotImplementedError(
                    f"Unsupported attribute type: {endpoint_config.requested_attributes.get(name)}"
                )

    if entries_generated > 0:
        logger.debug(
            f"[{request_id}] Generated {entries_generated} entries with {len(processed_attrs)} attributes each"
        )

    serialized_result = result.SerializeToString()
    logger.debug(f"[{request_id}] Serialized result size: {len(serialized_result)} bytes")

    return serialized_result


@router.post(SEARCH_LEADERBOARD_ENTRIES_ENDPOINT_PATH)
async def search_leaderboard_entries(request: Request) -> Response:
    """Handle requests for leaderboard entries search."""
    request_id = getattr(request.state, "id", f"req-{random_string(8)}")
    metrics: RequestMetrics = request.state.metrics

    try:
        # Parse request body
        parse_start_ms = time.time() * 1000
        raw_body = await request.json()
        parsed_request = SearchLeaderboardEntriesParamsDTO.from_dict(raw_body)

        # Get the configuration from middleware
        perf_config = getattr(request.state, "perf_config", None)
        if not perf_config:
            logger.warning(f"[{request_id}] No performance configuration found")
            raise MalformedRequestError("Missing or invalid X-Perf-Request header")

        # Get endpoint-specific configuration using the config path (with /api prefix)
        endpoint_config = perf_config.get_endpoint_config(SEARCH_LEADERBOARD_ENTRIES_CONFIG_PATH, "POST")
        if not endpoint_config or not isinstance(endpoint_config, SearchLeaderboardEntriesConfig):
            logger.warning(f"[{request_id}] Missing configuration for this endpoint")
            raise MalformedRequestError("Missing configuration for this endpoint")

        metrics.parse_time_ms = time.time() * 1000 - parse_start_ms

        # Extract pagination parameters
        limit = int(parsed_request.pagination.limit)
        offset = int(parsed_request.pagination.offset)

        # Log request details
        client_host = request.client.host if request.client else "unknown"
        attr_keys = list(endpoint_config.requested_attributes.keys())
        logger.info(
            f"Processing request: client={client_host} limit={limit} offset={offset} attrs={attr_keys} "
            f"total_entries={endpoint_config.total_entries_count}"
        )

        # Generate and return protobuf response
        generation_start_ms = time.time() * 1000
        payload = _build_page_result(endpoint_config, limit, offset, request_id)
        metrics.generation_time_ms = time.time() * 1000 - generation_start_ms
        metrics.returned_payload_size_bytes = len(payload)

        return Response(
            content=payload,
            media_type="application/octet-stream",
            status_code=200,
            headers={
                "X-Response-Entries": str(min(limit, max(0, endpoint_config.total_entries_count - offset))),
                "X-Response-Size": str(metrics.returned_payload_size_bytes),
                "X-Generation-Time-Ms": str(round(metrics.generation_time_ms, 2)),
            },
        )

    except MalformedRequestError as exc:
        logger.error(f"[{request_id}] Invalid request configuration: {str(exc)}")
        return Response(
            status_code=400,
            content=json.dumps({"error": str(exc), "request_id": request_id, "timestamp": time.time()}),
            media_type="application/json",
        )
    except Exception as exc:
        logger.exception(f"[{request_id}] Unhandled exception during request processing: {exc}")
        return Response(
            status_code=500,
            content=json.dumps({"error": "Internal server error", "request_id": request_id, "timestamp": time.time()}),
            media_type="application/json",
        )
