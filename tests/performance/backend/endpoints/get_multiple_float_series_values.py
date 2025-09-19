"""
Endpoint for handling multiple float series values requests.
"""
import hashlib
import json
import math
import random
from typing import (
    Final,
    Optional,
)

from fastapi import (
    APIRouter,
    Request,
    Response,
)
from neptune_api.models.float_time_series_values_request import FloatTimeSeriesValuesRequest
from neptune_api.proto.protobuf_v4plus.neptune_pb.api.v1.model.series_values_pb2 import (
    ProtoFloatSeriesValuesResponseDTO,
)
from neptune_api.types import Unset

from tests.performance.backend.middleware.read_perf_config_middleware import PERF_REQUEST_CONFIG_ATTRIBUTE_NAME
from tests.performance.backend.perf_request import FloatTimeSeriesValuesConfig
from tests.performance.backend.utils.exceptions import MalformedRequestError
from tests.performance.backend.utils.logging import setup_logger
from tests.performance.backend.utils.metrics import RequestMetrics
from tests.performance.backend.utils.timing import Timer

# Path without /api prefix since we're mounting under /api in the main app
GET_MULTIPLE_FLOAT_SERIES_VALUES_ENDPOINT_PATH: Final[str] = "/leaderboard/v1/proto/attributes/series/float"
# Path used for configuration matching (with /api prefix for backward compatibility)
GET_MULTIPLE_FLOAT_SERIES_VALUES_CONFIG_PATH: Final[str] = "/api" + GET_MULTIPLE_FLOAT_SERIES_VALUES_ENDPOINT_PATH

logger = setup_logger("get_multiple_float_series_values")

router = APIRouter()


def _compute_series_cardinality(
    config: FloatTimeSeriesValuesConfig,
    experiment_id: str,
    attribute_def: str,
) -> int:
    """Compute the number of points for a series based on configuration and series identity.

    Args:
        config: Endpoint configuration
        experiment_id: Experiment ID
        attribute_def: Attribute definition path

    Returns:
        Number of points to generate for this series
    """
    hash_value = _hash_to_uniform_0_1(experiment_id, attribute_def, config.seed)

    if config.series_cardinality_policy == "uniform":
        if not config.series_cardinality_uniform_range:
            raise ValueError("Missing uniform range for uniform cardinality policy")

        min_points, max_points = config.series_cardinality_uniform_range
        # Use the hash to determine a value in the range
        points = min_points + math.floor(hash_value * (max_points - min_points + 1))
        return points

    elif config.series_cardinality_policy == "bucketed":
        if not config.series_cardinality_buckets:
            raise ValueError("Missing buckets for bucketed cardinality policy")

        # Normalize the bucket probabilities
        total_prob = sum(prob for prob, _ in config.series_cardinality_buckets)
        normalized_buckets = [(prob / total_prob, points) for prob, points in config.series_cardinality_buckets]

        # Use the hash to select a bucket
        cumulative_prob = 0
        for prob, points in normalized_buckets:
            cumulative_prob += prob
            if hash_value <= cumulative_prob:
                return int(points)

        # Fallback to last bucket
        return int(normalized_buckets[-1][1])

    else:
        raise ValueError(f"Unknown cardinality policy: {config.series_cardinality_policy}")


def _generate_series_values(
    series_cardinality: int, after_step: Optional[float], max_values: int
) -> list[tuple[float, float, float]]:  # (timestamp in seconds, step, value)
    """Generate time series values for a specific series.

    Returns:
        List of (timestamp, step, value) tuples
    """

    initial_step = 1 if after_step is None else (after_step + 1)
    total_remaining_steps = series_cardinality - (initial_step - 1)
    steps_in_current_request = min(total_remaining_steps, max_values)
    max_step = initial_step + steps_in_current_request - 1
    step_range = range(int(initial_step), int(max_step) + 1)

    return [(1600000000 + step, step, random.uniform(-1e6, 1e6)) for step in step_range]


def _build_float_series_response(
    parsed_request: FloatTimeSeriesValuesRequest,
    endpoint_config: FloatTimeSeriesValuesConfig,
) -> ProtoFloatSeriesValuesResponseDTO:
    """Build a response for the float series values request.

    Args:
        parsed_request: Parsed request object
        endpoint_config: Endpoint configuration

    Returns:
        Protobuf response object
    """
    response = ProtoFloatSeriesValuesResponseDTO()

    # Process each series request
    for series_req in parsed_request.requests:
        request_id = series_req.request_id

        # Extract experiment_id and attribute_name from TimeSeries
        experiment_id = series_req.series.holder.identifier
        attribute_name = series_req.series.attribute
        after_step = _map_unset_to_none(series_req.after_step)

        # Check if this series exists based on probability
        # Always use the seed from the perf config for consistent hashing
        series_hash = hashlib.md5(f"{experiment_id}:{attribute_name}:{endpoint_config.seed}".encode()).hexdigest()
        hash_value = int(series_hash, 16) / (2**128 - 1)

        # Create a series entry in the response
        series_dto = response.series.add()
        series_dto.requestId = request_id

        # Skip generating points if the series doesn't exist according to probability
        if hash_value > endpoint_config.existence_probability:
            continue

        # Determine how many points to generate
        series_cardinality = _compute_series_cardinality(endpoint_config, experiment_id, attribute_name)
        # Limit points by the per_series_points_limit

        # Generate series values
        values = _generate_series_values(
            series_cardinality=series_cardinality,
            after_step=after_step,
            max_values=parsed_request.per_series_points_limit,
        )

        # Add values to the response
        for timestamp, step, value in values:
            point = series_dto.series.values.add()
            point.timestamp_millis = int(timestamp * 1000)  # Convert to milliseconds
            point.step = step
            point.value = value
            point.is_preview = False
            point.completion_ratio = 1.0

    return response


@router.post(GET_MULTIPLE_FLOAT_SERIES_VALUES_ENDPOINT_PATH)
async def get_multiple_float_series_values(request: Request) -> Response:
    """Handle requests for multiple float series values."""
    metrics: RequestMetrics = request.state.metrics

    try:
        # Parse request body
        with Timer() as parsing_timer:
            raw_body = await request.body()
            request_dict = json.loads(raw_body)
            parsed_request = FloatTimeSeriesValuesRequest.from_dict(request_dict)

            # Get the configuration from middleware
            perf_config = getattr(request.state, PERF_REQUEST_CONFIG_ATTRIBUTE_NAME, None)
            if not perf_config:
                logger.error("No performance configuration found")
                raise MalformedRequestError("Missing or invalid X-Perf-Request header")

            # Get endpoint-specific configuration using the config path (with /api prefix)
            endpoint_config = perf_config.get_endpoint_config(GET_MULTIPLE_FLOAT_SERIES_VALUES_CONFIG_PATH, "POST")
            if not endpoint_config or not isinstance(endpoint_config, FloatTimeSeriesValuesConfig):
                logger.warning("Missing configuration for this endpoint")
                raise MalformedRequestError("Missing configuration for this endpoint")

        metrics.parse_time_ms = parsing_timer.time_ms

        # Log request details
        logger.info(
            f"Processing: num_series={len(parsed_request.requests)} "
            f"per_series_limit={parsed_request.per_series_points_limit} "
            f"existence_probability={endpoint_config.existence_probability} "
            f"cardinality_policy={endpoint_config.series_cardinality_policy} "
            f"seed={endpoint_config.seed}"
            f"cardinality_uniform_range={endpoint_config.series_cardinality_uniform_range} "
            f"cardinality_buckets={endpoint_config.series_cardinality_buckets}"
        )

        # Generate and return response
        with Timer() as data_generation_timer:
            # Build the response using the extracted function
            response = _build_float_series_response(parsed_request, endpoint_config)

            # Serialize the response
            response_bytes = response.SerializeToString()

        metrics.generation_time_ms = data_generation_timer.time_ms
        metrics.returned_payload_size_bytes = len(response_bytes)

        logger.info(
            f"Generated response with {len(response.series)} series "
            f"({sum(1 if s.series.values else 0 for s in response.series)} non-empty), "
            f"{sum(len(s.series.values) for s in response.series)} total points "
            f"in {data_generation_timer.time_ms:.2f}ms, "
            f"size={metrics.returned_payload_size_bytes} bytes"
        )

        return Response(content=response_bytes, media_type="application/x-protobuf")

    except MalformedRequestError as exc:
        logger.error(f"Invalid request configuration: {str(exc)}")
        return Response(
            status_code=400,
            content=ProtoFloatSeriesValuesResponseDTO().SerializeToString(),
            media_type="application/x-protobuf",
        )
    except Exception as exc:
        logger.exception(f"Unhandled exception during request processing: {exc}")
        return Response(
            content=ProtoFloatSeriesValuesResponseDTO().SerializeToString(),
            media_type="application/x-protobuf",
            status_code=500,
        )


def _map_unset_to_none(value):
    return None if isinstance(value, Unset) else value


def _hash_to_uniform_64bit(*xs) -> float:
    s = json.dumps(xs, separators=(",", ":"), sort_keys=True).encode()
    # 8-byte deterministic hash -> integer in [0, 2^64-1]
    h = hashlib.blake2b(s, digest_size=8).digest()
    return int.from_bytes(h, "big")


def _hash_to_uniform_0_1(*xs) -> float:
    return _hash_to_uniform_64bit(*xs) / 2**64
