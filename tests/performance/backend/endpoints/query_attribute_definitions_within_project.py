"""
Endpoint for handling attribute definitions queries within a project.
"""
import json
import random
import time
from typing import (
    Final,
    Optional,
    Union,
)

from fastapi import (
    APIRouter,
    Request,
    Response,
)
from neptune_api.models import AttributeTypeDTO
from neptune_api.models.attribute_definition_dto import AttributeDefinitionDTO
from neptune_api.models.next_page_dto import NextPageDTO
from neptune_api.models.query_attribute_definitions_body_dto import QueryAttributeDefinitionsBodyDTO
from neptune_api.models.query_attribute_definitions_result_dto import QueryAttributeDefinitionsResultDTO
from neptune_api.types import Unset

from neptune_query.internal.retrieval.attribute_types import map_attribute_type_python_to_backend
from tests.performance.backend.middleware.read_perf_config_middleware import PERF_REQUEST_CONFIG_ATTRIBUTE_NAME
from tests.performance.backend.perf_request import QueryAttributeDefinitionsConfig
from tests.performance.backend.utils.exceptions import MalformedRequestError
from tests.performance.backend.utils.hashing_utils import hash_to_string
from tests.performance.backend.utils.logging import (
    map_unset_to_none,
    repr_list,
    setup_logger,
)
from tests.performance.backend.utils.metrics import RequestMetrics
from tests.performance.backend.utils.timing import Timer

# Path without /api prefix since we're mounting under /api in the main app
QUERY_ATTRIBUTE_DEFINITIONS_ENDPOINT_PATH: Final[str] = "/leaderboard/v1/leaderboard/attributes/definitions/query"
# Path used for configuration matching (with /api prefix for backward compatibility)
QUERY_ATTRIBUTE_DEFINITIONS_CONFIG_PATH: Final[str] = "/api" + QUERY_ATTRIBUTE_DEFINITIONS_ENDPOINT_PATH

logger = setup_logger("query_attribute_definitions")

router = APIRouter()


def _build_result(
    endpoint_config: QueryAttributeDefinitionsConfig, experiment_ids: list[str], pagination_token: Optional[NextPageDTO]
) -> QueryAttributeDefinitionsResultDTO:
    """Build a response according to the endpoint configuration."""
    logger.debug(f"Building result with {endpoint_config.total_definitions_count} definitions")

    # we're encoding an "offset" within the next_page_token to facilitate pagination
    limit = pagination_token.limit
    returned_in_previous_pages = (
        json.loads(pagination_token.next_page_token)["total_already_returned"]
        if pagination_token and pagination_token.next_page_token
        else 0
    )
    total_remaining_to_return = endpoint_config.total_definitions_count - returned_in_previous_pages
    to_be_returned_in_this_page = min(limit, total_remaining_to_return)

    return QueryAttributeDefinitionsResultDTO(
        entries=[
            AttributeDefinitionDTO(
                name=hash_to_string(
                    endpoint_config.seed,
                    returned_in_previous_pages + i,
                    length=10,
                ),
                type=AttributeTypeDTO(
                    map_attribute_type_python_to_backend(random.choice(endpoint_config.attribute_types))
                ),
            )
            for i in range(to_be_returned_in_this_page)
        ],
        next_page=NextPageDTO(
            limit=0,
            next_page_token=json.dumps(
                {"total_already_returned": returned_in_previous_pages + to_be_returned_in_this_page}
            ),
        ),
    )


@router.post(QUERY_ATTRIBUTE_DEFINITIONS_ENDPOINT_PATH)
async def query_attribute_definitions_within_project(request: Request) -> Response:
    """Handle requests for attribute definitions within a project."""
    metrics: RequestMetrics = request.state.metrics

    try:
        # Parse request body
        with Timer() as parsing_timer:
            raw_body = await request.json()
            parsed_request = QueryAttributeDefinitionsBodyDTO.from_dict(raw_body)

            # Get the configuration from middleware
            perf_config = getattr(request.state, PERF_REQUEST_CONFIG_ATTRIBUTE_NAME, None)
            if not perf_config:
                logger.error("No performance configuration found")
                raise MalformedRequestError("Missing or invalid X-Perf-Request header")

            # Get endpoint-specific configuration using the config path (with /api prefix)
            endpoint_config = perf_config.get_endpoint_config(QUERY_ATTRIBUTE_DEFINITIONS_CONFIG_PATH, "POST")
            if not endpoint_config or not isinstance(endpoint_config, QueryAttributeDefinitionsConfig):
                logger.warning("Missing configuration for this endpoint")
                raise MalformedRequestError("Missing configuration for this endpoint")

        metrics.parse_time_ms = parsing_timer.time_ms

        # Log request details
        project_ids = (
            parsed_request.project_identifiers if not isinstance(parsed_request.project_identifiers, type(None)) else []
        )
        logger.info(
            f"Processing: projects={project_ids} "
            f"seed={endpoint_config.seed} "
            f"total_definitions={endpoint_config.total_definitions_count} "
            f"experiments={map_unset_to_none(repr_list(parsed_request.experiment_ids_filter))} "
            f"attribute_types={repr_list(endpoint_config.attribute_types)} "
            f"next_page={_next_page_dto_to_str(parsed_request.next_page)}"
        )

        # Generate and return response
        with Timer() as data_generation_timer:
            result = _build_result(
                endpoint_config,
                experiment_ids=parsed_request.experiment_ids_filter,
                pagination_token=parsed_request.next_page,
            )
            payload = json.dumps(result.to_dict())

        metrics.generation_time_ms = data_generation_timer.time_ms
        metrics.returned_payload_size_bytes = len(payload)

        return Response(
            content=payload,
            media_type="application/json",
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


def _next_page_dto_to_str(next_page: Union[Unset, "NextPageDTO"]) -> str:
    if isinstance(next_page, Unset) or next_page is None:
        return "None"

    token = next_page.next_page_token
    return f"NextPageDTO(limit={next_page.limit}, token={token if not isinstance(token, Unset) else 'None'})"
