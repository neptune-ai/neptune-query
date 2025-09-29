import random
import string

import neptune_query.internal.retrieval.metrics as metrics
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.retrieval.attribute_types import FloatSeriesAggregations
from neptune_query.internal.retrieval.attribute_values import AttributeValue

# Set the random seed for reproducibility
random.seed(20250925)


def random_alnum(length: int) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def random_alnum_strings(count: int, length: int) -> list[str]:
    return [random_alnum(length) for _ in range(count)]


def float_point_value(i: int, exp: int) -> metrics.FloatPointValue:
    return (1234567890 + i * 1000.0, float(i) + exp, float(i) * 10, False, 1.0)


EXPERIMENT_IDENTIFIER = RunIdentifier(ProjectIdentifier("project/abc"), SysId("XXX-1"))


def float_series_value(path: str, exp: int):
    """Helper to create a float series value for testing."""
    return AttributeValue(
        attribute_definition=AttributeDefinition(path, "float_series"),
        value=FloatSeriesAggregations(last=float(exp), min=0.0, max=float(exp), average=float(exp) / 2, variance=0.0),
        run_identifier=EXPERIMENT_IDENTIFIER,
    )


def string_value(path: str, exp: int):
    """Helper to create a string value for testing."""
    return AttributeValue(
        attribute_definition=AttributeDefinition(path, "string"),
        value=f"value_{exp}",
        run_identifier=EXPERIMENT_IDENTIFIER,
    )


def run_attribute_definition(
    sys_id: int | str, path: int | str, attribute_type: str = "float_series"
) -> RunAttributeDefinition:
    return RunAttributeDefinition(
        RunIdentifier(ProjectIdentifier("foo/bar"), SysId(f"sysid{sys_id}")),
        AttributeDefinition(f"path{path}", attribute_type),
    )
