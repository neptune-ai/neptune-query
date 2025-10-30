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
from neptune_query.internal.retrieval.metric_buckets import TimeseriesBucket

# Set the random seed for reproducibility
random_gen = random.Random(20250925)


def random_alnum(length: int) -> str:
    return "".join(random_gen.choices(string.ascii_lowercase + string.digits, k=length))


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


def bucket_metrics(experiments: int, paths: int, buckets: int) -> dict[RunAttributeDefinition, list[TimeseriesBucket]]:
    return {
        run_attribute_definition(experiment, path): [bucket_metric(index=i) for i in range(buckets)]
        for experiment in range(experiments)
        for path in range(paths)
    }


def run_attribute_definition(
    sys_id: int | str, path: int | str, attribute_type: str = "float_series"
) -> RunAttributeDefinition:
    return RunAttributeDefinition(
        RunIdentifier(ProjectIdentifier("foo/bar"), SysId(f"sysid{sys_id}")),
        AttributeDefinition(f"path{path}", attribute_type),
    )


def bucket_metric(index: int) -> TimeseriesBucket:
    if index > 0:
        return TimeseriesBucket(
            index=index,
            from_x=20.0 * index,
            to_x=20.0 * (index + 1),
            first_x=20.0 * index + 2,
            first_y=100.0 * (index - 1) + 90.0,
            last_x=20.0 * (index + 1) - 2,
            last_y=100.0 * index,
            # y_min=80.0 * index,
            # y_max=110.0 * index,
            # finite_point_count=10 + index,
            # nan_count=5 - index,
            # positive_inf_count=2 * index,
            # negative_inf_count=index,
            # finite_points_sum=950.0 * index,
        )
    else:
        return TimeseriesBucket(
            index=index,
            from_x=float("-inf"),
            to_x=20.0,
            first_x=20.0,
            first_y=0.0,
            last_x=20.0,
            last_y=0.0,
            # y_min=0.0,
            # y_max=0.0,
            # finite_point_count=1,
            # nan_count=0,
            # positive_inf_count=0,
            # negative_inf_count=0,
            # finite_points_sum=0.0,
        )
