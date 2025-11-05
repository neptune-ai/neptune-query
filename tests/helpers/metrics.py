from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Mapping,
    Sequence,
    Union,
)

import numpy as np

from neptune_query.internal.identifiers import RunAttributeDefinition
from neptune_query.internal.retrieval.metrics import MetricDatapoints


@dataclass(frozen=True)
class FloatPointValue:
    timestamp_ms: float | None
    step: float | None
    value: float | None
    is_preview: bool | None = None
    completion_ratio: float | None = None

    @classmethod
    def create(
        cls,
        step: float | None,
        value: float | None,
        *,
        timestamp_ms: float | None = None,
        is_preview: bool | None = None,
        completion_ratio: float | None = None,
    ) -> "FloatPointValue":
        return cls(
            timestamp_ms=timestamp_ms,
            step=step,
            value=value,
            is_preview=is_preview,
            completion_ratio=completion_ratio,
        )

    def as_tuple(self) -> tuple[object, ...]:
        return self.timestamp_ms, self.step, self.value, self.is_preview, self.completion_ratio

    def __iter__(self):
        return iter(self.as_tuple())

    def __getitem__(self, index: int) -> object:
        return self.as_tuple()[index]

    def __len__(self) -> int:
        return len(self.as_tuple())

    def has_timestamp(self) -> bool:
        return self.timestamp_ms is not None

    def has_preview_data(self) -> bool:
        return self.is_preview is not None and self.completion_ratio is not None


def to_metric_datapoints(points: Sequence[FloatPointValue]) -> MetricDatapoints:
    size = len(points)
    include_timestamp = any(point.has_timestamp() for point in points)
    include_preview = any(point.has_preview_data() for point in points)

    metric_values = MetricDatapoints.allocate(
        size=size, include_timestamp=include_timestamp, include_preview=include_preview
    )

    for idx, point in enumerate(points):
        metric_values.steps[idx] = float(point.step)
        metric_values.values[idx] = float(point.value)

        if metric_values.timestamps is not None:
            metric_values.timestamps[idx] = float(point.timestamp_ms) if point.timestamp_ms is not None else np.nan

        if metric_values.is_preview is not None:
            metric_values.is_preview[idx] = bool(point.is_preview) if point.is_preview is not None else False

        if metric_values.completion_ratio is not None:
            metric_values.completion_ratio[idx] = (
                float(point.completion_ratio) if point.completion_ratio is not None else 1.0
            )

    return metric_values


def normalize_metrics_data(
    metrics_data: Mapping[
        RunAttributeDefinition,
        Union[MetricDatapoints, Sequence[FloatPointValue]],
    ],
) -> dict[RunAttributeDefinition, MetricDatapoints]:
    return {
        definition: value if isinstance(value, MetricDatapoints) else to_metric_datapoints(value)
        for definition, value in metrics_data.items()
    }


def assert_metric_mappings_equal(
    actual: Mapping[RunAttributeDefinition, MetricDatapoints],
    expected: Mapping[RunAttributeDefinition, MetricDatapoints],
) -> None:
    actual_keys = set(actual.keys())
    expected_keys = set(expected.keys())

    if actual_keys != expected_keys:
        missing = expected_keys - actual_keys
        unexpected = actual_keys - expected_keys
        raise AssertionError(f"Metric definitions mismatch. Missing: {missing}, unexpected: {unexpected}")

    for definition in expected_keys:
        actual_values = actual[definition]
        expected_values = expected[definition]
        if actual_values != expected_values:
            raise AssertionError(
                "Metric values differ for " f"{definition}: actual={actual_values!r}, expected={expected_values!r}"
            )
