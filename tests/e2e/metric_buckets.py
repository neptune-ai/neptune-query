import numpy as np

from neptune_query.internal.retrieval.metric_buckets import TimeseriesBucket


def calculate_global_range(
    series: list[tuple[float, float]], x_range: tuple[float, float] | None
) -> tuple[float, float]:
    if x_range is not None:
        range_from, range_to = x_range
    else:
        xs = [x for x, y in series]
        range_from, range_to = min(xs), max(xs)
    return range_from, range_to


def calculate_metric_bucket_ranges(range_from: float, range_to: float, limit: int) -> list[tuple[float, float]]:
    if range_from == range_to:
        return [(range_from, float("inf"))]  # TODO: seems to be a bug on the backend side...

    bucket_ranges = []
    bucket_width = (range_to - range_from) / (limit - 1)
    for bucket_i in range(limit + 1):
        if bucket_i == 0:
            from_x = float("-inf")
        else:
            from_x = range_from + bucket_width * (bucket_i - 1)

        if bucket_i == limit:
            to_x = float("inf")
        else:
            to_x = range_from + bucket_width * bucket_i
        bucket_ranges.append((from_x, to_x))
    return bucket_ranges


def aggregate_metric_buckets(
    series: list[tuple[float, float]], bucket_ranges: list[tuple[float, float]]
) -> list[TimeseriesBucket]:
    buckets = []
    for bucket_i, bucket_x_range in enumerate(bucket_ranges):
        from_x, to_x = bucket_x_range

        count = 0
        positive_inf_count = 0
        negative_inf_count = 0
        nan_count = 0
        xs = []
        ys = []
        for x, y in series:
            if from_x < x <= to_x or (bucket_i == 0 and x == from_x):
                # 2nd case fires for the special case of a [a, +inf) range that may be a bug, TODO remove
                count += 1
                if np.isposinf(y):
                    positive_inf_count += 1
                elif np.isneginf(y):
                    negative_inf_count += 1
                elif np.isnan(y):
                    nan_count += 1
                else:
                    xs.append(x)
                    ys.append(y)
        if count == 0:
            continue

        bucket = TimeseriesBucket(
            index=bucket_i,
            from_x=from_x,
            to_x=to_x,
            first_x=xs[0] if xs else float("nan"),
            first_y=ys[0] if ys else float("nan"),
            last_x=xs[-1] if xs else float("nan"),
            last_y=ys[-1] if ys else float("nan"),
            # y_min=float(np.min(ys)) if ys else float("nan"),
            # y_max=float(np.max(ys)) if ys else float("nan"),
            # finite_point_count=len(ys),
            # nan_count=nan_count,
            # positive_inf_count=positive_inf_count,
            # negative_inf_count=negative_inf_count,
            # finite_points_sum=float(np.sum(ys)) if ys else 0.0,
        )
        buckets.append(bucket)
    return buckets
