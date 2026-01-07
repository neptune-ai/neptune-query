# Query metric values

You have several options for querying `float_series` values logged with `log_metrics()`:

- To query logged metric values per step, use the `fetch_metrics()` function.
- To get summary values split by _X_-axis ranges, use the `fetch_metric_buckets()` function.

> [!NOTE]
>
> The examples on this page show how to query experiments.
>
> To query runs by ID instead of experiments by name, use the options as described in this guide together with `neptune_query.runs.fetch_metrics()` (the corresponding function from the `runs` module).

## Fetch metric values per step

To fetch logged metric values per step, use `fetch_metrics()`:

```py
import neptune_query as nq


metrics = nq.fetch_metrics(
    experiments=["kittiwake_week-1"],
    attributes=["loss", "accuracy"],
)
```

The function returns a data frame with specified attributes as columns and steps as rows:

```sh
                         loss  accuracy
experiment        step
kittiwake_week-1   1.0   0.31      0.95
                   2.0   0.30      0.96
                   3.0   0.27      0.97
                   4.0   0.28      0.98
                   5.0   0.26      0.99
```

### Limit step range

To fetch values from a limited step range, pass a 2-tuple to the `step_range` argument:

```py
import neptune_query as nq


metrics = nq.fetch_metrics(
    experiments=["kittiwake_week-1"],
    attributes=["loss", "accuracy"],
    step_range=(1.0, 3.0),
)
```

Sample output:

```sh
                         loss  accuracy
experiment        step
kittiwake_week-1   1.0   0.31      0.95
                   2.0   0.30      0.96
                   3.0   0.27      0.97
```

### Limit number of points

To limit how many values to include at most, from the tail end of the series, pass an integer to the `tail_limit` argument:

```py
import neptune_query as nq


metrics = nq.fetch_metrics(
    experiments=["kittiwake_week-1"],
    attributes=["loss", "accuracy"],
    tail_limit=3,
)
```

Sample output:

```sh
                         loss  accuracy
experiment        step
kittiwake_week-1   3.0   0.27      0.97
                   4.0   0.28      0.98
                   5.0   0.26      0.99
```

### Include timestamps

To include timestamps, set the `include_time` argument to `"absolute"`:

```py
import neptune_query as nq


metrics = nq.fetch_metrics(
    experiments=["kittiwake_week-1"],
    attributes=["loss"],
    include_time="absolute",
)
```

Sample output:

```sh
                                                    loss
                                           absolute_time  value
experiment        step
kittiwake_week-1   1.0  2025-08-29 09:18:27.875000+00:00  0.31
                   2.0  2025-08-29 09:18:27.884000+00:00  0.30
                   3.0  2025-08-29 09:18:27.893000+00:00  0.27
                   4.0  2025-08-29 09:18:27.902000+00:00  0.28
                   5.0  2025-08-29 09:18:27.911000+00:00  0.26
```

### Include point previews

To include point previews, set the `include_point_previews` argument to `True`:

```py
import neptune_query as nq


metrics = nq.fetch_metrics(
    experiments=["kittiwake_week-1"],
    attributes=["loss"],
    include_point_previews=True,
)
```

Sample output:

```sh
                               loss
                         is_preview preview_completion   value
experiment        step
kittiwake_week-1   1.0        False           1.000000    0.31
                   2.0        False           1.000000    0.96
                   3.0        False           1.000000    0.27
                   4.0        False           1.000000    0.28
                   5.0        False           1.000000    0.26
                   6.0         True           0.326681    0.23
```

### Exclude inherited metrics

For forked experiments, by default, the function returns the full history of metrics inherited from ancestor experiments. To only include metrics from the specified experiments, set the `lineage_to_the_root` argument to `False`.

For example, if the experiment `kittiwake_week-2` is forked from `kittiwake_week-1` at step `5.0`:

- Lineage to the root disabled:

    ```py
    import neptune_query as nq


    metrics = nq.fetch_metrics(
        experiments=["kittiwake_week-2"],
        attributes=["loss"],
        lineage_to_the_root=False,
    )
    ```

    Sample output:

    ```sh
                            loss
    experiment        step
    kittiwake_week-2   6.0   0.23
                       7.0   0.20
                       8.0   0.17
                       9.0   0.16
    ```

- Lineage to the root enabled (default):

    ```py
    import neptune_query as nq


    metrics = nq.fetch_metrics(
        experiments=["kittiwake_week-2"],
        attributes=["loss"],
        lineage_to_the_root=True,
    )
    ```

    Sample output:

    ```sh
                            loss
    experiment        step
    kittiwake_week-2   1.0   0.31
                       2.0   0.30
                       3.0   0.27
                       4.0   0.28
                       5.0   0.26
                       6.0   0.23
                       7.0   0.20
                       8.0   0.17
                       9.0   0.16
    ```

### Example: Get last step of each metric

To fetch the last step of the specified metrics, set the `tail_limit` argument to `1`:

```py
import neptune_query as nq


metrics_df = nq.fetch_metrics(
    experiments=["kittiwake_week-1"],
    attributes=r"^metrics",
    tail_limit=1,
)
```

In the returned `metrics_df` data frame, the number of rows is equal to the number of distinct last step values.

- If all metrics are logged at every step, there is only one row. In this case, you can use the following code to obtain all of the last steps:

    ```py
    print(metrics_df.head())
    ```

- To get the last step logged for each metric, even when the last steps are different across the series:

    ```py
    last_step = metrics_df.index.get_level_values("step").max()
    print(last_step)
    ```

> [!TIP]
>
> If the step isn't important, you can also query the last value of each metric using the `fetch_experiments_table()` or `fetch_runs_table()` functions.
>
> The returned data frame mimics the runs table of the web app.

## Fetch metric values split by _X_-axis buckets

Instead of fetching values for each step, you can use `fetch_metric_buckets()` to fetch summary values representing a broad range of steps. The total range is split into buckets, and one point is returned from each bucket.

Both the first and last points of each metric are always included:

- For every first bucket of a given series, the first point is returned.
- For the remaining buckets, the last point is returned.

Specify the _X_-axis series with the `x` argument and the metrics with the `y` argument:

```py
import neptune_query as nq


nq.fetch_metric_buckets(
    experiments=["seagull-week1"],
    x="step",
    y=r"^train/",
    limit=5,  # Only 5 buckets for broad trends
)
```

Sample output:

```sh
experiment    seagull-week1
metric           train/loss              train/accuracy
bucket                    x           y               x          y
(0.0, 200.0]       0.766337   46.899769        0.629231  29.418603
(200.0, 400.0]   200.435899   42.001229      200.825488  11.989595
(400.0, 600.0]   400.798869   10.429626      400.640794  10.276835
(600.0, 800.0]   600.856616   20.633254      600.033832   0.927636
(800.0, 1000.0]  800.522183    6.084259      800.019450  39.666397
```

Use the following additional parameters to control the output:

- To set the number of buckets, pass an integer to the `limit` argument.
- To include point previews, set the `include_point_previews` argument to `True`.
- For forked experiments, to exclude inherited metrics, set the `lineage_to_the_root` argument to `False`.

## Query NaN and infinity values

You can query metrics that contain non-finite values.

However, filtering by such values isn't supported. For example, the following code would return a `NeptuneUnexpectedResponseError`:

```py
import neptune_query as nq


nq.fetch_metrics(
    experiments=nq.Filter.gt("metrics/m_0", float("-inf")),
    attributes=r"^metrics",
)
```
