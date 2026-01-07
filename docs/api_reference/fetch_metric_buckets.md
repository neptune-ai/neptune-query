# `fetch_metric_buckets()`

> **⚠️ Experimental:** This feature is experimental and may change in future versions.

Fetches a table of metric values split by X-axis buckets.

One point is returned from each bucket. To control the number of buckets, use the `limit` parameter.

## Fetch experiment metrics

You can filter the results by:

- Experiments: Specify which experiments to search.
- Attributes: Only list metrics that match certain criteria.

### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `project` | `str` | `None` | Path of the Neptune project, as `workspace-name/project-name`.<br><br>If not provided, the `NEPTUNE_PROJECT` environment variable is used. |
| `experiments` | `str` \| list of `str` \| `Filter` | - | **Required.** Filter specifying which experiments to include.<br><br>- If a string is provided, it's treated as a regex pattern that the experiment names must match.<br>- If a list of strings is provided, it's treated as exact experiment names to match.<br>- To provide a more complex condition on an arbitrary attribute value, pass a `Filter` object.<br><br>If no filter is specified, all experiments are returned. |
| `x` | `Literal["step"]` | `"step"` | The X-axis series used for the bucketing. Only "step" is currently supported. |
| `y` | `str` \| list of `str` \| `AttributeFilter` | - | **Required.** Filter specifying which metrics to include.<br><br>- If a string is provided, it's treated as a regex pattern that the metric names must match.<br>- If a list of strings is provided, it's treated as exact metric names to match.<br>- To provide a more complex condition, pass an `AttributeFilter` object. |
| `limit` | `int` | `1000` | Number of buckets to use. The default and maximum value is 1000. |
| `lineage_to_the_root` | `bool` | `True` | If `True`, includes all values from the complete experiment history.<br><br>If `False`, only includes values from the most recent experiment in the lineage. |
| `include_point_previews` | `bool` | `False` | If set to `True`, metric previews are included in the returned DataFrame. The DataFrame will have additional sub-columns with preview information: `is_preview` and `preview_completion`. |

### Returns

The returned DataFrame is a multi-index table with the following levels:

- **experiment**: The experiment name.
- **metric**: The metric name, such as `train/loss`.
- **bucket**: The _x_- and _y_-values of the point returned from the bucket.

Both the first and last points of each metric are always included:

- For every first bucket of a given series, the first point is returned.
- For the remaining buckets, the last point is returned.

#### Point previews

If `include_point_previews=True`, the following additional sub-columns are included for each bucket:
- `is_preview`: Whether the value is a preview.
- `preview_completion`: The completion rate.

### Example

From two specific experiments, fetch training metrics split into 5 buckets:

```py
import neptune_query as nq


nq.fetch_metric_buckets(
    experiments=["seagull-week1", "seagull-week2"],
    x="step",
    y=r"^train/",
    limit=5,  # Only 5 buckets for broad trends
)
```

Output:

```sh
experiment    seagull-week1                                        seagull-week2
metric           train/loss              train/accuracy               train/loss             train/accuracy
bucket                    x           y               x          y             x          y               x          y
(0.0, 200.0]       0.766337   46.899769        0.629231  29.418603      0.793347   3.618248        0.445641  16.923348
(200.0, 400.0]   200.435899   42.001229      200.825488  11.989595    200.151307  21.244816      200.720397  20.515981
(400.0, 600.0]   400.798869   10.429626      400.640794  10.276835    400.338434  33.692977      400.381568  15.954130
(600.0, 800.0]   600.856616   20.633254      600.033832   0.927636    600.002655  37.048722      600.713322  49.537098
(800.0, 1000.0]  800.522183    6.084259      800.019450  39.666397    800.003379  22.569435      800.745987  42.658697
```


