# `fetch_series()`

Fetches a table of series values per step, for non-numerical series attributes.

To narrow the results, define filters for experiments to search or attributes to include.

Supported types:

- `FileSeries`
- `HistogramSeries`
- `StringSeries`

> **Related:**
>
> - [Fetch series from runs][fetch-series-runs]

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `project` | `str` | `None` | Path of the Neptune project, as `WorkspaceName/ProjectName`.<br><br>If not provided, the `NEPTUNE_PROJECT` environment variable is used. |
| `experiments` | `str` \| list of `str` \| `Filter` | - | **Required.** Filter specifying which experiments to include.<br><br>- If a string is provided, it's treated as a regex pattern that the experiment names must match.<br>- If a list of strings is provided, it's treated as exact experiment names to match.<br>- To provide a more complex condition on an arbitrary attribute value, pass a `Filter` object. |
| `attributes` | `str` \| list of `str` \| `AttributeFilter` | - | **Required.** Filter specifying which attributes to include.<br><br>- If a string is provided, it's treated as a regex pattern that the attribute names must match.<br>- If a list of strings is provided, it's treated as exact attribute names to match.<br>- To provide a more complex condition, pass an `AttributeFilter` object. |
| `include_time` | `"absolute"` | `None` | Whether to include absolute timestamp. If set, each metric column has an additional sub-column with requested timestamp values. |
| `step_range` | `Tuple[float \| None, float \| None]` | `(None, None)` | Tuple specifying the range of steps to include.<br><br>If `None` is used, it represents an open interval. |
| `lineage_to_the_root` | `bool` | `True` | If `True`, includes all points from the complete experiment history.<br><br>If `False`, only includes points from the selected experiment. |
| `tail_limit` | `int` | `None` | From the tail end of each series, how many points to include at most. |

## Returns

`pandas.DataFrame` – A table of series values per step for non-numerical series. The DataFrame has a MultiIndex with:

- **Index:** `["experiment", "step"]` for experiments or `["run", "step"]` for runs
- **Columns:**
    - `"value"` – File objects, strings, or histograms
    - `"absolute_time"` if the `include_time` argument is set to `"absolute"`

## Raises

- `AttributeTypeInferenceError` – If the attribute type wasn't specified in a filter passed to the `experiments` argument, and the attribute has multiple types across the project's experiments.
- `ConflictingAttributeTypes` – If there are conflicting attribute types under the same path and the `type_suffix_in_column_names` argument is set to `False`.

## Examples

The following examples show how to fetch series of different types.

### File series

Fetch file series of two specific experiments from step 1 to 3 and include the absolute timestamp:

```py
import neptune_query as nq


nq.fetch_series(
    experiments=["seabird-4", "seabird-5"],
    attributes=r"^predictions/",
    step_range=(1.0, 3.0),
    include_time="absolute",
)
```

Sample output:

```py
                                     predictions
                                   absolute_time                                     value
experiment step
seabird-4  1.0  2025-08-29 09:18:27.946000+00:00  File(size=24.89 KB, mime_type=image/png)
           2.0  2025-08-29 09:18:29.949000+00:00  File(size=26.66 KB, mime_type=image/png)
           3.0  2025-08-29 09:19:01.952000+00:00   File(size=6.89 KB, mime_type=image/png)
seabird-5  1.0  2025-08-29 09:21:07.946000+00:00  File(size=23.66 KB, mime_type=image/png)
           2.0  2025-08-29 09:22:25.949000+00:00  File(size=25.43 KB, mime_type=image/png)
           3.0  2025-08-29 09:24:27.952000+00:00   File(size=6.58 KB, mime_type=image/png)
```

### Histogram series

Fetch histogram series of a specific experiment, including only the last 10 steps:

```py
import neptune_query as nq

nq.fetch_series(
    experiments=["seabird-4"],
    attributes=["activations"],
    tail_limit=10,
)
```

Sample output:

```py
                                                                                              activations
experiment step
seabird-4  40.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[1.0, 9.0, 7.0, 3.0])
           41.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[0.0, 4.0, 5.0, 6.0])
           42.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[6.0, 7.0, 9.0, 2.0])
           43.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[2.0, 1.0, 5.0, 8.0])
           44.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[8.0, 7.0, 6.0, 1.0])
           45.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[7.0, 4.0, 5.0, 9.0])
           46.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[7.0, 6.0, 2.0, 9.0])
           47.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[4.0, 3.0, 6.0, 7.0])
           48.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[9.0, 7.0, 3.0, 0.0])
           49.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[0.0, 3.0, 4.0, 7.0])
```

### String series

Fetch text logs of a specific experiment:

```py
import neptune_query as nq


nq.fetch_series(
    experiments=["seabird-4"],
    attributes=["journal"],
)
```

Sample output:

```py
                                   journal
experiment step
seabird-4  0.0    training my model, day 0
           1.0    training my model, day 1
           2.0    training my model, day 2
           3.0    training my model, day 3
           4.0    training my model, day 4
           5.0    training my model, day 5
...
           47.0  training my model, day 47
           48.0  training my model, day 48
           49.0  training my model, day 49
```

[fetch-series-runs]: runs/fetch_series.md
