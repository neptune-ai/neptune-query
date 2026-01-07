# `fetch_series()`

Fetches a table of series values per step, for non-numerical series attributes.

To limit the results, define filters for runs to search or attributes to include.

Supported types:

- `FileSeries`
- `HistogramSeries`
- `StringSeries`

> **Related:**
>
> - [Fetch series from experiments][fetch-series]

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `project` | `str` | `None` | Path of the Neptune project, as `WorkspaceName/ProjectName`.<br><br>If not provided, the `NEPTUNE_PROJECT` environment variable is used. |
| `runs` | `str` \| list of `str` \| `Filter` | - | **Required.** Filter specifying which runs to include.<br><br>- If a string is provided, it's treated as a regex pattern that the run IDs must match.<br>- If a list of strings is provided, it's treated as exact run IDs to match.<br>- To provide a more complex condition on an arbitrary attribute value, pass a `Filter` object. |
| `attributes` | `str` \| list of `str` \| `AttributeFilter` | - | **Required.** Filter specifying which attributes to include.<br><br>- If a string is provided, it's treated as a regex pattern that the attribute names must match.<br>- If a list of strings is provided, it's treated as exact attribute names to match.<br>- To provide a more complex condition, pass an `AttributeFilter` object. |
| `include_time` | `"absolute"` | `None` | Whether to include absolute timestamp. If set, each metric column has an additional sub-column with requested timestamp values. |
| `step_range` | `Tuple[float \| None, float \| None]` | `(None, None)` | Tuple specifying the range of steps to include.<br><br>If `None` is used, it represents an open interval. |
| `lineage_to_the_root` | `bool` | `True` | If `True`, includes all points from the complete run history.<br><br>If `False`, only includes points from the selected run. |
| `tail_limit` | `int` | `None` | From the tail end of each series, how many points to include at most. |

## Returns

`pandas.DataFrame` – A table of series values per step for non-numerical series. The DataFrame has a MultiIndex with:

- **Index:** `["run", "step"]`
- **Columns:**
    - `"value"` – File objects, strings, or histograms
    - `"absolute_time"` if the `include_time` argument is set to `"absolute"`

## Raises

- `AttributeTypeInferenceError` – If the attribute type wasn't specified in a filter passed to the `runs` argument, and the attribute has multiple types across the project's runs.
- `ConflictingAttributeTypes` – If there are conflicting attribute types under the same path and the `type_suffix_in_column_names` argument is set to `False`.

## Examples

The following examples show how to fetch series of different types.

### File series

Fetch file series of of a specific run from step 1 to 3 and include the absolute timestamp:

```py
import neptune_query.runs as nq_runs


nq_runs.fetch_series(
    runs=["prompt-wolf-202506051321-2g2r1"],
    attributes=r"^predictions/",
    step_range=(1.0, 3.0),
    include_time="absolute",
)
```

Sample output:

```py
                                                          predictions
                                                        absolute_time                                     value
run                            step
prompt-wolf-202506051321-2g2r1  1.0  2025-08-29 09:18:27.946000+00:00  File(size=24.89 KB, mime_type=image/png)
                                2.0  2025-08-29 09:18:29.949000+00:00  File(size=26.66 KB, mime_type=image/png)
                                3.0  2025-08-29 09:19:01.952000+00:00   File(size=6.89 KB, mime_type=image/png)
```

### Histogram series

Fetch histogram series of a specific run, including only the last 10 steps:

```py
import neptune_query.runs as nq_runs


nq_runs.fetch_series(
    runs=["nano-terminal-20250829091720668-xga60"],
    attributes="activations",
    tail_limit=10,
)
```

Sample output:

```py
                                                                                                                         activations
run                                   step
nano-terminal-20250829091720668-xga60 40.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[1.0, 7.0, 4.0, 8.0])
                                      41.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[3.0, 8.0, 0.0, 9.0])
                                      42.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[5.0, 3.0, 0.0, 2.0])
                                      43.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[7.0, 1.0, 2.0, 4.0])
                                      44.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[1.0, 6.0, 2.0, 9.0])
                                      45.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[7.0, 4.0, 5.0, 1.0])
                                      46.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[4.0, 1.0, 2.0, 0.0])
                                      47.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[8.0, 7.0, 6.0, 0.0])
                                      48.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[0.0, 3.0, 2.0, 5.0])
                                      49.0  Histogram(type='COUNTING', edges=[0.0, 1.0, 2.0, 4.0, 8.0], values=[8.0, 1.0, 7.0, 0.0])
```

### String series

Fetch text logs of a specific run:

```py
import neptune_query.runs as nq_runs


nq_runs.fetch_series(
    runs=["nano-terminal-20250829091720668-xga60"],
    attributes="journal",
)
```

Sample output:

```py
                                                              journal
run                                   step
nano-terminal-20250829091720668-xga60 0.0    training my model, day 0
                                      1.0    training my model, day 1
                                      2.0    training my model, day 2
                                      3.0    training my model, day 3
                                      4.0    training my model, day 4
                                      5.0    training my model, day 5

                                      47.0  training my model, day 47
                                      48.0  training my model, day 48
                                      49.0  training my model, day 49
```

[fetch-series]: ../fetch_series.md
