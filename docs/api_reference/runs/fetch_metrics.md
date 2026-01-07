# `fetch_metrics()`

Fetches a table of metric values per step.

The values are raw, without any aggregation, approximation, or interpolation.

To limit the results, set the step range or the number of values to include from the tail end.

You can also filter the results by:

- Runs: Specify which runs to search.
- Attributes: Only list attributes that match certain criteria.

> **Related:**
>
> - [Fetch metrics from experiments][fetch-metrics]

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
| `type_suffix_in_column_names` | `bool` | `False` | If `True`, columns of the returned DataFrame will be suffixed with `":<type>"`. For example: `"attribute1:float_series"`, `"attribute1:string"`.<br><br>If set to `False`, the method throws an exception if there are multiple types under one path. |
| `include_point_previews` | `bool` | `False` | If set to `True`, metric previews are included in the returned DataFrame. The DataFrame will have additional sub-columns with preview information: `is_preview` and `preview_completion`. |

## Returns

`pandas.DataFrame` – A table of metric values per step. The DataFrame has a MultiIndex with:

- **Index:** `["run", "step"]`
- **Columns:** attribute names with the following sub-columns:
    - `"value"` – metric values
    - `"absolute_time"` if the `include_time` argument is set to `"absolute"`
    - `"is_preview"` if the `include_point_previews` argument is set to `True`
    - `"preview_completion"` if the `include_point_previews` argument is set to `True`

## Raises

- `AttributeTypeInferenceError` – If the attribute type wasn't specified in a filter passed to the `runs` argument, and the attribute has multiple types across the project's runs.
- `ConflictingAttributeTypes` – If there are conflicting attribute types under the same path and the `type_suffix_in_column_names` argument is set to `False`.

## Examples

Fetch the last 12 values of a specific run's losses and accuracies, including incomplete points:

```py
import neptune_query.runs as nq_runs


nq_runs.fetch_metrics(
    runs=["prompt-wolf-2g2r1"],  # custom run ID
    attributes=r"loss | acc",
    tail_limit=12,
    include_point_previews=True,
)
```

Sample output:

```py
                        accuracy                                loss
                        is_preview preview_completion     value is_preview preview_completion     value
run                step
prompt-wolf-2g2r1  37.0      False           1.000000  0.822683      False           1.000000  0.192384
                   38.0      False           1.000000  0.821014      False           1.000000  0.200655
                   39.0      False           1.000000  0.800180      False           1.000000  0.181736
                   41.0       True           0.756098  0.813834       True           0.756098  0.178080
                   42.0       True           0.761905  0.802206       True           0.761905  0.178129
                   43.0       True           0.767442  0.822235       True           0.767442  0.192478
                   44.0       True           0.772727  0.809570       True           0.772727  0.192476
                   45.0       True           0.777778  0.817433       True           0.777778  0.191994
                   46.0       True           0.782609  0.802809       True           0.782609  0.186171
                   47.0       True           0.787234  0.803025       True           0.787234  0.196133
                   48.0       True           0.791667  0.820802       True           0.791667  0.188257
                   49.0       True           0.795918  0.821936       True           0.795918  0.182162
```

Fetch the last 10 values of a specific run's losses, including all extra information:

```py
import neptune_query.runs as nq_runs


nq_runs.fetch_metrics(
    runs=["prompt-wolf-2g2r1"],  # custom run ID
    attributes=r"loss",
    tail_limit=10,
    include_time="absolute",
    type_suffix_in_column_names=True,
    include_point_previews=True,
)
```

Sample output:

```py
                                           loss:float_series
                                           absolute_time is_preview preview_completion     value
run                step
prompt-wolf-2g2r1  34.0 2025-08-29 09:18:27.875000+00:00      False           1.000000  0.184275
                   35.0 2025-08-29 09:18:27.884000+00:00      False           1.000000  0.191002
                   36.0 2025-08-29 09:18:27.888000+00:00      False           1.000000  0.197030
                   37.0 2025-08-29 09:18:27.891000+00:00      False           1.000000  0.192384
                   38.0 2025-08-29 09:18:27.895000+00:00      False           1.000000  0.200655
                   39.0 2025-08-29 09:18:27.899000+00:00      False           1.000000  0.181736
                   41.0 2025-08-29 09:18:27.902000+00:00       True           0.756098  0.178080
                   42.0 2025-08-29 09:18:27.902000+00:00       True           0.761905  0.178129
                   43.0 2025-08-29 09:18:27.916000+00:00       True           0.767442  0.192478
                   44.0 2025-08-29 09:18:27.920000+00:00       True           0.772727  0.192476
```

[fetch-metrics]: ../fetch_metrics.md

