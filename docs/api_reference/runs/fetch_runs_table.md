# `fetch_runs_table()`

Fetches a table of run metadata, with runs as rows and attributes as columns.

To limit the results, define filters for runs to search or attributes to include.

> **Related:**
>
> - [Fetch experiments table][fetch-experiments-table]

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `project` | `str` | `None` | Path of the Neptune project, as `WorkspaceName/ProjectName`.<br><br>If not provided, the `NEPTUNE_PROJECT` environment variable is used. |
| `runs` | `str` \| list of `str` \| `Filter` | `None` | Filter specifying which runs to include.<br><br>- If a string is provided, it's treated as a regex pattern that the run IDs must match.<br>- If a list of strings is provided, it's treated as exact run IDs to match.<br>- To provide a more complex condition on an arbitrary attribute value, pass a `Filter` object.<br><br>If no filter is specified, all runs are returned. |
| `attributes` | `str` \| list of `str` \| `AttributeFilter` | `None` | Filter specifying which attributes to include.<br><br>- If a string is provided, it's treated as a regex pattern that the attribute names must match.<br>- If a list of strings is provided, it's treated as exact attribute names to match.<br>- To provide a more complex condition, pass an `AttributeFilter` object. |
| `sort_by` | `str` \| `Attribute` | `"sys/creation_time"` | An attribute name or an `Attribute` object specifying type and, optionally, aggregation. |
| `sort_direction` | `"asc"` \| `"desc"` | `"desc"` | Sorting direction of the column specified by the `sort_by` parameter. |
| `limit` | `int` | `None` | Maximum number of runs to return. By default all runs are returned. |
| `type_suffix_in_column_names` | `bool` | `False` | If `True`, columns of the returned DataFrame are suffixed with `:<type>`. For example, `"attribute1:float_series"`, `"attribute1:string"`.<br><br>If set to `False`, the method throws an exception if there are multiple types under one path. |

## Returns

`pandas.DataFrame` – A DataFrame similar to the runs table in the web app.

The DataFrame has:

- a single-level index `"run"` with run IDs
- a single-level column index with attribute names

For series attributes, the last logged value is returned.

## Raises

- `AttributeTypeInferenceError` – If the attribute type wasn't specified in a filter passed to the `runs` argument, and the attribute has multiple types across the project's runs.
- `ConflictingAttributeTypes` – If there are conflicting attribute types under the same path and the `type_suffix_in_column_names` argument is set to `False`.

## Examples

Fetch specific runs, with attributes matching `loss`, `batch_size` or `learning_rate` as columns:

```py
import neptune_query.runs as nq_runs


nq_runs.fetch_runs_table(
    runs=[
        "geometric-composite-20250902060724545-i2ufv",
        "glowing-database-20250902060704895-9ps29",
        "centered-decoder-20250829091346879-mo8l9",
    ],
    attributes=r"loss | batch_size | learning_rate",
)
```

Sample output:

```sh
                                             config/batch_size  config/learning_rate      loss
run
geometric-composite-20250902060724545-i2ufv               32.0                 0.001  0.025725
glowing-database-20250902060704895-9ps29                  32.0                 0.001  0.030208
centered-decoder-20250829091346879-mo8l9                  64.0                 0.002  0.123372
```

Fetch metadata from the constituent runs of an experiment:

```py
import neptune_query.runs as nq_runs
from neptune_query.filters import Filter


nq_runs.fetch_runs_table(
    runs=Filter.eq("sys/experiment/name", "seabird-4"),
    attributes=r"loss | configs",
)
```

[fetch-experiments-table]: ../fetch_experiments_table.md
