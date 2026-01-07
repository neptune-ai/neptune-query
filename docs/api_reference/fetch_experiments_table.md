# `fetch_experiments_table()`

Fetches a table of experiment metadata, with runs as rows and attributes as columns.

To narrow the results, define filters for experiments to search or attributes to include.

> **Related:**
>
> - [Fetch runs table][fetch-runs-table]

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `project` | `str` | `None` | Path of the Neptune project, as `WorkspaceName/ProjectName`.<br><br>If not provided, the `NEPTUNE_PROJECT` environment variable is used. |
| `experiments` | `str` \| list of `str` \| `Filter` | `None` | Filter specifying which experiments to include.<br><br>- If a string is provided, it's treated as a regex pattern that the experiment names must match.<br>- If a list of strings is provided, it's treated as exact experiment names to match.<br>- To provide a more complex condition on an arbitrary attribute value, pass a `Filter` object.<br><br>If no filter is specified, all experiments are returned. |
| `attributes` | `str` \| list of `str` \| `AttributeFilter` | `None` | Filter specifying which attributes to include.<br><br>- If a string is provided, it's treated as a regex pattern that the attribute names must match.<br>- If a list of strings is provided, it's treated as exact attribute names to match.<br>- To provide a more complex condition, pass an `AttributeFilter` object. |
| `sort_by` | `str` \| `Attribute` | `"sys/creation_time"` | Name of the attribute to sort the table by.<br><br>Alternatively, an `Attribute` object that specifies the attribute type. |
| `sort_direction` | `"asc"` \| `"desc"` | `"desc"` | Sorting direction of the column specified by the `sort_by` parameter. |
| `limit` | `int` | `None` | Maximum number of experiments to return. By default all experiments are returned. |
| `type_suffix_in_column_names` | `bool` | `False` | If `True`, columns of the returned DataFrame are suffixed with `:<type>`. For example, `"attribute1:float_series"`, `"attribute1:string"`.<br><br>If set to `False`, the method throws an exception if there are multiple types under one path. |

## Returns

`pandas.DataFrame` – A DataFrame similar to the runs table in the web app.

The DataFrame has:

- a single-level index `"experiment"` with experiment names
- a single-level column index with attribute names

For series attributes, the last logged value is returned.

## Raises

- `AttributeTypeInferenceError` – If the attribute type wasn't specified in a filter passed to the `experiments` argument, and the attribute has multiple types across the project's experiments.
- `ConflictingAttributeTypes` – If there are conflicting attribute types under the same path and the `type_suffix_in_column_names` argument is set to `False`.

## Example

Fetch attributes matching `loss` or `batch_size` from four specific experiments:

```py
import neptune_query as nq


nq.fetch_experiments_table(
    experiments=["seabird-1", "seabird-2", "seabird-3", "seabird-4"],
    attributes=r"loss | batch_size",
    type_suffix_in_column_names=True,
)
```

Sample output:

```py
            config/batch_size:float  config/batch_size:int  loss:float_series
experiment
seabird-4                      64.0                    NaN           0.181736
seabird-3                       NaN                   64.0           0.123372
seabird-2                       NaN                   32.0           0.224408
seabird-1                       NaN                   32.0           0.205908
```

[fetch-runs-table]: runs/fetch_runs_table.md

