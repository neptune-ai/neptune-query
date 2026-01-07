# `fetch_experiments_table_global()`

> **⚠️ Experimental:** This feature is experimental and may change in future versions.

Fetches a table of experiment metadata, with runs as rows and attributes as columns. The query targets projects globally, across all workspaces that the user can access with the provided API token.

To limit the results, define filters for experiments to search or attributes to include.

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `experiments` | `str` \| list of `str` \| `Filter` | `None` | Filter specifying which experiments to include.<br><br>- If a string is provided, it's treated as a regex pattern that the names must match.<br>- If a list of strings is provided, it's treated as exact experiment names to match.<br>- To provide a more complex condition on an arbitrary attribute value, pass a `Filter` object. The filter must use only select attributes from the `sys` and `env` namespaces. Ask your administrator for a list of supported attributes. Use the `Attribute` class to specify the attribute name and type. |
| `attributes` | `str` \| list of `str` \| `AttributeFilter` | `None` | Filter specifying which attributes to include.<br><br>- If a string is provided, it's treated as a regex pattern that the attribute names must match.<br>- If a list of strings is provided, it's treated as exact attribute names to match.<br>- To provide a more complex condition, pass an `AttributeFilter` object. |
| `sort_by` | `Attribute` | `None` | Attribute to sort the table by. If provided, needs to specify the attribute type. |
| `sort_direction` | `"asc"` \| `"desc"` | `"desc"` | The direction to sort columns by: `"desc"` (default) or `"asc"`. |
| `limit` | `int` | `None` | Maximum number of experiments to return. By default, all experiments are included. |
| `type_suffix_in_column_names` | `bool` | `False` | If `True`, columns of the returned DataFrame are suffixed with `:<type>`. For example, `"attribute1:float_series"`, `"attribute1:string"`.<br><br>If `False` (default), the method throws an exception if there are multiple types under one path. |

## Returns

`pandas.DataFrame` – A DataFrame similar to the runs table in the web app.

The DataFrame has:

- a single-level index `"experiment"` with experiment names
- a single-level column index with attribute names

For series attributes, the last logged value is returned.

## Raises

- `AttributeTypeInferenceError` – If the attribute type wasn't specified in a filter passed to the `experiments` argument.
- `ConflictingAttributeTypes` – If there are conflicting attribute types under the same path and the `type_suffix_in_column_names` argument is set to `False`.

## Example

Fetch attributes matching `loss` or `configs` from two specific experiments across all projects:

```py
import neptune_query.experimental as nq_experimental


nq_experimental.fetch_experiments_table_global(
    experiments=["seagull-week1", "seagull-week2"],
    attributes=r"loss | configs",
)
```

Fetch attributes matching `loss` or `configs` from experiments created by a specific user across all projects:

```py
import neptune_query.experimental as nq_experimental
from neptune_query.filters import Filter, Attribute


nq_experimental.fetch_experiments_table_global(
    experiments=Filter.eq(Attribute("sys/owner", "string"), "my-username"),
    attributes=r"loss | configs",
)
```


