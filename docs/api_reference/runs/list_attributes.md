# `list_attributes()`

Lists unique attributes in the runs of a Neptune project.

To limit the results, define filters for runs to search or attributes to include.

> **Related:**
>
> - [List attributes from experiments][list-attributes]

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `project` | `str` | `None` | Path of the Neptune project, as `WorkspaceName/ProjectName`.<br><br>If not provided, the `NEPTUNE_PROJECT` environment variable is used. |
| `runs` | `str` \| list of `str` \| `Filter` | `None` | Filter specifying which runs to search.<br><br>- If a string is provided, it's treated as a regex pattern that the run IDs must match.<br>- If a list of strings is provided, it's treated as exact run IDs to match.<br>- To provide a more complex condition on an arbitrary attribute value, pass a `Filter` object. |
| `attributes` | `str` \| list of `str` \| `AttributeFilter` | `None` | Filter specifying which attributes to include.<br><br>- If a string is provided, it's treated as a regex pattern that the attribute names must match.<br>- If a list of strings is provided, it's treated as exact attribute names to match.<br>- To provide a more complex condition, pass an `AttributeFilter` object. |

## Returns

`list[str]` – A list of attribute names in the runs of the Neptune project.

## Example

List all attributes of a specific run:

```py
import neptune_query.runs as nq_runs


nq_runs.list_attributes(
    runs=["prompt-wolf-20250605132116671-2g2r1"],  # run ID
)
```

List all attributes that begin with "metrics":

```py
import neptune_query.runs as nq_runs


nq_runs.list_attributes(attributes=r"^metrics")
```

Search a specific project for runs with a learning rate less than 0.01 and return the logged attributes:

```py
import neptune_query.runs as nq_runs
from neptune_query.filters import Filter


nq_runs.list_attributes(
    project="team-alpha/sandbox",
    runs=Filter.lt("config/lr", 0.01),
)
```

[list-attributes]: ../list_attributes.md
