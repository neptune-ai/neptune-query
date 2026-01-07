# `list_attributes()`

Lists unique attributes in the experiments of a Neptune project.

To limit the results, define filters for experiments to search or attributes to include.

> **Related:**
>
> - [List attributes from runs][list-attributes-runs]

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `project` | `str` | `None` | Path of the Neptune project, as `WorkspaceName/ProjectName`.<br><br>If not provided, the `NEPTUNE_PROJECT` environment variable is used. |
| `experiments` | `str` \| list of `str` \| `Filter` | `None` | Filter specifying which experiments to search.<br><br>- If a string is provided, it's treated as a regex pattern that the experiment names must match.<br>- If a list of strings is provided, it's treated as exact experiment names to match.<br>- To provide a more complex condition on an arbitrary attribute value, pass a `Filter` object. |
| `attributes` | `str` \| list of `str` \| `AttributeFilter` | `None` | Filter specifying which attributes to include.<br><br>- If a string is provided, it's treated as a regex pattern that the attribute names must match.<br>- If a list of strings is provided, it's treated as exact attribute names to match.<br>- To provide a more complex condition, pass an `AttributeFilter` object. |

## Returns

`list[str]` – A list of attribute names in the experiments of the Neptune project.

## Example

List all attributes that begin with "metrics":

```py
import neptune_query as nq


nq.list_attributes(attributes=r"^metrics")
```

Search a specific project for experiments with a learning rate less than 0.01 and
return the logged attributes:

```py
from neptune_query.filters import Filter


nq.list_attributes(
    project="team-alpha/sandbox",
    experiments=Filter.lt("config/lr", 0.01),
)
```

[list-attributes-runs]: runs/list_attributes.md
