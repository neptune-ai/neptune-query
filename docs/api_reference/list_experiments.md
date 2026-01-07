# `list_experiments()`

Lists the names of experiments in a Neptune project.

> **Related:**
>
> - [List runs][list-runs]

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `project` | `str` | `None` | Path of the Neptune project, as `WorkspaceName/ProjectName`.<br><br>If not provided, the `NEPTUNE_PROJECT` environment variable is used. |
| `experiments` | `str` \| list of `str` \| `Filter` | `None` | Filter specifying which experiments to search.<br><br>- If a string is provided, it's treated as a regex pattern that the experiment names must match.<br>- If a list of strings is provided, it's treated as exact experiment names to match.<br>- To provide a more complex condition on an arbitrary attribute value, pass a `Filter` object. |

## Returns

`list[str]` – A list of experiment names in the Neptune project.

## Examples

List all experiments whose names begin with "sigurd":

```py
import neptune_query as nq


nq.list_experiments(experiments=r"^sigurd")
```

Search a specific project for experiments with a learning rate less than 0.01:

```py
from neptune_query.filters import Filter


nq.list_experiments(
    project="team-alpha/sandbox",
    experiments=Filter.lt("config/lr", 0.01),
)
```

[list-runs]: runs/list_runs.md

