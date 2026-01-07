# `list_runs()`

Lists the IDs of runs in a Neptune project.

The ID refers to the string stored in the `sys/custom_run_id` attribute of a run.

> **Related:**
>
> - [List experiments][list-experiments]

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `project` | `str` | `None` | Path of the Neptune project, as `WorkspaceName/ProjectName`.<br><br>If not provided, the `NEPTUNE_PROJECT` environment variable is used. |
| `runs` | `str` \| list of `str` \| `Filter` | `None` | Filter specifying which runs to include.<br><br>- If a string is provided, it's treated as a regex pattern that the run IDs must match.<br>- If a list of strings is provided, it's treated as exact run IDs to match.<br>- To provide a more complex condition on an arbitrary attribute value, pass a `Filter` object.<br><br>If no filter is specified, all runs are returned. |

## Returns

`list[str]` – A list of run IDs in the Neptune project.

## Examples

List all my runs in a specific project:

```py
import neptune_query.runs as nq_runs
from neptune_query.filters import Filter


nq_runs.list_runs(
    project="team-alpha/sandbox",
    runs=Filter.eq("sys/owner", "MyUsername"),
)
```

[list-experiments]: ../list_experiments.md
