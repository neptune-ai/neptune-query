# Query single attribute values

To fetch configs, scores, or other single values from experiments, use the `fetch_experiments_table()` function.

The returned data frame mimics the runs table of the web app.

If series attributes are included, only the last logged value is returned.

```py
loss_and_config_df = nq.fetch_experiments_table(
    experiments=["kittiwake_week-1", "kittiwake_week-2", "kittiwake_week-3"],
    attributes=r"loss | config",
)
```

```pycon title="Sample output"
                   loss  config/n_layer  config/learning_rate
experiment
kittiwake_week-1   0.22              12                  0.01
kittiwake_week-2   0.28              12                  0.02
kittiwake_week-3   0.24              12                  0.01
```

To specify experiments or attributes to include, pass a list of exact names or a regular expression to the `experiments` or `attributes` argument.

To provide more complex criteria or join multiple conditions together, use filter objects. For a guide, see [How to construct filters](how_to_construct_filters.md).

## Target runs instead of experiments

To specify runs by ID instead of experiments by name, use the `fetch_runs_table()` function from the `runs` module:

```py
import neptune_query.runs as nq_runs


runs_df = nq_runs.fetch_runs_table(
    runs=["glowing-database-60704895-9ps29"],  # list of run IDs, or regex to match
    attributes=r"loss | config",
)
```

```pycon title="Sample output"
                                          loss  config/n_layer  config/learning_rate
run
glowing-database-60704895-9ps29           0.02              12                  0.01
```
