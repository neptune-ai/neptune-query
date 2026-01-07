# Neptune Query API

The `neptune_query` package is a read-only API for fetching metadata.

With the Query API, you can:

- List experiments, runs, and attributes of a project.
- Fetch experiment or run metadata as a data frame.
- Define filters to fetch experiments, runs, and attributes that meet certain criteria.

## Installation

```bash
pip install "neptune-query<2.0.0"
```

Set your Neptune API token and project name as environment variables:

```bash
export NEPTUNE_API_TOKEN="ApiTokenFromYourNeptuneProfile"
```

```bash
export NEPTUNE_PROJECT="workspace-name/project-name"
```

> **Note:** You can also pass the project path to the `project` argument of any querying function.

## Usage

```python
import neptune_query as nq
```

Available functions:

- `download_files()` &ndash; download files from the specified experiments.
- `fetch_experiments_table()` &ndash; runs as rows and attributes as columns.
- (experimental) `fetch_experiments_table_global()` &ndash; like `fetch_experiments_table()`, but searches across all projects that the user has access to.
- (experimental) `fetch_metric_buckets()` &ndash; get summary values split by X-axis buckets.
- `fetch_metrics()` &ndash; series of float or int values, with steps as rows.
- ([runs API](#runs-api)) `fetch_runs_table()` &ndash; like `fetch_experiments_table()`, but for individual runs.
- `fetch_series()` &ndash; for series of strings or histograms.
- `list_attributes()` &ndash; all logged attributes of the target project's experiment runs.
- `list_experiments()` &ndash; names of experiments in the target project.
- `set_api_token()` &ndash; set the Neptune API token to use for the session.

For details, see the [API reference](./docs/api_reference/).

### Runs API

You can target individual runs by ID instead of experiment runs by name.

To use the corresponding methods for runs, import the `runs` module:

```python
import neptune_query.runs as nq_runs

nq_runs.fetch_metrics(...)
```

You can use these methods to target individual runs by ID instead of experiment runs by name.

## Documentation

For how-tos and the complete API reference, see the [docs](./docs) directory.

## Examples

The following are some examples of how to use the Query API. For all functions and options, see the [API reference](./docs/api_reference/).

### Example 1: Fetch metric values

To fetch values at each step, use `fetch_metrics()`.

- To filter experiments to return, use the `experiments` parameter.
- To specify attributes to include as columns, use the `attributes` parameter.
- To limit the returned values, use the available parameters.

```python
nq.fetch_metrics(
    experiments=["exp_dczjz"],
    attributes=r"metrics/val_.+_estimated$",
    tail_limit=10,
)
```

```pycon
                  metrics/val_accuracy_estimated  metrics/val_loss_estimated
experiment  step
exp_dczjz    1.0                        0.432187                    0.823375
             2.0                        0.649685                    0.971732
             3.0                        0.760142                    0.154741
             4.0                        0.719508                    0.504652
```

### Example 2: Fetch metadata as one row per run

To fetch experiment metadata from your project, use the `fetch_experiments_table()` function. The output mimics the runs table in the web app.

- To filter experiments to return, use the `experiments` parameter.
- To specify attributes to include as columns, use the `attributes` parameter.

```python
nq.fetch_experiments_table(
    experiments=r"^exp_",
    attributes=["metrics/train_accuracy", "metrics/train_loss", "learning_rate"],
)
```

```pycon
            metrics/train_accuracy   metrics/train_loss   learning_rate
experiment
exp_ergwq                 0.278149             0.336344            0.01
exp_qgguv                 0.160260             0.790268            0.02
exp_hstrj                 0.365521             0.459901            0.01
```

> For series attributes, the value of the last logged step is returned.

### Example 3: Define filters

List my experiments that have a "dataset_version" attribute and "val/loss" less than 0.1:

```py
from neptune_query.filters import Filter


owned_by_me = Filter.eq("sys/owner", "sigurd")
dataset_check = Filter.exists("dataset_version")
loss_filter = Filter.lt("val/loss", 0.1)

interesting = owned_by_me & dataset_check & loss_filter
nq.list_experiments(experiments=interesting)
```

```pycon
['exp_ergwq', 'exp_qgguv', 'exp_hstrj']
```

Then fetch configs from the experiments, including also the interesting metric:

```py
nq.fetch_experiments_table(
    experiments=interesting,
    attributes=r"config/ | val/loss",
)
```

```pycon
            config/optimizer  config/batch_size  config/learning_rate   val/loss
experiment
exp_ergwq               Adam                 32                 0.001     0.0901
exp_qgguv           Adadelta                 32                 0.002     0.0876
exp_hstrj           Adadelta                 64                 0.001     0.0891
```

### Example 4: Exclude archived runs

To exclude archived experiments or runs from the results, create a filter on the `sys/archived` attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter


exclude_archived = Filter.eq("sys/archived", False)
nq.fetch_experiments_table(experiments=exclude_archived)
```

To use this filter in combination with other criteria, use the `&` operator to join multiple filters:

```py
name_matches_regex = Filter.name(r"^exp_")
exclude_archived = Filter.eq("sys/archived", False)

nq.fetch_experiments_table(experiments=name_matches_regex & exclude_archived)
```

### Example 5: Fetch runs belonging to specific experiment

Each run's experiment information is stored in the `sys/experiment` namespace.

To query runs belonging to a specific experiment, use the runs API and construct a filter on the `sys/experiment/name` attribute:

```py
import neptune_query.runs as nq_runs
from neptune_query.filters import Filter


experiment_name_filter = Filter.eq("sys/experiment/name", "kittiwake-week-1")
nq_runs.list_runs(runs=experiment_name_filter)
```

---

## License

This project is licensed under the Apache License Version 2.0. For details, see [Apache License Version 2.0][license].


[license]: http://www.apache.org/licenses/LICENSE-2.0
