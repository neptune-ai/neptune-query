# How to construct filters

When querying logged metadata from Neptune, you can define criteria for experiments, runs, and attributes to include.

## Specifying experiments

To specify experiment criteria, use the `experiments` argument of any querying function. The criteria can be simple or complex:

- In the simplest case, you can list the exact names of experiments or runs to include.
- Instead of providing exact names, you can use regular expressions to match names.
- To specify more complex criteria that can involve any attributes and not just names, use the `Filter` class. For example, you can include only experiments with a final loss of less than 0.1.

### Specify exact experiments

To specify exact experiments to include, pass a list of experiment names directly to the `experiments` argument:

```py
import neptune_query as nq


nq.list_experiments(experiments=["kittiwake_week-1", "kittiwake_week-2"])
```

### Match regular expression

To specify experiments by name pattern, pass a regular expression to the `experiments` argument:

```py
import neptune_query as nq


nq.list_experiments(experiments=r"kittiwake_week-\d+")
```

Extended regex syntax is supported, so you can join multiple expressions with logical operators.

### Specify attribute criterion

To only include experiments where an attribute value meets certain criteria, use the `Filter` class. The class exposes various methods that you can use to set conditions on an attribute's value or existence.

For example, to list experiments with a final validation loss of less than 0.1, use:

```py
import neptune_query as nq
from neptune_query.filters import Filter


low_loss = Filter.lt("val/loss", 0.1)
nq.list_experiments(experiments=low_loss)
```

> For a list of all available methods, see the [Filter reference](./api_reference/filters/filter.md#methods).

You can also negate or join multiple filters together:

```py
import neptune_query as nq
from neptune_query.filters import Filter


low_loss = Filter.lt("val/loss", 0.1)
owned_by_me = Filter.eq("sys/owner", "sigurd")

interesting = owned_by_me & low_loss
nq.list_experiments(experiments=interesting)
```

## Specifying runs

Setting run criteria works the same as for experiments, but runs are identified by ID instead of name.

To query specific runs, pass the run IDs, a regular expression, or a `Filter` object to the `runs` argument:

```py title="Exact run IDs"
import neptune_query.runs as nq_runs


nq_runs.list_runs(runs=["spurious-kittiwake_025c425", "cunning-kittiwake_x56jjh2"])
```

```py title="Regular expression"
import neptune_query.runs as nq_runs


nq_runs.list_runs(runs=r"^\w+-kittiwake_")
```

```py title="Filter object"
import neptune_query.runs as nq_runs
from neptune_query.filters import Filter


low_loss = Filter.lt("val/loss", 0.1)
nq_runs.list_runs(runs=low_loss)
```

### Runs of a specific experiment

Each run's experiment information is stored in the `sys/experiment` namespace.

To query runs belonging to a specific experiment, construct a filter on the `sys/experiment/name` attribute:

```py
import neptune_query.runs as nq_runs
from neptune_query.filters import Filter


nq_runs.list_runs(runs=Filter.eq("sys/experiment/name", "kittiwake-week-1"))
```

> Note that `list_runs()` is just an example. You can pass such filters to any querying function that takes a `runs` argument.

## Specifying attributes

Attribute criteria are specified with the `attributes` argument of any querying function. This option is available for functions that return data frames – the attributes that pass the filter are returned as columns.

The criteria can be simple or complex:

- In the simplest case, you can list the exact names of attributes to include.
- Instead of providing exact names, you can use regular expressions to match attribute names.
- To additionally specify the type of an attribute, use the `AttributeFilter` class.

### Specify exact attributes

To specify exact attributes to include, pass a list of attribute names directly to the `attributes` argument:

```py title="Include only learning rate and batch size as columns"
import neptune_query as nq


nq.fetch_experiments_table(attributes=["configs/learning_rate", "configs/batch_size"])
```

### Match regular expression

To specify attributes by name pattern, pass a regular expression to the `attributes` argument:

```py title="Include only attributes starting with 'configs/' as columns"
import neptune_query as nq


nq.fetch_experiments_table(attributes=r"^configs/")
```

### Specify attribute type

To specify the type of an attribute, use the `AttributeFilter` class.

For example, to include batch size of type int as a column:

```py
import neptune_query as nq
from neptune_query.filters import AttributeFilter


batch_size_int = AttributeFilter(name="configs/batch_size", type="int")
nq.fetch_experiments_table(attributes=batch_size_int)
```

To join multiple attribute filters with OR, use the `|` operator:

```py
import neptune_query as nq
from neptune_query.filters import AttributeFilter


batch_size_int = AttributeFilter(name="configs/batch_size", type="int")
learning_rate_float = AttributeFilter(name="configs/learning_rate", type="float")
nq.fetch_experiments_table(attributes=batch_size_int | learning_rate_float)
```
