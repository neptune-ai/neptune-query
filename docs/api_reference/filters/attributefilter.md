# `AttributeFilter`

Specifies criteria for attributes to include as columns when using a fetching method that returns a data frame.

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `name` | `str` \| list of `str` | `None` | Criterion for attribute names.<br><br>If a string is provided, it's treated as a regex pattern that the attribute name must match. Supports Neptune's extended regex syntax.<br><br>If a list of strings is provided, it's treated as exact attribute names to match. |
| `type` | `"bool"` \| `"datetime"` \| `"file"` \| `"float"` \| `"int"` \| `"string"` \| `"string_set"` \| `"float_series"` \| `"histogram_series"` \| `"string_series"` \| `"file_series"` | `all available types` | Allowed attribute types, as a literal string or a list of literal strings. |

## Examples

From a particular experiment, fetch values from all FloatSeries attributes with "loss" in the name:

```py
import neptune_query as nq
from neptune_query.filters import AttributeFilter


losses = AttributeFilter(name=r"loss", type="float_series")
loss_values = nq.fetch_metrics(
    experiments=["training-week-34"],
    attributes=losses,
)
```

To join multiple attribute filters with OR, use the `|` operator:

```py
import neptune_query as nq
from neptune_query.filters import AttributeFilter


losses = AttributeFilter(name=r"loss", type="float_series")
learning_rate = AttributeFilter(name="configs/learning_rate")
nq.fetch_experiments_table(attributes=losses | learning_rate)
```
