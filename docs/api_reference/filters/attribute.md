# `Attribute`

Specifies an attribute and its type.

When fetching experiments or runs, use this class to filter and sort the returned entries.

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `name` | `str` | - | **Required.** Attribute name to match exactly. |
| `type` | `"bool"` \| `"datetime"` \| `"file"` \| `"float"` \| `"int"` \| `"string"` \| `"string_set"` \| `"float_series"` \| `"histogram_series"` \| `"string_series"` \| `"file_series"` | `None` | Attribute type. Specify it to resolve ambiguity, in case some of the project's runs contain attributes that have the same name but are of a different type. |

## Examples

Fetch metadata from experiments with "config/batch_size" set to the integer 64:

```py
import neptune_query as nq
from neptune_query.filters import Attribute, Filter


batch_size = Attribute(
    name="config/batch_size",
    type="int",
)

nq.fetch_experiments_table(experiments=Filter.eq(batch_size, 64))
```
