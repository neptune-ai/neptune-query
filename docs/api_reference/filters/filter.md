# `Filter`

Specifies criteria for experiments or attributes when using a fetching method.

Examples of filters:

- Name or attribute value matches regular expression.
- Attribute value passes a condition, like "greater than 0.9".
- Attribute of a given name must exist.

You can negate a filter or join multiple filters with logical operators.

## Methods

The following functions create a criterion based on the value of an attribute:

| Method                           | Description                                               | Example |
| -------------------------------- | --------------------------------------------------------- | ------- |
| [`name()`](#name)                | Run or experiment name matches a regex or a list of names                                                                 | `Filter.name(["kittiwake_week12"])`                |
| [`eq()`](#eq)                    | Attribute value equals                                                                                                    | `Filter.eq("lr", 0.001)`                           |
| [`ne()`](#ne)                    | Attribute value doesn't equal                                                                                             | `Filter.ne("sys/owner", "bot@my-workspace")`       |
| [`gt()`](#gt)                    | Attribute value is greater than                                                                                           | `Filter.gt("acc", 0.9)`                            |
| [`ge()`](#ge)                    | Attribute value is greater than or equal to                                                                               | `Filter.ge("acc", 0.93)`                           |
| [`lt()`](#lt)                    | Attribute value is less than                                                                                              | `Filter.lt("loss", 0.1)`                           |
| [`le()`](#le)                    | Attribute value is less than or equal to                                                                                  | `Filter.le("loss", 0.11)`                          |
| [`matches()`](#matches)          | `String` attribute value matches a regular expression                                                   | `Filter.matches("optimizer", r"^Ada \| grad")`     |
| [`contains_all()`](#contains_all)| - `StringSet` attribute contains a string or all in a list of strings<br>- `String` attribute value contains a substring or all in a list of substrings | `Filter.contains_all("sys/tags", ["best", "v2.1"])`|
| [`contains_none()`](#contains_none)| - `StringSet` attribute doesn't contain a string or any in a list of strings<br>- `String` attribute value doesn't contain a substring or any in a list of substrings | `Filter.contains_none("tokenizer", "bpe")`         |
| [`exists()`](#exists)            | Attribute exists                                                                                                          | `Filter.exists("metric7")`                         |

> **Info:** When passing a series attribute to a method that compares an attribute against a value, the last logged value is used.

---

### `name()`

Creates a filter that matches runs or experiments by their name.

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `name` | `str` \| list of `str` | - | **Required.** A regular expression pattern to match run names, or a list of run names to match exactly. |

#### Examples

Match exact names:

```py
import neptune_query as nq
from neptune_query.filters import Filter


name_filter = Filter.name(["kittiwake_week12", "kittiwake_week13"])

# Use the filter in a fetching method
nq.fetch_experiments_table(experiments=name_filter)
```

Use regex to match names:

```py
import neptune_query as nq
from neptune_query.filters import Filter


name_regex_filter = Filter.name(r"kittiwake_week\d+")

# Use the filter in a fetching method
nq.fetch_experiments_table(experiments=name_regex_filter)
```

Note that the `experiments` argument also takes a string or list of strings directly, without using the `Filter` class. Use the `name()` method to create a name filter object that you can combine with other filters.

---

### `eq()`

Creates a filter that matches runs or experiments where the specified attribute is equal to the given value.

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` \| `Attribute` | - | **Required.** The attribute to compare. Can be an attribute path, such as `config/lr`, or an `Attribute` object.<br><br>If passing a series attribute, the last logged value is used. |
| `value` | `int` \| `float` \| `str` \| `datetime` | - | **Required.** The value to compare against. Must be an integer, float, string, or datetime. |

#### Returns

`Filter`: A filter object used to specify which runs or experiments to query.

#### Examples

Use a string to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments owned by username "sigurd"
owner_filter = Filter.eq("sys/owner", "sigurd")

# Use the filter in a fetching method
nq.fetch_metrics(experiments=owner_filter, attributes="metrics/(val|test)/")
```

Use the Attribute class to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter, Attribute


# Create a filter for experiments with a batch size of 64
# and specify the attribute type
batch_size_filter = Filter.eq(Attribute(name="batch_size", type="int"), 64)

# Use the filter in a fetching method
nq.fetch_metrics(experiments=batch_size_filter, attributes="metrics/(val|test)/")
```

---

### `ne()`

Creates a filter that matches runs or experiments where the specified attribute is not equal to the given value.

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` \| `Attribute` | - | **Required.** The attribute to compare. Can be an attribute path, such as `config/lr`, or an `Attribute` object.<br><br>If passing a series attribute, the last logged value is used. |
| `value` | `int` \| `float` \| `str` \| `datetime` | - | **Required.** The value to compare against. Must be an integer, float, string, or datetime. |

#### Examples

Use a string to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments not owned by a particular service account
human_filter = Filter.ne("sys/owner", "bot@my-workspace")

# List experiments matching the filter
nq.list_experiments(experiments=human_filter)
```

Use the Attribute class to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter, Attribute


# Create a filter for experiments with a batch size not equal to 64
# and specify the attribute type
batch_size_filter = Filter.ne(Attribute(name="batch_size", type="int"), 64)

# List experiments matching the filter
nq.list_experiments(experiments=batch_size_filter)
```

---

### `gt()`

Creates a filter that matches runs or experiments where the specified attribute is greater than the given value.

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` \| `Attribute` | - | **Required.** The attribute to compare. Can be an attribute path, such as `config/lr`, or an `Attribute` object.<br><br>If passing a series attribute, the last logged value is used. |
| `value` | `int` \| `float` \| `str` \| `datetime` | - | **Required.** The value to compare against. Must be an integer, float, string, or datetime. |

#### Examples

Use a string to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments with an accuracy greater than 0.9
acc_filter = Filter.gt("metrics/val/acc", 0.9)

# List configs of experiments that match the filter
nq.fetch_experiments_table(experiments=acc_filter, attributes="config/")
```

Use the Attribute class to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter, Attribute


# Create a filter for experiments with an accuracy greater than 0.9
# and specify the attribute type
acc_filter = Filter.gt(Attribute(name="metrics/val/acc", type="float_series"), 0.9)

# List configs of experiments that match the filter
nq.fetch_experiments_table(experiments=acc_filter, attributes="config/")
```

---

### `ge()`

Creates a filter that matches runs or experiments where the specified attribute is greater than or equal to the given value.

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` \| `Attribute` | - | **Required.** The attribute to compare. Can be an attribute path, such as `config/lr`, or an `Attribute` object.<br><br>If passing a series attribute, the last logged value is used. |
| `value` | `int` \| `float` \| `str` \| `datetime` | - | **Required.** The value to compare against. Must be an integer, float, string, or datetime. |

#### Examples

Use a string to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments with an accuracy greater than 0.9
acc_filter = Filter.ge("metrics/val/acc", 0.9)

# List configs of experiments that match the filter
nq.fetch_experiments_table(experiments=acc_filter, attributes="config/")
```

Use the Attribute class to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter, Attribute


# Create a filter for experiments with an accuracy greater than 0.9
# and specify the attribute type
acc_filter = Filter.ge(Attribute(name="metrics/val/acc", type="float_series"), 0.9)

# List configs of experiments that match the filter
nq.fetch_experiments_table(experiments=acc_filter, attributes="config/")
```

---

### `lt()`

Creates a filter that matches runs or experiments where the specified attribute is less than the given value.

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` \| `Attribute` | - | **Required.** The attribute to compare. Can be an attribute path, such as `config/lr`, or an `Attribute` object.<br><br>If passing a series attribute, the last logged value is used. |
| `value` | `int` \| `float` \| `str` \| `datetime` | - | **Required.** The value to compare against. Must be an integer, float, string, or datetime. |

#### Examples

Use a string to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments with a loss less than 0.1
loss_filter = Filter.lt("metrics/val/loss", 0.1)

# List configs of experiments that match the filter
nq.fetch_experiments_table(experiments=loss_filter, attributes="config/")
```

Use the Attribute class to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter, Attribute


# Create a filter for experiments with a loss less than 0.1
# and specify the attribute type
loss_filter = Filter.lt(Attribute(name="metrics/val/loss", type="float_series"), 0.1)

# List configs of experiments that match the filter
nq.fetch_experiments_table(experiments=loss_filter, attributes="config/")
```

---

### `le()`

Creates a filter that matches runs or experiments where the specified attribute is less than or equal to the given value.

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` \| `Attribute` | - | **Required.** The attribute to compare. Can be an attribute path, such as `config/lr`, or an `Attribute` object.<br><br>If passing a series attribute, the last logged value is used. |
| `value` | `int` \| `float` \| `str` \| `datetime` | - | **Required.** The value to compare against. Must be an integer, float, string, or datetime. |

#### Examples

Use a string to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments with a loss less than or equal to 0.1
loss_filter = Filter.le("metrics/val/loss", 0.1)

# List configs of experiments that match the filter
nq.fetch_experiments_table(experiments=loss_filter, attributes="config/")
```

Use the Attribute class to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter, Attribute


# Create a filter for experiments with a loss less than or equal to 0.1
# and specify the attribute type
loss_filter = Filter.le(Attribute(name="metrics/val/loss", type="float_series"), 0.1)

# List configs of experiments that match the filter
nq.fetch_experiments_table(experiments=loss_filter, attributes="config/")
```

---

### `matches()`

Creates a filter that matches runs or experiments where the specified attribute value matches a regular expression.

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` \| `Attribute` | - | **Required.** The attribute to match. Can be an attribute path, such as `config/lr`, or an `Attribute` object.<br><br>If passing a string series attribute, the last logged value is used. |
| `pattern` | `str` | - | **Required.** The regular expression pattern to match against the attribute value.<br><br>Neptune's extended regular expression syntax is supported. |

#### Examples

Use a string to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter

# Create a filter for experiments with an optimizer value starting with "Ada" or containing "grad"
optimizer_filter = Filter.matches("optimizer", r"^Ada | grad")

# List experiments matching the filter
nq.list_experiments(experiments=optimizer_filter)
```

Use the Attribute class to specify the attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter, Attribute


# Create a filter for experiments with an optimizer value starting with "Ada" or containing "grad"
# and specify the attribute type
optimizer_filter = Filter.matches(Attribute(name="optimizer", type="string_series"), r"^Ada | grad")

# List experiments matching the filter
nq.list_experiments(experiments=optimizer_filter)
```

Negate an expression:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments with an optimizer not starting with "Ada" or containing "grad"
optimizer_filter = Filter.matches("optimizer", r"!^Ada & !grad")

# List experiments matching the filter
nq.list_experiments(experiments=optimizer_filter)
```

Note that you can also [negate entire filters](#negation).

---

### `contains_all()`

Creates a filter that matches runs or experiments where the specified attribute contains all of the given values.

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` \| `Attribute` | - | **Required.** The attribute to check. Can be an attribute path, such as `sys/tags`, or an `Attribute` object. |
| `values` | `str` \| `list[str]` | - | **Required.** - For `StringSet` attributes: a string or list of strings that must all be present in the tag set.<br>- For `String` attributes: a substring or list of substrings that must all be present in the string value. |

#### Examples

Check for tags:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments belonging to group1 and group2
group_filter = Filter.contains_all("sys/group_tags", ["group1", "group2"])

# List experiments matching the filter
nq.list_experiments(experiments=group_filter)
```

Check for substrings:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments with a tokenizer containing "bpe"
tokenizer_filter = Filter.contains_all("tokenizer", "bpe")

# List experiments matching the filter
nq.list_experiments(experiments=tokenizer_filter)
```

---

### `contains_none()`

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` \| `Attribute` | - | **Required.** The attribute to check. Can be an attribute path, such as `sys/tags`, or an `Attribute` object. |
| `values` | `str` \| `list[str]` | - | **Required.** - For `StringSet` attributes: a string or list of strings that must not be present in the tag set.<br>- For `String` attributes: a substring or list of substrings that must not be present in the string value. |

#### Examples

Exclude tags:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments not belonging to group2
group_filter = Filter.contains_none("sys/group_tags", "group2")

# List experiments not belonging to group2
nq.list_experiments(experiments=group_filter)
```

Exclude substrings:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments not using a tokenizer containing "bpe"
tokenizer_filter = Filter.contains_none("tokenizer", "bpe")

# List experiments not using a tokenizer containing "bpe"
nq.list_experiments(experiments=tokenizer_filter)
```

---

### `exists()`

Creates a filter that matches runs or experiments where the specified attribute exists.

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` \| `Attribute` | - | **Required.** The attribute to check. Can be an attribute path, such as `metrics/val/loss`, or an `Attribute` object. |

#### Examples

Check for existence of an attribute:

```py
import neptune_query as nq
from neptune_query.filters import Filter


# Create a filter for experiments with a validation loss
loss_filter = Filter.exists("metrics/val/loss")

# List experiments with a validation loss
nq.list_experiments(experiments=loss_filter)
```

Check for existence of an attribute of a specific type:

```py
import neptune_query as nq
from neptune_query.filters import Filter, Attribute


# Create a filter for experiments with a validation loss attribute of type FloatSeries
loss_filter = Filter.exists(Attribute(name="metrics/val/loss", type="float_series"))

# List experiments with a validation loss
nq.list_experiments(experiments=loss_filter)
```

## Operate on filters

You can negate filters and combine them with logical operators.

### Negation

To negate a filter, use `negate()` or prepend `~` to the filter:

```py
from neptune_query.filters import Filter


owned_by_me = Filter.eq("sys/owner", "vidar")

owned_by_someone_else = negate(owned_by_me)
# equivalent to
owned_by_someone_else = ~owned_by_me
```

### Conjunction (logical AND)

Conjoin filters with `&`:

```py
loss_filter = Filter.lt("validation/loss", 0.1)

owned_by_me_and_small_loss = owned_by_me & loss_filter
```

### Alternation (logical OR)

Alternate filters with `|`:

```py
owned_by_me_or_small_loss = owned_by_me | loss_filter
```

## Examples

Fetch loss values from experiments with specific tags:

```py
import neptune_query as nq
from neptune_query.filters import Filter


specific_tags = Filter.contains_all("sys/tags", ["fly", "swim", "nest"])
nq.fetch_metrics(experiments=specific_tags, attributes=r"^metrics/loss/")
```

List my experiments that have a "dataset_version" attribute and "validation/loss" less than 0.1:

```py
owned_by_me = Filter.eq("sys/owner", "sigurd")
dataset_check = Filter.exists("dataset_version")
loss_filter = Filter.lt("validation/loss", 0.1)

interesting = owned_by_me & dataset_check & loss_filter
nq.list_experiments(experiments=interesting)
```

Fetch configs from the interesting experiments:

```py
nq.fetch_experiments_table(experiments=interesting, attributes=r"config/")
```


