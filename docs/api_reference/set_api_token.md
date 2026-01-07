# `set_api_token()`

Sets the Neptune API token for the session.

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `api_token` | `str` | - | **Required.** Your Neptune API token or a service account's API token.<br><br>If `None`, the value of the `NEPTUNE_API_TOKEN` environment variable is used. |

## Example

```py
import neptune_query as nq


nq.set_api_token("SomeOtherNeptuneApiToken")
```
