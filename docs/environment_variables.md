# neptune-query environment variables

Environment variables related to neptune-query Python package.

## `NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS`

Controls HTTPX timeouts. The timeout is in seconds and applies to individual networking operations, such as connect, read, and write.

The default duration is `60`.

## `NEPTUNE_QUERY_MAX_WORKERS`

Controls the number of workers in the thread pool.

The default number is `10`.

## `NEPTUNE_QUERY_RETRY_SOFT_TIMEOUT`

Controls the soft timeout for retrying failed server requests. The soft limit doesn't include wait time due to Retry-After responses.

The soft limit plus Retry-After wait time together can't exceed the limit specified in the [`NEPTUNE_QUERY_RETRY_HARD_TIMEOUT`](#neptune_query_retry_hard_timeout) environment variable.

The default duration is `1800` seconds.

## `NEPTUNE_QUERY_RETRY_HARD_TIMEOUT`

Controls the hard timeout for retrying failed server requests. The hard limit is the maximum time allowed for a single HTTP request until an exception is raised.

The soft limit specified in the [`NEPTUNE_QUERY_RETRY_SOFT_TIMEOUT`](#neptune_query_retry_soft_timeout) environment variable plus wait time due to Retry-After responses together can't exceed the hard limit.

The default duration is `3600` seconds.
