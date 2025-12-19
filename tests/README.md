# Environment variables

* `NEPTUNE_E2E_API_TOKEN` - an API TOKEN to use in the e2e tests
* `NEPTUNE_E2E_WORKSPACE` - a workspace that e2e tests can create projects in
* `NEPTUNE_E2E_VERIFY_SSL` - whether to verify SSL certificates when running the e2e tests;
   only required if the instance you're running the e2e tests against has an invalid cert

## Projects used and created in end-to-end tests

End-to-end tests create projects in the workspace specified by `NEPTUNE_E2E_WORKSPACE` using the `ensure_project` fixture.

Project names follow this pattern:

- Name: `pye2e__<EXECUTION_ID>__<MODULE>.<FUNCTION>`
- Full identifier: `<WORKSPACE>/pye2e__<EXECUTION_ID>__<MODULE>.<FUNCTION>`

Where:
- `<EXECUTION_ID>` is a per-test-run identifier shared across all workers. It is taken from `NEPTUNE_TEST_EXECUTION_ID` if set, otherwise generated as a timestamp-based value like `YYYYMMDD_HHMMSS_xxxxxx`.
- `<MODULE>` and `<FUNCTION>` are the Python module and function names of the caller that invoked `ensure_project` (e.g., a fixture like `project(...)`).

Example:
- `NEPTUNE_E2E_WORKSPACE=my-org`
- Caller: module `tests.e2e.v1.test_fetch_series`, function `project`
- Project identifier: `my-org/pye2e__20250101_123456_abcdef__tests.e2e.v1.test_fetch_series.project`

Behavior and notes:
- If a project with the target name does not exist, it is created and populated with the requested test data; otherwise it is reused for idempotency within a single run.
- A file lock guards concurrent creation across workers: `${TMPDIR}/neptune_e2e__<WORKSPACE>__<PROJECT_NAME>.lock`.
- Projects have private visibility (`priv`).

Cleanup:
- After a run, you may safely delete projects with the `pye2e__` prefix (optionally filtered by a specific `<EXECUTION_ID>`) from the `NEPTUNE_E2E_WORKSPACE` to free up space.
