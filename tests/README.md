# Environment variables

* `NEPTUNE_E2E_API_TOKEN` - an API TOKEN to use in the e2e tests
* `NEPTUNE_E2E_WORKSPACE` - a workspace that e2e tests can create projects in
* `NEPTUNE_E2E_PROJECT` - a project to use for (some of) the e2e tests
* `NEPTUNE_E2E_REUSE_PROJECT` - see below
* `NEPTUNE_E2E_VERIFY_SSL` - whether to verify SSL certificates when running the e2e tests;
   only required if the instance you're running the e2e tests against has an invalid cert

## Projects used and created in end-to-end tests
Each e2e run uses 2 projects. You can control them with environment variables.

- The first project is specified explicitly.\
If the data required by a test doesn't exist in this project, it's populated during the test run.\
Example: `NEPTUNE_E2E_PROJECT="neptune/e2e-tests"`


- The second project's name is generated depending on the `NEPTUNE_E2E_REUSE_PROJECT` env.\
The project is created / expected in the workspace under the `NEPTUNE_E2E_WORKSPACE` env.\
Importantly, your workspace needs the ability to set project visibility to 'workspace', since that's what our tests use

  - If `NEPTUNE_E2E_REUSE_PROJECT=false` (the default), the project name is .e.g. `pye2e-runs-<datetime>-v1`.\
  The project is always created and populated with data.

  - If `NEPTUNE_E2E_REUSE_PROJECT=true`, the project name is `pye2e-runs-<hash>-v1`.\
  The name contains a hash of the data the tests need in this project.\
  If the project doesn't exist, it's created and populated.
