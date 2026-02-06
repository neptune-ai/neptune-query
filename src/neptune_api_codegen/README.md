# neptune_api_codegen

Utility for generating Neptune API bindings and local API spec used by `neptune_query`.
It orchestrates Dockerized generators to:

- copy OpenAPI (swagger) and Protobuf sources from the private Neptune backend repository
- build a unified OpenAPI document
- generate Python client code from OpenAPI
- generate Python modules from `.proto` definitions
- place artifacts under `src/neptune_query/generated/`

Note: some files under `generated/neptune_api/` are intentionally custom-managed and copied
from `src/neptune_api_codegen/docker/rofiles/neptune_api/` during generation.

See also: `src/neptune_query/generated/README.md` for a map of generated outputs.

## Prerequisites
- Docker installed and running (required to build images and run generators)
- Git installed (used to embed the backend commit reference)
- Access to the Neptune backend repository (cloned locally)

## Quick start
From the `neptune-query` repository root:

```
poetry run python -m neptune_api_codegen.cli
```

By default this performs a dry-run:
- copies swagger/proto inputs from the backend repo into a temporary work dir
- generates OpenAPI, Python client code, and Python protobuf modules there
- removes the temporary dir afterwards

To apply the change to `src/neptune_query/generated/`, run with `--update`:

```
poetry run python -m neptune_api_codegen.cli --update
```

To keep the temporary directory for inspection:

```
poetry run python -m neptune_api_codegen.cli --keep-tmpdir
```

## Temporary working directory
A unique working directory is created under `./tmp/` per run, for example:

```
./tmp/neptune-api__20260114_141229_2195/
├── api_spec/          # swagger + proto copied from backend + GIT_REF
└── generated_python/  # openapi client + protobuf python generated here
```

If `--keep-tmpdir` is not used, the work directory is removed.

## Backend repository detection
If `--neptune-repo-path` is not provided, the tool attempts to auto-detect the Neptune backend repository
on your local filesystem next to this project (../neptune).
