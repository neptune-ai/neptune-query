This directory contains mostly generated files.

`src/neptune_query/generated/neptune_api/` also includes a small set of custom-managed files
that are copied during codegen from `src/neptune_api_codegen/docker/rofiles/neptune_api/`:

- `client.py`
- `auth_helpers.py`
- `credentials.py`
- `types.py`

These files should be edited in `rofiles` and then propagated by running codegen.

```
src/
└── neptune_query/
    └── generated/
        ├── neptune_api_spec/
        │   ├─ GIT_REF               Git reference (commit hash and date) from which the API specification was copied
        │   ├─ proto/                Files COPIED from Neptune backend Git repo
        │   ├─ swagger/              Files COPIED from Neptune backend Git repo
        │   └─ neptune-openapi.json  OpenAPI specification file GENERATED from files in neptune_api_spec/swagger
        │
        └── neptune_api/             Python code GENERATED from neptune_api_spec/neptune-openapi.json
            ├── client.py            Custom-managed, COPIED from codegen rofiles
            ├── auth_helpers.py      Custom-managed, COPIED from codegen rofiles
            ├── credentials.py       Custom-managed, COPIED from codegen rofiles
            ├── types.py             Custom-managed, COPIED from codegen rofiles
            └── proto                Python code GENERATED from files in neptune_api_spec/proto/
```

To regenerate these files, run the following from neptune-query repo root:

    poetry run python -m neptune_api_codegen.cli
