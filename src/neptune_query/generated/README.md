This directory contains files that are generated and cannot be edited by hand.

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
            └── proto                Python code GENERATED from files in neptune_api_spec/proto/
```

To regenerate these files, run the following from neptune-query repo root:

    poetry run python -m neptune_api_codegen.cli
