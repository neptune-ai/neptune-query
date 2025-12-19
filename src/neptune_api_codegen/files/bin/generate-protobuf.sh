#!/usr/bin/env bash
# Executed in docker container to generate Python proto code
# See Dockerfile.proto*.

set -euxo pipefail

OUTPUT_DIR="${1:-}"

if [ -z "${OUTPUT_DIR}" ]; then
    echo "Usage: $0 <output_dir>"
    exit 1
fi

mkdir -p "${OUTPUT_DIR}"

python -m grpc_tools.protoc \
  --python_out="${OUTPUT_DIR}" \
  --mypy_out="${OUTPUT_DIR}" \
  --proto_path="./apispec/proto/" \
  "./apispec/proto/google_rpc/"*".proto" \
  "./apispec/proto/neptune_pb/ingest/v1/"*".proto" \
  "./apispec/proto/neptune_pb/ingest/v1/pub/"*".proto" \
  "./apispec/proto/neptune_pb/api/v1/model/"*".proto"

# Note that we're passing the protoc-path argument to protol, which makes sure
# it's using the same protoc as above, instead of a system-wide installation.
protol --create-package --in-place \
  --python-out "${OUTPUT_DIR}" \
  protoc \
  --protoc-path="python -m grpc_tools.protoc" \
  --proto-path "./apispec/proto" \
  "./apispec/proto/google_rpc/"*".proto" \
  "./apispec/proto/neptune_pb/ingest/v1/"*".proto" \
  "./apispec/proto/neptune_pb/ingest/v1/pub/"*".proto" \
  "./apispec/proto/neptune_pb/api/v1/model/"*".proto"
