#!/usr/bin/env bash
#
# This script merges files in the swagger directory into a single OpenAPI spec,
# then generates Python code from it using openapi-python-client.
#
# It's assumed to be running from Docker, see Dockerfile.openapi.

set -ex

output_dir=$1

if [ -z "$output_dir" ]; then
  echo "Usage: $0 <output_dir>"
  exit 1
fi

tmpdir=/tmp/work
rm -fr "$tmpdir"
mkdir "$tmpdir"


cp /rofiles/redocly.yaml "$tmpdir/"

# Converts Swagger 2.0 to OpenAPI 3.1 in two steps:
# - Swagger 2.0 → OpenAPI 3.0.1 (via swagger-converter)
# - OpenAPI 3.0.1 → OpenAPI 3.1 (via openapi-format)
convert_swagger_to_openapi() {
    local file_path=$1
    local tmp_file="$tmpdir/to-openapi.$RANDOM.json"

    swagger2openapi --resolveInternal -p -o "${tmp_file}" "${file_path}"

    openapi-format "${tmp_file}" -o "${file_path}" --convertTo "3.1"
}

# Updates a Swagger 2.0 spec to:
# - Keep only application/x-protobuf if both JSON and protobuf are listed in "produces"
# - Set application/json as the default if no content type is defined
# - Keep only application/x-protobuf if both JSON and protobuf are listed in "consumes"
#
# This avoids invalid JSON parsing in generated clients and ensures correctly
# returning protobuf as binary data.
fix_content_types() {
  local file_path=$1
  local tmp_file="$tmpdir/protobuf-fix.$RANDOM.json"

  jq '
  .paths |= with_entries(
    .value |= with_entries(
      if .value.produces? and (.value.produces | index("application/json")) and (.value.produces | index("application/x-protobuf"))
      then
        .value.produces = ["application/x-protobuf"]
      else
        .
      end
    )
  )
  |
  .paths |= with_entries(
    .value |= with_entries(
      if (.value.produces | not)
      then .value.produces = ["application/json"]
      else .
      end
    )
  )
  |
  .paths |= with_entries(
    .value |= with_entries(
      if .value.consumes? and (.value.consumes | index("application/json")) and (.value.consumes | index("application/x-protobuf"))
      then
        .value.consumes = ["application/x-protobuf"]
      else
        .
      end
    )
  )
' "$file_path" > "$tmp_file" && cp "$tmp_file" "$file_path"
}

# Fixes OpenAPI 3.1 specs to ensure that any response or requestBody
# with content-type application/x-protobuf has schema type string and format binary.
# This ensures that the generated client code correctly handles protobuf as binary data.
fix_binary_format() {
  local file_path=$1
  local tmp_file="$tmpdir/format-fix.$RANDOM.json"

  jq '
  .paths |= with_entries(
    .value |= with_entries(
      if .value.requestBody? and (.value.requestBody.content | has("application/x-protobuf"))
      then
        .value.requestBody.content = {
          "application/x-protobuf": {
            "schema": {
              "type": "string",
              "format": "binary"
            }
          }
        }
      else
        .
      end
    )
  )
  |
  .paths |= with_entries(
    .value |= with_entries(
      if .value.responses? and (.value.responses."200".content | has("application/x-protobuf"))
      then
        .value.responses."200".content = {
          "application/x-protobuf": {
            "schema": {
              "type": "string",
              "format": "binary"
            }
          }
        }
      else
        .
      end
    )
  )
' "$file_path" > "$tmp_file" && cp "$tmp_file" "$file_path"
}

# OpenAPI requires a license object, so add an empty one.
add_empty_license() {
  local file_path=$1
  local tmp_file="$tmpdir/add-license.$RANDOM.json"

  jq '.info.license = {"name": "", "url": ""}' "$file_path" > "$tmp_file" \
    && cp "$tmp_file" "$file_path"
}

# Filter endpoints using redocly to keep only the ones that are defined in redocly.yml
filter_endpoints() {
  local file_path=$1
  local api=$2
  local tmp_file="$tmpdir/filter-endpoints-$api.$RANDOM.json"

  redocly --config "$tmpdir/redocly.yaml" bundle "$api" -o "$tmp_file" --remove-unused-components
  cp "$tmp_file" "$file_path"
}

# Sets the given tag on all endpoints to control submodule placement in openapi-python-client.
# Example: tag "storage" → src/neptune_api/api/storage/ → import as neptune_api.api.storage
set_tag_on_all_endpoints() {
  local file_path=$1
  local tag=$2
  local tmp_file="$tmpdir/set-tag.$RANDOM.json"

  jq --arg tag "$tag" '
  .paths |= with_entries(
    .value |= with_entries(
      if .key | test("^(get|post|put|delete|patch|options|head)$")
      then .value.tags = [$tag] | .
      else .
      end
    )
  )
  ' "$file_path" > "$tmp_file" && cp "$tmp_file" "$file_path"
}

# Prepares a Swagger input file for the final client spec:
# - Applies response content-type fix
# - Converts to OpenAPI 3.1
# - Filters endpoints based on redocly.yml
# - Sets the specified <api> tag on all endpoints
# - Adds an empty license field for OpenAPI 3.1 compliance
#
# Output is written to $tmpdir/<api>.json and must match the path in redocly.yml
pre_process_file() {
  local input_path=$1
  local api=$2
  local convert_to_openapi=${3:-true}

  local file_path="$tmpdir/$api.json"
  cp "$input_path" "$file_path"

  fix_content_types "$file_path"

  if [ "$convert_to_openapi" = true ]; then
    convert_swagger_to_openapi "$file_path"
  fi

  fix_binary_format "$file_path"

  filter_endpoints "$file_path" "$api"
  set_tag_on_all_endpoints "$file_path" "$api"
  add_empty_license "$file_path"
}

# Prepare all the API specs for bundling into the final client spec.
pre_process_file ./api_spec/swagger/retrieval.json "retrieval"
pre_process_file ./api_spec/swagger/backend.json "backend"
pre_process_file ./api_spec/swagger/storagebridge_openapi.json "storage" false
pre_process_file ./api_spec/swagger/ingestion_openapi.json "ingestion" false

# Join all the API specs into a single one
redocly join --config "$tmpdir/redocly.yaml" -o "$tmpdir/neptune.json" \
  "$tmpdir/retrieval.json" \
  "$tmpdir/backend.json" \
  "$tmpdir/storage.json" \
  "$tmpdir/ingestion.json"

# Add empty license to the final file
add_empty_license "$tmpdir/neptune.json"

# Save the final OpenAPI spec to apispec/ for reference
cp "$tmpdir/neptune.json" ./api_spec/neptune-openapi.json

# Generate python code from the OpenAPI spec via uv-managed tool
uv tool run openapi-python-client generate \
    --overwrite \
    --meta none \
    --path "$tmpdir/neptune.json" \
    --custom-template-path=/rofiles/templates/ \
    --config /rofiles/openapi-generator-config.yaml \
    --output-path "$tmpdir/neptune_api"


# Ensure the target directory exists
mkdir -p "$output_dir/neptune_api/"

# Replace the generated code in the source directory
rm -fr "$output_dir"/neptune_api/api
mv "$tmpdir/neptune_api/api/" "$output_dir/neptune_api/"

# We intentionally do not move generated client.py/types.py from $tmpdir.
# Instead we copy custom-managed versions from /rofiles/neptune_api below.
for file in __init__.py errors.py py.typed; do
  mv "$tmpdir/neptune_api/$file" "$output_dir/neptune_api/"
done

# Copy our custom versions of some files instead of the generated ones
for file in client.py auth_helpers.py credentials.py types.py; do
  cp /rofiles/neptune_api/$file "$output_dir/neptune_api/"
done

rm -fr "$output_dir"/neptune_api/models
mv "$tmpdir/neptune_api/models" "$output_dir/neptune_api/"

rm -fr "$tmpdir"
