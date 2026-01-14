from io import BytesIO
from unittest import mock

from neptune_query.generated.neptune_api.proto.neptune_pb.api.v1.model.requests_pb2 import (
    ProtoCustomExpression,
    ProtoGetTimeseriesBucketsRequest,
    ProtoScale,
    ProtoView,
)
from neptune_query.generated.neptune_api.types import File
from neptune_query.internal.retrieval.util import body_from_protobuf


@mock.patch("neptune_query.internal.retrieval.util.BytesIO", wraps=BytesIO)
def test_bytesio_recreation_on_retry(mock_bytesio):
    """Test that BytesIO instance is recreated when the API call is retried."""

    file = body_from_protobuf(
        ProtoGetTimeseriesBucketsRequest(
            expressions=[ProtoCustomExpression(requestId="0123", customYFormula="${abc}")],
            view=ProtoView(xScale=ProtoScale.linear),
        )
    )

    # Verify file is neptune_api.types.File as expected
    assert isinstance(file, File)

    # Read file's payload two times to simulate two API calls:
    read1 = file.payload.read()
    read2 = file.payload.read()

    # Verify that both reads return the same content
    # If the same BytesIO instance was used, the second read would return b''
    assert read1 == read2
    assert read2 != b""

    # Verify BytesIO was called three times:
    # Once during ProtobufPayload creation and twice for the two reads
    assert mock_bytesio.call_count == 3
