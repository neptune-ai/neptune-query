from typing import (
    Any,
    Dict,
    Type,
    TypeVar,
    Tuple,
    Optional,
    BinaryIO,
    TextIO,
    TYPE_CHECKING,
)

from typing import List


from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

from typing import cast, List
from typing import Dict
from typing import cast

if TYPE_CHECKING:
    from ..models.multipart_part import MultipartPart


T = TypeVar("T", bound="CompleteMultipartUploadRequest")


@_attrs_define
class CompleteMultipartUploadRequest:
    """
    Attributes:
        parts (List['MultipartPart']):
        path (str):
        project_identifier (str):
        upload_id (str):
    """

    parts: List["MultipartPart"]
    path: str
    project_identifier: str
    upload_id: str
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        from ..models.multipart_part import MultipartPart

        parts = []
        for parts_item_data in self.parts:
            parts_item = parts_item_data.to_dict()
            parts.append(parts_item)

        path = self.path

        project_identifier = self.project_identifier

        upload_id = self.upload_id

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "parts": parts,
                "path": path,
                "project_identifier": project_identifier,
                "upload_id": upload_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.multipart_part import MultipartPart

        d = src_dict.copy()
        parts = []
        _parts = d.pop("parts")
        for parts_item_data in _parts:
            parts_item = MultipartPart.from_dict(parts_item_data)

            parts.append(parts_item)

        path = d.pop("path")

        project_identifier = d.pop("project_identifier")

        upload_id = d.pop("upload_id")

        complete_multipart_upload_request = cls(
            parts=parts,
            path=path,
            project_identifier=project_identifier,
            upload_id=upload_id,
        )

        complete_multipart_upload_request.additional_properties = d
        return complete_multipart_upload_request

    @property
    def additional_keys(self) -> List[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
