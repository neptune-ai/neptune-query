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


T = TypeVar("T", bound="MultipartUpload")


@_attrs_define
class MultipartUpload:
    """
    Attributes:
        part_size (int):
        part_urls (List[str]):
        upload_id (str):
    """

    part_size: int
    part_urls: List[str]
    upload_id: str
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        part_size = self.part_size

        part_urls = self.part_urls

        upload_id = self.upload_id

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "part_size": part_size,
                "part_urls": part_urls,
                "upload_id": upload_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        part_size = d.pop("part_size")

        part_urls = cast(List[str], d.pop("part_urls"))

        upload_id = d.pop("upload_id")

        multipart_upload = cls(
            part_size=part_size,
            part_urls=part_urls,
            upload_id=upload_id,
        )

        multipart_upload.additional_properties = d
        return multipart_upload

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
