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
from ..types import UNSET, Unset
from typing import Union


T = TypeVar("T", bound="AttributeQueryDTO")


@_attrs_define
class AttributeQueryDTO:
    """
    Attributes:
        attribute_paths_filter (Union[Unset, List[str]]): Filter attribute paths, if null api returns all attributes
    """

    attribute_paths_filter: Union[Unset, List[str]] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        attribute_paths_filter: Union[Unset, List[str]] = UNSET
        if not isinstance(self.attribute_paths_filter, Unset):
            attribute_paths_filter = self.attribute_paths_filter

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if attribute_paths_filter is not UNSET:
            field_dict["attributePathsFilter"] = attribute_paths_filter

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        attribute_paths_filter = cast(List[str], d.pop("attributePathsFilter", UNSET))

        attribute_query_dto = cls(
            attribute_paths_filter=attribute_paths_filter,
        )

        attribute_query_dto.additional_properties = d
        return attribute_query_dto

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
