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

from ..types import UNSET, Unset
from typing import Union


T = TypeVar("T", bound="ClientVersionsConfigDTO")


@_attrs_define
class ClientVersionsConfigDTO:
    """
    Attributes:
        min_recommended_version (Union[Unset, str]):
        min_compatible_version (Union[Unset, str]):
        max_compatible_version (Union[Unset, str]):
    """

    min_recommended_version: Union[Unset, str] = UNSET
    min_compatible_version: Union[Unset, str] = UNSET
    max_compatible_version: Union[Unset, str] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        min_recommended_version = self.min_recommended_version

        min_compatible_version = self.min_compatible_version

        max_compatible_version = self.max_compatible_version

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if min_recommended_version is not UNSET:
            field_dict["minRecommendedVersion"] = min_recommended_version
        if min_compatible_version is not UNSET:
            field_dict["minCompatibleVersion"] = min_compatible_version
        if max_compatible_version is not UNSET:
            field_dict["maxCompatibleVersion"] = max_compatible_version

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        min_recommended_version = d.pop("minRecommendedVersion", UNSET)

        min_compatible_version = d.pop("minCompatibleVersion", UNSET)

        max_compatible_version = d.pop("maxCompatibleVersion", UNSET)

        client_versions_config_dto = cls(
            min_recommended_version=min_recommended_version,
            min_compatible_version=min_compatible_version,
            max_compatible_version=max_compatible_version,
        )

        client_versions_config_dto.additional_properties = d
        return client_versions_config_dto

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
