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


T = TypeVar("T", bound="QueryLeaderboardParamsNameAliasDTO")


@_attrs_define
class QueryLeaderboardParamsNameAliasDTO:
    """
    Attributes:
        name_alias (str):
        original_id (str):
    """

    name_alias: str
    original_id: str
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        name_alias = self.name_alias

        original_id = self.original_id

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "nameAlias": name_alias,
                "originalId": original_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        name_alias = d.pop("nameAlias")

        original_id = d.pop("originalId")

        query_leaderboard_params_name_alias_dto = cls(
            name_alias=name_alias,
            original_id=original_id,
        )

        query_leaderboard_params_name_alias_dto.additional_properties = d
        return query_leaderboard_params_name_alias_dto

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
