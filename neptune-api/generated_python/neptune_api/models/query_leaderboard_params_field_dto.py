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

from ..models.attribute_type_dto import AttributeTypeDTO
from ..models.query_leaderboard_params_field_dto_aggregation_mode import (
    QueryLeaderboardParamsFieldDTOAggregationMode,
)
from ..types import UNSET, Unset
from typing import Union


T = TypeVar("T", bound="QueryLeaderboardParamsFieldDTO")


@_attrs_define
class QueryLeaderboardParamsFieldDTO:
    """
    Attributes:
        name (str):
        type (AttributeTypeDTO):
        aggregation_mode (Union[Unset, QueryLeaderboardParamsFieldDTOAggregationMode]):
    """

    name: str
    type: AttributeTypeDTO
    aggregation_mode: Union[Unset, QueryLeaderboardParamsFieldDTOAggregationMode] = (
        UNSET
    )
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        name = self.name

        type = self.type.value

        aggregation_mode: Union[Unset, str] = UNSET
        if not isinstance(self.aggregation_mode, Unset):
            aggregation_mode = self.aggregation_mode.value

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "type": type,
            }
        )
        if aggregation_mode is not UNSET:
            field_dict["aggregationMode"] = aggregation_mode

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        name = d.pop("name")

        type = AttributeTypeDTO(d.pop("type"))

        _aggregation_mode = d.pop("aggregationMode", UNSET)
        aggregation_mode: Union[Unset, QueryLeaderboardParamsFieldDTOAggregationMode]
        if isinstance(_aggregation_mode, Unset):
            aggregation_mode = UNSET
        else:
            aggregation_mode = QueryLeaderboardParamsFieldDTOAggregationMode(
                _aggregation_mode
            )

        query_leaderboard_params_field_dto = cls(
            name=name,
            type=type,
            aggregation_mode=aggregation_mode,
        )

        query_leaderboard_params_field_dto.additional_properties = d
        return query_leaderboard_params_field_dto

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
