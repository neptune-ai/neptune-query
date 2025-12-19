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

from typing import Dict
from typing import Union
from ..types import UNSET, Unset
from ..models.query_leaderboard_params_sorting_params_dto_dir import (
    QueryLeaderboardParamsSortingParamsDTODir,
)
from typing import cast

if TYPE_CHECKING:
    from ..models.query_leaderboard_params_field_dto import (
        QueryLeaderboardParamsFieldDTO,
    )


T = TypeVar("T", bound="QueryLeaderboardParamsSortingParamsDTO")


@_attrs_define
class QueryLeaderboardParamsSortingParamsDTO:
    """
    Attributes:
        sort_by (QueryLeaderboardParamsFieldDTO):
        dir_ (Union[Unset, QueryLeaderboardParamsSortingParamsDTODir]):
    """

    sort_by: "QueryLeaderboardParamsFieldDTO"
    dir_: Union[Unset, QueryLeaderboardParamsSortingParamsDTODir] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        from ..models.query_leaderboard_params_field_dto import (
            QueryLeaderboardParamsFieldDTO,
        )

        sort_by = self.sort_by.to_dict()

        dir_: Union[Unset, str] = UNSET
        if not isinstance(self.dir_, Unset):
            dir_ = self.dir_.value

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "sortBy": sort_by,
            }
        )
        if dir_ is not UNSET:
            field_dict["dir"] = dir_

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.query_leaderboard_params_field_dto import (
            QueryLeaderboardParamsFieldDTO,
        )

        d = src_dict.copy()
        sort_by = QueryLeaderboardParamsFieldDTO.from_dict(d.pop("sortBy"))

        _dir_ = d.pop("dir", UNSET)
        dir_: Union[Unset, QueryLeaderboardParamsSortingParamsDTODir]
        if isinstance(_dir_, Unset):
            dir_ = UNSET
        else:
            dir_ = QueryLeaderboardParamsSortingParamsDTODir(_dir_)

        query_leaderboard_params_sorting_params_dto = cls(
            sort_by=sort_by,
            dir_=dir_,
        )

        query_leaderboard_params_sorting_params_dto.additional_properties = d
        return query_leaderboard_params_sorting_params_dto

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
