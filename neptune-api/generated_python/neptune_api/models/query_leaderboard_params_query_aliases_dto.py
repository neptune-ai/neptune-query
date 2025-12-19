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
from typing import Union
from ..types import UNSET, Unset
from typing import cast

if TYPE_CHECKING:
    from ..models.query_leaderboard_params_name_alias_dto import (
        QueryLeaderboardParamsNameAliasDTO,
    )


T = TypeVar("T", bound="QueryLeaderboardParamsQueryAliasesDTO")


@_attrs_define
class QueryLeaderboardParamsQueryAliasesDTO:
    """
    Attributes:
        experiment_aliases (Union[Unset, List['QueryLeaderboardParamsNameAliasDTO']]):
        run_aliases (Union[Unset, List['QueryLeaderboardParamsNameAliasDTO']]):
    """

    experiment_aliases: Union[Unset, List["QueryLeaderboardParamsNameAliasDTO"]] = UNSET
    run_aliases: Union[Unset, List["QueryLeaderboardParamsNameAliasDTO"]] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        from ..models.query_leaderboard_params_name_alias_dto import (
            QueryLeaderboardParamsNameAliasDTO,
        )

        experiment_aliases: Union[Unset, List[Dict[str, Any]]] = UNSET
        if not isinstance(self.experiment_aliases, Unset):
            experiment_aliases = []
            for experiment_aliases_item_data in self.experiment_aliases:
                experiment_aliases_item = experiment_aliases_item_data.to_dict()
                experiment_aliases.append(experiment_aliases_item)

        run_aliases: Union[Unset, List[Dict[str, Any]]] = UNSET
        if not isinstance(self.run_aliases, Unset):
            run_aliases = []
            for run_aliases_item_data in self.run_aliases:
                run_aliases_item = run_aliases_item_data.to_dict()
                run_aliases.append(run_aliases_item)

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if experiment_aliases is not UNSET:
            field_dict["experimentAliases"] = experiment_aliases
        if run_aliases is not UNSET:
            field_dict["runAliases"] = run_aliases

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.query_leaderboard_params_name_alias_dto import (
            QueryLeaderboardParamsNameAliasDTO,
        )

        d = src_dict.copy()
        experiment_aliases: Union[Unset, List["QueryLeaderboardParamsNameAliasDTO"]] = (
            UNSET
        )
        _experiment_aliases = d.pop("experimentAliases", UNSET)
        if not isinstance(_experiment_aliases, Unset):
            experiment_aliases = []
            for experiment_aliases_item_data in _experiment_aliases:
                experiment_aliases_item = QueryLeaderboardParamsNameAliasDTO.from_dict(
                    experiment_aliases_item_data
                )

                experiment_aliases.append(experiment_aliases_item)

        run_aliases: Union[Unset, List["QueryLeaderboardParamsNameAliasDTO"]] = UNSET
        _run_aliases = d.pop("runAliases", UNSET)
        if not isinstance(_run_aliases, Unset):
            run_aliases = []
            for run_aliases_item_data in _run_aliases:
                run_aliases_item = QueryLeaderboardParamsNameAliasDTO.from_dict(
                    run_aliases_item_data
                )

                run_aliases.append(run_aliases_item)

        query_leaderboard_params_query_aliases_dto = cls(
            experiment_aliases=experiment_aliases,
            run_aliases=run_aliases,
        )

        query_leaderboard_params_query_aliases_dto.additional_properties = d
        return query_leaderboard_params_query_aliases_dto

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
