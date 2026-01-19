#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from collections.abc import Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
)

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import (
    UNSET,
    Unset,
)

if TYPE_CHECKING:
    from ..models.nql_query_params_dto import NqlQueryParamsDTO
    from ..models.query_leaderboard_params_pagination_dto import QueryLeaderboardParamsPaginationDTO
    from ..models.query_leaderboard_params_sorting_params_dto import QueryLeaderboardParamsSortingParamsDTO


T = TypeVar("T", bound="GlobalSearchParamsDTO")


@_attrs_define
class GlobalSearchParamsDTO:
    """
    Attributes:
        experiment_leader (bool | Unset):
        pagination (QueryLeaderboardParamsPaginationDTO | Unset):
        query (NqlQueryParamsDTO | Unset):
        sorting (QueryLeaderboardParamsSortingParamsDTO | Unset):
    """

    experiment_leader: bool | Unset = UNSET
    pagination: QueryLeaderboardParamsPaginationDTO | Unset = UNSET
    query: NqlQueryParamsDTO | Unset = UNSET
    sorting: QueryLeaderboardParamsSortingParamsDTO | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        experiment_leader = self.experiment_leader

        pagination: dict[str, Any] | Unset = UNSET
        if not isinstance(self.pagination, Unset):
            pagination = self.pagination.to_dict()

        query: dict[str, Any] | Unset = UNSET
        if not isinstance(self.query, Unset):
            query = self.query.to_dict()

        sorting: dict[str, Any] | Unset = UNSET
        if not isinstance(self.sorting, Unset):
            sorting = self.sorting.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if experiment_leader is not UNSET:
            field_dict["experimentLeader"] = experiment_leader
        if pagination is not UNSET:
            field_dict["pagination"] = pagination
        if query is not UNSET:
            field_dict["query"] = query
        if sorting is not UNSET:
            field_dict["sorting"] = sorting

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.nql_query_params_dto import NqlQueryParamsDTO
        from ..models.query_leaderboard_params_pagination_dto import QueryLeaderboardParamsPaginationDTO
        from ..models.query_leaderboard_params_sorting_params_dto import QueryLeaderboardParamsSortingParamsDTO

        d = dict(src_dict)
        experiment_leader = d.pop("experimentLeader", UNSET)

        _pagination = d.pop("pagination", UNSET)
        pagination: QueryLeaderboardParamsPaginationDTO | Unset
        if isinstance(_pagination, Unset):
            pagination = UNSET
        else:
            pagination = QueryLeaderboardParamsPaginationDTO.from_dict(_pagination)

        _query = d.pop("query", UNSET)
        query: NqlQueryParamsDTO | Unset
        if isinstance(_query, Unset):
            query = UNSET
        else:
            query = NqlQueryParamsDTO.from_dict(_query)

        _sorting = d.pop("sorting", UNSET)
        sorting: QueryLeaderboardParamsSortingParamsDTO | Unset
        if isinstance(_sorting, Unset):
            sorting = UNSET
        else:
            sorting = QueryLeaderboardParamsSortingParamsDTO.from_dict(_sorting)

        global_search_params_dto = cls(
            experiment_leader=experiment_leader,
            pagination=pagination,
            query=query,
            sorting=sorting,
        )

        global_search_params_dto.additional_properties = d
        return global_search_params_dto

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
