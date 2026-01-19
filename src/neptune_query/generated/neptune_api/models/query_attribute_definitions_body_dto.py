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
    cast,
)

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import (
    UNSET,
    Unset,
)

if TYPE_CHECKING:
    from ..models.attribute_filter_dto import AttributeFilterDTO
    from ..models.attribute_name_filter_dto import AttributeNameFilterDTO
    from ..models.next_page_dto import NextPageDTO


T = TypeVar("T", bound="QueryAttributeDefinitionsBodyDTO")


@_attrs_define
class QueryAttributeDefinitionsBodyDTO:
    """
    Attributes:
        attribute_filter (list[AttributeFilterDTO] | Unset): Filter by attribute (match any), if null no type or
            property value filtering is applied
        attribute_name_filter (AttributeNameFilterDTO | Unset):
        attribute_name_regex (str | Unset): Filter by attribute name with regex, if null no name filtering is applied;
            deprecated, use attributeNameFilter instead; if attributeNameFilter is set, this field is ignored
        experiment_ids_filter (list[str] | Unset): Filter by experiment id, if null all experiments are considered
        next_page (NextPageDTO | Unset):
        project_identifiers (list[str] | Unset): Project identifiers to filter by
    """

    attribute_filter: list[AttributeFilterDTO] | Unset = UNSET
    attribute_name_filter: AttributeNameFilterDTO | Unset = UNSET
    attribute_name_regex: str | Unset = UNSET
    experiment_ids_filter: list[str] | Unset = UNSET
    next_page: NextPageDTO | Unset = UNSET
    project_identifiers: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        attribute_filter: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.attribute_filter, Unset):
            attribute_filter = []
            for attribute_filter_item_data in self.attribute_filter:
                attribute_filter_item = attribute_filter_item_data.to_dict()
                attribute_filter.append(attribute_filter_item)

        attribute_name_filter: dict[str, Any] | Unset = UNSET
        if not isinstance(self.attribute_name_filter, Unset):
            attribute_name_filter = self.attribute_name_filter.to_dict()

        attribute_name_regex = self.attribute_name_regex

        experiment_ids_filter: list[str] | Unset = UNSET
        if not isinstance(self.experiment_ids_filter, Unset):
            experiment_ids_filter = self.experiment_ids_filter

        next_page: dict[str, Any] | Unset = UNSET
        if not isinstance(self.next_page, Unset):
            next_page = self.next_page.to_dict()

        project_identifiers: list[str] | Unset = UNSET
        if not isinstance(self.project_identifiers, Unset):
            project_identifiers = self.project_identifiers

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if attribute_filter is not UNSET:
            field_dict["attributeFilter"] = attribute_filter
        if attribute_name_filter is not UNSET:
            field_dict["attributeNameFilter"] = attribute_name_filter
        if attribute_name_regex is not UNSET:
            field_dict["attributeNameRegex"] = attribute_name_regex
        if experiment_ids_filter is not UNSET:
            field_dict["experimentIdsFilter"] = experiment_ids_filter
        if next_page is not UNSET:
            field_dict["nextPage"] = next_page
        if project_identifiers is not UNSET:
            field_dict["projectIdentifiers"] = project_identifiers

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.attribute_filter_dto import AttributeFilterDTO
        from ..models.attribute_name_filter_dto import AttributeNameFilterDTO
        from ..models.next_page_dto import NextPageDTO

        d = dict(src_dict)
        attribute_filter: list[AttributeFilterDTO] | Unset = UNSET
        _attribute_filter = d.pop("attributeFilter", UNSET)
        if not isinstance(_attribute_filter, Unset):
            attribute_filter = []
            for attribute_filter_item_data in _attribute_filter:
                attribute_filter_item = AttributeFilterDTO.from_dict(attribute_filter_item_data)

                attribute_filter.append(attribute_filter_item)

        _attribute_name_filter = d.pop("attributeNameFilter", UNSET)
        attribute_name_filter: AttributeNameFilterDTO | Unset
        if isinstance(_attribute_name_filter, Unset):
            attribute_name_filter = UNSET
        else:
            attribute_name_filter = AttributeNameFilterDTO.from_dict(_attribute_name_filter)

        attribute_name_regex = d.pop("attributeNameRegex", UNSET)

        experiment_ids_filter = cast(list[str], d.pop("experimentIdsFilter", UNSET))

        _next_page = d.pop("nextPage", UNSET)
        next_page: NextPageDTO | Unset
        if isinstance(_next_page, Unset):
            next_page = UNSET
        else:
            next_page = NextPageDTO.from_dict(_next_page)

        project_identifiers = cast(list[str], d.pop("projectIdentifiers", UNSET))

        query_attribute_definitions_body_dto = cls(
            attribute_filter=attribute_filter,
            attribute_name_filter=attribute_name_filter,
            attribute_name_regex=attribute_name_regex,
            experiment_ids_filter=experiment_ids_filter,
            next_page=next_page,
            project_identifiers=project_identifiers,
        )

        query_attribute_definitions_body_dto.additional_properties = d
        return query_attribute_definitions_body_dto

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
