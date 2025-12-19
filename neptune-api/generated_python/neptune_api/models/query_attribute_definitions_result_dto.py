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
    from ..models.attribute_definition_dto import AttributeDefinitionDTO
    from ..models.next_page_dto import NextPageDTO


T = TypeVar("T", bound="QueryAttributeDefinitionsResultDTO")


@_attrs_define
class QueryAttributeDefinitionsResultDTO:
    """
    Attributes:
        entries (List['AttributeDefinitionDTO']):
        next_page (NextPageDTO):
    """

    entries: List["AttributeDefinitionDTO"]
    next_page: "NextPageDTO"
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        from ..models.attribute_definition_dto import AttributeDefinitionDTO
        from ..models.next_page_dto import NextPageDTO

        entries = []
        for entries_item_data in self.entries:
            entries_item = entries_item_data.to_dict()
            entries.append(entries_item)

        next_page = self.next_page.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "entries": entries,
                "nextPage": next_page,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.attribute_definition_dto import AttributeDefinitionDTO
        from ..models.next_page_dto import NextPageDTO

        d = src_dict.copy()
        entries = []
        _entries = d.pop("entries")
        for entries_item_data in _entries:
            entries_item = AttributeDefinitionDTO.from_dict(entries_item_data)

            entries.append(entries_item)

        next_page = NextPageDTO.from_dict(d.pop("nextPage"))

        query_attribute_definitions_result_dto = cls(
            entries=entries,
            next_page=next_page,
        )

        query_attribute_definitions_result_dto.additional_properties = d
        return query_attribute_definitions_result_dto

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
