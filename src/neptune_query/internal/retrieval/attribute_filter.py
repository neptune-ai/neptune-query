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
import itertools as it
import re
from typing import (
    Any,
    Iterable,
    Optional,
    Union,
)

from .. import filters  # noqa: E402
from ..retrieval import attribute_types as types  # noqa: E402


def split_attribute_filters(
    _attribute_filter: filters._BaseAttributeFilter,
) -> list[filters._AttributeFilter]:
    if isinstance(_attribute_filter, filters._AttributeFilter):
        return [_attribute_filter]
    elif isinstance(_attribute_filter, filters._AttributeFilterAlternative):
        return list(it.chain.from_iterable(split_attribute_filters(child) for child in _attribute_filter.filters))
    else:
        raise RuntimeError(f"Unexpected filter type: {type(_attribute_filter)}")


def transform_attribute_filter_into_params(
    attribute_filter: filters._AttributeFilter,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "attributeNameFilter": {},
    }

    name_regexes = None
    if attribute_filter.name_eq is not None:
        name_regexes = _escape_name_eq(_variants_to_list(attribute_filter.name_eq))

    if attribute_filter.must_match_any is not None:
        attribute_name_filter_dtos = []
        for alternative in attribute_filter.must_match_any:
            attribute_name_filter_dto = {}
            must_match_regexes = _union_options([name_regexes, alternative.must_match_regexes])
            if must_match_regexes is not None:
                attribute_name_filter_dto["mustMatchRegexes"] = must_match_regexes
            if alternative.must_not_match_regexes is not None:
                attribute_name_filter_dto["mustNotMatchRegexes"] = alternative.must_not_match_regexes
            if attribute_name_filter_dto:
                attribute_name_filter_dtos.append(attribute_name_filter_dto)
        params["attributeNameFilter"]["mustMatchAny"] = attribute_name_filter_dtos

    elif name_regexes is not None:
        params["attributeNameFilter"]["mustMatchAny"] = [{"mustMatchRegexes": name_regexes}]

    attribute_types = _variants_to_list(attribute_filter.type_in)
    if attribute_types is not None:
        params["attributeFilter"] = [
            {"attributeType": types.map_attribute_type_python_to_backend(_type)} for _type in attribute_types
        ]

    # note: attribute_filter.aggregations is intentionally ignored

    return params


def _escape_name_eq(names: Optional[list[str]]) -> Optional[list[str]]:
    if names is None:
        return None

    escaped = [f"{re.escape(name)}" for name in names]

    if len(escaped) == 1:
        return [f"^{escaped[0]}$"]
    else:
        joined = "|".join(escaped)
        return [f"^({joined})$"]


def _variants_to_list(param: Union[str, Iterable[str], None]) -> Optional[list[str]]:
    if param is None:
        return None
    if isinstance(param, str):
        return [param]
    return list(param)


def _union_options(options: list[Optional[list[str]]]) -> Optional[list[str]]:
    result = None

    for option in options:
        if option is not None:
            if result is None:
                result = []
            result.extend(option)

    return result
