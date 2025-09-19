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
import asyncio
from typing import Optional

from .. import client as _client
from .. import context as _context
from ..composition import (
    concurrency,
    type_inference,
)
from ..filters import _Filter
from ..identifiers import ProjectIdentifier
from ..retrieval import search

__all__ = ("list_containers",)


def list_containers(
    *,
    project_identifier: ProjectIdentifier,
    filter_: Optional[_Filter],
    context: Optional[_context.Context] = None,
    container_type: search.ContainerType,
) -> list[str]:
    validated_context = _context.validate_context(context or _context.get_context())
    client = _client.get_client(context=validated_context)

    inference_result = type_inference.infer_attribute_types_in_filter(
        client=client,
        project_identifier=project_identifier,
        filter_=filter_,
    )
    filter_ = inference_result.get_result_or_raise()
    inference_result.emit_warnings()

    output = search.fetch_sys_id_labels(container_type)(client, project_identifier, filter_)

    sys_attr_pages = asyncio.run(concurrency.gather_results(output))
    return sorted(attrs.label for page in sys_attr_pages for attrs in page.items)
