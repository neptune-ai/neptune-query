import pytest

from neptune_query.internal.filters import (
    _Attribute,
    _Filter,
)
from neptune_query.internal.identifiers import SysId
from neptune_query.internal.retrieval import search


@pytest.fixture(scope="module", autouse=True)
def run_with_attributes_autouse(run_with_attributes):
    pass


def get_sys_id_for_run(client, project_identifier, run_id) -> SysId | None:
    sys_ids = []
    for page in search.fetch_run_sys_ids(
        client=client,
        project_identifier=project_identifier,
        filter_=_Filter.eq(_Attribute("sys/custom_run_id", type="string"), run_id),
    ):
        for item in page.items:
            sys_ids.append(item)
    if len(sys_ids) == 1:
        return SysId(sys_ids[0])
    if len(sys_ids) == 0:
        return None

    raise RuntimeError(f"Expected exactly one sys_id for run_id {run_id}, got {sys_ids}")
