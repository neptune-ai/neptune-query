from unittest.mock import patch

from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.retrieval import metrics
from neptune_query.internal.retrieval.search import ContainerType


def _run_attribute(project: str, run_id: str) -> RunAttributeDefinition:
    return RunAttributeDefinition(
        run_identifier=RunIdentifier(
            project_identifier=ProjectIdentifier(project),
            sys_id=SysId(run_id),
        ),
        attribute_definition=AttributeDefinition(name="metric/a", type="float_series"),
    )


def test_fetch_multiple_series_values_uses_custom_holder_identifier():
    run_attribute = _run_attribute(project="my-org/my-project", run_id="my-run-id")
    captured_params = {}

    def fake_fetch_pages(**kwargs):
        captured_params["initial_params"] = kwargs["initial_params"]
        return iter([])

    with patch("neptune_query.internal.retrieval.metrics.util.fetch_pages", side_effect=fake_fetch_pages):
        result = metrics.fetch_multiple_series_values(
            client=None,
            run_attribute_definitions=[run_attribute],
            include_inherited=False,
            container_type=ContainerType.RUN,
            include_preview=False,
            run_identifier_mode="custom_run_id",
        )

    assert result == {run_attribute: []}
    request = captured_params["initial_params"]["requests"][0]
    assert request["series"]["holder"]["identifier"] == "CUSTOM/my-org/my-project/my-run-id"
    assert request["series"]["holder"]["type"] == "experiment"


def test_fetch_multiple_series_values_default_identifier_uses_project_and_sys_id():
    run_attribute = _run_attribute(project="my-org/my-project", run_id="sysid-123")
    captured_params = {}

    def fake_fetch_pages(**kwargs):
        captured_params["initial_params"] = kwargs["initial_params"]
        return iter([])

    with patch("neptune_query.internal.retrieval.metrics.util.fetch_pages", side_effect=fake_fetch_pages):
        result = metrics.fetch_multiple_series_values(
            client=None,
            run_attribute_definitions=[run_attribute],
            include_inherited=False,
            container_type=ContainerType.RUN,
            include_preview=False,
        )

    assert result == {run_attribute: []}
    request = captured_params["initial_params"]["requests"][0]
    assert request["series"]["holder"]["identifier"] == "my-org/my-project/sysid-123"
