from collections import Counter
from unittest.mock import (
    ANY,
    call,
    patch,
)

import pytest

import neptune_query as npt
from neptune_query.filters import AttributeFilter
from neptune_query.internal import context
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
    SysName,
)
from neptune_query.internal.retrieval import util
from neptune_query.internal.retrieval.attribute_values import AttributeValue
from neptune_query.internal.retrieval.search import ExperimentSysAttrs


@pytest.mark.parametrize(
    "sys_id_length, exp_count, expected_calls",
    [
        (100, 0, []),
        (100, 1, [1]),
        (100, 10, [10]),
        (100, 1000, [1000]),
        (100, 10000, [3334, 3334, 3332]),
        (10000, 1000, [1000]),
        (10000, 10000, [3334, 3334, 3332]),
        (100, 8800, [4400, 4400]),
        (100, 8801, [2934, 2934, 2933]),
    ],
)
def test_list_attributes_patched(sys_id_length, exp_count, expected_calls):
    #  given
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    sys_ids = [SysId(f"{i:0{sys_id_length}d}") for i in range(exp_count)]

    # when
    with (
        patch("neptune_query.internal.client.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_sys_ids") as fetch_sys_ids,
        patch(
            "neptune_query.internal.retrieval.attribute_definitions.fetch_attribute_definitions_single_filter"
        ) as fetch_attribute_definitions_single_filter,
    ):
        get_client.return_value = None
        fetch_sys_ids.return_value = iter([util.Page(sys_ids)])
        attributes = [AttributeDefinition(name=f"{i:020d}", type="irrelevant") for i in range(100)]
        fetch_attribute_definitions_single_filter.side_effect = lambda **kwargs: iter([util.Page(attributes)])

        npt.list_attributes(
            project=project,
            experiments="ignored",
            attributes=AttributeFilter(name="ignored"),
        )

    # then
    call_sizes = Counter(
        len(fetch_attribute_definitions_single_filter.call_args_list[i].kwargs["run_identifiers"])
        for i in range(fetch_attribute_definitions_single_filter.call_count)
    )
    assert call_sizes == Counter(expected_calls)
    fetch_attribute_definitions_single_filter.assert_has_calls(
        [
            call(
                client=ANY,
                project_identifiers=[project],
                run_identifiers=[
                    RunIdentifier(project_identifier=project, sys_id=sys_id) for sys_id in sys_ids[start:end]
                ],
                attribute_filter=ANY,
                batch_size=ANY,
            )
            for start, end in _edges(expected_calls)
        ],
        any_order=True,
    )


@pytest.mark.parametrize(
    "sys_id_length, exp_count, expected_calls",
    [
        (100, 0, []),
        (100, 1, [1]),
        (100, 10, [10]),
        (100, 1000, [1000]),
        (100, 10000, [3334, 3334, 3332]),
        (10000, 1000, [1000]),
        (10000, 10000, [3334, 3334, 3332]),
        (100, 8800, [4400, 4400]),
        (100, 8801, [2934, 2934, 2933]),
    ],
)
def test_fetch_experiments_table_patched(sys_id_length, exp_count, expected_calls):
    #  given
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    experiments = [
        ExperimentSysAttrs(sys_id=SysId(f"{i:0{sys_id_length}d}"), sys_name=SysName("irrelevant"))
        for i in range(exp_count)
    ]
    attribute_filter = AttributeFilter(name="ignored")
    attribute_filter_internal = attribute_filter._to_internal()

    # when
    with (
        patch("neptune_query.internal.client.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_experiment_sys_attrs") as fetch_experiment_sys_attrs,
        patch("neptune_query.internal.retrieval.attribute_values.fetch_attribute_values") as fetch_attribute_values,
    ):
        get_client.return_value = None
        fetch_experiment_sys_attrs.return_value = iter([util.Page(experiments)])
        fetch_attribute_values.return_value = iter([])

        npt.fetch_experiments_table(
            project=project,
            experiments="ignored",
            attributes=attribute_filter,
        )

    # then
    call_sizes = Counter(
        len(fetch_attribute_values.call_args_list[i].kwargs["run_identifiers"])
        for i in range(fetch_attribute_values.call_count)
    )
    assert call_sizes == Counter(expected_calls)
    fetch_attribute_values.assert_has_calls(
        [
            call(
                client=ANY,
                project_identifier=project,
                run_identifiers=[
                    RunIdentifier(project_identifier=project, sys_id=e.sys_id) for e in experiments[start:end]
                ],
                attribute_definitions=attribute_filter_internal,
                batch_size=ANY,
            )
            for start, end in _edges(expected_calls)
        ],
        any_order=True,
    )


@pytest.mark.parametrize(
    "sys_id_length, exp_count, attr_name_length, attr_count, expected_calls",
    [
        (100, 0, 100, 0, []),
        (100, 1, 100, 0, []),
        (100, 1, 100, 1, [1]),
        (1000, 1, 100, 400, [400]),
        (1000, 1, 1000, 400, [220, 180]),
        (1000, 1, 10000, 400, [22] * 18 + [4]),
        (1000, 1, 1000000, 20, [1] * 20),
        (100, 400, 1000, 1, [220, 180]),
        (1000, 400, 1000, 1, [220, 180]),
        (10000, 400, 1000, 1, [220, 180]),
        (10, 40000, 10, 1, [4000] * 10),
        (1000, 10, 1000, 40, [220, 180]),
        (1000, 20, 1000, 40, [220, 220, 220, 140]),
    ],
)
def test_fetch_series_patched(sys_id_length, exp_count, attr_name_length, attr_count, expected_calls):
    #  given
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    experiments = [
        ExperimentSysAttrs(sys_id=SysId(f"{i:0{sys_id_length}d}"), sys_name=SysName("irrelevant"))
        for i in range(exp_count)
    ]
    attributes = [AttributeDefinition(name=f"{i:0{attr_name_length}d}", type="irrelevant") for i in range(attr_count)]
    run_attribute_definitions = [
        RunAttributeDefinition(
            run_identifier=RunIdentifier(project_identifier=project, sys_id=experiment.sys_id),
            attribute_definition=attribute,
        )
        for experiment in experiments
        for attribute in attributes
    ]

    def generate_fetch_attribute_values(**kwargs):
        run_ids_set = set(kwargs["run_identifiers"])
        filtered = [
            AttributeValue(attribute_definition=r.attribute_definition, run_identifier=r.run_identifier, value="")
            for r in run_attribute_definitions
            if r.run_identifier in run_ids_set
        ]
        return iter([util.Page(filtered)])

    # when
    with (
        patch("neptune_query.internal.composition.fetch_series.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_experiment_sys_attrs") as fetch_experiment_sys_attrs,
        patch("neptune_query.internal.retrieval.attribute_values.fetch_attribute_values") as fetch_attribute_values,
        patch("neptune_query.internal.retrieval.series.fetch_series_values") as fetch_series_values,
    ):
        get_client.return_value = None
        fetch_experiment_sys_attrs.return_value = iter([util.Page(experiments)])
        fetch_attribute_values.side_effect = generate_fetch_attribute_values
        fetch_series_values.return_value = iter([])

        npt.fetch_series(
            project=project,
            experiments="ignored",
            attributes=AttributeFilter(name="ignored"),
        )

    # then
    call_sizes = Counter(
        len(fetch_series_values.call_args_list[i].kwargs["run_attribute_definitions"])
        for i in range(fetch_series_values.call_count)
    )
    assert call_sizes == Counter(expected_calls)
    fetch_series_values.assert_has_calls(
        [
            call(
                run_attribute_definitions=run_attribute_definitions[start:end],
                client=ANY,
                include_inherited=ANY,
                container_type=ANY,
                step_range=ANY,
                tail_limit=ANY,
            )
            for start, end in _edges(expected_calls)
        ],
        any_order=True,
    )


@pytest.mark.parametrize(
    "sys_id_length, exp_count, attr_name_length, attr_count, expected_calls",
    [
        (100, 0, 100, 0, []),
        (100, 1, 100, 0, []),
        (100, 1, 100, 1, [1]),
        (1000, 1, 100, 400, [400]),
        (1000, 1, 1000, 400, [220, 180]),
        (1000, 1, 10000, 400, [22] * 18 + [4]),
        (1000, 1, 1000000, 20, [1] * 20),
        (100, 400, 1000, 1, [220, 180]),
        (1000, 400, 1000, 1, [220, 180]),
        (10000, 400, 1000, 1, [220, 180]),
        (10, 40000, 10, 1, [4000] * 10),
        (1000, 10, 1000, 40, [220, 180]),
        (1000, 20, 1000, 40, [220, 220, 220, 140]),
    ],
)
def test_fetch_metrics_patched(sys_id_length, exp_count, attr_name_length, attr_count, expected_calls):
    #  given
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    experiments = [
        ExperimentSysAttrs(sys_id=SysId(f"{i:0{sys_id_length}d}"), sys_name=SysName("irrelevant"))
        for i in range(exp_count)
    ]
    attributes = [AttributeDefinition(name=f"{i:0{attr_name_length}d}", type="float_series") for i in range(attr_count)]
    run_attribute_definitions = [
        RunAttributeDefinition(
            run_identifier=RunIdentifier(project_identifier=project, sys_id=experiment.sys_id),
            attribute_definition=attribute,
        )
        for experiment in experiments
        for attribute in attributes
    ]

    def generate_fetch_attribute_values(**kwargs):
        run_ids_set = set(kwargs["run_identifiers"])
        filtered = [
            AttributeValue(attribute_definition=r.attribute_definition, run_identifier=r.run_identifier, value="")
            for r in run_attribute_definitions
            if r.run_identifier in run_ids_set
        ]
        return iter([util.Page(filtered)])

    # when
    with (
        patch("neptune_query.internal.composition.fetch_metrics.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_experiment_sys_attrs") as fetch_experiment_sys_attrs,
        patch("neptune_query.internal.retrieval.attribute_values.fetch_attribute_values") as fetch_attribute_values,
        patch(
            "neptune_query.internal.composition.fetch_metrics.fetch_multiple_series_values"
        ) as fetch_multiple_series_values,
    ):
        get_client.return_value = None
        fetch_experiment_sys_attrs.return_value = iter([util.Page(experiments)])
        fetch_attribute_values.side_effect = generate_fetch_attribute_values
        fetch_multiple_series_values.return_value = {}

        npt.fetch_metrics(
            project=project,
            experiments="ignored",
            attributes=AttributeFilter(name="ignored"),
        )

    # then
    call_sizes = Counter(
        len(fetch_multiple_series_values.call_args_list[i].kwargs["run_attribute_definitions"])
        for i in range(fetch_multiple_series_values.call_count)
    )
    assert call_sizes == Counter(expected_calls)
    fetch_multiple_series_values.assert_has_calls(
        [
            call(
                client=ANY,
                run_attribute_definitions=run_attribute_definitions[start:end],
                include_inherited=ANY,
                container_type=ANY,
                include_preview=ANY,
                step_range=ANY,
                tail_limit=ANY,
            )
            for start, end in _edges(expected_calls)
        ],
        any_order=True,
    )


def _edges(sizes):
    start = 0
    for size in sizes:
        end = start + size
        yield start, end
        start = end
