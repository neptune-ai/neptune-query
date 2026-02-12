from collections import Counter
from unittest.mock import (
    ANY,
    call,
    patch,
)

import pytest

import neptune_query as npt
import neptune_query.runs as nq_runs
from neptune_query.filters import AttributeFilter
from neptune_query.internal import context
from neptune_query.internal.env import NEPTUNE_QUERY_ENTRIES_SEARCH_MAX_PROJECTION_ATTRIBUTES
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    CustomRunId,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
    SysName,
)
from neptune_query.internal.retrieval import util
from neptune_query.internal.retrieval.search import (
    ContainerType,
    ExperimentSysAttrs,
    RunSysAttrs,
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
    "sys_id_length, exp_count, attr_name_length, attr_count, expected_calls",
    [
        (100, 0, 100, 0, []),
        (100, 1, 100, 0, []),
        (100, 1, 100, 1, [(1, [1])]),
        (1000, 1, 100, 400, [(1, [400])]),
        (1000, 1, 1000, 400, [(1, [219, 181])]),
        (1000, 1, 10000, 400, [(1, [21] * 19 + [1])]),
        (1000, 1, 1000000, 20, [(1, [1] * 20)]),
        (1000, 2, 1000, 400, [(2, [219, 181])]),
        (1000, 20, 1000, 400, [(20, [219, 181])]),
        (1000, 21, 1000, 400, [(20, [219, 181]), (1, [219, 181])]),
        (1000, 42, 1000, 400, [(20, [219, 181])] * 2 + [(2, [219, 181])]),
        (1000, 10, 100, 4000, [(2, [2199, 1801])] * 5),
        (1000, 10, 200, 4000, [(4, [1099, 1099, 1099, 703])] * 2 + [(2, [1099, 1099, 1099, 703])]),
        (100, 400, 1000, 1, [(400, [1])]),
        (10000, 400, 1000, 1, [(400, [1])]),
        (10, 40000, 10, 1, [(4000, [1])] * 10),
    ],
)
def test_fetch_experiments_table_patched(sys_id_length, exp_count, attr_name_length, attr_count, expected_calls):
    #  given
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    experiments = [
        ExperimentSysAttrs(sys_id=SysId(f"{i:0{sys_id_length}d}"), sys_name=SysName("irrelevant"))
        for i in range(exp_count)
    ]
    attributes = [AttributeDefinition(name=f"{i:0{attr_name_length}d}", type="irrelevant") for i in range(attr_count)]

    expected_runs_attributes = []
    experiment_start = 0
    for experiment_size, attribute_sizes in expected_calls:
        attribute_start = 0
        for attribute_size in attribute_sizes:
            run_identifiers = [
                RunIdentifier(project_identifier=project, sys_id=experiment.sys_id)
                for experiment in experiments[experiment_start : experiment_start + experiment_size]
            ]
            attribute_definitions = attributes[attribute_start : attribute_start + attribute_size]
            expected_runs_attributes.append((run_identifiers, attribute_definitions))
            attribute_start += attribute_size
        experiment_start += experiment_size
    expected_runs_attributes_sizes = [
        (len(run_identifiers), len(attribute_definitions))
        for run_identifiers, attribute_definitions in expected_runs_attributes
    ]

    # when
    with (
        patch("neptune_query.internal.client.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_experiment_sys_attrs") as fetch_experiment_sys_attrs,
        patch(
            "neptune_query.internal.retrieval.attribute_definitions.fetch_attribute_definitions_single_filter"
        ) as fetch_attribute_definitions_single_filter,
        patch("neptune_query.internal.retrieval.attribute_values.fetch_attribute_values") as fetch_attribute_values,
    ):
        get_client.return_value = None
        fetch_experiment_sys_attrs.return_value = iter([util.Page(experiments)])
        fetch_attribute_definitions_single_filter.side_effect = lambda **kwargs: iter([util.Page(attributes)])
        fetch_attribute_values.return_value = iter([])

        npt.fetch_experiments_table(
            project=project,
            experiments="ignored",
            attributes=AttributeFilter(name="ignored"),
        )

    # then
    call_sizes = Counter(
        (
            len(fetch_attribute_values.call_args_list[i].kwargs["run_identifiers"]),
            len(fetch_attribute_values.call_args_list[i].kwargs["attribute_definitions"]),
        )
        for i in range(fetch_attribute_values.call_count)
    )
    assert call_sizes == Counter(expected_runs_attributes_sizes)
    fetch_attribute_values.assert_has_calls(
        [
            call(
                client=ANY,
                project_identifier=project,
                run_identifiers=expected_run_identifiers,
                attribute_definitions=expected_run_attribute_definitions,
            )
            for expected_run_identifiers, expected_run_attribute_definitions in expected_runs_attributes
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

    # when
    with (
        patch("neptune_query.internal.composition.fetch_series.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_experiment_sys_attrs") as fetch_experiment_sys_attrs,
        patch(
            "neptune_query.internal.retrieval.attribute_definitions.fetch_attribute_definitions_single_filter"
        ) as fetch_attribute_definitions_single_filter,
        patch("neptune_query.internal.retrieval.series.fetch_series_values") as fetch_series_values,
    ):
        get_client.return_value = None
        fetch_experiment_sys_attrs.return_value = iter([util.Page(experiments)])
        fetch_attribute_definitions_single_filter.side_effect = lambda **kwargs: iter([util.Page(attributes)])
        fetch_series_values.return_value = {}

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
    exp_attributes = [
        RunAttributeDefinition(
            run_identifier=RunIdentifier(project_identifier=project, sys_id=experiment.sys_id),
            attribute_definition=attribute,
        )
        for experiment in experiments
        for attribute in attributes
    ]

    # when
    with (
        patch("neptune_query.internal.composition.fetch_metrics._client"),
        patch("neptune_query.internal.retrieval.search.fetch_experiment_sys_attrs") as fetch_experiment_sys_attrs,
        patch(
            "neptune_query.internal.retrieval.attribute_definitions.fetch_attribute_definitions_single_filter"
        ) as fetch_attribute_definitions_single_filter,
        patch(
            "neptune_query.internal.composition.fetch_metrics.fetch_multiple_series_values"
        ) as fetch_multiple_series_values,
    ):
        fetch_experiment_sys_attrs.return_value = iter([util.Page(experiments)])
        fetch_attribute_definitions_single_filter.side_effect = lambda **kwargs: iter([util.Page(attributes)])
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
                run_attribute_definitions=exp_attributes[start:end],
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


def test_fetch_metrics_uses_fast_path_for_exact_attribute_list():
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    experiments = [ExperimentSysAttrs(sys_id=SysId("sysid0"), sys_name=SysName("irrelevant"))]

    with (
        patch("neptune_query.internal.composition.fetch_metrics._client"),
        patch("neptune_query.internal.retrieval.search.fetch_experiment_sys_attrs") as fetch_experiment_sys_attrs,
        patch("neptune_query.internal.composition.fetch_metrics.fetch_attribute_definitions_split") as fetch_defs_split,
        patch("neptune_query.internal.composition.fetch_metrics.fetch_multiple_series_values") as fetch_series_values,
    ):
        fetch_experiment_sys_attrs.side_effect = lambda **kwargs: iter([util.Page(experiments)])
        fetch_series_values.return_value = {}

        df = npt.fetch_metrics(
            project=project,
            experiments="ignored",
            attributes=["metric/a", "metric/a", "metric/b"],
        )

    assert df.empty
    fetch_defs_split.assert_not_called()
    fetch_series_values.assert_called_once()
    assert fetch_series_values.call_args.kwargs["run_attribute_definitions"] == [
        RunAttributeDefinition(
            run_identifier=RunIdentifier(project_identifier=project, sys_id=experiments[0].sys_id),
            attribute_definition=AttributeDefinition(name="metric/a", type="float_series"),
        ),
        RunAttributeDefinition(
            run_identifier=RunIdentifier(project_identifier=project, sys_id=experiments[0].sys_id),
            attribute_definition=AttributeDefinition(name="metric/b", type="float_series"),
        ),
    ]


def test_fetch_runs_metrics_uses_fast_path_for_exact_attribute_list():
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")

    with (
        patch("neptune_query.internal.composition.fetch_metrics._client"),
        patch("neptune_query.internal.retrieval.search.fetch_run_sys_attrs") as fetch_run_sys_attrs,
        patch("neptune_query.internal.composition.fetch_metrics.fetch_attribute_definitions_split") as fetch_defs_split,
        patch("neptune_query.internal.composition.fetch_metrics.fetch_multiple_series_values") as fetch_series_values,
    ):
        fetch_series_values.return_value = {}

        df = nq_runs.fetch_metrics(
            project=project,
            runs=["run-0"],
            attributes=["metric/a"],
        )

    assert df.empty
    fetch_run_sys_attrs.assert_not_called()
    fetch_defs_split.assert_not_called()
    fetch_series_values.assert_called_once()
    assert fetch_series_values.call_args.kwargs["run_identifier_mode"] == "custom_run_id"
    assert fetch_series_values.call_args.kwargs["run_attribute_definitions"] == [
        RunAttributeDefinition(
            run_identifier=RunIdentifier(project_identifier=project, sys_id=SysId("run-0")),
            attribute_definition=AttributeDefinition(name="metric/a", type="float_series"),
        )
    ]


def test_fetch_runs_metrics_with_non_exact_runs_uses_sys_id_based_path():
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    runs = [RunSysAttrs(sys_id=SysId("sysid0"), sys_custom_run_id=CustomRunId("run-0"))]

    with (
        patch("neptune_query.internal.composition.fetch_metrics._client"),
        patch("neptune_query.internal.retrieval.search.fetch_run_sys_attrs") as fetch_run_sys_attrs,
        patch("neptune_query.internal.composition.fetch_metrics.fetch_attribute_definitions_split") as fetch_defs_split,
        patch("neptune_query.internal.composition.fetch_metrics.fetch_multiple_series_values") as fetch_series_values,
    ):
        fetch_run_sys_attrs.side_effect = lambda **kwargs: iter([util.Page(runs)])
        fetch_series_values.return_value = {}

        df = nq_runs.fetch_metrics(
            project=project,
            runs="ignored",
            attributes=["metric/a"],
        )

    assert df.empty
    fetch_run_sys_attrs.assert_called()
    fetch_defs_split.assert_not_called()
    fetch_series_values.assert_called_once()
    assert fetch_series_values.call_args.kwargs.get("run_identifier_mode", "sys_id") == "sys_id"
    assert fetch_series_values.call_args.kwargs["run_attribute_definitions"] == [
        RunAttributeDefinition(
            run_identifier=RunIdentifier(project_identifier=project, sys_id=runs[0].sys_id),
            attribute_definition=AttributeDefinition(name="metric/a", type="float_series"),
        )
    ]


def test_fetch_metrics_fast_path_failure_falls_back_to_existing_path():
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    experiments = [ExperimentSysAttrs(sys_id=SysId("sysid0"), sys_name=SysName("irrelevant"))]
    attributes = [AttributeDefinition(name="metric/a", type="float_series")]

    with (
        patch("neptune_query.internal.composition.fetch_metrics._client"),
        patch("neptune_query.internal.retrieval.search.fetch_experiment_sys_attrs") as fetch_experiment_sys_attrs,
        patch(
            "neptune_query.internal.retrieval.attribute_definitions.fetch_attribute_definitions_single_filter"
        ) as fetch_attribute_definitions_single_filter,
        patch(
            "neptune_query.internal.composition.fetch_metrics.fetch_multiple_series_values"
        ) as fetch_multiple_series_values,
    ):
        fetch_experiment_sys_attrs.side_effect = lambda **kwargs: iter([util.Page(experiments)])
        fetch_attribute_definitions_single_filter.side_effect = lambda **kwargs: iter([util.Page(attributes)])
        fetch_multiple_series_values.side_effect = [RuntimeError("fast path failed"), {}]

        df = npt.fetch_metrics(
            project=project,
            experiments="ignored",
            attributes=["metric/a"],
        )

    assert df.empty
    assert fetch_multiple_series_values.call_count == 2
    fetch_attribute_definitions_single_filter.assert_called()


def test_fetch_metrics_attribute_filter_exact_names_uses_existing_path():
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    experiments = [ExperimentSysAttrs(sys_id=SysId("sysid0"), sys_name=SysName("irrelevant"))]
    attributes = [AttributeDefinition(name="metric/a", type="float_series")]

    with (
        patch("neptune_query.internal.composition.fetch_metrics._client"),
        patch("neptune_query.internal.retrieval.search.fetch_experiment_sys_attrs") as fetch_experiment_sys_attrs,
        patch(
            "neptune_query.internal.retrieval.attribute_definitions.fetch_attribute_definitions_single_filter"
        ) as fetch_attribute_definitions_single_filter,
        patch(
            "neptune_query.internal.composition.fetch_metrics.fetch_multiple_series_values"
        ) as fetch_multiple_series_values,
    ):
        fetch_experiment_sys_attrs.side_effect = lambda **kwargs: iter([util.Page(experiments)])
        fetch_attribute_definitions_single_filter.side_effect = lambda **kwargs: iter([util.Page(attributes)])
        fetch_multiple_series_values.return_value = {}

        df = npt.fetch_metrics(
            project=project,
            experiments="ignored",
            attributes=AttributeFilter(name=["metric/a"]),
        )

    assert df.empty
    fetch_attribute_definitions_single_filter.assert_called()


def test_fetch_experiments_table_uses_entries_search_fast_path_for_exact_attribute_list():
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")

    with (
        patch("neptune_query.internal.client.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_table_rows_exact_attributes") as fetch_fast_path,
        patch("neptune_query.internal.retrieval.search.fetch_sys_id_labels") as fetch_sys_id_labels,
    ):
        get_client.return_value = None
        fetch_fast_path.return_value = iter([])

        df = npt.fetch_experiments_table(
            project=project,
            attributes=["config/a", "config/b"],
        )

    assert df.empty
    fetch_fast_path.assert_called_once()
    assert fetch_fast_path.call_args.kwargs["requested_attribute_names"] == {"config/a", "config/b"}
    fetch_sys_id_labels.assert_not_called()


def test_fetch_runs_table_uses_entries_search_fast_path_for_exact_attribute_list():
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")

    with (
        patch("neptune_query.internal.client.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_table_rows_exact_attributes") as fetch_fast_path,
        patch("neptune_query.internal.retrieval.search.fetch_sys_id_labels") as fetch_sys_id_labels,
    ):
        get_client.return_value = None
        fetch_fast_path.return_value = iter([])

        df = nq_runs.fetch_runs_table(
            project=project,
            attributes=["config/a", "config/b"],
        )

    assert df.empty
    fetch_fast_path.assert_called_once()
    assert fetch_fast_path.call_args.kwargs["requested_attribute_names"] == {"config/a", "config/b"}
    fetch_sys_id_labels.assert_not_called()


def test_fetch_experiments_table_large_attribute_list_uses_existing_path(monkeypatch):
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    monkeypatch.setenv(NEPTUNE_QUERY_ENTRIES_SEARCH_MAX_PROJECTION_ATTRIBUTES.name, "1")

    with (
        patch("neptune_query.internal.client.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_table_rows_exact_attributes") as fetch_fast_path,
        patch("neptune_query.internal.retrieval.search.fetch_sys_id_labels") as fetch_sys_id_labels,
    ):
        get_client.return_value = None
        fetch_fast_path.return_value = iter([])
        fetch_sys_id_labels.return_value = lambda **kwargs: iter([])

        df = npt.fetch_experiments_table(
            project=project,
            attributes=["config/a", "config/b"],
        )

    assert df.empty
    fetch_fast_path.assert_not_called()
    fetch_sys_id_labels.assert_called_once_with(ContainerType.EXPERIMENT)


def test_fetch_experiments_table_uses_existing_path_when_required_sys_attrs_exceed_projection_limit(monkeypatch):
    project = ProjectIdentifier("project")
    context.set_api_token("irrelevant")
    # Fast path decision is based on user-requested attributes count.
    monkeypatch.setenv(NEPTUNE_QUERY_ENTRIES_SEARCH_MAX_PROJECTION_ATTRIBUTES.name, "3")

    with (
        patch("neptune_query.internal.client.get_client") as get_client,
        patch("neptune_query.internal.retrieval.search.fetch_table_rows_exact_attributes") as fetch_fast_path,
        patch("neptune_query.internal.retrieval.search.fetch_sys_id_labels") as fetch_sys_id_labels,
    ):
        get_client.return_value = None
        fetch_fast_path.return_value = iter([])
        fetch_sys_id_labels.return_value = lambda **kwargs: iter([])

        df = npt.fetch_experiments_table(
            project=project,
            attributes=["config/a", "config/b"],
        )

    assert df.empty
    fetch_fast_path.assert_called_once()
    fetch_sys_id_labels.assert_not_called()


def _edges(sizes):
    start = 0
    for size in sizes:
        end = start + size
        yield start, end
        start = end
