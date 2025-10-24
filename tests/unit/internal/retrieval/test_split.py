import pytest

from neptune_query.internal import identifiers
from neptune_query.internal.env import (
    NEPTUNE_QUERY_ATTRIBUTE_VALUES_BATCH_SIZE,
    NEPTUNE_QUERY_MAX_ATTRIBUTE_FILTER_SIZE,
    NEPTUNE_QUERY_MAX_REQUEST_SIZE,
    NEPTUNE_QUERY_SERIES_BATCH_SIZE,
)
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
)
from neptune_query.internal.retrieval.split import (
    split_attribute_names,
    split_series_attributes,
    split_sys_ids,
    split_sys_ids_attributes,
)

SYS_IDS = [identifiers.SysId(f"XXX-{n}") for n in range(10)]
SYS_ID = SYS_IDS[0]
RUN_ID = RunIdentifier(ProjectIdentifier("test_project"), SYS_ID)
ATTRIBUTE_DEFINITIONS = [AttributeDefinition(f"config/attribute{n}", "string") for n in range(10)]
ATTRIBUTE_DEFINITION = ATTRIBUTE_DEFINITIONS[0]
ATTRIBUTE_NAMES = [attr.name for attr in ATTRIBUTE_DEFINITIONS]
ATTRIBUTE_NAME = ATTRIBUTE_NAMES[0]
ATTRIBUTE_DEFINITION_SIZES = [len(name) for name in ATTRIBUTE_NAMES]
ATTRIBUTE_DEFINITION_SIZE = ATTRIBUTE_DEFINITION_SIZES[0]

UUID_SIZE = 50


@pytest.mark.parametrize(
    "sys_ids, expected",
    [
        ([], []),
        ([SYS_ID], [[SYS_ID]]),
        (
            [SYS_IDS[0], SYS_IDS[1]],
            [[SYS_IDS[0], SYS_IDS[1]]],
        ),
    ],
)
def test_split_sys_ids(sys_ids, expected):
    # when
    groups = list(split_sys_ids(sys_ids))

    # then
    assert groups == expected


@pytest.mark.parametrize(
    "given_num, query_size_limit, expected_nums",
    [
        (0, 0, []),
        (1, 0, [1]),
        (2, 0, [1, 1]),
        (3, UUID_SIZE * 2, [2, 1]),
        (4, UUID_SIZE * 2, [2, 2]),
        (5, UUID_SIZE * 2, [2, 2, 1]),
        (9, UUID_SIZE * 3 - 1, [2, 2, 2, 2, 1]),
        (9, UUID_SIZE * 3, [3, 3, 3]),
        (9, UUID_SIZE * 3 + 1, [3, 3, 3]),
        (9, UUID_SIZE * 4, [3, 3, 3]),  # 4 -> split into 3 batches -> 3, 3, 3 is more balanced than 4, 4, 1
        (8, UUID_SIZE * 4, [4, 4]),
        (8, UUID_SIZE * 3, [3, 3, 2]),
        (7, UUID_SIZE * 3, [3, 3, 1]),
        (6, UUID_SIZE * 3, [3, 3]),
        (5, UUID_SIZE * 3, [3, 2]),
    ],
)
def test_split_sys_ids_custom_envs(monkeypatch, given_num, query_size_limit, expected_nums):
    # given
    monkeypatch.setenv(NEPTUNE_QUERY_MAX_REQUEST_SIZE.name, str(query_size_limit))
    sys_ids = [SYS_ID] * given_num
    expected = [[SYS_ID] * num for num in expected_nums]

    # when
    groups = list(split_sys_ids(sys_ids))

    # then
    assert groups == expected


@pytest.mark.parametrize(
    "attribute_names, expected",
    [
        ([], []),
        ([ATTRIBUTE_NAME], [[ATTRIBUTE_NAME]]),
        (
            [ATTRIBUTE_NAMES[0], ATTRIBUTE_NAMES[1]],
            [[ATTRIBUTE_NAMES[0], ATTRIBUTE_NAMES[1]]],
        ),
    ],
)
def test_split_attribute_names(attribute_names, expected):
    # when
    groups = list(split_attribute_names(attribute_names))

    # then
    assert groups == expected


@pytest.mark.parametrize(
    "given_num, filter_size_limit, expected_nums",
    [
        (0, 0, []),
        (1, 0, [1]),
        (2, 0, [1, 1]),
        (3, ATTRIBUTE_DEFINITION_SIZE * 2, [2, 1]),
        (4, ATTRIBUTE_DEFINITION_SIZE * 2, [2, 2]),
        (5, ATTRIBUTE_DEFINITION_SIZE * 2, [2, 2, 1]),
        (9, ATTRIBUTE_DEFINITION_SIZE * 3 - 1, [2, 2, 2, 2, 1]),
        (9, ATTRIBUTE_DEFINITION_SIZE * 3, [3, 3, 3]),
        (9, ATTRIBUTE_DEFINITION_SIZE * 3 + 1, [3, 3, 3]),
        (9, ATTRIBUTE_DEFINITION_SIZE * 4, [4, 4, 1]),
        (8, ATTRIBUTE_DEFINITION_SIZE * 4, [4, 4]),
        (8, ATTRIBUTE_DEFINITION_SIZE * 3, [3, 3, 2]),
        (7, ATTRIBUTE_DEFINITION_SIZE * 3, [3, 3, 1]),
        (6, ATTRIBUTE_DEFINITION_SIZE * 3, [3, 3]),
        (5, ATTRIBUTE_DEFINITION_SIZE * 3, [3, 2]),
    ],
)
def test_split_attribute_names_custom_envs(monkeypatch, given_num, filter_size_limit, expected_nums):
    # given
    monkeypatch.setenv(NEPTUNE_QUERY_MAX_ATTRIBUTE_FILTER_SIZE.name, str(filter_size_limit))
    sys_ids = [ATTRIBUTE_NAME] * given_num
    expected = [[ATTRIBUTE_NAME] * num for num in expected_nums]

    # when
    groups = list(split_attribute_names(sys_ids))

    # then
    assert groups == expected


@pytest.mark.parametrize(
    "sys_ids, attributes, expected",
    [
        ([], [], []),
        ([SYS_ID], [], []),
        ([], [ATTRIBUTE_DEFINITION], []),
        ([SYS_ID], [ATTRIBUTE_DEFINITION], [([SYS_ID], [ATTRIBUTE_DEFINITION])]),
        ([SYS_ID] * 2, [ATTRIBUTE_DEFINITION], [([SYS_ID] * 2, [ATTRIBUTE_DEFINITION])]),
        ([SYS_ID], [ATTRIBUTE_DEFINITION] * 2, [([SYS_ID], [ATTRIBUTE_DEFINITION] * 2)]),
        (
            [SYS_ID] * 2,
            [ATTRIBUTE_DEFINITION] * 2,
            [([SYS_ID] * 2, [ATTRIBUTE_DEFINITION] * 2)],
        ),
    ],
)
def test_split_sys_ids_attributes(sys_ids, attributes, expected):
    # when
    groups = list(split_sys_ids_attributes(sys_ids=sys_ids, attribute_definitions=attributes))

    # then
    assert groups == expected


@pytest.mark.parametrize(
    "sys_id_num, attribute_num, query_size_limit, values_batch_size, expected_nums",
    [
        (1, 1, 500, 500, [(1, 1)]),
        (1, 1, 1, 500, [(1, 1)]),
        (1, 1, 500, 1, [(1, 1)]),
        (1, 1, 1, 1, [(1, 1)]),
        (2, 3, 500, 500, [(2, 3)]),
        (2, 3, 500, 1, [(1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)]),
        (2, 3, 1, 500, [(1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)]),
        (2, 3, 1, 1, [(1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)]),
        (2, 3, 500, 2, [(1, 2), (1, 1), (1, 2), (1, 1)]),
        (2, 3, 500, 3, [(1, 3), (1, 3)]),
        (2, 3, 500, 4, [(1, 3), (1, 3)]),
        (2, 3, 500, 5, [(1, 3), (1, 3)]),
        (2, 3, 500, 6, [(2, 3)]),
        (3, 3, 500, 6, [(2, 3), (1, 3)]),
        (2, 3, UUID_SIZE + 2 * ATTRIBUTE_DEFINITION_SIZE, 500, [(1, 2), (1, 1), (1, 2), (1, 1)]),
        (2, 3, UUID_SIZE + 3 * ATTRIBUTE_DEFINITION_SIZE - 1, 500, [(1, 2), (1, 1), (1, 2), (1, 1)]),
        (2, 3, UUID_SIZE + 3 * ATTRIBUTE_DEFINITION_SIZE, 500, [(1, 3), (1, 3)]),
        (2, 3, 2 * UUID_SIZE + 3 * ATTRIBUTE_DEFINITION_SIZE - 1, 500, [(1, 3), (1, 3)]),
        (2, 3, 2 * UUID_SIZE + 3 * ATTRIBUTE_DEFINITION_SIZE, 500, [(2, 3)]),
    ],
)
def test_split_sys_ids_attributes_custom_envs(
    monkeypatch, sys_id_num, attribute_num, query_size_limit, values_batch_size, expected_nums
):
    # given
    monkeypatch.setenv(NEPTUNE_QUERY_MAX_REQUEST_SIZE.name, str(query_size_limit))
    monkeypatch.setenv(NEPTUNE_QUERY_ATTRIBUTE_VALUES_BATCH_SIZE.name, str(values_batch_size))
    sys_ids = [SYS_ID] * sys_id_num
    attributes = [ATTRIBUTE_DEFINITION] * attribute_num
    expected = [([SYS_ID] * a, [ATTRIBUTE_DEFINITION] * b) for a, b in expected_nums]

    # when
    groups = list(split_sys_ids_attributes(sys_ids=sys_ids, attribute_definitions=attributes))

    # then
    assert groups == expected


@pytest.mark.parametrize(
    "attributes, expected",
    [
        ([], []),
        ([ATTRIBUTE_DEFINITION], [[ATTRIBUTE_DEFINITION]]),
        ([ATTRIBUTE_DEFINITION] * 2, [[ATTRIBUTE_DEFINITION] * 2]),
        ([ATTRIBUTE_DEFINITION] * 9, [[ATTRIBUTE_DEFINITION] * 5, [ATTRIBUTE_DEFINITION] * 4]),
        ([ATTRIBUTE_DEFINITION] * 27, [[ATTRIBUTE_DEFINITION] * 9] * 3),
        ([ATTRIBUTE_DEFINITION] * 64, [[ATTRIBUTE_DEFINITION] * 16] * 4),
        ([ATTRIBUTE_DEFINITION] * 729, [[ATTRIBUTE_DEFINITION] * 81] * 9),
        ([ATTRIBUTE_DEFINITION] * 4096, [[ATTRIBUTE_DEFINITION] * 256] * 16),
        ([ATTRIBUTE_DEFINITION] * 32768, [[ATTRIBUTE_DEFINITION] * 1024] * 32),
        (
            [ATTRIBUTE_DEFINITION] * 35937,
            [[ATTRIBUTE_DEFINITION] * 1124] * 31 + [[ATTRIBUTE_DEFINITION] * 1093],
        ),  # MAX_WORKERS=32 takes over
    ],
)
def test_split_series_attributes(attributes, expected):
    # given
    run_attributes = _add_run(attributes)
    expected = [_add_run(group) for group in expected]

    # when
    groups = list(split_series_attributes(run_attributes))

    # then
    assert groups == expected


@pytest.mark.parametrize(
    "given_num, query_size_limit, batch_size, expected_nums",
    [
        (0, 500, 500, []),
        (1, 500, 500, [1]),
        (2, 500, 500, [2]),
        (2, 1, 500, [1, 1]),
        (2, ATTRIBUTE_DEFINITION_SIZE, 500, [1, 1]),
        (2, 2 * ATTRIBUTE_DEFINITION_SIZE, 500, [2]),
        (2, 2 * ATTRIBUTE_DEFINITION_SIZE - 1, 500, [1, 1]),
        (2, 500, 1, [1, 1]),
        (3, 500, 1, [1, 1, 1]),
        (3, 500, 2, [2, 1]),
        (3, 500, 3, [3]),
        (3, 500, 4, [3]),
        (3, ATTRIBUTE_DEFINITION_SIZE, 500, [1, 1, 1]),
        (3, 2 * ATTRIBUTE_DEFINITION_SIZE, 500, [2, 1]),
        (3, 3 * ATTRIBUTE_DEFINITION_SIZE, 500, [3]),
        (3, 4 * ATTRIBUTE_DEFINITION_SIZE, 500, [3]),
        (10, 10 * ATTRIBUTE_DEFINITION_SIZE, 3, [3, 3, 3, 1]),
        (64, 64 * ATTRIBUTE_DEFINITION_SIZE, 17, [16] * 4),
        (64, 64 * ATTRIBUTE_DEFINITION_SIZE, 16, [16] * 4),
        (64, 64 * ATTRIBUTE_DEFINITION_SIZE, 15, [15, 15, 15, 15, 4]),
    ],
)
def test_split_series_attributes_custom_envs(monkeypatch, given_num, query_size_limit, batch_size, expected_nums):
    # given
    monkeypatch.setenv(NEPTUNE_QUERY_MAX_REQUEST_SIZE.name, str(query_size_limit))
    monkeypatch.setenv(NEPTUNE_QUERY_SERIES_BATCH_SIZE.name, str(batch_size))
    run_attributes = _add_run([ATTRIBUTE_DEFINITION] * given_num)
    expected = [_add_run([ATTRIBUTE_DEFINITION] * num) for num in expected_nums]

    # when
    groups = list(split_series_attributes(run_attributes))

    # then
    assert groups == expected


def _add_run(attribute_definitions):
    return [RunAttributeDefinition(RUN_ID, attr) for attr in attribute_definitions]
