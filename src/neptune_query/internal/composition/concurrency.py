#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

import asyncio
import contextlib
import threading
from asyncio import Task
from collections.abc import Awaitable
from typing import (
    Any,
    Callable,
    Generator,
    Optional,
    ParamSpec,
    Type,
    TypeVar, AsyncGenerator, Tuple, Iterable, Iterator,
)

T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")


async def take_head(generator: AsyncGenerator[T]) -> Optional[Tuple[T, AsyncGenerator[T]]]:
    try:
        head = await anext(generator)
        return head, generator
    except StopAsyncIteration:
        return None


async def merge_async_generators(generators: Iterable[AsyncGenerator[T]]) -> AsyncGenerator[T]:
    tasks: set[Task[Optional[Tuple[T, AsyncGenerator[T]]]]] = {
        asyncio.create_task(take_head(gen)) for gen in generators
    }

    while tasks:
        tasks_done, tasks_pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        tasks_new = set()
        for task in tasks_done:
            result = await task
            if result is not None:
                item, generator = result
                new_task = asyncio.create_task(take_head(generator))
                tasks_new.add(new_task)
                yield item

        tasks = tasks_pending | tasks_new


async def map_async_generator(
    items: AsyncGenerator[T],
    downstream: Callable[[T], R],
) -> AsyncGenerator[R]:
    async for item in items:
        yield downstream(item)


async def flat_map_async_generator(
    items: AsyncGenerator[T],
    downstream: Callable[[T], AsyncGenerator[R]],
) -> AsyncGenerator[R]:
    try:
        head = await anext(items)
    except StopAsyncIteration:
        return

    generators = [
        downstream(head),
        flat_map_async_generator(items, downstream),
    ]

    async for item in merge_async_generators(generators):
        yield item


async def flat_map_sync(
    items: Iterator[T],
    downstream: Callable[[T], AsyncGenerator[R]],
) -> AsyncGenerator[R]:
    try:
        head = next(items)
    except StopIteration:
        return

    generators = [
        downstream(head),
        flat_map_sync(items, downstream),
    ]

    async for item in merge_async_generators(generators):
        yield item


async def return_value_async(item: Awaitable[R]) -> AsyncGenerator[R]:
    result = await item
    yield result


async def return_value(item: R) -> AsyncGenerator[R]:
    yield item


async def gather_results(output: AsyncGenerator[R]) -> list[R]:
    results = []
    async for item in output:
        results.append(item)
    return results


_thread_local_storage = threading.local()


THREAD_LOCAL_PREFIX = "neptune_query_"


@contextlib.contextmanager
def use_thread_local(values: dict[str, Any]) -> Generator[None, None, None]:
    for key, value in values.items():
        setattr(_thread_local_storage, f"{THREAD_LOCAL_PREFIX}{key}", value)
    try:
        yield
    finally:
        for key in values.keys():
            attr = f"{THREAD_LOCAL_PREFIX}{key}"
            if hasattr(_thread_local_storage, attr):
                delattr(_thread_local_storage, attr)


def get_thread_local(key: str, expected_type: Type[T]) -> Optional[T]:
    value = getattr(_thread_local_storage, f"{THREAD_LOCAL_PREFIX}{key}", None)
    if value is not None and not isinstance(value, expected_type):
        raise RuntimeError(f"Expected {expected_type} for key '{key}', got {type(value)}")
    return value
