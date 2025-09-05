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
import logging

from neptune_query.internal import env

NEPTUNE_LOGGER_NAME = "neptune"
DEFAULT_LOG_FORMAT = "[%(name)s] [%(levelname)s] %(message)s"
DEBUG_LOG_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s] [%(processName)s/%(threadName)s/%(funcName)s] %(message)s"


def get_logger() -> logging.Logger:
    logger = logging.getLogger(NEPTUNE_LOGGER_NAME)
    _initialize_logger(logger)
    return logger


def _initialize_logger(logger: logging.Logger) -> None:
    if logger.hasHandlers():
        return
    level = env.NEPTUNE_LOGGER_LEVEL.get()
    logger.setLevel(level)

    handler = logging.StreamHandler()
    fmt = DEBUG_LOG_FORMAT if level == "DEBUG" else DEFAULT_LOG_FORMAT
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
