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


class AttributeWarning(UserWarning):
    """Warning for attribute issues."""


class ExperimentalWarning(UserWarning):
    """Warning for use of experimental API elements."""


class Http429Warning(UserWarning):
    """Warning for retryable HTTP 429 responses (rate limiting)."""


class Http503Warning(UserWarning):
    """Warning for retryable HTTP 503 responses (service unavailable)."""


class Http5xxWarning(UserWarning):
    """Warning for retryable HTTP 5xx responses (server errors)."""


class HttpOtherWarning(UserWarning):
    """Warning for other retryable HTTP issues."""
