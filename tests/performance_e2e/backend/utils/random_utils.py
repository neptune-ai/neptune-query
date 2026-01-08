"""
Utilities for generating random data for test responses.
"""

import random
import string
from typing import Final

# Constants for data generation
DEFAULT_RANDOM_STRING_LENGTH: Final[int] = 10
MIN_NUMERIC_VALUE: Final[float] = -1_000_000_000.0
MAX_NUMERIC_VALUE: Final[float] = 1_000_000_000.0
MIN_VARIANCE: Final[float] = 0.0


def random_string(length: int = DEFAULT_RANDOM_STRING_LENGTH) -> str:
    """Generate a random string of specified length."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))
