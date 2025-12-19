from enum import Enum


class TimeSeriesLineage(str, Enum):
    FULL = "FULL"
    NONE = "NONE"

    def __str__(self) -> str:
        return str(self.value)
