from enum import Enum


class TimeSeriesLineageEntityType(str, Enum):
    EXPERIMENT = "EXPERIMENT"
    RUN = "RUN"

    def __str__(self) -> str:
        return str(self.value)
