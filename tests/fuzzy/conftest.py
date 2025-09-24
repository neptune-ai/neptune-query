import os
from datetime import timedelta

from hypothesis import settings


def pytest_set_filtered_exceptions() -> list[type[BaseException]]:
    class DoNotFilterAnythingMarker(Exception):
        pass

    return [DoNotFilterAnythingMarker]


settings.register_profile(
    "ci-quick",
    settings.get_profile("ci"),
    max_examples=100,
    deadline=timedelta(seconds=10),
    derandomize=True,
)


settings.register_profile(
    "ci-nightly",
    settings.get_profile("ci"),
    max_examples=2000,
    deadline=timedelta(seconds=20),
    derandomize=False,
)

settings.load_profile(os.getenv("NEPTUNE_E2E_HYPOTHESIS_PROFILE", "ci-quick"))
