"""Root conftest: make @pytest.mark.slow tests opt-in via --slow flag."""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--slow", action="store_true", default=False, help="run slow tests (submit Databricks jobs)")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--slow"):
        return
    skip = pytest.mark.skip(reason="pass --slow to run job-submission tests")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip)
