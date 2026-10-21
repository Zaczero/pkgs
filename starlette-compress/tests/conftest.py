from __future__ import annotations

import functools
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

import pytest

if TYPE_CHECKING:
    from starlette.testclient import TestClient

    TestClientFactory = Callable[..., TestClient]
else:
    TestClientFactory = Callable[..., Any]


def pytest_configure(config: pytest.Config) -> None:
    # Keep warnings-as-errors except CPython's expected GIL fallback for Google Brotli.
    config.addinivalue_line(
        'filterwarnings',
        r"default:The global interpreter lock \(GIL\) has been enabled to load module '_brotli':RuntimeWarning",
    )


# https://anyio.readthedocs.io/en/stable/testing.html#specifying-the-backends-to-run-on
@pytest.fixture
def test_client_factory(
    anyio_backend_name: Literal['asyncio', 'trio'],
    anyio_backend_options: dict[str, Any],
) -> TestClientFactory:
    from starlette.testclient import TestClient

    return functools.partial(
        TestClient,
        backend=anyio_backend_name,
        backend_options=anyio_backend_options,
    )
