"""Shared test configuration.

The async test suite runs under every available event-loop policy — the
standard-library asyncio loop always, and uvloop when it is installed (the
`h2corn[uvloop]` extra, pulled in by the dev group). Each async test is
parametrized as ``[asyncio]`` / ``[uvloop]`` so both the in-process test
server and the test client exercise both loops. This is the coverage that
keeps the pump (settable ``_asyncio_future_blocking``, the eventfd
``add_reader`` doorbell, direct ``Task`` construction, the ``_enter_task``
guard) working on whichever loop a deployment picks.
"""

import asyncio
import warnings

import pytest


def _event_loop_policies():
    # pytest-asyncio's `event_loop_policy` fixture still takes a policy object,
    # but the policy classes are deprecated (removal in 3.16). Use the
    # supported API and scope-silence that one unavoidable warning.
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        params = [pytest.param(asyncio.DefaultEventLoopPolicy(), id='asyncio')]
        try:
            import uvloop
        except ModuleNotFoundError:
            return params
        return [*params, pytest.param(uvloop.EventLoopPolicy(), id='uvloop')]


@pytest.fixture(params=_event_loop_policies())
def event_loop_policy(request):
    return request.param
