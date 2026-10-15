"""Reference retirement, cyclic ownership and callback reentry."""

import subprocess
import sys
import textwrap

import pytest


def run_child(source: str) -> None:
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(source)],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('owner', ['key', 'value'])
def test_clear_retires_outside_lock(owner):
    run_child(f"""
        import gc
        from lrucache_rs import LRUCache

        cache = LRUCache(4)
        finalized = []

        class Finalizer:
            def __del__(self):
                finalized.append(len(cache))
                cache['reentered'] = 73

        cache['deleted'] = 11
        del cache['deleted']
        if {owner!r} == 'key':
            cache[Finalizer()] = 29
        else:
            cache['owned'] = Finalizer()
        cache.clear()
        gc.collect()
        assert finalized == [0], finalized
        assert list(cache) == ['reentered']
        assert cache['reentered'] == 73
    """)
