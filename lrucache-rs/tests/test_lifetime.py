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


def test_concurrent_disjoint_writes_and_cleanup():
    run_child("""
        import threading
        from lrucache_rs import LRUCache

        cache = LRUCache(256)
        written = threading.Barrier(5, timeout=10)
        checked = threading.Barrier(5, timeout=10)
        errors = []

        def worker(start):
            try:
                for key in range(start, start + 32):
                    cache[key] = key * 7 + 13
                written.wait()
                checked.wait()
                for key in range(start, start + 32):
                    assert cache.pop(key) == key * 7 + 13
            except BaseException as error:
                errors.append(error)
                written.abort()
                checked.abort()

        threads = [threading.Thread(target=worker, args=(start,))
                   for start in (0, 32, 64, 96)]
        for thread in threads:
            thread.start()
        completed = False
        try:
            written.wait()
            assert len(cache) == 128
            for key in range(128):
                assert cache[key] == key * 7 + 13
            checked.wait()
            completed = True
        finally:
            if not completed:
                written.abort()
                checked.abort()
            for thread in threads:
                thread.join(timeout=10)
        assert not errors, errors
        assert all(not thread.is_alive() for thread in threads)
        assert len(cache) == 0
    """)


@pytest.mark.parametrize(
    'owner',
    [
        'key',
        'value',
        'self',
        'two-caches',
        'key-iterator',
        'item-iterator',
        'value-iterator',
        'popitem-pair',
    ],
)
def test_cycles_are_collectible(owner):
    run_child(f"""
        import gc
        import weakref
        from lrucache_rs import LRUCache

        class Value:
            pass

        def cycle():
            cache = LRUCache(4)
            value = Value()
            observed = weakref.ref(value)
            value.cache = cache
            if {owner!r} == 'key':
                cache[value] = 23
            elif {owner!r} == 'self':
                cache['self'] = cache
                cache['value'] = value
            elif {owner!r} == 'two-caches':
                other = LRUCache(4)
                cache['other'] = other
                other['value'] = value
            elif {owner!r} == 'key-iterator':
                cache[value] = 23
                cache['iterator'] = iter(cache)
            elif {owner!r} == 'item-iterator':
                cache['value'] = value
                cache['iterator'] = iter(cache.items())
            elif {owner!r} == 'value-iterator':
                cache['value'] = value
                cache['iterator'] = iter(cache.values())
            elif {owner!r} == 'popitem-pair':
                cache['value'] = value
                pair = cache.popitem()
                cache['pair'] = pair
            else:
                cache['value'] = value
            return observed

        observed = cycle()
        for _ in range(5):
            gc.collect()
        assert observed() is None
    """)


def test_gc_resurrection_preserves_cache():
    run_child("""
        import gc
        from lrucache_rs import LRUCache

        resurrected = []

        class Value:
            def __del__(self):
                resurrected.append(self.cache)
                assert self.cache['value'] is self
                self.cache['new'] = 47

        def cycle():
            cache = LRUCache(4)
            value = Value()
            value.cache = cache
            cache['value'] = value

        cycle()
        for _ in range(5):
            gc.collect()
        assert len(resurrected) == 1
        cache = resurrected.pop()
        assert cache['new'] == 47
        assert len(cache) == 2
        cache.clear()
        cache['reused'] = 71
        assert list(cache.items()) == [('reused', 71)]
    """)


@pytest.mark.parametrize('mutation', ['read', 'clear', 'replace', 'error'])
def test_key_equality_reentry(mutation):
    run_child(f"""
        import gc
        from lrucache_rs import LRUCache

        cache = LRUCache(2)
        active = True
        replacement = object()

        class Key:
            def __hash__(self):
                return 7
            def __eq__(self, other):
                global active
                assert len(cache) >= 0
                gc.collect()
                if active:
                    active = False
                    if {mutation!r} == 'clear':
                        cache.clear()
                        cache[self] = replacement
                    elif {mutation!r} == 'replace':
                        cache[self] = replacement
                    elif {mutation!r} == 'error':
                        cache['callback'] = 79
                        raise ValueError('comparison')
                    else:
                        assert cache.peek(self) == 31
                        assert cache.get(self) == 31
                return isinstance(other, Key)

        stored = Key()
        cache[stored] = 31
        if {mutation!r} == 'error':
            try:
                cache[Key()] = 83
            except ValueError:
                pass
            else:
                raise AssertionError('comparison error swallowed')
            assert cache['callback'] == 79
            assert cache[stored] == 31
        else:
            expected = replacement if {mutation!r} in ('clear', 'replace') else 31
            assert cache[Key()] is expected
    """)


def test_cross_cache_comparison_does_not_hold_locks():
    run_child("""
        import threading
        from lrucache_rs import LRUCache

        caches = [LRUCache(2), LRUCache(2)]
        comparing = threading.Barrier(2, timeout=10)
        errors = []

        class Key:
            def __init__(self, index):
                self.index = index
            def __hash__(self):
                return 7
            def __eq__(self, other):
                comparing.wait()
                assert len(caches[1 - self.index]) == 1
                return isinstance(other, Key) and self.index == other.index

        keys = [Key(0), Key(1)]
        for index in range(2):
            caches[index][keys[index]] = index + 17

        def worker(index):
            try:
                assert caches[index][Key(index)] == index + 17
            except BaseException as error:
                errors.append(error)

        threads = [threading.Thread(target=worker, args=(index,))
                   for index in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        assert all(not thread.is_alive() for thread in threads)
        assert not errors, errors
    """)


def test_comparison_result_finalizer_can_reenter():
    run_child("""
        import gc
        from lrucache_rs import LRUCache

        cache = LRUCache(2)
        finalized = []

        class Result:
            def __bool__(self):
                return False
            def __del__(self):
                finalized.append(len(cache))

        class Key:
            def __hash__(self):
                return 7
            def __eq__(self, other):
                return Result()

        cache[Key()] = 31
        assert cache.get(Key(), 'missing') == 'missing'
        gc.collect()
        assert finalized == [1]
    """)


def test_concurrent_setdefault_keeps_one_value_identity():
    run_child("""
        import threading
        from lrucache_rs import LRUCache

        cache = LRUCache(2)
        ready = threading.Barrier(4, timeout=10)
        proposed = [object() for _ in range(4)]
        returned = []
        errors = []

        def worker(index):
            try:
                ready.wait()
                returned.append(cache.setdefault('shared', proposed[index]))
            except BaseException as error:
                errors.append(error)

        threads = [threading.Thread(target=worker, args=(index,))
                   for index in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        assert all(not thread.is_alive() for thread in threads)
        assert not errors, errors
        assert len(returned) == 4
        assert all(value is returned[0] for value in returned)
        assert cache['shared'] is returned[0]
        assert len(cache) == 1
    """)


@pytest.mark.parametrize('operation', ['replace', 'evict', 'delete', 'pop', 'popitem'])
def test_removed_value_finalizers_observe_committed_state(operation):
    run_child(f"""
        import gc
        from lrucache_rs import LRUCache

        cache = LRUCache(1)
        finalized = []

        class Value:
            def __del__(self):
                finalized.append(len(cache))
                cache['callback'] = 101

        cache['old'] = Value()
        if {operation!r} == 'replace':
            cache['old'] = 83
        elif {operation!r} == 'evict':
            cache['new'] = 97
        elif {operation!r} == 'delete':
            del cache['old']
        elif {operation!r} == 'pop':
            cache.pop('old')
        else:
            cache.popitem()
        for _ in range(5):
            gc.collect()
        expected = 1 if {operation!r} in ('replace', 'evict') else 0
        assert finalized == [expected], finalized
        assert cache['callback'] == 101
    """)


def test_evicted_key_and_value_finalize_after_commit():
    run_child("""
        import gc
        from lrucache_rs import LRUCache

        cache = LRUCache(1)
        finalized = []

        class Key:
            def __del__(self):
                finalized.append(('key', len(cache)))
        class Value:
            def __del__(self):
                finalized.append(('value', len(cache)))

        cache[Key()] = Value()
        cache['new'] = 103
        gc.collect()
        assert sorted(finalized) == [('key', 1), ('value', 1)]
        assert cache['new'] == 103
    """)


def test_failed_insertion_retires_input_after_comparison():
    run_child("""
        import gc
        from lrucache_rs import LRUCache

        cache = LRUCache(2)
        finalized = []

        class Key:
            def __hash__(self):
                return 7
            def __eq__(self, other):
                raise ValueError('comparison')
        class Value:
            def __del__(self):
                finalized.append(len(cache))

        key = Key()
        cache[key] = 107
        try:
            cache[Key()] = Value()
        except ValueError:
            pass
        else:
            raise AssertionError('comparison error swallowed')
        gc.collect()
        assert finalized == [1]
        assert cache[key] == 107
    """)
