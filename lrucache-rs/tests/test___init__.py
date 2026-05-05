import gc
import threading

import pytest
from lrucache_rs import LRUCache


def test_invalid_maxsize():
    with pytest.raises(OverflowError):
        LRUCache(-1)
    with pytest.raises(ValueError):
        LRUCache(0)


def test_maxsize_attribute():
    cache = LRUCache[str, int](7)
    assert cache.maxsize == 7


def test_maxsize_eviction():
    cache = LRUCache[str, int](2)
    cache['1'] = 1
    cache['2'] = 2
    cache['3'] = 3
    assert cache.get('1') is None
    assert cache.get('2') == 2
    assert cache.get('3') == 3
    cache.get('2')
    cache['4'] = 4
    assert cache.get('2') == 2
    assert cache.get('3') is None
    assert cache.get('4') == 4


def test_move_to_end_on_assign():
    cache = LRUCache[str, int](2)
    cache['1'] = 1
    cache['2'] = 2
    cache['3'] = 3
    assert cache.get('1') is None
    assert cache.get('2') == 2
    assert cache.get('3') == 3
    cache['2'] = 2
    cache['4'] = 4
    assert cache.get('2') == 2
    assert cache.get('3') is None
    assert cache.get('4') == 4


def test_get_default():
    cache = LRUCache[str, int](1)
    cache['1'] = 1
    cache['2'] = 2
    assert cache.get('1') is None
    assert cache.get('1', None) is None
    not_found = object()
    assert cache.get('1', not_found) is not_found


def test_len_and_delitem():
    cache = LRUCache[str, int](2)
    assert len(cache) == 0
    cache['1'] = 1
    cache['2'] = 2
    assert len(cache) == 2
    cache['3'] = 3
    assert len(cache) == 2
    del cache['3']
    assert len(cache) == 1
    del cache['2']
    assert len(cache) == 0
    with pytest.raises(KeyError, match="'1'"):
        del cache['1']


def test_contains():
    cache = LRUCache[str, int](2)
    assert '1' not in cache
    cache['1'] = 1
    assert '1' in cache
    del cache['1']
    assert '1' not in cache


def test_iter_snapshots_keys():
    cache = LRUCache[str, int](2)
    cache['1'] = 1
    cache['2'] = 2
    iterator = iter(cache)
    del cache['1']
    del cache['2']
    assert len(cache) == 0
    assert next(iterator) == '1'
    assert next(iterator) == '2'
    with pytest.raises(StopIteration):
        next(iterator)


def test_getitem_raises_keyerror():
    cache = LRUCache[str, int](2)
    cache['1'] = 1
    cache['2'] = 2
    assert cache['1'] == 1
    assert cache['2'] == 2
    with pytest.raises(KeyError, match="'3'"):
        cache['3']


def test_hash_collision_with_non_equal_keys():
    class Key:
        def __init__(self, value: str) -> None:
            self.value = value

        def __hash__(self) -> int:
            return 0

        def __eq__(self, other: object) -> bool:
            if not isinstance(other, Key):
                return NotImplemented
            return self.value == other.value

        def __repr__(self) -> str:
            return f'Key({self.value!r})'

    k1 = Key('a')
    k2 = Key('b')
    k3 = Key('c')
    assert hash(k1) == hash(k2) == hash(k3)
    assert k1 != k2

    cache = LRUCache[object, int](2)

    cache[k1] = 1
    cache[k2] = 2

    assert len(cache) == 2
    assert cache.get(k1) == 1
    assert cache.get(k2) == 2

    cache.get(Key('a'))
    cache[k3] = 3

    assert cache.get(k2) is None
    assert cache.get(k1) == 1
    assert cache.get(k3) == 3


def test_unhashable_key_raises_typeerror():
    cache = LRUCache[object, int](2)
    with pytest.raises(TypeError):
        cache[[1, 2]] = 1
    with pytest.raises(TypeError):
        _ = cache[[1, 2]]
    with pytest.raises(TypeError):
        _ = [1, 2] in cache
    with pytest.raises(TypeError):
        del cache[[1, 2]]
    with pytest.raises(TypeError):
        cache.get([1, 2])
    # Unhashable key never inserted, cache untouched.
    assert len(cache) == 0


def test_eq_exception_propagates():
    """A user __eq__ that raises must surface as a Python exception, not panic the interpreter."""

    class BadEq:
        def __init__(self, value: str) -> None:
            self.value = value

        def __hash__(self) -> int:
            return 0  # force hash collision so __eq__ is consulted

        def __eq__(self, other: object) -> bool:
            raise ValueError(f'boom: {self.value}')

    cache = LRUCache[object, int](4)
    a = BadEq('a')
    b = BadEq('b')
    cache[a] = 1
    # Storing a second key with the same hash forces collision resolution -> __eq__.
    with pytest.raises(ValueError, match='boom'):
        cache[b] = 2
    # The cache is left in a sane state - a is still findable by identity.
    assert cache.get(a) == 1


def test_hash_exception_propagates():
    class BadHash:
        def __hash__(self) -> int:
            raise RuntimeError('no hash for you')

    cache = LRUCache[object, int](4)
    bad = BadHash()
    with pytest.raises(RuntimeError, match='no hash for you'):
        cache[bad] = 1
    with pytest.raises(RuntimeError):
        _ = cache.get(bad)
    with pytest.raises(RuntimeError):
        _ = bad in cache


def test_pointer_equality_short_circuits_eq():
    """Looking up the exact same Python object must not invoke __eq__ at all.

    This mirrors the CPython dict optimisation: identical pointers compare equal regardless of
    user-defined __eq__.
    """

    eq_calls = 0

    class CountingEq:
        def __init__(self, value: int) -> None:
            self.value = value

        def __hash__(self) -> int:
            return 7

        def __eq__(self, other: object) -> bool:
            nonlocal eq_calls
            eq_calls += 1
            return isinstance(other, CountingEq) and self.value == other.value

    cache = LRUCache[object, int](2)
    k = CountingEq(1)
    cache[k] = 1
    eq_calls = 0
    # Identity lookup: should hit the pointer-equal fast path without calling __eq__.
    assert cache[k] == 1
    assert eq_calls == 0


def test_peek_does_not_touch_recency():
    cache = LRUCache[str, int](2)
    cache['a'] = 1
    cache['b'] = 2
    # peek 'a' should leave 'a' as LRU.
    assert cache.peek('a') == 1
    cache['c'] = 3
    # 'a' was the LRU; insertion of 'c' must evict 'a', not 'b'.
    assert cache.get('a') is None
    assert cache.get('b') == 2
    assert cache.get('c') == 3


def test_peek_default():
    cache = LRUCache[str, int](2)
    sentinel = object()
    assert cache.peek('missing') is None
    assert cache.peek('missing', sentinel) is sentinel


def test_clear():
    cache = LRUCache[str, int](4)
    for i in range(4):
        cache[str(i)] = i
    assert len(cache) == 4
    cache.clear()
    assert len(cache) == 0
    # Cache is reusable after clear.
    cache['x'] = 1
    assert cache['x'] == 1


def test_popitem_returns_lru():
    cache = LRUCache[str, int](3)
    cache['a'] = 1
    cache['b'] = 2
    cache['c'] = 3
    cache['a']  # touch 'a' so 'b' becomes the LRU
    assert cache.popitem() == ('b', 2)
    assert cache.popitem() == ('c', 3)
    assert cache.popitem() == ('a', 1)
    with pytest.raises(KeyError, match='cache is empty'):
        cache.popitem()


def test_repr():
    cache = LRUCache[str, int](5)
    cache['a'] = 1
    cache['b'] = 2
    assert repr(cache) == 'LRUCache(maxsize=5, currsize=2)'


def test_drop_releases_python_objects():
    """When the cache is dropped, references to stored values must be released."""

    cache = LRUCache[int, object](16)
    obj = object()
    obj_id = id(obj)

    import sys

    base_refs = sys.getrefcount(obj)
    cache[1] = obj
    cache[2] = obj
    assert sys.getrefcount(obj) == base_refs + 2
    del cache
    gc.collect()
    assert sys.getrefcount(obj) == base_refs
    assert id(obj) == obj_id  # not freed by gc


def test_setitem_updates_value():
    cache = LRUCache[str, int](2)
    cache['a'] = 1
    cache['a'] = 2
    assert cache['a'] == 2
    assert len(cache) == 1


def test_get_returns_existing_value_unchanged():
    """Repeated get on the same key returns the same Python object (no copy)."""

    cache = LRUCache[str, list[int]](2)
    value = [1, 2, 3]
    cache['a'] = value
    fetched = cache['a']
    assert fetched is value
    fetched.append(4)
    assert cache['a'] == [1, 2, 3, 4]


def test_concurrent_access_does_not_crash():
    """Smoke test: parallel writers should not crash, deadlock, or violate maxsize."""

    cache = LRUCache[int, int](1024)
    errors: list[BaseException] = []

    def worker(start: int) -> None:
        try:
            for i in range(start, start + 2_000):
                cache[i % 1024] = i
                _ = cache.get(i % 1024)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(start,)) for start in (0, 10_000, 20_000, 30_000)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(cache) <= 1024


def test_hash_preserves_insertion_for_equal_keys_in_collision():
    """If two distinct keys collide and the second is inserted while the first exists,
    the cache must keep them as separate entries."""

    class Coll:
        def __init__(self, value: int) -> None:
            self.value = value

        def __hash__(self) -> int:
            return 42

        def __eq__(self, other: object) -> bool:
            return isinstance(other, Coll) and self.value == other.value

        def __repr__(self) -> str:
            return f'Coll({self.value})'

    cache = LRUCache[object, int](4)
    k1 = Coll(1)
    k2 = Coll(2)
    k3 = Coll(3)

    cache[k1] = 1
    cache[k2] = 2
    cache[k3] = 3
    assert cache[k1] == 1
    assert cache[k2] == 2
    assert cache[k3] == 3
