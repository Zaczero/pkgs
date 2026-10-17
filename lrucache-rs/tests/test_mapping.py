"""MutableMapping protocols, live views, and recency-neutral observation."""

from collections.abc import ItemsView, KeysView, MutableMapping, ValuesView

import pytest
from lrucache_rs import LRUCache


def test_mapping_update_and_dict_conversion():
    cache = LRUCache[str, int](3)
    assert isinstance(cache, MutableMapping)
    cache.update({'a': 11}, b=23)
    cache.update([('c', 37), ('d', 41)])
    assert dict(cache) == {'b': 23, 'c': 37, 'd': 41}


def test_update_preserves_partial_progress():
    cache = LRUCache[str, int](3)
    provider_error = ValueError('provider')

    def entries():
        yield 'kept', 17
        raise provider_error

    with pytest.raises(ValueError, check=lambda error: error is provider_error):
        cache.update(entries())
    assert list(cache.items()) == [('kept', 17)]


def test_views_are_live_with_snapshot_iterators():
    cache = LRUCache[str, int](3)
    cache['a'] = 11
    cache['b'] = 23
    keys, items, values = cache.keys(), cache.items(), cache.values()
    assert isinstance(keys, KeysView)
    assert isinstance(items, ItemsView)
    assert isinstance(values, ValuesView)
    key_iterator, item_iterator, value_iterator = iter(keys), iter(items), iter(values)
    del cache['a']
    cache['c'] = 37
    assert list(key_iterator) == ['a', 'b']
    assert list(item_iterator) == [('a', 11), ('b', 23)]
    assert list(value_iterator) == [11, 23]
    assert list(keys) == ['b', 'c']
    assert list(items) == [('b', 23), ('c', 37)]
    assert list(values) == [23, 37]
    assert len(items) == 2
    assert keys & {'c', 'missing'} == {'c'}
    assert ('c', 37) in items


def test_observation_and_equality_do_not_touch_recency():
    cache = LRUCache[str, int](3)
    cache.update(a=11, b=23, c=37)
    assert cache.peek('a') == 11
    assert 'a' in cache
    assert ('a', 11) in cache.items()
    assert 11 in cache.values()
    assert list(cache.items()) == [('a', 11), ('b', 23), ('c', 37)]
    assert cache == {'a': 11, 'b': 23, 'c': 37}
    assert cache != {'a': 99}
    assert list(cache) == ['a', 'b', 'c']
    assert cache.popitem() == ('a', 11)
    with pytest.raises(TypeError):
        hash(cache)


def test_pop_defaults_and_setdefault_recency():
    cache = LRUCache[str, object](2)
    marker = object()
    assert cache.setdefault('a', marker) is marker
    cache['b'] = 23
    assert cache.setdefault('a', object()) is marker
    cache['c'] = 37
    assert 'b' not in cache
    assert cache.pop('a') is marker
    assert cache.pop('missing', None) is None
    assert cache.pop('missing', marker) is marker
    with pytest.raises(KeyError):
        cache.pop('missing')
    assert cache.setdefault('none') is None
