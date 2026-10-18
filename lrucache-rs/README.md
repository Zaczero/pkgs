# lrucache-rs

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/lrucache-rs)](https://pypi.org/p/lrucache-rs)

`LRUCache` is a bounded mutable mapping for Python applications that need
least-recently-used eviction without wrapping a function. It supports CPython 3.11
and newer, including free-threaded builds, and PyPy 3.11.

## Installation

```sh
pip install lrucache-rs
```

## Basic usage

```py
from lrucache_rs import LRUCache

cache: LRUCache[str, int] = LRUCache(maxsize=2)
cache['1'] = 1
cache['2'] = 2
cache['3'] = 3
assert cache.get('1') is None
assert cache.get('2') == 2
assert cache.get('3') == 3
```

## Mapping operations

`LRUCache` implements
[`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping),
including `keys`, `items`, `values`, `update`, `pop`, `setdefault`, and
content equality with other mappings. It is mutable and unhashable.
See the [public class reference](python/lrucache_rs/__init__.py) for types and
method documentation.

`maxsize` must be a positive integer within the native index range, and keys
must be hashable. Invalid capacities raise `ValueError`; values that do not
support Python's integer index protocol raise `TypeError`.

### Recency and views

Item lookup, `get`, assignment, and `setdefault` mark an existing key most
recently used. Insertion marks a new key most recently used and evicts the
least-recently-used entry if necessary. `peek`, containment, iteration, view
observation, and mapping equality do not change recency.

Iteration runs from least to most recently used. Each iterator snapshots
the keys or key/value pairs when it is created, so later mutations cannot
invalidate it. Views are live: retaining a view and iterating it again
observes the current cache.

Unlike `dict.popitem`, **`popitem` removes the least-recently-used pair**, not
the most recently inserted pair. It raises `KeyError` on an empty cache.

### Threads, callbacks, and collection

Individual operations are thread-safe, including atomic `pop` and
`setdefault`. `update` consumes a mapping or iterable of pairs in encounter
order, then keyword entries. Each insertion can evict an entry. A later
provider error leaves preceding insertions in place; the whole update is
not an atomic transaction.

Key equality runs outside native locks and may read or mutate the same
cache. A lookup retries if its captured entries changed. Equality that
continually changes those entries may prevent the lookup from stabilizing.
Mapping equality uses independent snapshots rather than a transaction
across both mappings.

Finalizers may reenter the cache. In particular,
entries inserted by a finalizer during `clear` remain in the cache.
Reference cycles through the cache can be garbage-collected.

The cache reserves storage for `maxsize`, so choose it for your working set.

## Support

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
