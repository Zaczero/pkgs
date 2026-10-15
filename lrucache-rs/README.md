# lrucache-rs

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/lrucache-rs)](https://pypi.org/p/lrucache-rs)

`LRUCache` is a bounded mutable mapping for Python applications that need
least-recently-used eviction without wrapping a function. Its storage and
individual cache operations are implemented in Rust. It supports CPython 3.10
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
See the [public class reference](python/lrucache_rs/__init__.py) and
[native method reference](python/lrucache_rs/_lib.pyi) for types and
method documentation.

Construction requires a positive maximum number of entries within the native
index range; an invalid capacity raises `ValueError` or `OverflowError`.
Keys must be hashable. Values and returned objects retain their identity.
Missing item lookup, deletion, or `pop` without a default raises `KeyError`.
Hash, equality, and input-provider exceptions propagate to the caller.

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

Individual native operations are thread-safe, including atomic `pop` and
`setdefault`. `update` consumes a mapping or iterable of pairs in encounter
order, then keyword entries. Each insertion can evict an entry. A later
provider error leaves preceding insertions in place; the whole update is
not an atomic transaction.

Key equality runs outside native locks and may read or mutate the same
cache. A lookup retries if its captured entries changed. Equality that
continually changes those entries may prevent the lookup from stabilizing.
Mapping equality uses independent snapshots rather than a transaction
across both mappings.

Removed keys and values are released after the cache state is coherent and
the lock is released. Their finalizers may reenter the cache. In particular,
entries inserted by a finalizer during `clear` remain in the cache.
Cache-owned reference edges are visible to Python's cyclic collector. PyPy
has an additional C-extension boundary limitation: cycles involving tuples
passed to native code may remain uncollected, even when that native call
retains nothing. This can affect tuple keys, tuple values, or a `popitem`
result stored back into a cycle. See
[PyO3's PyPy garbage-collection guidance](https://pyo3.rs/v0.29.3/class/protocols.html#garbage-collector-integration).

The cache reserves storage for its configured maximum. Collection visits
the reference slots for that capacity, including currently unused slots.
Choose capacity for the intended working set rather than an arbitrary
upper bound.

## Support

Report reproducible problems through the
[issue tracker](https://github.com/Zaczero/pkgs/issues), including the
Python implementation and version, free-threaded build mode if applicable,
and a small example of the unexpected behavior.
