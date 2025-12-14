# lrucache-rs

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/lrucache-rs)](https://pypi.org/project/lrucache-rs)

An efficient LRU cache written in Rust with Python bindings. Unlike other LRU cache implementations, this one behaves like a Python dictionary and does not wrap around a function.

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
