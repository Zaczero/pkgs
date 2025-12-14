# sizestr

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/sizestr)](https://pypi.org/project/sizestr)

Simple and fast formatting of sizes for Python.

## Installation

```sh
pip install sizestr
```

## Basic usage

```py
from sizestr import sizestr

sizestr(10000)  # '9.77 KiB'
sizestr(-42)  # '-42 B'
sizestr(float('inf'))  # '(inf)'
```
