# sizestr

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/sizestr)](https://pypi.org/project/sizestr)
[![Liberapay Patrons](https://shields.monicz.dev/liberapay/patrons/Zaczero?logo=liberapay&label=Patrons)](https://liberapay.com/Zaczero/)
[![GitHub Sponsors](https://shields.monicz.dev/github/sponsors/Zaczero?logo=github&label=Sponsors&color=%23db61a2)](https://github.com/sponsors/Zaczero)

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
