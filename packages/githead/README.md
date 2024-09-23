# githash

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/githash)](https://pypi.org/project/githash)
[![Liberapay Patrons](https://shields.monicz.dev/liberapay/patrons/Zaczero?logo=liberapay&label=Patrons)](https://liberapay.com/Zaczero/)
[![GitHub Sponsors](https://shields.monicz.dev/github/sponsors/Zaczero?logo=github&label=Sponsors&color=%23db61a2)](https://github.com/sponsors/Zaczero)

Simple utility for getting the current git commit hash.

## Installation

The recommended installation method is through the PyPI package manager. The project is implemented in Rust and several pre-built binary wheels are available for Linux, macOS, and Windows, with support for both x64 and ARM architectures.

```sh
pip install githash
```

## Basic usage

```py
from githash import githash

githash() # -> 'bca663418428d603eea8243d08a5ded19eb19a34'

# defaults to '.git' directory but can be changed:
githash('path/to/.git')
```
