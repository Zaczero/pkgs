# githead

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/githead)](https://pypi.org/project/githead)

Simple utility for getting the current git commit hash (HEAD).

## Installation

```sh
pip install githead
```

## Basic usage

```py
from githead import githead

githead() # -> 'bca663418428d603eea8243d08a5ded19eb19a34'

# defaults to '.git' directory but can be changed:
githead('path/to/.git')
```
