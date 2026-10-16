# sizestr

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/sizestr)](https://pypi.org/p/sizestr)

Human-readable byte counts for Python 3.10+.

## Installation

```sh
pip install sizestr
```

## Format byte counts

```py
from sizestr import sizestr

print(sizestr(10_000))  # 9.77 KiB
print(sizestr(-42))     # -42 B
```

## sizestr reference

`sizestr(size)` accepts an integer or float and returns display text. Units use
powers of 1024, from bytes through ZiB. Bytes are truncated to integers; scaled
values use two decimal places below 10 and one otherwise.

Non-finite values display as `(inf)`, `(-inf)`, or `(nan)`. Values beyond ZiB
display as `(too large to display)`.

[Full reference](sizestr/__init__.py) ·
[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
