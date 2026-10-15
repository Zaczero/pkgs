# sizestr

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/sizestr)](https://pypi.org/p/sizestr)

Use the `sizestr` Python package to format byte counts as short strings using binary units.

## Installation

```sh
pip install sizestr
```

## Format byte counts

```py
from sizestr import sizestr

print(sizestr(10_000))
print(sizestr(-42))
```

The output is display text, not a machine-readable numeric format. Sub-KiB magnitudes are truncated toward zero to whole bytes. Larger values use powers of 1024 and the suffixes `KiB`, `MiB`, `GiB`, `TiB`, `PiB`, `EiB`, and `ZiB`. Scaled magnitudes below 10 use two decimal places; the rest use one. Ordinary float rounding applies, without carrying a rounded value into the next suffix.

| Input | Output |
| --- | --- |
| `10_000` | `9.77 KiB` |
| `1024` | `1.00 KiB` |
| `1023.9` | `1023 B` |
| `-0.5` | `-0 B` |
| `-0.0` | `0 B` |
| `float("inf")` | `(inf)` |
| `float("-inf")` | `(-inf)` |
| `float("nan")` | `(nan)` |
| `1e24` | `847.0 ZiB` |
| `1e25` | `(too large to display)` |

`ZiB` is the largest suffix. Values whose computed binary exponent is larger return `(too large to display)`. Extremely large integers can raise `OverflowError` while the function converts them for finite-value and exponent checks, before it can return that marker; nonnumeric inputs raise `TypeError`.

## `sizestr`

`sizestr(size)` returns a string for a byte count. Integers and floating-point values are accepted. Non-finite floating-point values are enclosed in parentheses.
