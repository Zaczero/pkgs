# githead

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/githead)](https://pypi.org/p/githead)

Read a checkout's Git HEAD from Python without launching Git. Python 3.11+.

## Installation

```sh
pip install githead
```

<a id="first-result"></a>

## Read a checkout's HEAD

Run from a checkout with a `.git` directory and a loose HEAD reference:

```py
from githead import githead

print(githead())
```

## githead reference

`githead(git_dir=".git")` accepts the metadata directory, not the working-tree
root. It returns the stripped contents of a detached `HEAD` or its loose
reference file.

This is a small file reader, not a general Git interface: packed refs, reftable,
unborn branches, and `.git` files used by worktrees or submodules are unsupported.
Missing files raise `FileNotFoundError`; references outside `git_dir` raise
`ValueError`. Other file-reading errors propagate.

[Full reference](githead/__init__.py) ·
[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
