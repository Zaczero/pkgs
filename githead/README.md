# githead

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/githead)](https://pypi.org/p/githead)

`githead` is a Python helper that reads a `HEAD` file from a Git metadata directory and follows a symbolic reference only when its target is a loose file beneath that directory. It does not discover a repository from an arbitrary working directory or run Git. Requires Python 3.10 or newer. [Install](#installation), try the [first result](#first-result), and see the [`githead` reference](#githead-reference). For help, [report an issue](https://github.com/Zaczero/pkgs/issues) and name `githead`.

## Installation

```sh
pip install githead
```

## First result

```py
from pathlib import Path
from tempfile import TemporaryDirectory

from githead import githead

with TemporaryDirectory() as temporary:
    git_dir = Path(temporary) / ".git"
    ref_path = git_dir / "refs" / "heads" / "main"
    ref_path.parent.mkdir(parents=True)
    ref_path.write_text("abc123\n", encoding="utf-8")
    (git_dir / "HEAD").write_text(
        "ref: refs/heads/main\n",
        encoding="utf-8",
    )

    assert githead(git_dir) == "abc123"
```

## Read a checkout's HEAD

The default `githead()` reads `.git/HEAD` relative to the current working directory. The argument must name the metadata directory itself, such as `.git`, rather than the repository's working-tree root. A `.git` file used by a worktree or submodule is not supported.

```py
from githead import githead

# Run from a checkout where .git is a directory and HEAD targets a loose ref.
print(githead())
```

The returned string is the stripped contents of detached `HEAD` or its loose reference file; it is not validated as a full object ID. Packed refs, reftable, unborn branches, and common-directory reference lookup are not supported. Git documents these distinct [repository layouts](https://git-scm.com/docs/gitrepository-layout).

## githead reference

`githead(git_dir=".git")` returns a string. A missing `HEAD` or loose reference raises `FileNotFoundError`; a symbolic reference that resolves outside `git_dir` raises `ValueError`. Other I/O and text-decoding errors propagate.
