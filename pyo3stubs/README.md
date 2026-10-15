# pyo3stubs

`pyo3stubs` helps maintainers of Python packages built with PyO3 keep
hand-authored `.pyi` stubs aligned with an importable extension and its Rust
source. It requires Python 3.11 or newer; gates that inspect the compiled
extension need that extension built in the interpreter running the checks.

[Install](#install) · [First check](#first-check) ·
[Configuration and gate reference](REFERENCE.md) ·
[Report an issue](https://github.com/Zaczero/pkgs/issues)

## Install

Add `pyo3stubs`, Maturin, and pytest to the consumer project's development
dependencies:

```bash
uv add --dev pyo3stubs maturin pytest
```

Build and install the extension into that project's environment before running
gates that compare against the runtime:

```bash
uv run maturin develop --release
```

## Why

PyO3 packages in this monorepo ship **hand-authored** stubs so the public
surface can express overloads, `Literal` unions, and precise return types that
the compiled module cannot declare. That is the right call for typing quality.

The cost is drift across three truths:

| Source | Owns |
|--------|------|
| **Rust** (`///`, `#[pyclass]`, `Option`, signatures) | prose, registration, real nullability, call shapes |
| **Compiled runtime** (`_lib.so`) | members, inspectable signatures, subclassability, `__doc__` |
| **Hand-authored `.pyi`** | types, overloads, IDE/docs typing surface |

`pyo3stubs` keeps those three from lying about each other. It does **not**
invent signatures; it validates, and for docs only it injects runtime `__doc__`
into the stub.

The [configuration and gate reference](REFERENCE.md) is the canonical list of
public configuration fields, defaults, gate activation, runtime requirements,
result statuses, and remedies. The CLI also summarizes each gate with
`uv run pyo3stubs --help`.

`validity` and `stubtest` use mypy. Several source-scanning and prose-generation
gates also require CPython because their parser or generator dependencies do
not support PyPy. The reference identifies each CPython-only gate and its
reason; other gates can still depend on the consumer extension and configuration.

**Both checkers, because neither sees everything.** pyright reports an
`__all__` entry that names nothing and other diagnostics mypy has no code for.
mypy catches a dead overload — one an earlier variant fully covers — that
pyright files under `reportOverlappingOverload`, the rule a stub turns off when
its scalar and batch overloads overlap on purpose; `validity` therefore enables
mypy's separate `overload-cannot-match` whatever the project's config says.

An active gate that finds no subjects reports `nothing to check`; an optional
gate with no non-empty configuration reports `inactive`. Neither means that
the gate found parity. A CLI exit status of 0 means no violation was reported,
not that every gate examined a subject.

Domain rules stay with their package. `doc-structure` owns the format-agnostic
half — parameter coverage, `Returns`, prose defaults — while a `Raises`
taxonomy naming a project's own exception classes does not travel.

Adding a monorepo standard means adding one `Gate` to `pyo3stubs.gates.GATES` —
there is no plugin bag. A rule that needs one package's own structure stays in
that package's `tools/` until a second consumer forces promotion.

## First check

```bash
uv run pyo3stubs init --package mypkg
```

`init` creates `tools/stubconfig.py` and `tests/test_stubs.py` only when those
files do not already exist. It does not build the extension or author the
hand-written stub. Adjust the generated paths and import name to match the
consumer package.

The generated configuration is a template. It assumes a package named `mypkg`
whose compiled module is `mypkg._lib`, whose stub is at
`python/mypkg/_lib.pyi`, and whose Rust sources are under `src/`.

```python
# Configuration template: replace the module and paths if the project differs.
from pathlib import Path
from pyo3stubs import StubConfig

ROOT = Path(__file__).resolve().parent.parent


def config() -> StubConfig:
    return StubConfig(
        module='mypkg._lib',
        stub_path=ROOT / 'python' / 'mypkg' / '_lib.pyi',
        src_root=ROOT / 'src',
    )
```

Build the extension into the same Python environment, then run one gate against
the configured files:

```bash
uv run maturin develop --release
uv run pyo3stubs structural --config tools/stubconfig.py
```

For a positive check, use the stub that matches the compiled module. To see the
failure path, temporarily add `@final` to a stub class whose PyO3 class is
subclassable, then run the same command: `structural` reports the mismatch and
returns status 1. Remove the annotation and rerun the command to restore the
clean result.

## pytest integration: `pyo3stubs.testing.gate_test`

Call `gate_test` once in the consumer test module to collect one named pytest
case per registered gate.

```python
# tests/test_stubs.py
from pyo3stubs.testing import gate_test

test_pyo3stubs_gate = gate_test('tools/stubconfig.py')
```

The relative config path resolves against the test file and its parent
directories. Violations fail their case; unsupported, disabled, inactive, and
`nothing to check` results are skipped with their reason. Configuration shims
are imported as trusted Python code; they run with the current process's
authority and are not sandboxed.

## Optional gates

Each optional gate is switched on by one nested config. `surface` requires
non-empty targets, `duality` requires non-empty class pairs, and
`token-vocabulary` requires a `TokenConfig` naming the Rust macro, runtime
vocabulary export, and `Literal` module. An unset or inactive gate is reported
separately; it is never presented as a passing check.

Configure `surface`, `duality`, and `tokens` with the corresponding record
classes. Their accepted values and defaults are listed under their field
headings in the [configuration reference](REFERENCE.md).

## CLI

```bash
uv run pyo3stubs check-all --config tools/stubconfig.py
uv run pyo3stubs rust-nullability --config tools/stubconfig.py
uv run pyo3stubs gen-docs --config tools/stubconfig.py
uv run pyo3stubs --help
```

`check-all` reports every gate outcome in registry order. A skipped, disabled,
inactive, or `nothing to check` result is visible but does not change its exit
status; a collector crash is reported as a violation, and the remaining gates
still run. The pytest adapter skips non-violation outcomes that did not examine
subjects, with the reason.

`check-all` runs the gates concurrently and prints one total, so a run costs
about its slowest gate — `stubtest`, which rebuilds from scratch because it
needs full semantic trees. A single gate by name prints its own time instead:
under `check-all` the gates wait on each other's imports, and a millisecond
gate can read seconds. `validity` keeps a mypy cache per stub under
`$XDG_CACHE_HOME/pyo3stubs`, so it re-analyses only what changed.

## Operator loop

Run these commands from the consumer project root after installing its
development dependencies and building the extension into the selected Python
environment:

```bash
uv run maturin develop --release
uv run pyo3stubs gen-docs --config tools/stubconfig.py
uv run pyo3stubs check-all --config tools/stubconfig.py
# Or run: uv run pytest tests/test_stubs.py
```

`gen-docs` writes the rendered stub even when `doc-contract` reports a
violation, then returns status 1. Review and repair the generated stub before
using the gate result as certification.

The reference is regenerated in the `pyo3stubs` source checkout under CPython,
where the doc renderer's LibCST dependency is installed, and is checked by its
pytest suite:

```bash
uv run pyo3stubs reference > REFERENCE.md
```

## How the Rust source is read

Rust-source gates parse Rust syntax rather than searching comments and strings.
Class declarations hidden inside `macro_rules!` definitions and test-only
modules are not treated as public exports. Declare classes created by a
project's own macros with `macro_exports`, using argument positions:

```python
# Partial configuration fragment.
macro_exports=(MacroExport(macro='geometry_leaf', rust=0, python=1),)
```

For `geometry_leaf!(PyPoint, "Point", "docstring")`, `rust=0` names the Rust
identifier and `python=1` names the Python export. The macro invocation is
parsed by syntax, so commas inside string arguments do not affect those
positions. An `impl` wrapped in a project macro is scanned as well.

`gen-docs` reads a `#[new]`'s `///` from the Rust source, because PyO3 exposes
it nowhere at runtime — `__new__.__doc__` is CPython's inherited boilerplate,
and injecting that published *"Create and return a new object. See help(type)
for accurate signature."* over the real prose.

The public exports are `StubConfig`, `MacroExport`, `SurfaceConfig`,
`DualityConfig`, `TokenConfig`, `TextSignatureDefault`, `Reasons`,
`DEFAULT_IGNORED_TYPE_NAMES`, and `render_stub_with_docs`. Gate registry,
scanner, context, and collector objects are implementation details.
