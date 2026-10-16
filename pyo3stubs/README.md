# pyo3stubs

<a id="why"></a>

Keep your PyO3 extension's typing stubs, runtime API, and Rust docstrings in
sync. `pyo3stubs` checks hand-authored `.pyi` files and fills in their
documentation without replacing precise overloads or return types. Python 3.11+.

[Install](#install) · [First check](#first-check) ·
[Configuration and gate reference](REFERENCE.md) ·
[Report an issue](https://github.com/Zaczero/pkgs/issues)

## Install

Add to your PyO3 project's development dependencies:

```bash
uv add --dev pyo3stubs maturin pytest
```

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

For example, `structural` reports a mismatch if a stub declares a class
`@final` but the extension allows subclassing.

## pytest integration: `pyo3stubs.testing.gate_test`

Call `gate_test` once in your test module to collect one named pytest
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

`validity` and `stubtest` use mypy. Several source-scanning and documentation
gates require CPython; the reference identifies them. Other gates can also
depend on the extension and configuration being checked.

## CLI

```bash
uv run pyo3stubs check-all --config tools/stubconfig.py
uv run pyo3stubs rust-nullability --config tools/stubconfig.py
uv run pyo3stubs gen-docs --config tools/stubconfig.py
uv run pyo3stubs reference
uv run pyo3stubs --help
```

`check-all` runs gates concurrently and reports every outcome in registry order.
A skipped, disabled,
inactive, or `nothing to check` result is visible but does not change its exit
status; a collector crash is reported as a violation, and the remaining gates
still run. The pytest adapter skips non-violation outcomes that did not examine
subjects, with the reason. `reference` prints the configuration and gate
reference to standard output.

<a id="operator-loop"></a>

## Update stub documentation

After changing the extension, rebuild it and synchronize its docstrings:

```bash
uv run maturin develop --release
uv run pyo3stubs gen-docs --config tools/stubconfig.py
uv run pyo3stubs check-all --config tools/stubconfig.py
# Or run: uv run pytest tests/test_stubs.py
```

`gen-docs` writes the rendered stub even when `doc-contract` reports a
violation, then returns status 1. Review and repair the generated stub before
rerunning the checks.

<a id="how-the-rust-source-is-read"></a>

## Classes declared by Rust macros

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

`gen-docs` reads the `#[new]` Rust comment because PyO3 does not expose that
constructor prose through `__new__.__doc__`; CPython supplies boilerplate
there. The generator places the Rust prose in the stub's constructor
declaration.

## Support

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
