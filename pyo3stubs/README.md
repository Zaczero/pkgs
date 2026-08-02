# pyo3stubs

Keep hand-authored PyO3 `.pyi` stubs honest against the compiled extension and
the Rust source they come from.

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

## Gates

| Gate | Activated by | What it catches |
|------|--------------|-----------------|
| `validity` | always-on | Illegal stub syntax, bare impl defs, bad overloads |
| `structural` | always-on | Overload hygiene, `@final` vs runtime, signature coverage, `__match_args__` |
| `text-signature` | always-on | Manual Rust `text_signature` vs the real `signature` |
| `surface` | `surface` | Option-kwarg drift across dual API surfaces |
| `duality` | `duality` | Scalar↔array return lies (`-> Self` on kind-changing ops) |
| `token-vocabulary` | `tokens` | `Literal` tokens vs runtime `token_enum!` |
| `leaked-types` | always-on | Unregistered / unstubbed public pyclasses |
| `rust-nullability` | always-on | `Option` getters/fields typed without `\| None` |
| `doc-contract` | always-on | Missing runtime docs / stub-only overrides without prose |
| `doc-structure` | always-on | Undocumented parameters, missing `Returns`, prose defaults that lie |
| `gen-docs-sync` | always-on | Stub file out of date vs `render_stub_with_docs` |
| `stubtest` | always-on | Members, params, defaults, properties vs runtime |

`validity` and `stubtest` are backed by mypy, which does not run on PyPy; they
report themselves *skipped* there rather than passing.

**A gate reports what it examined.** A clean run reads `text-signature:
matches signature attributes (99 text_signature attributes)`, and a gate that
finds none of what it inspects says `nothing to check — this project has no
text_signature attributes` rather than printing success. Three always-on gates
are structurally vacuous on a package with no `text_signature` attributes, no
public `#[pyclass]` and no `Option<..>` surface; that has to be visible.

Domain rules stay with their package. `doc-structure` owns the format-agnostic
half — parameter coverage, `Returns`, prose defaults — while a `Raises`
taxonomy naming a project's own exception classes does not travel.

Adding a monorepo standard means adding one `Gate` to `pyo3stubs.gates.GATES` —
there is no plugin bag. A rule that needs one package's own structure stays in
that package's `tools/` until a second consumer forces promotion.

## Setup

```bash
pyo3stubs init --package mypkg
```

```python
# tools/stubconfig.py
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

```python
# tests/test_stubs.py
from pyo3stubs.testing import gate_test

test_pyo3stubs_gate = gate_test('tools/stubconfig.py')
```

The config path resolves against the test file and its parent directories, so
the suite runs the same from the repository root and from inside `tests/`.

## Optional gates

Each optional gate is switched on by one nested config, and reports itself
*inactive* until that config is set **and non-empty** — never as a pass. A
`SurfaceConfig(targets=())` compares nothing, so calling that parity is the same
lie as an unconfigured gate printing success.

```python
from pyo3stubs import DualityConfig, StubConfig, SurfaceConfig, TokenConfig

def config() -> StubConfig:
    return StubConfig(
        module='gometry._lib',
        stub_path=...,
        src_root=...,
        surface=SurfaceConfig(
            targets=(('Geometry', gm.Geometry), ('GeometryArray', gm.GeometryArray)),
        ),
        duality=DualityConfig(pairs=(('Geometry', 'GeometryArray'),)),
        tokens=TokenConfig(
            types_module='gometry._types',
            vocabulary_export='_token_vocabulary',
        ),
    )
```

`disabled_gates` turns a gate off entirely; like an inactive gate it is
reported, so a disabled check can never read as a passing one.

## CLI

```bash
pyo3stubs check-all --config tools/stubconfig.py   # every gate, in order
pyo3stubs rust-nullability --config …              # one gate by name
pyo3stubs gen-docs --config …                      # inject runtime docstrings
pyo3stubs --help                                   # what each gate proves
```

Exit status is 1 when any gate reported a violation and 0 otherwise; a skipped,
disabled, or inactive gate is reported and does not affect the status. A gate
that crashes reports the crash as its own violation, with the raising frame, and
the remaining gates still run.

## Operator loop

```bash
maturin develop --release
pyo3stubs gen-docs --config tools/stubconfig.py
pyo3stubs check-all --config tools/stubconfig.py
# or: pytest tests/test_stubs.py
```

## How the Rust source is read

Every Rust-facing gate shares one tree-sitter parse per file
(`pyo3stubs.rust_scan`), so a `#[pyclass]` inside a comment, a string, a
`macro_rules!` *definition*, or a `#[cfg(test)]` module is not an export, and a
delimiter inside a literal cannot derail a scan. Classes declared by a project's
own macros are declared with `macro_exports` by argument position:

```python
macro_exports=(MacroExport(macro='geometry_leaf', rust=0, python=1),)
```

reading `geometry_leaf!(PyPoint, "Point", "docstring")` off the parsed token
tree — no regex, and no capture-group convention to remember. An `impl` wrapped
in a project macro is scanned too: the body is re-parsed as Rust, which is what
makes `frozen_pymethods! { impl … }` visible to the getter and constructor
scans.

`gen-docs` reads a `#[new]`'s `///` from the Rust source, because PyO3 exposes
it nowhere at runtime — `__new__.__doc__` is CPython's inherited boilerplate,
and injecting that published *"Create and return a new object. See help(type)
for accurate signature."* over the real prose.

## Design rules

- Signatures are **authored**, not generated — generators only touch docstrings.
- Single source of prose: Rust `///` → extension `__doc__` → stub / IDE / docs.
- Allowlists must not rot: unused entries fail the run.
- A gate cannot be registered without a clean-direction test and a
  seeded-violation test (`tests/test_checks.py::CASES`). A gate proven only in
  the failing direction can go permanently red without anyone noticing — two
  did.
