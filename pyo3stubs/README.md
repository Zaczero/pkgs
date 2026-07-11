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

`pyo3stubs` is the test-suite layer that keeps those three from lying about each
other. It does **not** invent signatures; it validates, and for docs only it
injects runtime `__doc__` into the stub.

## Gate tiers (how extension works)

There is **no plugin bag**. Extension is:

1. write a `collect_errors(cfg) -> list[str]` module
2. register it in `pyo3stubs.gates.REGISTRY`
3. add tests

| Tier | When it runs | Examples |
|------|--------------|----------|
| **A — always-on** | Every adopter | validity, structural, text-signature, leaked-types, rust-nullability, doc-contract, gen-docs-sync, stubtest |
| **B — config-activated** | When the matching `StubConfig` field is set | surface, duality, namespace-facade, token-vocabulary |
| **C — package-local** | Outside this toolkit until a second package needs it | gometry `_doc_coverage` Raises taxonomies, ops inventories |

Rule of thumb: if the check is free when the pattern is absent and catching a
lie is always good, it is Tier A. If it needs package structure (dual classes,
namespaces, token enums), it is Tier B with optional nested config.

## Gate table

| Gate | Tier | What it catches |
|------|------|-----------------|
| `validity` | A | Illegal stub syntax, bare impl defs, bad overloads |
| `structural` | A | Overload hygiene, `@final` vs runtime, signature coverage, `__match_args__` |
| `text-signature` | A | Manual Rust `text_signature` vs `signature` attrs |
| `surface` | B | Option-kwarg drift across dual API surfaces |
| `duality` | B | Scalar↔array return lies (`-> Self` on kind-changing ops) |
| `namespace-facade` | B | Namespace re-exports must be plain `_lib` aliases |
| `token-vocabulary` | B | `Literal` tokens vs runtime `token_enum!` |
| `leaked-types` | A | Unregistered / unstubbed public pyclasses |
| `rust-nullability` | A | `Option` getters/fields typed without `\| None` |
| `doc-contract` | A | Missing runtime docs / stub-only overrides without prose |
| `gen-docs-sync` | A | Stub file out of date vs `render_stub_with_docs` |
| `stubtest` | A | Members, params, defaults, properties vs runtime |

All gates share one registry (`pyo3stubs.gates.REGISTRY`). The CLI, pytest
helper, and `check-all` read only that list.

## Minimal setup

```bash
python -m pyo3stubs init --package mypkg
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

## Config-activated features (Tier B)

```python
from pyo3stubs import DualityConfig, NamespaceConfig, StubConfig, TokenConfig

def config() -> StubConfig:
    return StubConfig(
        module='gometry._lib',
        stub_path=...,
        src_root=...,
        surfaces=(('Geometry', gm.Geometry), ('GeometryArray', gm.GeometryArray)),
        duality=DualityConfig(pairs=(('Geometry', 'GeometryArray'),)),
        namespace=NamespaceConfig(package_module='gometry', modules=('crs', 'h3', 's2')),
        tokens=TokenConfig(
            types_module='gometry._types',
            vocabulary_export='_token_vocabulary',
        ),
    )
```

Unset Tier B fields → those gates return clean immediately.

## Operator loop

```bash
maturin develop --release
python -m pyo3stubs gen-docs --config tools/stubconfig.py
python -m pyo3stubs check-all --config tools/stubconfig.py
# or: pytest tests/test_stubs.py
```

## Design rules

- Signatures are **authored**, not generated — generators only touch docstrings.
- Single source of prose: Rust `///` → extension `__doc__` → stub / IDE / docs.
- Allowlists must not rot: unused entries fail the run.
- Monorepo standards get a **named gate**; package-only rules stay in package
  `tools/` until a second consumer forces promotion.
- Future docs-style structure (Parameters/Returns/defaults) promotes to a named
  gate — not a plugin. Domain Raises taxonomies stay package-local until shared.

## Adopting on another package

1. Public pyfunctions need Rust `///` docs (doc-contract + gen-docs require
   runtime `__doc__`).
2. Hand-authored `_lib.pyi` must pass mypy validity and stubtest.
3. `python -m pyo3stubs init --package mypkg`, wire the path dep, then
   `pytest tests/test_stubs.py`.
