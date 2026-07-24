---
description: gometry licensing — Apache-2.0 OR MIT dual license, third-party notices, the bundled libPROJ backend, and the Rust dependency posture.
---

# License

gometry is distributed under the **Apache-2.0 OR MIT** dual license. You may use it
under the terms of either license, at your option — the standard permissive
licensing convention in the Rust ecosystem, chosen for maximum downstream
compatibility.

```toml
# in pyproject.toml
license = "Apache-2.0 OR MIT"
```

The full license texts ship with the package as `LICENSE-APACHE.md` and
`LICENSE-MIT.md` — generated at build time from this declaration by the
shared `.github/scripts/gen_licenses.py`, never committed.

## Third-party components

gometry statically links a set of third-party Rust crates into its compiled
extension, each under its own permissive license. The authoritative inventory is
`LICENSE-THIRD-PARTY.md` — generated from the linked dependency graph and shipped
inside every wheel under `*.dist-info/licenses/` (alongside `LICENSE-APACHE.md` and
`LICENSE-MIT.md`); the summary below reflects the 1.0.0 dependency graph.

### Bundled libPROJ (CRS authority backend)

gometry bundles **libPROJ** (via the GeoRust `proj-sys` crate) as its CRS authority
backend, so release wheels do **not** require a system PROJ shared library. This is
the deliberate exception to gometry's pure-Rust preference: CRS transformation is a
deep, standards-heavy domain where an authority backend is the correct product
boundary. gometry's admitted closed-form fast paths do not replace PROJ's CRS
database, parsing, datum/grid pipelines, or general fallback.

- libPROJ is distributed under the **MIT/X11** license.
- PROJ resource files — the bundled CRS database and any local transformation grids —
  carry their **own licenses** from their respective data providers. If you redistribute
  a grid, check its upstream terms.

### GeoRust PROJ binding

The CRS boundary uses GeoRust's `proj-sys` binding, distributed under the
permissive **MIT / Apache-2.0** licenses. Geometry, indexing, grid, and codec
kernels are gometry-owned Rust implementations; the release does not link
`geo` or `geo-types`.

### PyO3 and maturin

The Python bindings use **PyO3** (`MIT OR Apache-2.0`). **maturin** (same dual
license) is build tooling for the wheel/sdist — it is not a statically linked
runtime component of the extension.

### Production Rust implementations of reference libraries

Several pure-Rust crates implement algorithms whose *reference* implementations
are C/C++:

| Reference (dev oracle / research) | Production Rust dep | Role |
|---|---|---|
| H3 (Uber C library / h3-py) | **`h3o`** | H3 cell math in the release wheel |
| GeographicLib (C++) | **`geographiclib-rs`** | Ellipsoidal geodesic/rhumb math |
| S2 Geometry (C++ / s2sphere) | gometry's own `src/grid/s2/` | Production S2; the `s2` crate is **dev-only** differential oracle |

Official C/C++ libraries (GEOS, JTS, the H3 C core, GeographicLib, S2) and their
Python bindings remain **development-time oracles** for differential testing —
they are **not** linked into the release wheel. The pure-Rust ports above *are*
production dependencies and appear in `LICENSE-THIRD-PARTY.md`.

## Rust dependency posture

gometry prefers pure-Rust, permissively-licensed (MIT / Apache-2.0) dependencies.
Heavy C/C++ libraries are avoided except where an authority backend is the correct
product boundary — as with libPROJ for CRS semantics.

A gometry release wheel therefore carries no mandatory GEOS/GDAL dependency and no
system PROJ shared-library requirement.

See the [design page](design.md#reliability-and-supply-chain-posture) for the
reasoning behind this supply-chain posture.
