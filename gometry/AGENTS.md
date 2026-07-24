# AGENTS.md — gometry

Rust-first, PyO3-backed unified geospatial package: geometry, CRS/PROJ, geodesics, H3,
S2, spatial indexing, and Arrow/WKB/EWKB/GeoJSON/WKT interop. Workspace member of
`/home/user/Source/pkgs`. First public release: **v1.0.0** (no prior public version —
docs/changelog present it as the initial release, never as a "v2 redesign on top of v1").

## API Constitution (BINDING — supersedes any older binding prose below)

Three questions decide every public surface; there is one obvious spelling per concept.

| Question | Binding | Examples |
|---|---|---|
| **What is this?** (a fact) | `@property` | `crs`, `is_empty`, `bounds`, `area`, `length`, `cell.area`, `edge.length` |
| **Do something to this** (unary transform/measure of ONE) | **instance method** on `Geometry`/`GeometryArray` | `geom.buffer(d)`, `geom.simplify(t)`, `geom.centroid()`, `geom.to_crs(crs)`, `arr.union_all()`, `geom.prepare()` |
| **How do these two relate?** (binary) | **free function** | `contains(a, b)`, `distance(a, b)`, `intersection(a, b)` (+ `& | - ^` operators) |

- **Rule 4 — a second spelling is allowed ONLY when it adds a distinct capability**, never as a bare
  duplicate: `geom.area` (property) + `gm.area(geom, unit='planar')` (override only); `.parts` (lazy
  view) + `gm.parts()` (materialized column); `.coords` (object view) + `get_coordinates()` (numeric
  matrices). If the free function adds nothing over the property/method, it is DELETED (it breaks one
  obvious way). No `gm.is_*(geom)` / scalar `gm.bounds(geom)` mirrors. The capability that earns a
  free `gm.area`/`gm.length` is `unit=` plus raw-iterable input — **not** a mandatory `unit=`: those
  two default it exactly like their properties and like every sibling metric free function
  (`distance`, `dwithin`, …), so `gm.area(geom) == geom.area`. Requiring `unit=` on two members of a
  family that defaults it everywhere else is an arbitrary wall, not a distinct capability.
- **`GeometryArray` mirrors `Geometry`** — same spellings, columnar results (`arr.buffer(d)`,
  `arr.is_empty`). NOT a third parallel API via `gm.is_*(arr)` when `arr.is_*` exists. Column-native
  ops (e.g. `coverage_*`) are `GeometryArray` methods; a free function exists only for raw iterables.
- **`CellArray` mirrors scalar cells at the same name/altitude** — scalar facts stay singular on the
  array (`cells.token`, `cells.center`, `cells.area`), returning columnar/list
  results as needed. Do not pluralize array accessors just because the receiver has many rows.
- **Naming:** the probe geometry is always `geom` (`prepared.contains(geom)`, `idx.query(geom)`); bulk
  inputs at a module boundary are `values`.
- **Synthesis bet:** GeoPandas-style `.area` / `.to_crs()` chaining + pandas-style array methods +
  Shapely-2 vectorized binary free functions. v1.0.0 ships unary ops as instance methods on
  `Geometry`/`GeometryArray` and binary ops as free functions.

### Serialization & I/O placement (BINDING)

*Placement follows what you hold.* Same "one obvious spelling" rule, applied to I/O:

| You hold… | Binding | Class | Examples |
|---|---|---|---|
| a geometry/array → another representation **of it** | **method** on `Geometry`+`GeometryArray` | `serializer_method` | `geom.to_wkb()`, `arr.to_wkt()`, `geom.to_geojson()`, `geom.to_polyline()`, `geom.to_arrow()` |
| foreign bytes/text/obj → **create** geometry | **free function** `from_*` | `constructor_free` | `from_wkb`, `from_wkt`, `from_geojson`, `from_polyline`, `from_arrow`, `from_features` |
| a geometry **+ non-geometry side-data** → a composite | **free function** | `composite_free` | `to_feature(g, properties=…)`, `to_feature_collection(...)` |

A serializer is a method ONLY — **no free `gm.to_wkb`** (the array method is already the vectorized
path; a free twin would add zero capability and break one-obvious-way; gometry has a real `GeometryArray`,
unlike Shapely which is free-fn-first only because it lacks one). Direct raw-scalar encoding is
`gm.Point(x, y, crs=…).to_wkb(include_srid=True, precision=…)` — the serializer method with a
`precision=` knob, not a separate namespace. The `to_`/`from_` asymmetry (method
encode, free decode) is the Python norm (`int.to_bytes()` / `int.from_bytes()`). Every `serializer_method`
`to_X` requires a `constructor_free` `from_X` (gate-checked symmetry).

### Flat domain families (BINDING)

The public package has **no domain namespaces**. All capability is globally discoverable on `gm`, grouped
by a semantic prefix: `crs_*`, `h3_*`, `s2_*`, `geohash_*`, `tile_*`, `pluscode_*`, and
`osm_shortlink_*`. A family member is a thin `_lib.*` alias, never a wrapper; the family inventory is
explicitly gate-checked, so a new native symbol cannot accidentally become public. `gm.crs_info(v)` is the
raw-dict escape hatch (metadata from an identifier without constructing a `CRS`); it is NOT a duplicate of
`CRS(v).info`. `gometry.crs`, `gometry.geocode`, `gometry.h3`, `gometry.s2`, `gometry.geohash`, and
`gometry.tiles` do not exist, including as direct imports.

### Grid/cell surface (BINDING)

Grids obey the geometry model exactly: a **unary receiver-op is a method on the cell AND `CellArray`**
(kind-preserving), never a namespace free function. `polygon`/`center`/`area` (row-aligned) and
`compact`/`uncompact`/`to_polygon` (aggregating; array + coverage, never scalar) are methods; `parent` too.
Each prefixed grid family holds only factories (`h3_cover`, `h3_cells`, `h3_bounding_cell`, and its
same-shaped S2/geohash/tile counterparts) **+ binary set-algebra** (`h3_union`, `h3_intersection`,
`h3_difference`) — things with no single cell receiver, exactly like geometry's free `from_*` + binary
relations. On the single generic `CellArray`, depth-parametrized methods take a **generic
positional `depth`** (`cells.parent(5)`, `cells.compact(5)`) — the scalar cells keep their native keyword
(`cell.parent(resolution=5)`), since a per-grid keyword can't type cleanly on one erased class. `CellArray`
constructs from ids / cell objects / **string tokens** (`CellArray(tokens, type=H3Cell)`), so a separate
raw-token path is not needed. Factories stay free functions (a covering is a lossy discretization
returning a different-domain object — not a `Geometry` representation — so it does not belong on `Geometry`).
**Deliberate divergence from the binary→free rule:** cell↔cell predicates (`cell.contains(other)`,
`cell.intersects(other)`, `H3Cell.is_neighbor(other)`) are **receiver methods**, not free functions —
hierarchical cell containment is an intrinsic property of the id (a parent *contains* a child by id
arithmetic), matches h3-py, and has no natural grid-family free-function home. This is the one place the grid surface intentionally departs from geometry's
`contains(a, b)` free-function binary rule.

## Review process (STANDING — zero compromises)

Every completed work item gets a **native opus subagent review** for completeness, consistency, and
optimality BEFORE it is considered done — no work ships unreviewed. Implement (delegate one coherent
mechanical lane to a direct implementer when useful; delegated agents never delegate again) → independent
review → fix to green → re-run gates. VERIFY > TRUST: confirm against code, not the self-report.

## Architecture (invariants)

- **Rust owns all logic and hot paths.** Domain kernels live under `src/crs/`,
  `src/geometry/`, `src/grid/`, and `src/io/`. `src/array/` owns packed/mixed array storage plus
  the `GeometryArray` class and methods; `src/broadcast/`, `src/dispatch/`, `src/measures/`, and
  `src/predicates/` own their cross-shape boundary orchestration. The remaining Python classes
  and coherent binding domains live under `src/py/` (`classes/`, `crs/`, `arrow/`, `cells/`,
  `index/`, `methods/`, and `functions/`). `src/lib.rs` is the compact crate assembly and
  `#[pymodule]` registration root. Shared boundary extraction/validation lives in
  `src/boundary/`; shared PyO3 helpers live in `src/py/support/`. **Module rule:** keep generic
  trust-boundary logic in those shared owners, keep each domain's `register(m)` beside the
  classes/functions it registers, and keep `lib.rs` as explicit assembly rather than moving
  algorithms or parser helpers back into it.
  Python is only a thin facade + type stubs:
  - `python/gometry/__init__.py` — curated top-level surface: geometry constructors, ops,
    predicates, IO, geometry/index classes, and all flat domain families. No algorithms, loops,
    parsing, or math in Python. It aliases the native `_lib` entry points directly, so typed
    signatures live once in `_lib.pyi`. Cell construction is `H3Cell(...)`/`S2Cell(...)` with
    bulk `gm.h3_cells(...)`/`gm.s2_cells(...)`; global CRS work is `gm.crs_transform(...)`,
    `gm.crs_info(...)`, and related `crs_*` functions, while per-CRS work lives on `CRS`.
- **Cell systems share machinery, not just shape.** All four DGGS (H3, S2, geohash, tiles)
  emit the same cell/coverage API via the kernel-side [`GridCell`](src/grid/cell.rs) trait
  (per-system `grid_cell.rs` impls) and shared PyO3 helpers in `cell_ops.rs` /
  `coverage_ops.rs`: each cell class has hand-written `#[pymethods]` that delegate to the
  generic `cell_*` backing fns (center/boundary/parent/children/contains/intersects/hash/…;
  numeric u64 ids vs geohash's text token, with optional `__int__` where applicable).
  Coverages share `CoverageIterState`, `coverage_member_point`, `coverage_boundary_cells`, and
  related helpers; H3/S2/Geohash/Tile coverage classes keep hand-written `#[pymethods]` that
  call into those ops (rectangular geohash/tile coverages additionally blanket-share
  `coverage_ops` pymethods). All four coverages expose the uniform surface incl.
  `compact`/`uncompact`/`with_parents`/`to_polygon` and slice (`cov[i:j]`). `gometry.Cell`
  is the structural protocol; a pyright fixture + AST gate enforce conformance across all four.
  The `cell_rule` token (`center`/`within`/`overlap`/`bbox`) shapes only the visible cells —
  the exact membership predicates always answer against the source geometry, never the cells.
- **Own kernels for S2, geohash, tiles, geocodes, curves, antimeridian.** `src/grid/s2/` is a
  ground-up S2 (the `s2` crate is a dev-only differential oracle). `src/grid/` holds the
  geohash (packed `Geohash{bits,prec}`, int order == token order) and tile (packed `TileId`,
  `morton_interleave(x,y)` so id order == quadkey order) kernels plus the shared hierarchical
  `RectGridCoverer` (cells are exact lon/lat rectangles, so classification is plain
  covers/intersects — no spherical-edge ambiguity). `src/py/functions/geocode/` ports Google's OLC and
  OSM's shortlink; `src/curves.rs` the Hilbert/Morton keys (the only new dep, `fast_hilbert`);
  `src/geometry/antimeridian/` the JOSS split. `src/crs/rhumb.rs` is the GeographicLib
  order-6 ellipsoidal rhumb. Match these against their authoritative reference, not memory.
  - `python/gometry/_types.py` — private single source of truth for stub token aliases,
    protocols, and structured return types. `_lib.pyi` imports them; only `Cell` and
    `Coverage` are re-exported publicly at top level. Never redeclare these types in the stub.
  - `python/gometry/_arrow.py`, `_optional.py`, and the SANCTIONED ADAPTER MODULES (`_pandas.py`,
    `_polars.py`, `_geopandas.py`, `_geoparquet.py`, `_viz.py`) — thin Python glue for
    optional-dependency ecosystems. Adapter rules: import the target package lazily (`import
    gometry` never requires any of them), cross the FFI once per call at batch granularity, and
    keep algorithms/parsing/math in `_lib`. Adapters are explicit conversion boundaries: they
    never install import hooks, register framework accessors/dtypes, or mutate framework classes.
  - `python/gometry/_lib.pyi` — stub for the compiled extension; imports types from
    `gometry._types`.
  Explicit optional-adapter entry points are statically declared for IDE discovery
  and documented, but stay outside `__all__` and runtime `dir(gometry)`: `pydoc`
  and `inspect.getmembers` resolve every returned name, which must not force
  optional imports. Runtime discovery remains the import-safe core surface.
- **One canonical name per operation.** No alias pairs (`offset_curve` not `parallel_offset`;
  `minimum_rotated_rectangle` not `oriented_envelope`; `length` not `perimeter`;
  `bounds` mirrors scalar bounds per element; `GeometryArray.total_bounds` is the one aggregate
  array-wide box — do not add `gm.total_bounds` or alias pairs). Constructors are the
  typed geometry **classes** (`Point(...)`, `LineString(...)`, `Polygon(...)`, `Multi*`/
  `GeometryCollection(...)`, Shapely-style — see the typed-hierarchy decision below).
  Bulk construction uses `GeometryArray([...])`, column factories like `points(...)`, and
  parsers (`from_geojson`, `from_wkt`, …); `__geo_interface__` ingestion rides those paths.
- **String enum params are `Literal`-typed in `_lib.pyi`** (`cap_style`, `join_style`,
  `clip`, `method`, `mode`, `direction`) — exact supersets of the Rust
  parser tokens, so IDE/pyright catch typos while plain strings still pass at runtime.
- **The CRS is the single metric knob; default units are CRS-natural.** Bare `area`/`length`/
  `distance`/`buffer`/`offset_curve`/`dwithin`/LRS are CRS-aware via `crs::MetricModel`
  (`Planar { to_metre } | Geodesic(crs) | COORDINATE`): a geographic CRS measures geodesically
  on its own ellipsoid (meters); a projected CRS reports/accepts **native linear units** by
  default (a US-survey-foot CRS returns feet, not metres — non-uniform/angular axes are
  rejected); a CRS-free geometry stays in coordinate units. Pass `unit='meters'` to force SI
  scaling through `to_metre` (raises without a CRS); pass `unit='planar'` for raw coordinate
  Cartesian. Metre *inputs* under `unit='meters'` (buffer/offset/dwithin/destination/
  interpolate/LRS) divide by `to_metre`; SI outputs multiply. There is one `geom.area`
  property; free `gm.area(geom, unit=…)` is the override surface, defaulting `unit` to the
  property's own CRS-natural result when omitted (Rule 4); `to_crs` changes
  the frame. Per-CRS introspection lives on the first-class `CRS` (`PyCrs`) object;
  `geom.crs` returns it. The `gm.h3_cover` / `gm.s2_cover` / `gm.geohash_cover` /
  `gm.tile_cover` factory receives a zero-copy `Py<PyGeometry>` handle.
- **Geographic distance and LRS are true geodesic.** `crs::geodesic_shape_distance` folds
  vertex-to-segment minima (golden-section + branch-and-bound pruning) and detects antimeridian
  segment crossings the planar `intersects` shortcut misses. Geographic `line_interpolate`/
  `line_substring`/`line_locate` with the default ``basis='distance'`` use exact ellipsoid segment
  lengths (`crs::geodesic_line_*`),
  consistent with `length()`. (A cross-track-seeded Newton inner solver is the noted future perf
  lever; golden-section is kept for robustness.)
- **Binary ops are strict and commutative.** `ensure_compatible_metadata` (CRS presence+value AND
  coordinate epoch) gates every metric/predicate/overlay/snap/split/LRS/nearest and the spatial
  index (query/nearest/insert all share one frame). Mixed CRS-tagged/CRS-free or epoch mismatch
  is rejected, never coerced. 3D `distance_3d`/`length_3d` require Z on every vertex (`MissingZ`);
  geographic metrics reject out-of-domain lat. Negative `buffer` erodes (empty for points/lines).
  No-panic invariant: huge-but-finite coordinates never panic across the FFI boundary.
- **Target Python >= 3.11.** Modern `X | Y` unions; no `typing.Union`/3.9 workarounds.
- **NumPy is the only unconditional numeric runtime dependency**; the small standard
  `typing-extensions>=4.15` backport is required before Python 3.15 for truthful
  `Buffer`/defaulted `TypeVar`/`disjoint_base` stubs. `pyarrow` is optional (the `arrow` extra). All Rust
  direct dependencies declared by gometry disable default features. Vendored and transitive defaults are audited individually; they are not assumed disabled.
- **Pinned build inputs (2026-07-09).** Development and artifacts use
  `nightly-2026-07-09` and `maturin==1.14.1`. The `s2 0.1.0` crate remains a
  dev-only differential oracle and is never linked into production artifacts;
  its upstream `cgmath 0.18.0` dependency carries RUSTSEC-2026-0196 and
  RUSTSEC-2026-0197. Gometry does not call the affected `swap_columns` API.
  Reassess and remove the exception when S2 replaces cgmath; do not suppress a
  production advisory under this exception.
- **CRS backend:** bundled libPROJ via GeoRust `proj-sys` (`bundled_proj`).

## Hard boundaries

- **Arrow C capsule producers are ABI-trusted; data/layout is validated.** The
  Arrow C Data Interface carries no buffer capacity, so a forged/lying producer
  cannot be made memory-safe solely inside the consumer (inherent to the format;
  pyarrow and every other Arrow consumer share this line). gometry validates
  schema/layout/offsets/encoding defensively and rejects malformed *data*, but
  `__arrow_c_array__` / `__arrow_c_stream__` producers are trusted to be
  ABI-conforming. Deliberately hostile duck-typed producers that lie about their
  own buffers or metadata (`column_names`, `to_pybytes`, `__arrow_ext_serialize__`,
  `type.names`) are out of the threat model — document, do not chase. Keep the
  data-driven validation airtight. Public prose: `docs/ecosystem/arrow.md` and
  `from_arrow`.
- **Ingress threat model (BINDING).** **Classify the carrier before calling any
  validation "security".** Four carriers, each with a different contract:
  1. **Untrusted serialized data** — WKB/EWKB, WKT/EWKT, GeoJSON, GeoParquet,
     and Arrow data from an ABI-conforming producer. Validate syntax, bounded
     structural recursion, declared counts against available bytes,
     schema/storage agreement, offsets, nulls, encoding, and every expressible
     layout invariant *before* constructing trusted internal state. A small
     payload must never cause disproportionate allocation or work.
  2. **Executable Python providers** — arbitrary iterators, mappings,
     `__geo_interface__`, `__arrow_c_array__`/`__arrow_c_stream__`. These run
     caller-controlled methods and are **not** a sandbox boundary. Validate the
     values they return and keep native growth fallible, but provider progress
     and deliberately infinite behaviour are caller-owned.
  3. **Advisory sizes** — a Python `__len__` or length hint is a *hint*: never
     promote it into an exact allocation or iteration count. Authoritative
     counts come only from trusted receiver state, already-materialized data,
     exact built-ins/buffers, or validated serialized layout.
  4. **Pickle** — trusted-code persistence only. Validate representation
     safety, parameter domains, and bounded reconstruction; do **not** validate
     for authenticity. Unpickling a hostile payload is already arbitrary code
     execution, so payload-content checks defend nothing already lost — any
     surviving check must justify itself as reconstruction or invariant
     protection, described as correctness validation, never anti-forgery.
     The private `_unpickle_*` reconstructors are **not** a safety boundary and
     are unsupported as `RestrictedUnpickler` allowlist targets; a loader that
     allowlists them takes them into *its own* trust boundary.
  - **A check is always-on only if it is FALLIBLE** — a typed error, never an
    `assert!` that merely relocates a panic. Always-on is earned by memory
    safety, checked indexing/arithmetic, no-panic, bounded recursion or
    termination, disproportionate-work prevention, an explicit resource budget,
    or the semantic meaning promised by an external representation. After
    admission into trusted typed storage, enforce invariants **by construction**
    and use tests or `debug_assert!` for own-bug-only semantic failures.
  - **In model:** amplification, memory unsafety (SIGSEGV/UB/OOB, uncapped
    recursion), reachable panic on non-proportionally-large input, silent
    corruption, non-termination, over-rejection of valid input, super-linear
    CPU on legitimately-sized input. Defense is
    structural validation + fallible collection at iterator/count boundaries —
    never artificial hard caps on merely-large-and-valid input.
  - **Out of model:** (1) **abort on genuine OOM from proportionally large
    valid input** (bytes ~ memory). Same contract as pyarrow/numpy/polars;
    gometry guarantees **no amplification** (memory bounded *relative to*
    input), not infallible allocation under an arbitrary rlimit. Callers
    parsing untrusted data must bound **input size** at the trust boundary. A
    bare unsized/infinite iterable is deliberately supported without an
    artificial element ceiling (user decision): `Polygon(holes=...)` is the
    known proportional-OOM edge because each hole owns `CoordSeq`
    `Arc<[f64]>` columns and std has no fallible Arc-slice constructor. Do not
    restore `MAX_BARE_COLLECT`; the collector's outer growth remains fallible.
    (2) **Forged/lying duck-typed Arrow-C ABI producers** (see capsule bullet
    above). (3) **No flat byte or element-count ceiling on parsers** — the
    caller already holds the bytes. But gometry is NOT cap-free, and the
    inventory is exactly this (verified 2026-07-23; keep it accurate):
    the overridable coverage knob `max_cells` (default `1_000_000` for
    fixed-depth grids and fixed-level S2, `None` = unlimited; S2's adaptive
    coverer with `level` omitted separately defaults `target_cells` to 8, the
    S2-idiomatic approximation target); generated-work ceiling 16M
    (`geometry/expansion.rs`); recursive WKT/WKB/GeoJSON ingestion depth 128
    (`io/mod.rs`); CRS-holder traversal depth 4 (`py/crs/parsing.rs`);
    non-coverage grid collectors and ordinary uncompact 1M (`grid/mod.rs`);
    transform-bounds densification 10,000 (`crs/mod.rs`); CRS search 1,000
    (`crs/catalog/mod.rs`). Recursion and holder depths are termination/stack
    protections; the generated-work and result-count limits are budgets. Do not
    add a cap without adding it here. Public prose: `docs/about/security.md`,
    `docs/ecosystem/arrow.md`.
- No GEOS / GDAL / PostGIS / Polars / DataFusion / DuckDB in the core.
- No allocator feature knobs in gometry v1: use the platform allocator unless
  a measured, always-on production workload proves otherwise.
- No Rayon or thread-pool dependencies in gometry v1. The current design
  optimizes single-threaded efficiency and explicit batching.
- No `cargo-deny`.
- Licenses (monorepo-wide convention): ALL license files are GENERATED build
  artifacts — **gitignored, never committed**. `.github/scripts/gen_licenses.py
  <pkg>` renders the package's own texts from pyproject's SPDX expression
  (`LICENSE-APACHE.md` + `LICENSE-MIT.md` here, one file per OR choice), and
  for rusty packages the third-party bundle: the wheel statically links the
  Rust crates (cdylib), so it is a BINARY redistribution
  and must ship their notices. The shared generator
  (shared `.github/scripts/about.toml` + `about.hbs`) runs `cargo about` over the
  package's dependency graph; the `LICENSE-*` name matches maturin's PEP 639 default
  glob, so maturin auto-bundles it into the wheel's `.dist-info/licenses/` with NO
  `license-files` entry. CI's `build` job (in `.github/workflows/_test.yaml`) installs
  cargo-about (prebuilt, via `taiki-e/install-action`) and runs the generator BEFORE
  cibuildwheel/`uv build`, on every PR — so the bundle is always fresh and generation
  FAILS loudly on a license not in `about.toml` `accepted`. `cargo-about` comes from
  the monorepo `shell.nix` locally (bare name, like ruff/pyright); to build a release
  wheel locally, run the generator first. Keep the CI pin (`cargo-about@<ver>` in
  `_test.yaml`) in sync with shell.nix's version.
- Free-threaded CPython is a v1 target: `#[pymodule(gil_used = false)]` keeps
  PyO3 0.29's free-threading-safe posture instead of forcing the GIL back on.
  PyO3's `#[pyclass]` macro compiler-checks Send+Sync for every class
  (`assert_pyclass_send_sync`, independent of `gil_used`); this is not just a
  comment assertion. The `ArrowSchemaPtr`/`ArrowArrayPtr` wrappers in
  `src/py/arrow_c/native.rs` (each with a hand-written `unsafe impl Send` +
  `unsafe impl Sync`) are the deliberate unsafe exception, justified by
  Arc-pinned read-only Arrow access.
  Send+Sync proves memory-safety eligibility, not absence of logic races, so
  shared-mutable-state stress tests remain the verification story. The shared
  matrix runs rusty packages on Linux CPython 3.14t and asserts the GIL is
  actually disabled before pytest; once gometry is tracked, that lane is part
  of every ordinary CI run.
- No PyO3 `freelist` on any pyclass: 0.28's freelist takes a lock for
  free-threaded safety, and A/B release benchmarks (array iteration,
  `grid_disk` cell fan-outs) showed 0-20% regressions, never wins. Re-measure
  before ever reintroducing one.

## Design decisions (don't re-litigate)

- **Epoch through `to_crs` is dynamic-aware (2026-07-10).** Omitted `epoch=` PRESERVES the source
  epoch when the target CRS is dynamic (`crs::is_dynamic`, cached `info()` datum introspection,
  ensemble-member-aware — EPSG authority decides: 4326/3857/4258 dynamic, 2180 static) and CLEARS it
  when static; explicit `epoch=` always wins; same-CRS no-ops always preserve. Policy lives in ONE
  place (`GeometryTransformFrame::new`); docs in `docs/guide/crs.md`.
- **NO fuzzy, randomized, mutation, or generative testing anywhere (user decision, reaffirmed
  2026-07-23).** No Hypothesis/proptest framework, and no mutation/differential fuzz campaigns:
  they are unreasonably slow, provide little to no benefit, and complicate a great deal. The suite
  is a high-quality, hand-written **deterministic** corpus — a small number of well-crafted tests
  beats a large generated one. The 75k-mutation in-core differential fuzzer was DELETED on
  2026-07-23 along with its `fuzz` marker; do not reintroduce it or any successor.
  **`tests/test_crs_incore_parity.py` is NOT fuzz and stays**, including its `@pytest.mark.exhaustive`
  EPSG observer: it is a deterministic, complete catalog scan and the load-bearing safety net that
  caught a real 34 km wrong-formula admission. Coverage gaps may be discovered with
  `tools/coverage.py`, then captured by hand as focused deterministic tests. Cut no-benefit
  validation and extensive tests; do not re-test what a third party already guarantees.
- **Polars is an explicit binary-Series boundary in v1.** `GeometryArray.to_polars()` encodes a
  whole column through native batched WKB/EWKB; `gm.from_polars()` decodes it. There is no Series
  or Expr namespace, Python UDF dispatch, PyArrow dependency, native plugin, or framework-class
  mutation. Compute on `GeometryArray`, then convert at the storage boundary.
- **Empty geometries carry axes (2026-07-10).** `Shape::Empty(EmptyKind, CoordinateAxes)` with
  EmptyKind = {Point, Polygon, MultiLineString, MultiPolygon, GeometryCollection}; MultiPoint/
  LineString empties carry axes in their zero-length seqs. `Shape::typed_empty(kind, axes)` is the
  normalizing ctor (XY containers keep the canonical empty-`Vec` form, so value equality can never
  split across representations). `POINT Z EMPTY` round-trips WKT/WKB (axes-typed codes, NaN
  sentinels); eq/hash are axes-sensitive (`POINT Z EMPTY != POINT EMPTY`); topological `equals()`
  is unchanged; GeoJSON deliberately flattens (RFC 7946 has no dimensional empty); `force_2d`/
  `force_3d`/`set_z`/`set_m` retag empties.
- **Fancy-selection gather is memoized, not fused (2026-07-10, measured-first).** A
  `RowSelection::Gather` array resolves packed columns through `gathered_memo`
  (`Arc<OnceLock<Arc<GeometryArrayStorage>>>` on the array, the `bounds_cache` pattern): the first
  packed op gathers once, later ops reuse contiguous columns zero-copy (`normalized_gather_storage`
  in `src/array/packed_gather.rs`, wired through every packed-column chokepoint). Full
  selection-fusion into kernels was REJECTED as unproven churn — contiguous columns are what the
  SIMD reducers want; escalate only with new A/B evidence.

- **FFI-seam perf — measured findings (2026-06-29 pass; don't re-attempt the rejections without NEW
  steady-state evidence).** The Python↔Rust boundary (ingest→compute→export→interop), not the kernels, is
  where operators pay. **DELIVERED, real:** `query_pairs` borrowed-refine (drops the per-outer-row
  `Shape::clone`) + two antimeridian correctness fixes (insert/remove envelope widening; array-dwithin bounds
  widening); GeoPandas vectorized round-trip (`series.to_wkb()`, not `[g.wkb for g]` — ~11×); batch
  `from_wkt`/`from_wkb` → `Vec<Shape>` fast path (skips the `Vec<PyGeometry>` staging, ~5× on point arrays —
  but EWKT/EWKB embedded-SRID rows STILL reconcile via `Frame::resolve_items`, gated on a per-row SRID scan);
  native-Rust `from_features` (text/buffer/mapping/iterable; properties stay opaque Python objects); GeoJSON `bytes` lane + geometry-only
  FC dict walker; bulk WKB/`__geo_interface__` `geometry_items` lane; H3 `cover` reuses the default-overlap
  tiling for membership (~2×); packed geodesic **length** (1.5×). **REJECTED by measurement (cold-benchmark
  artifacts / within-variance / regressions):** shared thread-local `PJ_CONTEXT` (the "213× cold churn" is
  per-distinct-CRS descriptor work, ALREADY cached; steady-state churn 1.4×); mixed `to_arrow` (already
  optimal; the "25ms" was a cold `pyarrow`-import artifact); routing `query_pairs` through the
  convex-containment fast path (interleaved A/B showed 1.5–1.7× **slower** — per-row scalar-query setup doesn't
  amortize across a self-join); packed geodesic **area** is within-variance (geodesic `PolygonArea` math is
  inherent — kept only for area/length code uniformity, bit-identical, NOT a perf claim). **LESSON:** a "perf
  saturated" verdict needs END-TO-END steady-state measurement, and EVERY optimization gets an interleaved A/B —
  static analysis and cold/artificial benchmarks were wrong three times this pass.
- **GeoJSON output key order** (DONE 2026-06-29): the `to_geojson` writer (`src/io/geojson.rs`
  `write_geojson_to`, a manual string builder) now emits `type` FIRST — the RFC 7946 / shapely / GDAL
  convention — e.g. `{"type":"Point","coordinates":[1.0,2.0]}`. The writer-parity test compares PARSED values
  (order-independent), not strings. The READER still accepts any key order (parses `coordinates`-first input
  fine). (`serde_json` `preserve_order`/`arbitrary_precision` were the WRONG fix — the former changes all
  output + broke a test, the latter breaks the typed coordinate deserializer; the manual-writer reorder is the
  right one.)
- **Featureset philosophy & domain line (LOCKED 2026-06-25).** gometry is THE one-stop, best-in-class,
  all-in-one package for Python geo developers — a *comprehensive, excellent* featureset (1 trusted dep, not N;
  supply-chain reduction is real), never a minimization exercise. **Default = make every feature excellent
  (well-done + optimal), not blind-cut; don't reduce usability/experience without a clear reason; turn weak
  spots into opportunities (best-in-class) rather than delete them.** Inclusion bar: operates strictly on the
  geometry/spatial data AND practically useful AND done well. **Prefer implementing a sensible published
  spec/algorithm OURSELVES in Rust** (polylabel, Chaikin/Catmull-Rom smoothing, …) over pulling a crate —
  highest quality, most efficient integration with our storage/dispatch/SIMD, logically-organized internals
  (reserve deps for PROJ/GEOS-class complexity, per platform-first). **Cut ONLY**: half-assed partial overlaps a
  specialized lib does exhaustively + hands off cleanly via numpy/Arrow (`interpolate`→scipy/scikit-gstat), and
  true impractical dead-weight (geometry clustering APIs). **Integrate, never reimplement** the
  adjacent specialized domains (stats/ML→sklearn, geostats→scipy, viz→lonboard, distributed→dask/DuckDB,
  routing→networkx) via clean numpy/Arrow handoffs (handoffs verified clean). **Do NOT add**: map-matching,
  network/graph, Minkowski, medial-axis, kriging, raster, GML, FlatGeobuf, Maidenhead, bespoke streaming.
  **DX = one-obvious-way / no redundant aliases.** **Benchmark match-or-smash**: every op with a competitor
  equivalent is registered in the broad `benches/python/bench_competitors.py` catalog. The bounded release
  manifest selects marketed and hot-path pairs from that catalog; a statistically significant regression in
  this release surface is a release blocker. **Tests** prove correctness (vs spec/oracle), lock against regressions, and cover the edge
  cases that occur in practice.

- **NumPy-native bulk outputs (the v1 output doctrine).** The target public rule is:
  numbers/masks/index ids/curve keys/bounds return fixed-width, read-only `numpy.ndarray`
  lanes; geometries return `GeometryArray`; ragged row groups stay `Groups`. NumPy is a
  real runtime dependency. Index scalar query/candidates/nearest, clustering labels, and
  join/query_pairs already follow this rule (`IndexPairs` is no longer a public return shape);
  `Groups.values`, `.offsets`, and integer rows are read-only `int64` ndarray views over
  stable CSR storage, with rebased sliced offsets allowed to allocate. **The migration is
  COMPLETE**: every metric/predicate/curve/bounds/index lane returns ndarray; the old public
  buffer-vector classes (`Float64Vector`/`BoolVector`/`IndexVector`/`UInt64Vector`/`BoundsMatrix`/
  `IndexPairs`) are DELETED (`Float64Vector` survives only as the internal `_Float64Buffer` arrow
  mover). Never reintroduce a public buffer-vector class — new numeric surfaces return ndarray.
  `GeometryArray`/`Coordinates` set `__array_ufunc__ = None` and expose `__array__` (object /
  float64); coords `.x/.y/.z/.m` are read-only float64 ndarrays (NaN for absent Z/M) plus
  `get_coordinates(...)`. Text/bytes outputs stay Python lists; genuinely ragged diagnostics need
  an explicit row grouping design instead of ad hoc `list[list[...]]`.

- **Row-id sorts use `sort_row_ids`** (`py/index/mod.rs`): index row ids are unique values in
  `0..rows.len()`, so dense result sets sort via a bitmap pass (O(universe/64 + n)) instead of
  pdqsort — this flipped `candidates` from 0.44-0.66x to ~1.05x vs GEOS. Never add a plain
  `sort_unstable()` on row-id sets.

- **Algebraic-float placement (2026-06-24).** `f64::algebraic_{add,sub,mul,…}` (stable since Rust 1.98;
  the pinned toolchain accepts them ungated — `portable_simd` is the only feature gate in `lib.rs`) are
  allowed ONLY in
  measurement/aggregation kernels whose result is inexact by nature (signed area, length, distance sums,
  centroid coords, weighted means) — there they break the serial-accumulator carry so LLVM reassociates +
  vectorizes. FORBIDDEN in anything feeding a topology/robust DECISION (orientation, the EFT/two-product
  code in `segments.rs`, the `robust` crate, ring membership/crossing-parity, snapping, predicates, overlay,
  any shoelace-as-orientation `>0.0` probe): reassociation flips ties near zero and corrupts the branch.
  `shoelace_columns<const ALGEBRAIC>` encodes this — `::<true>` = area measurement (algebraic), `::<false>`
  = orientation decision (deterministic plain). Enforced by `tools/gates/_check_algebraic_float.py` (whitelist of
  named measurement fns) + the no-`call fma`/`vfmadd` asm discipline. NEVER whitelist a decision path; never
  put `algebraic_mul`/`mul_add` in a SIMD lane body (re-enables a per-lane libm `fma` call on the pre-FMA
  baseline — a measured regression).

- **SIMD column-API (2026-06-24).** The unified family in `geometry/access/reduce.rs` —
  `simd_reduce_f64`, `simd_map_f64`/`try_simd_map_f64`/`simd_xy_map_f64`, `pair_map4_guarded_f64`,
  `pair_select_mask`, `column_sum`/`column_mean`/`column_mean2`, plus `column_all_finite`/`column_minmax`/
  `columns_within` — is THE way to write columnar SIMD: one shared `as_chunks::<REDUCE_LANES>` + masked-tail
  + `REDUCE_SIMD_MIN` crossover, comptime 8-lane. The measurement-vs-bit-exact choice lives in the CALLER's
  closure body (plain ops ⇒ bit-identical-to-scalar for decisions like dwithin; `algebraic_add` in the
  accumulator for measurement) — NOT a runtime/const flag (the driver can't see the closure; the source +
  asm gates enforce it). Distance = the GUARDED pair map (hypot overflow/underflow rescue; bad lanes fall
  back scalar); dwithin = the squared-compare mask (bit-identical ties). No runtime multiversioning /
  `#[target_feature]` / `#[inline]`. Any refactor onto these drivers must be byte-identical asm. Measured:
  packed point×point distance/dwithin ~1.4× @10k (grows at scale).

- **TYPE_CHECKING sentinel — shipped Python only (2026-06-24).** Shipped `python/gometry/*.py` use the
  import-less `TYPE_CHECKING = False` sentinel (EXACT name — type-checker recognition of the literal form is
  name-based; an alias like `_TYPE_CHECKING = False` is NOT recognized) + `if TYPE_CHECKING:`, not
  `from typing import TYPE_CHECKING`. Scope is shipped code ONLY (tools/tests/benches/examples are out of
  scope — internal). The top-level facade deletes the sentinel after its conditional imports so
  `gm.TYPE_CHECKING` never pollutes runtime/IDE discovery. `typing.cast` is a real CPython call;
  `[tool.pyright]` (with `venvPath`/`venv` so a bare
  `pyright` resolves the env) enables `reportUnnecessaryCast` + `reportUnnecessaryTypeIgnoreComment` to gate
  redundant ones; `include` is scoped to `python/gometry`. pyright is gated through
  `tools/gates/_check_typing_runtime.py` (full-project scope locked at 0 errors, run by pytest
  together with mypy over the conformance + negatives corpora).

- **One canonical constructor / ingest (2026-06-24).** `Point(lon, lat, crs=4326)` is THE geographic-point
  constructor (x=lon, y=lat — the WKT/GeoJSON/stored order); `latlon` was DELETED (a lat-first constructor
  is a competing axis-order convention that confuses more than it helps). `GeometryArray([...])` is THE
  iterable ingest (with `object`/ndarray `@overload`s for the `__geo_interface__` path); `from_numpy` was
  DELETED (pure synonym). Do not reintroduce `lonlat`, `gm.array`, or `gm.shape`.

- **in_core CRS fast path (2026-06-24).** `src/crs/in_core/` is a closed-form projection registry
  (`ProjectionKernel` trait + erased `ProjSetup`/`InCoreTransform` One/Two pipeline; `tmerc`/`webmerc`/`lcc`/
  `stere` kernels, ellipsoid-general) that bypasses bundled C PROJ for ~3,569 EPSG CRS (TM incl. UTM,
  Pseudo-Mercator, LCC 1SP/2SP, polar stereographic) — measured **1.5–3.8× faster than the PROJ FFI,
  bit-identical (≤1.3e-8 m)**. The STRICT admission gate (`admission.rs`) admits ONLY: horizontal
  geographic/projected, same celestial body, Greenwich PM, degree/metre axes, no grids/steps/epoch/dynamic
  datum, finite params, and **EXACT-same-datum (authority:code — NEVER by name; cross-datum / "looks-null"
  → PROJ; NO Helmert engine** — a closed-form Helmert would be a 30-70× silent regression vs PROJ's NTv2
  grids). A method is admitted ONLY if its kernel computes that method's EXACT math: 1024 (spherical
  Pseudo-Mercator) yes; 9804 (ellipsoidal Mercator) NO → PROJ; 9809 oblique stereo NO → PROJ; US-survey-foot
  axes NO (metre-axis gate). THE SAFETY NET (load-bearing): `tests/test_crs_incore_parity.py` has an
  EXHAUSTIVE OBSERVER that scans every EPSG projected CRS and asserts that any CRS admitted to in_core
  (proj_pipeline cold) matches pyproj — so a wrong-formula admission can NEVER silently diverge (it caught
  the 9804 bug after a 34 km divergence). PROJ stays the authoritative fallback + the differential oracle.
  Keep the gate conservative; every admission must be observer-covered.

- **API naming rules (the v1 polish doctrine).** (A) *Dimension verbs + pure setters* —
  `force_2d()` drops Z and M (the one obvious flatten; use it, NOT `set_z(None).set_m(None)`) and
  `force_3d(z=0.0)` fills only vertices that lack Z (existing Z kept; M passes through). `set_z(value
  | None)` / `set_m(...)` assign an ordinate at EVERY vertex (overwriting) or CLEAR it with `None`
  — there is NO `overwrite` flag (`force_3d` owns fill-missing-Z; fill-missing-M is intentionally
  not offered — Z is a spatial dimension with force verbs, M is an attribute). Other guard flags
  stay `overwrite=` (`set_crs(*, overwrite=False)` — NOT `force=`). `interpolate_m(start, end, *,
  overwrite=False)` is the
  along-line M ramp (NOT `add_measure`). (B) *facts → `@property`* (per the API Constitution):
  `is_*`/`has_*` (incl. `is_convex`), `area`, `length`, `bounds`, and the cell metrics (`cell.area`,
  `edge.length`) are properties. `gm.area`/`gm.length` survive as free functions for the `unit=`
  override and raw-iterable input (Rule 4), and default `unit` like the properties do; unary
  transforms/measures are instance methods.
  None-policy: on `set_*`/mutable-config `None` clears; on filters/format/optional-selection
  params `None` means unspecified. Candidate-geometry predicate params are `geom` (coverage +
  PreparedGeometry); collection inputs (array ctor, `index`/cluster) stay `values`. CRS
  `to_2d`/`to_3d` STAY (they convert CRS *definitions*, not ordinates — distinct from the geometry
  `force_2d`/`force_3d` verbs).
  (C) *Magnitude vocabulary*: coordinate-space thresholds are `tolerance`
  (`simplify`/`snap`/`remove_repeated_points` — raw coordinate units,
  CRS-independent); real-world lengths are `distance` (`buffer`/`offset_curve`/`dwithin`/
  LRS — CRS-aware via `crs::MetricModel`, native units by default). One concept, one spelling; never mix them.
  (`snap_to_grid` is the exception: its argument is a grid *spacing* `size` — accepting `(sx, sy)`,
  matching PostGIS `ST_SnapToGrid`/GEOS `gridSize` — not a `tolerance`.)
  Public affine parameters are descriptive (`x_offset`/`y_offset`,
  `x_factor`/`y_factor`, `x_angle`/`y_angle`); a pandas DataFrame is the standard
  `GeometryArray.to_pandas().to_frame()` chain; H3 vertices expose their location as `.point` while cells retain
  `.center`. These have no legacy aliases.
  (D) *Bbox/ordinate-extent family are properties* — `bounds`, `bounds_3d`, `min_z`/`max_z`/
  `z_range`, and the symmetric `min_m`/`max_m`/`m_range` are all `@property` (cheap bbox-derived
  accessors, scalar and `GeometryArray`); a free function exists only where the bulk form is
  load-bearing (`gm.bounds`, not the niche rest). The M-aggregate lane skips the column-native
  fast path (`m_extreme_lane`, per-shape) — M is niche; Z keeps its SIMD `z_extremes_rows`.
  (E) *Multipart members are `.parts`* (the accessor matches the free function `gm.parts`; the
  accessor returns a lazy `GeometryParts` view, the free function the materialized `GeometryArray`
  — the scalar-view/bulk-array split, like `.coords` vs `get_coordinates`). (F) *Multi-output ops
  return a NamedTuple ONLY when the shape is mixup-prone with **no positional mnemonic*** — same-typed
  fields under a single input, where nothing pins the order. `Extremes` (W/S/E/N) and
  `PolygonizeResult` earn names this way. `Features` is the deliberate record exception: a frozen slots
  dataclass with validated aligned tuple columns, field-wise unpacking/pattern matching, no misleading
  field-count `len()`, and a bounded repr. Missing feature properties normalize to `{}`, explicit GeoJSON
  `null` remains `None`, a scalar properties `Mapping` broadcasts, and ids never scalar-broadcast;
  `to_feature*` side data is keyword-only. Everything else stays a **plain tuple** (a NamedTuple is never cheaper —
  from Rust it allocates the tuple *plus* a Python-level type-call — so it must buy real swap-protection):
  *distinct-typed* outputs have no mixup risk (`dissolve` → `(GeometryArray, list[keys])`,
  `value_counts`/`factorize`, the `(indices, distances)` lanes); and *same-typed-but-positionally-obvious*
  outputs are pinned by a built-in mnemonic and stay plain — `nearest_points` → `(Point, Point)` (first =
  point on `left`, the argument order), `geocode.osm_shortlink_location` → `(lon, lat, zoom)` (lon-first per
  `Point(lon, lat)`). Their docstrings document the unpack order; that is the mixup mitigation, not a wrapper.
  (Refined from a blanket "all multi-output → named tuple" by the tuple-first lesson.)
  (G) *3D missing-Z policy*: extent accessors (`bounds_3d`/`min_z`/...) describe what is there
  (`None` scalar / `nan` array element); 3D metrics (`length_3d`/`distance_3d`) require Z, so the
  SCALAR raises `InvalidGeometryError` (`GeometryErrorKind::MissingZ`) while the ARRAY degrades per-element to `nan` (one
  missing-Z row never fails the batch — the numpy-native nan-row doctrine). Points/empties are
  `length_3d == 0` (never MissingZ). (H) *`__match_args__` is mandatory* on every geometry leaf
  (incl. `LineString → ('coords',)`, so `Polygon(LineString(_), _)` recursion is complete) and
  every multi-field result container — gated by `tests/test_pythonic_protocols.py`.

- **Coordinate epoch ⟹ CRS (a coordinate epoch is meaningless without a CRS).** Per-geometry
  epoch is `Option<f64>`, canonicalized at every ingress (`coordinate_epoch_option` rejects
  non-finite and maps `-0.0 → 0.0`), so epoch equality is plain `==` everywhere via
  `metadata::epochs_equal` (the `float_cmp` allow lives there once) — NEVER `to_bits` for
  equality (`to_bits` is hash-key only). There is no `CoordinateEpoch` newtype (canonicalization
  at the boundary is the right depth). The `epoch ⟹ crs` invariant is enforced at EVERY ingress
  via `guard_epoch_requires_crs`: constructors (through `parse_crs_epoch`/`typed_with_crs_epoch`),
  `set_epoch`, the `GeometryArray` ctor, and the serialized boundaries (`deserialized_epoch` on
  pickle + Arrow). `set_epoch(value | None, *, overwrite=False)` is the assign/clear setter;
  `set_crs(None)` CLEARS the epoch (CRS-free + epoch is incoherent). Geometry/array `to_crs(crs,
  *, epoch=None, ...)` takes ONE `epoch=` (the OUTPUT epoch; source = `self.epoch`) — NOT the raw
  transforms' `source_epoch`/`target_epoch` pair, which stay ONLY on `gm.crs_transform`/
  `gm.crs_apply`/
  `roundtrip`/`transform_bounds` + `CRS.operation(s)`/`operation_at` (no geometry metadata there).
  `parse_geometry_transform_options` (no epoch fields) backs the geometry surface;
  `parse_transform_options` (both epochs) backs the raw surface.

- **Coverage code is dedup'd via SHARED HELPERS, not a universal macro.** `coverage_ops.rs`
  holds the shared iterator/membership/boundary helpers; GeohashCoverage/TileCoverage blanket-
  delegate rectangular-grid pymethods from there, while H3Coverage/S2Coverage keep hand-written
  `#[pymethods]` that call the SAME helpers (`coverage_members`/`member`/`member_point`/
  `coverage_getitem`/`coverage_iter`/`coverage_to_polygon`). The four systems have genuinely
  different internals (H3 lazy `OnceLock` membership, S2 eager leaf-range classify, rect prefix
  buckets) and depth shapes (S2 has FOUR level params vs others' one), so a one-size-fits-all
  pymethods macro was REJECTED — it would trade logic-clarity for over-parameterized indirection.
  The real dedup (the predicate logic) is already shared. compact/uncompact/with_parents/to_polygon
  transform only the visible `cells` and REUSE
  the source-keyed membership unchanged (no depth-aware membership needed — predicates answer
  against the source, not the cell representation).

- **One crate error, one boundary seam, one exception hierarchy.** Rust core returns
  `crate::error::Result<T>` (`Error(Box<ErrorKind>)`, pointer-sized — the `== 8` assert is the
  contract; domain kinds `GeometryErrorKind`/`CrsError`/`IoError`/`FrameError` live in their owning
  modules and carry fields unboxed because the outer Box pays the freight). The ONLY Rust→Python
  conversion is `impl From<Error> for PyErr` in `src/py/errors.rs` — an exhaustive no-wildcard
  match, so adding a variant forces a deliberate class decision. Python side: 7 classes rooted at
  `GeometryError(ValueError)` (`json.JSONDecodeError` precedent — `except ValueError` always works);
  `GeometryTypeError` dual-bases `(GeometryError, TypeError)` (`numpy.AxisError` precedent, built
  via `type()` at module init because `create_exception!` is single-base). Raise-class lanes:
  structured kind for reused domain failures; `ClassName::new_err(...)` directly for one-off
  boundary failures with domain meaning; builtins ONLY for Python protocol semantics (wrong
  Python type, `IndexError`, `BufferError`, `StopIteration`, imports). Parameter-value violations
  raise `GeometryError`; geometry CONTENT/structural rules raise `InvalidGeometryError`; wrong geometry
  KIND raises `GeometryTypeError`; tokens raise `GeometryError` with the canonical message.
  There is NO `GridError` (demoted pre-1.0): grid depth parameters out of range are
  `GeometryError` value lanes; invalid cell ids/tokens/quadkeys are `ParseError` with `.format`
  tags (`'h3'`/`'s2'`/`'geohash'`/`'tile'`/`'quadkey'`); empty-geometry coverage requests are
  `InvalidGeometryError`.
  `GeometryArray.index` ("not in array") and `__format__` keep bare `ValueError` by Python
  convention. Message grammar: lowercase start, no trailing period, public
  kwarg names exactly, and repr for user-supplied strings. Focused regression
  tests pin public messages that callers realistically match; do not restore a
  parallel source-grammar policy scanner. Raises docs are contract-checked by
  `tools/stubs/_doc_coverage.py` (vocabulary =
  the top-level exception inventory + allowed builtins; bare `ValueError` banned; the fallible-family floor in
  `required_raises` must be documented — and is per-surface: the array-degrading linref ops carry the
  `InvalidGeometryError` floor only on the scalar surface, see degrade policy below).

- **Validated-number newtypes (`src/numeric.rs`).** Domain-constrained `f64` parameters cross into the
  kernels as `#[repr(transparent)]` `NonNegative` / `Positive` (zero-cost, `size_of == f64` asserted),
  built ONCE at the boundary via `try_new(name, value)` (emits the canonical `GeometryErrorKind::{NonFinite, NonNegativeFinite, PositiveFinite}`, which map to the public base `GeometryError`,
  with message `, got {v}`); kernels take the newtype and trust `.get()` — DbC made
  type-level, no per-call re-check. Boundary parsers (`validate_distance`→`NonNegative`,
  `validate_max_segment_length`/`parse_buffer_miter_limit`→`Positive`, `accuracy_option`→`Option<NonNegative>`)
  return them. Adjacent type-safety: `quad_segs` is `NonZeroU32` (no `.max(1)`/`==0` re-guards),
  `EllipsoidShape { semi_major: Positive, flattening }` validates once (flattening is half-open `[0,1)`),
  `CdtRefinement` is an enum (no derived `active` bool), `MeasureRange` carries the `start<=end` linref
  invariant (three domain ctors preserving distinct messages), `Strictness{Strict,Lenient}` replaces the
  overlay `strict` bool. Magic geographic constants are named in `src/boundary/geographic.rs`
  (`WEB_MERCATOR_MAX_LATITUDE`, `MIN/MAX_LATITUDE/LONGITUDE`) and `Ring::MIN_VERTICES_OPEN/CLOSED`.
  REJECTED (don't re-litigate): a `Finite`/`UnitInterval` newtype (no genuine consumer — fraction domains
  differ: densify `(0,1]`, projection `[0,1]`, snap `(0,1)`), a `Degrees`/`Radians` newtype (the public
  `radians: bool` API stays, so a wrapper relabels f64 without removing the bool branching — ceremony,
  compiles identically), and an `ensure!`/`bail!` macro (unadopted). `Z`/`M` ordinate newtypes, const-
  generic `Coord<N>`, and `f32` storage all fight the SoA/FFI zero-copy layout — declined.

- **Error crate verdict (platform-first, don't re-evaluate).** `thiserror 2.0` is the ONLY error crate.
  DECLINED: `anyhow`/`eyre`/`snafu`/`miette` (untyped — would break the exhaustive no-wildcard
  `From<Error>` seam, the system's strongest guarantee), `garde`/`validator` (heavyweight, wrong fit),
  `ordered-float` (`f64::total_cmp` + `to_bits` already used everywhere, zero-cost), `bitflags`
  (`CoordinateAxes` is already a clean `u8` flag), `smallvec`/`arrayvec` (stdlib `[f64; N]` + `try_from`
  covers the 2/4-ordinate cases), `zerocopy` (`bytemuck` already present and used correctly).

- **Structured error attributes** (`with_attrs` in `src/py/errors.rs`, typed via the `Attr` enum) let
  handlers branch without parsing messages, with class-level `None` defaults registered so hand-built
  instances never `AttributeError`: `ParseError.format`; `CRSMismatchError.left/.right` (+ `.index` for
  collection item-disagreement); value-lane `GeometryError.param`/`.value` (the offending kwarg + value);
  `CRSError.crs` (unit-mismatch); `InvalidGeometryError.operation` (overlay op). Add new attrs the same way:
  widen `Attr`, attach in the `From<Error>` arm, register the `None` default. Every such attr is typed
  `X | None` in the stub (it is `None` on instances of a different error lane) — see the nullability gate.

- **Optional-with-default params vs genuine `None` (convention).** A scalar parameter that has a sensible
  default uses pyo3's native literal default and a plain type (`#[pyo3(signature = (*, min_resolution = 0))]`
  + `min_resolution: i64`), so it is typed `int = 0` and REJECTS `None`. Do NOT model "optional with
  default" as `Option<&Bound>` + `signature(= None)` + a `*_or_default(None, N)` parse — that accepts a
  meaningless `None` (a redundant alias for the default), forces a lying `int | None` stub, and makes the
  text_signature diverge from reality. Reserve `Option`/`| None` for parameters where `None` is a DISTINCT
  value (`crs=None`, `unit=None`, `epoch=None`, `Grid.s2(level=None)` adaptive budget). The cell-coverage
  floor params (`min_resolution`/`min_level`/`min_precision`/`min_zoom`) follow the plain-default form.

- **Stub nullability gates (two lanes, both in `check-all`).** `mypy.stubtest` verifies that every
  runtime class attribute whose value is `None` is typed `X | None` in the stub (the
  structured-error-attr class of bug). The `rust-nullability` check covers what NO runtime signal can
  see: it reads the Rust source for `Option<T>` getters / `#[pyo3(get)]` fields and requires the stub
  surface to admit `None` (both live in the shared `pyo3stubs` tool so all packages gain them).
  Param-type accuracy still relies on the source-honesty convention above + the text-signature
  default-parity text-signature gate.

- **Array linear-referencing degrades per-row** (matching the Z-family). The ARRAY forms of
  `line_locate`/`line_interpolate`/`line_substring` (selected with ``basis='distance'`` or ``'m'``) degrade a per-row
  GEOMETRY-DATA failure (`EmptyLinework`/`MissingMeasure`/`NonMonotonicMeasure`, via
  `is_degradable_line_row`) to `nan` (float) or an output-type EMPTY sentinel (geometry), rather than
  aborting the whole batch — like `distance_3d`/`length_3d`/`shortest_line`. SCALAR forms still RAISE
  (the deliberate ergonomic-scalar / columnar-array split). Wrong-kind and parameter errors STILL raise
  on both paths (only those three data conditions degrade).

- **`Geometry` is a base class with seven real typed leaf subclasses** (`Point`, `MultiPoint`,
  `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`, `GeometryCollection`), all backed
  by the single Rust `Shape` enum — the base holds all data/shared methods; leaves carry no
  fields (zero-sized `#[pyclass(extends = PyGeometry)]` markers). Every geometry returned to
  Python is the leaf matching its `Shape` variant, so `isinstance(g, Point)` and precise stub
  returns hold. The single seam is the `Typed(PyGeometry)` newtype: its `IntoPyObject` dispatches
  on the variant via `GeometryKind::of`. **Any `#[pymethods]`/`#[pyfunction]` that returns a
  geometry to Python must return `Typed`** (helpers `PyGeometry::typed_shape`/`typed_with_epoch`,
  or wrap), never a bare `PyGeometry` — the latter silently yields a base `Geometry` (no compile
  error). Internal Rust that needs a raw geometry keeps using `with_shape`/`with_epoch`. Leaf-only
  members live on the leaf impl (`Point.x/y/z/m`, `Polygon.exterior/interiors`, `.parts` + the
  sequence protocol on multiparts) reached via `PyRef::as_super`. `exact_geometry` uses `cast`
  (subclass-accepting), not `cast_exact`. Argument extraction is unaffected (leaves *are*
  `Geometry`). The old monolithic single-class model was reverted for v1: precise typing was the
  goal and it is a breaking change post-1.0.
- **CPython protocol surface is part of the API contract.** Data types are `frozen` +
  `weakref` (`Geometry` base covers the leaves), `GeometryArray` is `sequence` + `generic` +
  registered with `collections.abc.Sequence` (sets `Py_TPFLAGS_SEQUENCE`, so `match arr:
  case [a, *rest]` destructures), and `__match_args__` classattrs destructure every leaf
  (`Point` → `('x', 'y')`, `Polygon` → `('exterior', 'interiors')`, multiparts →
  `('parts',)`, cells → `('id',)`, coverages → `('cells',)`). The `& | - ^` operators ride
  `overlay_operator` (named-method defaults; `NotImplemented` for foreign operands —
  binary dunders always defer rather than raise). `__eq__` is value equality and `__hash__`
  agrees (arrays hash row-content, layout-independent, via `collections::python_hash{,er}`
  — one process-stable `ahash` state, never per-call `RandomState::new()`). Rejected
  protocol sugar is recorded in `docs/about/design.md` (no `bytes()`, `round()`, coverage
  operators, or empty singletons) — don't re-litigate. Every runtime-defined dunder must
  appear in the stub (stubtest-gated; `mypy.stubtest` understands the PyO3/CPython slot
  machinery), and `__repr__`/`__str__` stay un-stubbed per typeshed convention.
- **Precise stub typing.** Kind-preserving ops return `Self` (methods) / a bound `_GeometryT`
  TypeVar (free fns) so a typed leaf flows through `to_crs`/affine/clean. `GeometryArray` is
  `Generic[_GeometryT_co]` with a PEP 696 `default=Geometry`, so bare `GeometryArray` means
  `GeometryArray[Geometry]` while `points(...)`/`GeometryArray([pt,...])` yield `GeometryArray[Point]`
  and iteration/indexing preserve the element type. Always-point ops
  (`centroid`/`point_on_surface`/`polylabel`/geodesic `destination`/`interpolate`) return `Point`.
  Every vectorized free function carries scalar/array `@overload`s (`area(geom)->float`,
  `area(arr)->NDArray[float64]`; binary ops get 3 variants), and constructive ops return their true
  leaf unions (`buffer -> Polygon | MultiPolygon`, `concave_hull -> Polygon`, per-input
  `boundary` overloads). Narrowing convention: annotate the types produced by NON-EMPTY inputs
  (empty inputs degenerate to `GEOMETRYCOLLECTION EMPTY` per GEOS convention — not annotated).
- **Typing is a quality of the API, not a public catalog.** There is no `gometry.typing`
  namespace: precise overloads, generics, `Literal` tokens, and structured return types are
  delivered through the handwritten stub. `gometry/_types.py` is private stub/runtime support;
  only the genuine cross-grid domain protocols are public as `gometry.Cell` and
  `gometry.Coverage`. Do not publish operation-token aliases, third-party echo aliases, or
  `Groups` synonyms such as `IndexMatches`.
- **`Geometry` has structural value semantics.** `__eq__`/`__hash__` compare CRS + epoch + exact
  geometry (kind, coords incl. Z/M, vertex order — matching `equals_identical`), via derived
  `PartialEq`/`Eq`/`Hash` on `Shape`/`Ring`/`Polygon` and a custom `Point` impl that compares only
  active ordinates by bit pattern. Topological `equals()` stays the separate operation.
- **Ring winding is named `ccw` (default `True`), not `exterior_cw`** — consistent with
  `box(ccw=...)`. The Rust `Shape::orient_polygons` keeps the internal `exterior_cw` flag; the
  PyO3 boundary passes `!ccw`. There is one ordinate-layout property, `coordinate_axes` (the
  ambiguous `dimension` alias was removed); `topological_dimension` is the separate 0/1/2 one.
- **Docstrings are NumPy-style, authored tooling-safe.** (The monorepo-root `rustfmt.toml`
  does not set `wrap_comments`, so `cargo fmt` leaves `///` prose alone — keep the
  conventions anyway; they are what the numpydoc parser expects.) A blank `///` line
  between each `param : type` block, every indented description on ONE short line
  (≈ ≤ 78 chars of text), `Examples` on standalone `>>> ` lines — runnable, executed by
  `tools/gates/_check_docstring_examples.py` / `tests/test_docstring_examples.py`. After editing
  Rust `///`, run `cargo fmt`, then `python -m pyo3stubs gen-docs --config
  tools/stubs/stubconfig.py`. `buffer`, `simplify`, and `point` are the reference exemplars.
  - **snake_case parameter NAMES must stay bare** (`source_epoch : float`), not
    backticked — griffe's numpydoc parser can't read a backticked name and `properdocs
    build --strict` aborts on it. `clippy::doc_markdown` is turned OFF workspace-wide
    (`[workspace.lints.clippy] doc_markdown = "allow"`) precisely because these `///`
    comments are numpydoc source, not rustdoc prose — so there is NO ident allowlist to
    maintain and `clippy --fix` will not backtick param names. (`See Also` entries and
    inline identifiers in prose may still take backticks by hand.) Gate: `properdocs
    build --strict` must report zero griffe warnings.
  - **The stub and docstrings own the reference content.**
    `tools/docs/griffe_expand_aliases.py` only repairs Griffe's overload-only
    stub members/native aliases, expands private annotation aliases, and links
    resolvable See Also names. It does not rewrite prose or maintain parallel
    member/type registries. Keep Returns/Raises columns accurate at their
    source, and indent Raises continuations so numpydoc does not parse a phantom
    exception. Release check: `properdocs build --strict && .venv/bin/python
    tools/docs/check.py` validates canonical anchors, unique IDs, public
    signatures, and linked See Also entries in the built site.
- **Polygonal-coverage ops are `GeometryArray` methods named `coverage_*`** (`coverage_is_valid`/
  `coverage_invalid_edges`/`coverage_simplify`/`coverage_clean`) plus free-fn duals taking `values`;
  NO `gometry.coverage` namespace, and no rename of the DGGS `Coverage` classes (the collision is
  conceptual only — one docs admonition disambiguates). Kernels live in `src/geometry/coverage/`
  on the EDGE-MAP substrate (a segment present in two rows = exactly shared interface): the
  validator probes unmatched segments (covers-point/T-join, interior crossing, `gap_width`
  near-miss); the simplifier decomposes unique linework into node-to-node chains, VW-simplifies
  each ONCE under a conservative topology guard (original-segment crossing + swept-vertex checks)
  and splices the same chain into both neighbors; the cleaner nodes ALL boundaries, resolves
  nesting into regions (face minus direct children), assigns each region by parent count
  (`CoverageOverlapRule` token for overlaps, narrow-gap merge by `2*area/perimeter < gap_width`),
  and reassembles rows by exact EDGE CANCELLATION — never the boolean engine, whose output jitter
  would break the vector-identical-interface contract. `coverage_clean(grid_size=0.0, gap_width=0.0,
  overlap_rule='longest_border')` never snaps implicitly; valid input is bit/presentation-identical and
  cleaning is idempotent. Deterministic corpora (never property-based tests) cover validity/simplification
  against shapely 2.1.
- **The simplify family shares ONE parametrization and ONE contract.** `simplify(tolerance, *,
  method='vw'|'dp', preserve_topology=True)` selects Visvalingam-Whyatt (`method='vw'`, the
  default) or Douglas-Peucker (`method='dp'`); `coverage_simplify` also takes a distance-scale
  `tolerance`. VW converts internally via `vw_area_tolerance` (`t²/2`, the GEOS
  coverage-simplifier convention) so the same value is comparable/swappable across geometry
  simplify and coverage simplify. `simplify` defaults `preserve_topology=True`: raw algorithm
  first (fast path), `simplify_kept_topology` check, guarded greedy fallback only on breakage —
  one chain kernel (`simplify_chain_guarded` in `coverage/simplify.rs`) generic over the importance
  criterion. Never reintroduce a raw-area parameter (PostGIS's `ST_SimplifyVW` area knob is a
  documented confusion source) or geo's `simplify_vw_preserve` (silent polygon/line asymmetry).
- **`normalize()` is gometry's own canonical form — never re-align it to GEOS.** One
  principle: the lexicographically smallest equivalent presentation (one comparator —
  pointwise, then shorter-first; parts ascending; open lines smaller direction; closed
  lines smallest rotation×direction over the orbit, NO orientation predicate; polygon
  rings min-vertex-first, RFC 7946 winding: shell CCW, holes CW). GEOS's form (descending
  members, CW shells, the uphill-segment `isCCW` for closed lines) was studied via
  a one-off differential campaign (since removed) + upstream source and rejected as legacy;
  oracle tests compare
  equivalence classes, not canonical text. Witness lines (`shortest_line`,
  `minimum_clearance_line`, circle radii) build through `witness_pair` — the axes
  INTERSECTION of the endpoints — so resolvable Z/M carries and a 2D side never gets
  fabricated zeros (mixed-axes pushes are a debug-assert violation).
- **Z/M carry (`OrdinateSource`) is scale-tolerant and indexed.** The boolean engine
  snaps coordinates to an extent-proportional grid, so output copies of input vertices
  come back jittered ~extent x 2^-29 and the segment stage MUST allow an absolute
  `extent x 2^-28` slack (`OVERLAY_SNAP_RELATIVE`) on top of the per-segment relative
  epsilon — a bit-exact carry rejects nearly every non-grid-exact Z overlay with
  `OrdinateDropped`. Exact vertex matches resolve through a `PointKey` hash map
  (same equivalence as `same_point`); misses query a lazy R-tree over the source
  segments in input order (full scan was 34 ms of a 35 ms Z union). Keep both stages
  in this shape; the jittered-staircase case in test_equivalence.py pins the class.
- **`Shape` ops use per-variant `match`, not a visitor/fold trait.** The variant
  arms carry genuinely distinct logic; only the one-line `GeometryCollection`
  recursion repeats, and its combinator differs per method (`sum` for area/length,
  `bounds_from_iter` for bounds, `flat_map` for points). A visitor would add a
  6-method surface per op, hurt inlining on per-geometry hot paths, and trade
  compiler-checked exhaustiveness for opaque dispatch — net-negative.
- **Geometry coordinate storage is SoA at rest.** Scalar `Shape` line/ring/multipoint sequences are
  `CoordSeq { xs, ys: Box<[f64]>, zs, ms: Option<Box<[f64]>> }`, with a `Coordinates` trait for by-value
  iteration + `segment_pairs`/`xy_columns`/`nth_coord`. XY stores 16 B/vertex (vs the old 40-byte AoS
  `Point`) — 2.5× less cache/bandwidth — and the contiguous `f64` columns back gather-free SIMD reducers
  and the bytemuck Arrow/WKB paths. `Point` stays the by-value `Copy` interchange type.
  `GeometryArrayStorage` uses packed point, line,
  and polygon columns (CSR offsets for ragged line/ring/polygon boundaries), with `Mixed` storage
  only for heterogeneous rows. Identity/window/gather row selections preserve cheap views;
  gathered packed columns materialize once through the array's shared memo.
- **`std::simd` (portable, nightly) reduces directly over the SoA columns** (no transient gather),
  gated by focused benchmark evidence:
  - `line_length` / `shoelace_columns` (`ring_area`) read `xs()`/`ys()` straight from `CoordSeq` and SIMD
    the deltas/cross-products — the old AoS code gathered into temp `Vec`s first; that gather is gone.
    `hypot` is a scalar libcall LLVM can't vectorize, so SIMD `sqrt` over column deltas with a
    scalar-`hypot` overflow fallback per chunk preserves huge-coordinate robustness.
  - `bounds` (`Bounds::from_coords`) is a scalar min/max fold over the `x`/`y` columns — LLVM already
    auto-vectorizes it (`maxpd`/`minpd`); an explicit SoA-gather SIMD pass measured ~1.9× *slower* on the
    old AoS layout, so don't re-add SIMD here without new evidence (re-measure post-SoA before trusting old ratios).
  - Both SIMD kernels gate on `REDUCE_SIMD_MIN` (64): smaller geometries (the common
    case) take the scalar path so they never pay the gather allocation.
  - Re-measure reduction kernels with the maintained benchmark harness before changing them; do
    not add thread-pool benchmark dependencies to do it.
  - Acceptance bar for refactors onto these SIMD kernels (2026-07-10): byte-identical asm OR an
    interleaved `bench_ab` equivalence/win verdict on the covering case, with the asm delta
    explained. Precedent: the `line_length_columns` `<const HAS_Z>` unification measures 0.999
    (NOISE) — its instruction-count growth is the OUTLINED COLD rescue helpers
    (`line_length_segment_hypot::<Z>`), the hot loop is equivalent. A refactor that regresses the
    covering bench reverts, whatever the asm looks like.
- **`robust::orient2d` is the only adaptive-precision predicate needed.** It backs
  `orientation()` (hence `point_on_segment`, `segment_contact`, the ring
  point-membership boundary test). `line_intersection` needs intersection
  *coordinates*, not orientation *signs*, so `robust` does not apply there — its
  degenerate/overflow cases are guarded by a denominator threshold + a
  `fraction.is_finite()` check instead.
- **The contact classifier is the relate-class predicate spine.** `segment_contact`
  grades a segment pair `None`/`Touch`/`Cross` (`Cross` = transversal, strictly
  interior to both — `segments_intersect` IS `!= None`). `Cross` between valid
  areal boundaries settles the matrix locally (the interiors' side-pairs share a
  quadrant and each owns one): overlaps TRUE, touches/contains/covers FALSE,
  line/area crosses TRUE (both operand orders; JTS grades symmetrically). `None`
  means every 1D/2D component lies uniformly inside or outside the other operand,
  so per-component representative raycasts decide the matrix entries EXACTLY
  (`interior_part_uniform`/`exterior_part_uniform`; bare point parts split on the
  strict-vs-covered kernels — a point resting ON a boundary IS the OGC touch).
  Only tangential `Touch` contact falls past the lanes, with the bounded vertex
  witnesses retried first inside that arm — and for HOMOGENEOUS pairs even that
  is native: `relate.rs` derives the FULL DE-9IM matrix for polygon pairs from
  the winding arrangement (`build_areal_arrangement`; interiors = face
  windings, boundaries = edge-piece weights + side faces, corner touches =
  shared nodes) and for line pairs from one classified contact scan (mod-2
  boundary sets, Cross ⇒ interior point, collinear runs + t-interval coverage
  gaps decide IE/EI exactly, endpoint touches classify by the boundary sets).
  and for mixed line/area pairs from a split-scan (splits only at crossings
  and mid-segment ring corners; collinear runs mark boundary + ring-cover
  intervals; one strict midpoint raycast per sub-piece; `De9im::transpose`
  serves the operand order). `native_relate_shapes` backs
  `relate`/`relate_pattern` and every tangential arm at 2.5-7.6x GEOS with
  exact string parity. GeometryCollection relate uses the same native contact
  spine. `touches` is ONE contact-classified lane for all dimension pairs.
  TRAP: `interior_part_uniform` against a PUNTAL target needs the dimension
  guard (a coinciding vertex is boundary contact, not interior-meet). Overlaps/crosses readings are
  DIM-DEPENDENT (`is_overlaps_lineal` needs II=='1'; line/line crosses IS
  II=='0') — never reuse the areal readings on a lineal matrix. The predicate lanes take the scan as a lazy
  `FnOnce() -> SegmentContact`: raw shapes use `linework_contact`
  (envelope-prechecked brute / `SegmentIndex`), `ShapeData::*_cached` rides
  `parts_linework_contact` over the handles' cached facet trees (the generic
  `for_each_overlapping_segment_pair` ControlFlow visitors on `FacetBvh`).
  Measured: every relate-class predicate 2-6x GEOS at 40 vertices, 65-78x at 300
  (touches 6x); never re-route these through geo wholesale.

- **Pair-scan structure choice is measured, not stylistic.** Single-set all-pairs
  candidate scans (`visit_interacting_pairs`, `indexed_segments_are_simple`,
  `self_intersections`) run through ONE kernel — `for_each_candidate_pair` in
  `segment_index.rs`: sweep-and-prune (sort envelope minima along the wider global
  axis, forward scan while sweep extents overlap, cross-axis filter), brute below
  `SWEEP_MIN_PAIRS = 32` (measured: the sweep wins from ~6-8 segments). An R-tree
  for this shape costs 3-4x more (bulk-load sort + selection iterators were 38% of
  the 16k validity profile). The R-tree (`SegmentIndex`) stays for bipartite scans
  with order contracts and for build-once/query-many shapes (overlay noding,
  simplify's topology guard, coverage rows).
- **Chain-adjacent segments never re-test intersection.** Segments `i`/`i+1` of one
  chain share an endpoint by construction, so the validity/simplicity visitors skip
  `segments_intersect` for `segments_are_adjacent` pairs and go straight to the
  simplicity classifier. Witness/path strings are built ONLY on failure — that
  includes the per-vertex JSON path in `validate_points` (an eager `format!` per
  coordinate was ~24% of bulk `is_valid`).
- **The intersects oracle probes before it scans.** In `parts_intersect` and
  `Shape::intersects`, the containment probes (isolated points, one representative
  vertex per component) run BEFORE the segment-pair scan: each is sufficient alone,
  so any-area-overlap resolves in one raycast. `ShapeData::distance`/`dwithin` gate
  the zero-distance oracle on bounds overlap (disjoint boxes cannot intersect) and
  hoist the separation gate above all parts work.
- **Winding seeds ride `outside_winding_seeds`, never per-loop raycasts.** Both
  arrangement consumers (`winding_overlay`, `winding_region`) seed component
  windings through one kernel; the ray-crossing rule is additive per segment, so
  an R-tree ray query answers all loops at once when the time model
  (`probes*longest <= 64*segments + 256*probes` stays brute) says the bulk load
  pays. The dense cascade's 59%-of-profile shell re-raycast is the failure mode
  this guards against; flat probe-count thresholds lost on both ends.
- **Bounds-disjoint overlays never node.** `Shape::overlay` mirrors GEOS's envelope
  optimization: union/symmetric-difference = both operands side by side, difference
  = the left operand, intersection = empty — narrowed by `build_overlay_shape`
  (gometry stays self-consistent: covered points still absorb) with Z/M carried
  verbatim. Input chains are preserved exactly; the full pipeline must not be
  reintroduced for disjoint operands (it shatters lines into atomic segments).
  `union_all` over a packed point array dissolves as ONE `Shape::MultiPoint` over
  the shared columns (same kernel, zero per-row boxing).
- **SpatialIndex = StaticStrTree bulk + rstar overflow, rows packed-or-boxed.**
  `IndexRows { packed: PackedRows::{None, Points(Arc<CoordSeq>), Lines{coords,
  offsets}}, boxed: Vec<PyGeometry> }`: packed point AND line arrays share
  their columns zero-copy (line entries from window-bounds scans); Mixed/
  iterable rows and every `insert` are boxed; handles are positional and
  never reused. The bulk tree is an immutable STR-packed flat-level tree
  (`str_tree.rs`, cap 16, tombstone-by-handle removal); post-build inserts
  live in a small rstar overflow every query lane chains after the bulk.
  Query paths ride `ShapeRow` (`with_data` stack views for packed rows,
  persistent handles with prepared caches for boxed); array query/join
  lanes are row-direct (one frame check + `candidate_ids_core` /
  `topological_matches` / `dwithin_query_row_matches` — `.items()` only
  for Mixed/empty-row occupancy and iterable fallbacks). Geodesic
  non-point lanes ride a lazy per-row cap table (`GeodesicRowCaps`:
  anchor + proven aux-sphere reach, row-count keyed, insert-invalidated);
  `Shape::geodesic_cap` + the tiered capped sweep in `distance.rs` are
  the scalar siblings. Clustering reuses `packed_spatial_index`.
- **No scalar `mul_add`/`hypot` in hot kernels** (x86-64-v2 baseline: both are libm
  CALLS; clippy's `suboptimal_flops` suggests the anti-optimization).
  `interpolate_f64` is plain `start*(1-f) + end*f` (endpoint-exact, convex-bounded —
  fused rounding buys nothing); lengths in measurement loops use
  `point_distance_fast` (guarded sqrt with `hypot` rescue). Subdivision
  (`segmentize`/`densify`) is columnar via `subdivide_columns` +
  `CoordSeq::from_columns`; ring rebuilds go through `Polygon::map_ring_seqs`
  (reverse/segmentize/densified) — no AoS `Vec<Point>` round-trips in chain
  transforms.

- **Binary broadcast kernels see `ShapeData` handles, never bare `Shape`.** The
  whole binary-geometry combinator family (`broadcast2_geometry`,
  `geometry_kernel_over_array`, `array_binary_geometry`, `OverlayOp::kernel`,
  `crs_metric_binary_geometry_broadcast`) hands kernels `&ShapeData` (persistent
  Mixed-row handles with prepared caches; stack handles for packed points).
  Shape-level kernels call `.shape()` EXPLICITLY. Rationale: `ShapeData` Derefs to
  `Shape`, so a `&Shape` parameter silently coerces a prepared handle down to the
  brute kernels with zero visible signal at the call site (`shortest_line` ran 20x
  slow this way). When adding a binary surface, take `&ShapeData` and degrade
  explicitly — never the reverse. `clip_by_rect` keeps its verbatim contained-case
  gate (it also shields untouched inputs from the polygon clipper's grid snapping)
  and per-family disjoint empties; do not remove them when touching the clipper.

- **Convex hole-free polygons buffer constructively** (`convex_round_buffer`:
  offset edges + inscribed vertex arcs, no boolean resolution — a convex ring's
  offset never self-intersects). Routing: positive distance + round joins only;
  concave/holes/erosion/miter take the general i_overlay engine. The arc rule is
  `ceil(sweep / (pi/2/quad_segs))` inscribed steps — deliberately NOT GEOS's
  fillet rounding (which chord-cuts coarse corners) nor i_overlay's denser rule;
  acceptance is the inscribed-area envelope vs a high-q reference, not vertex
  parity. Do not "fix" arc density to match another engine.

- **`Arrangement` (geometry/arrangement.rs) is the shared planar-subdivision
  core** — columnar half-edge structure (u32 ids, CSR adjacency CCW-sorted,
  one global face walk keeping positive AND negative walks, directed
  multiplicities, BFS winding fill seeded once per connected component). The
  live consumer is the coverage global arrangement; grow new face/region
  consumers (polygonize, repair) onto it rather than SegmentGraph when
  touching them.
- **`XY` is the planar engine's currency; `Point` is the ordinate-carrying
  interchange.** The noding/arrangement/sweep/winding kernels and their
  storage run on `XY {x, y}` (16 B) and `Segment` = two XYs — the type system
  states that engine code can never depend on Z/M. Ordinate lanes source full
  `Point`s explicitly: `lerp_point` (vs the engine's XY
  `interpolate_segment_point`), `LineIndex`'s `OrdinateSegment`, the offset
  lane's `OffsetEdge`, `Shape::for_each_vertex_pair`, and the witness lanes'
  lift-through-host-pair pattern. Never store `Point` in engine pools, and
  never route an ordinate lane through the planar `Segment` — both directions
  of that mistake were paid for once already.
- **The winding overlay engine IS the binary set-op core** (`winding_overlay`,
  overlay.rs): union/intersection/difference/xor of polygonal operands run on
  `Arrangement<[i32; 2]>` (one winding per operand; op = face predicate), and
  `union_all` dissolves through `dissolve_polygons` (joint arrangement under
  2048 segments, kd-split cascade above). INPUT VERTICES ARE PRESERVED
  BIT-EXACTLY — never reintroduce i_overlay/grid-snapping here (it perturbed
  untouched vertices in the 8th decimal). PINCH RULE: `region_rings` splits
  every boundary walk at repeated vertices into simple rings (JTS
  maximal-to-minimal) — no local successor rule handles both corner-touching
  lobes and holes touching shells; hole nesting probes past pinch vertices.
  The mixed-dimension overlay driver (lines/points/boundary-contact) is
  separate machinery and stays.
- **The winding buffer engine IS the general buffer** for polygonal
  expansion, polygonal erosion, AND lineal strokes, in ALL join styles
  (`winding_buffer`/`winding_erosion`/`winding_stroke`/`winding_region`,
  constructive.rs, on the `Arrangement` core): raw offset walks via
  `WalkPlan` (plan-validate-emit), each hole shrunk in its own
  sub-arrangement (`winding <= -1` — partial/full inversion cancels exactly),
  final selection `winding >= 1` (strokes: one closed loop per chain — right
  side, cap, left side, cap). Output is exact-float (never i_overlay's snapped
  grid). OUTSIDE turns follow `WalkJoinRule` (arc / bevel chord / miter with
  the GEOS limited-miter clip — the spike cut flat at `limit * distance`
  along the bisector, continuous in the corner angle; matches GEOS to
  ~1e-15). INSIDE turns are style-independent: crossing when the offset
  edges cross, demoted back to the through-vertex excursion whenever the
  clip parameters run backwards on an edge (`WalkPlan::validate` —
  inversion consumes edges, and greedy crossings mint phantom holes;
  negative "textbook" arcs are WRONG at shrunk hole corners). Invariants
  pinned by tests: cyclic-shoelace orientation for open cycles, the
  inside-join rule, per-component outside-winding seeds (components never
  cross), exact miter/bevel areas, the limit-clip reach. EROSION runs the
  mirrored construction (flipped walk orientations, keep `winding <= -1`,
  shell-side inversion cleaning). STYLED-EROSION SEMANTICS ARE OURS, NOT
  GEOS'S: the output is the exact monotone styled-offset construction;
  GEOS's negative-buffer depth machinery both manufactures phantom slivers
  past the inscribed radius (non-monotone) and drops legitimate
  corner-allowance slivers when they survive as isolated components. Both
  findings came from a one-off differential investigation that adjudicated with
  the Chebyshev radius and a corner-disk allowance test; that campaign is long
  gone and no live oracle exists — the conclusions are pinned by the
  deterministic `bevel_erosion_keeps_the_exact_corner_allowance`. Never "fix"
  the engine toward GEOS here.
  EVERY shape kind routes: points are inscribed-circle disks
  (`point_buffer`/`circle_loop` — a single point skips the arrangement
  entirely), zero-length chains are disks, negative distances annihilate
  puntal/lineal input to the typed empty polygon exactly, and a
  `GeometryCollection` reduces to ONE shared arrangement
  (`winding_collection` — the union of part buffers IS the `winding >= 1`
  region; never buffer parts separately and union). Degenerate polygon rings
  or coordinate overflow take the native `winding_route` fallback (`None` →
  typed empty). The noder's shared-endpoint fast path is load-bearing for
  performance — adjacent loop segments must never reach the exact crossing
  solver. NODING EXACTNESS RULES: `segment_cross_point` returns
  collinear-certified touching endpoints bit-exactly, and `self_node_segments`
  re-nodes through the collinear-overlap pre-pass when folds are detected —
  both prevent cross-pair ULP-twin vertices (pinned by
  `erosion_annihilates_deep_notched_shell`); never replace them with tolerance
  snapping. `face_windings` seeds only the MOST NEGATIVE walk per component
  (zero-area degenerate faces must never take the outside seed).
- **Python ↔ Rust boundary (where wrapper logic lives).** The split is deliberate;
  keep new code on the right side of it.
  - *Rust owns:* all geometry/CRS/grid/index computation; anything per-element,
    per-vertex, array, or hot; zero-copy buffer/Arrow/NumPy handling; anything
    reading native (PROJ) data; and real scalar algorithms (e.g. geocode codecs).
    Verbose PyO3 dict/HTML/JSON assembly STAYS in Rust when it is a `#[pyclass]`
    method (`CRS` is `#[pyclass(frozen)]` — `_repr_html_`/`to_projjson_dict`/
    `info` can't move without fragmenting the class), when the values are
    Rust-built (the projection-factor/geodesic dicts hold NumPy arrays built from
    Rust columns), or when the source data is native — moving any of these to
    Python splits one operation across two languages for zero gain.
  - *Python owns (`python/gometry/*.py`):* the statically declared optional-ecosystem
    conversion facades and lazy exports; the `pyarrow` object bridge (`_arrow.py` — zero-copy
    `py_buffer`, runs at batch granularity, and `pa.ExtensionType` subclassing is
    inherently Python); and private typing support (`_types.py`). Public CRS/grid/geocode
    families are direct flat aliases in `__init__.py`, never domain shims or wrappers. Arbitrary,
    dynamically-typed GeoJSON Feature `properties`/`id` values remain opaque Python
    objects, but `from_features` validates and carries them through one native call.
  - *The rule:* a Python wrapper crosses the FFI ONCE PER CALL (batched), never in
    a loop calling a scalar Rust fn when a batched Rust entry exists. `from_features`
    validates and packs all rows in one native call, never one call per feature.
  - *Rejected (don't re-raise):* moving `CRS._repr_html_`/`to_projjson_dict`/`to_cf`
    or the projection-factor/geodesic dict builders to Python; `to_feature_collection`'s
    per-geometry `__geo_interface__` loop (properties are opaque Python objects, serialization
    cost dominates, and there's no batched alternative without marshaling arbitrary
    JSON into Rust); `GeometryArray([...])` is the one canonical iterable/ndarray ingest path.
- **Geographic antimeridian + poles (auto-handled in topology; geodesic-edge measures).**
  - *Topology auto-splits.* When a geometry is in a geographic CRS AND crosses ±180
    (`geographic_crossing(&Frame, &Shape)` in `src/geometry/antimeridian/`), the predicate,
    `relate`, overlay, and `clip_by_rect` paths SPLIT-NORMALIZE it (`split_antimeridian`) before the
    planar kernel — so a 170°→-170° edge is the 20°-wide seam region, NOT a fake 340°-wide polygon.
    The gate is a true no-op off the antimeridian (CRS-free / projected / non-crossing): cheap enum
    match → one no-alloc longitude scan → WGS84 string fast path → cached PROJ only for the rare
    has-CRS-and-crosses case. Overlay results stay in seam-split form (never re-joined). The single
    chokepoints: `predicate.rs` (`topology_scalar_pair`/`scalar_vs_shapes`), `broadcast/predicates.rs`,
    `geometry_binary_geometry_broadcast` (overlay free-fns + operator + arrays), `PyGeometry::overlay`,
    the clip wrappers. Mirrors how `distance` was already geodesic-aware.
  - *Centroid / point_on_surface / bounds UNWRAP, not split* (the split halves sit at opposite ends of
    [-180,180]): shift the crossing geometry to a contiguous lon frame, compute, rewrap. Crossing
    geographic `bounds` report west>east (minx>maxx) — the established geographic-bounds convention.
  - *Inherently planar, NOT auto-split (documented):* `convex_hull`, `buffer`, `offset_curve`,
    `simplify` are planar-in-lon/lat — split the geometry yourself first for crossing input.
  - *Geodesic AREA uses geodesic-edge semantics* (`PolygonArea`, great-circle edges). A full-longitude
    4-corner box `[-180,180]×[lat0,lat1]` is therefore DEGENERATE (the ±180 corners are the same point)
    and correctly returns area 0 — NOT a bug. To measure a zonal band, densify the parallel edges. A
    genuine pole-encircling polygon (proper vertices) measures correctly.
  - *Pole-enclosure containment is complete across the frame-aware point lanes.* A geographic ring
    whose longitude winding is ≈±360 encloses a pole; the original unsplit shape classifies exact
    pole interior/boundary/exterior before normalization, and literal ±180 probes distinguish real
    boundary from the split representation's fabricated seam. Full predicates, prepared predicates,
    `contains_xy`/`intersects_xy`, packed/missing array broadcasts, and index refinement share that
    gate. Crossing holes are split as independent areal operands and subtracted from the split shell,
    so polar annuli remain valid; any polar ring (including a hole that excludes the pole) forces a
    conservative full-longitude envelope, with all ring latitudes included for index narrowing.
  - *Unary topology uses the same frame gate.* `topology_split` lives in
    `geometry/antimeridian/normalize.rs`; `is_valid`/`validate`/`is_simple`/`is_ring`, `require`,
    `self_intersections`, `repair`, and `snap_to_grid(repair=True)` normalize only geographic
    crossings. Projected and CRS-free inputs stay planar. Normalized verdicts are never cached on
    `ShapeData` (a frozen shape may be retagged to another frame); non-crossing verdicts keep the
    existing shape caches, and packed arrays fall back per-row only when a geographic crossing is
    present. A valid crossing repairs as the original handle/storage; an invalid crossing repairs
    from the Z/M-preserving split (and honestly errors if a polar cap must invent Z/M).
  - *Single gated per-pair entry (no footgun).* `topology_scalar_pair` / `topology_scalar_pair_frames`
    are the ONLY per-pair topology entries for frame-aware surfaces; they split-normalize then call the
    bare `scalar_pair` kernel, so `scalar_pair` is only ever reached post-normalization. They are
    bool-only: `topology_split` force-2D-splits valid input (predicates are 2D, so Z/M is irrelevant)
    and falls back to the planar 2D shape only when malformed ring topology cannot be split; the
    frame-aware validation APIs report that normalization failure explicitly. The `geographic`
    verdict is always derived from a frame (`is_geographic_frame`), never hardcoded.
  - *Spatial index + `PreparedGeometry` are fully covered.* The index derives `geographic` from its own
    frame (`SpatialIndex::geographic()`); refine / `topological_matches` / `point_rows_matches` /
    prepared route through the gated pair; the convex MBR fast path is skipped for a crossing query; and
    crossing / pole-enclosing items and queries get a `crossing_index_bounds` envelope (full-longitude
    band, latitude extended to any enclosed pole) so R-tree narrowing never misses them.
    `join` / `query_pairs` inherit this through the shared candidate+refine engine.
  - *Keystone: `geo_split_pair` / `geo_binary` (predicate.rs).* The single place a binary topology op
    decides "does this pair cross, and if so split it" — `topology_scalar_pair`, the `relate` /
    `relate_pattern` broadcasts (scalar/array/free-fn), and any future binary op wrap their kernel in it
    so a crossing pair can never reach a kernel on its false-middle planar box. The metric family
    (distance/dwithin/nearest_points/shortest_line) has the analogous `geodesic_split_operands`
    (pair.rs); the prepared geometry×array / array nearest fast paths route through
    `geodesic_nearest_points_cached_split` (borrows the non-crossing operand, splits only a crossing one)
    so they no longer bypass the gate. The recurring round-1/round-2 footgun was each entry point
    re-deciding the split; centralizing it here is what stops new entry points from re-introducing the
    seam bug. `contains_xy`/`intersects_xy` likewise normalize via `antimeridian_scalar_operand`.
  - *Poles are tri-state (`PolePosition`: Interior/Boundary/Exterior, pole.rs).* A pole strictly
    inside is contained; a pole ON the boundary (a ring vertex at ±90 — e.g. an S2 pole-corner cell) only
    touches. `try_geographic_point_pair` returns the boundary-aware verdict for every predicate (incl.
    `touches`), so `intersects`/`covers`/`disjoint` can no longer contradict `touches`/`relate` at a pole
    vertex. `centroid`/`point_on_surface` of a pole-enclosing (degenerate-when-unwrapped) geometry route
    through the seam+pole split; `point_on_surface` always uses the split for crossing input (guaranteed
    interior), while `centroid` keeps unwrap-rewrap for the normal crossing case (the true centroid).
    Public `bounds()` widens to the pole exactly like `crossing_index_bounds`.
  - *Z/M antimeridian split rejects only a SURVIVING cap.* `split_antimeridian` defers the pole-ordinate
    gate to a post-assembly `reject_fabricated_pole_ordinates`: a plain seam crossing (no pole) keeps Z/M
    (seam vertices interpolate); only a genuine pole-closure cap that fabricates a ±90 vertex rejects a
    measured/3D source. Overlay of a crossing Z/M polygon now works.
  - *`index()` skips empty geometries* (they can never be a candidate) instead of erroring, keeping the
    boxed-row handles aligned: `StaticStrTree::build(entries, total_rows)` pre-tombstones the skipped
    rows so `removed`/`initial_len` still span every input row (the tree↔overflow handle boundary).
  - *`from_geojson` rejects a non-WGS84 `crs=`* (reader/writer symmetry with `to_geojson`, RFC 7946);
    `from_features` accepts `epoch=`; `is_convex` returns `False` for non-Polygons (degrades like the rest
    of the `is_*` family, no array-abort); the polygon-areal centroid normalizes `-0.0`.

## Verification gates (non-obvious commands)

- **Pre-release blocker that no default lane runs:** the exhaustive EPSG in-core observer is an
  opt-in marker excluded by default addopts, so it runs nowhere automatically.
  `.venv/bin/python -m pytest -m exhaustive` is a MANDATORY manual gate before any
  release/baseline re-lock; a release is not green without it. (The `fuzz` marker and its
  75k-mutation differential fuzzer were deleted 2026-07-23 — see the no-fuzzy-testing decision.)

`uv` is at `/run/current-system/sw/bin/uv`; the project venv is `./.venv` (Python 3.14).

Plain `.venv/bin/python -m pytest` is the deterministic behavioral/oracle lane.
Standalone release checks are listed explicitly below; do not add source-policy
scanners or synthetic "gate catches text" tests when behavior, typing, or Rust
visibility can enforce the invariant directly.

- **Rebuild the native extension after any Rust edit** with maturin. The interpreter MUST be
  pinned to the venv — a bare `uv run --with maturin` defaults to 3.9 and now fails the
  `requires-python >=3.11` floor:
  `uv run --no-project --python .venv/bin/python --with maturin==1.14.1 maturin develop --release`
  (`cargo check`/`clippy` passing does NOT mean the installed `.so` was rebuilt — pytest will
  silently run against the stale extension until you rebuild.)
- **pyright MUST run against the venv** or you get ~20 false "import could not be resolved"
  errors: `uv run --no-project --with pyright pyright --pythonpath .venv/bin/python`
- ruff: `ruff check python tools tests scripts` (local maintenance check; shared CI stays behavioral)
- tests: `.venv/bin/python -m pytest`
- CRS-unit behavior matrix: `.venv/bin/python -m pytest tests/test_metric_matrix.py`.
- Cross-surface signature parity (one op = one option parametrization across Geometry /
  GeometryArray / free fn; deliberate divergences registered with reasons):
  `.venv/bin/python -m pyo3stubs surface --config tools/stubs/stubconfig.py` — caught cross-surface
  `method=` / `tolerance` parametrization drift on `simplify`.
- Zero-copy behavior is verified by pointer/storage identity tests at the public
  conversion boundaries; do not restore source-spelling scans.
- rust: `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` are local maintenance
  checks; shared CI intentionally runs behavior only. Keep both clean before handoff — prefer a
  scoped `#[expect(clippy::X, reason=...)]` over a broad workspace allow.
- Combined Rust+Python coverage (a DISCOVERY tool, not a target — read the gaps for edge
  cases worth testing or dead code worth deleting): `.venv/bin/python tools/coverage.py`.
  It installs an INSTRUMENTED debug extension (debug asserts live — this is also how the
  mixed-axes witness bug surfaced); rerun `maturin develop --release` afterwards. The
  facet-BVH distance engine only engages at ≥64 segments (`BVH_MIN_INDEXED_SEGMENTS`) —
  keep `tests/test_equivalence.py`'s gate-straddling battery in mind when touching it.
- `test_oracles.py` is a differential test vs shapely / h3-py / s2sphere / pyproj.

## Documentation site

The docs site is a Proper Docs / MkDocs Material site in `docs/`, configured by
`properdocs.yml`.

- **Rust docstrings are the source of truth.** Public Python prose lives on the PyO3 surface
  as Rust `///` comments. After any Rust docstring edit, rebuild the extension and regenerate
  `python/gometry/_lib.pyi` with:
  `.venv/bin/python -m pyo3stubs gen-docs --config tools/stubs/stubconfig.py` (the shared toolkit;
  `--check` verifies sync without writing).
  The stub keeps hand-authored signatures/overloads/types, but receives runtime `__doc__`
  prose so IDE hover, `help()`, mkdocstrings, and the Rust source cannot drift.
- `properdocs.yml` intentionally sets `allow_inspection: false`; mkdocstrings must read the
  authoritative typed stub statically, not the compiled extension.
- The `pyo3stubs` doc generator (`gen-docs`) must not put docstrings on `@overload` definitions. Griffe needs
  concrete fallback definitions for overloaded native functions, while Ruff requires overload
  variants to stay docstring-free. The injector also enforces two prose contracts: a public
  runtime symbol whose docstring is deleted/emptied fails (stale stub prose never outlives
  its source), and a stub-only subclass override of an inherited runtime member (e.g.
  `Polygon.boundary` narrowing the return type) keeps — and must carry — its own
  hand-written docstring; the injector never overwrites it with the base-class doc.
- **Stub signatures are stubtest-gated.** `mypy.stubtest` (run by
  `tests/test_stubs.py`) compares every runtime symbol, parameter name/kind/order,
  and default against the stub; `pyo3stubs structural` adds what mypy cannot see —
  overload hygiene (docstring on the LAST canonical `@overload` variant; bare
  implementation defs are illegal in stubs), `@final` vs runtime finality, signature
  coverage, and `__match_args__` parity. The always-on ``text-signature`` gate
  cross-checks manual Rust `text_signature` overrides against the real `signature`
  attributes. Where a PyO3 signature default is a non-literal Rust expression
  (rendered `...`) or a `None` sentinel with a semantic public default, present the
  true default with a manual `text_signature` (`origin='centroid'`,
  `min_confidence=70` are the precedents; `holes=None` is the live Polygon
  default) — the gate keeps overrides honest.
- **Flat families are direct aliases.** Every public domain function belongs to one explicit
  top-level family (`crs_*`, grids, or point codecs), and every export must BE its raw ``_lib``
  object (thin alias, never a wrapper). Domain modules are not part of the public surface. Stub
  class bases must mirror the runtime hierarchy both ways
  (`Generic`/protocol machinery stays stub-side); the docstring-example harness walks every
  top-level callable, so an alias's ``Examples`` block executes the moment one is written.
- **Container annotations are behavior- and typing-gated.** Focused runtime tests exercise
  every parser family with iterators, buffers, abstract mappings, scalar-or-collection
  inputs, invalid elements, broken iterators, and size limits; positive/negative mypy and
  pyright fixtures pin the handwritten overloads. Do not restore a parallel parameter-to-
  parser classification matrix: it became stale metadata about the API rather than testing
  the API itself. The standing input policy: float lanes are `Iterable[float] | Buffer`
  (buffer fast path first, then `Vec<f64>`, then a `try_iter` collect — one parser:
  `coordinate_values`); every dict boundary takes any `Mapping` (the `mapping_as_dict`
  seam); geometry batches share one private `_GeometryLike` row vocabulary (native
  `Geometry`, `Buffer` WKB, `Mapping`, or `SupportsGeoInterface`, plus `None` only
  where missing rows are supported), and take any iterable; ambiguous strings remain
  explicit `from_wkt`/`from_geojson` inputs;
  bounds-like params take any 4/6-float iterable. Returns stay concrete (`list`/`tuple`/
  `dict`); stubs import `Buffer`, defaulted `TypeVar`, and `disjoint_base` from the
  stdlib on Python 3.15+ and from `typing_extensions` before it.
- **Token vocabularies are config-gated (`token-vocabulary`).** `_lib._token_vocabulary()` exports every
  `token_enum!` surface (private alias name, canonical tokens straight from
  the generated `TOKENS` table); the gate compares each alias's `_types.py`
  `Literal` against it (tokens and order) and source-scans `src/` for `token_enum!`
  declarations so a new token enum cannot ship unregistered. When adding a token enum,
  register it in `_token_vocabulary()` (lib.rs) and, if it has a public alias, keep the
  `_types.py` `Literal` in declaration order. Hand-parsed vocabularies (`Predicate`,
  `DistanceUnit`, `VoronoiClip`, `CoordinateAxes`, `GeometryType`) stay outside this gate.
- `tools/stubs/_doc_coverage.py` enforces the docstring contract across the public surface:
  every signature parameter documented (numpydoc or Google style), no stale documented
  parameters, a `Returns` section everywhere, and any `default X` stated in prose matching
  the runtime default.
- Run the strict site build with:
  `uv run properdocs build --strict`.
  This executes markdown examples and fails on broken links/autorefs.
- `tools/gates/_check_examples.py` is a durable docs verification helper used by CI; keep it in
  step with markdown-exec semantics so runnable examples fail loudly with file/line context.
- Versioned docs use an isolated `mike` invocation rather than adding a runtime or
  development dependency. Publish a release version with
  `uv run --no-project --python .venv/bin/python --with mike mike deploy --push --update-aliases <version> latest`;
  switch the default alias with `uv run --no-project --python .venv/bin/python --with mike mike set-default --push latest`.
  The shared CI runs one gometry-only Linux docs job: build the extension, run
  `properdocs build --strict`, then `tools/docs/check.py` for canonical anchors
  and local links/fragments. Keep gometry docs/overrides/properdocs changes in
  the path filter so documentation cannot bypass that lane.
- Keep `overrides/main.html` social/OpenGraph metadata and `docs/stylesheets/extra.css`
  contrast choices in step with branding changes; geometry SVG examples should stay readable
  in both light and dark schemes.

## Benchmarks

`benches/drivers/bench.py` is bounded by one manifest and two profiles:
`smoke` (single-value execution check) and `release` (the only statistical
release configuration). There is no profile/policy matrix or benchmark-harness
implementation test suite.

- **Per-change interleaved A/B** (the gold standard for keep/revert calls on Rust-core perf
  work): `benches/drivers/bench_ab.py --a <baseline-venv>/bin/python --b .venv/bin/python --case
  benches/cases/<case>.py --rounds 9 --seed 20260709 --cpu <idle-cpu> --json-out
  /tmp/<case>-ab.json`. One `.so` per process, balanced blocked A/B then B/A lead order,
  fixed-seed bootstrap confidence intervals, median/IQR/maximum block time, explicit affinity and
  governor/frequency evidence, and a NOISE verdict unless both the paired-delta IQR floor and
  ratio confidence interval clear equivalence. Never claim a win from a NOISE verdict.
  Build the baseline side from a checkpoint wheel:
  `uv run --no-project --python .venv/bin/python --with maturin==1.14.1 maturin build --release -o <dir>` +
  `uv venv -p .venv/bin/python` + `uv pip install`.
  Case scripts print ONE float (seconds) on stdout; they live in `benches/cases/`.
- **Baseline re-lock:** run `benches/drivers/bench.py --profile smoke`, then
  `--profile release` on the final quiet, committed, kernel-isolated
  release worktree and store the artifacts in
  `benches/results/baseline/`. Finish with
  `tools/gates/_check_bench_regression.py`; smoke and release artifacts never
  cross-compare. The release driver runs every competitor pair in A/B and B/A
  lead order with equal total samples, then records fresh-process p50/p99/p99.9,
  process-RSS, and Python-allocation probes. Only a full successful manifest
  with clean preflight and postflight contention evidence is publishable or
  accepted by the regression gate. The previous exploratory baseline was
  deleted after the API changed and is not release evidence.
