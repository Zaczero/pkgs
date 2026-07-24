# Gometry v1.0 final release plan

This is the single execution ledger for gometry's first public release. It
supersedes every earlier task list and the over-engineered branches discovered
during the 2026-07-12 simplification audit.

Status: **implementation, independent review, and automated validation complete;
quiet-host release performance evidence, tracked-source provenance, and live
visual QA remain external release gates. The dated changelog cutover is a
deliberate final release action and the release workflow blocks while the
target heading still says `Unreleased`**.

## Release standard

Gometry v1 must be capable, coherent, fast, precisely typed, and pleasant to
use without making its users or maintainers pay for defensive ceremony. The
design target is **maximum capability through the fewest durable mechanisms**:
shared kernels and representations, composable primitives, direct errors, and
one obvious public spelling per task.

Release blockers are demonstrated crashes, hangs, silent truncation or data
loss, wrong results, partial mutation, false typing/docs, broken adapters,
untrustworthy benchmarks, or unreproducible artifacts. Harmless Python edge
cases and theoretical misuse do not earn frameworks, mode knobs, overload
matrices, or policy gates.

## Binding design rules

1. **Generalize only along a real shared contract.** Parameterize algorithms
   when meaning, valid input domain, and return family align. Share the Rust
   kernel—but keep separate public names—when a parameter would instead switch
   intent or return shape.
2. **Keep the real, uniform receiver surface.** Unary operations remain declared
   once on `Geometry` and once on `GeometryArray`; leaf classes add only genuine
   leaf facts/protocols such as `Point.x` and `Polygon.exterior`. Unsupported
   kinds raise a direct `GeometryTypeError`. Delete fake completion entries.
3. **Keep the flat namespace and handwritten stub.** Top-level grid/CRS/code
   families stay prefix-discoverable. Precise overloads describe useful scalar,
   array, and broadcast behavior; `Never` is reserved for a small guaranteed
   error that cannot be expressed more simply—not a model of the whole dynamic
   geometry lattice.
4. **Use Python's numeric model.** `bool` follows ordinary `int`/`float`
   coercion. Validate finite values, domains, ranges, and dangerous expansion;
   do not add bool-specific extractors, overloads, or tests. External formats
   may still reject booleans where their specification distinguishes them.
5. **Keep exact value identity exact.** Geometry `==`, hashing, and
   `equals_identical` remain bit-exact (including signed zero, frame, kind, axes,
   and vertex order). `equals_exact` and `equals` provide numeric and
   topological equality.
6. **Make ordinate behavior automatic, not configurable everywhere.** Remove
   `ordinate_policy`. Each operation preserves/interpolates Z/M where meaningful
   and otherwise returns the mathematically natural XY result—no ceremonial
   `force_2d()` before a buffer, centroid, Voronoi, or crossing overlay. True
   serialization loss remains explicit: a format that cannot represent an
   active ordinate raises until the user removes it or selects an explicit
   format option.
7. **Keep distinct aggregation intents distinct.** `arr.union_all()` returns one
   geometry; `arr.dissolve(by)` returns grouped unions. They share the same
   union kernel. Do not add `dissolve(by=None)`.
8. **Do not add state for symmetry alone.** Scalar grid input returns a rich
   source-aware Coverage; array input returns efficient
   `Groups[CellArray[...]]`. No `CoverageArray`.
9. **Use general Python protocols at natural boundaries.** Accept an `Iterable`
   when values can be consumed once, `Sequence` when length/random access is
   required, `Mapping` for mappings, and `Buffer` for binary data. Reject text
   only where it would be mistaken for a collection. Accept scalar and row-wise
   parameters when both are practically useful and the existing broadcast
   machinery handles them directly.
10. **Optional framework integrations are typed, inert boundaries.** pandas and
    Polars expose only statically declared conversion/storage APIs—no `.geo`
    accessors, registration functions, framework-class mutation, dtype-name
    registry mutation, or dynamically discovered algorithm surface. Lazy
    optional imports are fine because their call signatures remain visible to
    type checkers; geometry computation has one home on gometry objects and
    functions.
11. **Internal cleanup must delete code or coupling.** Keep the current cohesive
    module ownership: `lib.rs` is already a small explicit assembly root,
    `array` owns the array value/storage/methods, and point navigation is one
    coherent subsystem. No directory-purity reshuffle or speculative split.
12. **Tests protect behavior and load-bearing invariants.** Prefer focused
    deterministic regressions, oracle comparisons, typing fixtures, built-site
    checks, and measured benchmarks over source-spelling policy scanners.

### Additional public-surface simplifications

- Generalize `gm.require(value, *, crs=None, axes=None)` to scalar geometry-like
  input, `GeometryArray`, and raw iterables; it parses and atomically validates
  validity/frame/axes. Delete duplicate `require_valid`, `require_crs`, and
  `require_axes` receiver methods.
- Keep selection on `GeometryArray` in `arr[...]`; delete `.take()` and
  `.filter()` and update adapters/callers. Support the useful existing concrete
  integer/boolean lanes—do not invent generator-index classification.
- Delete constrained-triangulation `refine=`. `min_angle` or `max_area`
  explicitly requests refinement; neither means plain constrained
  triangulation.
- Delete `explain=` from `gm.join` and `SpatialIndex.join`; joins always return
  pairs. `SpatialIndex.explain(...)` owns diagnostics.
- Delete `GeometryArray.to_numpy(dtype=...)`; object dtype is the only behavior,
  so `to_numpy()` is sufficient.
- Keep `rhumb_distance`. It is point-route length, not the general minimum
  distance between arbitrary shapes. `bearing`, `destination`, and
  `point_between` retain `path=` because both route models share their exact
  domain and result.
- Extend `contains_xy`/`intersects_xy` to `GeometryArray` with scalar/column
  x/y broadcasting and a direct packed lane. Raw coordinates are the distinct
  capability; do not force an intermediate `gm.points(x, y)` allocation.
- Keep plus-code reference inputs as explicit lon/lat. Do not add Point/array
  overloads without a distinct capability.
- Make `Features(geometries, properties=None, ids=None)` pleasant: `None`
  expands to row-aligned missing values, one `Mapping` broadcasts as independent
  ordinary copied dictionaries, and general iterables are consumed once.
  Outer columns are immutable tuples; inner property dictionaries remain
  editable. Do not introduce `MappingProxyType` or scalar ID broadcasting.

## Verified current evidence

| Gate | Result |
|---|---|
| Default Python suite | 3,791 passed, 33 skipped, 3 deselected, 4 xfailed; 0 failures/errors |
| Exhaustive/fuzz marker | **STALE — must be re-run.** Last green (2 passed in 769.07 s) predates the storage, contract, error-surface, typing, performance, and docs passes, which moved the CRS and storage boundaries this marker covers. |
| Rust nextest | 380/380 passed |
| Formatting | passed |
| Clippy / Ruff | strict Clippy `-D warnings` and Ruff passed |
| Runtime/stub/type parity | `pyo3stubs check-all` passed, including stubtest + mypy |
| Strict docs + built-site links/fragments | passed; canonical anchors and local targets clean |
| Local release-artifact audit | clean sdist-to-wheel build, inspection, isolated install, and behavior smoke passed |
| Local production advisory scan | no vulnerabilities beyond the two documented dev-oracle ignores |
| Coverage discovery | Python 88%; merged Python-driven + native Rust 88.37% lines; concrete gaps fixed, no percentage gate |
| Benchmark smoke | 15/15 gometry, competitor, and real-world rows passed; 62-row/57-command release manifest complete; quiet isolated run pending |
| Source provenance | source is tracked in the monorepo (not an untracked tree) |
| Live browser QA | no in-app browser session was available; still required before release |
| Changelog cutover | pending the release date; workflow blocks a new 1.0.0 tag while its heading says `Unreleased` |

Final coverage review (2026-07-13) fixed visible-value equality for selected
`Coordinates` views, added direct scalar/broadcast coverage of the admitted
in-core `crs_roundtrip` path, deleted the obsolete scalar grid-dispatch layer,
and made `tools/coverage.py` merge native nextest profiles with Python-driven
Rust coverage in one package-local target. Remaining low-hit regions are
optional-adapter subprocess/error arms, defensive exact fallbacks, test-only
generic specializations, and notebook presentation—not untested release-critical
behavior.

The sections below preserve the evidence that drove the completed changes; the
checkboxes and current gate table are the source of truth for their resolution.

## Confirmed release work resolved below

### Safety and correctness blockers

- Deep WKT/WKB/GeoJSON recursion and a tiny cyclic Python coordinate container
  can terminate Python.
- `segmentize`/densification truncates `usize` counts through `u32`; extreme
  sampling/interpolation/buffer parameters can panic, hang, or expand without a
  checked bound.
- H3 descendants/disk/ring/path, ragged cell children, parent expansion,
  uncompact, cover, and set algebra bypass the existing one-million-cell rule
  and sometimes stage nested vectors before CSR.
- Polylabel can panic on extreme aspect ratios, refine excessively on elongated
  polygons, and its fixed default tolerance is poor on ordinary unit-scale
  geometry.
- Constrained triangulation ignores Spade's incomplete-refinement result and can
  return triangles that violate `max_area`/`min_angle`.
- `minimum_bounding_circle()` emits an inscribed polygon that may not contain
  the input support points.
- `SpatialIndex.insert()` can partially insert a missing batch row as an
  unremovable NaN entry.
- Polygon coordinate paths can silently discard Z/M; empty LineString,
  MultiPoint, and Polygon constructors lose explicitly requested axes.
- Antimeridian splitting has a known-invalid fallback instead of a valid result
  or explicit failure.

### Public-contract blockers

- Constructor and method stubs accept impossible forms or disagree with runtime
  (`Geometry.__hash__`, `set_coordinates`, densify, missing rows, coverage
  kinds, and several returns).
- The declared `typing-extensions` marker/floor cannot supply features imported
  by the shipped stub on all supported Python versions.
- Missing-row behavior differs between a `GeometryArray` and an equivalent raw
  iterable.
- Runtime navigation signatures expose sentinel defaults/order that differ from
  the handwritten stub.
- Before this pass, pandas/Polars installed a process-global import hook and
  exposed a dynamic `.geo` algorithm surface that IDEs and portable static type
  checkers could not see; pandas adapter import also mutated the dtype-name
  registry.
- Bulk/features/general collection boundaries remain unnecessarily concrete in
  several practical APIs.
- Grid token/protocol/NumPy documentation and `Coordinates.to_dict()` output are
  inconsistent with the package's shared conventions.
- Pre-v1 signposts, fake `dir()` entries, stale aliases/inventories, and private
  `__module__` identities still leak old implementation history.

### Documentation, evidence, and release blockers

- Generated class pages hide real leaf/protocol/cell/coverage/error members and
  replace them with partial manual fragments.
- Several guides/reference pages retain stale spellings, claims, labels,
  signatures, examples, and release placeholders.
- Every benchmark driver contains stale calls, so competitor/regression claims
  are not executable.
- Default pytest's rendered-site checks can pass only because an ignored local
  `site/` already exists; clean CI does not build it first.
- Source is tracked in the monorepo; clean-checkout CI, versioning, and artifacts
  follow ordinary git workflow. Git mutation still requires explicit user
  authorization.

## Dependency-ordered execution

### 0. Simplify the contract before fixing implementations

- [x] Update `AGENTS.md` and the API constitution with the binding rules above;
  delete stale namespace, dissolve, interop-hook, registry, docs-CI, and
  free-threading claims.
- [x] Remove `ordinate_policy` end to end—runtime arguments and branches, token,
  stubs, registry rows, tests, and docs. Collapse each affected path onto its
  existing automatic behavior: preserve/interpolate when meaningful, otherwise
  XY; retain direct errors only at true representation-loss boundaries.
- [x] Apply the surface deletions/generalizations above (`require`, selection,
  triangulation refinement, join diagnostics, NumPy conversion, rhumb) before
  regenerating any derived documentation.
- [x] Add focused failing regressions for every demonstrated crash, truncation,
  wrong result, silent data loss, and partial mutation. Use subprocesses only
  for actual abort/hang cases.
- [x] Fix the three trivial Clippy findings and stale benchmark call sites so
  later evidence starts from working tools; do not add lint to wheel CI.

### 1. Bound only real recursive and amplifying paths

- [x] Add one private recursion-depth constant/counter to recursive WKT, WKB,
  and Python GeoJSON/feature traversal. Merge the duplicate Python recursive
  walkers if that produces less code. Depth exhaustion alone handles cyclic
  containers; do not add identity graphs or a parser state framework.
- [x] Extract one tiny checked expansion counter from the proven smoothing
  pattern: checked add/multiply, the existing practical generated-coordinate
  ceiling, fallible reservation, and operation-specific errors.
- [x] Apply it only where a small parameter amplifies work: segmentize/densify,
  smoothing/subdivision, sampling, count interpolation, buffer/offset arcs, and
  constrained refinement. Do not cap input-sized constructors, parsers, Arrow,
  overlay, or polygonize merely because they allocate.
- [x] Replace the lossy subdivision lane with `usize`, precompute cumulative
  output, and return ordinary errors before reservation/iteration.
- [x] Reuse `GRID_MAX_CELLS` through one capped flat collector and one direct CSR
  groups builder. Apply them to genuinely expanding operations across all four
  grids; do not wrap non-expanding paths ceremonially.
- [x] Acceptance: exact limit, limit+1, overflow, cyclic/deep input, and current
  crash/hang witnesses return normal documented errors; ordinary large
  input-sized workloads remain unrestricted.

Evidence (2026-07-12): all 380 current Rust library tests pass; focused recursion,
resource-bound, grid, constructor, algorithm, and state-integrity Python
regressions pass. Capped collectors remain only on true grid fan-out paths;
ordinary `CellArray`, bulk-cell, pickle, set-union, and group-slice inputs use
their natural input-sized storage.

### 2. Repair algorithms and state integrity

- [x] Rework polylabel around one root square, uniform finite scaling, adaptive
  subdivision, and the global inradius upper bound. Use `tolerance=None` with a
  scale-aware default based on the geometry's narrow span and floating-point
  resolution; explicit tolerance remains absolute. Charge queued/subdivided
  cells to the shared checked expansion budget so an absurdly small explicit
  tolerance cannot recreate unbounded work; do not add a second polylabel-only
  resource framework.
- [x] For constrained triangulation, let `min_angle`/`max_area` activate
  refinement, cheaply preflight output scale, cap added vertices through the
  expansion budget, reject mathematically impractical angle requests, inspect
  `refinement_complete`, and error rather than violate the request.
- [x] Emit a circumscribed approximation for minimum bounding circles while
  leaving maximum-inscribed circles inscribed.
- [x] Validate a complete SpatialIndex insertion batch before changing frame,
  row storage, handles, or the tree; prove every returned handle is removable.
- [x] Route sequence/column constructors through one coordinate parser that
  preserves requested XY/XYZ/XYM/XYZM axes for empty and non-empty inputs and
  closes polygon rings without dropping columns.
- [x] Replace invalid antimeridian fallback output with a valid result or an
  actionable error.

Evidence (2026-07-12): the focused algorithm/state suite passes 57 tests,
including extreme-aspect polylabel, constrained-refinement rejection and
postconditions, circumscribed bounding-circle containment, atomic index batch
failure, empty/non-empty XY/XYZ/XYM/XYZM construction, and antimeridian
regressions.

### 3. Finish the practical Python and typing surface

- [x] Rewrite constructor overloads around the few legal input forms; do not
  encode epoch-without-CRS and every other obvious misuse as a negative overload.
- [x] Correct all confirmed runtime/stub mismatches and the
  `typing-extensions` dependency marker. Keep one handwritten `_lib.pyi`.
- [x] Implement the universal `require` boundary and indexing-only array
  selection; update pandas, benchmarks, docs, and tests and delete old twins.
  Dispatch precedence is exact Geometry/GeometryArray, then WKT/WKB/GeoJSON
  text/mapping/buffer, then generic iterable. Raw iterables return
  `GeometryArray`; `crs=` retains attach-only-if-unframed behavior; validation
  is atomic.
- [x] Normalize missing rows: row-preserving outputs retain positions,
  reductions skip missing rows, constructors treat equivalent iterable/array
  inputs alike, and mutable index insertion rejects missing rows atomically.
- [x] Keep the existing scalar-or-row parameter machinery where variation is a
  real bulk workflow: sampling count/seed, count interpolation, and constrained
  triangulation `min_angle`/`max_area`. Reuse `I64Param`/`F64Param`; add no second
  broadcast framework.
- [x] Add direct packed `GeometryArray` lanes for `contains_xy` and
  `intersects_xy`, with scalar/row x/y broadcasting and no temporary Point
  array.
- [x] Finish `Features` defaults/broadcast/iterable handling with copied mutable
  dictionaries and immutable aligned outer tuples.
- [x] Generalize concrete collection annotations only at natural boundaries;
  accept intuitive keywords for named operation parameters; normalize error
  classes, `polygonize(values=...)`, tile naming, public module identities, and
  read-only NumPy outputs.
- [x] Replace runtime sentinel navigation defaults with real values and align
  runtime/stub keyword order. Use `CRSError` for epoch-without-CRS and consistent
  `ParseError(format='GeoJSON')` for equivalent text/mapping failures.
- [x] Remove stale `*_crs` Literal aliases and private `TYPE_CHECKING`/module
  leakage; make `CellArray.grid == 'tile'`, document Geohash object-token NumPy
  output, and return read-only ndarray columns from `Coordinates.to_dict()`.
- [x] Keep common `Cell`/`Coverage` Protocols minimal and truthful—only members
  used by realistic grid-generic code, not a duplicate catalog of every class.

### 4. Make optional interop statically typed and inert

- [x] Delete `_interop_hook.py`, all `sys.meta_path` finder/loader/lock/pending
  state, conversion-triggered registration, `.geo` accessor/namespace classes,
  registration functions, and the dynamic `_geo_ops.py` dispatch/result model.
  First move the still-useful narrowly scoped `missing_optional_dependency`
  helper into a tiny neutral `_optional.py` module and update every adapter
  import.
- [x] Remove pandas' import-time `GeometryDtype` string registration. Keep the
  concrete extension array/dtype storage path because it provides zero-copy
  sharing and native pandas missing/container behavior; `to_pandas()` constructs
  it directly without mutating pandas global state. Delete both
  `@register_series_accessor` and `_register_dtype()`; keep
  `GeometryDtype.name` as a descriptive name but remove every documented
  `dtype='gometry.geometry'` construction path because string lookup requires
  registry mutation.
- [x] Keep only precisely typed conversion boundaries:
  `GeometryArray.to_pandas()` / `gm.from_pandas()`,
  `GeometryArray.to_polars()` / `gm.from_polars()`, and the vectorized GeoPandas
  conversions. Computation happens after conversion on `GeometryArray`; do not
  replace `.geo` with a wrapper, mirrored adapter namespace, framework stub
  overlay, checker plugin, or duplicate algorithm functions.
- [x] Route every Polars encode path through native batched `to_wkb()` rather
  than `to_arrow(...).storage`; keep decode on Polars' binary/Arrow-C boundary
  without importing PyArrow. Prove pandas and Polars conversions work with
  PyArrow absent so their extras remain independent.
- [x] Add regressions proving that importing gometry or any converter does not
  mutate pandas/Polars classes or registries, while conversion return types,
  missing values, CRS-loss acknowledgements, zero-copy pandas storage, and
  batched Polars WKB round trips remain correct. In particular,
  `pd.api.types.pandas_dtype('gometry.geometry')` must fail both before and
  after importing/converting, while `arr.to_pandas()` and `gm.from_pandas()`
  continue to work zero-copy through the concrete extension type.
- [x] Delete/rewrite the accessor activation tests; remove accessor sections
  from pandas/Polars interop tests while retaining storage, conversion,
  missingness, and metadata coverage. Remove `GeoSeriesAccessor`,
  `GeoSeriesNamespace`, `GeoExprNamespace`, `_geo_ops`, and `.geo`-driven
  `_operations` prose from typing/tool inventories. Rewrite every DataFrame,
  installation, changelog, and reference claim that promises `.geo` or string
  dtype registration.
- [x] Keep extras exactly `arrow`, `pandas`, `polars`, `geopandas`, and `viz`;
  delete `parquet`, and do not pull PyArrow into pandas/Polars extras when the
  batch WKB/NumPy path suffices.
- [x] Keep core star import free of optional dependencies, expose version from
  package metadata, and cover adapter typing and missing-extra behavior.

### 5. Delete proven internal and tooling debt

- [x] Delete `_operations`, `_io_classes`, `_metric_classes`, and
  `_ordinate_classes` plus their manual registries and policy gates after their
  real consumers are removed. Keep `dispatch::Operation` runtime facts.
- [x] Keep `_token_vocabulary`: it is derived directly from parser token tables
  and cheaply protects the handwritten Literal surface. Do not replace private
  metadata with source parsers, generated manifests, or build features.
- [x] Drive the real CRS-unit behavior matrix from its local case table rather
  than a production MetricClass export.
- [x] Delete source-spelling gates whose guarantees are covered better by direct
  behavior, types, or measurement: operation/I/O/ordinate registries, parameter
  and signature doctrine scanners, container scanner, codegen-attribute scan,
  error grammar scan, frame-construction scan after constructor visibility is
  narrowed, and zero-copy source scan. Delete `test_gate_wiring` and synthetic
  “gate catches planted text” tests for the removed gates.
- [x] Replace the frame-construction scanner with the `Frame` enum: its three
  variants make epoch-without-CRS structurally unrepresentable, while checked
  boundary parsers own finiteness/canonicalization. No extra epoch wrapper or
  trusted-internal policing layer is warranted.
- [x] Retain deterministic behavior/oracle tests, pyo3stubs `check-all`, curated
  pyright/mypy positive+negative fixtures, focused return tests, free-threaded
  stress, pointer/storage zero-copy behavior tests, and the algebraic-float
  topology-safety gate.
- [x] Retain the focused packed-execution and typed-return guards until Rust
  visibility/types make their forbidden paths unrepresentable: behavior tests
  alone do not prove GIL detachment/one-time row normalization or preservation
  of concrete geometry subclasses at the PyO3 boundary.
- [x] Review selected-ID storage without extracting it: the final shape would
  add abstraction without net branch/field deletion, so the conditional
  acceptance test correctly rejects it.
- [x] Delete confirmed dead/future helpers and move genuine oracles behind test
  configuration. Keep current module ownership and do not split `point_nav` or
  reshuffle `lib.rs`/`array`.
- [x] Delete the superseded `dev/` planning/archive/experiment dump after
  preserving durable decisions in `AGENTS.md` and this ledger; remove the stale
  packaging/lint exclusions and the final source reference to it.
- [x] Leave tiny-tolerance Voronoi bucketing and speculative cache/GIL changes
  unchanged: the final review found no reproduced defect or measured win that
  justified more code.

### 6. Rebuild docs, benchmarks, and release evidence once

- [x] Replace the docs generator's parallel member model with one compact page
  manifest containing an exact core-`gometry.__all__` partition plus an explicit
  optional-export partition (`from_pandas`, `from_polars`, GeoParquet helpers,
  and the other lazy adapters). Anchor-check both partitions.
  Mkdocstrings renders real members alphabetically, a stable technical-reference
  order that keeps related names such as `force_2d` / `force_3d` adjacent.
  Shared Geometry/GeometryArray members are documented once; leaf pages list
  their own members and link to inherited canonical anchors. Remove
  `filters: ['^$']`,
  manual member HTML, preview maps, heuristic descriptions, and phantom groups.
- [x] Shrink the Griffe extension to the empirically necessary jobs only:
  overload-only stub functions, top-level native aliases, and readable private
  annotation aliases. Resolve public See Also names from the loaded inventory
  with no allowlist or parallel mapping, and keep a small rendered-link check
  that proves every explicit target became an actual link.
- [x] Keep useful visuals as curated executable Markdown examples—not a promise
  that every callable has a preview. Use direct technical headings, conventional
  overload styling, useful page ToCs, explicit descriptions, and canonical
  `https://gometry.monicz.dev/` links. The strict build executes the examples
  and currently emits 73 accessible, captioned inline SVGs across nine guide
  pages through the single `examples/_figures.py` helper.
- [x] Correct every known stale signature/name/type claim (including Arrow/WKB/
  GeoJSON inputs and returns, grid-specific children counts, constructor pages,
  adapter installation, and flat prefixes) and perform the initial-release
  changelog/security/compatibility wording cutover atomically.
- [x] Move rendered-site assertions out of default pytest. One docs lane builds
  the extension/site, then checks exact API anchors, duplicate/private/stale
  names, examples, and links/fragments. Run it once on Linux and ensure docs
  changes are included by CI path filtering.
- [ ] Perform final manual docs QA at desktop and narrow widths in light and dark
  mode. Do not require keyboard/no-JS/print matrices without a demonstrated
  defect.
- [x] Consolidate benchmarks around one manifest, one bounded smoke profile, and
  one release profile. The release-comparable marketed/hot operations have
  executable rows; final claims require a quiet-host release run. Keep
  interleaved A/B statistics and delete shadow catalogs/AST rediscovery and
  redundant harness-implementation tests. The release profile fails before
  timing if the benchmark doctor reports contention, missing dependencies,
  non-performance CPU policy, or uncommitted source.
- [x] Finalize dependency locks/notices before performance measurement. Run the
  75k in-core mutation campaign only when admission logic changes; retain the
  exhaustive EPSG observer for release/dependency changes.
- [x] Keep cibuildwheel testing import-only on every ABI. Run one post-build
  Linux artifact verification for metadata, licenses, native extension, stub,
  `py.typed`, clean install, sdist rebuild from bundled sources, version equality,
  and path leaks—no duplicated behavioral suite in every wheel. Dependency
  advisories remain the separate release audit recorded in the gate table.
- [ ] Once the user explicitly authorizes/ensures source tracking, validate the
  exact release from a clean detached checkout. Any red behavior, typing, docs,
  benchmark-smoke, artifact, or provenance gate blocks v1.0.0.
- [ ] Immediately before dispatching the 1.0.0 release, replace the changelog's
  `Unreleased` heading with the actual release date. The workflow deliberately
  rejects a new gometry tag until this manual date cutover is complete.

## Regression selection rule

Do not construct a Cartesian test product. For each changed path, select only
the dimensions that alter its implementation or contract: scalar/array,
packed/mixed storage, missingness, axes, frame, grid kind, and adversarial
boundary. Parameterize shared cases instead of copying files. Use catalog-wide
observers and mutation campaigns as targeted discovery tools, then pin every
found defect as a small deterministic regression. No property-test framework is
introduced.

## Definition of done

- [x] Every confirmed crash, unbounded amplification, silent truncation/data
  loss, wrong result, and partial mutation witness is fixed and regression
  tested.
- [x] Runtime, handwritten stubs, docstrings, adapters, generated site,
  examples, and benchmarks expose the same final API with no compatibility
  aliases, fake completion, `.geo` accessor or namespace surface, stale
  namespace, or gate-only public surface.
- [x] The implementation contains no `ordinate_policy`, ambient import hook,
  pandas/Polars class or dtype-registry mutation, `CoverageArray`, merged
  dissolve mode, duplicate selection/check APIs, redundant
  refinement/diagnostic/dtype knobs, speculative module reorg, or replacement
  policy framework.
- [x] Behavioral/oracle suites, typing/stub parity, strict built docs and links,
  benchmark smoke, local dependency audit, and local release-artifact audit are
  green.
- [ ] Quiet-host final performance evidence and the exact tracked source's
  detached-checkout artifact validation are green.
- [ ] Source provenance and desktop/narrow light/dark visual review are complete;
  nothing is waived or deferred merely because it is inconvenient.
