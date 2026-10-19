# Gometry divergence

This in-tree `h3o` 0.11.0 source is vendored from `HydroniumLabs/h3o` revision
`fa1557fd1edd56f64e4da95b2fd294609ce2353e`. Its BSD-3-Clause license is
retained in `LICENSE`.

This checkout carries the exact H3 projection carriers needed by gometry's
private certified grid predicates.  They are not upstream API proposals: keep
them when updating h3o, or port the consuming certificate at the same time.

| File | Item | Exposed invariant unavailable upstream |
| --- | --- | --- |
| `src/boundary.rs` | `Hex2Int`, `normalized_ijk_sum` | Exact doubled lattice coordinates and checked normalized-domain sum for cap centers. |
| `src/boundary.rs` | `ProjectionSeed`, `ProjectionFrame`, `ProjectionSeam` | Raw rational boundary/projection inputs and paired pre-normalization face seams; public `LatLng` is rounded. |
| `src/boundary.rs` | `CapLatticeProfile` | Real suffix, vertex, rotation, and 60-seam finite target lattice profile for certified descendant caps. |
| `src/boundary.rs` | `ProjectedBoundary` | Keeps public boundary ABI unchanged while pairing every point with its raw seed and source edge. |
| `src/coord/faceijk.rs` | `cap_lattice_profile` and seed boundary builders | Reuses H3 aperture/overage/neighbor operations to emit exact carrier data instead of reconstructed tables. |
| `src/coord/faceijk.rs` | `target_substrate_center_seed` | Gives each ancestor its center in the target substrate frame shared by descendants. |
| `src/coord/mod.rs` | projection constants and `native_projection_point` | Central canonical constants plus a diagnostic raw-seed projector; predicates replay the seed outward. |
| `src/coord/vec3d.rs` | shared `ONE_THIRD` use | Prevents a second substrate-scale literal from drifting from the raw projector. |
| `src/coord/vec3d.rs`, `src/coord/faceijk.rs`, `src/coord/vec2d.rs` | unit-norm diagnostics | Allows accumulated transcendental, product, dot-product, and square-root rounding without changing coordinates or geometric decisions. All indexing consumers and unit-sphere tests use the same diagnostic allowance. |
| `src/index/cell.rs` | `boundary_with_seeds`, target center | Native boundary and target-frame center carriers, neither represented by public rounded boundary/LatLng APIs. |
| `src/index/cell_tests.rs` | projected-boundary/profile/raw-carrier tests | Pins public ABI preservation and every raw-carrier ordering/identity invariant. |
| `src/lib.rs` | hidden carrier re-exports | Makes the private carrier usable by gometry without widening ordinary h3o public APIs. |
| `src/resolution.rs` | `ExtendedResolution::new_unchecked`, `From` | Preserves the native adjusted-resolution carrier without lossy conversion. |
| `src/grid/iterator.rs` | safe disk storage growth | Keeps the existing radius-bound reserve through 65,536 cells; for larger bounds, seeds from the origin resolution with a 65,536-cell ceiling on initial reservation only. Both containers keep growing for the full traversal. |
| `src/grid/iterator.rs` | `RingUnsafe::size_hint` | Does not claim a disk bound as a ring's exact length or as a valid lower bound when distortion can stop traversal. |
| `src/grid/iterator_tests.rs` | fast-ring size-hint regression | Checks the standard iterator length-hint bounds against a concrete undistorted ring. |

Every source divergence above has a nearby `gometry:` marker.  Update this
table and its marker together when the carrier changes.  Current audit: 27
markers — 6 in `boundary.rs`, 6 in `coord/faceijk.rs`, 3 in `coord/mod.rs`, 2
in `coord/vec3d.rs`, 1 in `coord/vec2d.rs`, 2 in `index/cell.rs`, 1 each in `index/cell_tests.rs`,
`lib.rs`, and `grid/iterator_tests.rs`, 2 in `grid/iterator.rs`, and 2 in
`resolution.rs`.

The source patch in `patches/h3o/` records the fork against this revision.
The `ProjectionSeed` size expectation preserves the fixed boundary sidecar
under upstream's compiler lint policy.
