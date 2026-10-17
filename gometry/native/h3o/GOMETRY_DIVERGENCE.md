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
| `src/index/cell.rs` | `boundary_with_seeds`, target center | Native boundary and target-frame center carriers, neither represented by public rounded boundary/LatLng APIs. |
| `src/index/cell_tests.rs` | projected-boundary/profile/raw-carrier tests | Pins public ABI preservation and every raw-carrier ordering/identity invariant. |
| `src/lib.rs` | hidden carrier re-exports | Makes the private carrier usable by gometry without widening ordinary h3o public APIs. |
| `src/resolution.rs` | `ExtendedResolution::new_unchecked`, `From` | Preserves the native adjusted-resolution carrier without lossy conversion. |
| `src/grid/iterator.rs` | safe disk storage growth | Keeps the existing radius-bound reserve through 65,536 cells; for larger bounds, seeds from the origin resolution with a 65,536-cell ceiling on initial reservation only. Both containers keep growing for the full traversal. |
| `src/grid/iterator.rs` | `RingUnsafe::size_hint` | Does not claim a disk bound as a ring's exact length or as a valid lower bound when distortion can stop traversal. |
| `src/grid/iterator_tests.rs` | fast-ring size-hint regression | Checks the standard iterator length-hint bounds against a concrete undistorted ring. |

Every source divergence above has a nearby `gometry:` marker.  Update this
table and its marker together when the carrier changes.  Current audit: 24
markers — 6 in `boundary.rs`, 5 in `coord/faceijk.rs`, 3 in `coord/mod.rs`, 1
in `coord/vec3d.rs`, 2 in `index/cell.rs`, 1 each in `index/cell_tests.rs`,
`lib.rs`, and `grid/iterator_tests.rs`, 2 in `grid/iterator.rs`, and 2 in
`resolution.rs`.

## Clippy expectations

The vendored manifest keeps h3o's crate-level deny groups without opting into
the consuming workspace's lint table. Narrow expectations preserve these
fork contracts:

| Lint | Owner | Reason |
| --- | --- | --- |
| `cargo_common_metadata` | crate root | The gometry sdist packages `proj-sys` separately, and that package owns its metadata. |
| `exhaustive_enums`, `variant_size_differences` | `ProjectionSeed` | Gometry's proof consumers exhaustively distinguish the two hidden carrier variants and retain exact intersection seeds in the fixed boundary sidecar. |
| `large_types_passed_by_value` | `CapLatticeProfile::new` | The fixed arrays become owned profile fields; a borrowed constructor would still copy them into the owner. |
| `too_many_lines`, `large_stack_arrays` | `cap_lattice_profile` | One bounded profile build constructs the fixed 60-seam table with shared face context. |
| `suboptimal_flops` | `math::mul_add` | The fork's unfused `(a * b) + c` keeps exact H3 decisions independent of host FMA support on the x86-64-v2 baseline. |
