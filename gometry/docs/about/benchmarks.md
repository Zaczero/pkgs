---
description: Current gometry benchmark status.
---

# Benchmarks

No comparative benchmark artifact is published.

The implementation properties users can rely on without a timing claim are:

- geometry kernels are implemented in Rust;
- homogeneous geometry columns use packed coordinate and offset storage where
  the GeoArrow layout supports it; and
- array APIs expose vectorized operations and typed NumPy results.

## See also

- [Arrays & performance](../guide/arrays.md) — packed construction and batch results.
- [Internals](internals.md) — durable ownership and interchange boundaries.
- [Compatibility](compatibility.md) — supported runtime and integration matrix.
