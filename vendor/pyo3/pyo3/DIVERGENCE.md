# PyO3 divergence

PyO3 0.30.1 from the `v0.30.1` release (`f168538672f2c32e32f7c1b702a502ee92de1edf`), with local fixes for primitive buffer byte-order matching and scalar buffers with null shape and strides. The source patch series in `patches/pyo3/` records these changes and their regression tests. Remove this divergence when a published PyO3 release contains equivalent fixes.

The normalized manifest restores sibling path-plus-version dependencies so this checkout resolves one PyO3 graph and excludes Cargo's reserved archive metadata from local path-package listings.

The optional synchronization integration uses parking_lot 0.13 and lock_api
0.5, matching the workspace's locks. Its sealed reentrant-mutex implementations
carry the new `RawMutex` bound. Remove this integration patch when PyO3
supports those releases upstream.
