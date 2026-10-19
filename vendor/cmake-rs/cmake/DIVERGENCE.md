# cmake-rs divergence

cmake 0.1.58 from the published crate archive (`63190697197b34e5e5cd3cabf17da4fcab348bef`), with the environment-tracking fix in `patches/cmake-rs/` applied to `src/lib.rs`. Its normalized manifest excludes Cargo's reserved source metadata files from path-package listings. Track CMake and compiler environment inputs at their reads so Cargo rebuilds native dependencies when those inputs change. Remove this divergence when a cmake-rs release provides that tracking.
