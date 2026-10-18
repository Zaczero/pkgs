# PyO3 divergence

PyO3 0.30.1 from the `v0.30.1` release (`f168538672f2c32e32f7c1b702a502ee92de1edf`), with upstream commits `bc5fcebb066c48bac629896f578461116b42349d` and `5ced647b6dafb0cfea8e40dbf33a1531548717f6` applied to `src/buffer.rs`. Keep the source patch series in `patches/pyo3/`; remove this divergence when a PyO3 release includes both fixes.

The normalized manifest restores sibling path-plus-version dependencies so this checkout resolves one PyO3 graph. It also excludes Cargo's `.cargo_vcs_info.json` and `Cargo.toml.orig` metadata from local path-package listings.
