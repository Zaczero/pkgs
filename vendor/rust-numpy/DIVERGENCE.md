# rust-numpy divergence

rust-numpy 0.30.0 from the published crate archive (`a64b0350682c1f9a99f908c38533f31bfd120eec`), with local fallible-array-construction and ownership-transfer fixes recorded in `patches/rust-numpy/`. The local manifest links both production and test `pyo3` dependencies and `pyo3-build-config` to the sibling vendored PyO3 0.30.1 packages, and excludes Cargo's reserved source metadata files from path-package listings. Remove these divergences when rust-numpy releases equivalent fallible-construction APIs and supports the adopted PyO3 release directly.
