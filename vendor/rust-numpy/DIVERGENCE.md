# rust-numpy divergence

rust-numpy 0.30.0 from the published crate archive, with upstream commit `89747d565fcf14191796dc47db24e1334b4389d2` applied. The local manifest links both `pyo3` and `pyo3-build-config` to the sibling vendored PyO3 0.30.1 packages. Remove these divergences when rust-numpy releases the fallible-construction APIs and supports the adopted PyO3 release directly.
