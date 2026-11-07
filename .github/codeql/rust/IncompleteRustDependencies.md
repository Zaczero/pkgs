# Rust dependency declarations are missing from semantic analysis

Contract analysis needs resolved declarations from Rust's standard library and
the selected PyO3 dependencies. A crate graph entry without its associated
root source is not equivalent to extracting those declarations: source bytes
elsewhere in the archive do not establish their association with that crate.

## Recommendation

Include local path dependencies in the analysis inputs. Supply the selected
compiler's matching standard-library sources and procedural-macro tooling.
On installations with symlinked sources, derive the physical library root
from a resolved crate-root file rather than only resolving its parent directory.

The query checks registered dependency roots, not universal semantic coverage.
Unregistered crates, unresolved individual generic calls, configuration variants
and incomplete macro expansion require their own representative witnesses.
