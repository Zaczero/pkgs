/**
 * @name Rust dependency declarations are missing from semantic analysis
 * @description A registered core dependency without its crate-root source
 *              cannot provide the declarations required by the contract models.
 * @kind problem
 * @problem.severity error
 * @precision high
 * @id pkgs/rust/incomplete-dependency-declarations
 * @tags correctness
 */

import rust

/** Attach the diagnostic to analyzed source: a missing library root has no alert location. */
private SourceFile projectSource() {
  result.getFile().fromSource() and
  result.getFile().getRelativePath() =
    min(SourceFile candidate |
      candidate.getFile().fromSource()
    |
      candidate.getFile().getRelativePath()
    )
}

from Crate dependency, SourceFile anchor
where
  anchor = projectSource() and
  dependency.getName() = ["core", "alloc", "std", "pyo3", "pyo3_ffi"] and
  not exists(dependency.getSourceFile())
select anchor,
  "The registered " + dependency.getName() +
    " dependency has no associated crate-root source. Repair source selection or sysroot source identity before relying on semantic analysis."
