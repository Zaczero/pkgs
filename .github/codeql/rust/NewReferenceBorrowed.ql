/**
 * @name New Python reference passed to a borrowed-pointer constructor
 * @description Borrowing a newly returned owned reference acquires an additional
 *              reference without releasing or transferring the original.
 * @kind problem
 * @problem.severity error
 * @precision high
 * @id pkgs/rust/new-python-reference-borrowed
 * @tags correctness
 *       efficiency
 *       external/cwe/cwe-401
 */

import rust
import codeql.rust.internal.PathResolution as PathResolution

/** CPython APIs whose successful return transfers one Python reference. */
private predicate returnsOwnedReference(Function f) {
  f.getCanonicalPath() =
    [
      "pyo3_ffi::abstract_::PyNumber_Index",
      "pyo3_ffi::bytesobject::PyBytes_FromStringAndSize",
      "pyo3_ffi::listobject::PyList_New",
      "pyo3_ffi::pycapsule::PyCapsule_New",
      "pyo3_ffi::refcount::Py_NewRef",
      "pyo3_ffi::tupleobject::PyTuple_New"
    ]
}

/** These constructors acquire a reference without consuming the pointer's owner. */
private predicate borrowsPointer(Function f) {
  f.getCanonicalPath() =
    [
      "<pyo3::instance::Bound>::from_borrowed_ptr",
      "<pyo3::instance::Bound>::from_borrowed_ptr_or_err",
      "<pyo3::instance::Bound>::from_borrowed_ptr_or_opt"
    ]
}

// Associated-function paths resolve even when generic return-type inference cannot
// produce a static call target. Resolve declarations, including imported aliases.
from CallExpr source, CallExpr sink
where
  returnsOwnedReference(PathResolution::resolvePath(source.getFunction().(PathExpr).getPath())) and
  borrowsPointer(PathResolution::resolvePath(sink.getFunction().(PathExpr).getPath())) and
  sink.getSyntacticPositionalArgument(1) = source
select sink,
  "This borrowed-pointer constructor acquires another reference without consuming the new reference returned by $@.",
  source, "this CPython call"
