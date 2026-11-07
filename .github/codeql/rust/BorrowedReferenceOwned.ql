/**
 * @name Borrowed Python reference consumed as an owner
 * @description Adopting a borrowed reference without acquiring ownership can release
 *              a reference still owned by its provider.
 * @kind problem
 * @problem.severity error
 * @precision high
 * @id pkgs/rust/borrowed-python-reference-owned
 * @tags correctness
 *       external/cwe/cwe-416
 */

import CPythonContracts

from BorrowedPointerSource source, PointerConversion sink
where
  sink.consumesReference() and
  localPointerFlow(source, sink.getPointer()) and
  not exists(Call retain |
    resolvedFunction(retain).(CPythonFunction).getApiName() =
      ["Py_INCREF", "Py_XINCREF", "Py_IncRef"] and
    localPointerFlow(source, retain.getSyntacticPositionalArgument(0))
  )
select sink,
  "This owning constructor consumes the borrowed reference returned by $@ without acquiring a new reference.",
  source, "this pointer provider"
