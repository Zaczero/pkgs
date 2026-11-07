/**
 * @name Borrowed Python reference passed to a stealing API
 * @description A stealing API consumes a reference still owned by the provider
 *              when its argument was borrowed rather than transferred.
 * @kind problem
 * @problem.severity error
 * @precision high
 * @id pkgs/rust/borrowed-python-reference-stolen
 * @tags correctness
 *       external/cwe/cwe-416
 */

import CPythonContracts

from BorrowedPointerSource source, Call sink, StealingFunction target
where
  target = resolvedFunction(sink) and
  localPointerFlow(source, sink.getSyntacticPositionalArgument(target.getStolenArgument())) and
  not exists(Call retain |
    resolvedFunction(retain).(CPythonFunction).getApiName() =
      ["Py_INCREF", "Py_XINCREF", "Py_IncRef"] and
    localPointerFlow(source, retain.getSyntacticPositionalArgument(0))
  )
select sink,
  "This API consumes a reference, but $@ lends its reference without transferring ownership.",
  source, "this pointer provider"
