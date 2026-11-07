/**
 * @name Python object API called while detached
 * @description Raw Python object operations require attachment even when a raw
 *              pointer or unsafe wrapper can cross the detachment boundary.
 * @kind problem
 * @problem.severity error
 * @precision high
 * @id pkgs/rust/python-call-while-detached
 * @tags correctness
 *       external/cwe/cwe-664
 */

import PythonEffects

from ClosureExpr closure, Call detach, Call call
where
  detachedClosure(closure, detach) and
  callsInContext(closure, call) and
  requiresAttachment(resolvedFunction(call))
select call, "This Python object operation requires attachment, but it runs in $@.", detach,
  "this detached execution"
