/**
 * @name Python execution or reference-count effect during GC traversal
 * @description Garbage-collector traversal must report existing edges without
 *              acquiring interpreter attachment or executing Python operations.
 * @kind problem
 * @problem.severity error
 * @precision high
 * @id pkgs/rust/python-effect-during-traversal
 * @tags correctness
 *       external/cwe/cwe-664
 */

import PythonEffects

from Function traverse, Call call
where
  traverse.getName().getText() = "__traverse__" and
  exists(Param parameter, Struct visit |
    parameter = traverse.getParam(_) and
    visit = PathResolution::resolvePath(parameter.getTypeRepr().(PathTypeRepr).getPath()) and
    visit.getCanonicalPath() = "pyo3::pyclass::gc::PyVisit"
  ) and
  callsInContext(traverse, call) and
  forbiddenDuringTraversal(resolvedFunction(call))
select call,
  "This operation changes Python state or executes Python while $@ is reporting GC edges.",
  traverse, "this traversal"
