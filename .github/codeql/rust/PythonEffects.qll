/**
 * Execution effects at Python attachment and garbage-collector boundaries.
 *
 * Attachment is permission to use the interpreter, not storage immutability.
 * The reachable-call relation follows resolved calls without entering nested
 * callback bodies: a separately attached callback has its own execution context.
 */

import CPythonContracts

/** Reviewed raw object APIs that require an attached interpreter thread. */
predicate requiresAttachment(Function function) {
  function instanceof NewReferenceFunction
  or
  function instanceof BorrowedReferenceFunction
  or
  function instanceof BooleanOrErrorFunction
  or
  function instanceof StealingFunction
  or
  function.(CPythonFunction).getApiName() =
    [
      "Py_INCREF", "Py_DECREF", "Py_XINCREF", "Py_XDECREF", "Py_IncRef", "Py_DecRef",
      "PyObject_GetBuffer", "PyBuffer_Release", "PyObject_SetAttr", "PyObject_SetItem",
      "PyErr_Clear", "PyErr_Fetch", "PyErr_Restore", "PyErr_SetObject", "PyErr_SetString",
      "PyDict_GetItemRef", "PyDict_SetItem", "PyDict_Next", "PyIter_NextItem",
      "PyCapsule_GetPointer", "PyCapsule_GetContext", "PyCapsule_IsValid"
    ]
}

/** Calls reachable in the same execution context, without entering callback values. */
predicate callsInContext(Callable root, Call call) {
  call.getEnclosingCallable() = root
  or
  exists(Call previous, Function callee |
    callsInContext(root, previous) and
    callee = resolvedFunction(previous) and
    call.getEnclosingCallable() = callee
  )
}

/** A closure actually supplied to PyO3's detachment operation. */
predicate detachedClosure(ClosureExpr closure, Call detach) {
  resolvedFunction(detach).getCanonicalPath() = "<pyo3::marker::Python>::detach" and
  localPointerFlow(closure, detach.getPositionalArgument(0))
}

/** A PyO3 method which dispatches Python code on its receiver. */
predicate dispatchesPython(Function function) {
  function.getCanonicalPath().matches("%pyo3::types::any::PyAnyMethods>::%") and
  function.getName().getText() =
    [
      "call", "call0", "call1", "call_method", "call_method0", "call_method1", "getattr", "setattr",
      "delattr", "hash", "eq", "ne", "lt", "le", "gt", "ge", "repr", "str", "is_truthy", "get_item",
      "set_item", "del_item", "try_iter"
    ]
}

/** Effects forbidden while reporting GC edges: allocation, refcounts or Python dispatch. */
predicate forbiddenDuringTraversal(Function function) {
  requiresAttachment(function)
  or
  dispatchesPython(function)
  or
  function.getCanonicalPath() =
    [
      "<pyo3::marker::Python>::attach", "<pyo3::marker::Python>::attach_unchecked",
      "<pyo3::marker::Python>::try_attach", "<pyo3::instance::Bound>::clone",
      "<pyo3::instance::Py>::clone_ref"
    ]
}
