/**
 * @name Python-owned object storage adopted as a Rust allocation
 * @description Borrowed object data is not an allocation whose ownership may
 *              be reconstructed with a Rust raw-parts constructor.
 * @kind problem
 * @problem.severity error
 * @precision high
 * @id pkgs/rust/python-storage-adopted
 * @tags correctness
 *       external/cwe/cwe-762
 */

import CPythonContracts

/** These APIs lend storage retained by the Python object's own allocation. */
private predicate borrowsStorage(Function function) {
  function.(CPythonFunction).getApiName() =
    [
      "PyBytes_AsString", "PyBytes_AS_STRING", "PyByteArray_AsString", "PyByteArray_AS_STRING",
      "PyUnicode_AsUTF8AndSize", "PyUnicode_DATA", "PyUnicode_1BYTE_DATA"
    ]
}

/** Global-allocator owners require a matching, separately transferred Rust allocation. */
private predicate adoptsAllocation(Function function) {
  function.getCanonicalPath() =
    [
      "<alloc::boxed::Box>::from_raw", "<alloc::vec::Vec>::from_raw_parts",
      "<alloc::string::String>::from_raw_parts", "<alloc::sync::Arc>::from_raw",
      "<alloc::rc::Rc>::from_raw"
    ]
}

/**
 * Raw-pointer casts change the pointee type, not its allocator or owner.
 * Keep recursive origins bounded to storage providers; syntax narrows method
 * lookup work while declaration identity still decides whether a call is a cast.
 */
private predicate storagePointerFlow(Call source, Expr sink) {
  borrowsStorage(resolvedFunction(source)) and
  (
    localPointerFlow(source, sink)
    or
    exists(MethodCall cast |
      (
        cast.(MethodCallExpr).getIdentifier().getText() = ["cast", "cast_const", "cast_mut"]
        or
        cast instanceof CallExpr
      ) and
      resolvedFunction(cast).getCanonicalPath() =
        ["<*mut>::cast", "<*const>::cast", "<*mut>::cast_const", "<*const>::cast_mut"] and
      storagePointerFlow(source, cast.getReceiver()) and
      localPointerFlow(cast, sink)
    )
  )
}

from Call source, Call sink
where
  borrowsStorage(resolvedFunction(source)) and
  adoptsAllocation(resolvedFunction(sink)) and
  storagePointerFlow(source, sink.getSyntacticPositionalArgument(0))
select sink,
  "This Rust constructor adopts Python-owned storage lent by $@ as though it were a transferred Rust allocation.",
  source, "this object data accessor"
