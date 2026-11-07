/**
 * @name Python allocation or protocol error interpreted as absence
 * @description An optional pointer conversion can hide a failed CPython call
 *              while leaving its exception pending.
 * @kind problem
 * @problem.severity error
 * @precision high
 * @id pkgs/rust/python-error-as-absence
 * @tags correctness
 *       external/cwe/cwe-252
 */

import CPythonContracts
import codeql.rust.security.Barriers

private module ErrorConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) {
    resolvedFunction(source.asExpr().(Call)) instanceof ErrorOnNullFunction
  }

  predicate isSink(DataFlow::Node sink) {
    exists(PointerConversion conversion |
      resolvedFunction(conversion).getName().getText() =
        [
          "from_owned_ptr_or_opt", "from_borrowed_ptr_or_opt", "from_ptr_or_opt",
          "assume_owned_or_opt", "assume_borrowed_or_opt"
        ] and
      sink.asExpr() = conversion.getPointer()
    )
  }

  predicate isBarrier(DataFlow::Node node) { node instanceof NotNullCheckBarrier }

  // Report the caller's conversion, rather than its generic pointer plumbing.
  predicate isBarrierOut(DataFlow::Node node) {
    exists(PointerConversion conversion | node.asExpr() = conversion.getPointer())
  }
}

private module ErrorFlow = DataFlow::Global<ErrorConfig>;

from DataFlow::Node source, DataFlow::Node sink
where ErrorFlow::flow(source, sink)
select sink.asExpr(),
  "This optional conversion treats failure of $@ as absence instead of consuming the pending Python exception.",
  source.asExpr(), "this CPython call"
