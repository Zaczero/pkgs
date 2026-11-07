/**
 * @name Python error status interpreted as Boolean true
 * @description A negative CPython error result is nonzero, so a nonzero test
 *              can convert an exception into a successful Boolean result.
 * @kind problem
 * @problem.severity error
 * @precision high
 * @id pkgs/rust/python-error-as-boolean
 * @tags correctness
 *       external/cwe/cwe-252
 */

import CPythonContracts
import codeql.rust.controlflow.BasicBlocks
import codeql.rust.controlflow.ControlFlowGraph

/** Only the zero boundary is relevant; suffixes do not change that value. */
private predicate isZero(Expr expr) {
  expr.(IntegerLiteralExpr)
      .getTextValue()
      .regexpMatch("0[0_]*(i8|i16|i32|i64|i128|isize|u8|u16|u32|u64|u128|usize)?")
}

/** A successful branch establishes that this tri-state value is not negative. */
private predicate checkedNonnegative(AstNode guard, Expr checked, boolean branch) {
  exists(ComparisonOperation comparison |
    comparison = guard and
    (
      comparison.getOperand(0) = checked and
      isZero(comparison.getOperand(1)) and
      (
        comparison.getOperatorName() = ">=" and branch = true
        or
        comparison.getOperatorName() = "<" and branch = false
      )
      or
      comparison.getOperand(1) = checked and
      isZero(comparison.getOperand(0)) and
      (
        comparison.getOperatorName() = "<=" and branch = true
        or
        comparison.getOperatorName() = ">" and branch = false
      )
    )
  )
}

private module CheckedStatus = DataFlow::BarrierGuard<checkedNonnegative/3>;

private module StatusConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) {
    resolvedFunction(source.asExpr().(Call)) instanceof BooleanOrErrorFunction
  }

  predicate isSink(DataFlow::Node sink) {
    exists(NotEqualsOperation comparison |
      sink.asExpr() = comparison.getOperand(0) and isZero(comparison.getOperand(1))
      or
      sink.asExpr() = comparison.getOperand(1) and isZero(comparison.getOperand(0))
    )
  }

  predicate isBarrier(DataFlow::Node node) { node = CheckedStatus::getABarrierNode() }
}

private module StatusFlow = DataFlow::Global<StatusConfig>;

/**
 * PyO3's checked helper establishes the same property through a Result.
 * Only its successful `?` edge is evidence; merely calling the helper is not.
 */
private predicate checkedByResult(Expr source, Expr sink) {
  exists(
    Call check, TryExpr attempt, BasicBlock guardBlock, BasicBlock sinkBlock, CfgNode sinkNode,
    MatchingSuccessor success
  |
    resolvedFunction(check).getCanonicalPath() =
      ["pyo3::err::error_on_negative", "pyo3::err::error_on_negative_with_result"] and
    localPointerFlow(source, check.getSyntacticPositionalArgument(1)) and
    attempt.getExpr() = check and
    guardBlock.getLastNode().getAstNode() = attempt and
    sinkBlock.getNode(_) = sinkNode and
    sinkNode.getAstNode() = sink and
    success.getValue() = true and
    guardBlock.edgeDominates(sinkBlock, success)
  )
}

from DataFlow::Node source, DataFlow::Node sink
where
  StatusFlow::flow(source, sink) and
  not checkedByResult(source.asExpr(), sink.asExpr())
select sink.asExpr(), "This nonzero test also accepts the negative error status returned by $@.",
  source.asExpr(), "this CPython call"
