/**
 * @name New Python reference passed to a borrowed-pointer constructor
 * @description Interpreting a newly returned reference as borrowed leaves its
 *              ownership obligation unconsumed.
 * @kind problem
 * @problem.severity error
 * @precision high
 * @id pkgs/rust/new-python-reference-borrowed
 * @tags correctness
 *       efficiency
 *       external/cwe/cwe-401
 */

import CPythonContracts

from Call source, PointerConversion sink
where
  resolvedFunction(source) instanceof NewReferenceFunction and
  sink.leavesReferenceUnconsumed() and
  exists(DataFlow::Node sourceNode, DataFlow::Node sinkNode |
    sourceNode.asExpr() = source and
    sinkNode.asExpr() = sink.getPointer() and
    NewReferenceFlow::flow(sourceNode, sinkNode)
  ) and
  not exists(Call release |
    releasesReference(release) and
    exists(DataFlow::Node sourceNode, DataFlow::Node releaseNode |
      sourceNode.asExpr() = source and
      releaseNode.asExpr() = release.getSyntacticPositionalArgument(0) and
      NewReferenceFlow::flow(sourceNode, releaseNode)
    )
  )
select sink, "This borrowed-pointer conversion does not consume the new reference returned by $@.",
  source, "this CPython call"
