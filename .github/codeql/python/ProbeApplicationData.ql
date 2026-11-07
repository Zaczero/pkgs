/**
 * @name Application data sent through an address-selection probe
 * @description An address-policy selection probe may identify and close a
 *              destination, but must not send application bytes before admission.
 * @kind path-problem
 * @problem.severity error
 * @security-severity 8.1
 * @precision high
 * @id pkgs/python/application-data-on-selection-probe
 * @tags security
 *       correctness
 *       external/cwe/cwe-918
 */

import python
import semmle.python.ApiGraphs
import semmle.python.dataflow.new.DataFlow

/** Only the policy's address-selection connector has this restricted purpose. */
private predicate selectionProbe(DataFlow::Node source) {
  exists(API::CallNode call, Function resolver |
    call = API::moduleImport("anyio").getMember("connect_tcp").getACall() and
    call.getScope() = resolver and
    resolver.getName() = "_resolve" and
    resolver.getEnclosingScope().(Class).getQualifiedName() = "_AddressPolicy" and
    resolver.getEnclosingModule().getName() = "httpx_secure.__init__" and
    source = call.getReturn().getAwaited().asSource()
  )
}

private module ProbeConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) { selectionProbe(source) }

  predicate isSink(DataFlow::Node sink) {
    exists(DataFlow::MethodCallNode call |
      call.calls(sink, ["send", "send_all", "write", "writelines", "start_tls"])
    )
  }
}

private module ProbeFlow = DataFlow::Global<ProbeConfig>;

import ProbeFlow::PathGraph

from ProbeFlow::PathNode source, ProbeFlow::PathNode sink
where ProbeFlow::flowPath(source, sink)
select sink.getNode(), source, sink,
  "This operation sends data or starts a protocol on the $@ before destination admission.",
  source.getNode(), "address-selection probe"
