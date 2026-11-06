/** Data-flow roles owned by HTTPX Secure's data-bearing connector. */

import python
import semmle.python.dataflow.new.DataFlow
import semmle.python.dataflow.new.TaintTracking

private predicate protectedMethod(Function f) {
  f.getEnclosingModule().getName() = "httpx_secure._network" and
  f.getEnclosingScope().(Class).getQualifiedName() = "_ProtectedBackend"
}

private predicate protectedReceiver(DataFlow::Node source) {
  exists(Function f |
    protectedMethod(f) and
    source = DataFlow::parameterNode(f.getArg(0))
  )
}

private module ReceiverConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) { protectedReceiver(source) }

  predicate isSink(DataFlow::Node sink) {
    exists(DataFlow::AttrRead read |
      read.getAttributeName() in ["_backend", "_policy"] and
      sink = read.getObject()
    )
  }
}

private module ReceiverFlow = DataFlow::Global<ReceiverConfig>;

/** Provider reads retain the protected receiver through aliases and closures. */
private predicate provider(DataFlow::AttrRead read, string field) {
  exists(DataFlow::Node receiver |
    protectedReceiver(receiver) and
    read.getAttributeName() = field and
    ReceiverFlow::flow(receiver, read.getObject())
  )
}

private module ProviderConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) { provider(source, ["_backend", "_policy"]) }

  predicate isSink(DataFlow::Node sink) {
    exists(DataFlow::MethodCallNode call | call.calls(sink, ["connect_tcp", "admit"]))
  }
}

private module ProviderFlow = DataFlow::Global<ProviderConfig>;

private predicate providerCall(DataFlow::MethodCallNode call, string field, string method) {
  exists(DataFlow::AttrRead source, DataFlow::Node receiver |
    provider(source, field) and
    call.calls(receiver, method) and
    ProviderFlow::flow(source, receiver)
  )
}

/** The logical hostname entering the protected backend, before IP admission. */
predicate originalHostname(DataFlow::Node source) {
  exists(Function f |
    protectedMethod(f) and
    f.getName() = "connect_tcp" and
    source = DataFlow::parameterNode(f.getArgByName("host"))
  )
}

/** The selected-IP provider's result, not arbitrary functions named `admit`. */
predicate admittedAddress(DataFlow::Node address) { providerCall(address, "_policy", "admit") }

/** Destination arguments of the owned data-bearing backend, excluding DNS probes. */
predicate connectionDestination(DataFlow::Node destination) {
  exists(DataFlow::MethodCallNode call |
    providerCall(call, "_backend", "connect_tcp") and
    destination = [call.getArg(0), call.getArgByName("host")]
  )
}

module HostnameConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) { originalHostname(source) }

  predicate isSink(DataFlow::Node sink) { connectionDestination(sink) }

  predicate isBarrier(DataFlow::Node node) { admittedAddress(node) }
}

module HostnameFlow = TaintTracking::Global<HostnameConfig>;
