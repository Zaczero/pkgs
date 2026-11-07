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

/** The completed result of a coroutine, rather than the coroutine object itself. */
private predicate awaitedResult(DataFlow::Node operation, DataFlow::Node completed) {
  exists(Await awaited |
    DataFlow::localFlow(operation, DataFlow::exprNode(awaited.getValue())) and
    completed = DataFlow::exprNode(awaited)
  )
}

/** The awaited policy result, not arbitrary functions named `admit`. */
predicate admittedAddress(DataFlow::Node address) {
  exists(DataFlow::MethodCallNode operation |
    providerCall(operation, "_policy", "admit") and awaitedResult(operation, address)
  )
}

/** Destination arguments of the owned data-bearing backend, excluding DNS probes. */
predicate connectionDestination(DataFlow::Node destination) {
  exists(DataFlow::MethodCallNode call |
    providerCall(call, "_backend", "connect_tcp") and
    destination = [call.getArg(0), call.getArgByName("host")]
  )
}

/** An admitted endpoint's address keeps the endpoint's admission provenance. */
module AdmissionConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) { admittedAddress(source) }

  predicate isSink(DataFlow::Node sink) { connectionDestination(sink) }

  predicate isAdditionalFlowStep(DataFlow::Node source, DataFlow::Node sink) {
    exists(DataFlow::AttrRead read |
      read.getAttributeName() = "address" and read.getObject() = source and sink = read
    )
  }
}

module AdmissionFlow = DataFlow::Global<AdmissionConfig>;

/**
 * Unadmitted origins remain unapproved when another branch supplies an
 * admitted result to the same destination variable. Local origins include
 * parameters, field reads and call results; admitted helper results and
 * endpoint fields retain their separately established provenance.
 */
module UnadmittedDestinationConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) {
    source instanceof DataFlow::LocalSourceNode and
    // A closure entry carries its caller's value; it is not a fresh origin.
    not source instanceof DataFlow::ScopeEntryDefinitionNode and
    source.getScope().getEnclosingModule().getName() = "httpx_secure._network" and
    not exists(DataFlow::Node admitted |
      admittedAddress(admitted) and AdmissionFlow::flow(admitted, source)
    )
  }

  predicate isSink(DataFlow::Node sink) { connectionDestination(sink) }

  predicate isBarrier(DataFlow::Node node) { admittedAddress(node) }
}

module UnadmittedDestinationFlow = TaintTracking::Global<UnadmittedDestinationConfig>;

module HostnameConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) { originalHostname(source) }

  predicate isSink(DataFlow::Node sink) { connectionDestination(sink) }

  predicate isBarrier(DataFlow::Node node) { admittedAddress(node) }
}

module HostnameFlow = TaintTracking::Global<HostnameConfig>;
