/**
 * @name Selection probe call resolution
 * @kind table
 * @id pkgs/python/audit-selection-probes
 */

import python
import semmle.python.ApiGraphs
import semmle.python.dataflow.new.DataFlow

from DataFlow::CallCfgNode call, Function function
where
  call = API::moduleImport("anyio").getMember("connect_tcp").getACall() and
  call.getScope() = function
select call, function.getName(), function.getEnclosingScope().toString(),
  function.getEnclosingModule().getName()
