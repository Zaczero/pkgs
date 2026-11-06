/**
 * @name Unadmitted hostname reaches a protected connection
 * @description Using the logical hostname instead of the admitted numeric IP can
 *              resolve a different destination after authorization.
 * @kind path-problem
 * @problem.severity error
 * @security-severity 8.1
 * @precision high
 * @id pkgs/python/unadmitted-connection-hostname
 * @tags security
 *       correctness
 *       external/cwe/cwe-918
 */

import ProtectedConnections
import HostnameFlow::PathGraph

from HostnameFlow::PathNode source, HostnameFlow::PathNode sink
where HostnameFlow::flowPath(source, sink)
select sink.getNode(), source, sink,
  "The data-bearing connection uses this $@ instead of the policy's admitted address.",
  source.getNode(), "unadmitted hostname"
