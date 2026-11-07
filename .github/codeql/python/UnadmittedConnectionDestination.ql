/**
 * @name Protected connection destination lacks policy admission
 * @description A protected data-bearing connection must use the policy's
 *              admitted result, including when a literal fallback is available.
 * @kind problem
 * @problem.severity error
 * @security-severity 8.1
 * @precision high
 * @id pkgs/python/unadmitted-connection-destination
 * @tags security
 *       correctness
 *       external/cwe/cwe-918
 */

import ProtectedConnections

from DataFlow::Node destination
where
  connectionDestination(destination) and
  (
    not exists(DataFlow::Node admitted |
      admittedAddress(admitted) and AdmissionFlow::flow(admitted, destination)
    )
    or
    exists(DataFlow::Node origin | UnadmittedDestinationFlow::flow(origin, destination))
  )
select destination,
  "This protected connection destination is not exclusively supplied by the address policy's admitted result."
