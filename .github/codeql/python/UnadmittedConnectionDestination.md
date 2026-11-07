# Protected connection destination lacks policy admission

A numeric IP is not automatically an authorized destination. The data-bearing
connector must use the selected address returned by its own policy, rather than
a constant, unrelated resolver, or fallback used after admission fails.

## Recommendation

Connect using the admitted endpoint and preserve its port and local-bind
parameters. Propagate admission failures instead of creating an alternative
connection.

This query checks admission-result provenance through aliases and helpers,
including the admitted endpoint's address field. It tracks unadmitted
alternatives independently, so a valid branch does not hide a fallback.
The companion hostname-flow query detects logical-hostname bypasses.
These checks do not prove the implementation of arbitrary custom validators.
