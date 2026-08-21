---
description: Where to file gometry bugs and security reports, and what commercial support covers.
---

# Support

## Issues, questions, and feature requests

Bug reports, questions, feature requests, and design discussions go to the
[GitHub issue tracker](https://github.com/Zaczero/pkgs/issues).

Issue reports include the gometry version, the Python version and platform, the
CRS of the geometry involved, and a minimal reproduction.

## Security disclosures

A suspected vulnerability goes through private reporting rather than the public
tracker. [Security & untrusted input](security.md#reporting-vulnerabilities)
carries the channel, along with the threat model for parsing and the resource
boundaries that apply to caller-supplied geometry.

## Premium support

Paid engineering support is available for teams building on gometry, covering
the surrounding spatial pipeline as well as the library.

Common scopes:

- **Correctness review** — CRS declarations, frame and unit choices, geodesic
  versus planar metrics, and antimeridian and validity handling, before the
  results reach production.
- **Migration** from Shapely, pyproj, h3-py, s2sphere, or rtree, with an
  equivalence plan for the operations already in the pipeline.
- **Performance audit** — profiling a real workload, moving scalar loops onto
  array kernels, and tuning cover rules and index plans.
- **Prioritized fixes and features** on a defined timeline.

Reach out at [monicz.dev](https://monicz.dev/#get-in-touch).
