# abi3audit divergence

abi3audit 0.0.26 is vendored from its PyPI source distribution, SHA-256
`44b5fa5ddc8acdfdbd221f400d48b8a205df519e8f4c6d57a349837ac88d0ff8`.

The scanner recognizes Python 3.15's `PyModExport_*` module entry points
alongside `PyInit_*`. These are extension-defined exports, not calls to
non-stable interpreter APIs. The source patch is in `patches/abi3audit/`.
Remove the vendored copy and the `CIBW_AUDIT_REQUIRES` override when an
upstream release accepts these entry points.
