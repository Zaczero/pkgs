---
description: gometry 1.0.0 runtime, platform, optional-integration, and semantic-versioning compatibility matrix.
---

# Compatibility & deprecation policy

gometry 1.0.0 is the first public release. Everything stated for the 1.0.0 line
holds until a major version changes it.

## Runtime and platform matrix

| Surface | Supported in the 1.0.0 release line | Notes |
| --- | --- | --- |
| CPython | 3.11, 3.12, 3.13, and 3.14 | The core package requires Python 3.11 or newer. |
| CPython free-threaded | 3.14t | The published no-GIL target is `cp314t`; `cp313t` is not in the supported wheel matrix. |
| PyPy | No documented wheel matrix | Use CPython for the documented wheels and runtime behavior. |
| Linux | manylinux and musllinux, `x86_64` and `aarch64` | Use a matching wheel or build from source. |
| macOS | 11 or newer, `x86_64` and `arm64` | Use a matching wheel or build from source. |
| Windows | `win_amd64` and `win_arm64` | Use a matching wheel or build from source. |
| Source builds | Supported for CPython 3.11+ with the documented Rust/C++ build prerequisites | The extension and bundled CRS resources are built for the target interpreter. |

The wheel contains the geometry engine and its CRS authority resources. A core
installation requires NumPy; it does not require system GEOS, GDAL, or PROJ.
See [Installation](../get-started/installation.md) for wheel selection and
source-build prerequisites.

## Optional integration matrix

Optional dependencies are not imported by `import gometry`. Minimum package
requirements come from the published package metadata. Each adapter defines its
copy, CRS, epoch, missing-row, and loss behavior.

| Boundary | Extra and minimum dependency | Supported entry points | Important boundary behavior |
| --- | --- | --- | --- |
| Arrow / GeoArrow | `gometry[arrow]`, `pyarrow>=24.0.0` for PyArrow objects | `to_arrow`, `from_arrow` | Native Arrow capsules do not require PyArrow; the C provider is a trusted ABI participant and buffer contents are validated. |
| pandas | `gometry[pandas]`, `pandas>=3.0.3` | `to_pandas`, `from_pandas` | Uses gometry's extension storage; missing rows remain missing. |
| Polars | `gometry[polars]`, `polars>=1.42.0` | `to_polars`, `from_polars` | Uses WKB/EWKB binary storage; non-EPSG CRS and coordinate epochs need explicit restoration or loss acknowledgement. |
| GeoPandas | `gometry[geopandas]`, `geopandas>=1.1.4` | `to_geopandas`, `from_geopandas` | Converts through the GeoPandas/Shapely boundary; coordinate epochs require explicit loss acknowledgement on export. |
| GeoParquet | `gometry[arrow]` | `to_geoparquet`, `from_geoparquet` | Uses PyArrow and GeoParquet column metadata; see [Arrow & storage](../ecosystem/arrow.md) for CRS and epoch portability. |
| lonboard maps | `gometry[viz]`, `lonboard>=0.16.0` plus its map runtime | `explore`, notebook HTML preview | Reproject to a WGS 84 display frame when the source CRS is not equivalent to WGS 84. |
| NumPy | Core dependency `numpy>=1.26` | Coordinate views and dense result arrays | Numeric results are read-only NumPy arrays; gometry is not a Python Array API provider. See [Array API compatibility](../ecosystem/numpy.md#python-array-api-compatibility). |

For protocol-level details, see [Ecosystem & interoperability](../ecosystem/index.md),
[Arrow & storage](../ecosystem/arrow.md), and [Text & binary formats](../ecosystem/text-formats.md).

## Semantic versioning

- Patch releases fix bugs and documentation without intentionally breaking the
  documented public contract.
- Minor releases may add public APIs and may deprecate APIs with warnings before
  removal.
- Incompatible public removals wait for the next major release unless the API is
  security-sensitive or demonstrably unusable as documented.

The normal deprecation window is at least one minor release with a runtime
warning and a changelog entry. A warning names the replacement when one exists.

## Numeric parsing guarantees

WKT and GeoJSON ingest parse ordinates as binary64 values. A finite decimal that
gometry writes in its shortest round-trip form re-imports to the same `float64`;
mapping integers must be exactly representable as binary64 when they are used as
geometry coordinates. Feature properties and IDs remain Python side data and are
not converted to binary64 merely because they arrived as GeoJSON text.

## No legacy aliases

gometry does not keep a second spelling solely for compatibility. Unary geometry
facts are properties (`geom.area`), unary transforms are methods (`geom.buffer`),
and binary relationships are free functions (`gm.contains(a, b)`). The
[migration guide](../migrating/index.md) maps common source-library names to
these canonical entry points.

## See also

- [Installation](../get-started/installation.md) — supported wheel tags and
  source-build requirements.
- [Changelog](changelog.md) — user-observable changes by release.
- [License](license.md) — package and bundled-component licensing.
