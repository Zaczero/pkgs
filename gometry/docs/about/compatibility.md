---
description: gometry runtime, platform, optional-integration, and semantic-versioning compatibility matrix.
---

# Compatibility & deprecation policy

gometry runs on CPython 3.11 or newer, requires NumPy, and needs no system GEOS,
GDAL, or PROJ. Incompatible public removals wait for a major release.

## Runtime and platform matrix

| Surface | Supported |
| --- | --- |
| CPython | 3.11 or newer |
| Free-threaded CPython | 3.14t |
| PyPy | Unsupported |
| Linux | glibc and musl, `x86_64` and `aarch64` |
| macOS | 11 or newer, `x86_64` and `arm64` |
| Windows | `amd64` and `arm64` |

The package contains the geometry engine and its CRS authority resources. A core
installation requires NumPy; it does not require system GEOS, GDAL, or PROJ.
See [Installation](../get-started/installation.md) for the install command and
source-build prerequisites.

## Optional integration matrix

Optional dependencies are not imported by `import gometry`. Minimum package
requirements come from the published package metadata. Each adapter defines its
copy, CRS, epoch, missing-row, and loss behavior.

| Boundary | Extra and minimum dependency | Supported entry points | Important boundary behavior |
| --- | --- | --- | --- |
| Arrow / GeoArrow | `gometry[arrow]`, `pyarrow>=24.0.0` for PyArrow objects | [`to_arrow`][gometry.Geometry.to_arrow], [`from_arrow`][gometry.from_arrow] | Native Arrow capsules do not require PyArrow; the C provider is a trusted ABI participant and buffer contents are validated. |
| pandas | `gometry[pandas]`, `pandas>=3.0.3` | [`to_pandas`][gometry.GeometryArray.to_pandas], [`from_pandas`][gometry.from_pandas] | Uses gometry's extension storage; missing rows remain missing. |
| Polars | `gometry[polars]`, `polars>=1.42.0` | [`to_polars`][gometry.GeometryArray.to_polars], [`from_polars`][gometry.from_polars] | Uses WKB/EWKB binary storage; non-EPSG CRS and coordinate epochs need explicit restoration or loss acknowledgement. |
| GeoPandas | `gometry[geopandas]`, `geopandas>=1.1.4` | [`to_geopandas`][gometry.GeometryArray.to_geopandas], [`from_geopandas`][gometry.from_geopandas] | Converts through the GeoPandas/Shapely boundary; coordinate epochs require explicit loss acknowledgement on export. |
| GeoParquet | `gometry[arrow]` | [`to_geoparquet`][gometry.GeometryArray.to_geoparquet], [`from_geoparquet`][gometry.from_geoparquet] | Uses PyArrow and GeoParquet column metadata; see [Arrow & storage](../ecosystem/arrow.md) for CRS and epoch portability. |
| lonboard maps | `gometry[viz]`, `lonboard>=0.16.0` plus its map runtime | [`explore`][gometry.explore], notebook HTML preview | Reproject to a WGS 84 display frame when the source CRS is not equivalent to WGS 84. |
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
warning. A warning names the replacement when one exists.

## Numeric parsing guarantees

WKT and GeoJSON ingest parse ordinates as binary64 values. A finite decimal that
gometry writes in its shortest round-trip form re-imports to the same `float64`;
mapping integers must be exactly representable as binary64 when they are used as
geometry coordinates. Feature properties and IDs remain Python side data and are
not converted to binary64 merely because they arrived as GeoJSON text.

## No legacy aliases

gometry does not keep a second spelling solely for compatibility. Unary geometry
facts are properties ([`geom.area`][gometry.Geometry.area]), unary transforms are methods ([`geom.buffer`][gometry.Geometry.buffer]),
and binary relationships are free functions ([`gm.contains(a, b)`][gometry.contains]). The
[migration guide](../migrating/index.md) maps common source-library names to
these canonical entry points.

## See also

- [Installation](../get-started/installation.md) — install command, required
  dependency, and source-build requirements.
- [License](license.md) — package and bundled-component licensing.
- [Support](support.md) — issue tracker, security disclosure, and commercial
  support.
