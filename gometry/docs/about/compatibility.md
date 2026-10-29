---
description: gometry runtime, platform, optional-integration, and semantic-versioning compatibility matrix.
---

# Compatibility & deprecation policy

gometry supports CPython 3.11–3.15 and free-threaded CPython 3.14t and 3.15t.
NumPy is required; system GEOS, GDAL, and PROJ installations are not.

## Runtime and platform matrix

| Surface | Supported |
| --- | --- |
| Optimized CPython wheels | 3.11–3.15, including 3.14t and 3.15t |
| Generic stable-ABI wheels | CPython 3.15 and newer, standard and free-threaded builds (`abi3` and `abi3t`) |
| Linux | glibc and musl 1.2 or newer, `x86_64` and `aarch64` |
| macOS | 11 or newer, `x86_64` and `arm64` |
| Windows | `amd64` and `arm64` |

See [Installation](../get-started/installation.md) for CPU requirements and
optional extras.

Installers prefer a version-specific optimized wheel when one matches the
interpreter. Generic stable-ABI wheels provide a fallback from CPython 3.15
onward. Both wheel families cover the listed platforms. Runtime dependencies
must also support the selected interpreter and platform.

## Optional integration matrix

Optional dependencies are imported when their adapters are used. Each adapter
defines how it handles copying, CRS, epochs, and missing rows.
On Alpine, the `geopandas` and `viz` extras can require source builds of upstream
dependencies such as Pyogrio and geoarrow-rust-core when matching musllinux
wheels are unavailable. The core, Arrow, pandas, and Polars installations do not
require those extras.

| Boundary | Extra and minimum dependency | Supported entry points | Important boundary behavior |
| --- | --- | --- | --- |
| Arrow / GeoArrow | `gometry[arrow]`, `pyarrow>=24.0.0` for PyArrow objects | [`to_arrow`][gometry.Geometry.to_arrow], [`from_arrow`][gometry.from_arrow] | Geometry uses GeoArrow capsules; cell arrays export primitive IDs or geohash tokens without PyArrow. C providers are trusted ABI participants and their buffer contents are validated. |
| pandas | `gometry[pandas]`, `pandas>=3.0.3`; `gometry[arrow]` for GeoArrow-backed `ArrowDtype` | [`to_pandas`][gometry.GeometryArray.to_pandas], [`from_pandas`][gometry.from_pandas] | Extension storage is shared; object Series rows and GeoArrow-backed Arrow columns are imported, and missing values become missing rows. |
| Polars | `gometry[polars]`, `polars>=1.42.0` | [`to_polars`][gometry.GeometryArray.to_polars], [`from_polars`][gometry.from_polars] | Uses WKB/EWKB binary storage; non-EPSG CRS and coordinate epochs need explicit restoration or loss acknowledgement. |
| GeoPandas | `gometry[geopandas]`, `geopandas>=1.1.4` | [`to_geopandas`][gometry.GeometryArray.to_geopandas], [`from_geopandas`][gometry.from_geopandas] | Converts through the GeoPandas/Shapely boundary; coordinate epochs require explicit loss acknowledgement on export. |
| GeoParquet | `gometry[arrow]` | [`to_geoparquet`][gometry.GeometryArray.to_geoparquet], [`from_geoparquet`][gometry.from_geoparquet] | Uses PyArrow and GeoParquet column metadata; see [Arrow & storage](../ecosystem/arrow.md) for CRS and epoch portability. |
| lonboard maps | `gometry[viz]`, `lonboard>=0.16.0` plus its map runtime | [`explore`][gometry.explore], notebook HTML preview | Reproject to a WGS 84 display frame when the source CRS is not equivalent to WGS 84. |
| NumPy | Core dependency `numpy>=1.26` | Coordinate views and dense result arrays | Numeric results are read-only NumPy arrays; input buffer capture requires a quiescent producer and copies into native storage. Gometry is not a Python Array API provider. See [NumPy arrays & coordinates](../ecosystem/numpy.md). |

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
geometry coordinates. Feature properties and IDs are not converted to binary64
merely because they arrived as GeoJSON text.

## No legacy aliases

The [migration guide](../migrating/index.md) maps source-library names to
gometry's [API conventions](design.md). Compatibility aliases are not provided.

## See also

- [Installation](../get-started/installation.md) — Python and CPU requirements,
  dependencies, and optional extras.
- [License](license.md) — package and bundled-component licensing.
- [Support](support.md) — issue tracker, security disclosure, and commercial
  support.
