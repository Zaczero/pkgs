---
description: Install gometry from PyPI — NumPy is required, with no system GEOS, GDAL, or PROJ installation.
---

# Installation

Install gometry from PyPI on CPython 3.11–3.14 or free-threaded CPython 3.14t.

=== "uv"

    ```bash
    uv add gometry
    ```

=== "pip"

    ```bash
    pip install gometry
    ```

## Requirements

| Requirement | Detail |
|---|---|
| Python | **CPython 3.11–3.14; free-threaded CPython 3.14t.** |
| Runtime dependencies | **NumPy.** `typing-extensions>=4.15` is also installed when Python is below 3.15. |
| System libraries | No GEOS, GDAL, or system PROJ. |
| CPU | **x86_64: x86-64-v2** (roughly 2009 or newer); **arm64: native arm64.** |

On x86_64, importing a wheel checks detectable baseline features before loading
the native extension. If required features are missing, `ImportError` names
them and recommends an x86-64-v2-capable CPU. When the operating system cannot
report CPU features, the check does not block import.

Wheels bundle PROJ and its CRS resources. To add regional transformation grids,
see [Transformation resources](../guide/crs.md#transformation-resources).

## Optional extras

Install the extras for the integrations you use. They are imported only when
needed.

| Extra | Purpose |
|---|---|
| `arrow` | [`to_arrow`][gometry.Geometry.to_arrow] / [`gm.from_arrow`][gometry.from_arrow] pyarrow materialization |
| `pandas` | concrete extension storage via [`arr.to_pandas()`][gometry.GeometryArray.to_pandas] / [`gm.from_pandas()`][gometry.from_pandas] |
| `polars` | WKB/EWKB Binary Series via [`arr.to_polars()`][gometry.GeometryArray.to_polars] / [`gm.from_polars()`][gometry.from_polars] |
| `geopandas` | vectorized `GeoSeries` / `GeoDataFrame` conversion |
| `viz` | [`gm.explore`][gometry.explore] + lonboard notebook previews |

<span id="the-arrow-extra"></span>

For example, add PyArrow support with:

=== "uv"

    ```bash
    uv add "gometry[arrow]"
    ```

=== "pip"

    ```bash
    pip install "gometry[arrow]"
    ```

Geometry objects also expose Arrow PyCapsules without PyArrow installed.
[Arrow & storage](../ecosystem/arrow.md) covers both interfaces.

<span id="verify-your-install"></span>
## Next steps

- [Quickstart](quickstart.md) — find features within a station's service area.
- [Migrating to gometry](../migrating/index.md) — adapt Shapely, pyproj, H3, S2,
  and rtree workflows.
