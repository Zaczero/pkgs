---
description: Install gometry from PyPI or a source checkout — NumPy is required, with no system GEOS, GDAL, or PROJ installation.
---

# Installation

gometry installs from PyPI:

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
| Python | **3.11 or newer.** |
| Runtime dependencies | **NumPy.** Bulk numeric, boolean, index, and coordinate results are read-only `numpy.ndarray` objects. |
| System libraries | **None.** No GEOS, GDAL, or system PROJ. |

The wheel carries the geometry engine and the CRS authority together: the
bundled PROJ database, its datum pipelines, and the grid files shipped with the
package, with no system PROJ shared library and no `PROJ_LIB` data directory.
[`gm.crs_configure(search_paths=...)`][gometry.crs_configure] makes additional grids of your own visible;
gometry does not download grids at runtime.

## Optional extras

Core install is NumPy plus the Rust extension. Optional integrations are opt-in
and imported lazily; see [DataFrames](../ecosystem/dataframes.md).

| Extra | Purpose |
|---|---|
| `arrow` | [`to_arrow`][gometry.Geometry.to_arrow] / [`gm.from_arrow`][gometry.from_arrow] pyarrow materialization |
| `pandas` | concrete extension storage via [`arr.to_pandas()`][gometry.GeometryArray.to_pandas] / [`gm.from_pandas()`][gometry.from_pandas] |
| `polars` | WKB/EWKB Binary Series via [`arr.to_polars()`][gometry.GeometryArray.to_polars] / [`gm.from_polars()`][gometry.from_polars] |
| `geopandas` | vectorized `GeoSeries` / `GeoDataFrame` conversion |
| `viz` | [`gm.explore`][gometry.explore] + lonboard notebook previews |

### The `arrow` extra

[PyArrow](https://arrow.apache.org/docs/python/) is enabled through the `arrow`
extra. The extra lets gometry materialize or consume `pyarrow` arrays, chunked
arrays, tables, and record batches:

=== "uv"

    ```bash
    uv add "gometry[arrow]"
    ```

=== "pip"

    ```bash
    pip install "gometry[arrow]"
    ```

With the extra installed, [geom.to_arrow][gometry.Geometry.to_arrow] and
[gm.from_arrow][gometry.from_arrow] exchange pyarrow objects carrying
[GeoArrow](https://geoarrow.org/)-compatible buffers:

```python exec="on" source="block" result="text"
import gometry as gm

points = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
ga = points.to_arrow()           # requires pyarrow / gometry[arrow]
roundtrip = gm.from_arrow(ga)

```

PyArrow is imported lazily, so `import gometry` never requires it. Geometry
objects still expose dependency-free Arrow PyCapsules without the extra; see the
[Arrow & storage](../ecosystem/arrow.md) for the columnar boundary.

## Verify your install

```python exec="on" source="block" result="text"
import importlib.metadata as md
import gometry as gm

print("gometry", md.version("gometry"))

# bundled PROJ is available without any system library
print("PROJ", gm.crs_engine()["version"])

# a CRS-driven measurement runs end to end (geographic CRS -> geodesic m^2)
poly = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
print(f"geodesic area: {poly.area:,.0f} m^2")

```

## Build from a checkout

Wheels cover the platforms in [Compatibility](../about/compatibility.md), and
pip falls back to a source build elsewhere. A clone builds the same way, and
needs a recent nightly Rust toolchain, a C/C++ toolchain, and Python headers.

```bash
uv sync --all-groups
uv run --no-project --python .venv/bin/python \
  --with maturin==1.14.1 maturin develop --release
```

## Next steps

- The [mental model](mental-model.md) defines CRS and frame rules.
- The [quickstart](quickstart.md) constructs a spatial query.
- The [migration guides](../migrating/index.md) map Shapely, pyproj, H3, S2, and rtree
  patterns onto gometry spellings.
