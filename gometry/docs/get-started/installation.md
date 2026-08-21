---
description: Install gometry from PyPI or a source checkout — NumPy is required, with no system GEOS, GDAL, or PROJ installation.
---

# Installation

gometry is a compiled Rust extension behind a Python facade. Install the matching
wheel from PyPI:

=== "uv"

    ```bash
    uv add gometry
    ```

=== "pip"

    ```bash
    pip install gometry
    ```

## Development install

From a clone of this repository, with Python **3.11+** and a project virtualenv:

```bash
# create a venv, then install all project groups:
uv sync --all-groups                     # or: python -m venv .venv && .venv/bin/pip install -e .
uv run --no-project --python .venv/bin/python \
  --with maturin==1.14.1 maturin develop --release
```

That builds and installs the native extension into `.venv`. Verify with:

```bash
.venv/bin/python -c "import gometry as gm; print(gm.__version__)"
```

## Requirements

| Requirement | Detail |
|---|---|
| Python | **3.11 or newer.** The stubs use `X | Y` unions and 3.11+ typing throughout. |
| Runtime dependencies | **NumPy.** Required for native numeric bulk outputs. |
| System libraries | **None.** No GEOS, GDAL, or system PROJ. |
| Build (from source) | Recent nightly Rust toolchain, a C/C++ toolchain, Python headers, and network/cache access to the dependency graph. |

## No system GIS stack to install

The core wheel carries the native geometry engine and its CRS authority resources
at the package boundary, so the application does not need to provision GEOS,
GDAL, or a system PROJ installation separately.

- The geometry, geodesy, indexing, and grid kernels are **implemented in Rust**, not bound
  to GEOS or GDAL.
- The CRS authority backend is **[libPROJ](https://proj.org/), bundled inside the wheel**. You get the
  bundled PROJ database, supported datum pipelines, and the grid files shipped with the package without a
  system PROJ shared library or a `PROJ_LIB` data directory. Additional caller-supplied grids can be made
  visible with `gm.crs_configure(search_paths=...)`; gometry does not download grids at runtime.

## Required Python dependency

gometry depends on [NumPy](https://numpy.org/) because bulk numeric, boolean,
index, and coordinate results are native read-only `numpy.ndarray` objects:

```bash
python -c "import gometry, numpy"
```

NumPy keeps the runtime dependency surface limited while providing native
ndarray results; pandas, GeoPandas, and PyArrow are optional data interchange
dependencies.

## Optional extras

Core install is NumPy plus the Rust extension. Optional integrations are opt-in
and imported lazily; see [DataFrames](../ecosystem/dataframes.md).

| Extra | Purpose |
|---|---|
| `arrow` | [`to_arrow`][gometry.Geometry.to_arrow] / [`gm.from_arrow`][gometry.from_arrow] pyarrow materialization |
| `pandas` | concrete extension storage via `arr.to_pandas()` / `gm.from_pandas()` |
| `polars` | WKB/EWKB Binary Series via `arr.to_polars()` / `gm.from_polars()` |
| `geopandas` | vectorized `GeoSeries` / `GeoDataFrame` conversion |
| `viz` | `gm.explore` + lonboard notebook previews |

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

## Prebuilt wheels

Release wheels target these platform and interpreter tags:

| Platform | Architecture | Python tags |
|---|---|---|
| Linux manylinux | `x86_64` | `cp311`, `cp312`, `cp313`, `cp314` |
| Linux manylinux | `aarch64` | `cp311`, `cp312`, `cp313`, `cp314` |
| Linux musllinux | `x86_64` | `cp311`, `cp312`, `cp313`, `cp314` |
| Linux musllinux | `aarch64` | `cp311`, `cp312`, `cp313`, `cp314` |
| macOS 11+ | `x86_64` | `cp311`, `cp312`, `cp313`, `cp314` |
| macOS 11+ | `arm64` | `cp311`, `cp312`, `cp313`, `cp314` |
| Windows | `win_amd64` | `cp311`, `cp312`, `cp313`, `cp314` |
| Windows | `win_arm64` | `cp311`, `cp312`, `cp313`, `cp314` |

Free-threaded `cp314t` wheels are supported on Linux in manylinux and musllinux
variants. PyPy and `cp313t` are unsupported.

If a target has no matching wheel, pip falls back to a source build. Source
builds require a recent nightly Rust toolchain, a working C/C++ build
environment, Python headers for the target interpreter, and network or cached
access to the Rust/Python dependency graph. The wheel still bundles libPROJ; you
do not need system GEOS, GDAL, or PROJ.

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

## Next steps

- The [mental model](mental-model.md) defines CRS and frame rules.
- The [quickstart](quickstart.md) constructs a spatial query.
- The [migration guides](../migrating/index.md) map Shapely, pyproj, H3, S2, and rtree
  patterns onto gometry spellings.
