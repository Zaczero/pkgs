---
description: Install gometry from PyPI or a source checkout — NumPy is required, with no system GEOS, GDAL, or PROJ installation.
---

# Installation

gometry is a compiled Rust extension behind a thin Python facade. Install the
matching wheel from PyPI with one command; there is no system GIS library to
track down first:

```bash
python -m pip install gometry
# or
uv add gometry
```

## Development install

From a clone of this repository, with Python **3.11+** and a project virtualenv:

```bash
# create / activate a venv, then:
uv sync                                  # or: python -m venv .venv && .venv/bin/pip install -e .
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
| Python | **3.11 or newer.** gometry uses modern `X | Y` unions and assumes 3.11+ typing throughout. |
| Platform | Prebuilt wheels cover the CI-supported tags listed below. |
| Runtime dependencies | **NumPy.** Required for native numeric bulk outputs. |
| System libraries | **None.** No GEOS, GDAL, or system PROJ. |
| Build (from source) | Recent nightly Rust toolchain, a C/C++ toolchain, Python headers, and network/cache access to the dependency graph. |

## No system GIS stack to install

The usual Python geospatial install fails at the C library boundary: Shapely needs GEOS,
pyproj needs PROJ, and a mismatched system package turns a one-line install into an
afternoon. gometry removes that failure mode by design.

- The geometry, geodesy, indexing, and grid kernels are **implemented in Rust**, not bound
  to GEOS or GDAL.
- The CRS authority backend is **[libPROJ](https://proj.org/), bundled inside the wheel**. You get the
  bundled PROJ database, supported datum pipelines, and the grid files shipped with the package without a
  system PROJ shared library or a `PROJ_LIB` data directory. Additional caller-supplied grids can be made
  visible with `gm.crs_configure(search_paths=...)`; gometry does not download grids at runtime.

!!! note "Why this matters"
    A gometry wheel is self-contained. CI images, slim containers, and lambda-style
    deployments do not need `apt-get install libgeos-dev libproj-dev gdal-bin` — the
    geometry engine and the CRS database travel with the package.

## Required Python dependency

gometry depends on [NumPy](https://numpy.org/) because bulk numeric, boolean,
index, and coordinate results are native read-only `numpy.ndarray` objects:

```bash
python -c "import gometry, numpy"
```

This keeps the runtime dependency surface small while making the common
vectorized path first-class in the Python numeric ecosystem. pandas,
GeoPandas, and PyArrow remain things you *may* hand data to or from, not
things gometry forces into your environment.

## Optional extras

Core install is NumPy + the Rust extension. Everything else is opt-in and
imported lazily — see [DataFrames](../ecosystem/dataframes.md).

| Extra | Purpose |
|---|---|
| `arrow` | [`to_arrow`][gometry.Geometry.to_arrow] / [`gm.from_arrow`][gometry.from_arrow] pyarrow materialization |
| `pandas` | concrete extension storage via `arr.to_pandas()` / `gm.from_pandas()` |
| `polars` | WKB/EWKB Binary Series via `arr.to_polars()` / `gm.from_polars()` |
| `geopandas` | vectorized `GeoSeries` / `GeoDataFrame` conversion |
| `viz` | `gm.explore` + lonboard notebook previews |

### The `arrow` extra

[PyArrow](https://arrow.apache.org/docs/python/) is enabled through the `arrow`
extra. Install it when you want gometry itself to materialize or consume
`pyarrow` arrays, chunked arrays, tables, and record batches:

```bash
python -m pip install "gometry[arrow]"
# or
uv add "gometry[arrow]"
```

With the extra installed, [geom.to_arrow][gometry.Geometry.to_arrow] and
[gm.from_arrow][gometry.from_arrow] exchange pyarrow objects carrying
[GeoArrow](https://geoarrow.org/)-compatible buffers:

```python
import gometry as gm

points = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
ga = points.to_arrow()           # requires pyarrow / gometry[arrow]
roundtrip = gm.from_arrow(ga)

```

PyArrow is imported lazily, so `import gometry` never requires it. Geometry
objects still expose dependency-free Arrow PyCapsules without the extra; see the
[Arrow & storage](../ecosystem/arrow.md) for the full columnar boundary.

## Prebuilt wheels

Release wheels target these CI-supported tags:

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

CI also tests free-threaded `cp314t` on Linux and cibuildwheel 4 builds its
supported free-threaded wheel tags (including musllinux alongside manylinux).
PyPy and `cp313t` are not part of the current matrix.

If a target has no matching wheel, pip falls back to a source build. Source
builds require a recent nightly Rust toolchain, a working C/C++ build
environment, Python headers for the target interpreter, and network or cached
access to the Rust/Python dependency graph. The wheel still bundles libPROJ; you
do not need system GEOS, GDAL, or PROJ.

## Verify your install

Run this once after installing to confirm the native extension, the bundled PROJ database,
and the geometry kernels are all wired up:

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

If that prints a version, a PROJ version, and an area in square meters, you are ready.

## Next steps

- New to the model-explicit design? Start with the [mental model](mental-model.md) — it is
  the one page that makes everything else click.
- Want a fast tour of the API? See the [quickstart](quickstart.md).
- Coming from Shapely, pyproj, H3, S2, or rtree? The [migration guides](../migrating/index.md)
  map each tool's patterns onto one gometry spelling.
