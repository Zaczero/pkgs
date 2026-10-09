"""Runtime typing gate for precise public signatures and private type support.

Core public annotations resolve against their real runtime globals. Optional
adapter annotations stay postponed in the import-cheap package and are resolved
here with the installed interop namespaces; their checked signatures must retain
the actual external classes.

Also drives the static gates: pyright AND mypy over the shared ``assert_type``
conformance and negative fixtures, and Pyright over its primary precision
fixture. The separate pyo3stubs Pyright gate checks the shipped
``python/gometry`` package at zero errors. Negative misuse carries
``# type: ignore`` whose continued necessity catches a loosened stub.

Usage::

    .venv/bin/python tools/gates/_check_typing_runtime.py

Exit 1 on any failure. The final line is ``TOTAL FAILURES: N``.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import typing
from collections.abc import Hashable, Mapping, Sequence
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
_TOOLS = _ROOT / 'tools'
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))
from _gatelib import prepend_tools_import_paths
from runtime_artifact import require_runtime

prepend_tools_import_paths()
require_runtime(_ROOT)

import gometry
from gometry import _types

_CONFORMANCE = _ROOT / 'tests/typing/test_conformance.py'
_NEGATIVES = _ROOT / 'tests/typing/negatives.py'
_PYRIGHT_PRECISION = _ROOT / 'tests/typing/pyright_precision.py'

_TOP_LEVEL_HINT_TARGETS = (
    'from_geojson',
    'to_feature',
    'to_feature_collection',
)


def _hintable_names(module: object, names: tuple[str, ...]) -> list[str]:
    out: list[str] = []
    for name in names:
        obj = getattr(module, name)
        if isinstance(obj, type):
            out.append(name)
    return out


def _check_get_type_hints(
    label: str,
    obj: object,
    errors: list[str],
    globalns: dict[str, typing.Any] | None = None,
) -> dict[str, typing.Any] | None:
    try:
        return typing.get_type_hints(obj, globalns=globalns)
    except Exception as error:
        errors.append(f'{label}: {error}')
        return None


def _check_get_type_alias(
    label: str,
    alias: object,
    errors: list[str],
    globalns: dict[str, typing.Any],
) -> object | None:
    def carrier() -> None:
        pass

    carrier.__annotations__ = {'return': alias}
    hints = _check_get_type_hints(label, carrier, errors, globalns)
    return None if hints is None else hints['return']


def _check_optional_adapter_hints(errors: list[str]) -> None:
    """Resolve selected adapter signatures against installed nominal types."""
    import geopandas as gpd
    import lonboard
    import numpy as np
    import pandas as pd
    import polars as pl
    import pyarrow as pa
    from pandas._typing import ListLike

    interop_globals: dict[str, typing.Any] = {
        'pd': pd,
        'gpd': gpd,
        'pl': pl,
        'pa': pa,
        'Map': lonboard.Map,
        '_ArrowArray': pa.Array | pa.ChunkedArray,
        'ListLike': ListLike,
        # pandas._typing.ListLike retains these names as unresolved ForwardRefs.
        'ExtensionArray': pd.api.extensions.ExtensionArray,
        'Index': pd.Index,
        'Series': pd.Series,
    }
    resolved_list_like = _check_get_type_alias(
        'pandas._typing.ListLike', ListLike, errors, interop_globals
    )
    if resolved_list_like is None:
        resolved_list_like = ListLike
    geometry_rows = gometry.GeometryArray[gometry.Geometry | None]
    table_attributes = pa.Table | Mapping[str, typing.Any] | None
    contracts = (
        (
            'gometry._from_pandas',
            'from_pandas',
            {
                'series': ('pandas.Series', pd.Series),
                'return': ('GeometryArray[Geometry | None]', geometry_rows),
            },
        ),
        (
            'gometry._geopandas',
            'from_geopandas',
            {
                'geoseries_or_gdf': (
                    'GeoSeries | GeoDataFrame',
                    gpd.GeoSeries | gpd.GeoDataFrame,
                ),
                'return': ('GeometryArray[Geometry | None]', geometry_rows),
            },
        ),
        (
            'gometry._geopandas',
            'to_geopandas',
            {'return': ('geopandas.GeoSeries', gpd.GeoSeries)},
        ),
        (
            'gometry._polars',
            'to_polars',
            {'return': ('polars.Series', pl.Series)},
        ),
        (
            'gometry._polars',
            'from_polars',
            {
                'series': ('polars.Series', pl.Series),
                'return': ('GeometryArray[Geometry | None]', geometry_rows),
            },
        ),
        (
            'gometry._pandas',
            'to_pandas',
            {
                'index': (
                    'pandas.Index | Sequence[Hashable] | None',
                    pd.Index | Sequence[Hashable] | None,
                ),
                'return': ('pandas.Series', pd.Series),
            },
        ),
        (
            'gometry._pandas',
            'GeometryDtype.__from_arrow__',
            {
                'array': (
                    'pyarrow.Array | pyarrow.ChunkedArray',
                    pa.Array | pa.ChunkedArray,
                ),
            },
        ),
        (
            'gometry._pandas',
            'GeometryExtensionArray.__setitem__',
            {
                'key': (
                    'int | numpy.integer[Any] | pandas._typing.ListLike',
                    int | np.integer[typing.Any] | resolved_list_like,
                ),
            },
        ),
        (
            'gometry._geoparquet',
            'to_geoparquet',
            {
                'attributes': (
                    'pyarrow.Table | Mapping[str, Any] | None',
                    table_attributes,
                ),
            },
        ),
        (
            'gometry._geoparquet',
            'from_geoparquet',
            {
                'return': (
                    'tuple[GeometryArray[Geometry | None], pyarrow.Table]',
                    tuple[geometry_rows, pa.Table],
                ),
            },
        ),
        (
            'gometry._viz',
            'explore',
            {
                'attributes': (
                    'pyarrow.Table | Mapping[str, Any] | None',
                    table_attributes,
                ),
                'return': ('lonboard.Map', lonboard.Map),
            },
        ),
    )

    for module_name, member_path, expectations in contracts:
        module = importlib.import_module(module_name)
        member: object = module
        for part in member_path.split('.'):
            member = getattr(member, part)
        # Fill names omitted under TYPE_CHECKING without masking module bindings.
        globalns = dict(interop_globals)
        globalns.update(vars(module))
        label = f'{module_name}.{member_path}'
        hints = _check_get_type_hints(label, member, errors, globalns)
        if hints is None:
            continue
        for name, (expected_name, expected_type) in expectations.items():
            actual = hints.get(name)
            if actual != expected_type:
                errors.append(
                    f'{label}.{name}: expected nominal {expected_name}, got {actual!r}'
                )


def _check_pyright(errors: list[str], label: str, *targets: str) -> None:
    """One pyright run; no targets means the whole configured project scope."""
    result = subprocess.run(
        [
            'pyright',
            *targets,
            '--pythonpath',
            sys.executable,
        ],
        cwd=_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        output = (result.stdout + result.stderr).strip()
        errors.append(f'{label} pyright failed:\n' + output)


def _check_mypy(errors: list[str], label: str, *targets: str) -> None:
    """Run mypy over the same probes — its overload resolution genuinely differs
    from pyright's, and half of gometry's users run it.
    """
    from mypy import api

    # A cache directory private to this gate.  Both typing gates call
    # `mypy.api.run` in-process; sharing mypy's default `.mypy_cache` means each
    # one invalidates the other's build, so alternating them leaves both cold
    # forever (~12.5s and ~10.8s every run instead of ~3.7s warm).  It is also a
    # latent hazard: under xdist the two gates can run concurrently in different
    # workers against one cache directory.  mypy validates cache entries by
    # content hash, so a warm cache still fails on a real error.
    stdout, stderr, status = api.run([
        *targets,
        '--cache-dir',
        str(_ROOT / 'target' / 'mypy-cache' / 'typing-runtime'),
        '--warn-unused-ignores',
        '--no-error-summary',
        '--no-color-output',
    ])
    if status != 0:
        errors.append(f'{label} mypy failed:\n' + (stdout + stderr).strip())


def collect_errors() -> list[str]:
    errors: list[str] = []

    for name in _hintable_names(_types, tuple(_types.__all__)):
        _check_get_type_hints(f'gometry._types.{name}', getattr(_types, name), errors)

    for name in _TOP_LEVEL_HINT_TARGETS:
        _check_get_type_hints(f'gometry.{name}', getattr(gometry, name), errors)

    _check_optional_adapter_hints(errors)

    # Shared conformance, Pyright-primary precision, and negatives run in one
    # Pyright process; shared conformance and negatives run in one Mypy
    # process. The checkers dominate gate runtime. The shipped package is
    # pyo3stubs' `pyright` gate, which holds it at zero under the same
    # [tool.pyright] settings; running it here too only pays twice.
    _check_pyright(
        errors,
        'pyright (conformance + precision + negatives)',
        str(_CONFORMANCE),
        str(_PYRIGHT_PRECISION),
        str(_NEGATIVES),
    )
    _check_mypy(
        errors,
        'mypy (conformance + negatives)',
        str(_CONFORMANCE),
        str(_NEGATIVES),
    )
    return errors


def main() -> int:
    errors = collect_errors()
    for error in errors:
        print(error, file=sys.stderr)
        print(file=sys.stderr)
    print(f'TOTAL FAILURES: {len(errors)}')
    return 1 if errors else 0


if __name__ == '__main__':
    raise SystemExit(main())
