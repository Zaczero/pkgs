"""Optional integrations stay import-cheap and never mutate framework globals."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from importlib import metadata

import gometry as gm
import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def _run_isolated(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, '-c', textwrap.dedent(script)],
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize('extra', ['pandas', 'polars'])
def test_dataframe_extra_is_pyarrow_independent(extra: str) -> None:
    distribution = metadata.distribution('gometry')
    assert extra in (distribution.metadata.get_all('Provides-Extra') or [])

    requirements = [Requirement(value) for value in distribution.requires or ()]
    assert not any(
        canonicalize_name(requirement.name) == 'pyarrow'
        and (
            requirement.marker is None
            or requirement.marker.evaluate(environment={'extra': extra})
        )
        for requirement in requirements
    )


def test_import_gometry_stays_dep_free() -> None:
    _run_isolated(
        """
        import sys
        import gometry

        heavy = {'pandas', 'polars', 'pyarrow', 'arro3', 'geopandas', 'lonboard'}
        assert not heavy & set(sys.modules)
        """
    )


def test_star_import_exposes_optional_exports_without_dependencies() -> None:
    _run_isolated(
        """
        import sys
        from gometry import *

        heavy = {'pandas', 'polars', 'pyarrow', 'arro3', 'geopandas', 'lonboard'}
        assert not heavy & set(sys.modules)
        assert 'GeometryArray' in globals()
        assert {
            'explore',
            'from_geopandas',
            'from_geoparquet',
            'from_pandas',
            'from_polars',
        } <= globals().keys()
        """
    )


def test_minimal_install_introspection_discovers_optional_exports() -> None:
    _run_isolated(
        """
        import inspect
        import pydoc
        import sys

        for dependency in (
            'pandas',
            'polars',
            'pyarrow',
            'arro3',
            'geopandas',
            'lonboard',
        ):
            sys.modules[dependency] = None

        import gometry

        names = {
            'explore',
            'from_geopandas',
            'from_geoparquet',
            'from_pandas',
            'from_polars',
        }
        rendered = pydoc.render_doc(gometry, renderer=pydoc.plaintext)
        assert 'GeometryArray' in rendered
        assert names <= set(dir(gometry))
        assert names <= set(dict(inspect.getmembers(gometry)))
        assert all(f'{name}(' in rendered for name in names)
        assert names <= set(gometry.__all__)
        assert all(callable(getattr(gometry, name)) for name in names)
        assert 'gometry._pandas' not in sys.modules
        """
    )


def test_pandas_and_polars_converters_are_inert_and_pyarrow_independent() -> None:
    _run_isolated(
        """
        import sys

        # Simulate installations containing only the requested dataframe extras.
        sys.modules['pyarrow'] = None

        import pandas as pd
        import polars as pl

        assert not hasattr(pd.Series, 'geo')
        assert not hasattr(pl.Series, 'geo')
        assert not hasattr(pl.Expr, 'geo')
        try:
            pd.api.types.pandas_dtype('gometry.geometry')
        except TypeError:
            pass
        else:
            raise AssertionError('gometry dtype was registered before import')

        import gometry as gm

        values = gm.GeometryArray([
            gm.Point(0, 0, crs=4326),
            None,
            gm.Point(1, 1, crs=4326),
        ])
        pandas_series = values.to_pandas(name='geometry')
        pandas_back = gm.from_pandas(pandas_series)
        assert pandas_back.to_wkt() == values.to_wkt()
        assert pandas_back.crs == values.crs

        polars_series = values.to_polars()
        assert polars_series.dtype == pl.Binary
        polars_back = gm.from_polars(polars_series)
        assert polars_back.to_wkt() == values.to_wkt()
        assert polars_back.crs == values.crs

        assert not hasattr(pd.Series, 'geo')
        assert not hasattr(pl.Series, 'geo')
        assert not hasattr(pl.Expr, 'geo')
        try:
            pd.api.types.pandas_dtype('gometry.geometry')
        except TypeError:
            pass
        else:
            raise AssertionError('converter registered the gometry dtype name')
        """
    )


def test_pandas_arrow_protocol_names_the_arrow_extra() -> None:
    _run_isolated(
        """
        import sys
        sys.modules['pyarrow'] = None

        import gometry as gm

        extension = gm.points([0.0], [1.0]).to_pandas().array
        try:
            extension.__arrow_array__()
        except ModuleNotFoundError as error:
            assert error.name == 'pyarrow'
            assert 'gometry[arrow]' in str(error)
            assert 'gometry[pandas]' not in str(error)
        else:
            raise AssertionError('missing pyarrow was accepted')
        """
    )


def test_optional_dependency_rewrite_is_narrow() -> None:
    from gometry._optional import missing_optional_dependency

    missing_target = ModuleNotFoundError("No module named 'pandas'", name='pandas')
    rewritten = missing_optional_dependency(missing_target, 'pandas', 'install pandas')
    assert rewritten.name == 'pandas'
    assert str(rewritten) == 'install pandas'

    nested = ModuleNotFoundError("No module named 'numpy'", name='numpy')
    with pytest.raises(ModuleNotFoundError) as raised:
        missing_optional_dependency(nested, 'pandas', 'install pandas')
    assert raised.value is nested


@pytest.mark.parametrize(
    ('dependency', 'extra', 'call'),
    [
        pytest.param(
            'pandas',
            'pandas',
            'gm.from_pandas(object())',
            id='from-pandas',
        ),
        pytest.param(
            'pandas',
            'pandas',
            'gm.points([0.0], [1.0]).to_pandas()',
            id='to-pandas',
        ),
        pytest.param(
            'geopandas',
            'geopandas',
            'gm.from_geopandas(object())',
            id='from-geopandas',
        ),
        pytest.param(
            'geopandas',
            'geopandas',
            'gm.points([0.0], [1.0]).to_geopandas()',
            id='to-geopandas',
        ),
        pytest.param(
            'polars',
            'polars',
            'gm.from_polars(object())',
            id='from-polars',
        ),
        pytest.param(
            'polars',
            'polars',
            'gm.points([0.0], [1.0]).to_polars()',
            id='to-polars',
        ),
        pytest.param(
            'lonboard',
            'viz',
            'gm.explore(gm.points([0.0], [1.0], crs=4326))',
            id='explore',
        ),
    ],
)
def test_missing_optional_adapter_extra_has_targeted_install_hint(
    dependency: str,
    extra: str,
    call: str,
) -> None:
    _run_isolated(
        f"""
        import sys
        sys.modules[{dependency!r}] = None

        import gometry as gm

        try:
            {call}
        except ModuleNotFoundError as error:
            assert error.name == {dependency!r}
            assert 'gometry[{extra}]' in str(error)
        else:
            raise AssertionError('missing optional dependency was accepted')
        """
    )


def test_optional_exports_are_discoverable_without_dependency_import() -> None:
    _run_isolated(
        """
        import sys
        import inspect
        import gometry as gm

        names = {
            'explore',
            'from_geopandas',
            'from_geoparquet',
            'from_pandas',
            'from_polars',
        }
        assert names <= set(dir(gm))
        assert names <= set(dict(inspect.getmembers(gm)))
        assert names <= set(gm.__all__)
        assert all(callable(getattr(gm, name)) for name in names)
        assert not {
            'pandas',
            'polars',
            'pyarrow',
            'arro3',
            'geopandas',
            'lonboard',
        } & set(sys.modules)
        """
    )


def test_import_loads_numpy_keeps_optional_adapters_import_cheap() -> None:
    """Fresh process: public adapters are visible without optional imports."""
    code = """
import sys
import gometry as gm
assert "numpy" in sys.modules
assert not hasattr(gm, "TYPE_CHECKING")
assert gm.Cell is __import__("gometry._types", fromlist=["Cell"]).Cell
from gometry._lib import H3Cell
# Protocol structural check still works after identity re-export.
_ = H3Cell
names = {
    "explore",
    "from_geopandas",
    "from_geoparquet",
    "from_pandas",
    "from_polars",
}
assert names <= set(dir(gm))
assert names <= set(gm.__all__)
for name in ("pandas", "polars", "geopandas", "lonboard", "pyarrow", "arro3"):
    assert name not in sys.modules, name
# NumPy surfaces work immediately (core dependency already loaded).
xs = gm.Point(1.0, 2.0).coords.x
assert list(xs) == [1.0]
print("ok")
"""
    result = subprocess.run(
        [sys.executable, '-c', code],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == 'ok'


def test_optional_exports_are_eager_facade_bindings() -> None:
    names = (
        'explore',
        'from_geopandas',
        'from_geoparquet',
        'from_pandas',
        'from_polars',
    )
    assert set(names) <= set(dir(gm))
    assert set(names) <= set(gm.__all__)
    assert set(names) <= vars(gm).keys()
