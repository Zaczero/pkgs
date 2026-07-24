"""Optional integrations stay lazy and never mutate framework globals."""

from __future__ import annotations

import subprocess
import sys
import textwrap
import tomllib
from pathlib import Path

import pytest


def _run_isolated(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, '-c', textwrap.dedent(script)],
        check=True,
        capture_output=True,
        text=True,
    )


def test_optional_extra_vocabulary_is_minimal_and_independent() -> None:
    project = tomllib.loads(Path('pyproject.toml').read_text(encoding='utf-8'))
    extras = project['project']['optional-dependencies']

    assert set(extras) == {'arrow', 'pandas', 'polars', 'geopandas', 'viz'}
    assert not any(
        dependency.startswith('pyarrow')
        for extra in ('pandas', 'polars')
        for dependency in extras[extra]
    )


def test_import_gometry_stays_dep_free() -> None:
    _run_isolated(
        """
        import sys
        import gometry

        heavy = {'pandas', 'polars', 'pyarrow', 'geopandas', 'lonboard'}
        assert not heavy & set(sys.modules)
        """
    )


def test_star_import_is_core_only_and_dep_free() -> None:
    _run_isolated(
        """
        import sys
        from gometry import *

        heavy = {'pandas', 'polars', 'pyarrow', 'geopandas', 'lonboard'}
        assert not heavy & set(sys.modules)
        assert 'GeometryArray' in globals()
        assert 'from_pandas' not in globals()
        assert 'from_polars' not in globals()
        assert 'from_geopandas' not in globals()
        assert 'from_geoparquet' not in globals()
        assert 'GeometryDtype' not in globals()
        assert 'explore' not in globals()
        """
    )


def test_minimal_install_introspection_does_not_resolve_optional_exports() -> None:
    _run_isolated(
        """
        import pydoc
        import sys

        for dependency in ('pandas', 'polars', 'pyarrow', 'geopandas', 'lonboard'):
            sys.modules[dependency] = None

        import gometry

        rendered = pydoc.render_doc(gometry)
        assert 'GeometryArray' in rendered
        assert 'GeometryDtype' not in dir(gometry)
        assert 'from_pandas' not in dir(gometry)
        assert 'gometry._pandas' not in sys.modules
        assert 'gometry._polars' not in sys.modules
        assert 'gometry._geoparquet' not in sys.modules
        assert 'gometry._viz' not in sys.modules
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
    ('attribute', 'dependency', 'extra'),
    [
        ('from_pandas', 'pandas', 'pandas'),
        ('from_polars', 'polars', 'polars'),
    ],
)
def test_missing_dataframe_extra_has_targeted_install_hint(
    attribute: str,
    dependency: str,
    extra: str,
) -> None:
    _run_isolated(
        f"""
        import sys
        sys.modules[{dependency!r}] = None

        import gometry as gm

        try:
            getattr(gm, {attribute!r})
        except ModuleNotFoundError as error:
            assert error.name == {dependency!r}
            assert 'gometry[{extra}]' in str(error)
        else:
            raise AssertionError('missing optional dependency was accepted')
        """
    )


@pytest.mark.parametrize(
    ('attribute', 'dependency'),
    [('from_geoparquet', 'pyarrow'), ('explore', 'lonboard')],
)
def test_lazy_exports_do_not_import_their_heavy_dependency(
    attribute: str,
    dependency: str,
) -> None:
    _run_isolated(
        f"""
        import sys
        import gometry as gm

        assert {dependency!r} not in sys.modules
        assert callable(getattr(gm, {attribute!r}))
        assert {dependency!r} not in sys.modules
        """
    )
