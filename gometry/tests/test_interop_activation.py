"""Optional integrations stay import-cheap and never mutate framework globals."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from importlib import metadata

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
            getattr(gm, {attribute!r})(object())
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
