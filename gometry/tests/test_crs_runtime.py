"""CRS behavior — geodesic measurement, transforms, best-UTM selection,
runtime config, cache info, and the PROJ authority metadata surface.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import gometry as gm
import pytest

_GOMETRY_PYTHON = str(Path(gm.__file__).resolve().parent.parent)
_CHILD_TIMEOUT_SEC = 30

_GRS80_LONLAT = '+proj=longlat +ellps=GRS80 +type=crs'
_GRS80_TMERC = '+proj=tmerc +ellps=GRS80 +lat_0=0 +lon_0=0 +k=1 +x_0=0 +y_0=0 +type=crs'
_GRS80_TMERC_PODER = (
    '+proj=tmerc +ellps=GRS80 +lat_0=0 +lon_0=0 +k=1 +x_0=0 +y_0=0 '
    '+algo=poder_engsager +type=crs'
)
_GRS80_TMERC_EVENDEN = (
    '+proj=tmerc +ellps=GRS80 +lat_0=0 +lon_0=0 +k=1 +x_0=0 +y_0=0 '
    '+algo=evenden_snyder +type=crs'
)
_TMERC_AOI = (-10.0, 40.0, 30.0, 60.0)
_HOSTILE_INI = '[general]\nnetwork = on\ntmerc_default_algo = evenden_snyder\n'


def _proj_tmerc_xy(target: str) -> tuple[float, float]:
    gm.crs_clear_cache()
    point = gm.Point(20.0, 50.0, crs=_GRS80_LONLAT)
    out = point.to_crs(target, area_of_interest=_TMERC_AOI)
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'
    return (float(out.x), float(out.y))


def _isolated_child_env(tmpdir: str) -> dict[str, str]:
    return {
        'TMPDIR': tmpdir,
        'PYTHONPATH': _GOMETRY_PYTHON,
        'PYTHONDONTWRITEBYTECODE': '1',
    }


def _run_isolated_child(code: str, tmpdir: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, '-c', code],
        capture_output=True,
        text=True,
        env=_isolated_child_env(tmpdir),
        timeout=_CHILD_TIMEOUT_SEC,
        check=False,
    )


def test_crs_runtime_config_controls_proj_contexts_and_resets(tmp_path: Path) -> None:
    original = gm.crs_configure()
    assert original == {'search_paths': None, 'user_writable_directory': None}
    before = gm.CRS(4326).info
    engine = gm.crs_engine()
    search_paths = None
    if engine['database_path'] is not None:
        search_paths = [str(Path(engine['database_path']).parent)]

    try:
        from gometry import _lib

        native_crs_configure = _lib.crs_configure
        for name in ('crs_download_grid', 'crs_clear_grid_cache'):
            assert not hasattr(gm, name)
            assert not hasattr(_lib, name)

        if search_paths is not None:
            assert (
                gm.crs_configure(search_paths=search_paths[0])['search_paths']
                == search_paths
            )
            assert (
                gm.crs_configure(search_paths=Path(search_paths[0]))['search_paths']
                == search_paths
            )
            assert (
                gm.crs_configure(search_paths=[Path(search_paths[0])])['search_paths']
                == search_paths
            )

            assert (
                native_crs_configure(search_paths=search_paths[0])['search_paths']
                == search_paths
            )
            assert (
                native_crs_configure(search_paths=Path(search_paths[0]))['search_paths']
                == search_paths
            )
            assert (
                native_crs_configure(search_paths=[Path(search_paths[0])])[
                    'search_paths'
                ]
                == search_paths
            )
        configured = gm.crs_configure(
            search_paths=search_paths, user_writable_directory=tmp_path
        )
        assert configured == {
            'search_paths': search_paths,
            'user_writable_directory': str(tmp_path),
        }
        native_configured = native_crs_configure(user_writable_directory=tmp_path)
        assert native_configured['user_writable_directory'] == str(tmp_path)
        configured_engine = gm.crs_engine()
        assert configured_engine['user_writable_directory'] == str(tmp_path)
        assert configured_engine['paths'] == (search_paths or [])
        if search_paths is not None:
            assert (
                configured_engine['database_path'].split('&vfs=', 1)[0]
                == engine['database_path'].split('&vfs=', 1)[0]
            )
        assert gm.CRS(4326).info == before
        assert gm.Point(1, 2, crs=4326).to_crs(3857).crs == 'EPSG:3857'
        assert gm.crs_clear_cache() is None
        assert gm.CRS(4326).info == before
        assert gm.Point(1, 2, crs=4326).to_crs(3857).crs == 'EPSG:3857'

        with pytest.raises(ValueError, match='search_paths entries'):
            gm.crs_configure(search_paths=[''])
        with pytest.raises(ValueError, match='user_writable_directory'):
            gm.crs_configure(user_writable_directory='')
    finally:
        assert gm.crs_reset() == original


def test_crs_cache_info_reports_current_thread_cache_state() -> None:
    gm.crs_clear_cache()
    empty = gm.crs_cache_info()
    assert empty['total_entries'] == 0
    assert empty['total_capacity'] > 0
    assert {bucket['name'] for bucket in empty['buckets']} == {
        'proj_pipeline',
        'proj_diagnostic_pipeline',
        'proj_operation',
        'crs_info',
        'crs_catalog',
        'crs_units',
        'crs_celestial_bodies',
        'crs_non_deprecated',
        'crs_authority_matches',
        'crs_search',
        'crs_operations',
        'crs_comparison',
        'crs_factors',
        'crs_geodesic',
        'crs_export',
        'geodesic_line',
    }

    assert gm.CRS(4326).info['name'] == 'WGS 84'
    gm.CRS(4267).operation(4326)
    gm.CRS(4267).operations(4326)
    gm.CRS(4326).to_wkt()
    warm = gm.crs_cache_info()
    assert warm['generation'] == empty['generation']
    assert warm['total_entries'] >= 4
    assert all(
        0 <= bucket['entries'] <= bucket['capacity'] for bucket in warm['buckets']
    )

    gm.crs_clear_cache()
    cleared = gm.crs_cache_info()
    assert cleared['generation'] > warm['generation']
    assert cleared['total_entries'] == 0


def test_crs_configure_noop_preserves_generation_and_products(tmp_path: Path) -> None:
    original = gm.crs_configure()
    try:
        gm.crs_clear_cache()
        configured = gm.crs_configure(search_paths=tmp_path)
        assert gm.CRS(4326).info['name'] == 'WGS 84'
        before = gm.crs_cache_info()
        assert before['total_entries'] > 0

        assert gm.crs_configure(search_paths=tmp_path) == configured
        after = gm.crs_cache_info()
        assert after['generation'] == before['generation']
        assert after['total_entries'] == before['total_entries']
    finally:
        assert gm.crs_reset() == original


def test_crs_runtime_leaves_no_proj_config_directory(tmp_path: Path) -> None:
    """A public CRS transform must not leave gometry-proj-* under TMPDIR."""
    code = f"""
import os
from pathlib import Path
import gometry as gm
tmp = Path(os.environ['TMPDIR'])
for _ in range(8):
    gm.crs_clear_cache()
    point = gm.Point(20.0, 50.0, crs={_GRS80_LONLAT!r})
    point.to_crs({_GRS80_TMERC!r}, area_of_interest={_TMERC_AOI!r})
    leftover = list(tmp.glob('gometry-proj-*'))
    if leftover:
        raise SystemExit('leftover ' + repr([str(path) for path in leftover]))
print('ok')
"""
    completed = _run_isolated_child(code, str(tmp_path))
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == 'ok'
    assert list(tmp_path.glob('gometry-proj-*')) == []


@pytest.mark.skipif(
    sys.platform != 'linux', reason='RLIMIT_FSIZE is a Linux write limit'
)
def test_crs_runtime_does_not_use_tmpdir_for_configuration(tmp_path: Path) -> None:
    """CRS operation succeeds without a usable temporary directory or writes."""
    unavailable = tmp_path / 'does-not-exist'
    code = """
import os
import resource
import warnings
from pathlib import Path
import gometry as gm
resource.setrlimit(resource.RLIMIT_FSIZE, (0, 0))
tmp = Path(os.environ['TMPDIR'])
if tmp.exists():
    raise SystemExit('TMPDIR unexpectedly exists')
with warnings.catch_warnings(record=True) as seen:
    warnings.simplefilter('always')
    gm.crs_transform(4267, 4326, -104.0, 40.0)
if not any(
    issubclass(item.category, gm.AccuracyWarning)
    and 'us_noaa_conus.tif' in str(item.message)
    for item in seen
):
    raise SystemExit('missing missing-grid warning')
if tmp.exists():
    raise SystemExit('TMPDIR was created')
print('ok')
"""
    completed = _run_isolated_child(code, str(unavailable))
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == 'ok'
    assert not unavailable.exists()


def test_crs_runtime_empty_paths_use_bare_lookup_not_proj_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty configured list keeps cwd lookup but excludes PROJ_DATA."""
    current = tmp_path / 'current'
    ambient = tmp_path / 'ambient'
    current.mkdir()
    ambient.mkdir()
    grid_name = 'us_noaa_conus.tif'
    fixture = Path(__file__).with_name('fixtures') / grid_name
    shutil.copyfile(fixture, current / grid_name)
    shutil.copyfile(fixture, ambient / grid_name)
    original = gm.crs_configure()
    try:
        gm.crs_reset()
        monkeypatch.chdir(current)
        monkeypatch.setenv('PROJ_DATA', str(ambient))
        gm.crs_clear_cache()
        assert gm.crs_grid(grid_name)['available'] is True
        (current / grid_name).unlink()
        gm.crs_clear_cache()
        assert gm.crs_grid(grid_name)['available'] is False
    finally:
        gm.crs_reset()
        if original != {'search_paths': None, 'user_writable_directory': None}:
            assert (
                gm.crs_configure(
                    search_paths=original['search_paths'],
                    user_writable_directory=original['user_writable_directory'],
                )
                == original
            )


def test_crs_runtime_tmerc_ignores_hostile_proj_ini(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Caller and cwd proj.ini must not replace gometry Transverse Mercator defaults."""
    original = gm.crs_configure()
    poder = _proj_tmerc_xy(_GRS80_TMERC_PODER)
    evenden = _proj_tmerc_xy(_GRS80_TMERC_EVENDEN)
    assert poder != evenden
    assert _proj_tmerc_xy(_GRS80_TMERC) == poder
    (tmp_path / 'proj.ini').write_text(_HOSTILE_INI)
    try:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv('PROJ_NETWORK', 'ON')
        monkeypatch.setenv('PROJ_NETWORK_ENDPOINT', 'https://invalid.example/')
        monkeypatch.setenv('PROJ_ONLY_BEST_DEFAULT', 'ON')
        assert _proj_tmerc_xy(_GRS80_TMERC) == poder
        with pytest.warns(gm.AccuracyWarning, match='us_noaa_conus'):
            gm.crs_transform(4267, 4326, -104.0, 40.0)
        gm.crs_configure(search_paths=tmp_path)
        assert _proj_tmerc_xy(_GRS80_TMERC) == poder
        with pytest.warns(gm.AccuracyWarning, match='us_noaa_conus'):
            gm.crs_transform(4267, 4326, -104.0, 40.0)
        gm.crs_reset()
        gm.crs_configure(user_writable_directory=tmp_path)
        assert _proj_tmerc_xy(_GRS80_TMERC) == poder
        gm.crs_configure(search_paths=tmp_path, user_writable_directory=tmp_path)
        assert _proj_tmerc_xy(_GRS80_TMERC) == poder
    finally:
        assert gm.crs_reset() == original
