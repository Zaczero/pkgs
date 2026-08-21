"""CRS behavior — geodesic measurement, transforms, best-UTM selection,
runtime config, cache info, and the PROJ authority metadata surface.
"""

from pathlib import Path

import gometry as gm
import pytest


def test_crs_runtime_config_controls_proj_contexts_and_resets(tmp_path: Path) -> None:
    original = gm.crs_config()
    assert original == {'search_paths': None, 'user_writable_directory': None}
    before = gm.crs_info(4326)
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
        assert gm.crs_info(4326) == before
        assert gm.Point(1, 2, crs=4326).to_crs(3857).crs == 'EPSG:3857'
        assert gm.crs_clear_cache() is None
        assert gm.crs_info(4326) == before
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
    }

    gm.crs_info(4326)
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
