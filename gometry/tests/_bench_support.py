"""Producer-identity fixtures shared by the benchmark artifact tests."""

from __future__ import annotations

import hashlib

HASH = hashlib.sha256(b'x').hexdigest()


def file_identity(path: str = '/checkout/file') -> dict[str, object]:
    return {'path': path, 'sha256': HASH, 'size_bytes': 1}


def _distribution(name: str) -> dict[str, object]:
    return {
        'name': name,
        'version': '1',
        'location': '/checkout',
        'metadata': file_identity(f'/checkout/{name}.dist-info/METADATA'),
        'record': file_identity(f'/checkout/{name}.dist-info/RECORD'),
        'record_available': True,
        '_record_files': [
            f'/checkout/{name}/__init__.py',
            f'/checkout/{name}/lib.cpython.so',
            f'/checkout/{name}/_crs.cpython.so',
        ],
    }


def package_identity(name: str) -> dict[str, object]:
    return {
        'available': True,
        'version': '1',
        'module': file_identity(f'/checkout/{name}/__init__.py'),
        'distribution': _distribution(name),
        'native_libraries': [],
    }


def producer(row: str, side: str, lock_path: str = 'lock') -> dict[str, object]:
    packages = {
        name: package_identity(name)
        for name in (
            'gometry',
            'numpy',
            'shapely',
            'pyproj',
            'h3',
            'geopandas',
            'pyarrow',
            'mercantile',
            'pygeohash',
        )
    }
    extension = {
        **file_identity('/checkout/target/release/gometry.so'),
        'build_info': {
            'schema_version': 1,
            'source_inputs_format': 'sha256-path-nul-content-nul',
            'source_inputs_sha256': HASH,
            'profile_family': 'release',
            'opt_level': '3',
            'debug': False,
            'debug_assertions': False,
            'target': 'test-target',
            'target_features': 'sse2',
        },
    }
    runtime = 'gometry' if side == 'A' else 'comparator'
    package_name = 'gometry' if runtime == 'gometry' else row.split('.', 1)[0]
    geos = file_identity('/checkout/libgeos_c.so')
    proj = file_identity('/checkout/libproj.so')
    python_native = file_identity('/checkout/libpython.so')
    native_libraries = (
        [{key: extension[key] for key in ('path', 'sha256', 'size_bytes')}]
        if runtime == 'gometry'
        else (
            [geos]
            if package_name in {'shapely', 'geopandas'}
            else [proj]
            if package_name == 'pyproj'
            else []
        )
    )
    implementation = {
        'runtime': runtime,
        'row': row,
        'module_name': 'gometry._lib' if runtime == 'gometry' else package_name,
        'module': packages[package_name]['module'],
        'distribution': packages[package_name]['distribution'],
        'native_libraries': native_libraries,
        'native_library_status': 'identified' if native_libraries else 'not-applicable',
    }
    return {
        'identity_schema_version': 1,
        'git_commit': 'deadbeef',
        'source_inputs_sha256': HASH,
        'extension': extension,
        'uv_lock': {'path': lock_path, 'sha256': HASH, 'size_bytes': 1},
        'python': {
            'executable': '/python',
            'version': '3.14 test build',
            'implementation': 'CPython',
            'cache_tag': 'cpython-314',
            'build': {
                'implementation': 'CPython',
                'version': '3.14',
                'build': ['build', 'date'],
                'debug': False,
            },
        },
        'packages': packages,
        'loaded_native_libraries': [
            {key: extension[key] for key in ('path', 'sha256', 'size_bytes')},
            geos,
            proj,
            python_native,
        ],
        'native_library_identity': {
            'available': True,
            'source': '/proc/self/maps',
            'reason': None,
        },
        'implementation': implementation,
        'arm': {'side': side, 'row': row},
    }
