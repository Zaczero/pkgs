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
        'location': '/checkout/site-packages',
        'metadata': file_identity(f'/checkout/site-packages/{name}.dist-info/METADATA'),
        'record': file_identity(f'/checkout/site-packages/{name}.dist-info/RECORD'),
        'record_available': True,
        '_record_files': [
            f'/checkout/site-packages/{name}/__init__.py',
            f'/checkout/site-packages/{name}/lib.cpython.so',
            f'/checkout/site-packages/{name}/_crs.cpython.so',
        ],
    }


def package_identity(name: str) -> dict[str, object]:
    return {
        'available': True,
        'version': '1',
        'module': file_identity(f'/checkout/site-packages/{name}/__init__.py'),
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
        **file_identity('/checkout/site-packages/gometry/_lib.cpython-314.so'),
        'build_info': {
            'schema_version': 1,
            'source_inputs_format': 'sha256-path-nul-content-nul',
            'source_inputs_sha256': HASH,
            'native_source_inputs_sha256': HASH,
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
        'source_commit': None,
        'source_receipt': None,
        'source_wheel': None,
    }
    facade = packages['gometry']['module']
    distribution = packages['gometry']['distribution']
    runtime_binding = {
        'kind': 'release',
        'path': '/checkout/release-manifest.json',
        'sha256': HASH,
        'profile': 'release-dist',
        'package_root': '/checkout/site-packages',
        'facade': facade,
        'extension': extension,
        'wheel': {
            'path': '/checkout/gometry-1-py3-none-any.whl',
            'filename': 'gometry-1-py3-none-any.whl',
            'archive': file_identity('/checkout/gometry-1-py3-none-any.whl'),
            'extension_member': 'gometry/_lib.cpython-314.so',
            'extension_sha256': HASH,
        },
        'distribution': distribution,
        'python_members': {
            'gometry/__init__.py': HASH,
            'gometry/_lib.pyi': HASH,
        },
    }
    return {
        'identity_schema_version': 3,
        'harness_commit': 'deadbeef',
        'source_inputs_sha256': HASH,
        'extension': extension,
        'uv_lock': {'path': lock_path, 'sha256': HASH, 'size_bytes': 1},
        'python': {
            'executable': '/python',
            'resolved_executable': '/python',
            'prefix': '/venv',
            'version': '3.14 test build',
            'implementation': 'CPython',
            'cache_tag': 'cpython-314',
            'ext_suffix': '.cpython-314.so',
            'gil_disabled': False,
            'gil_enabled': True,
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
        'runtime_binding': runtime_binding,
        'implementation': implementation,
        'arm': {'side': side, 'row': row},
    }
