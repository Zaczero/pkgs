"""Publication transaction rejection, atomicity, and deterministic output."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_BENCHES = _ROOT / 'benches'
for _path in (_BENCHES / 'support', _BENCHES / 'drivers'):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import bench as bench_driver  # noqa: E402
from _bench_registry import RELEASE_OPERATIONS  # noqa: E402


def _result(tmp_path: Path) -> dict[str, object]:
    artifact = tmp_path / 'pair.json'
    benchmarks = []
    for operation in RELEASE_OPERATIONS:
        for name, value in ((operation.gometry, 1.0), (operation.competitor, 2.0)):
            benchmarks.append({
                'metadata': {'name': name, 'gometry_pair_id': 'pair-1', 'gometry_pair_lead': 'ab'},
                'runs': [{'values': [value, value]}],
            })
    artifact.write_text(json.dumps({'benchmarks': benchmarks}), encoding='utf-8')
    metadata = {
        'git_commit': 'deadbeef',
        'source_inputs_sha256': 'source-digest',
        'python_executable': '/python',
        'python': '3.14',
        'python_version': '3.14 test build',
        'python_build': ['build', 'date'],
        'python_implementation': 'CPython',
        'python_debug': False,
        'python_cache_tag': 'cpython-314',
        'extension': {
            'path': '/checkout/target/release/gometry.so',
            'sha256': 'extension-digest',
            'size_bytes': 10,
            'debug_build': False,
            'release_build': True,
            'tied_to_checkout': True,
        },
        'uv_lock': {'path': '/checkout/uv.lock', 'sha256': 'lock-digest', 'size_bytes': 10},
        'packages': {
            package: {'version': '1'}
            for package in (
                'gometry', 'numpy', 'pyperf', 'shapely', 'pyproj', 'h3',
                'geopandas', 'pyarrow', 'mercantile', 'pygeohash',
            )
        },
        'cpu_model': 'test',
        'cpu_governor': 'performance',
        'cpu_affinity': [1],
        'isolated_cpus': '1',
        'selected_cpu': 1,
        'selected_cpu_source': 'explicit-kernel-isolated',
        'cpu_frequency_state': {},
    }
    operations = [
        {
            'domain': operation.domain,
            'label': operation.label,
            'workload': operation.workload,
            'suite': operation.suite,
            'gometry': operation.gometry,
            'competitor': operation.competitor,
            'competitor_label': operation.competitor_label,
            'footnotes': list(operation.footnotes),
        }
        for operation in RELEASE_OPERATIONS
    ]
    return {
        'profile': 'release',
        'plan_only': False,
        'full_manifest': True,
        'filter': None,
        'oracle_ok': True,
        'commands_complete': True,
        'publishable': True,
        'row_counts': {'competitors': 74},
        'public_operations': operations,
        'pyperf_artifacts': [str(artifact)],
        'doctor': {'metadata': metadata, 'hard_failures': [], 'warnings': ['load advisory']},
    }


def test_publication_rejects_filtered_run_without_touching_current(tmp_path: Path) -> None:
    result = _result(tmp_path)
    result['filter'] = 'gometry.points/10k_numpy_xy'
    current = tmp_path / 'current'
    current.mkdir()
    sentinel = current / 'sentinel'
    sentinel.write_text('keep', encoding='utf-8')
    with pytest.raises(SystemExit, match='filtered'):
        bench_driver.publish_current_run(result, output_dir=tmp_path / 'stage', current_dir=current)
    assert sentinel.read_text(encoding='utf-8') == 'keep'


def test_publication_rejects_missing_artifact_and_metadata(tmp_path: Path) -> None:
    result = _result(tmp_path)
    result['pyperf_artifacts'] = [str(tmp_path / 'missing.json')]
    with pytest.raises(SystemExit, match='missing artifact'):
        bench_driver.publish_current_run(result, output_dir=tmp_path / 'stage', current_dir=tmp_path / 'current')
    incomplete = _result(tmp_path)
    del incomplete['doctor']['metadata']['uv_lock']  # type: ignore[index]
    with pytest.raises(SystemExit, match='missing benchmark identity metadata'):
        bench_driver.publish_current_run(incomplete, output_dir=tmp_path / 'stage2', current_dir=tmp_path / 'current')


def test_publication_is_complete_and_deterministic(tmp_path: Path) -> None:
    result = _result(tmp_path)
    current = tmp_path / 'current'
    bench_driver.publish_current_run(result, output_dir=tmp_path / 'stage', current_dir=current)
    first = {
        path.relative_to(current).as_posix(): path.read_bytes()
        for path in current.rglob('*')
        if path.is_file()
    }
    assert set(first) == {'identity.json', 'summary.json', 'summary.md', 'raw/pair.json'}
    summary = json.loads((current / 'summary.json').read_text(encoding='utf-8'))
    assert len(summary['operations']) == 37
    assert all(operation['blocks'] for operation in summary['operations'])
    bench_driver.publish_current_run(
        result, output_dir=tmp_path / 'stage-again', current_dir=current
    )
    second = {
        path.relative_to(current).as_posix(): path.read_bytes()
        for path in current.rglob('*')
        if path.is_file()
    }
    assert second == first
