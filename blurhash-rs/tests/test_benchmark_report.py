import hashlib
import json
from pathlib import Path

import pyperf
import pytest

from benchmark import CASE_DEFINITIONS
from tools.benchmark_report import _admit, report


def _write_results(
    path: Path,
    implementation: str,
    *,
    pillow_version: str = '13.0.0',
) -> None:
    version = {
        'blurhash-rs': '1.1.0',
        'blurhash': '1.1.5',
        'blurhash-python': '1.2.2',
    }[implementation]
    common = {
        'benchmark_sha256': hashlib.sha256(
            (Path(__file__).resolve().parents[1] / 'benchmark.py').read_bytes()
        ).hexdigest(),
        'case_definitions': json.dumps(
            CASE_DEFINITIONS, separators=(',', ':'), sort_keys=True
        ),
        'cpu_model_name': 'test CPU',
        'implementation': f'{implementation} {version}',
        'implementation_name': implementation,
        'implementation_version': version,
        'native_extension_sha256': 'extension-sha',
        'native_package_version': '1.1.0',
        'numpy_version': '2.5.4',
        'pillow_version': pillow_version,
        'platform': 'Linux-test',
        'python_cflags': 'test flags',
        'python_compiler': 'test compiler',
        'python_config_args': 'test config',
        'pyperf_version': '2.10.0',
        'python_implementation': 'CPython',
        'python_version': '3.15.0',
        'report_sha256': 'report-sha',
        'source_revision': 'a' * 40,
        'workload': 'test workload',
        'implementation_dependencies': json.dumps({
            'Pillow': pillow_version,
            'NumPy': '2.5.4',
            'pyperf': '2.10.0',
            **(
                {'cffi': '2.1.1', 'six': '1.17.0'}
                if implementation == 'blurhash-python'
                else {}
            ),
        }),
    }
    benchmarks = []
    for case_index, case in enumerate(CASE_DEFINITIONS):
        runs = [
            pyperf.Run(
                [1e-6 * (case_index + run_index + 1)],
                metadata={
                    **common,
                    'name': case['name'],
                    'unit': 'second',
                },
                collect_metadata=False,
            )
            for run_index in range(12)
        ]
        benchmarks.append(pyperf.Benchmark(runs))
    pyperf.BenchmarkSuite(benchmarks).dump(str(path))


def test_report_keeps_every_case_in_tables_and_charts(tmp_path: Path) -> None:
    paths = []
    for implementation in ('blurhash-rs', 'blurhash', 'blurhash-python'):
        path = tmp_path / f'{implementation}.json'
        _write_results(path, implementation)
        paths.append(str(path))

    report(paths, str(tmp_path / 'report'))

    markdown = (tmp_path / 'report/results.md').read_text()
    assert 'Median time' in markdown
    assert '95% interval' in markdown
    assert '12 independent worker-process means' in markdown
    for case in CASE_DEFINITIONS:
        assert case['label'] in markdown
    for operation in ('encode', 'decode'):
        for group in ('', '-workloads'):
            for theme in ('', '-dark'):
                assert (
                    tmp_path / 'report' / f'{operation}{group}{theme}.svg'
                ).is_file()


def test_report_rejects_mismatched_pillow_versions(tmp_path: Path) -> None:
    paths = []
    for implementation, pillow_version in (
        ('blurhash-rs', '13.0.0'),
        ('blurhash', '12.0.0'),
        ('blurhash-python', '13.0.0'),
    ):
        path = tmp_path / f'{implementation}.json'
        _write_results(path, implementation, pillow_version=pillow_version)
        paths.append(str(path))

    with pytest.raises(ValueError, match='incompatible pillow_version'):
        _admit(paths)
