import hashlib
import json
from pathlib import Path

import pyperf
import pytest

from benchmark_cases import case_definitions
from tools.benchmark_report import _admit, report


def _write_results(
    path: Path, implementation: str, *, source_revision: str = 'a' * 40
) -> None:
    version = {'pyotp': '2.10.0', 'totp-rs': '1.1.0'}[implementation]
    definitions = case_definitions(implementation)
    common = {
        'benchmark_sha256': hashlib.sha256(
            (Path(__file__).resolve().parents[1] / 'benchmark.py').read_bytes()
        ).hexdigest(),
        'case_definitions': json.dumps(
            definitions, separators=(',', ':'), sort_keys=True
        ),
        'cpu_model_name': 'test CPU',
        'implementation': f'{implementation} {version}',
        'implementation_name': implementation,
        'implementation_version': version,
        'native_extension_sha256': 'extension-sha',
        'native_package_version': '1.1.0',
        'platform': 'Linux-test',
        'pyotp_version': '2.10.0',
        'pyperf_version': '2.10.0',
        'python_cflags': 'test flags',
        'python_compiler': 'test compiler',
        'python_config_args': 'test config',
        'python_implementation': 'CPython',
        'python_version': '3.15.0',
        'report_sha256': 'report-sha',
        'source_revision': source_revision,
        'comparison_workload': 'test workload',
    }
    benchmarks = []
    for case_index, case in enumerate(definitions):
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


def test_report_separates_matched_and_native_only_workloads(tmp_path: Path) -> None:
    pyotp_path = tmp_path / 'pyotp.json'
    native_path = tmp_path / 'totp-rs.json'
    _write_results(pyotp_path, 'pyotp')
    _write_results(native_path, 'totp-rs')

    report(str(pyotp_path), str(native_path), str(tmp_path / 'results.svg'))

    markdown = (tmp_path / 'results.md').read_text()
    assert '## Matched PyOTP comparison' in markdown
    assert '## Native-only input forms' in markdown
    assert 'not included in the speed comparison' in markdown
    for case in case_definitions('totp-rs'):
        assert case['label'] in markdown
    assert (tmp_path / 'results.svg').is_file()
    assert (tmp_path / 'results-dark.svg').is_file()


def test_report_rejects_different_source_revisions(tmp_path: Path) -> None:
    pyotp_path = tmp_path / 'pyotp.json'
    native_path = tmp_path / 'totp-rs.json'
    _write_results(pyotp_path, 'pyotp')
    _write_results(native_path, 'totp-rs', source_revision='b' * 40)

    with pytest.raises(ValueError, match='incompatible source_revision'):
        _admit(str(pyotp_path), str(native_path))
