"""Resource-probe schema and endpoint-memory semantics."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import sys
from pathlib import Path

_BENCHES = Path(__file__).resolve().parents[2] / 'benches'
_DRIVERS = _BENCHES / 'drivers'
_SUPPORT = _BENCHES / 'support'
for _path in (_SUPPORT, _DRIVERS):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import benchmark_runtime  # ruff: ignore[module-import-not-at-top-of-file]

from tests.tooling._bench_support import (  # ruff: ignore[module-import-not-at-top-of-file]
    HASH,
    producer,
)

_bench = importlib.import_module('bench')
_CASE_PATH = _BENCHES / 'cases' / 'case_resource_tail.py'
_case_spec = importlib.util.spec_from_file_location('case_resource_tail', _CASE_PATH)
assert _case_spec is not None and _case_spec.loader is not None
_case = importlib.util.module_from_spec(_case_spec)
_case_spec.loader.exec_module(_case)


def test_release_resource_plan_writes_to_run_manifest(tmp_path: Path) -> None:
    plans = _bench._resource_plans(
        output_dir=tmp_path,
        timestamp='measurement',
        cpu=0,
        receipt_binding={'path': str(tmp_path / 'wheel.whl'), 'receipt_sha256': HASH},
        package_root=tmp_path,
    )
    output = tmp_path / 'run.json'
    _bench._write_json_once(output, {'planned_commands': plans}, label='run manifest')
    recorded = json.loads(output.read_text())
    assert recorded['planned_commands'][0]['case_path'] == str(_CASE_PATH)


def test_resource_summary_consumes_signed_endpoint_deltas(tmp_path: Path) -> None:
    row = str(_CASE_PATH.resolve())
    identity = producer(row, 'A')
    runtime_binding = identity['runtime_binding']
    subject = {
        'kind': 'release',
        'path': runtime_binding['path'],
        'receipt_sha256': runtime_binding['sha256'],
        'source': {
            'commit': None,
            'version': '1',
            'source_inputs_sha256': identity['source_inputs_sha256'],
            'native_source_inputs_sha256': HASH,
            'python_members': runtime_binding['python_members'],
        },
        'wheels': [
            {
                'filename': runtime_binding['wheel']['filename'],
                'sha256': runtime_binding['wheel']['archive']['sha256'],
                'extension_member': runtime_binding['wheel']['extension_member'],
                'extension_sha256': runtime_binding['wheel']['extension_sha256'],
            }
        ],
    }
    case_process = {
        'pid': 1234,
        'invocation_id': 'resource-test-invocation',
        'case': {
            'path': row,
            'sha256': hashlib.sha256(_CASE_PATH.read_bytes()).hexdigest(),
        },
        'identity_sha256': benchmark_runtime.identity_sha256(identity),
    }
    payload = {
        'mode': 'test',
        'median_seconds': 1.0,
        'p99_seconds': 2.0,
        'p999_seconds': 3.0,
        'rss_current_isolated_endpoint_delta_kib': -128,
        'pss_current_isolated_endpoint_delta_kib': -96,
        'rss_current_latency_endpoint_delta_kib': 12,
        'pss_current_latency_endpoint_delta_kib': -4,
        'python_peak_traced_bytes': 64,
        'python_allocated_blocks_delta': 0,
        'producer_identity': identity,
        'case_process': case_process,
    }
    output = tmp_path / 'resource.json'
    output.write_text(json.dumps(payload), encoding='utf-8')

    summary = _bench._resource_summaries(
        [
            {
                'kind': 'resource',
                'output': str(output),
                'case_path': _CASE_PATH,
            }
        ],
        subject=subject,
    )

    assert summary == [
        {
            key: payload[key]
            for key in (
                'mode',
                'median_seconds',
                'p99_seconds',
                'p999_seconds',
                'rss_current_isolated_endpoint_delta_kib',
                'pss_current_isolated_endpoint_delta_kib',
                'rss_current_latency_endpoint_delta_kib',
                'pss_current_latency_endpoint_delta_kib',
                'python_peak_traced_bytes',
                'python_allocated_blocks_delta',
                'producer_identity',
                'case_process',
            )
        }
    ]
    assert 'rss_peak_kib' not in summary[0]
    assert 'rss_current_after_setup_kib' not in summary[0]


def test_memory_probe_names_and_signed_deltas_are_endpoint_only() -> None:
    rss = _case.current_rss_kib()
    pss = _case.current_pss_kib()
    assert rss is None or rss > 0
    assert pss is None or pss > 0
    assert _case.memory_delta_kib(12, 5) == 7
    assert _case.memory_delta_kib(5, 12) == -7
    assert _case.memory_delta_kib(None, 5) is None
    assert _case.memory_delta_kib(5, None) is None
