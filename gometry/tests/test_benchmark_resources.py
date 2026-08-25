"""Resource-probe schema and endpoint-memory semantics."""

from __future__ import annotations

import importlib
import importlib.util
import json
import sys
from pathlib import Path

_BENCHES = Path(__file__).resolve().parents[1] / 'benches'
_DRIVERS = _BENCHES / 'drivers'
if str(_DRIVERS) not in sys.path:
    sys.path.insert(0, str(_DRIVERS))

_bench = importlib.import_module('bench')
_CASE_PATH = _BENCHES / 'cases' / 'case_resource_tail.py'
_case_spec = importlib.util.spec_from_file_location('case_resource_tail', _CASE_PATH)
assert _case_spec is not None and _case_spec.loader is not None
_case = importlib.util.module_from_spec(_case_spec)
_case_spec.loader.exec_module(_case)


def test_resource_summary_consumes_signed_endpoint_deltas(tmp_path: Path) -> None:
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
    }
    output = tmp_path / 'resource.json'
    output.write_text(json.dumps(payload), encoding='utf-8')

    summary = _bench._resource_summaries([
        {
            'kind': 'resource',
            'output': str(output),
        }
    ])

    assert summary == [payload]
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
