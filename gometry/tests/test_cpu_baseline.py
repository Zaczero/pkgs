from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_CPU_CHECK_PATH = Path(__file__).parents[1] / 'python' / 'gometry' / '_cpu.py'
_CPU_CHECK_SPEC = importlib.util.spec_from_file_location(
    'gometry_cpu_check_test', _CPU_CHECK_PATH
)
assert _CPU_CHECK_SPEC is not None and _CPU_CHECK_SPEC.loader is not None
_CPU_CHECK = importlib.util.module_from_spec(_CPU_CHECK_SPEC)
_CPU_CHECK_SPEC.loader.exec_module(_CPU_CHECK)


def test_x86_64_v2_feature_set_passes() -> None:
    flags = {
        'cx16',
        'lahf_lm',
        'popcnt',
        'pni',
        'sse4_1',
        'sse4_2',
        'ssse3',
    }
    _CPU_CHECK._require_v2(flags)


def test_x86_64_v2_missing_feature_names_are_actionable() -> None:
    flags = {'cx16', 'lahf_lm', 'popcnt', 'pni', 'sse4_1', 'ssse3'}
    with pytest.raises(ImportError, match='requires an x86-64-v2 CPU') as error:
        _CPU_CHECK._require_v2(flags)
    assert 'SSE4.2' in str(error.value)
    assert 'roughly 2009 or newer' in str(error.value)


def test_unknown_cpu_features_do_not_block() -> None:
    _CPU_CHECK._require_v2(None)


def test_partial_windows_feature_evidence_checks_only_exposed_features() -> None:
    assert (
        _CPU_CHECK._missing_v2_features(
            {'cmpxchg16b', 'sse3', 'ssse3', 'sse4.1', 'sse4.2'},
            {'CMPXCHG16B', 'SSE3', 'SSSE3', 'SSE4.1', 'SSE4.2'},
        )
        == ()
    )
    assert _CPU_CHECK._missing_v2_features(
        {'sse3', 'ssse3', 'sse4.1', 'sse4.2'},
        {'CMPXCHG16B', 'SSE3', 'SSSE3', 'SSE4.1', 'SSE4.2'},
    ) == ('CMPXCHG16B',)


def test_linux_cpuinfo_uses_first_processor_flags() -> None:
    def cpuinfo_lines():
        yield 'processor : 0\n'
        yield 'flags : cx16 lahf_lm popcnt pni sse4_1 sse4_2 ssse3\n'
        raise AssertionError('read past the first processor flags')

    assert _CPU_CHECK._parse_linux_cpuinfo(cpuinfo_lines()) == frozenset({
        'cx16',
        'lahf_lm',
        'popcnt',
        'pni',
        'sse4_1',
        'sse4_2',
        'ssse3',
    })
    assert _CPU_CHECK._parse_linux_cpuinfo(iter(('processor : 0\n',))) is None
