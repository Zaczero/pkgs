from __future__ import annotations

import builtins
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

_CPU_CHECK_PATH = Path(__file__).parents[1] / 'python' / 'gometry' / '_cpu.py'
_CPU_CHECK_SPEC = importlib.util.spec_from_file_location(
    'gometry_cpu_check_test', _CPU_CHECK_PATH
)
assert _CPU_CHECK_SPEC is not None and _CPU_CHECK_SPEC.loader is not None
_CPU_CHECK = importlib.util.module_from_spec(_CPU_CHECK_SPEC)
_CPU_CHECK_SPEC.loader.exec_module(_CPU_CHECK)


def _windows_cpu_check():
    if hasattr(_CPU_CHECK, '_windows_cpu_features'):
        return _CPU_CHECK

    original_platform = _CPU_CHECK.sys.platform
    _CPU_CHECK.sys.platform = 'win32'
    try:
        check = importlib.util.module_from_spec(_CPU_CHECK_SPEC)
        _CPU_CHECK_SPEC.loader.exec_module(check)
    finally:
        _CPU_CHECK.sys.platform = original_platform
    return check


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


def test_unavailable_cpu_feature_probes_are_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    windows = _windows_cpu_check()

    def unavailable_library(*_args, **_kwargs):
        raise OSError('CPU feature library is unavailable')

    monkeypatch.setattr(builtins, 'open', unavailable_library)
    assert _CPU_CHECK._linux_cpu_features() is None

    monkeypatch.setattr(_CPU_CHECK.ctypes, 'CDLL', unavailable_library)
    assert _CPU_CHECK._macos_cpu_features() is None

    monkeypatch.setattr(_CPU_CHECK.ctypes, 'CDLL', lambda *_args, **_kwargs: object())
    assert _CPU_CHECK._macos_cpu_features() is None

    monkeypatch.setattr(windows.ctypes, 'WinDLL', unavailable_library, raising=False)
    assert windows._windows_cpu_features() is None

    monkeypatch.setattr(
        windows.ctypes,
        'WinDLL',
        lambda *_args, **_kwargs: object(),
        raising=False,
    )
    assert windows._windows_cpu_features() is None


def test_macos_cpu_probe_treats_invalid_sysctl_text_as_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def sysctlbyname(_name, value, size, _new_value, _new_size):
        size._obj.value = 1
        if value is not None:
            _CPU_CHECK.ctypes.memmove(value, b'\xff', 1)
        return 0

    monkeypatch.setattr(
        _CPU_CHECK.ctypes,
        'CDLL',
        lambda *_args, **_kwargs: SimpleNamespace(sysctlbyname=sysctlbyname),
    )

    assert _CPU_CHECK._macos_cpu_features() is None


def test_ensure_supported_cpu_surfaces_a_broken_feature_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_CPU_CHECK.platform, 'machine', lambda: 'x86_64')
    monkeypatch.setattr(_CPU_CHECK.sys, 'platform', 'linux')

    def broken_probe():
        raise RuntimeError('broken CPU feature probe')

    monkeypatch.setattr(_CPU_CHECK, '_linux_cpu_features', broken_probe)
    with pytest.raises(RuntimeError, match='broken CPU feature probe'):
        _CPU_CHECK.ensure_supported_cpu()
