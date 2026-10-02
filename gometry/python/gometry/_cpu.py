"""Best-effort detection of the x86-64-v2 instruction baseline."""

from __future__ import annotations

import ctypes
import platform
import sys

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Collection, Iterable

_V2_FEATURES = {
    'CMPXCHG16B': frozenset({'cmpxchg16b', 'cx16'}),
    'LAHF-SAHF': frozenset({'lahf', 'lahf_lm', 'lahf-sahf'}),
    'POPCNT': frozenset({'popcnt'}),
    'SSE3': frozenset({'pni', 'sse3'}),
    'SSE4.1': frozenset({'sse4.1', 'sse4_1'}),
    'SSE4.2': frozenset({'sse4.2', 'sse4_2'}),
    'SSSE3': frozenset({'ssse3'}),
}

_WINDOWS_FEATURE_IDS = {
    'CMPXCHG16B': 14,
    'SSE3': 13,
    'SSSE3': 36,
    'SSE4.1': 37,
    'SSE4.2': 38,
}


def _missing_v2_features(
    flags: Collection[str] | None,
    checked: Collection[str] | None = None,
) -> tuple[str, ...]:
    """Return known-missing v2 features; ``None`` means detection is unknown."""
    if flags is None:
        return ()
    available = {flag.strip().lower() for flag in flags}
    checked_features = _V2_FEATURES if checked is None else checked
    return tuple(
        feature
        for feature, aliases in _V2_FEATURES.items()
        if feature in checked_features and aliases.isdisjoint(available)
    )


def _require_v2(
    flags: Collection[str] | None,
    checked: Collection[str] | None = None,
) -> None:
    missing = _missing_v2_features(flags, checked)
    if missing:
        raise ImportError(
            'This gometry x86_64 wheel requires an x86-64-v2 CPU '
            '(roughly 2009 or newer); missing CPU features: '
            f'{", ".join(missing)}. Run on an x86-64-v2-capable CPU.'
        )


def _parse_linux_cpuinfo(cpuinfo: Iterable[str]) -> frozenset[str] | None:
    for line in cpuinfo:
        key, separator, value = line.partition(':')
        if separator and key.strip().lower() == 'flags':
            return frozenset(value.lower().split())
    return None


def _linux_cpu_features() -> tuple[frozenset[str], None] | None:
    try:
        with open('/proc/cpuinfo', encoding='ascii') as cpuinfo:
            flags = _parse_linux_cpuinfo(cpuinfo)
    except (OSError, UnicodeError):
        return None
    if flags is None:
        return None
    return flags, None


def _macos_cpu_features() -> tuple[frozenset[str], None] | None:
    library = ctypes.CDLL('/usr/lib/libSystem.B.dylib')
    query = library.sysctlbyname
    query.argtypes = (
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_void_p,
        ctypes.c_size_t,
    )
    query.restype = ctypes.c_int
    values = []
    for name in (b'machdep.cpu.features', b'machdep.cpu.extfeatures'):
        size = ctypes.c_size_t()
        if query(name, None, ctypes.byref(size), None, 0) != 0 or not size.value:
            return None
        value = ctypes.create_string_buffer(size.value)
        if query(name, value, ctypes.byref(size), None, 0) != 0:
            return None
        values.append(value.value.decode('ascii').lower().split())
    return frozenset(token for value in values for token in value), None


if sys.platform == 'win32':

    def _windows_cpu_features() -> tuple[frozenset[str], frozenset[str]]:
        kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
        is_processor_feature_present = kernel32.IsProcessorFeaturePresent
        is_processor_feature_present.argtypes = (ctypes.c_uint32,)
        is_processor_feature_present.restype = ctypes.c_int
        available = frozenset(
            feature.lower()
            for feature, feature_id in _WINDOWS_FEATURE_IDS.items()
            if is_processor_feature_present(feature_id)
        )
        # Windows does not expose POPCNT or LAHF-SAHF through this API.
        return available, frozenset(_WINDOWS_FEATURE_IDS)


def ensure_supported_cpu() -> None:
    """Reject a detectable x86_64 CPU that lacks the wheel's v2 baseline."""
    try:
        if platform.machine().lower() not in {'amd64', 'x86_64'}:
            return
        if sys.platform == 'linux':
            evidence = _linux_cpu_features()
        elif sys.platform == 'darwin':
            evidence = _macos_cpu_features()
        elif sys.platform == 'win32':
            evidence = _windows_cpu_features()
        else:
            return
    except Exception:
        return
    if evidence is None:
        return
    flags, checked = evidence
    _require_v2(flags, checked)
