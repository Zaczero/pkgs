"""Release identity evidence tests for the benchmark doctor."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_DRIVERS = Path(__file__).resolve().parents[1] / 'benches' / 'drivers'
if str(_DRIVERS) not in sys.path:
    sys.path.insert(0, str(_DRIVERS))

import bench_doctor as doctor  # noqa: E402


def test_debug_assertion_marker_presence_is_the_profile_evidence() -> None:
    marker = doctor.DEBUG_ASSERTION_MARKER
    assert doctor._debug_assertion_marker_count(b'release bytes') == 0
    assert doctor._debug_assertion_marker_count(b'prefix' + marker + b'suffix') == 1
    assert doctor._debug_assertion_marker_count(marker + marker) == 2


def test_extension_profile_rejects_marker_mutation() -> None:
    release = doctor._extension_profile_evidence(b'optimized extension')
    debug = doctor._extension_profile_evidence(
        b'optimized extension' + doctor.DEBUG_ASSERTION_MARKER
    )
    assert release['debug_build'] is False
    assert release['release_build'] is True
    assert release['debug_assertion_marker_count'] == 0
    assert release['profile_evidence_method'] == 'extension_bytes_marker_scan'
    assert debug['debug_build'] is True
    assert debug['release_build'] is False
    assert debug['debug_assertion_marker_count'] == 1


def test_selected_cpu_governor_is_distinct_from_other_cpu_state() -> None:
    state = {
        '0': {'governor': 'powersave'},
        '7': {'governor': 'performance'},
    }
    assert doctor.selected_cpu_governor(state, 7) == 'performance'
    assert doctor.selected_cpu_governor(state, 0) == 'powersave'
    with pytest.raises(KeyError):
        doctor.selected_cpu_governor(state, 3)
