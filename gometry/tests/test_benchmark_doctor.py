"""Build identity evidence tests for the benchmark doctor."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

_DRIVERS = Path(__file__).resolve().parents[1] / 'benches' / 'drivers'
if str(_DRIVERS) not in sys.path:
    sys.path.insert(0, str(_DRIVERS))

import bench_doctor as doctor  # noqa: E402


def _release_build_info(source_inputs_sha256: str) -> dict[str, object]:
    return {
        'schema_version': doctor.BUILD_INFO_SCHEMA_VERSION,
        'source_inputs_format': doctor.SOURCE_INPUTS_FORMAT,
        'source_inputs_sha256': source_inputs_sha256,
        'profile_family': 'release',
        'opt_level': '3',
        'debug': False,
        'debug_assertions': False,
        'target': doctor._rustc_host_target(),
        'target_features': 'sse2',
    }


def test_source_identity_and_effective_release_facts_pass() -> None:
    source_inputs_sha256 = doctor._source_inputs_digest()
    assert source_inputs_sha256
    assert doctor._build_info_failures(
        _release_build_info(source_inputs_sha256),
        source_inputs_sha256,
        doctor._rustc_host_target(),
    ) == []


def test_source_identity_mutation_rejects_without_rebuild() -> None:
    source_inputs_sha256 = doctor._source_inputs_digest()
    assert source_inputs_sha256
    failures = doctor._build_info_failures(
        _release_build_info(source_inputs_sha256),
        hashlib.sha256(b'one changed tracked build input').hexdigest(),
        doctor._rustc_host_target(),
    )
    assert any('stale' in failure for failure in failures)


@pytest.mark.parametrize(
    ('field', 'value', 'message'),
    [
        ('profile_family', 'debug', 'release-family'),
        ('opt_level', '0', 'optimization level'),
        ('debug', 'true', 'debug-info flag'),
        ('debug_assertions', True, 'debug assertions'),
        ('target', 'wrong-target', 'targets'),
    ],
)
def test_effective_build_fact_mutation_rejects(
    field: str, value: object, message: str
) -> None:
    source_inputs_sha256 = doctor._source_inputs_digest()
    assert source_inputs_sha256
    build_info = _release_build_info(source_inputs_sha256)
    build_info[field] = value
    failures = doctor._build_info_failures(
        build_info,
        source_inputs_sha256,
        doctor._rustc_host_target(),
    )
    assert any(message in failure for failure in failures)


def test_private_runtime_build_info_matches_current_source() -> None:
    import gometry._lib as lib

    build_info = lib.__build_info__
    assert isinstance(build_info, dict)
    assert '__build_info__' not in lib.__all__
    assert build_info['source_inputs_sha256'] == doctor._source_inputs_digest()
    extension = doctor._extension_identity()
    path = Path(extension['path'])
    assert extension['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert 'tied_to_checkout' not in extension


def test_selected_cpu_governor_is_distinct_from_other_cpu_state() -> None:
    state = {
        '0': {'governor': 'powersave'},
        '7': {'governor': 'performance'},
    }
    assert doctor.selected_cpu_governor(state, 7) == 'performance'
    assert doctor.selected_cpu_governor(state, 0) == 'powersave'
    with pytest.raises(KeyError):
        doctor.selected_cpu_governor(state, 3)
