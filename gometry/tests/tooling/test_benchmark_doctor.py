"""Build identity evidence tests for the benchmark doctor."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import sys
from pathlib import Path

import pytest

from tests._support import GOMETRY_ROOT

_DRIVERS = Path(__file__).resolve().parents[2] / 'benches' / 'drivers'
if str(_DRIVERS) not in sys.path:
    sys.path.insert(0, str(_DRIVERS))
_TOOLS = GOMETRY_ROOT / 'tools'
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

import _artifact_common as artifact_common  # noqa: E402
import artifact_content  # noqa: E402
import bench_doctor as doctor  # noqa: E402
import release_artifact  # noqa: E402
import source_artifact  # noqa: E402


def _release_build_info(source_inputs_sha256: str) -> dict[str, object]:
    native_source_inputs_sha256 = doctor._native_source_inputs_digest()
    assert native_source_inputs_sha256
    return {
        'schema_version': doctor.BUILD_INFO_SCHEMA_VERSION,
        'source_inputs_format': doctor.SOURCE_INPUTS_FORMAT,
        'source_inputs_sha256': source_inputs_sha256,
        'native_source_inputs_format': doctor.SOURCE_INPUTS_FORMAT,
        'native_source_inputs_sha256': native_source_inputs_sha256,
        'profile_family': 'release',
        'opt_level': '3',
        'debug_assertions': False,
        'target': doctor._rustc_host_target(),
        'target_arch': 'x86_64',
        'target_features': 'sse2',
    }


def test_source_identity_and_effective_release_facts_pass() -> None:
    source_inputs_sha256 = doctor._source_inputs_digest()
    assert source_inputs_sha256
    assert (
        doctor._build_info_failures(
            _release_build_info(source_inputs_sha256),
            source_inputs_sha256,
            doctor._rustc_host_target(),
            doctor._native_source_inputs_digest(),
        )
        == []
    )


def test_source_identity_mutation_rejects_without_rebuild() -> None:
    source_inputs_sha256 = doctor._source_inputs_digest()
    assert source_inputs_sha256
    failures = doctor._build_info_failures(
        _release_build_info(source_inputs_sha256),
        hashlib.sha256(b'one changed tracked build input').hexdigest(),
        doctor._rustc_host_target(),
    )
    assert any('stale' in failure for failure in failures)


def test_parent_benchmark_identity_override_must_match_embedded_build() -> None:
    checkout = 'a' * 64
    parent = 'b' * 64
    build_info = {'source_inputs_sha256': parent}

    with pytest.raises(ValueError, match='does not match'):
        doctor.benchmark_source_inputs_identity(build_info, checkout)
    assert (
        doctor.benchmark_source_inputs_identity(build_info, checkout, parent) == parent
    )

    with pytest.raises(ValueError, match='does not match'):
        doctor.benchmark_source_inputs_identity(build_info, checkout, 'c' * 64)
    with pytest.raises(ValueError, match='lowercase SHA-256'):
        doctor.benchmark_source_inputs_identity(build_info, checkout, 'B' * 64)


@pytest.mark.parametrize(
    ('field', 'value', 'message'),
    [
        ('profile_family', 'debug', 'release-family'),
        ('opt_level', '0', 'optimization level'),
        ('debug', True, 'unexpected fields'),
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
    assert not {
        'source_commit',
        'debug',
        'release_target_policy',
        'release_rustflags',
        'release_cflags',
        'release_cxxflags',
        'gil_used',
    }.intersection(build_info)
    assert build_info['source_inputs_sha256'] == doctor._source_inputs_digest()
    assert (
        build_info['native_source_inputs_sha256']
        == doctor._native_source_inputs_digest()
    )
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


def test_topology_fixture_preserves_smt_and_numa_boundaries(tmp_path: Path) -> None:
    cpu_root = tmp_path / 'cpu'
    for cpu, socket, core, siblings in (
        ('0', '0', '0', '0,2'),
        ('2', '0', '0', '0,2'),
        ('1', '1', '1', '1'),
    ):
        topology = cpu_root / f'cpu{cpu}' / 'topology'
        topology.mkdir(parents=True)
        (topology / 'physical_package_id').write_text(socket)
        (topology / 'core_id').write_text(core)
        (topology / 'thread_siblings_list').write_text(siblings)
        (topology / 'core_cpus_list').write_text(siblings)
    parsed = doctor.parse_cpu_topology(cpu_root)
    assert parsed['cpus']['0']['thread_siblings'] == [0, 2]
    assert parsed['cpus']['2']['core'] == '0'
    assert parsed['cpus']['1']['socket'] == '1'

    node_root = tmp_path / 'node'
    for node, cpus in (('0', '0-1'), ('1', '2')):
        path = node_root / f'node{node}'
        path.mkdir(parents=True)
        (path / 'cpulist').write_text(cpus)
        (path / 'meminfo').write_text(
            'Node MemTotal:       1024 kB\nNode MemFree:         512 kB\n'
        )
    numa = doctor.parse_numa_topology(node_root)
    assert numa['nodes']['0']['cpus'] == [0, 1]
    assert numa['nodes']['1']['cpus'] == [2]


def test_cgroup_and_memory_placement_fixture_are_explicit(tmp_path: Path) -> None:
    cgroup = tmp_path / 'cg'
    cgroup.mkdir()
    (cgroup / 'memory.max').write_text('1048576')
    (cgroup / 'cpu.max').write_text('100000 100000')
    proc_cgroup = tmp_path / 'cgroup'
    proc_cgroup.write_text('0::/bench\n')
    parsed = doctor.parse_cgroup_v2(proc_cgroup, tmp_path)
    assert parsed['available'] is False
    mount = tmp_path / 'mount' / 'bench'
    mount.mkdir(parents=True)
    (mount / 'memory.max').write_text('1048576')
    (mount / 'cpu.max').write_text('100000 100000')
    parsed = doctor.parse_cgroup_v2(proc_cgroup, tmp_path / 'mount')
    assert parsed['available'] is True
    assert parsed['values']['memory.max'] == '1048576'

    status = tmp_path / 'status'
    status.write_text('Mems_allowed_list:\t0-1\n')
    maps = tmp_path / 'numa_maps'
    maps.write_text('7f000 default file=/x N0=4 N1=2\n')
    placement = doctor.parse_memory_placement(status, maps)
    assert placement['allowed_memory_nodes'] == '0-1'
    assert placement['observed_pages_by_node'] == {'0': 4, '1': 2}


def test_native_library_ownership_uses_record_and_engine_associations() -> None:
    def file_identity(path: str) -> dict[str, object]:
        return {'path': path, 'sha256': '0' * 64, 'size_bytes': 1}

    def package(name: str, module: str, extension: str) -> dict[str, object]:
        return {
            'module': file_identity(module),
            'distribution': {
                'name': name,
                'version': '1',
                'location': '/site-packages',
                'metadata': file_identity(f'/site-packages/{name}.dist-info/METADATA'),
                'record': file_identity(f'/site-packages/{name}.dist-info/RECORD'),
                'record_available': True,
                '_record_files': [extension],
            },
        }

    shapely_extension = '/site-packages/shapely/lib.cpython-313-x86_64-linux-gnu.so'
    pyproj_extension = '/site-packages/pyproj/_crs.cpython-313-x86_64-linux-gnu.so'
    loaded = [
        file_identity(shapely_extension),
        file_identity('/nix/store/libgeos_c.so.1'),
        file_identity(pyproj_extension),
        file_identity('/nix/store/libproj.so.25'),
        file_identity('/nix/store/libpython3.13.so'),
    ]
    shapely = doctor.implementation_identity(
        'shapely.area/1',
        file_identity('/site-packages/gometry/_lib.so'),
        package_info=package(
            'shapely', '/site-packages/shapely/__init__.py', shapely_extension
        ),
        loaded_native_libraries=loaded,
    )
    pyproj = doctor.implementation_identity(
        'pyproj.transform/1',
        file_identity('/site-packages/gometry/_lib.so'),
        package_info=package(
            'pyproj', '/site-packages/pyproj/__init__.py', pyproj_extension
        ),
        loaded_native_libraries=loaded,
    )
    assert {item['path'] for item in shapely['native_libraries']} == {
        shapely_extension,
        '/nix/store/libgeos_c.so.1',
    }
    assert {item['path'] for item in pyproj['native_libraries']} == {
        pyproj_extension,
        '/nix/store/libproj.so.25',
    }
    assert '/nix/store/libpython3.13.so' not in {
        item['path']
        for item in shapely['native_libraries'] + pyproj['native_libraries']
    }


def test_installed_native_identity_keeps_real_engine_ownership_bounded() -> None:
    loaded: list[dict[str, object]] = []
    for module_name in ('shapely.lib', 'pyproj._crs'):
        try:
            available = importlib.util.find_spec(module_name) is not None
        except ModuleNotFoundError:
            available = False
        if available:
            try:
                importlib.import_module(module_name)
            except ImportError:
                pass
    loaded = doctor._loaded_native_libraries()
    extension = {
        'path': '/checkout/gometry/_lib.so',
        'sha256': '0' * 64,
        'size_bytes': 1,
    }
    for package_name, engine_token in (('shapely', 'geos'), ('pyproj', 'proj')):
        package = doctor._package_identity(package_name)
        if package is None:
            continue
        identity = doctor.implementation_identity(
            f'{package_name}.installed/1',
            extension,
            package_info=package,
            loaded_native_libraries=loaded,
        )
        engine_paths = [
            item['path'] for item in loaded if engine_token in str(item['path']).lower()
        ]
        if engine_paths:
            owned_paths = {item['path'] for item in identity['native_libraries']}
            assert set(engine_paths) <= owned_paths
            assert identity['native_library_status'] == 'identified'


def test_source_identity_ignores_packaging_metadata_but_detects_code_changes(
    tmp_path: Path,
) -> None:
    package = tmp_path / 'gometry'
    (package / 'src').mkdir(parents=True)
    (package / 'native' / 'visible').mkdir(parents=True)
    (package / 'Cargo.toml').write_text('[package]\nname = "gometry"\n')
    (package / 'build.rs').write_text('fn main() {}\n')
    (package / 'src' / 'lib.rs').write_text('')
    (package / 'native' / 'visible' / 'input.c').write_text('')
    for relative in (
        'native/.git',
        'native/libtiff/.clang-format',
        'native/libtiff/doc/_static/.gitignore',
    ):
        path = package / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('metadata\n')

    files, native = artifact_content._source_input_records(package)
    assert any(name == 'gometry/native/visible/input.c' for name, _ in native)
    assert not any(artifact_content.is_hidden_native_path(name) for name, _ in files)
    before = artifact_content.source_input_digests(package)
    (package / 'native' / '.git').write_text('changed metadata\n')
    (package / 'Cargo.toml').write_text(
        '[package]\nname = "gometry"\nreadme = "README.md"\n'
    )
    (package / 'native' / 'visible' / 'Cargo.toml').write_text(
        '[package]\nname = "native"\nreadme = "README.md"\n'
    )
    after = artifact_content.source_input_digests(package)
    assert before == after
    (package / 'native' / 'visible' / 'input.c').write_text('int changed = 1;\n')
    changed = artifact_content.source_input_digests(package)
    assert changed['source_inputs_sha256'] != before['source_inputs_sha256']
    assert (
        changed['native_source_inputs_sha256'] != before['native_source_inputs_sha256']
    )

    bootstrap = tmp_path / 'bootstrap'
    final = tmp_path / 'final'
    bootstrap_native = bootstrap / 'gometry' / 'native'
    final_native = final / 'gometry' / 'native'
    bootstrap_native.mkdir(parents=True)
    final_native.mkdir(parents=True)
    (bootstrap_native / '.git').write_text('metadata\n')
    (bootstrap_native / 'input.c').write_text('input\n')
    (final_native / 'input.c').write_text('input\n')
    source_artifact._compare_restage_trees(bootstrap, final)

    (final_native / '.git').write_text('admitted metadata\n')
    with pytest.raises(AssertionError, match='hidden native files'):
        source_artifact._compare_restage_trees(bootstrap, final)


@pytest.mark.parametrize(
    ('target', 'policy', 'rustflags', 'cflags'),
    [
        (
            'x86_64-unknown-linux-gnu',
            'x86-64-v2',
            '-C target-cpu=x86-64-v2',
            '-march=x86-64-v2',
        ),
        (
            'x86_64-apple-darwin',
            'x86-64-v2',
            '-C target-cpu=x86-64-v2',
            '-march=x86-64-v2',
        ),
        (
            'x86_64-pc-windows-msvc',
            'x86-64-v2-msvc-c-default',
            '-C target-cpu=x86-64-v2',
            '',
        ),
        (
            'aarch64-unknown-linux-gnu',
            'compiler-default-arm64',
            '',
            '',
        ),
    ],
)
def test_effective_release_policy_has_no_architecture_crossing(
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    policy: str,
    rustflags: str,
    cflags: str,
) -> None:
    monkeypatch.delenv('CFLAGS', raising=False)
    monkeypatch.delenv('CXXFLAGS', raising=False)
    release_values = {
        'GOMETRY_RELEASE_TARGET': target,
        'GOMETRY_RELEASE_TARGET_POLICY': policy,
        'GOMETRY_RELEASE_RUSTFLAGS': rustflags,
        'GOMETRY_RELEASE_CFLAGS': cflags,
        'GOMETRY_RELEASE_CXXFLAGS': cflags,
    }
    for name, value in release_values.items():
        monkeypatch.setenv(name, value)
    target_suffix = target.replace('-', '_')
    monkeypatch.setenv('CARGO_BUILD_TARGET', target)
    monkeypatch.setenv('RUSTFLAGS', rustflags)
    monkeypatch.delenv('CARGO_ENCODED_RUSTFLAGS', raising=False)
    if target.startswith('x86_64'):
        monkeypatch.setenv('CFLAGS', '-march=native')
        with pytest.raises(AssertionError, match='CFLAGS disagrees'):
            artifact_common._effective_release_policy(target)
        monkeypatch.setenv('CFLAGS', cflags)
        for prefix in ('CFLAGS', 'CXXFLAGS'):
            name = f'{prefix}_{target_suffix}'
            monkeypatch.setenv(name, '-march=native')
            with pytest.raises(AssertionError, match=f'{name} disagrees'):
                artifact_common._effective_release_policy(target)
            monkeypatch.setenv(name, cflags)
    else:
        monkeypatch.setenv(f'CFLAGS_{target_suffix}', cflags)
        monkeypatch.setenv(f'CXXFLAGS_{target_suffix}', cflags)
    assert artifact_common._effective_release_policy(target)['target'] == target


def test_partial_release_identity_environment_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in artifact_common.RELEASE_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(
        'GOMETRY_RELEASE_TARGET',
        'x86_64-unknown-linux-gnu',
    )
    with pytest.raises(AssertionError, match='environment is partial'):
        artifact_common._effective_release_policy()


def test_release_build_env_does_not_embed_profile_or_source_declarations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('GOMETRY_RELEASE_PROFILE', raising=False)
    monkeypatch.delenv('GOMETRY_RELEASE_SOURCE_COMMIT', raising=False)
    environment = artifact_common.release_build_env(target='x86_64-unknown-linux-gnu')
    assert environment['MATURIN_PEP517_ARGS'] == '--locked --profile release-dist'
    assert 'GOMETRY_RELEASE_PROFILE' not in environment
    assert 'GOMETRY_RELEASE_SOURCE_COMMIT' not in environment


def test_x86_64_v2_marker_check_requires_evidence(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match='no native extension'):
        artifact_content._verify_x86_64_v2_markers({}, tmp_path / 'empty.whl')

    extension = 'gometry/_lib.cpython-314-x86_64-linux-gnu.so'
    v2_features = artifact_content.X86_64_V2_TARGET_FEATURES | {
        'fxsr',
        'sse',
        'sse2',
        'x87',
    }
    artifact_content._verify_x86_64_v2_markers(
        {extension: b'target_features' + ','.join(sorted(v2_features)).encode('ascii')},
        tmp_path / 'v2.whl',
    )
    with pytest.raises(AssertionError, match='missing x86-64-v2 target features'):
        artifact_content._verify_x86_64_v2_markers(
            {extension: b'target_featuresfxsr,sse,sse2,x87'},
            tmp_path / 'generic.whl',
        )


def test_identifier_mapping_rejects_ambiguous_wheels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheels = [Path('one.whl'), Path('two.whl')]
    monkeypatch.setattr(release_artifact, 'wheel_matches_identifier', lambda *_: True)
    with pytest.raises(AssertionError, match='one-to-one'):
        release_artifact._match_wheels_to_identifiers(
            wheels,
            ['cp314-manylinux_x86_64', 'cp314-manylinux_2_17_x86_64'],
        )
