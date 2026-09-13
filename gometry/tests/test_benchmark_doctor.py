"""Build identity evidence tests for the benchmark doctor."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import sys
import zipfile
from pathlib import Path

import pytest

from tests._support import GOMETRY_ROOT, load_tool

_DRIVERS = Path(__file__).resolve().parents[1] / 'benches' / 'drivers'
if str(_DRIVERS) not in sys.path:
    sys.path.insert(0, str(_DRIVERS))

import bench_doctor as doctor  # noqa: E402

wheel_smoke = load_tool(
    'wheel_smoke',
    GOMETRY_ROOT / 'tools' / 'wheel_smoke.py',
)


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
    assert doctor._build_info_failures(
        _release_build_info(source_inputs_sha256),
        source_inputs_sha256,
        doctor._rustc_host_target(),
        doctor._native_source_inputs_digest(),
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
    assert build_info['native_source_inputs_sha256'] == doctor._native_source_inputs_digest()
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
        (path / 'meminfo').write_text('Node MemTotal:       1024 kB\nNode MemFree:         512 kB\n')
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
        package_info=package('shapely', '/site-packages/shapely/__init__.py', shapely_extension),
        loaded_native_libraries=loaded,
    )
    pyproj = doctor.implementation_identity(
        'pyproj.transform/1',
        file_identity('/site-packages/gometry/_lib.so'),
        package_info=package('pyproj', '/site-packages/pyproj/__init__.py', pyproj_extension),
        loaded_native_libraries=loaded,
    )
    assert {item['path'] for item in shapely['native_libraries']} == {
        shapely_extension, '/nix/store/libgeos_c.so.1'
    }
    assert {item['path'] for item in pyproj['native_libraries']} == {
        pyproj_extension, '/nix/store/libproj.so.25'
    }
    assert '/nix/store/libpython3.13.so' not in {
        item['path'] for item in shapely['native_libraries'] + pyproj['native_libraries']
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
    extension = {'path': '/checkout/gometry/_lib.so', 'sha256': '0' * 64, 'size_bytes': 1}
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


def test_native_identity_walkers_drop_hidden_metadata_and_preserve_visible_inputs(
    tmp_path: Path,
) -> None:
    package = tmp_path / 'gometry'
    (package / 'src').mkdir(parents=True)
    (package / 'native' / 'visible').mkdir(parents=True)
    (package / 'Cargo.toml').write_text('[package]\nname = "gometry"\n')
    (package / 'build.rs').write_text('fn main() {}\n')
    (package / 'rust-toolchain.toml').write_text('[toolchain]\nchannel = "nightly"\n')
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

    files, native = wheel_smoke._source_input_records(package)
    assert any(name == 'gometry/native/visible/input.c' for name, _ in native)
    assert not any(
        wheel_smoke._is_hidden_native_path(name) for name, _ in files
    )
    before = wheel_smoke.source_input_digests(package)
    (package / 'native' / '.git').write_text('changed metadata\n')
    after = wheel_smoke.source_input_digests(package)
    assert before == after

    bootstrap = tmp_path / 'bootstrap'
    final = tmp_path / 'final'
    bootstrap_native = bootstrap / 'gometry' / 'native'
    final_native = final / 'gometry' / 'native'
    bootstrap_native.mkdir(parents=True)
    final_native.mkdir(parents=True)
    (bootstrap_native / '.git').write_text('metadata\n')
    (bootstrap_native / 'input.c').write_text('input\n')
    (final_native / 'input.c').write_text('input\n')
    wheel_smoke._compare_restage_trees(bootstrap, final)

    (final_native / '.git').write_text('admitted metadata\n')
    with pytest.raises(AssertionError, match='hidden native files'):
        wheel_smoke._compare_restage_trees(bootstrap, final)


@pytest.mark.parametrize(
    ('target', 'policy', 'rustflags', 'cflags'),
    [
        (
            'x86_64-unknown-linux-gnu',
            'generic-x86-64',
            '-C target-cpu=x86-64',
            '-march=x86-64',
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
    release_values = {
        'GOMETRY_RELEASE_TARGET': target,
        'GOMETRY_RELEASE_TARGET_POLICY': policy,
        'GOMETRY_RELEASE_RUSTFLAGS': rustflags,
        'GOMETRY_RELEASE_CFLAGS': cflags,
        'GOMETRY_RELEASE_CXXFLAGS': cflags,
    }
    for name, value in release_values.items():
        monkeypatch.setenv(name, value)
    if target.startswith('x86_64'):
        monkeypatch.setenv('CFLAGS', '-march=native')
        with pytest.raises(AssertionError, match='CFLAGS disagrees'):
            wheel_smoke._effective_release_policy(target)
        monkeypatch.setenv('CFLAGS', cflags)
        target_suffix = target.replace('-', '_')
        for prefix in ('CFLAGS', 'CXXFLAGS'):
            name = f'{prefix}_{target_suffix}'
            monkeypatch.setenv(name, '-march=native')
            with pytest.raises(AssertionError, match=f'{name} disagrees'):
                wheel_smoke._effective_release_policy(target)
            monkeypatch.setenv(name, cflags)
    assert wheel_smoke._effective_release_policy(target)['target'] == target


def test_partial_release_identity_environment_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in wheel_smoke.RELEASE_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(
        'GOMETRY_RELEASE_TARGET',
        'x86_64-unknown-linux-gnu',
    )
    with pytest.raises(AssertionError, match='environment is partial'):
        wheel_smoke._effective_release_policy()


def test_release_build_env_does_not_embed_profile_or_source_declarations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('GOMETRY_RELEASE_PROFILE', raising=False)
    monkeypatch.delenv('GOMETRY_RELEASE_SOURCE_COMMIT', raising=False)
    environment = wheel_smoke.release_build_env(
        target='x86_64-unknown-linux-gnu'
    )
    assert environment['MATURIN_PEP517_ARGS'] == '--locked --profile release-dist'
    assert 'GOMETRY_RELEASE_PROFILE' not in environment
    assert 'GOMETRY_RELEASE_SOURCE_COMMIT' not in environment


def _identity_source_manifest(tmp_path: Path) -> Path:
    source_dir = tmp_path / 'source'
    source_dir.mkdir()
    sdist = source_dir / 'gometry-1.0.0.tar.gz'
    sdist.write_bytes(b'sdist')
    prepared = {'README.md': hashlib.sha256(b'readme').hexdigest()}
    value = {
        'schema': 2,
        'kind': 'gometry-source-manifest',
        'package': 'gometry',
        'commit': '0' * 40,
        'source_inputs_format': wheel_smoke.SOURCE_INPUTS_FORMAT,
        'source_inputs_sha256': '1' * 64,
        'native_source_inputs_format': wheel_smoke.SOURCE_INPUTS_FORMAT,
        'native_source_inputs_sha256': '2' * 64,
        'prepared_inputs': {
            'identity_sha256': wheel_smoke._prepared_input_identity(prepared),
            'files': prepared,
        },
        'generated_output_hashes': prepared,
        'sdist': {
            'filename': sdist.name,
            'sha256': hashlib.sha256(b'sdist').hexdigest(),
            'version': '1.0.0',
        },
        'cargo': {
            'lock_preimage_sha256': '3' * 64,
            'lock_normalized_sha256': '4' * 64,
            'required_sources': [{}],
        },
        'toolchain': {
            'channel': wheel_smoke._package_toolchain_channel(),
            'maturin': wheel_smoke.MATURIN_VERSION,
            'maturin_pep517_args': '--locked --profile release-dist',
            'cargo': 'cargo 1.100.0-nightly',
            'rustc': 'rustc 1.100.0-nightly',
        },
    }
    path = source_dir / wheel_smoke.SOURCE_MANIFEST_NAME
    path.write_text(json.dumps(value), encoding='utf-8')
    return path


def _identity_wheel(path: Path) -> None:
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr(
            'gometry-1.0.0.dist-info/WHEEL',
            'Wheel-Version: 1.0\n'
            'Tag: cp314-cp314-manylinux_2_17_x86_64\n',
        )


def _identity_receipt(source_manifest: Path, wheel: Path) -> Path:
    manifest = json.loads(source_manifest.read_text(encoding='utf-8'))
    build = wheel_smoke.release_target_policy('x86_64-unknown-linux-gnu')
    receipt = {
        'schema': 2,
        'kind': 'gometry-wheel-receipt',
        'profile': wheel_smoke.RELEASE_PROFILE,
        'runner': 'ubuntu-latest',
        'source': wheel_smoke._source_identity(manifest),
        'build': build,
        'planned_identifiers': ['cp314-manylinux_x86_64'],
        'wheels': [{
            'identifier': 'cp314-manylinux_x86_64',
            'filename': wheel.name,
            'sha256': wheel_smoke.sha256_file(wheel),
        }],
    }
    path = wheel.parent / wheel_smoke.RUNNER_RECEIPT_NAME
    path.write_text(json.dumps(receipt), encoding='utf-8')
    return path


def test_receipt_identity_mutations_are_rejected(tmp_path: Path) -> None:
    source_manifest = _identity_source_manifest(tmp_path)
    wheel_dir = tmp_path / 'wheels'
    wheel_dir.mkdir()
    wheel = wheel_dir / 'gometry-1.0.0-cp314-cp314-manylinux_2_17_x86_64.whl'
    _identity_wheel(wheel)
    receipt = _identity_receipt(source_manifest, wheel)
    wheel_smoke.validate_runner_receipt(
        receipt,
        wheel_dir,
        source_manifest,
        inspect_archives=False,
    )

    value = json.loads(receipt.read_text(encoding='utf-8'))
    value['build']['rustflags'] = ['-C', 'target-cpu=x86-64-v2']
    receipt.write_text(json.dumps(value), encoding='utf-8')
    with pytest.raises(AssertionError, match='release flags'):
        wheel_smoke.validate_runner_receipt(
            receipt,
            wheel_dir,
            source_manifest,
            inspect_archives=False,
        )

    value = json.loads(_identity_receipt(source_manifest, wheel).read_text())
    value['profile'] = 'release'
    receipt.write_text(json.dumps(value), encoding='utf-8')
    with pytest.raises(AssertionError, match='profile'):
        wheel_smoke.validate_runner_receipt(
            receipt,
            wheel_dir,
            source_manifest,
            inspect_archives=False,
        )

    value = json.loads(_identity_receipt(source_manifest, wheel).read_text())
    value['source']['native_source_inputs_sha256'] = '5' * 64
    receipt.write_text(json.dumps(value), encoding='utf-8')
    with pytest.raises(AssertionError, match='source identity'):
        wheel_smoke.validate_runner_receipt(
            receipt,
            wheel_dir,
            source_manifest,
            inspect_archives=False,
        )


def test_identifier_mapping_rejects_ambiguous_wheels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheels = [Path('one.whl'), Path('two.whl')]
    monkeypatch.setattr(wheel_smoke, '_wheel_matches_identifier', lambda *_: True)
    with pytest.raises(AssertionError, match='one-to-one'):
        wheel_smoke._match_wheels_to_identifiers(
            wheels,
            ['cp314-manylinux_x86_64', 'cp314-manylinux_2_17_x86_64'],
        )
