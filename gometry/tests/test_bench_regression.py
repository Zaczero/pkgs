from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest
from conftest import GOMETRY_ROOT, load_tool

if TYPE_CHECKING:
    from pathlib import Path


def _artifact(rows: dict[str, list[float]]) -> dict[str, Any]:
    return {
        'benchmarks': [
            {
                'metadata': {'name': name},
                'runs': [{'values': values}],
            }
            for name, values in rows.items()
        ]
    }


def _write(path: Path, rows: dict[str, list[float]]) -> Path:
    path.write_text(json.dumps(_artifact(rows)), encoding='utf-8')
    return path


def test_bench_regression_gate_flags_only_slow_common_rows(tmp_path: Path) -> None:
    gate = load_tool(
        'gometry_bench_regression_gate_test',
        GOMETRY_ROOT / 'tools' / 'gates' / '_check_bench_regression.py',
    )
    baseline = _write(
        tmp_path / 'baseline.json',
        {
            'gometry.points/10k': [1.0, 1.0],
            'gometry.from_wkb/1k': [2.0, 2.0],
        },
    )
    clean = _write(
        tmp_path / 'clean.json',
        {
            'gometry.points/10k': [1.01, 1.01],
            'gometry.from_wkb/1k': [1.9, 1.9],
            'gometry.new/1k': [3.0, 3.0],
        },
    )
    regressed = _write(
        tmp_path / 'regressed.json',
        {
            'gometry.points/10k': [1.1, 1.1],
            'gometry.from_wkb/1k': [1.9, 1.9],
        },
    )

    assert gate.main(baseline, clean) == 0
    assert gate.main(baseline, regressed) == 1


def test_bench_regression_gate_skips_degenerate_baseline(tmp_path: Path) -> None:
    gate = load_tool(
        'gometry_bench_regression_gate_degenerate',
        GOMETRY_ROOT / 'tools' / 'gates' / '_check_bench_regression.py',
    )
    # A zero baseline mean is a malformed artifact: skipped, never a crash.
    baseline = _write(tmp_path / 'baseline.json', {'gometry.points/10k': [0.0, 0.0]})
    candidate = _write(tmp_path / 'candidate.json', {'gometry.points/10k': [1.0, 1.0]})
    assert gate.main(baseline, candidate) == 0


def test_bench_regression_gate_merges_pool_directories(tmp_path: Path) -> None:
    gate = load_tool(
        'gometry_bench_regression_gate_pool',
        GOMETRY_ROOT / 'tools' / 'gates' / '_check_bench_regression.py',
    )
    # A pool directory merges per-suite artifacts (disjoint name spaces) into one
    # name->mean map, so a directory baseline/candidate compares correctly.
    baseline_dir = tmp_path / 'baseline'
    candidate_dir = tmp_path / 'candidate'
    baseline_dir.mkdir()
    candidate_dir.mkdir()
    _write(baseline_dir / 'gometry.json', {'gometry.points/10k': [1.0]})
    _write(baseline_dir / 'competitors.json', {'shapely.points/10k': [2.0]})
    _write(candidate_dir / 'gometry.json', {'gometry.points/10k': [1.5]})
    _write(candidate_dir / 'competitors.json', {'shapely.points/10k': [2.0]})
    # gometry.points regressed 1.5x; the comparison must find it across suites.
    assert gate.main(baseline_dir, candidate_dir) == 1


def test_bench_regression_gate_pools_duplicate_lead_order_artifacts(
    tmp_path: Path,
) -> None:
    gate = load_tool(
        'gometry_bench_regression_gate_lead_order_pool',
        GOMETRY_ROOT / 'tools' / 'gates' / '_check_bench_regression.py',
    )
    _write(
        tmp_path / '20260712T000000Z-competitors-pair-01-ab-release.json',
        {'gometry.points/10k': [1.0, 2.0]},
    )
    _write(
        tmp_path / '20260712T000000Z-competitors-pair-01-ba-release.json',
        {'gometry.points/10k': [3.0, 4.0]},
    )

    stats = gate._stats(tmp_path)['release']['gometry.points/10k']
    assert stats.mean == 2.5
    assert stats.median == 2.5
    assert stats.samples == 4


def test_bench_regression_gate_rejects_newest_incomplete_run(tmp_path: Path) -> None:
    gate = load_tool(
        'gometry_bench_regression_gate_incomplete_run',
        GOMETRY_ROOT / 'tools' / 'gates' / '_check_bench_regression.py',
    )
    manifest = {
        'profile': 'release',
        'timestamp': '20260712T000000Z',
        'plan_only': False,
        'full_manifest': True,
        'publishable': True,
        'benchmark_names': ['gometry.points/10k'],
        'planned_commands': [
            {'output': '20260712T000000Z-a-release.json'},
            {'output': '20260712T000000Z-b-release.json'},
        ],
        'commands': [{'returncode': 0}],
        'artifacts': [
            '20260712T000000Z-a-release.json',
            '20260712T000000Z-b-release.json',
        ],
    }
    (tmp_path / '20260712T000000Z-release.json').write_text(
        json.dumps(manifest), encoding='utf-8'
    )

    with pytest.raises(SystemExit, match='incomplete'):
        gate._stats(tmp_path)


def test_bench_regression_gate_rejects_filtered_release_run(tmp_path: Path) -> None:
    gate = load_tool(
        'gometry_bench_regression_gate_filtered_run',
        GOMETRY_ROOT / 'tools' / 'gates' / '_check_bench_regression.py',
    )
    manifest = {
        'profile': 'release',
        'timestamp': '20260712T000000Z',
        'plan_only': False,
        'full_manifest': False,
        'publishable': False,
        'benchmark_names': ['gometry.points/10k'],
        'planned_commands': [{'output': '20260712T000000Z-a-release.json'}],
        'commands': [{'returncode': 0}],
        'artifacts': ['20260712T000000Z-a-release.json'],
    }
    (tmp_path / '20260712T000000Z-release.json').write_text(
        json.dumps(manifest), encoding='utf-8'
    )

    with pytest.raises(SystemExit, match='filtered'):
        gate._stats(tmp_path)


def test_bench_regression_gate_rejects_nonpublishable_release_run(
    tmp_path: Path,
) -> None:
    gate = load_tool(
        'gometry_bench_regression_gate_nonpublishable_run',
        GOMETRY_ROOT / 'tools' / 'gates' / '_check_bench_regression.py',
    )
    manifest = {
        'profile': 'release',
        'timestamp': '20260712T000000Z',
        'plan_only': False,
        'full_manifest': True,
        'publishable': False,
        'benchmark_names': ['gometry.points/10k'],
        'planned_commands': [{'output': '20260712T000000Z-a-release.json'}],
        'commands': [{'returncode': 0}],
        'artifacts': ['20260712T000000Z-a-release.json'],
    }
    (tmp_path / '20260712T000000Z-release.json').write_text(
        json.dumps(manifest), encoding='utf-8'
    )

    with pytest.raises(SystemExit, match='non-publishable'):
        gate._stats(tmp_path)


def test_bench_regression_gate_rejects_missing_manifest_row(tmp_path: Path) -> None:
    gate = load_tool(
        'gometry_bench_regression_gate_missing_row',
        GOMETRY_ROOT / 'tools' / 'gates' / '_check_bench_regression.py',
    )
    timestamp = '20260712T000000Z'
    manifest = {
        'profile': 'release',
        'timestamp': timestamp,
        'plan_only': False,
        'full_manifest': True,
        'publishable': True,
        'benchmark_names': ['gometry.points/10k', 'gometry.from_wkb/1k'],
        'planned_commands': [
            {'output': f'{timestamp}-gometry-release.json'},
        ],
        'commands': [{'returncode': 0}],
        'artifacts': [f'{timestamp}-gometry-release.json'],
    }
    (tmp_path / f'{timestamp}-release.json').write_text(
        json.dumps(manifest), encoding='utf-8'
    )
    _write(
        tmp_path / f'{timestamp}-gometry-release.json',
        {'gometry.points/10k': [1.0]},
    )

    with pytest.raises(SystemExit, match=r'missing: gometry\.from_wkb/1k'):
        gate._stats(tmp_path)


def test_bench_regression_gate_rejects_missing_resource_artifact(
    tmp_path: Path,
) -> None:
    gate = load_tool(
        'gometry_bench_regression_gate_missing_resource',
        GOMETRY_ROOT / 'tools' / 'gates' / '_check_bench_regression.py',
    )
    timestamp = '20260712T000000Z'
    benchmark_name = f'{timestamp}-gometry-release.json'
    resource_name = f'{timestamp}-resource-bearing-release.json'
    manifest = {
        'profile': 'release',
        'timestamp': timestamp,
        'plan_only': False,
        'full_manifest': True,
        'publishable': True,
        'benchmark_names': ['gometry.points/10k'],
        'planned_commands': [
            {'output': benchmark_name},
            {'output': resource_name},
        ],
        'commands': [{'returncode': 0}, {'returncode': 0}],
        'artifacts': [benchmark_name, resource_name],
    }
    (tmp_path / f'{timestamp}-release.json').write_text(
        json.dumps(manifest), encoding='utf-8'
    )
    _write(
        tmp_path / benchmark_name,
        {'gometry.points/10k': [1.0]},
    )

    with pytest.raises(SystemExit, match='missing manifest artifacts'):
        gate._stats(tmp_path)


def test_benchmark_summary_resolves_exact_run_manifest(tmp_path: Path) -> None:
    summary = load_tool(
        'gometry_benchmark_summary_manifest',
        GOMETRY_ROOT / 'benches' / 'support' / 'summarize_bench.py',
        GOMETRY_ROOT / 'benches' / 'python',
    )
    artifact = _write(
        tmp_path / '20260712T000000Z-gometry-release.json',
        {'gometry.points/10k': [1.0]},
    )
    manifest = tmp_path / '20260712T000000Z-release.json'
    manifest.write_text(
        json.dumps({'artifacts': [f'/moved/archive/{artifact.name}']}),
        encoding='utf-8',
    )

    assert summary._resolve_paths(str(manifest)) == [artifact]
    assert summary._resolve_paths(str(tmp_path)) == [artifact]


def test_benchmark_summary_rejects_directory_with_multiple_runs(
    tmp_path: Path,
) -> None:
    summary = load_tool(
        'gometry_benchmark_summary_ambiguous_directory',
        GOMETRY_ROOT / 'benches' / 'support' / 'summarize_bench.py',
        GOMETRY_ROOT / 'benches' / 'python',
    )
    for timestamp in ('20260712T000000Z', '20260712T010000Z'):
        artifact = _write(
            tmp_path / f'{timestamp}-gometry-release.json',
            {'gometry.points/10k': [1.0]},
        )
        (tmp_path / f'{timestamp}-release.json').write_text(
            json.dumps({'artifacts': [str(artifact)]}), encoding='utf-8'
        )

    with pytest.raises(SystemExit, match='multiple benchmark runs'):
        summary._resolve_paths(str(tmp_path))


def test_release_benchmark_rejects_unpublishable_environment() -> None:
    driver = load_tool(
        'gometry_release_bench_driver_test',
        GOMETRY_ROOT / 'benches' / 'drivers' / 'bench.py',
        GOMETRY_ROOT / 'benches' / 'drivers',
    )
    driver._validate_run_environment('smoke', plan_only=False, warnings=['busy'])
    driver._validate_run_environment('release', plan_only=True, warnings=['busy'])
    driver._validate_run_environment('release', plan_only=False, warnings=[])
    with pytest.raises(SystemExit, match='not publishable') as exc_info:
        driver._validate_run_environment(
            'release',
            plan_only=False,
            warnings=['system load is high'],
        )
    assert 'system load is high' in str(exc_info.value)


def test_benchmark_doctor_checks_every_workspace_build_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    doctor = load_tool(
        'gometry_benchmark_doctor_build_inputs',
        GOMETRY_ROOT / 'benches' / 'drivers' / 'bench_doctor.py',
    )
    observed: dict[str, object] = {}

    def fake_run(args: list[str], cwd: Path = doctor.GOMETRY_ROOT) -> str:
        observed['args'] = args
        observed['cwd'] = cwd
        return ''

    monkeypatch.setattr(doctor, '_run', fake_run)
    assert doctor._git_build_inputs_dirty() is False
    assert observed == {
        'args': [
            'git',
            'status',
            '--porcelain',
            '--untracked-files=all',
            '--',
            'gometry',
            '.cargo/config.toml',
            'Cargo.toml',
            'Cargo.lock',
        ],
        'cwd': doctor.REPO_ROOT,
    }


def test_release_competitor_plan_balances_every_pair(tmp_path: Path) -> None:
    driver = load_tool(
        'gometry_release_bench_pair_plan_test',
        GOMETRY_ROOT / 'benches' / 'drivers' / 'bench.py',
        GOMETRY_ROOT / 'benches' / 'drivers',
    )
    profile = driver.PROFILES['release']
    plans, outputs = driver._benchmark_plans(
        profile_name='release',
        profile=profile,
        selected={
            'competitors': profile.competitors,
            'real_world': profile.real_world,
        },
        output_dir=tmp_path,
        timestamp='20260712T000000Z',
        cpu=7,
    )

    assert len(plans) == len(profile.competitors) + len(profile.real_world)
    assert len(outputs['competitors']) == len(profile.competitors)
    assert len(outputs['real_world']) == len(profile.real_world)
    for left, right in zip(plans[::2], plans[1::2], strict=True):
        assert left['label'].endswith('-ab')
        assert right['label'].endswith('-ba')
        assert left['rows'] == right['rows'][::-1]
        assert left['command'][left['command'].index('--affinity') + 1] == '7'
        assert '--processes' in left['command']
        assert left['command'][left['command'].index('--processes') + 1] == '3'


def test_pyperf_summaries_pool_balanced_order_passes(tmp_path: Path) -> None:
    driver = load_tool(
        'gometry_release_bench_summary_pool_test',
        GOMETRY_ROOT / 'benches' / 'drivers' / 'bench.py',
        GOMETRY_ROOT / 'benches' / 'drivers',
    )
    first = _write(tmp_path / 'ab.json', {'gometry.points/10k': [1.0, 2.0]})
    second = _write(tmp_path / 'ba.json', {'gometry.points/10k': [3.0, 4.0]})

    assert driver._pyperf_summaries([first, second]) == [
        {
            'name': 'gometry.points/10k',
            'mean': 2.5,
            'median': 2.5,
            'stdev': pytest.approx(1.2909944487358056),
            'samples': 4,
        }
    ]


def test_release_filter_can_run_one_exact_row(tmp_path: Path) -> None:
    driver = load_tool(
        'gometry_release_bench_single_row_test',
        GOMETRY_ROOT / 'benches' / 'drivers' / 'bench.py',
        GOMETRY_ROOT / 'benches' / 'drivers',
    )
    profile = driver.PROFILES['release']
    driver._validate_release_manifest(profile)
    plans, _ = driver._benchmark_plans(
        profile_name='release',
        profile=profile,
        selected={'competitors': ('gometry.from_wkb.batch/1k',)},
        output_dir=tmp_path,
        timestamp='20260712T000000Z',
        cpu=7,
    )

    assert len(plans) == 1
    assert plans[0]['rows'] == ('gometry.from_wkb.batch/1k',)
    assert plans[0]['label'] == 'competitors-row-01'
    assert plans[0]['command'][plans[0]['command'].index('--processes') + 1] == '6'
