"""Compare two pyperf JSON artifacts and flag benchmark regressions.

Benchmark identity is the pyperf metadata ``name``. Rows present only in the
candidate are reported as new coverage, not failures.

STATISTICAL POWER (read before trusting a verdict): this gate compares
candidate and baseline effective mean intervals using the same
BenchStats/noise-band machinery as ``benches/support/summarize_bench.py``. The interval
radius is 1.96 standard errors with the configured noise band as a floor. A
regression only fires when the candidate is slower and its interval no longer
overlaps the baseline interval. Per-change interleaved A/B remains the gold
standard; this gate is a coarse CI backstop, not a substitute for it.
"""

from __future__ import annotations

import json
import math
import statistics
import sys
from pathlib import Path

GOMETRY_ROOT = Path(__file__).resolve().parents[2]
_BENCH_SUPPORT = GOMETRY_ROOT / 'benches' / 'support'
if str(_BENCH_SUPPORT) not in sys.path:
    sys.path.insert(0, str(_BENCH_SUPPORT))

from summarize_bench import (
    BenchStats,
    _load_all_stats,
    _noise_radius,
    _stats_intervals_overlap,
)

DEFAULT_NOISE_BAND = 0.03
# Baseline and candidate are drawn from physically separate pools so the
# default can never compare candidate-vs-candidate: locked release artifacts
# are the baseline and fresh orchestrator output is the candidate.
DEFAULT_BASELINE_DIR = GOMETRY_ROOT / 'benches' / 'results' / 'baseline'
# Keep fresh evidence in the project-local ignored target directory.
DEFAULT_CANDIDATE_DIR = GOMETRY_ROOT / 'target' / 'bench' / 'results'


def _load_minimal_all_stats(paths: list[Path]) -> dict[str, BenchStats]:
    values_by_name: dict[str, list[float]] = {}
    runs_by_name: dict[str, int] = {}
    for path in paths:
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            continue
        for bench in data.get('benchmarks', []):
            name = bench.get('metadata', {}).get('name')
            values = [
                float(value)
                for run in bench.get('runs', [])
                for value in run.get('values', [])
                if math.isfinite(float(value))
            ]
            if not name or not values:
                continue
            values_by_name.setdefault(name, []).extend(values)
            runs_by_name[name] = runs_by_name.get(name, 0) + len(bench.get('runs', []))
    return {
        name: BenchStats(
            name=name,
            mean=statistics.fmean(values),
            median=statistics.median(values),
            stdev=statistics.stdev(values) if len(values) > 1 else 0.0,
            nrun=runs_by_name[name],
            samples=len(values),
        )
        for name, values in values_by_name.items()
    }


def _load_stats(path: Path) -> dict[str, BenchStats]:
    return _load_paths_stats([path])


def _load_paths_stats(paths: list[Path]) -> dict[str, BenchStats]:
    stats = _load_all_stats(paths)
    return stats or _load_minimal_all_stats(paths)


#: Harness artifacts are ``<stamp>[-<suite>]-<profile>.json``. The profile
#: suffix keeps single-value smoke rows from superseding release rows.
_PROFILES = ('smoke', 'release')


def _artifact_profile(path: Path) -> str | None:
    stem = path.stem
    return next((p for p in _PROFILES if stem.endswith(f'-{p}')), None)


def _run_manifest(
    path: Path,
) -> tuple[str, str, bool, frozenset[str], frozenset[str]] | None:
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return None
    profile = payload.get('profile')
    timestamp = payload.get('timestamp')
    planned = payload.get('planned_commands')
    commands = payload.get('commands')
    benchmark_names = payload.get('benchmark_names')
    artifacts = payload.get('artifacts')
    if (
        profile not in _PROFILES
        or not isinstance(timestamp, str)
        or not isinstance(planned, list)
        or not isinstance(commands, list)
        or not isinstance(benchmark_names, list)
        or not all(isinstance(name, str) for name in benchmark_names)
        or not isinstance(artifacts, list)
        or not all(isinstance(artifact, str) for artifact in artifacts)
    ):
        return None
    planned_artifacts = frozenset(
        Path(command['output']).name
        for command in planned
        if isinstance(command, dict) and isinstance(command.get('output'), str)
    )
    reported_artifacts = frozenset(Path(artifact).name for artifact in artifacts)
    complete = (
        payload.get('plan_only') is False
        and payload.get('full_manifest') is True
        and (profile != 'release' or payload.get('publishable') is True)
        and len(commands) == len(planned)
        and len(planned_artifacts) == len(planned)
        and reported_artifacts == planned_artifacts
        and bool(commands)
        and all(command.get('returncode') == 0 for command in commands)
    )
    return (
        profile,
        timestamp,
        complete,
        frozenset(benchmark_names),
        planned_artifacts,
    )


def _stats(target: Path) -> dict[str | None, dict[str, BenchStats]]:
    """Profile -> benchmark name -> BenchStats for an artifact OR a pool dir.

    A driver run writes one manifest plus many pyperf artifacts. Within each
    profile, use the newest manifest, require it to be complete, and pool every
    artifact with its timestamp. This combines the A/B and B/A lead-order files
    instead of letting one overwrite the other. Profiles never cross-compare.

    Direct pyperf pools without a run manifest are pooled by profile. This is
    useful for explicit file collections and small test fixtures, but only a
    complete run manifest qualifies as canonical release evidence.
    """
    if target.is_dir():
        paths = sorted(target.rglob('*.json'))
        manifests: dict[
            str,
            list[tuple[str, bool, frozenset[str], frozenset[str]]],
        ] = {}
        for path in paths:
            manifest = _run_manifest(path)
            if manifest is None:
                continue
            profile, timestamp, complete, benchmark_names, artifacts = manifest
            manifests.setdefault(profile, []).append((
                timestamp,
                complete,
                benchmark_names,
                artifacts,
            ))
        if manifests:
            pools: dict[str | None, dict[str, BenchStats]] = {}
            for profile, runs in manifests.items():
                timestamp, complete, benchmark_names, artifacts = max(
                    runs, key=lambda run: run[0]
                )
                if not complete:
                    raise SystemExit(
                        f'newest {profile} benchmark run under {target} is incomplete, '
                        'filtered, or non-publishable: '
                        f'{timestamp}'
                    )
                available_artifacts = {path.name for path in paths} & artifacts
                missing_artifacts = sorted(artifacts - available_artifacts)
                if missing_artifacts:
                    raise SystemExit(
                        f'{profile} benchmark evidence under {target} is missing '
                        'manifest artifacts: ' + ', '.join(missing_artifacts)
                    )
                run_paths = [
                    path
                    for path in paths
                    if path.name.startswith(f'{timestamp}-')
                    and _artifact_profile(path) == profile
                ]
                stats = _load_paths_stats(run_paths)
                actual_names = frozenset(stats)
                if actual_names != benchmark_names:
                    missing = sorted(benchmark_names - actual_names)
                    unexpected = sorted(actual_names - benchmark_names)
                    details = []
                    if missing:
                        details.append('missing: ' + ', '.join(missing))
                    if unexpected:
                        details.append('unexpected: ' + ', '.join(unexpected))
                    raise SystemExit(
                        f'{profile} benchmark evidence under {target} does not match '
                        f'the {timestamp} manifest ({"; ".join(details)})'
                    )
                pools[profile] = stats
            return pools
        grouped: dict[str | None, list[Path]] = {}
        for path in paths:
            grouped.setdefault(_artifact_profile(path), []).append(path)
        return {
            profile: _load_paths_stats(profile_paths)
            for profile, profile_paths in grouped.items()
        }
    return {_artifact_profile(target): _load_stats(target)}


def main(
    baseline_path: str | Path | None = None,
    candidate_path: str | Path | None = None,
    noise_band: float = DEFAULT_NOISE_BAND,
) -> int:
    baseline = (
        Path(baseline_path) if baseline_path is not None else DEFAULT_BASELINE_DIR
    )
    candidate = (
        Path(candidate_path) if candidate_path is not None else DEFAULT_CANDIDATE_DIR
    )
    for label, path in (('baseline', baseline), ('candidate', candidate)):
        if not path.exists():
            raise SystemExit(
                f'{label} path {path} does not exist; pass explicit baseline and '
                'candidate artifact (or pool) paths'
            )

    baseline_pools = _stats(baseline)
    candidate_pools = _stats(candidate)
    if not any(candidate_pools.values()):
        raise SystemExit(f'no pyperf benchmark stats found under candidate {candidate}')
    regressions: list[tuple[str, float, BenchStats, BenchStats]] = []

    candidate_stats: dict[str, BenchStats] = {}
    baseline_stats: dict[str, BenchStats] = {}
    for profile, rows in candidate_pools.items():
        baseline_rows = baseline_pools.get(profile, {})
        prefix = f'{profile or "unknown-profile"}:'
        candidate_stats.update({f'{prefix}{name}': row for name, row in rows.items()})
        baseline_stats.update({
            f'{prefix}{name}': row for name, row in baseline_rows.items()
        })

    for name in sorted(candidate_stats):
        candidate_row = candidate_stats[name]
        baseline_row = baseline_stats.get(name)
        if baseline_row is None:
            print(f'new (no baseline): {name}')
            continue
        # A non-positive or non-finite mean is a malformed artifact, not a
        # regression: skip with a notice rather than dividing by zero.
        if not (
            math.isfinite(baseline_row.mean)
            and math.isfinite(candidate_row.mean)
            and baseline_row.mean > 0.0
        ):
            print(f'skip (degenerate mean): {name}')
            continue
        ratio = candidate_row.mean / baseline_row.mean
        if candidate_row.mean > baseline_row.mean and not _stats_intervals_overlap(
            candidate_row,
            baseline_row,
            noise_band=noise_band,
        ):
            regressions.append((name, ratio, baseline_row, candidate_row))
        else:
            print(
                f'ok {name}: ratio={ratio:.4f} '
                f'baseline={baseline_row.mean:.9g}s±{_noise_radius(baseline_row, noise_band):.3g}s '
                f'candidate={candidate_row.mean:.9g}s±{_noise_radius(candidate_row, noise_band):.3g}s'
            )

    for name, ratio, baseline_row, candidate_row in regressions:
        print(
            f'REGRESSION {name}: ratio={ratio:.4f} '
            f'baseline={baseline_row.mean:.9g}s±{_noise_radius(baseline_row, noise_band):.3g}s '
            f'candidate={candidate_row.mean:.9g}s±{_noise_radius(candidate_row, noise_band):.3g}s'
        )
    return 1 if regressions else 0


if __name__ == '__main__':
    argv = sys.argv[1:]
    noise_band = DEFAULT_NOISE_BAND
    if argv and argv[0] in {'--noise-band', '--threshold'}:
        flag = argv[0]
        if len(argv) < 2:
            raise SystemExit(f'{flag} requires a value, e.g. {flag} 0.03')
        value = float(argv[1])
        if flag == '--threshold' and value > 1.0:
            value -= 1.0
        noise_band = value
        argv = argv[2:]
    if len(argv) not in {0, 2}:
        raise SystemExit(
            'usage: python tools/gates/_check_bench_regression.py '
            '[--noise-band R] [baseline candidate]  (paths may be files or pool dirs)'
        )
    paths = argv or [None, None]
    raise SystemExit(main(paths[0], paths[1], noise_band))
