from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from dataclasses import dataclass, field
from glob import glob
from pathlib import Path

import pyperf

_BENCHES = Path(__file__).resolve().parents[1]
GOMETRY_ROOT = _BENCHES.parent
_BENCHES_PYTHON = _BENCHES / 'python'
if str(_BENCHES_PYTHON) not in sys.path:
    sys.path.insert(0, str(_BENCHES_PYTHON))
from _bench_pairs import find_competitor

MEAN_INTERVAL_Z = 1.96


@dataclass(frozen=True)
class BenchStats:
    name: str
    mean: float
    median: float
    stdev: float
    nrun: int
    samples: int


@dataclass
class OpRow:
    op: str
    gometry: BenchStats
    competitor: BenchStats | None = None
    lib: str = ''
    speedup: float | None = None
    rel_stdev: float = 0.0
    notes: list[str] = field(default_factory=list)


def _manifest_artifacts(path: Path) -> list[Path] | None:
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return None
    artifacts = payload.get('artifacts')
    if not isinstance(artifacts, list) or not all(
        isinstance(artifact, str) for artifact in artifacts
    ):
        return None
    resolved: list[Path] = []
    for artifact in artifacts:
        candidate = Path(artifact)
        if not candidate.is_file():
            candidate = path.parent / candidate.name
        if not candidate.is_file():
            raise SystemExit(
                f'run manifest {path} references a missing artifact: {artifact}'
            )
        resolved.append(candidate)
    return resolved


def _resolve_paths(pattern: str) -> list[Path]:
    path = Path(pattern)
    if path.is_dir():
        manifests = [
            candidate
            for candidate in sorted(path.glob('*.json'))
            if _manifest_artifacts(candidate) is not None
        ]
        if len(manifests) > 1:
            raise SystemExit(
                f'{path} contains multiple benchmark runs; pass one exact '
                '<timestamp>-<profile>.json run manifest'
            )
        if manifests:
            return _manifest_artifacts(manifests[0]) or []
        return sorted(path.glob('*.json'))
    if path.exists():
        artifacts = _manifest_artifacts(path)
        return artifacts if artifacts is not None else [path]
    matched = sorted(Path(match) for match in glob(pattern))
    if matched:
        return matched
    raise SystemExit(f'no benchmark result files matched: {pattern}')


def _is_pyperf_suite(path: Path) -> bool:
    try:
        pyperf.BenchmarkSuite.load(str(path))
    except (OSError, ValueError, KeyError, TypeError):
        return False
    return True


def _load_all_stats(paths: list[Path]) -> dict[str, BenchStats]:
    samples: dict[str, list[float]] = {}
    runs: dict[str, int] = {}
    for path in paths:
        if not _is_pyperf_suite(path):
            continue
        suite = pyperf.BenchmarkSuite.load(str(path))
        for bench in suite.get_benchmarks():
            name = bench.get_name()
            samples.setdefault(name, []).extend(bench.get_values())
            runs[name] = runs.get(name, 0) + bench.get_nrun()
    merged: dict[str, BenchStats] = {}
    for name, values in samples.items():
        merged[name] = BenchStats(
            name=name,
            mean=statistics.fmean(values),
            median=statistics.median(values),
            stdev=statistics.stdev(values) if len(values) > 1 else 0.0,
            nrun=runs[name],
            samples=len(values),
        )
    return merged


def _competitor_lib(name: str) -> str:
    return name.split('.', 1)[0]


def _find_competitor(
    gometry_name: str, competitors: dict[str, BenchStats]
) -> str | None:
    return find_competitor(gometry_name, set(competitors))


def _humanize_seconds(value: float) -> str:
    if value >= 0.001:
        return f'{value * 1000.0:.2f} ms'
    if value >= 1e-06:
        return f'{value * 1000000.0:.2f} µs'
    return f'{value * 1000000000.0:.1f} ns'


def _rel_stdev(mean: float, stdev: float) -> float:
    if mean == 0.0:
        return 0.0
    return stdev / mean


def _intervals_overlap(
    mean_a: float, stdev_a: float, mean_b: float, stdev_b: float
) -> bool:
    return mean_a - stdev_a <= mean_b + stdev_b and mean_b - stdev_b <= mean_a + stdev_a


def _noise_radius(stats: BenchStats, noise_band: float) -> float:
    sampling_radius = (
        MEAN_INTERVAL_Z * stats.stdev / math.sqrt(stats.samples)
        if stats.samples > 1
        else stats.stdev
    )
    return max(sampling_radius, stats.mean * noise_band)


def _stats_intervals_overlap(
    a: BenchStats, b: BenchStats, *, noise_band: float
) -> bool:
    return _intervals_overlap(
        a.mean,
        _noise_radius(a, noise_band),
        b.mean,
        _noise_radius(b, noise_band),
    )


def _geomean(values: list[float]) -> float | None:
    positive = [value for value in values if value > 0]
    if not positive:
        return None
    return math.exp(sum(math.log(value) for value in positive) / len(positive))


def _build_rows(
    gometry_stats: dict[str, BenchStats],
    competitor_stats: dict[str, BenchStats],
    *,
    noise_band: float,
    baseline_stats: dict[str, BenchStats] | None,
) -> list[OpRow]:
    rows: list[OpRow] = []
    for name in sorted(gometry_stats):
        gometry = gometry_stats[name]
        rel = _rel_stdev(gometry.mean, gometry.stdev)
        comp_name = _find_competitor(name, competitor_stats)
        competitor = competitor_stats.get(comp_name) if comp_name else None
        notes: list[str] = []
        speedup: float | None = None
        lib = ''
        if competitor is not None:
            speedup = competitor.mean / gometry.mean
            lib = _competitor_lib(competitor.name)
            tie = _stats_intervals_overlap(gometry, competitor, noise_band=noise_band)
            if speedup < 1.0 and (not tie):
                notes.append('SLOW')
            if tie:
                notes.append('~tie')
        if rel > noise_band:
            notes.append('noisy')
        if baseline_stats is not None and name in baseline_stats:
            baseline = baseline_stats[name]
            overlaps_baseline = _stats_intervals_overlap(
                gometry,
                baseline,
                noise_band=noise_band,
            )
            if gometry.mean > baseline.mean and (not overlaps_baseline):
                notes.append('regression')
            elif gometry.mean < baseline.mean and (not overlaps_baseline):
                notes.append('improved')
        rows.append(
            OpRow(
                op=name,
                gometry=gometry,
                competitor=competitor,
                lib=lib,
                speedup=speedup,
                rel_stdev=rel,
                notes=notes,
            )
        )
    return rows


def _sort_rows(rows: list[OpRow], sort: str) -> list[OpRow]:
    if sort == 'name':
        return sorted(rows, key=lambda row: row.op)
    if sort == 'speedup':
        return sorted(
            rows,
            key=lambda row: (
                row.speedup is None,
                row.speedup if row.speedup is not None else math.inf,
            ),
        )
    if sort == 'gometry':
        return sorted(rows, key=lambda row: row.gometry.mean, reverse=True)
    return sorted(rows, key=lambda row: row.gometry.mean, reverse=True)


def _format_table(rows: list[OpRow], *, markdown: bool) -> list[str]:
    headers = ['op', 'gometry', 'competitor', 'lib', 'speedup', '±%gometry', 'note']
    body: list[list[str]] = []
    for row in rows:
        speedup = '' if row.speedup is None else f'{row.speedup:.2f}x'
        comp_time = (
            '' if row.competitor is None else _humanize_seconds(row.competitor.mean)
        )
        body.append([
            row.op,
            _humanize_seconds(row.gometry.mean),
            comp_time,
            row.lib,
            speedup,
            f'{row.rel_stdev * 100:.1f}%',
            ','.join(row.notes),
        ])
    if markdown:
        lines = [
            '| ' + ' | '.join(headers) + ' |',
            '|' + '|'.join(['---'] * len(headers)) + '|',
        ]
        lines.extend('| ' + ' | '.join(cells) + ' |' for cells in body)
        return lines
    widths = [len(header) for header in headers]
    for cells in body:
        for index, cell in enumerate(cells):
            widths[index] = max(widths[index], len(cell))
    numeric_cols = {1, 2, 4, 5}
    lines = [
        '  '.join(
            (
                header.rjust(widths[index])
                if index in numeric_cols
                else header.ljust(widths[index])
                for index, header in enumerate(headers)
            )
        ),
        '  '.join('-' * widths[index] for index in range(len(headers))),
    ]
    for cells in body:
        lines.append(
            '  '.join(
                (
                    cell.rjust(widths[index])
                    if index in numeric_cols
                    else cell.ljust(widths[index])
                    for index, cell in enumerate(cells)
                )
            )
        )
    return lines


def _header_summary(rows: list[OpRow], *, noise_band: float) -> str:
    paired = sum(1 for row in rows if row.competitor is not None)
    gometry_only = len(rows) - paired
    speedups = [row.speedup for row in rows if row.speedup is not None]
    geomean = _geomean(speedups)
    geomean_text = f'{geomean:.2f}x' if geomean is not None else 'n/a'
    noisy = sum(1 for row in rows if row.rel_stdev > noise_band)
    slow = sum(1 for row in rows if 'SLOW' in row.notes)
    return f'{len(rows)} ops, {paired} paired, {gometry_only} gometry-only, overall geomean {geomean_text}, {noisy} noisy, {slow} slow'


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description='Summarize and compare Gometry pyperf benchmark results.'
    )
    parser.add_argument(
        'results', help='results directory or glob pattern for pyperf JSON files'
    )
    parser.add_argument(
        '--baseline',
        help='prior run directory with gometry pyperf JSONs for regression flags',
    )
    parser.add_argument(
        '--sort',
        choices=['speedup', 'gometry', 'name'],
        default='gometry',
        help='row sort order (default: slowest gometry mean first)',
    )
    parser.add_argument(
        '--format', choices=['table', 'md'], default='table', help='output format'
    )
    parser.add_argument(
        '--noise-band',
        type=float,
        default=0.03,
        help='relative stdev threshold for noisy/regression flags',
    )
    args = parser.parse_args(argv)
    paths = _resolve_paths(args.results)
    all_stats = _load_all_stats(paths)
    gometry_stats = {
        name: stats for name, stats in all_stats.items() if name.startswith('gometry.')
    }
    competitor_stats = {
        name: stats
        for name, stats in all_stats.items()
        if not name.startswith('gometry.')
    }
    if not gometry_stats:
        raise SystemExit('no gometry benchmark rows found in the result files')
    baseline_stats: dict[str, BenchStats] | None = None
    if args.baseline is not None:
        baseline_paths = _resolve_paths(args.baseline)
        baseline_all = _load_all_stats(baseline_paths)
        baseline_stats = {
            name: stats
            for name, stats in baseline_all.items()
            if name.startswith('gometry.')
        }
    rows = _build_rows(
        gometry_stats,
        competitor_stats,
        noise_band=args.noise_band,
        baseline_stats=baseline_stats,
    )
    rows = _sort_rows(rows, args.sort)
    lines = [_header_summary(rows, noise_band=args.noise_band), '']
    lines.extend(_format_table(rows, markdown=args.format == 'md'))
    output = '\n'.join(lines)
    print(output)


if __name__ == '__main__':
    main()
