"""Summarize a gometry release/smoke run JSON as a domain-grouped competitive table.

Reads ordered ``public_operations`` metadata embedded in the run manifest —
never re-infers domains, labels, or pairs from raw benchmark names. Row order
is the manifest editorial order only.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import Any

import pyperf

_BENCHES = Path(__file__).resolve().parents[1]
GOMETRY_ROOT = _BENCHES.parent

MEAN_INTERVAL_Z = 1.96

# Canonical domain order (must match embedded metadata / registry).
DOMAIN_ORDER = (
    'Array construction & I/O',
    'Geometry',
    'CRS & geodesy',
    'Discrete global grids',
    'Spatial index',
    'Real-world workflows',
)

FOOTNOTE_MARKERS: dict[str, str] = {
    'geodesic': '†',
    'in_core': '‡',
    'batched': '§',
    'noisy': '*',
}

FOOTNOTE_LEGEND = (
    '† geodesic/ellipsoidal work.',
    '‡ gometry admitted in-core projection path (not arbitrary CRS transforms).',
    '§ both sides use a full-column/batched API without a timed Python row loop.',
    '* statistically noisy measurement (relative stdev exceeds the noise band).',
)

SMOKE_BANNER = 'Smoke/debug run — not publishable release evidence.'


@dataclass(frozen=True, slots=True)
class BenchStats:
    name: str
    mean: float
    median: float
    stdev: float
    nrun: int
    samples: int


@dataclass(frozen=True, slots=True)
class PublicOpMeta:
    domain: str
    label: str
    workload: str
    suite: str
    gometry: str
    competitor: str | None
    competitor_label: str | None
    footnotes: tuple[str, ...]


@dataclass(slots=True)
class OpRow:
    meta: PublicOpMeta
    gometry: BenchStats
    competitor: BenchStats | None = None
    speedup: float | None = None
    rel_stdev: float = 0.0
    noisy: bool = False
    tie: bool = False

    @property
    def solo(self) -> bool:
        return self.meta.competitor is None


def _manifest_payload(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if 'artifacts' not in payload and 'profile' not in payload:
        return None
    return payload


def _resolve_artifact_paths(payload: dict[str, Any], manifest_path: Path) -> list[Path]:
    artifacts = payload.get('artifacts')
    if not isinstance(artifacts, list):
        return []
    resolved: list[Path] = []
    for artifact in artifacts:
        if not isinstance(artifact, str):
            raise SystemExit(
                f'run manifest {manifest_path} has a non-string artifact entry'
            )
        candidate = Path(artifact)
        if not candidate.is_file():
            candidate = manifest_path.parent / candidate.name
        if not candidate.is_file():
            raise SystemExit(
                f'run manifest {manifest_path} references a missing artifact: '
                f'{artifact}'
            )
        resolved.append(candidate)
    return resolved


def _resolve_run(pattern: str) -> tuple[dict[str, Any], list[Path]]:
    """Load one run manifest and its pyperf artifact paths.

    Public summaries require embedded ``public_operations`` metadata. Raw
    pyperf globs alone are not supported for competitive presentation.
    """
    path = Path(pattern)
    if path.is_dir():
        manifests = [
            candidate
            for candidate in sorted(path.glob('*.json'))
            if _manifest_payload(candidate) is not None
            and 'profile' in (_manifest_payload(candidate) or {})
        ]
        if len(manifests) > 1:
            raise SystemExit(
                f'{path} contains multiple benchmark runs; pass one exact '
                '<timestamp>-<profile>.json run manifest'
            )
        if not manifests:
            raise SystemExit(
                f'{path} has no run manifest with embedded public_operations; '
                'pass the <timestamp>-<profile>.json written by bench.py'
            )
        path = manifests[0]
    if not path.exists():
        matched = sorted(Path(match) for match in glob(pattern))
        if len(matched) == 1 and matched[0].suffix == '.json':
            path = matched[0]
        elif matched:
            raise SystemExit(
                'summarize_bench requires a single run manifest JSON with '
                f'embedded public_operations; matched {len(matched)} paths'
            )
        else:
            raise SystemExit(f'no benchmark result files matched: {pattern}')

    payload = _manifest_payload(path)
    if payload is None or 'profile' not in payload:
        raise SystemExit(
            f'{path} is not a bench.py run manifest. Competitive summaries '
            'require the run JSON so domains/labels come from embedded '
            'public_operations metadata (never re-inferred from row names).'
        )
    if 'public_operations' not in payload:
        raise SystemExit(
            f'run manifest {path} lacks embedded public_operations metadata. '
            'Re-run benches/drivers/bench.py so the ordered RELEASE operations '
            'are written into the run JSON; the summarizer does not re-infer '
            'domains or labels from raw benchmark names.'
        )
    return payload, _resolve_artifact_paths(payload, path)


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


def _parse_public_operations(raw: Any) -> list[PublicOpMeta]:
    if not isinstance(raw, list) or not raw:
        raise SystemExit(
            'public_operations must be a non-empty list of operation records'
        )
    ops: list[PublicOpMeta] = []
    seen_gometry: set[str] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise SystemExit(f'public_operations[{index}] is not an object')
        try:
            domain = str(item['domain'])
            label = str(item['label'])
            workload = str(item['workload'])
            suite = str(item['suite'])
            gometry = str(item['gometry'])
            competitor = item.get('competitor')
            competitor_label = item.get('competitor_label')
            footnotes_raw = item.get('footnotes') or ()
        except KeyError as exc:
            raise SystemExit(
                f'public_operations[{index}] missing field: {exc.args[0]}'
            ) from exc
        if competitor is not None:
            competitor = str(competitor)
        if competitor_label is not None:
            competitor_label = str(competitor_label)
        if competitor is None and competitor_label is not None:
            raise SystemExit(
                f'public_operations[{index}] ({gometry}): solo op has competitor_label'
            )
        if competitor is not None and not competitor_label:
            raise SystemExit(
                f'public_operations[{index}] ({gometry}): paired op missing '
                'competitor_label'
            )
        footnotes = tuple(str(f) for f in footnotes_raw)
        if gometry in seen_gometry:
            raise SystemExit(f'duplicate public gometry row: {gometry}')
        seen_gometry.add(gometry)
        ops.append(
            PublicOpMeta(
                domain=domain,
                label=label,
                workload=workload,
                suite=suite,
                gometry=gometry,
                competitor=competitor,
                competitor_label=competitor_label,
                footnotes=footnotes,
            )
        )
    return ops


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
    ra = _noise_radius(a, noise_band)
    rb = _noise_radius(b, noise_band)
    return a.mean - ra <= b.mean + rb and b.mean - rb <= a.mean + ra


def _geomean(values: list[float]) -> float | None:
    positive = [value for value in values if value > 0]
    if not positive:
        return None
    return math.exp(sum(math.log(value) for value in positive) / len(positive))


def format_speedup(row: OpRow) -> str:
    """Public speedup cell language (ratio = competitor / gometry)."""
    if row.solo or row.speedup is None:
        return '—'
    if row.tie:
        return '≈ parity'
    ratio = row.speedup
    if ratio >= 1.0:
        text = f'{ratio:.2f}× faster'  # noqa: RUF001 — intentional speedup glyph
    else:
        text = f'{ratio:.2f}× as fast'  # noqa: RUF001 — intentional speedup glyph
    if row.noisy:
        text += '*'
    return text


def _footnote_suffix(meta: PublicOpMeta, *, noisy: bool) -> str:
    markers = [
        FOOTNOTE_MARKERS[key]
        for key in ('geodesic', 'in_core', 'batched')
        if key in meta.footnotes
    ]
    if noisy or 'noisy' in meta.footnotes:
        markers.append(FOOTNOTE_MARKERS['noisy'])
    return ''.join(markers)


def row_label(row: OpRow) -> str:
    """Keep the full workload text (including any `/`); append footnote markers."""
    base = f'{row.meta.label} — {row.meta.workload}'
    suffix = _footnote_suffix(row.meta, noisy=row.noisy)
    return f'{base} {suffix}'.rstrip() if suffix else base


def format_competitor_cell(row: OpRow) -> str:
    if row.competitor is None:
        return '—'
    label = row.meta.competitor_label or 'competitor'
    return f'{label} · {_humanize_seconds(row.competitor.mean)}'


def _build_rows(
    operations: list[PublicOpMeta],
    stats: dict[str, BenchStats],
    *,
    noise_band: float,
) -> list[OpRow]:
    rows: list[OpRow] = []
    missing: list[str] = []
    for meta in operations:
        gometry = stats.get(meta.gometry)
        if gometry is None:
            missing.append(meta.gometry)
            continue
        competitor: BenchStats | None = None
        if meta.competitor is not None:
            competitor = stats.get(meta.competitor)
            if competitor is None:
                missing.append(meta.competitor)
                continue
        rel = _rel_stdev(gometry.mean, gometry.stdev)
        speedup: float | None = None
        tie = False
        if competitor is not None:
            if gometry.mean <= 0:
                raise SystemExit(f'non-positive gometry mean for {meta.gometry}')
            speedup = competitor.mean / gometry.mean
            tie = _stats_intervals_overlap(gometry, competitor, noise_band=noise_band)
        rows.append(
            OpRow(
                meta=meta,
                gometry=gometry,
                competitor=competitor,
                speedup=speedup,
                rel_stdev=rel,
                noisy=rel > noise_band,
                tie=tie,
            )
        )
    if missing:
        raise SystemExit(
            'run is missing declared public benchmark rows (or artifacts):\n'
            + '\n'.join(f'  - {name}' for name in missing)
        )
    # Reject extra public-named gometry rows not declared in the embedding.
    declared = {op.gometry for op in operations} | {
        op.competitor for op in operations if op.competitor
    }
    extras = sorted(
        name for name in stats if name.startswith('gometry.') and name not in declared
    )
    if extras:
        raise SystemExit(
            'run contains undeclared public gometry benchmark rows:\n'
            + '\n'.join(f'  - {name}' for name in extras)
        )
    return rows


def domain_stats(rows: list[OpRow]) -> dict[str, dict[str, Any]]:
    """Per-domain geomean and W/T/L, excluding solo rows from all speedup stats."""
    by_domain: dict[str, list[OpRow]] = {domain: [] for domain in DOMAIN_ORDER}
    for row in rows:
        by_domain.setdefault(row.meta.domain, []).append(row)

    out: dict[str, dict[str, Any]] = {}
    for domain in DOMAIN_ORDER:
        domain_rows = by_domain.get(domain, [])
        paired = [
            row for row in domain_rows if not row.solo and row.speedup is not None
        ]
        ratios = [row.speedup for row in paired if row.speedup is not None]
        wins = sum(1 for row in paired if not row.tie and (row.speedup or 0) > 1.0)
        losses = sum(1 for row in paired if not row.tie and (row.speedup or 0) < 1.0)
        parity = sum(1 for row in paired if row.tie)
        out[domain] = {
            'geomean': _geomean(ratios),
            'wins': wins,
            'parity': parity,
            'losses': losses,
            'n_paired': len(paired),
            'n_rows': len(domain_rows),
        }
    return out


def overall_geomean(domain_summaries: dict[str, dict[str, Any]]) -> float | None:
    """Equal-domain-weight overall: geomean of domain geomeans (not row-weighted)."""
    domain_means = [
        summary['geomean']
        for domain in DOMAIN_ORDER
        for summary in (domain_summaries.get(domain),)
        if summary is not None and summary['geomean'] is not None
    ]
    return _geomean(domain_means)


def _is_publishable_run(payload: dict[str, Any]) -> bool:
    return bool(payload.get('publishable'))


def _format_geomean(value: float | None) -> str:
    if value is None:
        return '—'
    return f'{value:.2f}×'  # noqa: RUF001 — intentional speedup glyph


def _header_table(
    domain_summaries: dict[str, dict[str, Any]], *, markdown: bool
) -> list[str]:
    headers = ['Domain', 'Geomean', 'Wins / parity / losses']
    body: list[list[str]] = []
    for domain in DOMAIN_ORDER:
        summary = domain_summaries.get(domain)
        if summary is None or summary['n_rows'] == 0:
            continue
        wtl = f'{summary["wins"]} / {summary["parity"]} / {summary["losses"]}'
        body.append([domain, _format_geomean(summary['geomean']), wtl])
    overall = overall_geomean(domain_summaries)
    total_w = sum(s['wins'] for s in domain_summaries.values())
    total_p = sum(s['parity'] for s in domain_summaries.values())
    total_l = sum(s['losses'] for s in domain_summaries.values())
    body.append([
        '**Overall, equal domain weight**'
        if markdown
        else 'Overall, equal domain weight',
        (f'**{_format_geomean(overall)}**' if markdown else _format_geomean(overall)),
        f'**{total_w} / {total_p} / {total_l}**'
        if markdown
        else f'{total_w} / {total_p} / {total_l}',
    ])
    if markdown:
        lines = [
            '| ' + ' | '.join(headers) + ' |',
            '|---|---:|---:|',
        ]
        lines.extend('| ' + ' | '.join(cells) + ' |' for cells in body)
        return lines
    widths = [len(h) for h in headers]
    for cells in body:
        for i, cell in enumerate(cells):
            # strip markdown bold for width
            plain = cell.replace('**', '')
            widths[i] = max(widths[i], len(plain))
    lines = [
        '  '.join(
            h.rjust(widths[i]) if i else h.ljust(widths[i])
            for i, h in enumerate(headers)
        ),
        '  '.join('-' * widths[i] for i in range(len(headers))),
    ]
    for cells in body:
        plain = [c.replace('**', '') for c in cells]
        lines.append(
            '  '.join(
                plain[i].rjust(widths[i]) if i else plain[i].ljust(widths[i])
                for i in range(len(headers))
            )
        )
    return lines


def _section_table(rows: list[OpRow], *, markdown: bool) -> list[str]:
    headers = ['Operation', 'gometry', 'competitor', 'Speedup']
    body = [
        [
            row_label(row),
            _humanize_seconds(row.gometry.mean),
            format_competitor_cell(row),
            format_speedup(row),
        ]
        for row in rows
    ]
    if markdown:
        lines = [
            '| ' + ' | '.join(headers) + ' |',
            '|---|---:|---:|---:|',
        ]
        lines.extend('| ' + ' | '.join(cells) + ' |' for cells in body)
        return lines
    widths = [len(h) for h in headers]
    for cells in body:
        for i, cell in enumerate(cells):
            widths[i] = max(widths[i], len(cell))
    numeric = {1, 2, 3}
    lines = [
        '  '.join(
            h.rjust(widths[i]) if i in numeric else h.ljust(widths[i])
            for i, h in enumerate(headers)
        ),
        '  '.join('-' * widths[i] for i in range(len(headers))),
    ]
    for cells in body:
        lines.append(
            '  '.join(
                cells[i].rjust(widths[i]) if i in numeric else cells[i].ljust(widths[i])
                for i in range(len(headers))
            )
        )
    return lines


def _used_footnotes(rows: list[OpRow]) -> list[str]:
    used: set[str] = set()
    for row in rows:
        for key in row.meta.footnotes:
            if key in FOOTNOTE_MARKERS:
                used.add(key)
        if row.noisy:
            used.add('noisy')
    return [
        next(line for line in FOOTNOTE_LEGEND if line.startswith(FOOTNOTE_MARKERS[key]))
        for key in ('geodesic', 'in_core', 'batched', 'noisy')
        if key in used
    ]


def render(
    rows: list[OpRow],
    *,
    markdown: bool,
    publishable: bool,
) -> list[str]:
    lines: list[str] = []
    if not publishable:
        if markdown:
            lines.append(f'> **{SMOKE_BANNER}**')
        else:
            lines.append(SMOKE_BANNER)
        lines.append('')

    summaries = domain_stats(rows)
    lines.extend(_header_table(summaries, markdown=markdown))
    lines.append('')

    by_domain: dict[str, list[OpRow]] = {}
    for row in rows:
        by_domain.setdefault(row.meta.domain, []).append(row)

    for domain in DOMAIN_ORDER:
        domain_rows = by_domain.get(domain)
        if not domain_rows:
            continue
        if markdown:
            lines.append(f'### {domain}')
            lines.append('')
        else:
            lines.append(domain)
            lines.append('=' * len(domain))
        lines.extend(_section_table(domain_rows, markdown=markdown))
        lines.append('')

    lines.extend(_used_footnotes(rows))
    while lines and lines[-1] == '':
        lines.pop()
    return lines


def summarize_payload(
    payload: dict[str, Any],
    stats: dict[str, BenchStats],
    *,
    noise_band: float = 0.03,
    markdown: bool = True,
) -> list[str]:
    operations = _parse_public_operations(payload.get('public_operations'))
    rows = _build_rows(operations, stats, noise_band=noise_band)
    return render(rows, markdown=markdown, publishable=_is_publishable_run(payload))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            'Summarize a gometry bench.py run JSON as a domain-grouped '
            'competitive table (manifest order; embedded public_operations).'
        )
    )
    parser.add_argument(
        'results',
        help='path to a <timestamp>-<profile>.json run manifest (or its directory)',
    )
    parser.add_argument(
        '--format', choices=['table', 'md'], default='table', help='output format'
    )
    parser.add_argument(
        '--noise-band',
        type=float,
        default=0.03,
        help='relative stdev threshold for the noisy-row marker',
    )
    args = parser.parse_args(argv)
    payload, artifact_paths = _resolve_run(args.results)
    stats = _load_all_stats(artifact_paths)
    if not stats and not payload.get('plan_only'):
        raise SystemExit('no pyperf benchmark rows found in the run artifacts')
    lines = summarize_payload(
        payload,
        stats,
        noise_band=args.noise_band,
        markdown=args.format == 'md',
    )
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
