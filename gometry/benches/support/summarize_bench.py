"""Normalize accepted pyperf artifacts into compact paired benchmark evidence.

Each pyperf run is an independent process/block.  Values inside one process
are reduced to one process median before ratios or bootstrap intervals are
computed.  This keeps pyperf's repeated values from masquerading as additional
independent observations.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

_BENCHES = Path(__file__).resolve().parents[1]
GOMETRY_ROOT = _BENCHES.parent

DOMAIN_ORDER = (
    'Array construction & I/O',
    'Geometry',
    'CRS & geodesy',
    'Discrete global grids',
    'Spatial index',
)
EQUIVALENCE_BAND = 0.03
BOOTSTRAP_SAMPLES = 10_000
DEFAULT_BOOTSTRAP_SEED = 20_260_824

SMOKE_BANNER = 'Smoke/debug run — not publishable release evidence.'


@dataclass(frozen=True, slots=True)
class ProcessSample:
    """One independent pyperf process median for one raw benchmark row."""

    artifact: str
    process_index: int
    values: tuple[float, ...]
    median: float
    pair_id: str | None = None
    lead: str | None = None

    @property
    def block_id(self) -> str:
        return f'{self.artifact}#process-{self.process_index}'


@dataclass(frozen=True, slots=True)
class BenchStats:
    name: str
    mean: float
    median: float
    stdev: float
    nrun: int
    samples: int
    processes: tuple[ProcessSample, ...] = ()


@dataclass(frozen=True, slots=True)
class PublicOpMeta:
    domain: str
    label: str
    workload: str
    suite: str
    gometry: str
    competitor: str
    competitor_label: str
    footnotes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PairedBlock:
    artifact: str
    process_index: int
    pair_id: str | None
    lead: str | None
    gometry_median_seconds: float
    competitor_median_seconds: float
    ratio: float

    @property
    def block_id(self) -> str:
        return f'{self.artifact}#process-{self.process_index}'


@dataclass(slots=True)
class OpRow:
    meta: PublicOpMeta
    gometry: BenchStats
    competitor: BenchStats
    ratio: float
    ratio_ci95: tuple[float, float]
    blocks: tuple[PairedBlock, ...]
    status: str

    @property
    def speedup(self) -> float:
        """Compatibility alias: ratio is competitor seconds / gometry seconds."""
        return self.ratio

    @property
    def tie(self) -> bool:
        return self.status == 'similar'

    @property
    def noisy(self) -> bool:
        return self.status == 'inconclusive'


def _manifest_payload(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _resolve_artifact_paths(payload: dict[str, Any], manifest_path: Path) -> list[Path]:
    artifacts = payload.get('artifacts')
    if not isinstance(artifacts, list):
        return []
    resolved: list[Path] = []
    for artifact in artifacts:
        if not isinstance(artifact, str):
            raise SystemExit(f'run manifest {manifest_path} has a non-string artifact entry')
        candidate = Path(artifact)
        if not candidate.is_file():
            candidate = manifest_path.parent / candidate.name
        if not candidate.is_file():
            raise SystemExit(
                f'run manifest {manifest_path} references a missing artifact: {artifact}'
            )
        resolved.append(candidate)
    return resolved


def _resolve_run(pattern: str) -> tuple[dict[str, Any], list[Path]]:
    path = Path(pattern)
    if path.is_dir():
        manifests = [
            candidate for candidate in sorted(path.glob('*.json'))
            if (_manifest_payload(candidate) or {}).get('profile')
        ]
        if len(manifests) > 1:
            raise SystemExit(
                f'{path} contains multiple benchmark runs; pass one exact run manifest'
            )
        if not manifests:
            raise SystemExit(f'{path} has no bench.py run manifest')
        path = manifests[0]
    if not path.exists():
        matched = sorted(Path(match) for match in glob(pattern))
        if len(matched) == 1 and matched[0].suffix == '.json':
            path = matched[0]
        elif matched:
            raise SystemExit(
                'summarize_bench requires one run manifest JSON; '
                f'matched {len(matched)} paths'
            )
        else:
            raise SystemExit(f'no benchmark result files matched: {pattern}')
    payload = _manifest_payload(path)
    if payload is None or 'profile' not in payload:
        raise SystemExit(f'{path} is not a bench.py run manifest')
    if 'public_operations' not in payload:
        raise SystemExit(f'run manifest {path} lacks public_operations metadata')
    return payload, _resolve_artifact_paths(payload, path)


def _parse_public_operations(raw: Any) -> list[PublicOpMeta]:
    if not isinstance(raw, list) or not raw:
        raise SystemExit('public_operations must be a non-empty list')
    operations: list[PublicOpMeta] = []
    seen: set[str] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise SystemExit(f'public_operations[{index}] is not an object')
        try:
            domain = str(item['domain'])
            label = str(item['label'])
            workload = str(item['workload'])
            suite = str(item['suite'])
            gometry = str(item['gometry'])
            competitor = str(item['competitor'])
            competitor_label = str(item['competitor_label'])
        except KeyError as exc:
            raise SystemExit(
                f'public_operations[{index}] missing field: {exc.args[0]}'
            ) from exc
        if gometry in seen:
            raise SystemExit(f'duplicate public gometry row: {gometry}')
        if not competitor or not competitor_label:
            raise SystemExit(f'public_operations[{index}] must be fully paired')
        seen.add(gometry)
        footnotes = tuple(str(value) for value in (item.get('footnotes') or ()))
        operations.append(
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
    return operations


def _metadata_for(
    payload: dict[str, Any], benchmark: dict[str, Any], run: dict[str, Any]
) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for source in (
        payload.get('metadata'),
        benchmark.get('metadata'),
        run.get('metadata'),
    ):
        if isinstance(source, dict):
            metadata.update(source)
    return metadata


def _load_all_stats(paths: list[Path]) -> dict[str, BenchStats]:
    """Load one process median per pyperf run, retaining artifact identity."""
    processes: dict[str, list[ProcessSample]] = {}
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError) as exc:
            raise SystemExit(f'invalid pyperf artifact {path}: {exc}') from exc
        if not isinstance(payload, dict) or not isinstance(payload.get('benchmarks'), list):
            continue
        for benchmark in payload['benchmarks']:
            if not isinstance(benchmark, dict):
                continue
            common = benchmark.get('metadata')
            name = common.get('name') if isinstance(common, dict) else None
            if not isinstance(name, str):
                name = payload.get('metadata', {}).get('name') if isinstance(payload.get('metadata'), dict) else None
            if not isinstance(name, str):
                continue
            runs = benchmark.get('runs')
            if not isinstance(runs, list):
                values = benchmark.get('values')
                runs = [{'values': values}] if isinstance(values, list) else []
            for process_index, run in enumerate(runs):
                if not isinstance(run, dict):
                    continue
                values = run.get('values')
                if not isinstance(values, list) or not values:
                    continue
                numeric = tuple(float(value) for value in values)
                if not all(math.isfinite(value) and value > 0.0 for value in numeric):
                    raise SystemExit(f'{path} contains invalid values for {name}')
                metadata = _metadata_for(payload, benchmark, run)
                processes.setdefault(name, []).append(
                    ProcessSample(
                        artifact=str(path),
                        process_index=process_index,
                        values=numeric,
                        median=statistics.median(numeric),
                        pair_id=metadata.get('gometry_pair_id'),
                        lead=metadata.get('gometry_pair_lead'),
                    )
                )
    result: dict[str, BenchStats] = {}
    for name, rows in processes.items():
        medians = [row.median for row in rows]
        result[name] = BenchStats(
            name=name,
            mean=statistics.fmean(medians),
            median=statistics.median(medians),
            stdev=statistics.stdev(medians) if len(medians) > 1 else 0.0,
            nrun=len(rows),
            samples=sum(len(row.values) for row in rows),
            processes=tuple(rows),
        )
    return result


def _percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        raise ValueError('percentile requires values')
    ordered = sorted(values)
    position = quantile * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _median_process_values(values: Iterable[float | Sequence[float]]) -> list[float]:
    medians: list[float] = []
    for value in values:
        if isinstance(value, (int, float)):
            medians.append(float(value))
        else:
            samples = tuple(float(item) for item in value)
            if not samples:
                raise ValueError('process values cannot be empty')
            medians.append(statistics.median(samples))
    return medians


def paired_process_ratios(
    gometry_processes: Sequence[float | Sequence[float]],
    competitor_processes: Sequence[float | Sequence[float]],
) -> list[float]:
    """Return one competitor/gometry ratio for each independent process."""
    gometry = _median_process_values(gometry_processes)
    competitor = _median_process_values(competitor_processes)
    if len(gometry) != len(competitor):
        raise ValueError('paired process samples must have equal counts')
    if not gometry:
        raise ValueError('paired process samples cannot be empty')
    return [right / left for left, right in zip(gometry, competitor, strict=True)]


def bootstrap_paired_ratio_ci(
    ratios: Sequence[float],
    seed: int = DEFAULT_BOOTSTRAP_SEED,
    *,
    samples: int = BOOTSTRAP_SAMPLES,
) -> tuple[float, float]:
    """Deterministic percentile bootstrap over independent process ratios."""
    if not ratios:
        raise ValueError('paired bootstrap requires at least one ratio')
    rng = random.Random(seed)
    medians = [
        statistics.median(ratios[rng.randrange(len(ratios))] for _ in ratios)
        for _ in range(samples)
    ]
    return _percentile(medians, 0.025), _percentile(medians, 0.975)


def paired_ratio_ci(
    gometry_processes: Sequence[float | Sequence[float]],
    competitor_processes: Sequence[float | Sequence[float]],
    seed: int = DEFAULT_BOOTSTRAP_SEED,
    *,
    samples: int = BOOTSTRAP_SAMPLES,
) -> tuple[float, float]:
    return bootstrap_paired_ratio_ci(
        paired_process_ratios(gometry_processes, competitor_processes),
        seed,
        samples=samples,
    )


def classify_ratio(
    ratio_ci95: tuple[float, float], noise_band: float = EQUIVALENCE_BAND
) -> str:
    """Classify only intervals that clear or fit the predeclared noise band."""
    low, high = ratio_ci95
    lower_equivalent = 1.0 - noise_band
    upper_equivalent = 1.0 + noise_band
    if low > upper_equivalent:
        return 'faster'
    if high < lower_equivalent:
        return 'slower'
    if low >= lower_equivalent and high <= upper_equivalent:
        return 'similar'
    return 'inconclusive'


def _paired_blocks(gometry: BenchStats, competitor: BenchStats) -> tuple[PairedBlock, ...]:
    left = {(sample.artifact, sample.process_index): sample for sample in gometry.processes}
    right = {(sample.artifact, sample.process_index): sample for sample in competitor.processes}
    keys = sorted(left.keys() & right.keys())
    if not keys:
        # Synthetic callers without raw process identity still receive a
        # deterministic one-block statistic, while real artifacts never use it.
        return (
            PairedBlock(
                artifact='synthetic',
                process_index=0,
                pair_id=None,
                lead=None,
                gometry_median_seconds=gometry.median,
                competitor_median_seconds=competitor.median,
                ratio=competitor.median / gometry.median,
            ),
        )
    return tuple(
        PairedBlock(
            artifact=key[0],
            process_index=key[1],
            pair_id=left[key].pair_id or right[key].pair_id,
            lead=left[key].lead or right[key].lead,
            gometry_median_seconds=left[key].median,
            competitor_median_seconds=right[key].median,
            ratio=right[key].median / left[key].median,
        )
        for key in keys
    )


def _build_rows(
    operations: list[PublicOpMeta],
    stats: dict[str, BenchStats],
    *,
    noise_band: float,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> list[OpRow]:
    rows: list[OpRow] = []
    missing: list[str] = []
    for meta in operations:
        gometry = stats.get(meta.gometry)
        competitor = stats.get(meta.competitor)
        if gometry is None:
            missing.append(meta.gometry)
        if competitor is None:
            missing.append(meta.competitor)
        if gometry is None or competitor is None:
            continue
        if gometry.median <= 0.0 or competitor.median <= 0.0:
            raise SystemExit(f'non-positive median for {meta.gometry}')
        blocks = _paired_blocks(gometry, competitor)
        ratios = [block.ratio for block in blocks]
        ratio = statistics.median(ratios)
        ratio_ci95 = bootstrap_paired_ratio_ci(ratios, seed, samples=BOOTSTRAP_SAMPLES)
        rows.append(
            OpRow(
                meta=meta,
                gometry=gometry,
                competitor=competitor,
                ratio=ratio,
                ratio_ci95=ratio_ci95,
                blocks=blocks,
                status=classify_ratio(ratio_ci95, noise_band),
            )
        )
    if missing:
        raise SystemExit(
            'run is missing declared public benchmark rows (or artifacts):\n'
            + '\n'.join(f'  - {name}' for name in missing)
        )
    declared = {op.gometry for op in operations} | {op.competitor for op in operations}
    extras = sorted(name for name in stats if name not in declared)
    if extras:
        raise SystemExit(
            'run contains undeclared public benchmark rows:\n'
            + '\n'.join(f'  - {name}' for name in extras)
        )
    return rows


def _humanize_seconds(value: float) -> str:
    if value >= 0.001:
        return f'{value * 1000.0:.2f} ms'
    if value >= 1e-6:
        return f'{value * 1e6:.2f} us'
    return f'{value * 1e9:.1f} ns'


def format_speedup(row: OpRow) -> str:
    """Accessible relative language; no color or interval column is required."""
    if row.status == 'similar':
        label = 'Similar'
    elif row.status == 'inconclusive':
        label = 'Inconclusive'
    elif row.ratio >= 1.0:
        label = f'{row.ratio:.2f}× faster'  # noqa: RUF001 — product glyph
    else:
        label = f'{1.0 / row.ratio:.2f}× slower'  # noqa: RUF001 — product glyph
    return f'<span class="benchmark-status benchmark-{row.status}">{label}</span>'


def format_competitor_cell(row: OpRow) -> str:
    return f'{row.meta.competitor_label} · {_humanize_seconds(row.competitor.median)}'


def row_label(row: OpRow) -> str:
    marker = _footnote_suffix(row.meta)
    text = f'{row.meta.label} — {row.meta.workload}'
    return f'{text} {marker}'.rstrip() if marker else text


def _footnote_suffix(meta: PublicOpMeta) -> str:
    markers = []
    if 'geodesic' in meta.footnotes:
        markers.append('†')
    if 'in_core' in meta.footnotes:
        markers.append('‡')
    return ''.join(markers)


FOOTNOTE_LEGEND = (
    '† geodesic/ellipsoidal metric.',
    '‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.',
)


def _section_table(rows: list[OpRow]) -> list[str]:
    headers = ['API / input', 'gometry', 'Comparator', 'Relative']
    body = [
        [
            row_label(row),
            _humanize_seconds(row.gometry.median),
            format_competitor_cell(row),
            format_speedup(row),
        ]
        for row in rows
    ]
    lines = [
        '| ' + ' | '.join(headers) + ' |',
        '|---|---:|---:|---:|',
    ]
    lines.extend('| ' + ' | '.join(cells) + ' |' for cells in body)
    return lines


def _used_footnotes(rows: Iterable[OpRow]) -> list[str]:
    used = {key for row in rows for key in row.meta.footnotes}
    return [
        line for line in FOOTNOTE_LEGEND
        if (line.startswith('†') and 'geodesic' in used)
        or (line.startswith('‡') and 'in_core' in used)
    ]


def render(rows: list[OpRow], *, markdown: bool, publishable: bool) -> list[str]:
    del markdown  # The release artifact is Markdown; table text is accessible in both uses.
    lines: list[str] = []
    if not publishable:
        lines.extend((f'> **{SMOKE_BANNER}**', ''))
    by_domain: dict[str, list[OpRow]] = {}
    for row in rows:
        by_domain.setdefault(row.meta.domain, []).append(row)
    for domain in DOMAIN_ORDER:
        domain_rows = by_domain.get(domain)
        if not domain_rows:
            continue
        lines.extend((
            f'### {domain}',
            '',
            f'<div class="benchmark-table-scroll" role="region" tabindex="0" '
            f'aria-label="{domain} benchmark results">',
            *_section_table(domain_rows),
            '</div>',
            '',
        ))
    lines.extend(_used_footnotes(rows))
    while lines and not lines[-1]:
        lines.pop()
    return lines


def normalized_summary_payload(
    payload: dict[str, Any],
    stats: dict[str, BenchStats],
    *,
    noise_band: float = EQUIVALENCE_BAND,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> dict[str, Any]:
    operations = _parse_public_operations(payload.get('public_operations'))
    rows = _build_rows(operations, stats, noise_band=noise_band, seed=seed)
    return {
        'schema_version': 2,
        'profile': payload.get('profile'),
        'publishable': bool(payload.get('publishable')),
        'equivalence_noise_band': noise_band,
        'bootstrap_seed': seed,
        'operations': [
            {
                'domain': row.meta.domain,
                'label': row.meta.label,
                'workload': row.meta.workload,
                'gometry': row.meta.gometry,
                'competitor': row.meta.competitor,
                'competitor_label': row.meta.competitor_label,
                'gometry_median_seconds': row.gometry.median,
                'competitor_median_seconds': row.competitor.median,
                'ratio_competitor_over_gometry': row.ratio,
                'ratio_ci95': list(row.ratio_ci95),
                'status': row.status,
                'blocks': [
                    {
                        'block_id': block.block_id,
                        'artifact': block.artifact,
                        'process_index': block.process_index,
                        'pair_id': block.pair_id,
                        'lead': block.lead,
                        'gometry_median_seconds': block.gometry_median_seconds,
                        'competitor_median_seconds': block.competitor_median_seconds,
                        'ratio_competitor_over_gometry': block.ratio,
                    }
                    for block in row.blocks
                ],
            }
            for row in rows
        ],
    }


def summarize_payload(
    payload: dict[str, Any],
    stats: dict[str, BenchStats],
    *,
    noise_band: float = EQUIVALENCE_BAND,
    markdown: bool = True,
) -> list[str]:
    operations = _parse_public_operations(payload.get('public_operations'))
    rows = _build_rows(operations, stats, noise_band=noise_band)
    return render(rows, markdown=markdown, publishable=bool(payload.get('publishable')))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', help='one bench.py run manifest JSON or its directory')
    parser.add_argument('--format', choices=['table', 'md'], default='table')
    parser.add_argument('--noise-band', type=float, default=EQUIVALENCE_BAND)
    args = parser.parse_args(argv)
    payload, artifacts = _resolve_run(args.results)
    stats = _load_all_stats(artifacts)
    if not stats and not payload.get('plan_only'):
        raise SystemExit('no pyperf benchmark rows found in run artifacts')
    print('\n'.join(summarize_payload(
        payload, stats, noise_band=args.noise_band, markdown=args.format == 'md'
    )))


if __name__ == '__main__':
    main()
