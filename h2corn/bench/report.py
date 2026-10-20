"""Series identity and text alternatives for the cross-server benchmark figures."""

from __future__ import annotations

from typing import Any

from benchroom.plot import PALETTE

SERVER_COLORS = {
    'h2corn': PALETTE['blue'],
    'gunicorn': PALETTE['green'],
    'hypercorn': PALETTE['red'],
    'uvicorn': PALETTE['gold'],
}
SERVER_MARKERS = {'h2corn': 'o', 'gunicorn': 's', 'hypercorn': 'D', 'uvicorn': '^'}
SERVER_LINESTYLES = {'h2corn': '-', 'gunicorn': ':', 'hypercorn': '-.', 'uvicorn': '--'}


def alt_text(title: str, results: dict[str, Any], unit: str) -> str:
    ordered = sorted(results.items(), key=lambda item: item[1]['rps'], reverse=True)
    parts = [
        f'{name} {result["rps"]:,.0f} {unit}'
        + (
            f' p99 {result["latency_percentiles"]["p99"] * 1000:.1f}ms'
            if 'p99' in result['latency_percentiles']
            else ''
        )
        + (
            f' peak memory (PSS) {result["peak_memory_bytes"] / 1e6:.1f} MB'
            if 'peak_memory_bytes' in result
            else ''
        )
        for name, result in ordered
    ]
    return f'{title}. ' + ', '.join(parts) + '.'


def render_accessible_results(record: dict[str, Any]) -> str:
    """Render the measured aggregate as a conclusion and an exact data table."""
    results = record['aggregate']
    websocket = record['type'] == 'ws'
    unit = 'sessions/s' if websocket else 'requests/s'
    ordered = sorted(results.items(), key=lambda item: item[1]['rps'], reverse=True)
    conclusion = (
        f'{ordered[0][0]} has the highest observed median rate. '
        'Paired comparisons below describe rate ratios within the same trial blocks; '
        'an interval including 1 does not establish equivalence.'
    )
    if len(ordered) == 1:
        conclusion = (
            f'Only {ordered[0][0]} produced an eligible result, so no cross-server '
            'comparison is available.'
        )

    headers = ['Server (version)', 'Measure', 'Value', 'Observed rate range']
    percentile_names = sorted(
        {name for result in results.values() for name in result['latency_percentiles']},
        key=lambda name: float(name.removeprefix('p')),
    )
    rows = [
        '| ' + ' | '.join(headers) + ' |',
        '| ' + ' | '.join('---' for _ in headers) + ' |',
    ]
    versions = record['package_versions']
    for name, result in ordered:
        low, high = result['rps_range']
        server = f'{name} ({versions.get(name) or "unknown"})'
        duration_name = 'session duration' if websocket else 'latency'
        measurements = [
            ('Measured trials', str(len(result['rps_samples'])), '-'),
            (
                f'Median rate ({unit})',
                f'{result["rps"]:,.0f}',
                f'{low:,.0f}-{high:,.0f}',
            ),
            (
                'Maximum sampled PSS (MB)',
                f'{result["peak_memory_bytes"] / 1e6:,.1f}',
                '-',
            ),
            *(
                (
                    f'Median {percentile} {duration_name} (ms)',
                    f'{result["latency_percentiles"][percentile] * 1000:,.3f}',
                    '-',
                )
                for percentile in percentile_names
                if percentile in result['latency_percentiles']
            ),
        ]
        rows.extend(
            f'| {server} | {measure} | {value} | {range_} |'
            for measure, value, range_ in measurements
        )

    profile = record['network_profile']
    environment = record['system_summary'].replace('\n', '; ')
    measured_rtt = record.get('measured_rtt_ms')
    if measured_rtt is not None:
        profile += f' ({measured_rtt:.1f} ms measured RTT)'
    text = [
        f'**Conclusion:** {conclusion}',
        '',
        f'**Workload:** {record.get("workload", record["scenario"])}.',
        '',
        f'**Environment:** {environment}; {profile}; '
        f'{record["duration"]} load after {record["warmup_duration"]} warmup.',
        '',
        *rows,
        '',
        'Rates are medians across cold-start trials; the observed rate range is '
        'the trial minimum-maximum, not a confidence interval. A dash means no '
        'range is reported for that measure. Latency percentiles are medians '
        'across trials and have no interval shown. Peak proportional set size '
        '(PSS) is the maximum sampled value across the supervisor and workers; '
        'MB uses decimal units.',
    ]
    contrasts = record.get('paired_contrasts', {})
    if contrasts:
        text.extend((
            '',
            '| Paired rate comparison | Median h2corn / competitor | 95% interval |',
            '| --- | --- | --- |',
        ))
        for name, contrast in contrasts.items():
            low, high = contrast['interval_95']
            text.append(
                f'| h2corn / {name} | {contrast["h2corn_over_competitor"]:.3f} '
                f'| {low:.3f}-{high:.3f} |'
            )
        text.extend((
            '',
            'Intervals use a percentile bootstrap of complete paired trial blocks. '
            'Each ratio compares the same block, with rotated server order. '
            'These are per-comparison intervals, without a simultaneous ranking claim. '
            'Load is closed-loop: clients wait for a response before issuing more work, '
            'so the latency percentiles are not an open-arrival service-level estimate.',
        ))
    if record['excluded']:
        text.extend((
            '',
            '**Excluded servers:** '
            + '; '.join(
                f'{name}: {reason}'
                for name, reason in sorted(record['excluded'].items())
            ),
        ))
    return '\n'.join(text) + '\n'
