"""Text alternatives for the cross-server benchmark figures."""

from __future__ import annotations

from typing import Any


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
    winner, _ = ordered[0]
    half_width = record['leader_half_width']
    target = record['target_half_width']
    if len(ordered) == 1:
        conclusion = (
            f'Only {winner} produced an eligible result, so no cross-server '
            'comparison is available.'
        )
    elif record['leader_separated']:
        conclusion = (
            f'The 95% bootstrap intervals separate {winner} from the runner-up. '
            f'The leader interval half-width is ±{half_width:.1%} of its median '
            f'(target ±{target:.0%}).'
        )
    else:
        conclusion = (
            'The 95% bootstrap intervals do not separate a leader from the '
            'runner-up; the largest median is descriptive, not a winner.'
        )

    headers = [
        'Server',
        'Version',
        'Measured trials',
        f'Median {unit}',
        f'Observed range ({unit})',
        'Maximum sampled PSS (MB)',
    ]
    percentile_names = sorted(
        {name for result in results.values() for name in result['latency_percentiles']},
        key=lambda name: float(name.removeprefix('p')),
    )
    duration_name = 'session duration' if websocket else 'latency'
    headers.extend(f'Median {name} {duration_name} (ms)' for name in percentile_names)
    rows = [
        '| ' + ' | '.join(headers) + ' |',
        '| ' + ' | '.join('---' for _ in headers) + ' |',
    ]
    versions = record['package_versions']
    for name, result in ordered:
        low, high = result['rps_range']
        row = [
            name,
            str(versions.get(name) or 'unknown'),
            str(len(result['rps_samples'])),
            f'{result["rps"]:,.0f}',
            f'{low:,.0f}-{high:,.0f}',
            f'{result["peak_memory_bytes"] / 1e6:,.1f}',
        ]
        row.extend(
            f'{result["latency_percentiles"][percentile] * 1000:,.3f}'
            if percentile in result['latency_percentiles']
            else '—'
            for percentile in percentile_names
        )
        rows.append('| ' + ' | '.join(row) + ' |')

    profile = record['network_profile']
    environment = record['system_summary'].replace('\n', '; ')
    measured_rtt = record.get('measured_rtt_ms')
    if measured_rtt is not None:
        profile += f' ({measured_rtt:.1f} ms measured RTT)'
    text = [
        f'**Conclusion:** {conclusion}',
        '',
        f'**Environment:** {environment}; {profile}; '
        f'{record["duration"]} load after {record["warmup_duration"]} warmup.',
        '',
        *rows,
        '',
        'Rates are medians across cold-start trials; the range is the observed '
        'minimum-maximum, not a confidence interval. Latency percentiles are '
        'medians across trials and have no interval shown. Peak proportional '
        'set size (PSS) is the maximum sampled value across the supervisor and '
        'workers; MB uses decimal units.',
    ]
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
