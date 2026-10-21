"""Series identity and text alternatives for the cross-server benchmark figures."""

from __future__ import annotations

from typing import Any

from benchroom.plot import PALETTE

SERVER_COLORS = {
    'h2corn': PALETTE['blue'],
    'gunicorn': PALETTE['green'],
    'hypercorn': PALETTE['red'],
    'uvicorn': PALETTE['gold'],
    'grpcio': PALETTE['green'],
    'grpclib': PALETTE['gold'],
}
SERVER_MARKERS = {
    'h2corn': 'o',
    'gunicorn': 's',
    'hypercorn': 'D',
    'uvicorn': '^',
    'grpcio': 's',
    'grpclib': '^',
}
SERVER_LINESTYLES = {
    'h2corn': '-',
    'gunicorn': ':',
    'hypercorn': '-.',
    'uvicorn': '--',
    'grpcio': ':',
    'grpclib': '--',
}


def format_rate(value: float) -> str:
    return f'{value:,.0f}' if value >= 100 else f'{value:.2f}'


def alt_text(
    title: str,
    results: dict[str, Any],
    unit: str,
    failures: dict[str, Any] | None = None,
    labels: dict[str, str] | None = None,
) -> str:
    ordered = sorted(results.items(), key=lambda item: item[1]['rps'], reverse=True)
    parts = [
        f'{(labels or {}).get(name, name)} {format_rate(result["rps"])} {unit}'
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
    parts.extend(
        f'{(labels or {}).get(name, name)}: failed' for name in (failures or {})
    )
    return f'{title}. ' + ', '.join(parts) + '.'


def render_accessible_results(record: dict[str, Any]) -> str:
    """Render compact results with throughput classifications."""
    results = record['aggregate']
    failures = record.get('failures', {})
    websocket = record['type'] == 'ws'
    rpc = record.get('benchmark_family') == 'grpc'
    message_rate = rpc and record['rate_unit'] == 'messages/s'
    duration = 'session' if websocket else 'latency'
    headers = [
        'Server',
        (
            f'Median {record["rate_unit"]}'
            if rpc
            else record.get('rate_unit', 'Sessions/s' if websocket else 'Requests/s')
        ),
        'Blocks',
        *(['Median complete RPC/s'] if message_rate else []),
        *(['Median completed RPCs/trial'] if rpc else []),
        f'p50 {duration} (ms)',
        f'p99 {duration} (ms)',
        'PSS (MB)',
        'Throughput vs ' + record.get('reference_label', 'h2corn'),
    ]
    rows = [
        '| ' + ' | '.join(headers) + ' |',
        '| ' + ' | '.join('---' for _ in headers) + ' |',
    ]
    contrasts = record.get('paired_contrasts', {})
    names = record.get('requested_servers', list(results))
    for name in names:
        if name not in results:
            status = (
                'failed'
                if name in failures
                else 'not selected'
                if name in record.get('not_selected', {})
                else 'unsupported'
            )
            rows.append(
                '| ' + ' | '.join([name, status, *(['-'] * (len(headers) - 2))]) + ' |'
            )
            continue
        result = results[name]
        comparison = '-'
        if name in contrasts:
            contrast = contrasts[name]
            comparison = contrast.get('decision')
            if comparison is None:
                if interval := contrast.get('interval_95'):
                    low, high = interval
                    comparison = (
                        'slower'
                        if low > 1
                        else 'faster'
                        if high < 1
                        else 'inconclusive'
                    )
                else:
                    comparison = 'inconclusive'
            if contrast.get('decision_reason'):
                comparison += f' ({contrast["decision_reason"]})'
        percentiles = result['latency_percentiles']
        cells = [
            record.get('stack_labels', {}).get(name, name),
            format_rate(result['rps']),
            str(len(result['rps_samples'])),
            *([format_rate(result['rpc_rate'])] if message_rate else []),
            *([f'{result["completed"]:,.1f}'.removesuffix('.0')] if rpc else []),
            *(
                f'{percentiles[key] * 1000:,.3f}' if key in percentiles else '-'
                for key in ('p50', 'p99')
            ),
            f'{result["peak_memory_bytes"] / 1e6:,.1f}',
            comparison,
        ]
        rows.append('| ' + ' | '.join(cells) + ' |')
    return '\n'.join(rows) + '\n'
