import argparse
import importlib.metadata
import json
import os
import platform
import shutil
import socket
import subprocess
import time
import traceback
import urllib.request
from contextlib import contextmanager
from pathlib import Path

import matplotlib as mpl

mpl.use('Agg')
import matplotlib.pyplot as plt

REQUESTS = 200000
CONCURRENCY = 100
STREAMING_CONCURRENCY = 1000
HOST = '127.0.0.1'
PORT = 8000
UNIX_SOCKET_PATH = Path('bench/results/benchmark.sock')

SERVERS = {
    # h2corn: supports h1, h2, ws
    'h2corn': [
        'h2corn',
        'bench.bench_app:app',
    ],
    # uvicorn: supports h1, ws (no h2)
    'uvicorn': [
        'uvicorn',
        'bench.bench_app:app',
    ],
    # hypercorn: supports h1, h2, ws
    'hypercorn': [
        'hypercorn',
        'bench.bench_app:app',
        '--access-logfile',
        '-',
    ],
    # gunicorn (uvicorn workers): supports h1, ws (no h2)
    'gunicorn': [
        'gunicorn',
        'bench.bench_app:app',
        '-k',
        'uvicorn.workers.UvicornWorker',
        '--access-logfile',
        '-',
    ],
}

SERVER_COLORS = {
    'h2corn': '#4477AA',
    'gunicorn': '#228833',
    'hypercorn': '#CC3311',
    'uvicorn': '#B39B00',
}

SERVER_MARKERS = {
    'h2corn': 'o',
    'gunicorn': 's',
    'hypercorn': 'D',
    'uvicorn': '^',
}

SERVER_LINESTYLES = {
    'h2corn': '-',
    'gunicorn': ':',
    'hypercorn': '-.',
    'uvicorn': '--',
}

FALLBACK_COLOR = '#4C4C4C'


def get_bind_args(server_name, socket_path=None):
    if server_name == 'uvicorn':
        if socket_path is None:
            return ['--host', HOST, '--port', str(PORT)]
        return ['--uds', str(socket_path)]

    bind_target = f'{HOST}:{PORT}' if socket_path is None else f'unix:{socket_path}'
    if server_name in {'h2corn', 'hypercorn', 'gunicorn'}:
        return ['-b', bind_target]

    raise ValueError(f'unsupported server binding mode for {server_name}')


def get_server_command(server_name, workers, socket_path=None):
    cmd = SERVERS[server_name].copy()
    cmd.extend(get_bind_args(server_name, socket_path))
    if server_name == 'h2corn':
        cmd.extend(['-w', str(workers)])
    elif server_name in {'uvicorn', 'hypercorn'}:
        cmd.extend(['--workers', str(workers)])
    elif server_name == 'gunicorn':
        cmd.extend(['-w', str(workers)])
    return cmd


def _wait_for_unix_server(process, socket_path):
    request = b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n'
    for _ in range(50):
        if process.poll() is not None:
            return False
        if not socket_path.exists():
            time.sleep(0.1)
            continue
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(1)
                client.connect(str(socket_path))
                client.sendall(request)
                response = client.recv(64)
                if response.startswith(b'HTTP/1.1 '):
                    return True
        except OSError:
            time.sleep(0.1)
            continue
        time.sleep(0.1)
    return False


def wait_for_server(process, socket_path=None):
    if socket_path is not None:
        return _wait_for_unix_server(process, socket_path)
    for _ in range(50):
        if process.poll() is not None:
            return False
        try:
            req = urllib.request.Request(f'http://{HOST}:{PORT}/')
            with urllib.request.urlopen(req, timeout=1) as response:  # noqa: S310
                if response.status == 200:
                    return True
        except Exception:
            time.sleep(0.1)
    return False


def get_system_info():
    kernel = f'{platform.system()} {platform.release()}'.strip()
    machine = platform.machine()
    summary = f'Python {platform.python_version()} | {kernel} | {machine}'

    cpu_model = None
    cpuinfo = Path('/proc/cpuinfo')
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding='utf-8', errors='ignore').splitlines():
            if line.startswith('model name'):
                cpu_model = line.partition(':')[2].strip()
                break
    elif shutil.which('sysctl'):
        result = subprocess.run(
            ['sysctl', '-n', 'machdep.cpu.brand_string'],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            cpu_model = result.stdout.strip()

    if cpu_model is None:
        raise RuntimeError('failed to determine CPU model for benchmark plot metadata')

    return f'{summary}\nCPU: {cpu_model}'


def get_versions():
    return {
        pkg: importlib.metadata.version(pkg)
        for pkg in ['h2corn', 'uvicorn', 'hypercorn', 'gunicorn']
    }


def run_oha(
    url,
    http_version='1.1',
    method='GET',
    body=None,
    content_type=None,
    requests=REQUESTS,
    concurrency=CONCURRENCY,
    unix_socket=None,
):
    cmd = [
        'oha',
        '-n',
        str(requests),
        '-c',
        str(concurrency),
        '--output-format',
        'json',
        '-m',
        method,
    ]
    if http_version == '2':
        cmd.append('--http2')
    if body is not None:
        cmd.extend(['-d', body])
    if content_type is not None:
        cmd.extend(['-T', content_type])
    if unix_socket is not None:
        cmd.extend(['--unix-socket', str(unix_socket)])
    cmd.append(url)

    env = os.environ.copy()
    env.pop('NO_COLOR', None)
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    if result.returncode != 0:
        print(f'oha failed: {result.stderr}')
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def run_k6(url, requests=REQUESTS, concurrency=CONCURRENCY):
    k6_script = Path('bench/k6/ws.js')
    summary_file = Path('bench/results/k6_summary.json')
    if summary_file.exists():
        summary_file.unlink()

    cmd = [
        'k6',
        'run',
        '--iterations',
        str(requests),
        '--vus',
        str(concurrency),
        '--summary-export',
        str(summary_file),
        '-e',
        f'WS_URL={url}',
        str(k6_script),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        print(f'k6 failed: {result.stderr}')
        return None
    if not summary_file.exists():
        return None
    with open(summary_file) as f:
        return json.load(f)


@contextmanager
def running_server(server_name, workers, socket_path=None):
    if socket_path is not None:
        socket_path.unlink(missing_ok=True)
    cmd = get_server_command(server_name, workers, socket_path)
    print(f'Starting {" ".join(cmd)}')
    process = None
    try:
        process = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        if not wait_for_server(process, socket_path):
            raise RuntimeError(f'Failed to start {server_name} with {workers} workers')
        yield process
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if socket_path is not None:
            socket_path.unlink(missing_ok=True)


def extract_oha_metrics(oha_json):
    if not oha_json:
        return None

    rps = oha_json.get('summary', {}).get('requestsPerSec', 0)
    percentiles = oha_json.get('latencyPercentiles', {})

    mapped_pcts = {}
    for p in ['p50', 'p75', 'p90', 'p95', 'p99']:
        val = percentiles.get(p)
        if val is not None:
            mapped_pcts[p] = val

    return {'rps': rps, 'latency_percentiles': mapped_pcts}


def extract_k6_metrics(k6_json):
    if not k6_json:
        return None

    metrics = k6_json.get('metrics', {})
    if not metrics:
        return None

    # K6 RPS (iterations per second)
    rps_obj = metrics.get('iterations', {})
    rps = rps_obj.get('rate', 0) if isinstance(rps_obj, dict) else 0

    # K6 Session Duration
    ws_duration = metrics.get('ws_session_duration', {})
    if not isinstance(ws_duration, dict):
        ws_duration = {}

    mapped_pcts = {}
    for source, target in (
        ('med', 'p50'),
        ('p(90)', 'p90'),
        ('p(95)', 'p95'),
        ('p(99)', 'p99'),
    ):
        value = ws_duration.get(source)
        if value is not None:
            mapped_pcts[target] = value / 1000.0

    return {'rps': rps, 'latency_percentiles': mapped_pcts}


def extract_metrics(raw, config_type):
    if config_type in {'h1', 'h2', 'h1_file', 'h2_file'}:
        return extract_oha_metrics(raw)
    elif config_type == 'ws':
        return extract_k6_metrics(raw)
    elif config_type in {'h1_stream', 'h2_stream', 'h1_uds'}:
        return extract_oha_metrics(raw)
    return None


def metric_p99_ms(metrics):
    p99 = metrics.get('latency_percentiles', {}).get('p99')
    if p99 is None:
        return None
    return p99 * 1000.0


def format_server_summary(name, metrics, version=None):
    summary = f'{name}'
    if version is not None:
        summary += f' {version}'
    summary += f': {metrics["rps"]:,.2f} RPS'
    p99_ms = metric_p99_ms(metrics)
    if p99_ms is not None:
        summary += f', p99 {p99_ms:.1f} ms'
    return summary


def format_alt_text(title, all_results, versions):
    parts = [
        format_server_summary(name, all_results[name], versions[name])
        for name in all_results
    ]
    return f'{title}. {"; ".join(parts)}.'


def plot_results(all_results, title, filename):
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle(title, fontsize=16)

    names = list(all_results.keys())
    versions = get_versions()
    display_names = [f'{name}\n{versions[name]}' for name in names]
    rps_values = [all_results[n].get('rps', 0) for n in names]
    bar_colors = [SERVER_COLORS.get(name, FALLBACK_COLOR) for name in names]

    ax1 = axes[0]
    ax1.bar(
        display_names,
        rps_values,
        color=bar_colors,
        edgecolor='#222222',
        linewidth=1.0,
    )
    ax1.set_title('Requests Per Second (Higher is better)')
    ax1.set_ylabel('RPS')
    for i, v in enumerate(rps_values):
        ax1.text(i, v, f'{v:,.0f}', ha='center', va='bottom')

    ax2 = axes[1]
    has_latency = False
    for name, display_name in zip(names, display_names, strict=False):
        res = all_results[name]
        if res.get('latency_percentiles'):
            pcts = res['latency_percentiles']
            sorted_pct_items = sorted(
                pcts.items(), key=lambda item: float(item[0].replace('p', ''))
            )
            x = [float(k.replace('p', '')) for k, _ in sorted_pct_items]
            y = [
                v * 1000 for _, v in sorted_pct_items if v is not None
            ]  # Convert to ms

            if len(x) == len(y) and len(y) > 0:
                ax2.plot(
                    x,
                    y,
                    color=SERVER_COLORS.get(name, FALLBACK_COLOR),
                    linestyle=SERVER_LINESTYLES.get(name, '-'),
                    marker=SERVER_MARKERS.get(name, 'o'),
                    label=display_name,
                    linewidth=2,
                    markersize=6,
                    markeredgecolor='#222222',
                    markeredgewidth=1.0,
                )
                has_latency = True

    if has_latency:
        ax2.set_title('Latency Distribution (Lower is better)')
        ax2.set_xlabel('Percentile')
        ax2.set_ylabel('Latency (ms)')
        ax2.legend()
        ax2.grid(True, linestyle='--', alpha=0.7)
    else:
        ax2.set_title('Latency data unavailable')

    system_info = get_system_info()
    plt.figtext(
        0.02,
        0.02,
        system_info,
        fontsize=9,
        verticalalignment='bottom',
        bbox={'boxstyle': 'round', 'facecolor': 'whitesmoke', 'alpha': 0.75},
    )

    plt.tight_layout(rect=[0, 0.05, 1, 0.95])
    plt.savefig(filename, format='svg')
    plt.close()
    print(f'Saved plot to {filename}')
    print(format_alt_text(title, all_results, versions))


def run_benchmarks(selected_servers=None, selected_types=None):
    all_configs = [
        {'name': 'HTTP/1 GET (1 Worker)', 'workers': 1, 'type': 'h1'},
        {'name': 'HTTP/1 GET (4 Workers)', 'workers': 4, 'type': 'h1'},
        {'name': 'HTTP/1 GET over UDS (1 Worker)', 'workers': 1, 'type': 'h1_uds'},
        {'name': 'HTTP/1 GET over UDS (4 Workers)', 'workers': 4, 'type': 'h1_uds'},
        {'name': 'HTTP/2 GET (1 Worker)', 'workers': 1, 'type': 'h2'},
        {'name': 'HTTP/2 GET (4 Workers)', 'workers': 4, 'type': 'h2'},
        {
            'name': 'HTTP/1 Static file (1 Worker)',
            'workers': 1,
            'type': 'h1_file',
        },
        {
            'name': 'HTTP/1 Static file (4 Workers)',
            'workers': 4,
            'type': 'h1_file',
        },
        {
            'name': 'HTTP/2 Static file (1 Worker)',
            'workers': 1,
            'type': 'h2_file',
        },
        {
            'name': 'HTTP/2 Static file (4 Workers)',
            'workers': 4,
            'type': 'h2_file',
        },
        {
            'name': 'HTTP/1 Streaming POST (1 Worker)',
            'workers': 1,
            'type': 'h1_stream',
            'concurrency': STREAMING_CONCURRENCY,
        },
        {
            'name': 'HTTP/1 Streaming POST (4 Workers)',
            'workers': 4,
            'type': 'h1_stream',
            'concurrency': STREAMING_CONCURRENCY,
        },
        {
            'name': 'HTTP/2 Streaming POST (1 Worker)',
            'workers': 1,
            'type': 'h2_stream',
            'concurrency': STREAMING_CONCURRENCY,
        },
        {
            'name': 'HTTP/2 Streaming POST (4 Workers)',
            'workers': 4,
            'type': 'h2_stream',
            'concurrency': STREAMING_CONCURRENCY,
        },
        {'name': 'HTTP/1 WebSocket (1 Worker)', 'workers': 1, 'type': 'ws'},
        {'name': 'HTTP/1 WebSocket (4 Workers)', 'workers': 4, 'type': 'ws'},
    ]

    configs = [
        c for c in all_configs if not selected_types or c['type'] in selected_types
    ]
    servers_to_run = [
        s for s in SERVERS if not selected_servers or s in selected_servers
    ]

    Path('bench/results/plots').mkdir(exist_ok=True, parents=True)

    for config in configs:
        print(f'\n=== Benchmarking {config["name"]} ===')
        results = {}
        for server in servers_to_run:
            if config['name'].startswith('HTTP/2') and server in {
                'uvicorn',
                'gunicorn',
            }:
                print(f'Skipping HTTP/2 for {server}')
                continue

            socket_path = UNIX_SOCKET_PATH if config['type'] == 'h1_uds' else None

            try:
                with running_server(server, config['workers'], socket_path):
                    print(f'Running load generation for {server}...')
                    requests = config.get('requests', REQUESTS)
                    concurrency = config.get('concurrency', CONCURRENCY)
                    if config['type'] == 'h1':
                        raw = run_oha(
                            f'http://{HOST}:{PORT}/',
                            http_version='1.1',
                            requests=requests,
                            concurrency=concurrency,
                        )
                    elif config['type'] == 'h1_uds':
                        raw = run_oha(
                            'http://localhost/',
                            http_version='1.1',
                            requests=requests,
                            concurrency=concurrency,
                            unix_socket=socket_path,
                        )
                    elif config['type'] == 'h2':
                        raw = run_oha(
                            f'http://{HOST}:{PORT}/',
                            http_version='2',
                            requests=requests,
                            concurrency=concurrency,
                        )
                    elif config['type'] in {'h1_file', 'h2_file'}:
                        http_version = '2' if config['type'] == 'h2_file' else '1.1'
                        raw = run_oha(
                            f'http://{HOST}:{PORT}/static-file',
                            http_version=http_version,
                            requests=requests,
                            concurrency=concurrency,
                        )
                    elif config['type'] in {'h1_stream', 'h2_stream'}:
                        http_version = '2' if config['type'] == 'h2_stream' else '1.1'
                        raw = run_oha(
                            f'http://{HOST}:{PORT}/streaming-post',
                            http_version=http_version,
                            method='POST',
                            body='x' * 1024,
                            content_type='application/octet-stream',
                            requests=requests,
                            concurrency=concurrency,
                        )
                    elif config['type'] == 'ws':
                        raw = run_k6(
                            f'ws://{HOST}:{PORT}/ws',
                            requests=requests,
                            concurrency=concurrency,
                        )
                    else:
                        raw = None

                    metrics = extract_metrics(raw, config['type'])

                    if metrics:
                        results[server] = metrics
                        print(format_server_summary(server, metrics))
                    else:
                        print(f'{server}: Load generation failed')
            except RuntimeError as err:
                traceback.print_exception(err)
                continue

        if results:
            clean_name = (
                config['name']
                .lower()
                .replace('/', '_')
                .replace(' ', '_')
                .replace('(', '')
                .replace(')', '')
            )
            filename = f'bench/results/plots/benchmark_{clean_name}.svg'
            plot_results(
                results,
                f'Benchmark: {config["name"]} ({config.get("requests", REQUESTS)} reqs, {config.get("concurrency", CONCURRENCY)} conn)',
                filename,
            )


def main():
    global REQUESTS, CONCURRENCY
    parser = argparse.ArgumentParser(
        description='h2corn benchmark suite',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--servers',
        nargs='+',
        choices=list(SERVERS.keys()),
        help='List of servers to benchmark',
    )
    parser.add_argument(
        '--types',
        nargs='+',
        choices=[
            'h1',
            'h1_uds',
            'h2',
            'h1_file',
            'h2_file',
            'h1_stream',
            'h2_stream',
            'ws',
        ],
        help='List of benchmark types to run',
    )
    parser.add_argument(
        '--requests',
        type=int,
        default=REQUESTS,
        help='Number of requests per benchmark',
    )
    parser.add_argument(
        '--concurrency',
        type=int,
        default=CONCURRENCY,
        help='Number of concurrent connections',
    )
    args = parser.parse_args()

    REQUESTS = args.requests
    CONCURRENCY = args.concurrency

    selected_servers = set(args.servers) if args.servers else None
    selected_types = set(args.types) if args.types else None

    if not shutil.which('oha'):
        print('Error: oha is not installed.')
        return

    if not shutil.which('k6'):
        print('Error: k6 is not installed.')
        return

    run_benchmarks(selected_servers, selected_types)


if __name__ == '__main__':
    main()
