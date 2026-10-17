"""Everyday utility workloads, measured by pyperf.

Run from a package's dev environment:
    uv run --project polyline-rs python tools/benchmark.py --package polyline-rs -o baseline.json
    uv run --project polyline-rs python tools/benchmark.py --package polyline-rs -o candidate.json
    uv run --project polyline-rs python -m pyperf compare_to baseline.json candidate.json

Fixture construction and result checks are excluded. Stateful cases repeat a
complete cycle so later iterations retain the same hit, miss, and eviction mix.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import inspect
import json
import math
import sys
from fnmatch import fnmatchcase
from functools import partial
from importlib.metadata import distribution
from pathlib import Path
from tempfile import TemporaryDirectory

import pyperf


def githead_cases(scratch: Path) -> dict:
    from githead import githead

    expected = '1234567890abcdef1234567890abcdef12345678'
    detached = scratch / 'detached'
    detached.mkdir()
    (detached / 'HEAD').write_text(expected + '\n')
    symbolic = scratch / 'symbolic'
    (symbolic / 'refs/heads').mkdir(parents=True)
    (symbolic / 'HEAD').write_text('ref: refs/heads/main\n')
    (symbolic / 'refs/heads/main').write_text(expected + '\n')
    assert githead(detached) == expected
    assert githead(symbolic) == expected
    return {
        'detached HEAD': partial(githead, detached),
        'loose branch HEAD': partial(githead, symbolic),
    }


def sizestr_cases(_scratch: Path) -> dict:
    from sizestr import sizestr

    values = (
        (512, '512 B'),
        (1536, '1.50 KiB'),
        (12 * 1024**2, '12.0 MiB'),
        (3 * 1024**4, '3.00 TiB'),
        (-1536, '-1.50 KiB'),
    )
    for value, expected in values:
        assert sizestr(value) == expected
    return {f'format {expected}': partial(sizestr, value) for value, expected in values}


def polyline_cases(_scratch: Path) -> dict:
    from polyline_rs import decode_latlon, decode_lonlat, encode_latlon, encode_lonlat

    known = [(38.5, -120.2), (40.7, -120.95), (43.252, -126.453)]
    encoded = '_p~iF~ps|U_ulLnnqC_mqNvxq`@'
    assert encode_latlon(known) == encoded
    assert all(
        abs(a - b) <= math.ulp(b)
        for pair, expected in zip(decode_latlon(encoded), known, strict=True)
        for a, b in zip(pair, expected, strict=True)
    )
    cases = {
        'encode latlon 3': partial(encode_latlon, known),
        'decode latlon 3': partial(decode_latlon, encoded),
    }
    for size in (10, 1000):
        points = [(21 + (i % 37) * 0.00001, 52 + i * 0.00001) for i in range(size)]
        for precision in (5, 6):
            text = encode_lonlat(points, precision)
            restored = decode_lonlat(text, precision)
            assert len(restored) == size
            assert all(
                abs(a - b) < 1e-9
                for pair, expected in zip(restored, points, strict=True)
                for a, b in zip(pair, expected, strict=True)
            )
            cases[f'encode lonlat {size} precision {precision}'] = partial(
                encode_lonlat, points, precision
            )
            cases[f'decode lonlat {size} precision {precision}'] = partial(
                decode_lonlat, text, precision
            )
    return cases


def shortlink_cases(_scratch: Path) -> dict:
    from osm_shortlink import shortlink_decode, shortlink_encode

    assert shortlink_encode(0.055, 51.511, 9) == '0EEQjE--'
    assert shortlink_decode('0EEQjE--') == (0.054931640625, 51.510772705078125, 9)
    cases = {}
    for zoom in (0, 9, 18, 22):
        token = shortlink_encode(21.0122, 52.2297, zoom)
        assert shortlink_decode(token)[2] == zoom
        cases[f'encode zoom {zoom}'] = partial(shortlink_encode, 21.0122, 52.2297, zoom)
        cases[f'decode zoom {zoom}'] = partial(shortlink_decode, token)
    return cases


def zid_cases(_scratch: Path) -> dict:
    from zid import parse_zid_timestamp, zid, zids

    identifier = (1_700_000_000_000 << 16) | 12345
    assert parse_zid_timestamp(identifier) == 1_700_000_000_000
    batch = zids(256)
    assert batch == list(range(batch[0], batch[0] + 256))
    return {
        'reserve one': zid,
        'reserve 256': partial(zids, 256),
        'extract timestamp': partial(parse_zid_timestamp, identifier),
    }


def lru_cases(_scratch: Path) -> dict:
    from lrucache_rs import LRUCache

    cache = LRUCache(64)
    cache.update((f'key-{i}', i) for i in range(64))
    assert cache['key-31'] == 31
    assert cache.get('absent') is None
    evicting = LRUCache(64)

    def insert_cycle():
        for i in range(128):
            evicting[i] = i

    insert_cycle()
    assert list(evicting) == list(range(64, 128))
    return {
        'hit string key': partial(cache.get, 'key-31'),
        'miss string key': partial(cache.get, 'absent'),
        'snapshot 64 items': lambda: list(cache.items()),
        'insert and evict 128 integers': insert_cycle,
    }


def jinja_cases(_scratch: Path) -> dict:
    from jinja2 import DictLoader, Environment
    from jinja2_htmlmin import minify_loader

    source = (
        '<!-- catalog -->\n<ul>\n'
        '{% for item in items %}<li>{{ item }}</li>{% endfor %}\n</ul>'
    )
    expected = '<ul> ' + ''.join(f'<li>{i}</li>' for i in range(32)) + ' </ul>'
    cases = {}

    def render(environment):
        return environment.get_template('catalog.html').render(items=range(32))

    for label, capacity in (
        ('load compile render 32 items', 0),
        ('cached render 32 items', 400),
    ):
        environment = Environment(
            loader=minify_loader(
                DictLoader({'catalog.html': source}),
                remove_comments=True,
                remove_empty_space=True,
                remove_all_empty_space=True,
            ),
            autoescape=True,
            cache_size=capacity,
        )

        call = partial(render, environment)
        assert call() == expected
        cases[label] = call
    return cases


def httpx_cases(_scratch: Path) -> dict:
    import httpx
    from httpx_secure import httpx_ssrf_protection

    async def client_lifecycle():
        async with httpx.AsyncClient(trust_env=False) as client:
            return httpx_ssrf_protection(client) is client

    assert asyncio.run(client_lifecycle())
    return {'protected client lifecycle without network I/O': client_lifecycle}


def compression_exchange(
    encoding: str, messages: tuple[bytes, ...], content_type: bytes
):
    from starlette_compress import CompressMiddleware

    async def app(scope, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', content_type)],
        })
        for index, chunk in enumerate(messages):
            await send({
                'type': 'http.response.body',
                'body': chunk,
                'more_body': index + 1 < len(messages),
            })

    middleware = CompressMiddleware(app)
    scope = {
        'type': 'http',
        'method': 'GET',
        'headers': [(b'accept-encoding', encoding.encode())],
    }

    async def exchange():
        output = []

        async def receive():
            return {'type': 'http.request', 'body': b''}

        async def send(message):
            if message['type'] == 'http.response.body':
                output.append(message.get('body', b''))

        await middleware(scope, receive, send)
        return b''.join(output)

    return exchange


def compression_cases(_scratch: Path) -> dict:
    import gzip

    cases = {}
    for encoding in ('identity', 'gzip', 'br', 'zstd'):
        for streaming in (False, True):
            messages = (
                (b'data: {"value":123,"label":"sensor"}\n\n',) * 32
                if streaming
                else (
                    json.dumps(
                        {'items': [{'value': 123, 'label': 'sensor'}] * 32},
                        separators=(',', ':'),
                    ).encode(),
                )
            )
            body = b''.join(messages)
            content_type = b'text/event-stream' if streaming else b'application/json'
            exchange = compression_exchange(encoding, messages, content_type)
            encoded = asyncio.run(exchange())
            if encoding == 'gzip':
                decoded = gzip.decompress(encoded)
            elif encoding == 'br':
                if sys.implementation.name == 'cpython':
                    import brotli
                else:
                    import brotlicffi as brotli

                decoded = brotli.decompress(encoded)
            elif encoding == 'zstd':
                if sys.version_info >= (3, 14):
                    from compression.zstd import decompress

                    decoded = decompress(encoded)
                else:
                    from zstandard import ZstdDecompressor

                    with ZstdDecompressor().stream_reader(encoded) as reader:
                        decoded = reader.read()
            else:
                decoded = encoded
            assert decoded == body
            cases[
                f'{encoding} {"SSE 32 messages" if streaming else f"one-shot JSON {len(body)} bytes"}'
            ] = exchange
    return cases


def benchroom_cases(_scratch: Path) -> dict:
    from benchroom import Report, Series

    series = tuple(
        Series(
            f'operation-{i}', implementation, tuple(0.001 * (j + 2) for j in range(20))
        )
        for i in range(4)
        for implementation in ('baseline', 'candidate')
    )

    def summarize():
        return Report('Operation time', series, unit='seconds').markdown()

    assert 'operation-3' in summarize()
    return {'summarize 8 series of 20 runs': summarize}


PROVIDERS = {
    'benchroom': benchroom_cases,
    'githead': githead_cases,
    'httpx-secure': httpx_cases,
    'jinja2-htmlmin': jinja_cases,
    'lrucache-rs': lru_cases,
    'osm-shortlink': shortlink_cases,
    'polyline-rs': polyline_cases,
    'sizestr': sizestr_cases,
    'starlette-compress': compression_cases,
    'zid': zid_cases,
}


def main() -> None:
    def add_worker_args(command, args):
        command.extend(('--package', args.package))
        for pattern in args.case:
            command.extend(('--case', pattern))

    runner = pyperf.Runner(add_cmdline_args=add_worker_args)
    parser = runner.argparser
    parser.description = __doc__
    parser.formatter_class = partial(argparse.RawDescriptionHelpFormatter, width=88)
    parser.allow_abbrev = False
    parser.add_argument(
        '--package', required=True, choices=PROVIDERS, help='Select package workloads'
    )
    parser.add_argument(
        '--case',
        action='append',
        default=[],
        metavar='GLOB',
        help='Select matching case names (repeatable)',
    )
    parser.add_argument(
        '--list', action='store_true', help='List selected cases without measuring'
    )
    args = runner.parse_args()
    with TemporaryDirectory(prefix='pkgs-benchmark-') as scratch:
        cases = PROVIDERS[args.package](Path(scratch))
        selected = {
            name: call
            for name, call in cases.items()
            if not args.case or any(fnmatchcase(name, pattern) for pattern in args.case)
        }
        if not selected:
            parser.error('--case matches no workloads; use --list to inspect names')
        if args.list:
            print('\n'.join(selected))
            return
        package = distribution(args.package)
        digest = hashlib.sha256()
        module = importlib.import_module(args.package.replace('-', '_'))
        package_path = Path(module.__file__).parent
        for file in sorted(package_path.rglob('*')):
            if file.suffix in {'.py', '.so', '.pyd'}:
                digest.update(file.relative_to(package_path).as_posix().encode())
                digest.update(b'\0')
                digest.update(file.read_bytes())
                digest.update(b'\0')
        runner.metadata['implementation'] = f'{args.package} {package.version}'
        runner.metadata['package_code_sha256'] = digest.hexdigest()
        runner.metadata['benchmark_sha256'] = hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest()
        for name, call in selected.items():
            benchmark = (
                runner.bench_async_func
                if inspect.iscoroutinefunction(call)
                else runner.bench_func
            )
            benchmark(f'{args.package}: {name}', call)


if __name__ == '__main__':
    main()
