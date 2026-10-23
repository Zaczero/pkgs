"""Synthetic operation mix, normalized by verified requests, events and messages."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from fractions import Fraction
from pathlib import Path

import trustme
from websockets.sync.client import connect

from bench.pgo import client, workload
from bench.pgo.collection import reconcile, server


@dataclass(frozen=True)
class Leaf:
    name: str
    weight: str
    kind: str
    protocol: str
    operations: int
    workers: int = 1
    tls: str = ''
    log: str = 'off'
    file_log: bool = False
    loop: str = 'asyncio'
    unix: bool = False
    proxy: str = ''
    mtls: bool = False

    @property
    def units(self) -> int:
        return self.operations * (
            64 if self.kind.startswith('ws') else 32 if self.kind == 'events' else 1
        )

    @property
    def expected_functions(self) -> list[str]:
        families = ['4__lib6bridge', '4__lib2' + self.protocol, 'tokio']
        if self.tls:
            families.append('6rustls')
        if self.kind == 'file':
            families.append('pathsend')
        if self.kind.startswith('ws'):
            families.append('WebSocketCodec')
        if self.kind == 'ws-deflate':
            families.append('PerMessageDeflateEnabled')
        return families


# Weights describe chosen useful-operation shares, not deployment observations.
LEAVES = (
    Leaf('proxy-h1-text', '18', 'web', 'h1', 10000, workers=2, log='text'),
    Leaf('proxy-h1-json', '7.2', 'web', 'h1', 4000, workers=2, log='json'),
    Leaf('proxy-h1-off', '8.6', 'web', 'h1', 6000, workers=2),
    Leaf('failure-h1', '0.1', 'failure', 'h1', 256),
    Leaf('failure-h2', '0.1', 'failure', 'h2', 256),
    Leaf('proxy-v1', '1', 'web', 'h1', 2000, proxy='v1'),
    Leaf('proxy-h1-uds', '1', 'web', 'h1', 2000, unix=True),
    Leaf('proxy-h2', '23', 'web', 'h2', 16000, log='json', loop='uvloop'),
    Leaf('proxy-v2', '1', 'web', 'h2', 2000, proxy='v2'),
    Leaf('tls13-h1', '7.2', 'web', 'h1', 8000, tls='1.3', log='text'),
    Leaf('tls13-h2', '6.2', 'web', 'h2', 8000, tls='1.3', log='text'),
    Leaf('mtls-h2', '1', 'web', 'h2', 2000, tls='1.3', mtls=True),
    Leaf('tls12-h1', '0.8', 'web', 'h1', 2000, tls='1.2'),
    Leaf('tls12-h2', '0.8', 'web', 'h2', 2000, tls='1.2'),
    Leaf('upload-h1', '2.5', 'upload', 'h1', 256),
    Leaf('upload-h2-tls', '2.5', 'upload', 'h2', 256, tls='1.3'),
    Leaf('download-h1', '2.5', 'download', 'h1', 256),
    Leaf('download-h2-tls', '2.5', 'download', 'h2', 256, tls='1.3'),
    Leaf('events-h1', '1.5', 'events', 'h1', 256, log='json'),
    Leaf('events-h2', '1.5', 'events', 'h2', 256, log='json'),
    Leaf('file-h1', '1.6', 'file', 'h1', 512, log='json', file_log=True),
    Leaf('file-h1-uds', '0.4', 'file', 'h1', 256, unix=True),
    Leaf('file-h2', '1.2', 'file', 'h2', 512, log='json'),
    Leaf('file-h2-tls', '0.8', 'file', 'h2', 512, tls='1.3', log='json', file_log=True),
    Leaf('ws-plain', '1.5', 'ws', 'h1', 128, workers=2),
    Leaf('ws-deflate', '1.5', 'ws-deflate', 'h1', 128, workers=2),
    *(
        Leaf(
            f'{kind}-{transport}', str(Fraction(weight) / 2), kind, 'h2', count, tls=tls
        )
        for kind, weight, count in (
            ('grpc-unary', '2.8', 2048),
            ('grpc-upload', '0.4', 256),
            ('grpc-download', '0.4', 256),
            ('grpc-duplex', '0.4', 256),
        )
        for transport, tls in (('plain', ''), ('tls', '1.3'))
    ),
)


def exchange(leaf: Leaf, index: int) -> client.Exchange:
    kind = leaf.kind
    body = b''
    headers = (
        ('x-request-id', str(index)),
        ('cookie', f'session={index // 128}'),
        ('accept', 'application/octet-stream'),
        ('x-forwarded-for', '192.0.2.1'),
    )
    if kind == 'failure':
        return (
            client.Exchange(f'/fail/{index}', b'', b'', status=500)
            if index % 2
            else client.Exchange('/web/0', b'', workload.payload(0, 512))
        )
    if kind == 'web':
        body = workload.payload(index, workload.upload_size(index))
        expected = workload.payload(index, workload.response_size(index))
        if index % 20 == 1:
            kind = 'sync'
        if leaf.protocol == 'h2':
            size = 12000 if index % 20 == 19 else 2000 if index % 5 == 4 else 400
            headers += (('x-metadata', workload.payload(index + 1, size // 2).hex()),)
    elif kind == 'upload':
        body = workload.payload(index, workload.FILE_SIZES[index % 4])
        expected = client.digest(body)
    elif kind == 'download':
        expected = workload.payload(index, workload.FILE_SIZES[index % 4])
    elif kind == 'file':
        expected = workload.payload(0, workload.FILE_SIZES[index % 4])
        if index % 10 == 0:
            kind = 'range'
    elif kind == 'events':
        expected = workload.stream_body(index)
    elif kind.startswith('grpc-'):
        size = (
            65536
            if kind == 'grpc-unary' and index % 10 == 0
            else 256
            if kind == 'grpc-unary'
            else 4096
        )
        message = workload.payload(index, size)
        frame = b'\0' + size.to_bytes(4, 'big') + message
        body = frame * (16 if kind in {'grpc-upload', 'grpc-duplex'} else 1)
        if kind == 'grpc-upload':
            result = hashlib.sha256(body).digest()
            expected = b'\0' + len(result).to_bytes(4, 'big') + result
        else:
            expected = frame * 16 if kind == 'grpc-download' else body
        headers += (('content-type', 'application/grpc'), ('te', 'trailers'))
    else:
        raise ValueError(f'unknown HTTP workload: {kind}')
    return client.Exchange(
        f'/{kind}/{index}',
        body,
        expected,
        headers,
        grpc=kind.startswith('grpc-'),
        slow=kind == 'download' and index % 5 == 0,
    )


def websocket(
    port: int, leaf: Leaf, index: int
) -> tuple[dict[int, int], client.TlsState | None]:
    with connect(
        f'ws://127.0.0.1:{port}/echo',
        compression='deflate' if leaf.kind == 'ws-deflate' else None,
        open_timeout=60,
        close_timeout=60,
    ) as connection:
        assert connection.response is not None
        pid = int(connection.response.headers['x-worker'])
        for message in range(64):
            size = (
                65536
                if message % 20 == 19
                else 4096
                if message % 4 == 3
                else 32 + message * 7
            )
            body = workload.payload(index * 64 + message, size)
            value = body if message % 10 < 3 else body.hex()
            connection.send(value)
            if connection.recv(timeout=60) != value:
                raise RuntimeError('WebSocket echo mismatch')
            if message % 16 == 15 and not connection.ping().wait(60):
                raise TimeoutError('WebSocket pong missing')
            if message % 8 == 7:
                time.sleep(0.005)
    return {pid: 64}, None


def batch(
    leaf, port, context, connections, start: int, stop: int
) -> tuple[dict[int, int], client.TlsState | None]:
    exchanges = [exchange(leaf, index) for index in range(start, stop)]
    function = client.http1 if leaf.protocol == 'h1' else client.http2
    state = client.TlsState() if context is not None else None
    batch_size = 128 if leaf.kind == 'web' else 16
    # One of four connection cohorts reconnects with its freshly received ticket.
    parts = (
        (exchanges[: len(exchanges) // 2], exchanges[len(exchanges) // 2 :])
        if state is not None and (start // batch_size) % 4 == 0
        else (exchanges,)
    )
    counts = {}
    for part in parts:
        options = (
            {'slots': 1} if leaf.kind == 'failure' and leaf.protocol == 'h2' else {}
        )
        for pid, total in function(
            port,
            part,
            context,
            connections,
            proxy=leaf.proxy,
            tls_state=state,
            **options,
        ).items():
            counts[pid] = counts.get(pid, 0) + total
    return counts, state


def collect(root: Path, leaves: tuple[Leaf, ...] = LEAVES) -> Path:
    root.mkdir(parents=True)
    sources = root / 'sources'
    sources.mkdir()
    source_hashes = {}
    for name in ('corpus.py', 'client.py', 'workload.py', 'collection.py'):
        data = Path(__file__).with_name(name).read_bytes()
        (sources / name).write_bytes(data)
        source_hashes[name] = hashlib.sha256(data).hexdigest()
    files = root / 'files'
    files.mkdir()
    for size in workload.FILE_SIZES:
        data = workload.payload(0, size)
        (files / str(size)).write_bytes(data)
        (files / f'{size}.range').write_bytes(
            b'prefix'.ljust(37, b'!') + data + b'suffix'
        )
    ca = trustme.CA()
    certificate = ca.issue_cert('localhost')
    certificate.cert_chain_pems[0].write_to_path(files / 'cert.pem')
    certificate.private_key_pem.write_to_path(files / 'key.pem')
    ca.cert_pem.write_to_path(files / 'ca.pem')
    client_certificate = ca.issue_cert('pgo-client')
    client_certificate.cert_chain_pems[0].write_to_path(files / 'client.pem')
    client_certificate.private_key_pem.write_to_path(files / 'client-key.pem')
    plan = {
        'version': 1,
        'seed': workload.SEED.hex(),
        'sources': source_hashes,
        'dependencies': {
            name: importlib.metadata.version(name)
            for name in (
                'h2',
                'hpack',
                'hyperframe',
                'starlette',
                'anyio',
                'trustme',
                'cryptography',
                'websockets',
                'uvloop',
            )
        },
        'leaves': [
            {
                **asdict(leaf),
                'units': leaf.units,
                'expected_functions': leaf.expected_functions,
            }
            for leaf in leaves
        ],
    }
    plan_bytes = json.dumps(plan, sort_keys=True).encode()
    (root / 'plan.json').write_bytes(plan_bytes)
    records = []
    # Stable permutation avoids making family order an incidental scheduling input.
    for leaf in sorted(
        leaves,
        key=lambda item: hashlib.sha256(workload.SEED + item.name.encode()).digest(),
    ):
        leaf_root = root / leaf.name
        config = {
            'workers': leaf.workers,
            'loop': leaf.loop,
            'access_log': leaf.log != 'off',
            'log_format': 'json' if leaf.log == 'off' else leaf.log,
            'grpc': leaf.kind.startswith('grpc-'),
            'forwarded_allow_ips': ['127.0.0.1', 'unix'],
            'proxy_headers': leaf.name.startswith('proxy-') and not leaf.proxy,
            'proxy_protocol': leaf.proxy or 'off',
        }
        if leaf.kind == 'upload' and leaf.protocol == 'h2':
            config.update(
                h2_initial_stream_window_size=65536,
                h2_initial_connection_window_size=262144,
            )
        context = None
        if leaf.tls:
            config.update(
                certfile=str(files / 'cert.pem'), keyfile=str(files / 'key.pem')
            )
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ca.configure_trust(context)
            version = (
                ssl.TLSVersion.TLSv1_3 if leaf.tls == '1.3' else ssl.TLSVersion.TLSv1_2
            )
            context.minimum_version = context.maximum_version = version
            context.set_alpn_protocols(['h2' if leaf.protocol == 'h2' else 'http/1.1'])
            if leaf.mtls:
                config.update(ca_certs=str(files / 'ca.pem'), cert_reqs='required')
                context.load_cert_chain(files / 'client.pem', files / 'client-key.pem')
        counts = {}
        handshakes = {'full': 0, 'resumed': 0}
        started = time.monotonic()
        with (
            client.Connections() as connections,
            server(
                leaf_root, config, files=files, pipe=not leaf.file_log, unix=leaf.unix
            ) as port,
            ThreadPoolExecutor(
                max_workers=32 if leaf.protocol == 'h1' else 4
            ) as clients,
        ):
            if leaf.kind.startswith('ws'):
                assert isinstance(port, int)
                futures = [
                    clients.submit(websocket, port, leaf, index)
                    for index in range(leaf.operations)
                ]
            else:
                batch_size = (
                    128 if leaf.kind == 'web' else 2 if leaf.kind == 'failure' else 16
                )
                futures = [
                    clients.submit(
                        batch,
                        leaf,
                        port,
                        context,
                        connections,
                        start,
                        min(start + batch_size, leaf.operations),
                    )
                    for start in range(0, leaf.operations, batch_size)
                ]
            for future in futures:
                completed, state = future.result()
                if state is not None:
                    handshakes['full'] += state.full
                    handshakes['resumed'] += state.resumed
                for pid, total in completed.items():
                    counts[pid] = counts.get(pid, 0) + total
        record = reconcile(leaf_root, counts, leaf.workers)
        expected = leaf.operations * (64 if leaf.kind.startswith('ws') else 1)
        if sum(counts.values()) != expected:
            raise RuntimeError(f'{leaf.name}: incomplete useful work')
        if leaf.tls and not handshakes['resumed']:
            raise RuntimeError(f'{leaf.name}: TLS session resumption was not exercised')
        units = expected * (32 if leaf.kind == 'events' else 1)
        record.update(
            name=leaf.name,
            weight=leaf.weight,
            units=units,
            image_sha256=record['server']['image_sha256'],
            expected_functions=leaf.expected_functions,
            elapsed_seconds=time.monotonic() - started,
            handshakes=handshakes,
        )
        (leaf_root / 'collection.json').write_text(json.dumps(record, indent=2) + '\n')
        records.append(record)
        print(
            f'{leaf.name}: {units} verified units in {record["elapsed_seconds"]:.2f}s',
            flush=True,
            file=sys.stderr,
        )
    receipt = root / 'collection.json'
    receipt.write_text(
        json.dumps(
            {
                'plan_sha256': hashlib.sha256(plan_bytes).hexdigest(),
                'leaves': records,
            },
            indent=2,
        )
        + '\n'
    )
    return receipt
