"""Isolated PostGIS fixture and persistent psql binary tile transport."""

from __future__ import annotations

import atexit
import json
import os
import selectors
import struct
import subprocess
import time
import uuid
from typing import Any

from _bench_mvt_workload import (
    BUFFER,
    EXTENT,
    FRACTION,
    TILE_X,
    TILE_Y,
    UNIT,
    Workload,
    X_STEP,
    Y_STEP,
    ZOOM,
)

_IMAGE = 'postgis/postgis:18-3.6'


def _query(workload: Workload) -> str:
    return f"""
SELECT ST_AsMVT(q, '{workload.layer}', {EXTENT}, 'geom', 'id')
FROM (
  SELECT id, class, rank,
         ST_AsMVTGeom(geom, ST_TileEnvelope({ZOOM}, {TILE_X}, {TILE_Y}),
                      {EXTENT}, {BUFFER}, true) AS geom
  FROM benchmark_features
  WHERE geom && ST_TileEnvelope({ZOOM}, {TILE_X}, {TILE_Y},
                                margin => {BUFFER + 1}.0 / {EXTENT})
  ORDER BY id
) AS q
"""


def _docker(*args: str, input: str | None = None, timeout: float = 120) -> str:
    completed = subprocess.run(
        ['docker', *args],
        input=input,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode:
        raise RuntimeError(f'docker {args[0]} failed: {completed.stderr.strip()}')
    return completed.stdout


def _uses_index(plan: dict[str, Any], name: str) -> bool:
    return plan.get('Index Name') == name or any(
        _uses_index(child, name) for child in plan.get('Plans', ())
    )


class PostGIS:
    """One isolated database and a persistent psql binary-COPY session."""

    def __init__(self, workload: Workload) -> None:
        self.name = f'gometry-mvt-bench-{uuid.uuid4().hex}'
        self.process: subprocess.Popen[bytes] | None = None
        self.started = True
        self.worker_affinity = sorted(os.sched_getaffinity(0))
        self.cpu = self.worker_affinity[0]
        atexit.register(self.close)
        started = time.perf_counter()
        try:
            _docker(
                'run',
                '-d',
                '--rm',
                '--network',
                'none',
                '--name',
                self.name,
                '-e',
                'POSTGRES_PASSWORD=gometry-benchmark',
                '--entrypoint',
                'taskset',
                _IMAGE,
                '--cpu-list',
                str(self.cpu),
                '/usr/local/bin/docker-entrypoint.sh',
                'postgres',
                timeout=180,
            )
            self._wait_ready()
            status = _docker('exec', self.name, 'cat', '/proc/1/status')
            affinity = next(
                line.split(':', 1)[1].strip()
                for line in status.splitlines()
                if line.startswith('Cpus_allowed_list:')
            )
            if affinity != str(self.cpu):
                raise RuntimeError(
                    f'PostGIS CPU affinity {affinity!r} does not match {self.cpu}'
                )
            setup = f"""
SET jit=off;
CREATE TABLE benchmark_features AS
WITH coordinates AS (
  SELECT i,
    (((i - 1) * {X_STEP} % {workload.period} + {workload.offset}
      + CASE WHEN (i - 1) % {workload.select_every} = 0 THEN 0
             ELSE {workload.alternate_x} END)::float8 + {FRACTION}::float8) AS x,
    (((i - 1) * {Y_STEP} % {workload.period} + {workload.offset})::float8 + {FRACTION}::float8) AS y
  FROM generate_series(1, {workload.rows}) AS i
)
SELECT i::bigint AS id,
       ST_SetSRID(CASE
         WHEN i <= {workload.points} THEN ST_MakePoint(x * {UNIT!r}, -y * {UNIT!r})
         WHEN i <= {workload.points + workload.lines} THEN ST_MakeLine(
           ST_MakePoint(x * {UNIT!r}, -y * {UNIT!r}),
           ST_MakePoint((x + 37) * {UNIT!r}, -(y + 13) * {UNIT!r})
         )
         ELSE ST_MakePolygon(ST_MakeLine(ARRAY[
           ST_MakePoint(x * {UNIT!r}, -y * {UNIT!r}),
           ST_MakePoint((x + 16) * {UNIT!r}, -y * {UNIT!r}),
           ST_MakePoint((x + 16) * {UNIT!r}, -(y + 11) * {UNIT!r}),
           ST_MakePoint(x * {UNIT!r}, -(y + 11) * {UNIT!r}),
           ST_MakePoint(x * {UNIT!r}, -y * {UNIT!r})
         ]))
       END, 3857) AS geom,
       CASE WHEN i <= {workload.points} THEN 'place'
            WHEN i <= {workload.points + workload.lines} THEN 'road'
            ELSE 'building' END::text AS class,
       ((i - 1) % 11)::bigint AS rank
FROM coordinates;
CREATE INDEX benchmark_features_geom_idx ON benchmark_features USING gist (geom);
ANALYZE benchmark_features;
CREATE FUNCTION benchmark_tile() RETURNS bytea LANGUAGE plpgsql STABLE AS $$
DECLARE result bytea;
BEGIN
  {_query(workload)} INTO result;
  RETURN result;
END $$;
"""
            self._psql(setup)
            self.process = subprocess.Popen(
                [
                    'docker',
                    'exec',
                    '-i',
                    '-e',
                    'PGOPTIONS=-c jit=off',
                    self.name,
                    'taskset',
                    '--cpu-list',
                    str(self.cpu),
                    'psql',
                    '-X',
                    '-q',
                    '-v',
                    'ON_ERROR_STOP=1',
                    '-U',
                    'postgres',
                    '-d',
                    'postgres',
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
            )
            self.tile()
            self.tile()
            settings = json.loads(
                self._psql(
                    "SELECT json_build_object('postgres', current_setting('server_version'), "
                    "'jit', current_setting('jit'), 'postgis', postgis_full_version())::text;"
                )
            )
            if settings['jit'] != 'off':
                raise RuntimeError('PostgreSQL JIT was not disabled')
            explain = json.loads(
                self._psql(
                    f'EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) {_query(workload)}'
                )
            )[0]
            if not _uses_index(explain['Plan'], 'benchmark_features_geom_idx'):
                raise RuntimeError('PostGIS fixture did not use its spatial index')
            self.metadata = {
                'image': _IMAGE,
                'image_id': _docker(
                    'inspect', '--format', '{{.Image}}', self.name
                ).strip(),
                'postgres': settings['postgres'],
                'postgis': settings['postgis'],
                'jit': settings['jit'],
                'worker_affinity': self.worker_affinity,
                'postgres_affinity': affinity,
                'server_execution_ms': explain['Execution Time'],
                'selection_index': 'benchmark_features_geom_idx',
                'transport': 'persistent docker exec psql, binary COPY',
                'query_plan': 'cached PL/pgSQL statement after warmup',
                'setup_seconds': time.perf_counter() - started,
            }
        except BaseException:
            self.close()
            raise

    def _wait_ready(self) -> None:
        marker = b'PostgreSQL init process complete; ready for start up.'
        ready = b'database system is ready to accept connections'
        deadline = time.monotonic() + 120
        watcher = subprocess.Popen(
            ['docker', 'logs', '--follow', self.name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert watcher.stdout is not None
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(watcher.stdout, selectors.EVENT_READ)
                output = bytearray()
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not selector.select(remaining):
                        raise TimeoutError('PostGIS did not finish initialization')
                    chunk = os.read(watcher.stdout.fileno(), 8192)
                    if not chunk:
                        raise RuntimeError('PostGIS stopped before becoming ready')
                    output.extend(chunk)
                    marker_at = output.find(marker)
                    if marker_at >= 0 and ready in output[marker_at + len(marker) :]:
                        return
        finally:
            watcher.terminate()
            try:
                watcher.wait(timeout=5)
            except subprocess.TimeoutExpired:
                watcher.kill()
                watcher.wait()
            watcher.stdout.close()

    def _psql(self, sql: str) -> str:
        return _docker(
            'exec',
            '-i',
            '-e',
            'PGOPTIONS=-c jit=off',
            self.name,
            'taskset',
            '--cpu-list',
            str(self.cpu),
            'psql',
            '-X',
            '-qAt',
            '-v',
            'ON_ERROR_STOP=1',
            '-U',
            'postgres',
            '-d',
            'postgres',
            input=sql,
        ).strip()

    def tile(self) -> bytes:
        assert self.process is not None
        assert self.process.stdin is not None
        assert self.process.stdout is not None
        self.process.stdin.write(
            b'COPY (SELECT benchmark_tile()) TO STDOUT WITH (FORMAT binary);\n'
        )
        self.process.stdin.flush()

        def read(count: int) -> bytes:
            value = self.process.stdout.read(count)
            if len(value) != count:
                raise EOFError('PostGIS psql exited during binary tile output')
            return value

        if read(19) != b'PGCOPY\n\xff\r\n\x00' + bytes(8):
            raise RuntimeError('invalid PostgreSQL binary COPY header')
        if struct.unpack('!h', read(2))[0] != 1:
            raise RuntimeError('PostgreSQL returned an unexpected row')
        length = struct.unpack('!i', read(4))[0]
        if length < 0:
            raise RuntimeError('PostGIS returned a null tile')
        payload = read(length)
        if struct.unpack('!h', read(2))[0] != -1:
            raise RuntimeError('PostgreSQL returned multiple tile rows')
        return payload

    def close(self) -> None:
        process, self.process = self.process, None
        try:
            if process is not None:
                if process.stdin is not None:
                    try:
                        process.stdin.close()
                    except BrokenPipeError:
                        pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                if process.stdout is not None:
                    process.stdout.close()
        finally:
            if self.started:
                result = subprocess.run(
                    ['docker', 'rm', '-f', self.name],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
                if result.returncode and 'No such container' not in result.stderr:
                    raise RuntimeError(
                        f'could not remove PostGIS container: {result.stderr.strip()}'
                    )
                self.started = False
            atexit.unregister(self.close)
